"""Tests for ccledger. Run: python -m unittest -v test_ccledger"""
from __future__ import annotations

import io
import json
import os
import shutil
import sqlite3
import tempfile
import time
import tracemalloc
import unittest
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import ccledger as cl

OPUS = "claude-opus-5-5"
SONNET = "claude-sonnet-5"


def line(mid="msg_1", rid="req_1", sid="s1", ts="2026-10-30T12:00:00.000Z", model=OPUS,
         inp=10, out=5, cr=0, cc=0, c5=None, c1=None, side=False, speed=None, cost=None,
         drop=(), **extra) -> str:
    usage = {"input_tokens": inp, "output_tokens": out, "cache_read_input_tokens": cr,
             "cache_creation_input_tokens": cc}
    if c5 is not None or c1 is not None:
        usage["cache_creation"] = {"ephemeral_5m_input_tokens": c5 or 0,
                                   "ephemeral_1h_input_tokens": c1 or 0}
    if speed:
        usage["speed"] = speed
    usage.update(extra.pop("usage_extra", {}))
    d = {"timestamp": ts, "sessionId": sid, "requestId": rid, "isSidechain": side,
         "message": {"id": mid, "model": model, "usage": usage,
                     "content": [{"type": "text", "text": "SECRET-CONTENT"}]}}
    if cost is not None:
        d["costUSD"] = cost
    d.update(extra)
    for k in drop:
        d.pop(k, None)
    return json.dumps(d, separators=(",", ":"))


class LedgerCase(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.root = self.tmp / "projects"
        self.root.mkdir()
        self.db = self.tmp / "ledger.db"
        self.prices = cl.PriceTable(cl.bundled_raw(), "bundled")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def write(self, rel: str, lines, raw: bytes | None = None) -> Path:
        p = self.root / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(raw if raw is not None else ("\n".join(lines) + "\n").encode())
        return p

    def ingest(self, prices=None):
        conn = cl.connect(self.db)
        try:
            return cl.ingest(conn, [self.root], prices or self.prices)
        finally:
            conn.close()

    def q(self, sql, params=()):
        conn = sqlite3.connect(self.db)
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()


class DedupTests(LedgerCase):
    def test_streaming_duplicates_keep_greatest_total_in_any_order(self):
        self.write("p/s1.jsonl", [line(out=1), line(out=50), line(out=7)])
        st = self.ingest()
        self.assertEqual((st.rows_added, st.replaced), (1, 1))
        self.assertEqual(self.q("SELECT output_tokens FROM calls"), [(50,)])

    def test_reingest_is_noop_and_totals_identical(self):
        self.write("p/s1.jsonl", [line(out=1), line(out=50), line(mid="msg_2", rid="req_2")])
        self.ingest()
        before = self.q("SELECT * FROM calls ORDER BY dedupe_key")
        st = self.ingest()
        self.assertEqual((st.rows_added, st.replaced), (0, 0))
        self.assertEqual(before, self.q("SELECT * FROM calls ORDER BY dedupe_key"))

    def test_request_id_is_part_of_identity(self):
        self.write("p/s1.jsonl", [line(rid="req_a"), line(rid="req_b")])
        self.assertEqual(self.ingest().rows_added, 2)

    def test_requestless_msg_id_collapses_per_session_across_timestamps(self):
        self.write("p/s1.jsonl", [
            line(rid=None, drop=("requestId",), ts="2026-10-30T12:00:00Z"),
            line(rid=None, drop=("requestId",), ts="2026-10-30T12:00:05Z", out=9),
            line(rid=None, drop=("requestId",), sid="s2"),
        ])
        self.ingest()
        self.assertEqual(self.q("SELECT session_id, output_tokens FROM calls ORDER BY session_id"),
                         [("s1", 9), ("s2", 5)])

    def test_gateway_ids_without_request_id_scope_by_timestamp(self):
        self.write("p/s1.jsonl", [
            line(mid="gw-same", drop=("requestId",), ts="2026-10-30T12:00:00Z"),
            line(mid="gw-same", drop=("requestId",), ts="2026-10-30T12:01:00Z"),
        ])
        self.assertEqual(self.ingest().rows_added, 2)

    def test_missing_or_empty_message_id_is_undupable_not_inserted(self):
        a = json.loads(line()); del a["message"]["id"]
        b = json.loads(line()); b["message"]["id"] = ""
        self.write("p/s1.jsonl", [json.dumps(a), json.dumps(b), json.dumps(a)])
        st = self.ingest()
        self.assertEqual((st.undupable, st.rows_added), (3, 0))

    def test_global_dedup_across_roots_and_files(self):
        self.write("p/s1.jsonl", [line(out=3)])
        other = self.tmp / "other" / "projects"
        (other / "p").mkdir(parents=True)
        (other / "p" / "s1.jsonl").write_text(line(out=8) + "\n")
        conn = cl.connect(self.db)
        try:
            st = cl.ingest(conn, [self.root, other], self.prices)
        finally:
            conn.close()
        self.assertEqual((st.rows_added, st.replaced), (1, 1))
        self.assertEqual(self.q("SELECT output_tokens FROM calls"), [(8,)])

    def test_tie_prefers_speed_then_non_sidechain(self):
        self.write("p/s1.jsonl", [line(side=True), line(side=False)])
        self.ingest()
        self.assertEqual(self.q("SELECT is_sidechain FROM calls"), [(0,)])
        self.write("p/s1.jsonl", [line(mid="msg_9", rid="r9"), line(mid="msg_9", rid="r9", speed="standard")])
        self.ingest()
        self.assertEqual(self.q("SELECT speed FROM calls WHERE message_id='msg_9'"), [("standard",)])

    def test_sidechain_replay_excluded_from_billable(self):
        self.write("p/s1.jsonl", [line(rid="req_parent")])
        self.write("p/s1/subagents/agent-1.jsonl", [line(rid="req_replay", side=True),
                                                    line(mid="msg_own", rid="r_own", side=True)])
        self.ingest()
        self.assertEqual(self.q("SELECT COUNT(*) FROM calls"), [(3,)])
        self.assertEqual(self.q("SELECT message_id FROM billable ORDER BY message_id"),
                         [("msg_1",), ("msg_own",)])


class ParseTests(LedgerCase):
    def test_malformed_lines_are_skipped_and_ingest_continues(self):
        bad = [
            '{"message":{"usage":{"input_tokens":1}',  # truncated (live write)
            line(inp=True),  # bool is not a token count
            line(inp=-3),
            line(inp=10**13),
            line(ts="2026-10-30T12:00:00"),  # naive: zone unknown
            line(ts="1969-12-31T23:59:59Z"),
            line(ts="garbage"),
            '{"message":{"id":"msg_x","usage":{"input_tokens":NaN}},"timestamp":"2026-10-30T12:00:00Z"}',
            '{"message":{"id":"msg_x","usage":{"input_tokens":1e400}},"timestamp":"2026-10-30T12:00:00Z"}',
        ]
        raw = ("\n".join(bad) + "\n").encode() + b'{"usage":{\xff\xfe}\n' + (line(mid="msg_ok") + "\n").encode()
        self.write("p/s1.jsonl", None, raw=raw)
        st = self.ingest()
        self.assertEqual(st.rows_added, 1)
        self.assertEqual(sum(st.skipped.values()), 10)
        self.assertEqual(st.skipped["bad_tokens"], 4)

    def test_prefilter_skips_non_usage_lines_and_ignores_tool_result_usage(self):
        tool = {"type": "user", "toolUseResult": {"usage": {"input_tokens": 999}},
                "message": {"role": "user", "content": "x"}, "timestamp": "2026-10-30T12:00:00Z"}
        self.write("p/s1.jsonl", ['{"type":"summary"}', json.dumps(tool, separators=(",", ":")), line()])
        st = self.ingest()
        self.assertEqual((st.lines, st.candidates, st.non_usage, st.rows_added), (3, 2, 1, 1))

    def test_oversized_line_is_drained_without_breaking_next_line(self):
        big = '{"usage":{"x":"' + "a" * 5000 + '"}}'
        self.write("p/s1.jsonl", [big, line()])
        with mock.patch.object(cl, "MAX_LINE_BYTES", 1024):
            st = self.ingest()
        self.assertEqual((st.skipped["oversized_line"], st.rows_added), (1, 1))

    def test_no_content_is_ever_stored(self):
        self.write("p/s1.jsonl", [line()])
        self.ingest()
        self.assertNotIn(b"SECRET-CONTENT", self.db.read_bytes())

    def test_project_and_session_attribution_from_path(self):
        self.assertEqual(cl.attribute(["C--a-b", "abc.jsonl"]), ("C--a-b", "abc"))
        self.assertEqual(cl.attribute(["C--a-b", "abc", "subagents", "agent-1.jsonl"]), ("C--a-b", "abc"))
        self.write("C--x-proj/sess-from-path.jsonl", [line(drop=("sessionId",))])
        self.ingest()
        self.assertEqual(self.q("SELECT project, session_id FROM calls"), [("C--x-proj", "sess-from-path")])

    def test_timestamps_normalize_to_fixed_width_utc(self):
        self.assertEqual(cl.parse_ts("2026-11-01T01:30:00-04:00"), "2026-11-01T05:30:00.000000Z")
        self.assertEqual(cl.parse_ts("2026-11-01T05:30:00.123456789Z"), "2026-11-01T05:30:00.123456Z")

    def test_cache_split_nested_wins_and_never_drops_tokens(self):
        self.assertEqual(cl._cache_split({"cache_creation_input_tokens": 30}), (30, 0))
        u = {"cache_creation_input_tokens": 30,
             "cache_creation": {"ephemeral_5m_input_tokens": 10, "ephemeral_1h_input_tokens": 20}}
        self.assertEqual(cl._cache_split(u), (10, 20))
        u["cache_creation_input_tokens"] = 35
        self.assertEqual(cl._cache_split(u), (15, 20))

    def test_advisor_iterations_add_rows_but_message_iterations_do_not(self):
        it = [{"type": "message", "input_tokens": 10, "output_tokens": 5},
              {"type": "advisor_message", "model": "claude-fable-5-1", "input_tokens": 100, "output_tokens": 50}]
        self.write("p/s1.jsonl", [line(usage_extra={"iterations": it})])
        self.ingest()
        self.assertEqual(self.q("SELECT message_id, model, input_tokens FROM calls ORDER BY message_id"),
                         [("msg_1", OPUS, 10), ("msg_1:advisor:1", "claude-fable-5-1", 100)])


class PricingTests(unittest.TestCase):
    def setUp(self):
        self.t = cl.PriceTable(cl.bundled_raw(), "bundled")

    def cost(self, model, speed=None, inp=0, out=0, c5=0, c1=0, cr=0, table=None):
        return (table or self.t).calculate(model, speed, inp, out, c5, c1, cr)

    def test_basic_and_cache_tiers(self):
        m = 1_000_000
        self.assertAlmostEqual(self.cost(OPUS, inp=m, out=m)[0], 24.0)
        self.assertAlmostEqual(self.cost(OPUS, c5=m)[0], 5.0)   # 1.25x input
        self.assertAlmostEqual(self.cost(OPUS, c1=m)[0], 8.0)   # 2x input
        self.assertAlmostEqual(self.cost(OPUS, cr=m)[0], 0.20)  # explicit, not 0.1x
        self.assertAlmostEqual(self.cost(SONNET, cr=m)[0], 0.20)  # 0.1x fallback

    def test_model_matching_rules(self):
        t = cl.PriceTable({"claude-opus-4": {"input": 15e-6, "output": 75e-6},
                           "claude-opus-4-5-20251101": {"input": 5e-6, "output": 25e-6},
                           "gpt-4-0613": {"input": 1e-6, "output": 1e-6}}, "t")
        self.assertEqual(t.resolve("claude-opus-4-5").input, 5e-6)  # dated key answers undated
        self.assertEqual(t.resolve("claude-opus-4-20250514").input, 15e-6)
        self.assertEqual(t.resolve("claude-opus-4-2025-05-14").input, 15e-6)
        self.assertIsNone(t.resolve("claude-opus-4-6"))  # never cross a version boundary
        self.assertEqual(t.resolve("claude-opus-4-thinking").input, 15e-6)
        self.assertEqual(cl.strip_date_suffix("gpt-4-0613"), "gpt-4-0613")
        self.assertEqual(t.resolve("anthropic/claude-opus-4-5@20251101").input, 5e-6)

    def test_unpriced_and_synthetic(self):
        self.assertEqual(self.cost("glm-4.6", inp=100), (0.0, cl.NOTE_UNPRICED))
        self.assertEqual(self.cost(cl.SYNTHETIC, inp=100), (0.0, cl.NOTE_UNPRICED))

    def test_above_200k_is_marginal_per_field(self):
        t = cl.PriceTable({"m": {"input": 1e-6, "output": 1e-6, "input_200k": 2e-6}}, "t")
        self.assertAlmostEqual(self.cost("m", inp=300_000, table=t)[0], 0.2 + 0.2)
        self.assertAlmostEqual(self.cost("m", c1=300_000, table=t)[0], 0.4 + 0.4)  # 1h above = 2x input_200k

    def test_fast_mode(self):
        base = self.cost(OPUS, inp=1000)[0]
        self.assertAlmostEqual(self.cost(OPUS, "fast", inp=1000)[0], base * 2)
        c, note = self.cost("claude-sonnet-4-5", "fast", inp=1000)
        self.assertEqual(note, cl.NOTE_FAST_STANDARD)
        t = cl.PriceTable({"m": {"input": 1e-6, "output": 1e-6}, "m-fast": {"input": 9e-6, "output": 9e-6}}, "t")
        self.assertAlmostEqual(self.cost("m", "fast", inp=1000, table=t)[0], 0.009)

    def test_long_context_flag(self):
        self.assertIsNone(self.cost("claude-haiku-5-5", inp=50_000)[1])
        self.assertEqual(self.cost("claude-haiku-5-5", inp=50_000, cr=60_000)[1], cl.NOTE_LONG_CTX)

    def test_litellm_filter_and_merge(self):
        data = {
            "claude-opus-5-5": {"input_cost_per_token": 4e-6, "output_cost_per_token": 2e-5,
                                "provider_specific_entry": {"fast": 2.0}},
            "anthropic/claude-opus-5-5": {"input_cost_per_token": 9e-6, "output_cost_per_token": 9e-6},
            "bedrock/claude-x": {"input_cost_per_token": 1e-6, "output_cost_per_token": 1e-6},
            "claude-broken": {"input_cost_per_token": "free"},
            "claude-nan": {"input_cost_per_token": float("inf"), "output_cost_per_token": 1e-6},
            "gpt-5": {"input_cost_per_token": 1e-6, "output_cost_per_token": 1e-6},
        }
        f = cl.filter_litellm(data)
        self.assertEqual(set(f), {"claude-opus-5-5"})
        self.assertEqual(f["claude-opus-5-5"]["input"], 4e-6)
        merged = cl.merge_prices(cl.bundled_raw(), f)
        self.assertAlmostEqual(merged["claude-opus-5-5"]["cache_read"], 0.20e-6)  # bundled fills the gap

    def test_garbage_fetch_falls_back_and_never_zero_costs(self):
        tmp = Path(tempfile.mkdtemp())
        try:
            for body in (b"<html>rate limited</html>", b'{"gpt-5": {}}', b'{"claude-x": {"input_cost_per_token": 1e-6'):
                resp = mock.MagicMock()
                resp.__enter__.return_value.read.return_value = body
                with mock.patch.object(cl.urllib.request, "urlopen", return_value=resp):
                    msgs = []
                    t = cl.load_prices(tmp / "prices.json", offline=False, refresh=True, log=msgs.append)
                self.assertEqual(t.source, "bundled snapshot")
                self.assertIn("price refresh failed", msgs[0])
                self.assertGreater(t.calculate(OPUS, None, 1000, 0, 0, 0, 0)[0], 0)
            self.assertFalse((tmp / "prices.json").exists())
            (tmp / "prices.json").write_text("{truncated")
            self.assertIsNone(cl.read_price_cache(tmp / "prices.json"))
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_good_fetch_is_cached_atomically_and_reused_offline(self):
        tmp = Path(tempfile.mkdtemp())
        try:
            body = json.dumps({"claude-new-9": {"input_cost_per_token": 1e-6, "output_cost_per_token": 2e-6}}).encode()
            resp = mock.MagicMock()
            resp.__enter__.return_value.read.return_value = body
            with mock.patch.object(cl.urllib.request, "urlopen", return_value=resp):
                cl.load_prices(tmp / "prices.json", offline=False, refresh=True, log=print)
            t = cl.load_prices(tmp / "prices.json", offline=True, refresh=False, log=print)
            self.assertIsNotNone(t.resolve("claude-new-9"))
            self.assertIsNotNone(t.resolve(OPUS))
            self.assertEqual([p.name for p in tmp.iterdir()], ["prices.json"])  # no temp leftovers
        finally:
            shutil.rmtree(tmp, ignore_errors=True)

    def test_memo_is_bounded(self):
        with mock.patch.object(cl, "MEMO_LIMIT", 10):
            for i in range(100):
                self.t.resolve(f"unknown-{i}")
            self.assertLessEqual(len(self.t._memo), 10)


class CostSourceTests(LedgerCase):
    def test_stored_cost_wins_and_bad_cost_is_ignored(self):
        self.write("p/s1.jsonl", [line(mid="msg_a", rid="a", cost=0.5),
                                  line(mid="msg_b", rid="b", cost=-1),
                                  line(mid="msg_c", rid="c", model=cl.SYNTHETIC, cost=None)])
        st = self.ingest()
        rows = dict((r[0], r[1:]) for r in self.q("SELECT message_id, cost_usd, cost_source, price_note FROM calls"))
        self.assertEqual(rows["msg_a"], (0.5, "stored", None))
        self.assertEqual(rows["msg_b"][1], "calculated")
        self.assertEqual(rows["msg_c"], (0.0, "calculated", cl.NOTE_SYNTHETIC))
        self.assertEqual(st.bad_cost_ignored, 1)


class ReportTests(LedgerCase):
    def test_period_bounds(self):
        self.assertEqual(cl.period_days("week", date(2026, 11, 1)), (date(2026, 10, 26), date(2026, 11, 2)))
        self.assertEqual(cl.period_days("month", date(2026, 12, 15)), (date(2026, 12, 1), date(2027, 1, 1)))

    def test_day_bucketing_uses_local_zone_not_utc(self):
        est = timezone(timedelta(hours=-5))
        # 02:00Z on Nov 2 is 21:00 Nov 1 local: must land on Nov 1.
        self.assertEqual(cl.local_midnight_utc(date(2026, 11, 2), est), "2026-11-02T05:00:00.000000Z")
        self.assertLess("2026-11-02T02:00:00.000000Z", cl.local_midnight_utc(date(2026, 11, 2), est))

    def test_reports_run_on_readonly_db(self):
        self.write("p/s1.jsonl", [line(), line(mid="msg_2", rid="r2", model="mystery-model")])
        self.ingest()
        conn = cl.connect(self.db, readonly=True)
        try:
            buf = io.StringIO()
            with redirect_stdout(buf):
                cl.report_period(conn, "all", timezone.utc)
                cl.report_sessions(conn, timezone.utc, 5)
                cl.report_projects(conn)
                self.assertEqual(cl.report_schema(conn), 0)
            self.assertIn("unpriced models ($0): ['mystery-model']", buf.getvalue())
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute("DELETE FROM calls")
        finally:
            conn.close()

    def test_interrupted_ingest_rolls_back(self):
        self.write("p/s1.jsonl", [line(mid=f"msg_{i}", rid=f"r{i}") for i in range(10)])
        with mock.patch.object(cl, "BATCH_ROWS", 3), \
             mock.patch.object(cl, "attribute", side_effect=[("p", "s1"), KeyboardInterrupt]):
            self.write("p/s2.jsonl", [line(mid="msg_z", rid="rz")])
            with self.assertRaises(KeyboardInterrupt):
                self.ingest()
        self.assertEqual(self.q("SELECT COUNT(*) FROM calls"), [(0,)])


class MemoryTests(LedgerCase):
    def test_peak_memory_is_bounded_by_batch_not_history(self):
        n = 40_000
        with open(self.root / "big.jsonl", "w") as fh:
            for i in range(n):
                fh.write(line(mid=f"msg_{i}", rid=f"r{i}") + "\n")
        size_mb = (self.root / "big.jsonl").stat().st_size / 1e6
        tracemalloc.start()
        try:
            st = self.ingest()
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(st.rows_added, n)
        self.assertLess(peak / 1e6, 8, f"peak {peak / 1e6:.1f} MB for a {size_mb:.0f} MB input")


def iso(dt: datetime) -> str:
    return cl.fmt_utc(dt)


def snapshot(db: Path):
    conn = sqlite3.connect(db)
    try:
        return conn.execute("SELECT * FROM calls ORDER BY dedupe_key").fetchall()
    finally:
        conn.close()


class IncrementalTests(LedgerCase):
    def test_appends_read_only_new_bytes(self):
        p = self.write("p/s1.jsonl", [line(mid="msg_a", rid="a")])
        self.ingest()
        size = p.stat().st_size
        with open(p, "a") as fh:
            fh.write(line(mid="msg_b", rid="b") + "\n")
        st = self.ingest()
        self.assertEqual((st.rows_added, st.files_read, st.bytes_skipped), (1, 1, size))
        st = self.ingest()  # unchanged file: not opened at all
        self.assertEqual((st.files_read, st.bytes_skipped), (0, p.stat().st_size))

    def test_partial_last_line_waits_for_its_newline(self):
        full = line(mid="msg_a", rid="a")
        p = self.write("p/s1.jsonl", None, raw=full[:40].encode())
        self.assertEqual(self.ingest().rows_added, 0)
        with open(p, "ab") as fh:
            fh.write(full[40:].encode())  # complete JSON, still no newline
        self.assertEqual(self.ingest().rows_added, 0)
        with open(p, "ab") as fh:
            fh.write(b"\n")
        st = self.ingest()
        self.assertEqual((st.rows_added, sum(st.skipped.values())), (1, 0))

    def test_truncation_and_in_place_rewrite_reset_to_zero(self):
        p = self.write("p/s1.jsonl", [line(mid=f"msg_{i}", rid=f"r{i}") for i in range(3)])
        self.ingest()
        p.write_text(line(mid="msg_new", rid="rn") + "\n")  # smaller: truncated/rotated
        st = self.ingest()
        self.assertEqual((st.checkpoint_resets, st.rows_added), (1, 1))
        long = [line(mid=f"msg_w{i}", rid=f"w{i}", out=12345) for i in range(4)]
        p.write_text("\n".join(long) + "\n")  # larger and different: tail hash catches it
        st = self.ingest()
        self.assertEqual((st.checkpoint_resets, st.rows_added), (1, 4))

    def test_wiping_checkpoints_changes_nothing(self):
        self.write("p/s1.jsonl", [line(out=1), line(out=50), line(mid="msg_2", rid="r2")])
        self.write("p/s1/subagents/agent-1.jsonl", [line(mid="msg_3", rid="r3", side=True)])
        self.ingest()
        before = snapshot(self.db)
        conn = sqlite3.connect(self.db)
        conn.execute("DELETE FROM file_state")
        conn.commit()
        conn.close()
        st = self.ingest()
        self.assertEqual((st.rows_added, st.replaced, st.bytes_skipped), (0, 0, 0))
        self.assertEqual(before, snapshot(self.db))
        self.assertEqual(self.ingest().rows_added, 0)

    def test_checkpoints_of_deleted_files_are_pruned(self):
        p = self.write("p/s1.jsonl", [line()])
        self.ingest()
        p.unlink()
        self.ingest()
        self.assertEqual(self.q("SELECT COUNT(*) FROM file_state"), [(0,)])
        self.assertEqual(self.q("SELECT COUNT(*) FROM calls"), [(1,)])  # history is kept

    def test_migrates_v1_ledger_in_place(self):
        conn = sqlite3.connect(self.db, isolation_level=None)
        conn.executescript(cl.SCHEMA_SQL)
        conn.execute("PRAGMA user_version = 1")
        conn.execute(cl.UPSERT_SQL, cl._make_row(self.prices, "msg_old", "r", "2026-10-01T00:00:00.000000Z",
                                                 "s", "p", OPUS, 0, None, {"input_tokens": 5}, None))
        conn.close()
        self.write("p/s1.jsonl", [line()])
        self.ingest()
        self.assertEqual(self.q("PRAGMA user_version"), [(2,)])
        self.assertEqual(self.q("SELECT value FROM meta WHERE key='schema_version'"), [("2",)])
        self.assertEqual(self.q("SELECT COUNT(*) FROM calls"), [(2,)])
        cols = [r[1] for r in self.q("PRAGMA table_info(file_state)")]
        self.assertEqual(cols, ["path", "size", "mtime", "offset", "tail_sha"])


class WarmupTests(LedgerCase):
    def run_main(self, *args) -> str:
        buf = io.StringIO()
        with redirect_stdout(buf), mock.patch("sys.stderr", io.StringIO()):
            cl.main(["--db", str(self.db), "--tz", "UTC", *args, "--root", str(self.root),
                     "--offline", "--prices", str(self.tmp / "prices.json")])
        return buf.getvalue()

    def test_past_month_selects_that_month_only(self):
        self.write("p/s1.jsonl", [line(mid="msg_sep", rid="a", ts="2026-09-30T23:59:59Z"),
                                  line(mid="msg_oct1", rid="b", ts="2026-10-01T00:00:00Z"),
                                  line(mid="msg_oct2", rid="c", ts="2026-10-31T23:59:59Z"),
                                  line(mid="msg_nov", rid="d", ts="2026-11-01T00:00:00Z")])
        out = self.run_main("report", "month", "2026-10")
        self.assertIn("month: 2026-10-01 -> 2026-10-31", out)
        self.assertIn("Calls: 2 ", out)
        with self.assertRaises(SystemExit):
            self.run_main("report", "month", "2026-13")

    def test_report_ingests_first_unless_told_not_to(self):
        now = datetime.now(timezone.utc)
        p = self.write("p/s1.jsonl", [line(mid="msg_a", rid="a", ts=iso(now))])
        self.assertIn("Calls: 1 ", self.run_main("report", "all"))
        with open(p, "a") as fh:
            fh.write(line(mid="msg_b", rid="b", ts=iso(now)) + "\n")
        self.assertIn("Calls: 1 ", self.run_main("report", "all", "--no-ingest"))
        self.assertIn("Calls: 2 ", self.run_main("report", "all"))


class BlockTests(unittest.TestCase):
    EDT = timezone(timedelta(hours=-4))

    def at(self, hhmm: str, day: int = 30) -> datetime:
        h, m = map(int, hhmm.split(":"))
        return datetime(2026, 10, day, h, m, tzinfo=self.EDT).astimezone(timezone.utc)

    def blocks(self, *times):
        return list(cl.iter_blocks((self.at(t), 10, 1.0) for t in times))

    def test_brief_example_anchor_and_new_block(self):
        b = self.blocks("09:12", "11:00", "13:30", "19:00")
        self.assertEqual(len(b), 2)
        self.assertEqual(b[0].start.astimezone(self.EDT).strftime("%H:%M"), "09:00")
        self.assertEqual((b[0].calls, b[0].cost), (3, 3.0))
        self.assertEqual(b[1].start.astimezone(self.EDT).strftime("%H:%M"), "19:00")

    def test_more_than_5h_after_start_without_a_gap(self):
        b = self.blocks("09:12", "11:00", "13:30", "14:30")  # 14:30 is 5h30m after 09:00, 1h after 13:30
        self.assertEqual([x.calls for x in b], [3, 1])
        self.assertEqual(b[1].start.astimezone(self.EDT).strftime("%H:%M"), "14:00")

    def test_exactly_5h_stays_in_block(self):
        self.assertEqual(len(self.blocks("09:00", "14:00")), 1)

    def test_active_detection(self):
        b = self.blocks("09:12", "10:00")[0]
        self.assertTrue(b.is_active(self.at("11:00")))  # last call 1h old, inside window
        self.assertFalse(b.is_active(self.at("16:00")))  # last call 6h old
        self.assertFalse(b.is_active(self.at("14:01")))  # window over though last call is 4h old


class WatchTests(LedgerCase):
    def setUp(self):
        super().setUp()
        self.proj = self.tmp / "work dir+x" / "proj.v2"
        (self.proj / "src").mkdir(parents=True)
        (self.proj / "src" / "main.py").write_text("print(1)\n")
        (self.proj / ".git").mkdir()
        self.encoded = cl.encode_project(str(self.proj.resolve()))
        self.session = self.root / self.encoded / "sess-1.jsonl"
        self.session.parent.mkdir(parents=True)
        self.session.write_text("")
        self.age_project(3600)
        self.conn = cl.connect(self.db)
        self.w = cl.Watcher(self.conn, [self.root], self.prices, timezone.utc, 1.0)

    def tearDown(self):
        self.conn.close()
        super().tearDown()

    def age_project(self, seconds: float):
        old = time.time() - seconds
        for dirpath, dirnames, filenames in os.walk(self.proj):
            for n in dirnames + filenames:
                os.utime(os.path.join(dirpath, n), (old, old))

    def append(self, *lines_, raw: bytes = b""):
        with open(self.session, "ab") as fh:
            fh.write("".join(x + "\n" for x in lines_).encode() + raw)

    def costly(self, i: int, cost: float = 0.6, ago: float = 120) -> str:
        ts = iso(datetime.now(timezone.utc) - timedelta(seconds=ago))
        return line(mid=f"msg_{i}", rid=f"r{i}", sid="sess-1", ts=ts, cost=cost)

    def test_decodes_lossy_project_name_from_names_only(self):
        self.assertEqual(cl.decode_project_dir(self.encoded), self.proj.resolve())
        self.assertIsNone(cl.decode_project_dir(self.encoded + "-missing"))

    def test_batches_show_up_within_one_poll_and_partial_line_waits(self):
        self.assertNotIn("totals", self.w.poll())
        self.append(self.costly(1, 0.1), self.costly(2, 0.1))
        self.assertEqual(self.w.poll()["totals"]["calls"], 2)
        tail = self.costly(3, 0.1)
        self.append(raw=tail[:50].encode())
        self.assertEqual(self.w.poll()["totals"]["calls"], 2)
        self.append(raw=tail[50:].encode() + b"\n")
        s = self.w.poll()
        self.assertEqual(s["totals"]["calls"], 3)
        self.assertTrue(any("session sess-1" in ln for ln in self.w.render(s)))

    def test_stall_alert_fires_once_with_evidence_then_clears_on_progress(self):
        self.append(self.costly(1), self.costly(2))  # $1.20 in the window, nothing touched
        s = self.w.poll()
        self.assertEqual(s["alert"]["changed"], 0)
        self.assertAlmostEqual(s["alert"]["spent"], 1.2)
        self.assertTrue(s["bell"])
        text = "\n".join(self.w.render(s))
        self.assertIn("$1.20 in 10 min, 0 project files changed", text)
        self.assertFalse(self.w.poll()["bell"])  # same episode: no second bell
        (self.proj / "src" / "main.py").write_text("print(2)\n")
        s = self.w.poll()
        self.assertIsNone(s["alert"])
        self.assertFalse(self.w.alerting)

    def test_no_alert_when_a_file_changes_each_minute(self):
        for minute in range(10):
            self.age_project(3600)
            old = time.time() - 60
            os.utime(self.proj / "src" / "main.py", (old, old))
            self.append(self.costly(minute, 0.3, ago=600 - minute * 60))
            self.assertIsNone(self.w.poll()["alert"])

    def test_no_alert_below_threshold_and_ignored_dirs_are_not_progress(self):
        (self.proj / "src" / "__pycache__").mkdir()
        own = self.proj / "ledger.db"
        own.write_text("x")
        self.age_project(3600)
        self.append(self.costly(1, 0.4))
        self.assertIsNone(self.w.poll()["alert"])
        # Churn that is not progress: git internals, bytecode, ccledger's own DB.
        (self.proj / ".git" / "index").write_text("x")
        (self.proj / "src" / "__pycache__" / "main.pyc").write_text("x")
        own.write_text("y")
        self.w.excluded = frozenset({os.path.normcase(str(own.resolve()))})
        self.append(self.costly(2, 0.8))
        self.assertIsNotNone(self.w.poll()["alert"])

    def test_unknown_project_dir_disables_check_instead_of_alerting(self):
        other = self.root / "Z--no-such-dir" / "s.jsonl"
        other.parent.mkdir()
        other.write_text(line(mid="msg_z", rid="z", sid="s", ts=iso(datetime.now(timezone.utc)), cost=5) + "\n")
        s = self.w.poll()
        self.assertIsNone(s["alert"])
        self.assertIn("project directory not found", s["note"])

    def test_report_block_on_live_data(self):
        self.append(self.costly(1, 0.5, ago=60))
        self.w.poll()
        buf = io.StringIO()
        with redirect_stdout(buf):
            cl.report_block(self.conn, timezone.utc)
        out = buf.getvalue()
        self.assertIn("ACTIVE", out)
        self.assertIn("projection at current $0.1000/min", out)  # $0.50 over the last 5 min
        self.assertIn("(estimate)", out)
        self.assertIn("not quota", out)


if __name__ == "__main__":
    unittest.main()
