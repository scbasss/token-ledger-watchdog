"""Tests for Layer 2: the sync client in ccledger.py.

The reference server is not part of this repository. End-to-end tests run
against StubEndpoint below, a minimal endpoint that enforces the same payload
contract.

Run: python -m unittest -v test_layer2
"""
from __future__ import annotations

import functools
import getpass
import hashlib
import http.server
import io
import json
import os
import shutil
import socket
import tempfile
import re
import threading
import unittest
from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import ccledger as cl
from test_ccledger import OPUS, SONNET, LedgerCase, line

# The privacy guarantee, written out a second time on purpose: changing the
# contract constants alone must fail these tests. Edit both, deliberately.
CONTRACT_ROLLUP_KEYS = {
    "date", "model", "project", "calls", "input_tokens", "output_tokens",
    "cache_read_tokens", "cache_write_5m_tokens", "cache_write_1h_tokens",
    "cost_stored_usd", "cost_calculated_usd",
}
CONTRACT_ENVELOPE_KEYS = {"device_name", "schema_version", "rollups"}

UTC = timezone.utc
TODAY = date(2026, 10, 31)
CFG = cl.SyncConfig("http://127.0.0.1:1", "wdk_test", "test-pc")


def ts(day: str, hhmm: str = "12:00") -> str:
    return f"{day}T{hhmm}:00.000Z"


def run(fn, *a, **k):
    out, err = io.StringIO(), io.StringIO()
    with redirect_stdout(out), redirect_stderr(err):
        rv = fn(*a, **k)
    return rv, out.getvalue(), err.getvalue()


class SyncCase(LedgerCase):
    """A small ledger spread over four UTC days, today = 2026-10-31."""

    def seed(self):
        self.write("projA/sess-one.jsonl", [
            line("msg_1", "req_1", "sess-one", ts("2026-10-28", "10:00"), inp=100, out=50, cr=1000, c5=200, c1=300),
            line("msg_2", "req_2", "sess-one", ts("2026-10-28", "11:00"), model=SONNET, inp=10, out=5),
            line("msg_4", "req_4", "sess-one", ts("2026-10-30", "09:00"), inp=7, out=3),
            line("msg_5", "req_5", "sess-one", ts("2026-10-31", "08:00"), inp=1, out=2),
        ])
        self.write("projB/sess-two.jsonl", [
            line("msg_3", "req_3", "sess-two", ts("2026-10-29", "23:30"), inp=40, out=20),
        ])
        self.ingest()
        self.conn = cl.connect(self.db)
        self.addCleanup(self.conn.close)

    def setUp(self):
        super().setUp()
        self.seed()

    def sync(self, post, dry_run=False, today=TODAY, cfg=CFG):
        return run(cl.sync_ledger, self.conn, UTC, cfg, dry_run, today, post,
                   salt_path=self.tmp / cl.SYNC_SALT_FILE)

    def rollups(self, after=None, today=TODAY, tz=UTC):
        return cl.daily_rollups(self.conn, tz, after, today)

    def sent_names(self):
        """Encoded project -> the name sync sends, with this ledger's salt."""
        projects = [r[0] for r in self.conn.execute("SELECT DISTINCT project FROM billable")]
        return cl.project_display_names(projects, cl.load_project_salt(self.tmp / cl.SYNC_SALT_FILE))


class PayloadContractTests(SyncCase):
    def test_exact_key_sets(self):
        batches = cl.build_batches(self.rollups(), "pc")
        self.assertEqual(len(batches), 1)
        self.assertEqual(set(batches[0]), CONTRACT_ENVELOPE_KEYS)
        self.assertEqual(batches[0]["schema_version"], 1)
        self.assertTrue(batches[0]["rollups"])
        for r in batches[0]["rollups"]:
            self.assertEqual(set(r), CONTRACT_ROLLUP_KEYS)
        self.assertEqual(set(cl.ROLLUP_FIELDS), CONTRACT_ROLLUP_KEYS)
        self.assertEqual(set(cl.SYNC_ENVELOPE_FIELDS), CONTRACT_ENVELOPE_KEYS)

    def test_no_identifiers_or_content_in_payload(self):
        text = json.dumps(cl.build_batches(self.rollups(), "pc"))
        for forbidden in ("sess-one", "sess-two", "msg_", "req_", "SECRET-CONTENT", "session",
                          str(self.tmp), "jsonl", "ts_utc", "request"):
            self.assertNotIn(forbidden, text)

    def test_value_types(self):
        for r in self.rollups():
            self.assertRegex(r["date"], r"^\d{4}-\d{2}-\d{2}$")
            for f in ("calls", "input_tokens", "output_tokens", "cache_read_tokens",
                      "cache_write_5m_tokens", "cache_write_1h_tokens"):
                self.assertIs(type(r[f]), int)
            for f in ("cost_stored_usd", "cost_calculated_usd"):
                self.assertIs(type(r[f]), float)

    def test_rollups_sum_to_the_ledger_report(self):
        agg = cl._agg(self.conn, "0", "9")
        rs = self.rollups()
        self.assertEqual(sum(r["calls"] for r in rs), agg["calls"])
        self.assertEqual(sum(r["input_tokens"] for r in rs), agg["input"])
        self.assertEqual(sum(r["output_tokens"] for r in rs), agg["output"])
        self.assertEqual(sum(r["cache_write_5m_tokens"] for r in rs), agg["cw5m"])
        self.assertEqual(sum(r["cache_write_1h_tokens"] for r in rs), agg["cw1h"])
        self.assertEqual(sum(r["cache_read_tokens"] for r in rs), agg["cread"])
        self.assertAlmostEqual(sum(r["cost_calculated_usd"] for r in rs), agg["calc"], places=9)
        self.assertEqual(sum(r["cost_stored_usd"] for r in rs), 0)
        first = next(r for r in rs if r["date"] == "2026-10-28" and r["model"] == OPUS)
        self.assertEqual((first["calls"], first["input_tokens"], first["cache_write_1h_tokens"]), (1, 100, 300))

    def test_stored_and_calculated_costs_stay_separate(self):
        self.write("projA/sess-three.jsonl", [
            line("msg_9", "req_9", "sess-three", ts("2026-10-30", "10:00"), cost=1.25),
            line("msg_10", "req_10", "sess-three", ts("2026-10-30", "10:05"), inp=1000, out=1000)])
        self.ingest()
        day = [r for r in self.rollups() if r["date"] == "2026-10-30" and r["project"] == "projA"]
        self.assertEqual(len(day), 1)
        self.assertEqual(day[0]["cost_stored_usd"], 1.25)
        self.assertGreater(day[0]["cost_calculated_usd"], 0)

    def test_days_are_local_not_utc(self):
        self.assertEqual({r["date"] for r in self.rollups(tz=UTC)},
                         {"2026-10-28", "2026-10-29", "2026-10-30", "2026-10-31"})
        # The only 10-29 call is 23:30Z: at UTC+9 it is 08:30 on the 30th.
        east = self.rollups(tz=timezone(timedelta(hours=9)), today=date(2026, 11, 1))
        self.assertEqual({r["date"] for r in east}, {"2026-10-28", "2026-10-30", "2026-10-31"})
        self.assertEqual({r["project"] for r in east if r["date"] == "2026-10-30"}, {"projA", "projB"})
        # 02:00Z on the 30th is the evening of the 29th at UTC-5.
        self.write("projA/late.jsonl", [line("msg_l", "req_l", "late", ts("2026-10-30", "02:00"))])
        self.ingest()
        west = self.rollups(tz=timezone(timedelta(hours=-5)))
        self.assertTrue(any(r["date"] == "2026-10-29" and r["project"] == "projA" for r in west))

    def test_sidechain_replays_are_not_double_counted(self):
        self.write("projA/side.jsonl", [
            line("msg_1", "req_replay", "sess-one", ts("2026-10-28", "10:00"), inp=100, out=50, cr=1000,
                 c5=200, c1=300, side=True)])
        self.ingest()
        day = [r for r in self.rollups() if r["date"] == "2026-10-28" and r["model"] == OPUS]
        self.assertEqual(day[0]["calls"], 1)

    def test_batches_never_split_a_date(self):
        rs = self.rollups()
        for size in (1, 2, 3, 100):
            batches = cl.build_batches(rs, "pc", batch_rows=size)
            self.assertEqual([r for b in batches for r in b["rollups"]], rs)
            dates = [{r["date"] for r in b["rollups"]} for b in batches]
            for i, a in enumerate(dates):
                for b in dates[i + 1:]:
                    self.assertFalse(a & b, f"a date was split at batch_rows={size}")


class WatermarkTests(SyncCase):
    def test_first_sync_sends_everything_and_sets_watermark_to_yesterday(self):
        sent = []
        rc, out, _ = self.sync(lambda ep, key, b: sent.append((ep, key, b)) or len(b["rollups"]))
        self.assertEqual(rc, 0)
        self.assertEqual(len(sent), 1)
        self.assertEqual(sent[0][1], "wdk_test")
        self.assertEqual(sent[0][0], "http://127.0.0.1:1/v1/rollups")
        self.assertEqual({r["date"] for r in sent[0][2]["rollups"]},
                         {"2026-10-28", "2026-10-29", "2026-10-30", "2026-10-31"})
        self.assertEqual(cl.read_watermark(self.conn), date(2026, 10, 30))
        self.assertIn("watermark 2026-10-30", out)

    def test_second_sync_sends_only_today(self):
        post = lambda ep, key, b: len(b["rollups"])  # noqa: E731
        self.sync(post)
        sent = []
        self.sync(lambda ep, key, b: sent.append(b) or len(b["rollups"]))
        self.assertEqual({r["date"] for b in sent for r in b["rollups"]}, {"2026-10-31"})

    def test_growing_today_is_resent_with_new_totals(self):
        sent = []
        post = lambda ep, key, b: sent.append(b) or len(b["rollups"])  # noqa: E731
        self.sync(post)
        self.write("projA/more.jsonl", [line("msg_6", "req_6", "more", ts("2026-10-31", "09:00"))])
        self.ingest()
        sent.clear()
        self.sync(post)
        today_rows = [r for b in sent for r in b["rollups"] if r["project"] == self.sent_names()["projA"]]
        self.assertEqual(today_rows[0]["calls"], 2)

    def test_next_day_completes_yesterday_then_advances(self):
        post = lambda ep, key, b: len(b["rollups"])  # noqa: E731
        self.sync(post)
        self.write("projA/nov.jsonl", [line("msg_7", "req_7", "nov", ts("2026-11-01", "07:00"))])
        self.ingest()
        sent = []
        self.sync(lambda ep, key, b: sent.append(b) or len(b["rollups"]), today=date(2026, 11, 1))
        self.assertEqual({r["date"] for b in sent for r in b["rollups"]}, {"2026-10-31", "2026-11-01"})
        self.assertEqual(cl.read_watermark(self.conn), date(2026, 10, 31))

    def test_failure_does_not_advance_the_watermark(self):
        def boom(ep, key, b):
            raise cl.SyncError("could not reach 127.0.0.1: refused")
        rc, out, err = self.sync(boom)
        self.assertEqual(rc, 1)
        self.assertIsNone(cl.read_watermark(self.conn))
        self.assertIn("sync failed", err)
        self.assertIn("watermark not advanced", err)

    def test_server_accepting_fewer_rows_than_sent_is_a_failure(self):
        rc, _, err = self.sync(lambda ep, key, b: len(b["rollups"]) - 1)
        self.assertEqual(rc, 1)
        self.assertIsNone(cl.read_watermark(self.conn))
        self.assertIn("accepted", err)

    def test_partial_failure_across_batches_does_not_advance(self):
        calls = []

        def flaky(ep, key, b):
            calls.append(len(b["rollups"]))
            if len(calls) == 2:
                raise cl.SyncError("server returned HTTP 503")
            return len(b["rollups"])
        small = functools.partial(cl.build_batches, batch_rows=1)
        with mock.patch.object(cl, "build_batches", small):
            rc, _, _ = self.sync(flaky)
        self.assertEqual(rc, 1)
        self.assertEqual(len(calls), 2)  # stopped at the first failure
        self.assertIsNone(cl.read_watermark(self.conn))

    def test_dry_run_sends_nothing_and_prints_the_exact_payload(self):
        def never(*a):
            raise AssertionError("dry run must not send")
        rc, out, _ = self.sync(never, dry_run=True)
        self.assertEqual(rc, 0)
        want = cl.publish_projects(self.rollups(), self.sent_names())
        self.assertEqual(json.loads(out), cl.build_batches(want, "test-pc")[0])
        self.assertIsNone(cl.read_watermark(self.conn))

    def test_dry_run_works_unconfigured(self):
        rc, out, _ = self.sync(None, dry_run=True, cfg=cl.SyncConfig(None, None, "pc"))
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(out)["device_name"], "pc")

    def test_unconfigured_says_so_and_exits_zero(self):
        rc, out, _ = self.sync(None, cfg=cl.SyncConfig(None, None, "pc"))
        self.assertEqual((rc, out.strip()), (0, "sync not configured"))

    def test_nothing_to_send(self):
        self.conn.execute("DELETE FROM calls")
        rc, out, _ = self.sync(None)
        self.assertEqual((rc, out.strip()), (0, "sync: nothing to send"))

    def test_corrupt_watermark_means_full_resync(self):
        self.conn.execute("INSERT OR REPLACE INTO meta VALUES (?, 'not-a-date')", (cl.SYNC_WATERMARK_KEY,))
        self.assertIsNone(cl.read_watermark(self.conn))
        self.assertEqual(len(self.rollups(after=cl.read_watermark(self.conn))), len(self.rollups()))

    def test_a_future_watermark_cannot_hide_today(self):
        rs = self.rollups(after=date(2030, 1, 1))
        self.assertEqual({r["date"] for r in rs}, {"2026-10-31"})

    def test_sync_leaves_the_calls_untouched(self):
        before = self.conn.execute("SELECT * FROM calls ORDER BY dedupe_key").fetchall()
        self.sync(lambda ep, key, b: len(b["rollups"]))
        self.assertEqual(before, self.conn.execute("SELECT * FROM calls ORDER BY dedupe_key").fetchall())


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def write(self, obj):
        (self.tmp / "sync.json").write_text(obj if isinstance(obj, str) else json.dumps(obj), encoding="utf-8")

    def test_nothing_configured(self):
        cfg = cl.load_sync_config(self.tmp, env={})
        self.assertEqual((cfg.url, cfg.key), (None, None))
        self.assertEqual(cfg.device_name, "unnamed-device")

    def test_default_device_name_never_identifies_the_machine(self):
        cfg = cl.load_sync_config(self.tmp, env={})
        for ident in {socket.gethostname(), os.environ.get("USERNAME"),
                      os.environ.get("USER"), os.environ.get("COMPUTERNAME")} - {None, ""}:
            self.assertNotIn(ident.lower(), cfg.device_name.lower())

    def test_file_values(self):
        self.write({"url": "http://h:1", "key": "k1", "device_name": "from-file"})
        self.assertEqual(cl.load_sync_config(self.tmp, env={}), cl.SyncConfig("http://h:1", "k1", "from-file"))

    def test_env_overrides_file(self):
        self.write({"url": "http://h:1", "key": "k1", "device_name": "from-file"})
        env = {"CCLEDGER_SYNC_URL": "http://e:2", "CCLEDGER_SYNC_KEY": "k2", "CCLEDGER_DEVICE_NAME": "from-env"}
        self.assertEqual(cl.load_sync_config(self.tmp, env=env), cl.SyncConfig("http://e:2", "k2", "from-env"))

    def test_bad_config_is_a_clear_error(self):
        for bad in ("{not json", "[1]", json.dumps({"key": 5}), json.dumps({"key": "has space"}),
                    json.dumps({"device_name": "x" * 500})):
            self.write(bad)
            with self.assertRaises(SystemExit, msg=bad):
                cl.load_sync_config(self.tmp, env={})

    def test_shipped_example_is_placeholders_only(self):
        ex = json.loads((Path(__file__).parent / "sync.example.json").read_text(encoding="utf-8"))
        self.assertEqual(set(ex), {"url", "key", "device_name"})
        self.assertTrue(ex["url"].startswith("http://127.0.0.1"))
        self.assertIn("PASTE", ex["key"])

    def test_endpoint_normalisation(self):
        want = "http://127.0.0.1:8787/v1/rollups"
        for given in ("http://127.0.0.1:8787", "http://127.0.0.1:8787/", want, want + "/"):
            self.assertEqual(cl.sync_endpoint(given), want)
        self.assertEqual(cl.sync_endpoint("https://x.example/base"), "https://x.example/base/v1/rollups")

    def test_endpoint_rejects_other_schemes_and_credentials(self):
        for bad in ("file:///etc/passwd", "ftp://h/x", "http://", "h:8787", "http://u:p@h:1",
                    "http://h:1?x=1", "http://h:1#f", "http://h:notaport"):
            with self.assertRaises(cl.SyncError, msg=bad):
                cl.sync_endpoint(bad)

    def test_plain_http_to_a_remote_host_warns(self):
        # sync_ledger warns before sending the key anywhere but loopback.
        self.assertTrue(cl._is_loopback("127.0.0.1"))
        self.assertTrue(cl._is_loopback("localhost"))
        self.assertTrue(cl._is_loopback("::1"))
        self.assertFalse(cl._is_loopback("example.com"))
        self.assertFalse(cl._is_loopback("10.0.0.5"))


def rrow(**over):
    row = {"date": "2026-10-30", "model": OPUS, "project": "projA", "calls": 3, "input_tokens": 100,
           "output_tokens": 50, "cache_read_tokens": 1000, "cache_write_5m_tokens": 20,
           "cache_write_1h_tokens": 30, "cost_stored_usd": 0.0, "cost_calculated_usd": 1.5}
    row.update(over)
    return row


def batch(*rows, **over):
    b = {"device_name": "pc", "schema_version": 1, "rollups": list(rows) or [rrow()]}
    b.update(over)
    return b


class StubEndpoint:
    """A minimal endpoint implementing the payload contract, for client tests.

    Bearer-key auth (401 otherwise), the exact key sets above (400 naming the
    field otherwise), no repeated (date, model, project) in a batch (400), and
    the rows of every date in a batch replaced as a unit. `force` makes every
    POST answer with a fixed (status, json body) instead.
    """

    def __init__(self, key):
        self.key = key
        self.rows: dict[tuple, dict] = {}  # (date, model, project) -> row
        self.force = None
        stub = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("Content-Length") or 0))
                status, reply = stub.handle(self.path, self.headers.get("Authorization"), body)
                data = json.dumps(reply).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, *a):
                pass

        self.httpd = http.server.HTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        threading.Thread(target=functools.partial(self.httpd.serve_forever, 0.05), daemon=True).start()

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()

    def handle(self, path, auth, body):
        if self.force:
            return self.force
        if path != cl.SYNC_PATH:
            return 404, {"error": "not found"}
        if auth != f"Bearer {self.key}":
            return 401, {"error": "unauthorized"}
        try:
            obj = json.loads(body)
        except ValueError:
            return 400, {"error": "body is not JSON"}
        if not isinstance(obj, dict) or set(obj) != CONTRACT_ENVELOPE_KEYS:
            return 400, {"error": "envelope fields differ from the contract"}
        keys = set()
        for i, r in enumerate(obj["rollups"]):
            if set(r) != CONTRACT_ROLLUP_KEYS:
                extra = ", ".join(sorted(set(r) - CONTRACT_ROLLUP_KEYS)) or "none"
                return 400, {"error": f"rollups[{i}] fields differ from the contract (extra: {extra})"}
            k = (r["date"], r["model"], r["project"])
            if k in keys:
                return 400, {"error": f"rollups[{i}] repeats a (date, model, project)"}
            keys.add(k)
        dates = {r["date"] for r in obj["rollups"]}
        self.rows = {k: v for k, v in self.rows.items() if k[0] not in dates}
        self.rows.update({(r["date"], r["model"], r["project"]): r for r in obj["rollups"]})
        return 200, {"accepted": len(obj["rollups"])}


HASHED = re.compile(r"h:[0-9a-f]{16}")
SALT = bytes(range(32))


class ProjectNameTests(unittest.TestCase):
    """Real folders under a temp dir, resolved by the decoder the stall alert uses.

    The username is the real one, read at run time, so it never lands in the repo.
    """

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.user = getpass.getuser()
        self.home = self.tmp / "Users" / self.user
        self.dirs = {
            "dotclaude": self.home / ".claude",
            "desktop": self.home / "OneDrive" / "Desktop" / "token ledger+watchdog",
            "app1": self.home / "code" / "app",
            "app2": self.tmp / "work" / "app",
            "named": self.home / "projects" / f"{self.user}-notes",
            "home": self.home,
        }
        for d in self.dirs.values():
            d.mkdir(parents=True, exist_ok=True)
        self.dirs["vanished"] = self.home / "Desktop" / "gone-project"  # never created
        self.enc = {k: cl.encode_project(str(d)) for k, d in self.dirs.items()}

    def names(self, salt=SALT):
        return cl.project_display_names(self.enc.values(), salt, home=self.home)

    def test_folder_names_hashes_and_suffixes(self):
        n = {k: self.names()[e] for k, e in self.enc.items()}
        self.assertEqual(n["dotclaude"], ".claude")
        self.assertEqual(n["desktop"], "token ledger+watchdog")
        for k in ("app1", "app2"):
            self.assertRegex(n[k], r"^app·[0-9a-f]{4}$")
        self.assertNotEqual(n["app1"], n["app2"])
        for k in ("vanished", "home", "named"):  # gone, the home folder, contains the username
            self.assertRegex(n[k], HASHED, k)
            self.assertTrue(HASHED.fullmatch(n[k]), k)
        self.assertEqual(len(set(n.values())), len(n))

    def test_no_value_carries_the_user_a_drive_or_a_path(self):
        idents = {self.user, Path.home().name, socket.gethostname(), os.environ.get("COMPUTERNAME")} - {None, ""}
        for v in self.names().values():
            for ident in idents:
                self.assertNotIn(ident.casefold(), v.casefold())
            self.assertNotIn("/", v)
            self.assertNotIn("\\", v)
            self.assertNotIn(":", v.removeprefix("h:"))  # no drive letter
            self.assertNotRegex(v, r"(?i)\b[a-z]--")  # no encoded drive prefix either
            self.assertTrue(v.isprintable() and v)

    def test_username_match_is_case_insensitive(self):
        upper = self.home / "other" / self.user.upper()
        upper.mkdir(parents=True)
        enc = cl.encode_project(str(upper))
        self.assertRegex(cl.project_display_names([enc], SALT, home=self.home)[enc], HASHED)

    def test_same_salt_is_stable_and_a_new_salt_changes_every_hash(self):
        self.assertEqual(self.names(), self.names())
        a, b = self.names(), self.names(bytes(32))
        for k in ("vanished", "home", "named", "app1", "app2"):
            self.assertNotEqual(a[self.enc[k]], b[self.enc[k]], k)
        for k in ("dotclaude", "desktop"):
            self.assertEqual(a[self.enc[k]], b[self.enc[k]], k)
        hashed = a[self.enc["vanished"]]
        want = "h:" + hashlib.sha256(SALT + self.enc["vanished"].encode()).hexdigest()[:16]
        self.assertEqual(hashed, want)

    def test_suffixes_depend_on_the_whole_set_not_the_window(self):
        alone = cl.project_display_names([self.enc["app1"]], SALT, home=self.home)
        self.assertEqual(alone[self.enc["app1"]], "app")  # the documented rename limit

    def test_salt_file_is_created_once_and_reused(self):
        path = self.tmp / cl.SYNC_SALT_FILE
        first = cl.load_project_salt(path)
        self.assertEqual(len(first), 32)
        self.assertEqual(cl.load_project_salt(path), first)
        self.assertEqual(path.read_text(encoding="ascii").strip(), first.hex())

    def test_a_damaged_salt_is_an_error_not_a_silent_reset(self):
        path = self.tmp / cl.SYNC_SALT_FILE
        path.write_text("not hex\n", encoding="ascii")
        with self.assertRaisesRegex(cl.SyncError, "damaged"):
            cl.load_project_salt(path)
        self.assertEqual(path.read_text(encoding="ascii"), "not hex\n")


class ProjectSyncTests(SyncCase):
    def salt_hex(self):
        return (self.tmp / cl.SYNC_SALT_FILE).read_text(encoding="ascii").strip()

    def test_dry_run_sends_hashes_and_never_prints_the_salt(self):
        rc, out, err = self.sync(None, dry_run=True)
        self.assertEqual(rc, 0)
        projects = {r["project"] for b in map(json.loads, [out]) for r in b["rollups"]}
        self.assertEqual(len(projects), 2)
        for p in projects:  # projA/projB are not real folders, so both are hashed
            self.assertRegex(p, HASHED)
        self.assertNotIn(self.salt_hex(), out + err)
        self.assertNotIn(self.salt_hex().upper(), out + err)
        rc, again, _ = self.sync(None, dry_run=True)
        self.assertEqual(again, out)
        # The ledger itself is untouched: local views keep the encoded names.
        self.assertEqual({r[0] for r in self.conn.execute("SELECT DISTINCT project FROM calls")},
                         {"projA", "projB"})

    def test_unconfigured_sync_creates_no_salt(self):
        self.sync(None, cfg=cl.SyncConfig(None, None, "pc"))
        self.assertFalse((self.tmp / cl.SYNC_SALT_FILE).exists())

    def test_a_damaged_salt_fails_the_sync_before_sending(self):
        (self.tmp / cl.SYNC_SALT_FILE).write_text("", encoding="ascii")
        sent = []
        rc, _, err = self.sync(lambda e, k, b: sent.append(b) or len(b["rollups"]))
        self.assertEqual((rc, sent), (1, []))
        self.assertIn("damaged", err)
        self.assertIsNone(cl.read_watermark(self.conn))

    def test_renaming_moves_no_token_or_cent(self):
        raw = self.rollups()
        names = cl.project_display_names([r["project"] for r in raw], SALT)
        sent = cl.publish_projects(raw, names)
        self.assertEqual(len(sent), len(raw))
        for a, b in zip(raw, sent):
            self.assertEqual({k: v for k, v in a.items() if k != "project"},
                             {k: v for k, v in b.items() if k != "project"})
            self.assertEqual(b["project"], names[a["project"]])


class ProjectEndpointTests(unittest.TestCase):
    def test_colliding_base_names_stay_separate_at_the_endpoint(self):
        stub = StubEndpoint("wdk_test")
        self.addCleanup(stub.close)
        a, b = "C--one-app", "C--two-app"
        folders = {a: Path("one") / "app", b: Path("two") / "app"}
        names = cl.project_display_names([a, b], SALT, resolve=folders.get, home=Path("home"), usernames=())
        self.assertNotEqual(names[a], names[b])
        sent = cl.publish_projects([rrow(project=a, calls=3), rrow(project=b, calls=5)], names)
        for _ in range(2):
            self.assertEqual(cl.post_batch(stub.url + cl.SYNC_PATH, "wdk_test", batch(*sent)), 2)
        self.assertEqual(sorted((r["project"], r["calls"]) for r in stub.rows.values()),
                         sorted([(names[a], 3), (names[b], 5)]))


class ClientServerTests(unittest.TestCase):
    """The real client, through the CLI, against StubEndpoint over HTTP.

    The client's clock is pinned (no flake across midnight) and the caller's
    CCLEDGER_* variables are hidden (a developer's real sync config must never
    be used by a test).
    """
    NOW = datetime(2026, 10, 31, 12, 0, tzinfo=UTC)

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.key = "wdk_" + "k" * 43  # a stand-in shaped like a real device key
        self.stub = StubEndpoint(self.key)
        self.addCleanup(self.stub.close)
        self.url = self.stub.url
        self.root = self.tmp / "projects"
        now = self.NOW
        two_days = (now - timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        now_s = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        (self.root / "projX").mkdir(parents=True)
        (self.root / "projX" / "s1.jsonl").write_text("\n".join([
            line("msg_1", "req_1", "s1", two_days, inp=11, out=22, cr=33),
            line("msg_2", "req_2", "s1", now_s, inp=1, out=2, cr=3)]) + "\n", encoding="utf-8")
        self.ldb = self.tmp / "ledger.db"
        self.env = {"CCLEDGER_SYNC_URL": self.url, "CCLEDGER_SYNC_KEY": self.key,
                    "CCLEDGER_DEVICE_NAME": "test-pc"}

    def cli(self, *args, env=None):
        base = ["--db", str(self.ldb), "--tz", "UTC"]
        tail = ["--root", str(self.root), "--offline", "--prices", str(self.tmp / "prices.json")]
        with mock.patch.dict(os.environ, {}, clear=False):
            for k in [k for k in os.environ if k.startswith("CCLEDGER_")]:
                del os.environ[k]
            os.environ.update(env if env is not None else self.env)
            with mock.patch.object(cl, "local_now", lambda tz: self.NOW.astimezone(tz)):
                return run(cl.main, [*base, *args, *tail])

    def server_rows(self):
        return sorted((r["date"], r["calls"], r["input_tokens"]) for r in self.stub.rows.values())

    def watermark(self):
        c = cl.connect(self.ldb, readonly=True)
        try:
            r = c.execute("SELECT value FROM meta WHERE key = ?", (cl.SYNC_WATERMARK_KEY,)).fetchone()
            return r[0] if r else None
        finally:
            c.close()

    def test_end_to_end_push_then_idempotent_second_push(self):
        rc, out, _ = self.cli("sync")
        self.assertEqual(rc, 0, out)
        first = self.server_rows()
        self.assertEqual([(c, i) for _, c, i in first], [(1, 11), (1, 1)])
        self.assertEqual(self.watermark(), "2026-10-30")
        rc, out, _ = self.cli("sync", "--no-ingest")
        self.assertEqual(rc, 0)
        self.assertIn("pushed 1 rollup rows", out)  # only the still-open day
        self.assertEqual(self.server_rows(), first)

    def test_dry_run_through_the_cli_sends_nothing(self):
        rc, out, _ = self.cli("sync", "--dry-run")
        self.assertEqual(rc, 0)
        self.assertEqual(set(json.loads(out)), CONTRACT_ENVELOPE_KEYS)
        self.assertEqual(self.server_rows(), [])
        self.assertIsNone(self.watermark())

    def test_dry_run_payload_carries_no_machine_or_user_identity(self):
        env = {k: v for k, v in self.env.items() if k != "CCLEDGER_DEVICE_NAME"}
        rc, out, _ = self.cli("sync", "--dry-run", env=env)
        self.assertEqual(rc, 0)
        self.assertEqual(json.loads(out)["device_name"], "unnamed-device")
        for ident in {socket.gethostname(), os.environ.get("USERNAME"), os.environ.get("USER"),
                      os.environ.get("COMPUTERNAME"), str(Path.home())} - {None, ""}:
            self.assertNotIn(ident.lower(), out.lower())

    def test_unconfigured_cli_says_so_before_touching_the_ledger(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            rc, out, _ = run(cl.main, ["--db", str(self.ldb), "sync"])
            self.assertEqual((rc, out.strip()), (0, "sync not configured"))
            self.assertFalse(self.ldb.exists())  # not even an ingest ran
            self.assertEqual(self.cli("ingest", env={})[0], 0)
            rc, out, _ = run(cl.main, ["--db", str(self.ldb), "sync", "--no-ingest"])
            self.assertEqual((rc, out.strip()), (0, "sync not configured"))

    def test_server_down_is_a_clean_error_and_reports_still_work(self):
        # Bound but never listening, and held for the whole test: the port is
        # refused and cannot be reused (or self-connected to) meanwhile.
        s = socket.socket()
        self.addCleanup(s.close)
        s.bind(("127.0.0.1", 0))
        dead = s.getsockname()[1]
        env = {**self.env, "CCLEDGER_SYNC_URL": f"http://127.0.0.1:{dead}"}
        rc, _, err = self.cli("sync", env=env)
        self.assertEqual(rc, 1)
        self.assertIn("sync failed: could not reach", err)
        self.assertNotIn("Traceback", err)
        self.assertNotIn(self.key, err)
        self.assertIsNone(self.watermark())
        rc, out, _ = self.cli("report", "today", "--no-ingest")
        self.assertEqual(rc, 0)
        self.assertIn("Calls:", out)

    def test_wrong_key_is_401_and_stores_nothing(self):
        rc, _, err = self.cli("sync", env={**self.env, "CCLEDGER_SYNC_KEY": "wdk_wrong"})
        self.assertEqual(rc, 1)
        self.assertIn("HTTP 401", err)
        self.assertEqual(self.server_rows(), [])
        self.assertIsNone(self.watermark())

    def test_server_error_is_a_failure_that_advances_nothing(self):
        self.stub.force = (503, {"error": "overloaded"})
        rc, out, err = self.cli("sync")
        self.assertEqual(rc, 1)
        self.assertIn("HTTP 503: overloaded", err)
        self.assertNotIn(self.key, out + err)
        self.assertEqual(self.server_rows(), [])
        self.assertIsNone(self.watermark())

    def test_server_error_detail_is_shown(self):
        # A contract-enforcing endpoint's reply: patch the client to send an extra field.
        real = cl.build_batches

        def leaky(*a, **k):
            bs = real(*a, **k)
            bs[0]["rollups"][0]["session_id"] = "abc"
            return bs
        with mock.patch.object(cl, "build_batches", leaky):
            rc, _, err = self.cli("sync")
        self.assertEqual(rc, 1)
        self.assertIn("HTTP 400", err)
        self.assertIn("session_id", err)
        self.assertEqual(self.server_rows(), [])
        self.assertIsNone(self.watermark())

    def test_redirects_are_not_followed_so_the_key_cannot_leak(self):
        seen = []

        class Sink(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                seen.append(self.headers.get("Authorization"))
                self.send_response(200)
                self.send_header("Content-Length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *a):
                pass

        sink = http.server.HTTPServer(("127.0.0.1", 0), Sink)
        threading.Thread(target=functools.partial(sink.serve_forever, 0.05), daemon=True).start()
        self.addCleanup(lambda: (sink.shutdown(), sink.server_close()))
        target = f"http://127.0.0.1:{sink.server_address[1]}/v1/rollups"

        class Redirector(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(307)
                self.send_header("Location", target)
                self.send_header("Content-Length", "0")
                self.end_headers()

            def log_message(self, *a):
                pass

        red = http.server.HTTPServer(("127.0.0.1", 0), Redirector)
        threading.Thread(target=functools.partial(red.serve_forever, 0.05), daemon=True).start()
        self.addCleanup(lambda: (red.shutdown(), red.server_close()))
        env = {**self.env, "CCLEDGER_SYNC_URL": f"http://127.0.0.1:{red.server_address[1]}"}
        rc, _, err = self.cli("sync", env=env)
        self.assertEqual(rc, 1)
        self.assertIn("HTTP 307", err)
        self.assertEqual(seen, [])

    def test_non_http_urls_are_refused(self):
        rc, _, err = self.cli("sync", env={**self.env, "CCLEDGER_SYNC_URL": "file:///etc/passwd"})
        self.assertEqual(rc, 1)
        self.assertIn("sync failed", err)

    def test_garbage_replies_are_failures(self):
        class Junk(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.send_response(200)
                self.send_header("Content-Length", "6")
                self.end_headers()
                self.wfile.write(b"hello!")

            def log_message(self, *a):
                pass

        junk = http.server.HTTPServer(("127.0.0.1", 0), Junk)
        threading.Thread(target=functools.partial(junk.serve_forever, 0.05), daemon=True).start()
        self.addCleanup(lambda: (junk.shutdown(), junk.server_close()))
        rc, _, err = self.cli("sync", env={**self.env, "CCLEDGER_SYNC_URL": f"http://127.0.0.1:{junk.server_address[1]}"})
        self.assertEqual(rc, 1)
        self.assertIn("accepted", err)
        self.assertIsNone(self.watermark())

    def test_reconciles_with_the_local_report_exactly(self):
        self.assertEqual(self.cli("sync")[0], 0)
        fields = ("calls", "input_tokens", "output_tokens", "cache_read_tokens", "cache_write_5m_tokens",
                  "cache_write_1h_tokens", "cost_stored_usd", "cost_calculated_usd")
        got = tuple(sum(r[f] for r in self.stub.rows.values()) for f in fields)
        lc = cl.connect(self.ldb, readonly=True)
        a = cl._agg(lc, "0", "9")
        lc.close()
        self.assertEqual(got[:6], (a["calls"], a["input"], a["output"], a["cread"], a["cw5m"], a["cw1h"]))
        self.assertEqual(got[6], a["stored"])
        self.assertAlmostEqual(got[7], a["calc"], places=9)


if __name__ == "__main__":
    unittest.main()
