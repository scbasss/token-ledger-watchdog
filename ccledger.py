#!/usr/bin/env python3
"""ccledger - read-only Claude Code token ledger (Watchdog Layer 1A).

Parses Claude Code transcripts into a local SQLite ledger and reports usage
in tokens and dollar-equivalent per session / day / project. Stdlib only.

Privacy invariant: only whitelisted numeric and ID fields are read from a
transcript line. message.content and every other text field are never
accessed, stored, or printed - debug output is counts and IDs only.

Usage:
    python ccledger.py ingest [--offline] [--refresh-prices] [--root DIR ...]
    python ccledger.py report today|week|all|block
    python ccledger.py report month [YYYY-MM]
    python ccledger.py report sessions [--limit N]
    python ccledger.py report projects
    python ccledger.py watch [--poll 3] [--stall-usd 1.00]
    python ccledger.py schema
Global options: --db PATH (or CCLEDGER_DB), --tz IANA_NAME.
Reports ingest incrementally first; --no-ingest skips that.
Data lives in %LOCALAPPDATA%\\ccledger (or $XDG_DATA_HOME/ccledger elsewhere).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sqlite3
import sys
import tempfile
import time
import urllib.request
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

def _data_dir() -> Path:
    # Kept out of synced folders (OneDrive locks and uploads files it watches).
    if os.name == "nt" and os.environ.get("LOCALAPPDATA"):
        return Path(os.environ["LOCALAPPDATA"]) / "ccledger"
    base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
    return Path(base) / "ccledger"


DATA_DIR = _data_dir()
DEFAULT_DB = DATA_DIR / "ledger.db"
DEFAULT_PRICES = DATA_DIR / "prices.json"
LITELLM_URL = (
    "https://raw.githubusercontent.com/BerriAI/litellm/main/"
    "model_prices_and_context_window.json"
)
SCHEMA_VERSION = 2

# Only lines containing one of these byte strings are JSON-parsed. The spaced
# form never appears in current transcripts but costs nothing to accept.
USAGE_MARKERS = (b'"usage":{', b'"usage": {')
MAX_LINE_BYTES = 64 * 1024 * 1024  # bounds memory per line; real max seen ~1.3 MB
MAX_TOKENS_FIELD = 10**10  # a single call can't plausibly exceed this
MAX_ID_LEN = 512
MAX_ITERATIONS = 64  # bound work on a pathological iterations[] array
BATCH_ROWS = 2000
PRICE_TTL_SECONDS = 24 * 3600
MAX_PRICE_DOWNLOAD = 64 * 1024 * 1024
FETCH_TIMEOUT_SECONDS = 20
TIER_THRESHOLD = 200_000
MEMO_LIMIT = 4096
TAIL_CHECK_BYTES = 1024  # fingerprint before a checkpoint, to spot in-place rewrites

BLOCK = timedelta(hours=5)
BURN_WINDOW = timedelta(minutes=5)
STALL_WINDOW = timedelta(minutes=10)
WALK_LIMIT = 200_000  # project-tree entries scanned per stall check
# Never progress: VCS internals, Claude's own state (per brief), and caches
# or environments that tools rewrite on their own.
PRUNE_DIRS = frozenset({
    ".git", ".claude", "node_modules", "__pycache__", ".venv", "venv",
    ".mypy_cache", ".pytest_cache", ".ruff_cache", ".tox",
})

SYNTHETIC = "<synthetic>"
UNKNOWN_MODEL = "<unknown>"
NOTE_UNPRICED = "unpriced"
NOTE_SYNTHETIC = "synthetic"
NOTE_FAST_STANDARD = "fast@standard"
NOTE_LONG_CTX = "long_ctx@base"

# Offline price floor, USD per million tokens. Snapshot 2026-11-01 of
# Anthropic's first-party rates. Cache fields are explicit wherever the
# published rate differs from the default ratios (5m write 1.25x, 1h write
# 2x, read 0.1x input) - Opus 5.5 and Fable 5.1 reads are 0.05x and 0.025x.
BUNDLED_PRICES_PER_MTOK: dict[str, dict[str, float]] = {
    "claude-fable-5-1": {"input": 10.0, "output": 50.0, "cache_read": 0.25},
    "claude-mythos-5-1": {"input": 10.0, "output": 50.0, "cache_read": 0.25},
    "claude-fable-5": {"input": 10.0, "output": 50.0, "cache_read": 1.0},
    "claude-mythos-5": {"input": 10.0, "output": 50.0, "cache_read": 1.0},
    "claude-opus-5-5": {"input": 4.0, "output": 20.0, "cache_read": 0.20, "fast_multiplier": 2.0},
    "claude-opus-5": {"input": 5.0, "output": 25.0, "fast_multiplier": 2.0},
    "claude-opus-4-8": {"input": 5.0, "output": 25.0},
    "claude-opus-4-7": {"input": 5.0, "output": 25.0},
    "claude-opus-4-6": {"input": 5.0, "output": 25.0},
    "claude-opus-4-5": {"input": 5.0, "output": 25.0},
    "claude-opus-4-1": {"input": 15.0, "output": 75.0},
    "claude-opus-4": {"input": 15.0, "output": 75.0},
    "claude-sonnet-5-5": {"input": 2.0, "output": 10.0, "cache_read": 0.20},
    "claude-sonnet-5": {"input": 2.0, "output": 10.0},
    "claude-sonnet-4-6": {"input": 3.0, "output": 15.0},
    "claude-sonnet-4-5": {"input": 3.0, "output": 15.0},
    "claude-sonnet-4": {"input": 3.0, "output": 15.0},
    # Above a 100K-token prompt the whole request bills at $0.50 / $2.50.
    # Whole-request repricing is out of scope; such turns are flagged instead.
    "claude-haiku-5-5": {"input": 0.10, "output": 0.50, "long_context_threshold": 100_000},
    "claude-haiku-4-5": {"input": 1.0, "output": 5.0},
}

# LiteLLM field -> internal rate name.
LITELLM_FIELDS = {
    "input_cost_per_token": "input",
    "output_cost_per_token": "output",
    "cache_creation_input_token_cost": "cache_5m",
    "cache_creation_input_token_cost_above_1hr": "cache_1h",
    "cache_read_input_token_cost": "cache_read",
    "input_cost_per_token_above_200k_tokens": "input_200k",
    "output_cost_per_token_above_200k_tokens": "output_200k",
    "cache_creation_input_token_cost_above_200k_tokens": "cache_5m_200k",
    "cache_read_input_token_cost_above_200k_tokens": "cache_read_200k",
}


# ---------------------------------------------------------------- parsing


class Skip(Exception):
    """A candidate line that can't become a ledger row; .reason is a short code."""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


@dataclass
class IngestStats:
    roots: int = 0
    files: int = 0
    unreadable_files: int = 0
    lines: int = 0
    candidates: int = 0
    rows_seen: int = 0
    rows_added: int = 0
    replaced: int = 0
    undupable: int = 0
    non_usage: int = 0
    bad_cost_ignored: int = 0
    files_read: int = 0
    bytes_skipped: int = 0
    checkpoint_resets: int = 0
    latest: tuple | None = None  # (mtime, file key, parts) of the newest transcript
    skipped: Counter = field(default_factory=Counter)

    def skip(self, reason: str) -> None:
        self.skipped[reason] += 1


def _reject_constant(name: str):
    raise ValueError("non-finite number")


def _tokens(obj: dict, key: str) -> int:
    v = obj.get(key)
    if v is None:
        return 0
    if isinstance(v, bool):  # bool is an int subclass; True must not count as 1 token
        raise Skip("bad_tokens")
    if isinstance(v, float):
        if not v.is_integer():
            raise Skip("bad_tokens")
        v = int(v)
    if not isinstance(v, int) or v < 0 or v > MAX_TOKENS_FIELD:
        raise Skip("bad_tokens")
    return v


def _id(obj: dict, key: str, empty_is_missing: bool = False) -> str | None:
    v = obj.get(key)
    if v is None:
        return None
    if not isinstance(v, str) or len(v) > MAX_ID_LEN:
        raise Skip("bad_id")
    if v == "":
        if empty_is_missing:
            return None
        raise Skip("empty_id")
    return v


def _model(v) -> str:
    if v is None:
        return UNKNOWN_MODEL
    if not isinstance(v, str) or not v or len(v) > 200:
        raise Skip("bad_model")
    return v


def _speed(v) -> str | None:
    if isinstance(v, str) and 0 < len(v) <= 32:
        return v.lower()
    return None


def _stored_cost(v, stats: IngestStats) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v) or not 0 <= v < 1e6:
        stats.bad_cost_ignored += 1
        return None
    return float(v)


_TS_RE = re.compile(
    r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:?\d{2})\Z"
)


def fmt_utc(dt: datetime) -> str:
    # Fixed width, so lexicographic order == chronological order in SQL.
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def parse_utc(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def parse_ts(v) -> str:
    """Strict ISO-8601 with Z or numeric offset -> normalized UTC string."""
    if not isinstance(v, str):
        raise Skip("bad_timestamp")
    m = _TS_RE.match(v)
    if not m:
        raise Skip("bad_timestamp")  # includes naive timestamps: no guessing the zone
    y, mo, d, h, mi, s, frac, off = m.groups()
    try:
        micros = int((frac or "0").ljust(6, "0")[:6])
        if off == "Z":
            tz = timezone.utc
        else:
            sign = -1 if off[0] == "-" else 1
            tz = timezone(sign * timedelta(hours=int(off[1:3]), minutes=int(off[-2:])))
        dt = datetime(int(y), int(mo), int(d), int(h), int(mi), int(s), micros, tzinfo=tz)
    except ValueError:
        raise Skip("bad_timestamp") from None
    # Out-of-range years would also crash local-time conversion on Windows.
    if not 2000 <= dt.year <= 2100:
        raise Skip("bad_timestamp")
    return fmt_utc(dt)


def dedupe_key(message_id: str, request_id: str | None, session_id: str, ts_utc: str) -> str:
    if request_id is not None:
        parts = ["r", message_id, request_id]
    elif message_id.startswith("msg_"):
        # Real Anthropic IDs are unique per response; one line per content
        # block shares the ID across timestamps, so collapse within the session.
        parts = ["s", message_id, session_id]
    else:
        # Gateways reuse placeholder IDs for every response.
        parts = ["t", message_id, session_id, ts_utc]
    return json.dumps(parts, separators=(",", ":"))


ROW_COLUMNS = (
    "dedupe_key", "message_id", "request_id", "ts_utc", "session_id", "project",
    "model", "is_sidechain", "speed", "input_tokens", "output_tokens",
    "cache_create_5m", "cache_create_1h", "cache_read", "stored_cost",
    "cost_usd", "cost_source", "price_note",
)


def _cache_split(usage: dict) -> tuple[int, int]:
    flat = _tokens(usage, "cache_creation_input_tokens")
    nested = usage.get("cache_creation")
    if not isinstance(nested, dict):
        return flat, 0  # older lines: the flat total is all 5-minute writes
    c5 = _tokens(nested, "ephemeral_5m_input_tokens")
    c1 = _tokens(nested, "ephemeral_1h_input_tokens")
    if flat > c5 + c1:
        c5 += flat - (c5 + c1)  # unexplained remainder: never drop tokens
    return c5, c1


def _make_row(prices, message_id, request_id, ts, session_id, project, model,
              sidechain, speed, usage, stored) -> tuple:
    inp = _tokens(usage, "input_tokens")
    out = _tokens(usage, "output_tokens")
    cr = _tokens(usage, "cache_read_input_tokens")
    c5, c1 = _cache_split(usage)
    if model == SYNTHETIC:
        cost, source, note = 0.0, "calculated", NOTE_SYNTHETIC
    elif stored is not None:
        cost, source, note = stored, "stored", None
    else:
        cost, note = prices.calculate(model, speed, inp, out, c5, c1, cr)
        source = "calculated"
    key = dedupe_key(message_id, request_id, session_id, ts)
    return (key, message_id, request_id, ts, session_id, project, model,
            sidechain, speed, inp, out, c5, c1, cr, stored, cost, source, note)


def extract_rows(entry, project: str, path_session: str, prices, stats: IngestStats):
    """Whitelist extraction. Returns (status, rows); raises Skip on bad data.

    Never touches message.content or any other free-text field.
    """
    if not isinstance(entry, dict):
        return "non_usage", ()
    msg = entry.get("message")
    usage = msg.get("usage") if isinstance(msg, dict) else None
    if not isinstance(usage, dict):
        return "non_usage", ()
    message_id = _id(msg, "id", empty_is_missing=True)
    if message_id is None:
        return "undupable", ()
    ts = parse_ts(entry.get("timestamp"))
    request_id = _id(entry, "requestId")
    session_id = _id(entry, "sessionId") or path_session
    model = _model(msg.get("model"))
    sidechain = 1 if entry.get("isSidechain") is True else 0
    speed = _speed(usage.get("speed"))
    stored = _stored_cost(entry.get("costUSD"), stats)

    rows = [_make_row(prices, message_id, request_id, ts, session_id, project,
                      model, sidechain, speed, usage, stored)]

    # Advisor iterations bill a second model on the same line. Plain
    # "message" iterations repeat the top-level usage and must not be added.
    iterations = usage.get("iterations")
    if isinstance(iterations, list):
        for idx, it in enumerate(iterations[:MAX_ITERATIONS]):
            if not isinstance(it, dict) or it.get("type") != "advisor_message":
                continue
            adv_model = it.get("model")
            if not isinstance(adv_model, str) or not adv_model or len(adv_model) > 200:
                continue
            adv_usage = it.get("usage") if isinstance(it.get("usage"), dict) else it
            rows.append(_make_row(prices, f"{message_id}:advisor:{idx}", request_id, ts,
                                  session_id, project, adv_model, sidechain,
                                  _speed(adv_usage.get("speed")), adv_usage, None))
    return "ok", rows


# ---------------------------------------------------------------- files


def transcript_roots(extra: list[str] | None = None) -> list[Path]:
    """Existing `projects` directories, deduplicated by resolved path."""
    candidates: list[Path] = []
    if extra:
        candidates += [Path(p) for p in extra]
    else:
        env = os.environ.get("CLAUDE_CONFIG_DIR", "")
        for part in env.split(","):
            part = part.strip()
            if part:
                p = Path(part).expanduser()
                candidates.append(p if p.name == "projects" else p / "projects")
        candidates.append(Path.home() / ".claude" / "projects")
        candidates.append(Path.home() / ".config" / "claude" / "projects")
    seen, roots = set(), []
    for p in candidates:
        try:
            if not p.is_dir():
                continue
            real = os.path.normcase(str(p.resolve()))
        except OSError:
            continue
        if real not in seen:
            seen.add(real)
            roots.append(p)
    return roots


def _long_path(p: str) -> str:
    """Opt into Windows long paths; deep subagent dirs can pass MAX_PATH."""
    if os.name != "nt":
        return p
    p = os.path.abspath(p)
    if p.startswith("\\\\?\\"):
        return p
    if p.startswith("\\\\"):
        return "\\\\?\\UNC\\" + p[2:]
    return "\\\\?\\" + p


def iter_jsonl(root: Path, stats: IngestStats):
    """Yield (path, parts relative to root), sorted for deterministic ingest."""
    base = _long_path(str(root))

    def on_error(err: OSError):
        stats.unreadable_files += 1

    for dirpath, dirnames, filenames in os.walk(base, onerror=on_error, followlinks=False):
        dirnames.sort()
        for name in sorted(filenames):
            if name.endswith(".jsonl"):
                full = os.path.join(dirpath, name)
                yield full, os.path.relpath(full, base).split(os.sep)


def attribute(parts: list[str]) -> tuple[str, str]:
    """(project, fallback session) from the path below `projects/`.

    Project is the encoded-cwd directory name, unique per full working dir.
    Main files are <project>/<session>.jsonl; subagent and workflow files
    live under <project>/<session>/...
    """
    if len(parts) < 2:
        return "(root)", Path(parts[0]).stem
    if len(parts) == 2:
        return parts[0], Path(parts[1]).stem
    return parts[0], parts[1]


def file_key(path: str) -> str:
    """Stable checkpoint key: no long-path prefix, case-folded on Windows."""
    if path.startswith("\\\\?\\UNC\\"):
        path = "\\\\" + path[8:]
    elif path.startswith("\\\\?\\"):
        path = path[4:]
    return os.path.normcase(path)


def tail_sha(path: str, offset: int) -> str:
    """Hash of the bytes just before `offset`; never interpreted, only compared."""
    if offset <= 0:
        return ""
    with open(path, "rb") as fh:
        start = max(0, offset - TAIL_CHECK_BYTES)
        fh.seek(start)
        return hashlib.sha256(fh.read(offset - start)).hexdigest()[:32]


def resume_offset(path: str, st: os.stat_result, prev: tuple | None, stats: IngestStats) -> int | None:
    """Where to resume reading, or None when the file is unchanged.

    Checkpoints only save re-reading: every fallback is offset 0, and the
    upsert makes a full re-read harmless.
    """
    if prev is None:
        return 0
    size, mtime, offset, tail = prev
    if st.st_size == size and st.st_mtime == mtime:
        return None
    if offset > st.st_size or (offset and tail_sha(path, offset) != tail):
        stats.checkpoint_resets += 1  # truncated, rotated, or rewritten in place
        return 0
    return offset


def iter_lines_from(path: str, offset: int, stats: IngestStats):
    """Yield (line or None, end offset) for each complete line after `offset`.

    A last line without a newline is left unconsumed: it may still be being
    written. Oversized lines yield None so their bytes still advance the offset.
    """
    with open(path, "rb") as fh:
        fh.seek(offset)
        pos = offset
        while True:
            raw = fh.readline(MAX_LINE_BYTES)
            if not raw.endswith(b"\n"):
                if len(raw) < MAX_LINE_BYTES:
                    return  # EOF, or a partial line still being written
                size = len(raw)
                while True:  # drain the oversized line without keeping it
                    chunk = fh.readline(MAX_LINE_BYTES)
                    size += len(chunk)
                    if chunk.endswith(b"\n"):
                        break
                    if len(chunk) < MAX_LINE_BYTES:
                        return  # still unterminated: retry on the next pass
                pos += size
                stats.lines += 1
                stats.skip("oversized_line")
                yield None, pos
                continue
            pos += len(raw)
            stats.lines += 1
            yield raw, pos


def rows_from_file(path: str, offset: int, project: str, path_session: str, prices,
                   stats: IngestStats, progress: list):
    """Rows from complete lines after `offset`; progress[0] tracks the end offset."""
    for raw, end in iter_lines_from(path, offset, stats):
        progress[0] = end
        if raw is None or (USAGE_MARKERS[0] not in raw and USAGE_MARKERS[1] not in raw):
            continue
        stats.candidates += 1
        try:
            entry = json.loads(raw, parse_constant=_reject_constant)
        except (ValueError, RecursionError):  # includes bad UTF-8 and a partial last line
            stats.skip("bad_json")
            continue
        try:
            status, rows = extract_rows(entry, project, path_session, prices, stats)
        except Skip as s:
            stats.skip(s.reason)
            continue
        if status == "undupable":
            stats.undupable += 1
        elif status == "non_usage":
            stats.non_usage += 1
        yield from rows


# ---------------------------------------------------------------- pricing


class PriceFetchError(Exception):
    pass


def _rate(v) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    if not math.isfinite(v) or not 0 <= v < 1.0:  # $1/token is not a real rate
        return None
    return float(v)


def _multiplier(v) -> float | None:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v):
        return None
    return float(v) if 1.0 <= v <= 20.0 else None


_DATE_SUFFIX = re.compile(r"-(?:\d{8}|\d{4}-\d{2}-\d{2})$")


def strip_date_suffix(model: str) -> str:
    """Strip -YYYYMMDD / -YYYY-MM-DD only; short suffixes like -0613 are kept."""
    return _DATE_SUFFIX.sub("", model)


def normalize_model(model: str) -> str:
    k = model.strip().lower()
    if k.startswith("anthropic/"):
        k = k[len("anthropic/"):]
    k = re.sub(r"^(?:us|eu|apac|global)\.", "", k)
    if k.startswith("anthropic."):
        k = k[len("anthropic."):]
    k = k.split("@", 1)[0]  # vertex: claude-x@20250101
    k = re.sub(r"-v\d+(?::\d+)?$", "", k)  # bedrock: -v1:0
    k = re.sub(r"\[[^\]]*\]$", "", k)  # alias suffix like [1m]
    return k


def filter_litellm(data) -> dict[str, dict[str, float]]:
    """Keep first-party Claude entries that carry usable input/output rates."""
    if not isinstance(data, dict):
        return {}
    out: dict[str, dict[str, float]] = {}
    bare: set[str] = set()
    for key, entry in data.items():
        if not isinstance(key, str) or not isinstance(entry, dict):
            continue
        k = key.strip().lower()
        prefixed = k.startswith("anthropic/")
        if prefixed:
            k = k[len("anthropic/"):]
        elif "/" in k:
            continue  # bedrock/, vertex_ai/, openrouter/...: partner prices differ
        if not k.startswith("claude") or len(k) > 200:
            continue
        fields = {}
        for src, dst in LITELLM_FIELDS.items():
            r = _rate(entry.get(src))
            if r is not None:
                fields[dst] = r
        pse = entry.get("provider_specific_entry")
        if isinstance(pse, dict):
            fm = _multiplier(pse.get("fast"))
            if fm is not None:
                fields["fast_multiplier"] = fm
        if "input" not in fields or "output" not in fields:
            continue
        if prefixed and k in bare:
            continue  # the bare key wins over anthropic/<key>
        out[k] = fields
        if not prefixed:
            bare.add(k)
    return out


def _validate_cached_entries(entries) -> dict[str, dict[str, float]]:
    out = {}
    if not isinstance(entries, dict):
        return out
    for k, fields in entries.items():
        if not isinstance(k, str) or not isinstance(fields, dict):
            continue
        clean = {}
        for name, v in fields.items():
            r = _multiplier(v) if name == "fast_multiplier" else _rate(v)
            if r is not None and (name in LITELLM_FIELDS.values() or name == "fast_multiplier"):
                clean[name] = r
        if "input" in clean and "output" in clean:
            out[k] = clean
    return out


@dataclass(frozen=True)
class Rates:
    input: float
    output: float
    cache_5m: float
    cache_1h: float
    cache_read: float
    input_200k: float | None = None
    output_200k: float | None = None
    cache_5m_200k: float | None = None
    cache_1h_200k: float | None = None
    cache_read_200k: float | None = None
    fast_multiplier: float | None = None
    long_context_threshold: int | None = None

    @classmethod
    def from_raw(cls, raw: dict) -> Rates:
        i = raw["input"]
        i2 = raw.get("input_200k")
        return cls(
            input=i,
            output=raw["output"],
            # Missing-rate fallbacks are Anthropic's published ratios, never the raw input rate.
            cache_5m=raw.get("cache_5m", i * 1.25),
            cache_1h=raw.get("cache_1h", i * 2.0),
            cache_read=raw.get("cache_read", i * 0.1),
            input_200k=i2,
            output_200k=raw.get("output_200k"),
            cache_5m_200k=raw.get("cache_5m_200k"),
            cache_1h_200k=i2 * 2.0 if i2 is not None else None,
            cache_read_200k=raw.get("cache_read_200k"),
            fast_multiplier=raw.get("fast_multiplier"),
            long_context_threshold=raw.get("long_context_threshold"),
        )


def _tiered(tokens: int, base: float, above: float | None) -> float:
    if above is not None and tokens > TIER_THRESHOLD:
        return TIER_THRESHOLD * base + (tokens - TIER_THRESHOLD) * above
    return tokens * base


class PriceTable:
    def __init__(self, raw_by_key: dict[str, dict], source: str):
        self.source = source
        self._rates: dict[str, Rates] = {}
        for k, raw in raw_by_key.items():
            self._rates[k] = Rates.from_raw(raw)
        for k in list(self._rates):  # dated keys also answer for their undated form
            self._rates.setdefault(strip_date_suffix(k), self._rates[k])
        self._by_length = sorted(self._rates, key=len, reverse=True)
        # Bounded instance memo (functools.lru_cache on a method would pin self).
        self._memo: dict[str, Rates | None] = {}

    def __len__(self) -> int:
        return len(self._rates)

    def resolve(self, model: str) -> Rates | None:
        if model in self._memo:
            return self._memo[model]
        found = self._resolve(model)
        if len(self._memo) >= MEMO_LIMIT:
            self._memo.clear()
        self._memo[model] = found
        return found

    def _resolve(self, model: str) -> Rates | None:
        if model in (SYNTHETIC, UNKNOWN_MODEL):
            return None
        key = normalize_model(model)
        base = strip_date_suffix(key)
        for cand in (model, key, base):
            if cand in self._rates:
                return self._rates[cand]
        # Longest prefix, but never across a version boundary: claude-opus-4
        # must not price claude-opus-4-5-x (that is the $15 -> $5 trap).
        for k in self._by_length:
            if base.startswith(k):
                rest = base[len(k):]
                if rest.startswith("-") and not rest[1:2].isdigit():
                    return self._rates[k]
        return None

    def calculate(self, model, speed, inp, out, c5, c1, cr) -> tuple[float, str | None]:
        r = self.resolve(model)
        if r is None:
            return 0.0, NOTE_UNPRICED
        notes = []
        mult = 1.0
        if speed == "fast":
            base = strip_date_suffix(normalize_model(model))
            variant = self._rates.get(base + "-fast")
            if variant is not None:
                r = variant
            elif r.fast_multiplier:
                mult = r.fast_multiplier
            else:
                notes.append(NOTE_FAST_STANDARD)
        if r.long_context_threshold and inp + cr + c5 + c1 > r.long_context_threshold:
            notes.append(NOTE_LONG_CTX)
        cost = (
            _tiered(inp, r.input, r.input_200k)
            + _tiered(out, r.output, r.output_200k)
            + _tiered(c5, r.cache_5m, r.cache_5m_200k)
            + _tiered(c1, r.cache_1h, r.cache_1h_200k)
            + _tiered(cr, r.cache_read, r.cache_read_200k)
        ) * mult
        return cost, ",".join(notes) or None


def bundled_raw() -> dict[str, dict]:
    out = {}
    for k, v in BUNDLED_PRICES_PER_MTOK.items():
        out[k] = {
            name: (val / 1e6 if name not in ("fast_multiplier", "long_context_threshold") else val)
            for name, val in v.items()
        }
    return out


def merge_prices(bundled: dict, litellm: dict) -> dict:
    """Per-field merge: a LiteLLM rate wins, a bundled rate fills its gaps."""
    merged = {k: dict(v) for k, v in bundled.items()}
    for k, fields in litellm.items():
        base = bundled.get(k) or bundled.get(strip_date_suffix(k)) or {}
        merged[k] = {**base, **fields}
    return merged


def fetch_litellm(url: str = LITELLM_URL) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "ccledger/1"})
    try:
        with urllib.request.urlopen(req, timeout=FETCH_TIMEOUT_SECONDS) as resp:
            body = resp.read(MAX_PRICE_DOWNLOAD + 1)
    except Exception as e:  # network boundary: any failure falls back
        raise PriceFetchError(type(e).__name__) from None
    if len(body) > MAX_PRICE_DOWNLOAD:
        raise PriceFetchError("download too large")
    try:
        data = json.loads(body, parse_constant=_reject_constant)
    except (ValueError, RecursionError):
        raise PriceFetchError("not valid JSON") from None
    entries = filter_litellm(data)
    if not entries:
        raise PriceFetchError("no usable Claude entries")
    return entries


def read_price_cache(path: Path) -> tuple[float, dict] | None:
    try:
        if path.stat().st_size > MAX_PRICE_DOWNLOAD:
            return None
        doc = json.loads(path.read_bytes(), parse_constant=_reject_constant)
    except (OSError, ValueError, RecursionError):
        return None
    if not isinstance(doc, dict):
        return None
    fetched = doc.get("fetched_at")
    entries = _validate_cached_entries(doc.get("entries"))
    if not isinstance(fetched, (int, float)) or not entries:
        return None
    return float(fetched), entries


def write_price_cache(path: Path, entries: dict) -> None:
    doc = {"format": 1, "source": LITELLM_URL, "fetched_at": time.time(), "entries": entries}
    fd, tmp = tempfile.mkstemp(prefix=".prices-", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, separators=(",", ":"))
        os.replace(tmp, path)  # atomic: a crash never leaves a half-written cache
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def load_prices(cache_path: Path, offline: bool, refresh: bool, log=print) -> PriceTable:
    bundled = bundled_raw()
    cached = read_price_cache(cache_path)
    stale = cached is None or time.time() - cached[0] > PRICE_TTL_SECONDS
    if not offline and (refresh or stale):
        try:
            entries = fetch_litellm()
            try:
                write_price_cache(cache_path, entries)
            except OSError as e:
                log(f"note: could not write price cache ({type(e).__name__})")
            cached = (time.time(), entries)
        except PriceFetchError as e:
            fallback = "cached LiteLLM table" if cached else "bundled snapshot only"
            log(f"note: price refresh failed ({e}); using {fallback}")
    if cached is None:
        return PriceTable(bundled, "bundled snapshot")
    when = datetime.fromtimestamp(cached[0], timezone.utc).strftime("%Y-%m-%d %H:%MZ")
    return PriceTable(merge_prices(bundled, cached[1]),
                      f"LiteLLM ({len(cached[1])} Claude entries, fetched {when}) + bundled")


# ---------------------------------------------------------------- storage

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS calls (
    dedupe_key      TEXT    NOT NULL UNIQUE,
    message_id      TEXT    NOT NULL,
    request_id      TEXT,
    ts_utc          TEXT    NOT NULL,
    session_id      TEXT    NOT NULL,
    project         TEXT    NOT NULL,
    model           TEXT    NOT NULL,
    is_sidechain    INTEGER NOT NULL DEFAULT 0,
    speed           TEXT,
    input_tokens    INTEGER NOT NULL DEFAULT 0,
    output_tokens   INTEGER NOT NULL DEFAULT 0,
    cache_create_5m INTEGER NOT NULL DEFAULT 0,
    cache_create_1h INTEGER NOT NULL DEFAULT 0,
    cache_read      INTEGER NOT NULL DEFAULT 0,
    stored_cost     REAL,
    cost_usd        REAL    NOT NULL,
    cost_source     TEXT    NOT NULL CHECK (cost_source IN ('stored', 'calculated')),
    price_note      TEXT
);
CREATE INDEX IF NOT EXISTS calls_ts ON calls (ts_utc);
CREATE INDEX IF NOT EXISTS calls_session ON calls (session_id, ts_utc);
CREATE INDEX IF NOT EXISTS calls_project ON calls (project);
CREATE INDEX IF NOT EXISTS calls_replay ON calls (message_id, session_id, is_sidechain);

-- /btw sidechain logs replay parent messages under new requestIds; a
-- sidechain row whose (message_id, session_id) has a non-sidechain twin is
-- a replay and is excluded from every total.
CREATE VIEW IF NOT EXISTS billable AS
SELECT c.* FROM calls AS c
WHERE c.is_sidechain = 0
   OR NOT EXISTS (
       SELECT 1 FROM calls AS p
       WHERE p.message_id = c.message_id
         AND p.session_id = c.session_id
         AND p.is_sidechain = 0);

CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
"""

# Read checkpoints. An optimization only: deleting every row here must not
# change any total (the calls upsert is the source of truth).
SCHEMA_V2_SQL = """
CREATE TABLE IF NOT EXISTS file_state (
    path     TEXT    PRIMARY KEY,
    size     INTEGER NOT NULL,
    mtime    REAL    NOT NULL,
    "offset" INTEGER NOT NULL,
    tail_sha TEXT    NOT NULL DEFAULT ''
);
"""

# Version reached -> script that reaches it from the version before.
MIGRATIONS = {1: SCHEMA_SQL, 2: SCHEMA_V2_SQL}

_TOTAL = "({t}.input_tokens + {t}.output_tokens + {t}.cache_create_5m + {t}.cache_create_1h + {t}.cache_read)"
_NEW, _OLD = _TOTAL.format(t="excluded"), _TOTAL.format(t="calls")

# Greatest total wins, whole-row (input_tokens means different things on
# different lines, so fields are never mixed). Ties prefer a copy that
# reports speed, then a non-sidechain copy. Strict comparisons make a
# re-run a no-op.
UPSERT_SQL = (
    f"INSERT INTO calls ({', '.join(ROW_COLUMNS)}) VALUES ({', '.join('?' * len(ROW_COLUMNS))}) "
    "ON CONFLICT (dedupe_key) DO UPDATE SET "
    + ", ".join(f"{c} = excluded.{c}" for c in ROW_COLUMNS[1:])
    + f" WHERE {_NEW} > {_OLD}"
    + f" OR ({_NEW} = {_OLD} AND excluded.speed IS NOT NULL AND calls.speed IS NULL)"
    + f" OR ({_NEW} = {_OLD} AND (excluded.speed IS NULL) = (calls.speed IS NULL)"
    + " AND excluded.is_sidechain < calls.is_sidechain)"
)

FORBIDDEN_COLUMN_WORDS = ("content", "text", "prompt", "body", "cwd", "path", "summary")


def connect(path: Path, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        conn = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True,
                               isolation_level=None, timeout=30)
    else:
        conn = sqlite3.connect(str(path), isolation_level=None, timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version > SCHEMA_VERSION:
        raise SystemExit(f"ledger schema v{version} is newer than this script (v{SCHEMA_VERSION})")
    for v in range(version + 1, SCHEMA_VERSION + 1):
        # Each step is one transaction; user_version lives in the DB header
        # and commits or rolls back with it.
        conn.executescript(
            f"BEGIN;\n{MIGRATIONS[v]}\nPRAGMA user_version = {v};\n"
            f"INSERT OR REPLACE INTO meta (key, value) VALUES ('schema_version', '{v}');\nCOMMIT;")


def check_readable_schema(conn: sqlite3.Connection) -> None:
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    if version != SCHEMA_VERSION:
        raise SystemExit(f"ledger schema v{version} != v{SCHEMA_VERSION}; run `ingest` first")


def ingest(conn: sqlite3.Connection, roots: list[Path], prices: PriceTable) -> IngestStats:
    stats = IngestStats(roots=len(roots))
    ensure_schema(conn)
    before = conn.execute("SELECT COUNT(*) FROM calls").fetchone()[0]
    batch: list[tuple] = []
    # One transaction: an interrupted ingest leaves rows and checkpoints untouched.
    conn.execute("BEGIN IMMEDIATE")
    try:
        states = {r[0]: r[1:] for r in conn.execute(
            'SELECT path, size, mtime, "offset", tail_sha FROM file_state')}
        changes0 = conn.total_changes
        seen: set[str] = set()
        checkpoints: list[tuple] = []
        for root in roots:
            for path, parts in iter_jsonl(root, stats):
                stats.files += 1
                key = file_key(path)
                seen.add(key)
                try:
                    st = os.stat(path)
                    if stats.latest is None or st.st_mtime > stats.latest[0]:
                        stats.latest = (st.st_mtime, key, parts)
                    start = resume_offset(path, st, states.get(key), stats)
                    if start is None:
                        stats.bytes_skipped += st.st_size
                        continue
                    stats.bytes_skipped += start
                    stats.files_read += 1
                    project, path_session = attribute(parts)
                    progress = [start]
                    for row in rows_from_file(path, start, project, path_session, prices,
                                              stats, progress):
                        stats.rows_seen += 1
                        batch.append(row)
                        if len(batch) >= BATCH_ROWS:
                            conn.executemany(UPSERT_SQL, batch)
                            batch.clear()
                    checkpoints.append((key, st.st_size, st.st_mtime, progress[0],
                                        tail_sha(path, progress[0])))
                except OSError:
                    # No checkpoint: the file is simply re-read next time.
                    stats.unreadable_files += 1
        if batch:
            conn.executemany(UPSERT_SQL, batch)
            batch.clear()
        after = conn.execute("SELECT COUNT(*) FROM calls").fetchone()[0]
        stats.rows_added = after - before
        stats.replaced = (conn.total_changes - changes0) - stats.rows_added
        conn.executemany('INSERT OR REPLACE INTO file_state (path, size, mtime, "offset", tail_sha) '
                         "VALUES (?, ?, ?, ?, ?)", checkpoints)
        # Forget checkpoints for files that vanished from the roots just scanned.
        prefixes = tuple(file_key(_long_path(str(r))) + os.sep for r in roots)
        conn.executemany("DELETE FROM file_state WHERE path = ?",
                         [(k,) for k in states if k not in seen and k.startswith(prefixes)])
        if stats.files_read == 0:
            conn.execute("COMMIT")  # nothing new: keep the last real ingest summary
            return stats
        summary = {
            "at": fmt_utc(datetime.now(timezone.utc)), "prices": prices.source,
            "files": stats.files, "unreadable_files": stats.unreadable_files,
            "lines": stats.lines, "rows_seen": stats.rows_seen,
            "rows_added": stats.rows_added, "replaced": stats.replaced,
            "undupable": stats.undupable, "skipped": dict(stats.skipped),
            "non_usage": stats.non_usage, "bad_cost_ignored": stats.bad_cost_ignored,
            "files_read": stats.files_read, "bytes_skipped": stats.bytes_skipped,
        }
        conn.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('last_ingest', ?)",
                     (json.dumps(summary),))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return stats


# ---------------------------------------------------------------- reports


def resolve_tz(name: str | None):
    """None means the system zone, which needs no tz database (Windows has none)."""
    if not name:
        return None
    if name.upper() == "UTC":
        return timezone.utc
    try:
        from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
    except ImportError:
        raise SystemExit("--tz needs Python 3.9+ (zoneinfo)") from None
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError):
        raise SystemExit(
            f"time zone {name!r} not found. Windows Python ships no tz database: "
            "`pip install tzdata`, or omit --tz to use the system zone") from None


def tz_label(tz) -> str:
    if tz is None:
        return f"system local ({time.tzname[time.localtime().tm_isdst > 0]})"
    return str(tz)


def local_now(tz) -> datetime:
    return datetime.now(tz) if tz is not None else datetime.now()


def local_midnight_utc(d: date, tz) -> str:
    naive = datetime(d.year, d.month, d.day)
    # A naive datetime's astimezone() uses the system zone's rules for that
    # date, so DST is right without a tz database.
    aware = naive.replace(tzinfo=tz) if tz is not None else naive.astimezone()
    return fmt_utc(aware)


def to_local(ts_utc: str, tz) -> datetime:
    dt = parse_utc(ts_utc)
    return dt.astimezone(tz) if tz is not None else dt.astimezone()


def period_days(period: str, today: date) -> tuple[date, date]:
    """[start, end) local dates. Weeks start Monday; a month is the one containing `today`."""
    if period == "today":
        return today, today + timedelta(days=1)
    if period == "week":
        start = today - timedelta(days=today.weekday())
        return start, start + timedelta(days=7)
    if period == "month":
        start = today.replace(day=1)
        end = date(start.year + (start.month == 12), start.month % 12 + 1, 1)
        return start, end
    raise ValueError(period)


def parse_month(s: str) -> date:
    m = re.fullmatch(r"(\d{4})-(\d{2})", s or "")
    if not m or not 1 <= int(m.group(2)) <= 12:
        raise SystemExit(f"month must be YYYY-MM, got {s!r}")
    return date(int(m.group(1)), int(m.group(2)), 1)


AGG_COLS = (
    "COUNT(*), COUNT(DISTINCT session_id), "
    "COALESCE(SUM(input_tokens),0), COALESCE(SUM(output_tokens),0), "
    "COALESCE(SUM(cache_create_5m),0), COALESCE(SUM(cache_create_1h),0), "
    "COALESCE(SUM(cache_read),0), COALESCE(SUM(cost_usd),0), "
    "COALESCE(SUM(CASE WHEN cost_source='stored' THEN cost_usd END),0), "
    "COALESCE(SUM(CASE WHEN cost_source='calculated' THEN cost_usd END),0)"
)
AGG_KEYS = ("calls", "sessions", "input", "output", "cw5m", "cw1h", "cread", "cost", "stored", "calc")


def _agg(conn, lo: str, hi: str, extra: str = "", params: tuple = ()) -> dict:
    row = conn.execute(f"SELECT {AGG_COLS} FROM billable WHERE ts_utc >= ? AND ts_utc < ? {extra}",
                       (lo, hi, *params)).fetchone()
    return dict(zip(AGG_KEYS, row))


def fmt_int(n) -> str:
    return f"{int(n):,}"


def fmt_usd(x) -> str:
    return f"${x:,.4f}" if 0 < abs(x) < 1 else f"${x:,.2f}"


def print_table(headers: list[str], rows: list[list], left: int = 1) -> None:
    """Plain-text table; the first `left` columns are left-aligned."""
    cells = [[str(c) for c in r] for r in rows]
    widths = [max(len(h), *(len(r[i]) for r in cells)) if cells else len(h) for i, h in enumerate(headers)]

    def line(vals):
        return "  ".join(v.ljust(w) if i < left else v.rjust(w) for i, (v, w) in enumerate(zip(vals, widths)))

    print(line(headers))
    print("  ".join("-" * w for w in widths))
    for r in cells:
        print(line(r))


def _short(s: str, n: int) -> str:
    return s if len(s) <= n else "..." + s[-(n - 3):]


def _print_totals(a: dict) -> None:
    total = a["input"] + a["output"] + a["cw5m"] + a["cw1h"] + a["cread"]
    print(f"Calls: {fmt_int(a['calls'])}   Sessions: {fmt_int(a['sessions'])}")
    print(f"Tokens: in {fmt_int(a['input'])} | out {fmt_int(a['output'])} | "
          f"cache-write 5m {fmt_int(a['cw5m'])} | 1h {fmt_int(a['cw1h'])} | "
          f"cache-read {fmt_int(a['cread'])} | total {fmt_int(total)}")
    print(f"Shadow cost: {fmt_usd(a['cost'])}   (stored costUSD {fmt_usd(a['stored'])} | "
          f"calculated {fmt_usd(a['calc'])})")


def _print_caveats(conn, lo: str, hi: str) -> None:
    q = "SELECT COUNT(*) FROM billable WHERE ts_utc >= ? AND ts_utc < ? AND price_note LIKE ?"
    fast = conn.execute(q, (lo, hi, f"%{NOTE_FAST_STANDARD}%")).fetchone()[0]
    longctx = conn.execute(q, (lo, hi, f"%{NOTE_LONG_CTX}%")).fetchone()[0]
    unpriced = [r[0] for r in conn.execute(
        "SELECT DISTINCT model FROM billable WHERE ts_utc >= ? AND ts_utc < ? "
        "AND price_note = ? ORDER BY model", (lo, hi, NOTE_UNPRICED))]
    replays = conn.execute(
        "SELECT (SELECT COUNT(*) FROM calls WHERE ts_utc >= ?1 AND ts_utc < ?2) - "
        "(SELECT COUNT(*) FROM billable WHERE ts_utc >= ?1 AND ts_utc < ?2)", (lo, hi)).fetchone()[0]
    print(f"fast turns priced at standard rates: {fast} (undercount)")
    print(f"long-context turns priced at base rates: {longctx} (undercount)")
    print(f"unpriced models ($0): {unpriced}")
    print(f"sidechain replays excluded: {replays}")


def _model_breakdown(conn, lo: str, hi: str) -> None:
    rows = conn.execute(
        "SELECT model, COUNT(*), SUM(input_tokens), SUM(output_tokens), "
        "SUM(cache_create_5m + cache_create_1h), SUM(cache_read), SUM(cost_usd) "
        "FROM billable WHERE ts_utc >= ? AND ts_utc < ? GROUP BY model "
        "ORDER BY SUM(cost_usd) DESC, model", (lo, hi)).fetchall()
    print_table(["model", "calls", "in", "out", "cache-write", "cache-read", "$"],
                [[m, fmt_int(n), fmt_int(i), fmt_int(o), fmt_int(cw), fmt_int(cr), fmt_usd(c)]
                 for m, n, i, o, cw, cr, c in rows])


def report_period(conn, period: str, tz, month: date | None = None) -> None:
    if period == "all":
        lo, hi = "0", "9"
        span = conn.execute("SELECT MIN(ts_utc), MAX(ts_utc) FROM billable").fetchone()
        label = ("all history: " + " -> ".join(
            to_local(s, tz).strftime("%Y-%m-%d %H:%M") for s in span)) if span[0] else "all history: empty"
        days = None
    else:
        start, end = period_days(period, month or local_now(tz).date())
        lo, hi = local_midnight_utc(start, tz), local_midnight_utc(end, tz)
        label = f"{period}: {start} -> {end - timedelta(days=1)}"
        days = [start + timedelta(days=i) for i in range((end - start).days)]
    print(f"== {label}   [{tz_label(tz)}]")
    _print_totals(_agg(conn, lo, hi))
    print()
    _model_breakdown(conn, lo, hi)
    if days and len(days) > 1:
        print()
        rows = []
        for d in days:
            a = _agg(conn, local_midnight_utc(d, tz), local_midnight_utc(d + timedelta(days=1), tz))
            if a["calls"]:
                tok = a["input"] + a["output"] + a["cw5m"] + a["cw1h"] + a["cread"]
                rows.append([d.isoformat(), fmt_int(a["calls"]), fmt_int(tok), fmt_usd(a["cost"])])
        print_table(["day", "calls", "tokens", "$"], rows)
    print()
    _print_caveats(conn, lo, hi)
    if period == "all":
        raw = conn.execute("SELECT value FROM meta WHERE key = 'last_ingest'").fetchone()
        if raw:
            s = json.loads(raw[0])
            skipped = sum(s["skipped"].values())
            print(f"last ingest {s['at']}: files {s['files']} | rows seen {s['rows_seen']} | "
                  f"undupable {s['undupable']} | skipped {skipped}"
                  f"{' ' + str(s['skipped']) if skipped else ''} | prices: {s['prices']}")


def report_sessions(conn, tz, limit: int) -> None:
    rows = conn.execute(
        "SELECT session_id, project, MIN(ts_utc), MAX(ts_utc), COUNT(*), SUM(input_tokens), "
        "SUM(output_tokens), SUM(cache_create_5m + cache_create_1h), SUM(cache_read), "
        "SUM(cost_usd) FROM billable GROUP BY session_id ORDER BY MAX(ts_utc) DESC LIMIT ?",
        (limit,)).fetchall()
    out = []
    for sid, project, first, last, n, i, o, cw, cr, c in rows:
        dur = parse_utc(last) - parse_utc(first)
        hours, rem = divmod(int(dur.total_seconds()), 3600)
        out.append([sid[:8], _short(project, 32), to_local(first, tz).strftime("%Y-%m-%d %H:%M"),
                    f"{hours}h{rem // 60:02d}m", fmt_int(n), fmt_int(i), fmt_int(o),
                    fmt_int(cw), fmt_int(cr), fmt_usd(c)])
    print(f"== sessions (most recent {limit})   [{tz_label(tz)}]")
    print_table(["session", "project", "start", "duration", "calls", "in", "out",
                 "cache-write", "cache-read", "$"], out, left=3)


def report_projects(conn) -> None:
    rows = conn.execute(
        "SELECT project, COUNT(DISTINCT session_id), COUNT(*), "
        "SUM(input_tokens + output_tokens + cache_create_5m + cache_create_1h + cache_read), "
        "SUM(cost_usd), SUM(CASE WHEN cost_source='stored' THEN cost_usd ELSE 0 END) "
        "FROM billable GROUP BY project ORDER BY SUM(cost_usd) DESC, project").fetchall()
    print("== projects")
    print_table(["project", "sessions", "calls", "tokens", "$", "stored $"],
                [[_short(p, 60), fmt_int(s), fmt_int(n), fmt_int(t), fmt_usd(c), fmt_usd(st)]
                 for p, s, n, t, c, st in rows])


def report_schema(conn) -> int:
    cols = conn.execute("PRAGMA table_info(calls)").fetchall()
    print_table(["cid", "name", "type", "notnull"], [[c[0], c[1], c[2], c[3]] for c in cols], left=2)
    bad = [c[1] for c in cols if any(w in c[1].lower() for w in FORBIDDEN_COLUMN_WORDS)]
    print(f"\ncontent-column check: {'FAIL ' + str(bad) if bad else 'PASS (no text/content columns)'}")
    print(f"schema version: {conn.execute('PRAGMA user_version').fetchone()[0]}")
    return 1 if bad else 0


# ---------------------------------------------------------------- blocks


@dataclass
class Block:
    """A 5-hour pacing window. Not Anthropic's quota: that is server-side and weighted."""
    start: datetime
    first: datetime
    last: datetime
    calls: int = 0
    tokens: int = 0
    cost: float = 0.0

    @property
    def end(self) -> datetime:
        return self.start + BLOCK

    def is_active(self, now: datetime) -> bool:
        return now - self.last < BLOCK and now < self.end


def iter_blocks(points):
    """Group (ts, tokens, cost) points, sorted by ts, into blocks.

    A block starts at the UTC hour floor of its first call. A call more than
    5h after the block start, or more than 5h after the previous call, opens
    a new block. Constant memory: only the open block is held.
    """
    cur: Block | None = None
    for ts, tokens, cost in points:
        t = parse_utc(ts) if isinstance(ts, str) else ts
        if cur is not None and (t - cur.start > BLOCK or t - cur.last > BLOCK):
            yield cur
            cur = None
        if cur is None:
            cur = Block(start=t.replace(minute=0, second=0, microsecond=0), first=t, last=t)
        cur.calls += 1
        cur.tokens += tokens
        cur.cost += cost
        cur.last = t
    if cur is not None:
        yield cur


TOKENS_SQL = "input_tokens + output_tokens + cache_create_5m + cache_create_1h + cache_read"


def latest_block(conn) -> Block | None:
    """Most recent block. O(history) scan in timestamp order, O(1) memory."""
    cur = conn.execute(
        f"SELECT ts_utc, {TOKENS_SQL}, cost_usd FROM billable "
        "WHERE model != ? ORDER BY ts_utc", (SYNTHETIC,))  # local notices aren't API calls
    block = None
    for block in iter_blocks(cur):
        pass
    return block


def burn(conn, now: datetime, window: timedelta, session: str | None = None) -> tuple[int, float]:
    """(tokens, $) with timestamps inside the trailing window."""
    sql = f"SELECT COALESCE(SUM({TOKENS_SQL}),0), COALESCE(SUM(cost_usd),0) FROM billable WHERE ts_utc >= ?"
    params: tuple = (fmt_utc(now - window),)
    if session is not None:
        sql += " AND session_id = ?"
        params += (session,)
    tokens, cost = conn.execute(sql, params).fetchone()
    return int(tokens), float(cost)


def fmt_span(td: timedelta) -> str:
    minutes = max(0, int(td.total_seconds() // 60))
    return f"{minutes // 60}h{minutes % 60:02d}m"


def block_lines(conn, block: Block | None, now: datetime, tz) -> list[str]:
    if block is None:
        return ["block: no calls yet"]
    fmt = "%Y-%m-%d %H:%M"
    start_local = to_local(fmt_utc(block.start), tz).strftime(fmt)
    end_local = to_local(fmt_utc(block.end), tz).strftime("%H:%M")
    head = f"block {start_local} -> {end_local}: {fmt_int(block.calls)} calls | " \
           f"{fmt_int(block.tokens)} tokens | {fmt_usd(block.cost)}"
    if not block.is_active(now):
        return [head + f" | ended (last call {to_local(fmt_utc(block.last), tz).strftime(fmt)})"]
    left = block.end - now
    _, cost5 = burn(conn, now, BURN_WINDOW)
    per_min = cost5 / (BURN_WINDOW.total_seconds() / 60)
    projected = block.cost + per_min * left.total_seconds() / 60
    return [head + f" | ACTIVE, {fmt_span(now - block.start)} elapsed, {fmt_span(left)} left",
            f"  projection at current {fmt_usd(per_min)}/min: {fmt_usd(projected)} by window end (estimate)"]


def report_block(conn, tz, now: datetime | None = None) -> None:
    now = now or datetime.now(timezone.utc)
    print(f"== 5-hour block (pacing view, not quota)   [{tz_label(tz)}]")
    for ln in block_lines(conn, latest_block(conn), now, tz):
        print(ln)
    print("Quota % is server-side and weighted; the account usage page is the only quota source.")


# ---------------------------------------------------------------- watch


def encode_project(path: str) -> str:
    """Claude Code's project-dir naming: every non-alphanumeric char becomes '-'."""
    return re.sub(r"[^A-Za-z0-9]", "-", path)


def decode_project_dir(encoded: str, max_listings: int = 2000) -> Path | None:
    """Find the real directory whose encoding is `encoded`.

    The encoding is lossy, so this matches directory *names* against it, one
    level at a time. Names only - nothing is opened. None if not found.
    """
    if os.name == "nt":
        m = re.match(r"([A-Za-z])--", encoded)
        if not m:
            return None
        stack = [(Path(m.group(1).upper() + ":\\"), encoded[3:])]
    else:
        if not encoded.startswith("-"):
            return None
        stack = [(Path("/"), encoded[1:])]
    listings = 0
    while stack:
        path, rest = stack.pop()
        if not rest:
            return path
        listings += 1
        if listings > max_listings:
            return None
        try:
            names = sorted(os.listdir(path), reverse=True)  # pop() then tries them in order
        except OSError:
            continue
        for name in names:
            enc = encode_project(name)
            if rest == enc or rest.startswith(enc + "-"):
                child = path / name
                if child.is_dir():
                    stack.append((child, rest[len(enc) + 1:]))
    return None


def recent_changes(root: Path, since: float, excluded: frozenset[str],
                   limit: int = WALK_LIMIT) -> int | None:
    """0 if nothing under `root` changed after `since`, else >= 1 (stops at the first).

    Uses directory listings and mtimes only - file contents are never read.
    Directory mtimes count too (a deleted or renamed file is a change).
    None when the tree is larger than `limit` entries.
    """
    stack = [str(root)]
    scanned = 0
    while stack:
        top = stack.pop()
        try:
            it = os.scandir(top)
        except OSError:
            continue
        with it:
            for entry in it:
                scanned += 1
                if scanned > limit:
                    return None
                if os.path.normcase(entry.path) in excluded:
                    continue
                try:
                    is_dir = entry.is_dir(follow_symlinks=False)
                    if is_dir and entry.name in PRUNE_DIRS:
                        continue
                    if entry.stat(follow_symlinks=False).st_mtime > since:
                        return 1
                except OSError:
                    continue
                if is_dir:
                    stack.append(entry.path)
    return 0


def own_files(db: Path, prices: Path) -> frozenset[str]:
    """Files ccledger itself writes; their churn is never project progress."""
    names = [db, Path(f"{db}-journal"), Path(f"{db}-wal"), Path(f"{db}-shm"), prices]
    return frozenset(os.path.normcase(str(p.resolve())) for p in names)


def enable_ansi() -> bool:
    if not sys.stdout.isatty():
        return False
    if os.name != "nt":
        return True
    try:
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if not kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            return False
        return bool(kernel32.SetConsoleMode(handle, mode.value | 0x0004))  # VT processing
    except (AttributeError, OSError):
        return False


class Watcher:
    """Observed mode: reads transcripts and the ledger, prints status. It never
    writes to transcript files and never signals any process."""

    def __init__(self, conn, roots, prices: PriceTable, tz, stall_usd: float,
                 excluded: frozenset[str] = frozenset()):
        self.conn, self.roots, self.prices, self.tz = conn, roots, prices, tz
        self.stall_usd = stall_usd
        self.excluded = excluded
        self.block: Block | None = None
        self.block_fresh = False
        self.alerting = False
        self._dirs: dict[str, Path | None] = {}  # encoded project -> real dir

    def _session(self, project: str, path_session: str) -> str | None:
        hit = self.conn.execute("SELECT 1 FROM calls WHERE session_id = ? LIMIT 1",
                                (path_session,)).fetchone()
        if hit:
            return path_session
        row = self.conn.execute("SELECT session_id FROM billable WHERE project = ? "
                                "ORDER BY ts_utc DESC LIMIT 1", (project,)).fetchone()
        return row[0] if row else None

    def _project_dir(self, project: str) -> Path | None:
        if project not in self._dirs:
            if len(self._dirs) >= MEMO_LIMIT:
                self._dirs.clear()
            self._dirs[project] = decode_project_dir(project)
        return self._dirs[project]

    def stall(self, project: str, session: str, now: datetime) -> tuple[dict | None, str | None]:
        """(alert evidence, note). Burn is checked first; the tree walk only
        happens when money is actually moving."""
        _, spent = burn(self.conn, now, STALL_WINDOW, session)
        if spent < self.stall_usd:
            return None, None
        root = self._project_dir(project)
        if root is None:
            return None, "stall check off: project directory not found on disk"
        changed = recent_changes(root, (now - STALL_WINDOW).timestamp(), self.excluded)
        if changed is None:
            return None, f"stall check off: project tree over {WALK_LIMIT:,} entries"
        if changed:
            return None, None
        return {"spent": spent, "minutes": int(STALL_WINDOW.total_seconds() // 60),
                "changed": 0, "dir": str(root)}, None

    def poll(self, now: datetime | None = None) -> dict:
        stats = ingest(self.conn, self.roots, self.prices)
        now = now or datetime.now(timezone.utc)
        if stats.rows_added or stats.replaced or not self.block_fresh:
            self.block = latest_block(self.conn)  # recompute only when data changed
            self.block_fresh = True
        status = {"now": now, "block": self.block, "rows_added": stats.rows_added}
        if stats.latest is None:
            return status
        project, path_session = attribute(stats.latest[2])
        session = self._session(project, path_session)
        status.update(project=project, session=session)
        if session is None:
            return status
        status["totals"] = _agg(self.conn, "0", "9", "AND session_id = ?", (session,))
        status["start"] = parse_utc(self.conn.execute(
            "SELECT MIN(ts_utc) FROM billable WHERE session_id = ?", (session,)).fetchone()[0])
        status["burn"] = burn(self.conn, now, BURN_WINDOW, session)
        alert, note = self.stall(project, session, now)
        status["alert"], status["note"] = alert, note
        status["bell"] = alert is not None and not self.alerting  # once per episode
        self.alerting = alert is not None
        return status

    def render(self, s: dict) -> list[str]:
        now = s["now"]
        out = [f"ccledger watch  {to_local(fmt_utc(now), self.tz).strftime('%Y-%m-%d %H:%M:%S')}"
               "  (observed mode: read-only, controls nothing; Ctrl-C to exit)"]
        if "totals" not in s:
            out.append("waiting for a session with usage...")
        else:
            a, (btok, bcost) = s["totals"], s["burn"]
            minutes = BURN_WINDOW.total_seconds() / 60
            out += [
                f"session {s['session'][:8]}  {_short(s['project'], 50)}",
                f"  started {to_local(fmt_utc(s['start']), self.tz).strftime('%Y-%m-%d %H:%M')}, "
                f"elapsed {fmt_span(now - s['start'])}",
                f"  calls {fmt_int(a['calls'])} | in {fmt_int(a['input'])} | out {fmt_int(a['output'])} | "
                f"cache-write {fmt_int(a['cw5m'] + a['cw1h'])} | cache-read {fmt_int(a['cread'])}",
                f"  shadow {fmt_usd(a['cost'])} (stored {fmt_usd(a['stored'])} | calculated {fmt_usd(a['calc'])})",
                f"  burn (last 5 min): {fmt_int(btok / minutes)} tokens/min | {fmt_usd(bcost / minutes)}/min",
            ]
        out += block_lines(self.conn, s["block"], now, self.tz)
        if s.get("alert"):
            e = s["alert"]
            out.append(f"!! BURN WITHOUT PROGRESS: {fmt_usd(e['spent'])} in {e['minutes']} min, "
                       f"{e['changed']} project files changed ({e['dir']})")
        elif s.get("note"):
            out.append(f"  ({s['note']})")
        return out


def run_watch(conn, roots, prices, tz, poll: float, stall_usd: float, excluded,
              max_polls: int | None = None) -> None:
    w = Watcher(conn, roots, prices, tz, stall_usd, excluded)
    ansi = enable_ansi()
    n = 0
    try:
        while True:
            status = w.poll()
            prefix = "\x1b[H\x1b[2J" if ansi else ("" if n == 0 else "\n")
            sys.stdout.write(prefix + "\n".join(w.render(status)) + "\n")
            if status.get("bell"):
                sys.stdout.write("\a")
            sys.stdout.flush()
            n += 1
            if max_polls is not None and n >= max_polls:
                return
            time.sleep(poll)
    except KeyboardInterrupt:
        print("\nwatch stopped (nothing was written to transcripts or signalled)")


# ---------------------------------------------------------------- cli


def print_ingest_summary(stats: IngestStats, conn, prices: PriceTable, secs: float) -> None:
    replays = conn.execute("SELECT (SELECT COUNT(*) FROM calls) - (SELECT COUNT(*) FROM billable)").fetchone()[0]
    unpriced = [r[0] for r in conn.execute(
        "SELECT DISTINCT model FROM calls WHERE price_note = ? ORDER BY model", (NOTE_UNPRICED,))]
    kept = stats.rows_seen - stats.rows_added - stats.replaced
    skipped = sum(stats.skipped.values())
    print(f"files: {stats.files} | rows added: {stats.rows_added} | duplicates replaced: "
          f"{stats.replaced} | duplicates kept: {kept} | undupable lines: {stats.undupable} | "
          f"skipped: {skipped} | replays excluded: {replays}")
    print(f"files read: {stats.files_read} | bytes skipped by checkpoints: {stats.bytes_skipped:,} | "
          f"checkpoint resets: {stats.checkpoint_resets}")
    detail = f" {dict(stats.skipped)}" if skipped else ""
    print(f"roots: {stats.roots} | lines: {stats.lines} | usage lines: {stats.candidates} | "
          f"unreadable: {stats.unreadable_files} | non-message usage: {stats.non_usage} | "
          f"bad costUSD ignored: {stats.bad_cost_ignored}{detail}")
    print(f"prices: {prices.source} | unpriced models: {unpriced} | {secs:.2f}s")


def main(argv: list[str] | None = None) -> int:
    try:  # Windows consoles may not be UTF-8; never crash on a model name
        sys.stdout.reconfigure(errors="replace")
    except (AttributeError, ValueError):
        pass
    ap = argparse.ArgumentParser(prog="ccledger", description="Claude Code token ledger")
    ap.add_argument("--db", default=os.environ.get("CCLEDGER_DB", str(DEFAULT_DB)))
    ap.add_argument("--tz", default=None, help="IANA zone for day/week/month buckets (default: system)")
    src = argparse.ArgumentParser(add_help=False)  # options shared by everything that ingests
    src.add_argument("--offline", action="store_true", help="never fetch prices")
    src.add_argument("--root", action="append", help="projects dir to scan (replaces defaults)")
    src.add_argument("--prices", default=str(DEFAULT_PRICES))
    sub = ap.add_subparsers(dest="cmd", required=True)
    ing = sub.add_parser("ingest", parents=[src], help="parse transcripts into the ledger (idempotent)")
    ing.add_argument("--refresh-prices", action="store_true", help="fetch prices even if cache is fresh")
    rep = sub.add_parser("report", parents=[src], help="reports (ingest incrementally first)")
    rep.add_argument("what", choices=["today", "week", "month", "all", "block", "sessions", "projects"])
    rep.add_argument("month", nargs="?", help="YYYY-MM, with `month` only")
    rep.add_argument("--limit", type=int, default=20)
    rep.add_argument("--no-ingest", action="store_true", help="report the ledger as it stands")
    wat = sub.add_parser(
        "watch", parents=[src],
        help="live readout of the active session (observed mode)",
        description="Observed mode: tails the most recently active Claude Code session and "
                    "reports burn, the 5-hour block, and burn-without-progress alerts. It never "
                    "writes to transcript files and never signals any process - it reports, it "
                    "does not control.")
    wat.add_argument("--poll", type=float, default=3.0, help="seconds between polls (min 0.5)")
    wat.add_argument("--stall-usd", type=float, default=1.0,
                     help="alert when this much $ is spent in 10 min with no project file changes")
    wat.add_argument("--polls", type=int, default=None, help=argparse.SUPPRESS)  # tests
    sub.add_parser("schema", help="print the calls schema and the no-content check")
    args = ap.parse_args(argv)
    db = Path(args.db)
    if args.cmd == "report" and args.month and args.what != "month":
        ap.error("a YYYY-MM argument only goes with `report month`")
    month = parse_month(args.month) if args.cmd == "report" and args.month else None
    tz = resolve_tz(args.tz)

    def open_and_ingest(quiet: bool):
        roots = transcript_roots(args.root)
        if not roots:
            raise SystemExit("no transcript roots found")
        db.parent.mkdir(parents=True, exist_ok=True)
        prices_path = Path(args.prices)
        prices_path.parent.mkdir(parents=True, exist_ok=True)
        prices = load_prices(prices_path, args.offline, getattr(args, "refresh_prices", False),
                             log=lambda m: print(m, file=sys.stderr))
        t0 = time.perf_counter()
        conn = connect(db)
        try:
            stats = ingest(conn, roots, prices)
        except BaseException:
            conn.close()
            raise
        if quiet:
            print(f"(ingested: +{stats.rows_added} rows, {stats.replaced} replaced, "
                  f"{stats.files_read} files read, {time.perf_counter() - t0:.2f}s)", file=sys.stderr)
        else:
            print_ingest_summary(stats, conn, prices, time.perf_counter() - t0)
        return conn, roots, prices

    if args.cmd == "ingest":
        conn, _, _ = open_and_ingest(quiet=False)
        conn.close()
        return 0

    if args.cmd == "watch":
        conn, roots, prices = open_and_ingest(quiet=True)
        try:
            run_watch(conn, roots, prices, tz, max(0.5, args.poll), args.stall_usd,
                      own_files(db, Path(args.prices)), args.polls)
        finally:
            conn.close()
        return 0

    if args.cmd == "report" and not args.no_ingest:
        open_and_ingest(quiet=True)[0].close()
    if not db.exists():
        print(f"no ledger at {db}; run `ingest` first", file=sys.stderr)
        return 1
    conn = connect(db, readonly=True)
    try:
        check_readable_schema(conn)
        if args.cmd == "schema":
            return report_schema(conn)
        if args.what == "sessions":
            report_sessions(conn, tz, max(1, args.limit))
        elif args.what == "projects":
            report_projects(conn)
        elif args.what == "block":
            report_block(conn, tz)
        else:
            report_period(conn, args.what, tz, month)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
