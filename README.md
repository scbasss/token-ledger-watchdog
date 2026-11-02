# token-ledger-watchdog

A local-first, content-blind token ledger and watchdog for Claude Code.

- **Exact per-call accounting** from the session transcripts Claude Code already writes to disk. Each call is counted once: streaming duplicates collapse to their most complete copy, and the same response seen under several config roots is deduplicated.
- **Live watch**: per-session tokens and shadow cost, burn rate (tokens/min and $/min over the last 5 minutes), and 5-hour block pacing with a time-left readout and an end-of-window projection.
- **Stall alert**: fires when money is moving but nothing is being produced. The trigger is at least $1.00 (configurable) spent in 10 minutes while zero files in the session's project directory changed. It rings the terminal bell once per episode.

## Privacy architecture

The parser reads a fixed whitelist of usage and metadata fields: timestamps, session/request/message IDs, model, token counts, cache tiers, and speed. **Message content is never read, stored, or transmitted.**

This is provable from the schema, which has no content columns anywhere. `python ccledger.py schema` prints it and checks it. The stall alert looks only at file names and modification times in your project directory. It never opens a file.

The only network call is an optional refresh of the public [LiteLLM price table](https://github.com/BerriAI/litellm). `--offline` disables it, and a bundled price snapshot keeps every cost figure working without it.

## Shadow cost

Dollar figures are **API-list-price equivalents** of your usage, for pacing and comparison. They are not bills. On a subscription plan you are not charged these amounts.

Costs come from Anthropic's per-entry `costUSD` when a transcript carries it. Otherwise they are calculated from token counts, including:
- 5-minute vs 1-hour cache-write tiers,
- per-model cache-read rates,
- above-200K-token rates,
- fast-mode multipliers.

Every total shows its stored/calculated split. Turns the tool knows it underprices are counted and shown, never silently absorbed.

## Observed mode

The watchdog **alerts; it never kills or signals a session it didn't launch.** `watch` reads transcripts and the local ledger and prints status. It never writes to transcript files and never sends a signal to any process.

The 5-hour block is a pacing view, not quota: Anthropic's real usage limits are computed server-side and weighted, and the account usage page is the only source for them.

## Quickstart

Python 3, standard library only. No dependencies, no install.

```bash
python ccledger.py ingest                 # parse transcripts into the local ledger (idempotent)
python ccledger.py report today           # also: week | month [YYYY-MM] | all | block
python ccledger.py report sessions        # per-session table
python ccledger.py report projects        # totals by project
python ccledger.py watch                  # live readout; Ctrl-C to exit
```

- Reports ingest new transcript lines first (`--no-ingest` to skip). Ingest is incremental: unchanged bytes are never re-read.
- The ledger lives in `%LOCALAPPDATA%\ccledger` on Windows and `$XDG_DATA_HOME/ccledger` (default `~/.local/share/ccledger`) elsewhere. Override it with `--db` or `CCLEDGER_DB`.
- Transcripts are read from `CLAUDE_CONFIG_DIR` (if set), `~/.claude/projects`, and `~/.config/claude/projects`.
- `--tz` takes an IANA zone name. On Windows that needs `pip install tzdata`. Without `--tz`, the system zone is used, and that needs nothing extra.

Tests:

```bash
python -m unittest -v test_ccledger test_layer2
```

## Layer 2: sync client

Optional. The ledger stays local-first: ingest, reports and `watch` never know sync exists, and they work the same with sync unconfigured, misconfigured, or the endpoint down.

`sync` posts daily aggregates to an endpoint that implements the payload contract below. **The reference server is not included in this repository.** The contract is documented in full here so you can audit exactly what leaves your machine, or run your own endpoint.

### What sync sends

Only **daily rollups**: one row per local date, model and project, with exactly these fields (`ROLLUP_FIELDS` in [`ccledger.py`](ccledger.py)):

| Field | Type | Meaning |
|---|---|---|
| `date` | string `YYYY-MM-DD` | the local calendar day (your timezone, or `--tz`) |
| `model` | string | model ID as recorded in the transcript |
| `project` | string | a display name, never a path (see below) |
| `calls` | integer | billable API calls |
| `input_tokens`, `output_tokens` | integer | |
| `cache_read_tokens` | integer | |
| `cache_write_5m_tokens`, `cache_write_1h_tokens` | integer | cache writes by tier |
| `cost_stored_usd` | number | shadow cost taken from the transcript's own `costUSD` |
| `cost_calculated_usd` | number | shadow cost calculated from token counts |

Rows travel in a batch with exactly three keys: `device_name`, `schema_version` (currently `1`) and `rollups`. If a field isn't listed, it doesn't ship: no session IDs, no message content, no file paths, no hostname. Stored and calculated costs stay separate all the way through.

A unit test asserts the exact key sets of a produced payload, written out a second time in the test file, so a new field can't ship without a deliberate edit to both.

`python ccledger.py sync --dry-run` prints the exact JSON that would be sent and sends nothing.

### Project names

The ledger keeps Claude Code's encoded folder name (for example `C--Users-name-code-myapp`), and local reports show it unchanged. Only `sync` maps it, like this:
- **Folder still exists:** the folder's own name, with no drive or parent path (`myapp`, `.claude`).
- **Folder gone, or not safe to name:** `h:` plus 16 hex characters of SHA-256(salt + encoded name). This covers your home folder itself and any folder whose name contains your username, matched case-insensitively.
- **Two projects with the same folder name:** both get a suffix of 4 hex characters from the same salted hash (`app·a3f9`), so an endpoint can keep them apart.

The salt is 32 random bytes in `sync.salt`, in the ledger's data directory. It is created on first sync, never sent or printed, and git-ignored. **Back it up:** a new salt gives every hashed or suffixed project a new name at the endpoint. A damaged salt file stops sync with an error rather than being replaced.

Suffixes are worked out from every project in the ledger, not just the days being sent. A new project that shares an older project's folder name therefore renames the older one from its next pushed day onward, so the endpoint sees it under both names. A project whose folder is later moved or deleted switches to its hash the same way.

`device_name` is whatever you configure; unset, it is `unnamed-device`, never the hostname.

### Configure

Set environment variables `CCLEDGER_SYNC_URL`, `CCLEDGER_SYNC_KEY` and `CCLEDGER_DEVICE_NAME`, or copy [`sync.example.json`](sync.example.json) to `sync.json` in the ledger's data directory (`%LOCALAPPDATA%\ccledger` on Windows, `~/.local/share/ccledger` elsewhere, or beside the `--db` file). Environment variables win over the file. `sync.json` holds your device key and is git-ignored; never commit it. Then:

```bash
python ccledger.py sync                  # ingests first (--no-ingest to skip), then pushes
```

How it behaves:
- Dates after the **sync watermark** are pushed through today. The watermark is the last completed date pushed, and it moves only after the endpoint accepts every batch. Today's rollup is re-sent on each sync as it grows.
- On any failure `sync` prints the error, advances nothing and exits non-zero. Without an endpoint and key it prints `sync not configured` and exits 0.
- The key is sent only to the configured host: redirects are never followed, and `sync` warns before sending it over plain `http` to anything but loopback.

### The endpoint contract

- `POST <url>/v1/rollups` with `Content-Type: application/json` and `Authorization: Bearer <device key>`. The URL must be `http` or `https`, with no credentials, query or fragment.
- The body is one batch of up to 2,000 rows. A date is never split across batches, so a single day with more rows than that arrives as one larger batch.
- Reply `200` with `{"accepted": N}`, where N is the number of rows in the batch. Any other status, or a different N, is a failure. A JSON `{"error": "..."}` body is shown to the user.
- Treat each batch as replacing that device's rows for every date it contains, as a unit, because a growing day is re-sent. Re-sending must be harmless.
- Reject fields outside the contract (HTTP 400) rather than storing them.

Known limits: a date at or before the watermark is not re-sent, so late-ingested older transcripts won't reach the endpoint unless the `sync_watermark` row in the ledger's `meta` table is deleted. Sync from the same device should not run concurrently with itself.

## Status

Layer 1 is complete. It was verified against Anthropic's own account accounting: within ~3%, with the gap reconciled.

Layer 2: the sync client is here. Its reference server and dashboard are not published.

Known limits:
- Block anchoring floors to the UTC hour, so it assumes a whole-hour timezone offset.
- Claude Code is the only transcript source so far.
- Cost is computed at ingest time, so a price-table change does not reprice history already in the ledger.

## License

MIT. See [LICENSE](LICENSE).
