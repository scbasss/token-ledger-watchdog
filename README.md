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
python -m unittest -v test_ccledger
```

## Status

Layer 1 is complete. It was verified against Anthropic's own account accounting: within ~3%, with the gap reconciled.

Known limits:
- Block anchoring floors to the UTC hour, so it assumes a whole-hour timezone offset.
- Claude Code is the only transcript source so far.
- Cost is computed at ingest time, so a price-table change does not reprice history already in the ledger.

## License

MIT. See [LICENSE](LICENSE).
