# codex-recall

Local search and recall for Codex session JSONL archives.

`codex-recall` indexes Codex transcripts from:

- `~/.codex/sessions`
- `~/.codex/archived_sessions`

It stores a disposable SQLite FTS5 index at:

- `~/.local/share/codex-recall/index.sqlite`

Raw JSONL files remain the source of truth.

## Commands

```bash
codex-recall index
codex-recall search "Stripe webhook"
codex-recall search "Stripe webhook" --json
codex-recall show <session-id>
codex-recall stats
```

Useful flags:

```bash
codex-recall index --db /tmp/index.sqlite --source ~/.codex/sessions/2026/04
codex-recall search "source-map" --limit 5
codex-recall show <session-id> --limit 20
```

## Behavior

- Streams JSONL files and indexes high-signal user, assistant, and command events.
- Skips Codex instruction preambles such as `AGENTS.md` and environment context blocks.
- Deduplicates exact duplicate transcript events.
- Keeps exact source provenance as `path:line`.
- Uses SQLite FTS5 with safe query normalization, so punctuation-heavy queries like `source-map` work.
- Tracks file size and mtime so repeat indexing skips unchanged sessions.
- Groups text search output by session, with the best receipts under each session.
- Keeps `--json` output compact by returning `text_preview` instead of full transcript blobs.

## Local Verification

The full archive on this machine indexed 1,079 session files and 472,286 events in about 59 seconds on the first run.

A repeat run skipped 1,077 unchanged files and completed in about 0.55 seconds.

