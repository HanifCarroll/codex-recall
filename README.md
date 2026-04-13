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
codex-recall rebuild
codex-recall watch
codex-recall watch --once
codex-recall status
codex-recall status --json
codex-recall search "Stripe webhook"
codex-recall search "Stripe webhook" --repo palabruno --since 2026-04-01
codex-recall search "Stripe webhook" --since 7d
codex-recall search "Stripe webhook" --cwd projects/palabruno
codex-recall search "Stripe webhook" --json
codex-recall bundle "Stripe webhook" --repo palabruno --since 14d
codex-recall show <session-id-or-session-key>
codex-recall doctor --json
codex-recall stats
```

Useful flags:

```bash
codex-recall index --db /tmp/index.sqlite --source ~/.codex/sessions/2026/04
codex-recall watch --interval 30 --quiet-for 5
codex-recall watch --install-launch-agent
codex-recall search "source-map" --limit 5
codex-recall search "source-map" --all-repos
codex-recall show <session-key> --limit 20
```

## Behavior

- Streams JSONL files and indexes high-signal user, assistant, and command events.
- Redacts common secret shapes before writing searchable text to SQLite.
- Skips Codex instruction preambles such as `AGENTS.md` and environment context blocks.
- Deduplicates exact duplicate transcript events.
- Keeps exact source provenance as `path:line`.
- Stores a stable `session_key` derived from `session_id + source_file_path`, so duplicate active/archive transcripts do not collapse.
- Uses SQLite FTS5 with safe query normalization, so punctuation-heavy queries like `source-map` work.
- Falls back to matching any query term when no single event contains every term.
- Supports search filters by repo slug, cwd substring, and session start date. Repo matching uses both the session cwd and command cwd values seen inside the session.
- Accepts absolute `--since` dates plus relative values like `7d`, `30d`, `today`, and `yesterday`.
- Interprets `today` and `yesterday` using the local day boundary, then compares against UTC transcript timestamps.
- Boosts results from the current git repo by default. Use `--repo` to filter to a repo, or `--all-repos` to disable the current-repo boost.
- Tracks file size and mtime so repeat indexing skips unchanged sessions.
- Reports indexing progress to stderr with discovered file totals, bytes processed, elapsed time, ETA, current file, and skipped-file reason counts.
- Watches session roots with a polling freshness loop, waits for files to be quiet before indexing, and records watcher state in `~/.local/state/codex-recall/watch.json`.
- Reports freshness status with pending file counts, stable/waiting file counts, last indexed time, and the last watcher error.
- Can write a macOS LaunchAgent plist for the watcher with `watch --install-launch-agent`.
- Groups text search output by session, with the best receipts under each session.
- Formats search results into an agent-ready context bundle with top sessions, receipts, and follow-up `show` commands.
- Ranks sessions by current-repo match, hit count, event kind, FTS rank, and recency.
- Reports source-file counts and duplicate source-file counts in `stats`.
- Keeps `--json` output compact by returning `text_preview` instead of full transcript blobs.
- Separates progress and diagnostics onto stderr so `--json` output stays pipe-safe.

## Maintenance

Use `doctor` when the index feels stale or suspicious:

```bash
codex-recall doctor
codex-recall doctor --json
```

`doctor` is read-only when the database is missing; it reports the missing index instead of creating an empty one.

Use `rebuild` when the disposable SQLite index should be recreated from the raw JSONL source files:

```bash
codex-recall rebuild
```

Use `watch` when the index should stay fresh while Codex or Hermes writes new transcripts:

```bash
codex-recall watch
codex-recall status
```

`watch --install-launch-agent` writes a plist to `~/Library/LaunchAgents/com.hanif.codex-recall.watch.plist` and prints the `launchctl bootstrap` command to start it.

Use `bundle` when an agent needs compact prior-session context:

```bash
codex-recall bundle "Hermes global skills" --since 14d --limit 5
```

## Local Verification

The April 13, 2026 full historical rebuild on this machine parsed 1,090 session files and 485,037 events in about 28 minutes. Large live archives can have long gaps between files, so use stderr progress for current-file and ETA visibility.

A repeat index run skips unchanged files and should finish much faster.
