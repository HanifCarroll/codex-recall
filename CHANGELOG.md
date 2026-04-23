# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed

- Added `watch --once --repo <repo> --since <date-or-window>` for bounded one-shot catch-ups, including date-directory pruning so agents can avoid walking large old archives when only recent repo context matters.
- Added watcher retry/backoff for transient SQLite lock failures and a `using-stale-index` freshness state so agents can tell a refresh failure from an empty result set.
- Fixed `watch` so a few live writes no longer block indexing an older stable backlog, clarified mixed backlog freshness messages, and added matching `--repo`/`--since` filters to `status` and `doctor`.
- Accepted `recent --all-repos` as a compatibility alias so agent-generated commands stop failing, and clarified in the docs that `recent` is already cross-repo unless `--repo` is set.

## [0.1.3] - 2026-04-15

### Added

- Added deterministic memory extraction during indexing, with stable memory ids plus evidence receipts for decisions, tasks, facts, blockers, and open questions.
- Added `memories`, `memory-show`, `delta`, `related`, `eval`, `resources`, and `read-resource` commands for agent-facing memory retrieval and MCP-style resource access.
- Added append-only `chg_<id>` delta cursors so incremental polling is deterministic and independent of timestamp ordering.
- Expanded `search --trace --json` with normalized query terms, concrete FTS queries, source priority, duplicate identity, and fetch-window details.
- Expanded the fixture-driven eval harness so `search`, `memories`, and `delta` retrieval regressions can be asserted in CI.

## [0.1.2] - 2026-04-15

### Added

- Published `codex-recall` to crates.io and documented the registry install path in the README.
- Added a crates.io badge so the canonical package version is visible from the repo homepage.

## [0.1.1] - 2026-04-15

### Fixed

- Made the LaunchAgent CLI tests platform-aware so GitHub Actions passes on Linux runners while still exercising the full install path on macOS.
- Bumped `actions/checkout` to a Node 24 compatible major in the CI workflow to avoid the GitHub-hosted runner deprecation warning.

## [0.1.0] - 2026-04-15

### Added

- Initial public release of `codex-recall`.
- Full-text search, recent-session listing, day views, bundles, pins, and freshness diagnostics for Codex transcript archives.
- macOS watcher support with LaunchAgent install and bootstrap helpers.
- GitHub Actions CI for formatting, clippy, and tests.
- Public README improvements with support scope, quick-start examples, and privacy guidance.

### Changed

- Switched default local data paths to honor `XDG_DATA_HOME` and `XDG_STATE_HOME` when available.
- Switched transcript source discovery to honor `CODEX_HOME` when set.
- Replaced the personal LaunchAgent label default with the generic `dev.codex-recall.watch`.
- Scrubbed personal repo and vault names from public-facing docs and test fixtures.
