# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.1] - 2026-04-15

### Fixed

- Made the LaunchAgent CLI tests platform-aware so GitHub Actions passes on Linux runners while still exercising the full install path on macOS.

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
