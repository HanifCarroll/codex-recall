use crate::parser::{EventKind, ParsedSession};
use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};

pub struct Store {
    conn: Connection,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    pub session_count: u64,
    pub event_count: u64,
    pub source_file_count: u64,
    pub duplicate_source_file_count: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub session_id: String,
    pub repo: String,
    pub kind: EventKind,
    pub text: String,
    pub snippet: String,
    pub score: f64,
    pub cwd: String,
    pub source_file_path: PathBuf,
    pub source_line_number: usize,
    pub source_timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOptions {
    pub query: String,
    pub limit: usize,
    pub repo: Option<String>,
    pub cwd: Option<String>,
    pub since: Option<String>,
}

impl SearchOptions {
    pub fn new(query: impl Into<String>, limit: usize) -> Self {
        Self {
            query: query.into(),
            limit,
            repo: None,
            cwd: None,
            since: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEvent {
    pub session_id: String,
    pub kind: EventKind,
    pub text: String,
    pub cwd: String,
    pub source_file_path: PathBuf,
    pub source_line_number: usize,
    pub source_timestamp: Option<String>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create db directory {}", parent.display()))?;
        }

        let conn = Connection::open(path).with_context(|| format!("open db {}", path.display()))?;
        let store = Self { conn };
        store.init_schema()?;
        Ok(store)
    }

    pub fn index_session(&self, parsed: &ParsedSession) -> Result<()> {
        self.conn.execute("BEGIN IMMEDIATE", [])?;
        let result = self.index_session_inner(parsed);
        match result {
            Ok(()) => {
                self.conn.execute("COMMIT", [])?;
                Ok(())
            }
            Err(error) => {
                let _ = self.conn.execute("ROLLBACK", []);
                Err(error)
            }
        }
    }

    pub fn stats(&self) -> Result<Stats> {
        let session_count = self
            .conn
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| {
                row.get::<_, u64>(0)
            })?;
        let event_count = self
            .conn
            .query_row("SELECT COUNT(*) FROM events", [], |row| {
                row.get::<_, u64>(0)
            })?;
        let source_file_count =
            self.conn
                .query_row("SELECT COUNT(*) FROM ingestion_state", [], |row| {
                    row.get::<_, u64>(0)
                })?;
        let unique_ingested_sessions = self.conn.query_row(
            "SELECT COUNT(DISTINCT session_id) FROM ingestion_state WHERE session_id IS NOT NULL",
            [],
            |row| row.get::<_, u64>(0),
        )?;

        Ok(Stats {
            session_count,
            event_count,
            source_file_count,
            duplicate_source_file_count: source_file_count.saturating_sub(unique_ingested_sessions),
        })
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        self.search_with_options(SearchOptions::new(query, limit))
    }

    pub fn search_with_options(&self, options: SearchOptions) -> Result<Vec<SearchResult>> {
        let terms = fts_terms(&options.query);
        if terms.is_empty() {
            return Ok(Vec::new());
        }

        let limit = options.limit.max(1).min(100);
        let results = self.search_with_fts_query(&options, &and_fts_query(&terms), limit)?;
        if !results.is_empty() || terms.len() == 1 {
            return Ok(results);
        }

        self.search_with_fts_query(&options, &or_fts_query(&terms), limit)
    }

    fn search_with_fts_query(
        &self,
        options: &SearchOptions,
        fts_query: &str,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        let mut statement = self.conn.prepare(
            r#"
            SELECT
                events.session_id,
                sessions.repo,
                events.kind,
                events.text,
                snippet(events_fts, 3, '', '', ' ... ', 16) AS snippet,
                bm25(events_fts) AS score,
                sessions.cwd,
                events.source_file_path,
                events.source_line_number,
                events.source_timestamp
            FROM events_fts
            JOIN events ON events.id = events_fts.event_id
            JOIN sessions ON sessions.session_id = events.session_id
            WHERE events_fts MATCH ?
              AND (? IS NULL OR lower(sessions.repo) = lower(?))
              AND (? IS NULL OR sessions.cwd LIKE '%' || ? || '%')
              AND (
                ? IS NULL OR
                datetime(replace(replace(sessions.session_timestamp, 'T', ' '), 'Z', '')) >= datetime(?)
              )
            ORDER BY score ASC, events.source_line_number ASC
            LIMIT ?
            "#,
        )?;

        let rows = statement.query_map(
            params![
                fts_query,
                options.repo,
                options.repo,
                options.cwd,
                options.cwd,
                options.since,
                options.since,
                limit as i64
            ],
            |row| {
                let kind_text: String = row.get(2)?;
                let kind = EventKind::from_str(&kind_text).ok_or_else(|| {
                    rusqlite::Error::InvalidColumnType(
                        2,
                        "kind".to_owned(),
                        rusqlite::types::Type::Text,
                    )
                })?;
                let source_file_path: String = row.get(7)?;
                let source_line_number: i64 = row.get(8)?;

                Ok(SearchResult {
                    session_id: row.get(0)?,
                    repo: row.get(1)?,
                    kind,
                    text: row.get(3)?,
                    snippet: row.get(4)?,
                    score: row.get(5)?,
                    cwd: row.get(6)?,
                    source_file_path: PathBuf::from(source_file_path),
                    source_line_number: source_line_number as usize,
                    source_timestamp: row.get(9)?,
                })
            },
        )?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn session_events(&self, session_id: &str, limit: usize) -> Result<Vec<SessionEvent>> {
        let limit = limit.max(1).min(500);
        let mut statement = self.conn.prepare(
            r#"
            SELECT
                events.session_id,
                events.kind,
                events.text,
                sessions.cwd,
                events.source_file_path,
                events.source_line_number,
                events.source_timestamp
            FROM events
            JOIN sessions ON sessions.session_id = events.session_id
            WHERE events.session_id = ?
            ORDER BY events.source_line_number ASC
            LIMIT ?
            "#,
        )?;

        let rows = statement.query_map(params![session_id, limit as i64], |row| {
            let kind_text: String = row.get(1)?;
            let kind = EventKind::from_str(&kind_text).ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    1,
                    "kind".to_owned(),
                    rusqlite::types::Type::Text,
                )
            })?;
            let source_file_path: String = row.get(4)?;
            let source_line_number: i64 = row.get(5)?;

            Ok(SessionEvent {
                session_id: row.get(0)?,
                kind,
                text: row.get(2)?,
                cwd: row.get(3)?,
                source_file_path: PathBuf::from(source_file_path),
                source_line_number: source_line_number as usize,
                source_timestamp: row.get(6)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn is_source_current(
        &self,
        source_file_path: &Path,
        source_file_mtime_ns: i64,
        source_file_size: i64,
    ) -> Result<bool> {
        let count = self.conn.query_row(
            r#"
            SELECT COUNT(*)
            FROM ingestion_state
            WHERE source_file_path = ?
              AND source_file_mtime_ns = ?
              AND source_file_size = ?
            "#,
            params![
                source_file_path.display().to_string(),
                source_file_mtime_ns,
                source_file_size
            ],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count > 0)
    }

    pub fn mark_source_indexed(
        &self,
        source_file_path: &Path,
        source_file_mtime_ns: i64,
        source_file_size: i64,
        session_id: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO ingestion_state (
                source_file_path, source_file_mtime_ns, source_file_size, session_id, indexed_at
            ) VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            ON CONFLICT(source_file_path) DO UPDATE SET
                source_file_mtime_ns = excluded.source_file_mtime_ns,
                source_file_size = excluded.source_file_size,
                session_id = excluded.session_id,
                indexed_at = excluded.indexed_at
            "#,
            params![
                source_file_path.display().to_string(),
                source_file_mtime_ns,
                source_file_size,
                session_id,
            ],
        )?;
        Ok(())
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;

            CREATE TABLE IF NOT EXISTS sessions (
                session_id TEXT PRIMARY KEY,
                session_timestamp TEXT NOT NULL,
                cwd TEXT NOT NULL,
                repo TEXT NOT NULL DEFAULT '',
                cli_version TEXT,
                source_file_path TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                role TEXT,
                text TEXT NOT NULL,
                command TEXT,
                cwd TEXT,
                exit_code INTEGER,
                source_timestamp TEXT,
                source_file_path TEXT NOT NULL,
                source_line_number INTEGER NOT NULL,
                FOREIGN KEY(session_id) REFERENCES sessions(session_id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS events_session_id_idx ON events(session_id);
            CREATE INDEX IF NOT EXISTS events_source_idx ON events(source_file_path, source_line_number);

            CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
                event_id UNINDEXED,
                session_id UNINDEXED,
                kind UNINDEXED,
                text,
                tokenize = 'porter unicode61'
            );

            CREATE TABLE IF NOT EXISTS ingestion_state (
                source_file_path TEXT PRIMARY KEY,
                source_file_mtime_ns INTEGER NOT NULL,
                source_file_size INTEGER NOT NULL,
                session_id TEXT,
                indexed_at TEXT NOT NULL
            );
            "#,
        )?;
        self.ensure_sessions_repo_column()?;
        self.backfill_session_repos()?;
        Ok(())
    }

    fn index_session_inner(&self, parsed: &ParsedSession) -> Result<()> {
        let repo = repo_slug(&parsed.session.cwd);
        self.conn.execute(
            r#"
            INSERT INTO sessions (
                session_id, session_timestamp, cwd, repo, cli_version, source_file_path
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                session_timestamp = excluded.session_timestamp,
                cwd = excluded.cwd,
                repo = excluded.repo,
                cli_version = excluded.cli_version,
                source_file_path = excluded.source_file_path
            "#,
            params![
                parsed.session.id,
                parsed.session.timestamp,
                parsed.session.cwd,
                repo,
                parsed.session.cli_version,
                parsed.session.source_file_path.display().to_string(),
            ],
        )?;

        self.conn.execute(
            "DELETE FROM events_fts WHERE session_id = ?",
            params![parsed.session.id],
        )?;
        self.conn.execute(
            "DELETE FROM events WHERE session_id = ?",
            params![parsed.session.id],
        )?;

        for event in &parsed.events {
            self.conn.execute(
                r#"
                INSERT INTO events (
                    session_id, kind, role, text, command, cwd, exit_code,
                    source_timestamp, source_file_path, source_line_number
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                params![
                    parsed.session.id,
                    event.kind.as_str(),
                    event.role,
                    event.text,
                    event.command,
                    event.cwd,
                    event.exit_code,
                    event.source_timestamp,
                    event.source_file_path.display().to_string(),
                    event.source_line_number as i64,
                ],
            )?;
            let event_id = self.conn.last_insert_rowid();
            self.conn.execute(
                "INSERT INTO events_fts (event_id, session_id, kind, text) VALUES (?, ?, ?, ?)",
                params![event_id, parsed.session.id, event.kind.as_str(), event.text],
            )?;
        }

        Ok(())
    }

    fn ensure_sessions_repo_column(&self) -> Result<()> {
        let has_repo = self
            .conn
            .prepare("PRAGMA table_info(sessions)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<std::result::Result<Vec<_>, _>>()?
            .iter()
            .any(|column| column == "repo");

        if !has_repo {
            match self.conn.execute(
                "ALTER TABLE sessions ADD COLUMN repo TEXT NOT NULL DEFAULT ''",
                [],
            ) {
                Ok(_) => {}
                Err(error) if error.to_string().contains("duplicate column name") => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }

    fn backfill_session_repos(&self) -> Result<()> {
        let mut statement = self
            .conn
            .prepare("SELECT session_id, cwd FROM sessions WHERE repo = ''")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        for row in rows {
            let (session_id, cwd) = row?;
            self.conn.execute(
                "UPDATE sessions SET repo = ? WHERE session_id = ?",
                params![repo_slug(&cwd), session_id],
            )?;
        }
        Ok(())
    }
}

fn fts_terms(query: &str) -> Vec<String> {
    let mut terms = Vec::new();
    let mut current = String::new();

    for ch in query.chars() {
        if ch.is_alphanumeric() || ch == '_' {
            current.push(ch);
        } else if !current.is_empty() {
            terms.push(std::mem::take(&mut current));
        }
    }

    if !current.is_empty() {
        terms.push(current);
    }

    terms
}

fn quote_fts_term(term: &str) -> String {
    format!("\"{}\"", term.replace('"', "\"\""))
}

fn and_fts_query(terms: &[String]) -> String {
    terms
        .iter()
        .map(|term| quote_fts_term(term))
        .collect::<Vec<_>>()
        .join(" AND ")
}

fn or_fts_query(terms: &[String]) -> String {
    terms
        .into_iter()
        .map(|term| quote_fts_term(term))
        .collect::<Vec<_>>()
        .join(" OR ")
}

fn repo_slug(cwd: &str) -> String {
    Path::new(cwd)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(cwd)
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::{EventKind, ParsedEvent, ParsedSession, SessionMetadata};
    use std::path::PathBuf;

    fn temp_db_path(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "codex-recall-store-test-{}-{}",
            std::process::id(),
            name
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("index.sqlite")
    }

    fn sample_session() -> ParsedSession {
        sample_session_with(
            "session-1",
            "/Users/me/project",
            "2026-04-13T01:00:00Z",
            "/tmp/session.jsonl",
        )
    }

    fn sample_session_with(
        id: &str,
        cwd: &str,
        timestamp: &str,
        source_path: &str,
    ) -> ParsedSession {
        let source = PathBuf::from(source_path);
        ParsedSession {
            session: SessionMetadata {
                id: id.to_owned(),
                timestamp: timestamp.to_owned(),
                cwd: cwd.to_owned(),
                cli_version: Some("0.1.0".to_owned()),
                source_file_path: source.clone(),
            },
            events: vec![
                ParsedEvent {
                    session_id: "session-1".to_owned(),
                    kind: EventKind::UserMessage,
                    role: Some("user".to_owned()),
                    text: "Find the RevenueCat Stripe webhook bug".to_owned(),
                    command: None,
                    cwd: None,
                    exit_code: None,
                    source_timestamp: Some("2026-04-13T01:00:01Z".to_owned()),
                    source_file_path: source.clone(),
                    source_line_number: 2,
                },
                ParsedEvent {
                    session_id: "session-1".to_owned(),
                    kind: EventKind::AssistantMessage,
                    role: Some("assistant".to_owned()),
                    text: "The webhook secret was missing in production.".to_owned(),
                    command: None,
                    cwd: None,
                    exit_code: None,
                    source_timestamp: Some("2026-04-13T01:00:02Z".to_owned()),
                    source_file_path: source,
                    source_line_number: 3,
                },
            ],
        }
    }

    #[test]
    fn indexes_sessions_idempotently_and_counts_rows() {
        let store = Store::open(temp_db_path("idempotent")).unwrap();
        let session = sample_session();

        store.index_session(&session).unwrap();
        store.index_session(&session).unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.session_count, 1);
        assert_eq!(stats.event_count, 2);
    }

    #[test]
    fn searches_fts_with_source_provenance() {
        let store = Store::open(temp_db_path("search")).unwrap();
        store.index_session(&sample_session()).unwrap();

        let results = store.search("webhook secret", 5).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].session_id, "session-1");
        assert_eq!(results[0].kind, EventKind::AssistantMessage);
        assert_eq!(results[0].source_line_number, 3);
        assert_eq!(results[0].cwd, "/Users/me/project");
        assert!(results[0].snippet.contains("webhook"));
        assert!(results[0].score < 0.0);
    }

    #[test]
    fn filters_search_by_repo_cwd_and_since() {
        let store = Store::open(temp_db_path("filters")).unwrap();
        store
            .index_session(&sample_session_with(
                "old-palabruno",
                "/Users/me/projects/palabruno",
                "2026-04-01T01:00:00Z",
                "/tmp/old.jsonl",
            ))
            .unwrap();
        store
            .index_session(&sample_session_with(
                "new-palabruno",
                "/Users/me/projects/palabruno",
                "2026-04-13T01:00:00Z",
                "/tmp/new.jsonl",
            ))
            .unwrap();
        store
            .index_session(&sample_session_with(
                "genrupt",
                "/Users/me/projects/Genrupt",
                "2026-04-13T01:00:00Z",
                "/tmp/genrupt.jsonl",
            ))
            .unwrap();

        let results = store
            .search_with_options(SearchOptions {
                query: "webhook secret".to_owned(),
                limit: 10,
                repo: Some("palabruno".to_owned()),
                cwd: Some("projects/palabruno".to_owned()),
                since: Some("2026-04-10".to_owned()),
            })
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].session_id, "new-palabruno");
        assert_eq!(results[0].repo, "palabruno");
    }

    #[test]
    fn falls_back_to_any_query_term_when_all_terms_match_no_single_event() {
        let store = Store::open(temp_db_path("fallback")).unwrap();
        store.index_session(&sample_session()).unwrap();

        let results = store.search("RevenueCat missing", 5).unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].session_id, "session-1");
    }

    #[test]
    fn search_accepts_punctuation_without_exposing_fts_syntax() {
        let store = Store::open(temp_db_path("punctuation")).unwrap();
        store.index_session(&sample_session()).unwrap();

        let results = store.search("webhook-secret", 5).unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].source_line_number, 3);
    }
}
