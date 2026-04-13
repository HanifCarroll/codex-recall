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
}

#[derive(Debug, Clone, PartialEq)]
pub struct SearchResult {
    pub session_id: String,
    pub kind: EventKind,
    pub text: String,
    pub snippet: String,
    pub score: f64,
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

        Ok(Stats {
            session_count,
            event_count,
        })
    }

    pub fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchResult>> {
        let limit = limit.max(1).min(100);
        let mut statement = self.conn.prepare(
            r#"
            SELECT
                events.session_id,
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
            ORDER BY score ASC, events.source_line_number ASC
            LIMIT ?
            "#,
        )?;

        let rows = statement.query_map(params![query, limit as i64], |row| {
            let kind_text: String = row.get(1)?;
            let kind = EventKind::from_str(&kind_text).ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    1,
                    "kind".to_owned(),
                    rusqlite::types::Type::Text,
                )
            })?;
            let source_file_path: String = row.get(6)?;
            let source_line_number: i64 = row.get(7)?;

            Ok(SearchResult {
                session_id: row.get(0)?,
                kind,
                text: row.get(2)?,
                snippet: row.get(3)?,
                score: row.get(4)?,
                cwd: row.get(5)?,
                source_file_path: PathBuf::from(source_file_path),
                source_line_number: source_line_number as usize,
                source_timestamp: row.get(8)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
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
            "#,
        )?;
        Ok(())
    }

    fn index_session_inner(&self, parsed: &ParsedSession) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO sessions (
                session_id, session_timestamp, cwd, cli_version, source_file_path
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                session_timestamp = excluded.session_timestamp,
                cwd = excluded.cwd,
                cli_version = excluded.cli_version,
                source_file_path = excluded.source_file_path
            "#,
            params![
                parsed.session.id,
                parsed.session.timestamp,
                parsed.session.cwd,
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
        let source = PathBuf::from("/tmp/session.jsonl");
        ParsedSession {
            session: SessionMetadata {
                id: "session-1".to_owned(),
                timestamp: "2026-04-13T01:00:00Z".to_owned(),
                cwd: "/Users/me/project".to_owned(),
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
}
