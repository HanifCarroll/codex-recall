use crate::parser::{EventKind, ParsedSession};
use anyhow::{Context, Result};
use rusqlite::{params, params_from_iter, Connection};
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::time::Duration;

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
    pub session_key: String,
    pub session_id: String,
    pub repo: String,
    pub kind: EventKind,
    pub text: String,
    pub snippet: String,
    pub score: f64,
    pub session_timestamp: String,
    pub cwd: String,
    pub source_file_path: PathBuf,
    pub source_line_number: usize,
    pub source_timestamp: Option<String>,
    repo_matches_current: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchOptions {
    pub query: String,
    pub limit: usize,
    pub repo: Option<String>,
    pub cwd: Option<String>,
    pub since: Option<String>,
    pub current_repo: Option<String>,
}

impl SearchOptions {
    pub fn new(query: impl Into<String>, limit: usize) -> Self {
        Self {
            query: query.into(),
            limit,
            repo: None,
            cwd: None,
            since: None,
            current_repo: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionEvent {
    pub session_key: String,
    pub session_id: String,
    pub kind: EventKind,
    pub text: String,
    pub cwd: String,
    pub source_file_path: PathBuf,
    pub source_line_number: usize,
    pub source_timestamp: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMatch {
    pub session_key: String,
    pub session_id: String,
    pub cwd: String,
    pub repo: String,
    pub source_file_path: PathBuf,
}

struct OldSessionRow {
    session_id: String,
    session_timestamp: String,
    cwd: String,
    repo: String,
    cli_version: Option<String>,
    source_file_path: String,
}

struct OldEventRow {
    session_id: String,
    kind: String,
    role: Option<String>,
    text: String,
    command: Option<String>,
    cwd: Option<String>,
    exit_code: Option<i64>,
    source_timestamp: Option<String>,
    source_file_path: String,
    source_line_number: i64,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create db directory {}", parent.display()))?;
        }

        let conn = Connection::open(path).with_context(|| format!("open db {}", path.display()))?;
        conn.busy_timeout(Duration::from_secs(5))?;
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
            "SELECT COUNT(DISTINCT COALESCE(session_key, session_id)) FROM ingestion_state WHERE COALESCE(session_key, session_id) IS NOT NULL",
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

    pub fn quick_check(&self) -> Result<String> {
        self.conn
            .query_row("PRAGMA quick_check", [], |row| row.get::<_, String>(0))
            .map_err(Into::into)
    }

    pub fn fts_integrity_check(&self) -> Result<()> {
        self.conn.execute(
            "INSERT INTO events_fts(events_fts) VALUES('integrity-check')",
            [],
        )?;
        Ok(())
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
        let fetch_limit = limit.saturating_mul(50).max(200).min(1_000);
        let results = self.search_with_fts_query(&options, &and_fts_query(&terms), fetch_limit)?;
        if !results.is_empty() || terms.len() == 1 {
            return Ok(rank_search_results(
                results,
                options.current_repo.as_deref(),
                limit,
            ));
        }

        let results = self.search_with_fts_query(&options, &or_fts_query(&terms), fetch_limit)?;
        Ok(rank_search_results(
            results,
            options.current_repo.as_deref(),
            limit,
        ))
    }

    fn search_with_fts_query(
        &self,
        options: &SearchOptions,
        fts_query: &str,
        limit: usize,
    ) -> Result<Vec<SearchResult>> {
        let mut query_params = Vec::<String>::new();
        let current_repo_expr = if let Some(current_repo) = &options.current_repo {
            query_params.push(current_repo.clone());
            "EXISTS (
                    SELECT 1 FROM session_repos current_repos
                    WHERE current_repos.session_key = sessions.session_key
                      AND lower(current_repos.repo) = lower(?)
                )"
        } else {
            "0"
        };
        query_params.push(fts_query.to_owned());

        let mut sql = format!(
            r#"
            SELECT
                events.session_key,
                events.session_id,
                sessions.repo,
                events.kind,
                events.text,
                snippet(events_fts, 4, '', '', ' ... ', 16) AS snippet,
                events_fts.rank AS score,
                sessions.session_timestamp,
                sessions.cwd,
                events.source_file_path,
                events.source_line_number,
                events.source_timestamp,
                {current_repo_expr} AS current_repo_match
            FROM events_fts
            JOIN events ON events.id = events_fts.event_id
            JOIN sessions ON sessions.session_key = events.session_key
            WHERE events_fts MATCH ?
            "#,
        );

        if let Some(repo) = &options.repo {
            sql.push_str(
                r#"
                AND EXISTS (
                    SELECT 1 FROM session_repos filter_repos
                    WHERE filter_repos.session_key = sessions.session_key
                      AND lower(filter_repos.repo) = lower(?)
                )
                "#,
            );
            query_params.push(repo.clone());
        }
        if let Some(cwd) = &options.cwd {
            sql.push_str(
                r#"
                AND (
                    sessions.cwd LIKE '%' || ? || '%'
                    OR EXISTS (
                        SELECT 1 FROM events cwd_events
                        WHERE cwd_events.session_key = sessions.session_key
                          AND cwd_events.cwd LIKE '%' || ? || '%'
                    )
                )
                "#,
            );
            query_params.push(cwd.clone());
            query_params.push(cwd.clone());
        }
        if let Some(since) = &options.since {
            append_since_clause(&mut sql, &mut query_params, since)?;
        }

        sql.push_str(" ORDER BY events_fts.rank ASC, events.source_line_number ASC LIMIT ");
        sql.push_str(&limit.to_string());

        let mut statement = self.conn.prepare(&sql)?;

        let rows = statement.query_map(params_from_iter(query_params.iter()), |row| {
            let kind_text: String = row.get(3)?;
            let kind = EventKind::from_str(&kind_text).ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    3,
                    "kind".to_owned(),
                    rusqlite::types::Type::Text,
                )
            })?;
            let source_file_path: String = row.get(9)?;
            let source_line_number: i64 = row.get(10)?;

            Ok(SearchResult {
                session_key: row.get(0)?,
                session_id: row.get(1)?,
                repo: row.get(2)?,
                kind,
                text: row.get(4)?,
                snippet: row.get(5)?,
                score: row.get(6)?,
                session_timestamp: row.get(7)?,
                cwd: row.get(8)?,
                source_file_path: PathBuf::from(source_file_path),
                source_line_number: source_line_number as usize,
                source_timestamp: row.get(11)?,
                repo_matches_current: row.get::<_, i64>(12)? != 0,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn resolve_session_reference(&self, reference: &str) -> Result<Vec<SessionMatch>> {
        let mut statement = self.conn.prepare(
            r#"
            SELECT session_key, session_id, cwd, repo, source_file_path
            FROM sessions
            WHERE session_key = ? OR session_id = ?
            ORDER BY session_timestamp DESC, source_file_path ASC
            "#,
        )?;
        let rows = statement.query_map(params![reference, reference], |row| {
            let source_file_path: String = row.get(4)?;
            Ok(SessionMatch {
                session_key: row.get(0)?,
                session_id: row.get(1)?,
                cwd: row.get(2)?,
                repo: row.get(3)?,
                source_file_path: PathBuf::from(source_file_path),
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn session_events(&self, session_key: &str, limit: usize) -> Result<Vec<SessionEvent>> {
        let limit = limit.max(1).min(500);
        let mut statement = self.conn.prepare(
            r#"
            SELECT
                events.session_key,
                events.session_id,
                events.kind,
                events.text,
                sessions.cwd,
                events.source_file_path,
                events.source_line_number,
                events.source_timestamp
            FROM events
            JOIN sessions ON sessions.session_key = events.session_key
            WHERE events.session_key = ?
            ORDER BY events.source_line_number ASC
            LIMIT ?
            "#,
        )?;

        let rows = statement.query_map(params![session_key, limit as i64], |row| {
            let kind_text: String = row.get(2)?;
            let kind = EventKind::from_str(&kind_text).ok_or_else(|| {
                rusqlite::Error::InvalidColumnType(
                    2,
                    "kind".to_owned(),
                    rusqlite::types::Type::Text,
                )
            })?;
            let source_file_path: String = row.get(5)?;
            let source_line_number: i64 = row.get(6)?;

            Ok(SessionEvent {
                session_key: row.get(0)?,
                session_id: row.get(1)?,
                kind,
                text: row.get(3)?,
                cwd: row.get(4)?,
                source_file_path: PathBuf::from(source_file_path),
                source_line_number: source_line_number as usize,
                source_timestamp: row.get(7)?,
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
        session_key: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO ingestion_state (
                source_file_path, source_file_mtime_ns, source_file_size, session_id, session_key, indexed_at
            ) VALUES (?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
            ON CONFLICT(source_file_path) DO UPDATE SET
                source_file_mtime_ns = excluded.source_file_mtime_ns,
                source_file_size = excluded.source_file_size,
                session_id = excluded.session_id,
                session_key = excluded.session_key,
                indexed_at = excluded.indexed_at
            "#,
            params![
                source_file_path.display().to_string(),
                source_file_mtime_ns,
                source_file_size,
                session_id,
                session_key,
            ],
        )?;
        Ok(())
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            "#,
        )?;

        if self.table_exists("sessions")? && !self.table_has_column("sessions", "session_key")? {
            self.migrate_to_session_key_schema()?;
        }

        self.create_schema_objects()?;
        self.ensure_ingestion_state_session_key_column()?;
        self.backfill_session_repos()?;
        self.backfill_session_repo_memberships()?;
        self.backfill_ingestion_session_keys()?;
        Ok(())
    }

    fn create_schema_objects(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS sessions (
                session_key TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                session_timestamp TEXT NOT NULL,
                cwd TEXT NOT NULL,
                repo TEXT NOT NULL DEFAULT '',
                cli_version TEXT,
                source_file_path TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS sessions_session_id_idx ON sessions(session_id);
            CREATE INDEX IF NOT EXISTS sessions_repo_idx ON sessions(repo);

            CREATE TABLE IF NOT EXISTS session_repos (
                session_key TEXT NOT NULL,
                repo TEXT NOT NULL,
                PRIMARY KEY(session_key, repo),
                FOREIGN KEY(session_key) REFERENCES sessions(session_key) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS session_repos_repo_idx ON session_repos(repo);

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_key TEXT NOT NULL,
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
                FOREIGN KEY(session_key) REFERENCES sessions(session_key) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS events_session_key_idx ON events(session_key);
            CREATE INDEX IF NOT EXISTS events_session_id_idx ON events(session_id);
            CREATE INDEX IF NOT EXISTS events_source_idx ON events(source_file_path, source_line_number);

            CREATE VIRTUAL TABLE IF NOT EXISTS events_fts USING fts5(
                event_id UNINDEXED,
                session_key UNINDEXED,
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
                session_key TEXT,
                indexed_at TEXT NOT NULL
            );
            "#,
        )?;
        Ok(())
    }

    fn index_session_inner(&self, parsed: &ParsedSession) -> Result<()> {
        let session_key = session_key(&parsed.session.id, &parsed.session.source_file_path);
        let repo = repo_slug(&parsed.session.cwd);
        self.conn.execute(
            r#"
            INSERT INTO sessions (
                session_key, session_id, session_timestamp, cwd, repo, cli_version, source_file_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_key) DO UPDATE SET
                session_id = excluded.session_id,
                session_timestamp = excluded.session_timestamp,
                cwd = excluded.cwd,
                repo = excluded.repo,
                cli_version = excluded.cli_version,
                source_file_path = excluded.source_file_path
            "#,
            params![
                session_key.as_str(),
                parsed.session.id,
                parsed.session.timestamp,
                parsed.session.cwd,
                repo,
                parsed.session.cli_version,
                parsed.session.source_file_path.display().to_string(),
            ],
        )?;

        self.conn.execute(
            "DELETE FROM events_fts WHERE session_key = ?",
            params![session_key.as_str()],
        )?;
        self.conn.execute(
            "DELETE FROM events WHERE session_key = ?",
            params![session_key.as_str()],
        )?;
        self.conn.execute(
            "DELETE FROM session_repos WHERE session_key = ?",
            params![session_key.as_str()],
        )?;

        for repo in session_repos(parsed) {
            self.conn.execute(
                "INSERT OR IGNORE INTO session_repos (session_key, repo) VALUES (?, ?)",
                params![session_key.as_str(), repo],
            )?;
        }

        for event in &parsed.events {
            self.conn.execute(
                r#"
                INSERT INTO events (
                    session_key, session_id, kind, role, text, command, cwd, exit_code,
                    source_timestamp, source_file_path, source_line_number
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#,
                params![
                    session_key.as_str(),
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
                "INSERT INTO events_fts (event_id, session_key, session_id, kind, text) VALUES (?, ?, ?, ?, ?)",
                params![
                    event_id,
                    session_key.as_str(),
                    parsed.session.id,
                    event.kind.as_str(),
                    event.text
                ],
            )?;
        }

        Ok(())
    }

    fn backfill_session_repos(&self) -> Result<()> {
        let mut statement = self
            .conn
            .prepare("SELECT session_key, cwd FROM sessions WHERE repo = ''")?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        for row in rows {
            let (session_key, cwd) = row?;
            self.conn.execute(
                "UPDATE sessions SET repo = ? WHERE session_key = ?",
                params![repo_slug(&cwd), session_key],
            )?;
        }
        Ok(())
    }

    fn table_exists(&self, table_name: &str) -> Result<bool> {
        let count = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
            params![table_name],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count > 0)
    }

    fn table_has_column(&self, table_name: &str, column_name: &str) -> Result<bool> {
        let mut statement = self
            .conn
            .prepare(&format!("PRAGMA table_info({table_name})"))?;
        let columns = statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(columns.iter().any(|column| column == column_name))
    }

    fn ensure_ingestion_state_session_key_column(&self) -> Result<()> {
        if self.table_has_column("ingestion_state", "session_key")? {
            return Ok(());
        }

        match self.conn.execute(
            "ALTER TABLE ingestion_state ADD COLUMN session_key TEXT",
            [],
        ) {
            Ok(_) => Ok(()),
            Err(error) if error.to_string().contains("duplicate column name") => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn backfill_ingestion_session_keys(&self) -> Result<()> {
        if !self.table_exists("ingestion_state")? {
            return Ok(());
        }

        self.conn.execute(
            r#"
            UPDATE ingestion_state
            SET session_key = (
                SELECT sessions.session_key
                FROM sessions
                WHERE sessions.source_file_path = ingestion_state.source_file_path
                LIMIT 1
            )
            WHERE session_key IS NULL
              AND session_id IS NOT NULL
            "#,
            [],
        )?;
        Ok(())
    }

    fn backfill_session_repo_memberships(&self) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT OR IGNORE INTO session_repos (session_key, repo)
            SELECT session_key, repo
            FROM sessions
            WHERE repo != ''
            "#,
            [],
        )?;

        let mut statement = self.conn.prepare(
            r#"
            SELECT DISTINCT session_key, cwd
            FROM events
            WHERE cwd IS NOT NULL
              AND cwd != ''
            "#,
        )?;
        let rows = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;

        for row in rows {
            let (session_key, cwd) = row?;
            let repo = repo_slug(&cwd);
            if repo.is_empty() {
                continue;
            }
            self.conn.execute(
                "INSERT OR IGNORE INTO session_repos (session_key, repo) VALUES (?, ?)",
                params![session_key, repo],
            )?;
        }

        Ok(())
    }

    fn migrate_to_session_key_schema(&self) -> Result<()> {
        let old_sessions = self.load_old_sessions()?;
        let old_events = self.load_old_events()?;
        let mut keys_by_session_id = HashMap::new();
        for session in &old_sessions {
            keys_by_session_id.insert(
                session.session_id.clone(),
                session_key(&session.session_id, Path::new(&session.source_file_path)),
            );
        }

        self.conn.execute("BEGIN IMMEDIATE", [])?;
        let result = (|| -> Result<()> {
            self.conn.execute_batch(
                r#"
                DROP TABLE IF EXISTS events_fts;
                DROP TABLE IF EXISTS events;
                DROP TABLE IF EXISTS sessions;
                "#,
            )?;
            self.create_schema_objects()?;

            for session in &old_sessions {
                let session_key = keys_by_session_id
                    .get(&session.session_id)
                    .expect("session key exists");
                let repo = if session.repo.is_empty() {
                    repo_slug(&session.cwd)
                } else {
                    session.repo.clone()
                };
                self.conn.execute(
                    r#"
                    INSERT INTO sessions (
                        session_key, session_id, session_timestamp, cwd, repo, cli_version, source_file_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    "#,
                    params![
                        session_key,
                        session.session_id,
                        session.session_timestamp,
                        session.cwd,
                        repo,
                        session.cli_version,
                        session.source_file_path,
                    ],
                )?;
                self.conn.execute(
                    "INSERT OR IGNORE INTO session_repos (session_key, repo) VALUES (?, ?)",
                    params![session_key, repo],
                )?;
            }

            for event in &old_events {
                let Some(session_key) = keys_by_session_id.get(&event.session_id) else {
                    continue;
                };
                self.conn.execute(
                    r#"
                    INSERT INTO events (
                        session_key, session_id, kind, role, text, command, cwd, exit_code,
                        source_timestamp, source_file_path, source_line_number
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    "#,
                    params![
                        session_key,
                        event.session_id,
                        event.kind,
                        event.role,
                        event.text,
                        event.command,
                        event.cwd,
                        event.exit_code,
                        event.source_timestamp,
                        event.source_file_path,
                        event.source_line_number,
                    ],
                )?;
                let event_id = self.conn.last_insert_rowid();
                self.conn.execute(
                    "INSERT INTO events_fts (event_id, session_key, session_id, kind, text) VALUES (?, ?, ?, ?, ?)",
                    params![
                        event_id,
                        session_key,
                        event.session_id,
                        event.kind,
                        event.text
                    ],
                )?;
            }

            Ok(())
        })();

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

    fn load_old_sessions(&self) -> Result<Vec<OldSessionRow>> {
        let has_repo = self.table_has_column("sessions", "repo")?;
        let sql = if has_repo {
            "SELECT session_id, session_timestamp, cwd, repo, cli_version, source_file_path FROM sessions"
        } else {
            "SELECT session_id, session_timestamp, cwd, '' AS repo, cli_version, source_file_path FROM sessions"
        };
        let mut statement = self.conn.prepare(sql)?;
        let rows = statement.query_map([], |row| {
            Ok(OldSessionRow {
                session_id: row.get(0)?,
                session_timestamp: row.get(1)?,
                cwd: row.get(2)?,
                repo: row.get(3)?,
                cli_version: row.get(4)?,
                source_file_path: row.get(5)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn load_old_events(&self) -> Result<Vec<OldEventRow>> {
        if !self.table_exists("events")? {
            return Ok(Vec::new());
        }

        let mut statement = self.conn.prepare(
            r#"
            SELECT
                session_id, kind, role, text, command, cwd, exit_code,
                source_timestamp, source_file_path, source_line_number
            FROM events
            ORDER BY id ASC
            "#,
        )?;
        let rows = statement.query_map([], |row| {
            Ok(OldEventRow {
                session_id: row.get(0)?,
                kind: row.get(1)?,
                role: row.get(2)?,
                text: row.get(3)?,
                command: row.get(4)?,
                cwd: row.get(5)?,
                exit_code: row.get(6)?,
                source_timestamp: row.get(7)?,
                source_file_path: row.get(8)?,
                source_line_number: row.get(9)?,
            })
        })?;

        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

enum SinceFilter {
    Absolute(String),
    LastDays(u32),
    Today,
    Yesterday,
}

fn parse_since_filter(value: &str) -> Result<SinceFilter> {
    let trimmed = value.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower == "today" {
        return Ok(SinceFilter::Today);
    }
    if lower == "yesterday" {
        return Ok(SinceFilter::Yesterday);
    }
    if let Some(days) = lower.strip_suffix('d') {
        let days = days
            .parse::<u32>()
            .with_context(|| format!("parse --since relative day value `{value}`"))?;
        if days == 0 {
            return Ok(SinceFilter::Today);
        }
        return Ok(SinceFilter::LastDays(days));
    }
    if looks_like_absolute_date(trimmed) {
        return Ok(SinceFilter::Absolute(trimmed.to_owned()));
    }

    anyhow::bail!(
        "unsupported --since value `{value}`; use YYYY-MM-DD, today, yesterday, or Nd like 7d"
    )
}

fn looks_like_absolute_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() >= 10
        && bytes[0..4].iter().all(|byte| byte.is_ascii_digit())
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(|byte| byte.is_ascii_digit())
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(|byte| byte.is_ascii_digit())
}

fn append_since_clause(
    sql: &mut String,
    query_params: &mut Vec<String>,
    value: &str,
) -> Result<()> {
    sql.push_str(
        " AND datetime(replace(replace(sessions.session_timestamp, 'T', ' '), 'Z', '')) >= ",
    );
    match parse_since_filter(value)? {
        SinceFilter::Absolute(value) => {
            sql.push_str("datetime(?)");
            query_params.push(value);
        }
        SinceFilter::LastDays(days) => {
            sql.push_str("datetime('now', ?)");
            query_params.push(format!("-{days} days"));
        }
        SinceFilter::Today => {
            sql.push_str("datetime('now', 'start of day')");
        }
        SinceFilter::Yesterday => {
            sql.push_str("datetime('now', 'start of day', '-1 day')");
        }
    }
    Ok(())
}

struct SessionGroup {
    session_key: String,
    repo_matches_current: bool,
    hit_count: usize,
    best_score: f64,
    best_kind_weight: u8,
    session_timestamp: String,
    results: Vec<SearchResult>,
}

fn rank_search_results(
    results: Vec<SearchResult>,
    _current_repo: Option<&str>,
    limit: usize,
) -> Vec<SearchResult> {
    let mut groups = Vec::<SessionGroup>::new();

    for result in results {
        let kind_weight = event_kind_weight(result.kind);
        if let Some(group) = groups
            .iter_mut()
            .find(|group| group.session_key == result.session_key)
        {
            group.hit_count += 1;
            group.best_score = group.best_score.min(result.score);
            group.best_kind_weight = group.best_kind_weight.min(kind_weight);
            group.results.push(result);
        } else {
            groups.push(SessionGroup {
                session_key: result.session_key.clone(),
                repo_matches_current: result.repo_matches_current,
                hit_count: 1,
                best_score: result.score,
                best_kind_weight: kind_weight,
                session_timestamp: result.session_timestamp.clone(),
                results: vec![result],
            });
        }
    }

    groups.sort_by(|left, right| {
        right
            .repo_matches_current
            .cmp(&left.repo_matches_current)
            .then_with(|| right.hit_count.cmp(&left.hit_count))
            .then_with(|| left.best_kind_weight.cmp(&right.best_kind_weight))
            .then_with(|| {
                left.best_score
                    .partial_cmp(&right.best_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| right.session_timestamp.cmp(&left.session_timestamp))
            .then_with(|| left.session_key.cmp(&right.session_key))
    });

    let mut ranked = Vec::new();
    for mut group in groups {
        group.results.sort_by(|left, right| {
            left.score
                .partial_cmp(&right.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| event_kind_weight(left.kind).cmp(&event_kind_weight(right.kind)))
                .then_with(|| left.source_line_number.cmp(&right.source_line_number))
        });
        ranked.extend(group.results);
        if ranked.len() >= limit {
            ranked.truncate(limit);
            break;
        }
    }

    ranked
}

fn event_kind_weight(kind: EventKind) -> u8 {
    match kind {
        EventKind::UserMessage => 0,
        EventKind::AssistantMessage => 1,
        EventKind::Command => 2,
    }
}

fn session_repos(parsed: &ParsedSession) -> BTreeSet<String> {
    let mut repos = BTreeSet::new();
    let session_repo = repo_slug(&parsed.session.cwd);
    if !session_repo.is_empty() {
        repos.insert(session_repo);
    }

    for event in &parsed.events {
        let Some(cwd) = &event.cwd else {
            continue;
        };
        let repo = repo_slug(cwd);
        if !repo.is_empty() {
            repos.insert(repo);
        }
    }

    repos
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

pub fn build_session_key(session_id: &str, source_file_path: &Path) -> String {
    session_key(session_id, source_file_path)
}

fn session_key(session_id: &str, source_file_path: &Path) -> String {
    format!(
        "{}:{:016x}",
        session_id,
        fnv1a64(source_file_path.display().to_string().as_bytes())
    )
}

fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
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
        assert!(results[0].session_key.starts_with("session-1:"));
        assert_eq!(results[0].session_id, "session-1");
        assert_eq!(results[0].kind, EventKind::AssistantMessage);
        assert_eq!(results[0].source_line_number, 3);
        assert_eq!(results[0].cwd, "/Users/me/project");
        assert!(results[0].snippet.contains("webhook"));
        assert!(results[0].score < 0.0);
    }

    #[test]
    fn keeps_duplicate_session_ids_by_source_file() {
        let store = Store::open(temp_db_path("duplicate-session-ids")).unwrap();
        store
            .index_session(&sample_session_with(
                "session-1",
                "/Users/me/project",
                "2026-04-13T01:00:00Z",
                "/tmp/active-session.jsonl",
            ))
            .unwrap();
        store
            .index_session(&sample_session_with(
                "session-1",
                "/Users/me/project",
                "2026-04-13T01:00:00Z",
                "/tmp/archived-session.jsonl",
            ))
            .unwrap();

        let stats = store.stats().unwrap();
        assert_eq!(stats.session_count, 2);
        assert_eq!(stats.event_count, 4);

        let results = store.search("webhook secret", 10).unwrap();
        let mut keys = results
            .iter()
            .map(|result| result.session_key.as_str())
            .collect::<Vec<_>>();
        keys.sort_unstable();
        keys.dedup();

        assert_eq!(keys.len(), 2);
        assert!(results
            .iter()
            .all(|result| result.session_id == "session-1"));
    }

    #[test]
    fn ranks_current_repo_sessions_before_other_repos() {
        let store = Store::open(temp_db_path("current-repo-rank")).unwrap();
        store
            .index_session(&sample_session_with(
                "other",
                "/Users/me/projects/other",
                "2026-04-13T01:00:00Z",
                "/tmp/other.jsonl",
            ))
            .unwrap();
        store
            .index_session(&sample_session_with(
                "project",
                "/Users/me/projects/project",
                "2026-04-01T01:00:00Z",
                "/tmp/project.jsonl",
            ))
            .unwrap();

        let results = store
            .search_with_options(SearchOptions {
                query: "webhook secret".to_owned(),
                limit: 10,
                repo: None,
                cwd: None,
                since: None,
                current_repo: Some("project".to_owned()),
            })
            .unwrap();

        assert_eq!(results[0].session_id, "project");
        assert_eq!(results[0].repo, "project");
    }

    #[test]
    fn ranks_current_repo_when_only_a_command_ran_inside_that_repo() {
        let store = Store::open(temp_db_path("current-repo-command-cwd")).unwrap();
        store
            .index_session(&sample_session_with(
                "other",
                "/Users/me/projects/other",
                "2026-04-13T01:00:00Z",
                "/tmp/other-command.jsonl",
            ))
            .unwrap();

        let mut project_session = sample_session_with(
            "project",
            "/Users/me/hanif-md",
            "2026-04-01T01:00:00Z",
            "/tmp/project-command.jsonl",
        );
        project_session.events.push(ParsedEvent {
            session_id: "project".to_owned(),
            kind: EventKind::Command,
            role: None,
            text: "$ rg webhook".to_owned(),
            command: Some("rg webhook".to_owned()),
            cwd: Some("/Users/me/projects/project".to_owned()),
            exit_code: Some(0),
            source_timestamp: Some("2026-04-01T01:00:03Z".to_owned()),
            source_file_path: PathBuf::from("/tmp/project-command.jsonl"),
            source_line_number: 4,
        });
        store.index_session(&project_session).unwrap();

        let results = store
            .search_with_options(SearchOptions {
                query: "webhook secret".to_owned(),
                limit: 10,
                repo: None,
                cwd: None,
                since: None,
                current_repo: Some("project".to_owned()),
            })
            .unwrap();

        assert_eq!(results[0].session_id, "project");
        assert_eq!(results[0].repo, "hanif-md");
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
                current_repo: None,
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
