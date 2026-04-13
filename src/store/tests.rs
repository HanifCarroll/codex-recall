use super::*;
use crate::parser::{EventKind, ParsedEvent, ParsedSession, SessionMetadata};
use rusqlite::{params, Connection};
use std::path::{Path, PathBuf};

fn temp_db_path(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "codex-recall-store-test-{}-{}",
        std::process::id(),
        name
    ));
    let _ = std::fs::remove_dir_all(&dir);
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

fn sample_session_with(id: &str, cwd: &str, timestamp: &str, source_path: &str) -> ParsedSession {
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
                session_id: id.to_owned(),
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
                session_id: id.to_owned(),
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
fn since_today_and_yesterday_use_local_day_boundaries() {
    let mut sql = String::new();
    let mut params = Vec::new();
    append_since_clause(&mut sql, &mut params, "today").unwrap();
    assert!(sql.contains("datetime('now', 'localtime', 'start of day', 'utc')"));
    assert!(params.is_empty());

    let mut sql = String::new();
    append_since_clause(&mut sql, &mut params, "yesterday").unwrap();
    assert!(sql.contains("datetime('now', 'localtime', 'start of day', '-1 day', 'utc')"));
}

#[test]
fn unchanged_sources_with_old_content_version_are_reindexed() {
    let store = Store::open(temp_db_path("content-version")).unwrap();
    let source = PathBuf::from("/tmp/content-version.jsonl");
    store
        .mark_source_indexed(&source, 10, 100, Some("session-1"), Some("session-1:key"))
        .unwrap();
    assert!(store.is_source_current(&source, 10, 100).unwrap());

    store
        .conn
        .execute(
            "UPDATE ingestion_state SET content_version = 0 WHERE source_file_path = ?",
            params![source.display().to_string()],
        )
        .unwrap();

    assert!(!store.is_source_current(&source, 10, 100).unwrap());
}

#[test]
fn migrates_legacy_session_id_schema_without_losing_searchability() {
    let db = temp_db_path("legacy-migration");
    let source_file = "/tmp/legacy-session.jsonl";
    let conn = Connection::open(&db).unwrap();
    conn.execute_batch(
        r#"
        CREATE TABLE sessions (
            session_id TEXT PRIMARY KEY,
            session_timestamp TEXT NOT NULL,
            cwd TEXT NOT NULL,
            repo TEXT NOT NULL DEFAULT '',
            cli_version TEXT,
            source_file_path TEXT NOT NULL
        );

        CREATE TABLE events (
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
            source_line_number INTEGER NOT NULL
        );

        CREATE TABLE ingestion_state (
            source_file_path TEXT PRIMARY KEY,
            source_file_mtime_ns INTEGER NOT NULL,
            source_file_size INTEGER NOT NULL,
            session_id TEXT,
            indexed_at TEXT NOT NULL
        );
        "#,
    )
    .unwrap();
    conn.execute(
        "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?)",
        params![
            "legacy-session",
            "2026-04-13T01:00:00Z",
            "/Users/me/projects/codex-recall",
            "",
            "0.1.0",
            source_file,
        ],
    )
    .unwrap();
    conn.execute(
        r#"
        INSERT INTO events (
            session_id, kind, role, text, command, cwd, exit_code,
            source_timestamp, source_file_path, source_line_number
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        params![
            "legacy-session",
            "assistant_message",
            "assistant",
            "Legacy migration preserved webhook recall.",
            Option::<String>::None,
            Option::<String>::None,
            Option::<i64>::None,
            "2026-04-13T01:00:01Z",
            source_file,
            2_i64,
        ],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO ingestion_state VALUES (?, ?, ?, ?, ?)",
        params![
            source_file,
            1_i64,
            100_i64,
            "legacy-session",
            "2026-04-13T01:00:02Z"
        ],
    )
    .unwrap();
    drop(conn);

    let store = Store::open(&db).unwrap();
    let stats = store.stats().unwrap();
    let results = store.search("legacy webhook", 5).unwrap();
    let matches = store.resolve_session_reference("legacy-session").unwrap();

    assert_eq!(stats.session_count, 1);
    assert_eq!(stats.event_count, 1);
    assert_eq!(stats.source_file_count, 1);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].session_id, "legacy-session");
    assert_eq!(
        results[0].session_key,
        build_session_key("legacy-session", Path::new(source_file))
    );
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].repo, "codex-recall");
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
