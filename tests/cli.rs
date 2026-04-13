use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "codex-recall-cli-test-{}-{}",
        std::process::id(),
        name
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_sample_session(root: &std::path::Path) {
    let session_dir = root.join("2026/04/13");
    fs::create_dir_all(&session_dir).unwrap();
    fs::write(
        session_dir.join("rollout-2026-04-13T01-00-00-session-1.jsonl"),
        r#"{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{"id":"session-1","timestamp":"2026-04-13T01:00:00Z","cwd":"/Users/me/project","cli_version":"0.1.0"}}
{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"Debug RevenueCat Stripe webhook"}}
{"timestamp":"2026-04-13T01:00:02Z","type":"event_msg","payload":{"type":"agent_message","message":"The production webhook secret was missing."}}
"#,
    )
    .unwrap();
}

fn write_session(root: &std::path::Path, id: &str, cwd: &str, timestamp: &str) {
    let session_dir = root.join("2026/04/13");
    fs::create_dir_all(&session_dir).unwrap();
    fs::write(
        session_dir.join(format!("{id}.jsonl")),
        format!(
            r#"{{"timestamp":"{timestamp}","type":"session_meta","payload":{{"id":"{id}","timestamp":"{timestamp}","cwd":"{cwd}","cli_version":"0.1.0"}}}}
{{"timestamp":"{timestamp}","type":"event_msg","payload":{{"type":"agent_message","message":"The production webhook secret was missing."}}}}
"#
        ),
    )
    .unwrap();
}

fn write_many_sessions(root: &std::path::Path, count: usize) {
    let session_dir = root.join("2026/04/13");
    fs::create_dir_all(&session_dir).unwrap();
    for index in 0..count {
        let id = format!("session-{index}");
        fs::write(
            session_dir.join(format!("{id}.jsonl")),
            format!(
                r#"{{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{{"id":"{id}","timestamp":"2026-04-13T01:00:00Z","cwd":"/tmp/project"}}}}
{{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{{"type":"user_message","message":"Need progress output {index}"}}}}
"#
            ),
        )
        .unwrap();
    }
}

#[test]
fn index_then_search_outputs_ranked_receipts() {
    let temp = temp_dir("index-search");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_sample_session(&source);

    let index = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["index", "--db"])
        .arg(&db)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        index.status.success(),
        "index failed: {}",
        String::from_utf8_lossy(&index.stderr)
    );
    assert!(String::from_utf8_lossy(&index.stdout).contains("indexed 1 session files"));

    let search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["search", "webhook secret", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        search.status.success(),
        "search failed: {}",
        String::from_utf8_lossy(&search.stderr)
    );

    let stdout = String::from_utf8_lossy(&search.stdout);
    assert!(stdout.contains("session-1"));
    assert!(stdout.contains("/Users/me/project"));
    assert!(stdout.contains(":3"));
    assert!(stdout.contains("production webhook secret"));
}

#[test]
fn search_json_outputs_machine_readable_receipts() {
    let temp = temp_dir("search-json");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_sample_session(&source);

    let index = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["index", "--db"])
        .arg(&db)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(index.status.success());

    let search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["search", "webhook secret", "--json", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        search.status.success(),
        "search failed: {}",
        String::from_utf8_lossy(&search.stderr)
    );

    let json: serde_json::Value = serde_json::from_slice(&search.stdout).unwrap();
    assert_eq!(json["query"], "webhook secret");
    assert_eq!(json["results"][0]["session_id"], "session-1");
    assert_eq!(json["results"][0]["source_line_number"], 3);
    assert_eq!(json["results"][0]["kind"], "assistant_message");
    assert!(json["results"][0]["source"]
        .as_str()
        .unwrap()
        .contains(":3"));
    assert!(json["results"][0].get("text").is_none());
    assert!(json["results"][0]["text_preview"]
        .as_str()
        .unwrap()
        .contains("production webhook secret"));
}

#[test]
fn show_prints_session_events_with_line_receipts() {
    let temp = temp_dir("show");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_sample_session(&source);

    let index = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["index", "--db"])
        .arg(&db)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(index.status.success());

    let show = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["show", "session-1", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        show.status.success(),
        "show failed: {}",
        String::from_utf8_lossy(&show.stderr)
    );

    let stdout = String::from_utf8_lossy(&show.stdout);
    assert!(stdout.contains("session-1"));
    assert!(stdout.contains("user_message"));
    assert!(stdout.contains(":2"));
    assert!(stdout.contains("Debug RevenueCat Stripe webhook"));
    assert!(stdout.contains("assistant_message"));
    assert!(stdout.contains(":3"));
}

#[test]
fn search_filters_by_repo_cwd_and_since_flags() {
    let temp = temp_dir("search-filters");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_session(
        &source,
        "old-palabruno",
        "/Users/me/projects/palabruno",
        "2026-04-01T01:00:00Z",
    );
    write_session(
        &source,
        "new-palabruno",
        "/Users/me/projects/palabruno",
        "2026-04-13T01:00:00Z",
    );
    write_session(
        &source,
        "genrupt",
        "/Users/me/projects/Genrupt",
        "2026-04-13T01:00:00Z",
    );

    let index = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["index", "--db"])
        .arg(&db)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(index.status.success());

    let search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "search",
            "webhook secret",
            "--repo",
            "palabruno",
            "--cwd",
            "projects/palabruno",
            "--since",
            "2026-04-10",
            "--db",
        ])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        search.status.success(),
        "search failed: {}",
        String::from_utf8_lossy(&search.stderr)
    );

    let stdout = String::from_utf8_lossy(&search.stdout);
    assert!(stdout.contains("new-palabruno"));
    assert!(!stdout.contains("old-palabruno"));
    assert!(!stdout.contains("genrupt"));
}

#[test]
fn index_reports_progress_for_larger_sources() {
    let temp = temp_dir("progress");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_many_sessions(&source, 101);

    let index = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["index", "--db"])
        .arg(&db)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(index.status.success());

    let stderr = String::from_utf8_lossy(&index.stderr);
    assert!(stderr.contains("scanned 100 files"));
}
