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
    write_session_file(
        root,
        &format!("{id}.jsonl"),
        id,
        cwd,
        timestamp,
        "The production webhook secret was missing.",
    );
}

fn write_session_file(
    root: &std::path::Path,
    file_name: &str,
    id: &str,
    cwd: &str,
    timestamp: &str,
    message: &str,
) {
    let session_dir = root.join("2026/04/13");
    fs::create_dir_all(&session_dir).unwrap();
    fs::write(
        session_dir.join(file_name),
        format!(
            r#"{{"timestamp":"{timestamp}","type":"session_meta","payload":{{"id":"{id}","timestamp":"{timestamp}","cwd":"{cwd}","cli_version":"0.1.0"}}}}
{{"timestamp":"{timestamp}","type":"event_msg","payload":{{"type":"agent_message","message":"{message}"}}}}
"#
        ),
    )
    .unwrap();
}

fn write_mixed_session_file(
    root: &std::path::Path,
    file_name: &str,
    id: &str,
    cwd: &str,
    timestamp: &str,
) {
    let session_dir = root.join("2026/04/13");
    fs::create_dir_all(&session_dir).unwrap();
    fs::write(
        session_dir.join(file_name),
        format!(
            r#"{{"timestamp":"{timestamp}","type":"session_meta","payload":{{"id":"{id}","timestamp":"{timestamp}","cwd":"{cwd}","cli_version":"0.1.0"}}}}
{{"timestamp":"{timestamp}","type":"event_msg","payload":{{"type":"user_message","message":"Find the webhook secret bug"}}}}
{{"timestamp":"{timestamp}","type":"event_msg","payload":{{"type":"agent_message","message":"The webhook secret was missing in production."}}}}
{{"timestamp":"{timestamp}","type":"event_msg","payload":{{"type":"exec_command_end","command":["/bin/zsh","-lc","rg webhook"],"cwd":"{cwd}","exit_code":0,"aggregated_output":"webhook command receipt"}}}}
"#
        ),
    )
    .unwrap();
}

fn index_sources(db: &std::path::Path, sources: &[&std::path::Path]) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_codex-recall"));
    command.args(["index", "--db"]).arg(db);
    for source in sources {
        command.args(["--source"]).arg(source);
    }
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "index failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
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
fn search_does_not_create_missing_database() {
    let temp = temp_dir("search-missing-db");
    let db = temp.join("missing").join("index.sqlite");

    let search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["search", "webhook", "--db"])
        .arg(&db)
        .output()
        .unwrap();

    assert!(!search.status.success());
    assert!(!db.exists(), "search should not create the database");
    assert!(
        !db.parent().unwrap().exists(),
        "search should not create the database directory"
    );
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
fn cli_supports_top_level_version_flag() {
    let version = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(
        version.status.success(),
        "version failed: {}",
        String::from_utf8_lossy(&version.stderr)
    );
    assert!(
        String::from_utf8_lossy(&version.stdout).contains(env!("CARGO_PKG_VERSION")),
        "{}",
        String::from_utf8_lossy(&version.stdout)
    );
}

#[test]
fn subcommand_help_lists_typed_flags() {
    let help = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["watch", "--help"])
        .output()
        .unwrap();
    assert!(
        help.status.success(),
        "help failed: {}",
        String::from_utf8_lossy(&help.stderr)
    );
    let stdout = String::from_utf8_lossy(&help.stdout);
    assert!(stdout.contains("--quiet-for"), "{stdout}");
    assert!(stdout.contains("--install-launch-agent"), "{stdout}");
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
    assert!(json["results"][0]["session_key"]
        .as_str()
        .unwrap()
        .starts_with("session-1:"));
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
fn show_disambiguates_duplicate_session_ids_and_accepts_session_key() {
    let temp = temp_dir("show-duplicate");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_session_file(
        &source,
        "active.jsonl",
        "session-dup",
        "/Users/me/project",
        "2026-04-13T01:00:00Z",
        "The active webhook secret was missing.",
    );
    write_session_file(
        &source,
        "archived.jsonl",
        "session-dup",
        "/Users/me/project",
        "2026-04-13T01:00:00Z",
        "The archived webhook secret was missing.",
    );

    let index = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["index", "--db"])
        .arg(&db)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(index.status.success());

    let ambiguous_show = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["show", "session-dup", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(!ambiguous_show.status.success());
    assert!(
        String::from_utf8_lossy(&ambiguous_show.stderr).contains("multiple indexed sessions match")
    );

    let search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["search", "archived webhook", "--json", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(search.status.success());
    let json: serde_json::Value = serde_json::from_slice(&search.stdout).unwrap();
    let key = json["results"][0]["session_key"].as_str().unwrap();

    let show = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["show", key, "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        show.status.success(),
        "show failed: {}",
        String::from_utf8_lossy(&show.stderr)
    );
    let stdout = String::from_utf8_lossy(&show.stdout);
    assert!(stdout.contains(key));
    assert!(stdout.contains("archived webhook secret"));
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
fn search_filters_by_from_until_and_excluded_session() {
    let temp = temp_dir("search-range-exclude");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_session(
        &source,
        "too-old",
        "/Users/me/projects/project",
        "2026-04-01T01:00:00Z",
    );
    write_session(
        &source,
        "in-range",
        "/Users/me/projects/project",
        "2026-04-13T01:00:00Z",
    );
    write_session(
        &source,
        "excluded",
        "/Users/me/projects/project",
        "2026-04-13T02:00:00Z",
    );
    write_session(
        &source,
        "too-new",
        "/Users/me/projects/project",
        "2026-04-15T01:00:00Z",
    );
    index_sources(&db, &[&source]);

    let search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "search",
            "webhook secret",
            "--from",
            "2026-04-10",
            "--until",
            "2026-04-14",
            "--exclude-session",
            "excluded",
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
    assert!(stdout.contains("in-range"), "{stdout}");
    assert!(!stdout.contains("too-old"), "{stdout}");
    assert!(!stdout.contains("excluded"), "{stdout}");
    assert!(!stdout.contains("too-new"), "{stdout}");
}

#[test]
fn search_rejects_since_and_from_together() {
    let temp = temp_dir("search-since-from");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_sample_session(&source);
    index_sources(&db, &[&source]);

    let search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "search",
            "webhook secret",
            "--since",
            "2026-04-01",
            "--from",
            "2026-04-01",
            "--db",
        ])
        .arg(&db)
        .output()
        .unwrap();

    assert!(!search.status.success());
    assert!(String::from_utf8_lossy(&search.stderr).contains("use either --since or --from"));
}

#[test]
fn search_dedupes_active_and_archived_session_copies_by_default() {
    let temp = temp_dir("search-dedupe");
    let active = temp.join("sessions");
    let archived = temp.join("archived_sessions");
    let db = temp.join("index.sqlite");
    write_session_file(
        &active,
        "active.jsonl",
        "session-dup",
        "/Users/me/project",
        "2026-04-13T01:00:00Z",
        "The active webhook secret was missing.",
    );
    write_session_file(
        &archived,
        "archived.jsonl",
        "session-dup",
        "/Users/me/project",
        "2026-04-13T01:00:00Z",
        "The archived webhook secret was missing.",
    );
    index_sources(&db, &[&active, &archived]);

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
    assert_eq!(json["count"], 1);
    assert!(json["results"][0]["source_file_path"]
        .as_str()
        .unwrap()
        .contains("/sessions/"));

    let duplicate_search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "search",
            "webhook secret",
            "--include-duplicates",
            "--json",
            "--db",
        ])
        .arg(&db)
        .output()
        .unwrap();
    assert!(duplicate_search.status.success());
    let json: serde_json::Value = serde_json::from_slice(&duplicate_search.stdout).unwrap();
    assert_eq!(json["count"], 2);
}

#[test]
fn search_supports_day_kind_json_and_exclude_current() {
    let temp = temp_dir("search-day-kind-current");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_mixed_session_file(
        &source,
        "current.jsonl",
        "current-session",
        "/Users/me/project",
        "2026-04-13T01:00:00Z",
    );
    write_mixed_session_file(
        &source,
        "kept.jsonl",
        "kept-session",
        "/Users/me/project",
        "2026-04-13T02:00:00Z",
    );
    write_mixed_session_file(
        &source,
        "other-day.jsonl",
        "other-day",
        "/Users/me/project",
        "2026-04-14T01:00:00Z",
    );
    index_sources(&db, &[&source]);

    let search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .env("CODEX_THREAD_ID", "current-session")
        .args([
            "search",
            "webhook",
            "--day",
            "2026-04-13",
            "--kind",
            "assistant",
            "--exclude-current",
            "--json",
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

    let json: serde_json::Value = serde_json::from_slice(&search.stdout).unwrap();
    assert_eq!(json["count"], 1);
    assert_eq!(json["results"][0]["session_id"], "kept-session");
    assert_eq!(json["results"][0]["kind"], "assistant_message");
}

#[test]
fn search_rejects_day_with_other_date_filters() {
    let temp = temp_dir("search-day-conflict");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_sample_session(&source);
    index_sources(&db, &[&source]);

    let search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "search",
            "webhook",
            "--day",
            "2026-04-13",
            "--since",
            "7d",
            "--db",
        ])
        .arg(&db)
        .output()
        .unwrap();

    assert!(!search.status.success());
    assert!(String::from_utf8_lossy(&search.stderr).contains("use --day by itself"));
}

#[test]
fn search_accepts_relative_since_days() {
    let temp = temp_dir("relative-since");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_session(
        &source,
        "ancient",
        "/Users/me/projects/palabruno",
        "1970-01-01T01:00:00Z",
    );
    write_session(
        &source,
        "future",
        "/Users/me/projects/palabruno",
        "2999-01-01T01:00:00Z",
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
        .args(["search", "webhook secret", "--since", "7d", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        search.status.success(),
        "search failed: {}",
        String::from_utf8_lossy(&search.stderr)
    );

    let stdout = String::from_utf8_lossy(&search.stdout);
    assert!(stdout.contains("future"));
    assert!(!stdout.contains("ancient"));
}

#[test]
fn search_prioritizes_current_repo_by_default() {
    let temp = temp_dir("current-repo");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    let current_repo = temp.join("project");
    fs::create_dir_all(current_repo.join(".git")).unwrap();

    write_session(
        &source,
        "other",
        "/Users/me/projects/other",
        "2026-04-13T01:00:00Z",
    );
    write_session_file(
        &source,
        "project.jsonl",
        "project",
        current_repo.to_str().unwrap(),
        "2026-04-01T01:00:00Z",
        "The production webhook secret was missing.",
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
        .current_dir(&current_repo)
        .args(["search", "webhook secret", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        search.status.success(),
        "search failed: {}",
        String::from_utf8_lossy(&search.stderr)
    );

    let first_line = String::from_utf8_lossy(&search.stdout)
        .lines()
        .next()
        .unwrap()
        .to_owned();
    assert!(first_line.contains("project"), "{first_line}");
}

#[test]
fn doctor_json_reports_database_checks() {
    let temp = temp_dir("doctor-json");
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

    let doctor = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["doctor", "--json", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        doctor.status.success(),
        "doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );

    let json: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(json["ok"], true);
    assert_eq!(json["db_exists"], true);
    assert_eq!(json["checks"]["quick_check"], "ok");
    assert_eq!(json["checks"]["fts_integrity"], "ok");
}

#[test]
fn doctor_does_not_create_missing_database() {
    let temp = temp_dir("doctor-missing");
    let db = temp.join("missing").join("index.sqlite");

    let doctor = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["doctor", "--json", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        doctor.status.success(),
        "doctor failed: {}",
        String::from_utf8_lossy(&doctor.stderr)
    );

    let json: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(json["ok"], false);
    assert_eq!(json["db_exists"], false);
    assert_eq!(json["checks"]["quick_check"], "missing");
    assert!(!db.exists(), "doctor should not create the database");
    assert!(
        !db.parent().unwrap().exists(),
        "doctor should not create the database directory"
    );
}

#[test]
fn rebuild_recreates_index_from_current_sources() {
    let temp = temp_dir("rebuild");
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

    fs::remove_dir_all(&source).unwrap();
    fs::create_dir_all(&source).unwrap();

    let rebuild = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["rebuild", "--db"])
        .arg(&db)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        rebuild.status.success(),
        "rebuild failed: {}",
        String::from_utf8_lossy(&rebuild.stderr)
    );

    let stats = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["stats", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(stats.status.success());
    assert!(String::from_utf8_lossy(&stats.stdout).starts_with("0 sessions, 0 events"));
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
    assert!(stderr.contains("progress: 100/101 files"), "{stderr}");
    assert!(stderr.contains("bytes"), "{stderr}");
    assert!(stderr.contains("elapsed"), "{stderr}");
    assert!(stderr.contains("eta"), "{stderr}");
    assert!(stderr.contains("current "), "{stderr}");
}

#[test]
fn status_json_reports_pending_files_without_creating_missing_database() {
    let temp = temp_dir("status-pending");
    let source = temp.join("sessions");
    let db = temp.join("missing").join("index.sqlite");
    let state = temp.join("state.json");
    write_sample_session(&source);

    let status = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["status", "--json", "--db"])
        .arg(&db)
        .args(["--state"])
        .arg(&state)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        status.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&status.stderr)
    );

    let json: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(json["db_exists"], false);
    assert_eq!(json["freshness"], "watcher-not-running");
    assert_eq!(json["pending_files"], 1);
    assert_eq!(json["last_error"], serde_json::Value::Null);
    assert!(!db.exists(), "status should not create the database");
}

#[test]
fn status_json_reports_fresh_and_live_write_verdicts() {
    let temp = temp_dir("status-freshness");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    let state = temp.join("watch-state.json");
    write_sample_session(&source);

    let watch = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "watch",
            "--once",
            "--quiet-for",
            "0",
            "--interval",
            "0",
            "--db",
        ])
        .arg(&db)
        .args(["--state"])
        .arg(&state)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(watch.status.success());

    let fresh = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["status", "--json", "--db"])
        .arg(&db)
        .args(["--state"])
        .arg(&state)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(fresh.status.success());
    let json: serde_json::Value = serde_json::from_slice(&fresh.stdout).unwrap();
    assert_eq!(json["freshness"], "fresh");
    assert_eq!(json["freshness_message"], "index is current");

    write_session_file(
        &source,
        "live.jsonl",
        "live-write",
        "/Users/me/project",
        "2026-04-13T02:00:00Z",
        "A live write is still settling.",
    );
    let live = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["status", "--json", "--quiet-for", "86400", "--db"])
        .arg(&db)
        .args(["--state"])
        .arg(&state)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(live.status.success());
    let json: serde_json::Value = serde_json::from_slice(&live.stdout).unwrap();
    assert_eq!(json["freshness"], "pending-live-writes");
    assert_eq!(json["waiting_files"], 1);
}

#[test]
fn watch_once_indexes_stable_sessions_and_writes_status() {
    let temp = temp_dir("watch-once");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    let state = temp.join("watch-state.json");
    write_sample_session(&source);

    let watch = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "watch",
            "--once",
            "--quiet-for",
            "0",
            "--interval",
            "0",
            "--db",
        ])
        .arg(&db)
        .args(["--state"])
        .arg(&state)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(
        watch.status.success(),
        "watch failed: {}",
        String::from_utf8_lossy(&watch.stderr)
    );

    let stdout = String::from_utf8_lossy(&watch.stdout);
    assert!(stdout.contains("watch indexed"), "{stdout}");

    let stats = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["stats", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(stats.status.success());
    assert!(String::from_utf8_lossy(&stats.stdout).starts_with("1 sessions, 2 events"));

    let json: serde_json::Value = serde_json::from_slice(&fs::read(&state).unwrap()).unwrap();
    assert_eq!(json["pending_files"], 0);
    assert_eq!(json["last_error"], serde_json::Value::Null);
    assert_eq!(json["last_indexed_sessions"], 1);
    assert!(json["last_run_at"].as_str().is_some());
}

#[test]
fn watch_installs_launch_agent_plist_when_requested() {
    let temp = temp_dir("watch-launch-agent");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    let state = temp.join("watch-state.json");
    let agent = temp.join("com.example.codex-recall.watch.plist");
    fs::create_dir_all(&source).unwrap();

    let install = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "watch",
            "--install-launch-agent",
            "--agent-label",
            "com.example.codex-recall.watch",
            "--agent-path",
        ])
        .arg(&agent)
        .args(["--db"])
        .arg(&db)
        .args(["--state"])
        .arg(&state)
        .args(["--source"])
        .arg(&source)
        .args(["--interval", "60", "--quiet-for", "5"])
        .output()
        .unwrap();
    assert!(
        install.status.success(),
        "install failed: {}",
        String::from_utf8_lossy(&install.stderr)
    );

    let plist = fs::read_to_string(&agent).unwrap();
    assert!(plist.contains("<key>Label</key>"), "{plist}");
    assert!(plist.contains("com.example.codex-recall.watch"), "{plist}");
    assert!(plist.contains("<string>watch</string>"), "{plist}");
    assert!(plist.contains("<string>--interval</string>"), "{plist}");
    assert!(plist.contains("<string>60</string>"), "{plist}");
    assert!(plist.contains(source.to_str().unwrap()), "{plist}");
}

#[test]
fn watch_can_install_and_start_launch_agent_with_configurable_launchctl() {
    let temp = temp_dir("watch-launch-agent-start");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    let state = temp.join("watch-state.json");
    let agent = temp.join("com.example.codex-recall.watch.plist");
    let launchctl_log = temp.join("launchctl.log");
    let fake_launchctl = temp.join("launchctl");
    fs::create_dir_all(&source).unwrap();
    fs::write(
        &fake_launchctl,
        format!(
            "#!/bin/sh\nprintf '%s ' \"$@\" >> {}\nprintf '\\n' >> {}\nexit 0\n",
            launchctl_log.display(),
            launchctl_log.display()
        ),
    )
    .unwrap();
    let chmod = Command::new("chmod")
        .args(["+x"])
        .arg(&fake_launchctl)
        .output()
        .unwrap();
    assert!(chmod.status.success());

    let install = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .env("CODEX_RECALL_LAUNCHCTL", &fake_launchctl)
        .env("CODEX_RECALL_UID", "501")
        .args([
            "watch",
            "--install-launch-agent",
            "--start-launch-agent",
            "--agent-label",
            "com.example.codex-recall.watch",
            "--agent-path",
        ])
        .arg(&agent)
        .args(["--db"])
        .arg(&db)
        .args(["--state"])
        .arg(&state)
        .args(["--source"])
        .arg(&source)
        .args(["--interval", "60", "--quiet-for", "5"])
        .output()
        .unwrap();
    assert!(
        install.status.success(),
        "install failed: {}",
        String::from_utf8_lossy(&install.stderr)
    );

    let stdout = String::from_utf8_lossy(&install.stdout);
    assert!(stdout.contains("installed launch agent"), "{stdout}");
    assert!(stdout.contains("started launch agent"), "{stdout}");
    let log = fs::read_to_string(launchctl_log).unwrap();
    assert!(log.contains("bootstrap gui/501"), "{log}");
    assert!(log.contains(agent.to_str().unwrap()), "{log}");
    assert!(
        log.contains("print gui/501/com.example.codex-recall.watch"),
        "{log}"
    );
}

#[test]
fn doctor_json_includes_freshness_verdict() {
    let temp = temp_dir("doctor-freshness");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    let state = temp.join("watch-state.json");
    write_sample_session(&source);

    let doctor = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["doctor", "--json", "--db"])
        .arg(&db)
        .args(["--state"])
        .arg(&state)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(doctor.status.success());
    let json: serde_json::Value = serde_json::from_slice(&doctor.stdout).unwrap();
    assert_eq!(json["ok"], false);
    assert_eq!(json["freshness"]["state"], "watcher-not-running");
    assert!(json["freshness"]["message"]
        .as_str()
        .unwrap()
        .contains("pending stable files"));
}

#[test]
fn bundle_outputs_agent_ready_context() {
    let temp = temp_dir("bundle");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_session_file(
        &source,
        "revenuecat.jsonl",
        "revenuecat",
        "/Users/me/projects/payments",
        "2026-04-13T01:00:00Z",
        "The RevenueCat webhook secret was missing from production.",
    );
    write_session_file(
        &source,
        "stripe.jsonl",
        "stripe",
        "/Users/me/projects/payments",
        "2026-04-13T02:00:00Z",
        "Stripe retries were caused by the missing webhook secret.",
    );

    let index = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["index", "--db"])
        .arg(&db)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(index.status.success());

    let bundle = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["bundle", "webhook secret", "--db"])
        .arg(&db)
        .args(["--limit", "2", "--repo", "payments"])
        .output()
        .unwrap();
    assert!(
        bundle.status.success(),
        "bundle failed: {}",
        String::from_utf8_lossy(&bundle.stderr)
    );

    let stdout = String::from_utf8_lossy(&bundle.stdout);
    assert!(stdout.contains("# codex-recall bundle"), "{stdout}");
    assert!(stdout.contains("Query: webhook secret"), "{stdout}");
    assert!(stdout.contains("## Top Sessions"), "{stdout}");
    assert!(stdout.contains("## Receipts"), "{stdout}");
    assert!(stdout.contains("revenuecat"), "{stdout}");
    assert!(stdout.contains("stripe"), "{stdout}");
    assert!(stdout.contains("codex-recall show"), "{stdout}");
}

#[test]
fn recent_lists_latest_sessions_with_filters() {
    let temp = temp_dir("recent");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_session(
        &source,
        "old-project",
        "/Users/me/projects/project",
        "2026-04-01T01:00:00Z",
    );
    write_session(
        &source,
        "new-project",
        "/Users/me/projects/project",
        "2026-04-13T02:00:00Z",
    );
    write_session(
        &source,
        "new-other",
        "/Users/me/projects/other",
        "2026-04-13T03:00:00Z",
    );

    let index = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["index", "--db"])
        .arg(&db)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(index.status.success());

    let recent = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "recent",
            "--repo",
            "project",
            "--since",
            "2026-04-10",
            "--db",
        ])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        recent.status.success(),
        "recent failed: {}",
        String::from_utf8_lossy(&recent.stderr)
    );

    let stdout = String::from_utf8_lossy(&recent.stdout);
    assert!(stdout.contains("new-project"), "{stdout}");
    assert!(stdout.contains("codex-recall show"), "{stdout}");
    assert!(!stdout.contains("old-project"), "{stdout}");
    assert!(!stdout.contains("new-other"), "{stdout}");
}

#[test]
fn recent_filters_by_from_until_exclusion_and_dedupes_duplicates() {
    let temp = temp_dir("recent-range-dedupe");
    let active = temp.join("sessions");
    let archived = temp.join("archived_sessions");
    let db = temp.join("index.sqlite");
    write_session(
        &active,
        "too-old",
        "/Users/me/projects/project",
        "2026-04-01T01:00:00Z",
    );
    write_session_file(
        &active,
        "active.jsonl",
        "session-dup",
        "/Users/me/projects/project",
        "2026-04-13T01:00:00Z",
        "Active duplicate.",
    );
    write_session_file(
        &archived,
        "archived.jsonl",
        "session-dup",
        "/Users/me/projects/project",
        "2026-04-13T01:00:00Z",
        "Archived duplicate.",
    );
    write_session(
        &active,
        "excluded",
        "/Users/me/projects/project",
        "2026-04-13T02:00:00Z",
    );
    write_session(
        &active,
        "too-new",
        "/Users/me/projects/project",
        "2026-04-15T01:00:00Z",
    );
    index_sources(&db, &[&active, &archived]);

    let recent = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "recent",
            "--from",
            "2026-04-10",
            "--until",
            "2026-04-14",
            "--exclude-session",
            "excluded",
            "--db",
        ])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        recent.status.success(),
        "recent failed: {}",
        String::from_utf8_lossy(&recent.stderr)
    );

    let stdout = String::from_utf8_lossy(&recent.stdout);
    assert!(stdout.contains("session-dup"), "{stdout}");
    assert!(stdout.contains("/sessions/"), "{stdout}");
    assert!(!stdout.contains("/archived_sessions/"), "{stdout}");
    assert!(!stdout.contains("too-old"), "{stdout}");
    assert!(!stdout.contains("excluded"), "{stdout}");
    assert!(!stdout.contains("too-new"), "{stdout}");

    let duplicate_recent = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["recent", "--include-duplicates", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(duplicate_recent.status.success());
    let stdout = String::from_utf8_lossy(&duplicate_recent.stdout);
    assert!(stdout.contains("/sessions/"), "{stdout}");
    assert!(stdout.contains("/archived_sessions/"), "{stdout}");
}

#[test]
fn recent_json_supports_day_and_kind_filter() {
    let temp = temp_dir("recent-json-day-kind");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_mixed_session_file(
        &source,
        "with-command.jsonl",
        "with-command",
        "/Users/me/projects/project",
        "2026-04-13T01:00:00Z",
    );
    write_session(
        &source,
        "other-day",
        "/Users/me/projects/project",
        "2026-04-14T01:00:00Z",
    );
    index_sources(&db, &[&source]);

    let recent = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args([
            "recent",
            "--day",
            "2026-04-13",
            "--kind",
            "command",
            "--json",
            "--db",
        ])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        recent.status.success(),
        "recent failed: {}",
        String::from_utf8_lossy(&recent.stderr)
    );

    let json: serde_json::Value = serde_json::from_slice(&recent.stdout).unwrap();
    assert_eq!(json["count"], 1);
    assert_eq!(json["sessions"][0]["session_id"], "with-command");
}

#[test]
fn show_json_supports_kind_filter() {
    let temp = temp_dir("show-json-kind");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_mixed_session_file(
        &source,
        "mixed.jsonl",
        "mixed-session",
        "/Users/me/projects/project",
        "2026-04-13T01:00:00Z",
    );
    index_sources(&db, &[&source]);

    let show = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["show", "mixed-session", "--kind", "user", "--json", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        show.status.success(),
        "show failed: {}",
        String::from_utf8_lossy(&show.stderr)
    );

    let json: serde_json::Value = serde_json::from_slice(&show.stdout).unwrap();
    assert_eq!(json["session_id"], "mixed-session");
    assert_eq!(json["events"].as_array().unwrap().len(), 1);
    assert_eq!(json["events"][0]["kind"], "user_message");
}

#[test]
fn day_json_outputs_session_inventory() {
    let temp = temp_dir("day-json");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    write_session(
        &source,
        "project-one",
        "/Users/me/projects/project",
        "2026-04-13T01:00:00Z",
    );
    write_session(
        &source,
        "other-one",
        "/Users/me/projects/other",
        "2026-04-13T02:00:00Z",
    );
    write_session(
        &source,
        "next-day",
        "/Users/me/projects/project",
        "2026-04-14T01:00:00Z",
    );
    index_sources(&db, &[&source]);

    let day = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["day", "2026-04-13", "--json", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(
        day.status.success(),
        "day failed: {}",
        String::from_utf8_lossy(&day.stderr)
    );

    let json: serde_json::Value = serde_json::from_slice(&day.stdout).unwrap();
    assert_eq!(json["day"], "2026-04-13");
    assert_eq!(json["from"], "2026-04-13");
    assert_eq!(json["until"], "2026-04-14");
    assert_eq!(json["count"], 2);
    assert_eq!(json["repo_counts"]["project"], 1);
    assert_eq!(json["repo_counts"]["other"], 1);
}

#[test]
fn pin_stores_durable_session_anchor_and_pins_filters_it() {
    let temp = temp_dir("pins");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    let pins = temp.join("pins.json");
    write_session_file(
        &source,
        "watcher.jsonl",
        "watcher-session",
        "/Users/me/projects/codex-recall",
        "2026-04-13T02:00:00Z",
        "LaunchAgent watcher freshness decision.",
    );
    write_session_file(
        &source,
        "other.jsonl",
        "other-session",
        "/Users/me/projects/other",
        "2026-04-13T03:00:00Z",
        "Different project decision.",
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
        .args(["search", "watcher freshness", "--json", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(search.status.success());
    let json: serde_json::Value = serde_json::from_slice(&search.stdout).unwrap();
    let session_key = json["results"][0]["session_key"].as_str().unwrap();

    let pin = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["pin", session_key, "--label", "watcher design", "--db"])
        .arg(&db)
        .args(["--pins"])
        .arg(&pins)
        .output()
        .unwrap();
    assert!(
        pin.status.success(),
        "pin failed: {}",
        String::from_utf8_lossy(&pin.stderr)
    );
    assert!(String::from_utf8_lossy(&pin.stdout).contains("pinned watcher-session"));

    let pins_output = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["pins", "--repo", "codex-recall", "--pins"])
        .arg(&pins)
        .output()
        .unwrap();
    assert!(
        pins_output.status.success(),
        "pins failed: {}",
        String::from_utf8_lossy(&pins_output.stderr)
    );

    let stdout = String::from_utf8_lossy(&pins_output.stdout);
    assert!(stdout.contains("watcher design"), "{stdout}");
    assert!(stdout.contains("watcher-session"), "{stdout}");
    assert!(stdout.contains("codex-recall show"), "{stdout}");
    assert!(!stdout.contains("other-session"), "{stdout}");

    let pins_json = fs::read_to_string(&pins).unwrap();
    assert!(pins_json.contains("watcher design"), "{pins_json}");
    assert!(pins_json.contains(session_key), "{pins_json}");
}

#[test]
fn pins_filter_by_command_cwd_repo_membership() {
    let temp = temp_dir("pins-command-repo");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    let pins = temp.join("pins.json");
    let session_dir = source.join("2026/04/13");
    fs::create_dir_all(&session_dir).unwrap();
    fs::write(
        session_dir.join("mixed-repo.jsonl"),
        r#"{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{"id":"mixed-repo","timestamp":"2026-04-13T01:00:00Z","cwd":"/Users/me/hanif-md","cli_version":"0.1.0"}}
{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{"type":"exec_command_end","command":["/bin/zsh","-lc","cargo test"],"cwd":"/Users/me/projects/codex-recall","exit_code":0,"aggregated_output":"watcher freshness tested"}}
"#,
    )
    .unwrap();

    let index = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["index", "--db"])
        .arg(&db)
        .args(["--source"])
        .arg(&source)
        .output()
        .unwrap();
    assert!(index.status.success());

    let search = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["search", "watcher freshness", "--json", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(search.status.success());
    let json: serde_json::Value = serde_json::from_slice(&search.stdout).unwrap();
    let session_key = json["results"][0]["session_key"].as_str().unwrap();

    let pin = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["pin", session_key, "--label", "mixed repo", "--db"])
        .arg(&db)
        .args(["--pins"])
        .arg(&pins)
        .output()
        .unwrap();
    assert!(pin.status.success());

    let pins_output = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["pins", "--repo", "codex-recall", "--pins"])
        .arg(&pins)
        .output()
        .unwrap();
    assert!(pins_output.status.success());
    let stdout = String::from_utf8_lossy(&pins_output.stdout);
    assert!(stdout.contains("mixed repo"), "{stdout}");
    assert!(stdout.contains("codex-recall"), "{stdout}");
}

#[test]
fn pins_json_outputs_machine_readable_pin_records() {
    let temp = temp_dir("pins-json");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    let pins = temp.join("pins.json");
    write_session_file(
        &source,
        "watcher.jsonl",
        "watcher-session",
        "/Users/me/projects/codex-recall",
        "2026-04-13T02:00:00Z",
        "LaunchAgent watcher freshness decision.",
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
        .args(["search", "watcher freshness", "--json", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(search.status.success());
    let json: serde_json::Value = serde_json::from_slice(&search.stdout).unwrap();
    let session_key = json["results"][0]["session_key"].as_str().unwrap();

    let pin = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["pin", session_key, "--label", "watcher design", "--db"])
        .arg(&db)
        .args(["--pins"])
        .arg(&pins)
        .output()
        .unwrap();
    assert!(pin.status.success());

    let pins_output = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["pins", "--repo", "codex-recall", "--json", "--pins"])
        .arg(&pins)
        .output()
        .unwrap();
    assert!(
        pins_output.status.success(),
        "pins failed: {}",
        String::from_utf8_lossy(&pins_output.stderr)
    );

    let json: serde_json::Value = serde_json::from_slice(&pins_output.stdout).unwrap();
    assert_eq!(json["count"], 1);
    assert_eq!(json["pins"][0]["label"], "watcher design");
    assert_eq!(json["pins"][0]["session_id"], "watcher-session");
    assert_eq!(
        json["pins"][0]["show_command"],
        format!("codex-recall show '{session_key}' --limit 120")
    );
}

#[test]
fn unpin_removes_existing_pin() {
    let temp = temp_dir("unpin");
    let source = temp.join("sessions");
    let db = temp.join("index.sqlite");
    let pins = temp.join("pins.json");
    write_session_file(
        &source,
        "watcher.jsonl",
        "watcher-session",
        "/Users/me/projects/codex-recall",
        "2026-04-13T02:00:00Z",
        "LaunchAgent watcher freshness decision.",
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
        .args(["search", "watcher freshness", "--json", "--db"])
        .arg(&db)
        .output()
        .unwrap();
    assert!(search.status.success());
    let json: serde_json::Value = serde_json::from_slice(&search.stdout).unwrap();
    let session_key = json["results"][0]["session_key"].as_str().unwrap();

    let pin = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["pin", session_key, "--label", "watcher design", "--db"])
        .arg(&db)
        .args(["--pins"])
        .arg(&pins)
        .output()
        .unwrap();
    assert!(pin.status.success());

    let unpin = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["unpin", session_key, "--pins"])
        .arg(&pins)
        .output()
        .unwrap();
    assert!(
        unpin.status.success(),
        "unpin failed: {}",
        String::from_utf8_lossy(&unpin.stderr)
    );
    assert!(String::from_utf8_lossy(&unpin.stdout).contains("unpinned watcher-session"));

    let pins_output = Command::new(env!("CARGO_BIN_EXE_codex-recall"))
        .args(["pins", "--json", "--pins"])
        .arg(&pins)
        .output()
        .unwrap();
    assert!(pins_output.status.success());
    let json: serde_json::Value = serde_json::from_slice(&pins_output.stdout).unwrap();
    assert_eq!(json["count"], 0);
    assert_eq!(json["pins"].as_array().unwrap().len(), 0);
}
