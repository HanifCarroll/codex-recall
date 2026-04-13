use crate::config::{default_db_path, default_source_roots, default_state_path};
use crate::indexer::{
    index_sources_with_progress, scan_sources_for_pending, IndexReport, SourceScanReport,
};
use crate::store::{SearchOptions, SearchResult, Store};
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    let mut args = args.into_iter();
    let Some(command) = args.next() else {
        print_help();
        return Ok(());
    };

    match command.as_str() {
        "index" => run_index(args.collect()),
        "rebuild" => run_rebuild(args.collect()),
        "watch" => run_watch(args.collect()),
        "status" => run_status(args.collect()),
        "search" => run_search(args.collect()),
        "bundle" => run_bundle(args.collect()),
        "show" => run_show(args.collect()),
        "doctor" => run_doctor(args.collect()),
        "stats" => run_stats(args.collect()),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        _ => bail!("unknown command `{command}`"),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct WatchState {
    last_run_at: Option<String>,
    last_indexed_at: Option<String>,
    last_error: Option<String>,
    last_indexed_sessions: usize,
    last_indexed_events: usize,
    last_files_seen: usize,
    last_files_total: usize,
    pending_files: usize,
}

fn run_show(args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        bail!("show requires a session id");
    }

    let session_ref = args[0].clone();
    let mut db_path = default_db_path()?;
    let mut limit = 80usize;
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--limit" => {
                index += 1;
                let raw = args
                    .get(index)
                    .ok_or_else(|| anyhow!("--limit requires a value"))?;
                limit = raw
                    .parse()
                    .with_context(|| format!("parse --limit value `{raw}`"))?;
            }
            flag => bail!("unknown show flag `{flag}`"),
        }
        index += 1;
    }

    let store = Store::open(&db_path)?;
    let matches = store.resolve_session_reference(&session_ref)?;
    if matches.is_empty() {
        println!("no indexed events for {session_ref}");
        return Ok(());
    }
    if matches.len() > 1 {
        let choices = matches
            .iter()
            .map(|session| {
                format!(
                    "  {}  {}  {}",
                    session.session_key,
                    session.cwd,
                    session.source_file_path.display()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        bail!("multiple indexed sessions match `{session_ref}`; use one session_key:\n{choices}");
    }

    let session = &matches[0];
    let events = store.session_events(&session.session_key, limit)?;
    if events.is_empty() {
        println!("no indexed events for {session_ref}");
        return Ok(());
    }

    println!("{}  {}", session.session_key, session.session_id);
    println!("{}", events[0].cwd);
    for event in events {
        println!(
            "\n{}  {}:{}",
            event.kind.as_str(),
            event.source_file_path.display(),
            event.source_line_number
        );
        println!("{}", preview(&event.text, 900));
    }

    Ok(())
}

fn run_index(args: Vec<String>) -> Result<()> {
    let mut db_path = default_db_path()?;
    let mut sources = Vec::new();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--source" => {
                index += 1;
                sources.push(required_path(&args, index, "--source")?);
            }
            flag => bail!("unknown index flag `{flag}`"),
        }
        index += 1;
    }

    if sources.is_empty() {
        sources = default_source_roots()?;
    }

    let store = Store::open(&db_path)?;
    let started = Instant::now();
    let report = index_sources_with_progress(&store, &sources, |report| {
        eprintln!("{}", progress_line(report, started.elapsed()));
    })?;
    println!(
        "indexed {} session files, {} events from {}/{} files ({} skipped: {} unchanged, {} missing, {} non-session) into {}",
        report.sessions_indexed,
        report.events_indexed,
        report.files_seen,
        report.files_total,
        report.files_skipped,
        report.skipped_unchanged,
        report.skipped_missing,
        report.skipped_non_session,
        db_path.display()
    );
    Ok(())
}

fn run_rebuild(args: Vec<String>) -> Result<()> {
    let mut db_path = default_db_path()?;
    let mut sources = Vec::new();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--source" => {
                index += 1;
                sources.push(required_path(&args, index, "--source")?);
            }
            flag => bail!("unknown rebuild flag `{flag}`"),
        }
        index += 1;
    }

    if sources.is_empty() {
        sources = default_source_roots()?;
    }

    remove_db_files(&db_path)?;
    let store = Store::open(&db_path)?;
    let started = Instant::now();
    let report = index_sources_with_progress(&store, &sources, |report| {
        eprintln!("{}", progress_line(report, started.elapsed()));
    })?;
    println!(
        "rebuilt {} session files, {} events from {}/{} files ({} skipped: {} unchanged, {} missing, {} non-session) into {}",
        report.sessions_indexed,
        report.events_indexed,
        report.files_seen,
        report.files_total,
        report.files_skipped,
        report.skipped_unchanged,
        report.skipped_missing,
        report.skipped_non_session,
        db_path.display()
    );
    Ok(())
}

fn run_watch(args: Vec<String>) -> Result<()> {
    let mut db_path = default_db_path()?;
    let mut state_path = default_state_path()?;
    let mut sources = Vec::new();
    let mut interval = Duration::from_secs(10);
    let mut quiet_for = Duration::from_secs(3);
    let mut once = false;
    let mut install_launch_agent = false;
    let mut agent_label = "com.hanif.codex-recall.watch".to_owned();
    let mut agent_path = None;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--state" => {
                index += 1;
                state_path = required_path(&args, index, "--state")?;
            }
            "--source" => {
                index += 1;
                sources.push(required_path(&args, index, "--source")?);
            }
            "--interval" => {
                index += 1;
                interval = parse_seconds(&args, index, "--interval")?;
            }
            "--quiet-for" => {
                index += 1;
                quiet_for = parse_seconds(&args, index, "--quiet-for")?;
            }
            "--once" => once = true,
            "--install-launch-agent" => install_launch_agent = true,
            "--agent-label" => {
                index += 1;
                agent_label = required_value(&args, index, "--agent-label")?.to_owned();
            }
            "--agent-path" => {
                index += 1;
                agent_path = Some(required_path(&args, index, "--agent-path")?);
            }
            flag => bail!("unknown watch flag `{flag}`"),
        }
        index += 1;
    }

    if sources.is_empty() {
        sources = default_source_roots()?;
    }

    if install_launch_agent {
        let agent_path = agent_path.unwrap_or(default_launch_agent_path(&agent_label)?);
        install_launch_agent_plist(
            &agent_path,
            &agent_label,
            &db_path,
            &state_path,
            &sources,
            interval,
            quiet_for,
        )?;
        println!("installed launch agent: {}", agent_path.display());
        println!(
            "next: launchctl bootstrap gui/$(id -u) {}",
            agent_path.display()
        );
        return Ok(());
    }

    loop {
        match run_watch_iteration(&db_path, &state_path, &sources, quiet_for) {
            Ok(()) => {
                if once {
                    return Ok(());
                }
            }
            Err(error) => {
                let mut state = read_watch_state(&state_path).unwrap_or_default();
                state.last_run_at = Some(now_timestamp());
                state.last_error = Some(format!("{error:#}"));
                let _ = write_watch_state(&state_path, &state);
                if once {
                    return Err(error);
                }
                eprintln!("watch error: {error:#}");
            }
        }
        thread::sleep(interval);
    }
}

fn run_watch_iteration(
    db_path: &Path,
    state_path: &Path,
    sources: &[PathBuf],
    quiet_for: Duration,
) -> Result<()> {
    let mut state = read_watch_state(state_path).unwrap_or_default();
    let (scan, last_indexed_at) = scan_status(db_path, sources, quiet_for)?;
    state.last_run_at = Some(now_timestamp());
    state.pending_files = scan.pending_files;
    state.last_error = None;

    if scan.pending_files == 0 {
        state.last_indexed_at = last_indexed_at.or(state.last_indexed_at);
        write_watch_state(state_path, &state)?;
        println!("watch idle: no pending files");
        return Ok(());
    }

    if scan.waiting_files > 0 {
        state.last_indexed_at = last_indexed_at.or(state.last_indexed_at);
        write_watch_state(state_path, &state)?;
        println!(
            "watch waiting: {} pending files, {} still within quiet window",
            scan.pending_files, scan.waiting_files
        );
        return Ok(());
    }

    let store = Store::open(db_path)?;
    let started = Instant::now();
    let report = index_sources_with_progress(&store, sources, |report| {
        eprintln!("{}", progress_line(report, started.elapsed()));
    })?;
    let remaining = scan_sources_for_pending(Some(&store), sources, quiet_for)?;

    state.last_run_at = Some(now_timestamp());
    state.last_indexed_at = store
        .last_indexed_at()?
        .or_else(|| state.last_run_at.clone());
    state.last_indexed_sessions = report.sessions_indexed;
    state.last_indexed_events = report.events_indexed;
    state.last_files_seen = report.files_seen;
    state.last_files_total = report.files_total;
    state.pending_files = remaining.pending_files;
    state.last_error = None;
    write_watch_state(state_path, &state)?;

    println!(
        "watch indexed {} session files, {} events from {}/{} files; {} pending files remain",
        report.sessions_indexed,
        report.events_indexed,
        report.files_seen,
        report.files_total,
        remaining.pending_files
    );
    Ok(())
}

fn run_status(args: Vec<String>) -> Result<()> {
    let mut db_path = default_db_path()?;
    let mut state_path = default_state_path()?;
    let mut sources = Vec::new();
    let mut json_output = false;
    let mut quiet_for = Duration::from_secs(3);
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--json" => json_output = true,
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--state" => {
                index += 1;
                state_path = required_path(&args, index, "--state")?;
            }
            "--source" => {
                index += 1;
                sources.push(required_path(&args, index, "--source")?);
            }
            "--quiet-for" => {
                index += 1;
                quiet_for = parse_seconds(&args, index, "--quiet-for")?;
            }
            flag => bail!("unknown status flag `{flag}`"),
        }
        index += 1;
    }

    if sources.is_empty() {
        sources = default_source_roots()?;
    }

    let state = read_watch_state(&state_path).unwrap_or_default();
    let (scan, last_indexed_at) = scan_status(&db_path, &sources, quiet_for)?;
    let last_indexed_at = last_indexed_at.or(state.last_indexed_at.clone());

    if json_output {
        let value = json!({
            "db_path": db_path,
            "db_exists": db_path.exists(),
            "state_path": state_path,
            "files_total": scan.files_total,
            "pending_files": scan.pending_files,
            "pending_bytes": scan.pending_bytes,
            "stable_pending_files": scan.stable_pending_files,
            "waiting_files": scan.waiting_files,
            "missing_sources": scan.missing_sources,
            "last_run_at": state.last_run_at,
            "last_indexed_at": last_indexed_at,
            "last_error": state.last_error,
            "last_indexed_sessions": state.last_indexed_sessions,
            "last_indexed_events": state.last_indexed_events,
        });
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    let db_status = if db_path.exists() {
        "exists"
    } else {
        "missing"
    };
    println!("database: {} ({db_status})", db_path.display());
    println!("state: {}", state_path.display());
    println!(
        "pending: {} files, {} stable, {} waiting, {}",
        scan.pending_files,
        scan.stable_pending_files,
        scan.waiting_files,
        format_bytes(scan.pending_bytes)
    );
    println!(
        "last_indexed_at: {}",
        last_indexed_at.unwrap_or_else(|| "never".to_owned())
    );
    println!(
        "last_error: {}",
        state.last_error.unwrap_or_else(|| "none".to_owned())
    );
    for source in sources {
        let status = if source.exists() { "exists" } else { "missing" };
        println!("source: {} ({status})", source.display());
    }
    Ok(())
}

fn progress_line(report: &IndexReport, elapsed: Duration) -> String {
    let percent = if report.files_total == 0 {
        100.0
    } else {
        (report.files_seen as f64 / report.files_total as f64) * 100.0
    };
    let eta = estimate_eta(elapsed, report.files_seen, report.files_total)
        .map(format_duration)
        .unwrap_or_else(|| "unknown".to_owned());
    let current = report
        .current_file
        .as_ref()
        .map(|path| shorten_path(path, 96))
        .unwrap_or_else(|| "-".to_owned());

    format!(
        "progress: {}/{} files ({percent:.1}%), bytes {}/{}, indexed {}, skipped {} (unchanged {}, missing {}, non-session {}), elapsed {}, eta {}, current {}",
        report.files_seen,
        report.files_total,
        format_bytes(report.bytes_seen),
        format_bytes(report.bytes_total),
        report.sessions_indexed,
        report.files_skipped,
        report.skipped_unchanged,
        report.skipped_missing,
        report.skipped_non_session,
        format_duration(elapsed),
        eta,
        current
    )
}

fn estimate_eta(elapsed: Duration, seen: usize, total: usize) -> Option<Duration> {
    if seen == 0 || total == 0 || seen >= total {
        return None;
    }
    let elapsed_secs = elapsed.as_secs_f64();
    if elapsed_secs <= 0.0 {
        return None;
    }
    let per_file = elapsed_secs / seen as f64;
    Some(Duration::from_secs_f64(per_file * (total - seen) as f64))
}

fn format_duration(duration: Duration) -> String {
    let total = duration.as_secs();
    let hours = total / 3600;
    let minutes = (total % 3600) / 60;
    let seconds = total % 60;
    if hours > 0 {
        format!("{hours}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes}:{seconds:02}")
    }
}

fn now_timestamp() -> String {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    format_unix_timestamp(duration.as_secs() as i64, duration.subsec_millis())
}

fn format_unix_timestamp(seconds: i64, millis: u32) -> String {
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    let hour = seconds_of_day / 3_600;
    let minute = (seconds_of_day % 3_600) / 60;
    let second = seconds_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{millis:03}Z")
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i64, u32, u32) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month as u32, day as u32)
}

fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn shorten_path(path: &Path, max_chars: usize) -> String {
    let value = path.display().to_string();
    if value.chars().count() <= max_chars {
        return value;
    }

    let tail_len = max_chars.saturating_sub(3);
    let tail = value
        .chars()
        .rev()
        .take(tail_len)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    format!("...{tail}")
}

fn run_search(args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        bail!("search requires a query");
    }

    let query = args[0].clone();
    let mut db_path = default_db_path()?;
    let mut limit = 10usize;
    let mut json_output = false;
    let mut repo = None;
    let mut cwd = None;
    let mut since = None;
    let mut all_repos = false;
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--json" => {
                json_output = true;
            }
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--limit" => {
                index += 1;
                let raw = args
                    .get(index)
                    .ok_or_else(|| anyhow!("--limit requires a value"))?;
                limit = raw
                    .parse()
                    .with_context(|| format!("parse --limit value `{raw}`"))?;
            }
            "--repo" => {
                index += 1;
                repo = Some(required_value(&args, index, "--repo")?.to_owned());
            }
            "--all-repos" => {
                all_repos = true;
            }
            "--cwd" => {
                index += 1;
                cwd = Some(required_value(&args, index, "--cwd")?.to_owned());
            }
            "--since" => {
                index += 1;
                since = Some(required_value(&args, index, "--since")?.to_owned());
            }
            flag => bail!("unknown search flag `{flag}`"),
        }
        index += 1;
    }

    let store = Store::open(&db_path)?;
    let current_repo = if repo.is_none() && !all_repos {
        detect_current_repo()
    } else {
        None
    };
    let search_limit = if json_output {
        limit
    } else {
        limit.saturating_mul(5)
    };
    let results = store.search_with_options(SearchOptions {
        query: query.clone(),
        limit: search_limit,
        repo,
        cwd,
        since,
        current_repo,
    })?;
    if json_output {
        print_search_json(&query, &results)?;
        return Ok(());
    }

    if results.is_empty() {
        println!("no matches");
        return Ok(());
    }

    print_grouped_search_results(&results, limit);

    Ok(())
}

fn run_bundle(args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        bail!("bundle requires a query");
    }

    let query = args[0].clone();
    let mut db_path = default_db_path()?;
    let mut limit = 5usize;
    let mut repo = None;
    let mut cwd = None;
    let mut since = None;
    let mut all_repos = false;
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--limit" => {
                index += 1;
                let raw = args
                    .get(index)
                    .ok_or_else(|| anyhow!("--limit requires a value"))?;
                limit = raw
                    .parse()
                    .with_context(|| format!("parse --limit value `{raw}`"))?;
            }
            "--repo" => {
                index += 1;
                repo = Some(required_value(&args, index, "--repo")?.to_owned());
            }
            "--all-repos" => all_repos = true,
            "--cwd" => {
                index += 1;
                cwd = Some(required_value(&args, index, "--cwd")?.to_owned());
            }
            "--since" => {
                index += 1;
                since = Some(required_value(&args, index, "--since")?.to_owned());
            }
            flag => bail!("unknown bundle flag `{flag}`"),
        }
        index += 1;
    }

    let store = Store::open(&db_path)?;
    let current_repo = if repo.is_none() && !all_repos {
        detect_current_repo()
    } else {
        None
    };
    let results = store.search_with_options(SearchOptions {
        query: query.clone(),
        limit: limit.saturating_mul(5).max(limit),
        repo: repo.clone(),
        cwd: cwd.clone(),
        since: since.clone(),
        current_repo,
    })?;

    print_bundle(&query, &db_path, limit, &repo, &cwd, &since, &results);
    Ok(())
}

fn run_doctor(args: Vec<String>) -> Result<()> {
    let mut db_path = default_db_path()?;
    let mut json_output = false;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--json" => json_output = true,
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            flag => bail!("unknown doctor flag `{flag}`"),
        }
        index += 1;
    }

    let db_existed = db_path.exists();
    let sources = default_source_roots()?;
    let source_status = sources
        .iter()
        .map(|source| {
            json!({
                "path": source,
                "exists": source.exists(),
            })
        })
        .collect::<Vec<_>>();

    if !db_existed {
        if json_output {
            let value = json!({
                "ok": false,
                "db_path": db_path,
                "db_existed": false,
                "db_exists": false,
                "checks": {
                    "quick_check": "missing",
                    "fts_integrity": "missing",
                },
                "stats": {
                    "sessions": 0,
                    "events": 0,
                    "source_files": 0,
                    "duplicate_source_files": 0,
                },
                "sources": source_status,
            });
            println!("{}", serde_json::to_string_pretty(&value)?);
            return Ok(());
        }

        println!("database: {} (missing)", db_path.display());
        println!("quick_check: missing");
        println!("fts_integrity: missing");
        println!("stats: 0 sessions, 0 events, 0 source files, 0 duplicate source files");
        for source in sources {
            let status = if source.exists() { "exists" } else { "missing" };
            println!("source: {} ({status})", source.display());
        }
        println!("next: run codex-recall index");
        return Ok(());
    }

    let store = Store::open_readonly(&db_path)?;
    let stats = store.stats()?;
    let quick_check = match store.quick_check() {
        Ok(value) => value,
        Err(error) => format!("error: {error:#}"),
    };
    let fts_integrity = match store.fts_read_check() {
        Ok(()) => "ok".to_owned(),
        Err(error) => format!("error: {error:#}"),
    };
    let ok = quick_check == "ok" && fts_integrity == "ok";

    if json_output {
        let value = json!({
            "ok": ok,
            "db_path": db_path,
            "db_existed": db_existed,
            "db_exists": db_existed,
            "checks": {
                "quick_check": quick_check,
                "fts_integrity": fts_integrity,
            },
            "stats": {
                "sessions": stats.session_count,
                "events": stats.event_count,
                "source_files": stats.source_file_count,
                "duplicate_source_files": stats.duplicate_source_file_count,
            },
            "sources": source_status,
        });
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    println!("database: {}", db_path.display());
    println!("quick_check: {quick_check}");
    println!("fts_integrity: {fts_integrity}");
    println!(
        "stats: {} sessions, {} events, {} source files, {} duplicate source files",
        stats.session_count,
        stats.event_count,
        stats.source_file_count,
        stats.duplicate_source_file_count
    );
    for source in sources {
        let status = if source.exists() { "exists" } else { "missing" };
        println!("source: {} ({status})", source.display());
    }
    Ok(())
}

fn run_stats(args: Vec<String>) -> Result<()> {
    let mut db_path = default_db_path()?;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            flag => bail!("unknown stats flag `{flag}`"),
        }
        index += 1;
    }

    let stats = Store::open(db_path)?.stats()?;
    println!(
        "{} sessions, {} events, {} source files, {} duplicate source files",
        stats.session_count,
        stats.event_count,
        stats.source_file_count,
        stats.duplicate_source_file_count
    );
    Ok(())
}

fn scan_status(
    db_path: &Path,
    sources: &[PathBuf],
    quiet_for: Duration,
) -> Result<(SourceScanReport, Option<String>)> {
    if !db_path.exists() {
        return Ok((scan_sources_for_pending(None, sources, quiet_for)?, None));
    }

    let store = Store::open_readonly(db_path)?;
    let last_indexed_at = store.last_indexed_at()?;
    let scan = scan_sources_for_pending(Some(&store), sources, quiet_for)?;
    Ok((scan, last_indexed_at))
}

fn read_watch_state(path: &Path) -> Result<WatchState> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(WatchState::default());
        }
        Err(error) => return Err(error).with_context(|| format!("read {}", path.display())),
    };
    serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))
}

fn write_watch_state(path: &Path, state: &WatchState) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(state)?;
    fs::write(path, bytes).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn parse_seconds(args: &[String], index: usize, flag: &str) -> Result<Duration> {
    let raw = required_value(args, index, flag)?;
    let seconds = raw
        .parse::<u64>()
        .with_context(|| format!("parse {flag} value `{raw}`"))?;
    Ok(Duration::from_secs(seconds))
}

fn default_launch_agent_path(label: &str) -> Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME is not set"))?;
    Ok(home
        .join("Library")
        .join("LaunchAgents")
        .join(format!("{label}.plist")))
}

fn install_launch_agent_plist(
    path: &Path,
    label: &str,
    db_path: &Path,
    state_path: &Path,
    sources: &[PathBuf],
    interval: Duration,
    quiet_for: Duration,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }

    let executable = std::env::current_exe().context("resolve current executable")?;
    let stdout_path = state_path.with_extension("out.log");
    let stderr_path = state_path.with_extension("err.log");
    let mut args = vec![
        executable.display().to_string(),
        "watch".to_owned(),
        "--db".to_owned(),
        db_path.display().to_string(),
        "--state".to_owned(),
        state_path.display().to_string(),
        "--interval".to_owned(),
        interval.as_secs().to_string(),
        "--quiet-for".to_owned(),
        quiet_for.as_secs().to_string(),
    ];
    for source in sources {
        args.push("--source".to_owned());
        args.push(source.display().to_string());
    }

    let program_arguments = args
        .iter()
        .map(|arg| format!("    <string>{}</string>", xml_escape(arg)))
        .collect::<Vec<_>>()
        .join("\n");
    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>{}</string>
  <key>ProgramArguments</key>
  <array>
{}
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>{}</string>
  <key>StandardErrorPath</key>
  <string>{}</string>
</dict>
</plist>
"#,
        xml_escape(label),
        program_arguments,
        xml_escape(&stdout_path.display().to_string()),
        xml_escape(&stderr_path.display().to_string())
    );
    fs::write(path, plist).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn xml_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn required_path(args: &[String], index: usize, flag: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(required_value(args, index, flag)?))
}

fn required_value<'a>(args: &'a [String], index: usize, flag: &str) -> Result<&'a str> {
    args.get(index)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn remove_db_files(db_path: &Path) -> Result<()> {
    for path in [
        db_path.to_path_buf(),
        PathBuf::from(format!("{}-wal", db_path.display())),
        PathBuf::from(format!("{}-shm", db_path.display())),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("remove {}", path.display()));
            }
        }
    }
    Ok(())
}

fn detect_current_repo() -> Option<String> {
    let mut path = std::env::current_dir().ok()?;
    loop {
        if path.join(".git").exists() {
            return path
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned);
        }
        if !path.pop() {
            return None;
        }
    }
}

fn compact_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn print_grouped_search_results(results: &[SearchResult], limit: usize) {
    let mut session_order = Vec::<&str>::new();
    for result in results {
        if !session_order
            .iter()
            .any(|session_id| *session_id == result.session_key)
        {
            session_order.push(&result.session_key);
        }
        if session_order.len() == limit {
            break;
        }
    }

    for (index, session_id) in session_order.iter().enumerate() {
        let session_results = results
            .iter()
            .filter(|result| &result.session_key == session_id)
            .collect::<Vec<_>>();
        let first = session_results[0];
        println!(
            "{}. {}  {}  {}",
            index + 1,
            first.session_key,
            first.session_id,
            first.cwd
        );
        for result in session_results.into_iter().take(3) {
            println!(
                "   - {}  {}:{}",
                result.kind.as_str(),
                result.source_file_path.display(),
                result.source_line_number
            );
            println!("     {}", compact_whitespace(&result.snippet));
        }
    }
}

fn print_bundle(
    query: &str,
    db_path: &Path,
    limit: usize,
    repo: &Option<String>,
    cwd: &Option<String>,
    since: &Option<String>,
    results: &[SearchResult],
) {
    println!("# codex-recall bundle");
    println!();
    println!("Query: {query}");
    println!("Database: {}", db_path.display());
    let filters = bundle_filters(repo, cwd, since);
    if !filters.is_empty() {
        println!("Filters: {}", filters.join(", "));
    }
    println!("Generated: {}", now_timestamp());

    if results.is_empty() {
        println!();
        println!("No matches.");
        return;
    }

    let session_keys = top_session_keys(results, limit);
    println!();
    println!("## Top Sessions");
    for (index, session_key) in session_keys.iter().enumerate() {
        let session_results = results
            .iter()
            .filter(|result| &result.session_key == session_key)
            .collect::<Vec<_>>();
        let first = session_results[0];
        println!(
            "{}. {}  {}  {}",
            index + 1,
            first.session_key,
            first.session_id,
            first.cwd
        );
        println!("   when: {}", first.session_timestamp);
        println!(
            "   show: codex-recall show {} --limit 120",
            shell_quote(&first.session_key)
        );
        println!("   receipts: {}", session_results.len().min(3));
    }

    println!();
    println!("## Receipts");
    for session_key in &session_keys {
        println!();
        println!("### {session_key}");
        for result in results
            .iter()
            .filter(|result| &result.session_key == session_key)
            .take(3)
        {
            println!(
                "- {}  {}:{}",
                result.kind.as_str(),
                result.source_file_path.display(),
                result.source_line_number
            );
            println!("  {}", preview(&result.text, 500));
        }
    }

    println!();
    println!("## Next Commands");
    for session_key in session_keys {
        println!(
            "- codex-recall show {} --limit 120",
            shell_quote(session_key)
        );
    }
}

fn bundle_filters(
    repo: &Option<String>,
    cwd: &Option<String>,
    since: &Option<String>,
) -> Vec<String> {
    let mut filters = Vec::new();
    if let Some(repo) = repo {
        filters.push(format!("repo={repo}"));
    }
    if let Some(cwd) = cwd {
        filters.push(format!("cwd={cwd}"));
    }
    if let Some(since) = since {
        filters.push(format!("since={since}"));
    }
    filters
}

fn top_session_keys(results: &[SearchResult], limit: usize) -> Vec<&str> {
    let mut session_keys = Vec::<&str>::new();
    for result in results {
        if !session_keys
            .iter()
            .any(|session_key| *session_key == result.session_key)
        {
            session_keys.push(&result.session_key);
        }
        if session_keys.len() == limit {
            break;
        }
    }
    session_keys
}

fn shell_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}

fn preview(value: &str, limit: usize) -> String {
    let compact = compact_whitespace(value);
    if compact.len() <= limit {
        return compact;
    }

    let mut output = compact
        .char_indices()
        .take_while(|(index, _)| *index < limit)
        .map(|(_, ch)| ch)
        .collect::<String>();
    output.push_str(" ...");
    output
}

fn print_search_json(query: &str, results: &[SearchResult]) -> Result<()> {
    let results = results
        .iter()
        .map(|result| {
            let source = format!(
                "{}:{}",
                result.source_file_path.display(),
                result.source_line_number
            );
            json!({
                "session_key": result.session_key,
                "session_id": result.session_id,
                "repo": result.repo,
                "kind": result.kind.as_str(),
                "cwd": result.cwd,
                "session_timestamp": result.session_timestamp,
                "source_file_path": result.source_file_path,
                "source_line_number": result.source_line_number,
                "source": source,
                "source_timestamp": result.source_timestamp,
                "score": result.score,
                "snippet": compact_whitespace(&result.snippet),
                "text_preview": preview(&result.text, 500),
            })
        })
        .collect::<Vec<_>>();

    let value = json!({
        "query": query,
        "count": results.len(),
        "results": results,
    });
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn print_help() {
    println!(
        "codex-recall\n\nCommands:\n  index [--db PATH] [--source PATH ...]\n  rebuild [--db PATH] [--source PATH ...]\n  watch [--db PATH] [--state PATH] [--source PATH ...] [--interval SECONDS] [--quiet-for SECONDS] [--once] [--install-launch-agent]\n  status [--db PATH] [--state PATH] [--source PATH ...] [--quiet-for SECONDS] [--json]\n  search QUERY [--db PATH] [--limit N] [--repo NAME] [--all-repos] [--cwd PATH_PART] [--since DATE|Nd|today] [--json]\n  bundle QUERY [--db PATH] [--limit N] [--repo NAME] [--all-repos] [--cwd PATH_PART] [--since DATE|Nd|today]\n  show SESSION_ID_OR_KEY [--db PATH] [--limit N]\n  doctor [--db PATH] [--json]\n  stats [--db PATH]"
    );
}

#[allow(dead_code)]
fn os_string_to_string(value: OsString) -> Result<String> {
    value
        .into_string()
        .map_err(|_| anyhow!("argument is not valid UTF-8"))
}
