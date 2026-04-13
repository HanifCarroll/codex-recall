use crate::commands::index::resolve_sources;
use crate::config::{default_db_path, default_state_path};
use crate::indexer::{index_sources_with_progress, scan_sources_for_pending, SourceScanReport};
use crate::output::{format_bytes, now_timestamp, progress_line};
use crate::store::Store;
use anyhow::{anyhow, Context, Result};
use clap::Args;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Args)]
pub struct WatchArgs {
    #[arg(long, help = "SQLite index path")]
    pub db: Option<PathBuf>,
    #[arg(long, help = "Watch state JSON path")]
    pub state: Option<PathBuf>,
    #[arg(long = "source", help = "Session archive root to scan; repeatable")]
    pub sources: Vec<PathBuf>,
    #[arg(long, default_value_t = 10, help = "Seconds between watch scans")]
    pub interval: u64,
    #[arg(
        long,
        default_value_t = 3,
        help = "Seconds a file must be quiet before indexing"
    )]
    pub quiet_for: u64,
    #[arg(long, help = "Run one scan and exit")]
    pub once: bool,
    #[arg(long, help = "Write a macOS LaunchAgent plist for background indexing")]
    pub install_launch_agent: bool,
    #[arg(
        long,
        default_value = "com.hanif.codex-recall.watch",
        help = "LaunchAgent label"
    )]
    pub agent_label: String,
    #[arg(long, help = "LaunchAgent plist output path")]
    pub agent_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct StatusArgs {
    #[arg(long, help = "SQLite index path")]
    pub db: Option<PathBuf>,
    #[arg(long, help = "Watch state JSON path")]
    pub state: Option<PathBuf>,
    #[arg(long = "source", help = "Session archive root to scan; repeatable")]
    pub sources: Vec<PathBuf>,
    #[arg(
        long,
        default_value_t = 3,
        help = "Seconds a file must be quiet before indexing"
    )]
    pub quiet_for: u64,
    #[arg(long, help = "Emit machine-readable JSON")]
    pub json: bool,
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

pub fn run_watch(args: WatchArgs) -> Result<()> {
    let db_path = args.db.unwrap_or(default_db_path()?);
    let state_path = args.state.unwrap_or(default_state_path()?);
    let sources = resolve_sources(args.sources)?;
    let interval = Duration::from_secs(args.interval);
    let quiet_for = Duration::from_secs(args.quiet_for);

    if args.install_launch_agent {
        let agent_path = args
            .agent_path
            .unwrap_or(default_launch_agent_path(&args.agent_label)?);
        install_launch_agent_plist(
            &agent_path,
            &args.agent_label,
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
                if args.once {
                    return Ok(());
                }
            }
            Err(error) => {
                let mut state = read_watch_state(&state_path).unwrap_or_default();
                state.last_run_at = Some(now_timestamp());
                state.last_error = Some(format!("{error:#}"));
                let _ = write_watch_state(&state_path, &state);
                if args.once {
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

pub fn run_status(args: StatusArgs) -> Result<()> {
    let db_path = args.db.unwrap_or(default_db_path()?);
    let state_path = args.state.unwrap_or(default_state_path()?);
    let sources = resolve_sources(args.sources)?;
    let quiet_for = Duration::from_secs(args.quiet_for);
    let state = read_watch_state(&state_path).unwrap_or_default();
    let (scan, last_indexed_at) = scan_status(&db_path, &sources, quiet_for)?;
    let last_indexed_at = last_indexed_at.or(state.last_indexed_at.clone());

    if args.json {
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
