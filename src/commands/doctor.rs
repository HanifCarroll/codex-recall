use crate::config::{default_db_path, default_source_roots};
use crate::store::Store;
use anyhow::Result;
use clap::Args;
use serde_json::json;
use std::path::PathBuf;

#[derive(Debug, Clone, Args)]
pub struct DoctorArgs {
    #[arg(long, help = "SQLite index path")]
    pub db: Option<PathBuf>,
    #[arg(long, help = "Emit machine-readable JSON")]
    pub json: bool,
}

#[derive(Debug, Clone, Args)]
pub struct StatsArgs {
    #[arg(long, help = "SQLite index path")]
    pub db: Option<PathBuf>,
}

pub fn run_doctor(args: DoctorArgs) -> Result<()> {
    let db_path = args.db.unwrap_or(default_db_path()?);
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
        if args.json {
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

    if args.json {
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

pub fn run_stats(args: StatsArgs) -> Result<()> {
    let db_path = args.db.unwrap_or(default_db_path()?);
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
