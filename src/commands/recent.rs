use crate::config::default_db_path;
use crate::output::shell_quote;
use crate::store::{RecentOptions, RecentSession, Store};
use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

#[derive(Debug, Clone, Args)]
pub struct RecentArgs {
    #[arg(long, help = "SQLite index path")]
    pub db: Option<PathBuf>,
    #[arg(long, default_value_t = 20, help = "Maximum sessions to print")]
    pub limit: usize,
    #[arg(long, help = "Restrict sessions to a repo name")]
    pub repo: Option<String>,
    #[arg(long, help = "Restrict sessions to a cwd substring")]
    pub cwd: Option<String>,
    #[arg(long, help = "Restrict by age, for example 7d, today, or 2026-04-01")]
    pub since: Option<String>,
}

pub fn run_recent(args: RecentArgs) -> Result<()> {
    let db_path = args.db.unwrap_or(default_db_path()?);
    let store = Store::open_readonly(&db_path)?;
    let sessions = store.recent_sessions(RecentOptions {
        limit: args.limit,
        repo: args.repo,
        cwd: args.cwd,
        since: args.since,
    })?;

    if sessions.is_empty() {
        println!("no recent sessions");
        return Ok(());
    }

    print_recent_sessions(&sessions);
    Ok(())
}

pub(crate) fn print_recent_sessions(sessions: &[RecentSession]) {
    for (index, session) in sessions.iter().enumerate() {
        println!(
            "{}. {}  {}  {}",
            index + 1,
            session.session_key,
            session.session_id,
            session.repo
        );
        println!("   when: {}", session.session_timestamp);
        println!("   cwd: {}", session.cwd);
        println!("   source: {}", session.source_file_path.display());
        println!(
            "   show: codex-recall show {} --limit 120",
            shell_quote(&session.session_key)
        );
    }
}
