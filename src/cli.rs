use crate::commands::doctor::{run_doctor, run_stats, DoctorArgs, StatsArgs};
use crate::commands::index::{run_index, run_rebuild, IndexArgs, RebuildArgs};
use crate::commands::pins::{run_pin, run_pins, run_unpin, PinArgs, PinsArgs, UnpinArgs};
use crate::commands::recent::{run_day, run_recent, DayArgs, RecentArgs};
use crate::commands::search::{run_bundle, run_search, run_show, BundleArgs, SearchArgs, ShowArgs};
use crate::commands::watch::{run_status, run_watch, StatusArgs, WatchArgs};
use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    version,
    about = "Local search and recall for Codex session JSONL archives"
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Incrementally index Codex session archives.
    Index(IndexArgs),
    /// Delete and rebuild the index from session archives.
    Rebuild(RebuildArgs),
    /// Continuously index stable pending session files.
    Watch(WatchArgs),
    /// Show watch/index freshness and pending-file status.
    Status(StatusArgs),
    /// Search indexed sessions and print grouped receipts.
    Search(SearchArgs),
    /// Show latest indexed sessions without a query.
    Recent(RecentArgs),
    /// Show an indexed session inventory for one local calendar day.
    Day(DayArgs),
    /// Print a compact Markdown bundle for a search query.
    Bundle(BundleArgs),
    /// Show indexed events for one session.
    Show(ShowArgs),
    /// Pin a high-value session anchor.
    Pin(PinArgs),
    /// List pinned session anchors.
    Pins(PinsArgs),
    /// Remove a pinned session anchor.
    Unpin(UnpinArgs),
    /// Check database health, FTS integrity, and source paths.
    Doctor(DoctorArgs),
    /// Print database counts.
    Stats(StatsArgs),
}

pub fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    let cli = Cli::parse_from(std::iter::once("codex-recall".to_owned()).chain(args));
    match cli.command {
        Some(Command::Index(args)) => run_index(args),
        Some(Command::Rebuild(args)) => run_rebuild(args),
        Some(Command::Watch(args)) => run_watch(args),
        Some(Command::Status(args)) => run_status(args),
        Some(Command::Search(args)) => run_search(args),
        Some(Command::Recent(args)) => run_recent(args),
        Some(Command::Day(args)) => run_day(args),
        Some(Command::Bundle(args)) => run_bundle(args),
        Some(Command::Show(args)) => run_show(args),
        Some(Command::Pin(args)) => run_pin(args),
        Some(Command::Pins(args)) => run_pins(args),
        Some(Command::Unpin(args)) => run_unpin(args),
        Some(Command::Doctor(args)) => run_doctor(args),
        Some(Command::Stats(args)) => run_stats(args),
        None => {
            Cli::command().print_help()?;
            println!();
            Ok(())
        }
    }
}
