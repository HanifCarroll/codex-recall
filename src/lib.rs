pub mod cli;
mod commands;
pub mod config;
pub mod indexer;
mod output;
pub mod parser;
pub mod redact;
pub mod store;

use anyhow::Result;

pub fn run() -> Result<()> {
    cli::run(std::env::args().skip(1))
}
