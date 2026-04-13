pub mod cli;
pub mod config;
pub mod indexer;
pub mod parser;
pub mod redact;
pub mod store;

use anyhow::Result;

pub fn run() -> Result<()> {
    cli::run(std::env::args().skip(1))
}
