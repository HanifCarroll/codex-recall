use anyhow::{anyhow, Result};
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub index_path: PathBuf,
    pub source_roots: Vec<PathBuf>,
}

pub fn default_db_path() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("CODEX_RECALL_DB") {
        return Ok(PathBuf::from(path));
    }

    Ok(home_dir()?
        .join(".local")
        .join("share")
        .join("codex-recall")
        .join("index.sqlite"))
}

pub fn default_state_path() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("CODEX_RECALL_STATE") {
        return Ok(PathBuf::from(path));
    }

    Ok(home_dir()?
        .join(".local")
        .join("state")
        .join("codex-recall")
        .join("watch.json"))
}

pub fn default_pins_path() -> Result<PathBuf> {
    if let Ok(path) = std::env::var("CODEX_RECALL_PINS") {
        return Ok(PathBuf::from(path));
    }

    Ok(home_dir()?
        .join(".local")
        .join("share")
        .join("codex-recall")
        .join("pins.json"))
}

pub fn default_source_roots() -> Result<Vec<PathBuf>> {
    let home = home_dir()?;
    Ok(vec![
        home.join(".codex").join("sessions"),
        home.join(".codex").join("archived_sessions"),
    ])
}

fn home_dir() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME is not set"))
}
