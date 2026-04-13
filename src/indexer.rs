use crate::parser::parse_session_file;
use crate::store::Store;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexReport {
    pub files_seen: usize,
    pub sessions_indexed: usize,
    pub events_indexed: usize,
}

pub fn index_sources(store: &Store, sources: &[PathBuf]) -> Result<IndexReport> {
    let mut report = IndexReport {
        files_seen: 0,
        sessions_indexed: 0,
        events_indexed: 0,
    };

    for source in sources {
        for path in jsonl_files(source)? {
            report.files_seen += 1;
            if let Some(parsed) = parse_session_file(&path)? {
                report.events_indexed += parsed.events.len();
                store.index_session(&parsed)?;
                report.sessions_indexed += 1;
            }
        }
    }

    Ok(report)
}

fn jsonl_files(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_jsonl_files(root, &mut files)?;
    files.sort();
    Ok(files)
}

fn collect_jsonl_files(path: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    if path.is_file() {
        if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            files.push(path.to_path_buf());
        }
        return Ok(());
    }

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl_files(&path, files)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("jsonl") {
            files.push(path);
        }
    }

    Ok(())
}
