use crate::parser::parse_session_file;
use crate::store::Store;
use anyhow::{Context, Result};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexReport {
    pub files_seen: usize,
    pub files_skipped: usize,
    pub sessions_indexed: usize,
    pub events_indexed: usize,
}

pub fn index_sources(store: &Store, sources: &[PathBuf]) -> Result<IndexReport> {
    index_sources_with_progress(store, sources, |_| {})
}

pub fn index_sources_with_progress<F>(
    store: &Store,
    sources: &[PathBuf],
    mut on_progress: F,
) -> Result<IndexReport>
where
    F: FnMut(&IndexReport),
{
    let mut report = IndexReport {
        files_seen: 0,
        files_skipped: 0,
        sessions_indexed: 0,
        events_indexed: 0,
    };

    for source in sources {
        for path in jsonl_files(source)? {
            report.files_seen += 1;
            if report.files_seen % 100 == 0 {
                on_progress(&report);
            }
            let file_state = FileState::from_path(&path)?;
            if store.is_source_current(
                &path,
                file_state.source_file_mtime_ns,
                file_state.source_file_size,
            )? {
                report.files_skipped += 1;
                continue;
            }

            if let Some(parsed) = parse_session_file(&path)? {
                report.events_indexed += parsed.events.len();
                store.index_session(&parsed)?;
                store.mark_source_indexed(
                    &path,
                    file_state.source_file_mtime_ns,
                    file_state.source_file_size,
                    Some(&parsed.session.id),
                )?;
                report.sessions_indexed += 1;
            } else {
                store.mark_source_indexed(
                    &path,
                    file_state.source_file_mtime_ns,
                    file_state.source_file_size,
                    None,
                )?;
            }
        }
    }

    Ok(report)
}

struct FileState {
    source_file_mtime_ns: i64,
    source_file_size: i64,
}

impl FileState {
    fn from_path(path: &Path) -> Result<Self> {
        let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
        let modified = metadata
            .modified()
            .with_context(|| format!("read mtime {}", path.display()))?;
        let source_file_mtime_ns = modified
            .duration_since(UNIX_EPOCH)
            .with_context(|| format!("mtime before unix epoch {}", path.display()))?
            .as_nanos() as i64;
        let source_file_size = metadata.len() as i64;

        Ok(Self {
            source_file_mtime_ns,
            source_file_size,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_root(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "codex-recall-indexer-test-{}-{}",
            std::process::id(),
            name
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_session(path: &Path, message: &str) {
        std::fs::write(
            path,
            format!(
                r#"{{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{{"id":"session-1","timestamp":"2026-04-13T01:00:00Z","cwd":"/tmp"}}}}
{{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{{"type":"user_message","message":"{message}"}}}}
"#
            ),
        )
        .unwrap();
    }

    #[test]
    fn skips_unchanged_files_on_second_index_run() {
        let root = temp_root("incremental");
        let source = root.join("sessions");
        std::fs::create_dir_all(&source).unwrap();
        write_session(&source.join("session.jsonl"), "First message");
        let store = Store::open(root.join("index.sqlite")).unwrap();

        let first = index_sources(&store, &[source.clone()]).unwrap();
        let second = index_sources(&store, &[source]).unwrap();

        assert_eq!(first.files_seen, 1);
        assert_eq!(first.files_skipped, 0);
        assert_eq!(first.sessions_indexed, 1);
        assert_eq!(second.files_seen, 1);
        assert_eq!(second.files_skipped, 1);
        assert_eq!(second.sessions_indexed, 0);
    }
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
