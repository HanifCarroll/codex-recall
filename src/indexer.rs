use crate::parser::parse_session_file;
use crate::store::{build_session_key, Store};
use anyhow::{Context, Result};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexReport {
    pub files_total: usize,
    pub files_seen: usize,
    pub files_skipped: usize,
    pub skipped_unchanged: usize,
    pub skipped_missing: usize,
    pub skipped_non_session: usize,
    pub sessions_indexed: usize,
    pub events_indexed: usize,
    pub bytes_total: u64,
    pub bytes_seen: u64,
    pub current_file: Option<PathBuf>,
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
    let mut files = Vec::new();
    for source in sources {
        files.extend(jsonl_files(source)?);
    }
    files.sort();

    let mut report = IndexReport {
        files_total: files.len(),
        files_seen: 0,
        files_skipped: 0,
        skipped_unchanged: 0,
        skipped_missing: 0,
        skipped_non_session: 0,
        sessions_indexed: 0,
        events_indexed: 0,
        bytes_total: total_known_bytes(&files),
        bytes_seen: 0,
        current_file: None,
    };

    on_progress(&report);

    for path in files {
        report.current_file = Some(path.clone());
        on_progress(&report);

        let file_state = match FileState::from_path(&path) {
            Ok(file_state) => file_state,
            Err(error) if is_not_found_error(&error) => {
                report.files_seen += 1;
                report.files_skipped += 1;
                report.skipped_missing += 1;
                if should_report_after_file(&report) {
                    on_progress(&report);
                }
                continue;
            }
            Err(error) => return Err(error),
        };
        report.bytes_seen = report
            .bytes_seen
            .saturating_add(file_state.source_file_size as u64);

        if store.is_source_current(
            &path,
            file_state.source_file_mtime_ns,
            file_state.source_file_size,
        )? {
            report.files_seen += 1;
            report.files_skipped += 1;
            report.skipped_unchanged += 1;
            if should_report_after_file(&report) {
                on_progress(&report);
            }
            continue;
        }

        let parsed = match parse_session_file(&path) {
            Ok(parsed) => parsed,
            Err(error) if is_not_found_error(&error) => {
                report.files_seen += 1;
                report.files_skipped += 1;
                report.skipped_missing += 1;
                if should_report_after_file(&report) {
                    on_progress(&report);
                }
                continue;
            }
            Err(error) => return Err(error),
        };

        if let Some(parsed) = parsed {
            let session_key =
                build_session_key(&parsed.session.id, &parsed.session.source_file_path);
            report.events_indexed += parsed.events.len();
            store.index_session(&parsed)?;
            store.mark_source_indexed(
                &path,
                file_state.source_file_mtime_ns,
                file_state.source_file_size,
                Some(&parsed.session.id),
                Some(&session_key),
            )?;
            report.sessions_indexed += 1;
        } else {
            store.mark_source_indexed(
                &path,
                file_state.source_file_mtime_ns,
                file_state.source_file_size,
                None,
                None,
            )?;
            report.files_skipped += 1;
            report.skipped_non_session += 1;
        }

        report.files_seen += 1;
        if should_report_after_file(&report) {
            on_progress(&report);
        }
    }

    Ok(report)
}

fn should_report_after_file(report: &IndexReport) -> bool {
    report.files_seen == 1 || report.files_seen % 25 == 0 || report.files_seen == report.files_total
}

fn total_known_bytes(files: &[PathBuf]) -> u64 {
    files
        .iter()
        .filter_map(|path| fs::metadata(path).ok())
        .map(|metadata| metadata.len())
        .sum()
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

fn is_not_found_error(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io_error| io_error.kind() == ErrorKind::NotFound)
    })
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
        let session_file = source.join("session.jsonl");
        write_session(&session_file, "First message");
        let store = Store::open(root.join("index.sqlite")).unwrap();

        let first = index_sources(&store, &[source.clone()]).unwrap();
        let second = index_sources(&store, &[source]).unwrap();

        assert_eq!(first.files_seen, 1);
        assert_eq!(first.files_total, 1);
        assert!(first.bytes_total > 0);
        assert_eq!(first.bytes_seen, first.bytes_total);
        assert_eq!(first.current_file, Some(session_file));
        assert_eq!(first.files_skipped, 0);
        assert_eq!(first.sessions_indexed, 1);
        assert_eq!(second.files_seen, 1);
        assert_eq!(second.files_total, 1);
        assert_eq!(second.files_skipped, 1);
        assert_eq!(second.skipped_unchanged, 1);
        assert_eq!(second.skipped_missing, 0);
        assert_eq!(second.skipped_non_session, 0);
        assert_eq!(second.sessions_indexed, 0);
    }

    #[test]
    fn skips_files_removed_after_scan() {
        let root = temp_root("removed-after-scan");
        let source = root.join("sessions");
        std::fs::create_dir_all(&source).unwrap();
        for index in 0..99 {
            write_session(
                &source.join(format!("a-{index:03}.jsonl")),
                &format!("Message {index}"),
            );
        }
        let disappearing = source.join("z-disappearing.jsonl");
        write_session(&disappearing, "This file disappears during indexing");
        let store = Store::open(root.join("index.sqlite")).unwrap();

        let mut removed = false;
        let report = index_sources_with_progress(&store, &[source], |report| {
            if report.files_seen == 0 && !removed {
                std::fs::remove_file(&disappearing).unwrap();
                removed = true;
            }
        })
        .unwrap();

        assert_eq!(report.files_seen, 100);
        assert_eq!(report.files_total, 100);
        assert_eq!(report.files_skipped, 1);
        assert_eq!(report.skipped_missing, 1);
        assert_eq!(report.sessions_indexed, 99);
    }

    #[test]
    fn reports_current_file_before_processing() {
        let root = temp_root("current-before-processing");
        let source = root.join("sessions");
        std::fs::create_dir_all(&source).unwrap();
        let session_file = source.join("session.jsonl");
        write_session(&session_file, "Current file");
        let store = Store::open(root.join("index.sqlite")).unwrap();

        let mut saw_current_before_index = false;
        index_sources_with_progress(&store, &[source], |report| {
            if report.current_file.as_ref() == Some(&session_file) && report.sessions_indexed == 0 {
                saw_current_before_index = true;
            }
        })
        .unwrap();

        assert!(saw_current_before_index);
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
