use crate::config::{default_db_path, default_source_roots};
use crate::indexer::index_sources_with_progress;
use crate::store::{SearchOptions, SearchResult, Store};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::json;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

pub fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    let mut args = args.into_iter();
    let Some(command) = args.next() else {
        print_help();
        return Ok(());
    };

    match command.as_str() {
        "index" => run_index(args.collect()),
        "rebuild" => run_rebuild(args.collect()),
        "search" => run_search(args.collect()),
        "show" => run_show(args.collect()),
        "doctor" => run_doctor(args.collect()),
        "stats" => run_stats(args.collect()),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        _ => bail!("unknown command `{command}`"),
    }
}

fn run_show(args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        bail!("show requires a session id");
    }

    let session_ref = args[0].clone();
    let mut db_path = default_db_path()?;
    let mut limit = 80usize;
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--limit" => {
                index += 1;
                let raw = args
                    .get(index)
                    .ok_or_else(|| anyhow!("--limit requires a value"))?;
                limit = raw
                    .parse()
                    .with_context(|| format!("parse --limit value `{raw}`"))?;
            }
            flag => bail!("unknown show flag `{flag}`"),
        }
        index += 1;
    }

    let store = Store::open(&db_path)?;
    let matches = store.resolve_session_reference(&session_ref)?;
    if matches.is_empty() {
        println!("no indexed events for {session_ref}");
        return Ok(());
    }
    if matches.len() > 1 {
        let choices = matches
            .iter()
            .map(|session| {
                format!(
                    "  {}  {}  {}",
                    session.session_key,
                    session.cwd,
                    session.source_file_path.display()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        bail!("multiple indexed sessions match `{session_ref}`; use one session_key:\n{choices}");
    }

    let session = &matches[0];
    let events = store.session_events(&session.session_key, limit)?;
    if events.is_empty() {
        println!("no indexed events for {session_ref}");
        return Ok(());
    }

    println!("{}  {}", session.session_key, session.session_id);
    println!("{}", events[0].cwd);
    for event in events {
        println!(
            "\n{}  {}:{}",
            event.kind.as_str(),
            event.source_file_path.display(),
            event.source_line_number
        );
        println!("{}", preview(&event.text, 900));
    }

    Ok(())
}

fn run_index(args: Vec<String>) -> Result<()> {
    let mut db_path = default_db_path()?;
    let mut sources = Vec::new();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--source" => {
                index += 1;
                sources.push(required_path(&args, index, "--source")?);
            }
            flag => bail!("unknown index flag `{flag}`"),
        }
        index += 1;
    }

    if sources.is_empty() {
        sources = default_source_roots()?;
    }

    let store = Store::open(&db_path)?;
    let report = index_sources_with_progress(&store, &sources, |report| {
        eprintln!(
            "scanned {} files, indexed {} session files, skipped {}",
            report.files_seen, report.sessions_indexed, report.files_skipped
        );
    })?;
    println!(
        "indexed {} session files, {} events from {} files ({} skipped) into {}",
        report.sessions_indexed,
        report.events_indexed,
        report.files_seen,
        report.files_skipped,
        db_path.display()
    );
    Ok(())
}

fn run_rebuild(args: Vec<String>) -> Result<()> {
    let mut db_path = default_db_path()?;
    let mut sources = Vec::new();
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--source" => {
                index += 1;
                sources.push(required_path(&args, index, "--source")?);
            }
            flag => bail!("unknown rebuild flag `{flag}`"),
        }
        index += 1;
    }

    if sources.is_empty() {
        sources = default_source_roots()?;
    }

    remove_db_files(&db_path)?;
    let store = Store::open(&db_path)?;
    let report = index_sources_with_progress(&store, &sources, |report| {
        eprintln!(
            "scanned {} files, indexed {} session files, skipped {}",
            report.files_seen, report.sessions_indexed, report.files_skipped
        );
    })?;
    println!(
        "rebuilt {} session files, {} events from {} files into {}",
        report.sessions_indexed,
        report.events_indexed,
        report.files_seen,
        db_path.display()
    );
    Ok(())
}

fn run_search(args: Vec<String>) -> Result<()> {
    if args.is_empty() {
        bail!("search requires a query");
    }

    let query = args[0].clone();
    let mut db_path = default_db_path()?;
    let mut limit = 10usize;
    let mut json_output = false;
    let mut repo = None;
    let mut cwd = None;
    let mut since = None;
    let mut all_repos = false;
    let mut index = 1;

    while index < args.len() {
        match args[index].as_str() {
            "--json" => {
                json_output = true;
            }
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            "--limit" => {
                index += 1;
                let raw = args
                    .get(index)
                    .ok_or_else(|| anyhow!("--limit requires a value"))?;
                limit = raw
                    .parse()
                    .with_context(|| format!("parse --limit value `{raw}`"))?;
            }
            "--repo" => {
                index += 1;
                repo = Some(required_value(&args, index, "--repo")?.to_owned());
            }
            "--all-repos" => {
                all_repos = true;
            }
            "--cwd" => {
                index += 1;
                cwd = Some(required_value(&args, index, "--cwd")?.to_owned());
            }
            "--since" => {
                index += 1;
                since = Some(required_value(&args, index, "--since")?.to_owned());
            }
            flag => bail!("unknown search flag `{flag}`"),
        }
        index += 1;
    }

    let store = Store::open(&db_path)?;
    let current_repo = if repo.is_none() && !all_repos {
        detect_current_repo()
    } else {
        None
    };
    let search_limit = if json_output {
        limit
    } else {
        limit.saturating_mul(5)
    };
    let results = store.search_with_options(SearchOptions {
        query: query.clone(),
        limit: search_limit,
        repo,
        cwd,
        since,
        current_repo,
    })?;
    if json_output {
        print_search_json(&query, &results)?;
        return Ok(());
    }

    if results.is_empty() {
        println!("no matches");
        return Ok(());
    }

    print_grouped_search_results(&results, limit);

    Ok(())
}

fn run_doctor(args: Vec<String>) -> Result<()> {
    let mut db_path = default_db_path()?;
    let mut json_output = false;
    let mut index = 0;

    while index < args.len() {
        match args[index].as_str() {
            "--json" => json_output = true,
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            flag => bail!("unknown doctor flag `{flag}`"),
        }
        index += 1;
    }

    let db_existed = db_path.exists();
    let store = Store::open(&db_path)?;
    let stats = store.stats()?;
    let quick_check = match store.quick_check() {
        Ok(value) => value,
        Err(error) => format!("error: {error:#}"),
    };
    let fts_integrity = match store.fts_integrity_check() {
        Ok(()) => "ok".to_owned(),
        Err(error) => format!("error: {error:#}"),
    };
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
    let ok = quick_check == "ok" && fts_integrity == "ok";

    if json_output {
        let value = json!({
            "ok": ok,
            "db_path": db_path,
            "db_existed": db_existed,
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

fn run_stats(args: Vec<String>) -> Result<()> {
    let mut db_path = default_db_path()?;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--db" => {
                index += 1;
                db_path = required_path(&args, index, "--db")?;
            }
            flag => bail!("unknown stats flag `{flag}`"),
        }
        index += 1;
    }

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

fn required_path(args: &[String], index: usize, flag: &str) -> Result<PathBuf> {
    Ok(PathBuf::from(required_value(args, index, flag)?))
}

fn required_value<'a>(args: &'a [String], index: usize, flag: &str) -> Result<&'a str> {
    args.get(index)
        .map(String::as_str)
        .ok_or_else(|| anyhow!("{flag} requires a value"))
}

fn remove_db_files(db_path: &Path) -> Result<()> {
    for path in [
        db_path.to_path_buf(),
        PathBuf::from(format!("{}-wal", db_path.display())),
        PathBuf::from(format!("{}-shm", db_path.display())),
    ] {
        match fs::remove_file(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(error).with_context(|| format!("remove {}", path.display()));
            }
        }
    }
    Ok(())
}

fn detect_current_repo() -> Option<String> {
    let mut path = std::env::current_dir().ok()?;
    loop {
        if path.join(".git").exists() {
            return path
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned);
        }
        if !path.pop() {
            return None;
        }
    }
}

fn compact_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn print_grouped_search_results(results: &[SearchResult], limit: usize) {
    let mut session_order = Vec::<&str>::new();
    for result in results {
        if !session_order
            .iter()
            .any(|session_id| *session_id == result.session_key)
        {
            session_order.push(&result.session_key);
        }
        if session_order.len() == limit {
            break;
        }
    }

    for (index, session_id) in session_order.iter().enumerate() {
        let session_results = results
            .iter()
            .filter(|result| &result.session_key == session_id)
            .collect::<Vec<_>>();
        let first = session_results[0];
        println!(
            "{}. {}  {}  {}",
            index + 1,
            first.session_key,
            first.session_id,
            first.cwd
        );
        for result in session_results.into_iter().take(3) {
            println!(
                "   - {}  {}:{}",
                result.kind.as_str(),
                result.source_file_path.display(),
                result.source_line_number
            );
            println!("     {}", compact_whitespace(&result.snippet));
        }
    }
}

fn preview(value: &str, limit: usize) -> String {
    let compact = compact_whitespace(value);
    if compact.len() <= limit {
        return compact;
    }

    let mut output = compact
        .char_indices()
        .take_while(|(index, _)| *index < limit)
        .map(|(_, ch)| ch)
        .collect::<String>();
    output.push_str(" ...");
    output
}

fn print_search_json(query: &str, results: &[SearchResult]) -> Result<()> {
    let results = results
        .iter()
        .map(|result| {
            let source = format!(
                "{}:{}",
                result.source_file_path.display(),
                result.source_line_number
            );
            json!({
                "session_key": result.session_key,
                "session_id": result.session_id,
                "repo": result.repo,
                "kind": result.kind.as_str(),
                "cwd": result.cwd,
                "session_timestamp": result.session_timestamp,
                "source_file_path": result.source_file_path,
                "source_line_number": result.source_line_number,
                "source": source,
                "source_timestamp": result.source_timestamp,
                "score": result.score,
                "snippet": compact_whitespace(&result.snippet),
                "text_preview": preview(&result.text, 500),
            })
        })
        .collect::<Vec<_>>();

    let value = json!({
        "query": query,
        "count": results.len(),
        "results": results,
    });
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

fn print_help() {
    println!(
        "codex-recall\n\nCommands:\n  index [--db PATH] [--source PATH ...]\n  rebuild [--db PATH] [--source PATH ...]\n  search QUERY [--db PATH] [--limit N] [--repo NAME] [--all-repos] [--cwd PATH_PART] [--since DATE|Nd|today] [--json]\n  show SESSION_ID_OR_KEY [--db PATH] [--limit N]\n  doctor [--db PATH] [--json]\n  stats [--db PATH]"
    );
}

#[allow(dead_code)]
fn os_string_to_string(value: OsString) -> Result<String> {
    value
        .into_string()
        .map_err(|_| anyhow!("argument is not valid UTF-8"))
}
