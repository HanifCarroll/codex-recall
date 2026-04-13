use crate::config::{default_db_path, default_source_roots};
use crate::indexer::index_sources;
use crate::store::{SearchResult, Store};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::json;
use std::ffi::OsString;
use std::path::PathBuf;

pub fn run(args: impl IntoIterator<Item = String>) -> Result<()> {
    let mut args = args.into_iter();
    let Some(command) = args.next() else {
        print_help();
        return Ok(());
    };

    match command.as_str() {
        "index" => run_index(args.collect()),
        "search" => run_search(args.collect()),
        "show" => run_show(args.collect()),
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

    let session_id = args[0].clone();
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
    let events = store.session_events(&session_id, limit)?;
    if events.is_empty() {
        println!("no indexed events for {session_id}");
        return Ok(());
    }

    println!("{session_id}");
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
    let report = index_sources(&store, &sources)?;
    println!(
        "indexed {} sessions, {} events from {} files ({} skipped) into {}",
        report.sessions_indexed,
        report.events_indexed,
        report.files_seen,
        report.files_skipped,
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
            flag => bail!("unknown search flag `{flag}`"),
        }
        index += 1;
    }

    let store = Store::open(&db_path)?;
    let search_limit = if json_output {
        limit
    } else {
        limit.saturating_mul(5)
    };
    let results = store.search(&query, search_limit)?;
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
        "{} sessions, {} events",
        stats.session_count, stats.event_count
    );
    Ok(())
}

fn required_path(args: &[String], index: usize, flag: &str) -> Result<PathBuf> {
    let value = args
        .get(index)
        .ok_or_else(|| anyhow!("{flag} requires a path"))?;
    Ok(PathBuf::from(value))
}

fn compact_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn print_grouped_search_results(results: &[SearchResult], limit: usize) {
    let mut session_order = Vec::<&str>::new();
    for result in results {
        if !session_order
            .iter()
            .any(|session_id| *session_id == result.session_id)
        {
            session_order.push(&result.session_id);
        }
        if session_order.len() == limit {
            break;
        }
    }

    for (index, session_id) in session_order.iter().enumerate() {
        let session_results = results
            .iter()
            .filter(|result| &result.session_id == session_id)
            .collect::<Vec<_>>();
        let first = session_results[0];
        println!("{}. {}  {}", index + 1, first.session_id, first.cwd);
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
                "session_id": result.session_id,
                "kind": result.kind.as_str(),
                "cwd": result.cwd,
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
        "codex-recall\n\nCommands:\n  index [--db PATH] [--source PATH ...]\n  search QUERY [--db PATH] [--limit N] [--json]\n  show SESSION_ID [--db PATH] [--limit N]\n  stats [--db PATH]"
    );
}

#[allow(dead_code)]
fn os_string_to_string(value: OsString) -> Result<String> {
    value
        .into_string()
        .map_err(|_| anyhow!("argument is not valid UTF-8"))
}
