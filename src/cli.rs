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
        "stats" => run_stats(args.collect()),
        "help" | "--help" | "-h" => {
            print_help();
            Ok(())
        }
        _ => bail!("unknown command `{command}`"),
    }
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
        "indexed {} sessions, {} events from {} files into {}",
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
    let results = store.search(&query, limit)?;
    if json_output {
        print_search_json(&query, &results)?;
        return Ok(());
    }

    if results.is_empty() {
        println!("no matches");
        return Ok(());
    }

    for (index, result) in results.iter().enumerate() {
        println!(
            "{}. {}  {}  {}",
            index + 1,
            result.session_id,
            result.kind.as_str(),
            result.cwd
        );
        println!(
            "   {}:{}",
            result.source_file_path.display(),
            result.source_line_number
        );
        println!("   {}", compact_whitespace(&result.snippet));
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
                "text": result.text,
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
        "codex-recall\n\nCommands:\n  index [--db PATH] [--source PATH ...]\n  search QUERY [--db PATH] [--limit N] [--json]\n  stats [--db PATH]"
    );
}

#[allow(dead_code)]
fn os_string_to_string(value: OsString) -> Result<String> {
    value
        .into_string()
        .map_err(|_| anyhow!("argument is not valid UTF-8"))
}
