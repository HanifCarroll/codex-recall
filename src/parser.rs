use crate::redact::redact_secrets;
use anyhow::{Context, Result};
use serde_json::Value;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;

const COMMAND_OUTPUT_LIMIT: usize = 4_000;
const COMMAND_OUTPUT_REDACTION_WINDOW: usize = COMMAND_OUTPUT_LIMIT + 512;
const MESSAGE_TEXT_LIMIT: usize = 20_000;
const MESSAGE_TEXT_REDACTION_WINDOW: usize = MESSAGE_TEXT_LIMIT + 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedSession {
    pub session: SessionMetadata,
    pub events: Vec<ParsedEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMetadata {
    pub id: String,
    pub timestamp: String,
    pub cwd: String,
    pub cli_version: Option<String>,
    pub source_file_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedEvent {
    pub session_id: String,
    pub kind: EventKind,
    pub role: Option<String>,
    pub text: String,
    pub command: Option<String>,
    pub cwd: Option<String>,
    pub exit_code: Option<i64>,
    pub source_timestamp: Option<String>,
    pub source_file_path: PathBuf,
    pub source_line_number: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    UserMessage,
    AssistantMessage,
    Command,
}

impl EventKind {
    pub fn as_str(self) -> &'static str {
        match self {
            EventKind::UserMessage => "user_message",
            EventKind::AssistantMessage => "assistant_message",
            EventKind::Command => "command",
        }
    }

    fn parse_kind(value: &str) -> Option<Self> {
        match value {
            "user_message" => Some(EventKind::UserMessage),
            "assistant_message" => Some(EventKind::AssistantMessage),
            "command" => Some(EventKind::Command),
            _ => None,
        }
    }
}

impl FromStr for EventKind {
    type Err = ();

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        Self::parse_kind(value).ok_or(())
    }
}

pub fn parse_session_file(path: &Path) -> Result<Option<ParsedSession>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut session = None;
    let mut pending_events = Vec::new();

    for (index, line) in reader.lines().enumerate() {
        let line_number = index + 1;
        let line = line.with_context(|| format!("read {}:{line_number}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }

        let record: Value = serde_json::from_str(&line)
            .with_context(|| format!("parse json {}:{line_number}", path.display()))?;
        let top_type = record
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let source_timestamp = record
            .get("timestamp")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let payload = record.get("payload").unwrap_or(&Value::Null);

        if top_type == "session_meta" {
            session = parse_session_meta(payload, path);
            continue;
        }

        if let Some(event) = parse_event(
            top_type,
            payload,
            path,
            line_number,
            source_timestamp.as_deref(),
        ) {
            pending_events.push(event);
        }
    }

    let Some(session) = session else {
        return Ok(None);
    };

    let mut seen = HashSet::new();
    let mut events = Vec::new();
    for mut event in pending_events {
        let dedupe_key = format!(
            "{}\u{1f}{}\u{1f}{}",
            event.kind.as_str(),
            event.role.as_deref().unwrap_or_default(),
            event.text
        );
        if seen.insert(dedupe_key) {
            event.session_id = session.id.clone();
            events.push(event);
        }
    }

    Ok(Some(ParsedSession { session, events }))
}

fn parse_session_meta(payload: &Value, path: &Path) -> Option<SessionMetadata> {
    let id = payload.get("id")?.as_str()?.to_owned();
    let timestamp = payload
        .get("timestamp")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let cwd = payload
        .get("cwd")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned();
    let cli_version = payload
        .get("cli_version")
        .and_then(Value::as_str)
        .map(str::to_owned);

    Some(SessionMetadata {
        id,
        timestamp,
        cwd,
        cli_version,
        source_file_path: path.to_path_buf(),
    })
}

fn parse_event(
    top_type: &str,
    payload: &Value,
    path: &Path,
    source_line_number: usize,
    source_timestamp: Option<&str>,
) -> Option<ParsedEvent> {
    let payload_type = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or_default();

    match (top_type, payload_type) {
        ("event_msg", "user_message") => {
            let text = payload.get("message").and_then(Value::as_str)?.trim();
            non_empty_text_event(
                EventKind::UserMessage,
                Some("user"),
                text,
                path,
                source_line_number,
                source_timestamp,
            )
        }
        ("event_msg", "agent_message") => {
            let text = payload.get("message").and_then(Value::as_str)?.trim();
            non_empty_text_event(
                EventKind::AssistantMessage,
                Some("assistant"),
                text,
                path,
                source_line_number,
                source_timestamp,
            )
        }
        ("event_msg", "exec_command_end") => {
            parse_command_event(payload, path, source_line_number, source_timestamp)
        }
        ("response_item", "message") => {
            let role = payload.get("role").and_then(Value::as_str)?;
            let kind = match role {
                "user" => EventKind::UserMessage,
                "assistant" => EventKind::AssistantMessage,
                _ => return None,
            };
            let text = extract_content_text(payload.get("content")?)?;
            non_empty_text_event(
                kind,
                Some(role),
                text.trim(),
                path,
                source_line_number,
                source_timestamp,
            )
        }
        _ => None,
    }
}

fn non_empty_text_event(
    kind: EventKind,
    role: Option<&str>,
    text: &str,
    path: &Path,
    source_line_number: usize,
    source_timestamp: Option<&str>,
) -> Option<ParsedEvent> {
    if text.is_empty() {
        return None;
    }
    if is_codex_preamble(text) {
        return None;
    }

    let capped_text = cap_text(text, MESSAGE_TEXT_REDACTION_WINDOW);
    let redacted_text = cap_text(&redact_secrets(&capped_text), MESSAGE_TEXT_LIMIT);

    Some(ParsedEvent {
        session_id: String::new(),
        kind,
        role: role.map(str::to_owned),
        text: redacted_text,
        command: None,
        cwd: None,
        exit_code: None,
        source_timestamp: source_timestamp.map(str::to_owned),
        source_file_path: path.to_path_buf(),
        source_line_number,
    })
}

fn is_codex_preamble(text: &str) -> bool {
    let trimmed = text.trim_start();
    trimmed.starts_with("# AGENTS.md instructions") || trimmed.contains("<environment_context>")
}

fn parse_command_event(
    payload: &Value,
    path: &Path,
    source_line_number: usize,
    source_timestamp: Option<&str>,
) -> Option<ParsedEvent> {
    let command = extract_command(payload.get("command")?)?;
    let command = command.trim();
    if command.is_empty() {
        return None;
    }
    let redacted_command = redact_secrets(command);

    let stdout = payload.get("stdout").and_then(Value::as_str).unwrap_or("");
    let stderr = payload.get("stderr").and_then(Value::as_str).unwrap_or("");
    let mut text = format!("$ {redacted_command}");
    let output = payload
        .get("aggregated_output")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .unwrap_or_else(|| join_command_output(stdout, stderr));
    if !output.is_empty() {
        text.push('\n');
        let capped_output = cap_text(&output, COMMAND_OUTPUT_REDACTION_WINDOW);
        text.push_str(&cap_text(
            &redact_secrets(&capped_output),
            COMMAND_OUTPUT_LIMIT,
        ));
    }

    Some(ParsedEvent {
        session_id: String::new(),
        kind: EventKind::Command,
        role: None,
        text,
        command: Some(redacted_command),
        cwd: payload
            .get("cwd")
            .and_then(Value::as_str)
            .map(str::to_owned),
        exit_code: payload.get("exit_code").and_then(Value::as_i64),
        source_timestamp: source_timestamp.map(str::to_owned),
        source_file_path: path.to_path_buf(),
        source_line_number,
    })
}

fn extract_command(value: &Value) -> Option<String> {
    if let Some(command) = value.as_str() {
        return Some(command.to_owned());
    }

    let argv = value.as_array()?;
    let args = argv.iter().filter_map(Value::as_str).collect::<Vec<_>>();
    if args.len() >= 3 && (args[1] == "-lc" || args[1] == "-c") {
        return Some(args[2].to_owned());
    }

    if args.is_empty() {
        None
    } else {
        Some(args.join(" "))
    }
}

fn extract_content_text(content: &Value) -> Option<String> {
    let parts = content.as_array()?;
    let text = parts
        .iter()
        .filter_map(|part| part.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");

    if text.trim().is_empty() {
        None
    } else {
        Some(text)
    }
}

fn join_command_output(stdout: &str, stderr: &str) -> String {
    match (stdout.trim().is_empty(), stderr.trim().is_empty()) {
        (true, true) => String::new(),
        (false, true) => stdout.to_owned(),
        (true, false) => stderr.to_owned(),
        (false, false) => format!("{stdout}\n{stderr}"),
    }
}

fn cap_text(text: &str, limit: usize) -> String {
    if text.len() <= limit {
        return text.to_owned();
    }

    let mut capped = text
        .char_indices()
        .take_while(|(index, _)| *index < limit)
        .map(|(_, ch)| ch)
        .collect::<String>();
    capped.push_str("\n[truncated]");
    capped
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn temp_jsonl(name: &str, contents: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "codex-recall-parser-test-{}-{}",
            std::process::id(),
            name
        ));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.jsonl");
        fs::write(&path, contents).unwrap();
        path
    }

    #[test]
    fn parses_session_metadata_and_high_signal_events() {
        let path = temp_jsonl(
            "basic",
            r#"{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{"id":"session-1","timestamp":"2026-04-13T01:00:00Z","cwd":"/Users/me/project","cli_version":"0.1.0"}}
{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"Find the Sentry issue","text_elements":[],"images":[],"local_images":[]}}
{"timestamp":"2026-04-13T01:00:02Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"I found the Sentry root cause."}]}}
{"timestamp":"2026-04-13T01:00:03Z","type":"event_msg","payload":{"type":"exec_command_end","command":"rg SENTRY","cwd":"/Users/me/project","exit_code":0,"stdout":"SENTRY_DSN=redacted\n","stderr":""}}
"#,
        );

        let parsed = parse_session_file(&path).unwrap().unwrap();

        assert_eq!(parsed.session.id, "session-1");
        assert_eq!(parsed.session.cwd, "/Users/me/project");
        assert_eq!(parsed.session.source_file_path, path);
        assert_eq!(parsed.events.len(), 3);
        assert_eq!(parsed.events[0].kind, EventKind::UserMessage);
        assert_eq!(parsed.events[0].text, "Find the Sentry issue");
        assert_eq!(parsed.events[0].source_line_number, 2);
        assert_eq!(parsed.events[1].kind, EventKind::AssistantMessage);
        assert_eq!(parsed.events[1].text, "I found the Sentry root cause.");
        assert_eq!(parsed.events[2].kind, EventKind::Command);
        assert_eq!(parsed.events[2].command.as_deref(), Some("rg SENTRY"));
        assert!(parsed.events[2].text.contains("rg SENTRY"));
        assert!(parsed.events[2].text.contains("SENTRY_DSN"));
    }

    #[test]
    fn skips_events_without_indexable_text() {
        let path = temp_jsonl(
            "noise",
            r#"{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{"id":"session-2","timestamp":"2026-04-13T01:00:00Z","cwd":"/tmp"}}
{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{"type":"token_count","info":{"total_token_usage":{"input_tokens":10}}}}
{"timestamp":"2026-04-13T01:00:02Z","type":"response_item","payload":{"type":"function_call","name":"exec_command","arguments":"{}","call_id":"call-1"}}
"#,
        );

        let parsed = parse_session_file(&path).unwrap().unwrap();

        assert_eq!(parsed.events.len(), 0);
    }

    #[test]
    fn removes_exact_duplicate_transcript_events() {
        let path = temp_jsonl(
            "duplicates",
            r#"{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{"id":"session-3","timestamp":"2026-04-13T01:00:00Z","cwd":"/tmp"}}
{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{"type":"agent_message","message":"Same assistant answer."}}
{"timestamp":"2026-04-13T01:00:01Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Same assistant answer."}]}}
"#,
        );

        let parsed = parse_session_file(&path).unwrap().unwrap();

        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].source_line_number, 2);
    }

    #[test]
    fn skips_codex_instruction_preamble_messages() {
        let path = temp_jsonl(
            "preamble",
            r##"{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{"id":"session-4","timestamp":"2026-04-13T01:00:00Z","cwd":"/tmp"}}
{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"# AGENTS.md instructions for /tmp\n\n<environment_context>\n  <cwd>/tmp</cwd>\n</environment_context>"}}
{"timestamp":"2026-04-13T01:00:02Z","type":"event_msg","payload":{"type":"user_message","message":"What did we decide about Sentry?"}}
"##,
        );

        let parsed = parse_session_file(&path).unwrap().unwrap();

        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].text, "What did we decide about Sentry?");
    }

    #[test]
    fn parses_exec_command_end_with_argv_and_aggregated_output() {
        let path = temp_jsonl(
            "argv-command",
            r#"{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{"id":"session-5","timestamp":"2026-04-13T01:00:00Z","cwd":"/Users/me/hanif-md"}}
{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{"type":"exec_command_end","command":["/bin/zsh","-lc","cargo test"],"cwd":"/Users/me/projects/codex-recall","exit_code":0,"aggregated_output":"test result: ok"}}
"#,
        );

        let parsed = parse_session_file(&path).unwrap().unwrap();

        assert_eq!(parsed.events.len(), 1);
        assert_eq!(parsed.events[0].kind, EventKind::Command);
        assert_eq!(parsed.events[0].command.as_deref(), Some("cargo test"));
        assert_eq!(
            parsed.events[0].cwd.as_deref(),
            Some("/Users/me/projects/codex-recall")
        );
        assert!(parsed.events[0].text.contains("test result: ok"));
    }

    #[test]
    fn redacts_secrets_before_events_are_indexed() {
        let path = temp_jsonl(
            "redaction",
            r#"{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{"id":"session-6","timestamp":"2026-04-13T01:00:00Z","cwd":"/Users/me/project"}}
{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{"type":"user_message","message":"Use API_KEY=abc123456789 and Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ"}}
{"timestamp":"2026-04-13T01:00:02Z","type":"event_msg","payload":{"type":"exec_command_end","command":"curl -H 'Authorization: Bearer supersecrettoken123456' https://example.com","cwd":"/Users/me/project","exit_code":0,"stdout":"github_pat_1234567890abcdefghijklmnop\n","stderr":""}}
"#,
        );

        let parsed = parse_session_file(&path).unwrap().unwrap();

        assert_eq!(parsed.events.len(), 2);
        assert!(parsed.events[0].text.contains("API_KEY=[REDACTED]"));
        assert!(parsed.events[0]
            .text
            .contains("Authorization: Bearer [REDACTED]"));
        assert!(parsed.events[1]
            .command
            .as_deref()
            .unwrap()
            .contains("Authorization: Bearer [REDACTED]"));
        assert!(parsed.events[1].text.contains("[REDACTED]"));
        assert!(!parsed.events.iter().any(|event| {
            event.text.contains("abc123456789")
                || event.text.contains("supersecrettoken123456")
                || event.text.contains("github_pat_1234567890")
        }));
    }

    #[test]
    fn caps_large_message_events_before_indexing() {
        let long_text = "alpha ".repeat(10_000);
        let escaped = serde_json::to_string(&long_text).unwrap();
        let path = temp_jsonl(
            "large-message",
            &format!(
                r#"{{"timestamp":"2026-04-13T01:00:00Z","type":"session_meta","payload":{{"id":"session-7","timestamp":"2026-04-13T01:00:00Z","cwd":"/Users/me/project"}}}}
{{"timestamp":"2026-04-13T01:00:01Z","type":"event_msg","payload":{{"type":"user_message","message":{escaped}}}}}
"#
            ),
        );

        let parsed = parse_session_file(&path).unwrap().unwrap();

        assert_eq!(parsed.events.len(), 1);
        assert!(parsed.events[0].text.len() <= MESSAGE_TEXT_LIMIT + "[truncated]".len() + 1);
        assert!(parsed.events[0].text.contains("[truncated]"));
    }
}
