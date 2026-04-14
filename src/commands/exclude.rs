use anyhow::{bail, Result};

pub fn resolve_excluded_sessions(
    mut excluded_sessions: Vec<String>,
    exclude_current: bool,
) -> Result<Vec<String>> {
    if !exclude_current {
        return Ok(excluded_sessions);
    }

    let Some(current_session) = current_session_id() else {
        bail!("--exclude-current requires CODEX_SESSION_ID or CODEX_THREAD_ID in the environment");
    };
    excluded_sessions.push(current_session);
    Ok(excluded_sessions)
}

fn current_session_id() -> Option<String> {
    ["CODEX_SESSION_ID", "CODEX_THREAD_ID"]
        .into_iter()
        .find_map(|name| {
            let value = std::env::var(name).ok()?;
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_owned())
        })
}
