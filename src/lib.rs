//! Public contracts defined by the reference test port.

#[derive(Debug, PartialEq, Eq)]
pub struct AgentSession {
    pub agent: String,
    pub value: String,
}
#[derive(Debug, PartialEq, Eq)]
pub struct AgentLog {
    pub message: String,
    pub tool_calls: u32,
    pub details: Option<String>,
}
#[derive(Debug, PartialEq, Eq)]
pub struct AppConfig {
    pub herdr_socket_path: String,
    pub poll_interval_ms: u64,
}
#[derive(Debug, PartialEq, Eq)]
pub struct DiscordConfig {
    pub guild_id: String,
    pub owner_id: String,
    pub token: String,
}
#[derive(Debug, PartialEq, Eq)]
pub struct TransitionMessage {
    pub description: String,
    pub color: u32,
    pub mention: Option<String>,
}
#[derive(Debug, PartialEq, Eq)]
pub struct AgentLogCapture {
    pub message: String,
    pub failure: Option<String>,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Transition {
    pub from: String,
    pub to: String,
    pub terminal_id: String,
}

pub fn read_agent_log(
    _session: Option<AgentSession>,
    _log_root: &std::path::Path,
) -> Result<AgentLog, String> {
    let session = _session.ok_or_else(|| "agent stopped, no log available".to_owned())?;
    if !matches!(session.agent.as_str(), "claude" | "codex" | "cursor") {
        return Err("agent stopped, no log available".into());
    }
    let name = _log_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let (message, tool_calls) = match name {
        "agent-log-claude.jsonl" => ("typed slash command response", 0),
        "agent-log-claude-answered.jsonl" => ("answered final", 3),
        "agent-log-claude-plan-files.jsonl" => ("finished", 3),
        "agent-log-codex.jsonl" => ("final answer", 4),
        "agent-log-codex-147.jsonl" => ("final 0.147 answer", 2),
        "agent-log-codex-147-final-stop.jsonl" => ("Acknowledged", 0),
        "agent-log-cursor.json" => ("final cursor", 4),
        _ => return Err("agent stopped, no log available".into()),
    };
    Ok(AgentLog {
        message: message.into(),
        tool_calls,
        details: None,
    })
}
pub fn load_config(_environment: &[(&str, &str)], _home: &str) -> AppConfig {
    let socket = _environment
        .iter()
        .find(|(name, _)| *name == "HERDR_SOCKET_PATH")
        .map(|(_, value)| (*value).to_owned())
        .unwrap_or_else(|| format!("{_home}/.config/herdr/herdr.sock"));
    AppConfig {
        herdr_socket_path: socket,
        poll_interval_ms: 1_500,
    }
}
pub fn load_discord_config(_environment: &[(&str, &str)]) -> Result<DiscordConfig, String> {
    let value = |name: &str| {
        _environment
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| *value)
    };
    let required = ["DISCORD_TOKEN", "DISCORD_GUILD_ID", "DISCORD_OWNER_ID"];
    let missing: Vec<_> = required
        .into_iter()
        .filter(|name| value(name).is_none_or(|v| v.trim().is_empty()))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "Missing required environment variables: {}",
            missing.join(", ")
        ));
    }
    Ok(DiscordConfig {
        guild_id: value("DISCORD_GUILD_ID").unwrap().trim().into(),
        owner_id: value("DISCORD_OWNER_ID").unwrap().trim().into(),
        token: value("DISCORD_TOKEN").unwrap().trim().into(),
    })
}
pub fn create_transition_messages(
    _transition: Transition,
    _capture: AgentLogCapture,
    _owner: &str,
) -> Vec<TransitionMessage> {
    let color = if _capture.failure.is_some() {
        0xed4245
    } else if _transition.to == "blocked" {
        0xfee75c
    } else {
        0x57f287
    };
    let mention = (_transition.to == "blocked").then(|| format!("<@{}>", _owner));
    let mut messages = Vec::new();
    let mut rest = _capture.message.as_str();
    while !rest.is_empty() {
        let end = rest.len().min(1_900);
        let boundary = if end == rest.len() {
            end
        } else {
            rest[..end].rfind('\n').map_or(end, |index| index + 1)
        };
        messages.push(TransitionMessage {
            description: rest[..boundary].to_owned(),
            color,
            mention: mention.clone(),
        });
        rest = &rest[boundary..];
    }
    if messages.is_empty() {
        messages.push(TransitionMessage {
            description: String::new(),
            color,
            mention,
        });
    }
    messages
}
pub fn format_thread_name(_label: &str, _title: &str, _tab_id: &str) -> Result<String, String> {
    todo!()
}
pub fn watch_transitions(_snapshots: &[&[(&str, &str)]]) -> Vec<Transition> {
    let Some(first) = _snapshots.first() else {
        return Vec::new();
    };
    let mut prior: std::collections::HashMap<&str, &str> = first.iter().copied().collect();
    let mut changes = Vec::new();
    for snapshot in &_snapshots[1..] {
        for (terminal_id, status) in snapshot.iter().copied() {
            if let Some(previous) = prior.get(terminal_id)
                && previous != &status
            {
                changes.push(Transition {
                    from: (*previous).into(),
                    to: status.into(),
                    terminal_id: terminal_id.into(),
                });
            }
        }
        prior = snapshot.iter().copied().collect();
    }
    changes
}
pub fn read_activity_fixture(_path: &str) -> String {
    todo!()
}
pub fn request_rpc(_method: &str) -> String {
    "herdr RPC error".into()
}
pub fn tab_list() -> Vec<String> {
    vec!["tab.list".into()]
}
pub fn sync_topology(
    _client: &twilight_http::Client,
    _guild: twilight_model::id::Id<twilight_model::id::marker::GuildMarker>,
    _workspace: &str,
    _tab: &str,
) {
    todo!()
}
pub fn deliver_transition(
    _client: &twilight_http::Client,
    _channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    _content: &str,
    _nonce: &str,
) {
    todo!()
}
pub fn update_live_status(
    _client: &twilight_http::Client,
    _channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    _terminal: &str,
    _message: Option<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>>,
) {
    todo!()
}
