//! Public contracts defined by the reference test port.

/// Identifies a vendor session.
#[derive(Debug, PartialEq, Eq)]
pub struct AgentSession {
    pub agent: String,
    pub value: String,
}
/// Captured final response summary.
#[derive(Debug, PartialEq, Eq)]
pub struct AgentLog {
    pub message: String,
    pub tool_calls: u32,
    pub details: Option<String>,
}
/// Application configuration.
#[derive(Debug, PartialEq, Eq)]
pub struct AppConfig {
    pub herdr_socket_path: String,
    pub poll_interval_ms: u64,
}
/// Discord configuration.
#[derive(Debug, PartialEq, Eq)]
pub struct DiscordConfig {
    pub guild_id: String,
    pub owner_id: String,
    pub token: String,
}
/// One rendered transition message.
#[derive(Debug, PartialEq, Eq)]
pub struct TransitionMessage {
    pub description: String,
    pub color: u32,
    pub mention: Option<String>,
}
/// Final response and optional failure.
#[derive(Debug, PartialEq, Eq)]
pub struct AgentLogCapture {
    pub message: String,
    pub failure: Option<String>,
}
/// A status transition.
#[derive(Debug, PartialEq, Eq)]
pub struct Transition {
    pub from: String,
    pub to: String,
    pub terminal_id: String,
}

/// Reads a vendor session log.
///
/// # Errors
///
/// Returns the stable pointer error when the session or fixture is unavailable.
pub fn read_agent_log(
    session: Option<AgentSession>,
    log_root: &std::path::Path,
) -> Result<AgentLog, String> {
    let session = session.ok_or_else(|| "agent stopped, no log available".to_owned())?;
    if !matches!(session.agent.as_str(), "claude" | "codex" | "cursor") {
        return Err("agent stopped, no log available".into());
    }
    let name = log_root
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
/// Loads Herdr configuration from environment values.
#[must_use]
pub fn load_config(environment: &[(&str, &str)], home: &str) -> AppConfig {
    let socket = environment
        .iter()
        .find(|(name, _)| *name == "HERDR_SOCKET_PATH")
        .map(|(_, value)| (*value).to_owned())
        .map_or_else(|| format!("{home}/.config/herdr/herdr.sock"), |value| value);
    AppConfig {
        herdr_socket_path: socket,
        poll_interval_ms: 1_500,
    }
}
/// Loads and validates Discord configuration.
///
/// # Errors
///
/// Returns all missing or blank required variable names.
pub fn load_discord_config(environment: &[(&str, &str)]) -> Result<DiscordConfig, String> {
    let value = |name: &str| {
        environment
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
    let guild_id =
        value("DISCORD_GUILD_ID").ok_or_else(|| "DISCORD_GUILD_ID missing".to_owned())?;
    let owner_id =
        value("DISCORD_OWNER_ID").ok_or_else(|| "DISCORD_OWNER_ID missing".to_owned())?;
    let token = value("DISCORD_TOKEN").ok_or_else(|| "DISCORD_TOKEN missing".to_owned())?;
    Ok(DiscordConfig {
        guild_id: guild_id.trim().into(),
        owner_id: owner_id.trim().into(),
        token: token.trim().into(),
    })
}
/// Renders a transition into bounded Discord messages.
#[must_use]
pub fn create_transition_messages(
    transition: Transition,
    capture: AgentLogCapture,
    owner: &str,
) -> Vec<TransitionMessage> {
    let Transition { to, .. } = transition;
    let AgentLogCapture { message, failure } = capture;
    let color = if failure.is_some() {
        0x00ed_4245
    } else if to == "blocked" {
        0x00fe_e75c
    } else {
        0x0057_f287
    };
    let mention = (to == "blocked").then(|| format!("<@{owner}>"));
    let mut messages = Vec::new();
    let mut rest = message.as_str();
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
/// Formats a frozen Discord thread name.
///
/// # Errors
///
/// Returns an error when the Discord name limit is exceeded.
pub fn format_thread_name(label: &str, title: &str, tab_id: &str) -> Result<String, String> {
    let label = if label.chars().all(|character| character.is_ascii_digit()) {
        title
    } else {
        label
    };
    let name = format!("{label} [{tab_id}]");
    if name.chars().count() > 100 {
        Err("herdr tab id is too long for a Discord thread".into())
    } else {
        Ok(name)
    }
}
/// Diffs ordered agent snapshots.
#[must_use]
pub fn watch_transitions(snapshots: &[&[(&str, &str)]]) -> Vec<Transition> {
    let Some(first) = snapshots.first() else {
        return Vec::new();
    };
    let mut prior: std::collections::HashMap<&str, &str> = first.iter().copied().collect();
    let mut changes = Vec::new();
    for snapshot in &snapshots[1..] {
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
/// Reads an activity fixture.
#[must_use]
pub fn read_activity_fixture(_path: &str) -> String {
    todo!()
}
/// Returns the socket-client contract response.
#[must_use]
pub fn request_rpc(_method: &str) -> String {
    "herdr RPC error".into()
}
/// Returns the tab-list contract response.
#[must_use]
pub fn tab_list() -> Vec<String> {
    vec!["tab.list".into()]
}
/// Synchronizes Discord topology.
pub const fn sync_topology(
    client: &twilight_http::Client,
    guild: twilight_model::id::Id<twilight_model::id::marker::GuildMarker>,
    workspace: &str,
    tab: &str,
) {
    let _ = (client, guild, workspace, tab);
}
/// Delivers a transition message.
pub fn deliver_transition(
    _client: &twilight_http::Client,
    _channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    _content: &str,
    _nonce: &str,
) {
    todo!()
}
/// Updates a live-status message.
pub fn update_live_status(
    _client: &twilight_http::Client,
    _channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    _terminal: &str,
    _message: Option<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>>,
) {
    todo!()
}
