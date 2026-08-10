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
    todo!()
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
    todo!()
}
pub fn format_thread_name(_label: &str, _title: &str, _tab_id: &str) -> Result<String, String> {
    todo!()
}
pub fn watch_transitions(_snapshots: &[&[(&str, &str)]]) -> Vec<Transition> {
    todo!()
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
