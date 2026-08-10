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
pub struct Transition {
    pub from: String,
    pub to: String,
    pub terminal_id: String,
}

pub fn read_agent_log(_session: Option<AgentSession>) -> Result<AgentLog, String> {
    todo!()
}
pub fn load_config(_environment: &[(&str, &str)], _home: &str) -> AppConfig {
    todo!()
}
pub fn load_discord_config(_environment: &[(&str, &str)]) -> Result<DiscordConfig, String> {
    todo!()
}
pub fn create_transition_messages(
    _transition: Transition,
    _message: &str,
    _tools: u32,
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
    todo!()
}
pub fn tab_list() -> Vec<String> {
    todo!()
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
