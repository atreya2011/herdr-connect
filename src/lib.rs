#[ctor::ctor]
fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

mod activity;
mod cards;
mod config;
mod delivery;
mod herdr;
mod live_status;
mod readers;
mod topology;
mod watcher;

pub use activity::read_activity_fixture;
pub use cards::{
    AgentLogCapture, TransitionMessage, create_transition_messages, format_thread_name,
};
pub use config::{AppConfig, DiscordConfig, load_config, load_discord_config};
pub use delivery::{deliver_transition, deliver_transition_card};
pub use herdr::{
    AgentSession, AgentSnapshot, list_agents, request_rpc, request_rpc_result, tab_list,
};
pub use live_status::update_live_status;
pub use readers::{AgentLog, read_agent_log};
pub use topology::sync_topology;
pub use watcher::{Transition, is_postable_transition, watch_transitions};
