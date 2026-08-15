#[ctor::ctor]
fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

mod activity;
mod cards;
mod config;
mod delivery;
mod gateway;
mod herdr;
mod live_status;
mod prompting;
mod readers;
mod topology;
mod watcher;

pub use activity::read_activity_fixture;
pub use cards::{
    AgentLogCapture, TransitionMessage, create_transition_messages, format_thread_name,
};
pub use config::{AppConfig, DiscordConfig, load_config, load_discord_config};
pub use delivery::{deliver_transition, deliver_transition_card};
pub use gateway::{drive_gateway, drive_gateway_with_owner_prompt};
pub use herdr::{
    AgentSession, AgentSnapshot, HerdrTab, agent_prompt, list_agents, request_rpc,
    request_rpc_result, request_rpc_result_with_params, request_rpc_with_params, tab_list,
    tab_list_result,
};
pub use live_status::update_live_status;
pub use readers::{AgentLog, read_agent_log};
pub use topology::{TopologyRoute, route_topology, sync_topology, workspace_channel_name};
pub use watcher::{Transition, is_postable_transition, watch_transitions};
