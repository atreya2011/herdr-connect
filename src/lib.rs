#[ctor::ctor]
fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

mod broker;
mod cards;
mod config;
mod delivery;
mod gateway;
mod herdr;
mod permission;
mod prompting;
mod readers;
mod registry;
mod topology;
mod watcher;

pub use broker::{
    BrokerResponse, CorrelationError, PermissionResponder, correlate_decision, handle_component,
    request_decision, run_broker, serve_broker,
};
pub use cards::{
    AgentLogCapture, TransitionMessage, create_transition_messages, format_thread_name,
};
pub use config::{AppConfig, DiscordConfig, load_config, load_discord_config};
pub use delivery::{
    deliver_permission_card, deliver_transition_card, expire_permission_card, transition_card_nonce,
};
pub use gateway::{
    ComponentHandler, drive_gateway_with_components, drive_gateway_with_owner_prompt,
};
pub use herdr::{
    AgentSession, AgentSnapshot, HerdrTab, agent_prompt, list_agents, request_rpc_result,
    tab_list_result,
};
pub use permission::{
    ClaudePermissionRequest, ClaudePermissionToolInput, Decision, DecisionBehavior, Interaction,
    decode_claude_permission_request, encode_claude_decision,
};
pub use prompting::should_handle_owner_message;
pub use readers::{AgentLog, read_agent_log};
pub use registry::{ApprovalRequest, InteractionRegistry, IssuedApproval, ResolveError};
pub use topology::{TopologyRoute, route_topology, sync_topology, workspace_channel_name};
pub use watcher::{Transition, is_postable_transition};
