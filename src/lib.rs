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
    PermissionResponder, handle_component, hook_timeout, request_decision, run_broker,
};
pub use cards::{
    AgentLogCapture, TransitionMessage, create_transition_messages,
    create_unsupported_blocked_card, format_thread_name,
};
pub use config::{DiscordConfig, load_discord_config};
pub use delivery::{
    deliver_permission_card, deliver_transition_card, expire_informational_card,
    transition_card_nonce,
};
pub use gateway::{ComponentHandler, drive_gateway_with_components};
pub use herdr::{
    AgentSession, AgentSnapshot, HerdrSubscription, HerdrTab, SubscribeError,
    lifecycle_subscriptions, list_agents, request_rpc_result, status_subscriptions,
    subscribe_herdr_events, tab_list_result,
};
pub use permission::{
    ClaudePermissionRequest, ClaudePermissionToolInput, Decision, DecisionBehavior, Interaction,
    PermissionVendor, decode_claude_permission_request, decode_codex_permission_request,
    decode_cursor_permission_request, encode_claude_decision, encode_codex_decision,
    encode_cursor_decision,
};
pub use prompting::{
    maintain_typing_until_settled, should_handle_owner_message, submit_owner_prompt,
};
pub use readers::{AgentLog, read_agent_log};
pub use topology::{TopologyRoute, route_topology, sync_topology, workspace_channel_name};
pub use watcher::{Transition, is_postable_transition};
