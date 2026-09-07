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
    AgentLogCapture, ThreadNameError, TransitionMessage, create_transition_messages,
    create_unsupported_blocked_card, format_thread_name, split_live_message,
};
pub use config::{
    DiscordConfig, ENV_DISCORD_GUILD_ID, ENV_DISCORD_OWNER_ID, ENV_DISCORD_TOKEN, ENV_HOME,
    load_discord_config,
};
pub use delivery::{
    UNKNOWN_CHANNEL_DELIVERY_ERROR, deliver_live_message, deliver_permission_card,
    deliver_transition_card, expire_informational_card, live_message_nonce, transition_card_nonce,
};
pub use gateway::{ComponentHandler, drive_gateway_with_components};
pub use herdr::{
    AgentSession, AgentSnapshot, EVENT_KEY, HerdrSubscription, HerdrTab, HerdrWorkspace,
    STATUS_BLOCKED, STATUS_DONE, STATUS_IDLE, STATUS_WORKING, SubscribeError, agent_read_detection,
    lifecycle_subscriptions, list_agents, request_rpc_result, status_subscriptions,
    subscribe_herdr_events, tab_list_result, workspace_list_result,
};
pub use permission::{
    ClaudePermissionRequest, ClaudePermissionToolInput, Decision, DecisionBehavior, Interaction,
    PermissionVendor, VENDOR_CLAUDE, VENDOR_CODEX, VENDOR_CURSOR, decode_claude_permission_request,
    decode_codex_permission_request, decode_cursor_permission_request, encode_claude_decision,
    encode_codex_decision, encode_cursor_decision,
};
pub use prompting::{
    maintain_typing_until_settled, should_handle_owner_message, submit_owner_prompt,
};
pub use readers::{
    AgentLog, claude_turn_start_position, format_detection_question, read_agent_log,
    read_claude_incremental,
};
pub use topology::{
    RouteError, TopologyCache, TopologyRoute, archived_threads, cached_route, delete_tab_thread,
    delete_topology_absent_from_herdr, delete_workspace_channel, fetch_topology_lists,
    reconcile_topology_cache, route_topology, sync_topology, workspace_channel_name,
};
pub use watcher::{Transition, is_postable_transition};
