#[ctor::ctor]
fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

mod activity;
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

pub use activity::{
    ACTIVITY_KIND, ActivityFrame, ClaudeActivityRequest, activity_message_text,
    decode_claude_activity_request, decode_codex_activity_request, decode_cursor_activity_request,
};
pub use broker::{
    PermissionResponder, handle_component, hook_timeout, request_decision, run_broker,
    send_activity_frame,
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
    OwnerIdentity, UNKNOWN_CHANNEL_DELIVERY_ERROR, deliver_activity_message, deliver_live_message,
    deliver_permission_card, deliver_terminal_prompt, deliver_transition_card,
    execute_terminal_prompt_webhook, expire_informational_card, fetch_owner_identity,
    live_message_nonce, resolve_terminal_prompt_webhook, transition_card_nonce,
    update_activity_message,
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
    AgentLog, claude_turn_start_position, codex_turn_start_position, cursor_turn_start_rowid,
    format_detection_question, read_agent_log, read_claude_incremental,
    read_claude_prompts_incremental, read_codex_incremental, read_codex_prompts_incremental,
    read_cursor_incremental, read_cursor_prompts_incremental,
};
pub use topology::{
    RouteError, TopologyCache, TopologyRoute, archived_threads, cached_route, delete_tab_thread,
    delete_topology_absent_from_herdr, delete_workspace_channel, fetch_topology_lists,
    reconcile_topology_cache, route_topology, sync_topology, workspace_channel_name,
};
pub use watcher::{Transition, is_postable_transition};
