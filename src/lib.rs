#[ctor::ctor]
fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

mod activity;
mod broker;
mod cards;
mod config;
mod deletion;
mod delivery;
mod gateway;
mod herdr;
mod permission;
mod prompting;
mod question;
mod readers;
mod registry;
mod topology;
mod watcher;

pub use activity::{
    ACTIVITY_KIND, ActivityFrame, ClaudeActivityRequest, activity_message_text,
    decode_claude_activity_request, decode_codex_activity_request, decode_cursor_activity_request,
};
pub use broker::{
    PermissionResponder, handle_component, hook_timeout, question_hook_timeout, request_decision,
    request_question_answers, run_broker, send_activity_frame,
};
pub use cards::{
    AgentLogCapture, TransitionMessage, create_transition_messages, format_thread_name,
    split_live_message,
};
pub use config::{
    DiscordConfig, ENV_DISCORD_GUILD_ID, ENV_DISCORD_OWNER_ID, ENV_DISCORD_TOKEN, ENV_HOME,
    load_discord_config,
};
pub use deletion::{GuildDeletion, handle_guild_deletion};
pub use delivery::{
    OwnerIdentity, UNKNOWN_CHANNEL_DELIVERY_ERROR, UNKNOWN_WEBHOOK_DELIVERY_ERROR,
    deliver_activity_message, deliver_live_message, deliver_permission_card,
    deliver_question_button_card, deliver_question_select_card, deliver_transition_card,
    execute_terminal_prompt_webhook, expire_informational_card, expire_question_button_card,
    expire_question_select_card, fetch_owner_identity, live_message_nonce,
    resolve_terminal_prompt_webhook, transition_card_nonce, update_activity_message,
};
pub use gateway::{ComponentHandler, GatewayContext, drive_gateway_with_components};
pub use herdr::{
    AgentSession, AgentSnapshot, EVENT_KEY, HerdrSubscription, HerdrTab, HerdrWorkspace,
    STATUS_BLOCKED, STATUS_DONE, STATUS_IDLE, STATUS_WORKING, agent_read_detection,
    generated_tab_name, is_numeric_label, lifecycle_subscriptions, list_agents,
    name_unlabeled_tabs, request_rpc_result, status_subscriptions, subscribe_herdr_events,
    tab_close, tab_list_result, workspace_close, workspace_list_result,
};
pub use permission::{
    ClaudePermissionRequest, ClaudePermissionToolInput, Decision, DecisionBehavior, Interaction,
    PermissionVendor, VENDOR_CLAUDE, VENDOR_CODEX, VENDOR_CURSOR, cursor_argv_forces_allow,
    decode_claude_permission_request, decode_codex_permission_request,
    decode_cursor_permission_request, encode_claude_decision, encode_codex_decision,
    encode_cursor_decision, is_cursor_agent_argv,
};
pub use prompting::{
    maintain_typing_until_settled, should_handle_owner_message, submit_owner_prompt,
    take_owner_prompt_suppression,
};
pub use question::{
    ASK_QUESTION_TOOL, QUESTION_KIND, Question, QuestionAnswer, QuestionInteraction,
    QuestionOption, decode_claude_ask_question, encode_claude_question_decision,
};
pub use readers::{
    AgentLog, format_detection_question, read_agent_log, read_claude_incremental,
    read_claude_prompts_incremental, read_codex_incremental, read_codex_prompts_incremental,
    read_cursor_incremental, read_cursor_prompts_incremental,
};
pub use topology::{
    TopologyCache, TopologyRoute, archived_threads, cached_route, delete_tab_thread,
    delete_thread_created_message, delete_topology_absent_from_herdr, delete_workspace_channel,
    fetch_topology_lists, forget_owned, owned_thread_parent, reconcile_topology_cache,
    register_archived_tab_threads, remember_tab_threads, remember_workspace_channels,
    resolve_owner_deleted_tab, resolve_owner_deleted_workspace, route_topology, sync_topology,
    take_self_deletion, workspace_channel_id, workspace_channel_name,
};
pub use watcher::{Transition, is_postable_transition};

/// UTC wall-clock timestamp for one bridge log line, so a later occurrence in the same log can be
/// lined up against session-log times.
#[must_use]
pub fn log_timestamp() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|error| format!("<timestamp unavailable: {error}>"))
}

/// Prefixes a bridge stdout line with [`log_timestamp`].
///
/// Use in place of `println!` for every line the bridge process emits, so `bridge_eprintln!` and
/// this macro are the only sources of bridge log output.
#[macro_export]
macro_rules! bridge_println {
    ($($arg:tt)*) => {
        println!("{} {}", $crate::log_timestamp(), format!($($arg)*))
    };
}

/// Prefixes a bridge stderr line with [`log_timestamp`]. Use in place of `eprintln!` for every line
/// the bridge process emits.
#[macro_export]
macro_rules! bridge_eprintln {
    ($($arg:tt)*) => {
        eprintln!("{} {}", $crate::log_timestamp(), format!($($arg)*))
    };
}

#[cfg(test)]
mod log_timestamp_tests {
    use super::log_timestamp;

    #[test]
    fn log_timestamp_is_rfc3339_utc() {
        let stamp = log_timestamp();
        assert!(
            stamp.ends_with('Z'),
            "expected an RFC 3339 UTC ('Z') timestamp, got {stamp}"
        );
        let mut parts = stamp.splitn(2, 'T');
        let date = parts.next().expect("date component");
        assert_eq!(date.len(), 10, "expected YYYY-MM-DD, got {date}");
        assert_eq!(date.as_bytes()[4], b'-');
        assert_eq!(date.as_bytes()[7], b'-');
    }
}
