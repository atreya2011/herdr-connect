use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use twilight_model::channel::message::Embed;

use crate::cards::TransitionMessage;
use crate::permission::PermissionVendor;
use crate::question::{Question, QuestionOption};
use crate::topology::{is_unknown_channel_error, is_unknown_webhook_error};

/// Prefixes a delivery error whose target channel or thread no longer exists on Discord.
///
/// Lets a caller holding a cached route tell "the send failed" from "the cached route is stale"
/// and refetch instead of retrying the same, permanently-invalid target.
pub const UNKNOWN_CHANNEL_DELIVERY_ERROR: &str = "discord unknown channel";

/// Prefixes a delivery error whose target webhook no longer exists on Discord.
///
/// Its channel was deleted, which deletes its webhooks with it, independently of the channel
/// itself being recreated or still present under a stale cached id. Lets a caller holding a cached
/// webhook tell "the send failed" from "the cached webhook is stale" and re-resolve instead of
/// retrying the same, permanently-invalid webhook.
pub const UNKNOWN_WEBHOOK_DELIVERY_ERROR: &str = "discord unknown webhook";

const MAX_DISCORD_NONCE_LENGTH: usize = 25;
const MAX_PERMISSION_DESCRIPTION_LENGTH: usize = 3_800;
/// Discord's select-option description and select-option label length limits
/// (`twilight-validate`'s `SELECT_OPTION_DESCRIPTION_LENGTH`/label bound); `payload_json` bypasses
/// twilight's own validation, so this crate enforces them itself before a real question can exceed
/// them and silently fail to post its card.
const SELECT_OPTION_TEXT_LIMIT: usize = 100;
/// Discord's button label length limit (`twilight-validate`'s `COMPONENT_BUTTON_LABEL_LENGTH`).
const BUTTON_LABEL_LIMIT: usize = 80;
/// Discord's message content length limit (`twilight-validate`'s `MESSAGE_CONTENT_LENGTH_MAX`): a
/// free-text answer can run well past it, and an oversized `update_message` is rejected outright,
/// leaving the card's buttons enabled and unresolved.
pub const MAX_QUESTION_CARD_CONTENT_LENGTH: usize = 2_000;
const STARTUP_COMPONENT_MASK: u64 = (1_u64 << 44) - 1;
const PAYLOAD_COMPONENT_MASK: u64 = (1_u64 << 52) - 1;
const COMPONENT_TYPE_KEY: &str = "type";
const PAYLOAD_CONTENT_KEY: &str = "content";
const PAYLOAD_EMBEDS_KEY: &str = "embeds";
const PAYLOAD_COMPONENTS_KEY: &str = "components";
const ALLOWED_MENTIONS_KEY: &str = "allowed_mentions";
const ALLOWED_MENTIONS_PARSE_KEY: &str = "parse";
static PROCESS_START_COMPONENT: OnceLock<u64> = OnceLock::new();

/// Derives one process-stable nonce for a transition card delivery.
#[must_use]
pub fn transition_card_nonce(
    terminal_id: &str,
    state_change_seq: u64,
    card_index: usize,
) -> String {
    transition_card_nonce_for_start(
        process_start_component(),
        terminal_id,
        state_change_seq,
        card_index,
    )
}

/// Derived from the terminal id, log position, and part index, never from when the process
/// started.
///
/// A watch that reattaches after a restart and resumes at the same position reproduces the same
/// nonce. Discord's nonce dedupe lasts only a few minutes, so a restart within that window has its
/// repost of that text suppressed by `enforce_nonce`; a restart after it does not, and the text is
/// reposted. Distinct in shape (no dash) from [`transition_card_nonce`] so the two can never
/// collide.
#[must_use]
pub fn live_message_nonce(terminal_id: &str, position: i64, part_index: usize) -> String {
    let payload = format!("{terminal_id}-live-{position}-{part_index}");
    let payload_component = payload.bytes().fold(0_u64, |value, byte| {
        value.wrapping_mul(257).wrapping_add(u64::from(byte))
    });
    format!("{:013x}", payload_component & PAYLOAD_COMPONENT_MASK)
}

/// Maps a failed Discord send to a plain string, distinguishing "the target channel or thread no
/// longer exists" ([`UNKNOWN_CHANNEL_DELIVERY_ERROR`]-prefixed) and "the target webhook no longer
/// exists" ([`UNKNOWN_WEBHOOK_DELIVERY_ERROR`]-prefixed) from every other request failure.
fn map_send_error(error: &twilight_http::Error) -> String {
    if is_unknown_channel_error(error) {
        format!("{UNKNOWN_CHANNEL_DELIVERY_ERROR}: {error}")
    } else if is_unknown_webhook_error(error) {
        format!("{UNKNOWN_WEBHOOK_DELIVERY_ERROR}: {error}")
    } else {
        error.to_string()
    }
}

/// Delivers one plain, content-only message: no embed, no color, no mention, and mentions parsed
/// from nothing.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn deliver_live_message(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    content: &str,
    nonce: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    let nonce = bounded_nonce(nonce);
    let payload = serde_json::json!({
        PAYLOAD_CONTENT_KEY: content,
        ALLOWED_MENTIONS_KEY: {ALLOWED_MENTIONS_PARSE_KEY: []},
        "nonce": nonce,
        "enforce_nonce": true,
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .create_message(channel)
        .payload_json(&payload)
        .await
        .map_err(|error| map_send_error(&error))?
        .model()
        .await
        .map(|message| message.id)
        .map_err(|error| error.to_string())
}

/// The owner's identity as it should appear on a mirrored terminal prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnerIdentity {
    pub display_name: String,
    pub avatar_url: Option<String>,
}

/// Fetches the owner's identity once from Discord (`GET /users/{owner_id}`), independent of any
/// guild.
///
/// Display name falls back from `global_name` to the account username, and the avatar URL is the
/// account's own CDN avatar, `None` when it has none.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn fetch_owner_identity(
    client: &twilight_http::Client,
    owner_id: twilight_model::id::Id<twilight_model::id::marker::UserMarker>,
) -> Result<OwnerIdentity, String> {
    let user = client
        .user(owner_id)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?;
    Ok(OwnerIdentity {
        display_name: owner_display_name(user.global_name.as_deref(), &user.name),
        avatar_url: owner_avatar_url(user.id, user.avatar),
    })
}

fn owner_display_name(global_name: Option<&str>, username: &str) -> String {
    global_name.unwrap_or(username).to_owned()
}

fn owner_avatar_url(
    user_id: twilight_model::id::Id<twilight_model::id::marker::UserMarker>,
    avatar: Option<twilight_model::util::ImageHash>,
) -> Option<String> {
    let avatar = avatar?;
    let extension = if avatar.is_animated() { "gif" } else { "png" };
    Some(format!(
        "https://cdn.discordapp.com/avatars/{user_id}/{avatar}.{extension}"
    ))
}

/// Resolves the bridge-owned webhook for one workspace channel: the webhook named `webhook_name`
/// on that channel, creating it when none exists yet.
///
/// # Errors
///
/// Returns an error when the workspace channel has duplicate named webhooks, the resolved webhook
/// has no token, or Discord rejects the request, [`UNKNOWN_CHANNEL_DELIVERY_ERROR`]-prefixed when
/// the workspace channel no longer exists.
pub async fn resolve_terminal_prompt_webhook(
    client: &twilight_http::Client,
    workspace_channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    webhook_name: &str,
) -> Result<
    (
        twilight_model::id::Id<twilight_model::id::marker::WebhookMarker>,
        String,
    ),
    String,
> {
    let webhooks = client
        .channel_webhooks(workspace_channel)
        .await
        .map_err(|error| map_send_error(&error))?
        .model()
        .await
        .map_err(|error| error.to_string())?;
    let matching: Vec<_> = webhooks
        .into_iter()
        .filter(|webhook| webhook.name.as_deref() == Some(webhook_name))
        .collect();
    if matching.len() > 1 {
        return Err(format!(
            "Discord has multiple bridge-owned webhooks named {webhook_name}"
        ));
    }
    let webhook = if let Some(webhook) = matching.into_iter().next() {
        webhook
    } else {
        client
            .create_webhook(workspace_channel, webhook_name)
            .await
            .map_err(|error| map_send_error(&error))?
            .model()
            .await
            .map_err(|error| error.to_string())?
    };
    let token = webhook
        .token
        .ok_or_else(|| "bridge-owned webhook has no token".to_owned())?;
    Ok((webhook.id, token))
}

/// Executes one message through an already-resolved bridge-owned webhook, into a workspace
/// channel's thread, under the owner's mirrored display name and avatar.
///
/// # Errors
///
/// Returns Discord request or response errors, [`UNKNOWN_CHANNEL_DELIVERY_ERROR`]-prefixed when
/// the target thread no longer exists.
pub async fn execute_terminal_prompt_webhook(
    client: &twilight_http::Client,
    webhook_id: twilight_model::id::Id<twilight_model::id::marker::WebhookMarker>,
    webhook_token: &str,
    thread: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    username: &str,
    avatar_url: Option<&str>,
    content: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    let request = client
        .execute_webhook(webhook_id, webhook_token)
        .thread_id(thread)
        .username(username)
        .content(content);
    let request = if let Some(avatar_url) = avatar_url {
        request.avatar_url(avatar_url)
    } else {
        request
    };
    request
        .wait()
        .await
        .map_err(|error| map_send_error(&error))?
        .model()
        .await
        .map(|message| message.id)
        .map_err(|error| error.to_string())
}

/// Delivers one terminal-origin prompt through the bridge-owned workspace webhook, resolving (or
/// creating) it fresh on every call.
///
/// # Errors
///
/// Returns [`resolve_terminal_prompt_webhook`] or [`execute_terminal_prompt_webhook`] errors.
pub async fn deliver_terminal_prompt(
    client: &twilight_http::Client,
    workspace_channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    thread: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    webhook_name: &str,
    username: &str,
    avatar_url: Option<&str>,
    content: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    let (webhook_id, webhook_token) =
        resolve_terminal_prompt_webhook(client, workspace_channel, webhook_name).await?;
    execute_terminal_prompt_webhook(
        client,
        webhook_id,
        &webhook_token,
        thread,
        username,
        avatar_url,
        content,
    )
    .await
}

/// Delivers one plain, content-only activity message: no embed, no nonce -- a turn's activity
/// message is edited in place rather than deduplicated by nonce.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn deliver_activity_message(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    content: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    let payload = serde_json::json!({
        PAYLOAD_CONTENT_KEY: content,
        ALLOWED_MENTIONS_KEY: {ALLOWED_MENTIONS_PARSE_KEY: []},
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .create_message(channel)
        .payload_json(&payload)
        .await
        .map_err(|error| map_send_error(&error))?
        .model()
        .await
        .map(|message| message.id)
        .map_err(|error| error.to_string())
}

/// Edits an existing activity message's content in place.
///
/// # Errors
///
/// Returns Discord request errors.
pub async fn update_activity_message(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    message: twilight_model::id::Id<twilight_model::id::marker::MessageMarker>,
    content: &str,
) -> Result<(), String> {
    let payload = serde_json::json!({
        PAYLOAD_CONTENT_KEY: content,
        ALLOWED_MENTIONS_KEY: {ALLOWED_MENTIONS_PARSE_KEY: []},
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .update_message(channel, message)
        .payload_json(&payload)
        .await
        .map_err(|error| map_send_error(&error))?;
    Ok(())
}

/// Delivers a complete transition card with its embed color and optional mention.
///
/// # Errors
///
/// Returns Discord request or validation errors.
pub async fn deliver_transition_card(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    message: &TransitionMessage,
    nonce: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    deliver_payload(client, channel, &message.description, Some(message), nonce).await
}

/// Expires an informational blocked-pane card after the pane leaves `blocked`.
///
/// # Errors
///
/// Returns Discord request errors.
pub async fn expire_informational_card(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    message: twilight_model::id::Id<twilight_model::id::marker::MessageMarker>,
    content: &str,
) -> Result<(), String> {
    let payload = serde_json::json!({
        PAYLOAD_CONTENT_KEY: content,
        PAYLOAD_EMBEDS_KEY: [],
        PAYLOAD_COMPONENTS_KEY: [],
        ALLOWED_MENTIONS_KEY: {ALLOWED_MENTIONS_PARSE_KEY: []},
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .update_message(channel, message)
        .payload_json(&payload)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Delivers an owner decision card with opaque allow and deny component IDs.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn deliver_permission_card(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    vendor: PermissionVendor,
    tool: &str,
    command: &str,
    token: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    let payload = serde_json::json!({
        PAYLOAD_EMBEDS_KEY: [{
            "title": permission_card_title(vendor),
            "description": permission_card_description(tool, command),
            "color": 0x00f1_c40f,
        }],
        PAYLOAD_COMPONENTS_KEY: permission_components(token, false),
        ALLOWED_MENTIONS_KEY: {ALLOWED_MENTIONS_PARSE_KEY: []},
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .create_message(channel)
        .payload_json(&payload)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map(|message| message.id)
        .map_err(|error| error.to_string())
}

const fn permission_card_title(vendor: PermissionVendor) -> &'static str {
    match vendor {
        PermissionVendor::Claude => "Claude permission request",
        PermissionVendor::Codex => "Codex permission request",
        PermissionVendor::Cursor => "Cursor shell request",
    }
}

fn permission_card_description(tool: &str, command: &str) -> String {
    let tool_prefix = "Tool: `";
    let tool_suffix = "`\nCommand:\n```\n";
    let suffix = "\n```";
    let content_limit = MAX_PERMISSION_DESCRIPTION_LENGTH.saturating_sub(
        tool_prefix.chars().count() + tool_suffix.chars().count() + suffix.chars().count(),
    );
    let tool_limit = content_limit / 2;
    let sanitized_tool = tool
        .chars()
        .filter(|character| !character.is_control())
        .map(|character| if character == '`' { 'ˋ' } else { character })
        .collect::<String>();
    let tool_length = sanitized_tool.chars().count();
    let bounded_tool = if tool_length > tool_limit {
        let mut bounded = sanitized_tool
            .chars()
            .take(tool_limit.saturating_sub(1))
            .collect::<String>();
        bounded.push('…');
        bounded
    } else {
        sanitized_tool
    };
    let prefix = format!("{tool_prefix}{bounded_tool}{tool_suffix}");
    let command_limit = MAX_PERMISSION_DESCRIPTION_LENGTH
        .saturating_sub(prefix.chars().count() + suffix.chars().count());
    let sanitized_command = command
        .chars()
        .map(|character| if character == '`' { 'ˋ' } else { character })
        .collect::<String>();
    let command_length = sanitized_command.chars().count();
    let bounded_command = if command_length > command_limit {
        let mut bounded = sanitized_command
            .chars()
            .take(command_limit.saturating_sub(1))
            .collect::<String>();
        bounded.push('…');
        bounded
    } else {
        sanitized_command
    };
    format!("{prefix}{bounded_command}{suffix}")
}

/// Disables the controls on an expired or resolved permission card.
///
/// # Errors
///
/// Returns Discord request errors.
pub async fn expire_permission_card(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    message: twilight_model::id::Id<twilight_model::id::marker::MessageMarker>,
    token: &str,
    content: &str,
) -> Result<(), String> {
    let payload = serde_json::json!({
        PAYLOAD_CONTENT_KEY: content,
        PAYLOAD_COMPONENTS_KEY: permission_components(token, true),
        ALLOWED_MENTIONS_KEY: {ALLOWED_MENTIONS_PARSE_KEY: []},
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .update_message(channel, message)
        .payload_json(&payload)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn permission_components(token: &str, disabled: bool) -> serde_json::Value {
    serde_json::json!([{
        COMPONENT_TYPE_KEY: 1,
        PAYLOAD_COMPONENTS_KEY: [
            {COMPONENT_TYPE_KEY: 2, "style": 3, "label": "Allow", "custom_id": format!("herdr:allow:{token}"), "disabled": disabled},
            {COMPONENT_TYPE_KEY: 2, "style": 4, "label": "Deny", "custom_id": format!("herdr:deny:{token}"), "disabled": disabled}
        ]
    }])
}

/// Delivers a single-select question card: one button per option (Claude's `AskUserQuestion`
/// schema bounds this to 2-4), plus a "Type an answer" hint for a free-text thread reply.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn deliver_question_button_card(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    question: &Question,
    token: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    let payload = serde_json::json!({
        PAYLOAD_EMBEDS_KEY: [{
            "title": question_card_title(&question.header),
            "description": question_card_description(&question.question),
            "color": 0x00f1_c40f,
        }],
        PAYLOAD_COMPONENTS_KEY: question_button_components(&question.options, token, false),
        ALLOWED_MENTIONS_KEY: {ALLOWED_MENTIONS_PARSE_KEY: []},
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .create_message(channel)
        .payload_json(&payload)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map(|message| message.id)
        .map_err(|error| error.to_string())
}

/// Delivers a multiSelect question card: one Discord string select menu offering every option,
/// plus a "Type an answer" hint for a free-text thread reply.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn deliver_question_select_card(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    question: &Question,
    token: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    let payload = serde_json::json!({
        PAYLOAD_EMBEDS_KEY: [{
            "title": question_card_title(&question.header),
            "description": question_card_description(&question.question),
            "color": 0x00f1_c40f,
        }],
        PAYLOAD_COMPONENTS_KEY: question_select_components(&question.options, token, false),
        ALLOWED_MENTIONS_KEY: {ALLOWED_MENTIONS_PARSE_KEY: []},
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .create_message(channel)
        .payload_json(&payload)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map(|message| message.id)
        .map_err(|error| error.to_string())
}

fn question_card_title(header: &str) -> String {
    let sanitized: String = header.chars().filter(|c| !c.is_control()).collect();
    if sanitized.trim().is_empty() {
        "Claude question".to_owned()
    } else {
        format!("Claude question: {sanitized}")
    }
}

fn question_card_description(question: &str) -> String {
    let sanitized: String = question.chars().filter(|c| !c.is_control()).collect();
    format!("{sanitized}\n\nOr reply in this thread with your own answer.")
}

/// Truncates `value` to at most `limit` characters, marking a cut with a trailing `…`.
pub fn truncate_with_ellipsis(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_owned();
    }
    let mut truncated: String = value.chars().take(limit.saturating_sub(1)).collect();
    truncated.push('…');
    truncated
}

/// One action row of up to five option buttons, `herdrask:<token>:<option index>`.
fn question_button_components(options: &[QuestionOption], token: &str, disabled: bool) -> Value {
    let buttons: Vec<Value> = options
        .iter()
        .enumerate()
        .map(|(index, option)| {
            json!({
                COMPONENT_TYPE_KEY: 2,
                "style": 1,
                "label": truncate_with_ellipsis(&option.label, BUTTON_LABEL_LIMIT),
                "custom_id": format!("herdrask:{token}:{index}"),
                "disabled": disabled,
            })
        })
        .collect();
    json!([{COMPONENT_TYPE_KEY: 1, PAYLOAD_COMPONENTS_KEY: buttons}])
}

/// One action row holding a `herdrask-multi:<token>` string select menu offering every option.
fn question_select_components(options: &[QuestionOption], token: &str, disabled: bool) -> Value {
    let select_options: Vec<Value> = options
        .iter()
        .enumerate()
        .map(|(index, option)| {
            json!({
                "label": truncate_with_ellipsis(&option.label, SELECT_OPTION_TEXT_LIMIT),
                "value": index.to_string(),
                "description": truncate_with_ellipsis(&option.description, SELECT_OPTION_TEXT_LIMIT),
            })
        })
        .collect();
    json!([{
        COMPONENT_TYPE_KEY: 1,
        PAYLOAD_COMPONENTS_KEY: [{
            COMPONENT_TYPE_KEY: 3,
            "custom_id": format!("herdrask-multi:{token}"),
            "options": select_options,
            "min_values": 1,
            "max_values": options.len(),
            "disabled": disabled,
        }],
    }])
}

/// Disables the controls on an expired or resolved single-select question card.
///
/// # Errors
///
/// Returns Discord request errors.
pub async fn expire_question_button_card(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    message: twilight_model::id::Id<twilight_model::id::marker::MessageMarker>,
    options: &[QuestionOption],
    token: &str,
    content: &str,
) -> Result<(), String> {
    let payload = serde_json::json!({
        PAYLOAD_CONTENT_KEY: content,
        PAYLOAD_COMPONENTS_KEY: question_button_components(options, token, true),
        ALLOWED_MENTIONS_KEY: {ALLOWED_MENTIONS_PARSE_KEY: []},
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .update_message(channel, message)
        .payload_json(&payload)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// Disables the controls on an expired or resolved multiSelect question card.
///
/// # Errors
///
/// Returns Discord request errors.
pub async fn expire_question_select_card(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    message: twilight_model::id::Id<twilight_model::id::marker::MessageMarker>,
    options: &[QuestionOption],
    token: &str,
    content: &str,
) -> Result<(), String> {
    let payload = serde_json::json!({
        PAYLOAD_CONTENT_KEY: content,
        PAYLOAD_COMPONENTS_KEY: question_select_components(options, token, true),
        ALLOWED_MENTIONS_KEY: {ALLOWED_MENTIONS_PARSE_KEY: []},
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .update_message(channel, message)
        .payload_json(&payload)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

async fn deliver_payload(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    content: &str,
    card: Option<&TransitionMessage>,
    nonce: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    let nonce = bounded_nonce(nonce);
    let embed = Embed {
        author: None,
        color: card.map(|message| message.color),
        description: Some(content.to_owned()),
        fields: Vec::new(),
        footer: None,
        image: None,
        kind: "rich".into(),
        provider: None,
        thumbnail: None,
        timestamp: None,
        title: None,
        url: None,
        video: None,
    };
    let allowed = allowed_mentions(card);
    let message_content = card
        .and_then(|message| message.mention.as_deref())
        .unwrap_or_default();
    let payload = serde_json::json!({
        PAYLOAD_CONTENT_KEY: message_content,
        PAYLOAD_EMBEDS_KEY: [embed],
        ALLOWED_MENTIONS_KEY: allowed,
        "nonce": nonce,
        "enforce_nonce": true,
    });
    let payload = serde_json::to_vec(&payload).map_err(|error| error.to_string())?;
    client
        .create_message(channel)
        .payload_json(&payload)
        .await
        .map_err(|error| map_send_error(&error))?
        .model()
        .await
        .map(|message| message.id)
        .map_err(|error| error.to_string())
}

fn allowed_mentions(card: Option<&TransitionMessage>) -> Value {
    let Some(owner_mention) = card.and_then(|message| message.mention.as_deref()) else {
        return json!({ALLOWED_MENTIONS_PARSE_KEY: []});
    };
    let Some(owner_id) = owner_mention
        .strip_prefix("<@")
        .and_then(|mention| mention.strip_suffix('>'))
        .filter(|owner_id| !owner_id.is_empty())
    else {
        return json!({ALLOWED_MENTIONS_PARSE_KEY: []});
    };
    json!({ALLOWED_MENTIONS_PARSE_KEY: [], "users": [owner_id]})
}

fn bounded_nonce(nonce: &str) -> String {
    if nonce.len() <= MAX_DISCORD_NONCE_LENGTH {
        return nonce.to_owned();
    }
    let digest = nonce.bytes().fold(0_u64, |value, byte| {
        value.wrapping_mul(257).wrapping_add(u64::from(byte))
    });
    format!("{digest:016x}")
}

fn transition_card_nonce_for_start(
    startup_component: u64,
    terminal_id: &str,
    state_change_seq: u64,
    card_index: usize,
) -> String {
    let payload = format!("{terminal_id}-{state_change_seq}-{card_index}");
    let payload_component = payload.bytes().fold(0_u64, |value, byte| {
        value.wrapping_mul(257).wrapping_add(u64::from(byte))
    });
    format!(
        "{:011x}-{:013x}",
        startup_component & STARTUP_COMPONENT_MASK,
        payload_component & PAYLOAD_COMPONENT_MASK
    )
}

fn process_start_component() -> u64 {
    *PROCESS_START_COMPONENT.get_or_init(new_process_start_component)
}

#[ctor::ctor]
fn initialize_process_start_component() {
    let _ = PROCESS_START_COMPONENT.set(new_process_start_component());
}

fn new_process_start_component() -> u64 {
    let startup_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            duration
                .as_secs()
                .saturating_mul(1_000)
                .saturating_add(u64::from(duration.subsec_millis()))
        });
    let process_id = u64::from(std::process::id());
    startup_millis
        .rotate_left(17)
        .wrapping_add(process_id.wrapping_mul(97_531))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        MAX_DISCORD_NONCE_LENGTH, MAX_PERMISSION_DESCRIPTION_LENGTH, allowed_mentions,
        live_message_nonce, owner_avatar_url, owner_display_name, permission_card_description,
        permission_card_title, transition_card_nonce_for_start,
    };
    use crate::cards::TransitionMessage;
    use crate::permission::PermissionVendor;
    use twilight_model::id::Id;
    use twilight_model::util::ImageHash;

    #[test]
    fn allowed_mentions_only_allows_the_blocked_card_owner() {
        let blocked = TransitionMessage {
            description: "blocked".to_owned(),
            color: 0,
            mention: Some("<@42>".to_owned()),
        };
        let cases = [
            (None, json!({"parse": []})),
            (Some(&blocked), json!({"parse": [], "users": ["42"]})),
        ];
        for (card, expected) in cases {
            assert_eq!(allowed_mentions(card), expected);
        }
    }

    #[test]
    fn simulated_process_starts_produce_bounded_distinct_retry_safe_nonces() {
        let cases = [
            (1_700_000_000_000, 1_700_000_000_001, "terminal", 1, 0),
            (
                u64::MAX - 1,
                u64::MAX,
                "terminal-with-a-long-identifier",
                u64::MAX,
                usize::MAX,
            ),
        ];

        for (first_start, second_start, terminal, sequence, card_index) in cases {
            let first =
                transition_card_nonce_for_start(first_start, terminal, sequence, card_index);
            let second =
                transition_card_nonce_for_start(second_start, terminal, sequence, card_index);

            assert_ne!(first, second);
            assert!(first.len() <= MAX_DISCORD_NONCE_LENGTH);
            assert_eq!(
                first,
                transition_card_nonce_for_start(first_start, terminal, sequence, card_index)
            );
        }
    }

    #[test]
    fn live_message_nonce_is_stable_by_position_not_a_counter() {
        let first = live_message_nonce("terminal", 100, 0);
        // Same terminal and position: identical, whether this is the original read or a watch
        // restart that resumed at the same log position — the point of keying on position rather
        // than a resettable counter.
        assert_eq!(first, live_message_nonce("terminal", 100, 0));
        assert_ne!(
            first,
            live_message_nonce("terminal", 200, 0),
            "position differs"
        );
        assert_ne!(
            first,
            live_message_nonce("other-terminal", 100, 0),
            "terminal differs"
        );
        assert_ne!(
            first,
            live_message_nonce("terminal", 100, 1),
            "part index differs"
        );
        assert!(
            !first.contains('-'),
            "a live nonce carries no process-start component, unlike a transition card nonce"
        );
    }

    #[test]
    fn permission_card_description_is_safe_and_bounded() {
        let long_command = "x".repeat(4_097);
        let cases = [
            (
                "backtick run",
                "Bash",
                "printf 'before ``` after'",
                false,
                "Bash",
            ),
            (
                "backtick tool",
                "tool ``` injection",
                "echo ok",
                false,
                "tool ˋˋˋ injection",
            ),
            ("long command", "Bash", long_command.as_str(), true, "Bash"),
        ];

        for (name, tool, command, truncated, expected_tool) in cases {
            let description = permission_card_description(tool, command);
            assert!(
                description.chars().count() <= MAX_PERMISSION_DESCRIPTION_LENGTH,
                "{name} description exceeded the safe limit"
            );
            assert!(
                description.contains(expected_tool),
                "{name} tool was not sanitized"
            );
            assert_eq!(
                description.matches('`').count(),
                8,
                "{name} description contains an unescaped inline-code delimiter"
            );
            assert_eq!(
                description.matches("```").count(),
                2,
                "{name} description contains an unescaped code-fence run"
            );
            assert_eq!(
                description.contains('…'),
                truncated,
                "{name} truncation marker did not match the input size"
            );
        }
    }

    #[test]
    fn permission_card_description_keeps_tool_on_one_line() {
        let description = permission_card_description("Bash\nCommand:\ninjected", "echo ok");
        let tool_segment = description
            .strip_prefix("Tool: `")
            .unwrap()
            .split_once("`\nCommand:\n```")
            .unwrap()
            .0;

        assert!(!tool_segment.contains('\n'));
        assert_eq!(description.matches("Command:\n```").count(), 1);
        assert_eq!(description.matches("```").count(), 2);
    }

    #[test]
    fn permission_card_description_keeps_command_visible_with_long_tool() {
        let description = permission_card_description(&"T".repeat(5_000), "rm -rf /tmp/x");

        assert!(description.contains("rm -rf /tmp/x"));
        assert!(description.chars().count() <= MAX_PERMISSION_DESCRIPTION_LENGTH);
    }

    #[test]
    fn owner_display_name_prefers_global_name_over_username() {
        let cases = [
            (Some("Global Name"), "handle", "Global Name"),
            (None, "handle", "handle"),
        ];
        for (global_name, username, expected) in cases {
            assert_eq!(owner_display_name(global_name, username), expected);
        }
    }

    #[test]
    fn owner_avatar_url_is_none_without_an_avatar_hash() {
        assert_eq!(owner_avatar_url(Id::new(1), None), None);
    }

    #[test]
    fn owner_avatar_url_picks_extension_from_animation() {
        let user_id = Id::new(42);
        let static_hash = ImageHash::parse(b"06c16474723fe537c283b8efa61a30c8")
            .expect("parse static image hash fixture");
        let animated_hash = ImageHash::parse(b"a_06c16474723fe537c283b8efa61a30c8")
            .expect("parse animated image hash fixture");
        assert_eq!(
            owner_avatar_url(user_id, Some(static_hash)),
            Some(
                "https://cdn.discordapp.com/avatars/42/06c16474723fe537c283b8efa61a30c8.png"
                    .to_owned()
            )
        );
        assert_eq!(
            owner_avatar_url(user_id, Some(animated_hash)),
            Some(
                "https://cdn.discordapp.com/avatars/42/a_06c16474723fe537c283b8efa61a30c8.gif"
                    .to_owned()
            )
        );
    }

    #[test]
    fn permission_card_title_identifies_vendor() {
        let cases = [
            (PermissionVendor::Claude, "Claude permission request"),
            (PermissionVendor::Codex, "Codex permission request"),
            (PermissionVendor::Cursor, "Cursor shell request"),
        ];

        for (vendor, expected) in cases {
            assert_eq!(permission_card_title(vendor), expected);
        }
    }

    use super::{
        BUTTON_LABEL_LIMIT, SELECT_OPTION_TEXT_LIMIT, question_button_components,
        question_card_description, question_card_title, truncate_with_ellipsis,
    };
    use crate::question::QuestionOption;

    fn option(label: &str) -> QuestionOption {
        QuestionOption {
            label: label.to_owned(),
            description: format!("{label} description"),
        }
    }

    #[test]
    fn truncate_with_ellipsis_only_cuts_when_over_the_limit() {
        let cases = [
            ("at the limit", "x".repeat(80), 80, "x".repeat(80)),
            (
                "over the limit",
                "x".repeat(81),
                80,
                format!("{}…", "x".repeat(79)),
            ),
        ];
        for (name, input, limit, expected) in cases {
            let truncated = truncate_with_ellipsis(&input, limit);
            assert_eq!(truncated, expected, "case={name}");
        }
    }

    #[test]
    fn question_button_components_truncate_an_oversized_label() {
        let long_label = "x".repeat(BUTTON_LABEL_LIMIT + 1);
        let options = vec![option(&long_label)];
        let components = question_button_components(&options, "tok", false);
        let label = components[0]["components"][0]["label"]
            .as_str()
            .expect("button label is a string");
        assert!(label.chars().count() <= BUTTON_LABEL_LIMIT);
        assert!(label.ends_with('…'));
    }

    #[test]
    fn question_select_components_truncate_an_oversized_label_and_description() {
        let long_label = "x".repeat(SELECT_OPTION_TEXT_LIMIT + 1);
        let long_description = "y".repeat(SELECT_OPTION_TEXT_LIMIT + 50);
        let options = vec![QuestionOption {
            label: long_label,
            description: long_description,
        }];
        let components = super::question_select_components(&options, "tok", false);
        let menu_option = &components[0]["components"][0]["options"][0];
        let label = menu_option["label"].as_str().expect("label is a string");
        let description = menu_option["description"]
            .as_str()
            .expect("description is a string");
        assert!(label.chars().count() <= SELECT_OPTION_TEXT_LIMIT);
        assert!(label.ends_with('…'));
        assert!(description.chars().count() <= SELECT_OPTION_TEXT_LIMIT);
        assert!(description.ends_with('…'));
    }

    #[test]
    fn question_card_title_falls_back_without_a_header() {
        let cases = [
            ("Color", "Claude question: Color"),
            ("   ", "Claude question"),
        ];
        for (header, expected) in cases {
            assert_eq!(question_card_title(header), expected);
        }
    }

    #[test]
    fn question_card_description_always_hints_a_free_text_reply() {
        assert_eq!(
            question_card_description("Which color?"),
            "Which color?\n\nOr reply in this thread with your own answer."
        );
    }

    #[test]
    fn question_button_components_encode_one_button_per_option_with_the_option_index() {
        let options = vec![option("Red"), option("Blue")];
        let components = question_button_components(&options, "tok", false);
        assert_eq!(
            components,
            json!([{
                "type": 1,
                "components": [
                    {"type": 2, "style": 1, "label": "Red", "custom_id": "herdrask:tok:0", "disabled": false},
                    {"type": 2, "style": 1, "label": "Blue", "custom_id": "herdrask:tok:1", "disabled": false},
                ],
            }])
        );
        let disabled = question_button_components(&options, "tok", true);
        assert_eq!(disabled[0]["components"][0]["disabled"], json!(true));
    }

    #[test]
    fn question_select_components_offer_every_option_with_a_matching_value_range() {
        let options = vec![option("Cheese"), option("Olives"), option("Mushrooms")];
        let components = super::question_select_components(&options, "tok", false);
        let menu = &components[0]["components"][0];
        assert_eq!(menu["type"], json!(3));
        assert_eq!(menu["custom_id"], json!("herdrask-multi:tok"));
        assert_eq!(menu["min_values"], json!(1));
        assert_eq!(menu["max_values"], json!(3));
        assert_eq!(
            menu["options"],
            json!([
                {"label": "Cheese", "value": "0", "description": "Cheese description"},
                {"label": "Olives", "value": "1", "description": "Olives description"},
                {"label": "Mushrooms", "value": "2", "description": "Mushrooms description"},
            ])
        );
    }
}
