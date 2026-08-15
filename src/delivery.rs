use crate::cards::TransitionMessage;
use serde_json::{Value, json};
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};
use twilight_model::channel::message::Embed;

const MAX_DISCORD_NONCE_LENGTH: usize = 25;
const STARTUP_COMPONENT_MASK: u64 = (1_u64 << 44) - 1;
const PAYLOAD_COMPONENT_MASK: u64 = (1_u64 << 52) - 1;
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

/// Delivers an owner decision card with opaque allow and deny component IDs.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn deliver_permission_card(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    tool: &str,
    command: &str,
    token: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    let payload = serde_json::json!({
        "embeds": [{
            "title": "Claude permission request",
            "description": format!("Tool: `{tool}`\nCommand:\n```\n{command}\n```"),
            "color": 0x00f1_c40f,
        }],
        "components": permission_components(token, false),
        "allowed_mentions": {"parse": []},
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
        "content": content,
        "components": permission_components(token, true),
        "allowed_mentions": {"parse": []},
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
        "type": 1,
        "components": [
            {"type": 2, "style": 3, "label": "Allow", "custom_id": format!("herdr:allow:{token}"), "disabled": disabled},
            {"type": 2, "style": 4, "label": "Deny", "custom_id": format!("herdr:deny:{token}"), "disabled": disabled}
        ]
    }])
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
        .map_or_else(
            || content.to_owned(),
            |mention| format!("{mention} {content}"),
        );
    let payload = serde_json::json!({
        "content": message_content,
        "embeds": [embed],
        "allowed_mentions": allowed,
        "nonce": nonce,
        "enforce_nonce": true,
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

fn allowed_mentions(card: Option<&TransitionMessage>) -> Value {
    let Some(owner_mention) = card.and_then(|message| message.mention.as_deref()) else {
        return json!({"parse": []});
    };
    let Some(owner_id) = owner_mention
        .strip_prefix("<@")
        .and_then(|mention| mention.strip_suffix('>'))
        .filter(|owner_id| !owner_id.is_empty())
    else {
        return json!({"parse": []});
    };
    json!({"parse": [], "users": [owner_id]})
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
    use super::{MAX_DISCORD_NONCE_LENGTH, allowed_mentions, transition_card_nonce_for_start};
    use crate::cards::TransitionMessage;
    use serde_json::json;

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
                1_700_000_000_100,
                1_700_000_000_101,
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
            assert!(second.len() <= MAX_DISCORD_NONCE_LENGTH);
            assert_eq!(
                first,
                transition_card_nonce_for_start(first_start, terminal, sequence, card_index)
            );
        }
    }
}
