use crate::cards::TransitionMessage;
use twilight_model::channel::message::{AllowedMentions, Embed};

const MAX_DISCORD_NONCE_LENGTH: usize = 25;

/// Derives one stable nonce for a transition card delivery.
#[must_use]
pub fn transition_card_nonce(
    terminal_id: &str,
    state_change_seq: u64,
    card_index: usize,
) -> String {
    bounded_nonce(&format!("{terminal_id}-{state_change_seq}-{card_index}"))
}

/// Delivers one transition message.
///
/// # Errors
///
/// Returns Discord request errors.
pub async fn deliver_transition(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    content: &str,
    nonce: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>, String> {
    deliver_payload(client, channel, content, None, nonce).await
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
    let allowed = AllowedMentions::default();
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

fn bounded_nonce(nonce: &str) -> String {
    if nonce.len() <= MAX_DISCORD_NONCE_LENGTH {
        return nonce.to_owned();
    }
    let digest = nonce.bytes().fold(0_u64, |value, byte| {
        value.wrapping_mul(257).wrapping_add(u64::from(byte))
    });
    format!("{digest:016x}")
}
