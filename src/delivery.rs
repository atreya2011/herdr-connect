use crate::cards::TransitionMessage;
use twilight_model::channel::message::{AllowedMentions, Embed};

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
    let nonce = nonce.bytes().fold(0_u64, |value, byte| {
        value.wrapping_mul(257).wrapping_add(u64::from(byte))
    });
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
    client
        .create_message(channel)
        .content(&message_content)
        .embeds(std::slice::from_ref(&embed))
        .allowed_mentions(Some(&allowed))
        .nonce(nonce)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map(|message| message.id)
        .map_err(|error| error.to_string())
}
