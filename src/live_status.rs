use std::sync::{Mutex, OnceLock};

static LIVE_MESSAGES: OnceLock<
    Mutex<
        std::collections::HashMap<
            u64,
            twilight_model::id::Id<twilight_model::id::marker::MessageMarker>,
        >,
    >,
> = OnceLock::new();

/// Updates an existing live-status message.
///
/// # Errors
///
/// Returns Discord request errors.
pub async fn update_live_status(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    terminal: &str,
    message: Option<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>>,
) -> Result<(), String> {
    let content = format!("{terminal} working");
    let known = LIVE_MESSAGES.get_or_init(|| Mutex::new(std::collections::HashMap::new()));
    let message = message.or_else(|| known.lock().ok()?.get(&channel.get()).copied());
    if let Some(message) = message {
        client
            .update_message(channel, message)
            .content(Some(&content))
            .await
            .map(|_| ())
            .map_err(|error| error.to_string())
    } else {
        let created = client
            .create_message(channel)
            .content(&content)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        if let Ok(mut messages) = known.lock() {
            messages.insert(channel.get(), created.id);
        }
        Ok(())
    }
}
