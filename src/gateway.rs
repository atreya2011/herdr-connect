use std::sync::Arc;
use std::sync::mpsc::Sender;
use twilight_gateway::{
    ConfigBuilder, Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _,
};
use twilight_http::Client;
use twilight_model::id::{Id, marker::GuildMarker};

/// Connects to the Discord gateway and reports message events and gateway errors
/// through `notices` without returning on those errors.
pub async fn drive_gateway(token: String, gateway_url: Option<String>, notices: Sender<String>) {
    let intents = Intents::GUILDS | Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;
    let builder = ConfigBuilder::new(token, intents);
    let config = match gateway_url {
        Some(url) => builder.proxy_url(url).build(),
        None => builder.build(),
    };
    let mut shard = Shard::with_config(ShardId::ONE, config);
    while let Some(item) = shard.next_event(EventTypeFlags::MESSAGE_CREATE).await {
        let notice = match item {
            Ok(Event::MessageCreate(_)) => "discord gateway message: MESSAGE_CREATE".to_owned(),
            Ok(_) => continue,
            Err(error) => format!("discord gateway error: {error}"),
        };
        if notices.send(notice).is_err() {
            break;
        }
    }
}

/// Connects the Discord gateway and dispatches owner prompts without blocking gateway progress.
pub async fn drive_gateway_with_owner_prompt(
    token: String,
    gateway_url: Option<String>,
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: String,
    notices: Sender<String>,
) {
    let intents = Intents::GUILDS | Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;
    let builder = ConfigBuilder::new(token, intents);
    let config = match gateway_url {
        Some(url) => builder.proxy_url(url).build(),
        None => builder.build(),
    };
    let mut shard = Shard::with_config(ShardId::ONE, config);
    while let Some(item) = shard.next_event(EventTypeFlags::MESSAGE_CREATE).await {
        let notice = match item {
            Ok(Event::MessageCreate(message)) => {
                let client = Arc::clone(&client);
                let notices = notices.clone();
                let owner_id = owner_id.clone();
                tokio::spawn(async move {
                    if let Err(error) =
                        crate::prompting::handle_owner_message(client, guild, &owner_id, message.0)
                            .await
                    {
                        let _ = notices.send(format!("discord owner prompt error: {error}"));
                    }
                });
                "discord gateway message: MESSAGE_CREATE".to_owned()
            }
            Ok(_) => continue,
            Err(error) => format!("discord gateway error: {error}"),
        };
        if notices.send(notice).is_err() {
            break;
        }
    }
}
