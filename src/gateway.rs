use std::sync::mpsc::Sender;
use twilight_gateway::{
    ConfigBuilder, Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _,
};

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
