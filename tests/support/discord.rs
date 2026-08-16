use std::sync::Arc;
use std::time::Duration;
use twilight_http::Client;
use twilight_model::{
    channel::ChannelType,
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker},
    },
};

const PREFIX: &str = "testrun-";
const CLEANUP_CHECKS: u32 = 5;
const CLEANUP_BACKOFF: Duration = Duration::from_millis(200);

pub struct Guild {
    pub client: Arc<Client>,
    pub id: Id<GuildMarker>,
}

pub fn guild() -> Option<Guild> {
    Some(Guild {
        client: Arc::new(
            Client::builder()
                .token(std::env::var("DISCORD_TOKEN").ok()?)
                .timeout(std::time::Duration::from_secs(30))
                .build(),
        ),
        id: Id::new(std::env::var("DISCORD_GUILD_ID").ok()?.parse().ok()?),
    })
}

pub async fn cleanup(guild: &Guild) -> Result<usize, String> {
    let channels = guild
        .client
        .guild_channels(guild.id)
        .await
        .map_err(|e| e.to_string())?
        .model()
        .await
        .map_err(|e| e.to_string())?;
    for channel in channels.into_iter().filter(|channel| {
        channel
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with(PREFIX))
    }) {
        guild
            .client
            .delete_channel(channel.id)
            .await
            .map_err(|e| e.to_string())?;
    }
    for attempt in 0..CLEANUP_CHECKS {
        let leftover = guild
            .client
            .guild_channels(guild.id)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|channel| {
                channel
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with(PREFIX))
            })
            .count();
        if leftover == 0 || attempt + 1 == CLEANUP_CHECKS {
            return Ok(leftover);
        }
        tokio::time::sleep(CLEANUP_BACKOFF * (attempt + 1)).await;
    }
    unreachable!("cleanup checks always return a leftover count");
}

pub async fn channel(guild: &Guild, name: &str) -> Result<Id<ChannelMarker>, String> {
    Ok(guild
        .client
        .create_guild_channel(guild.id, name)
        .kind(ChannelType::GuildText)
        .await
        .map_err(|e| e.to_string())?
        .model()
        .await
        .map_err(|e| e.to_string())?
        .id)
}
