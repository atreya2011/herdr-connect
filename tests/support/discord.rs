use std::sync::Arc;
use std::time::Duration;
use twilight_http::Client;
use twilight_model::{
    channel::{Channel, ChannelType},
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker},
    },
};

const PREFIX: &str = "testrun-";

fn is_test_channel(channel: &Channel) -> bool {
    channel
        .name
        .as_deref()
        .is_some_and(|name| name.starts_with(PREFIX))
}

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
    for channel in channels.into_iter().filter(is_test_channel) {
        guild
            .client
            .delete_channel(channel.id)
            .await
            .map_err(|e| e.to_string())?;
    }
    let mut attempt = 0;
    loop {
        let leftover = guild
            .client
            .guild_channels(guild.id)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(is_test_channel)
            .count();
        if leftover == 0 || attempt == 4 {
            return Ok(leftover);
        }
        tokio::time::sleep(Duration::from_millis(200) * (attempt + 1)).await;
        attempt += 1;
    }
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
