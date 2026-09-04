use std::sync::Arc;
use std::time::Duration;

use twilight_http::{Client, api_error::ApiError, error::ErrorType, response::StatusCode};
use twilight_model::{
    channel::{Channel, ChannelType},
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker},
    },
};

const PREFIX: &str = "testrun-";
const WORKSPACE_TOPIC_PREFIX: &str = "herdr workspace [";

fn is_test_channel(channel: &Channel) -> bool {
    channel
        .name
        .as_deref()
        .is_some_and(|name| name.starts_with(PREFIX))
}

/// Every `testrun-` thread in the guild, active or archived, whichever channel parents it. Suite
/// threads outlive their tests when they hang off a channel the prefix filter does not delete.
///
/// Archived threads are listed only under channels the bridge marks as a herdr workspace, the only
/// channels it ever creates a thread in. This is the delete pass, so it pays for the full reach.
async fn testrun_threads(guild: &Guild) -> Result<Vec<Id<ChannelMarker>>, String> {
    let channels = guild
        .client
        .guild_channels(guild.id)
        .await
        .map_err(|e| e.to_string())?
        .model()
        .await
        .map_err(|e| e.to_string())?;
    let mut threads = guild
        .client
        .active_threads(guild.id)
        .await
        .map_err(|e| e.to_string())?
        .model()
        .await
        .map_err(|e| e.to_string())?
        .threads;
    for parent in channels.iter().filter(|channel| {
        channel
            .topic
            .as_deref()
            .is_some_and(|topic| topic.starts_with(WORKSPACE_TOPIC_PREFIX))
    }) {
        threads.extend(herdr_connect_rs::archived_threads(guild.client.as_ref(), parent.id).await?);
    }
    Ok(threads
        .into_iter()
        .filter(is_test_channel)
        .map(|thread| thread.id)
        .collect())
}

/// The `testrun-` threads a leftover recount has to see: the guild-wide active list only.
///
/// The recount deliberately skips the per-channel archived listings the delete pass runs. A thread
/// the delete pass just deleted cannot come back as an archived thread, and a thread the suite
/// leaked is active, because the suite never archives one. So an archived listing here could only
/// repeat what the active list already shows.
async fn testrun_active_threads(guild: &Guild) -> Result<usize, String> {
    Ok(guild
        .client
        .active_threads(guild.id)
        .await
        .map_err(|e| e.to_string())?
        .model()
        .await
        .map_err(|e| e.to_string())?
        .threads
        .iter()
        .filter(|thread| is_test_channel(thread))
        .count())
}

/// Deletes one thread, treating an already-deleted thread as done.
async fn delete_thread(guild: &Guild, thread: Id<ChannelMarker>) -> Result<(), String> {
    match guild.client.delete_channel(thread).await {
        Ok(_) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                ErrorType::Response {
                    status,
                    error: ApiError::General(api_error),
                    ..
                } if *status == StatusCode::NOT_FOUND && api_error.code == 10003
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    }
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
    for thread in testrun_threads(guild).await? {
        delete_thread(guild, thread).await?;
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
            .count()
            + testrun_active_threads(guild).await?;
        if leftover == 0 || attempt == 4 {
            return Ok(leftover);
        }
        tokio::time::sleep(Duration::from_millis(200) * (attempt + 1)).await;
        attempt += 1;
    }
}

pub async fn channel(guild: &Guild, name: &str) -> Result<Id<ChannelMarker>, String> {
    let id = guild
        .client
        .create_guild_channel(guild.id, name)
        .kind(ChannelType::GuildText)
        .await
        .map_err(|e| e.to_string())?
        .model()
        .await
        .map_err(|e| e.to_string())?
        .id;

    for attempt in 0..6 {
        match guild.client.channel(id).await {
            Ok(_) => return Ok(id),
            Err(error)
                if matches!(
                    error.kind(),
                    ErrorType::Response {
                        status,
                        error: ApiError::General(api_error),
                        ..
                    } if *status == StatusCode::NOT_FOUND && api_error.code == 10003
                ) && attempt < 5 =>
            {
                tokio::time::sleep(Duration::from_millis(100 * (attempt + 1))).await;
            }
            Err(error) => return Err(error.to_string()),
        }
    }

    unreachable!()
}
