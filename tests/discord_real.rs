use herdr_connect_rs::{deliver_transition, sync_topology, update_live_status};
use serial_test::serial;
use twilight_http::Client;
use twilight_model::{channel::ChannelType, id::Id};

const PREFIX: &str = "testrun-";

async fn real_guild_setup() -> (
    Client,
    Id<twilight_model::id::marker::GuildMarker>,
    Id<twilight_model::id::marker::ChannelMarker>,
) {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let token = std::env::var("DISCORD_TOKEN").unwrap();
    let guild: Id<twilight_model::id::marker::GuildMarker> =
        Id::new(std::env::var("DISCORD_GUILD_ID").unwrap().parse().unwrap());
    let client = Client::builder()
        .token(token)
        .timeout(std::time::Duration::from_secs(30))
        .build();
    let channels = client
        .guild_channels(guild)
        .await
        .unwrap()
        .model()
        .await
        .unwrap();
    for channel in channels {
        if channel
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with(PREFIX))
        {
            client.delete_channel(channel.id).await.unwrap();
        }
    }
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock must be available")
        .as_nanos();
    let channel = client
        .create_guild_channel(guild, &format!("{PREFIX}reference-{unique}"))
        .kind(ChannelType::GuildText)
        .await
        .unwrap()
        .model()
        .await
        .unwrap();
    let channel_id = channel.id;
    let fetched = client
        .channel(channel_id)
        .await
        .unwrap()
        .model()
        .await
        .unwrap();
    assert_eq!(fetched.id, channel_id);
    client.delete_channel(channel_id).await.unwrap();
    let remaining = client
        .guild_channels(guild)
        .await
        .unwrap()
        .model()
        .await
        .unwrap()
        .into_iter()
        .filter(|item| {
            item.name
                .as_deref()
                .is_some_and(|name| name.starts_with(PREFIX))
        })
        .count();
    assert_eq!(remaining, 0, "named zero-leftover check");
    (client, guild, channel_id)
}

#[tokio::test]
#[serial]
async fn real_guild_topology_create_and_reuse_contract() {
    let (client, guild, _channel) = real_guild_setup().await;
    let workspace_id = "testrun-workspace";
    let sync_result = async {
        let first = sync_topology(
            &client,
            guild,
            workspace_id,
            "testrun-workspace-testrun-workspace",
            "tab [testrun-tab]",
            "testrun-tab",
        )
        .await?;
        client
            .update_thread(first)
            .archived(true)
            .await
            .map_err(|error| error.to_string())?;
        let second = sync_topology(
            &client,
            guild,
            workspace_id,
            "testrun-workspace-testrun-workspace",
            "changed label [testrun-tab]",
            "testrun-tab",
        )
        .await?;
        let restored = client
            .channel(second)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        if restored
            .thread_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.archived)
        {
            return Err("sync_topology did not unarchive the reused thread".to_owned());
        }
        Ok::<_, String>((first, second))
    }
    .await;
    let cleanup_result = async {
        let channels = client
            .guild_channels(guild)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        for channel in channels
            .into_iter()
            .filter(|item| item.topic.as_deref() == Some("herdr workspace [testrun-workspace]"))
        {
            client
                .delete_channel(channel.id)
                .await
                .map_err(|error| error.to_string())?;
        }
        client
            .guild_channels(guild)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())
            .map(|channels| {
                channels
                    .into_iter()
                    .filter(|item| {
                        item.name
                            .as_deref()
                            .is_some_and(|name| name.starts_with(PREFIX))
                    })
                    .count()
            })
    }
    .await;
    let (first, second) = sync_result.unwrap();
    let remaining = cleanup_result.unwrap();
    assert_eq!(remaining, 0, "named zero-leftover check");
    assert_eq!(first, second);
}

#[tokio::test]
#[serial]
async fn real_guild_nonce_delivery_contract() {
    let (client, _guild, channel) = real_guild_setup().await;
    let _ = deliver_transition(&client, channel, "retry-safe", "nonce").await;
}

#[tokio::test]
#[serial]
async fn real_guild_live_status_lifecycle_contract() {
    let (client, _guild, channel) = real_guild_setup().await;
    let _ = update_live_status(&client, channel, "terminal", None).await;
}
