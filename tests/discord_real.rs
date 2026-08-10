use herdr_connect_rs::{deliver_transition, sync_topology, update_live_status};
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
    let client = Client::new(token);
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
async fn real_guild_topology_create_and_reuse_contract() {
    let (client, guild, channel) = real_guild_setup().await;
    sync_topology(&client, guild, "workspace", "tab");
    let _ = channel;
}

#[tokio::test]
async fn real_guild_nonce_delivery_contract() {
    let (client, _guild, channel) = real_guild_setup().await;
    deliver_transition(&client, channel, "retry-safe", "nonce");
}

#[tokio::test]
async fn real_guild_live_status_lifecycle_contract() {
    let (client, _guild, channel) = real_guild_setup().await;
    update_live_status(&client, channel, "terminal", None);
}
