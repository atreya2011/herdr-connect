//! Red Discord contracts against the real disposable guild.

use std::panic::{AssertUnwindSafe, catch_unwind};

use herdr_connect_rs::{
    LiveStatusUpdate, TopologySyncRequest, TransitionDelivery, deliver_transition, sync_topology,
    update_live_status,
};
use twilight_http::Client;
use twilight_model::{channel::ChannelType, id::Id};

const TESTRUN_PREFIX: &str = "testrun-";

fn discord_client() -> (Client, Id<twilight_model::id::marker::GuildMarker>) {
    let token =
        std::env::var("DISCORD_TOKEN").expect("DISCORD_TOKEN must be sourced at invocation");
    let guild = std::env::var("DISCORD_GUILD_ID")
        .expect("DISCORD_GUILD_ID must be sourced at invocation")
        .parse()
        .expect("DISCORD_GUILD_ID must be numeric");
    (Client::new(token), Id::new(guild))
}

async fn sweep_leftovers(client: &Client, guild: Id<twilight_model::id::marker::GuildMarker>) {
    let channels = client
        .guild_channels(guild)
        .await
        .expect("real guild channel listing must work")
        .model()
        .await
        .expect("real guild channel response must decode");
    for channel in channels {
        if channel
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with(TESTRUN_PREFIX))
        {
            client
                .delete_channel(channel.id)
                .await
                .expect("leftover testrun channel deletion must work");
        }
    }
}

async fn disposable_channel(
    client: &Client,
    guild: Id<twilight_model::id::marker::GuildMarker>,
) -> Id<twilight_model::id::marker::ChannelMarker> {
    client
        .create_guild_channel(guild, &format!("{TESTRUN_PREFIX}red"))
        .kind(ChannelType::GuildText)
        .topic("phase B red test")
        .await
        .expect("real testrun channel creation must work")
        .model()
        .await
        .expect("real channel response must decode")
        .id
}

#[tokio::test]
async fn real_guild_discord_contracts() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("real Discord TLS provider must install");
    let (client, guild) = discord_client();
    sweep_leftovers(&client, guild).await;
    let cases = ["topology sync", "transition delivery", "live status"];
    let mut failures = Vec::new();
    for case in cases {
        let channel = disposable_channel(&client, guild).await;
        let panic = catch_unwind(AssertUnwindSafe(|| match case {
            "topology sync" => {
                let request = TopologySyncRequest {
                    workspace_id: "workspace",
                    tab_id: "tab",
                    thread_name: "testrun-thread",
                };
                let _ = sync_topology(&client, guild, request);
            }
            "transition delivery" => {
                let request = TransitionDelivery {
                    tab_id: "tab",
                    nonce: "testrun-nonce",
                    content: "red",
                };
                let _ = deliver_transition(&client, request);
            }
            "live status" => {
                let request = LiveStatusUpdate {
                    terminal_id: "terminal",
                    status: "working",
                    content: "red",
                };
                let _ = update_live_status(&client, request);
            }
            _ => unreachable!(),
        }))
        .is_err();
        client
            .delete_channel(channel)
            .await
            .expect("testrun channel cleanup must work");
        if panic {
            failures.push(case);
        }
    }
    sweep_leftovers(&client, guild).await;
    let remaining = client
        .guild_channels(guild)
        .await
        .expect("named zero-leftover check must list channels")
        .model()
        .await
        .expect("named zero-leftover check must decode channels")
        .into_iter()
        .filter(|channel| {
            channel
                .name
                .as_deref()
                .is_some_and(|name| name.starts_with(TESTRUN_PREFIX))
        })
        .count();
    assert_eq!(remaining, 0, "named zero-leftover check");
    assert!(
        failures.is_empty(),
        "Discord stubs did not fail red: {failures:?}"
    );
}
