//! Multi-page archived pagination remains orchestrator live proof; this file covers real-guild
//! topology creation, archived reuse, and the duplicate-channel guard.

#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::sync_topology;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn topology_sync_and_duplicate_guard() {
        let Some(guild) = guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let result = exercise(&guild).await;
        let left = cleanup(&guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(left, 0, "named zero-leftover check");
    }

    async fn exercise(guild: &Guild) -> Result<(), String> {
        let first = sync_topology(
            guild.client.as_ref(),
            guild.id,
            "testrun-ws",
            "testrun-ws",
            "tab [testrun-tab]",
            "testrun-tab",
        )
        .await?;
        guild
            .client
            .update_thread(first)
            .archived(true)
            .await
            .map_err(|e| e.to_string())?;
        let second = sync_topology(
            guild.client.as_ref(),
            guild.id,
            "testrun-ws",
            "testrun-ws",
            "changed [testrun-tab]",
            "testrun-tab",
        )
        .await?;
        if first != second {
            return Err("archived thread was not reused".to_owned());
        }

        let duplicate = channel(guild, "testrun-duplicate-one").await?;
        guild
            .client
            .update_channel(duplicate)
            .topic("herdr workspace [testrun-duplicate]")
            .await
            .map_err(|e| e.to_string())?;
        guild
            .client
            .create_guild_channel(guild.id, "testrun-duplicate-two")
            .kind(twilight_model::channel::ChannelType::GuildText)
            .topic("herdr workspace [testrun-duplicate]")
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        let Err(error) = sync_topology(
            guild.client.as_ref(),
            guild.id,
            "testrun-duplicate",
            "testrun-duplicate-one",
            "tab [testrun-tab]",
            "testrun-tab",
        )
        .await
        else {
            return Err("duplicate channels were not refused".to_owned());
        };
        if error.contains("duplicate channels") {
            Ok(())
        } else {
            Err(error)
        }
    }
}
