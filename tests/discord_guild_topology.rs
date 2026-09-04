//! Multi-page archived pagination remains orchestrator live proof; this file covers real-guild
//! topology creation, archived reuse, and the duplicate-channel guard.

#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use serial_test::serial;

    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::{TopologyRoute, fetch_topology_lists, sync_topology};

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
        let route = TopologyRoute {
            workspace_id: "testrun-ws".to_owned(),
            tab_id: "testrun-tab".to_owned(),
            pane_id: "testrun-pane".to_owned(),
            channel_name: "testrun-ws".to_owned(),
            thread_name: "tab [testrun-tab]".to_owned(),
        };
        let (mut channels, mut active_threads) =
            fetch_topology_lists(guild.client.as_ref(), guild.id).await?;
        let first = sync_topology(
            guild.client.as_ref(),
            guild.id,
            &mut channels,
            &mut active_threads,
            &route,
        )
        .await?;
        guild
            .client
            .update_thread(first)
            .archived(true)
            .await
            .map_err(|e| e.to_string())?;
        let route = TopologyRoute {
            thread_name: "changed [testrun-tab]".to_owned(),
            ..route
        };
        let (mut channels, mut active_threads) =
            fetch_topology_lists(guild.client.as_ref(), guild.id).await?;
        let second = sync_topology(
            guild.client.as_ref(),
            guild.id,
            &mut channels,
            &mut active_threads,
            &route,
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
        let route = TopologyRoute {
            workspace_id: "testrun-duplicate".to_owned(),
            tab_id: "testrun-tab".to_owned(),
            pane_id: "testrun-pane".to_owned(),
            channel_name: "testrun-duplicate-one".to_owned(),
            thread_name: "tab [testrun-tab]".to_owned(),
        };
        let (mut channels, mut active_threads) =
            fetch_topology_lists(guild.client.as_ref(), guild.id).await?;
        let Err(error) = sync_topology(
            guild.client.as_ref(),
            guild.id,
            &mut channels,
            &mut active_threads,
            &route,
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

    #[tokio::test]
    #[serial]
    async fn stale_active_list_double_counts_an_archived_thread() {
        let Some(guild) = guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let result = stale_active_list_exercise(&guild).await;
        let left = cleanup(&guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(left, 0, "named zero-leftover check");
    }

    async fn stale_active_list_exercise(guild: &Guild) -> Result<(), String> {
        let route = TopologyRoute {
            workspace_id: "testrun-stale".to_owned(),
            tab_id: "testrun-stale-tab".to_owned(),
            pane_id: "testrun-stale-pane".to_owned(),
            channel_name: "testrun-stale".to_owned(),
            thread_name: "tab [testrun-stale-tab]".to_owned(),
        };
        let (mut channels, mut active_threads) =
            fetch_topology_lists(guild.client.as_ref(), guild.id).await?;
        let first = sync_topology(
            guild.client.as_ref(),
            guild.id,
            &mut channels,
            &mut active_threads,
            &route,
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
            &mut channels,
            &mut active_threads,
            &route,
        )
        .await?;
        if second != first {
            return Err("archived thread was not reused across the reused lists".to_owned());
        }
        Ok(())
    }
}
