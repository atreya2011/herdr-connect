#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::update_live_status;
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn live_status_updates_a_real_channel() {
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
        let channel = channel(guild, "testrun-live-status").await?;
        update_live_status(guild.client.as_ref(), channel, "testrun-terminal", None).await?;
        update_live_status(guild.client.as_ref(), channel, "testrun-terminal", None).await
    }
}
