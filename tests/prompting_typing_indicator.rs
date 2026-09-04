#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use serial_test::serial;
    use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _};

    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::maintain_typing_until_settled;

    #[tokio::test]
    #[serial]
    async fn typing_indicator_repeats_while_working_and_stops_once_settled() {
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
        let channel_id = channel(guild, "testrun-typing-indicator").await?;
        let token = std::env::var("DISCORD_TOKEN").map_err(|error| error.to_string())?;
        let intents = Intents::GUILDS | Intents::GUILD_MESSAGE_TYPING;
        let mut shard = Shard::new(ShardId::ONE, token, intents);
        loop {
            match shard.next_event(EventTypeFlags::READY).await {
                Some(Ok(Event::Ready(_))) => break,
                Some(Ok(_)) => {}
                Some(Err(error)) => return Err(error.to_string()),
                None => return Err("gateway closed before ready".to_owned()),
            }
        }

        let observed = Arc::new(AtomicUsize::new(0));
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let reader = {
            let observed = Arc::clone(&observed);
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = &mut stop_rx => break,
                        event = shard.next_event(EventTypeFlags::TYPING_START) => {
                            match event {
                                Some(Ok(Event::TypingStart(typing)))
                                    if typing.channel_id == channel_id =>
                                {
                                    observed.fetch_add(1, Ordering::SeqCst);
                                }
                                Some(Ok(_)) => {}
                                Some(Err(_)) | None => break,
                            }
                        }
                    }
                }
            })
        };

        let remaining = Arc::new(AtomicUsize::new(2));
        maintain_typing_until_settled(
            &guild.client,
            channel_id,
            Duration::from_millis(300),
            move || {
                let remaining = Arc::clone(&remaining);
                async move { Ok(remaining.fetch_sub(1, Ordering::SeqCst) > 1) }
            },
        )
        .await;

        tokio::time::sleep(Duration::from_millis(500)).await;
        let _ = stop_tx.send(());
        reader.await.map_err(|error| error.to_string())?;

        let seen = observed.load(Ordering::SeqCst);
        if seen != 2 {
            return Err(format!(
                "expected exactly two typing triggers before settling, saw {seen}"
            ));
        }
        Ok(())
    }
}
