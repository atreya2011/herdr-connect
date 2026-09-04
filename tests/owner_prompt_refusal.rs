use herdr_connect_rs::should_handle_owner_message;

#[test]
fn owner_filter_cases() {
    let cases = [
        ("42", false, "42", true),
        ("41", false, "42", false),
        ("42", true, "42", false),
        ("42", false, "", false),
        ("42", false, " 42 ", true),
    ];
    for (author_id, is_bot, owner_id, expected) in cases {
        assert_eq!(
            should_handle_owner_message(author_id, is_bot, owner_id),
            expected,
            "author_id={author_id}, is_bot={is_bot}, owner_id={owner_id:?}"
        );
    }
}

#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use serial_test::serial;
    use twilight_model::id::{Id, marker::UserMarker};

    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::should_handle_owner_message;

    #[tokio::test]
    #[serial]
    async fn bot_authored_message_is_ignored() {
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
        let owner = Id::<UserMarker>::new(
            std::env::var("DISCORD_OWNER_ID")
                .map_err(|e| e.to_string())?
                .parse::<u64>()
                .map_err(|e| e.to_string())?,
        );
        let channel = channel(guild, "testrun-owner-filter").await?;
        let message = guild
            .client
            .create_message(channel)
            .content("testrun bot input")
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        if !message.author.bot {
            return Err("real test message was not authored by the bot".to_owned());
        }
        if should_handle_owner_message(
            &message.author.id.to_string(),
            message.author.bot,
            &owner.to_string(),
        ) {
            return Err("bot-authored message was accepted by owner filter".to_owned());
        }
        Ok(())
    }
}
