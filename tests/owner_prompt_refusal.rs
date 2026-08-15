#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::handle_owner_message;
    use serial_test::serial;
    use std::sync::Arc;
    use twilight_model::id::{Id, marker::UserMarker};
    use twilight_model::user::User;

    #[tokio::test]
    #[serial]
    async fn owner_prompt_refuses_an_unmapped_real_channel() {
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
        let channel = channel(guild, "testrun-owner-refusal").await?;
        let mut message = guild
            .client
            .create_message(channel)
            .content("testrun owner input")
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        message.author = User {
            id: owner,
            name: "owner-test".into(),
            bot: false,
            discriminator: 0,
            accent_color: None,
            avatar: None,
            avatar_decoration: None,
            avatar_decoration_data: None,
            banner: None,
            email: None,
            flags: None,
            global_name: None,
            locale: None,
            mfa_enabled: None,
            premium_type: None,
            primary_guild: None,
            public_flags: None,
            system: None,
            verified: None,
        };
        handle_owner_message(
            Arc::clone(&guild.client),
            guild.id,
            &owner.to_string(),
            message,
        )
        .await
    }
}
