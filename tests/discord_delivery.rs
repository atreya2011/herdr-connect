#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::{
        AgentLogCapture, Transition, create_transition_messages, deliver_transition,
        deliver_transition_card,
    };
    use serial_test::serial;
    use twilight_model::id::{Id, marker::UserMarker};

    #[tokio::test]
    #[serial]
    async fn delivery_nonce_and_card() {
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
        let channel = channel(guild, "testrun-delivery").await?;
        let first = deliver_transition(
            guild.client.as_ref(),
            channel,
            "retry-safe",
            "testrun-nonce",
        )
        .await?;
        let second = deliver_transition(
            guild.client.as_ref(),
            channel,
            "retry-safe",
            "testrun-nonce",
        )
        .await?;
        if first != second {
            return Err("duplicate nonce created two messages".to_owned());
        }
        let card = create_transition_messages(
            &Transition {
                from: "working".into(),
                to: "blocked".into(),
                terminal_id: "testrun-terminal".into(),
                agent: "claude".into(),
            },
            &AgentLogCapture {
                message: "blocked".into(),
                failure: None,
                question: Some("choose".into()),
            },
            &owner.to_string(),
        )
        .into_iter()
        .next()
        .ok_or_else(|| "transition card was empty".to_owned())?;
        deliver_transition_card(guild.client.as_ref(), channel, &card, "testrun-card")
            .await
            .map(|_| ())
    }
}
