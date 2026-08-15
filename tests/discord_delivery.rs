#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::{
        AgentLogCapture, Transition, create_transition_messages, deliver_permission_card,
        deliver_transition_card, transition_card_nonce,
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

    #[tokio::test]
    #[serial]
    async fn permission_card_has_allow_and_deny_components() {
        let Some(guild) = guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let result = permission_card_exercise(&guild).await;
        let left = cleanup(&guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(left, 0, "named zero-leftover check");
    }

    async fn permission_card_exercise(guild: &Guild) -> Result<(), String> {
        let parent = channel(guild, "testrun-permission").await?;
        let thread = guild
            .client
            .create_thread(
                parent,
                "testrun-permission-thread",
                twilight_model::channel::ChannelType::PublicThread,
            )
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?
            .id;
        let long_command = "x".repeat(4_097);
        let cases = [
            ("backtick run", "printf 'before ``` after'", false),
            ("long command", long_command.as_str(), true),
        ];
        for (name, command, truncated) in cases {
            let message = deliver_permission_card(
                guild.client.as_ref(),
                thread,
                "Bash",
                command,
                "opaque-token-for-test",
            )
            .await?;
            let delivered = fetch_message(guild, thread, message).await?;
            let description = delivered
                .embeds
                .first()
                .and_then(|embed| embed.description.as_deref())
                .ok_or_else(|| format!("{name} permission card had no description"))?;
            if description.chars().count() > 3_800 {
                return Err(format!("{name} permission description was not bounded"));
            }
            if description.matches("```").count() != 2 {
                return Err(format!(
                    "{name} permission description broke its code fence"
                ));
            }
            if description.contains('…') != truncated {
                return Err(format!(
                    "{name} permission description truncation was wrong"
                ));
            }
            if delivered.components.len() != 1 {
                return Err("permission card did not have one action row".to_owned());
            }
            let twilight_model::channel::message::component::Component::ActionRow(row) =
                &delivered.components[0]
            else {
                return Err("permission card component was not an action row".to_owned());
            };
            if row.components.len() != 2 {
                return Err("permission card did not have two buttons".to_owned());
            }
            let labels = row
                .components
                .iter()
                .filter_map(|component| match component {
                    twilight_model::channel::message::component::Component::Button(button) => {
                        button.label.as_deref()
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            if labels != ["Allow", "Deny"] {
                return Err(format!("unexpected permission labels: {labels:?}"));
            }
        }
        Ok(())
    }

    async fn exercise(guild: &Guild) -> Result<(), String> {
        let owner = Id::<UserMarker>::new(
            std::env::var("DISCORD_OWNER_ID")
                .map_err(|e| e.to_string())?
                .parse::<u64>()
                .map_err(|e| e.to_string())?,
        );
        let channel = channel(guild, "testrun-delivery").await?;
        let test_terminal = format!("testrun-terminal-{}", channel.get());
        let retry_nonce = transition_card_nonce(&test_terminal, 1, 99);
        let first_card = create_transition_messages(
            &Transition {
                from: "working".into(),
                to: "blocked".into(),
                terminal_id: test_terminal.clone(),
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
        let first =
            deliver_transition_card(guild.client.as_ref(), channel, &first_card, &retry_nonce)
                .await?;
        let delivered_blocked = fetch_message(guild, channel, first).await?;
        assert_owner_mention(&delivered_blocked, owner)?;
        let second =
            deliver_transition_card(guild.client.as_ref(), channel, &first_card, &retry_nonce)
                .await?;
        if first != second {
            return Err("duplicate nonce created two messages".to_owned());
        }
        let second_card = create_transition_messages(
            &Transition {
                from: "blocked".into(),
                to: "idle".into(),
                terminal_id: test_terminal.clone(),
                agent: "claude".into(),
            },
            &AgentLogCapture {
                message: "idle".into(),
                failure: None,
                question: None,
            },
            &owner.to_string(),
        )
        .into_iter()
        .next()
        .ok_or_else(|| "second transition card was empty".to_owned())?;
        let first_card_nonce = transition_card_nonce(&test_terminal, 1, 0);
        let second_card_nonce = transition_card_nonce(&test_terminal, 2, 0);
        if first_card_nonce.len() > 25 || second_card_nonce.len() > 25 {
            return Err("delivery nonce exceeded Discord's limit".to_owned());
        }
        let first_card_id = deliver_transition_card(
            guild.client.as_ref(),
            channel,
            &first_card,
            &first_card_nonce,
        )
        .await?;
        let retry_card_id = deliver_transition_card(
            guild.client.as_ref(),
            channel,
            &first_card,
            &first_card_nonce,
        )
        .await?;
        if first_card_id != retry_card_id {
            return Err("retry of one card created two messages".to_owned());
        }
        let second_card_id = deliver_transition_card(
            guild.client.as_ref(),
            channel,
            &second_card,
            &second_card_nonce,
        )
        .await?;
        let delivered_idle = fetch_message(guild, channel, second_card_id).await?;
        assert_no_user_mentions(&delivered_idle)?;
        if first_card_id == second_card_id {
            return Err("distinct cards reused one message".to_owned());
        }
        Ok(())
    }

    async fn fetch_message(
        guild: &Guild,
        channel: Id<twilight_model::id::marker::ChannelMarker>,
        message: Id<twilight_model::id::marker::MessageMarker>,
    ) -> Result<twilight_model::channel::Message, String> {
        guild
            .client
            .message(channel, message)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())
    }

    fn assert_owner_mention(
        message: &twilight_model::channel::Message,
        owner: Id<UserMarker>,
    ) -> Result<(), String> {
        if message.mentions.iter().any(|mention| mention.id == owner) {
            Ok(())
        } else {
            Err("blocked card did not mention the owner".to_owned())
        }
    }

    fn assert_no_user_mentions(message: &twilight_model::channel::Message) -> Result<(), String> {
        if message.mentions.is_empty() {
            Ok(())
        } else {
            Err("non-blocked card mentioned a user".to_owned())
        }
    }
}
