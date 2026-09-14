#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use serial_test::serial;
    use twilight_model::channel::message::component::Component::{ActionRow, Button};
    use twilight_model::id::{
        Id,
        marker::{ChannelMarker, UserMarker},
    };

    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::{
        AgentLogCapture, PermissionVendor, TopologyRoute, Transition, create_transition_messages,
        deliver_permission_card, deliver_terminal_prompt, deliver_transition_card,
        fetch_owner_identity, fetch_topology_lists, sync_topology, transition_card_nonce,
    };

    const TERMINAL_WEBHOOK_NAME: &str = "herdr-connect-rs-terminal-prompts";
    const FIRST_TERMINAL_PROMPT: &str = "testrun terminal prompt one";
    const SECOND_TERMINAL_PROMPT: &str = "testrun terminal prompt two";

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

    async fn permission_card_exercise(guild: &Guild) -> Result<(), String> {
        let channel = channel(guild, "testrun-permission").await?;
        let message = deliver_permission_card(
            guild.client.as_ref(),
            channel,
            PermissionVendor::Claude,
            "Bash",
            "printf 'permission'",
            "opaque-token-for-test",
        )
        .await?;
        let delivered = fetch_message(guild, channel, message).await?;
        let ActionRow(row) = delivered
            .components
            .first()
            .ok_or("permission card had no row")?
        else {
            return Err("permission card component was not an action row".to_owned());
        };
        let labels = row
            .components
            .iter()
            .filter_map(|component| match component {
                Button(button) => button.label.as_deref(),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(labels, ["Allow", "Deny"]);
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
        permission_card_exercise(guild).await?;
        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn terminal_prompt_webhook_delivery_reuses_one_workspace_webhook() {
        let Some(guild) = guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let mut workspace_channel = None;
        let result = terminal_prompt_webhook_exercise(&guild, &mut workspace_channel).await;
        let webhook_left = cleanup_terminal_webhook(&guild, workspace_channel).await;
        let channel_left = cleanup(&guild).await;
        assert!(webhook_left.is_ok(), "{webhook_left:?}");
        assert!(channel_left.is_ok(), "{channel_left:?}");
        assert_eq!(
            webhook_left.unwrap(),
            0,
            "named zero-webhook-leftover check"
        );
        assert_eq!(channel_left.unwrap(), 0, "named zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
    }

    async fn terminal_prompt_webhook_exercise(
        guild: &Guild,
        workspace_channel_out: &mut Option<Id<ChannelMarker>>,
    ) -> Result<(), String> {
        let route = TopologyRoute {
            workspace_id: "testrun-terminal-prompts".to_owned(),
            tab_id: "testrun-terminal-prompts-tab".to_owned(),
            pane_id: "testrun-terminal-prompts-pane".to_owned(),
            channel_name: "testrun-terminal-prompts".to_owned(),
            thread_name: "testrun-terminal-prompts [testrun-terminal-prompts-tab]".to_owned(),
        };
        let (mut channels, mut active_threads) =
            fetch_topology_lists(guild.client.as_ref(), guild.id).await?;
        let thread = sync_topology(
            guild.client.as_ref(),
            guild.id,
            &mut channels,
            &mut active_threads,
            &route,
        )
        .await?;
        let topic = format!("herdr workspace [{}]", route.workspace_id);
        let workspace_channels = channels
            .iter()
            .filter(|channel| channel.topic.as_deref() == Some(topic.as_str()))
            .collect::<Vec<_>>();
        if workspace_channels.len() != 1 {
            return Err(format!(
                "expected one workspace channel, found {}",
                workspace_channels.len()
            ));
        }
        let workspace_channel = workspace_channels[0].id;
        *workspace_channel_out = Some(workspace_channel);
        let owner_id = Id::<UserMarker>::new(
            std::env::var("DISCORD_OWNER_ID")
                .map_err(|e| e.to_string())?
                .parse::<u64>()
                .map_err(|e| e.to_string())?,
        );
        let identity = fetch_owner_identity(guild.client.as_ref(), owner_id).await?;
        let (first, second) = terminal_prompt_messages(
            guild,
            workspace_channel,
            thread,
            &identity.display_name,
            identity.avatar_url.as_deref(),
        )
        .await?;
        let webhooks = guild
            .client
            .channel_webhooks(workspace_channel)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        let owned_webhooks = webhooks
            .iter()
            .filter(|webhook| webhook.name.as_deref() == Some(TERMINAL_WEBHOOK_NAME))
            .collect::<Vec<_>>();
        if owned_webhooks.len() != 1 {
            return Err(format!(
                "expected one bridge-owned webhook, found {}",
                owned_webhooks.len()
            ));
        }
        let webhook_id = owned_webhooks[0].id;
        if first.webhook_id != Some(webhook_id) {
            return Err("first prompt was not sent by the bridge-owned webhook".to_owned());
        }
        if second.webhook_id != Some(webhook_id) {
            return Err("second prompt was not sent by the bridge-owned webhook".to_owned());
        }
        let messages = guild
            .client
            .channel_messages(thread)
            .limit(100)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        let mirrored = messages
            .iter()
            .filter(|message| message.webhook_id == Some(webhook_id))
            .count();
        if mirrored != 2 {
            return Err(format!(
                "expected exactly two mirrored messages, found {mirrored}"
            ));
        }
        Ok(())
    }

    async fn terminal_prompt_messages(
        guild: &Guild,
        workspace_channel: Id<ChannelMarker>,
        thread: Id<ChannelMarker>,
        expected_display_name: &str,
        owner_avatar_url: Option<&str>,
    ) -> Result<
        (
            twilight_model::channel::Message,
            twilight_model::channel::Message,
        ),
        String,
    > {
        let first_id = deliver_terminal_prompt(
            guild.client.as_ref(),
            workspace_channel,
            thread,
            TERMINAL_WEBHOOK_NAME,
            expected_display_name,
            owner_avatar_url,
            FIRST_TERMINAL_PROMPT,
        )
        .await?;
        let first = fetch_message(guild, thread, first_id).await?;
        if first.content != FIRST_TERMINAL_PROMPT {
            return Err(format!(
                "first prompt content was {:?}, expected {FIRST_TERMINAL_PROMPT:?}",
                first.content
            ));
        }
        if first.author.name != expected_display_name {
            return Err(format!(
                "first prompt author was {:?}, expected {expected_display_name:?}",
                first.author.name
            ));
        }
        if owner_avatar_url.is_some() && first.author.avatar.is_none() {
            return Err("owner avatar was not mirrored".to_owned());
        }
        let second_id = deliver_terminal_prompt(
            guild.client.as_ref(),
            workspace_channel,
            thread,
            TERMINAL_WEBHOOK_NAME,
            expected_display_name,
            owner_avatar_url,
            SECOND_TERMINAL_PROMPT,
        )
        .await?;
        let second = fetch_message(guild, thread, second_id).await?;
        if first_id == second_id {
            return Err("two prompts reused one Discord message".to_owned());
        }
        if second.content != SECOND_TERMINAL_PROMPT {
            return Err(format!(
                "second prompt content was {:?}, expected {SECOND_TERMINAL_PROMPT:?}",
                second.content
            ));
        }
        if second.author.name != expected_display_name {
            return Err(format!(
                "second prompt author was {:?}, expected {expected_display_name:?}",
                second.author.name
            ));
        }
        if first.author.avatar != second.author.avatar {
            return Err("prompts used different webhook avatars".to_owned());
        }
        Ok((first, second))
    }

    async fn cleanup_terminal_webhook(
        guild: &Guild,
        workspace_channel: Option<Id<ChannelMarker>>,
    ) -> Result<usize, String> {
        let Some(workspace_channel) = workspace_channel else {
            return Ok(0);
        };
        let webhooks = guild
            .client
            .channel_webhooks(workspace_channel)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        for webhook in webhooks
            .into_iter()
            .filter(|webhook| webhook.name.as_deref() == Some(TERMINAL_WEBHOOK_NAME))
        {
            guild
                .client
                .delete_webhook(webhook.id)
                .await
                .map_err(|e| e.to_string())?;
        }
        let remaining = guild
            .client
            .channel_webhooks(workspace_channel)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        Ok(remaining
            .iter()
            .filter(|webhook| webhook.name.as_deref() == Some(TERMINAL_WEBHOOK_NAME))
            .count())
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
