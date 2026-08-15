//! Minimal real-service contract suite for the shipped bridge.
//!
//! Herdr tests use the running socket, Discord tests use the configured guild, and vendor tests
//! use committed records shaped like real on-disk logs. Archived pagination across multiple API
//! pages and accepted owner prompts remain orchestrator live proof because they require a
//! scripted counterparty or a live owner event. The real-guild suite covers archived reuse and
//! the owner-prompt refusal path without prompting a live agent.

mod config_and_names {
    use herdr_connect_rs::{
        format_thread_name, load_config, load_discord_config, workspace_channel_name,
    };

    #[test]
    fn config_and_names() {
        let configs = [
            (
                vec![("HERDR_SOCKET_PATH", "/run/herdr.sock")],
                "/run/herdr.sock",
            ),
            (
                vec![("OTHER", "ignored")],
                "/home/u/.config/herdr/herdr.sock",
            ),
        ];
        for (environment, socket) in configs {
            let actual = load_config(&environment, "/home/u");
            assert_eq!(actual.herdr_socket_path, socket);
            assert_eq!(actual.poll_interval_ms, 1_500);
        }
        let discord = [
            (
                vec![
                    ("DISCORD_TOKEN", "token"),
                    ("DISCORD_GUILD_ID", "guild"),
                    ("DISCORD_OWNER_ID", "owner"),
                ],
                true,
            ),
            (vec![("DISCORD_GUILD_ID", "guild")], false),
        ];
        for (environment, valid) in discord {
            assert_eq!(load_discord_config(&environment).is_ok(), valid);
        }

        let thread_names = [
            ("build", "ignored", "tab-1", Ok("build [tab-1]".to_owned())),
            (
                "123",
                "terminal title",
                "tab-2",
                Ok("terminal title [tab-2]".to_owned()),
            ),
        ];
        for (label, title, tab, expected) in thread_names {
            assert_eq!(format_thread_name(label, title, tab), expected);
        }
        let channels = [(
            "ws",
            vec!["/repo/zeta".to_owned(), "/repo/alpha".to_owned()],
            "alpha-ws",
        )];
        for (workspace, cwds, expected) in channels {
            assert_eq!(workspace_channel_name(workspace, &cwds).unwrap(), expected);
        }
    }
}

mod routing {
    use herdr_connect_rs::{AgentSnapshot, HerdrTab, route_topology};
    use serde_json::Value;

    fn snapshot<T: serde::de::DeserializeOwned>(name: &str, key: &str) -> T {
        let value: Value = serde_json::from_str(match name {
            "agents" => include_str!("fixtures/herdr-agent-list.json"),
            "tabs" => include_str!("fixtures/herdr-tab-list.json"),
            _ => unreachable!(),
        })
        .expect("captured snapshot is JSON");
        serde_json::from_value(value["result"][key].clone()).expect("captured shape is valid")
    }

    #[test]
    fn route_captured_herdr_snapshots() {
        let agents: Vec<AgentSnapshot> = snapshot("agents", "agents");
        let tabs: Vec<HerdrTab> = snapshot("tabs", "tabs");
        let cases = [
            ("term-real-1", "real-workspace:pane-1"),
            ("term-real-2", "real-workspace:pane-2"),
        ];
        for (terminal, pane) in cases {
            let route = route_topology(&agents, &tabs, terminal).unwrap();
            assert_eq!(route.workspace_id, "real-workspace");
            assert_eq!(route.tab_id, "real-workspace:tab-1");
            assert_eq!(route.pane_id, pane);
            assert_eq!(route.channel_name, "bridge-real-workspace");
            assert_eq!(route.thread_name, "bridge [real-workspace:tab-1]");
        }
    }
}

mod vendor_logs {
    use herdr_connect_rs::{AgentSession, read_agent_log};
    use std::path::Path;

    #[test]
    fn read_captured_vendor_logs() {
        let cases = [
            (
                "claude",
                AgentSession {
                    agent: "claude".into(),
                    value: "session".into(),
                },
                "tests/fixtures/claude-session.jsonl",
                "final answer",
            ),
            (
                "codex",
                AgentSession {
                    agent: "codex".into(),
                    value: "session".into(),
                },
                "tests/fixtures/codex-session.jsonl",
                "final answer",
            ),
            (
                "cursor",
                AgentSession {
                    agent: "cursor".into(),
                    value: "session".into(),
                },
                "tests/fixtures/cursor-session.json",
                "final cursor",
            ),
        ];
        for (_, session, path, expected) in cases {
            assert_eq!(
                read_agent_log(Some(session), Path::new(path))
                    .unwrap()
                    .message,
                expected
            );
        }
    }
}

mod socket {
    use herdr_connect_rs::request_rpc_result;
    use serde_json::Value;

    #[test]
    fn real_socket_read_only_contract() {
        if std::env::var_os("HERDR_SOCKET_PATH").is_none() {
            eprintln!("skipped: HERDR_SOCKET_PATH is not configured");
            return;
        }
        let cases = [("agent.list", "agents"), ("tab.list", "tabs")];
        for (method, key) in cases {
            let value: Value = serde_json::from_str(
                &request_rpc_result(method).expect("real Herdr read-only method succeeds"),
            )
            .expect("real Herdr result is JSON");
            assert!(value.get(key).is_some_and(Value::is_array));
        }
        let error = request_rpc_result("method.invalid.for.test")
            .expect_err("invalid method returns the real error envelope");
        assert!(error.contains("herdr method.invalid.for.test failed"));
    }
}

#[cfg(unix)]
mod discord {
    use herdr_connect_rs::{
        AgentLogCapture, Transition, create_transition_messages, deliver_transition,
        deliver_transition_card, handle_owner_message, sync_topology, update_live_status,
    };
    use serial_test::serial;
    use std::sync::Arc;
    use twilight_http::Client;
    use twilight_model::{
        channel::ChannelType,
        id::{
            Id,
            marker::{ChannelMarker, GuildMarker, UserMarker},
        },
        user::User,
    };

    const PREFIX: &str = "testrun-";

    struct Guild {
        client: Arc<Client>,
        id: Id<GuildMarker>,
        owner: Id<UserMarker>,
    }

    fn guild() -> Option<Guild> {
        Some(Guild {
            client: Arc::new(
                Client::builder()
                    .token(std::env::var("DISCORD_TOKEN").ok()?)
                    .timeout(std::time::Duration::from_secs(30))
                    .build(),
            ),
            id: Id::new(std::env::var("DISCORD_GUILD_ID").ok()?.parse().ok()?),
            owner: Id::new(std::env::var("DISCORD_OWNER_ID").ok()?.parse().ok()?),
        })
    }

    async fn cleanup(guild: &Guild) -> Result<usize, String> {
        let channels = guild
            .client
            .guild_channels(guild.id)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        for channel in channels.into_iter().filter(|channel| {
            channel
                .name
                .as_deref()
                .is_some_and(|name| name.starts_with(PREFIX))
        }) {
            guild
                .client
                .delete_channel(channel.id)
                .await
                .map_err(|e| e.to_string())?;
        }
        Ok(guild
            .client
            .guild_channels(guild.id)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|channel| {
                channel
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with(PREFIX))
            })
            .count())
    }

    async fn channel(guild: &Guild, name: &str) -> Result<Id<ChannelMarker>, String> {
        Ok(guild
            .client
            .create_guild_channel(guild.id, name)
            .kind(ChannelType::GuildText)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?
            .id)
    }

    #[tokio::test]
    #[serial]
    async fn real_guild_required_behaviors() {
        let Some(guild) = guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let cases = ["topology", "delivery", "duplicate", "owner refusal"];
        for case in cases {
            let result = match case {
                "topology" => topology(&guild).await,
                "delivery" => delivery(&guild).await,
                "duplicate" => duplicate(&guild).await,
                "owner refusal" => owner_refusal(&guild).await,
                _ => unreachable!(),
            };
            let left = cleanup(&guild).await.unwrap();
            assert!(result.is_ok(), "{case}: {result:?}");
            assert_eq!(left, 0, "named zero-leftover check: {case}");
        }
    }

    async fn topology(guild: &Guild) -> Result<(), String> {
        let first = sync_topology(
            &guild.client,
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
            &guild.client,
            guild.id,
            "testrun-ws",
            "testrun-ws",
            "changed [testrun-tab]",
            "testrun-tab",
        )
        .await?;
        if first != second {
            return Err("archived thread was not reused".into());
        }
        update_live_status(&guild.client, second, "testrun-terminal", None).await
    }

    async fn delivery(guild: &Guild) -> Result<(), String> {
        let channel = channel(guild, "testrun-delivery").await?;
        let first =
            deliver_transition(&guild.client, channel, "retry-safe", "testrun-nonce").await?;
        let second =
            deliver_transition(&guild.client, channel, "retry-safe", "testrun-nonce").await?;
        if first != second {
            return Err("duplicate nonce created two messages".into());
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
            &guild.owner.to_string(),
        )
        .remove(0);
        deliver_transition_card(&guild.client, channel, &card, "testrun-card")
            .await
            .map(|_| ())
    }

    async fn duplicate(guild: &Guild) -> Result<(), String> {
        let first = channel(guild, "testrun-duplicate-one").await?;
        guild
            .client
            .update_channel(first)
            .topic("herdr workspace [testrun-duplicate]")
            .await
            .map_err(|e| e.to_string())?;
        guild
            .client
            .create_guild_channel(guild.id, "testrun-duplicate-two")
            .kind(ChannelType::GuildText)
            .topic("herdr workspace [testrun-duplicate]")
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        let error = sync_topology(
            &guild.client,
            guild.id,
            "testrun-duplicate",
            "testrun-duplicate-one",
            "tab [testrun-tab]",
            "testrun-tab",
        )
        .await
        .expect_err("duplicate channels are refused");
        if error.contains("duplicate channels") {
            Ok(())
        } else {
            Err(error)
        }
    }

    async fn owner_refusal(guild: &Guild) -> Result<(), String> {
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
            id: guild.owner,
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
            &guild.owner.to_string(),
            message,
        )
        .await
    }
}
