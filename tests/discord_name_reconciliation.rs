#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use serial_test::serial;
    use twilight_model::id::{Id, marker::ChannelMarker};

    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::{
        Chat, ChatScope, Discord, TopologyRoute, format_thread_name, tab_list_result,
        workspace_channel_name, workspace_list_result,
    };

    struct Fixture {
        workspace_id: String,
        tab_id: String,
        pane_id: String,
        dir: std::path::PathBuf,
    }

    fn herdr(args: &[&str]) -> Result<serde_json::Value, String> {
        let output = Command::new("herdr")
            .args(args)
            .output()
            .map_err(|error| error.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "herdr {args:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
    }

    fn create_fixture(label: &str, tab_label: &str) -> Result<Fixture, String> {
        let dir = std::path::PathBuf::from(std::env::var("HOME").map_err(|e| e.to_string())?)
            .join(".cache/herdr-connect-testrun")
            .join(label);
        std::fs::create_dir_all(&dir).map_err(|error| error.to_string())?;
        let created = herdr(&[
            "workspace",
            "create",
            "--cwd",
            dir.to_str().ok_or("cwd is not UTF-8")?,
            "--label",
            label,
            "--no-focus",
        ])?;
        let text = |pointer: &str| {
            created
                .pointer(pointer)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| format!("herdr workspace create result missing {pointer}"))
        };
        let fixture = Fixture {
            workspace_id: text("/result/workspace/workspace_id")?,
            tab_id: text("/result/tab/tab_id")?,
            pane_id: text("/result/root_pane/pane_id")?,
            dir,
        };
        herdr(&["tab", "rename", &fixture.tab_id, tab_label])?;
        Ok(fixture)
    }

    fn remove_fixture(fixture: &Fixture) -> Result<(), String> {
        if !workspace_list_result()?.iter().any(|workspace| {
            workspace.workspace_id == fixture.workspace_id
                && workspace.label.starts_with("testrun-")
        }) {
            return Err("refused to close a non-testrun workspace".to_owned());
        }
        let closed = herdr(&["workspace", "close", &fixture.workspace_id]).map(drop);
        let removed = std::fs::remove_dir_all(&fixture.dir).map_err(|error| error.to_string());
        closed.and(removed)
    }

    fn discord(guild: &Guild) -> Result<Discord, String> {
        Ok(Discord::new(
            Arc::clone(&guild.client),
            std::env::var("DISCORD_TOKEN").map_err(|error| error.to_string())?,
            guild.id,
            std::env::var("DISCORD_OWNER_ID").map_err(|error| error.to_string())?,
            Arc::new(tokio::sync::Mutex::new(None)),
        )
        .with_scope(ChatScope::TestRuns))
    }

    async fn names(guild: &Guild, thread: Id<ChannelMarker>) -> Result<(String, String), String> {
        let thread = guild
            .client
            .channel(thread)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        let parent = guild
            .client
            .channel(thread.parent_id.ok_or("thread has no parent")?)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        Ok((
            parent.name.unwrap_or_default(),
            thread.name.unwrap_or_default(),
        ))
    }

    async fn wait_for_names(
        guild: &Guild,
        thread: Id<ChannelMarker>,
        channel_name: &str,
        thread_name: &str,
    ) -> Result<(), String> {
        let started = Instant::now();
        loop {
            let seen = names(guild, thread).await?;
            if seen == (channel_name.to_owned(), thread_name.to_owned()) {
                return Ok(());
            }
            if started.elapsed() > Duration::from_secs(60) {
                return Err(format!(
                    "Discord names stayed {seen:?}, wanted {channel_name:?} and {thread_name:?}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }

    fn route(fixture: &Fixture, label: &str, tab_label: &str) -> Result<TopologyRoute, String> {
        Ok(TopologyRoute {
            workspace_id: fixture.workspace_id.clone(),
            tab_id: fixture.tab_id.clone(),
            pane_id: fixture.pane_id.clone(),
            channel_name: workspace_channel_name(&fixture.workspace_id, label)?,
            thread_name: format_thread_name(tab_label, &fixture.tab_id)?,
        })
    }

    fn nonce() -> Result<String, String> {
        Ok(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .subsec_nanos()
            .to_string())
    }

    async fn run<F, Fut>(exercise: F)
    where
        F: FnOnce(Guild) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let Some(guild) = guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let cleanup_guild = Guild {
            client: Arc::clone(&guild.client),
            id: guild.id,
        };
        let result = exercise(guild).await;
        let left = cleanup(&cleanup_guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(left, 0, "named zero-leftover check");
    }

    #[tokio::test]
    #[serial]
    async fn renaming_a_workspace_and_tab_in_herdr_renames_the_channel_and_thread() {
        run(|guild| async move {
            let n = nonce()?;
            let label = format!("testrun-names-{n}");
            let first_tab = format!("testrun-first-{n}");
            let fixture = create_fixture(&label, &first_tab)?;
            let result = async {
                let chat = discord(&guild)?;
                let thread = chat
                    .sync_route(&route(&fixture, &label, &first_tab)?)
                    .await
                    .map_err(|error| error.to_string())?;
                let renamed_label = format!("testrun-Renamed Name {n}");
                let renamed_tab = format!("testrun-second-{n}");
                herdr(&["workspace", "rename", &fixture.workspace_id, &renamed_label])?;
                herdr(&["tab", "rename", &fixture.tab_id, &renamed_tab])?;
                chat.topology_changed()
                    .await
                    .map_err(|error| error.to_string())?;
                wait_for_names(
                    &guild,
                    thread,
                    &format!("testrun-renamed-name-{n}"),
                    &format!("{renamed_tab} [{}]", fixture.tab_id),
                )
                .await
            }
            .await;
            let removed = remove_fixture(&fixture);
            result.and(removed)
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn startup_renames_existing_names_that_differ_from_herdr() {
        run(|guild| async move {
            let n = nonce()?;
            let label = format!("testrun-startup-{n}");
            let tab = format!("testrun-tab-{n}");
            let fixture = create_fixture(&label, &tab)?;
            let result = async {
                let stale = channel(&guild, &format!("testrun-stale-{n}")).await?;
                guild
                    .client
                    .update_channel(stale)
                    .topic(&format!("herdr workspace [{}]", fixture.workspace_id))
                    .await
                    .map_err(|error| error.to_string())?;
                let stale_route = TopologyRoute {
                    thread_name: format!("testrun-stale-thread-{n} [{}]", fixture.tab_id),
                    ..route(&fixture, &label, &tab)?
                };
                let thread = discord(&guild)?
                    .sync_route(&stale_route)
                    .await
                    .map_err(|error| error.to_string())?;
                assert_eq!(
                    names(&guild, thread).await?,
                    (
                        format!("testrun-stale-{n}"),
                        stale_route.thread_name.clone()
                    )
                );
                let restarted = discord(&guild)?;
                restarted
                    .startup_sweep(vec![Ok(route(&fixture, &label, &tab)?)], || {
                        Ok((
                            workspace_list_result()?
                                .into_iter()
                                .map(|workspace| workspace.workspace_id)
                                .collect(),
                            tab_list_result()?
                                .into_iter()
                                .map(|listed| listed.tab_id)
                                .collect(),
                        ))
                    })
                    .await;
                wait_for_names(
                    &guild,
                    thread,
                    &label,
                    &format!("{tab} [{}]", fixture.tab_id),
                )
                .await
            }
            .await;
            let removed = remove_fixture(&fixture);
            result.and(removed)
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn a_rename_waiting_on_the_rate_limit_does_not_delay_messages() {
        run(|guild| async move {
            let n = nonce()?;
            let label = format!("testrun-limit-{n}");
            let first_tab = format!("testrun-one-{n}");
            let fixture = create_fixture(&label, &first_tab)?;
            let result = async {
                let chat = discord(&guild)?;
                let thread = chat
                    .sync_route(&route(&fixture, &label, &first_tab)?)
                    .await
                    .map_err(|error| error.to_string())?;
                for step in ["two", "three"] {
                    let tab = format!("testrun-{step}-{n}");
                    herdr(&["tab", "rename", &fixture.tab_id, &tab])?;
                    chat.topology_changed()
                        .await
                        .map_err(|error| error.to_string())?;
                    wait_for_names(
                        &guild,
                        thread,
                        &label,
                        &format!("{tab} [{}]", fixture.tab_id),
                    )
                    .await?;
                }
                herdr(&[
                    "tab",
                    "rename",
                    &fixture.tab_id,
                    &format!("testrun-four-{n}"),
                ])?;
                let started = Instant::now();
                chat.topology_changed()
                    .await
                    .map_err(|error| error.to_string())?;
                chat.post_live(&thread, "delivered while the rename waits", &n)
                    .await
                    .map_err(|error| error.to_string())?;
                assert!(
                    started.elapsed() < Duration::from_secs(5),
                    "delivery waited"
                );
                let delivered = guild
                    .client
                    .channel_messages(thread)
                    .await
                    .map_err(|error| error.to_string())?
                    .models()
                    .await
                    .map_err(|error| error.to_string())?
                    .iter()
                    .any(|message| message.content == "delivered while the rename waits");
                assert!(delivered, "message did not arrive");
                tokio::time::sleep(Duration::from_secs(3)).await;
                assert_eq!(
                    names(&guild, thread).await?.1,
                    format!("testrun-three-{n} [{}]", fixture.tab_id),
                    "the third rename is still waiting"
                );
                Ok(())
            }
            .await;
            let removed = remove_fixture(&fixture);
            result.and(removed)
        })
        .await;
    }
}
