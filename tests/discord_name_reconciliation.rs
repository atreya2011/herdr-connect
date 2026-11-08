#[cfg(unix)]
#[path = "support/discord.rs"]
mod support;

#[cfg(unix)]
mod real_guild {
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use serial_test::serial;
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
    use twilight_model::id::{
        Id,
        marker::{ChannelMarker, MessageMarker},
    };

    use super::support::{Guild, channel, cleanup, guild};
    use herdr_connect_rs::{
        Chat, ChatScope, Discord, GuildRename, Inbound, Renamed, TopologyCache, TopologyRoute,
        format_thread_name, tab_list_result, tab_rename, workspace_channel_name,
        workspace_list_result, workspace_rename,
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

    enum Seen {
        Posted(Id<MessageMarker>),
        Renamed(GuildRename),
    }

    fn read_events(chat: Arc<Discord>) -> (UnboundedReceiver<Seen>, tokio::task::JoinHandle<()>) {
        let (sender, events) = unbounded_channel();
        let task = tokio::spawn(async move {
            while let Some(event) = chat.next_event().await {
                let seen = match event {
                    Ok(Inbound::Posted(message)) => Seen::Posted(message.id),
                    Ok(Inbound::Renamed(rename)) => Seen::Renamed(rename),
                    Ok(_) => continue,
                    Err(_) => return,
                };
                if sender.send(seen).is_err() {
                    return;
                }
            }
        });
        (events, task)
    }

    async fn gateway_ready(
        guild: &Guild,
        thread: Id<ChannelMarker>,
        events: &mut UnboundedReceiver<Seen>,
    ) -> Result<(), String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        let mut sent = std::collections::HashSet::new();
        while tokio::time::Instant::now() < deadline {
            let message = guild
                .client
                .create_message(thread)
                .content("testrun rename gateway readiness")
                .await
                .map_err(|error| error.to_string())?
                .model()
                .await
                .map_err(|error| error.to_string())?;
            sent.insert(message.id);
            let tick = (tokio::time::Instant::now() + Duration::from_secs(2)).min(deadline);
            while let Ok(seen) = tokio::time::timeout_at(tick, events.recv()).await {
                if let Seen::Posted(id) = seen.ok_or("gateway reader stopped")?
                    && sent.contains(&id)
                {
                    return Ok(());
                }
            }
        }
        Err("Discord gateway did not deliver a readiness message".to_owned())
    }

    async fn next_rename(
        events: &mut UnboundedReceiver<Seen>,
        name: &str,
    ) -> Result<GuildRename, String> {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Seen::Renamed(rename) = events.recv().await.ok_or("gateway reader stopped")?
                    && matches!(&rename, GuildRename::Thread { name: seen, .. } | GuildRename::Channel { name: seen, .. } if seen == name)
                {
                    return Ok(rename);
                }
            }
        })
        .await
        .map_err(|_| format!("no rename event named {name:?} within 30 seconds"))?
    }

    async fn apply_owner_rename(
        chat: &Discord,
        workspace_id: &str,
        tab_id: &str,
        rename: GuildRename,
    ) -> Result<(String, String), String> {
        let (tab, workspace) = (tab_id.to_owned(), workspace_id.to_owned());
        chat.handle_rename(rename, move |renamed| match renamed {
            Renamed::Tab { tab_id, label } if tab_id == tab => tab_rename(&tab_id, &label),
            Renamed::Workspace {
                workspace_id,
                label,
            } if workspace_id == workspace => workspace_rename(&workspace_id, &label),
            other => Err(format!(
                "refused Herdr rename outside the fixture: {other:?}"
            )),
        })
        .await
        .map_err(|error| error.to_string())?;
        let tab = tab_list_result()?
            .into_iter()
            .find(|tab| tab.tab_id == tab_id)
            .ok_or("fixture tab disappeared")?;
        let workspace = workspace_list_result()?
            .into_iter()
            .find(|workspace| workspace.workspace_id == workspace_id)
            .ok_or("fixture workspace disappeared")?;
        Ok((workspace.label, tab.label))
    }

    fn second_route(
        fixture: &Fixture,
        label: &str,
        tab_label: &str,
    ) -> Result<TopologyRoute, String> {
        let created = herdr(&[
            "tab",
            "create",
            "--workspace",
            &fixture.workspace_id,
            "--cwd",
            fixture.dir.to_str().ok_or("cwd is not UTF-8")?,
            "--label",
            tab_label,
        ])?;
        let text = |pointer: &str| {
            created
                .pointer(pointer)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| format!("herdr tab create result missing {pointer}"))
        };
        let tab_id = text("/result/tab/tab_id")?;
        Ok(TopologyRoute {
            workspace_id: fixture.workspace_id.clone(),
            pane_id: text("/result/root_pane/pane_id")?,
            channel_name: workspace_channel_name(&fixture.workspace_id, label)?,
            thread_name: format_thread_name(tab_label, &tab_id)?,
            tab_id,
        })
    }

    async fn owner_as_bot(guild: &Guild, cache: TopologyCache) -> Result<Discord, String> {
        let bot = guild
            .client
            .current_user()
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .id;
        Ok(Discord::new(
            Arc::clone(&guild.client),
            std::env::var("DISCORD_TOKEN").map_err(|error| error.to_string())?,
            guild.id,
            bot.to_string(),
            cache,
        )
        .with_scope(ChatScope::TestRuns))
    }

    async fn rename_as_owner(
        guild: &Guild,
        target: Id<ChannelMarker>,
        thread: bool,
        name: &str,
    ) -> Result<(), String> {
        if thread {
            guild
                .client
                .update_thread(target)
                .name(name)
                .await
                .map(drop)
        } else {
            guild
                .client
                .update_channel(target)
                .name(name)
                .await
                .map(drop)
        }
        .map_err(|error| error.to_string())
    }

    struct Case {
        name: &'static str,
        target: Id<ChannelMarker>,
        thread: Id<ChannelMarker>,
        tab_id: String,
        renamed_to: String,
        labels: (String, String),
    }

    async fn check_case(
        guild: &Guild,
        creator: &Discord,
        owner: &Discord,
        events: &mut UnboundedReceiver<Seen>,
        workspace_id: &str,
        case: Case,
    ) -> Result<(), String> {
        rename_as_owner(
            guild,
            case.target,
            case.target == case.thread,
            &case.renamed_to,
        )
        .await?;
        let rename = next_rename(events, &case.renamed_to).await?;
        let labels = apply_owner_rename(owner, workspace_id, &case.tab_id, rename).await?;
        println!(
            "real discord rename {}: renamed_to={} herdr_labels={labels:?}",
            case.name, case.renamed_to
        );
        if labels != case.labels {
            return Err(format!(
                "{}: Herdr labels {labels:?}, wanted {:?}",
                case.name, case.labels
            ));
        }
        creator
            .topology_changed()
            .await
            .map_err(|error| error.to_string())?;
        let channel_name = workspace_channel_name(workspace_id, &labels.0)?;
        let thread_name = format_thread_name(&labels.1, &case.tab_id)?;
        wait_for_names(guild, case.thread, &channel_name, &thread_name).await?;
        println!(
            "real discord rename {}: chat names {channel_name:?} {thread_name:?}",
            case.name
        );
        let renamed_back = thread_name != case.renamed_to;
        if case.target == case.thread && renamed_back {
            let rename = next_rename(events, &thread_name).await?;
            let echoed = apply_owner_rename(owner, workspace_id, &case.tab_id, rename).await?;
            println!("real discord rename bridge echo: herdr_labels={echoed:?}");
            if echoed != labels {
                return Err(format!("bridge rename back changed Herdr to {echoed:?}"));
            }
        }
        Ok(())
    }

    async fn owner_renames(guild: Guild, n: String, fixture: &Fixture) -> Result<(), String> {
        let cache: TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let creator = Discord::new(
            Arc::clone(&guild.client),
            std::env::var("DISCORD_TOKEN").map_err(|error| error.to_string())?,
            guild.id,
            std::env::var("DISCORD_OWNER_ID").map_err(|error| error.to_string())?,
            Arc::clone(&cache),
        )
        .with_scope(ChatScope::TestRuns);
        let label = format!("testrun-owner-{n}");
        let thread = creator
            .sync_route(&route(fixture, &label, &format!("testrun-tab-{n}"))?)
            .await
            .map_err(|error| error.to_string())?;
        let second = second_route(fixture, &label, &format!("testrun-second-{n}"))?;
        let second_thread = creator
            .sync_route(&second)
            .await
            .map_err(|error| error.to_string())?;
        let parent = guild
            .client
            .channel(thread)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .parent_id
            .ok_or("thread has no parent")?;
        let owner = Arc::new(owner_as_bot(&guild, cache).await?);
        let (mut events, reader) = read_events(Arc::clone(&owner));
        let channel_label = format!("testrun-chan-{n}");
        let cases = [
            Case {
                name: "thread name keeps the tab suffix",
                target: second_thread,
                thread: second_thread,
                tab_id: second.tab_id.clone(),
                renamed_to: format!("testrun-kept-{n} [{}]", second.tab_id),
                labels: (label.clone(), format!("testrun-kept-{n}")),
            },
            Case {
                name: "thread name without the tab suffix",
                target: thread,
                thread,
                tab_id: fixture.tab_id.clone(),
                renamed_to: format!("testrun-whole-{n}"),
                labels: (label.clone(), format!("testrun-whole-{n}")),
            },
            Case {
                name: "workspace channel name",
                target: parent,
                thread,
                tab_id: fixture.tab_id.clone(),
                renamed_to: channel_label.clone(),
                labels: (channel_label, format!("testrun-whole-{n}")),
            },
        ];
        let result = async {
            gateway_ready(&guild, thread, &mut events).await?;
            for case in cases {
                check_case(
                    &guild,
                    &creator,
                    &owner,
                    &mut events,
                    &fixture.workspace_id,
                    case,
                )
                .await?;
            }
            Ok(())
        }
        .await;
        reader.abort();
        result
    }

    #[tokio::test]
    #[serial]
    async fn owner_renaming_a_thread_and_channel_renames_the_herdr_tab_and_workspace() {
        run(|guild| async move {
            let n = nonce()?;
            let fixture =
                create_fixture(&format!("testrun-owner-{n}"), &format!("testrun-tab-{n}"))?;
            let result = Box::pin(owner_renames(guild, n, &fixture)).await;
            let removed = remove_fixture(&fixture);
            result.and(removed)
        })
        .await;
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

    #[tokio::test]
    #[serial]
    async fn renaming_a_tab_back_while_its_rename_waits_keeps_the_original_name() {
        run(|guild| async move {
            let n = nonce()?;
            let label = format!("testrun-back-{n}");
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
                let kept = format!("testrun-three-{n}");
                herdr(&[
                    "tab",
                    "rename",
                    &fixture.tab_id,
                    &format!("testrun-four-{n}"),
                ])?;
                chat.topology_changed()
                    .await
                    .map_err(|error| error.to_string())?;
                tokio::time::sleep(Duration::from_secs(5)).await;
                herdr(&["tab", "rename", &fixture.tab_id, &kept])?;
                chat.topology_changed()
                    .await
                    .map_err(|error| error.to_string())?;
                tokio::time::sleep(Duration::from_secs(630)).await;
                assert_eq!(
                    names(&guild, thread).await?.1,
                    format!("{kept} [{}]", fixture.tab_id),
                    "the abandoned rename ran after the limit reset"
                );
                Ok(())
            }
            .await;
            let removed = remove_fixture(&fixture);
            result.and(removed)
        })
        .await;
    }

    #[tokio::test]
    #[serial]
    async fn a_thread_archived_before_its_tab_was_renamed_is_renamed_when_it_returns() {
        run(|guild| async move {
            let n = nonce()?;
            let label = format!("testrun-archived-{n}");
            let first_tab = format!("testrun-old-{n}");
            let fixture = create_fixture(&label, &first_tab)?;
            let result = async {
                let thread = discord(&guild)?
                    .sync_route(&route(&fixture, &label, &first_tab)?)
                    .await
                    .map_err(|error| error.to_string())?;
                guild
                    .client
                    .update_thread(thread)
                    .archived(true)
                    .await
                    .map_err(|error| error.to_string())?;
                let renamed_tab = format!("testrun-new-{n}");
                herdr(&["tab", "rename", &fixture.tab_id, &renamed_tab])?;
                let restarted = discord(&guild)?;
                restarted
                    .sync_route(&route(&fixture, &label, &renamed_tab)?)
                    .await
                    .map_err(|error| error.to_string())?;
                wait_for_names(
                    &guild,
                    thread,
                    &label,
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
    async fn a_rename_after_the_topology_cache_was_cleared_still_happens() {
        run(|guild| async move {
            let n = nonce()?;
            let label = format!("testrun-cleared-{n}");
            let first_tab = format!("testrun-before-{n}");
            let fixture = create_fixture(&label, &first_tab)?;
            let result = async {
                let chat = discord(&guild)?;
                let thread = chat
                    .sync_route(&route(&fixture, &label, &first_tab)?)
                    .await
                    .map_err(|error| error.to_string())?;
                *chat.topology_cache().lock().await = None;
                let renamed_tab = format!("testrun-after-{n}");
                herdr(&["tab", "rename", &fixture.tab_id, &renamed_tab])?;
                chat.topology_changed()
                    .await
                    .map_err(|error| error.to_string())?;
                wait_for_names(
                    &guild,
                    thread,
                    &label,
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
}
