use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use herdr_connect_rs::{
    Chat, ChatScope, Discord, Inbound, archived_threads, list_agents, list_mirrored,
    owned_thread_is_test_run, route_is_test_run, tab_list_result, topology_absent_from_herdr,
    workspace_list_result,
};
use serde_json::Value;
use serial_test::serial;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio::task::JoinHandle;
use twilight_gateway::{Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _};
use twilight_http::{Client, api_error::ApiError, error::ErrorType, response::StatusCode};
use twilight_model::channel::{Channel, ChannelType};
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, MessageMarker},
};

type TestResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

fn herdr(args: &[&str]) -> TestResult<Value> {
    let output = Command::new("herdr").args(args).output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(serde_json::from_slice::<Value>(&output.stdout)?["result"].clone())
}

fn field(value: &Value, path: &str) -> TestResult<String> {
    Ok(value
        .pointer(path)
        .and_then(Value::as_str)
        .ok_or("Herdr response has no required field")?
        .to_owned())
}

fn test_name(channel: &Channel) -> bool {
    channel
        .name
        .as_deref()
        .is_some_and(|name| name.starts_with("testrun-"))
}

async fn topology(
    client: &Client,
    guild: Id<GuildMarker>,
) -> TestResult<(Vec<Channel>, Vec<Channel>)> {
    let channels = client.guild_channels(guild).await?.model().await?;
    let mut threads = client.active_threads(guild).await?.model().await?.threads;
    for parent in channels.iter().filter(|channel| {
        matches!(
            channel.kind,
            ChannelType::GuildText | ChannelType::GuildAnnouncement
        )
    }) {
        threads.extend(archived_threads(client, parent.id).await?);
    }
    Ok((channels, threads))
}

async fn production(client: &Client, guild: Id<GuildMarker>) -> TestResult<Vec<Channel>> {
    let (channels, threads) = topology(client, guild).await?;
    Ok(channels
        .into_iter()
        .chain(threads)
        .filter(|channel| !test_name(channel))
        .collect())
}

async fn preserved(client: &Client, before: &[Channel]) -> TestResult<()> {
    for channel in before {
        let after = client.channel(channel.id).await?.model().await?;
        assert_eq!(
            after.name, channel.name,
            "non-testrun topology renamed: {}",
            channel.id
        );
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum Seen {
    Message(Id<MessageMarker>),
    Deleted(Id<ChannelMarker>),
}

struct Readers {
    events: UnboundedReceiver<Result<(bool, Seen), String>>,
    tasks: Vec<JoinHandle<()>>,
}

impl Readers {
    fn new(filtered: Arc<Discord>, token: String) -> Self {
        let (sender, events) = unbounded_channel();
        let filtered_sender = sender.clone();
        let filtered_task = tokio::spawn(async move {
            while let Some(event) = filtered.next_event().await {
                let seen = match event {
                    Ok(Inbound::Posted(message)) => Seen::Message(message.id),
                    Ok(Inbound::Deleted(herdr_connect_rs::GuildDeletion::Thread { id })) => {
                        Seen::Deleted(id)
                    }
                    Ok(Inbound::Deleted(herdr_connect_rs::GuildDeletion::Channel(channel))) => {
                        Seen::Deleted(channel.id)
                    }
                    Ok(Inbound::Component(_)) => continue,
                    Err(error) => {
                        let _ = filtered_sender.send(Err(error.to_string()));
                        return;
                    }
                };
                if filtered_sender.send(Ok((true, seen))).is_err() {
                    return;
                }
            }
        });
        let raw_task = tokio::spawn(async move {
            let mut shard = Shard::new(
                ShardId::ONE,
                token,
                Intents::GUILDS | Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT,
            );
            let flags = EventTypeFlags::MESSAGE_CREATE
                | EventTypeFlags::THREAD_DELETE
                | EventTypeFlags::CHANNEL_DELETE;
            while let Some(event) = shard.next_event(flags).await {
                let seen = match event {
                    Ok(Event::MessageCreate(message)) => Seen::Message(message.id),
                    Ok(Event::ThreadDelete(thread)) => Seen::Deleted(thread.id),
                    Ok(Event::ChannelDelete(channel)) => Seen::Deleted(channel.id),
                    Ok(_) => continue,
                    Err(error) => {
                        let _ = sender.send(Err(error.to_string()));
                        return;
                    }
                };
                if sender.send(Ok((false, seen))).is_err() {
                    return;
                }
            }
        });
        Self {
            events,
            tasks: vec![filtered_task, raw_task],
        }
    }

    async fn ready(&mut self, client: &Client, channel: Id<ChannelMarker>) -> TestResult<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        let mut sent = HashSet::new();
        let mut observed = HashSet::new();
        loop {
            let message = client
                .create_message(channel)
                .content("testrun filter gateway readiness")
                .await?
                .model()
                .await?;
            sent.insert(message.id);
            let tick = (tokio::time::Instant::now() + Duration::from_secs(2)).min(deadline);
            while let Ok(event) = tokio::time::timeout_at(tick, self.events.recv()).await {
                let (filtered, seen) = event.ok_or("gateway observer stopped")??;
                if let Seen::Message(id) = seen
                    && sent.contains(&id)
                {
                    observed.insert(filtered);
                }
                if observed.len() == 2 {
                    return Ok(());
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(
                    "gateway observers did not both receive the real readiness message".into(),
                );
            }
        }
    }

    async fn deletion(&mut self, target: Id<ChannelMarker>) -> TestResult<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let (filtered, seen) = tokio::time::timeout_at(deadline, self.events.recv())
                .await?
                .ok_or("gateway observer stopped")??;
            if let Seen::Deleted(id) = seen
                && id == target
            {
                assert!(!filtered, "filtered gateway dispatched a testrun deletion");
                break;
            }
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(4);
        while let Ok(event) = tokio::time::timeout_at(deadline, self.events.recv()).await {
            let (filtered, seen) = event.ok_or("gateway observer stopped")??;
            if let Seen::Deleted(id) = seen {
                assert!(
                    !(filtered && id == target),
                    "filtered gateway dispatched a testrun deletion"
                );
            }
        }
        Ok(())
    }
}

impl Drop for Readers {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

struct Fixture {
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner: String,
    token: String,
    label: String,
    directory: PathBuf,
    workspace: Option<String>,
    channels: Vec<Id<ChannelMarker>>,
    before: Vec<Channel>,
}

impl Fixture {
    async fn new() -> TestResult<Self> {
        let token = std::env::var("DISCORD_TOKEN")?;
        let client = Arc::new(Client::builder().token(token.clone()).build());
        let guild = std::env::var("DISCORD_GUILD_ID")?.parse()?;
        let before = production(&client, guild).await?;
        let label = format!(
            "testrun-service-filter-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let directory = std::env::temp_dir().join(&label);
        fs::create_dir(&directory)?;
        Ok(Self {
            client,
            guild,
            owner: std::env::var("DISCORD_OWNER_ID")?,
            token,
            label,
            directory,
            workspace: None,
            channels: Vec::new(),
            before,
        })
    }

    fn chat(&self, filtered: bool) -> Arc<Discord> {
        let scope = if filtered {
            ChatScope::Service
        } else {
            ChatScope::TestRuns
        };
        Arc::new(
            Discord::new(
                self.client.clone(),
                self.token.clone(),
                self.guild,
                self.owner.clone(),
                Arc::new(tokio::sync::Mutex::new(None)),
            )
            .with_scope(scope)
            .with_ignore_test_runs(filtered),
        )
    }

    fn agent(&mut self) -> TestResult<(String, String)> {
        let cwd = self.directory.to_str().ok_or("test cwd is not UTF-8")?;
        let mut args = vec![
            "workspace",
            "create",
            "--label",
            &self.label,
            "--cwd",
            cwd,
            "--no-focus",
        ];
        let inherited: Vec<_> = ["CLAUDE_CONFIG_DIR", "CODEX_HOME"]
            .iter()
            .filter_map(|key| {
                std::env::var(key)
                    .ok()
                    .map(|value| format!("{key}={value}"))
            })
            .collect();
        for value in &inherited {
            args.extend(["--env", value]);
        }
        let created = herdr(&args)?;
        let workspace = field(&created, "/workspace/workspace_id")?;
        self.workspace = Some(workspace);
        let tab = field(&created, "/tab/tab_id")?;
        let pane = field(&created, "/root_pane/pane_id")?;
        herdr(&["tab", "rename", &tab, "testrun-filter-agent"])?;
        herdr(&[
            "pane",
            "wait-output",
            &pane,
            "--match",
            "╰─",
            "--timeout",
            "15000",
        ])?;
        let name = format!("filter-{}", std::process::id());
        herdr(&[
            "agent",
            "start",
            &name,
            "--kind",
            "claude",
            "--pane",
            &pane,
            "--timeout",
            "60000",
            "--",
            "--model",
            "haiku",
        ])?;
        herdr(&[
            "agent",
            "prompt",
            &pane,
            "Reply exactly FILTER READY and use no tools.",
            "--wait",
            "--timeout",
            "90000",
        ])?;
        let agent = list_agents()?
            .into_iter()
            .find(|agent| agent.pane_id == pane)
            .ok_or("real agent missing")?;
        assert!(
            agent.session.is_some(),
            "real Claude must report its session"
        );
        let (agents, tabs) = list_mirrored()?;
        assert!(agents.iter().all(|agent| agent.pane_id != pane));
        assert!(tabs.iter().all(|entry| entry.tab_id != tab));
        assert!(route_is_test_run(&agent.workspace_id, &tab)?);
        Ok((tab, pane))
    }

    async fn channel(&mut self, name: &str, topic: Option<&str>) -> TestResult<Id<ChannelMarker>> {
        let request = self
            .client
            .create_guild_channel(self.guild, name)
            .kind(ChannelType::GuildText);
        let request = if let Some(topic) = topic {
            request.topic(topic)
        } else {
            request
        };
        let channel = request.await?.model().await?;
        self.channels.push(channel.id);
        Ok(channel.id)
    }

    async fn not_mirrored(&self, tab: &str) -> TestResult<()> {
        let workspace = self.workspace.as_ref().ok_or("test workspace missing")?;
        for _ in 0..6 {
            let channels = self
                .client
                .guild_channels(self.guild)
                .await?
                .model()
                .await?;
            let threads = self
                .client
                .active_threads(self.guild)
                .await?
                .model()
                .await?
                .threads;
            assert!(channels.iter().all(|channel| channel.topic.as_deref()
                != Some(format!("herdr workspace [{workspace}]").as_str())));
            assert!(threads.iter().all(|thread| {
                !thread
                    .name
                    .as_deref()
                    .is_some_and(|name| name.ends_with(&format!(" [{tab}]")))
            }));
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        Ok(())
    }

    fn close_herdr(&self) -> TestResult<()> {
        if let Some(id) = &self.workspace
            && let Some(workspace) = workspace_list_result()?
                .into_iter()
                .find(|workspace| &workspace.workspace_id == id)
        {
            assert_eq!(
                workspace.label, self.label,
                "refusing cleanup outside the owned workspace"
            );
            herdr(&["workspace", "close", id])?;
        }
        Ok(())
    }

    async fn sweep(&self, chat: &Discord) -> TestResult<()> {
        let workspaces = workspace_list_result()?;
        let tabs = tab_list_result()?;
        let live_workspaces: HashSet<_> = workspaces
            .iter()
            .map(|workspace| workspace.workspace_id.as_str())
            .collect();
        let live_tabs: HashSet<_> = tabs.iter().map(|tab| tab.tab_id.as_str()).collect();
        let (channels, threads) = topology(&self.client, self.guild).await?;
        let absent = topology_absent_from_herdr(
            &self.client,
            &channels,
            &threads,
            &live_workspaces,
            &live_tabs,
        )
        .await?;
        assert!(
            absent.iter().all(|channel| test_name(channel)
                && (self.channels.contains(&channel.id)
                    || channel
                        .parent_id
                        .is_some_and(|parent| self.channels.contains(&parent)))),
            "refusing sweep that could delete non-testrun topology"
        );
        let before = production(&self.client, self.guild).await?;
        chat.startup_sweep(Vec::new(), || {
            Ok((
                workspace_list_result()?
                    .into_iter()
                    .map(|workspace| workspace.workspace_id)
                    .collect(),
                tab_list_result()?
                    .into_iter()
                    .map(|tab| tab.tab_id)
                    .collect(),
            ))
        })
        .await;
        preserved(&self.client, &before).await
    }

    async fn exercise(&mut self) -> TestResult<()> {
        let (tab, pane) = self.agent()?;
        self.not_mirrored(&tab).await?;
        let filtered = self.chat(true);
        let mut readers = Readers::new(filtered.clone(), self.token.clone());
        let bootstrap = self.channel(&format!("{}-ready", self.label), None).await?;
        readers.ready(&self.client, bootstrap).await?;
        let topic = format!(
            "herdr workspace [{}]",
            self.workspace.as_ref().ok_or("workspace missing")?
        );
        let parent = self.channel(&self.label.clone(), Some(&topic)).await?;
        let thread = self
            .client
            .create_thread(
                parent,
                &format!("testrun-agent [{tab}]"),
                ChannelType::PublicThread,
            )
            .await?
            .model()
            .await?;
        readers.ready(&self.client, thread.id).await?;
        assert!(
            owned_thread_is_test_run(thread.id),
            "a live THREAD_CREATE must register its testrun identity before deletion"
        );
        for (label, chat, expected) in [
            ("service", filtered.as_ref(), true),
            ("test instance", self.chat(false).as_ref(), false),
        ] {
            for id in [parent, thread.id] {
                assert_eq!(
                    chat.channel_in_test_run(id).await?,
                    expected,
                    "{label} component surface filter"
                );
            }
        }
        let agent = list_agents()?
            .into_iter()
            .find(|agent| agent.pane_id == pane)
            .ok_or("test agent disappeared")?;
        assert!(agent.session.is_some());
        self.close_herdr()?;
        self.sweep(filtered.as_ref()).await?;
        for (label, id) in [("thread", thread.id), ("channel", parent)] {
            assert!(
                self.client.channel(id).await.is_ok(),
                "filtered sweep removed the test {label}"
            );
            self.client.delete_channel(id).await?;
            readers.deletion(id).await?;
        }
        preserved(&self.client, &self.before).await?;
        println!(
            "live filter: real agent excluded; component surfaces excluded; startup preserves test resources; real gateway deletions suppressed; production preserved"
        );
        Ok(())
    }

    async fn cleanup(&self) -> TestResult<()> {
        self.close_herdr()?;
        for id in &self.channels {
            match self.client.delete_channel(*id).await {
                Ok(_) => {}
                Err(error) if matches!(error.kind(), ErrorType::Response { status, error: ApiError::General(api), .. } if *status == StatusCode::NOT_FOUND && api.code == 10003) =>
                    {}
                Err(error) => return Err(error.into()),
            }
        }
        fs::remove_dir_all(&self.directory)?;
        preserved(&self.client, &self.before).await?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        let count = loop {
            let (channels, threads) = topology(&self.client, self.guild).await?;
            let count = channels
                .iter()
                .chain(&threads)
                .filter(|channel| test_name(channel))
                .count();
            if count == 0 || tokio::time::Instant::now() >= deadline {
                break count;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        println!("named zero-leftover check: {count} testrun Discord channels and threads");
        assert_eq!(count, 0);
        Ok(())
    }
}

#[tokio::test]
#[serial]
async fn service_filter_excludes_real_test_resources_and_preserves_production() -> TestResult<()> {
    let fixture = Arc::new(tokio::sync::Mutex::new(Fixture::new().await?));
    let exercise = fixture.clone();
    let result = tokio::spawn(async move { exercise.lock().await.exercise().await }).await;
    let cleanup = fixture.lock().await.cleanup().await;
    cleanup?;
    result??;
    Ok(())
}
