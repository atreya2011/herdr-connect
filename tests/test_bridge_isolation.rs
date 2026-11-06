use std::collections::HashSet;
use std::error::Error;
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use herdr_connect_rs::{
    Chat, ChatScope, Discord, GuildDeletion, Inbound, TopologyRoute, archived_threads,
    fetch_topology_lists, remember_tab_threads, remember_workspace_channels,
    resolve_owner_deleted_tab, resolve_owner_deleted_workspace, tab_list_result,
    topology_absent_from_herdr, workspace_list_result,
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

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type Observations = Vec<(String, bool)>;

fn record(observations: &[(String, bool)]) {
    for (case, observed) in observations {
        println!("real bridge isolation: {case}: {observed}");
    }
}

fn herdr(args: &[&str]) -> Result<Value> {
    let output = Command::new("herdr").args(args).output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(serde_json::from_slice::<Value>(&output.stdout)?["result"].clone())
}

fn field(value: &Value, pointer: &str) -> Result<String> {
    Ok(value
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or("missing Herdr result field")?
        .to_owned())
}

fn test_name(channel: &Channel) -> bool {
    channel
        .name
        .as_deref()
        .is_some_and(|name| name.starts_with("testrun-"))
}

fn live() -> std::result::Result<(HashSet<String>, HashSet<String>), String> {
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
}

async fn topology(client: &Client, guild: Id<GuildMarker>) -> Result<(Vec<Channel>, Vec<Channel>)> {
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
    let mut seen = HashSet::new();
    threads.retain(|thread| seen.insert(thread.id));
    Ok((channels, threads))
}

async fn production(client: &Client, guild: Id<GuildMarker>) -> Result<Vec<Channel>> {
    let (channels, threads) = topology(client, guild).await?;
    let test_parents = channels
        .iter()
        .filter(|channel| test_name(channel))
        .map(|channel| channel.id)
        .collect::<HashSet<_>>();
    Ok(channels
        .into_iter()
        .chain(threads)
        .filter(|channel| {
            !test_name(channel)
                && (!channel.kind.is_thread()
                    || channel
                        .parent_id
                        .is_none_or(|parent| !test_parents.contains(&parent)))
        })
        .collect())
}

async fn exists(client: &Client, id: Id<ChannelMarker>) -> Result<bool> {
    match client.channel(id).await {
        Ok(_) => Ok(true),
        Err(error) if matches!(error.kind(), ErrorType::Response { status, error: ApiError::General(api_error), .. } if *status == StatusCode::NOT_FOUND && api_error.code == 10003) => {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Seen {
    Message(Id<MessageMarker>),
    Archived(Id<ChannelMarker>),
    Deleted(Id<ChannelMarker>),
}

struct Readers {
    events: UnboundedReceiver<std::result::Result<(bool, Seen), String>>,
    tasks: Vec<JoinHandle<()>>,
}

impl Readers {
    fn new(
        chat: Arc<Discord>,
        token: String,
        parent: Id<ChannelMarker>,
        thread: Id<ChannelMarker>,
    ) -> Self {
        let (sender, events) = unbounded_channel();
        let filtered_sender = sender.clone();
        let filtered = tokio::spawn(async move {
            while let Some(event) = chat.next_event().await {
                let seen = match event {
                    Ok(Inbound::Posted(message)) if message.channel_id == thread => {
                        Seen::Message(message.id)
                    }
                    Ok(Inbound::Deleted(GuildDeletion::Thread { id })) if id == thread => {
                        Seen::Deleted(id)
                    }
                    Ok(Inbound::Deleted(GuildDeletion::Channel(channel)))
                        if channel.id == parent =>
                    {
                        Seen::Deleted(channel.id)
                    }
                    Ok(_) => continue,
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
        let raw = tokio::spawn(async move {
            let mut shard = Shard::new(
                ShardId::ONE,
                token,
                Intents::GUILDS | Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT,
            );
            let flags = EventTypeFlags::MESSAGE_CREATE
                | EventTypeFlags::THREAD_UPDATE
                | EventTypeFlags::THREAD_DELETE
                | EventTypeFlags::CHANNEL_DELETE;
            while let Some(event) = shard.next_event(flags).await {
                let seen = match event {
                    Ok(Event::MessageCreate(message)) if message.channel_id == thread => {
                        Seen::Message(message.id)
                    }
                    Ok(Event::ThreadUpdate(updated))
                        if updated.id == thread
                            && updated
                                .thread_metadata
                                .as_ref()
                                .is_some_and(|metadata| metadata.archived) =>
                    {
                        Seen::Archived(updated.id)
                    }
                    Ok(Event::ThreadDelete(deleted)) if deleted.id == thread => {
                        Seen::Deleted(deleted.id)
                    }
                    Ok(Event::ChannelDelete(deleted)) if deleted.id == parent => {
                        Seen::Deleted(deleted.id)
                    }
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
            tasks: vec![filtered, raw],
        }
    }

    async fn ready(&mut self, client: &Client, thread: Id<ChannelMarker>) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
        let mut sent = HashSet::new();
        let mut observed = HashSet::new();
        loop {
            let message = client
                .create_message(thread)
                .content("testrun isolation readiness")
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
                return Err("gateway observers did not receive owned readiness messages".into());
            }
        }
    }

    async fn wait_for(&mut self, expected: Seen, filtered_too: bool) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut observed = HashSet::new();
        loop {
            let (filtered, event) = tokio::time::timeout_at(deadline, self.events.recv())
                .await?
                .ok_or("gateway observer stopped")??;
            if event == expected {
                observed.insert(filtered);
            }
            if observed.contains(&false) && (!filtered_too || observed.contains(&true)) {
                return Ok(());
            }
        }
    }
}

impl Drop for Readers {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

struct Probe {
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    token: String,
    chat: Arc<Discord>,
    label: String,
    directory: PathBuf,
    route: Option<TopologyRoute>,
    owned: Vec<Id<ChannelMarker>>,
    before: Vec<Channel>,
}

impl Probe {
    async fn new() -> Result<Self> {
        let token = std::env::var("DISCORD_TOKEN")?;
        let guild = std::env::var("DISCORD_GUILD_ID")?.parse()?;
        let owner = std::env::var("DISCORD_OWNER_ID")?;
        let client = Arc::new(
            Client::builder()
                .token(token.clone())
                .timeout(Duration::from_secs(30))
                .build(),
        );
        let chat = Arc::new(Discord::new(
            client.clone(),
            token.clone(),
            guild,
            owner,
            Arc::new(tokio::sync::Mutex::new(None)),
        ));
        let before = production(&client, guild).await?;
        if before.is_empty() {
            return Err("no actual production Discord resources to verify".into());
        }
        let label = format!(
            "testrun-bridge-isolation-{}-{}",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        );
        let directory = std::env::temp_dir().join(&label);
        fs::create_dir(&directory)?;
        Ok(Self {
            client,
            guild,
            token,
            chat,
            label,
            directory,
            route: None,
            owned: Vec::new(),
            before,
        })
    }

    async fn observe_production(&self) -> Result<Observations> {
        let mut observations = vec![(
            "default adapter scope is TestRuns".into(),
            self.chat.scope() == ChatScope::TestRuns,
        )];
        record(&observations);
        let service = Discord::new(
            self.client.clone(),
            self.token.clone(),
            self.guild,
            self.chat.owner_id().to_owned(),
            Arc::new(tokio::sync::Mutex::new(None)),
        )
        .with_scope(ChatScope::Service);
        let (channels, threads) = fetch_topology_lists(&self.client, self.guild).await?;
        remember_workspace_channels(&channels);
        remember_tab_threads(&threads);
        self.chat.register_archived().await?;
        for channel in &self.before {
            let excluded = !self.chat.accepts_route(&channel.id).await?
                && !self.chat.channel_in_scope(channel.id).await?;
            let unregistered = if channel.kind.is_thread() {
                resolve_owner_deleted_tab(channel.id).is_none()
            } else {
                resolve_owner_deleted_workspace(channel).is_none()
            };
            let row = [
                (
                    format!(
                        "production channel {} excluded from common route guards",
                        channel.id
                    ),
                    excluded,
                ),
                (
                    format!(
                        "production channel {} absent from deletion registry",
                        channel.id
                    ),
                    unregistered,
                ),
                (
                    format!("service query admits production channel {}", channel.id),
                    service.accepts_route(&channel.id).await?,
                ),
            ];
            record(&row);
            observations.extend(row);
        }
        let workspaces = workspace_list_result()?;
        let tabs = tab_list_result()?;
        let tab = tabs
            .iter()
            .find(|tab| {
                !tab.label.starts_with("testrun-")
                    && workspaces.iter().any(|workspace| {
                        workspace.workspace_id == tab.workspace_id
                            && !workspace.label.starts_with("testrun-")
                    })
            })
            .ok_or("no actual production Herdr tab")?;
        observations.push((
            "actual production Herdr workspace/tab pair rejected".into(),
            !self
                .chat
                .accepts_herdr_route(&tab.workspace_id, &tab.tab_id)
                .await?,
        ));
        record(&observations[observations.len() - 1..]);
        Ok(observations)
    }

    async fn create_owned(&mut self) -> Result<(Id<ChannelMarker>, Id<ChannelMarker>)> {
        let cwd = self.directory.to_str().ok_or("test cwd is not UTF-8")?;
        let created = herdr(&[
            "workspace",
            "create",
            "--label",
            &self.label,
            "--cwd",
            cwd,
            "--no-focus",
        ])?;
        let workspace = field(&created, "/workspace/workspace_id")?;
        let tab = field(&created, "/tab/tab_id")?;
        let route = TopologyRoute {
            workspace_id: workspace,
            tab_id: tab,
            pane_id: field(&created, "/root_pane/pane_id")?,
            channel_name: self.label.clone(),
            thread_name: String::new(),
        };
        self.route = Some(route);
        let route = self.route.as_mut().ok_or("owned route missing")?;
        herdr(&["tab", "rename", &route.tab_id, &self.label])?;
        route.thread_name = format!("{} [{}]", self.label, route.tab_id);
        let channel = self
            .client
            .create_guild_channel(self.guild, &self.label)
            .topic(&format!("herdr workspace [{}]", route.workspace_id))
            .await?
            .model()
            .await?;
        self.owned.push(channel.id);
        let thread = self
            .client
            .create_thread(channel.id, &route.thread_name, ChannelType::PublicThread)
            .await?
            .model()
            .await?;
        self.owned.push(thread.id);
        Ok((channel.id, thread.id))
    }

    async fn sweep_owned(&mut self) -> Result<Observations> {
        let (channels, threads) = topology(&self.client, self.guild).await?;
        if channels
            .iter()
            .chain(threads.iter())
            .any(|channel| test_name(channel) && !self.owned.contains(&channel.id))
        {
            return Err("foreign testrun resource present; refused before sweep".into());
        }
        let current = live()?;
        let workspaces = current.0.iter().map(String::as_str).collect::<HashSet<_>>();
        let tabs = current.1.iter().map(String::as_str).collect::<HashSet<_>>();
        let absent =
            topology_absent_from_herdr(&self.client, &channels, &threads, &workspaces, &tabs)
                .await?;
        if absent
            .iter()
            .any(|channel| !self.owned.contains(&channel.id))
        {
            return Err("sweep could delete foreign topology; refused before sweep".into());
        }
        self.before = production(&self.client, self.guild).await?;
        self.chat
            .startup_sweep(
                vec![Ok(self.route.clone().ok_or("owned route missing")?)],
                live,
            )
            .await;
        Ok(vec![(
            "startup preserves every production channel/thread".into(),
            self.preserved().await?,
        )])
    }

    async fn observed_events(
        &self,
        parent: Id<ChannelMarker>,
        thread: Id<ChannelMarker>,
    ) -> Result<Observations> {
        let mut readers = Readers::new(self.chat.clone(), self.token.clone(), parent, thread);
        readers.ready(&self.client, thread).await?;
        let nonce = format!(
            "scope-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
        );
        if nonce.len() > 25 {
            return Err("probe nonce exceeds Discord limit".into());
        }
        self.chat
            .post_live(&thread, "testrun bridge isolation positive", &nonce)
            .await?;
        let messages = self
            .client
            .channel_messages(thread)
            .limit(100)
            .await?
            .model()
            .await?;
        let posted = messages
            .iter()
            .find(|message| message.content == "testrun bridge isolation positive")
            .ok_or("bridge positive post missing")?;
        readers.wait_for(Seen::Message(posted.id), true).await?;
        self.client.update_thread(thread).archived(true).await?;
        readers.wait_for(Seen::Archived(thread), false).await?;
        let archived = archived_threads(&self.client, parent).await?;
        let archived_owned = archived.iter().any(|channel| channel.id == thread)
            && resolve_owner_deleted_tab(thread).is_some();
        self.client.delete_channel(thread).await?;
        readers.wait_for(Seen::Deleted(thread), true).await?;
        self.client.delete_channel(parent).await?;
        readers.wait_for(Seen::Deleted(parent), true).await?;
        Ok(vec![
            (
                "owned bot message observed through real and filtered gateways".into(),
                true,
            ),
            (
                "owned archive observed and registered".into(),
                archived_owned,
            ),
            (
                "owned thread and channel deletions observed through both gateways".into(),
                true,
            ),
        ])
    }

    async fn preserved(&self) -> Result<bool> {
        for before in &self.before {
            let after = self.client.channel(before.id).await?.model().await?;
            if after.name != before.name
                || after.topic != before.topic
                || after.parent_id != before.parent_id
                || after.thread_metadata.as_ref().map(|meta| meta.archived)
                    != before.thread_metadata.as_ref().map(|meta| meta.archived)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn exercise(&mut self) -> Result<Observations> {
        let mut observations = self.observe_production().await?;
        let (parent, thread) = self.create_owned().await?;
        let route = self.route.as_ref().ok_or("owned route missing")?;
        let synced = self.chat.sync_route_with_parent(route).await?;
        let owned = [
            (
                "owned topology sync admitted".into(),
                synced == (parent, thread),
            ),
            (
                "owned Herdr route admitted".into(),
                self.chat
                    .accepts_herdr_route(&route.workspace_id, &route.tab_id)
                    .await?,
            ),
            (
                "owned component route admitted".into(),
                self.chat.accepts_route(&thread).await?,
            ),
            (
                "owned thread registered".into(),
                resolve_owner_deleted_tab(thread).is_some(),
            ),
        ];
        record(&owned);
        observations.extend(owned);
        let sweep = self.sweep_owned().await?;
        record(&sweep);
        observations.extend(sweep);
        let events = self.observed_events(parent, thread).await?;
        record(&events);
        observations.extend(events);
        observations.push((
            "production preserved after real owned events".into(),
            self.preserved().await?,
        ));
        record(&observations[observations.len() - 1..]);
        Ok(observations)
    }

    async fn cleanup(&self) -> Result<()> {
        let mut failures = Vec::new();
        for id in self.owned.iter().rev() {
            match exists(&self.client, *id).await {
                Ok(true) => {
                    if let Err(error) = self.client.delete_channel(*id).await {
                        failures.push(error.to_string());
                    }
                }
                Ok(false) => {}
                Err(error) => failures.push(error.to_string()),
            }
        }
        if let Some(route) = &self.route
            && let Err(error) = herdr(&["workspace", "close", &route.workspace_id])
        {
            failures.push(error.to_string());
        }
        if let Err(error) = fs::remove_dir_all(&self.directory) {
            failures.push(error.to_string());
        }
        let mut remaining = 0;
        for id in &self.owned {
            remaining += usize::from(exists(&self.client, *id).await?);
        }
        let (workspaces, tabs) = live()?;
        let native_remaining = self
            .route
            .iter()
            .map(|route| {
                usize::from(workspaces.contains(&route.workspace_id))
                    + usize::from(tabs.contains(&route.tab_id))
            })
            .sum::<usize>();
        for (kind, remaining) in [
            ("Discord channels/threads", remaining),
            ("Herdr workspace/tab", native_remaining),
        ] {
            println!("named zero-leftover check: {remaining} owned {kind}");
            if remaining != 0 {
                failures.push(format!("owned {kind} remain: {remaining}"));
            }
        }
        if !failures.is_empty() {
            return Err(failures.join("; ").into());
        }
        Ok(())
    }
}

#[tokio::test]
#[serial]
async fn test_bridge_scope_excludes_real_production() -> Result<()> {
    let mut probe = Probe::new().await?;
    let observed = probe.exercise().await;
    let cleanup = probe.cleanup().await;
    cleanup?;
    let observed = observed?;
    let count = observed.len();
    for (case, observed) in observed {
        assert!(observed, "{case}");
    }
    println!("verified {count} observed isolation cases");
    Ok(())
}
