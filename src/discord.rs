use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::{Arc, Mutex, PoisonError};

use twilight_gateway::{
    ConfigBuilder, Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _,
};
use twilight_http::Client;
use twilight_model::application::interaction::{
    Interaction as DiscordInteraction, InteractionData,
};
use twilight_model::channel::{ChannelType, message::MessageFlags};
use twilight_model::http::interaction::{
    InteractionResponse, InteractionResponseData, InteractionResponseType,
};
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, MessageMarker, UserMarker, WebhookMarker},
};

use crate::chat::{
    Ack, Chat, ChatError, ChatScope, Closed, Component, Inbound, OwnerMessage, TopologyClosure,
};
use crate::deletion::{GuildDeletion, handle_guild_deletion};
use crate::delivery::expire_permission_card;

use crate::{
    HerdrName, OwnerIdentity, PermissionVendor, Question, TEST_RUN_PREFIX, TopologyCache,
    TopologyRoute, TransitionMessage, UNKNOWN_CHANNEL_DELIVERY_ERROR,
    UNKNOWN_WEBHOOK_DELIVERY_ERROR, bridge_eprintln, cached_route, delete_tab_thread,
    delete_topology_absent_from_herdr, delete_workspace_channel, deliver_activity_message,
    deliver_live_message, deliver_permission_card, deliver_question_card, deliver_transition_card,
    execute_terminal_prompt_webhook, expire_informational_card, fetch_owner_identity,
    fetch_topology_lists, herdr_names, is_test_run_name, owned_thread_is_test_run,
    production_topology_enabled, reconcile_topology_cache, register_archived_tab_threads,
    resolve_terminal_prompt_webhook, should_handle_owner_message, sync_topology, tab_list_result,
    update_activity_message, update_question_card, workspace_channel_id, workspace_list_result,
};

pub const TERMINAL_PROMPT_WEBHOOK_NAME: &str = "herdr-connect owner";

type RenameTasks = HashMap<Id<ChannelMarker>, (String, tokio::task::JoinHandle<()>)>;

type WebhookCache = HashMap<Id<ChannelMarker>, (Id<WebhookMarker>, String)>;

pub struct Discord {
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: String,
    topology_cache: TopologyCache,
    webhooks: Mutex<WebhookCache>,
    shard: tokio::sync::Mutex<Shard>,
    ignore_test_runs: bool,
    scope: ChatScope,
    renames: tokio::sync::Mutex<RenameTasks>,
}

pub async fn fetch_startup_owner_identity(
    client: &Client,
    owner_id: &str,
) -> Result<OwnerIdentity, String> {
    let owner_id = owner_id.parse::<u64>().map_err(|error| {
        format!("owner identity fetch error: DISCORD_OWNER_ID is not numeric: {error}")
    })?;
    fetch_owner_identity(client, Id::<UserMarker>::new(owner_id))
        .await
        .map_err(|error| format!("owner identity fetch error: {error}"))
}

fn chat_error(error: impl Display) -> ChatError {
    ChatError::from(error.to_string())
}

pub async fn sync_route(
    client: &Client,
    guild: Id<GuildMarker>,
    route: &TopologyRoute,
    topology_cache: &TopologyCache,
    owner_id: &str,
) -> Result<Id<ChannelMarker>, String> {
    let mut guard = topology_cache.lock().await;
    sync_route_locked(client, guild, route, &mut guard, owner_id).await
}

async fn sync_route_locked(
    client: &Client,
    guild: Id<GuildMarker>,
    route: &TopologyRoute,
    guard: &mut tokio::sync::MutexGuard<
        '_,
        Option<(
            Vec<twilight_model::channel::Channel>,
            Vec<twilight_model::channel::Channel>,
        )>,
    >,
    owner_id: &str,
) -> Result<Id<ChannelMarker>, String> {
    let cached = match guard.as_ref() {
        Some((channels, active_threads)) => cached_route(channels, active_threads, route)
            .map_err(|error| format!("discord topology error: {error}"))?,
        None => None,
    };
    if let Some(channel) = cached {
        return Ok(channel);
    }
    let fetched = fetch_topology_lists(client, guild)
        .await
        .map_err(|error| format!("discord topology error: {error}"))?;
    let (channels, active_threads) = reconcile_topology_cache(guard, fetched);
    sync_topology(client, guild, channels, active_threads, route, owner_id)
        .await
        .map_err(|error| format!("discord topology error: {error}"))
}

impl Discord {
    #[must_use]
    pub fn new(
        client: Arc<Client>,
        token: String,
        guild: Id<GuildMarker>,
        owner_id: String,
        topology_cache: TopologyCache,
    ) -> Self {
        let intents = Intents::GUILDS | Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;
        let shard = Shard::with_config(ShardId::ONE, ConfigBuilder::new(token, intents).build());
        Self {
            client,
            guild,
            owner_id,
            topology_cache,
            webhooks: Mutex::new(HashMap::new()),
            shard: tokio::sync::Mutex::new(shard),
            ignore_test_runs: false,
            scope: ChatScope::TestRuns,
            renames: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    #[must_use]
    pub const fn with_ignore_test_runs(mut self, ignore_test_runs: bool) -> Self {
        self.ignore_test_runs = ignore_test_runs;
        self
    }

    #[must_use]
    pub const fn with_scope(mut self, scope: ChatScope) -> Self {
        self.scope = scope;
        self
    }

    async fn chat_channel_in_scope(
        &self,
        channel_id: Id<ChannelMarker>,
    ) -> Result<bool, ChatError> {
        if self.scope == ChatScope::Service {
            return Ok(true);
        }
        let channel = self
            .client
            .channel(channel_id)
            .await
            .map_err(chat_error)?
            .model()
            .await
            .map_err(chat_error)?;
        if is_test_run_name(channel.name.as_deref()) {
            return Ok(true);
        }
        let Some(parent) = channel
            .parent_id
            .filter(|_| is_thread_channel(channel.kind))
        else {
            return Ok(false);
        };
        let parent = self
            .client
            .channel(parent)
            .await
            .map_err(chat_error)?
            .model()
            .await
            .map_err(chat_error)?;
        Ok(is_test_run_name(parent.name.as_deref()))
    }

    async fn check_channel_scope(&self, channel: Id<ChannelMarker>) -> Result<(), ChatError> {
        if self.chat_channel_in_scope(channel).await? {
            Ok(())
        } else {
            Err(chat_error(
                "test bridge refused a non-testrun Discord channel",
            ))
        }
    }

    pub async fn channel_in_scope(&self, channel_id: Id<ChannelMarker>) -> Result<bool, ChatError> {
        if self.scope == ChatScope::Service {
            return Ok(true);
        }
        let channel = self
            .client
            .channel(channel_id)
            .await
            .map_err(chat_error)?
            .model()
            .await
            .map_err(chat_error)?;
        let Some(parent) = channel
            .parent_id
            .filter(|_| is_thread_channel(channel.kind))
        else {
            return Ok(false);
        };
        let parent = self
            .client
            .channel(parent)
            .await
            .map_err(chat_error)?
            .model()
            .await
            .map_err(chat_error)?;
        if !is_test_run_name(channel.name.as_deref()) && !is_test_run_name(parent.name.as_deref()) {
            return Ok(false);
        }
        let Some((tab, workspace)) = channel
            .name
            .as_deref()
            .zip(parent.topic.as_deref())
            .and_then(|(name, topic)| prompt_surface_markers(name, topic))
        else {
            return Ok(false);
        };
        self.accepts_herdr_route(workspace, tab).await
    }

    async fn queue_rename(&self, change: HerdrName) {
        if (self.ignore_test_runs && change.test_run)
            || (!change.test_run && !production_topology_enabled())
            || (self.scope == ChatScope::TestRuns
                && !(change.test_run && change.name.starts_with(TEST_RUN_PREFIX)))
        {
            return;
        }
        let mut jobs = self.renames.lock().await;
        if change.current {
            if let Some((_, job)) = jobs.remove(&change.id) {
                job.abort();
            }
            return;
        }
        if jobs
            .get(&change.id)
            .is_some_and(|(desired, job)| desired == &change.name && !job.is_finished())
        {
            return;
        }
        if let Some((_, job)) = jobs.remove(&change.id) {
            job.abort();
        }
        let (client, cache) = (self.client.clone(), self.topology_cache.clone());
        let desired = change.name.clone();
        jobs.insert(
            change.id,
            (
                desired,
                tokio::spawn(async move {
                    let renamed = match client.update_channel(change.id).name(&change.name).await {
                        Ok(response) => response.model().await.map_err(|error| error.to_string()),
                        Err(error) => Err(error.to_string()),
                    };
                    match renamed {
                        Ok(renamed) => {
                            if let Some((channels, threads)) = cache.lock().await.as_mut() {
                                for entry in channels.iter_mut().chain(threads.iter_mut()) {
                                    if entry.id == renamed.id {
                                        entry.name.clone_from(&renamed.name);
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            bridge_eprintln!("Discord rename error for {}: {error}", change.id);
                        }
                    }
                }),
            ),
        );
    }

    async fn refresh_names(&self) -> Result<(), ChatError> {
        let (workspaces, tabs) = tokio::task::spawn_blocking(|| {
            Ok::<_, String>((workspace_list_result()?, tab_list_result()?))
        })
        .await
        .map_err(chat_error)?
        .map_err(chat_error)?;
        let changes = {
            let mut guard = self.topology_cache.lock().await;
            if guard.is_none() {
                let fetched = self.fetch_scoped_topology().await.map_err(chat_error)?;
                reconcile_topology_cache(&mut guard, fetched);
            }
            guard.as_ref().map_or_else(Vec::new, |(channels, threads)| {
                herdr_names(channels, threads, &workspaces, &tabs)
            })
        };
        for change in changes {
            self.queue_rename(change).await;
        }
        Ok(())
    }

    async fn reconcile_synced_name(&self, route: &TopologyRoute, thread: Id<ChannelMarker>) {
        let stale = self
            .topology_cache
            .lock()
            .await
            .as_ref()
            .is_some_and(|(_, threads)| {
                threads.iter().any(|cached| {
                    cached.id == thread
                        && cached.name.as_deref() != Some(route.thread_name.as_str())
                })
            });
        if stale && let Err(error) = self.refresh_names().await {
            bridge_eprintln!("Discord name reconciliation error: {error}");
        }
    }

    async fn check_topology_route(&self, route: &TopologyRoute) -> Result<(), ChatError> {
        if self
            .accepts_herdr_route(&route.workspace_id, &route.tab_id)
            .await?
        {
            Ok(())
        } else {
            Err(chat_error("test bridge refused a non-testrun Herdr route"))
        }
    }

    async fn fetch_scoped_topology(
        &self,
    ) -> Result<
        (
            Vec<twilight_model::channel::Channel>,
            Vec<twilight_model::channel::Channel>,
        ),
        String,
    > {
        let (mut channels, mut threads) = fetch_topology_lists(&self.client, self.guild).await?;
        if self.scope == ChatScope::TestRuns {
            channels.retain(|channel| is_test_run_name(channel.name.as_deref()));
            threads.retain(|thread| {
                thread
                    .parent_id
                    .is_some_and(|parent| channels.iter().any(|channel| channel.id == parent))
            });
        }
        Ok((channels, threads))
    }

    async fn deletion_in_scope(&self, deletion: &GuildDeletion) -> Result<bool, ChatError> {
        if self.scope == ChatScope::Service {
            return Ok(true);
        }
        match deletion {
            GuildDeletion::Channel(channel) => {
                if !is_test_run_name(channel.name.as_deref()) {
                    return Ok(false);
                }
                let Some(workspace) = crate::resolve_owner_deleted_workspace(channel) else {
                    return Ok(false);
                };
                self.accepts_herdr_route(&workspace, "").await
            }
            GuildDeletion::Thread { id } => {
                if !owned_thread_is_test_run(*id) {
                    return Ok(false);
                }
                let Some((tab, _)) = crate::resolve_owner_deleted_tab(*id) else {
                    return Ok(false);
                };
                let Some((workspace, _)) = tab.split_once(':') else {
                    return Ok(false);
                };
                self.accepts_herdr_route(workspace, &tab).await
            }
        }
    }

    pub async fn channel_in_test_run(
        &self,
        channel_id: Id<ChannelMarker>,
    ) -> Result<bool, ChatError> {
        if !self.ignore_test_runs {
            return Ok(false);
        }
        let channel = self
            .client
            .channel(channel_id)
            .await
            .map_err(chat_error)?
            .model()
            .await
            .map_err(chat_error)?;
        if is_test_run_name(channel.name.as_deref()) {
            return Ok(true);
        }
        let Some(parent_id) = channel.parent_id else {
            return Ok(false);
        };
        let parent = self
            .client
            .channel(parent_id)
            .await
            .map_err(chat_error)?
            .model()
            .await
            .map_err(chat_error)?;
        if is_test_run_name(parent.name.as_deref()) {
            return Ok(true);
        }
        self.herdr_surface_in_test_run(channel.name.as_deref(), parent.topic.as_deref())
            .await
    }

    async fn herdr_surface_in_test_run(
        &self,
        name: Option<&str>,
        topic: Option<&str>,
    ) -> Result<bool, ChatError> {
        if !self.ignore_test_runs {
            return Ok(false);
        }
        let Some((tab_id, workspace_id)) = name
            .zip(topic)
            .and_then(|(name, topic)| prompt_surface_markers(name, topic))
        else {
            return Ok(false);
        };
        let (tab_id, workspace_id) = (tab_id.to_owned(), workspace_id.to_owned());
        tokio::task::spawn_blocking(move || crate::route_is_test_run(&workspace_id, &tab_id))
            .await
            .map_err(chat_error)?
            .map_err(chat_error)
    }

    async fn deletion_in_test_run(&self, deletion: &GuildDeletion) -> Result<bool, ChatError> {
        if !self.ignore_test_runs {
            return Ok(false);
        }
        let named_test_run = match deletion {
            GuildDeletion::Channel(channel) => is_test_run_name(channel.name.as_deref()),
            GuildDeletion::Thread { id } => owned_thread_is_test_run(*id),
        };
        if named_test_run {
            return Ok(true);
        }
        let deletion = deletion.clone();
        tokio::task::spawn_blocking(move || match deletion {
            GuildDeletion::Channel(channel) => {
                let Some(workspace_id) = crate::resolve_owner_deleted_workspace(&channel) else {
                    return Ok(false);
                };
                crate::route_is_test_run(&workspace_id, "")
            }
            GuildDeletion::Thread { id } => {
                let Some((tab_id, _)) = crate::resolve_owner_deleted_tab(id) else {
                    return Ok(false);
                };
                let Some(tab) = crate::tab_list_result()?
                    .into_iter()
                    .find(|tab| tab.tab_id == tab_id)
                else {
                    return Ok(false);
                };
                crate::route_is_test_run(&tab.workspace_id, &tab_id)
            }
        })
        .await
        .map_err(chat_error)?
        .map_err(chat_error)
    }

    #[must_use]
    pub const fn topology_cache(&self) -> &TopologyCache {
        &self.topology_cache
    }

    async fn gone(&self, error: String, webhook_channel: Option<Id<ChannelMarker>>) -> ChatError {
        if error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR) {
            *self.topology_cache.lock().await = None;
        } else if let Some(channel) = webhook_channel
            && error.starts_with(UNKNOWN_WEBHOOK_DELIVERY_ERROR)
        {
            self.webhooks
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&channel);
        }
        ChatError::from(error)
    }

    fn cached_webhook(&self, channel: Id<ChannelMarker>) -> Option<(Id<WebhookMarker>, String)> {
        self.webhooks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&channel)
            .cloned()
    }

    async fn terminal_prompt_webhook(
        &self,
        channel: Id<ChannelMarker>,
    ) -> Result<(Id<WebhookMarker>, String), String> {
        if let Some(webhook) = self.cached_webhook(channel) {
            return Ok(webhook);
        }
        let webhook =
            resolve_terminal_prompt_webhook(&self.client, channel, TERMINAL_PROMPT_WEBHOOK_NAME)
                .await?;
        self.webhooks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(channel, webhook.clone());
        Ok(webhook)
    }

    #[must_use]
    pub fn component(&self, interaction: DiscordInteraction) -> Option<Component<Self>> {
        let Some(InteractionData::MessageComponent(data)) = interaction.data.as_ref() else {
            return None;
        };
        let route = interaction.channel.as_ref()?.id;
        let authorized = interaction.guild_id == Some(self.guild)
            && interaction
                .author_id()
                .is_some_and(|id| id.to_string() == self.owner_id);
        let (custom_id, values) = (data.custom_id.clone(), data.values.clone());
        Some(Component {
            interaction,
            route,
            custom_id,
            values,
            authorized,
        })
    }
}

impl Chat for Discord {
    type Route = Id<ChannelMarker>;
    type Message = Id<MessageMarker>;
    type Interaction = DiscordInteraction;
    type Posted = twilight_model::channel::Message;
    type Deletion = GuildDeletion;

    fn owner_id(&self) -> &str {
        &self.owner_id
    }

    fn scope(&self) -> ChatScope {
        self.scope
    }

    fn lifecycle_subscriptions(&self) -> Vec<serde_json::Value> {
        let mut subscriptions = crate::lifecycle_subscriptions();
        subscriptions.extend([
            serde_json::json!({"type":"workspace.renamed"}),
            serde_json::json!({"type":"tab.renamed"}),
        ]);
        subscriptions
    }

    async fn topology_changed(&self) -> Result<(), ChatError> {
        self.refresh_names().await
    }

    async fn accepts_route(&self, route: &Self::Route) -> Result<bool, ChatError> {
        self.channel_in_scope(*route).await
    }

    async fn sync_route(&self, route: &TopologyRoute) -> Result<Self::Route, ChatError> {
        self.check_topology_route(route).await?;
        let thread = sync_route(
            &self.client,
            self.guild,
            route,
            &self.topology_cache,
            &self.owner_id,
        )
        .await?;
        self.reconcile_synced_name(route, thread).await;
        Ok(thread)
    }

    async fn sync_route_with_parent(
        &self,
        route: &TopologyRoute,
    ) -> Result<(Self::Route, Self::Route), ChatError> {
        self.check_topology_route(route).await?;
        let thread = sync_route(
            &self.client,
            self.guild,
            route,
            &self.topology_cache,
            &self.owner_id,
        )
        .await?;
        self.reconcile_synced_name(route, thread).await;
        let guard = self.topology_cache.lock().await;
        let found = guard
            .as_ref()
            .and_then(|(channels, _)| workspace_channel_id(channels, &route.workspace_id));
        drop(guard);
        let workspace_channel = found.ok_or_else(|| {
            ChatError::from(
                "herdr topology error: workspace channel missing from cache after sync".to_owned(),
            )
        })?;
        Ok((workspace_channel, thread))
    }

    async fn cached_route(&self, route: &TopologyRoute) -> Result<Option<Self::Route>, ChatError> {
        self.check_topology_route(route).await?;
        let guard = self.topology_cache.lock().await;
        let Some((channels, active_threads)) = guard.as_ref() else {
            return Ok(None);
        };
        let channel = cached_route(channels, active_threads, route);
        drop(guard);
        Ok(channel?)
    }

    async fn register_archived(&self) -> Result<(), ChatError> {
        Ok(register_archived_tab_threads(&self.client, self.guild).await?)
    }

    async fn startup_sweep<F>(&self, routes: Vec<Result<TopologyRoute, String>>, live: F)
    where
        F: FnOnce() -> Result<(HashSet<String>, HashSet<String>), String> + Send,
    {
        let fetched = match self.fetch_scoped_topology().await {
            Ok(lists) => lists,
            Err(error) => {
                bridge_eprintln!("herdr startup topology error: {error}");
                return;
            }
        };
        let mut guard = self.topology_cache.lock().await;
        reconcile_topology_cache(&mut guard, fetched);
        drop(guard);
        for route in routes {
            let route = match route {
                Ok(route) => route,
                Err(error) => {
                    bridge_eprintln!("herdr startup topology error: {error}");
                    continue;
                }
            };
            if let Err(error) = self.check_topology_route(&route).await {
                bridge_eprintln!("herdr startup topology error: {error}");
                continue;
            }
            let mut guard = self.topology_cache.lock().await;
            let (channels, active_threads) =
                if let Some((channels, active_threads)) = guard.as_mut() {
                    (channels, active_threads)
                } else {
                    let fetched = match self.fetch_scoped_topology().await {
                        Ok(fetched) => fetched,
                        Err(error) => {
                            bridge_eprintln!("herdr startup topology error: {error}");
                            continue;
                        }
                    };
                    reconcile_topology_cache(&mut guard, fetched)
                };
            let result = sync_topology(
                &self.client,
                self.guild,
                channels,
                active_threads,
                &route,
                &self.owner_id,
            )
            .await;
            drop(guard);
            if let Err(error) = result {
                bridge_eprintln!("herdr startup topology error: {error}");
            }
        }
        if let Err(error) = self.refresh_names().await {
            bridge_eprintln!("herdr startup name reconciliation error: {error}");
        }
        let (workspaces, live_tabs) = match live() {
            Ok(ids) => ids,
            Err(error) => {
                bridge_eprintln!("herdr startup topology reconciliation error: {error}");
                return;
            }
        };
        let live_workspace_ids: HashSet<&str> = workspaces.iter().map(String::as_str).collect();
        let live_tab_ids: HashSet<&str> = live_tabs.iter().map(String::as_str).collect();
        let mut guard = self.topology_cache.lock().await;
        let (channels, active_threads) = if let Some((channels, active_threads)) = guard.as_mut() {
            (channels, active_threads)
        } else {
            let fetched = match self.fetch_scoped_topology().await {
                Ok(fetched) => fetched,
                Err(error) => {
                    bridge_eprintln!("herdr startup topology error: {error}");
                    return;
                }
            };
            reconcile_topology_cache(&mut guard, fetched)
        };
        let result = delete_topology_absent_from_herdr(
            &self.client,
            channels,
            active_threads,
            &live_workspace_ids,
            &live_tab_ids,
            self.ignore_test_runs,
        )
        .await;
        drop(guard);
        if let Err(error) = result {
            bridge_eprintln!("herdr startup topology reconciliation error: {error}");
        }
    }

    async fn close_topology(&self, closure: &TopologyClosure) -> Result<(), ChatError> {
        let fetched = self.fetch_scoped_topology().await?;
        let mut guard = self.topology_cache.lock().await;
        let (channels, active_threads) = reconcile_topology_cache(&mut guard, fetched);
        let result = match closure {
            TopologyClosure::Tab {
                workspace_id,
                tab_id,
            } => {
                let mut archived_cache: HashMap<
                    Id<ChannelMarker>,
                    Vec<twilight_model::channel::Channel>,
                > = HashMap::new();
                delete_tab_thread(
                    &self.client,
                    channels,
                    active_threads,
                    &mut archived_cache,
                    workspace_id,
                    tab_id,
                    self.ignore_test_runs,
                )
                .await
            }
            TopologyClosure::Workspace { workspace_id } => {
                delete_workspace_channel(
                    &self.client,
                    channels,
                    active_threads,
                    workspace_id,
                    self.ignore_test_runs,
                )
                .await
            }
        };
        if let Err(error) = result {
            bridge_eprintln!("herdr topology closure error: {error}");
        }
        Ok(())
    }

    async fn sync_route_fresh(&self, route: &TopologyRoute) -> Result<Self::Route, ChatError> {
        self.check_topology_route(route).await?;
        let fetched = self.fetch_scoped_topology().await?;
        let mut guard = self.topology_cache.lock().await;
        let (channels, active_threads) = reconcile_topology_cache(&mut guard, fetched);
        let thread = sync_topology(
            &self.client,
            self.guild,
            channels,
            active_threads,
            route,
            &self.owner_id,
        )
        .await?;
        drop(guard);
        self.reconcile_synced_name(route, thread).await;
        Ok(thread)
    }

    async fn post_live(
        &self,
        route: &Self::Route,
        text: &str,
        nonce: &str,
    ) -> Result<(), ChatError> {
        self.check_channel_scope(*route).await?;
        match deliver_live_message(&self.client, *route, text, nonce).await {
            Ok(_) => Ok(()),
            Err(error) => Err(self.gone(error, None).await),
        }
    }

    async fn post_transition_card(
        &self,
        route: &Self::Route,
        card: &TransitionMessage,
        nonce: &str,
    ) -> Result<Self::Message, ChatError> {
        self.check_channel_scope(*route).await?;
        match deliver_transition_card(&self.client, *route, card, nonce).await {
            Ok(message) => Ok(message),
            Err(error) => Err(self.gone(error, None).await),
        }
    }

    async fn post_activity(
        &self,
        route: &Self::Route,
        text: &str,
        _plan: &crate::ActivityPlan,
    ) -> Result<Self::Message, ChatError> {
        self.check_channel_scope(*route).await?;
        match deliver_activity_message(&self.client, *route, text).await {
            Ok(message) => Ok(message),
            Err(error) => Err(self.gone(error, None).await),
        }
    }

    async fn edit_activity(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        text: &str,
        _plan: &crate::ActivityPlan,
    ) -> Result<(), ChatError> {
        self.check_channel_scope(*route).await?;
        match update_activity_message(&self.client, *route, *message, text).await {
            Ok(()) => Ok(()),
            Err(error) => Err(self.gone(error, None).await),
        }
    }

    async fn post_as_owner(
        &self,
        parent: &Self::Route,
        thread: &Self::Route,
        owner: &OwnerIdentity,
        text: &str,
    ) -> Result<(), ChatError> {
        self.check_channel_scope(*parent).await?;
        self.check_channel_scope(*thread).await?;
        let result = async {
            let (webhook_id, webhook_token) = self.terminal_prompt_webhook(*parent).await?;
            execute_terminal_prompt_webhook(
                &self.client,
                webhook_id,
                &webhook_token,
                *thread,
                &owner.display_name,
                owner.avatar_url.as_deref(),
                text,
            )
            .await
        }
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) => Err(self.gone(error, Some(*parent)).await),
        }
    }

    async fn expire_card(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        text: &str,
    ) -> Result<(), ChatError> {
        self.check_channel_scope(*route).await?;
        Ok(expire_informational_card(&self.client, *route, *message, text).await?)
    }

    async fn post_question_card(
        &self,
        route: &Self::Route,
        question: &Question,
        token: &str,
    ) -> Result<Self::Message, ChatError> {
        self.check_channel_scope(*route).await?;
        Ok(deliver_question_card(&self.client, *route, question, token, &self.owner_id).await?)
    }

    async fn edit_question_card(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        question: &Question,
        token: &str,
    ) -> Result<(), ChatError> {
        self.check_channel_scope(*route).await?;
        Ok(update_question_card(
            &self.client,
            *route,
            *message,
            question,
            token,
            &self.owner_id,
        )
        .await?)
    }

    async fn post_permission_card(
        &self,
        route: &Self::Route,
        vendor: PermissionVendor,
        tool: &str,
        command: &str,
        token: &str,
    ) -> Result<Self::Message, ChatError> {
        self.check_channel_scope(*route).await?;
        Ok(deliver_permission_card(&self.client, *route, vendor, tool, command, token).await?)
    }

    async fn expire_permission_card(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        token: &str,
        text: &str,
    ) -> Result<(), ChatError> {
        self.check_channel_scope(*route).await?;
        Ok(expire_permission_card(&self.client, *route, *message, token, text).await?)
    }

    async fn acknowledge(
        &self,
        interaction: &Self::Interaction,
        ack: Ack<'_>,
    ) -> Result<(), ChatError> {
        if self.scope == ChatScope::TestRuns {
            let Some(channel) = interaction.channel.as_ref() else {
                return Ok(());
            };
            if !self.channel_in_scope(channel.id).await? {
                return Ok(());
            }
        }

        let interactions = self.client.interaction(interaction.application_id);
        match ack {
            Ack::Ephemeral(text) => {
                let response = InteractionResponse {
                    kind: InteractionResponseType::ChannelMessageWithSource,
                    data: Some(InteractionResponseData {
                        content: Some(text.to_owned()),
                        flags: Some(MessageFlags::EPHEMERAL),
                        ..InteractionResponseData::default()
                    }),
                };
                interactions
                    .create_response(interaction.id, &interaction.token, &response)
                    .await
                    .map(drop)
                    .map_err(chat_error)
            }
            Ack::Defer => {
                let response = InteractionResponse {
                    kind: InteractionResponseType::DeferredChannelMessageWithSource,
                    data: Some(InteractionResponseData {
                        flags: Some(MessageFlags::EPHEMERAL),
                        ..InteractionResponseData::default()
                    }),
                };
                interactions
                    .create_response(interaction.id, &interaction.token, &response)
                    .await
                    .map(drop)
                    .map_err(chat_error)
            }
            Ack::EditDeferred(text) => interactions
                .update_response(&interaction.token)
                .content(Some(text))
                .await
                .map(drop)
                .map_err(chat_error),
        }
    }

    async fn typing(&self, route: &Self::Route) -> Result<(), ChatError> {
        self.check_channel_scope(*route).await?;
        self.client
            .create_typing_trigger(*route)
            .await
            .map(drop)
            .map_err(chat_error)
    }

    async fn reply(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        text: &str,
    ) -> Result<(), ChatError> {
        self.check_channel_scope(*route).await?;
        self.client
            .create_message(*route)
            .content(text)
            .reply(*message)
            .await
            .map(drop)
            .map_err(chat_error)
    }

    async fn owner_message(
        &self,
        posted: Self::Posted,
    ) -> Result<Option<OwnerMessage<Self>>, ChatError> {
        if posted.guild_id != Some(self.guild)
            || !should_handle_owner_message(
                &posted.author.id.to_string(),
                posted.author.bot,
                &self.owner_id,
            )
        {
            return Ok(None);
        }
        let thread = self
            .client
            .channel(posted.channel_id)
            .await
            .map_err(chat_error)?
            .model()
            .await
            .map_err(chat_error)?;
        if !is_thread_channel(thread.kind) {
            return Ok(None);
        }
        let Some(parent_id) = thread.parent_id else {
            return Ok(None);
        };
        let parent = self
            .client
            .channel(parent_id)
            .await
            .map_err(chat_error)?
            .model()
            .await
            .map_err(chat_error)?;
        let Some(thread_name) = thread.name.as_deref() else {
            return Ok(None);
        };
        if self.scope == ChatScope::TestRuns
            && !is_test_run_name(Some(thread_name))
            && !is_test_run_name(parent.name.as_deref())
        {
            return Ok(None);
        }
        if self.ignore_test_runs
            && (is_test_run_name(Some(thread_name)) || is_test_run_name(parent.name.as_deref()))
        {
            return Ok(None);
        }
        let Some(topic) = parent.topic.as_deref() else {
            return Ok(None);
        };
        if self
            .herdr_surface_in_test_run(Some(thread_name), Some(topic))
            .await?
        {
            return Ok(None);
        }
        let Some((tab_id, workspace_id)) = prompt_surface_markers(thread_name, topic) else {
            return Ok(None);
        };
        if !self.accepts_herdr_route(workspace_id, tab_id).await? {
            return Ok(None);
        }
        Ok(Some(OwnerMessage {
            route: posted.channel_id,
            message: posted.id,
            content: posted.content,
            tab_id: tab_id.to_owned(),
            workspace_id: workspace_id.to_owned(),
        }))
    }

    async fn next_event(&self) -> Option<Result<Inbound<Self>, ChatError>> {
        loop {
            let event = self
                .shard
                .lock()
                .await
                .next_event(
                    EventTypeFlags::MESSAGE_CREATE
                        | EventTypeFlags::INTERACTION_CREATE
                        | EventTypeFlags::THREAD_DELETE
                        | EventTypeFlags::CHANNEL_DELETE
                        | EventTypeFlags::CHANNEL_UPDATE
                        | EventTypeFlags::THREAD_UPDATE
                        | EventTypeFlags::CHANNEL_CREATE
                        | EventTypeFlags::THREAD_CREATE,
                )
                .await?;
            match event {
                Ok(Event::MessageCreate(message)) => {
                    if message.guild_id != Some(self.guild) {
                        continue;
                    }
                    match self.chat_channel_in_scope(message.channel_id).await {
                        Ok(true) => return Some(Ok(Inbound::Posted(message.0))),
                        Ok(false) => {}
                        Err(error) => return Some(Err(error)),
                    }
                }
                Ok(Event::InteractionCreate(interaction)) => {
                    if let Some(component) = self.component(interaction.0) {
                        match self.channel_in_scope(component.route).await {
                            Ok(true) => {}
                            Ok(false) => continue,
                            Err(error) => return Some(Err(error)),
                        }
                        match self.channel_in_test_run(component.route).await {
                            Ok(true) => {}
                            Ok(false) => return Some(Ok(Inbound::Component(component))),
                            Err(error) => return Some(Err(error)),
                        }
                    }
                }
                Ok(Event::ThreadDelete(thread)) if thread.guild_id == self.guild => {
                    let test_run = owned_thread_is_test_run(thread.id)
                        || crate::topology::owned_channel_is_test_run(thread.parent_id);
                    if (self.scope == ChatScope::TestRuns && !test_run)
                        || (self.ignore_test_runs && test_run)
                    {
                        continue;
                    }
                    let deletion = GuildDeletion::Thread { id: thread.id };
                    match self.deletion_in_test_run(&deletion).await {
                        Ok(true) => {}
                        Ok(false) => return Some(Ok(Inbound::Deleted(deletion))),
                        Err(error) => return Some(Err(error)),
                    }
                }
                Ok(Event::ChannelDelete(channel)) if channel.guild_id == Some(self.guild) => {
                    if self.scope == ChatScope::TestRuns
                        && !is_test_run_name(channel.name.as_deref())
                    {
                        continue;
                    }
                    let deletion = GuildDeletion::Channel(Box::new(channel.0));
                    match self.deletion_in_test_run(&deletion).await {
                        Ok(true) => {}
                        Ok(false) => return Some(Ok(Inbound::Deleted(deletion))),
                        Err(error) => return Some(Err(error)),
                    }
                }
                Ok(Event::ChannelCreate(channel)) if channel.guild_id == Some(self.guild) => {
                    crate::remember_workspace_channels(std::slice::from_ref(&channel.0));
                }
                Ok(Event::ChannelUpdate(channel)) if channel.guild_id == Some(self.guild) => {
                    crate::remember_workspace_channels(std::slice::from_ref(&channel.0));
                }
                Ok(Event::ThreadCreate(thread)) if thread.guild_id == Some(self.guild) => {
                    crate::remember_tab_threads(std::slice::from_ref(&thread.0));
                }
                Ok(Event::ThreadUpdate(thread)) if thread.guild_id == Some(self.guild) => {
                    crate::remember_tab_threads(std::slice::from_ref(&thread.0));
                }
                Ok(_) => {}
                Err(error) => return Some(Err(chat_error(error))),
            }
        }
    }

    async fn handle_deletion<F>(&self, deletion: Self::Deletion, close: F) -> Result<(), ChatError>
    where
        F: FnOnce(Closed) -> Result<(), String> + Send + 'static,
    {
        if !self.deletion_in_scope(&deletion).await? {
            return Ok(());
        }
        Ok(handle_guild_deletion(
            &self.client,
            self.guild,
            &self.owner_id,
            &self.topology_cache,
            deletion,
            close,
        )
        .await?)
    }
}

#[must_use]
const fn is_thread_channel(kind: ChannelType) -> bool {
    kind.is_thread()
}

#[must_use]
fn prompt_surface_markers<'a, 'b>(
    thread_name: &'a str,
    topic: &'b str,
) -> Option<(&'a str, &'b str)> {
    let tab_id = thread_name
        .rsplit_once(" [")
        .and_then(|(_, suffix)| suffix.strip_suffix(']'))
        .filter(|value| !value.trim().is_empty())?;
    let workspace_id = topic
        .strip_prefix("herdr workspace [")
        .and_then(|value| value.strip_suffix(']'))
        .filter(|value| !value.trim().is_empty())?;
    Some((tab_id, workspace_id))
}

#[cfg(test)]
mod tests {
    use twilight_model::channel::ChannelType;

    use super::{is_thread_channel, prompt_surface_markers};

    #[test]
    fn only_thread_channel_kinds_are_prompt_surfaces() {
        let cases = [
            (ChannelType::GuildText, false),
            (ChannelType::GuildCategory, false),
            (ChannelType::GuildForum, false),
            (ChannelType::AnnouncementThread, true),
            (ChannelType::PublicThread, true),
            (ChannelType::PrivateThread, true),
        ];
        for (kind, expected) in cases {
            assert_eq!(is_thread_channel(kind), expected);
        }
    }

    #[test]
    fn qualifying_prompt_surface_requires_both_discord_markers() {
        let cases = [
            (
                "bridge [real-workspace:tab-1]",
                "herdr workspace [real-workspace]",
                true,
            ),
            ("bridge", "herdr workspace [real-workspace]", false),
            ("bridge [real-workspace:tab-1]", "workspace", false),
        ];
        for (thread_name, topic, expected) in cases {
            assert_eq!(
                prompt_surface_markers(thread_name, topic).is_some(),
                expected
            );
        }
    }
}
