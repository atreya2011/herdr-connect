use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;

use twilight_http::Client;
use twilight_model::application::interaction::{
    Interaction as DiscordInteraction, InteractionData,
};
use twilight_model::channel::message::MessageFlags;
use twilight_model::http::interaction::{
    InteractionResponse, InteractionResponseData, InteractionResponseType,
};
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, MessageMarker},
};

use crate::chat::{Ack, Chat, ChatError, Component, TopologyClosure};
use crate::delivery::expire_permission_card;
use crate::{
    PermissionVendor, Question, TopologyCache, TopologyRoute, bridge_eprintln, cached_route,
    delete_tab_thread, delete_topology_absent_from_herdr, delete_workspace_channel,
    deliver_permission_card, deliver_question_card, expire_informational_card,
    fetch_topology_lists, reconcile_topology_cache, register_archived_tab_threads, sync_topology,
    update_question_card, workspace_channel_id,
};

pub struct Discord {
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: String,
    topology_cache: TopologyCache,
}

fn chat_error(error: impl Display) -> ChatError {
    ChatError::from(error.to_string())
}

pub async fn sync_route(
    client: &Client,
    guild: Id<GuildMarker>,
    route: &TopologyRoute,
    topology_cache: &TopologyCache,
) -> Result<Id<ChannelMarker>, String> {
    let mut guard = topology_cache.lock().await;
    sync_route_locked(client, guild, route, &mut guard).await
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
    sync_topology(client, guild, channels, active_threads, route)
        .await
        .map_err(|error| format!("discord topology error: {error}"))
}

impl Discord {
    #[must_use]
    pub const fn new(
        client: Arc<Client>,
        guild: Id<GuildMarker>,
        owner_id: String,
        topology_cache: TopologyCache,
    ) -> Self {
        Self {
            client,
            guild,
            owner_id,
            topology_cache,
        }
    }

    #[must_use]
    pub const fn topology_cache(&self) -> &TopologyCache {
        &self.topology_cache
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

    async fn sync_route(&self, route: &TopologyRoute) -> Result<Self::Route, ChatError> {
        Ok(sync_route(&self.client, self.guild, route, &self.topology_cache).await?)
    }

    async fn sync_route_with_parent(
        &self,
        route: &TopologyRoute,
    ) -> Result<(Self::Route, Self::Route), ChatError> {
        let thread = sync_route(&self.client, self.guild, route, &self.topology_cache).await?;
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
        let fetched = match fetch_topology_lists(&self.client, self.guild).await {
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
            let mut guard = self.topology_cache.lock().await;
            let (channels, active_threads) =
                if let Some((channels, active_threads)) = guard.as_mut() {
                    (channels, active_threads)
                } else {
                    let fetched = match fetch_topology_lists(&self.client, self.guild).await {
                        Ok(fetched) => fetched,
                        Err(error) => {
                            bridge_eprintln!("herdr startup topology error: {error}");
                            continue;
                        }
                    };
                    reconcile_topology_cache(&mut guard, fetched)
                };
            let result =
                sync_topology(&self.client, self.guild, channels, active_threads, &route).await;
            drop(guard);
            if let Err(error) = result {
                bridge_eprintln!("herdr startup topology error: {error}");
            }
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
            let fetched = match fetch_topology_lists(&self.client, self.guild).await {
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
        )
        .await;
        drop(guard);
        if let Err(error) = result {
            bridge_eprintln!("herdr startup topology reconciliation error: {error}");
        }
    }

    async fn close_topology(&self, closure: &TopologyClosure) -> Result<(), ChatError> {
        let fetched = fetch_topology_lists(&self.client, self.guild).await?;
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
                )
                .await
            }
            TopologyClosure::Workspace { workspace_id } => {
                delete_workspace_channel(&self.client, channels, workspace_id).await
            }
        };
        if let Err(error) = result {
            bridge_eprintln!("herdr topology closure error: {error}");
        }
        Ok(())
    }

    async fn sync_route_fresh(&self, route: &TopologyRoute) -> Result<Self::Route, ChatError> {
        let fetched = fetch_topology_lists(&self.client, self.guild).await?;
        let mut guard = self.topology_cache.lock().await;
        let (channels, active_threads) = reconcile_topology_cache(&mut guard, fetched);
        Ok(sync_topology(&self.client, self.guild, channels, active_threads, route).await?)
    }

    async fn expire_card(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        text: &str,
    ) -> Result<(), ChatError> {
        Ok(expire_informational_card(&self.client, *route, *message, text).await?)
    }

    async fn post_question_card(
        &self,
        route: &Self::Route,
        question: &Question,
        token: &str,
    ) -> Result<Self::Message, ChatError> {
        Ok(deliver_question_card(&self.client, *route, question, token, &self.owner_id).await?)
    }

    async fn edit_question_card(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        question: &Question,
        token: &str,
    ) -> Result<(), ChatError> {
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
        Ok(deliver_permission_card(&self.client, *route, vendor, tool, command, token).await?)
    }

    async fn expire_permission_card(
        &self,
        route: &Self::Route,
        message: &Self::Message,
        token: &str,
        text: &str,
    ) -> Result<(), ChatError> {
        Ok(expire_permission_card(&self.client, *route, *message, token, text).await?)
    }

    async fn acknowledge(
        &self,
        interaction: &Self::Interaction,
        ack: Ack<'_>,
    ) -> Result<(), ChatError> {
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
}
