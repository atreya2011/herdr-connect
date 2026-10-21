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

use crate::chat::{Ack, Chat, ChatError, Component};
use crate::delivery::expire_permission_card;
use crate::{
    PermissionVendor, Question, TopologyCache, TopologyRoute, deliver_permission_card,
    deliver_question_card, expire_informational_card, fetch_topology_lists,
    reconcile_topology_cache, sync_topology, update_question_card,
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
