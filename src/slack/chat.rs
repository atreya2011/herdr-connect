use std::collections::HashSet;

use slack_morphism::prelude::*;

use super::{
    Slack, SlackArchive, SlackRoute, SlackSocketEvent, delivery::escape, error, workspace_id,
};
use crate::{
    Ack, Chat, ChatError, Closed, Component, Inbound, OwnerIdentity, OwnerMessage,
    PermissionVendor, Question, TopologyClosure, TopologyRoute, TransitionMessage,
};

impl Slack {
    async fn component(
        &self,
        action: SlackInteractionBlockActionsEvent,
    ) -> Result<Option<Component<Self>>, ChatError> {
        if action
            .user
            .as_ref()
            .is_none_or(|user| user.id != self.owner)
        {
            return Ok(None);
        }
        let Some(message) = action
            .message
            .as_ref()
            .filter(|message| message.sender.user.as_ref() == Some(&self.bot))
        else {
            return Ok(None);
        };
        let Some(thread) = message.origin.thread_ts.clone() else {
            return Ok(None);
        };
        let Some(channel) = action.channel.as_ref() else {
            return Ok(None);
        };
        let route = SlackRoute {
            channel: channel.id.clone(),
            thread: Some(thread),
        };
        let mut topology = self.topology.lock().await;
        if !topology.threads.values().any(|entry| entry == &route) {
            self.load(&mut topology).await?;
        }
        let mapped = topology
            .threads
            .iter()
            .find(|(_, entry)| *entry == &route)
            .and_then(|(tab, _)| {
                topology
                    .channels
                    .iter()
                    .find(|channel| channel.id == route.channel)
                    .and_then(|channel| {
                        workspace_id(channel).map(|workspace| (workspace.to_owned(), tab.clone()))
                    })
            });
        drop(topology);
        let Some((workspace, tab)) = mapped else {
            return Ok(None);
        };
        if self.excluded(&workspace, &tab).await? {
            return Ok(None);
        }
        let Some([pressed]) = action.actions.as_deref() else {
            return Ok(None);
        };
        let custom_id = pressed.action_id.to_string();
        let values = if let Some(token) = custom_id.strip_prefix("herdrask-multi:") {
            let selector = SlackActionId::from(format!("herdrask-picks:{token}"));
            action
                .state
                .as_ref()
                .and_then(|state| {
                    state
                        .values
                        .values()
                        .find_map(|values| values.get(&selector))
                })
                .and_then(|value| value.selected_options.as_ref())
                .map(|options| options.iter().map(|option| option.value.clone()).collect())
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        if custom_id.starts_with("herdrask-picks:") {
            return Ok(None);
        }
        Ok(Some(Component {
            interaction: action,
            route,
            custom_id,
            values,
            authorized: true,
        }))
    }
}

impl Chat for Slack {
    type Route = SlackRoute;
    type Message = SlackTs;
    type Interaction = SlackInteractionBlockActionsEvent;
    type Posted = SlackMessageEvent;
    type Deletion = SlackArchive;

    fn owner_id(&self) -> &str {
        &self.owner.0
    }

    async fn sync_route(&self, route: &TopologyRoute) -> Result<SlackRoute, ChatError> {
        self.sync(route, false).await
    }
    async fn sync_route_fresh(&self, route: &TopologyRoute) -> Result<SlackRoute, ChatError> {
        self.sync(route, true).await
    }
    async fn sync_route_with_parent(
        &self,
        route: &TopologyRoute,
    ) -> Result<(SlackRoute, SlackRoute), ChatError> {
        let thread = self.sync(route, false).await?;
        Ok((
            SlackRoute {
                channel: thread.channel.clone(),
                thread: None,
            },
            thread,
        ))
    }
    async fn cached_route(&self, route: &TopologyRoute) -> Result<Option<SlackRoute>, ChatError> {
        Ok(self
            .topology
            .lock()
            .await
            .threads
            .get(&route.tab_id)
            .cloned())
    }
    async fn register_archived(&self) -> Result<(), ChatError> {
        Ok(())
    }
    async fn startup_sweep<F>(&self, routes: Vec<Result<TopologyRoute, String>>, live: F)
    where
        F: FnOnce() -> Result<(HashSet<String>, HashSet<String>), String> + Send,
    {
        let result = match live() {
            Ok(live) => self.sweep(routes, live).await,
            Err(failure) => Err(error(failure)),
        };
        if let Err(failure) = result {
            crate::bridge_eprintln!("Slack startup topology error: {failure}");
        }
    }
    async fn close_topology(&self, closure: &TopologyClosure) -> Result<(), ChatError> {
        self.close(closure).await
    }
    async fn post_live(
        &self,
        route: &SlackRoute,
        text: &str,
        _nonce: &str,
    ) -> Result<(), ChatError> {
        self.send(route, text).await.map(drop)
    }
    async fn post_transition_card(
        &self,
        route: &SlackRoute,
        card: &TransitionMessage,
        _nonce: &str,
    ) -> Result<SlackTs, ChatError> {
        let mention = card
            .mention_user
            .as_ref()
            .map_or_else(String::new, |owner| format!("<@{owner}> "));
        self.send_content(
            route,
            SlackMessageContent::new().with_text(format!("{mention}{}", escape(&card.description))),
        )
        .await
    }
    async fn post_activity(&self, route: &SlackRoute, text: &str) -> Result<SlackTs, ChatError> {
        self.send(route, text).await
    }
    async fn edit_activity(
        &self,
        route: &SlackRoute,
        message: &SlackTs,
        text: &str,
    ) -> Result<(), ChatError> {
        self.update(route, message, text).await
    }
    async fn post_as_owner(
        &self,
        _parent: &SlackRoute,
        thread: &SlackRoute,
        _owner: &OwnerIdentity,
        text: &str,
    ) -> Result<(), ChatError> {
        self.mirror_prompt(thread, text).await
    }
    async fn expire_card(
        &self,
        route: &SlackRoute,
        message: &SlackTs,
        text: &str,
    ) -> Result<(), ChatError> {
        self.update(route, message, text).await
    }
    async fn post_question_card(
        &self,
        route: &SlackRoute,
        question: &Question,
        token: &str,
    ) -> Result<SlackTs, ChatError> {
        self.question_card(route, question, token).await
    }
    async fn edit_question_card(
        &self,
        route: &SlackRoute,
        message: &SlackTs,
        question: &Question,
        token: &str,
    ) -> Result<(), ChatError> {
        self.update_question(route, message, question, token).await
    }
    async fn post_permission_card(
        &self,
        route: &SlackRoute,
        vendor: PermissionVendor,
        tool: &str,
        command: &str,
        token: &str,
    ) -> Result<SlackTs, ChatError> {
        self.permission_card(route, vendor, tool, command, token)
            .await
    }
    async fn expire_permission_card(
        &self,
        route: &SlackRoute,
        message: &SlackTs,
        _token: &str,
        text: &str,
    ) -> Result<(), ChatError> {
        self.update(route, message, text).await
    }
    async fn acknowledge(
        &self,
        interaction: &SlackInteractionBlockActionsEvent,
        ack: Ack<'_>,
    ) -> Result<(), ChatError> {
        let text = match ack {
            Ack::Defer => return Ok(()),
            Ack::Ephemeral(text) | Ack::EditDeferred(text) => text,
        };
        let channel = interaction
            .channel
            .as_ref()
            .ok_or_else(|| error("Slack interaction has no channel"))?;
        let user = interaction
            .user
            .as_ref()
            .ok_or_else(|| error("Slack interaction has no user"))?;
        self.client
            .open_session(&self.token)
            .chat_post_ephemeral(
                &SlackApiChatPostEphemeralRequest::new(
                    channel.id.clone(),
                    user.id.clone(),
                    SlackMessageContent::new().with_text(escape(text)),
                )
                .opt_thread_ts(
                    interaction
                        .message
                        .as_ref()
                        .and_then(|message| message.origin.thread_ts.clone()),
                ),
            )
            .await
            .map_err(error)?;
        Ok(())
    }
    async fn typing(&self, route: &SlackRoute) -> Result<(), ChatError> {
        self.set_working(route, true).await
    }
    async fn agent_status(&self, route: &TopologyRoute, working: bool) -> Result<(), ChatError> {
        if let Some(found) = self.cached_route(route).await? {
            self.track_working(&found, working).await?;
        }
        Ok(())
    }
    async fn reply(
        &self,
        route: &SlackRoute,
        _message: &SlackTs,
        text: &str,
    ) -> Result<(), ChatError> {
        self.send(route, text).await.map(drop)
    }
    async fn owner_message(
        &self,
        posted: SlackMessageEvent,
    ) -> Result<Option<OwnerMessage<Self>>, ChatError> {
        Ok(self.resolve_owner_message(posted).await?.map(
            |(route, message, content, tab_id, workspace_id)| OwnerMessage {
                route,
                message,
                content,
                tab_id,
                workspace_id,
            },
        ))
    }
    async fn next_event(&self) -> Option<Result<Inbound<Self>, ChatError>> {
        loop {
            match self.next_socket_event().await? {
                SlackSocketEvent::Failure(failure) => return Some(Err(failure)),
                SlackSocketEvent::Push(event) => match event.event {
                    SlackEventCallbackBody::Message(message) => {
                        return Some(Ok(Inbound::Posted(message)));
                    }
                    SlackEventCallbackBody::ChannelArchive(archive) => {
                        return Some(Ok(Inbound::Deleted(SlackArchive(archive))));
                    }
                    _ => {}
                },
                SlackSocketEvent::Action(action) => match self.component(*action).await {
                    Ok(Some(component)) => return Some(Ok(Inbound::Component(component))),
                    Err(failure) => return Some(Err(failure)),
                    Ok(None) => {}
                },
            }
        }
    }
    async fn handle_deletion<F>(&self, deletion: SlackArchive, close: F) -> Result<(), ChatError>
    where
        F: FnOnce(Closed) -> Result<(), String> + Send + 'static,
    {
        self.close_archived(deletion, close).await
    }
}
