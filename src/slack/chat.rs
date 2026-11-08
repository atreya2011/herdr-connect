use std::collections::HashSet;

use slack_morphism::prelude::*;

use super::{Slack, SlackArchive, SlackRoute, SlackSocketEvent, delivery::escape, error, identity};
use crate::{
    Ack, Chat, ChatError, Closed, Component, Inbound, OwnerIdentity, OwnerMessage,
    PermissionVendor, Question, Renamed, TopologyClosure, TopologyRoute, TransitionMessage,
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
        let Some([pressed]) = action.actions.as_deref() else {
            return Ok(None);
        };
        let custom_id = pressed.action_id.to_string();
        if custom_id.starts_with("herdrask-picks:") {
            return Ok(None);
        }
        let Some(_message) = action
            .message
            .as_ref()
            .filter(|message| message.sender.user.as_ref() == Some(&self.bot))
        else {
            return Ok(None);
        };
        let Some(channel) = action.channel.as_ref() else {
            return Ok(None);
        };
        let route = SlackRoute {
            channel: channel.id.clone(),
        };
        if !self.accepts_channel(&route.channel).await? {
            return Ok(None);
        }
        let mut topology = self.state.topology.lock().await;
        if !topology.tabs.values().any(|entry| entry == &route) {
            self.load(&mut topology).await?;
        }
        let mapped = topology
            .tabs
            .iter()
            .find(|(_, entry)| *entry == &route)
            .and_then(|(tab, _)| {
                topology
                    .channels
                    .iter()
                    .find(|channel| channel.id == route.channel)
                    .and_then(|channel| {
                        identity(channel).map(|(workspace, _)| (workspace.to_owned(), tab.clone()))
                    })
            });
        drop(topology);
        let Some((workspace, tab)) = mapped else {
            return Ok(None);
        };
        if self.excluded(&workspace, &tab).await? {
            return Ok(None);
        }
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
        if custom_id.starts_with("herdrask-multi:") && values.is_empty() {
            self.acknowledge(
                &action,
                Ack::Ephemeral("refused: choose at least one option"),
            )
            .await?;
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
    type Rename = SlackMessageEvent;
    const DELETES_ACTIVITY: bool = true;
    const PROMPT_ACTIVITY: bool = true;

    fn owner_id(&self) -> &str {
        &self.owner.0
    }
    fn scope(&self) -> crate::ChatScope {
        self.scope
    }
    async fn accepts_route(&self, route: &SlackRoute) -> Result<bool, ChatError> {
        if self.scope == crate::ChatScope::Service {
            return Ok(true);
        }
        self.accepts_channel(&route.channel).await
    }
    async fn owner_message_accepted(
        &self,
        route: &SlackRoute,
        message: &SlackTs,
    ) -> Result<(), ChatError> {
        if !self.accepts_route(route).await? {
            return Ok(());
        }
        self.use_status_message(route, message).await;
        self.track_working(route, true).await
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
        let channel = self.sync(route, false).await?;
        Ok((channel.clone(), channel))
    }
    async fn cached_route(&self, route: &TopologyRoute) -> Result<Option<SlackRoute>, ChatError> {
        if self.scope == crate::ChatScope::TestRuns
            && self.excluded(&route.workspace_id, &route.tab_id).await?
        {
            return Ok(None);
        }
        let found = self
            .state
            .topology
            .lock()
            .await
            .tabs
            .get(&route.tab_id)
            .cloned();
        if let Some(found) = &found
            && !self.accepts_route(found).await?
        {
            return Ok(None);
        }
        Ok(found)
    }
    async fn register_archived(&self) -> Result<(), ChatError> {
        Ok(())
    }
    async fn startup_sweep<F>(&self, routes: Vec<Result<TopologyRoute, String>>, live: F)
    where
        F: FnOnce() -> Result<(HashSet<String>, HashSet<String>), String> + Send,
    {
        let result = self.sweep(routes, live).await;
        if let Err(failure) = result {
            crate::bridge_eprintln!("Slack startup topology error: {failure}");
        }
    }
    async fn close_topology(&self, closure: &TopologyClosure) -> Result<(), ChatError> {
        let state = self.state.clone();
        let closure = closure.clone();
        let ignore_test_runs = self.ignore_test_runs;
        let scope = self.scope;
        tokio::spawn(async move {
            if let Err(failure) = state.close(&closure, scope, ignore_test_runs).await {
                crate::bridge_eprintln!("Slack topology closure error: {failure}");
            }
        });
        Ok(())
    }
    async fn post_live(
        &self,
        route: &SlackRoute,
        text: &str,
        _nonce: &str,
    ) -> Result<(), ChatError> {
        self.send_markdown(route, text).await.map(drop)
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
    async fn post_activity(
        &self,
        route: &SlackRoute,
        pane_id: &str,
        text: &str,
        plan: &crate::ActivityPlan,
    ) -> Result<SlackTs, ChatError> {
        self.follow_plan(route, plan).await;
        self.send_content(
            route,
            super::delivery::activity_content(text, plan, pane_id, None),
        )
        .await
    }
    async fn edit_activity(
        &self,
        route: &SlackRoute,
        message: &SlackTs,
        pane_id: &str,
        text: &str,
        plan: &crate::ActivityPlan,
    ) -> Result<(), ChatError> {
        self.follow_plan(route, plan).await;
        self.update_content(
            route,
            message,
            super::delivery::activity_content(
                text,
                plan,
                pane_id,
                Some(format!(
                    "activity-{message}-{}-{}",
                    plan.count, plan.prompts
                )),
            ),
        )
        .await?;
        self.refresh_working(route, "edit").await;
        Ok(())
    }
    async fn delete_activity(
        &self,
        route: &SlackRoute,
        message: &SlackTs,
    ) -> Result<(), ChatError> {
        self.require_route(route).await?;
        self.forget_plan(route).await;
        self.state
            .client
            .open_session(&self.state.token)
            .chat_delete(&SlackApiChatDeleteRequest::new(
                route.channel.clone(),
                message.clone(),
            ))
            .await
            .map_err(error)?;
        Ok(())
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
        let question = self.question_text.lock().await.remove(message);
        if text == "resolved: pane left blocked" {
            let text = question.map_or_else(
                || "resolved".to_owned(),
                |question| format!("{question}\n\nResolved"),
            );
            self.update(route, message, &text).await
        } else {
            self.update(route, message, text).await
        }
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
        if !self
            .accepts_route(&SlackRoute {
                channel: channel.id.clone(),
            })
            .await?
        {
            return Ok(());
        }
        self.state
            .client
            .open_session(&self.state.token)
            .chat_post_ephemeral(&SlackApiChatPostEphemeralRequest::new(
                channel.id.clone(),
                user.id.clone(),
                SlackMessageContent::new().with_text(escape(text)),
            ))
            .await
            .map_err(error)?;
        Ok(())
    }
    async fn typing(&self, route: &SlackRoute) -> Result<(), ChatError> {
        self.track_working(route, true).await
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
        if !self.accepts_route(route).await? {
            return Ok(());
        }
        if text.starts_with("accepted:") {
            self.state
                .client
                .open_session(&self.state.token)
                .chat_post_ephemeral(&SlackApiChatPostEphemeralRequest::new(
                    route.channel.clone(),
                    self.owner.clone(),
                    SlackMessageContent::new().with_text(escape(text)),
                ))
                .await
                .map_err(error)?;
            Ok(())
        } else {
            self.send(route, text).await.map(drop)
        }
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
                        let Some(channel) = message.origin.channel.as_ref() else {
                            continue;
                        };
                        match self
                            .accepts_route(&SlackRoute {
                                channel: channel.clone(),
                            })
                            .await
                        {
                            Ok(true)
                                if message.subtype == Some(SlackMessageEventType::ChannelName) =>
                            {
                                return Some(Ok(Inbound::Renamed(message)));
                            }
                            Ok(true) => return Some(Ok(Inbound::Posted(message))),
                            Ok(false) => {}
                            Err(failure) => return Some(Err(failure)),
                        }
                    }
                    SlackEventCallbackBody::ChannelArchive(archive) => {
                        match self
                            .accepts_route(&SlackRoute {
                                channel: archive.channel.clone(),
                            })
                            .await
                        {
                            Ok(true) => return Some(Ok(Inbound::Deleted(SlackArchive(archive)))),
                            Ok(false) => {}
                            Err(failure) => return Some(Err(failure)),
                        }
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
    async fn handle_rename<F>(&self, rename: SlackMessageEvent, apply: F) -> Result<(), ChatError>
    where
        F: FnOnce(Renamed) -> Result<(), String> + Send + 'static,
    {
        self.rename_channel(rename, apply).await
    }
}
