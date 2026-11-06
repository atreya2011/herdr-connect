use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use slack_morphism::prelude::*;
use tokio::sync::{Notify, mpsc::UnboundedSender};

use super::{Slack, SlackRoute, error, identity};
use crate::{ChatError, Closed};

pub enum SlackSocketEvent {
    Push(Box<SlackPushEventCallback>),
    Action(Box<SlackInteractionBlockActionsEvent>),
    Failure(ChatError),
}

pub struct SlackArchive(pub SlackChannelArchiveEvent);

impl fmt::Display for SlackArchive {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("channel_archive")
    }
}

struct SocketState {
    sender: UnboundedSender<SlackSocketEvent>,
    ready: Notify,
}

async fn on_hello(states: SlackClientEventsUserState) {
    if let Some(state) = states.read().await.get_user_state::<SocketState>() {
        state.ready.notify_one();
    }
}

async fn on_push(
    event: SlackPushEventCallback,
    _: Arc<SlackHyperClient>,
    states: SlackClientEventsUserState,
) -> UserCallbackResult<()> {
    let guard = states.read().await;
    let state = guard
        .get_user_state::<SocketState>()
        .ok_or("Slack event state missing")?;
    state
        .sender
        .send(SlackSocketEvent::Push(Box::new(event)))
        .map_err(|_| "Slack event queue closed")?;
    Ok(())
}

async fn on_action(
    event: SlackInteractionEvent,
    _: Arc<SlackHyperClient>,
    states: SlackClientEventsUserState,
) -> UserCallbackResult<()> {
    if let SlackInteractionEvent::BlockActions(action) = event {
        let guard = states.read().await;
        let state = guard
            .get_user_state::<SocketState>()
            .ok_or("Slack event state missing")?;
        state
            .sender
            .send(SlackSocketEvent::Action(Box::new(action)))
            .map_err(|_| "Slack event queue closed")?;
    }
    Ok(())
}

impl Slack {
    pub async fn connect_socket(&self, app_token: String) -> Result<(), ChatError> {
        let mut guard = self.listener.lock().await;
        if guard.is_some() {
            return Err(error("Slack Socket Mode is already connected"));
        }
        let environment = Arc::new(SlackClientEventsListenerEnvironment::new(Arc::clone(
            &self.state.client,
        )));
        environment
            .user_state
            .write()
            .await
            .set_user_state(SocketState {
                sender: self.state.events_tx.clone(),
                ready: Notify::new(),
            });
        let callbacks = SlackSocketModeListenerCallbacks::new()
            .with_hello_events(|_, _, states| on_hello(states))
            .with_push_events(on_push)
            .with_interaction_events(on_action);
        let listener = SlackClientSocketModeListener::new(
            &SlackClientSocketModeConfig::new().with_max_connections_count(1),
            Arc::clone(&environment),
            callbacks,
        );
        listener
            .listen_for(&SlackApiToken::new(app_token.into()))
            .await
            .map_err(error)?;
        listener.start().await;
        let ready = async {
            let states = environment.user_state.read().await;
            let state = states
                .get_user_state::<SocketState>()
                .ok_or_else(|| error("Slack event state missing"))?;
            state.ready.notified().await;
            Ok::<_, ChatError>(())
        };
        match tokio::time::timeout(Duration::from_secs(30), ready).await {
            Ok(Ok(())) => {
                *guard = Some(listener);
                drop(guard);
                Ok(())
            }
            result => {
                listener.shutdown().await;
                match result {
                    Ok(Err(failure)) => Err(failure),
                    _ => Err(error(
                        "Slack Socket Mode did not receive hello within 30 seconds",
                    )),
                }
            }
        }
    }

    pub async fn shutdown_socket(&self) -> Result<(), ChatError> {
        let routes: Vec<_> = self
            .state
            .working
            .lock()
            .await
            .drain()
            .filter_map(|(route, task)| {
                task.map(|task| {
                    task.abort();
                    route
                })
            })
            .collect();
        let mut result = Ok(());
        for route in routes {
            if let Err(failure) = self.set_working(&route, false).await {
                result = Err(failure);
            }
        }
        let listener = self.listener.lock().await.take();
        if let Some(listener) = listener {
            listener.shutdown().await;
        }
        result
    }

    pub async fn next_socket_event(&self) -> Option<SlackSocketEvent> {
        let mut events = self.events.lock().await;
        while let Some(event) = events.recv().await {
            let team = match &event {
                SlackSocketEvent::Push(event) => &event.team_id,
                SlackSocketEvent::Action(event) => &event.team.id,
                SlackSocketEvent::Failure(_) => return Some(event),
            };
            if team.to_string() == self.team {
                return Some(event);
            }
        }
        None
    }

    pub async fn resolve_owner_message(
        &self,
        posted: SlackMessageEvent,
    ) -> Result<Option<(SlackRoute, SlackTs, String, String, String)>, ChatError> {
        if posted.sender.user.as_ref() != Some(&self.owner)
            || posted.sender.bot_id.is_some()
            || !matches!(
                posted.subtype,
                None | Some(SlackMessageEventType::FileShare)
            )
        {
            return Ok(None);
        }
        let Some(channel) = posted.origin.channel else {
            return Ok(None);
        };
        let info = self
            .state
            .client
            .open_session(&self.state.token)
            .conversations_info(&SlackApiConversationsInfoRequest::new(channel.clone()))
            .await
            .map_err(error)?
            .channel;
        if !super::channel_scope(self.scope, self.ignore_test_runs, &info) {
            return Ok(None);
        }
        let Some((workspace, tab)) = identity(&info) else {
            return Ok(None);
        };
        if self.excluded(workspace, tab).await? {
            return Ok(None);
        }
        let route = SlackRoute { channel };
        let content = posted
            .content
            .and_then(|content| content.text)
            .unwrap_or_default();
        let content = decode_message_text(&content);
        Ok(Some((
            route,
            posted.origin.ts,
            content,
            tab.to_owned(),
            workspace.to_owned(),
        )))
    }

    pub(super) async fn excluded(&self, workspace: &str, tab: &str) -> Result<bool, ChatError> {
        if self.scope == crate::ChatScope::Service && !self.ignore_test_runs {
            return Ok(false);
        }
        let (workspace, tab) = (workspace.to_owned(), tab.to_owned());
        let test_run =
            tokio::task::spawn_blocking(move || crate::route_is_test_run(&workspace, &tab))
                .await
                .map_err(error)?
                .map_err(error)?;
        Ok((self.scope == crate::ChatScope::TestRuns && !test_run)
            || (self.ignore_test_runs && test_run))
    }

    pub(super) async fn accepts_channel(
        &self,
        channel: &SlackChannelId,
    ) -> Result<bool, ChatError> {
        let channel = self
            .state
            .client
            .open_session(&self.state.token)
            .conversations_info(&SlackApiConversationsInfoRequest::new(channel.clone()))
            .await
            .map_err(error)?
            .channel;
        if !super::channel_scope(self.scope, self.ignore_test_runs, &channel) {
            return Ok(false);
        }
        let Some((workspace, tab)) = identity(&channel) else {
            return Ok(false);
        };
        Ok(!self.excluded(workspace, tab).await?)
    }

    pub async fn close_archived<F>(&self, event: SlackArchive, close: F) -> Result<(), ChatError>
    where
        F: FnOnce(Closed) -> Result<(), String> + Send + 'static,
    {
        if event.0.user != self.owner {
            return Ok(());
        }
        let mut topology = self.state.topology.lock().await;
        let channel = self
            .state
            .client
            .open_session(&self.state.token)
            .conversations_info(&SlackApiConversationsInfoRequest::new(
                event.0.channel.clone(),
            ))
            .await
            .map_err(error)?
            .channel;
        if !super::channel_scope(self.scope, self.ignore_test_runs, &channel) {
            return Ok(());
        }
        let Some((workspace, tab)) = identity(&channel) else {
            return Ok(());
        };
        if self.excluded(workspace, tab).await? {
            return Ok(());
        }
        let tab = tab.to_owned();
        tokio::task::spawn_blocking(move || close(Closed::Tab(tab)))
            .await
            .map_err(error)?
            .map_err(error)?;
        topology.channels.retain(|entry| entry.id != channel.id);
        topology.tabs.retain(|_, route| route.channel != channel.id);
        drop(topology);
        Ok(())
    }
}

fn decode_message_text(text: &str) -> String {
    let mut remaining = text;
    let mut decoded = String::new();
    while let Some(start) = remaining.find('<') {
        decoded.push_str(&remaining[..start]);
        let tail = &remaining[start + 1..];
        let Some(end) = tail.find('>') else {
            decoded.push_str(&remaining[start..]);
            remaining = "";
            break;
        };
        let tag = &tail[..end];
        let (target, label) = tag
            .split_once('|')
            .map_or((tag, None), |(target, label)| (target, Some(label)));
        if let Some(user) = target.strip_prefix('@') {
            decoded.push('@');
            decoded.push_str(user);
        } else if let Some(channel) = target.strip_prefix('#') {
            decoded.push('#');
            decoded.push_str(label.unwrap_or(channel));
        } else if target.starts_with("https://") || target.starts_with("http://") {
            if let Some(label) = label {
                decoded.push_str(label);
                decoded.push_str(" (");
                decoded.push_str(target);
                decoded.push(')');
            } else {
                decoded.push_str(target);
            }
        } else {
            decoded.push('<');
            decoded.push_str(tag);
            decoded.push('>');
        }
        remaining = &tail[end + 1..];
    }
    decoded.push_str(remaining);
    decoded
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}
