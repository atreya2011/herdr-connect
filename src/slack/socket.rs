use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use slack_morphism::prelude::*;
use tokio::sync::{Notify, mpsc::UnboundedSender};

use super::{Slack, SlackRoute, error, tab_id, workspace_id};
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
            &self.client,
        )));
        environment
            .user_state
            .write()
            .await
            .set_user_state(SocketState {
                sender: self.events_tx.clone(),
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

    pub async fn shutdown_socket(&self) {
        let routes: Vec<_> = self
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
        for route in routes {
            if let Err(failure) = self.set_working(&route, false).await {
                crate::bridge_eprintln!("Slack status cleanup error: {failure}");
            }
        }
        let listener = self.listener.lock().await.take();
        if let Some(listener) = listener {
            listener.shutdown().await;
        }
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
        let Some(thread) = posted.origin.thread_ts.filter(|ts| ts != &posted.origin.ts) else {
            return Ok(None);
        };
        let info = self
            .client
            .open_session(&self.token)
            .conversations_info(&SlackApiConversationsInfoRequest::new(channel.clone()))
            .await
            .map_err(error)?
            .channel;
        if self.ignore_test_runs
            && info
                .name
                .as_deref()
                .is_some_and(|name| name.starts_with("testrun-"))
        {
            return Ok(None);
        }
        let Some(workspace) = workspace_id(&info) else {
            return Ok(None);
        };
        let messages = self.history(&channel).await?;
        let Some(parent) = messages.iter().find(|message| message.origin.ts == thread) else {
            return Ok(None);
        };
        if self.ignore_test_runs
            && parent
                .content
                .text
                .as_deref()
                .is_some_and(|text| text.starts_with("testrun-"))
        {
            return Ok(None);
        }
        let Some(tab) = tab_id(parent, workspace, &self.bot) else {
            return Ok(None);
        };
        if self.excluded(workspace, &tab).await? {
            return Ok(None);
        }
        let content = posted
            .content
            .and_then(|content| content.text)
            .unwrap_or_default();
        Ok(Some((
            SlackRoute {
                channel,
                thread: Some(thread),
            },
            posted.origin.ts,
            content,
            tab,
            workspace.to_owned(),
        )))
    }

    pub(super) async fn excluded(&self, workspace: &str, tab: &str) -> Result<bool, ChatError> {
        if !self.ignore_test_runs {
            return Ok(false);
        }
        let (workspace, tab) = (workspace.to_owned(), tab.to_owned());
        tokio::task::spawn_blocking(move || crate::route_is_test_run(&workspace, &tab))
            .await
            .map_err(error)?
            .map_err(error)
    }

    pub async fn close_archived<F>(&self, event: SlackArchive, close: F) -> Result<(), ChatError>
    where
        F: FnOnce(Closed) -> Result<(), String> + Send + 'static,
    {
        if event.0.user != self.owner {
            return Ok(());
        }
        let mut topology = self.topology.lock().await;
        let channel = self
            .client
            .open_session(&self.token)
            .conversations_info(&SlackApiConversationsInfoRequest::new(
                event.0.channel.clone(),
            ))
            .await
            .map_err(error)?
            .channel;
        if self.ignore_test_runs
            && channel
                .name
                .as_deref()
                .is_some_and(|name| name.starts_with("testrun-"))
        {
            return Ok(());
        }
        let Some(workspace) = workspace_id(&channel) else {
            return Ok(());
        };
        if self.excluded(workspace, "").await? {
            return Ok(());
        }
        let workspace = workspace.to_owned();
        tokio::task::spawn_blocking(move || close(Closed::Workspace(workspace)))
            .await
            .map_err(error)?
            .map_err(error)?;
        topology.channels.retain(|entry| entry.id != channel.id);
        topology
            .threads
            .retain(|_, route| route.channel != channel.id);
        drop(topology);
        Ok(())
    }
}
