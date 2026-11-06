use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;

use slack_morphism::prelude::*;
use tokio::sync::Mutex;

use crate::{ChatError, TopologyClosure, TopologyRoute, workspace_list_result};

mod cards;
mod chat;
mod delivery;
mod socket;
pub use socket::{SlackArchive, SlackSocketEvent};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SlackRoute {
    pub channel: SlackChannelId,
    pub thread: Option<SlackTs>,
}

#[derive(Default)]
struct Topology {
    channels: Vec<SlackChannelInfo>,
    threads: HashMap<String, SlackRoute>,
    loaded: bool,
}

pub struct Slack {
    client: Arc<SlackHyperClient>,
    token: SlackApiToken,
    owner: SlackUserId,
    bot: SlackUserId,
    team: String,
    topology: Mutex<Topology>,
    ignore_test_runs: bool,
    listener: Mutex<Option<SlackClientSocketModeListener<SlackClientHyperHttpsConnector>>>,
    events_tx: tokio::sync::mpsc::UnboundedSender<SlackSocketEvent>,
    events: Mutex<tokio::sync::mpsc::UnboundedReceiver<SlackSocketEvent>>,
    working: Mutex<HashMap<SlackRoute, Option<tokio::task::JoinHandle<()>>>>,
}

fn error(error: impl Display) -> ChatError {
    ChatError::from(error.to_string())
}

fn workspace_id(channel: &SlackChannelInfo) -> Option<&str> {
    channel
        .topic
        .as_ref()?
        .value
        .strip_prefix("herdr workspace [")?
        .strip_suffix(']')
}

fn tab_id(message: &SlackHistoryMessage, workspace: &str, bot: &SlackUserId) -> Option<String> {
    if message.sender.user.as_ref() != Some(bot)
        || message
            .origin
            .thread_ts
            .as_ref()
            .is_some_and(|ts| ts != &message.origin.ts)
    {
        return None;
    }
    let text = message.content.text.as_deref()?;
    let (_, suffix) = text.rsplit_once(" [")?;
    let id = suffix.strip_suffix(']')?;
    id.strip_prefix(workspace)?
        .starts_with(":t")
        .then(|| id.to_owned())
}

fn channel_name(label: &str) -> Result<String, ChatError> {
    let name: String = label
        .to_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-' {
                ch
            } else {
                '-'
            }
        })
        .take(80)
        .collect();
    let name = name.trim_matches('-');
    if name.is_empty() {
        return Err(error("workspace label has no Slack channel name"));
    }
    Ok(name.to_owned())
}

impl Slack {
    pub async fn new(token: String, owner: String, team: &str) -> Result<Self, ChatError> {
        let client = Arc::new(SlackClient::new(
            SlackClientHyperConnector::new().map_err(error)?,
        ));
        let token = SlackApiToken::new(token.into());
        let auth = client
            .open_session(&token)
            .auth_test()
            .await
            .map_err(error)?;
        if auth.team_id.to_string() != team {
            return Err(error("Slack token belongs to another team"));
        }
        let (events_tx, events) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            client,
            token,
            owner: owner.into(),
            bot: auth.user_id,
            team: team.to_owned(),
            topology: Mutex::new(Topology::default()),
            ignore_test_runs: false,
            listener: Mutex::new(None),
            events_tx,
            events: Mutex::new(events),
            working: Mutex::new(HashMap::new()),
        })
    }

    #[must_use]
    pub const fn with_ignore_test_runs(mut self, ignore: bool) -> Self {
        self.ignore_test_runs = ignore;
        self
    }

    pub async fn channels(&self) -> Result<Vec<SlackChannelInfo>, ChatError> {
        let session = self.client.open_session(&self.token);
        let mut request = SlackApiConversationsListRequest::new()
            .with_limit(200)
            .with_exclude_archived(true);
        let mut channels = Vec::new();
        loop {
            let response = session.conversations_list(&request).await.map_err(error)?;
            channels.extend(response.channels);
            let cursor = response
                .response_metadata
                .and_then(|meta| meta.next_cursor)
                .filter(|cursor| !cursor.to_string().is_empty());
            if cursor.is_none() {
                return Ok(channels);
            }
            request = request.opt_cursor(cursor);
        }
    }

    pub async fn history(
        &self,
        channel: &SlackChannelId,
    ) -> Result<Vec<SlackHistoryMessage>, ChatError> {
        let session = self.client.open_session(&self.token);
        let mut request = SlackApiConversationsHistoryRequest::new()
            .with_channel(channel.clone())
            .with_limit(100);
        let mut messages = Vec::new();
        loop {
            let response = session
                .conversations_history(&request)
                .await
                .map_err(error)?;
            messages.extend(response.messages);
            let cursor = response
                .response_metadata
                .and_then(|meta| meta.next_cursor)
                .filter(|cursor| !cursor.to_string().is_empty());
            if cursor.is_none() {
                return Ok(messages);
            }
            request = request.opt_cursor(cursor);
        }
    }

    async fn load(&self, topology: &mut Topology) -> Result<(), ChatError> {
        let channels = self.channels().await?;
        let mut threads = HashMap::new();
        for channel in &channels {
            if self.ignore_test_runs
                && channel
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("testrun-"))
            {
                continue;
            }
            let Some(workspace) = workspace_id(channel) else {
                continue;
            };
            for message in self.history(&channel.id).await? {
                if self.ignore_test_runs
                    && message
                        .content
                        .text
                        .as_deref()
                        .is_some_and(|text| text.starts_with("testrun-"))
                {
                    continue;
                }
                if let Some(tab) = tab_id(&message, workspace, &self.bot)
                    && threads
                        .insert(
                            tab,
                            SlackRoute {
                                channel: channel.id.clone(),
                                thread: Some(message.origin.ts),
                            },
                        )
                        .is_some()
                {
                    return Err(error("duplicate Slack tab parent messages"));
                }
            }
        }
        topology.channels = channels;
        topology.threads = threads;
        topology.loaded = true;
        Ok(())
    }

    pub async fn sync(&self, route: &TopologyRoute, fresh: bool) -> Result<SlackRoute, ChatError> {
        let mut topology = self.topology.lock().await;
        if fresh || !topology.loaded {
            self.load(&mut topology).await?;
        }
        if let Some(found) = topology.threads.get(&route.tab_id) {
            return Ok(found.clone());
        }
        let workspaces = tokio::task::spawn_blocking(workspace_list_result)
            .await
            .map_err(error)?
            .map_err(error)?;
        let workspace = workspaces
            .iter()
            .find(|workspace| workspace.workspace_id == route.workspace_id)
            .ok_or_else(|| error("Herdr workspace disappeared"))?;
        if self.ignore_test_runs
            && (workspace.label.starts_with("testrun-")
                || route.thread_name.starts_with("testrun-"))
        {
            return Err(error("test run excluded from service"));
        }
        let matching: Vec<_> = topology
            .channels
            .iter()
            .filter(|channel| workspace_id(channel) == Some(route.workspace_id.as_str()))
            .collect();
        let channel = match matching.as_slice() {
            [] => {
                let session = self.client.open_session(&self.token);
                let channel = session
                    .conversations_create(
                        &SlackApiConversationsCreateRequest::new(channel_name(&workspace.label)?)
                            .with_is_private(false),
                    )
                    .await
                    .map_err(error)?
                    .channel;
                topology.channels.push(channel.clone());
                session
                    .conversations_set_topic(&SlackApiConversationsSetTopicRequest::new(
                        channel.id.clone(),
                        format!("herdr workspace [{}]", route.workspace_id),
                    ))
                    .await
                    .map_err(error)?;
                let info = session
                    .conversations_info(&SlackApiConversationsInfoRequest::new(channel.id.clone()))
                    .await
                    .map_err(error)?
                    .channel;
                topology.channels.retain(|entry| entry.id != channel.id);
                topology.channels.push(info);
                session
                    .conversations_invite(&SlackApiConversationsInviteRequest::new(
                        channel.id.clone(),
                        vec![self.owner.clone()],
                    ))
                    .await
                    .map_err(error)?;
                channel.id
            }
            [channel] => channel.id.clone(),
            _ => return Err(error("duplicate Slack workspace channels")),
        };
        let response = self
            .client
            .open_session(&self.token)
            .chat_post_message(
                &SlackApiChatPostMessageRequest::new(
                    channel.clone(),
                    SlackMessageContent::new().with_text(delivery::escape(&route.thread_name)),
                )
                .with_parse("none".into()),
            )
            .await
            .map_err(error)?;
        let found = SlackRoute {
            channel,
            thread: Some(response.ts),
        };
        topology.threads.insert(route.tab_id.clone(), found.clone());
        drop(topology);
        Ok(found)
    }

    async fn archive(
        &self,
        topology: &mut Topology,
        channel: &SlackChannelInfo,
    ) -> Result<(), ChatError> {
        if self.ignore_test_runs
            && channel
                .name
                .as_deref()
                .is_some_and(|name| name.starts_with("testrun-"))
        {
            return Ok(());
        }
        let routes: Vec<_> = topology
            .threads
            .values()
            .filter(|route| route.channel == channel.id)
            .cloned()
            .collect();
        for route in routes {
            self.track_working(&route, false).await?;
            self.working.lock().await.remove(&route);
        }
        let session = self.client.open_session(&self.token);
        let name = format!(
            "{}-archived-{}",
            channel
                .name
                .as_deref()
                .ok_or_else(|| error("Slack channel has no name"))?
                .chars()
                .take(40)
                .collect::<String>(),
            channel.id.to_string().to_lowercase()
        );
        session
            .conversations_rename(&SlackApiConversationsRenameRequest::new(
                channel.id.clone(),
                name,
            ))
            .await
            .map_err(error)?;
        session
            .conversations_archive(&SlackApiConversationsArchiveRequest::new(
                channel.id.clone(),
            ))
            .await
            .map_err(error)?;
        topology.channels.retain(|entry| entry.id != channel.id);
        topology
            .threads
            .retain(|_, route| route.channel != channel.id);
        Ok(())
    }

    pub async fn close(&self, closure: &TopologyClosure) -> Result<(), ChatError> {
        let mut topology = self.topology.lock().await;
        self.load(&mut topology).await?;
        match closure {
            TopologyClosure::Workspace {
                workspace_id: workspace,
            } => {
                let channels: Vec<_> = topology
                    .channels
                    .iter()
                    .filter(|channel| workspace_id(channel) == Some(workspace))
                    .cloned()
                    .collect();
                for channel in channels {
                    self.archive(&mut topology, &channel).await?;
                }
            }
            TopologyClosure::Tab {
                workspace_id: _,
                tab_id,
            } => {
                if let Some(route) = topology.threads.get(tab_id).cloned() {
                    self.track_working(&route, false).await?;
                    self.working.lock().await.remove(&route);
                    self.client
                        .open_session(&self.token)
                        .chat_delete(&SlackApiChatDeleteRequest::new(
                            route.channel,
                            route
                                .thread
                                .ok_or_else(|| error("tab route has no parent message"))?,
                        ))
                        .await
                        .map_err(error)?;
                    topology.threads.remove(tab_id);
                }
            }
        }
        drop(topology);
        Ok(())
    }

    pub async fn sweep(
        &self,
        routes: Vec<Result<TopologyRoute, String>>,
        live: (HashSet<String>, HashSet<String>),
    ) -> Result<(), ChatError> {
        for route in routes {
            self.sync(&route.map_err(error)?, false).await?;
        }
        let mut topology = self.topology.lock().await;
        self.load(&mut topology).await?;
        let channels = topology.channels.clone();
        for channel in channels {
            if workspace_id(&channel).is_some_and(|id| !live.0.contains(id)) {
                self.archive(&mut topology, &channel).await?;
            }
        }
        let removed: Vec<_> = topology
            .threads
            .keys()
            .filter(|tab| !live.1.contains(*tab))
            .cloned()
            .collect();
        for tab in removed {
            if let Some(route) = topology.threads.get(&tab) {
                self.client
                    .open_session(&self.token)
                    .chat_delete(&SlackApiChatDeleteRequest::new(
                        route.channel.clone(),
                        route
                            .thread
                            .clone()
                            .ok_or_else(|| error("tab route has no parent message"))?,
                    ))
                    .await
                    .map_err(error)?;
            }
            topology.threads.remove(&tab);
        }
        drop(topology);
        Ok(())
    }
}
