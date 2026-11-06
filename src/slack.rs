use crate::{ChatError, TopologyClosure, TopologyRoute, tab_list_result, workspace_list_result};
use slack_morphism::prelude::*;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::Mutex;
mod cards;
mod chat;
mod delivery;
mod socket;
pub use socket::{SlackArchive, SlackSocketEvent};
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct SlackRoute {
    pub channel: SlackChannelId,
}
#[derive(Default)]
struct Topology {
    channels: Vec<SlackChannelInfo>,
    tabs: HashMap<String, SlackRoute>,
    loaded: bool,
}
type RenameTasks =
    HashMap<SlackChannelId, (String, tokio::task::JoinHandle<Result<(), ChatError>>)>;
pub struct Slack {
    client: Arc<SlackHyperClient>,
    token: SlackApiToken,
    owner: SlackUserId,
    bot: SlackUserId,
    team: String,
    topology: Arc<Mutex<Topology>>,
    ignore_test_runs: bool,
    listener: Mutex<Option<SlackClientSocketModeListener<SlackClientHyperHttpsConnector>>>,
    events_tx: tokio::sync::mpsc::UnboundedSender<SlackSocketEvent>,
    events: Mutex<tokio::sync::mpsc::UnboundedReceiver<SlackSocketEvent>>,
    working: Mutex<HashMap<SlackRoute, Option<tokio::task::JoinHandle<()>>>>,
    status_messages: Mutex<HashMap<SlackChannelId, SlackTs>>,
    renames: Mutex<RenameTasks>,
    question_text: Mutex<HashMap<SlackTs, String>>,
}
fn error(error: impl Display) -> ChatError {
    ChatError::from(error.to_string())
}
fn identity(channel: &SlackChannelInfo) -> Option<(&str, &str)> {
    let topic = channel
        .topic
        .as_ref()?
        .value
        .strip_prefix("herdr workspace [")?;
    let (workspace, tab) = topic.split_once("] tab [")?;
    let tab = tab.strip_suffix(']')?;
    let (parent, short) = tab.split_once(':')?;
    (parent == workspace && short.starts_with('t')).then_some((workspace, tab))
}
fn normalize(label: &str) -> String {
    label
        .to_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '_' | '-') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_owned()
}
pub fn slack_channel_name(
    workspace: &str,
    workspace_id: &str,
    tab: &str,
    tab_id: &str,
) -> Result<String, ChatError> {
    let (parent, short) = tab_id
        .split_once(':')
        .ok_or_else(|| error("tab id has no workspace prefix"))?;
    if parent != workspace_id {
        return Err(error("tab and workspace ids disagree"));
    }
    let (workspace, tab) = (normalize(workspace), normalize(tab));
    if workspace.is_empty() || tab.is_empty() {
        return Err(error("a label has no Slack channel-name characters"));
    }
    let (parent, short) = (normalize(parent), normalize(short));
    let room = 80_usize
        .checked_sub(parent.len() + short.len() + 3)
        .filter(|room| *room >= 2)
        .ok_or_else(|| error("Herdr ids cannot fit in a Slack channel name"))?;
    let first = workspace.len().min(room.div_ceil(2));
    let second = tab.len().min(room - first);
    let first = workspace.len().min(room - second);
    Ok(format!(
        "{}-{parent}-{}-{short}",
        &workspace[..first],
        &tab[..second]
    ))
}
fn named_tab(workspace: &str, tab: &str) -> Result<(String, bool), ChatError> {
    let workspace = workspace_list_result()
        .map_err(error)?
        .into_iter()
        .find(|entry| entry.workspace_id == workspace)
        .ok_or_else(|| error("Herdr workspace disappeared"))?;
    let tab = tab_list_result()
        .map_err(error)?
        .into_iter()
        .find(|entry| entry.tab_id == tab && entry.workspace_id == workspace.workspace_id)
        .ok_or_else(|| error("Herdr tab disappeared"))?;
    Ok((
        slack_channel_name(
            &workspace.label,
            &workspace.workspace_id,
            &tab.label,
            &tab.tab_id,
        )?,
        workspace.label.starts_with("testrun-") || tab.label.starts_with("testrun-"),
    ))
}
impl Slack {
    pub async fn new(token: String, owner: String) -> Result<Self, ChatError> {
        let connector = SlackClientHyperConnector::new()
            .map_err(error)?
            .with_rate_control(SlackApiRateControlConfig::new().with_max_retries(5));
        let client = Arc::new(SlackClient::new(connector));
        let token = SlackApiToken::new(token.into());
        let auth = client
            .open_session(&token)
            .auth_test()
            .await
            .map_err(error)?;
        let (events_tx, events) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            client,
            token,
            owner: owner.into(),
            bot: auth.user_id,
            team: auth.team_id.to_string(),
            topology: Arc::new(Mutex::new(Topology::default())),
            ignore_test_runs: false,
            listener: Mutex::new(None),
            events_tx,
            events: Mutex::new(events),
            working: Mutex::new(HashMap::new()),
            status_messages: Mutex::new(HashMap::new()),
            renames: Mutex::new(HashMap::new()),
            question_text: Mutex::new(HashMap::new()),
        })
    }
    pub async fn from_env() -> Result<Self, ChatError> {
        Self::new(
            std::env::var("SLACK_BOT_TOKEN").map_err(error)?,
            std::env::var("SLACK_OWNER_ID").map_err(error)?,
        )
        .await
    }
    #[must_use]
    pub fn team_id(&self) -> &str {
        &self.team
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
        let mut request = SlackApiConversationsHistoryRequest::new()
            .with_channel(channel.clone())
            .with_limit(100);
        let mut messages = Vec::new();
        loop {
            let response = self
                .client
                .open_session(&self.token)
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
        let mut tabs = HashMap::new();
        for channel in &channels {
            if self.ignore_test_runs
                && channel
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("testrun-"))
            {
                continue;
            }
            if let Some((_, tab)) = identity(channel)
                && tabs
                    .insert(
                        tab.to_owned(),
                        SlackRoute {
                            channel: channel.id.clone(),
                        },
                    )
                    .is_some()
            {
                return Err(error("duplicate Slack tab channels"));
            }
        }
        topology.channels = channels;
        topology.tabs = tabs;
        topology.loaded = true;
        Ok(())
    }
    pub async fn sync(&self, route: &TopologyRoute, fresh: bool) -> Result<SlackRoute, ChatError> {
        let mut topology = self.topology.lock().await;
        if fresh || !topology.loaded {
            self.load(&mut topology).await?;
        }
        if let Some(found) = topology.tabs.get(&route.tab_id) {
            return Ok(found.clone());
        }
        let (workspace, tab) = (route.workspace_id.clone(), route.tab_id.clone());
        let (name, test_run) = tokio::task::spawn_blocking(move || named_tab(&workspace, &tab))
            .await
            .map_err(error)??;
        if self.ignore_test_runs && test_run {
            return Err(error("test run excluded from service"));
        }
        let session = self.client.open_session(&self.token);
        let channel = session
            .conversations_create(
                &SlackApiConversationsCreateRequest::new(name).with_is_private(false),
            )
            .await
            .map_err(error)?
            .channel;
        topology.channels.push(channel.clone());
        session
            .conversations_set_topic(&SlackApiConversationsSetTopicRequest::new(
                channel.id.clone(),
                format!(
                    "herdr workspace [{}] tab [{}]",
                    route.workspace_id, route.tab_id
                ),
            ))
            .await
            .map_err(error)?;
        session
            .conversations_invite(&SlackApiConversationsInviteRequest::new(
                channel.id.clone(),
                vec![self.owner.clone()],
            ))
            .await
            .map_err(error)?;
        let channel = session
            .conversations_info(&SlackApiConversationsInfoRequest::new(channel.id))
            .await
            .map_err(error)?
            .channel;
        let found = SlackRoute {
            channel: channel.id.clone(),
        };
        topology.channels.retain(|entry| entry.id != channel.id);
        topology.channels.push(channel);
        topology.tabs.insert(route.tab_id.clone(), found.clone());
        drop(topology);
        Ok(found)
    }
    async fn queue_rename(&self, channel: SlackChannelInfo, name: String) {
        if channel.name.as_ref() == Some(&name) {
            return;
        }
        let mut jobs = self.renames.lock().await;
        if jobs
            .get(&channel.id)
            .is_some_and(|(desired, job)| desired == &name && !job.is_finished())
        {
            return;
        }
        if let Some((_, job)) = jobs.remove(&channel.id) {
            job.abort();
        }
        let (client, token, cache, events) = (
            self.client.clone(),
            self.token.clone(),
            self.topology.clone(),
            self.events_tx.clone(),
        );
        let desired = name.clone();
        jobs.insert(
            channel.id.clone(),
            (
                desired,
                tokio::spawn(async move {
                    let result = client
                        .open_session(&token)
                        .conversations_rename(&SlackApiConversationsRenameRequest::new(
                            channel.id.clone(),
                            name,
                        ))
                        .await
                        .map_err(error);
                    match result {
                        Ok(response) => {
                            if let Some(entry) = cache
                                .lock()
                                .await
                                .channels
                                .iter_mut()
                                .find(|entry| entry.id == channel.id)
                            {
                                entry.name = response.channel.name;
                            }
                            Ok(())
                        }
                        Err(failure) => {
                            let _ = events.send(SlackSocketEvent::Failure(failure.clone()));
                            Err(failure)
                        }
                    }
                }),
            ),
        );
    }
    pub async fn refresh_names(&self) -> Result<(), ChatError> {
        let (workspaces, tabs) = tokio::task::spawn_blocking(|| {
            Ok::<_, String>((workspace_list_result()?, tab_list_result()?))
        })
        .await
        .map_err(error)?
        .map_err(error)?;
        let channels = self.topology.lock().await.channels.clone();
        for channel in channels {
            let Some((workspace, tab)) = identity(&channel) else {
                continue;
            };
            let Some(workspace) = workspaces
                .iter()
                .find(|entry| entry.workspace_id == workspace)
            else {
                continue;
            };
            let Some(tab) = tabs
                .iter()
                .find(|entry| entry.tab_id == tab && entry.workspace_id == workspace.workspace_id)
            else {
                continue;
            };
            if self.ignore_test_runs
                && (workspace.label.starts_with("testrun-")
                    || tab.label.starts_with("testrun-")
                    || channel
                        .name
                        .as_deref()
                        .is_some_and(|name| name.starts_with("testrun-")))
            {
                continue;
            }
            let name = slack_channel_name(
                &workspace.label,
                &workspace.workspace_id,
                &tab.label,
                &tab.tab_id,
            )?;
            self.queue_rename(channel, name).await;
        }
        Ok(())
    }
    pub async fn wait_for_renames(&self) -> Result<(), ChatError> {
        let jobs: Vec<_> = self
            .renames
            .lock()
            .await
            .drain()
            .map(|(_, (_, job))| job)
            .collect();
        for job in jobs {
            job.await.map_err(error)??;
        }
        Ok(())
    }
    async fn archive(&self, channel: SlackChannelInfo) -> Result<(), ChatError> {
        if self.ignore_test_runs
            && channel
                .name
                .as_deref()
                .is_some_and(|name| name.starts_with("testrun-"))
        {
            return Ok(());
        }
        let job = self.renames.lock().await.remove(&channel.id);
        if let Some((_, job)) = job {
            job.abort();
        }
        let route = SlackRoute {
            channel: channel.id.clone(),
        };
        if self.status_messages.lock().await.contains_key(&channel.id) {
            self.track_working(&route, false).await?;
        }
        self.working.lock().await.remove(&route);
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
        let session = self.client.open_session(&self.token);
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
        let mut topology = self.topology.lock().await;
        topology.channels.retain(|entry| entry.id != channel.id);
        topology.tabs.retain(|_, route| route.channel != channel.id);
        drop(topology);
        Ok(())
    }
    pub async fn close(&self, closure: &TopologyClosure) -> Result<(), ChatError> {
        let mut topology = self.topology.lock().await;
        self.load(&mut topology).await?;
        let channels: Vec<_> = topology
            .channels
            .iter()
            .filter(|channel| {
                identity(channel).is_some_and(|(workspace, tab)| match closure {
                    TopologyClosure::Tab {
                        workspace_id,
                        tab_id,
                    } => workspace == workspace_id && tab == tab_id,
                    TopologyClosure::Workspace { workspace_id } => workspace == workspace_id,
                })
            })
            .cloned()
            .collect();
        drop(topology);
        for channel in channels {
            self.archive(channel).await?;
        }
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
        drop(topology);
        for channel in channels {
            let orphan = identity(&channel).is_some_and(|(workspace, tab)| {
                !live.0.contains(workspace) || !live.1.contains(tab)
            });
            let legacy = channel.topic.as_ref().is_some_and(|topic| {
                topic.value.starts_with("herdr workspace [") && topic.value.ends_with(']')
            }) && identity(&channel).is_none();
            if orphan || legacy {
                self.archive(channel).await?;
            }
        }
        self.refresh_names().await
    }
}
