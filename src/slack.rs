use crate::{
    ChatError, ChatScope, HerdrTab, HerdrWorkspace, TopologyClosure, TopologyRoute,
    tab_list_result, workspace_list_result,
};
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
struct SlackState {
    client: Arc<SlackHyperClient>,
    token: SlackApiToken,
    topology: Arc<Mutex<Topology>>,
    events_tx: tokio::sync::mpsc::UnboundedSender<SlackSocketEvent>,
    working: Mutex<HashMap<SlackRoute, Option<tokio::task::JoinHandle<()>>>>,
    status_messages: Mutex<HashMap<SlackChannelId, SlackTs>>,
    loading: Arc<Mutex<HashMap<SlackChannelId, String>>>,
    renames: Mutex<RenameTasks>,
}
pub struct Slack {
    state: Arc<SlackState>,
    owner: SlackUserId,
    owner_identity: crate::OwnerIdentity,
    bot: SlackUserId,
    team: String,
    ignore_test_runs: bool,
    scope: ChatScope,
    listener: Mutex<Option<SlackClientSocketModeListener<SlackClientHyperHttpsConnector>>>,
    events: Mutex<tokio::sync::mpsc::UnboundedReceiver<SlackSocketEvent>>,
    question_text: Mutex<HashMap<SlackTs, String>>,
    started: SlackTs,
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
fn channel_scope(scope: ChatScope, ignore_test_runs: bool, channel: &SlackChannelInfo) -> bool {
    let test_run = channel
        .name
        .as_deref()
        .is_some_and(|name| name.starts_with("testrun-"));
    (scope != ChatScope::TestRuns || test_run) && (!ignore_test_runs || !test_run)
}
fn normalize(label: &str) -> String {
    label
        .to_lowercase()
        .chars()
        .map(|ch| {
            if ch.is_alphanumeric() || matches!(ch, '_' | '-') {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_owned()
}
fn slack_name_parts(
    workspace: &str,
    workspace_id: &str,
    tab: &str,
    tab_id: &str,
) -> Result<[String; 4], ChatError> {
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
        .checked_sub(parent.chars().count() + short.chars().count() + 3)
        .filter(|room| *room >= 2)
        .ok_or_else(|| error("Herdr ids cannot fit in a Slack channel name"))?;
    let first = workspace.chars().count().min(room.div_ceil(2));
    let second = tab.chars().count().min(room - first);
    let first = workspace.chars().count().min(room - second);
    Ok([
        workspace.chars().take(first).collect(),
        parent,
        tab.chars().take(second).collect(),
        short,
    ])
}
pub fn slack_channel_name(
    workspace: &str,
    workspace_id: &str,
    tab: &str,
    tab_id: &str,
) -> Result<String, ChatError> {
    let [workspace, parent, tab, short] = slack_name_parts(workspace, workspace_id, tab, tab_id)?;
    Ok(format!("{workspace}-{parent}-{tab}-{short}"))
}
fn renamed_tab_label(
    name: &str,
    workspace: &HerdrWorkspace,
    tab: &HerdrTab,
) -> Result<Option<String>, ChatError> {
    let [workspace_part, parent, tab_part, short] = slack_name_parts(
        &workspace.label,
        &workspace.workspace_id,
        &tab.label,
        &tab.tab_id,
    )?;
    let prefix = format!("{workspace_part}-{parent}-");
    let suffix = format!("-{short}");
    if name == format!("{prefix}{tab_part}{suffix}") {
        return Ok(None);
    }
    Ok(Some(
        name.strip_prefix(&prefix)
            .and_then(|rest| rest.strip_suffix(&suffix))
            .filter(|middle| !middle.is_empty())
            .unwrap_or(name)
            .to_owned(),
    ))
}
fn herdr_tab(workspace: &str, tab: &str) -> Result<(HerdrWorkspace, HerdrTab), ChatError> {
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
    Ok((workspace, tab))
}
fn named_tab(workspace: &str, tab: &str) -> Result<(String, bool), ChatError> {
    let (workspace, tab) = herdr_tab(workspace, tab)?;
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
        let started = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(error)?;
        let started = SlackTs::new(format!(
            "{}.{:06}",
            started.as_secs(),
            started.subsec_micros()
        ));
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
        let owner = SlackUserId::from(owner);
        let user = client
            .open_session(&token)
            .users_info(&SlackApiUsersInfoRequest::new(owner.clone()))
            .await
            .map_err(error)?
            .user;
        let profile = user
            .profile
            .ok_or_else(|| error("Slack owner has no profile"))?;
        let display_name = profile
            .display_name
            .filter(|name| !name.trim().is_empty())
            .or_else(|| profile.real_name.filter(|name| !name.trim().is_empty()))
            .ok_or_else(|| error("Slack owner profile has no display name"))?;
        let avatar = profile
            .icon
            .and_then(|icon| icon.images)
            .and_then(|images| {
                images
                    .resolutions
                    .into_iter()
                    .max_by_key(|(size, _)| *size)
                    .map(|(_, url)| url)
            })
            .ok_or_else(|| error("Slack owner profile has no avatar"))?;
        let owner_identity = crate::OwnerIdentity {
            display_name,
            avatar_url: Some(avatar),
        };
        let (events_tx, events) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            state: Arc::new(SlackState {
                client,
                token,
                topology: Arc::new(Mutex::new(Topology::default())),
                events_tx,
                working: Mutex::new(HashMap::new()),
                status_messages: Mutex::new(HashMap::new()),
                loading: Arc::new(Mutex::new(HashMap::new())),
                renames: Mutex::new(HashMap::new()),
            }),
            owner,
            owner_identity,
            bot: auth.user_id,
            team: auth.team_id.to_string(),
            ignore_test_runs: false,
            scope: ChatScope::TestRuns,
            listener: Mutex::new(None),
            events: Mutex::new(events),
            question_text: Mutex::new(HashMap::new()),
            started,
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
    pub const fn owner_identity(&self) -> &crate::OwnerIdentity {
        &self.owner_identity
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
    #[must_use]
    pub const fn with_scope(mut self, scope: ChatScope) -> Self {
        self.scope = scope;
        self
    }
    pub(super) async fn require_route(&self, route: &SlackRoute) -> Result<(), ChatError> {
        if self.scope == ChatScope::TestRuns && !self.accepts_channel(&route.channel).await? {
            return Err(error("Slack route excluded from test scope"));
        }
        Ok(())
    }
    pub async fn channels(&self) -> Result<Vec<SlackChannelInfo>, ChatError> {
        self.state.channels().await
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
                .state
                .client
                .open_session(&self.state.token)
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
    pub async fn delete_stale_activity(&self, route: &SlackRoute) -> Result<(), ChatError> {
        self.require_route(route).await?;
        let session = self.state.client.open_session(&self.state.token);
        let messages = session
            .conversations_history(
                &SlackApiConversationsHistoryRequest::new()
                    .with_channel(route.channel.clone())
                    .with_latest(self.started.clone())
                    .with_limit(100),
            )
            .await
            .map_err(error)?
            .messages;
        for message in messages {
            if message.sender.user.as_ref() == Some(&self.bot)
                && message
                    .content
                    .text
                    .as_deref()
                    .is_some_and(crate::is_activity_message_text)
            {
                session
                    .chat_delete(&SlackApiChatDeleteRequest::new(
                        route.channel.clone(),
                        message.origin.ts,
                    ))
                    .await
                    .map_err(error)?;
            }
        }
        Ok(())
    }
    async fn load(&self, topology: &mut Topology) -> Result<(), ChatError> {
        let channels = self
            .channels()
            .await?
            .into_iter()
            .filter(|channel| channel_scope(self.scope, self.ignore_test_runs, channel))
            .collect::<Vec<_>>();
        let mut tabs = HashMap::new();
        for channel in &channels {
            if let Some((workspace, tab)) = identity(channel) {
                if self.scope == ChatScope::TestRuns && self.excluded(workspace, tab).await? {
                    continue;
                }
                if tabs
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
        }
        topology.channels = channels;
        topology.tabs = tabs;
        topology.loaded = true;
        Ok(())
    }
    pub async fn sync(&self, route: &TopologyRoute, fresh: bool) -> Result<SlackRoute, ChatError> {
        if self.scope == ChatScope::TestRuns
            && self.excluded(&route.workspace_id, &route.tab_id).await?
        {
            return Err(error("Herdr route excluded from Slack scope"));
        }
        let mut topology = self.state.topology.lock().await;
        if fresh || !topology.loaded {
            self.load(&mut topology).await?;
        }
        if let Some(found) = topology.tabs.get(&route.tab_id).cloned() {
            self.require_route(&found).await?;
            return Ok(found);
        }
        let (workspace, tab) = (route.workspace_id.clone(), route.tab_id.clone());
        let (name, test_run) = tokio::task::spawn_blocking(move || named_tab(&workspace, &tab))
            .await
            .map_err(error)??;
        if (self.ignore_test_runs && test_run)
            || (self.scope == ChatScope::TestRuns && !name.starts_with("testrun-"))
        {
            return Err(error("Slack channel excluded from scope"));
        }
        let session = self.state.client.open_session(&self.state.token);
        let topic = format!(
            "herdr workspace [{}] tab [{}]",
            route.workspace_id, route.tab_id
        );
        let channel = session
            .conversations_create(
                &SlackApiConversationsCreateRequest::new(name).with_is_private(false),
            )
            .await
            .map_err(error)?
            .channel;
        if channel
            .topic
            .as_ref()
            .is_none_or(|existing| existing.value != topic)
        {
            session
                .conversations_set_topic(&SlackApiConversationsSetTopicRequest::new(
                    channel.id.clone(),
                    topic,
                ))
                .await
                .map_err(error)?;
        }
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
        if channel.name.as_ref() == Some(&name)
            || !channel_scope(self.scope, self.ignore_test_runs, &channel)
            || (self.scope == ChatScope::TestRuns && !name.starts_with("testrun-"))
        {
            return;
        }
        let mut jobs = self.state.renames.lock().await;
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
            self.state.client.clone(),
            self.state.token.clone(),
            self.state.topology.clone(),
            self.state.events_tx.clone(),
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
        let channels = self.state.topology.lock().await.channels.clone();
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
            let test_run =
                workspace.label.starts_with("testrun-") || tab.label.starts_with("testrun-");
            if !channel_scope(self.scope, self.ignore_test_runs, &channel)
                || (self.scope == ChatScope::TestRuns && !test_run)
                || (self.ignore_test_runs && test_run)
            {
                continue;
            }
            let name = match slack_channel_name(
                &workspace.label,
                &workspace.workspace_id,
                &tab.label,
                &tab.tab_id,
            ) {
                Ok(name) => name,
                Err(failure) => {
                    crate::bridge_eprintln!(
                        "Slack name reconciliation error for {}: {failure}",
                        channel.id
                    );
                    continue;
                }
            };
            self.queue_rename(channel, name).await;
        }
        Ok(())
    }
    async fn archive(&self, channel: SlackChannelInfo) -> Result<(), ChatError> {
        self.state
            .archive(channel, self.scope, self.ignore_test_runs)
            .await
    }
    pub async fn close(&self, closure: &TopologyClosure) -> Result<(), ChatError> {
        self.state
            .close(closure, self.scope, self.ignore_test_runs)
            .await
    }
    pub async fn sweep<F>(
        &self,
        routes: Vec<Result<TopologyRoute, String>>,
        live: F,
    ) -> Result<(), ChatError>
    where
        F: FnOnce() -> Result<(HashSet<String>, HashSet<String>), String> + Send,
    {
        let mut failure = None;
        for route in routes {
            let result = match route {
                Ok(route) => match self.sync(&route, false).await {
                    Ok(channel) => self.delete_stale_activity(&channel).await,
                    Err(failure) => Err(failure),
                },
                Err(failure) => Err(error(failure)),
            };
            if let Err(error) = result {
                report_topology_failure(error, &mut failure);
            }
        }
        let orphans = async {
            let mut topology = self.state.topology.lock().await;
            let live = live().map_err(error)?;
            self.load(&mut topology).await?;
            let channels = topology
                .channels
                .iter()
                .filter(|channel| {
                    let orphan = identity(channel).is_some_and(|(workspace, tab)| {
                        !live.0.contains(workspace) || !live.1.contains(tab)
                    });
                    let legacy = channel.topic.as_ref().is_some_and(|topic| {
                        topic.value.starts_with("herdr workspace [") && topic.value.ends_with(']')
                    }) && identity(channel).is_none();
                    orphan || legacy
                })
                .cloned()
                .collect::<Vec<_>>();
            drop(topology);
            Ok::<_, ChatError>(channels)
        }
        .await;
        match orphans {
            Ok(channels) => {
                for channel in channels {
                    if let Err(error) = self.archive(channel).await {
                        report_topology_failure(error, &mut failure);
                    }
                }
            }
            Err(error) => report_topology_failure(error, &mut failure),
        }
        if let Err(error) = self.refresh_names().await {
            report_topology_failure(error, &mut failure);
        }
        failure.map_or(Ok(()), Err)
    }
}

impl SlackState {
    async fn channels(&self) -> Result<Vec<SlackChannelInfo>, ChatError> {
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
    async fn archive(
        &self,
        channel: SlackChannelInfo,
        scope: ChatScope,
        ignore_test_runs: bool,
    ) -> Result<(), ChatError> {
        if !channel_scope(scope, ignore_test_runs, &channel) {
            return Ok(());
        }
        let job = self.renames.lock().await.remove(&channel.id);
        if let Some((_, job)) = job {
            job.abort();
        }
        let route = SlackRoute {
            channel: channel.id.clone(),
        };
        if let Err(failure) = self.track_working(&route, false).await {
            crate::bridge_eprintln!("Slack status clear error: {failure}");
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
    async fn close(
        &self,
        closure: &TopologyClosure,
        scope: ChatScope,
        ignore_test_runs: bool,
    ) -> Result<(), ChatError> {
        for channel in self.channels().await? {
            if identity(&channel).is_some_and(|(workspace, tab)| match closure {
                TopologyClosure::Tab {
                    workspace_id,
                    tab_id,
                } => workspace == workspace_id && tab == tab_id,
                TopologyClosure::Workspace { workspace_id } => workspace == workspace_id,
            }) {
                self.archive(channel, scope, ignore_test_runs).await?;
            }
        }
        Ok(())
    }
}

fn report_topology_failure(failure: ChatError, last: &mut Option<ChatError>) {
    crate::bridge_eprintln!("Slack startup topology error: {failure}");
    *last = Some(failure);
}

#[cfg(test)]
mod tests {
    use super::renamed_tab_label;
    use crate::{HerdrTab, HerdrWorkspace};

    #[test]
    fn owner_channel_names_become_tab_labels_and_bridge_names_are_ignored() {
        let workspace = HerdrWorkspace {
            workspace_id: "w1F7".to_owned(),
            label: "testrun-slack-1794106534313".to_owned(),
        };
        let tab = HerdrTab {
            tab_id: "w1F7:t1".to_owned(),
            workspace_id: "w1F7".to_owned(),
            label: "testrun-build".to_owned(),
        };
        let cases = [
            (
                "a name that keeps the bridge prefix and suffix",
                "testrun-slack-1794106534313-w1f7-testrun-deploy-t1",
                Some("testrun-deploy"),
            ),
            (
                "a name without the bridge prefix and suffix",
                "testrun-slack-1794106534313-whole",
                Some("testrun-slack-1794106534313-whole"),
            ),
            (
                "the name the bridge sets",
                "testrun-slack-1794106534313-w1f7-testrun-build-t1",
                None,
            ),
        ];
        for (name, channel, expected) in cases {
            assert_eq!(
                renamed_tab_label(channel, &workspace, &tab).ok(),
                Some(expected.map(str::to_owned)),
                "{name}"
            );
        }
    }
}
