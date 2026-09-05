use std::collections::HashSet;
use std::sync::Arc;

use tokio::sync::Mutex;
use twilight_model::channel::Channel;
use twilight_model::id::Id;
use twilight_model::id::marker::{ChannelMarker, GuildMarker};

use crate::{AgentSnapshot, HerdrTab, ThreadNameError, format_thread_name};

/// Shared, per-process cache of one guild's channel list and active-thread list, reused across
/// tabs so a startup sweep does not refetch both lists for every tab.
pub type TopologyCache = Arc<Mutex<Option<(Vec<Channel>, Vec<Channel>)>>>;

/// The Discord topology and sole pane that owns it for one agent transition.
#[derive(Debug, PartialEq, Eq)]
pub struct TopologyRoute {
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
    pub channel_name: String,
    pub thread_name: String,
}

/// Why [`route_topology`] could not resolve a topology route.
#[derive(Debug)]
pub enum RouteError {
    /// The named tab's numeric label has no terminal title yet. Not a failure: the caller
    /// remembers the tab id and waits for a later snapshot to report one.
    TitlePending { tab_id: String },
    /// The tab's Discord thread name can never be produced from its current identity.
    Unusable { tab_id: String, message: String },
    /// Missing or ambiguous Herdr identity, missing workspace evidence, or a channel naming
    /// failure.
    Other(String),
}

impl std::fmt::Display for RouteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TitlePending { tab_id } => {
                write!(formatter, "herdr tab {tab_id} terminal title pending")
            }
            Self::Unusable { message, .. } | Self::Other(message) => write!(formatter, "{message}"),
        }
    }
}

impl From<RouteError> for String {
    fn from(error: RouteError) -> Self {
        error.to_string()
    }
}

/// Resolves one agent transition to an unambiguous workspace, tab, and pane.
///
/// # Errors
///
/// Returns [`RouteError::Other`] for missing or ambiguous Herdr identity, missing workspace
/// evidence, or a channel naming failure; [`RouteError::TitlePending`] when a numeric tab label
/// has no terminal title yet; and [`RouteError::Unusable`] when the tab can never produce a
/// Discord thread name.
pub fn route_topology(
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    terminal_id: &str,
) -> Result<TopologyRoute, RouteError> {
    let matching_agents: Vec<&AgentSnapshot> = agents
        .iter()
        .filter(|agent| agent.terminal_id == terminal_id)
        .collect();
    let [agent] = matching_agents.as_slice() else {
        return Err(RouteError::Other(format!(
            "herdr topology has duplicate or missing terminal {terminal_id}"
        )));
    };
    let tab_id = usable_agent_identity(agent.tab_id.as_deref(), "tab", terminal_id)
        .map_err(RouteError::Other)?;
    let workspace_id =
        usable_agent_identity(agent.workspace_id.as_deref(), "workspace", terminal_id)
            .map_err(RouteError::Other)?;
    let pane_id = usable_agent_identity(agent.pane_id.as_deref(), "pane", terminal_id)
        .map_err(RouteError::Other)?;
    let matching_tabs: Vec<&HerdrTab> = tabs.iter().filter(|tab| tab.tab_id == tab_id).collect();
    let [tab] = matching_tabs.as_slice() else {
        return Err(RouteError::Other(if matching_tabs.is_empty() {
            format!("herdr topology error: tab {tab_id} disappeared")
        } else {
            format!("herdr topology error: duplicate tab identifier {tab_id}")
        }));
    };
    if tab.workspace_id != workspace_id {
        return Err(RouteError::Other(format!(
            "herdr topology error: tab {tab_id} workspace mismatch"
        )));
    }
    let workspace_tabs: HashSet<&str> = tabs
        .iter()
        .filter(|candidate| candidate.workspace_id == workspace_id)
        .map(|candidate| candidate.tab_id.as_str())
        .collect();
    let cwds: Vec<String> = agents
        .iter()
        .filter(|candidate| candidate.workspace_id.as_deref() == Some(workspace_id))
        .filter(|candidate| {
            candidate
                .tab_id
                .as_deref()
                .is_some_and(|candidate_tab| workspace_tabs.contains(candidate_tab))
        })
        .filter_map(|candidate| candidate.cwd.clone())
        .collect();
    let channel_name = workspace_channel_name(workspace_id, &cwds).map_err(RouteError::Other)?;
    let thread_name = match format_thread_name(
        &tab.label,
        agent.terminal_title_stripped.as_deref().unwrap_or_default(),
        tab_id,
    ) {
        Ok(name) => name,
        Err(ThreadNameError::TitlePending) => {
            return Err(RouteError::TitlePending {
                tab_id: tab_id.to_owned(),
            });
        }
        Err(ThreadNameError::Unusable(message)) => {
            return Err(RouteError::Unusable {
                tab_id: tab_id.to_owned(),
                message,
            });
        }
    };
    Ok(TopologyRoute {
        workspace_id: workspace_id.to_owned(),
        tab_id: tab_id.to_owned(),
        pane_id: pane_id.to_owned(),
        channel_name,
        thread_name,
    })
}

fn usable_agent_identity<'a>(
    value: Option<&'a str>,
    identity: &str,
    terminal_id: &str,
) -> Result<&'a str, String> {
    value
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("herdr topology error: agent {terminal_id} has no {identity} id"))
}

/// Derives the Discord channel name from the most common usable agent cwd.
///
/// # Errors
///
/// Returns an error when the workspace has no usable cwd evidence or channel name.
pub fn workspace_channel_name(workspace_id: &str, cwds: &[String]) -> Result<String, String> {
    let mut counts = std::collections::HashMap::new();
    let mut common = None;
    let mut common_count = 0;
    for cwd in cwds {
        let cwd = cwd.trim();
        if !cwd.is_empty() {
            let count = counts.entry(cwd).or_insert(0usize);
            *count += 1;
            if *count > common_count
                || (*count == common_count && common.is_none_or(|current| cwd < current))
            {
                common = Some(cwd);
                common_count = *count;
            }
        }
    }
    let common = common
        .ok_or_else(|| format!("herdr workspace {workspace_id} has no agent cwd evidence"))?;
    let folder = common
        .rsplit('/')
        .find(|part| !part.is_empty())
        .unwrap_or("");
    let slug = |value: &str| {
        let mut output = String::new();
        for c in value.to_ascii_lowercase().chars() {
            if c.is_ascii_alphanumeric() || c == '-' {
                output.push(c);
            } else if !output.ends_with('-') {
                output.push('-');
            }
        }
        output.trim_matches('-').to_owned()
    };
    let folder = slug(folder);
    let workspace = slug(workspace_id);
    let suffix = format!("-{workspace}");
    if folder.is_empty() || workspace.is_empty() || suffix.len() >= 90 {
        return Err(format!(
            "herdr workspace {workspace_id} has no usable channel name"
        ));
    }
    Ok(format!(
        "{}{}",
        &folder[..folder.len().min(90 - suffix.len())],
        suffix
    ))
}

/// Fetches one guild's channel list and active-thread list.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn fetch_topology_lists(
    client: &twilight_http::Client,
    guild: Id<GuildMarker>,
) -> Result<(Vec<Channel>, Vec<Channel>), String> {
    let channels = client
        .guild_channels(guild)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?;
    let active_threads = client
        .active_threads(guild)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?
        .threads;
    Ok((channels, active_threads))
}

/// Installs a freshly fetched list pair into the cache and returns the stored lists.
///
/// A cached entry the fetch did not return is carried over only when it is newer than every
/// entry the fetch did return. Discord snowflake ids increase with creation time, so anything
/// older than that watermark would have come back in the fetch: its absence means it is gone or
/// archived, and only a newer entry can be a create this fetch raced.
pub fn reconcile_topology_cache(
    cached: &mut Option<(Vec<Channel>, Vec<Channel>)>,
    fetched: (Vec<Channel>, Vec<Channel>),
) -> (&mut Vec<Channel>, &mut Vec<Channel>) {
    let (mut channels, mut active_threads) = fetched;
    if let Some((cached_channels, cached_threads)) = cached.take() {
        carry_over_recent(&mut channels, cached_channels);
        carry_over_recent(&mut active_threads, cached_threads);
    }
    let (channels, active_threads) = cached.insert((channels, active_threads));
    (channels, active_threads)
}

fn carry_over_recent(fetched: &mut Vec<Channel>, cached: Vec<Channel>) {
    let known: HashSet<_> = fetched.iter().map(|entry| entry.id).collect();
    let watermark = fetched
        .iter()
        .map(|entry| entry.id.get())
        .max()
        .unwrap_or(0);
    fetched.extend(
        cached
            .into_iter()
            .filter(|entry| !known.contains(&entry.id) && entry.id.get() > watermark),
    );
}

/// Resolves the one thread that identifies a tab, ignoring repeated entries for the same id.
///
/// # Errors
///
/// Returns an error when more than one distinct thread claims the tab.
fn single_matching_thread(
    threads: &[Channel],
    workspace_channel: Id<ChannelMarker>,
    thread_suffix: &str,
    tab_id: &str,
) -> Result<Option<Channel>, String> {
    let mut seen = HashSet::new();
    let mut matching = threads
        .iter()
        .rev()
        .filter(|thread| seen.insert(thread.id))
        .filter(|thread| {
            thread.parent_id == Some(workspace_channel)
                && thread
                    .name
                    .as_deref()
                    .is_some_and(|name| name.ends_with(thread_suffix))
        });
    let resolved = matching.next();
    if matching.next().is_some() {
        return Err(format!(
            "Discord topology has duplicate threads for tab {tab_id}"
        ));
    }
    Ok(resolved.cloned())
}

/// Synchronizes the Discord workspace topology against caller-supplied channel and
/// active-thread lists, extending them in place when a channel or thread is created.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn sync_topology(
    client: &twilight_http::Client,
    guild: twilight_model::id::Id<twilight_model::id::marker::GuildMarker>,
    channels: &mut Vec<Channel>,
    active_threads: &mut Vec<Channel>,
    route: &TopologyRoute,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>, String> {
    let TopologyRoute {
        workspace_id,
        tab_id,
        channel_name,
        thread_name,
        ..
    } = route;
    if workspace_id.trim().is_empty()
        || channel_name.trim().is_empty()
        || thread_name.trim().is_empty()
        || tab_id.trim().is_empty()
    {
        return Err("herdr topology has unusable identity".to_owned());
    }
    let thread_suffix = format!(" [{tab_id}]");
    if !thread_name.ends_with(&thread_suffix) {
        return Err(format!(
            "herdr topology thread name does not identify tab {tab_id}"
        ));
    }
    let topic = format!("herdr workspace [{workspace_id}]");
    let matching_channels: Vec<_> = channels
        .iter()
        .filter(|channel| channel.topic.as_deref() == Some(topic.as_str()))
        .collect();
    if matching_channels.len() > 1 {
        return Err(format!(
            "Discord topology has duplicate channels for workspace {workspace_id}"
        ));
    }
    let matching_channel_id = matching_channels.first().map(|channel| channel.id);
    let workspace_channel = if let Some(id) = matching_channel_id {
        id
    } else {
        let created = client
            .create_guild_channel(guild, channel_name)
            .topic(&topic)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        let id = created.id;
        channels.push(created);
        id
    };
    let mut resolved =
        single_matching_thread(active_threads, workspace_channel, &thread_suffix, tab_id)?;
    if resolved.is_none() {
        resolved = single_matching_thread(
            &archived_threads(client, workspace_channel).await?,
            workspace_channel,
            &thread_suffix,
            tab_id,
        )?;
    }
    if let Some(thread) = resolved {
        if thread
            .thread_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.archived)
        {
            let unarchived = client
                .update_thread(thread.id)
                .archived(false)
                .await
                .map_err(|error| error.to_string())?
                .model()
                .await
                .map_err(|error| error.to_string())?;
            active_threads.push(unarchived);
        }
        return Ok(thread.id);
    }
    let created = client
        .create_thread(
            workspace_channel,
            thread_name,
            twilight_model::channel::ChannelType::PublicThread,
        )
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?;
    let id = created.id;
    active_threads.push(created);
    Ok(id)
}

/// Lists one channel's public archived threads, following every pagination page.
///
/// # Errors
///
/// Returns Discord request or response errors, and an error when Discord's pagination cursor
/// does not advance.
pub async fn archived_threads(
    client: &twilight_http::Client,
    workspace_channel: Id<ChannelMarker>,
) -> Result<Vec<Channel>, String> {
    let mut before: Option<String> = None;
    let mut threads = Vec::new();
    loop {
        let request = client.public_archived_threads(workspace_channel).limit(100);
        let response = if let Some(before) = before.as_deref() {
            request.before(&before.replace('+', "%2B")).await
        } else {
            request.await
        }
        .map_err(|error| error.to_string())?;
        let listing = response.model().await.map_err(|error| error.to_string())?;
        let has_more = listing.has_more.unwrap_or(false);
        if has_more && listing.threads.is_empty() {
            return Err("Discord returned an empty archived-thread page with has_more".to_owned());
        }
        let next_before = listing
            .threads
            .last()
            .and_then(|thread| thread.thread_metadata.as_ref())
            .map(|metadata| metadata.archive_timestamp.iso_8601().to_string());
        threads.extend(listing.threads);
        if !has_more {
            return Ok(threads);
        }
        if next_before.is_none() {
            return Err("Discord returned archived threads without a pagination cursor".to_owned());
        }
        if next_before == before {
            return Err(
                "Discord returned archived threads without an advancing pagination cursor"
                    .to_owned(),
            );
        }
        before = next_before;
    }
}

/// True when a Discord API error means the target channel or thread is already gone.
fn is_unknown_channel_error(error: &twilight_http::Error) -> bool {
    matches!(
        error.kind(),
        twilight_http::error::ErrorType::Response {
            status,
            error: twilight_http::api_error::ApiError::General(api_error),
            ..
        } if *status == twilight_http::response::StatusCode::NOT_FOUND && api_error.code == 10003
    )
}

/// Deletes one Discord channel or thread, treating an already-deleted target as done.
async fn delete_channel_if_present(
    client: &twilight_http::Client,
    id: Id<ChannelMarker>,
) -> Result<(), String> {
    match client.delete_channel(id).await {
        Ok(_) => Ok(()),
        Err(error) if is_unknown_channel_error(&error) => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

/// The id of the channel whose topic identifies `workspace_id`, if one is present.
fn workspace_channel_id(channels: &[Channel], workspace_id: &str) -> Option<Id<ChannelMarker>> {
    let topic = format!("herdr workspace [{workspace_id}]");
    channels
        .iter()
        .find(|channel| channel.topic.as_deref() == Some(topic.as_str()))
        .map(|channel| channel.id)
}

/// The workspace id named by a channel's `herdr workspace [id]` topic, if it has one.
fn workspace_topic_id(channel: &Channel) -> Option<&str> {
    channel
        .topic
        .as_deref()?
        .strip_prefix("herdr workspace [")?
        .strip_suffix(']')
}

/// The tab id named by a thread name's trailing ` [id]` suffix, if it has one.
fn thread_tab_suffix(name: &str) -> Option<&str> {
    let trimmed = name.strip_suffix(']')?;
    trimmed.rfind(" [").map(|start| &trimmed[start + 2..])
}

/// Deletes the Discord thread identifying one closed Herdr tab.
///
/// Searches active threads first and the workspace channel's archived threads on a miss. A
/// missing workspace channel or thread is not an error: the tab is already gone from Discord.
///
/// # Errors
///
/// Returns Discord request or response errors, or a duplicate-thread topology error.
pub async fn delete_tab_thread(
    client: &twilight_http::Client,
    channels: &[Channel],
    active_threads: &mut Vec<Channel>,
    workspace_id: &str,
    tab_id: &str,
) -> Result<(), String> {
    let Some(workspace_channel) = workspace_channel_id(channels, workspace_id) else {
        return Ok(());
    };
    let thread_suffix = format!(" [{tab_id}]");
    let mut resolved =
        single_matching_thread(active_threads, workspace_channel, &thread_suffix, tab_id)?;
    if resolved.is_none() {
        resolved = single_matching_thread(
            &archived_threads(client, workspace_channel).await?,
            workspace_channel,
            &thread_suffix,
            tab_id,
        )?;
    }
    let Some(thread) = resolved else {
        return Ok(());
    };
    delete_channel_if_present(client, thread.id).await?;
    active_threads.retain(|entry| entry.id != thread.id);
    Ok(())
}

/// Deletes the Discord channel representing one closed Herdr workspace, if one exists. Discord
/// removes the channel's threads with it. A missing channel is not an error.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn delete_workspace_channel(
    client: &twilight_http::Client,
    channels: &mut Vec<Channel>,
    workspace_id: &str,
) -> Result<(), String> {
    let Some(channel_id) = workspace_channel_id(channels, workspace_id) else {
        return Ok(());
    };
    delete_channel_if_present(client, channel_id).await?;
    channels.retain(|channel| channel.id != channel_id);
    Ok(())
}

/// Deletes every guild channel and tab thread that Herdr no longer lists.
///
/// A channel whose `herdr workspace [id]` topic names a workspace id absent from
/// `live_workspace_ids` is deleted, and under each surviving workspace channel, a thread whose
/// trailing ` [id]` suffix is that channel's own workspace id (Herdr tab ids are
/// `<workspace_id>:t<...>`) and is absent from `live_tab_ids` is deleted. A thread whose suffix
/// does not start with the channel's own workspace id is not bridge-owned and survives regardless
/// of `live_tab_ids`. An empty `live_workspace_ids` or `live_tab_ids` is authoritative: everything
/// bridge-owned and not named survives on nothing else, so it is deleted.
///
/// # Errors
///
/// Returns Discord request or response errors, or a duplicate-thread topology error.
pub async fn delete_topology_absent_from_herdr<S: std::hash::BuildHasher + Sync>(
    client: &twilight_http::Client,
    channels: &mut Vec<Channel>,
    active_threads: &mut Vec<Channel>,
    live_workspace_ids: &HashSet<&str, S>,
    live_tab_ids: &HashSet<&str, S>,
) -> Result<(), String> {
    let orphaned_workspace_ids: Vec<String> = channels
        .iter()
        .filter_map(workspace_topic_id)
        .filter(|workspace_id| !live_workspace_ids.contains(workspace_id))
        .map(ToOwned::to_owned)
        .collect();
    for workspace_id in orphaned_workspace_ids {
        delete_workspace_channel(client, channels, &workspace_id).await?;
    }
    let surviving_channels: Vec<(Id<ChannelMarker>, String)> = channels
        .iter()
        .filter_map(|channel| {
            workspace_topic_id(channel).map(|workspace_id| (channel.id, workspace_id.to_owned()))
        })
        .collect();
    for (workspace_channel, workspace_id) in surviving_channels {
        let mut threads: Vec<Channel> = active_threads
            .iter()
            .filter(|thread| thread.parent_id == Some(workspace_channel))
            .cloned()
            .collect();
        threads.extend(archived_threads(client, workspace_channel).await?);
        let tab_id_prefix = format!("{workspace_id}:t");
        for thread in threads {
            let Some(tab_id) = thread.name.as_deref().and_then(thread_tab_suffix) else {
                continue;
            };
            // A bracket suffix that is not one of this workspace's own tab ids is an
            // owner-made thread name coincidence, not a bridge-owned tab thread; leave it alone.
            if tab_id.starts_with(&tab_id_prefix) && !live_tab_ids.contains(tab_id) {
                delete_channel_if_present(client, thread.id).await?;
                active_threads.retain(|entry| entry.id != thread.id);
            }
        }
    }
    Ok(())
}
