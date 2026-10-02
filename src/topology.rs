use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use tokio::sync::Mutex;
use twilight_model::channel::Channel;
use twilight_model::id::Id;
use twilight_model::id::marker::{ChannelMarker, GuildMarker};

use crate::{AgentSnapshot, HerdrTab, format_thread_name};

/// Shared, per-process cache of one guild's channel list and active-thread list, reused across
/// tabs so a startup sweep does not refetch both lists for every tab.
pub type TopologyCache = Arc<Mutex<Option<(Vec<Channel>, Vec<Channel>)>>>;

/// The Discord topology and sole pane that owns it for one agent transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyRoute {
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
    pub channel_name: String,
    pub thread_name: String,
}

/// Resolves one agent transition to an unambiguous workspace, tab, and pane.
///
/// # Errors
///
/// Returns an error for missing or ambiguous Herdr identity, missing workspace evidence, a
/// channel naming failure, or a tab label that cannot produce a Discord thread name.
pub fn route_topology(
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    terminal_id: &str,
) -> Result<TopologyRoute, String> {
    let matching_agents: Vec<&AgentSnapshot> = agents
        .iter()
        .filter(|agent| agent.terminal_id == terminal_id)
        .collect();
    let [agent] = matching_agents.as_slice() else {
        return Err(format!(
            "herdr topology has duplicate or missing terminal {terminal_id}"
        ));
    };
    let (tab_id, workspace_id, pane_id) = (
        agent.tab_id.as_str(),
        agent.workspace_id.as_str(),
        agent.pane_id.as_str(),
    );
    let matching_tabs: Vec<&HerdrTab> = tabs.iter().filter(|tab| tab.tab_id == tab_id).collect();
    let [tab] = matching_tabs.as_slice() else {
        return Err(if matching_tabs.is_empty() {
            format!("herdr topology error: tab {tab_id} disappeared")
        } else {
            format!("herdr topology error: duplicate tab identifier {tab_id}")
        });
    };
    if tab.workspace_id != workspace_id {
        return Err(format!(
            "herdr topology error: tab {tab_id} workspace mismatch"
        ));
    }
    let workspace_tabs: HashSet<&str> = tabs
        .iter()
        .filter(|candidate| candidate.workspace_id == workspace_id)
        .map(|candidate| candidate.tab_id.as_str())
        .collect();
    let cwds: Vec<String> = agents
        .iter()
        .filter(|candidate| candidate.workspace_id == workspace_id)
        .filter(|candidate| workspace_tabs.contains(candidate.tab_id.as_str()))
        .filter_map(|candidate| candidate.cwd.clone())
        .collect();
    let channel_name = workspace_channel_name(workspace_id, &cwds)?;
    let thread_name = format_thread_name(&tab.label, tab_id)?;
    Ok(TopologyRoute {
        workspace_id: workspace_id.to_owned(),
        tab_id: tab_id.to_owned(),
        pane_id: pane_id.to_owned(),
        channel_name,
        thread_name,
    })
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
    remember_workspace_channels(&channels);
    remember_tab_threads(&active_threads);
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
    let watermark = fetched
        .iter()
        .map(|entry| entry.id.get())
        .max()
        .unwrap_or(0);
    fetched.extend(
        cached
            .into_iter()
            .filter(|entry| entry.id.get() > watermark),
    );
}

/// Resolves the one thread that identifies a tab.
///
/// # Errors
///
/// Returns an error when more than one thread claims the tab.
fn single_matching_thread(
    threads: &[Channel],
    workspace_channel: Id<ChannelMarker>,
    thread_suffix: &str,
    tab_id: &str,
) -> Result<Option<Channel>, String> {
    let mut matching = threads.iter().filter(|thread| {
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

/// Whether the cached channel/thread lists already resolve `route`'s tab thread, without any
/// Discord request.
///
/// `None` means the cache does not (yet) resolve the route: the caller must fetch fresh and let
/// [`sync_topology`] create whatever is missing. Only the active-thread list is consulted,
/// matching the existing miss-path contract: archived-thread listing stays a
/// Discord-request-issuing fallback that only [`sync_topology`] performs, on a miss.
///
/// # Errors
///
/// Returns an error when the cached active-thread list has duplicate threads for the tab.
pub fn cached_route(
    channels: &[Channel],
    active_threads: &[Channel],
    route: &TopologyRoute,
) -> Result<Option<Id<ChannelMarker>>, String> {
    let Some(workspace_channel) = workspace_channel_id(channels, &route.workspace_id) else {
        return Ok(None);
    };
    let thread_suffix = format!(" [{}]", route.tab_id);
    let resolved = single_matching_thread(
        active_threads,
        workspace_channel,
        &thread_suffix,
        &route.tab_id,
    )?;
    Ok(resolved.map(|thread| thread.id))
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
    let thread_suffix = format!(" [{tab_id}]");
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
        remember_workspace_channels(std::slice::from_ref(&created));
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
    remember_tab_threads(std::slice::from_ref(&created));
    active_threads.push(created);
    Ok(id)
}

/// Registers the archived tab threads of every workspace channel, independent of any Herdr call
/// and of the startup delete pass.
///
/// A tab whose pane carries no session is not synced, so its auto-archived thread is found only
/// by this listing. It fetches the guild once and lists each workspace channel's archived threads
/// once.
///
/// # Errors
///
/// Returns the first Discord fetch or listing error.
pub async fn register_archived_tab_threads(
    client: &twilight_http::Client,
    guild: Id<GuildMarker>,
) -> Result<(), String> {
    let (channels, _) = fetch_topology_lists(client, guild).await?;
    for channel in channels
        .iter()
        .filter(|channel| workspace_topic_id(channel).is_some())
    {
        archived_threads(client, channel.id)
            .await
            .map_err(|error| format!("archived threads of channel {}: {error}", channel.id))?;
    }
    Ok(())
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
        let has_more = listing
            .has_more
            .ok_or_else(|| "Discord archived-thread listing has no has_more".to_owned())?;
        if has_more && listing.threads.is_empty() {
            return Err("Discord returned an empty archived-thread page with has_more".to_owned());
        }
        let next_before = listing
            .threads
            .last()
            .and_then(|thread| thread.thread_metadata.as_ref())
            .map(|metadata| metadata.archive_timestamp.iso_8601().to_string());
        remember_tab_threads(&listing.threads);
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
pub fn is_unknown_channel_error(error: &twilight_http::Error) -> bool {
    matches!(
        error.kind(),
        twilight_http::error::ErrorType::Response {
            status,
            error: twilight_http::api_error::ApiError::General(api_error),
            ..
        } if *status == twilight_http::response::StatusCode::NOT_FOUND && api_error.code == 10003
    )
}

/// True when a Discord API error means the target webhook is already gone (deleting its channel
/// deletes its webhooks with it, so a cached webhook can go stale independently of its channel).
pub fn is_unknown_webhook_error(error: &twilight_http::Error) -> bool {
    matches!(
        error.kind(),
        twilight_http::error::ErrorType::Response {
            status,
            error: twilight_http::api_error::ApiError::General(api_error),
            ..
        } if *status == twilight_http::response::StatusCode::NOT_FOUND && api_error.code == 10015
    )
}

/// Ids of channels and threads the bridge itself is deleting, so the gateway's matching delete
/// event is recognised as the bridge's own doing and not forwarded to Herdr as an owner deletion.
static SELF_DELETIONS: LazyLock<std::sync::Mutex<HashSet<Id<ChannelMarker>>>> =
    LazyLock::new(|| std::sync::Mutex::new(HashSet::new()));

/// Consumes the self-deletion marker for `id`, if one is pending.
///
/// `true` means the bridge deleted this channel or thread itself and its gateway delete event must
/// be ignored; the marker is cleared, so a later event for the same id is not suppressed.
#[must_use]
pub fn take_self_deletion(id: Id<ChannelMarker>) -> bool {
    SELF_DELETIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&id)
}

pub fn record_self_deletion(id: Id<ChannelMarker>) {
    SELF_DELETIONS
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(id);
}

/// Deletes one Discord channel or thread, treating an already-deleted target as done.
///
/// The id is recorded as a self-deletion before the request, because the gateway event can arrive
/// before the response; a failed or already-gone delete forgets the record again.
async fn delete_channel_if_present(
    client: &twilight_http::Client,
    id: Id<ChannelMarker>,
) -> Result<(), String> {
    record_self_deletion(id);
    match client.delete_channel(id).await {
        Ok(_) => {
            forget_owned(id);
            Ok(())
        }
        Err(error) => {
            let _ = take_self_deletion(id);
            if is_unknown_channel_error(&error) {
                forget_owned(id);
                Ok(())
            } else {
                Err(error.to_string())
            }
        }
    }
}

/// The id of the channel whose topic identifies `workspace_id`, if one is present.
#[must_use]
pub fn workspace_channel_id(channels: &[Channel], workspace_id: &str) -> Option<Id<ChannelMarker>> {
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

/// Bridge-owned topology the gateway's delete events are resolved against.
///
/// A gateway thread-delete event carries only ids, so the thread's tab cannot be read from the
/// event. This registry is filled wherever a workspace channel or tab thread is fetched, created,
/// or found archived, and it is never cleared by cache invalidation or reconcile: a thread that
/// left the topology cache (an auto-archive, a failed delivery, a refetch) is still resolvable.
/// An entry leaves only when its deletion is resolved.
#[derive(Default)]
struct OwnedTopology {
    workspaces: HashMap<Id<ChannelMarker>, String>,
    threads: HashMap<Id<ChannelMarker>, OwnedThread>,
}

struct OwnedThread {
    tab_id: String,
    parent: Id<ChannelMarker>,
}

static OWNED_TOPOLOGY: LazyLock<std::sync::Mutex<OwnedTopology>> =
    LazyLock::new(|| std::sync::Mutex::new(OwnedTopology::default()));

fn owned_topology() -> std::sync::MutexGuard<'static, OwnedTopology> {
    OWNED_TOPOLOGY
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Records every channel whose topic is `herdr workspace [id]` as a bridge-owned workspace channel.
pub fn remember_workspace_channels(channels: &[Channel]) {
    let mut owned = owned_topology();
    for channel in channels {
        if let Some(workspace_id) = workspace_topic_id(channel) {
            owned.workspaces.insert(channel.id, workspace_id.to_owned());
        }
    }
}

/// Records every thread under a remembered workspace channel whose trailing ` [tab_id]` suffix
/// starts with that workspace id, the same ownership rule the startup sweep applies.
pub fn remember_tab_threads(threads: &[Channel]) {
    let mut owned = owned_topology();
    for thread in threads {
        let Some(parent) = thread.parent_id else {
            continue;
        };
        let Some(workspace_id) = owned.workspaces.get(&parent) else {
            continue;
        };
        let Some(tab_id) = thread.name.as_deref().and_then(thread_tab_suffix) else {
            continue;
        };
        if tab_id.starts_with(&format!("{workspace_id}:t")) {
            let tab_id = tab_id.to_owned();
            owned
                .threads
                .insert(thread.id, OwnedThread { tab_id, parent });
        }
    }
}

/// Forgets a resolved or bridge-made deletion: the thread, or the workspace channel with every
/// thread it parented.
pub fn forget_owned(id: Id<ChannelMarker>) {
    let mut owned = owned_topology();
    owned.threads.remove(&id);
    if owned.workspaces.remove(&id).is_some() {
        owned.threads.retain(|_, thread| thread.parent != id);
    }
}

/// The Herdr tab id behind a deleted thread, from the durable registry.
///
/// The registry holds the thread of every live tab, so a thread it does not hold is not a tab
/// thread (an owner-made thread, or one already forgotten) and is not the bridge's concern.
#[must_use]
pub fn resolve_owner_deleted_tab(thread_id: Id<ChannelMarker>) -> Option<String> {
    owned_topology()
        .threads
        .get(&thread_id)
        .map(|thread| thread.tab_id.clone())
}

/// The Herdr workspace id a deleted channel's `herdr workspace [id]` topic named, if it had one.
#[must_use]
pub fn resolve_owner_deleted_workspace(deleted: &Channel) -> Option<String> {
    workspace_topic_id(deleted).map(ToOwned::to_owned)
}

/// Deletes the Discord thread identifying one closed Herdr tab.
///
/// Searches active threads first and the workspace channel's archived threads on a miss, caching
/// the archived listing per workspace channel in `archived_cache` so repeated misses against the
/// same channel within one call site (for example, the startup sweep's delete pass over several
/// tabs) list it at most once. A missing workspace channel or thread is not an error: the tab is
/// already gone from Discord.
///
/// # Errors
///
/// Returns Discord request or response errors, or a duplicate-thread topology error.
pub async fn delete_tab_thread<S: std::hash::BuildHasher + Sync>(
    client: &twilight_http::Client,
    channels: &[Channel],
    active_threads: &mut Vec<Channel>,
    archived_cache: &mut HashMap<Id<ChannelMarker>, Vec<Channel>, S>,
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
        if let std::collections::hash_map::Entry::Vacant(entry) =
            archived_cache.entry(workspace_channel)
        {
            entry.insert(archived_threads(client, workspace_channel).await?);
        }
        resolved = single_matching_thread(
            &archived_cache[&workspace_channel],
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

#[cfg(test)]
pub mod owner_deletion_tests {
    use serde_json::json;
    use twilight_model::channel::Channel;
    use twilight_model::id::{Id, marker::ChannelMarker};

    use super::{
        forget_owned, record_self_deletion, remember_tab_threads, remember_workspace_channels,
        resolve_owner_deleted_tab, resolve_owner_deleted_workspace, take_self_deletion,
    };

    pub fn channel(id: u64, name: &str, topic: Option<&str>, parent: Option<u64>) -> Channel {
        let mut value = json!({
            "id": id.to_string(),
            "type": if parent.is_some() { 11 } else { 0 },
            "name": name,
            "guild_id": "1",
        });
        if let Some(topic) = topic {
            value["topic"] = json!(topic);
        }
        if let Some(parent) = parent {
            value["parent_id"] = json!(parent.to_string());
        }
        serde_json::from_value(value).expect("synthetic channel deserializes")
    }

    fn id(raw: u64) -> Id<ChannelMarker> {
        Id::new(raw)
    }

    #[test]
    fn owner_deleted_thread_resolves_from_the_registry_not_the_cache() {
        remember_workspace_channels(&[
            channel(8_010, "work", Some("herdr workspace [w1]"), None),
            channel(8_011, "chat", Some("just talking"), None),
        ]);
        remember_tab_threads(&[
            channel(8_020, "build [w1:t2]", None, Some(8_010)),
            channel(8_021, "build [w1:t2]", None, Some(8_011)),
            channel(8_022, "build [w9:t2]", None, Some(8_010)),
            channel(8_023, "plain thread", None, Some(8_010)),
        ]);
        let cases = [
            ("bridge tab thread", 8_020, Some("w1:t2")),
            ("thread under a non-workspace channel", 8_021, None),
            ("suffix from another workspace", 8_022, None),
            ("name without a tab suffix", 8_023, None),
            ("thread never recorded", 8_099, None),
        ];
        for (name, thread_id, expected) in cases {
            assert_eq!(
                resolve_owner_deleted_tab(id(thread_id)).as_deref(),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn a_thread_dropped_by_reconcile_still_resolves() {
        let workspace = channel(8_210, "work", Some("herdr workspace [w3]"), None);
        let thread = channel(8_220, "a [w3:t1]", None, Some(8_210));
        let newer = channel(8_230, "b [w3:t2]", None, Some(8_210));
        remember_workspace_channels(std::slice::from_ref(&workspace));
        remember_tab_threads(&[thread.clone(), newer.clone()]);
        let mut cache = Some((vec![workspace.clone()], vec![thread]));
        super::reconcile_topology_cache(&mut cache, (vec![workspace], vec![newer]));
        let cached: Vec<_> = cache
            .as_ref()
            .map(|(_, threads)| threads.iter().map(|entry| entry.id).collect())
            .unwrap_or_default();
        assert_eq!(
            cached,
            vec![id(8_230)],
            "the refetch dropped the older thread"
        );
        assert_eq!(
            resolve_owner_deleted_tab(id(8_220)),
            Some("w3:t1".to_owned()),
            "after reconcile"
        );
    }

    #[test]
    fn a_forgotten_thread_no_longer_resolves_and_a_forgotten_channel_takes_its_threads() {
        remember_workspace_channels(&[channel(8_110, "work", Some("herdr workspace [w2]"), None)]);
        remember_tab_threads(&[
            channel(8_120, "a [w2:t1]", None, Some(8_110)),
            channel(8_121, "b [w2:t2]", None, Some(8_110)),
        ]);
        forget_owned(id(8_120));
        assert_eq!(resolve_owner_deleted_tab(id(8_120)), None);
        assert_eq!(
            resolve_owner_deleted_tab(id(8_121)),
            Some("w2:t2".to_owned())
        );
        forget_owned(id(8_110));
        assert_eq!(resolve_owner_deleted_tab(id(8_121)), None);
    }

    #[test]
    fn owner_deleted_channel_resolves_only_a_workspace_topic() {
        let cases = [
            (
                "workspace channel",
                Some("herdr workspace [w1]"),
                Some("w1"),
            ),
            ("channel with another topic", Some("lounge"), None),
            ("channel without a topic", None, None),
        ];
        for (name, topic, expected) in cases {
            let deleted = channel(10, "work", topic, None);
            assert_eq!(
                resolve_owner_deleted_workspace(&deleted).as_deref(),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn a_self_deletion_is_ignored_once() {
        record_self_deletion(id(7_000_001));
        assert!(take_self_deletion(id(7_000_001)));
        assert!(!take_self_deletion(id(7_000_001)));
        assert!(!take_self_deletion(id(7_000_002)));
    }
}
