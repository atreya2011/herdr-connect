use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use tokio::sync::Mutex;
use twilight_model::channel::Channel;
use twilight_model::id::Id;
use twilight_model::id::marker::{ChannelMarker, GuildMarker, UserMarker};

use crate::{
    AgentSnapshot, HerdrTab, HerdrWorkspace, TEST_RUN_PREFIX, bridge_eprintln, format_thread_name,
    workspace_list_result,
};

pub type TopologyCache = Arc<Mutex<Option<(Vec<Channel>, Vec<Channel>)>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyRoute {
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
    pub channel_name: String,
    pub thread_name: String,
}

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
    let workspaces = workspace_list_result()?;
    let workspace = workspaces
        .iter()
        .find(|workspace| workspace.workspace_id == workspace_id)
        .ok_or_else(|| format!("herdr topology error: workspace {workspace_id} disappeared"))?;
    let channel_name = workspace_channel_name(workspace_id, &workspace.label)?;
    let thread_name = format_thread_name(&tab.label, tab_id)?;
    Ok(TopologyRoute {
        workspace_id: workspace_id.to_owned(),
        tab_id: tab_id.to_owned(),
        pane_id: pane_id.to_owned(),
        channel_name,
        thread_name,
    })
}

pub fn workspace_channel_name(workspace_id: &str, label: &str) -> Result<String, String> {
    let mut name = String::new();
    for c in label.trim().to_lowercase().chars() {
        if c.is_alphanumeric() || c == '_' {
            name.push(c);
        } else if (c == '-' || c.is_whitespace()) && !name.ends_with('-') {
            name.push('-');
        }
    }
    let name: String = name.trim_matches('-').chars().take(100).collect();
    if name.is_empty() {
        return Err(format!(
            "herdr workspace {workspace_id} has no usable channel name"
        ));
    }
    Ok(name)
}

pub struct NameChange {
    pub id: Id<ChannelMarker>,
    pub name: String,
    pub test_run: bool,
}

pub fn name_changes(
    channels: &[Channel],
    active_threads: &[Channel],
    workspaces: &[HerdrWorkspace],
    tabs: &[HerdrTab],
) -> Vec<NameChange> {
    let mut changes = Vec::new();
    for channel in channels {
        let Some(workspace) = workspace_topic_id(channel).and_then(|id| {
            workspaces
                .iter()
                .find(|workspace| workspace.workspace_id == id)
        }) else {
            continue;
        };
        match workspace_channel_name(&workspace.workspace_id, &workspace.label) {
            Ok(name) if channel.name.as_deref() != Some(name.as_str()) => {
                changes.push(NameChange {
                    id: channel.id,
                    name,
                    test_run: workspace.label.starts_with(TEST_RUN_PREFIX),
                });
            }
            Ok(_) => {}
            Err(error) => bridge_eprintln!("Discord name reconciliation error: {error}"),
        }
        for thread in active_threads
            .iter()
            .filter(|thread| thread.parent_id == Some(channel.id))
        {
            let Some(tab) = thread
                .name
                .as_deref()
                .and_then(thread_tab_suffix)
                .and_then(|id| {
                    tabs.iter()
                        .find(|tab| tab.tab_id == id && tab.workspace_id == workspace.workspace_id)
                })
            else {
                continue;
            };
            match format_thread_name(&tab.label, &tab.tab_id) {
                Ok(name) if thread.name.as_deref() != Some(name.as_str()) => {
                    changes.push(NameChange {
                        id: thread.id,
                        name,
                        test_run: workspace.label.starts_with(TEST_RUN_PREFIX)
                            || tab.label.starts_with(TEST_RUN_PREFIX),
                    });
                }
                Ok(_) => {}
                Err(error) => bridge_eprintln!("Discord name reconciliation error: {error}"),
            }
        }
    }
    changes
}

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

pub async fn sync_topology(
    client: &twilight_http::Client,
    guild: twilight_model::id::Id<twilight_model::id::marker::GuildMarker>,
    channels: &mut Vec<Channel>,
    active_threads: &mut Vec<Channel>,
    route: &TopologyRoute,
    owner_id: &str,
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
    if !production_topology_enabled()
        && !matching_channels.first().map_or_else(
            || is_test_run_name(Some(channel_name)),
            |channel| is_test_run_name(channel.name.as_deref()),
        )
    {
        return Err("test bridge refused a non-testrun workspace channel".to_owned());
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
    let owner = owner_id
        .parse::<Id<UserMarker>>()
        .map_err(|error| format!("DISCORD_OWNER_ID is not numeric: {error}"))?;
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
    client
        .add_thread_member(id, owner)
        .await
        .map_err(|error| error.to_string())?;
    Ok(id)
}

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

fn is_unknown_message_error(error: &twilight_http::Error) -> bool {
    matches!(
        error.kind(),
        twilight_http::error::ErrorType::Response {
            status,
            error: twilight_http::api_error::ApiError::General(api_error),
            ..
        } if *status == twilight_http::response::StatusCode::NOT_FOUND && api_error.code == 10008
    )
}

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

pub async fn delete_thread_created_message(
    client: &twilight_http::Client,
    parent: Id<ChannelMarker>,
    thread_id: Id<ChannelMarker>,
) -> Result<(), String> {
    match client
        .delete_message(parent, Id::new(thread_id.get()))
        .await
    {
        Err(error) if !is_unknown_message_error(&error) => Err(error.to_string()),
        _ => Ok(()),
    }
}

async fn delete_channel_if_present(
    client: &twilight_http::Client,
    id: Id<ChannelMarker>,
) -> Result<(), String> {
    match client.delete_channel(id).await {
        Ok(_) => {
            forget_owned(id);
            Ok(())
        }
        Err(error) if is_unknown_channel_error(&error) => {
            forget_owned(id);
            Ok(())
        }
        Err(error) => Err(error.to_string()),
    }
}

#[must_use]
pub fn workspace_channel_id(channels: &[Channel], workspace_id: &str) -> Option<Id<ChannelMarker>> {
    let topic = format!("herdr workspace [{workspace_id}]");
    channels
        .iter()
        .find(|channel| channel.topic.as_deref() == Some(topic.as_str()))
        .map(|channel| channel.id)
}

fn workspace_topic_id(channel: &Channel) -> Option<&str> {
    channel
        .topic
        .as_deref()?
        .strip_prefix("herdr workspace [")?
        .strip_suffix(']')
}

fn thread_tab_suffix(name: &str) -> Option<&str> {
    let trimmed = name.strip_suffix(']')?;
    trimmed.rfind(" [").map(|start| &trimmed[start + 2..])
}

#[derive(Default)]
struct OwnedTopology {
    production: bool,
    workspaces: HashMap<Id<ChannelMarker>, String>,
    threads: HashMap<Id<ChannelMarker>, OwnedThread>,
    test_run_channels: HashSet<Id<ChannelMarker>>,
}

struct OwnedThread {
    name: String,
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

pub fn enable_production_topology() {
    owned_topology().production = true;
}

fn production_topology_enabled() -> bool {
    owned_topology().production
}

pub fn remember_workspace_channels(channels: &[Channel]) {
    let mut owned = owned_topology();
    for channel in channels {
        if !owned.production && !is_test_run_name(channel.name.as_deref()) {
            owned.workspaces.remove(&channel.id);
            owned.test_run_channels.remove(&channel.id);
            owned
                .threads
                .retain(|_, thread| thread.parent != channel.id);
            continue;
        }
        if is_test_run_name(channel.name.as_deref()) {
            owned.test_run_channels.insert(channel.id);
        } else {
            owned.test_run_channels.remove(&channel.id);
        }
        if let Some(workspace_id) = workspace_topic_id(channel) {
            owned
                .workspaces
                .entry(channel.id)
                .or_insert_with(|| workspace_id.to_owned());
        }
    }
}

pub fn remember_tab_threads(threads: &[Channel]) {
    let mut owned = owned_topology();
    for thread in threads {
        if !owned.production
            && thread
                .parent_id
                .is_none_or(|parent| !owned.workspaces.contains_key(&parent))
        {
            owned.threads.remove(&thread.id);
            continue;
        }
        if let Some(existing) = owned.threads.get_mut(&thread.id)
            && let Some(name) = &thread.name
        {
            existing.name.clone_from(name);
        }
        let Some(parent) = thread.parent_id else {
            continue;
        };
        let Some(workspace_id) = owned.workspaces.get(&parent) else {
            continue;
        };
        let Some(name) = thread.name.as_ref() else {
            continue;
        };
        let Some(tab_id) = thread_tab_suffix(name) else {
            continue;
        };
        if tab_id.starts_with(&format!("{workspace_id}:t")) {
            let tab_id = tab_id.to_owned();
            let name = name.clone();
            owned.threads.insert(
                thread.id,
                OwnedThread {
                    name,
                    tab_id,
                    parent,
                },
            );
        }
    }
}

pub fn forget_owned(id: Id<ChannelMarker>) {
    let mut owned = owned_topology();
    owned.threads.remove(&id);
    owned.test_run_channels.remove(&id);
    if owned.workspaces.remove(&id).is_some() {
        owned.threads.retain(|_, thread| thread.parent != id);
    }
}

#[must_use]
pub fn resolve_owner_deleted_tab(
    thread_id: Id<ChannelMarker>,
) -> Option<(String, Id<ChannelMarker>)> {
    owned_topology()
        .threads
        .get(&thread_id)
        .map(|thread| (thread.tab_id.clone(), thread.parent))
}

#[must_use]
pub fn owned_channel_is_test_run(channel_id: Id<ChannelMarker>) -> bool {
    owned_topology().test_run_channels.contains(&channel_id)
}

#[must_use]
pub fn owned_thread_is_test_run(thread_id: Id<ChannelMarker>) -> bool {
    let owned = owned_topology();
    owned.threads.get(&thread_id).is_some_and(|thread| {
        is_test_run_name(Some(&thread.name)) || owned.test_run_channels.contains(&thread.parent)
    })
}

#[must_use]
pub fn is_test_run_name(name: Option<&str>) -> bool {
    name.is_some_and(|name| name.starts_with(TEST_RUN_PREFIX))
}

#[must_use]
pub fn resolve_owner_deleted_workspace(deleted: &Channel) -> Option<String> {
    owned_topology().workspaces.get(&deleted.id).cloned()
}

pub async fn delete_tab_thread<S: std::hash::BuildHasher + Sync>(
    client: &twilight_http::Client,
    channels: &[Channel],
    active_threads: &mut Vec<Channel>,
    archived_cache: &mut HashMap<Id<ChannelMarker>, Vec<Channel>, S>,
    workspace_id: &str,
    tab_id: &str,
    ignore_test_runs: bool,
) -> Result<(), String> {
    let Some(workspace_channel) = workspace_channel_id(channels, workspace_id) else {
        return Ok(());
    };
    if ignore_test_runs && channel_is_test_run(channels, workspace_channel) {
        return Ok(());
    }
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
    if (ignore_test_runs && is_test_run_name(thread.name.as_deref()))
        || (!production_topology_enabled()
            && !channel_is_test_run(channels, workspace_channel)
            && !is_test_run_name(thread.name.as_deref()))
    {
        return Ok(());
    }
    delete_channel_if_present(client, thread.id).await?;
    active_threads.retain(|entry| entry.id != thread.id);
    delete_thread_created_message(client, workspace_channel, thread.id).await
}

#[must_use]
pub fn channel_is_test_run(channels: &[Channel], id: Id<ChannelMarker>) -> bool {
    channels
        .iter()
        .find(|channel| channel.id == id)
        .is_some_and(|channel| is_test_run_name(channel.name.as_deref()))
}

pub async fn delete_workspace_channel(
    client: &twilight_http::Client,
    channels: &mut Vec<Channel>,
    active_threads: &[Channel],
    workspace_id: &str,
    ignore_test_runs: bool,
) -> Result<(), String> {
    let Some(channel_id) = workspace_channel_id(channels, workspace_id) else {
        return Ok(());
    };
    if !production_topology_enabled() && !channel_is_test_run(channels, channel_id) {
        return Ok(());
    }
    if ignore_test_runs {
        if channel_is_test_run(channels, channel_id)
            || active_threads.iter().any(|thread| {
                thread.parent_id == Some(channel_id) && is_test_run_name(thread.name.as_deref())
            })
        {
            return Ok(());
        }
        if archived_threads(client, channel_id)
            .await?
            .iter()
            .any(|thread| is_test_run_name(thread.name.as_deref()))
        {
            return Ok(());
        }
    }
    delete_channel_if_present(client, channel_id).await?;
    channels.retain(|channel| channel.id != channel_id);
    Ok(())
}

pub async fn topology_absent_from_herdr<S: std::hash::BuildHasher + Sync>(
    client: &twilight_http::Client,
    channels: &[Channel],
    active_threads: &[Channel],
    live_workspace_ids: &HashSet<&str, S>,
    live_tab_ids: &HashSet<&str, S>,
) -> Result<Vec<Channel>, String> {
    let mut absent = Vec::new();
    for channel in channels {
        let Some(workspace_id) = workspace_topic_id(channel) else {
            continue;
        };
        let workspace_absent = !live_workspace_ids.contains(workspace_id);
        if workspace_absent {
            absent.push(channel.clone());
        }
        let mut threads: Vec<Channel> = active_threads
            .iter()
            .filter(|thread| thread.parent_id == Some(channel.id))
            .cloned()
            .collect();
        threads.extend(archived_threads(client, channel.id).await?);
        let tab_id_prefix = format!("{workspace_id}:t");
        absent.extend(threads.into_iter().filter(|thread| {
            workspace_absent
                || thread
                    .name
                    .as_deref()
                    .and_then(thread_tab_suffix)
                    .is_some_and(|tab_id| {
                        tab_id.starts_with(&tab_id_prefix) && !live_tab_ids.contains(tab_id)
                    })
        }));
    }
    Ok(absent)
}

pub async fn delete_topology_absent_from_herdr<S: std::hash::BuildHasher + Sync>(
    client: &twilight_http::Client,
    channels: &mut Vec<Channel>,
    active_threads: &mut Vec<Channel>,
    live_workspace_ids: &HashSet<&str, S>,
    live_tab_ids: &HashSet<&str, S>,
    ignore_test_runs: bool,
) -> Result<(), String> {
    let absent = topology_absent_from_herdr(
        client,
        channels,
        active_threads,
        live_workspace_ids,
        live_tab_ids,
    )
    .await?;
    let orphaned_channels: HashSet<_> = absent
        .iter()
        .filter(|channel| workspace_topic_id(channel).is_some())
        .map(|channel| channel.id)
        .collect();
    for channel in absent {
        if !production_topology_enabled()
            && !is_test_run_name(channel.name.as_deref())
            && !channel
                .parent_id
                .is_some_and(|parent| channel_is_test_run(channels, parent))
        {
            continue;
        }
        if ignore_test_runs
            && (is_test_run_name(channel.name.as_deref())
                || channel
                    .parent_id
                    .is_some_and(|parent| channel_is_test_run(channels, parent)))
        {
            continue;
        }
        if let Some(workspace_id) = workspace_topic_id(&channel) {
            delete_workspace_channel(
                client,
                channels,
                active_threads,
                workspace_id,
                ignore_test_runs,
            )
            .await?;
        } else if let Some(parent) = channel.parent_id {
            if !orphaned_channels.contains(&parent) {
                delete_channel_if_present(client, channel.id).await?;
                delete_thread_created_message(client, parent, channel.id).await?;
            }
            active_threads.retain(|entry| entry.id != channel.id);
        }
    }
    Ok(())
}

#[cfg(test)]
pub mod owner_deletion_tests {
    use serde_json::json;
    use twilight_model::channel::Channel;

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
}
