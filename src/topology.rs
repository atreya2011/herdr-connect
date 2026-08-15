use crate::{AgentSnapshot, HerdrTab, format_thread_name};
use std::collections::HashSet;

/// The Discord topology and sole pane that owns it for one agent transition.
#[derive(Debug, PartialEq, Eq)]
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
/// Returns an error for missing or ambiguous Herdr identity, missing workspace evidence, or an
/// unusable Discord name.
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
    let tab_id = usable_agent_identity(agent.tab_id.as_deref(), "tab", terminal_id)?;
    let workspace_id =
        usable_agent_identity(agent.workspace_id.as_deref(), "workspace", terminal_id)?;
    let pane_id = usable_agent_identity(agent.pane_id.as_deref(), "pane", terminal_id)?;
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
    if agents
        .iter()
        .filter(|candidate| candidate.tab_id.as_deref() == Some(tab_id))
        .count()
        != 1
    {
        return Err(format!("herdr tab {tab_id} maps to multiple panes"));
    }
    let workspace_tabs: HashSet<&str> = tabs
        .iter()
        .filter(|candidate| candidate.workspace_id == workspace_id)
        .map(|candidate| candidate.tab_id.as_str())
        .collect();
    let cwds: Vec<String> = agents
        .iter()
        .filter(|candidate| {
            candidate
                .tab_id
                .as_deref()
                .is_some_and(|candidate_tab| workspace_tabs.contains(candidate_tab))
        })
        .filter_map(|candidate| candidate.cwd.clone())
        .collect();
    Ok(TopologyRoute {
        workspace_id: workspace_id.to_owned(),
        tab_id: tab_id.to_owned(),
        pane_id: pane_id.to_owned(),
        channel_name: workspace_channel_name(workspace_id, &cwds)?,
        thread_name: format_thread_name(
            &tab.label,
            agent.terminal_title_stripped.as_deref().unwrap_or_default(),
            tab_id,
        )?,
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
            if *count > common_count {
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

/// Synchronizes the Discord workspace topology.
///
/// # Errors
///
/// Returns Discord request or response errors.
pub async fn sync_topology(
    client: &twilight_http::Client,
    guild: twilight_model::id::Id<twilight_model::id::marker::GuildMarker>,
    workspace_id: &str,
    channel_name: &str,
    thread_name: &str,
    tab_id: &str,
) -> Result<twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>, String> {
    let channels = client
        .guild_channels(guild)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?;
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
    let workspace_channel = if let Some(channel) = channels
        .iter()
        .find(|channel| channel.topic.as_deref() == Some(topic.as_str()))
    {
        if channel.name.as_deref() != Some(channel_name) {
            client
                .update_channel(channel.id)
                .name(channel_name)
                .await
                .map_err(|error| error.to_string())?;
        }
        channel.id
    } else {
        client
            .create_guild_channel(guild, channel_name)
            .topic(&topic)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .id
    };
    let mut existing = client
        .active_threads(guild)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?
        .threads;
    existing.extend(archived_threads(client, workspace_channel).await?);
    let mut matching_threads: Vec<_> = existing
        .into_iter()
        .filter(|thread| {
            thread.parent_id == Some(workspace_channel)
                && thread
                    .name
                    .as_deref()
                    .is_some_and(|name| name.ends_with(&thread_suffix))
        })
        .collect();
    if matching_threads.len() > 1 {
        return Err(format!(
            "Discord topology has duplicate threads for tab {tab_id}"
        ));
    }
    if let Some(thread) = matching_threads.pop() {
        if thread
            .thread_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.archived)
        {
            client
                .update_thread(thread.id)
                .archived(false)
                .await
                .map_err(|error| error.to_string())?;
        }
        return Ok(thread.id);
    }
    Ok(client
        .create_thread(
            workspace_channel,
            thread_name,
            twilight_model::channel::ChannelType::PublicThread,
        )
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?
        .id)
}

async fn archived_threads(
    client: &twilight_http::Client,
    workspace_channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
) -> Result<Vec<twilight_model::channel::Channel>, String> {
    let mut before = None;
    let mut threads = Vec::new();
    loop {
        let request = client.public_archived_threads(workspace_channel).limit(100);
        let response = if let Some(before) = before.as_deref() {
            request.before(before).await
        } else {
            request.await
        }
        .map_err(|error| error.to_string())?;
        let listing = response.model().await.map_err(|error| error.to_string())?;
        let has_more = listing.has_more.unwrap_or(false);
        before = listing
            .threads
            .last()
            .and_then(|thread| thread.thread_metadata.as_ref())
            .map(|metadata| metadata.archive_timestamp.iso_8601().to_string());
        threads.extend(listing.threads);
        if !has_more {
            return Ok(threads);
        }
        if before.is_none() {
            return Err("Discord returned archived threads without a pagination cursor".to_owned());
        }
    }
}
