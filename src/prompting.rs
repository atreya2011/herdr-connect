//! Owner-message routing is restricted to mapped Discord threads.
//!
//! The owner-authored end-to-end path is deferred to the orchestrator's live proof because REST
//! message creation responses do not carry the guild identifier required by the gateway handler.

use crate::{AgentSnapshot, agent_prompt, list_agents};
use std::sync::Arc;
use twilight_http::Client;
use twilight_model::{
    channel::{ChannelType, Message},
    id::{Id, marker::GuildMarker},
};

/// Handles one Discord owner message after gateway-level filtering.
///
/// # Errors
///
/// Returns Discord, Herdr, or task-dispatch errors.
pub async fn handle_owner_message(
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: &str,
    message: Message,
) -> Result<(), String> {
    if message.guild_id != Some(guild)
        || !should_handle_owner_message(
            &message.author.id.to_string(),
            message.author.bot,
            owner_id,
        )
    {
        return Ok(());
    }

    let thread = client
        .channel(message.channel_id)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?;
    if !is_thread_channel(thread.kind) {
        return Ok(());
    }
    let Some(parent_id) = thread.parent_id else {
        return Ok(());
    };
    let parent = client
        .channel(parent_id)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?;
    let Some(thread_name) = thread.name.as_deref() else {
        return Ok(());
    };
    let Some(topic) = parent.topic.as_deref() else {
        return Ok(());
    };
    if !is_qualifying_prompt_surface(thread_name, topic) {
        return Ok(());
    }
    let agents = tokio::task::spawn_blocking(list_agents)
        .await
        .map_err(|error| format!("herdr agent.list task failed: {error}"))??;
    let pane_id = match resolve_prompt_pane(thread_name, topic, &agents) {
        Ok(pane_id) => pane_id,
        Err(reason) => {
            reply(&client, &message, &reason).await?;
            return Ok(());
        }
    };
    let text = message.content.clone();
    let prompt_pane = pane_id.clone();
    let result = tokio::task::spawn_blocking(move || agent_prompt(&prompt_pane, &text))
        .await
        .map_err(|error| format!("agent.prompt task failed: {error}"))?;
    match result {
        Ok(_) => reply(
            &client,
            &message,
            "accepted: prompt submitted; Herdr wait observes lifecycle state, not content delivery",
        )
        .await,
        Err(error) => {
            let response = format!("refused: prompt submission failed: {error}");
            reply(&client, &message, &response).await?;
            Err(format!("agent.prompt failed: {error}"))
        }
    }
}

#[must_use]
pub fn should_handle_owner_message(author_id: &str, is_bot: bool, owner_id: &str) -> bool {
    !is_bot && !owner_id.trim().is_empty() && author_id == owner_id.trim()
}

#[must_use]
const fn is_thread_channel(kind: ChannelType) -> bool {
    kind.is_thread()
}

#[must_use]
fn is_qualifying_prompt_surface(thread_name: &str, topic: &str) -> bool {
    let has_tab_suffix = thread_name
        .rsplit_once(" [")
        .and_then(|(_, suffix)| suffix.strip_suffix(']'))
        .is_some_and(|value| !value.trim().is_empty());
    let has_workspace_marker = topic
        .strip_prefix("herdr workspace [")
        .and_then(|value| value.strip_suffix(']'))
        .is_some_and(|value| !value.trim().is_empty());
    has_tab_suffix && has_workspace_marker
}

fn resolve_prompt_pane(
    thread_name: &str,
    topic: &str,
    agents: &[AgentSnapshot],
) -> Result<String, String> {
    let tab_id = thread_name
        .rsplit_once(" [")
        .and_then(|(_, suffix)| suffix.strip_suffix(']'))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "refused: unmapped Discord thread".to_owned())?;
    let workspace_id = topic
        .strip_prefix("herdr workspace [")
        .and_then(|value| value.strip_suffix(']'))
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "refused: unmapped Discord channel".to_owned())?;
    let matches: Vec<&AgentSnapshot> = agents
        .iter()
        .filter(|agent| agent.tab_id.as_deref() == Some(tab_id))
        .filter(|agent| agent.workspace_id.as_deref() == Some(workspace_id))
        .collect();
    let agent = match matches.as_slice() {
        [] => return Err("refused: unmapped pane".to_owned()),
        [_first, _second, ..] => return Err("refused: ambiguous pane mapping".to_owned()),
        [agent] => agent,
    };
    let pane_id = agent
        .pane_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "refused: unmapped pane".to_owned())?;
    match agent.agent_status.trim() {
        "idle" | "done" => Ok(pane_id.to_owned()),
        "working" => Err("refused: agent state is working".to_owned()),
        "blocked" => Err("refused: agent state is blocked".to_owned()),
        "" => Err("refused: agent state is unknown".to_owned()),
        state => Err(format!("refused: agent state is {state}")),
    }
}

async fn reply(client: &Client, message: &Message, content: &str) -> Result<(), String> {
    client
        .create_message(message.channel_id)
        .content(content)
        .reply(message.id)
        .await
        .map_err(|error| error.to_string())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{is_qualifying_prompt_surface, is_thread_channel, resolve_prompt_pane};
    use crate::AgentSnapshot;
    use serde_json::Value;
    use twilight_model::channel::ChannelType;

    #[test]
    fn only_thread_channel_kinds_are_prompt_surfaces() {
        let cases = [
            (ChannelType::GuildText, false),
            (ChannelType::GuildCategory, false),
            (ChannelType::GuildForum, false),
            (ChannelType::AnnouncementThread, true),
            (ChannelType::PublicThread, true),
            (ChannelType::PrivateThread, true),
        ];
        for (kind, expected) in cases {
            assert_eq!(is_thread_channel(kind), expected);
        }
    }

    #[test]
    fn qualifying_prompt_surface_requires_both_discord_markers() {
        let cases = [
            (
                "bridge [real-workspace:tab-1]",
                "herdr workspace [real-workspace]",
                true,
            ),
            ("bridge", "herdr workspace [real-workspace]", false),
            ("bridge [real-workspace:tab-1]", "workspace", false),
            ("bridge", "workspace", false),
        ];
        for (thread_name, topic, expected) in cases {
            assert_eq!(
                is_qualifying_prompt_surface(thread_name, topic),
                expected,
                "thread_name={thread_name:?}, topic={topic:?}"
            );
        }
    }

    #[test]
    fn resolve_prompt_pane_captured_snapshot_refusal_branches() {
        let value: Value =
            serde_json::from_str(include_str!("../tests/fixtures/herdr-agent-list.json"))
                .expect("captured agent snapshot is JSON");
        let captured: Vec<AgentSnapshot> =
            serde_json::from_value(value["result"]["agents"].clone())
                .expect("captured agent snapshot has the expected shape");
        let topic = "herdr workspace [real-workspace]";
        let mut missing_pane = captured[0].clone();
        missing_pane.pane_id = None;
        let mut working = captured[0].clone();
        working.agent_status = "working".to_owned();
        let mut blocked = captured[0].clone();
        blocked.agent_status = "blocked".to_owned();
        let mut unknown = captured[0].clone();
        unknown.agent_status = "paused".to_owned();
        let mut no_status = captured[0].clone();
        no_status.agent_status.clear();
        let cases = [
            (
                "missing thread suffix",
                "bridge",
                topic,
                captured.clone(),
                "refused: unmapped Discord thread",
            ),
            (
                "invalid workspace topic",
                "bridge [real-workspace:tab-1]",
                "workspace",
                captured.clone(),
                "refused: unmapped Discord channel",
            ),
            (
                "no matching pane",
                "bridge [missing-tab]",
                topic,
                vec![captured[0].clone()],
                "refused: unmapped pane",
            ),
            (
                "ambiguous pane",
                "bridge [real-workspace:tab-1]",
                topic,
                captured.clone(),
                "refused: ambiguous pane mapping",
            ),
            (
                "matching pane has no pane id",
                "bridge [real-workspace:tab-1]",
                topic,
                vec![missing_pane],
                "refused: unmapped pane",
            ),
            (
                "working pane",
                "bridge [real-workspace:tab-1]",
                topic,
                vec![working],
                "refused: agent state is working",
            ),
            (
                "blocked pane",
                "bridge [real-workspace:tab-1]",
                topic,
                vec![blocked],
                "refused: agent state is blocked",
            ),
            (
                "missing agent status",
                "bridge [real-workspace:tab-1]",
                topic,
                vec![no_status],
                "refused: agent state is unknown",
            ),
            (
                "unrecognized agent status",
                "bridge [real-workspace:tab-1]",
                topic,
                vec![unknown],
                "refused: agent state is paused",
            ),
        ];
        for (branch, thread_name, topic, agents, expected) in cases {
            assert_eq!(
                resolve_prompt_pane(thread_name, topic, &agents),
                Err(expected.to_owned()),
                "branch={branch}"
            );
        }
    }
}
