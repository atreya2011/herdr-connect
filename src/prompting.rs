//! Owner-message routing is restricted to mapped Discord threads.
//!
//! The owner-authored end-to-end path is deferred to the orchestrator's live proof because REST
//! message creation responses do not carry the guild identifier required by the gateway handler.

use crate::herdr::agent_prompt;
use crate::{AgentSnapshot, list_agents};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use twilight_http::Client;
use twilight_model::{
    channel::{ChannelType, Message},
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker},
    },
};

const PROMPT_ACCEPTED_REPLY: &str = "accepted: prompt submitted; Herdr state may be unconfirmed";
const TYPING_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(8);

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
    let Some((tab_id, workspace_id)) = prompt_surface_markers(thread_name, topic) else {
        return Ok(());
    };
    if !has_prompt_content(&message.content) {
        reply(&client, &message, "refused: prompt content is empty").await?;
        return Ok(());
    }
    let agents = match tokio::task::spawn_blocking(list_agents).await {
        Ok(Ok(agents)) => agents,
        Ok(Err(error)) => {
            let response = agent_list_failure_reply(&error);
            reply(&client, &message, &response).await?;
            return Err(format!("agent.list failed: {error}"));
        }
        Err(error) => {
            let error = format!("herdr agent.list task failed: {error}");
            let response = agent_list_failure_reply(&error);
            reply(&client, &message, &response).await?;
            return Err(error);
        }
    };
    let pane_id = match resolve_prompt_pane(tab_id, workspace_id, &agents) {
        Ok(pane_id) => pane_id,
        Err(reason) => {
            reply(&client, &message, &reason).await?;
            return Ok(());
        }
    };
    tokio::spawn(keep_typing_while_working(
        Arc::clone(&client),
        message.channel_id,
        pane_id.clone(),
    ));
    let text = message.content.clone();
    let prompt_pane = pane_id.clone();
    let result = tokio::task::spawn_blocking(move || agent_prompt(&prompt_pane, &text))
        .await
        .map_err(|error| format!("agent.prompt task failed: {error}"))?;
    match result {
        Ok(_) => reply(&client, &message, PROMPT_ACCEPTED_REPLY).await,
        Err(error) => {
            let response = format!("refused: prompt submission failed: {error}");
            reply(&client, &message, &response).await?;
            Err(format!("agent.prompt failed: {error}"))
        }
    }
}

#[must_use]
fn has_prompt_content(content: &str) -> bool {
    !content.trim().is_empty()
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
fn prompt_surface_markers<'a, 'b>(
    thread_name: &'a str,
    topic: &'b str,
) -> Option<(&'a str, &'b str)> {
    let tab_id = thread_name
        .rsplit_once(" [")
        .and_then(|(_, suffix)| suffix.strip_suffix(']'))
        .filter(|value| !value.trim().is_empty())?;
    let workspace_id = topic
        .strip_prefix("herdr workspace [")
        .and_then(|value| value.strip_suffix(']'))
        .filter(|value| !value.trim().is_empty())?;
    Some((tab_id, workspace_id))
}

fn resolve_prompt_pane(
    tab_id: &str,
    workspace_id: &str,
    agents: &[AgentSnapshot],
) -> Result<String, String> {
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

/// Keeps the Discord typing indicator alive in `channel` while `still_working` reports true.
///
/// Re-triggers on `interval` and stops as soon as `still_working` reports false or errors.
pub async fn maintain_typing_until_settled<F, Fut>(
    client: &Client,
    channel: Id<ChannelMarker>,
    interval: Duration,
    mut still_working: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool, String>>,
{
    loop {
        if client.create_typing_trigger(channel).await.is_err() {
            return;
        }
        tokio::time::sleep(interval).await;
        if !matches!(still_working().await, Ok(true)) {
            return;
        }
    }
}

async fn keep_typing_while_working(
    client: Arc<Client>,
    channel: Id<ChannelMarker>,
    pane_id: String,
) {
    maintain_typing_until_settled(&client, channel, TYPING_KEEPALIVE_INTERVAL, move || {
        let pane_id = pane_id.clone();
        async move { pane_still_working(&pane_id).await }
    })
    .await;
}

async fn pane_still_working(pane_id: &str) -> Result<bool, String> {
    let agents = tokio::task::spawn_blocking(list_agents)
        .await
        .map_err(|error| format!("herdr agent.list task failed: {error}"))?
        .map_err(|error| format!("agent.list failed: {error}"))?;
    Ok(pane_status_is_working(&agents, pane_id))
}

#[must_use]
fn pane_status_is_working(agents: &[AgentSnapshot], pane_id: &str) -> bool {
    agents.iter().any(|agent| {
        agent.pane_id.as_deref() == Some(pane_id) && agent.agent_status.trim() == "working"
    })
}

fn agent_list_failure_reply(error: &str) -> String {
    format!("refused: agent.list failed: {error}")
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
    use super::{
        agent_list_failure_reply, has_prompt_content, is_thread_channel, pane_status_is_working,
        prompt_surface_markers, resolve_prompt_pane,
    };
    use crate::AgentSnapshot;
    use serde_json::Value;
    use twilight_model::channel::ChannelType;

    #[test]
    fn empty_prompt_content_is_refused_before_herdr() {
        let cases = [("", false), ("   \n\t", false), ("prompt", true)];
        for (content, expected) in cases {
            assert_eq!(has_prompt_content(content), expected, "content={content:?}");
        }
    }

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
                prompt_surface_markers(thread_name, topic).is_some(),
                expected
            );
        }
    }

    #[test]
    fn agent_list_failure_is_refused_in_a_qualifying_thread() {
        let cases = [
            (
                "herdr RPC connect failed: no socket",
                "refused: agent.list failed: herdr RPC connect failed: no socket",
            ),
            (
                "agent.list response did not contain agents",
                "refused: agent.list failed: agent.list response did not contain agents",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(agent_list_failure_reply(error), expected);
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
        let workspace_id = captured[0]
            .workspace_id
            .as_deref()
            .expect("captured agent has a workspace id");
        let tab_id = captured[0]
            .tab_id
            .as_deref()
            .expect("captured agent has a tab id");
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
        let mut ambiguous = vec![captured[0].clone()];
        ambiguous.push(captured[0].clone());
        let cases = [
            (
                "no matching pane",
                "missing-tab",
                workspace_id,
                vec![captured[0].clone()],
                "refused: unmapped pane",
            ),
            (
                "ambiguous pane",
                tab_id,
                workspace_id,
                ambiguous,
                "refused: ambiguous pane mapping",
            ),
            (
                "matching pane has no pane id",
                tab_id,
                workspace_id,
                vec![missing_pane],
                "refused: unmapped pane",
            ),
            (
                "working pane",
                tab_id,
                workspace_id,
                vec![working],
                "refused: agent state is working",
            ),
            (
                "blocked pane",
                tab_id,
                workspace_id,
                vec![blocked],
                "refused: agent state is blocked",
            ),
            (
                "missing agent status",
                tab_id,
                workspace_id,
                vec![no_status],
                "refused: agent state is unknown",
            ),
            (
                "unrecognized agent status",
                tab_id,
                workspace_id,
                vec![unknown],
                "refused: agent state is paused",
            ),
        ];
        for (branch, tab_id, workspace_id, agents, expected) in cases {
            assert_eq!(
                resolve_prompt_pane(tab_id, workspace_id, &agents),
                Err(expected.to_owned()),
                "branch={branch}"
            );
        }
    }

    #[test]
    fn pane_status_is_working_captured_snapshot_branches() {
        let value: Value =
            serde_json::from_str(include_str!("../tests/fixtures/herdr-agent-list.json"))
                .expect("captured agent snapshot is JSON");
        let captured: Vec<AgentSnapshot> =
            serde_json::from_value(value["result"]["agents"].clone())
                .expect("captured agent snapshot has the expected shape");
        let pane_id = captured[0]
            .pane_id
            .as_deref()
            .expect("captured agent has a pane id")
            .to_owned();
        let mut working = captured[0].clone();
        working.agent_status = "working".to_owned();
        let mut idle = captured[0].clone();
        idle.agent_status = "idle".to_owned();
        let cases = [
            ("working pane matches", vec![working], true),
            ("idle pane does not match", vec![idle], false),
            ("no matching pane", vec![], false),
        ];
        for (branch, agents, expected) in cases {
            assert_eq!(
                pane_status_is_working(&agents, &pane_id),
                expected,
                "branch={branch}"
            );
        }
    }
}
