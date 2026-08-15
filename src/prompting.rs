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
        reply(&client, &message, "refused: unmapped Discord thread").await?;
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
        reply(&client, &message, "refused: unmapped Discord thread").await?;
        return Ok(());
    };
    let Some(topic) = parent.topic.as_deref() else {
        reply(&client, &message, "refused: unmapped Discord channel").await?;
        return Ok(());
    };
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
fn is_thread_channel(kind: ChannelType) -> bool {
    kind.is_thread()
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
    use super::is_thread_channel;
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
}
