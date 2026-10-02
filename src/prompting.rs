//! Owner-message routing is restricted to mapped Discord threads.
//!
//! The owner-authored end-to-end path is deferred to the orchestrator's live proof because REST
//! message creation responses do not carry the guild identifier required by the gateway handler.

use std::future::Future;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use twilight_http::Client;
use twilight_model::{
    channel::{ChannelType, Message},
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker},
    },
};

use crate::broker::PermissionResponder;
use crate::herdr::{
    PROMPT_ACKNOWLEDGED_UNCONFIRMED, STATUS_BLOCKED, STATUS_DONE, STATUS_IDLE, STATUS_WORKING,
    agent_prompt, agent_send_keys,
};
use crate::registry::ResolveError;
use crate::{AgentSnapshot, list_agents};

const PROMPT_ACCEPTED_REPLY: &str = "accepted: prompt submitted; Herdr state may be unconfirmed";
const QUESTION_ANSWER_ACCEPTED_REPLY: &str = "accepted: answer recorded";
const QUESTION_ANSWER_STALE_REPLY: &str = "refused: the question already resolved or expired";
const TYPING_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(8);
const STALL_RECOVERY_POLL_BOUND: Duration = Duration::from_secs(5);
const STALL_RECOVERY_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// How long a Discord-submitted prompt stays eligible to suppress its own terminal-log mirror.
/// Bounds a submission whose pane never actually echoes it (Herdr rejected it, the pane closed)
/// from leaking into the next unrelated prompt on the same pane that happens to share its text.
const OWNER_PROMPT_SUPPRESSION_TTL: Duration = Duration::from_secs(30);

/// One pending "the bridge itself submitted this text to this pane" marker: `(pane_id, text,
/// expires_at)`.
type OwnerPromptSuppression = (String, String, Instant);

/// Prompts the bridge submitted to Herdr on the owner's behalf, not yet observed back in the
/// pane's vendor log. Consulted by every vendor's terminal-prompt mirroring so a prompt the owner
/// typed in Discord is never mirrored back into the same thread it came from.
static OWNER_PROMPT_SUPPRESSIONS: LazyLock<Mutex<Vec<OwnerPromptSuppression>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Handles one Discord owner message after gateway-level filtering.
///
/// A thread reply while any question card -- single-select or multiSelect -- is pending for the
/// mapped pane's session is consumed as that question's free-text answer instead of being
/// submitted as an `agent.prompt` (the pane's Herdr status is `working`, not `idle`/`done`, while a
/// question is pending, so this check runs before -- not through -- [`resolve_prompt_pane`]'s
/// status gate).
///
/// # Errors
///
/// Returns Discord, Herdr, or task-dispatch errors.
pub async fn handle_owner_message(
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: &str,
    message: Message,
    responder: &PermissionResponder,
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
    if let Ok(agent) = matching_agent(tab_id, workspace_id, &agents)
        && let Some(session_id) = agent.session.as_ref().map(|session| session.value.as_str())
        && let Some(token) = responder.pending_question_token(session_id)
    {
        let response = match responder.resolve_question_text(
            &token,
            message.channel_id.get(),
            &message.content,
        ) {
            Ok(()) => QUESTION_ANSWER_ACCEPTED_REPLY,
            Err(ResolveError::UnknownOrExpired | ResolveError::WrongChannel) => {
                QUESTION_ANSWER_STALE_REPLY
            }
        };
        reply(&client, &message, response).await?;
        return Ok(());
    }
    let pane_id = match resolve_prompt_pane(tab_id, workspace_id, &agents) {
        Ok(target) => target,
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
    let result = tokio::task::spawn_blocking(move || submit_owner_prompt(&prompt_pane, &text))
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

/// The one pane mapped to `tab_id`/`workspace_id`, independent of its Herdr status: shared by
/// [`resolve_prompt_pane`] (which additionally gates on status) and the pending-question check in
/// [`handle_owner_message`] (which must not, since a pane awaiting a question answer reports
/// `working`).
fn matching_agent<'agents>(
    tab_id: &str,
    workspace_id: &str,
    agents: &'agents [AgentSnapshot],
) -> Result<&'agents AgentSnapshot, String> {
    let matches: Vec<&AgentSnapshot> = agents
        .iter()
        .filter(|agent| agent.tab_id.as_deref() == Some(tab_id))
        .filter(|agent| agent.workspace_id.as_deref() == Some(workspace_id))
        .collect();
    match matches.as_slice() {
        [] => Err("refused: unmapped pane".to_owned()),
        [_first, _second, ..] => Err("refused: ambiguous pane mapping".to_owned()),
        [agent] => Ok(agent),
    }
}

fn resolve_prompt_pane(
    tab_id: &str,
    workspace_id: &str,
    agents: &[AgentSnapshot],
) -> Result<String, String> {
    let agent = matching_agent(tab_id, workspace_id, agents)?;
    let pane_id = agent
        .pane_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| "refused: unmapped pane".to_owned())?;
    match agent.agent_status.trim() {
        STATUS_IDLE | STATUS_DONE => Ok(pane_id.to_owned()),
        STATUS_WORKING => Err("refused: agent state is working".to_owned()),
        STATUS_BLOCKED => Err("refused: agent state is blocked".to_owned()),
        "" => Err("refused: agent state is unknown".to_owned()),
        state => Err(format!("refused: agent state is {state}")),
    }
}

/// Submits an owner prompt to a Herdr agent pane.
///
/// Records a suppression marker for `(target, text)` before submitting, so a terminal-prompt
/// mirror that observes this exact text land in the pane's vendor log drops it instead of
/// mirroring the bridge's own Discord-originated prompt back into the thread it came from. The
/// marker is withdrawn if submission ultimately fails, since the pane never received the text.
///
/// A pane that reports a stalled submission (the composer received the text but never actually
/// submitted it) is recovered with a two-rung ladder: an Enter key press first, since that alone
/// submits a paste-block-stuck composer; if the pane still has not left `idle` shortly after,
/// a Ctrl+U clear followed by one fresh `agent.prompt` resubmission.
///
/// # Errors
///
/// Returns Herdr submission or follow-up key press errors.
pub fn submit_owner_prompt(target: &str, text: &str) -> Result<String, String> {
    record_owner_prompt_submission(target, text);
    let result = (|| {
        let result = agent_prompt(target, text);
        if result.as_deref() != Ok(PROMPT_ACKNOWLEDGED_UNCONFIRMED) {
            return result;
        }
        agent_send_keys(target, &["enter"])?;
        if pane_left_idle(target, STALL_RECOVERY_POLL_BOUND) {
            return result;
        }
        agent_send_keys(target, &["ctrl+u"])?;
        agent_prompt(target, text)
    })();
    if result.is_err() {
        forget_owner_prompt_submission(target, text);
    }
    result
}

fn record_owner_prompt_submission(pane_id: &str, text: &str) {
    let Ok(mut suppressions) = OWNER_PROMPT_SUPPRESSIONS.lock() else {
        return;
    };
    let now = Instant::now();
    suppressions.retain(|(_, _, expires)| *expires > now);
    suppressions.push((
        pane_id.to_owned(),
        text.to_owned(),
        now + OWNER_PROMPT_SUPPRESSION_TTL,
    ));
}

fn forget_owner_prompt_submission(pane_id: &str, text: &str) {
    let Ok(mut suppressions) = OWNER_PROMPT_SUPPRESSIONS.lock() else {
        return;
    };
    if let Some(index) = suppressions
        .iter()
        .position(|(pane, expected, _)| pane == pane_id && expected == text)
    {
        suppressions.swap_remove(index);
    }
}

/// Consumes the suppression marker for `(pane_id, text)`, if one is still pending.
///
/// `true` means this exact text was submitted by the bridge itself and must not be mirrored; the
/// marker is cleared either way it matches, so a later, unrelated prompt with the same text
/// mirrors normally.
#[must_use]
pub fn take_owner_prompt_suppression(pane_id: &str, text: &str) -> bool {
    let Ok(mut suppressions) = OWNER_PROMPT_SUPPRESSIONS.lock() else {
        return false;
    };
    let now = Instant::now();
    suppressions.retain(|(_, _, expires)| *expires > now);
    let Some(index) = suppressions
        .iter()
        .position(|(pane, expected, _)| pane == pane_id && expected == text)
    else {
        return false;
    };
    suppressions.swap_remove(index);
    true
}

/// Polls `list_agents` for up to `bound`, returning true as soon as `target`'s pane is observed
/// with an `agent_status` other than `idle`.
fn pane_left_idle(target: &str, bound: Duration) -> bool {
    let start = Instant::now();
    loop {
        let left_idle = list_agents().is_ok_and(|agents| {
            agents.iter().any(|agent| {
                agent.pane_id.as_deref() == Some(target) && agent.agent_status.trim() != STATUS_IDLE
            })
        });
        if left_idle {
            return true;
        }
        if start.elapsed() >= bound {
            return false;
        }
        std::thread::sleep(STALL_RECOVERY_POLL_INTERVAL);
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
        agent.pane_id.as_deref() == Some(pane_id) && agent.agent_status.trim() == STATUS_WORKING
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
    use serde_json::Value;
    use twilight_model::channel::ChannelType;

    use std::time::{Duration, Instant};

    use super::{
        OWNER_PROMPT_SUPPRESSIONS, agent_list_failure_reply, has_prompt_content, is_thread_channel,
        matching_agent, pane_status_is_working, prompt_surface_markers, resolve_prompt_pane,
        take_owner_prompt_suppression,
    };
    use crate::AgentSnapshot;

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
        let cases = [(
            "herdr RPC connect failed: no socket",
            "refused: agent.list failed: herdr RPC connect failed: no socket",
        )];
        for (error, expected) in cases {
            assert_eq!(agent_list_failure_reply(error), expected);
        }
    }

    #[test]
    fn owner_prompt_suppression_is_consumed_exactly_once() {
        let pane_id = "test-pane-suppression-once";
        let text = "typed once";
        OWNER_PROMPT_SUPPRESSIONS
            .lock()
            .expect("lock owner prompt suppression markers")
            .push((
                pane_id.to_owned(),
                text.to_owned(),
                Instant::now() + Duration::from_secs(30),
            ));
        assert!(take_owner_prompt_suppression(pane_id, text));
        assert!(!take_owner_prompt_suppression(pane_id, text));
    }

    #[test]
    fn owner_prompt_suppression_ignores_a_different_pane_or_text() {
        let pane_id = "test-pane-suppression-mismatch";
        let text = "typed for this pane";
        OWNER_PROMPT_SUPPRESSIONS
            .lock()
            .expect("lock owner prompt suppression markers")
            .push((
                pane_id.to_owned(),
                text.to_owned(),
                Instant::now() + Duration::from_secs(30),
            ));
        assert!(!take_owner_prompt_suppression("other-pane", text));
        assert!(!take_owner_prompt_suppression(pane_id, "different text"));
        assert!(take_owner_prompt_suppression(pane_id, text));
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

    /// Unlike [`resolve_prompt_pane`], `matching_agent` must resolve a `working` or `blocked` pane:
    /// it backs the pending-question check, which has to find a pane's session while a question is
    /// pending and the pane therefore reports `working`, not `idle`/`done`.
    #[test]
    fn matching_agent_ignores_status_unlike_resolve_prompt_pane() {
        let value: Value =
            serde_json::from_str(include_str!("../tests/fixtures/herdr-agent-list.json"))
                .expect("captured agent snapshot is JSON");
        let captured: Vec<AgentSnapshot> =
            serde_json::from_value(value["result"]["agents"].clone())
                .expect("captured agent snapshot has the expected shape");
        let workspace_id = captured[0]
            .workspace_id
            .as_deref()
            .expect("captured agent has a workspace id")
            .to_owned();
        let tab_id = captured[0]
            .tab_id
            .as_deref()
            .expect("captured agent has a tab id")
            .to_owned();
        let mut working = captured[0].clone();
        working.agent_status = "working".to_owned();
        let pane_id = working.pane_id.clone();

        let resolved = matching_agent(&tab_id, &workspace_id, std::slice::from_ref(&working))
            .expect("a working pane is still resolvable by matching_agent");
        assert_eq!(resolved.pane_id, pane_id);
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
        let mut other_pane_working = captured[0].clone();
        other_pane_working.pane_id = Some("other-pane".to_owned());
        other_pane_working.agent_status = "working".to_owned();
        let cases = [
            ("working pane matches", vec![working], true),
            ("another pane is working", vec![other_pane_working], false),
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
