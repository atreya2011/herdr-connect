//! Owner-message routing is restricted to mapped Discord threads.
//!
//! The owner-authored end-to-end path is deferred to the orchestrator's live proof because REST
//! message creation responses do not carry the guild identifier required by the gateway handler.

use std::collections::HashSet;
use std::future::Future;
use std::hash::BuildHasher;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};
use std::time::{Duration, Instant};

use twilight_http::Client;
use twilight_model::{
    channel::{ChannelType, Message},
    id::{
        Id,
        marker::{ChannelMarker, GuildMarker},
    },
};

use crate::broker::{PermissionResponder, QuestionOutcome};
use crate::herdr::{
    PROMPT_ACKNOWLEDGED_UNCONFIRMED, STATUS_BLOCKED, STATUS_DONE, STATUS_IDLE, STATUS_WORKING,
    agent_prompt, agent_send_keys,
};
use crate::question::Answer;
use crate::{AgentSnapshot, list_agents};

const PROMPT_ACCEPTED_REPLY: &str = "accepted: prompt submitted; Herdr state may be unconfirmed";
const TYPING_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(8);
const STALL_RECOVERY_POLL_BOUND: Duration = Duration::from_secs(5);
const STALL_RECOVERY_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// One pending "the bridge itself submitted this text to this pane" marker: `(pane_id, text)`.
type OwnerPromptSuppression = (String, String);

/// Prompts the bridge submitted to Herdr on the owner's behalf, not yet observed back in the
/// pane's vendor log. Consulted by every vendor's terminal-prompt mirroring so a prompt the owner
/// typed in Discord is never mirrored back into the same thread it came from. A prompt queued
/// behind a running turn reaches the log only when the agent starts it, so a marker lives until it
/// is consumed or its pane departs ([`forget_departed_owner_prompt_suppressions`]).
static OWNER_PROMPT_SUPPRESSIONS: LazyLock<Mutex<Vec<OwnerPromptSuppression>>> =
    LazyLock::new(|| Mutex::new(Vec::new()));

/// Handles one Discord owner message after gateway-level filtering.
///
/// A thread reply while a question card is open in that thread is typed into the pane's dialog as
/// its free-text answer instead of being submitted as an `agent.prompt`: the pane is `blocked`
/// then, and [`resolve_prompt_pane`] would refuse it.
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
    let agent = match matching_agent(tab_id, workspace_id, &agents) {
        Ok(agent) => agent,
        Err(reason) => {
            reply(&client, &message, &reason).await?;
            return Ok(());
        }
    };
    if let Some(response) = answer_pending_question(responder, &message).await {
        reply(&client, &message, &response).await?;
        return Ok(());
    }
    let pane_id = match resolve_prompt_pane(agent) {
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

/// Types `message` into the dialog of the question card open in its thread and returns the reply to
/// post; `None` when no card is open there.
async fn answer_pending_question(
    responder: &PermissionResponder,
    message: &Message,
) -> Option<String> {
    let token = responder.pending_question_token(message.channel_id)?;
    let answer = Answer::Text(message.content.clone());
    Some(match responder.answer_question(&token, &answer).await {
        Ok(QuestionOutcome::Sent) => "accepted: answer typed into the dialog".to_owned(),
        Ok(QuestionOutcome::AnsweredInTerminal) => {
            "refused: the question was already answered in the terminal".to_owned()
        }
        Ok(QuestionOutcome::Unknown) => "refused: the question card is no longer open".to_owned(),
        Err(error) => format!("refused: typing the answer failed: {error}"),
    })
}

#[must_use]
fn has_prompt_content(content: &str) -> bool {
    !content.trim().is_empty()
}

#[must_use]
pub fn should_handle_owner_message(author_id: &str, is_bot: bool, owner_id: &str) -> bool {
    !is_bot && author_id == owner_id
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
/// [`handle_owner_message`] (which must not gate on status).
fn matching_agent<'agents>(
    tab_id: &str,
    workspace_id: &str,
    agents: &'agents [AgentSnapshot],
) -> Result<&'agents AgentSnapshot, String> {
    let matches: Vec<&AgentSnapshot> = agents
        .iter()
        .filter(|agent| agent.tab_id == tab_id)
        .filter(|agent| agent.workspace_id == workspace_id)
        .collect();
    match matches.as_slice() {
        [] => Err("refused: unmapped pane".to_owned()),
        [_first, _second, ..] => Err("refused: ambiguous pane mapping".to_owned()),
        [agent] => Ok(agent),
    }
}

fn resolve_prompt_pane(agent: &AgentSnapshot) -> Result<String, String> {
    let pane_id = agent.pane_id.as_str();
    match agent.agent_status.trim() {
        STATUS_IDLE | STATUS_DONE | STATUS_WORKING => Ok(pane_id.to_owned()),
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
/// marker is withdrawn only when the first `agent.prompt` fails, since only then the pane never
/// received the text; once it was accepted, a later recovery failure keeps the marker because the
/// text may already be submitted.
///
/// A pane that reports a stalled submission (the composer received the text but never actually
/// submitted it) is recovered with a two-rung ladder: an Enter key press first, since that alone
/// submits a paste-block-stuck composer; if the pane still has not left `idle` shortly after,
/// a Ctrl+U clear followed by one fresh `agent.prompt` resubmission. The ladder runs only for a
/// pane that was `idle` or `done` at submission: the agent itself queues a prompt submitted
/// mid-turn, where a stalled prompt cannot be told from a queued one and Ctrl+U would clear the
/// queued text, and a `blocked` pane would take the Enter key press as an answer to its dialog.
///
/// # Errors
///
/// Returns Herdr submission, follow-up key press, or pane-state errors.
pub fn submit_owner_prompt(target: &str, text: &str) -> Result<String, String> {
    let status_at_submission = list_agents()?
        .iter()
        .find(|agent| agent.pane_id == target)
        .map(|agent| agent.agent_status.trim().to_owned())
        .unwrap_or_default();
    record_owner_prompt_submission(target, text);
    let result = agent_prompt(target, text);
    if result.is_err() {
        forget_owner_prompt_submission(target, text);
        return result;
    }
    if !stall_recovery_applies(&status_at_submission, &result) {
        return result;
    }
    agent_send_keys(target, &["enter"])?;
    if pane_left_idle(target, STALL_RECOVERY_POLL_BOUND)? {
        return result;
    }
    agent_send_keys(target, &["ctrl+u"])?;
    agent_prompt(target, text)
}

/// Whether a submission needs the stall-recovery ladder: the pane was `idle` or `done` when the
/// prompt went in and Herdr could not confirm the pane started working. Any other status leaves
/// the pane alone: a `working` pane queues the prompt itself, so Ctrl+U would clear the queued
/// text, and a `blocked` pane shows a dialog that an Enter key press would answer.
fn stall_recovery_applies(status_at_submission: &str, result: &Result<String, String>) -> bool {
    matches!(status_at_submission, STATUS_IDLE | STATUS_DONE)
        && result.as_deref() == Ok(PROMPT_ACKNOWLEDGED_UNCONFIRMED)
}

fn record_owner_prompt_submission(pane_id: &str, text: &str) {
    let mut suppressions = OWNER_PROMPT_SUPPRESSIONS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    suppressions.push((pane_id.to_owned(), text.to_owned()));
}

/// Drops every suppression marker whose pane is absent from `current_panes`.
pub fn forget_departed_owner_prompt_suppressions(
    current_panes: &HashSet<String, impl BuildHasher>,
) {
    let mut suppressions = OWNER_PROMPT_SUPPRESSIONS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    retain_present_panes(&mut suppressions, current_panes);
}

fn retain_present_panes(
    suppressions: &mut Vec<OwnerPromptSuppression>,
    current_panes: &HashSet<String, impl BuildHasher>,
) {
    suppressions.retain(|(pane, _)| current_panes.contains(pane));
}

fn forget_owner_prompt_submission(pane_id: &str, text: &str) {
    let mut suppressions = OWNER_PROMPT_SUPPRESSIONS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    if let Some(index) = suppressions
        .iter()
        .position(|(pane, expected)| pane == pane_id && expected == text)
    {
        suppressions.swap_remove(index);
    }
}

/// Consumes the suppression marker for `(pane_id, text)`, if one is pending.
///
/// `true` means this exact text was submitted by the bridge itself and must not be mirrored; the
/// marker is cleared either way it matches, so a later, unrelated prompt with the same text
/// mirrors normally.
#[must_use]
pub fn take_owner_prompt_suppression(pane_id: &str, text: &str) -> bool {
    let mut suppressions = OWNER_PROMPT_SUPPRESSIONS
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let Some(index) = suppressions
        .iter()
        .position(|(pane, expected)| pane == pane_id && expected == text)
    else {
        return false;
    };
    suppressions.swap_remove(index);
    true
}

/// Polls `list_agents` for up to `bound`, returning true as soon as `target`'s pane is observed
/// with an `agent_status` other than `idle`, or false when `bound` elapses first.
///
/// # Errors
///
/// Returns the `agent.list` error: with the pane's state unknown, the caller must not clear and
/// resubmit.
fn pane_left_idle(target: &str, bound: Duration) -> Result<bool, String> {
    let start = Instant::now();
    loop {
        let left_idle = list_agents()?
            .iter()
            .any(|agent| agent.pane_id == target && agent.agent_status.trim() != STATUS_IDLE);
        if left_idle {
            return Ok(true);
        }
        if start.elapsed() >= bound {
            return Ok(false);
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
    agents
        .iter()
        .any(|agent| agent.pane_id == pane_id && agent.agent_status.trim() == STATUS_WORKING)
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

    use std::collections::HashSet;

    use super::{
        OWNER_PROMPT_SUPPRESSIONS, PROMPT_ACKNOWLEDGED_UNCONFIRMED, agent_list_failure_reply,
        has_prompt_content, is_thread_channel, matching_agent, pane_status_is_working,
        prompt_surface_markers, resolve_prompt_pane, retain_present_panes, stall_recovery_applies,
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
    fn stall_recovery_runs_only_for_an_unconfirmed_prompt_to_an_idle_or_done_pane() {
        let unconfirmed = Ok(PROMPT_ACKNOWLEDGED_UNCONFIRMED.to_owned());
        let confirmed = Ok("acknowledged".to_owned());
        let failed = Err("agent.prompt failed".to_owned());
        let cases = [
            ("idle", &unconfirmed, true),
            ("done", &unconfirmed, true),
            ("working", &unconfirmed, false),
            ("blocked", &unconfirmed, false),
            ("unknown", &unconfirmed, false),
            ("", &unconfirmed, false),
            ("idle", &confirmed, false),
            ("done", &failed, false),
        ];
        for (status, result, expected) in cases {
            assert_eq!(
                stall_recovery_applies(status, result),
                expected,
                "{status:?} {result:?}"
            );
        }
    }

    #[test]
    fn owner_prompt_suppression_is_consumed_exactly_once() {
        let pane_id = "test-pane-suppression-once";
        let text = "typed once";
        OWNER_PROMPT_SUPPRESSIONS
            .lock()
            .expect("lock owner prompt suppression markers")
            .push((pane_id.to_owned(), text.to_owned()));
        assert!(take_owner_prompt_suppression(pane_id, text));
        assert!(!take_owner_prompt_suppression(pane_id, text));
    }

    #[test]
    fn owner_prompt_suppression_is_dropped_when_its_pane_departs() {
        let text = "queued behind a long turn";
        let mut suppressions = vec![
            ("departed".to_owned(), text.to_owned()),
            ("present".to_owned(), text.to_owned()),
        ];
        retain_present_panes(&mut suppressions, &HashSet::from(["present".to_owned()]));
        assert_eq!(suppressions, [("present".to_owned(), text.to_owned())]);
    }

    #[test]
    fn owner_prompt_suppression_ignores_a_different_pane_or_text() {
        let pane_id = "test-pane-suppression-mismatch";
        let text = "typed for this pane";
        OWNER_PROMPT_SUPPRESSIONS
            .lock()
            .expect("lock owner prompt suppression markers")
            .push((pane_id.to_owned(), text.to_owned()));
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
        let workspace_id = captured[0].workspace_id.as_str();
        let tab_id = captured[0].tab_id.as_str();
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
                matching_agent(tab_id, workspace_id, &agents).and_then(resolve_prompt_pane),
                Err(expected.to_owned()),
                "branch={branch}"
            );
        }
    }

    /// A `working` pane accepts the prompt: the agent queues it and answers after the current turn.
    #[test]
    fn resolve_prompt_pane_accepts_a_working_pane() {
        let value: Value =
            serde_json::from_str(include_str!("../tests/fixtures/herdr-agent-list.json"))
                .expect("captured agent snapshot is JSON");
        let captured: Vec<AgentSnapshot> =
            serde_json::from_value(value["result"]["agents"].clone())
                .expect("captured agent snapshot has the expected shape");
        let mut working = captured[0].clone();
        working.agent_status = "working".to_owned();
        let pane_id = working.pane_id.clone();
        assert_eq!(
            matching_agent(&working.tab_id, &working.workspace_id, &[working.clone()])
                .and_then(resolve_prompt_pane),
            Ok(pane_id)
        );
    }

    /// Unlike [`resolve_prompt_pane`], `matching_agent` resolves a pane whatever its status: it
    /// backs the pending-question check, which has to find a pane's session while a question is
    /// pending.
    #[test]
    fn matching_agent_ignores_status_unlike_resolve_prompt_pane() {
        let value: Value =
            serde_json::from_str(include_str!("../tests/fixtures/herdr-agent-list.json"))
                .expect("captured agent snapshot is JSON");
        let captured: Vec<AgentSnapshot> =
            serde_json::from_value(value["result"]["agents"].clone())
                .expect("captured agent snapshot has the expected shape");
        let workspace_id = captured[0].workspace_id.clone();
        let tab_id = captured[0].tab_id.clone();
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
        let pane_id = captured[0].pane_id.clone();
        let mut working = captured[0].clone();
        working.agent_status = "working".to_owned();
        let mut idle = captured[0].clone();
        idle.agent_status = "idle".to_owned();
        let mut other_pane_working = captured[0].clone();
        other_pane_working.pane_id = "other-pane".to_owned();
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
