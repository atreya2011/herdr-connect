use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use twilight_http::Client;
use twilight_model::application::interaction::{
    Interaction as DiscordInteraction, InteractionData,
};
use twilight_model::channel::message::MessageFlags;
use twilight_model::http::interaction::{
    InteractionResponse, InteractionResponseData, InteractionResponseType,
};
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, MessageMarker},
};

use crate::activity::{ACTIVITY_KIND, ActivityFrame};
use crate::delivery::expire_permission_card;
use crate::herdr::{STATUS_BLOCKED, agent_read_detection, agent_send_keys, pane_send_text};
use crate::permission::{Decision, DecisionBehavior, Interaction};
use crate::question::{Answer, AnswerStep, Question, answer_steps, dialog_shows_question};
use crate::registry::{
    ApprovalRequest, InteractionRegistry, PendingQuestion, QuestionRegistry, ResolveError,
    generate_token,
};
use crate::{
    TopologyCache, bridge_eprintln, deliver_permission_card, deliver_question_card,
    expire_informational_card, fetch_topology_lists, list_agents, route_topology, sync_topology,
    tab_list_result, update_question_card,
};

const MAX_FRAME_BYTES: usize = 64 * 1024;
const PERMISSION_TIMEOUT: Duration = Duration::from_secs(45);
const INITIAL_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
/// How long, and how often, to poll for the dialog to show the next question after an answer.
const NEXT_QUESTION_WAIT: Duration = Duration::from_secs(5);
const NEXT_QUESTION_POLL: Duration = Duration::from_millis(100);
/// Key pacing for typing into Claude's dialog. In a live probe a number key pressed within about a
/// second of the dialog appearing was ignored, and keys sent back to back missed toggles. With a
/// 2 s settle before the first input and 1 s between inputs, `1`, `3`, `right`, `enter` toggled
/// Cheese and Mushrooms and submitted both.
const DIALOG_SETTLE: Duration = Duration::from_secs(2);
const INPUT_GAP: Duration = Duration::from_secs(1);
const FAILED_CARD_TEXT: &str = "failed: answer the question in the terminal";
const NOT_ADVANCED_CARD_TEXT: &str =
    "resolved: answer sent, but the next question did not appear; answer it in the terminal";
#[must_use]
pub const fn hook_timeout() -> Duration {
    Duration::from_secs(PERMISSION_TIMEOUT.as_secs() + 5)
}

pub struct PermissionResponder {
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: String,
    registry: Arc<InteractionRegistry>,
    question_registry: QuestionRegistry,
    topology_cache: TopologyCache,
}

/// What came of an answer to a question card.
#[derive(Debug, Eq, PartialEq)]
pub enum QuestionOutcome {
    /// The answer was typed into the pane's dialog.
    Sent,
    /// The owner answered in the terminal: nothing was typed, and the card was retired, or moved
    /// to the call's next question if the dialog is on it.
    AnsweredInTerminal,
    /// The answer was typed, but the dialog did not show the call's next question in time, so the
    /// card was retired.
    SentNotAdvanced,
    /// No open card has this token.
    Unknown,
}

#[derive(Clone)]
struct HookLiveness {
    alive: Arc<AtomicBool>,
    closed: Arc<Notify>,
}

impl HookLiveness {
    fn new() -> Self {
        Self {
            alive: Arc::new(AtomicBool::new(true)),
            closed: Arc::new(Notify::new()),
        }
    }

    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }

    async fn wait_closed(&self) {
        let notified = self.closed.notified();
        if self.is_alive() {
            notified.await;
        }
    }
}

fn spawn_hook_monitor<R>(mut reader: R, liveness: HookLiveness) -> tokio::task::JoinHandle<()>
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tokio::spawn(async move {
        let mut buffer = [0_u8; 1024];
        while reader.read(&mut buffer).await.is_ok_and(|read| read > 0) {}
        liveness.alive.store(false, Ordering::Release);
        liveness.closed.notify_one();
    })
}

/// Returns `value` immediately, running `card_edit` to completion in a detached task: a caller
/// (the hook) never waits on the card's final edit, only on the decision or answer itself.
fn return_value_before_card_edit<T, F>(value: Option<T>, card_edit: F) -> Option<T>
where
    T: Send + 'static,
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    std::mem::drop(tokio::spawn(async move {
        if let Err(error) = card_edit.await {
            bridge_eprintln!("card edit failed: {error}");
        }
    }));
    value
}

impl PermissionResponder {
    #[must_use]
    pub fn new(
        client: Arc<Client>,
        guild: Id<GuildMarker>,
        owner_id: String,
        topology_cache: TopologyCache,
    ) -> Self {
        Self {
            client,
            guild,
            owner_id,
            registry: Arc::new(InteractionRegistry::default()),
            question_registry: QuestionRegistry::default(),
            topology_cache,
        }
    }

    #[must_use]
    pub fn has_pending_session(&self, session_id: &str) -> bool {
        self.registry.has_pending_session(session_id)
    }

    /// Posts the question card for the dialog Claude shows in `pane_id`, which is on
    /// `questions[index]`: one button per option for a single-select question, a select menu for a
    /// multiSelect question.
    ///
    /// # Errors
    ///
    /// Returns token-generation or Discord delivery errors.
    pub async fn deliver_question_card(
        &self,
        channel: Id<ChannelMarker>,
        pane_id: &str,
        questions: Vec<Question>,
        index: usize,
    ) -> Result<Id<MessageMarker>, String> {
        let token = generate_token()?;
        let message = deliver_question_card(
            self.client.as_ref(),
            channel,
            &questions[index],
            &token,
            &self.owner_id,
        )
        .await?;
        self.question_registry.insert(
            token,
            PendingQuestion {
                channel,
                message,
                pane_id: pane_id.to_owned(),
                questions,
                index,
            },
        );
        Ok(message)
    }

    /// Stops accepting answers for the question card `message`, once its card is retired.
    pub fn forget_question_card(&self, message: Id<MessageMarker>) {
        self.question_registry.forget_message(message);
    }

    /// The token of the open question card in `channel`, the one a thread reply there answers.
    #[must_use]
    pub fn pending_question_token(&self, channel: Id<ChannelMarker>) -> Option<String> {
        self.question_registry.token_in_channel(channel)
    }

    /// Types `answer` into the dialog of the pane behind question card `token`.
    ///
    /// The pane is re-read first: it must still be `blocked` and still show the card's question.
    /// Otherwise the owner answered in the terminal, so nothing is sent and the card is edited to
    /// say so, or to the call's next question if the dialog already shows it. When the answered question is not the last of its call, the same card is then edited
    /// in place to the next question, once the dialog shows it.
    ///
    /// A card whose pane could not be read keeps its token, so the owner can tap it again. A card
    /// whose typing or advance failed is edited to a failure text instead of keeping its buttons.
    ///
    /// # Errors
    ///
    /// Returns Herdr or Discord errors, or a refusal of free text for a multiSelect question.
    pub async fn answer_question(
        &self,
        token: &str,
        answer: &Answer,
    ) -> Result<QuestionOutcome, String> {
        let Some(pending) = self.question_registry.get(token) else {
            return Ok(QuestionOutcome::Unknown);
        };
        let steps = answer_steps(
            pending.question(),
            pending.index,
            pending.questions.len(),
            answer,
        )?;
        if self.question_registry.take(token).is_none() {
            return Ok(QuestionOutcome::Unknown);
        }
        let (pane_id, questions, from) = (
            pending.pane_id.clone(),
            pending.questions.clone(),
            pending.index,
        );
        let shown = blocking(move || shown_question_index(&pane_id, &questions, from)).await;
        match shown {
            Ok(Some(index)) if index == pending.index => {}
            Ok(Some(index)) => {
                // The owner answered this question in the terminal and the dialog moved on.
                self.move_card_to(&pending, index).await?;
                return Ok(QuestionOutcome::AnsweredInTerminal);
            }
            Ok(None) => {
                self.edit_card(&pending, "resolved: answered in the terminal")
                    .await?;
                return Ok(QuestionOutcome::AnsweredInTerminal);
            }
            Err(error) => {
                self.question_registry.insert(token.to_owned(), pending);
                return Err(error);
            }
        }
        let pane_id = pending.pane_id.clone();
        if let Err(error) = blocking(move || type_steps(&pane_id, &steps)).await {
            self.fail_card(&pending).await;
            return Err(error);
        }
        if pending.is_last() {
            return Ok(QuestionOutcome::Sent);
        }
        match self.advance_card(&pending).await {
            Ok(true) => Ok(QuestionOutcome::Sent),
            Ok(false) => {
                self.edit_card(&pending, NOT_ADVANCED_CARD_TEXT).await?;
                Ok(QuestionOutcome::SentNotAdvanced)
            }
            Err(error) => {
                self.fail_card(&pending).await;
                Err(error)
            }
        }
    }

    /// Waits for the dialog to show the call's next question and edits the card in place to it.
    /// Returns `false` when the dialog never showed it.
    async fn advance_card(&self, pending: &PendingQuestion) -> Result<bool, String> {
        let next = pending.index + 1;
        let (pane_id, question) = (pending.pane_id.clone(), pending.questions[next].clone());
        if !blocking(move || wait_for_dialog(&pane_id, &question)).await? {
            return Ok(false);
        }
        self.move_card_to(pending, next).await?;
        Ok(true)
    }

    /// Edits the card in place to `questions[index]` under a new token.
    async fn move_card_to(&self, pending: &PendingQuestion, index: usize) -> Result<(), String> {
        let next = PendingQuestion {
            index,
            ..pending.clone()
        };
        let token = generate_token()?;
        update_question_card(
            self.client.as_ref(),
            next.channel,
            next.message,
            next.question(),
            &token,
            &self.owner_id,
        )
        .await?;
        self.question_registry.insert(token, next);
        Ok(())
    }

    async fn edit_card(&self, pending: &PendingQuestion, content: &str) -> Result<(), String> {
        expire_informational_card(
            self.client.as_ref(),
            pending.channel,
            pending.message,
            content,
        )
        .await
    }

    /// Retires a card whose answer failed partway, so it does not keep live-looking buttons.
    async fn fail_card(&self, pending: &PendingQuestion) {
        if let Err(error) = self.edit_card(pending, FAILED_CARD_TEXT).await {
            bridge_eprintln!("question card edit failed: {error}");
        }
    }

    #[must_use]
    pub const fn topology_cache(&self) -> &TopologyCache {
        &self.topology_cache
    }

    async fn request(&self, interaction: &Interaction, liveness: HookLiveness) -> Option<Decision> {
        let route = self.route(&interaction.session_id, &liveness).await?;
        let channel = self.sync_channel(&route, &liveness).await?;
        let created_at = std::time::Instant::now();
        let issued = self
            .registry
            .issue_with_liveness(
                ApprovalRequest {
                    channel_id: channel.get(),
                    session_id: interaction.session_id.clone(),
                },
                created_at + PERMISSION_TIMEOUT,
                Arc::clone(&liveness.alive),
            )
            .ok()?;
        let message = self
            .deliver_card(
                channel,
                interaction.vendor,
                &interaction.tool_name,
                &interaction.tool_input.command,
                &issued.token,
                &liveness,
            )
            .await?;
        let token = issued.token.clone();
        let decision = Self::wait_decision(
            issued.receiver,
            &liveness,
            tokio::time::sleep(
                issued
                    .expiry
                    .saturating_duration_since(std::time::Instant::now()),
            ),
        )
        .await;
        let decision = decision.filter(|_| liveness.is_alive());
        let card_text = match decision.as_ref().map(|decision| &decision.behavior) {
            Some(DecisionBehavior::Allow) => "resolved: allowed",
            Some(DecisionBehavior::Deny) => "resolved: denied",
            None => {
                self.registry.remove(&token);
                if liveness.is_alive() {
                    "expired: no owner decision"
                } else {
                    "expired: hook disconnected"
                }
            }
        };
        let client = Arc::clone(&self.client);
        return_value_before_card_edit(decision, async move {
            expire_permission_card(client.as_ref(), channel, message, &token, card_text).await
        })
    }

    async fn route(
        &self,
        session_id: &str,
        liveness: &HookLiveness,
    ) -> Option<crate::TopologyRoute> {
        let session_id = session_id.to_owned();
        let route_task = tokio::task::spawn_blocking(move || {
            let agents = list_agents()?;
            let tabs = tab_list_result()?;
            let matches: Vec<_> = agents
                .iter()
                .filter(|agent| {
                    agent
                        .session
                        .as_ref()
                        .is_some_and(|session| session.value == session_id)
                })
                .collect();
            let agent = match matches.as_slice() {
                [agent] => *agent,
                [] => return Err("permission request has no mapped pane".to_owned()),
                [_first, _second, ..] => {
                    return Err("permission request has ambiguous pane mapping".to_owned());
                }
            };
            route_topology(&agents, &tabs, &agent.terminal_id)
        });
        let route = tokio::select! {
            result = route_task => result.ok().and_then(|result| match result {
                Ok(route) => Some(route),
                Err(error) => {
                    bridge_eprintln!("{error}");
                    None
                }
            }),
            () = liveness.wait_closed() => None,
        }?;
        liveness.is_alive().then_some(route)
    }

    async fn sync_channel(
        &self,
        route: &crate::TopologyRoute,
        liveness: &HookLiveness,
    ) -> Option<twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>> {
        let channel_task = Self::sync_channel_with_fresh_lists(
            self.client.as_ref(),
            self.guild,
            &self.topology_cache,
            route,
        );
        let channel = tokio::select! {
            result = channel_task => result.map_err(|error| {
                bridge_eprintln!("{error}");
                error
            }).ok(),
            () = liveness.wait_closed() => None,
        }?;
        liveness.is_alive().then_some(channel)
    }

    async fn sync_channel_with_fresh_lists(
        client: &Client,
        guild: Id<GuildMarker>,
        topology_cache: &TopologyCache,
        route: &crate::TopologyRoute,
    ) -> Result<twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>, String> {
        let fetched = fetch_topology_lists(client, guild).await?;
        let mut guard = topology_cache.lock().await;
        let (channels, active_threads) = crate::reconcile_topology_cache(&mut guard, fetched);
        sync_topology(client, guild, channels, active_threads, route).await
    }

    async fn deliver_card(
        &self,
        channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
        vendor: crate::permission::PermissionVendor,
        tool: &str,
        command: &str,
        token: &str,
        liveness: &HookLiveness,
    ) -> Option<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>> {
        let delivery =
            deliver_permission_card(self.client.as_ref(), channel, vendor, tool, command, token);
        tokio::pin!(delivery);
        tokio::select! {
            result = &mut delivery => {
                let message = match result {
                    Ok(message) => message,
                    Err(error) => {
                        bridge_eprintln!("permission card delivery failed: {error}");
                        self.registry.remove(token);
                        return None;
                    }
                };
                if liveness.is_alive() {
                    Some(message)
                } else {
                    self.expire_card(channel, message, token, "expired: hook disconnected").await;
                    None
                }
            }
            () = liveness.wait_closed() => {
                let message = delivery.await.ok();
                if let Some(message) = message {
                    self.expire_card(channel, message, token, "expired: hook disconnected").await;
                } else {
                    self.registry.remove(token);
                }
                None
            }
        }
    }

    async fn wait_decision(
        receiver: oneshot::Receiver<Decision>,
        liveness: &HookLiveness,
        expiry: tokio::time::Sleep,
    ) -> Option<Decision> {
        tokio::pin!(expiry);
        tokio::select! {
            biased;
            result = receiver => result.ok(),
            () = liveness.wait_closed() => None,
            () = &mut expiry => None,
        }
    }

    async fn expire_card(
        &self,
        channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
        message: twilight_model::id::Id<twilight_model::id::marker::MessageMarker>,
        token: &str,
        content: &str,
    ) {
        self.registry.remove(token);
        let _ =
            expire_permission_card(self.client.as_ref(), channel, message, token, content).await;
    }
}

/// Runs a blocking Herdr call off the async runtime.
async fn blocking<T: Send + 'static>(
    call: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    tokio::task::spawn_blocking(call)
        .await
        .map_err(|error| format!("question answer task failed: {error}"))?
}

/// The index, from `from` on, of the first of `questions` the pane's dialog shows while the pane
/// is `blocked`.
fn shown_question_index(
    pane_id: &str,
    questions: &[Question],
    from: usize,
) -> Result<Option<usize>, String> {
    let blocked = list_agents()?
        .iter()
        .any(|agent| agent.pane_id == pane_id && agent.agent_status == STATUS_BLOCKED);
    if !blocked {
        return Ok(None);
    }
    let detection = agent_read_detection(pane_id)?;
    Ok((from..questions.len()).find(|&index| dialog_shows_question(&detection, &questions[index])))
}

/// Polls until the pane's dialog shows `question`, for at most the next-question wait; `false`
/// when it never did.
fn wait_for_dialog(pane_id: &str, question: &Question) -> Result<bool, String> {
    let deadline = Instant::now() + NEXT_QUESTION_WAIT;
    while !dialog_shows_question(&agent_read_detection(pane_id)?, question) {
        if Instant::now() >= deadline {
            return Ok(false);
        }
        std::thread::sleep(NEXT_QUESTION_POLL);
    }
    Ok(true)
}

fn type_steps(pane_id: &str, steps: &[AnswerStep]) -> Result<(), String> {
    std::thread::sleep(DIALOG_SETTLE);
    let mut first = true;
    for step in steps {
        let inputs = match step {
            AnswerStep::Keys(keys) => keys.iter().map(|key| (true, key.as_str())).collect(),
            AnswerStep::Text(text) => vec![(false, text.as_str())],
        };
        for (is_key, input) in inputs {
            if !first {
                std::thread::sleep(INPUT_GAP);
            }
            first = false;
            if is_key {
                agent_send_keys(pane_id, &[input])?;
            } else {
                pane_send_text(pane_id, input)?;
            }
        }
    }
    Ok(())
}

/// Acknowledges a question tap at once, as an ephemeral deferred reply, because typing the answer
/// takes longer than the three seconds Discord allows for a response, then fills the reply in.
async fn answer_question_component(
    responder: &PermissionResponder,
    interaction: &DiscordInteraction,
    token: &str,
    indices: Vec<usize>,
) {
    let interactions = responder.client.interaction(interaction.application_id);
    let deferral = InteractionResponse {
        kind: InteractionResponseType::DeferredChannelMessageWithSource,
        data: Some(InteractionResponseData {
            flags: Some(MessageFlags::EPHEMERAL),
            ..InteractionResponseData::default()
        }),
    };
    if let Err(error) = interactions
        .create_response(interaction.id, &interaction.token, &deferral)
        .await
    {
        bridge_eprintln!("interaction response failed: {error}");
    }
    let reply = match responder
        .answer_question(token, &Answer::Options(indices))
        .await
    {
        Ok(QuestionOutcome::Sent) => "answer sent",
        Ok(QuestionOutcome::AnsweredInTerminal) => "answered in the terminal",
        Ok(QuestionOutcome::SentNotAdvanced) => {
            "answer sent, but the next question did not appear; answer it in the terminal"
        }
        Ok(QuestionOutcome::Unknown) => "expired",
        Err(error) => {
            bridge_eprintln!("question answer failed: {error}");
            "answer failed; see the bridge log"
        }
    };
    let update = interactions
        .update_response(&interaction.token)
        .content(Some(reply))
        .await;
    if let Err(error) = update {
        bridge_eprintln!("interaction reply failed: {error}");
    }
}

pub async fn handle_component(
    responder: Arc<PermissionResponder>,
    interaction: DiscordInteraction,
) {
    let Some(InteractionData::MessageComponent(data)) = interaction.data.as_ref() else {
        return;
    };
    let Some(channel) = interaction.channel.as_ref() else {
        return;
    };
    let authorized = interaction.guild_id == Some(responder.guild)
        && interaction
            .author_id()
            .is_some_and(|id| id.to_string() == responder.owner_id);
    let response = if !authorized {
        Some(ephemeral_response("not authorized"))
    } else if let Some(token) = data.custom_id.strip_prefix("herdrask-multi:") {
        let indices: Option<Vec<usize>> =
            data.values.iter().map(|value| value.parse().ok()).collect();
        let Some(indices) = indices else {
            return;
        };
        answer_question_component(&responder, &interaction, token, indices).await;
        return;
    } else if let Some(rest) = data.custom_id.strip_prefix("herdrask:") {
        let Some((token, index)) = rest
            .split_once(':')
            .and_then(|(token, index)| Some((token, index.parse().ok()?)))
        else {
            return;
        };
        answer_question_component(&responder, &interaction, token, vec![index]).await;
        return;
    } else if let Some((action, token)) = data
        .custom_id
        .split_once(':')
        .and_then(|(prefix, rest)| prefix.strip_prefix("herdr").map(|_| rest))
        .and_then(|rest| rest.split_once(':'))
    {
        permission_component_response(&responder, action, token, channel.id.get())
    } else {
        return;
    };
    let Some(response) = response else {
        return;
    };
    if let Err(error) = responder
        .client
        .interaction(interaction.application_id)
        .create_response(interaction.id, &interaction.token, &response)
        .await
    {
        bridge_eprintln!("interaction response failed: {error}");
    }
}

fn permission_component_response(
    responder: &PermissionResponder,
    action: &str,
    token: &str,
    channel_id: u64,
) -> Option<InteractionResponse> {
    let decision = match action {
        "allow" => Decision::allow(),
        "deny" => Decision::deny("operator denied this request".to_owned()),
        _ => return None,
    };
    Some(
        match responder
            .registry
            .resolve(token, channel_id, decision, std::time::Instant::now())
        {
            Ok(()) => ephemeral_response("decision recorded"),
            Err(ResolveError::UnknownOrExpired | ResolveError::WrongChannel) => {
                ephemeral_response("expired")
            }
        },
    )
}

fn ephemeral_response(content: &str) -> InteractionResponse {
    InteractionResponse {
        kind: InteractionResponseType::ChannelMessageWithSource,
        data: Some(InteractionResponseData {
            content: Some(content.to_owned()),
            flags: Some(MessageFlags::EPHEMERAL),
            ..InteractionResponseData::default()
        }),
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct RequestKey {
    session_id: String,
    prompt_id: String,
}

impl From<&Interaction> for RequestKey {
    fn from(interaction: &Interaction) -> Self {
        Self {
            session_id: interaction.session_id.clone(),
            prompt_id: interaction.prompt_id.clone(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct BrokerResponse {
    pub session_id: String,
    pub prompt_id: String,
    pub decision: Decision,
}

#[derive(Debug, Eq, PartialEq)]
enum CorrelationError {
    MismatchedRequest,
}

/// Accepts a broker response only when its session and prompt match the request.
///
/// # Errors
///
/// Returns `MismatchedRequest` for a response belonging to another or stale request.
fn correlate_decision(
    interaction: &Interaction,
    response: BrokerResponse,
) -> Result<Decision, CorrelationError> {
    if RequestKey::from(interaction)
        != (RequestKey {
            session_id: response.session_id,
            prompt_id: response.prompt_id,
        })
    {
        return Err(CorrelationError::MismatchedRequest);
    }
    Ok(response.decision)
}

pub async fn request_decision(
    interaction: &Interaction,
    socket_path: &Path,
    timeout_duration: Duration,
) -> Option<Decision> {
    tokio::time::timeout(timeout_duration, async {
        let mut stream = match UnixStream::connect(socket_path).await {
            Ok(stream) => stream,
            Err(error) => {
                bridge_eprintln!(
                    "broker request failed: connect to {}: {error}",
                    socket_path.display()
                );
                return None;
            }
        };
        write_json_line(&mut stream, interaction).await.ok()?;
        let response: BrokerResponse = read_json_line(&mut stream).await.ok()?;
        correlate_decision(interaction, response).ok()
    })
    .await
    .ok()
    .flatten()
}

/// Sends one activity frame to the broker socket and returns without waiting for a reply.
///
/// A connect failure, write failure, or timeout is silently discarded, matching the activity
/// hook's fire-and-forget contract.
pub async fn send_activity_frame(
    frame: &ActivityFrame,
    socket_path: &Path,
    connect_timeout: Duration,
) {
    let _ = tokio::time::timeout(connect_timeout, async {
        let mut stream = UnixStream::connect(socket_path).await.map_err(|_| ())?;
        write_json_line(&mut stream, frame).await.map_err(|_| ())
    })
    .await;
}

#[derive(Default)]
struct PendingRequests {
    fingerprints: Mutex<HashSet<RequestFingerprint>>,
}

impl PendingRequests {
    async fn register(&self, fingerprint: RequestFingerprint) -> bool {
        self.fingerprints.lock().await.insert(fingerprint)
    }

    async fn remove(&self, fingerprint: &RequestFingerprint) {
        self.fingerprints.lock().await.remove(fingerprint);
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RequestFingerprint {
    request: RequestKey,
    tool_name: String,
    command: String,
    description: String,
}

impl From<&Interaction> for RequestFingerprint {
    fn from(interaction: &Interaction) -> Self {
        Self {
            request: RequestKey::from(interaction),
            tool_name: interaction.tool_name.clone(),
            command: interaction.tool_input.command.clone(),
            description: interaction.tool_input.description.clone(),
        }
    }
}

/// Serves broker requests from an already-bound Unix listener until shutdown is signaled.
///
/// # Errors
///
/// Returns an I/O error when accepting a client connection fails.
async fn serve_broker(
    listener: UnixListener,
    mut shutdown: oneshot::Receiver<()>,
    responder: Arc<PermissionResponder>,
    activity_tx: mpsc::UnboundedSender<ActivityFrame>,
) -> io::Result<()> {
    let pending = Arc::new(PendingRequests::default());
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let pending = Arc::clone(&pending);
                let responder = Arc::clone(&responder);
                let activity_tx = activity_tx.clone();
                tokio::spawn(async move {
                    handle_connection(stream, pending, responder, activity_tx).await;
                });
            }
            _ = &mut shutdown => break,
        }
    }
    Ok(())
}

/// Binds and runs the Discord-backed permission broker until Ctrl-C or SIGTERM.
///
/// Every accepted activity frame is forwarded on `activity_tx`; a caller with no bridge to
/// forward to may pass a sender whose receiver it has already dropped.
///
/// # Errors
///
/// Returns an I/O error when the socket cannot be bound or the listener fails.
pub async fn run_broker(
    socket_path: &Path,
    responder: Arc<PermissionResponder>,
    activity_tx: mpsc::UnboundedSender<ActivityFrame>,
) -> io::Result<()> {
    remove_stale_socket(socket_path)?;
    let listener = UnixListener::bind(socket_path)?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let shutdown_task = tokio::spawn(async move {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
        let _ = shutdown_tx.send(());
    });
    let result = serve_broker(listener, shutdown_rx, responder, activity_tx).await;
    shutdown_task.abort();
    let _ = std::fs::remove_file(socket_path);
    result
}

fn remove_stale_socket(socket_path: &Path) -> io::Result<()> {
    let metadata = match std::fs::symlink_metadata(socket_path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !std::os::unix::fs::FileTypeExt::is_socket(&metadata.file_type()) {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "broker socket path is not a socket: {}",
                socket_path.display()
            ),
        ));
    }
    match std::os::unix::net::UnixStream::connect(socket_path) {
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::AddrInUse,
            format!("broker socket is already in use: {}", socket_path.display()),
        )),
        Err(_) => std::fs::remove_file(socket_path),
    }
}

async fn handle_connection(
    mut stream: UnixStream,
    pending: Arc<PendingRequests>,
    responder: Arc<PermissionResponder>,
    activity_tx: mpsc::UnboundedSender<ActivityFrame>,
) {
    let bytes = match tokio::time::timeout(INITIAL_FRAME_TIMEOUT, read_json_line_bytes(&mut stream))
        .await
    {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => {
            bridge_eprintln!("broker rejected initial frame: {error}");
            return;
        }
        Err(_) => {
            bridge_eprintln!("broker rejected initial frame: initial frame read timed out");
            return;
        }
    };
    if is_activity_frame(&bytes) {
        match serde_json::from_slice::<ActivityFrame>(&bytes) {
            Ok(frame) => {
                let _ = activity_tx.send(frame);
            }
            Err(error) => {
                bridge_eprintln!(
                    "broker rejected initial frame: malformed activity frame: {error}"
                );
            }
        }
        return;
    }
    let interaction = match serde_json::from_slice::<Interaction>(&bytes) {
        Ok(interaction) if is_valid_interaction(&interaction) => interaction,
        Ok(_) => return,
        Err(error) => {
            bridge_eprintln!("broker rejected initial frame: malformed broker frame: {error}");
            return;
        }
    };
    let (read_half, mut write_half) = stream.into_split();
    let liveness = HookLiveness::new();
    let monitor = spawn_hook_monitor(read_half, liveness.clone());
    let fingerprint = RequestFingerprint::from(&interaction);
    if !pending.register(fingerprint.clone()).await {
        monitor.abort();
        return;
    }

    let decision = responder.request(&interaction, liveness).await;
    if let Some(decision) = decision {
        let response = BrokerResponse {
            session_id: interaction.session_id.clone(),
            prompt_id: interaction.prompt_id.clone(),
            decision,
        };
        let _ = write_json_line(&mut write_half, &response).await;
    }
    monitor.abort();
    pending.remove(&fingerprint).await;
}

const fn is_valid_interaction(interaction: &Interaction) -> bool {
    !interaction.session_id.is_empty()
        && !interaction.prompt_id.is_empty()
        && !interaction.tool_name.is_empty()
        && !interaction.tool_input.command.is_empty()
}

/// Whether a raw initial frame names itself an activity frame, ahead of a typed decode: the
/// broker socket carries both permission [`Interaction`] frames (untagged) and [`ActivityFrame`]
/// frames (tagged `"kind":"activity"`), and this is the only way to tell them apart before
/// choosing which type to deserialize into.
fn is_activity_frame(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| {
            value
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .as_deref()
        == Some(ACTIVITY_KIND)
}

async fn read_json_line_bytes(stream: &mut UnixStream) -> Result<Vec<u8>, String> {
    let mut reader = BufReader::new(stream).take((MAX_FRAME_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    let length = reader
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(|error| error.to_string())?;
    if length == 0 {
        return Err("empty broker frame".to_owned());
    }
    if length > MAX_FRAME_BYTES {
        return Err("oversized broker frame".to_owned());
    }
    if bytes.last() != Some(&b'\n') {
        return Err("incomplete broker frame".to_owned());
    }
    bytes.pop();
    if bytes.is_empty() {
        return Err("empty broker frame".to_owned());
    }
    Ok(bytes)
}

async fn read_json_line<T>(stream: &mut UnixStream) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
    let bytes = read_json_line_bytes(stream).await?;
    serde_json::from_slice(&bytes).map_err(|error| format!("malformed broker frame: {error}"))
}

async fn write_json_line<T, W>(stream: &mut W, value: &T) -> Result<(), String>
where
    T: Serialize + Sync,
    W: AsyncWrite + Unpin,
{
    let mut bytes = serde_json::to_vec(value).map_err(|error| error.to_string())?;
    bytes.push(b'\n');
    stream
        .write_all(&bytes)
        .await
        .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixStream;
    use tokio::sync::oneshot;

    use super::{
        BrokerResponse, HookLiveness, PERMISSION_TIMEOUT, PendingRequests, PermissionResponder,
        RequestFingerprint, correlate_decision, hook_timeout, read_json_line,
        return_value_before_card_edit, spawn_hook_monitor,
    };
    use crate::permission::{ClaudePermissionToolInput, Decision, Interaction, PermissionVendor};
    use crate::registry::ApprovalRequest;

    fn interaction() -> Interaction {
        Interaction {
            session_id: "session".to_owned(),
            prompt_id: "prompt".to_owned(),
            tool_name: "Bash".to_owned(),
            tool_input: ClaudePermissionToolInput {
                command: "touch proof".to_owned(),
                description: "Create proof".to_owned(),
            },
            vendor: PermissionVendor::Claude,
        }
    }

    #[test]
    fn permission_hook_margin_matches_the_permission_timeout() {
        assert_eq!(PERMISSION_TIMEOUT, Duration::from_secs(45));
        assert_eq!(hook_timeout(), Duration::from_secs(50));
    }

    #[test]
    fn mismatched_session_or_prompt_is_rejected() {
        let request = interaction();
        let mut response = BrokerResponse {
            session_id: request.session_id.clone(),
            prompt_id: request.prompt_id.clone(),
            decision: crate::permission::Decision::allow(),
        };
        for (session_id, prompt_id) in [("other", "prompt"), ("session", "other")] {
            response.session_id = session_id.to_owned();
            response.prompt_id = prompt_id.to_owned();
            assert!(correlate_decision(&request, response).is_err());
            response = BrokerResponse {
                session_id: request.session_id.clone(),
                prompt_id: request.prompt_id.clone(),
                decision: crate::permission::Decision::allow(),
            };
        }
    }

    #[tokio::test]
    async fn disconnected_hook_ends_wait_before_broker_timeout_window() {
        let (client, server) = UnixStream::pair().expect("create hook socket pair");
        let (read_half, _write_half) = server.into_split();
        let liveness = HookLiveness::new();
        let monitor = spawn_hook_monitor(read_half, liveness.clone());
        drop(client);

        tokio::time::timeout(
            PERMISSION_TIMEOUT.min(Duration::from_millis(100)),
            liveness.wait_closed(),
        )
        .await
        .expect("hook EOF must preempt the broker timeout window");
        monitor.abort();
    }

    #[tokio::test]
    async fn pending_requests_allow_distinct_tool_calls_and_reject_replays() {
        let pending = PendingRequests::default();
        let first = interaction();
        let mut second = interaction();
        second.tool_input.command = "touch other-proof".to_owned();
        second.tool_input.description = "Create other proof".to_owned();
        let first_fingerprint = RequestFingerprint::from(&first);
        let second_fingerprint = RequestFingerprint::from(&second);

        assert!(pending.register(first_fingerprint.clone()).await);
        assert!(!pending.register(first_fingerprint.clone()).await);
        assert!(pending.register(second_fingerprint).await);
        pending.remove(&first_fingerprint).await;
    }

    #[tokio::test]
    async fn malformed_response_is_rejected() {
        let (mut writer, mut reader) = UnixStream::pair().expect("create Unix stream pair");
        writer
            .write_all(b"not json\n")
            .await
            .expect("write malformed response");
        assert!(read_json_line::<BrokerResponse>(&mut reader).await.is_err());
    }

    #[tokio::test]
    async fn resolved_decision_wins_expiry_race() {
        for _ in 0..128 {
            let (sender, receiver) = oneshot::channel();
            sender
                .send(Decision::allow())
                .expect("send resolved decision");
            let expiry = tokio::time::sleep(Duration::ZERO);
            tokio::time::sleep(Duration::from_millis(2)).await;
            assert_eq!(
                PermissionResponder::wait_decision(receiver, &HookLiveness::new(), expiry).await,
                Some(Decision::allow())
            );
        }
    }

    #[tokio::test]
    async fn resolved_decision_does_not_wait_for_card_edit() {
        let edit_started = Arc::new(AtomicBool::new(false));
        let edit_started_by_task = Arc::clone(&edit_started);
        let decision = return_value_before_card_edit(Some(Decision::allow()), async move {
            edit_started_by_task.store(true, Ordering::Release);
            std::future::pending::<Result<(), String>>().await
        });
        assert_eq!(decision, Some(Decision::allow()));
        assert!(!edit_started.load(Ordering::Acquire));
        tokio::task::yield_now().await;
        assert!(edit_started.load(Ordering::Acquire));
    }

    use super::permission_component_response;
    use std::time::Instant;
    use twilight_http::Client;
    use twilight_model::http::interaction::InteractionResponse;
    use twilight_model::id::Id;

    fn test_responder() -> PermissionResponder {
        PermissionResponder::new(
            Arc::new(Client::builder().token("test-token".to_owned()).build()),
            Id::new(1),
            "owner-id".to_owned(),
            Arc::new(tokio::sync::Mutex::new(None)),
        )
    }

    fn response_content(response: Option<InteractionResponse>) -> String {
        response
            .expect("component dispatch replies")
            .data
            .and_then(|data| data.content)
            .expect("ephemeral reply has content")
    }

    #[tokio::test]
    async fn permission_component_dispatch_records_allow_and_rejects_unknown_actions() {
        let responder = test_responder();
        let now = Instant::now();
        let issued = responder
            .registry
            .issue_with_liveness(
                ApprovalRequest {
                    channel_id: 7,
                    session_id: "session".to_owned(),
                },
                now + Duration::from_secs(30),
                Arc::new(AtomicBool::new(true)),
            )
            .expect("issue token");

        let unknown = permission_component_response(&responder, "snooze", &issued.token, 7);
        assert_eq!(unknown, None);
        let allowed = permission_component_response(&responder, "allow", &issued.token, 7);
        assert_eq!(response_content(allowed), "decision recorded");
        let replay = permission_component_response(&responder, "allow", &issued.token, 7);
        assert_eq!(response_content(replay), "expired");
    }
}
