use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

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
use twilight_model::id::{Id, marker::GuildMarker};

use crate::activity::{ACTIVITY_KIND, ActivityFrame};
use crate::delivery::{
    MAX_QUESTION_CARD_CONTENT_LENGTH, expire_permission_card, truncate_with_ellipsis,
};
use crate::permission::{Decision, DecisionBehavior, Interaction, PermissionVendor};
use crate::question::{
    QUESTION_KIND, Question, QuestionAnswer, QuestionInteraction, format_question_answer,
};
use crate::registry::{ApprovalRequest, InteractionRegistry, QuestionRegistry, ResolveError};
use crate::{
    TopologyCache, bridge_eprintln, deliver_permission_card, deliver_question_button_card,
    deliver_question_select_card, expire_question_button_card, expire_question_select_card,
    fetch_topology_lists, list_agents, route_topology, sync_topology, tab_list_result,
};

const MAX_FRAME_BYTES: usize = 64 * 1024;
const PERMISSION_TIMEOUT: Duration = Duration::from_secs(45);
const CURSOR_PERMISSION_TIMEOUT: Duration = PERMISSION_TIMEOUT;
const INITIAL_FRAME_TIMEOUT: Duration = Duration::from_secs(10);
/// How long one question card stays open for an owner answer before the hook falls through to
/// Claude's own dialog.
const QUESTION_TIMEOUT: Duration = Duration::from_secs(30);

#[must_use]
pub const fn hook_timeout() -> Duration {
    Duration::from_secs(PERMISSION_TIMEOUT.as_secs() + 5)
}

/// The `AskUserQuestion` hook's own margin over [`QUESTION_TIMEOUT`], mirroring [`hook_timeout`]'s
/// margin over `PERMISSION_TIMEOUT`.
#[must_use]
pub const fn question_hook_timeout() -> Duration {
    Duration::from_secs(QUESTION_TIMEOUT.as_secs() + 5)
}

/// One `AskUserQuestion` request forwarded to the broker over the shared socket, tagged the same
/// way [`ActivityFrame`] is so [`handle_connection`] can tell frame kinds apart before choosing
/// which type to decode into.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct QuestionFrame {
    kind: String,
    session_id: String,
    request_id: String,
    questions: Vec<Question>,
}

impl From<&QuestionInteraction> for QuestionFrame {
    fn from(interaction: &QuestionInteraction) -> Self {
        Self {
            kind: QUESTION_KIND.to_owned(),
            session_id: interaction.session_id.clone(),
            request_id: interaction.request_id.clone(),
            questions: interaction.questions.clone(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct QuestionBrokerResponse {
    session_id: String,
    request_id: String,
    answers: BTreeMap<String, QuestionAnswer>,
}

/// Accepts a broker answer set only when its session and request id match the request.
///
/// # Errors
///
/// Returns `MismatchedRequest` for a response belonging to another or stale request.
fn correlate_question_answers(
    interaction: &QuestionInteraction,
    response: QuestionBrokerResponse,
) -> Result<BTreeMap<String, QuestionAnswer>, CorrelationError> {
    if interaction.session_id != response.session_id
        || interaction.request_id != response.request_id
    {
        return Err(CorrelationError::MismatchedRequest);
    }
    Ok(response.answers)
}

/// Sends one `AskUserQuestion` request to the broker and awaits its resolved answers.
///
/// Returns `None` on any connect failure, malformed or mismatched response, or timeout, matching
/// the hook's best-effort contract: the caller falls through to Claude's own dialog rather than
/// failing the tool call.
pub async fn request_question_answers(
    interaction: &QuestionInteraction,
    socket_path: &Path,
    timeout_duration: Duration,
) -> Option<BTreeMap<String, QuestionAnswer>> {
    let frame = QuestionFrame::from(interaction);
    tokio::time::timeout(timeout_duration, async {
        let mut stream = UnixStream::connect(socket_path).await.map_err(|_| ())?;
        write_json_line(&mut stream, &frame).await.map_err(|_| ())?;
        let response: QuestionBrokerResponse = read_json_line(&mut stream).await.map_err(|_| ())?;
        correlate_question_answers(interaction, response).map_err(|_| ())
    })
    .await
    .ok()
    .and_then(Result::ok)
}

pub struct PermissionResponder {
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: String,
    registry: Arc<InteractionRegistry>,
    question_registry: Arc<QuestionRegistry>,
    topology_cache: TopologyCache,
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
            question_registry: Arc::new(QuestionRegistry::default()),
            topology_cache,
        }
    }

    #[must_use]
    pub fn has_pending_session(&self, session_id: &str) -> bool {
        self.registry.has_pending_session(session_id)
    }

    /// The token of the pending question card for `session_id`, if one is open, single-select or
    /// multiSelect alike.
    #[must_use]
    pub fn pending_question_token(&self, session_id: &str) -> Option<String> {
        self.question_registry.pending_question_token(session_id)
    }

    /// Resolves the pending question card `token` with a free-text owner answer.
    ///
    /// # Errors
    ///
    /// Returns the rejection reason for an unknown, expired, wrong-channel, or already-resolved
    /// token -- the same race an owner tapping a now-stale button would hit.
    pub fn resolve_question_text(
        &self,
        token: &str,
        channel_id: u64,
        text: &str,
    ) -> Result<(), ResolveError> {
        self.question_registry.resolve(
            token,
            channel_id,
            QuestionAnswer::Single(text.to_owned()),
            std::time::Instant::now(),
        )
    }

    #[must_use]
    pub const fn topology_cache(&self) -> &TopologyCache {
        &self.topology_cache
    }

    async fn request(&self, interaction: &Interaction, liveness: HookLiveness) -> Option<Decision> {
        let route = self.route(&interaction.session_id, &liveness).await?;
        let channel = self.sync_channel(&route, &liveness).await?;
        let created_at = std::time::Instant::now();
        let permission_timeout = match interaction.vendor {
            PermissionVendor::Cursor => CURSOR_PERMISSION_TIMEOUT,
            PermissionVendor::Claude | PermissionVendor::Codex => PERMISSION_TIMEOUT,
        };
        let issued = self
            .registry
            .issue_with_liveness(
                ApprovalRequest {
                    channel_id: channel.get(),
                    session_id: interaction.session_id.clone(),
                },
                created_at,
                created_at + permission_timeout,
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
            route_topology(&agents, &tabs, &agent.terminal_id).map_err(String::from)
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
                let Some(message) = result.ok() else {
                    self.registry.remove(token);
                    return None;
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

    /// Answers one `AskUserQuestion` request by posting its questions as Discord cards, in order,
    /// and collecting the owner's resolved answers.
    ///
    /// The first question that expires, or whose card cannot be delivered, aborts the whole
    /// request: no further question in the same call gets a card, and the caller falls through to
    /// Claude's own dialog -- an `AskUserQuestion` call is answered in full or not at all.
    ///
    /// Every card in the call shares one `call_deadline` (`QUESTION_TIMEOUT` from the call's own
    /// start), not its own fresh `QUESTION_TIMEOUT` window: a multi-question call otherwise lets a
    /// later card outlive the hook's own `question_hook_timeout` margin over a single card's
    /// window, discarding an already-answered earlier question when the hook gives up.
    async fn request_question(
        &self,
        frame: &QuestionFrame,
        liveness: HookLiveness,
    ) -> Option<BTreeMap<String, QuestionAnswer>> {
        let route = self.route(&frame.session_id, &liveness).await?;
        let channel = self.sync_channel(&route, &liveness).await?;
        let call_deadline = std::time::Instant::now() + QUESTION_TIMEOUT;
        let mut answers = BTreeMap::new();
        for question in &frame.questions {
            let answer = self
                .request_one_question(
                    channel,
                    &frame.session_id,
                    question,
                    call_deadline,
                    &liveness,
                )
                .await?;
            answers.insert(question.question.clone(), answer);
        }
        Some(answers)
    }

    async fn request_one_question(
        &self,
        channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
        session_id: &str,
        question: &Question,
        call_deadline: std::time::Instant,
        liveness: &HookLiveness,
    ) -> Option<QuestionAnswer> {
        let created_at = std::time::Instant::now();
        let issued = self
            .question_registry
            .issue_with_liveness(
                ApprovalRequest {
                    channel_id: channel.get(),
                    session_id: session_id.to_owned(),
                },
                question.options.clone(),
                created_at,
                call_deadline,
                Arc::clone(&liveness.alive),
            )
            .ok()?;
        let message = self
            .deliver_question_card(channel, question, &issued.token, liveness)
            .await?;
        let token = issued.token.clone();
        let answer = Self::wait_question_answer(
            issued.receiver,
            liveness,
            tokio::time::sleep(
                issued
                    .expiry
                    .saturating_duration_since(std::time::Instant::now()),
            ),
        )
        .await;
        let answer = answer.filter(|_| liveness.is_alive());
        let card_text = answer.as_ref().map_or_else(
            || {
                self.question_registry.remove(&token);
                if liveness.is_alive() {
                    "expired: no owner answer".to_owned()
                } else {
                    "expired: hook disconnected".to_owned()
                }
            },
            |resolved| {
                truncate_with_ellipsis(
                    &format!("resolved: {}", format_question_answer(resolved)),
                    MAX_QUESTION_CARD_CONTENT_LENGTH,
                )
            },
        );
        let client = Arc::clone(&self.client);
        let options = question.options.clone();
        let multi_select = question.multi_select;
        return_value_before_card_edit(answer, async move {
            if multi_select {
                expire_question_select_card(
                    client.as_ref(),
                    channel,
                    message,
                    &options,
                    &token,
                    &card_text,
                )
                .await
            } else {
                expire_question_button_card(
                    client.as_ref(),
                    channel,
                    message,
                    &options,
                    &token,
                    &card_text,
                )
                .await
            }
        })
    }

    async fn deliver_question_card(
        &self,
        channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
        question: &Question,
        token: &str,
        liveness: &HookLiveness,
    ) -> Option<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>> {
        let delivery: std::pin::Pin<
            Box<
                dyn Future<
                        Output = Result<
                            twilight_model::id::Id<twilight_model::id::marker::MessageMarker>,
                            String,
                        >,
                    > + Send,
            >,
        > = if question.multi_select {
            Box::pin(deliver_question_select_card(
                self.client.as_ref(),
                channel,
                question,
                token,
            ))
        } else {
            Box::pin(deliver_question_button_card(
                self.client.as_ref(),
                channel,
                question,
                token,
            ))
        };
        tokio::pin!(delivery);
        tokio::select! {
            result = &mut delivery => {
                let message = match result {
                    Ok(message) => message,
                    Err(error) => {
                        bridge_eprintln!("question card delivery failed: {error}");
                        self.question_registry.remove(token);
                        return None;
                    }
                };
                if liveness.is_alive() {
                    Some(message)
                } else {
                    self.expire_question_card_disconnected(channel, message, question, token).await;
                    None
                }
            }
            () = liveness.wait_closed() => {
                let message = delivery.await.ok();
                if let Some(message) = message {
                    self.expire_question_card_disconnected(channel, message, question, token).await;
                } else {
                    self.question_registry.remove(token);
                }
                None
            }
        }
    }

    async fn wait_question_answer(
        receiver: oneshot::Receiver<QuestionAnswer>,
        liveness: &HookLiveness,
        expiry: tokio::time::Sleep,
    ) -> Option<QuestionAnswer> {
        tokio::pin!(expiry);
        tokio::select! {
            biased;
            result = receiver => result.ok(),
            () = liveness.wait_closed() => None,
            () = &mut expiry => None,
        }
    }

    async fn expire_question_card_disconnected(
        &self,
        channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
        message: twilight_model::id::Id<twilight_model::id::marker::MessageMarker>,
        question: &Question,
        token: &str,
    ) {
        self.question_registry.remove(token);
        let _ = if question.multi_select {
            expire_question_select_card(
                self.client.as_ref(),
                channel,
                message,
                &question.options,
                token,
                "expired: hook disconnected",
            )
            .await
        } else {
            expire_question_button_card(
                self.client.as_ref(),
                channel,
                message,
                &question.options,
                token,
                "expired: hook disconnected",
            )
            .await
        };
    }

    /// Resolves the pending single-select question card `token` with the option chosen by button
    /// tap, identified by its index among the card's own options.
    ///
    /// # Errors
    ///
    /// Returns the rejection reason for an unknown, expired, wrong-channel, already-resolved, or
    /// out-of-range option index.
    fn resolve_question_option(
        &self,
        token: &str,
        channel_id: u64,
        option_index: usize,
    ) -> Result<(), ResolveError> {
        let label = self
            .question_registry
            .option_label(token, option_index)
            .ok_or(ResolveError::UnknownOrExpired)?;
        self.question_registry.resolve(
            token,
            channel_id,
            QuestionAnswer::Single(label),
            std::time::Instant::now(),
        )
    }

    /// Resolves the pending multiSelect question card `token` with the options chosen through its
    /// select menu, identified by index among the card's own options.
    ///
    /// # Errors
    ///
    /// Returns the rejection reason for an unknown, expired, wrong-channel, already-resolved, or
    /// out-of-range option index.
    fn resolve_question_options(
        &self,
        token: &str,
        channel_id: u64,
        option_indices: &[usize],
    ) -> Result<(), ResolveError> {
        let labels = option_indices
            .iter()
            .map(|&index| self.question_registry.option_label(token, index))
            .collect::<Option<Vec<_>>>()
            .ok_or(ResolveError::UnknownOrExpired)?;
        self.question_registry.resolve(
            token,
            channel_id,
            QuestionAnswer::Multiple(labels),
            std::time::Instant::now(),
        )
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
    } else if let Some(rest) = data.custom_id.strip_prefix("herdrask-multi:") {
        question_select_response(&responder, rest, channel.id.get(), &data.values)
    } else if let Some(rest) = data.custom_id.strip_prefix("herdrask:") {
        question_button_response(&responder, rest, channel.id.get())
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
    let _ = responder
        .client
        .interaction(interaction.application_id)
        .create_response(interaction.id, &interaction.token, &response)
        .await;
}

fn permission_component_response(
    responder: &PermissionResponder,
    action: &str,
    token: &str,
    channel_id: u64,
) -> Option<InteractionResponse> {
    if !responder.registry.has_pending(token) {
        return Some(ephemeral_response("expired"));
    }
    let decision = match action {
        "allow" => Decision::allow(),
        "deny" => Decision::deny(Some("operator denied this request".to_owned())),
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

/// `token` is `"<token>:<option index>"`, the tail of a `herdrask:` single-select button's
/// `custom_id`.
fn question_button_response(
    responder: &PermissionResponder,
    token: &str,
    channel_id: u64,
) -> Option<InteractionResponse> {
    let (token, index) = token.split_once(':')?;
    let index: usize = index.parse().ok()?;
    Some(
        match responder.resolve_question_option(token, channel_id, index) {
            Ok(()) => ephemeral_response("answer recorded"),
            Err(ResolveError::UnknownOrExpired | ResolveError::WrongChannel) => {
                ephemeral_response("expired")
            }
        },
    )
}

fn question_select_response(
    responder: &PermissionResponder,
    token: &str,
    channel_id: u64,
    values: &[String],
) -> Option<InteractionResponse> {
    let indices: Option<Vec<usize>> = values.iter().map(|value| value.parse().ok()).collect();
    let indices = indices?;
    Some(
        match responder.resolve_question_options(token, channel_id, &indices) {
            Ok(()) => ephemeral_response("answer recorded"),
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
    let timeout_duration = match interaction.vendor {
        PermissionVendor::Cursor => timeout_duration.min(CURSOR_PERMISSION_TIMEOUT),
        PermissionVendor::Claude | PermissionVendor::Codex => timeout_duration,
    };
    tokio::time::timeout(timeout_duration, async {
        let mut stream = match UnixStream::connect(socket_path).await {
            Ok(stream) => stream,
            Err(error) => {
                bridge_eprintln!(
                    "broker request failed: connect to {}: {error}",
                    socket_path.display()
                );
                return Err(());
            }
        };
        write_json_line(&mut stream, interaction)
            .await
            .map_err(|_| ())?;
        let response: BrokerResponse = read_json_line(&mut stream).await.map_err(|_| ())?;
        correlate_decision(interaction, response).map_err(|_| ())
    })
    .await
    .ok()
    .and_then(Result::ok)
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
    state: Mutex<PendingState>,
}

impl PendingRequests {
    async fn register(&self, key: PendingKey) -> bool {
        let mut state = self.state.lock().await;
        if !state.fingerprints.insert(key.fingerprint.clone()) {
            return false;
        }
        state.keys.insert(key)
    }

    async fn remove(&self, key: &PendingKey) {
        let mut state = self.state.lock().await;
        state.keys.remove(key);
        state.fingerprints.remove(&key.fingerprint);
    }
}

#[derive(Default)]
struct PendingState {
    keys: HashSet<PendingKey>,
    fingerprints: HashSet<RequestFingerprint>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct PendingKey {
    fingerprint: RequestFingerprint,
    connection_id: u64,
}

impl PendingKey {
    fn new(interaction: &Interaction, connection_id: u64) -> Self {
        Self {
            fingerprint: RequestFingerprint::from(interaction),
            connection_id,
        }
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
    let next_connection_id = AtomicU64::new(0);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let pending = Arc::clone(&pending);
                let responder = Arc::clone(&responder);
                let activity_tx = activity_tx.clone();
                let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    handle_connection(stream, pending, responder, activity_tx, connection_id).await;
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
    connection_id: u64,
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
        if let Ok(frame) = serde_json::from_slice::<ActivityFrame>(&bytes) {
            let _ = activity_tx.send(frame);
        }
        return;
    }
    if is_question_frame(&bytes) {
        let Ok(frame) = serde_json::from_slice::<QuestionFrame>(&bytes) else {
            bridge_eprintln!("broker rejected initial frame: malformed question frame");
            return;
        };
        let (read_half, mut write_half) = stream.into_split();
        let liveness = HookLiveness::new();
        let monitor = spawn_hook_monitor(read_half, liveness.clone());
        let answers = responder.request_question(&frame, liveness).await;
        if let Some(answers) = answers {
            let response = QuestionBrokerResponse {
                session_id: frame.session_id.clone(),
                request_id: frame.request_id.clone(),
                answers,
            };
            let _ = write_json_line(&mut write_half, &response).await;
        }
        monitor.abort();
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
    let key = PendingKey::new(&interaction, connection_id);
    if !pending.register(key.clone()).await {
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
    pending.remove(&key).await;
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

/// Whether a raw initial frame names itself a question frame (tagged `"kind":"question"`), the
/// same way [`is_activity_frame`] recognizes an activity frame ahead of a typed decode.
fn is_question_frame(bytes: &[u8]) -> bool {
    serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|value| {
            value
                .get("kind")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .as_deref()
        == Some(QUESTION_KIND)
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
        BrokerResponse, CURSOR_PERMISSION_TIMEOUT, HookLiveness, PERMISSION_TIMEOUT, PendingKey,
        PendingRequests, PermissionResponder, QUESTION_TIMEOUT, QuestionBrokerResponse,
        correlate_decision, correlate_question_answers, hook_timeout, question_hook_timeout,
        read_json_line, return_value_before_card_edit, spawn_hook_monitor,
    };
    use crate::permission::{ClaudePermissionToolInput, Decision, Interaction, PermissionVendor};
    use crate::question::{Question, QuestionAnswer, QuestionInteraction, QuestionOption};
    use crate::registry::ApprovalRequest;
    use std::collections::BTreeMap;

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
    fn cursor_permission_window_matches_generic_hook_margin() {
        assert_eq!(CURSOR_PERMISSION_TIMEOUT, PERMISSION_TIMEOUT);
        assert_eq!(hook_timeout(), Duration::from_secs(50));
    }

    #[test]
    fn question_hook_margin_matches_the_question_timeout() {
        assert_eq!(QUESTION_TIMEOUT, Duration::from_secs(30));
        assert_eq!(question_hook_timeout(), Duration::from_secs(35));
    }

    fn question_interaction() -> QuestionInteraction {
        QuestionInteraction {
            session_id: "session".to_owned(),
            request_id: "toolu_1".to_owned(),
            questions: vec![Question {
                question: "Which color?".to_owned(),
                header: "Color".to_owned(),
                options: vec![
                    QuestionOption {
                        label: "Red".to_owned(),
                        description: "The color red".to_owned(),
                    },
                    QuestionOption {
                        label: "Blue".to_owned(),
                        description: "The color blue".to_owned(),
                    },
                ],
                multi_select: false,
            }],
            raw_tool_input: serde_json::json!({"questions": []}),
        }
    }

    #[test]
    fn mismatched_question_session_or_request_id_is_rejected() {
        let request = question_interaction();
        let answers = BTreeMap::from([(
            "Which color?".to_owned(),
            QuestionAnswer::Single("Blue".to_owned()),
        )]);
        for (session_id, request_id) in [("other", "toolu_1"), ("session", "other")] {
            let response = QuestionBrokerResponse {
                session_id: session_id.to_owned(),
                request_id: request_id.to_owned(),
                answers: answers.clone(),
            };
            assert!(correlate_question_answers(&request, response).is_err());
        }
        let response = QuestionBrokerResponse {
            session_id: request.session_id.clone(),
            request_id: request.request_id.clone(),
            answers: answers.clone(),
        };
        assert_eq!(correlate_question_answers(&request, response), Ok(answers));
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
        let first_key = PendingKey::new(&first, 1);
        let replay_key = PendingKey::new(&first, 2);
        let second_key = PendingKey::new(&second, 3);

        assert!(pending.register(first_key.clone()).await);
        assert!(!pending.register(replay_key).await);
        assert!(pending.register(second_key).await);
        pending.remove(&first_key).await;
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
            assert_eq!(
                PermissionResponder::wait_decision(
                    receiver,
                    &HookLiveness::new(),
                    tokio::time::sleep(Duration::ZERO),
                )
                .await,
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

    use super::{
        permission_component_response, question_button_response, question_select_response,
    };
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

    fn question_option(label: &str) -> QuestionOption {
        QuestionOption {
            label: label.to_owned(),
            description: String::new(),
        }
    }

    fn response_content(response: Option<InteractionResponse>) -> String {
        response
            .expect("component dispatch replies")
            .data
            .and_then(|data| data.content)
            .expect("ephemeral reply has content")
    }

    #[tokio::test]
    async fn question_button_tap_resolves_by_option_index_and_rejects_a_replay() {
        let responder = test_responder();
        let now = Instant::now();
        let issued = responder
            .question_registry
            .issue_with_liveness(
                ApprovalRequest {
                    channel_id: 7,
                    session_id: "session".to_owned(),
                },
                vec![question_option("Red"), question_option("Blue")],
                now,
                now + Duration::from_secs(30),
                Arc::new(AtomicBool::new(true)),
            )
            .expect("issue token");

        let response = question_button_response(&responder, &format!("{}:1", issued.token), 7);
        assert_eq!(response_content(response), "answer recorded");
        assert_eq!(
            issued.receiver.await,
            Ok(QuestionAnswer::Single("Blue".to_owned()))
        );

        let replay = question_button_response(&responder, &format!("{}:0", issued.token), 7);
        assert_eq!(response_content(replay), "expired");
    }

    #[tokio::test]
    async fn question_button_tap_rejects_the_wrong_channel_or_a_bad_index() {
        let responder = test_responder();
        let now = Instant::now();
        let issued = responder
            .question_registry
            .issue_with_liveness(
                ApprovalRequest {
                    channel_id: 7,
                    session_id: "session".to_owned(),
                },
                vec![question_option("Red")],
                now,
                now + Duration::from_secs(30),
                Arc::new(AtomicBool::new(true)),
            )
            .expect("issue token");

        let out_of_range = question_button_response(&responder, &format!("{}:9", issued.token), 7);
        assert_eq!(response_content(out_of_range), "expired");

        let wrong_channel = question_button_response(&responder, &format!("{}:0", issued.token), 8);
        assert_eq!(response_content(wrong_channel), "expired");
    }

    #[tokio::test]
    async fn question_select_tap_resolves_every_chosen_index() {
        let responder = test_responder();
        let now = Instant::now();
        let issued = responder
            .question_registry
            .issue_with_liveness(
                ApprovalRequest {
                    channel_id: 7,
                    session_id: "session".to_owned(),
                },
                vec![
                    question_option("Cheese"),
                    question_option("Olives"),
                    question_option("Mushrooms"),
                ],
                now,
                now + Duration::from_secs(30),
                Arc::new(AtomicBool::new(true)),
            )
            .expect("issue token");

        let values = ["0".to_owned(), "2".to_owned()];
        let response = question_select_response(&responder, &issued.token, 7, &values);
        assert_eq!(response_content(response), "answer recorded");
        assert_eq!(
            issued.receiver.await,
            Ok(QuestionAnswer::Multiple(vec![
                "Cheese".to_owned(),
                "Mushrooms".to_owned()
            ]))
        );
    }

    #[tokio::test]
    async fn a_thread_reply_answers_a_pending_multi_select_question_as_free_text() {
        let responder = test_responder();
        let now = Instant::now();
        let issued = responder
            .question_registry
            .issue_with_liveness(
                ApprovalRequest {
                    channel_id: 7,
                    session_id: "session".to_owned(),
                },
                vec![
                    question_option("Cheese"),
                    question_option("Olives"),
                    question_option("Mushrooms"),
                ],
                now,
                now + Duration::from_secs(30),
                Arc::new(AtomicBool::new(true)),
            )
            .expect("issue token");

        let token = responder
            .pending_question_token("session")
            .expect("a pending multiSelect card is still found by a free-text lookup");
        assert_eq!(token, issued.token);
        responder
            .resolve_question_text(&token, 7, "none of these")
            .expect("free-text reply resolves a pending multiSelect card");
        assert_eq!(
            issued.receiver.await,
            Ok(QuestionAnswer::Single("none of these".to_owned()))
        );
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
                now,
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
