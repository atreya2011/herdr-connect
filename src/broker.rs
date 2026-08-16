use crate::delivery::expire_permission_card;
use crate::permission::{Decision, DecisionBehavior, Interaction};
use crate::registry::{ApprovalRequest, InteractionRegistry, ResolveError};
use crate::{deliver_permission_card, list_agents, route_topology, sync_topology, tab_list_result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Notify, oneshot};
use twilight_http::Client;
use twilight_model::application::interaction::{
    Interaction as DiscordInteraction, InteractionData,
};
use twilight_model::channel::message::MessageFlags;
use twilight_model::http::interaction::{
    InteractionResponse, InteractionResponseData, InteractionResponseType,
};
use twilight_model::id::{Id, marker::GuildMarker};

const MAX_FRAME_BYTES: usize = 64 * 1024;
const PERMISSION_TIMEOUT: Duration = Duration::from_secs(45);
const INITIAL_FRAME_TIMEOUT: Duration = Duration::from_secs(10);

#[must_use]
pub const fn hook_timeout() -> Duration {
    Duration::from_secs(PERMISSION_TIMEOUT.as_secs() + 5)
}

pub struct PermissionResponder {
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: String,
    registry: Arc<InteractionRegistry>,
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

fn return_decision_before_card_edit<F>(decision: Option<Decision>, card_edit: F) -> Option<Decision>
where
    F: Future<Output = Result<(), String>> + Send + 'static,
{
    std::mem::drop(tokio::spawn(async move {
        if let Err(error) = card_edit.await {
            eprintln!("permission card edit failed: {error}");
        }
    }));
    decision
}

impl PermissionResponder {
    #[must_use]
    pub fn new(client: Arc<Client>, guild: Id<GuildMarker>, owner_id: String) -> Self {
        Self {
            client,
            guild,
            owner_id,
            registry: Arc::new(InteractionRegistry::default()),
        }
    }

    #[must_use]
    pub fn has_pending_session(&self, session_id: &str) -> bool {
        self.registry.has_pending_session(session_id)
    }

    async fn request(&self, interaction: &Interaction, liveness: HookLiveness) -> Option<Decision> {
        let route = self.route(interaction, &liveness).await?;
        let channel = self.sync_channel(&route, &liveness).await?;
        let created_at = std::time::Instant::now();
        let issued = self
            .registry
            .issue_with_liveness(
                ApprovalRequest {
                    channel_id: channel.get(),
                    session_id: interaction.session_id.clone(),
                },
                created_at,
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
        return_decision_before_card_edit(decision, async move {
            expire_permission_card(client.as_ref(), channel, message, &token, card_text).await
        })
    }

    async fn route(
        &self,
        interaction: &Interaction,
        liveness: &HookLiveness,
    ) -> Option<crate::TopologyRoute> {
        let interaction_for_route = interaction.clone();
        let route_task = tokio::task::spawn_blocking(move || {
            let agents = list_agents()?;
            let tabs = tab_list_result()?;
            let matches: Vec<_> = agents
                .iter()
                .filter(|agent| {
                    agent
                        .session
                        .as_ref()
                        .is_some_and(|session| session.value == interaction_for_route.session_id)
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
                    eprintln!("{error}");
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
        let channel_task = sync_topology(
            self.client.as_ref(),
            self.guild,
            &route.workspace_id,
            &route.channel_name,
            &route.thread_name,
            &route.tab_id,
        );
        let channel = tokio::select! {
            result = channel_task => result.map_err(|error| {
                eprintln!("{error}");
                error
            }).ok(),
            () = liveness.wait_closed() => None,
        }?;
        liveness.is_alive().then_some(channel)
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
}

pub async fn handle_component(
    responder: Arc<PermissionResponder>,
    interaction: DiscordInteraction,
) {
    let Some(InteractionData::MessageComponent(data)) = interaction.data.as_ref() else {
        return;
    };
    let Some((action, token)) = data
        .custom_id
        .split_once(':')
        .and_then(|(prefix, rest)| prefix.strip_prefix("herdr").map(|_| rest))
        .and_then(|rest| rest.split_once(':'))
    else {
        return;
    };
    let Some(channel) = interaction.channel.as_ref() else {
        return;
    };
    let response = if interaction.guild_id != Some(responder.guild)
        || interaction
            .author_id()
            .is_none_or(|id| id.to_string() != responder.owner_id)
    {
        ephemeral_response("not authorized")
    } else if responder.registry.has_pending(token) {
        let decision = match action {
            "allow" => Decision::allow(),
            "deny" => Decision::deny(Some("operator denied this request".to_owned())),
            _ => return,
        };
        match responder.registry.resolve(
            token,
            channel.id.get(),
            decision,
            std::time::Instant::now(),
        ) {
            Ok(()) => ephemeral_response("decision recorded"),
            Err(ResolveError::UnknownOrExpired | ResolveError::WrongChannel) => {
                ephemeral_response("expired")
            }
        }
    } else {
        ephemeral_response("expired")
    };
    let _ = responder
        .client
        .interaction(interaction.application_id)
        .create_response(interaction.id, &interaction.token, &response)
        .await;
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
        let mut stream = UnixStream::connect(socket_path).await.map_err(|_| ())?;
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
) -> io::Result<()> {
    let pending = Arc::new(PendingRequests::default());
    let next_connection_id = AtomicU64::new(0);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let pending = Arc::clone(&pending);
                let responder = Arc::clone(&responder);
                let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    handle_connection(stream, pending, responder, connection_id).await;
                });
            }
            _ = &mut shutdown => break,
        }
    }
    Ok(())
}

/// Binds and runs the Discord-backed permission broker until Ctrl-C or SIGTERM.
///
/// # Errors
///
/// Returns an I/O error when the socket cannot be bound or the listener fails.
pub async fn run_broker(socket_path: &Path, responder: Arc<PermissionResponder>) -> io::Result<()> {
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
    let result = serve_broker(listener, shutdown_rx, responder).await;
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
    connection_id: u64,
) {
    let interaction =
        match tokio::time::timeout(INITIAL_FRAME_TIMEOUT, read_json_line(&mut stream)).await {
            Ok(Ok(interaction)) if is_valid_interaction(&interaction) => interaction,
            Ok(Ok(_)) => return,
            Ok(Err(error)) => {
                eprintln!("broker rejected initial frame: {error}");
                return;
            }
            Err(_) => {
                eprintln!("broker rejected initial frame: initial frame read timed out");
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

async fn read_json_line<T>(stream: &mut UnixStream) -> Result<T, String>
where
    T: for<'de> Deserialize<'de>,
{
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
    let payload = &bytes[..bytes.len() - 1];
    if payload.is_empty() {
        return Err("empty broker frame".to_owned());
    }
    serde_json::from_slice(payload).map_err(|error| format!("malformed broker frame: {error}"))
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
    use super::{
        BrokerResponse, HookLiveness, PERMISSION_TIMEOUT, PendingKey, PendingRequests,
        PermissionResponder, correlate_decision, read_json_line, return_decision_before_card_edit,
        spawn_hook_monitor,
    };
    use crate::permission::{ClaudePermissionToolInput, Decision, Interaction, PermissionVendor};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixStream;
    use tokio::sync::oneshot;

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
        let decision = return_decision_before_card_edit(Some(Decision::allow()), async move {
            edit_started_by_task.store(true, Ordering::Release);
            std::future::pending::<Result<(), String>>().await
        });
        assert_eq!(decision, Some(Decision::allow()));
        assert!(!edit_started.load(Ordering::Acquire));
        tokio::task::yield_now().await;
        assert!(edit_started.load(Ordering::Acquire));
    }
}
