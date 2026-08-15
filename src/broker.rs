use crate::permission::{Decision, DecisionBehavior, Interaction};
use crate::{
    ApprovalRequest, InteractionRegistry, ResolveError, deliver_permission_card,
    expire_permission_card, list_agents, route_topology, sync_topology, tab_list_result,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, oneshot};
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

pub struct PermissionResponder {
    pub client: Arc<Client>,
    pub guild: Id<GuildMarker>,
    pub owner_id: String,
    pub registry: Arc<InteractionRegistry>,
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

    async fn request(&self, interaction: &Interaction) -> Option<Decision> {
        let interaction_for_route = interaction.clone();
        let route = tokio::task::spawn_blocking(move || {
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
        })
        .await
        .ok()
        .and_then(Result::ok)?;
        let channel = sync_topology(
            self.client.as_ref(),
            self.guild,
            &route.workspace_id,
            &route.channel_name,
            &route.thread_name,
            &route.tab_id,
        )
        .await
        .ok()?;
        let created_at = std::time::Instant::now();
        let issued = self
            .registry
            .issue(
                ApprovalRequest {
                    owner_id: self.owner_id.clone(),
                    session_id: interaction.session_id.clone(),
                    prompt_id: interaction.prompt_id.clone(),
                    tool: interaction.tool_name.clone(),
                    channel_id: channel.get(),
                },
                created_at,
                created_at + PERMISSION_TIMEOUT,
            )
            .ok()?;
        let Ok(message) = deliver_permission_card(
            self.client.as_ref(),
            channel,
            &interaction.tool_name,
            &interaction.tool_input.command,
            &issued.token,
        )
        .await
        else {
            self.registry.remove(&issued.token);
            return None;
        };
        let decision = tokio::time::timeout(PERMISSION_TIMEOUT, issued.receiver)
            .await
            .ok()
            .and_then(Result::ok);
        let card_text = match decision.as_ref().map(|decision| &decision.behavior) {
            Some(DecisionBehavior::Allow) => "resolved: allowed",
            Some(DecisionBehavior::Deny) => "resolved: denied",
            None => {
                self.registry
                    .expire(&issued.token, std::time::Instant::now());
                "expired: no owner decision"
            }
        };
        let _ = expire_permission_card(
            self.client.as_ref(),
            channel,
            message,
            &issued.token,
            card_text,
        )
        .await;
        decision
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
    } else if let Some(session_id) = responder.registry.session_id(token) {
        let decision = match action {
            "allow" => Decision::allow(),
            "deny" => Decision::deny(Some("operator denied this request".to_owned())),
            _ => return,
        };
        match responder.registry.resolve(
            token,
            &responder.owner_id,
            &session_id,
            channel.id.get(),
            decision,
            std::time::Instant::now(),
        ) {
            Ok(()) => ephemeral_response("decision recorded"),
            Err(ResolveError::Unauthorized) => ephemeral_response("not authorized"),
            Err(
                ResolveError::UnknownOrExpired
                | ResolveError::WrongSession
                | ResolveError::WrongChannel,
            ) => ephemeral_response("expired"),
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
pub struct BrokerResponse {
    pub session_id: String,
    pub prompt_id: String,
    pub decision: Decision,
}

#[derive(Debug, Eq, PartialEq)]
pub enum CorrelationError {
    MismatchedRequest,
}

/// Accepts a broker response only when its session and prompt match the request.
///
/// # Errors
///
/// Returns `MismatchedRequest` for a response belonging to another or stale request.
pub fn correlate_decision(
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
pub async fn serve_broker(
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
    let interaction = match read_json_line(&mut stream).await {
        Ok(interaction) if is_valid_interaction(&interaction) => interaction,
        _ => return,
    };
    let key = PendingKey::new(&interaction, connection_id);
    if !pending.register(key.clone()).await {
        return;
    }

    let decision = responder.request(&interaction).await;
    if let Some(decision) = decision {
        let response = BrokerResponse {
            session_id: interaction.session_id.clone(),
            prompt_id: interaction.prompt_id.clone(),
            decision,
        };
        let _ = write_json_line(&mut stream, &response).await;
    }
    pending.remove(&key).await;
}

const fn is_valid_interaction(interaction: &Interaction) -> bool {
    !interaction.session_id.is_empty()
        && !interaction.prompt_id.is_empty()
        && !interaction.tool_name.is_empty()
        && !interaction.tool_input.command.is_empty()
        && !interaction.tool_input.description.is_empty()
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
    if length == 0 || length > MAX_FRAME_BYTES || bytes.last() != Some(&b'\n') {
        return Err("invalid broker frame".to_owned());
    }
    serde_json::from_slice(&bytes[..bytes.len() - 1]).map_err(|error| error.to_string())
}

async fn write_json_line<T>(stream: &mut UnixStream, value: &T) -> Result<(), String>
where
    T: Serialize + Sync,
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
    use super::{BrokerResponse, PendingKey, PendingRequests, correlate_decision, read_json_line};
    use crate::permission::{ClaudePermissionToolInput, Interaction};
    use tokio::io::AsyncWriteExt;
    use tokio::net::UnixStream;

    fn interaction() -> Interaction {
        Interaction {
            session_id: "session".to_owned(),
            prompt_id: "prompt".to_owned(),
            tool_name: "Bash".to_owned(),
            tool_input: ClaudePermissionToolInput {
                command: "touch proof".to_owned(),
                description: "Create proof".to_owned(),
            },
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
}
