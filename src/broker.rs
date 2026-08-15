use crate::permission::{Decision, Interaction};
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

const MAX_FRAME_BYTES: usize = 64 * 1024;

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

impl BrokerResponse {
    fn allow_for(interaction: &Interaction) -> Self {
        Self {
            session_id: interaction.session_id.clone(),
            prompt_id: interaction.prompt_id.clone(),
            decision: Decision::allow(),
        }
    }
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
pub async fn serve_tracer_broker(
    listener: UnixListener,
    mut shutdown: oneshot::Receiver<()>,
) -> io::Result<()> {
    let pending = Arc::new(PendingRequests::default());
    let next_connection_id = AtomicU64::new(0);
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let pending = Arc::clone(&pending);
                let connection_id = next_connection_id.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    handle_connection(stream, pending, connection_id).await;
                });
            }
            _ = &mut shutdown => break,
        }
    }
    Ok(())
}

/// Binds and runs the fixed-allow tracer broker until Ctrl-C or SIGTERM.
///
/// # Errors
///
/// Returns an I/O error when the socket cannot be bound or the listener fails.
pub async fn run_tracer_broker(socket_path: &Path) -> io::Result<()> {
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
    let result = serve_tracer_broker(listener, shutdown_rx).await;
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

    let response = BrokerResponse::allow_for(&interaction);
    let _ = write_json_line(&mut stream, &response).await;
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
