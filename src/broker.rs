use crate::permission::{Decision, Interaction};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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
    keys: Mutex<HashSet<RequestKey>>,
}

impl PendingRequests {
    async fn register(&self, key: RequestKey) -> bool {
        self.keys.lock().await.insert(key)
    }

    async fn remove(&self, key: &RequestKey) {
        self.keys.lock().await.remove(key);
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
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let pending = Arc::clone(&pending);
                tokio::spawn(async move {
                    handle_connection(stream, pending).await;
                });
            }
            _ = &mut shutdown => break,
        }
    }
    Ok(())
}

/// Binds and runs the fixed-allow tracer broker until Ctrl-C.
///
/// # Errors
///
/// Returns an I/O error when the socket cannot be bound or the listener fails.
pub async fn run_tracer_broker(socket_path: &Path) -> io::Result<()> {
    let listener = UnixListener::bind(socket_path)?;
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let shutdown_task = tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        let _ = shutdown_tx.send(());
    });
    let result = serve_tracer_broker(listener, shutdown_rx).await;
    shutdown_task.abort();
    let _ = std::fs::remove_file(socket_path);
    result
}

async fn handle_connection(mut stream: UnixStream, pending: Arc<PendingRequests>) {
    let interaction = match read_json_line(&mut stream).await {
        Ok(interaction) if is_valid_interaction(&interaction) => interaction,
        _ => return,
    };
    let key = RequestKey::from(&interaction);
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
    let mut reader = BufReader::new(stream);
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
    use super::{BrokerResponse, correlate_decision};
    use crate::permission::{ClaudePermissionToolInput, Interaction};

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
}
