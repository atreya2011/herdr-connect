use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{SocketAddr, UnixStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

use crate::config::ENV_HOME;

pub const STATUS_IDLE: &str = "idle";
pub const STATUS_WORKING: &str = "working";
pub const STATUS_BLOCKED: &str = "blocked";
pub const STATUS_DONE: &str = "done";

pub const EVENT_KEY: &str = "event";

const SUBSCRIPTION_TYPE_KEY: &str = "type";

/// Key for a JSON-RPC message's request/response correlation identifier.
const RPC_ID_KEY: &str = "id";
/// Key for a Herdr error object's machine-readable code.
const ERROR_CODE_KEY: &str = "code";
/// Key for a JSON-RPC response's error object.
const ERROR_KEY: &str = "error";
/// Key for a Herdr error object's human-readable message.
const ERROR_MESSAGE_KEY: &str = "message";
/// Key for the Herdr pane or agent targeted by an RPC call.
const TARGET_KEY: &str = "target";

static RPC_SEQUENCE: AtomicU64 = AtomicU64::new(0);

fn herdr_socket_path() -> String {
    std::env::var("HERDR_SOCKET_PATH").unwrap_or_else(|_| {
        format!(
            "{}/.config/herdr/herdr.sock",
            std::env::var(ENV_HOME).unwrap_or_default()
        )
    })
}

fn next_rpc_id() -> String {
    format!(
        "herdr-connect:{}:{}",
        std::process::id(),
        RPC_SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1
    )
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct AgentSession {
    pub agent: String,
    pub value: String,
}

/// Sends one bounded JSON-RPC request to Herdr.
///
/// # Errors
///
/// Returns connection, timeout, protocol, or Herdr-declared errors.
pub fn request_rpc_result(method: &str) -> Result<String, String> {
    request_rpc_result_with_params(method, &json!({}))
}

/// Sends one bounded JSON-RPC request with an object of method parameters.
///
/// # Errors
///
/// Returns connection, timeout, protocol, or Herdr-declared errors.
fn request_rpc_result_with_params(method: &str, params: &Value) -> Result<String, String> {
    request_rpc_result_with_params_and_timeout(method, params, Duration::from_secs(4))
}

fn request_rpc_result_with_params_and_timeout(
    method: &str,
    params: &Value,
    read_timeout: Duration,
) -> Result<String, String> {
    if !params.is_object() {
        return Err("herdr RPC params must be a JSON object".to_owned());
    }
    let path = herdr_socket_path();
    let id = next_rpc_id();
    let request = serde_json::json!({RPC_ID_KEY: id, "method": method, "params": params});
    let result = (|| -> Result<Value, String> {
        let address = SocketAddr::from_pathname(&path).map_err(|e| e.to_string())?;
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = sender.send(UnixStream::connect_addr(&address));
        });
        let mut stream = receiver
            .recv_timeout(Duration::from_secs(4))
            .map_err(|_| "herdr RPC connect timed out".to_owned())?
            .map_err(|e| format!("herdr RPC connect failed: {e}"))?;
        stream
            .set_write_timeout(Some(Duration::from_secs(4)))
            .map_err(|e| e.to_string())?;
        stream
            .set_read_timeout(Some(read_timeout))
            .map_err(|e| e.to_string())?;
        writeln!(stream, "{request}").map_err(|e| e.to_string())?;
        let mut line = String::new();
        BufReader::new(stream)
            .read_line(&mut line)
            .map_err(|e| e.to_string())?;
        let response: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
        if let Some(error) = response.get(ERROR_KEY) {
            return Err(format!(
                "herdr {method} failed: {} {}",
                error.get(ERROR_CODE_KEY).unwrap_or(&Value::Null),
                error
                    .get(ERROR_MESSAGE_KEY)
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ));
        }
        let returned_id = response
            .get(RPC_ID_KEY)
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        if returned_id != id {
            return Err(format!(
                "herdr returned response id {returned_id} for request {id}"
            ));
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    })();
    result.map(|value| value.to_string())
}

pub const PROMPT_ACKNOWLEDGED_UNCONFIRMED: &str = "prompt submitted; Herdr state unconfirmed";

fn acknowledge_prompt_result(result: Result<String, String>) -> Result<String, String> {
    match result {
        Err(error) if is_agent_prompt_stalled(&error) => {
            Ok(PROMPT_ACKNOWLEDGED_UNCONFIRMED.to_owned())
        }
        result => result,
    }
}

fn is_agent_prompt_stalled(error: &str) -> bool {
    error
        .strip_prefix("herdr agent.prompt failed: ")
        .and_then(|error| error.split_whitespace().next())
        .is_some_and(|code| code.trim_matches('"') == "agent_prompt_stalled")
}

/// Submits one vendor-neutral prompt to a Herdr agent and waits for a lifecycle state.
///
/// A successful result means Herdr accepted the prompt. The observed state may be working, idle,
/// done, or blocked; a Herdr observation stall is also accepted with an unconfirmed state.
///
/// # Errors
///
/// Returns socket, protocol, or Herdr-declared submission errors.
pub fn agent_prompt(target: &str, text: &str) -> Result<String, String> {
    let params = json!({
        TARGET_KEY: target,
        "text": text,
        "wait": {
            "until": [STATUS_IDLE, STATUS_DONE, STATUS_BLOCKED, STATUS_WORKING],
            "timeout_ms": 6000,
        },
    });
    acknowledge_prompt_result(request_rpc_result_with_params_and_timeout(
        "agent.prompt",
        &params,
        Duration::from_secs(10),
    ))
}

/// Sends key presses to a Herdr-tracked agent pane.
///
/// # Errors
///
/// Returns socket, protocol, or Herdr-declared errors.
pub fn agent_send_keys(target: &str, keys: &[&str]) -> Result<String, String> {
    request_rpc_result_with_params(
        "agent.send_keys",
        &json!({TARGET_KEY: target, "keys": keys}),
    )
}

/// Reads a pane's Herdr detection snapshot: the TUI-state detector's own screen render, distinct
/// from the vendor's on-disk session log.
///
/// # Errors
///
/// Returns socket, envelope, or payload errors.
pub fn agent_read_detection(target: &str) -> Result<String, String> {
    let value: Value = serde_json::from_str(&request_rpc_result_with_params(
        "agent.read",
        &json!({TARGET_KEY: target, "source": "detection", "strip_ansi": true}),
    )?)
    .map_err(|error| error.to_string())?;
    value
        .pointer("/read/text")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| "agent.read response did not contain read.text".to_owned())
}
#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct HerdrTab {
    pub tab_id: String,
    pub workspace_id: String,
    pub label: String,
}

/// Lists tabs while preserving Herdr and payload errors.
///
/// # Errors
///
/// Returns socket, envelope, or per-tab deserialization errors.
pub fn tab_list_result() -> Result<Vec<HerdrTab>, String> {
    let value: Value =
        serde_json::from_str(&request_rpc_result("tab.list")?).map_err(|e| e.to_string())?;
    value
        .get("tabs")
        .and_then(Value::as_array)
        .ok_or_else(|| "tab.list response did not contain tabs".to_owned())?
        .iter()
        .map(|tab| HerdrTab::deserialize(tab).map_err(|error| error.to_string()))
        .collect()
}

#[derive(Clone, Deserialize, Debug)]
pub struct AgentSnapshot {
    pub agent: String,
    pub terminal_id: String,
    pub agent_status: String,
    pub tab_id: Option<String>,
    pub workspace_id: Option<String>,
    pub pane_id: Option<String>,
    pub cwd: Option<String>,
    pub terminal_title_stripped: Option<String>,
    #[serde(alias = "agent_session")]
    pub session: Option<AgentSession>,
    #[serde(default)]
    pub state_change_seq: u64,
}
/// Lists agents from Herdr.
///
/// # Errors
///
/// Returns socket, envelope, or payload errors.
pub fn list_agents() -> Result<Vec<AgentSnapshot>, String> {
    let value: Value =
        serde_json::from_str(&request_rpc_result("agent.list")?).map_err(|e| e.to_string())?;
    value
        .get("agents")
        .and_then(Value::as_array)
        .ok_or_else(|| "agent.list response did not contain agents".into())
        .and_then(|a| {
            a.iter()
                .map(|v| AgentSnapshot::deserialize(v).map_err(|error| error.to_string()))
                .collect::<Result<Vec<_>, _>>()
        })
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize)]
pub struct HerdrWorkspace {
    pub workspace_id: String,
    pub label: String,
}

/// Lists workspaces while preserving Herdr and payload errors.
///
/// # Errors
///
/// Returns socket, envelope, or per-workspace deserialization errors.
pub fn workspace_list_result() -> Result<Vec<HerdrWorkspace>, String> {
    let value: Value =
        serde_json::from_str(&request_rpc_result("workspace.list")?).map_err(|e| e.to_string())?;
    value
        .get("workspaces")
        .and_then(Value::as_array)
        .ok_or_else(|| "workspace.list response did not contain workspaces".to_owned())?
        .iter()
        .map(|workspace| HerdrWorkspace::deserialize(workspace).map_err(|error| error.to_string()))
        .collect()
}

/// Session-wide pane-membership and tab/workspace-closure watches for the lifecycle subscribe
/// socket.
#[must_use]
pub fn lifecycle_subscriptions() -> Vec<Value> {
    vec![
        json!({SUBSCRIPTION_TYPE_KEY: "pane.created"}),
        json!({SUBSCRIPTION_TYPE_KEY: "pane.closed"}),
        json!({SUBSCRIPTION_TYPE_KEY: "pane.agent_detected"}),
        json!({SUBSCRIPTION_TYPE_KEY: "tab.closed"}),
        json!({SUBSCRIPTION_TYPE_KEY: "workspace.closed"}),
    ]
}

/// Per-pane status watches. `pane.agent_status_changed` requires `pane_id` and must not filter
/// status, so `working` is observed.
#[must_use]
pub fn status_subscriptions(pane_ids: &[String]) -> Vec<Value> {
    pane_ids
        .iter()
        .map(|pane_id| json!({SUBSCRIPTION_TYPE_KEY: "pane.agent_status_changed", "pane_id": pane_id}))
        .collect()
}

/// Long-lived Herdr `events.subscribe` stream. The first JSON line is the subscribe ack; later
/// lines are pushed events.
pub struct HerdrSubscription {
    reader: tokio::io::BufReader<tokio::net::UnixStream>,
}

/// Error from [`subscribe_herdr_events`], distinguishing a Herdr-declared `pane_not_found` from
/// every other connect, timeout, protocol, or Herdr-declared failure.
#[derive(Debug)]
pub enum SubscribeError {
    PaneNotFound,
    Other(String),
}

impl std::fmt::Display for SubscribeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PaneNotFound => write!(formatter, "pane_not_found"),
            Self::Other(message) => write!(formatter, "{message}"),
        }
    }
}

impl From<SubscribeError> for String {
    fn from(error: SubscribeError) -> Self {
        error.to_string()
    }
}

/// Opens `events.subscribe` and consumes the ack. The connection stays open for [`HerdrSubscription::next_event`].
///
/// # Errors
///
/// Returns connect, timeout, protocol, empty-subscription, or Herdr-declared errors.
pub async fn subscribe_herdr_events(
    subscriptions: &[Value],
) -> Result<HerdrSubscription, SubscribeError> {
    if subscriptions.is_empty() {
        return Err(SubscribeError::Other(
            "herdr events.subscribe requires at least one subscription".to_owned(),
        ));
    }
    let path = herdr_socket_path();
    let id = next_rpc_id();
    let request = json!({
        RPC_ID_KEY: id,
        "method": "events.subscribe",
        "params": {"subscriptions": subscriptions},
    });
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut stream = tokio::net::UnixStream::connect(&path)
            .await
            .map_err(|error| {
                SubscribeError::Other(format!("herdr subscribe connect failed: {error}"))
            })?;
        stream
            .write_all(format!("{request}\n").as_bytes())
            .await
            .map_err(|error| SubscribeError::Other(error.to_string()))?;
        stream
            .flush()
            .await
            .map_err(|error| SubscribeError::Other(error.to_string()))?;
        let mut reader = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        let read = reader
            .read_line(&mut line)
            .await
            .map_err(|error| SubscribeError::Other(error.to_string()))?;
        if read == 0 {
            return Err(SubscribeError::Other(
                "herdr subscribe stream closed before ack".to_owned(),
            ));
        }
        let response: Value = serde_json::from_str(&line)
            .map_err(|error| SubscribeError::Other(error.to_string()))?;
        if let Some(error) = response.get(ERROR_KEY) {
            if error.get(ERROR_CODE_KEY).and_then(Value::as_str) == Some("pane_not_found") {
                return Err(SubscribeError::PaneNotFound);
            }
            return Err(SubscribeError::Other(format!(
                "herdr events.subscribe failed: {} {}",
                error.get(ERROR_CODE_KEY).unwrap_or(&Value::Null),
                error
                    .get(ERROR_MESSAGE_KEY)
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            )));
        }
        let returned_id = response
            .get(RPC_ID_KEY)
            .and_then(Value::as_str)
            .unwrap_or("<missing>");
        if returned_id != id {
            return Err(SubscribeError::Other(format!(
                "herdr returned response id {returned_id} for request {id}"
            )));
        }
        let started = response
            .pointer("/result/type")
            .and_then(Value::as_str)
            .unwrap_or("");
        if started != "subscription_started" {
            return Err(SubscribeError::Other(format!(
                "herdr events.subscribe ack was not subscription_started: {response}"
            )));
        }
        Ok(HerdrSubscription { reader })
    })
    .await
    .map_err(|_| SubscribeError::Other("herdr subscribe connect timed out".to_owned()))?
}

impl HerdrSubscription {
    /// Reads the next pushed JSON event line.
    ///
    /// # Errors
    ///
    /// Returns stream-closed, parse, or Herdr-declared errors.
    pub async fn next_event(&mut self) -> Result<Value, String> {
        let mut line = String::new();
        let read = self
            .reader
            .read_line(&mut line)
            .await
            .map_err(|error| error.to_string())?;
        if read == 0 {
            return Err("herdr subscribe stream closed".to_owned());
        }
        let value: Value = serde_json::from_str(&line).map_err(|error| error.to_string())?;
        if let Some(error) = value.get(ERROR_KEY) {
            return Err(format!(
                "herdr subscribe event error: {} {}",
                error.get(ERROR_CODE_KEY).unwrap_or(&Value::Null),
                error
                    .get(ERROR_MESSAGE_KEY)
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ));
        }
        if value.get(EVENT_KEY).and_then(Value::as_str).is_none() {
            return Err(format!("herdr subscribe line was not an event: {value}"));
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::acknowledge_prompt_result;

    #[test]
    fn prompt_stalls_are_acknowledged_but_real_errors_are_preserved() {
        let cases = [
            (
                Err("herdr agent.prompt failed: agent_prompt_stalled state was not observed"),
                Ok("prompt submitted; Herdr state unconfirmed"),
            ),
            (
                Err("herdr RPC connect failed: no socket"),
                Err("herdr RPC connect failed: no socket"),
            ),
            (
                Err("herdr agent.prompt failed: agent_not_found agent_prompt_stalled"),
                Err("herdr agent.prompt failed: agent_not_found agent_prompt_stalled"),
            ),
            (Ok("{\"state\":\"working\"}"), Ok("{\"state\":\"working\"}")),
        ];
        for (result, expected) in cases {
            assert_eq!(
                acknowledge_prompt_result(result.map(str::to_owned).map_err(str::to_owned)),
                expected.map(str::to_owned).map_err(str::to_owned)
            );
        }
    }
}
