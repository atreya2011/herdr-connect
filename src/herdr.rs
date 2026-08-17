use serde::Deserialize;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{SocketAddr, UnixStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::Duration;

static RPC_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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
    let path = std::env::var("HERDR_SOCKET_PATH").unwrap_or_else(|_| {
        format!(
            "{}/.config/herdr/herdr.sock",
            std::env::var("HOME").unwrap_or_default()
        )
    });
    let id = format!(
        "herdr-connect:{}:{}",
        std::process::id(),
        RPC_SEQUENCE.fetch_add(1, Ordering::Relaxed) + 1
    );
    let request = serde_json::json!({"id": id, "method": method, "params": params});
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
        if let Some(error) = response.get("error") {
            return Err(format!(
                "herdr {method} failed: {} {}",
                error.get("code").map_or(Value::Null, Clone::clone),
                error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown error")
            ));
        }
        let returned_id = response
            .get("id")
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
        "target": target,
        "text": text,
        "wait": {
            "until": ["idle", "done", "blocked", "working"],
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
    request_rpc_result_with_params("agent.send_keys", &json!({"target": target, "keys": keys}))
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
        .map(|tab| serde_json::from_value(tab.clone()).map_err(|error| error.to_string()))
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
                .map(|v| {
                    serde_json::from_value::<AgentSnapshot>(v.clone())
                        .map_err(|error| error.to_string())
                })
                .collect::<Result<Vec<_>, _>>()
        })
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
