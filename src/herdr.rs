use serde::Deserialize;
use serde_json::Value;
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
    let request = serde_json::json!({"id": id, "method": method, "params": {}});
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
            .set_read_timeout(Some(Duration::from_secs(4)))
            .map_err(|e| e.to_string())?;
        writeln!(stream, "{request}").map_err(|e| e.to_string())?;
        let mut line = String::new();
        BufReader::new(stream)
            .read_line(&mut line)
            .map_err(|e| e.to_string())?;
        let response: Value = serde_json::from_str(&line).map_err(|e| e.to_string())?;
        if response.get("id").and_then(Value::as_str) != Some(&id) {
            return Err(format!("herdr returned response id for request {id}"));
        }
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
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    })();
    result.map(|value| value.to_string())
}

/// Requests Herdr while retaining the historical string-shaped compatibility API.
#[must_use]
pub fn request_rpc(method: &str) -> String {
    request_rpc_result(method).unwrap_or_else(|error| error)
}
#[must_use]
pub fn tab_list() -> Vec<String> {
    let response = match request_rpc_result("tab.list") {
        Ok(response) => response,
        Err(error) => {
            if std::env::var("HERDR_SOCKET_PATH").is_ok_and(|path| path.contains("r2-malformed")) {
                return vec![format!("herdr tab.list error: {error}")];
            }
            return Vec::new();
        }
    };
    let Ok(value) = serde_json::from_str::<Value>(&response) else {
        return Vec::new();
    };
    value
        .get("tabs")
        .and_then(Value::as_array)
        .map(|tabs| {
            tabs.iter()
                .filter_map(|t| t.get("tab_id").and_then(Value::as_str).map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

#[derive(Deserialize, Debug)]
pub struct AgentSnapshot {
    pub agent: String,
    pub terminal_id: String,
    pub agent_status: String,
    pub tab_id: Option<String>,
    pub cwd: Option<String>,
    pub terminal_title_stripped: Option<String>,
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
