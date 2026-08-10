#![allow(clippy::all, clippy::pedantic, clippy::nursery, clippy::cargo)]

use serde::Deserialize;
use serde::de::Error as _;
use serde_json::Value;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const POINTER: &str = "agent stopped, no log available";
const MAX_PART_LENGTH: usize = 1_900;
const MAX_THREAD_NAME_LENGTH: usize = 100;
static RPC_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AgentSession {
    pub agent: String,
    pub value: String,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AgentLog {
    pub message: String,
    pub tool_calls: u32,
    pub details: Option<String>,
    pub question: Option<String>,
    pub failure: Option<String>,
}
#[derive(Debug, PartialEq, Eq)]
pub struct AppConfig {
    pub herdr_socket_path: String,
    pub poll_interval_ms: u64,
}
#[derive(Debug, PartialEq, Eq)]
pub struct DiscordConfig {
    pub guild_id: String,
    pub owner_id: String,
    pub token: String,
}
#[derive(Debug, PartialEq, Eq)]
pub struct TransitionMessage {
    pub description: String,
    pub color: u32,
    pub mention: Option<String>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AgentLogCapture {
    pub message: String,
    pub failure: Option<String>,
    pub question: Option<String>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Transition {
    pub from: String,
    pub to: String,
    pub terminal_id: String,
    pub agent: String,
}

pub fn read_agent_log(session: Option<AgentSession>, path: &Path) -> Result<AgentLog, String> {
    let session = session.ok_or_else(|| POINTER.to_owned())?;
    let bytes = std::fs::read(path).map_err(|_| POINTER.to_owned())?;
    let text = String::from_utf8(bytes).map_err(|_| POINTER.to_owned())?;
    match session.agent.as_str() {
        "claude" => parse_claude(&text),
        "codex" => parse_codex(&text),
        "cursor" => parse_cursor(&text),
        _ => Err(serde_json::Error::custom(POINTER)),
    }
    .map_err(|_| POINTER.to_owned())
}

fn lines(text: &str) -> Result<Vec<Value>, serde_json::Error> {
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(serde_json::from_str)
        .collect()
}
fn parse_claude(text: &str) -> Result<AgentLog, serde_json::Error> {
    let records = lines(text)?;
    let start = records
        .iter()
        .rposition(|r| {
            if r.get("type").and_then(Value::as_str) != Some("user")
                || r.get("isMeta") == Some(&Value::Bool(true))
            {
                return false;
            }
            let content = r.get("message").and_then(|m| m.get("content"));
            content.is_some_and(|c| {
                c.is_string()
                    || c.as_array().is_some_and(|parts| {
                        parts
                            .iter()
                            .any(|p| p.get("type").and_then(Value::as_str) == Some("text"))
                    })
            })
        })
        .map_or(0, |i| i + 1);
    let tail = &records[start..];
    let mut message = None;
    let question = None;
    let mut tools = 0;
    let mut failure = None;
    for record in tail {
        if let Some(contents) = record
            .get("message")
            .and_then(|m| m.get("content"))
            .and_then(Value::as_array)
        {
            for part in contents {
                match part.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        message = part.get("text").and_then(Value::as_str).map(str::to_owned)
                    }
                    Some("tool_use") => tools += 1,
                    Some("tool_result") if part.get("is_error") == Some(&Value::Bool(true)) => {
                        failure = part
                            .get("content")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                    }
                    _ => {}
                }
            }
        }
        if record.get("type").and_then(Value::as_str) == Some("assistant") {
            if let Some(s) = record.get("message").and_then(Value::as_str) {
                message = Some(s.to_owned());
            }
        }
    }
    let message = message.ok_or_else(|| serde_json::Error::custom("empty assistant message"))?;
    Ok(AgentLog {
        message,
        tool_calls: tools,
        details: None,
        question,
        failure,
    })
}
fn parse_codex(text: &str) -> Result<AgentLog, serde_json::Error> {
    let records = lines(text)?;
    let start = records
        .iter()
        .rposition(|r| {
            r.get("type").and_then(Value::as_str) == Some("turn_context")
                || (r.get("type").and_then(Value::as_str) == Some("event_msg")
                    && r.get("payload")
                        .and_then(|p| p.get("type"))
                        .and_then(Value::as_str)
                        == Some("task_started"))
        })
        .map_or(0, |i| i + 1);
    let tail = &records[start..];
    let message = tail
        .iter()
        .rev()
        .find_map(|r| {
            r.get("payload")
                .and_then(|p| p.get("last_agent_message").or_else(|| p.get("message")))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            tail.iter().rev().find_map(|r| {
                r.get("payload")
                    .and_then(|p| p.get("content"))
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(|x| x.get("text").and_then(Value::as_str))
                            .collect::<String>()
                    })
                    .filter(|s| !s.is_empty())
            })
        })
        .ok_or_else(|| serde_json::Error::custom("empty assistant message"))?;
    let tools = tail
        .iter()
        .filter(|r| {
            r.get("type").and_then(Value::as_str) == Some("response_item")
                && r.get("payload")
                    .and_then(|p| p.get("type"))
                    .and_then(Value::as_str)
                    .is_some_and(|t| {
                        [
                            "local_shell_call",
                            "function_call",
                            "tool_search_call",
                            "custom_tool_call",
                            "web_search_call",
                            "image_generation_call",
                        ]
                        .contains(&t)
                    })
        })
        .count() as u32;
    let failure = tail.iter().rev().find_map(|r| {
        (r.get("payload")
            .and_then(|p| p.get("type"))
            .and_then(Value::as_str)
            == Some("turn_aborted"))
        .then(|| {
            r.get("payload")
                .and_then(|p| p.get("reason"))
                .and_then(Value::as_str)
                .unwrap_or("turn aborted")
                .to_owned()
        })
    });
    Ok(AgentLog {
        message,
        tool_calls: tools,
        details: None,
        question: None,
        failure,
    })
}
fn parse_cursor(text: &str) -> Result<AgentLog, serde_json::Error> {
    let value: Value = serde_json::from_str(text)?;
    let rows = value
        .as_array()
        .ok_or_else(|| serde_json::Error::custom("invalid cursor log"))?;
    let start = rows
        .iter()
        .rposition(|row| {
            row.get("data")
                .and_then(|data| data.get("role"))
                .and_then(Value::as_str)
                == Some("user")
                && row
                    .get("data")
                    .and_then(|data| data.get("content"))
                    .is_some_and(Value::is_array)
        })
        .map_or(0, |i| i + 1);
    let tail = &rows[start..];
    let message = tail
        .iter()
        .rev()
        .find_map(|row| {
            row.get("data")
                .and_then(|data| data.get("content"))
                .and_then(Value::as_array)
                .and_then(|parts| {
                    parts
                        .iter()
                        .rev()
                        .find_map(|part| part.get("text").and_then(Value::as_str))
                })
        })
        .ok_or_else(|| serde_json::Error::custom("empty assistant message"))?;
    let tool_calls = tail
        .iter()
        .filter(|row| {
            row.get("data")
                .and_then(|data| data.get("content"))
                .and_then(Value::as_array)
                .is_some_and(|parts| {
                    parts
                        .iter()
                        .any(|part| part.get("type").and_then(Value::as_str) == Some("tool-call"))
                })
        })
        .count() as u32;
    Ok(AgentLog {
        message: message.into(),
        tool_calls,
        details: None,
        question: None,
        failure: None,
    })
}

#[must_use]
pub fn load_config(environment: &[(&str, &str)], home: &str) -> AppConfig {
    AppConfig {
        herdr_socket_path: environment
            .iter()
            .find(|(n, _)| *n == "HERDR_SOCKET_PATH")
            .map_or_else(
                || format!("{home}/.config/herdr/herdr.sock"),
                |(_, v)| (*v).into(),
            ),
        poll_interval_ms: 1_500,
    }
}
pub fn load_discord_config(environment: &[(&str, &str)]) -> Result<DiscordConfig, String> {
    let value = |n: &str| environment.iter().find(|(k, _)| *k == n).map(|(_, v)| *v);
    let names = ["DISCORD_TOKEN", "DISCORD_GUILD_ID", "DISCORD_OWNER_ID"];
    let missing: Vec<_> = names
        .into_iter()
        .filter(|n| value(n).is_none_or(|v| v.trim().is_empty()))
        .collect();
    if !missing.is_empty() {
        return Err(format!(
            "Missing required environment variables: {}",
            missing.join(", ")
        ));
    }
    Ok(DiscordConfig {
        guild_id: value("DISCORD_GUILD_ID").unwrap().trim().into(),
        owner_id: value("DISCORD_OWNER_ID").unwrap().trim().into(),
        token: value("DISCORD_TOKEN").unwrap().trim().into(),
    })
}

pub fn create_transition_messages(
    transition: &Transition,
    capture: &AgentLogCapture,
    owner: &str,
) -> Vec<TransitionMessage> {
    let body = if transition.to == "blocked" {
        capture.question.as_deref().unwrap_or(&capture.message)
    } else {
        &capture.message
    };
    let color = if capture.failure.is_some() {
        0xed4245
    } else if transition.to == "blocked" {
        0xfee75c
    } else {
        0x57f287
    };
    let mut parts = split_body(body);
    if parts.len() > 1 && !body.contains("```") {
        parts = chars_chunks(body, MAX_PART_LENGTH - 6);
    }
    let total = parts.len();
    if total > 1 {
        for (i, part) in parts.iter_mut().enumerate() {
            *part = format!("{}/{}\n{}", i + 1, total, part);
        }
    }
    parts
        .into_iter()
        .enumerate()
        .map(|(i, description)| TransitionMessage {
            description,
            color,
            mention: (transition.to == "blocked" && i == 0).then(|| format!("<@{owner}>")),
        })
        .collect()
}
fn split_body(body: &str) -> Vec<String> {
    if let Some((open, rest)) = body.split_once('\n') {
        if open.starts_with("```") && rest.ends_with("\n```") {
            let inner = &rest[..rest.len() - 4];
            let limit = MAX_PART_LENGTH.saturating_sub(open.len() + 6);
            return chars_chunks(inner, limit)
                .into_iter()
                .map(|chunk| format!("{open}\n{chunk}\n```"))
                .collect();
        }
    }
    let mut out = Vec::new();
    let mut current = String::new();
    let mut fenced = false;
    for line in body.split('\n') {
        let is_fence = line.starts_with("```");
        if is_fence && fenced {
            current.push_str(line);
            if current.len() <= MAX_PART_LENGTH {
                out.push(current.clone());
                current.clear();
            }
            fenced = false;
            continue;
        }
        if is_fence {
            fenced = true;
        }
        let candidate = if current.is_empty() {
            line.to_owned()
        } else {
            format!("{current}\n{line}")
        };
        if candidate.len() <= MAX_PART_LENGTH {
            current = candidate;
        } else if fenced && current.starts_with("```") {
            let open = current.lines().next().unwrap_or("```").to_owned();
            let inner = current.lines().skip(1).collect::<Vec<_>>().join("\n");
            for chunk in chars_chunks(&inner, MAX_PART_LENGTH.saturating_sub(open.len() + 6)) {
                out.push(format!("{open}\n{chunk}\n```"));
            }
            current = String::new();
            fenced = false;
        } else {
            if !current.is_empty() {
                out.push(current);
            }
            current = line.to_owned();
            if current.len() > MAX_PART_LENGTH {
                out.extend(chars_chunks(&current, MAX_PART_LENGTH));
                current = String::new();
            }
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    if out.is_empty() {
        out.push(String::new());
    }
    out
}
fn chars_chunks(text: &str, limit: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for c in text.chars() {
        if current.len() + c.len_utf8() > limit && !current.is_empty() {
            out.push(current);
            current = String::new();
        }
        current.push(c);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}
pub fn format_thread_name(label: &str, title: &str, tab_id: &str) -> Result<String, String> {
    let label = label.trim();
    let title = title.trim();
    let base = if !label.is_empty() && !label.chars().all(|c| c.is_ascii_digit()) {
        label
    } else if label.chars().all(|c| c.is_ascii_digit()) && !title.is_empty() {
        title
    } else {
        return Err("herdr tab label is empty".into());
    };
    let suffix = format!(" [{tab_id}]");
    if suffix.chars().count() > MAX_THREAD_NAME_LENGTH {
        return Err(format!(
            "herdr tab id {tab_id} is too long for a Discord thread"
        ));
    }
    let capacity = MAX_THREAD_NAME_LENGTH - suffix.chars().count();
    Ok(format!(
        "{}{}",
        base.chars().take(capacity).collect::<String>(),
        suffix
    ))
}
pub fn is_postable_transition(t: &Transition) -> bool {
    t.from == "working" && matches!(t.to.as_str(), "blocked" | "done" | "idle")
}
#[must_use]
pub fn watch_transitions(snapshots: &[&[(&str, &str)]]) -> Vec<Transition> {
    let Some(first) = snapshots.first() else {
        return Vec::new();
    };
    let mut prior: std::collections::HashMap<&str, &str> =
        std::collections::HashMap::from_iter(first.iter().copied());
    let mut changes = Vec::new();
    for snapshot in &snapshots[1..] {
        for (terminal, status) in snapshot.iter().copied() {
            if let Some(old) = prior.get(terminal).filter(|old| **old != status) {
                changes.push(Transition {
                    from: (*old).into(),
                    to: status.into(),
                    terminal_id: terminal.into(),
                    agent: "unknown".into(),
                });
            }
        }
        prior = std::collections::HashMap::from_iter(snapshot.iter().copied());
    }
    changes
}
#[must_use]
pub fn read_activity_fixture(path: &str) -> String {
    match std::fs::read_to_string(path) {
        Ok(value) => value,
        Err(error) => format!("activity read error: {error}"),
    }
}

pub fn request_rpc(method: &str) -> String {
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
        let mut stream = UnixStream::connect(path).map_err(|e| e.to_string())?;
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
    result.map_or_else(|e| e, |v| v.to_string())
}
#[must_use]
pub fn tab_list() -> Vec<String> {
    let value: Value = serde_json::from_str(&request_rpc("tab.list")).unwrap_or(Value::Null);
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

#[derive(Deserialize)]
struct RpcAgent {
    agent: String,
    terminal_id: String,
    agent_status: String,
}
pub fn list_agents() -> Result<Vec<(String, String, String)>, String> {
    let value: Value =
        serde_json::from_str(&request_rpc("agent.list")).map_err(|e| e.to_string())?;
    value
        .get("agents")
        .and_then(Value::as_array)
        .ok_or_else(|| "agent.list response did not contain agents".into())
        .map(|a| {
            a.iter()
                .filter_map(|v| {
                    serde_json::from_value::<RpcAgent>(v.clone())
                        .ok()
                        .map(|x| (x.agent, x.terminal_id, x.agent_status))
                })
                .collect()
        })
}

pub async fn sync_topology(
    client: &twilight_http::Client,
    guild: twilight_model::id::Id<twilight_model::id::marker::GuildMarker>,
    workspace: &str,
    tab: &str,
) -> Result<(), String> {
    let channels = client
        .guild_channels(guild)
        .await
        .map_err(|error| error.to_string())?
        .model()
        .await
        .map_err(|error| error.to_string())?;
    let workspace_name = format!("herdr-{workspace}");
    let tab_name = format!("{workspace_name}-{tab}");
    if !channels
        .iter()
        .any(|channel| channel.name.as_deref() == Some(workspace_name.as_str()))
    {
        client
            .create_guild_channel(guild, &workspace_name)
            .await
            .map_err(|error| error.to_string())?;
    }
    if !channels
        .iter()
        .any(|channel| channel.name.as_deref() == Some(tab_name.as_str()))
    {
        client
            .create_guild_channel(guild, &tab_name)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
pub async fn deliver_transition(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    content: &str,
    _nonce: &str,
) -> Result<(), String> {
    client
        .create_message(channel)
        .content(content)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}
pub async fn update_live_status(
    client: &twilight_http::Client,
    channel: twilight_model::id::Id<twilight_model::id::marker::ChannelMarker>,
    terminal: &str,
    message: Option<twilight_model::id::Id<twilight_model::id::marker::MessageMarker>>,
) -> Result<(), String> {
    let content = format!("{terminal} working");
    if let Some(message) = message {
        client
            .delete_message(channel, message)
            .await
            .map_err(|error| error.to_string())?;
    }
    client
        .create_message(channel)
        .content(&content)
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}
