use crate::herdr::AgentSession;
use rusqlite::{Connection, OpenFlags};
use serde::de::Error as _;
use serde_json::Value;
use std::path::Path;

const POINTER: &str = "agent stopped, no log available";

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AgentLog {
    pub message: String,
    pub question: Option<String>,
    pub failure: Option<String>,
}

/// Reads and parses one vendor session log.
///
/// # Errors
///
/// Returns the stable pointer error when the session, file, or parsed response is unavailable.
pub fn read_agent_log(session: Option<AgentSession>, path: &Path) -> Result<AgentLog, String> {
    let session = session.ok_or_else(|| POINTER.to_owned())?;
    if session.agent == "cursor" {
        if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            let bytes = std::fs::read(path).map_err(|_| POINTER.to_owned())?;
            let value: Value = serde_json::from_slice(&bytes).map_err(|_| POINTER.to_owned())?;
            return parse_cursor_json(&value).map_err(|_| POINTER.to_owned());
        }
        return parse_cursor_path(path).map_err(|_| POINTER.to_owned());
    }
    let bytes = std::fs::read(path).map_err(|_| POINTER.to_owned())?;
    let text = String::from_utf8(bytes).map_err(|_| POINTER.to_owned())?;
    match session.agent.as_str() {
        "claude" => parse_claude(&text),
        "codex" => parse_codex(&text),
        _ => Err(serde_json::Error::custom(POINTER)),
    }
    .map_err(|_| POINTER.to_owned())
}

fn parse_cursor_path(path: &Path) -> Result<AgentLog, serde_json::Error> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|_| serde_json::Error::custom("invalid cursor log"))?;
    let mut statement = connection
        .prepare("SELECT data FROM blobs ORDER BY rowid")
        .map_err(|_| serde_json::Error::custom("invalid cursor log"))?;
    let rows: Vec<Value> = statement
        .query_map([], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            Ok(serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null))
        })
        .map_err(|_| serde_json::Error::custom("invalid cursor log"))?
        .filter_map(Result::ok)
        .collect();
    parse_cursor_rows(&rows)
}

fn lines(text: &str) -> Result<Vec<Value>, serde_json::Error> {
    let relevant: Vec<&str> = text
        .lines()
        .filter(|line| !line.trim().is_empty())
        .collect();
    let last = relevant.len().saturating_sub(1);
    let mut records = Vec::with_capacity(relevant.len());
    for (i, line) in relevant.into_iter().enumerate() {
        match serde_json::from_str(line) {
            Ok(value) => records.push(value),
            Err(_) if i == last => break,
            Err(err) => return Err(err),
        }
    }
    Ok(records)
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
    let answered: std::collections::HashSet<&str> = tail
        .iter()
        .flat_map(|record| {
            record
                .get("message")
                .and_then(|message| message.get("content"))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|part| {
            (part.get("type").and_then(Value::as_str) == Some("tool_result"))
                .then(|| part.get("tool_use_id").and_then(Value::as_str))
                .flatten()
        })
        .collect();
    let mut message = None;
    let mut question = None;
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
                        message = part.get("text").and_then(Value::as_str).map(str::to_owned);
                    }
                    Some("tool_use")
                        if part.get("name").and_then(Value::as_str) == Some("AskUserQuestion") =>
                    {
                        if !part
                            .get("id")
                            .and_then(Value::as_str)
                            .is_some_and(|id| answered.contains(id))
                        {
                            question = format_question(part.get("input"));
                        }
                    }
                    Some("tool_result") if part.get("is_error") == Some(&Value::Bool(true)) => {
                        failure = part
                            .get("content")
                            .and_then(Value::as_str)
                            .map(str::to_owned);
                    }
                    _ => {}
                }
            }
        }
        if record.get("type").and_then(Value::as_str) == Some("assistant")
            && let Some(s) = record.get("message").and_then(Value::as_str)
        {
            message = Some(s.to_owned());
        }
    }
    let message = message
        .filter(|text| !text.is_empty())
        .or_else(|| question.clone())
        .ok_or_else(|| serde_json::Error::custom("empty assistant message"))?;
    Ok(AgentLog {
        message,
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
        question: None,
        failure,
    })
}
fn parse_cursor_rows(rows: &[Value]) -> Result<AgentLog, serde_json::Error> {
    let records: Vec<Value> = rows
        .iter()
        .filter_map(|row| row.get("data").cloned().or_else(|| Some(row.clone())))
        .collect();
    let start = records
        .iter()
        .rposition(|row| {
            row.get("role").and_then(Value::as_str) == Some("user")
                && row.get("content").is_some_and(Value::is_array)
        })
        .map_or(0, |i| i + 1);
    let tail = &records[start..];
    let message = tail
        .iter()
        .rev()
        .find_map(|row| {
            row.get("content")
                .and_then(Value::as_array)
                .and_then(|parts| {
                    parts
                        .iter()
                        .rev()
                        .find_map(|part| part.get("text").and_then(Value::as_str))
                })
        })
        .ok_or_else(|| serde_json::Error::custom("empty assistant message"))?;
    Ok(AgentLog {
        message: message.into(),
        question: None,
        failure: None,
    })
}

fn parse_cursor_json(value: &Value) -> Result<AgentLog, serde_json::Error> {
    let rows = value
        .as_array()
        .ok_or_else(|| serde_json::Error::custom("invalid cursor log"))?;
    let start = rows
        .iter()
        .rposition(|row| {
            row.get("data")
                .and_then(|d| d.get("role"))
                .and_then(Value::as_str)
                == Some("user")
                && row
                    .get("data")
                    .and_then(|d| d.get("content"))
                    .is_some_and(Value::is_array)
        })
        .map_or(0, |i| i + 1);
    let tail = &rows[start..];
    let message = tail
        .iter()
        .rev()
        .find_map(|row| {
            row.get("data")
                .and_then(|d| d.get("content"))
                .and_then(Value::as_array)
                .and_then(|parts| {
                    parts
                        .iter()
                        .rev()
                        .find_map(|p| p.get("text").and_then(Value::as_str))
                })
        })
        .filter(|s| !s.is_empty())
        .ok_or_else(|| serde_json::Error::custom("empty assistant message"))?;
    Ok(AgentLog {
        message: message.into(),
        question: None,
        failure: None,
    })
}

fn format_question(input: Option<&Value>) -> Option<String> {
    let rendered: Vec<String> = input?
        .get("questions")?
        .as_array()?
        .iter()
        .filter_map(|item| {
            let question = item.get("question").and_then(Value::as_str)?;
            let mut lines = vec![question.to_owned()];
            if let Some(options) = item.get("options").and_then(Value::as_array) {
                lines.extend(options.iter().enumerate().map(|(i, option)| {
                    format!(
                        "{}. {}",
                        i + 1,
                        option.get("label").and_then(Value::as_str).unwrap_or("")
                    )
                }));
            }
            Some(lines)
        })
        .flatten()
        .collect();
    (!rendered.is_empty()).then(|| rendered.join("\n"))
}
