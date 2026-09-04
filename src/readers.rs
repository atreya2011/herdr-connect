use std::path::Path;

use rusqlite::{Connection, OpenFlags};
use serde::de::Error as _;
use serde_json::Value;

use crate::herdr::AgentSession;
use crate::permission::{VENDOR_CLAUDE, VENDOR_CODEX, VENDOR_CURSOR};

const POINTER: &str = "agent stopped, no log available";
/// Key for a vendor log record's own type field (Claude: `user`/`assistant`; Codex: `turn_context`/`event_msg`).
const RECORD_TYPE_KEY: &str = "type";
/// Key for a Claude message content part's type field (e.g. `text`, `tool_use`, `tool_result`).
const CONTENT_PART_TYPE_KEY: &str = "type";
/// Key for a Codex payload's type field (e.g. `task_started`, `turn_aborted`).
const PAYLOAD_TYPE_KEY: &str = "type";
const CONTENT_KEY: &str = "content";
/// Key holding a Claude/Codex/Cursor content part's own text payload.
const TEXT_KEY: &str = "text";
/// The `text` content part's type-discriminant value.
const CONTENT_PART_TEXT_VALUE: &str = "text";
const MESSAGE_KEY: &str = "message";
const PAYLOAD_KEY: &str = "payload";
/// Value of a Cursor row's `role` field marking it as user-authored.
const USER_ROLE_VALUE: &str = "user";
/// Value of a Claude record's own `type` field marking it as user-authored.
const USER_RECORD_TYPE_VALUE: &str = "user";
const ROW_DATA_KEY: &str = "data";

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
pub fn read_agent_log(session: Option<&AgentSession>, path: &Path) -> Result<AgentLog, String> {
    let session = session.ok_or_else(|| POINTER.to_owned())?;
    if session.agent == VENDOR_CURSOR {
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
        VENDOR_CLAUDE => parse_claude(&text),
        VENDOR_CODEX => parse_codex(&text),
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
            if r.get(RECORD_TYPE_KEY).and_then(Value::as_str) != Some(USER_RECORD_TYPE_VALUE)
                || r.get("isMeta") == Some(&Value::Bool(true))
            {
                return false;
            }
            let content = r.get(MESSAGE_KEY).and_then(|m| m.get(CONTENT_KEY));
            content.is_some_and(|c| {
                c.is_string()
                    || c.as_array().is_some_and(|parts| {
                        parts.iter().any(|p| {
                            p.get(CONTENT_PART_TYPE_KEY).and_then(Value::as_str)
                                == Some(CONTENT_PART_TEXT_VALUE)
                        })
                    })
            })
        })
        .map_or(0, |i| i + 1);
    let tail = &records[start..];
    let answered: std::collections::HashSet<&str> = tail
        .iter()
        .flat_map(|record| {
            record
                .get(MESSAGE_KEY)
                .and_then(|message| message.get(CONTENT_KEY))
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
        })
        .filter_map(|part| {
            (part.get(CONTENT_PART_TYPE_KEY).and_then(Value::as_str) == Some("tool_result"))
                .then(|| part.get("tool_use_id").and_then(Value::as_str))
                .flatten()
        })
        .collect();
    let mut message = None;
    let mut question = None;
    let mut failure = None;
    for record in tail {
        if let Some(contents) = record
            .get(MESSAGE_KEY)
            .and_then(|m| m.get(CONTENT_KEY))
            .and_then(Value::as_array)
        {
            for part in contents {
                match part.get(CONTENT_PART_TYPE_KEY).and_then(Value::as_str) {
                    Some(CONTENT_PART_TEXT_VALUE) => {
                        message = part
                            .get(TEXT_KEY)
                            .and_then(Value::as_str)
                            .map(str::to_owned);
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
                            .get(CONTENT_KEY)
                            .and_then(Value::as_str)
                            .map(str::to_owned);
                    }
                    _ => {}
                }
            }
        }
        if record.get(RECORD_TYPE_KEY).and_then(Value::as_str) == Some("assistant")
            && let Some(s) = record.get(MESSAGE_KEY).and_then(Value::as_str)
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
            r.get(RECORD_TYPE_KEY).and_then(Value::as_str) == Some("turn_context")
                || (r.get(RECORD_TYPE_KEY).and_then(Value::as_str) == Some("event_msg")
                    && r.get(PAYLOAD_KEY)
                        .and_then(|p| p.get(PAYLOAD_TYPE_KEY))
                        .and_then(Value::as_str)
                        == Some("task_started"))
        })
        .map_or(0, |i| i + 1);
    let tail = &records[start..];
    let message = tail
        .iter()
        .rev()
        .find_map(|r| {
            r.get(PAYLOAD_KEY)
                .and_then(|p| p.get("last_agent_message").or_else(|| p.get(MESSAGE_KEY)))
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .or_else(|| {
            tail.iter().rev().find_map(|r| {
                r.get(PAYLOAD_KEY)
                    .and_then(|p| p.get(CONTENT_KEY))
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(|x| x.get(TEXT_KEY).and_then(Value::as_str))
                            .collect::<String>()
                    })
                    .filter(|s| !s.is_empty())
            })
        })
        .ok_or_else(|| serde_json::Error::custom("empty assistant message"))?;
    let failure = tail.iter().rev().find_map(|r| {
        (r.get(PAYLOAD_KEY)
            .and_then(|p| p.get(PAYLOAD_TYPE_KEY))
            .and_then(Value::as_str)
            == Some("turn_aborted"))
        .then(|| {
            r.get(PAYLOAD_KEY)
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
        .filter_map(|row| row.get(ROW_DATA_KEY).cloned().or_else(|| Some(row.clone())))
        .collect();
    let start = records
        .iter()
        .rposition(|row| {
            row.get("role").and_then(Value::as_str) == Some(USER_ROLE_VALUE)
                && row.get(CONTENT_KEY).is_some_and(Value::is_array)
        })
        .map_or(0, |i| i + 1);
    let tail = &records[start..];
    let message = tail
        .iter()
        .rev()
        .find_map(|row| {
            row.get(CONTENT_KEY)
                .and_then(Value::as_array)
                .and_then(|parts| {
                    parts
                        .iter()
                        .rev()
                        .find_map(|part| part.get(TEXT_KEY).and_then(Value::as_str))
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
            row.get(ROW_DATA_KEY)
                .and_then(|d| d.get("role"))
                .and_then(Value::as_str)
                == Some(USER_ROLE_VALUE)
                && row
                    .get(ROW_DATA_KEY)
                    .and_then(|d| d.get(CONTENT_KEY))
                    .is_some_and(Value::is_array)
        })
        .map_or(0, |i| i + 1);
    let tail = &rows[start..];
    let message = tail
        .iter()
        .rev()
        .find_map(|row| {
            row.get(ROW_DATA_KEY)
                .and_then(|d| d.get(CONTENT_KEY))
                .and_then(Value::as_array)
                .and_then(|parts| {
                    parts
                        .iter()
                        .rev()
                        .find_map(|p| p.get(TEXT_KEY).and_then(Value::as_str))
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

/// Extracts a pending dialog question from a Herdr detection snapshot.
///
/// The question is the block between the last two horizontal rules (lines made only of
/// U+2500), with the checkbox header line and the `❯` cursor glyph dropped.
#[must_use]
pub fn format_detection_question(text: &str) -> Option<String> {
    let lines: Vec<&str> = text.lines().collect();
    let rule_lines: Vec<usize> = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| {
            let trimmed = line.trim();
            (!trimmed.is_empty() && trimmed.chars().all(|ch| ch == '─')).then_some(index)
        })
        .collect();
    if rule_lines.len() < 2 {
        return None;
    }
    let header_rule = rule_lines[rule_lines.len() - 2];
    let last_rule = rule_lines[rule_lines.len() - 1];
    let block: Vec<String> = lines
        .get(header_rule + 2..last_rule)?
        .iter()
        .map(|line| normalize_dialog_line(line))
        .collect();
    let start = block.iter().position(|line| !line.trim().is_empty())?;
    let end = block.iter().rposition(|line| !line.trim().is_empty())? + 1;
    Some(block[start..end].join("\n"))
}

fn normalize_dialog_line(line: &str) -> String {
    if let Some(stripped) = line.strip_prefix("❯ ") {
        return stripped.to_owned();
    }
    let trimmed = line.trim_start();
    let leading_spaces = line.len() - trimmed.len();
    if leading_spaces == 2 && starts_with_option_number(trimmed) {
        trimmed.to_owned()
    } else {
        line.to_owned()
    }
}

fn starts_with_option_number(text: &str) -> bool {
    let digits_end = text.find(|ch: char| !ch.is_ascii_digit()).unwrap_or(0);
    digits_end > 0 && text[digits_end..].starts_with('.')
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

#[cfg(test)]
mod tests {
    use super::format_detection_question;

    #[test]
    fn detection_snapshot_with_a_pending_dialog_yields_the_question_and_options() {
        let text = include_str!("../tests/fixtures/claude-detection-blocked-question.txt");
        assert_eq!(
            format_detection_question(text).as_deref(),
            Some(
                "Which color do you prefer?\n\n1. Red\n     The color red\n2. Blue\n     The color blue\n3. Type something."
            )
        );
    }

    #[test]
    fn detection_snapshot_with_no_dialog_yields_none() {
        let text = include_str!("../tests/fixtures/claude-detection-no-dialog.txt");
        assert_eq!(format_detection_question(text), None);
    }
}
