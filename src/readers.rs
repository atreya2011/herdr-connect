use std::path::Path;

use rusqlite::{Connection, OpenFlags};
use serde::de::Error as _;
use serde_json::Value;

use crate::herdr::AgentSession;
use crate::permission::{VENDOR_CLAUDE, VENDOR_CODEX, VENDOR_CURSOR};
use crate::question::Question;

const RECORD_TYPE_KEY: &str = "type";
const CONTENT_PART_TYPE_KEY: &str = "type";
const PAYLOAD_TYPE_KEY: &str = "type";
const CONTENT_KEY: &str = "content";
const TEXT_KEY: &str = "text";
const CONTENT_PART_TEXT_VALUE: &str = "text";
const MESSAGE_KEY: &str = "message";
const PAYLOAD_KEY: &str = "payload";
const ROLE_KEY: &str = "role";
const USER_ROLE_VALUE: &str = "user";
const ASSISTANT_ROLE_VALUE: &str = "assistant";
const ASSISTANT_RECORD_TYPE_VALUE: &str = "assistant";
const EVENT_MSG_RECORD_TYPE_VALUE: &str = "event_msg";
const USER_RECORD_TYPE_VALUE: &str = "user";

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AgentLog {
    pub message: String,
    pub question: Option<String>,
    pub pending_questions: Vec<Question>,
    pub failure: Option<String>,
}

pub fn read_agent_log(session: &AgentSession, path: &Path) -> Result<AgentLog, String> {
    let failed = |error: &dyn std::fmt::Display| format!("{}: {error}", path.display());
    if session.agent == VENDOR_CURSOR {
        return parse_cursor_path(path).map_err(|error| failed(&error));
    }
    let bytes = std::fs::read(path).map_err(|error| failed(&error))?;
    let text = String::from_utf8(bytes).map_err(|error| failed(&error))?;
    match session.agent.as_str() {
        VENDOR_CLAUDE => parse_claude(&text),
        VENDOR_CODEX => parse_codex(&text),
        _ => Err(serde_json::Error::custom(format!(
            "unsupported vendor {}",
            session.agent
        ))),
    }
    .map_err(|error| failed(&error))
}

fn read_new_bytes(path: &Path, offset: u64) -> Result<(Vec<u8>, usize), String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = std::fs::File::open(path).map_err(|error| error.to_string())?;
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    let complete_len = bytes
        .iter()
        .rposition(|&byte| byte == b'\n')
        .map_or(0, |index| index + 1);
    Ok((bytes, complete_len))
}

struct PositionedLine<'a> {
    text: &'a str,
    end_offset: u64,
}

fn positioned_complete_lines(text: &str, start_offset: u64) -> Vec<PositionedLine<'_>> {
    let mut out = Vec::new();
    let mut consumed: u64 = 0;
    for line in text.split_terminator('\n') {
        consumed += line.len() as u64 + 1;
        if !line.trim().is_empty() {
            out.push(PositionedLine {
                text: line,
                end_offset: start_offset + consumed,
            });
        }
    }
    out
}

fn extract_tolerant(
    lines: &[PositionedLine<'_>],
    start_offset: u64,
    extract: impl Fn(&Value) -> Vec<String>,
) -> Result<(Vec<(String, u64)>, u64), String> {
    let mut texts = Vec::new();
    let mut new_offset = start_offset;
    for (index, line) in lines.iter().enumerate() {
        match serde_json::from_str::<Value>(line.text) {
            Ok(record) => {
                for text in extract(&record) {
                    texts.push((text, line.end_offset));
                }
                new_offset = line.end_offset;
            }
            Err(_) if index + 1 == lines.len() => break,
            Err(error) => return Err(error.to_string()),
        }
    }
    Ok((texts, new_offset))
}

pub fn read_claude_incremental(
    path: &Path,
    offset: u64,
) -> Result<(Vec<(String, u64)>, u64), String> {
    let (bytes, complete_len) = read_new_bytes(path, offset)?;
    let text = std::str::from_utf8(&bytes[..complete_len]).map_err(|error| error.to_string())?;
    let lines = positioned_complete_lines(text, offset);
    extract_tolerant(&lines, offset, |record| {
        if record.get(RECORD_TYPE_KEY).and_then(Value::as_str) != Some(ASSISTANT_RECORD_TYPE_VALUE)
        {
            return Vec::new();
        }
        record
            .get(MESSAGE_KEY)
            .and_then(|message| message.get(CONTENT_KEY))
            .and_then(Value::as_array)
            .map(|parts| {
                parts
                    .iter()
                    .filter(|part| {
                        part.get(CONTENT_PART_TYPE_KEY).and_then(Value::as_str)
                            == Some(CONTENT_PART_TEXT_VALUE)
                    })
                    .filter_map(|part| part.get(TEXT_KEY).and_then(Value::as_str))
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    })
}

pub fn read_codex_incremental(
    path: &Path,
    offset: u64,
) -> Result<(Vec<(String, u64)>, u64), String> {
    let (bytes, complete_len) = read_new_bytes(path, offset)?;
    let text = std::str::from_utf8(&bytes[..complete_len]).map_err(|error| error.to_string())?;
    let lines = positioned_complete_lines(text, offset);
    extract_tolerant(&lines, offset, |record| {
        let Some(payload) = record.get(PAYLOAD_KEY) else {
            return Vec::new();
        };
        if record.get(RECORD_TYPE_KEY).and_then(Value::as_str) != Some("response_item")
            || payload.get(PAYLOAD_TYPE_KEY).and_then(Value::as_str) != Some("message")
            || payload.get(ROLE_KEY).and_then(Value::as_str) != Some(ASSISTANT_ROLE_VALUE)
        {
            return Vec::new();
        }
        payload
            .get(CONTENT_KEY)
            .and_then(Value::as_array)
            .map(|parts| {
                parts
                    .iter()
                    .filter(|part| {
                        part.get(CONTENT_PART_TYPE_KEY).and_then(Value::as_str)
                            == Some("output_text")
                    })
                    .filter_map(|part| part.get(TEXT_KEY).and_then(Value::as_str))
                    .filter(|text| !text.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    })
}

const COMPACT_SUMMARY_KEY: &str = "isCompactSummary";

const INJECTED_CLAUDE_TEXT_PREFIXES: &[&str] = &[
    "<command-name>",
    "<command-message>",
    "<bash-input>",
    "<local-command-stdout>",
    "<bash-stdout>",
    "<bash-stderr>",
    "<task-notification>",
    "[Request interrupted",
];

fn is_owner_typed_claude_text(text: &str) -> bool {
    !INJECTED_CLAUDE_TEXT_PREFIXES
        .iter()
        .any(|prefix| text.starts_with(prefix))
}

pub fn read_claude_prompts_incremental(
    path: &Path,
    offset: u64,
) -> Result<(Vec<(String, u64)>, u64), String> {
    let (bytes, complete_len) = read_new_bytes(path, offset)?;
    let text = std::str::from_utf8(&bytes[..complete_len]).map_err(|error| error.to_string())?;
    let lines = positioned_complete_lines(text, offset);
    extract_tolerant(&lines, offset, |record| {
        let Some(content) = record
            .get(MESSAGE_KEY)
            .and_then(|message| message.get(CONTENT_KEY))
        else {
            return Vec::new();
        };
        if !is_qualifying_claude_user_record(record)
            || record.get(COMPACT_SUMMARY_KEY) == Some(&Value::Bool(true))
        {
            return Vec::new();
        }
        let texts: Vec<String> = match content {
            Value::String(text) if !text.is_empty() => vec![text.clone()],
            Value::Array(parts) => parts
                .iter()
                .filter(|part| {
                    part.get(CONTENT_PART_TYPE_KEY).and_then(Value::as_str)
                        == Some(CONTENT_PART_TEXT_VALUE)
                })
                .filter_map(|part| part.get(TEXT_KEY).and_then(Value::as_str))
                .filter(|text| !text.is_empty())
                .map(str::to_owned)
                .collect(),
            _ => Vec::new(),
        };
        texts
            .into_iter()
            .filter(|text| is_owner_typed_claude_text(text))
            .collect()
    })
}

const INJECTED_CODEX_TEXT_PREFIXES: &[&str] = &[
    "<environment_context>",
    "# AGENTS.md",
    "<user_instructions>",
    "<subagent_notification>",
    "<turn_aborted>",
];

fn is_owner_typed_codex_text(text: &str) -> bool {
    !INJECTED_CODEX_TEXT_PREFIXES
        .iter()
        .any(|prefix| text.starts_with(prefix))
}

pub fn read_codex_prompts_incremental(
    path: &Path,
    offset: u64,
) -> Result<(Vec<(String, u64)>, u64), String> {
    let (bytes, complete_len) = read_new_bytes(path, offset)?;
    let text = std::str::from_utf8(&bytes[..complete_len]).map_err(|error| error.to_string())?;
    let lines = positioned_complete_lines(text, offset);
    extract_tolerant(&lines, offset, |record| {
        let Some(payload) = record.get(PAYLOAD_KEY) else {
            return Vec::new();
        };
        if record.get(RECORD_TYPE_KEY).and_then(Value::as_str) != Some("response_item")
            || payload.get(PAYLOAD_TYPE_KEY).and_then(Value::as_str) != Some("message")
            || payload.get(ROLE_KEY).and_then(Value::as_str) != Some(USER_ROLE_VALUE)
        {
            return Vec::new();
        }
        payload
            .get(CONTENT_KEY)
            .and_then(Value::as_array)
            .map(|parts| {
                parts
                    .iter()
                    .filter(|part| {
                        part.get(CONTENT_PART_TYPE_KEY).and_then(Value::as_str)
                            == Some("input_text")
                    })
                    .filter_map(|part| part.get(TEXT_KEY).and_then(Value::as_str))
                    .filter(|text| !text.is_empty())
                    .filter(|text| is_owner_typed_codex_text(text))
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    })
}

pub fn read_cursor_incremental(
    path: &Path,
    last_rowid: i64,
) -> Result<(Vec<(String, i64)>, i64), String> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| error.to_string())?;
    let mut statement = connection
        .prepare("SELECT rowid, data FROM blobs WHERE rowid > ?1 ORDER BY rowid")
        .map_err(|error| error.to_string())?;
    let rows: Vec<(i64, Value)> = statement
        .query_map([last_rowid], |row| {
            let rowid: i64 = row.get(0)?;
            let bytes: Vec<u8> = row.get(1)?;
            Ok((
                rowid,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            ))
        })
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let mut new_last_rowid = last_rowid;
    let mut texts = Vec::new();
    for (rowid, record) in rows {
        new_last_rowid = rowid;
        let data = &record;
        if data.get(ROLE_KEY).and_then(Value::as_str) != Some(ASSISTANT_ROLE_VALUE) {
            continue;
        }
        let Some(parts) = data.get(CONTENT_KEY).and_then(Value::as_array) else {
            continue;
        };
        for part in parts {
            if part.get(CONTENT_PART_TYPE_KEY).and_then(Value::as_str)
                == Some(CONTENT_PART_TEXT_VALUE)
                && let Some(part_text) = part.get(TEXT_KEY).and_then(Value::as_str)
                && !part_text.is_empty()
            {
                texts.push((part_text.to_owned(), rowid));
            }
        }
    }
    Ok((texts, new_last_rowid))
}

fn strip_cursor_user_query_wrapper(text: &str) -> &str {
    const OPEN_TAG: &str = "<user_query>";
    const CLOSE_TAG: &str = "</user_query>";
    let Some(after_open) = text
        .find(OPEN_TAG)
        .map(|start| &text[start + OPEN_TAG.len()..])
    else {
        return text;
    };
    after_open
        .find(CLOSE_TAG)
        .map_or(text, |end| after_open[..end].trim())
}

pub fn read_cursor_prompts_incremental(
    path: &Path,
    last_rowid: i64,
) -> Result<(Vec<(String, i64)>, i64), String> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| error.to_string())?;
    let mut statement = connection
        .prepare("SELECT rowid, data FROM blobs WHERE rowid > ?1 ORDER BY rowid")
        .map_err(|error| error.to_string())?;
    let rows: Vec<(i64, Value)> = statement
        .query_map([last_rowid], |row| {
            let rowid: i64 = row.get(0)?;
            let bytes: Vec<u8> = row.get(1)?;
            Ok((
                rowid,
                serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null),
            ))
        })
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    let mut new_last_rowid = last_rowid;
    let mut prompts = Vec::new();
    for (rowid, record) in rows {
        new_last_rowid = rowid;
        let data = &record;
        if data.get(ROLE_KEY).and_then(Value::as_str) != Some(USER_ROLE_VALUE) {
            continue;
        }
        let Some(parts) = data.get(CONTENT_KEY).and_then(Value::as_array) else {
            continue;
        };
        for part in parts {
            if part.get(CONTENT_PART_TYPE_KEY).and_then(Value::as_str)
                == Some(CONTENT_PART_TEXT_VALUE)
                && let Some(part_text) = part.get(TEXT_KEY).and_then(Value::as_str)
            {
                let prompt_text = strip_cursor_user_query_wrapper(part_text);
                if !prompt_text.is_empty() {
                    prompts.push((prompt_text.to_owned(), rowid));
                }
            }
        }
    }
    Ok((prompts, new_last_rowid))
}

fn parse_cursor_path(path: &Path) -> Result<AgentLog, String> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| error.to_string())?;
    let mut statement = connection
        .prepare("SELECT data FROM blobs ORDER BY rowid")
        .map_err(|error| error.to_string())?;
    let rows: Vec<Value> = statement
        .query_map([], |row| {
            let bytes: Vec<u8> = row.get(0)?;
            Ok(serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null))
        })
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    parse_cursor_rows(&rows).map_err(|error| error.to_string())
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
fn is_qualifying_claude_user_record(record: &Value) -> bool {
    if record.get(RECORD_TYPE_KEY).and_then(Value::as_str) != Some(USER_RECORD_TYPE_VALUE)
        || record.get("isMeta") == Some(&Value::Bool(true))
    {
        return false;
    }
    let content = record.get(MESSAGE_KEY).and_then(|m| m.get(CONTENT_KEY));
    content.is_some_and(|c| {
        c.is_string()
            || c.as_array().is_some_and(|parts| {
                parts.iter().any(|p| {
                    p.get(CONTENT_PART_TYPE_KEY).and_then(Value::as_str)
                        == Some(CONTENT_PART_TEXT_VALUE)
                })
            })
    })
}

fn parse_claude(text: &str) -> Result<AgentLog, serde_json::Error> {
    let records = lines(text)?;
    let start = records
        .iter()
        .rposition(is_qualifying_claude_user_record)
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
    let mut pending_questions = Vec::new();
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
                            pending_questions = part
                                .get("input")
                                .and_then(|input| input.get("questions"))
                                .and_then(|questions| {
                                    serde_json::from_value(questions.clone()).ok()
                                })
                                .unwrap_or_default();
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
    }
    let message = message
        .filter(|text| !text.is_empty())
        .or_else(|| question.clone())
        .ok_or_else(|| serde_json::Error::custom("empty assistant message"))?;
    Ok(AgentLog {
        message,
        question,
        pending_questions,
        failure,
    })
}
fn is_codex_turn_boundary_record(record: &Value) -> bool {
    record.get(RECORD_TYPE_KEY).and_then(Value::as_str) == Some("turn_context")
        || (record.get(RECORD_TYPE_KEY).and_then(Value::as_str)
            == Some(EVENT_MSG_RECORD_TYPE_VALUE)
            && record
                .get(PAYLOAD_KEY)
                .and_then(|p| p.get(PAYLOAD_TYPE_KEY))
                .and_then(Value::as_str)
                == Some("task_started"))
}

fn parse_codex(text: &str) -> Result<AgentLog, serde_json::Error> {
    let records = lines(text)?;
    let start = records
        .iter()
        .rposition(is_codex_turn_boundary_record)
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
        let payload = r.get(PAYLOAD_KEY)?;
        (payload.get(PAYLOAD_TYPE_KEY).and_then(Value::as_str) == Some("turn_aborted"))
            .then(|| payload.get("reason").and_then(Value::as_str))?
            .map(str::to_owned)
    });
    Ok(AgentLog {
        message,
        question: None,
        pending_questions: Vec::new(),
        failure,
    })
}

fn parse_cursor_rows(rows: &[Value]) -> Result<AgentLog, serde_json::Error> {
    let start = rows
        .iter()
        .rposition(|row| {
            row.get("role").and_then(Value::as_str) == Some(USER_ROLE_VALUE)
                && row.get(CONTENT_KEY).is_some_and(Value::is_array)
        })
        .map_or(0, |i| i + 1);
    let tail = &rows[start..];
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
        pending_questions: Vec::new(),
        failure: None,
    })
}

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
                for (i, option) in options.iter().enumerate() {
                    let label = option.get("label").and_then(Value::as_str)?;
                    lines.push(format!("{}. {label}", i + 1));
                }
            }
            Some(lines)
        })
        .flatten()
        .collect();
    (!rendered.is_empty()).then(|| rendered.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::{format_detection_question, parse_claude};
    use crate::question::{Question, QuestionOption};

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

    const PENDING_QUESTION_LOG: &str = concat!(
        r#"{"type":"user","message":{"role":"user","content":[{"type":"text","text":"ask"}]}}"#,
        "\n",
        r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","name":"AskUserQuestion","id":"t1","input":{"questions":[{"question":"Which toppings?","header":"Toppings","options":[{"label":"Cheese","description":"Add cheese"},{"label":"Olives","description":"Add olives"}],"multiSelect":true}]}}]}}"#,
        "\n",
    );

    #[test]
    fn a_pending_ask_user_question_yields_its_structured_questions() {
        let log = parse_claude(PENDING_QUESTION_LOG).expect("log parses");
        assert_eq!(
            log.pending_questions,
            vec![Question {
                question: "Which toppings?".to_owned(),
                header: "Toppings".to_owned(),
                options: vec![
                    QuestionOption {
                        label: "Cheese".to_owned(),
                        description: "Add cheese".to_owned(),
                    },
                    QuestionOption {
                        label: "Olives".to_owned(),
                        description: "Add olives".to_owned(),
                    },
                ],
                multi_select: true,
            }]
        );
    }

    #[test]
    fn an_answered_ask_user_question_yields_no_structured_questions() {
        let answered = format!(
            "{PENDING_QUESTION_LOG}{}\n",
            r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}}"#
        );
        let log = parse_claude(&format!(
            "{answered}{}\n",
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"done"}]}}"#
        ))
        .expect("log parses");
        assert_eq!(log.pending_questions, Vec::new());
    }
}
