use serde::{Deserialize, Serialize};

/// The Claude hook event a tool-activity frame is derived from.
const ACTIVITY_HOOK_EVENT: &str = "PreToolUse";
const CURSOR_ACTIVITY_HOOK_EVENT: &str = "preToolUse";

/// Discriminates an activity frame from a permission
/// [`Interaction`](crate::permission::Interaction) on the shared broker socket.
pub const ACTIVITY_KIND: &str = "activity";

/// Characters kept from the derived tool summary.
const MAX_SUMMARY_CHARS: usize = 80;

/// One tool-activity update, forwarded from a harness hook to the bridge over the broker socket.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ActivityFrame {
    pub kind: String,
    pub vendor: String,
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
    pub session_id: String,
    pub tool: String,
    pub summary: String,
}

#[derive(Debug, Deserialize)]
struct ClaudePreToolUseRequest {
    session_id: String,
    hook_event_name: String,
    tool_name: String,
    tool_input: ClaudeToolUseInput,
}

#[derive(Debug, Deserialize)]
struct CodexPreToolUseRequest {
    session_id: String,
    hook_event_name: String,
    tool_name: String,
    #[serde(default)]
    tool_input: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct CursorPreToolUseRequest {
    conversation_id: String,
    hook_event_name: String,
    tool_name: String,
    agent_message: String,
}

#[derive(Debug, Deserialize)]
struct ClaudeToolUseInput {
    command: Option<String>,
    file_path: Option<String>,
    pattern: Option<String>,
    description: Option<String>,
}

/// One Claude `PreToolUse` hook payload decoded into its activity essentials. The caller supplies
/// the Herdr identity (workspace, tab, pane) from its own environment.
pub struct ClaudeActivityRequest {
    pub session_id: String,
    pub tool: String,
    pub summary: String,
}

/// Decodes one Claude `PreToolUse` hook payload into its activity essentials.
///
/// `summary` is the first [`MAX_SUMMARY_CHARS`] characters of `tool_input.command`, else
/// `file_path`, else `pattern`, else `description`, else empty.
///
/// # Errors
///
/// Returns an error when the payload is not JSON, is missing a required field, or names a
/// different hook event.
pub fn decode_claude_activity_request(input: &[u8]) -> Result<ClaudeActivityRequest, String> {
    let request: ClaudePreToolUseRequest =
        serde_json::from_slice(input).map_err(|error| error.to_string())?;
    if request.hook_event_name != ACTIVITY_HOOK_EVENT {
        return Err("unexpected Claude hook event".to_owned());
    }
    let summary = request
        .tool_input
        .command
        .as_deref()
        .or(request.tool_input.file_path.as_deref())
        .or(request.tool_input.pattern.as_deref())
        .or(request.tool_input.description.as_deref())
        .map(truncate_chars)
        .unwrap_or_default();
    Ok(ClaudeActivityRequest {
        session_id: request.session_id,
        tool: request.tool_name,
        summary,
    })
}

/// Decodes one Codex `PreToolUse` hook payload into its activity essentials.
///
/// `summary` is the first [`MAX_SUMMARY_CHARS`] characters of `tool_input.command`, else empty:
/// Codex's hook schema declares `tool_input` as any JSON value, so non-Bash tools carry no
/// `command`.
///
/// # Errors
///
/// Returns an error when the payload is not JSON, is missing a required field, or names a
/// different hook event.
pub fn decode_codex_activity_request(input: &[u8]) -> Result<ClaudeActivityRequest, String> {
    let request: CodexPreToolUseRequest =
        serde_json::from_slice(input).map_err(|error| error.to_string())?;
    if request.hook_event_name != ACTIVITY_HOOK_EVENT {
        return Err("unexpected Codex hook event".to_owned());
    }
    let summary = request
        .tool_input
        .as_object()
        .and_then(|fields| fields.get("command"))
        .and_then(serde_json::Value::as_str)
        .map(truncate_chars)
        .unwrap_or_default();
    Ok(ClaudeActivityRequest {
        session_id: request.session_id,
        tool: request.tool_name,
        summary,
    })
}

/// Decodes one Cursor `preToolUse` hook payload into the shared activity essentials.
///
/// # Errors
///
/// Returns an error when the payload is not JSON, is missing a required field, or names a
/// different hook event.
pub fn decode_cursor_activity_request(input: &[u8]) -> Result<ClaudeActivityRequest, String> {
    let request: CursorPreToolUseRequest =
        serde_json::from_slice(input).map_err(|error| error.to_string())?;
    if request.hook_event_name != CURSOR_ACTIVITY_HOOK_EVENT {
        return Err("unexpected Cursor hook event".to_owned());
    }
    Ok(ClaudeActivityRequest {
        session_id: request.conversation_id,
        tool: request.tool_name,
        summary: truncate_chars(&request.agent_message),
    })
}

fn truncate_chars(value: &str) -> String {
    value.chars().take(MAX_SUMMARY_CHARS).collect()
}

/// The Discord text for one pane's turn-scoped activity message: `⚙️ {count} · {tool}`, with the
/// summary appended after a colon when non-empty.
#[must_use]
pub fn activity_message_text(count: u32, tool: &str, summary: &str) -> String {
    if summary.is_empty() {
        format!("⚙️ {count} · {tool}")
    } else {
        format!("⚙️ {count} · {tool}: {summary}")
    }
}

#[cfg(test)]
mod tests {
    use super::{
        activity_message_text, decode_claude_activity_request, decode_codex_activity_request,
    };

    #[test]
    fn decodes_the_fallback_chain_in_priority_order() {
        let cases = [
            (
                r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{"command":"cmd","file_path":"fp","pattern":"pt","description":"de"}}"#,
                "cmd",
            ),
            (
                r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"Read","tool_input":{"file_path":"fp","pattern":"pt","description":"de"}}"#,
                "fp",
            ),
            (
                r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"Grep","tool_input":{"pattern":"pt","description":"de"}}"#,
                "pt",
            ),
            (
                r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"Task","tool_input":{"description":"de"}}"#,
                "de",
            ),
            (
                r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"TodoWrite","tool_input":{}}"#,
                "",
            ),
        ];
        for (payload, expected_summary) in cases {
            let request = decode_claude_activity_request(payload.as_bytes())
                .unwrap_or_else(|error| panic!("{payload} decodes: {error}"));
            assert_eq!(request.summary, expected_summary);
        }
    }

    #[test]
    fn truncates_the_summary_to_eighty_characters() {
        let long = "x".repeat(90);
        let payload = format!(
            r#"{{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{{"command":"{long}"}}}}"#
        );
        let request = decode_claude_activity_request(payload.as_bytes()).expect("decodes");
        assert_eq!(request.summary, "x".repeat(80));
    }

    #[test]
    fn rejects_a_different_hook_event() {
        let payload = r#"{"session_id":"s","hook_event_name":"PostToolUse","tool_name":"Bash","tool_input":{"command":"cmd"}}"#;
        assert!(decode_claude_activity_request(payload.as_bytes()).is_err());
    }

    #[test]
    fn rejects_malformed_json() {
        assert!(decode_claude_activity_request(b"{ malformed").is_err());
    }

    #[test]
    fn decodes_codex_command_from_tool_input() {
        let cases = [
            (
                r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{"command":"cmd","other":"zz"}}"#,
                "Bash",
                "cmd",
            ),
            (
                r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"apply_patch","tool_input":{"patch":"zz"}}"#,
                "apply_patch",
                "",
            ),
        ];
        for (payload, tool, expected_summary) in cases {
            let request = decode_codex_activity_request(payload.as_bytes())
                .unwrap_or_else(|error| panic!("{payload} decodes: {error}"));
            assert_eq!(request.tool, tool);
            assert_eq!(request.summary, expected_summary);
        }
    }

    #[test]
    fn rejects_a_claude_payload_without_tool_input() {
        let payload = r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"Bash"}"#;
        assert!(decode_claude_activity_request(payload.as_bytes()).is_err());
    }

    #[test]
    fn rejects_a_different_codex_hook_event() {
        let payload = r#"{"session_id":"s","hook_event_name":"PermissionRequest","tool_name":"Bash","tool_input":{"command":"cmd"}}"#;
        assert!(decode_codex_activity_request(payload.as_bytes()).is_err());
    }

    #[test]
    fn rejects_codex_malformed_json() {
        assert!(decode_codex_activity_request(b"{ malformed").is_err());
    }

    #[test]
    fn activity_text_omits_the_colon_for_an_empty_summary() {
        assert_eq!(activity_message_text(1, "Bash", ""), "⚙️ 1 · Bash");
        assert_eq!(
            activity_message_text(3, "Bash", "ls -la"),
            "⚙️ 3 · Bash: ls -la"
        );
    }
}
