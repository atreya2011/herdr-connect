use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct ClaudePermissionRequest {
    pub session_id: String,
    pub prompt_id: String,
    pub hook_event_name: String,
    pub tool_name: String,
    pub tool_input: ClaudePermissionToolInput,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ClaudePermissionToolInput {
    pub command: String,
    pub description: String,
}

#[derive(Debug, Deserialize)]
struct CodexPermissionRequest {
    session_id: String,
    turn_id: String,
    hook_event_name: String,
    tool_name: String,
    tool_input: CodexPermissionToolInput,
}

#[derive(Debug, Deserialize)]
struct CodexPermissionToolInput {
    command: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Interaction {
    pub session_id: String,
    pub prompt_id: String,
    pub tool_name: String,
    pub tool_input: ClaudePermissionToolInput,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Decision {
    pub behavior: DecisionBehavior,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DecisionBehavior {
    Allow,
    Deny,
}

impl Decision {
    #[must_use]
    pub const fn allow() -> Self {
        Self {
            behavior: DecisionBehavior::Allow,
            message: None,
        }
    }

    #[must_use]
    pub const fn deny(message: Option<String>) -> Self {
        Self {
            behavior: DecisionBehavior::Deny,
            message,
        }
    }
}

/// Decodes one Claude `PermissionRequest` hook payload into the broker interaction.
///
/// # Errors
///
/// Returns an error when the payload is not JSON, does not have the fixture's required fields,
/// or names a different hook event.
pub fn decode_claude_permission_request(input: &[u8]) -> Result<Interaction, String> {
    let request: ClaudePermissionRequest =
        serde_json::from_slice(input).map_err(|error| error.to_string())?;
    if request.hook_event_name != "PermissionRequest" {
        return Err("unexpected Claude hook event".to_owned());
    }
    Ok(Interaction {
        session_id: request.session_id,
        prompt_id: request.prompt_id,
        tool_name: request.tool_name,
        tool_input: request.tool_input,
    })
}

/// Decodes one Codex `PermissionRequest` hook payload into the broker interaction.
///
/// Codex's `turn_id` is the vendor-neutral request id used by the broker in the existing
/// `prompt_id` field.
///
/// # Errors
///
/// Returns an error when the payload is not JSON, does not have the required fields, or names a
/// different hook event.
pub fn decode_codex_permission_request(input: &[u8]) -> Result<Interaction, String> {
    let request: CodexPermissionRequest =
        serde_json::from_slice(input).map_err(|error| error.to_string())?;
    if request.hook_event_name != "PermissionRequest" {
        return Err("unexpected Codex hook event".to_owned());
    }
    Ok(Interaction {
        session_id: request.session_id,
        prompt_id: request.turn_id,
        tool_name: request.tool_name,
        tool_input: ClaudePermissionToolInput {
            command: request.tool_input.command,
            description: String::new(),
        },
    })
}

/// Encodes a broker decision in Claude's object-form hook response schema.
///
/// # Errors
///
/// Returns an error if the response cannot be serialized.
pub fn encode_claude_decision(decision: &Decision) -> Result<Vec<u8>, String> {
    encode_permission_decision(decision)
}

/// Encodes a broker decision in Codex's object-form `PermissionRequest` hook response schema.
///
/// # Errors
///
/// Returns an error if the response cannot be serialized.
pub fn encode_codex_decision(decision: &Decision) -> Result<Vec<u8>, String> {
    encode_permission_decision(decision)
}

fn encode_permission_decision(decision: &Decision) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&serde_json::json!({
        "hookSpecificOutput": {
            "hookEventName": "PermissionRequest",
            "decision": decision,
        },
    }))
    .map_err(|error| error.to_string())
}
