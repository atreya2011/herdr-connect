use serde::de::Error as _;
use serde::{Deserialize, Serialize};

/// The Claude hook event name for a synchronous permission request.
const PERMISSION_REQUEST_EVENT: &str = "PermissionRequest";

pub const VENDOR_CLAUDE: &str = "claude";
pub const VENDOR_CODEX: &str = "codex";
pub const VENDOR_CURSOR: &str = "cursor";

#[derive(Debug, Eq, PartialEq)]
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
struct ClaudePermissionRequestPayload {
    session_id: String,
    prompt_id: String,
    hook_event_name: String,
    tool_name: String,
    tool_input: ClaudePermissionToolInputPayload,
}

#[derive(Debug, Deserialize)]
struct ClaudePermissionToolInputPayload {
    command: Option<String>,
    description: Option<String>,
    file_path: Option<String>,
}

impl<'de> Deserialize<'de> for ClaudePermissionRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let payload = ClaudePermissionRequestPayload::deserialize(deserializer)?;
        let ClaudePermissionRequestPayload {
            session_id,
            prompt_id,
            hook_event_name,
            tool_name,
            tool_input,
        } = payload;
        let ClaudePermissionToolInputPayload {
            command,
            description,
            file_path,
        } = tool_input;
        let tool_input = match (tool_name.as_str(), command, description, file_path) {
            (_, Some(command), Some(description), _) => ClaudePermissionToolInput {
                command,
                description,
            },
            ("Read", _, _, Some(file_path)) if !file_path.is_empty() => ClaudePermissionToolInput {
                command: file_path.clone(),
                description: file_path,
            },
            ("Read", _, _, _) => {
                return Err(D::Error::custom(
                    "Claude Read permission request has no usable file_path",
                ));
            }
            (_, _, _, _) => {
                return Err(D::Error::custom(
                    "Claude permission request requires command and description",
                ));
            }
        };
        Ok(Self {
            session_id,
            prompt_id,
            hook_event_name,
            tool_name,
            tool_input,
        })
    }
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

#[derive(Debug, Deserialize)]
struct CursorPermissionRequest {
    session_id: String,
    generation_id: String,
    command: String,
    hook_event_name: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Interaction {
    pub session_id: String,
    pub prompt_id: String,
    pub tool_name: String,
    pub tool_input: ClaudePermissionToolInput,
    #[serde(default)]
    pub vendor: PermissionVendor,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionVendor {
    #[default]
    Claude,
    Codex,
    Cursor,
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
    if request.hook_event_name != PERMISSION_REQUEST_EVENT {
        return Err("unexpected Claude hook event".to_owned());
    }
    Ok(Interaction {
        session_id: request.session_id,
        prompt_id: request.prompt_id,
        tool_name: request.tool_name,
        tool_input: request.tool_input,
        vendor: PermissionVendor::Claude,
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
    if request.hook_event_name != PERMISSION_REQUEST_EVENT {
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
        vendor: PermissionVendor::Codex,
    })
}

/// Decodes one Cursor `beforeShellExecution` hook payload into the broker interaction.
///
/// Cursor's `generation_id` is the vendor-neutral request id used by the broker in the existing
/// `prompt_id` field. Cursor's shell hook supplies the command at the top level.
///
/// # Errors
///
/// Returns an error when the payload is not JSON, does not have the required fields, or names a
/// different hook event.
pub fn decode_cursor_permission_request(input: &[u8]) -> Result<Interaction, String> {
    let request: CursorPermissionRequest =
        serde_json::from_slice(input).map_err(|error| error.to_string())?;
    if request.hook_event_name != "beforeShellExecution" {
        return Err("unexpected Cursor hook event".to_owned());
    }
    Ok(Interaction {
        session_id: request.session_id,
        prompt_id: request.generation_id,
        tool_name: "Shell".to_owned(),
        tool_input: ClaudePermissionToolInput {
            command: request.command,
            description: String::new(),
        },
        vendor: PermissionVendor::Cursor,
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

/// Encodes a broker decision in Cursor's native shell-hook response schema.
///
/// # Errors
///
/// Returns an error if the response cannot be serialized.
pub fn encode_cursor_decision(decision: &Decision) -> Result<Vec<u8>, String> {
    let output = match decision.behavior {
        DecisionBehavior::Allow => serde_json::json!({"permission": "allow"}),
        DecisionBehavior::Deny => serde_json::json!({
            "permission": "deny",
            "agent_message": decision.message.as_deref().unwrap_or("permission denied"),
        }),
    };
    serde_json::to_vec(&output).map_err(|error| error.to_string())
}

/// Returns whether a `cursor-agent` argv runs hands-off.
///
/// Cursor's `--force` and its aliases `-f` and `--yolo` allow every shell command unless a hook
/// explicitly denies it, so the bridge answers those seats itself instead of asking the owner.
/// Only exact tokens count.
#[must_use]
pub fn cursor_argv_forces_allow(argv: &[String]) -> bool {
    argv.iter()
        .any(|arg| matches!(arg.as_str(), "--yolo" | "-f" | "--force"))
}

/// Returns whether an argv belongs to the `cursor-agent` CLI process.
///
/// The installed launcher execs node as `<invoked name> [--use-system-ca]
/// <install>/cursor-agent/versions/<v>/index.js <flags>`, and the invoked name can be the `agent`
/// symlink, so the process is recognised by the bundle argument: a path with a `cursor-agent`
/// component whose file name is `index.js`. Wrappers such as `timeout 120 cursor-agent --yolo` or
/// `zsh -c` carry `cursor-agent` only as a bare word or inside a string and do not match.
#[must_use]
pub fn is_cursor_agent_argv(argv: &[String]) -> bool {
    argv.iter().any(|arg| {
        let mut components = arg.split('/');
        components.next_back() == Some("index.js")
            && components.any(|component| component == "cursor-agent")
    })
}

fn encode_permission_decision(decision: &Decision) -> Result<Vec<u8>, String> {
    serde_json::to_vec(&serde_json::json!({
        "hookSpecificOutput": {
            "hookEventName": PERMISSION_REQUEST_EVENT,
            "decision": decision,
        },
    }))
    .map_err(|error| error.to_string())
}
