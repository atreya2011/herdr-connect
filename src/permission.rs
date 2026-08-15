use serde::Deserialize;

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct ClaudePermissionRequest {
    pub session_id: String,
    pub prompt_id: String,
    pub tool_name: String,
    pub tool_input: ClaudePermissionToolInput,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
pub struct ClaudePermissionToolInput {
    pub command: String,
    pub description: String,
}
