use herdr_connect_rs::decode_claude_permission_request;

#[test]
fn observed_read_permission_request_decodes_into_a_nonempty_command() {
    let payload = r#"{
  "session_id": "55555555-5555-4555-8555-555555555555",
  "transcript_path": "<home>/.claude/projects/-tmp/read-session.jsonl",
  "cwd": "/tmp/read-session",
  "prompt_id": "66666666-6666-4666-8666-666666666666",
  "permission_mode": "default",
  "hook_event_name": "PermissionRequest",
  "tool_name": "Read",
  "tool_input": {
    "file_path": "/etc/hostname"
  },
  "permission_suggestions": []
}"#;

    let interaction = decode_claude_permission_request(payload.as_bytes())
        .expect("observed Claude Read PermissionRequest decodes");
    assert_eq!(interaction.tool_name, "Read");
    assert_eq!(interaction.tool_input.command, "/etc/hostname");
}
