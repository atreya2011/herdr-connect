use herdr_connect_rs::{ClaudePermissionRequest, decode_claude_permission_request};

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

#[test]
fn captured_permission_requests_decode_into_interaction_precursors() {
    let cases = [
        (
            "default",
            include_str!("fixtures/claude-permission-request/default.json"),
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
            "touch <tmp>/herdr-connect-rs-gauntlet-20260815-102236/t3-proof.txt",
            "Create proof file as requested",
        ),
        (
            "allow",
            include_str!("fixtures/claude-permission-request/allow.json"),
            "33333333-3333-4333-8333-333333333333",
            "44444444-4444-4444-8444-444444444444",
            "touch <tmp>/herdr-connect-rs-gauntlet-20260815-102236/t3-allow-proof.txt",
            "Create proof file",
        ),
    ];

    for (
        name,
        payload,
        expected_session_id,
        expected_prompt_id,
        expected_command,
        expected_description,
    ) in cases
    {
        let request: ClaudePermissionRequest = serde_json::from_str(payload)
            .unwrap_or_else(|error| panic!("{name} PermissionRequest fixture decodes: {error}"));

        assert_eq!(request.session_id, expected_session_id);
        assert_eq!(request.prompt_id, expected_prompt_id);
        assert_eq!(request.tool_name, "Bash");
        assert_eq!(request.tool_input.command, expected_command);
        assert_eq!(request.tool_input.description, expected_description);
    }
}
