use herdr_connect_rs::ClaudePermissionRequest;

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
