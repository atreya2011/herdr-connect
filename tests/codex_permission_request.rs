use serde::Deserialize;

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct CodexPermissionRequest {
    session_id: String,
    turn_id: String,
    hook_event_name: String,
    tool_name: String,
    tool_input: CodexPermissionToolInput,
}

#[derive(Debug, Deserialize, Eq, PartialEq)]
struct CodexPermissionToolInput {
    command: String,
}

#[derive(Debug, Eq, PartialEq)]
struct InteractionPrecursor {
    session_id: String,
    turn_id: String,
    tool_name: String,
    tool_input: CodexPermissionToolInput,
}

#[test]
fn captured_permission_request_decodes_into_interaction_precursor() {
    let request: CodexPermissionRequest = serde_json::from_str(include_str!(
        "fixtures/codex-permission-request/default.json"
    ))
    .expect("Codex PermissionRequest fixture decodes");

    assert_eq!(request.hook_event_name, "PermissionRequest");

    let precursor = InteractionPrecursor {
        session_id: request.session_id,
        turn_id: request.turn_id,
        tool_name: request.tool_name,
        tool_input: request.tool_input,
    };

    assert_eq!(precursor.session_id, "11111111-1111-4111-8111-111111111111");
    assert_eq!(precursor.turn_id, "22222222-2222-4222-8222-222222222222");
    assert_eq!(precursor.tool_name, "Bash");
    assert_eq!(
        precursor.tool_input.command,
        "touch <tmp>/herdr-connect-rs-gauntlet-20260815-102236/t6-proof.txt"
    );
}
