use herdr_connect_rs::{Decision, decode_codex_permission_request, encode_codex_decision};
use serde_json::Value;

#[test]
fn codex_permission_fixture_decodes_and_encodes_allow_and_deny() {
    let interaction = decode_codex_permission_request(include_bytes!(
        "fixtures/codex-permission-request/default.json"
    ))
    .expect("Codex PermissionRequest fixture decodes");

    assert_eq!(
        interaction.session_id,
        "11111111-1111-4111-8111-111111111111"
    );
    assert_eq!(
        interaction.prompt_id,
        "22222222-2222-4222-8222-222222222222"
    );
    assert_eq!(interaction.tool_name, "Bash");
    assert_eq!(
        interaction.tool_input.command,
        "touch <tmp>/herdr-connect-rs-gauntlet-20260815-102236/t6-proof.txt"
    );

    let cases = [
        (Decision::allow(), "allow", None),
        (
            Decision::deny(Some("operator denied this request".to_owned())),
            "deny",
            Some("operator denied this request"),
        ),
    ];
    for (decision, behavior, message) in cases {
        let encoded = encode_codex_decision(&decision).expect("Codex decision encodes");
        let value: Value = serde_json::from_slice(&encoded).expect("encoded decision is JSON");
        assert_eq!(
            value["hookSpecificOutput"]["hookEventName"],
            "PermissionRequest"
        );
        assert_eq!(
            value["hookSpecificOutput"]["decision"]["behavior"],
            behavior
        );
        assert_eq!(
            value["hookSpecificOutput"]["decision"]["message"].as_str(),
            message
        );
    }
}
