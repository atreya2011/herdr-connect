use herdr_connect_rs::{Decision, decode_cursor_permission_request, encode_cursor_decision};
use serde_json::Value;
use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn captured_cursor_permission_fixture_decodes_into_interaction() {
    let interaction = decode_cursor_permission_request(include_bytes!(
        "fixtures/cursor-permission-request/default.json"
    ))
    .expect("Cursor beforeShellExecution fixture decodes");

    assert_eq!(
        interaction.session_id,
        "603de9c6-4aa5-4e18-87fa-336ed03fcd70"
    );
    assert_eq!(
        interaction.prompt_id,
        "7ab2e513-0373-40b1-aa59-71077298c992"
    );
    assert_eq!(interaction.tool_name, "Shell");
    assert_eq!(
        interaction.tool_input.command,
        "touch <tmp>/herdr-connect-rs-gauntlet-20260815-102236/t8-proof.txt"
    );
    assert!(interaction.tool_input.description.is_empty());
}

#[test]
fn cursor_decisions_encode_as_native_objects() {
    let cases = [
        (Decision::allow(), "allow", None),
        (
            Decision::deny(Some("operator denied this request".to_owned())),
            "deny",
            Some("operator denied this request"),
        ),
    ];
    for (decision, permission, agent_message) in cases {
        let encoded = encode_cursor_decision(&decision).expect("Cursor decision encodes");
        let value: Value = serde_json::from_slice(&encoded).expect("encoded decision is JSON");
        assert_eq!(value["permission"], permission);
        assert_eq!(value["agent_message"].as_str(), agent_message);
        assert!(value.get("hookSpecificOutput").is_none());
    }
}

#[test]
fn cursor_hook_denies_when_broker_is_unavailable() {
    let socket = std::env::temp_dir().join(format!(
        "herdr-connect-rs-no-such-cursor-broker-{}-{}.sock",
        std::process::id(),
        std::thread::current().name().unwrap_or("test")
    ));
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
        .args(["hook", "--vendor", "cursor", "--socket"])
        .arg(socket)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn hook subcommand");
    child
        .stdin
        .take()
        .expect("hook stdin is piped")
        .write_all(include_bytes!(
            "fixtures/cursor-permission-request/default.json"
        ))
        .expect("write Cursor hook payload");
    let output = child.wait_with_output().expect("wait for hook subcommand");

    assert!(output.status.success());
    let value: Value = serde_json::from_slice(&output.stdout).expect("denial is JSON");
    assert_eq!(value["permission"], "deny");
    assert!(value["agent_message"].as_str().is_some());
}

#[test]
fn explicit_cursor_vendor_denies_when_payload_is_undecodable() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
        .args(["hook", "--vendor", "cursor"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn hook subcommand");
    child
        .stdin
        .take()
        .expect("hook stdin is piped")
        .write_all(b"{}")
        .expect("write undecodable hook payload");
    let output = child.wait_with_output().expect("wait for hook subcommand");

    assert!(output.status.success());
    let value: Value = serde_json::from_slice(&output.stdout).expect("denial is JSON");
    assert_eq!(value["permission"], "deny");
    assert!(value["agent_message"].as_str().is_some());
}

#[test]
fn explicit_codex_vendor_preserves_empty_output_when_payload_is_undecodable() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
        .args(["hook", "--vendor", "codex"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn hook subcommand");
    child
        .stdin
        .take()
        .expect("hook stdin is piped")
        .write_all(b"{}")
        .expect("write undecodable hook payload");
    let output = child.wait_with_output().expect("wait for hook subcommand");

    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}
