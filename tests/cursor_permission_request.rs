use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;

use herdr_connect_rs::{
    Decision, cursor_argv_forces_allow, encode_cursor_decision, is_cursor_agent_argv,
};

#[test]
fn cursor_decisions_encode_as_native_objects() {
    let cases = [
        (Decision::allow(), "allow", None),
        (
            Decision::deny("operator denied this request".to_owned()),
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
fn cursor_argv_force_flags_match_exact_tokens_only() {
    let cases = [
        (vec!["cursor-agent", "--yolo"], true),
        (vec!["cursor-agent", "-f"], true),
        (vec!["cursor-agent", "--force", "-p"], true),
        (vec!["cursor-agent", "--trust", "-p"], false),
        (vec!["cursor-agent", "--force-something"], false),
    ];
    for (argv, expected) in cases {
        let argv: Vec<String> = argv.into_iter().map(str::to_owned).collect();
        assert_eq!(cursor_argv_forces_allow(&argv), expected, "argv: {argv:?}");
    }
}

#[test]
fn cursor_agent_process_is_recognised_by_its_bundle_path_not_by_wrappers() {
    let bundle = "/home/user/.local/share/cursor-agent/versions/2026.09.18-9a7762b/index.js";
    let cases = [
        (
            vec![
                "/home/user/.local/bin/cursor-agent",
                "--use-system-ca",
                bundle,
                "--yolo",
            ],
            true,
        ),
        (
            vec![
                "/home/user/.local/bin/agent",
                "--use-system-ca",
                bundle,
                "--yolo",
            ],
            true,
        ),
        (vec!["timeout", "120", "cursor-agent", "--yolo"], false),
        (vec!["/usr/bin/zsh", "-c", "cursor-agent --yolo"], false),
    ];
    for (argv, expected) in cases {
        let argv: Vec<String> = argv.into_iter().map(str::to_owned).collect();
        assert_eq!(is_cursor_agent_argv(&argv), expected, "argv: {argv:?}");
    }
}
