use std::io::Write;
use std::process::{Command, Stdio};

use serde_json::Value;

use herdr_connect_rs::{
    Decision, cursor_argv_forces_allow, decode_cursor_permission_request, encode_cursor_decision,
    is_cursor_agent_argv,
};

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
fn cursor_hook_denies_and_reports_broker_connect_failure() {
    let cases = [(
        "missing broker socket",
        std::path::PathBuf::from(format!(
            "hc-cursor-{}-{}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        )),
    )];
    for (name, socket) in cases {
        let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
            .args(["hook", "--vendor", "cursor", "--socket"])
            .arg(&socket)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
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

        assert!(output.status.success(), "{name}: {output:?}");
        let value: Value = serde_json::from_slice(&output.stdout).expect("denial is JSON");
        assert_eq!(value["permission"], "deny", "{name}");
        assert_eq!(
            value["agent_message"],
            "permission broker did not return a decision; denying by default",
            "{name}"
        );
        assert!(value.get("hookSpecificOutput").is_none(), "{name}");

        let stderr = String::from_utf8_lossy(&output.stderr);
        let socket = socket.to_string_lossy();
        assert!(stderr.contains("broker request"), "{name}: {stderr}");
        assert!(stderr.contains("connect"), "{name}: {stderr}");
        assert!(stderr.contains(socket.as_ref()), "{name}: {stderr}");
        assert!(
            stderr.contains("No such file or directory"),
            "{name}: {stderr}"
        );
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
