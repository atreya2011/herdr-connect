use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixListener;

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

const CURSOR_SHELL_HOOK: &str = r#"{
    "session_id": "cursor-session",
    "generation_id": "cursor-generation",
    "command": "true",
    "hook_event_name": "beforeShellExecution"
}"#;

fn socket_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "herdr-connect-rs-hook-{label}-{}-{}.sock",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after unix epoch")
            .as_nanos()
    ))
}

fn invoke_cursor_hook(payload: &str, socket: &Path, pane_id: Option<&str>) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"));
    command
        .args(["hook", "--vendor", "cursor", "--socket"])
        .arg(socket)
        .env_remove("HERDR_CLAUDE_BROKER_SOCKET")
        .env_remove("HERDR_PANE_ID")
        .env_remove("HERDR_WORKSPACE_ID")
        .env_remove("HERDR_TAB_ID")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(pane_id) = pane_id {
        command.env("HERDR_PANE_ID", pane_id);
    }
    let mut child = command.spawn().expect("spawn hook subcommand");
    child
        .stdin
        .take()
        .expect("hook stdin is piped")
        .write_all(payload.as_bytes())
        .expect("write hook payload");
    child.wait_with_output().expect("wait for hook subcommand")
}

fn cursor_permission(output: &std::process::Output) -> Value {
    assert!(output.status.success(), "hook failed: {output:?}");
    serde_json::from_slice(&output.stdout).expect("hook stdout is JSON")
}

#[tokio::test]
async fn hook_outside_a_herdr_pane_allows_without_a_broker_call() {
    let path = socket_path("outside-pane");
    let listener = UnixListener::bind(&path).expect("bind broker listener");
    let output = tokio::task::spawn_blocking({
        let path = path.clone();
        move || invoke_cursor_hook(CURSOR_SHELL_HOOK, &path, None)
    })
    .await
    .expect("outside-pane hook process task completes");
    let connected = tokio::time::timeout(Duration::from_millis(50), listener.accept()).await;
    let _ = std::fs::remove_file(&path);

    let value = cursor_permission(&output);
    assert_eq!(value["permission"], "allow");
    assert!(
        connected.is_err(),
        "a hook outside a herdr pane must not contact the broker"
    );
}

#[tokio::test]
async fn hook_inside_a_herdr_pane_denies_when_the_broker_returns_no_decision() {
    let path = socket_path("inside-pane-no-decision");
    let output = tokio::task::spawn_blocking(move || {
        invoke_cursor_hook(CURSOR_SHELL_HOOK, &path, Some("w1:p1"))
    })
    .await
    .expect("inside-pane hook process task completes");

    let value = cursor_permission(&output);
    assert_eq!(value["permission"], "deny");
    assert_eq!(
        value["agent_message"],
        "permission broker did not return a decision; denying by default"
    );
}

#[tokio::test]
async fn hook_inside_a_herdr_pane_passes_through_a_broker_decision() {
    let path = socket_path("inside-pane-decision");
    let listener = UnixListener::bind(&path).expect("bind broker listener");
    let hook = tokio::task::spawn_blocking({
        let path = path.clone();
        move || invoke_cursor_hook(CURSOR_SHELL_HOOK, &path, Some("w1:p1"))
    });
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
        .await
        .expect("hook inside a herdr pane contacts the broker")
        .expect("accept broker connection");
    let mut request = Vec::new();
    let mut byte = [0; 1];
    loop {
        stream.read_exact(&mut byte).await.expect("read request");
        if byte[0] == b'\n' {
            break;
        }
        request.push(byte[0]);
    }
    let request: Value = serde_json::from_slice(&request).expect("broker request is JSON");
    let response = serde_json::json!({
        "session_id": request["session_id"],
        "prompt_id": request["prompt_id"],
        "decision": {
            "behavior": "deny",
            "message": "broker decided deny",
        },
    });
    stream
        .write_all(format!("{response}\n").as_bytes())
        .await
        .expect("write broker decision");
    drop(stream);
    let output = hook.await.expect("inside-pane hook process task completes");
    let _ = std::fs::remove_file(&path);

    let value = cursor_permission(&output);
    assert_eq!(value["permission"], "deny");
    assert_eq!(value["agent_message"], "broker decided deny");
}
