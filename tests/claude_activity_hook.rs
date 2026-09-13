use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;

const COMMAND_FIXTURE: &str = include_str!("fixtures/claude-activity-request/command.json");
const FILE_PATH_FIXTURE: &str = include_str!("fixtures/claude-activity-request/file-path.json");
const EMPTY_TOOL_INPUT_FIXTURE: &str =
    include_str!("fixtures/claude-activity-request/empty-tool-input.json");
const CODEX_COMMAND_FIXTURE: &str = include_str!("fixtures/codex-activity-request/command.json");
const CODEX_OTHER_FIELD_FIXTURE: &str =
    include_str!("fixtures/codex-activity-request/other-field.json");
const CODEX_EMPTY_TOOL_INPUT_FIXTURE: &str =
    include_str!("fixtures/codex-activity-request/empty-tool-input.json");

fn socket_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "herdr-connect-rs-activity-{label}-{}-{}.sock",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after unix epoch")
            .as_nanos()
    ))
}

fn invoke_activity(payload: &str, socket: &Path) -> std::process::Output {
    invoke_activity_for_vendor(payload, "claude", socket)
}

fn invoke_activity_for_vendor(payload: &str, vendor: &str, socket: &Path) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
        .args(["activity", "--vendor", vendor, "--socket"])
        .arg(socket)
        .env("HERDR_WORKSPACE_ID", "w1")
        .env("HERDR_TAB_ID", "w1:t1")
        .env("HERDR_PANE_ID", "w1:p1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn activity subcommand");
    child
        .stdin
        .take()
        .expect("activity stdin is piped")
        .write_all(payload.as_bytes())
        .expect("write activity payload");
    child
        .wait_with_output()
        .expect("wait for activity subcommand")
}

/// Accepts at most one connection on `listener` and reads it to EOF, within a short bound. `None`
/// means no peer connected (the subcommand wrote nothing).
async fn recv_frame(listener: &UnixListener) -> Option<Value> {
    let (mut stream, _) = tokio::time::timeout(Duration::from_millis(500), listener.accept())
        .await
        .ok()?
        .expect("accept activity connection");
    let mut buffer = Vec::new();
    tokio::time::timeout(Duration::from_millis(200), stream.read_to_end(&mut buffer))
        .await
        .expect("read activity frame within bound")
        .expect("read activity frame");
    serde_json::from_slice(&buffer).ok()
}

#[tokio::test]
async fn activity_subcommand_writes_the_expected_frame_by_tool_input_field() {
    let cases = [
        (
            "command",
            COMMAND_FIXTURE,
            "Bash",
            "find . -maxdepth 3 -name '*.rs' -newer Cargo.toml -print | xargs wc -l | tail -n",
        ),
        (
            "file-path",
            FILE_PATH_FIXTURE,
            "Read",
            "<tmp>/herdr-connect-rs-gauntlet-20260815-102236/t4-activity/src/main.rs",
        ),
        (
            "empty-tool-input",
            EMPTY_TOOL_INPUT_FIXTURE,
            "TodoWrite",
            "",
        ),
    ];
    for (name, payload, expected_tool, expected_summary) in cases {
        let path = socket_path(name);
        let listener = UnixListener::bind(&path).expect("bind test listener");
        let output = tokio::task::spawn_blocking({
            let path = path.clone();
            let payload = payload.to_owned();
            move || invoke_activity(&payload, &path)
        })
        .await
        .expect("activity process task completes");
        assert!(output.status.success(), "{name}: {output:?}");
        assert!(output.stdout.is_empty(), "{name}: unexpected stdout");

        let frame = recv_frame(&listener)
            .await
            .unwrap_or_else(|| panic!("{name}: no frame received"));
        assert_eq!(frame["kind"], "activity", "{name}");
        assert_eq!(frame["vendor"], "claude", "{name}");
        assert_eq!(frame["workspace_id"], "w1", "{name}");
        assert_eq!(frame["tab_id"], "w1:t1", "{name}");
        assert_eq!(frame["pane_id"], "w1:p1", "{name}");
        assert_eq!(frame["tool"], expected_tool, "{name}");
        assert_eq!(frame["summary"], expected_summary, "{name}");
        let _ = std::fs::remove_file(&path);
    }
}

#[tokio::test]
async fn codex_activity_subcommand_writes_the_expected_frame_by_tool_input_field() {
    let cases = [
        (
            "command",
            CODEX_COMMAND_FIXTURE,
            "Bash",
            "find . -maxdepth 3 -name '*.rs' -newer Cargo.toml -print | xargs wc -l",
        ),
        (
            "other-field",
            CODEX_OTHER_FIELD_FIXTURE,
            "Read",
            "<tmp>/herdr-connect-rs-gauntlet-20260815-102236/t4-activity/src/main.rs",
        ),
        (
            "empty-tool-input",
            CODEX_EMPTY_TOOL_INPUT_FIXTURE,
            "TodoWrite",
            "",
        ),
    ];
    for (name, payload, expected_tool, expected_summary) in cases {
        let path = socket_path(name);
        let listener = UnixListener::bind(&path).expect("bind test listener");
        let output = tokio::task::spawn_blocking({
            let path = path.clone();
            let payload = payload.to_owned();
            move || invoke_activity_for_vendor(&payload, "codex", &path)
        })
        .await
        .expect("activity process task completes");
        assert!(output.status.success(), "{name}: {output:?}");
        assert!(output.stdout.is_empty(), "{name}: unexpected stdout");

        let frame = recv_frame(&listener)
            .await
            .unwrap_or_else(|| panic!("{name}: no frame received"));
        assert_eq!(frame["kind"], "activity", "{name}");
        assert_eq!(frame["vendor"], "codex", "{name}");
        assert_eq!(frame["workspace_id"], "w1", "{name}");
        assert_eq!(frame["tab_id"], "w1:t1", "{name}");
        assert_eq!(frame["pane_id"], "w1:p1", "{name}");
        assert_eq!(frame["tool"], expected_tool, "{name}");
        assert_eq!(frame["summary"], expected_summary, "{name}");
        let _ = std::fs::remove_file(&path);
    }
}

#[tokio::test]
async fn cursor_activity_subcommand_writes_the_documented_frame() {
    let cases = [(
        "documented Cursor preToolUse event",
        r#"{
            "conversation_id": "cursor-conversation",
            "generation_id": "cursor-generation",
            "hook_event_name": "preToolUse",
            "workspace_roots": ["/tmp/cursor-workspace"],
            "tool_name": "Shell",
            "tool_input": {"command": "git status --short"},
            "tool_use_id": "cursor-tool-use",
            "cwd": "/tmp/cursor-workspace",
            "agent_message": "Inspect the repository state"
        }"#,
        "cursor-conversation",
        "Shell",
        "Inspect the repository state",
    )];
    for (name, payload, expected_session, expected_tool, expected_summary) in cases {
        let path = socket_path("cursor");
        let listener = UnixListener::bind(&path).expect("bind test listener");
        let output = tokio::task::spawn_blocking({
            let path = path.clone();
            let payload = payload.to_owned();
            move || invoke_activity_for_vendor(&payload, "cursor", &path)
        })
        .await
        .expect("activity process task completes");
        let frame = recv_frame(&listener).await;
        let _ = std::fs::remove_file(&path);

        assert!(output.status.success(), "{name}: {output:?}");
        assert!(output.stdout.is_empty(), "{name}: unexpected stdout");
        let frame = frame.unwrap_or_else(|| panic!("{name}: no frame received"));
        assert_eq!(frame["kind"], "activity", "{name}");
        assert_eq!(frame["vendor"], "cursor", "{name}");
        assert_eq!(frame["session_id"], expected_session, "{name}");
        assert_eq!(frame["tool"], expected_tool, "{name}");
        assert_eq!(frame["summary"], expected_summary, "{name}");
    }
}

#[test]
fn cursor_hooks_register_activity_and_permission_commands() {
    let config: Value = serde_json::from_str(include_str!("../examples/cursor-hooks.json"))
        .expect("parse Cursor hook config");
    let cases = [
        ("preToolUse", "herdr-connect-rs activity --vendor cursor"),
        (
            "beforeShellExecution",
            "herdr-connect-rs hook --vendor cursor",
        ),
    ];
    for (event, expected_command) in cases {
        let hooks = config["hooks"][event]
            .as_array()
            .unwrap_or_else(|| panic!("{event}: missing hook key"));
        assert_eq!(hooks.len(), 1, "{event}: expected one hook");
        assert_eq!(
            hooks[0]["command"], expected_command,
            "{event}: unexpected command"
        );
    }
    let permission_hook = &config["hooks"]["beforeShellExecution"][0];
    assert_eq!(
        permission_hook["timeout"], 50000,
        "beforeShellExecution: unexpected timeout"
    );
    assert_eq!(
        permission_hook["failClosed"], true,
        "beforeShellExecution: unexpected failClosed"
    );
}

#[test]
fn codex_hooks_register_activity_and_permission_commands() {
    let config: Value = serde_json::from_str(include_str!("../examples/codex-hooks.json"))
        .expect("parse Codex hook config");
    let activity_hook = &config["hooks"]["PreToolUse"][0]["hooks"][0];
    assert_eq!(
        activity_hook["command"], "herdr-connect-rs activity --vendor codex",
        "PreToolUse: unexpected command"
    );
    let permission_hook = &config["hooks"]["PermissionRequest"][0]["hooks"][0];
    assert_eq!(
        permission_hook["command"], "herdr-connect-rs hook --vendor codex",
        "PermissionRequest: unexpected command"
    );
    assert_eq!(
        config["hooks"]["PermissionRequest"][0]["matcher"], "Bash",
        "PermissionRequest: unexpected matcher"
    );
}

fn invoke_activity_with_args(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
        .arg("activity")
        .args(args)
        .env_remove("HERDR_CLAUDE_BROKER_SOCKET")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run activity subcommand")
}

/// An argument error is as much a no-op as a malformed payload or a missing socket: the hook's
/// fire-and-forget contract promises "always exits 0 with no output" for every input, not just the
/// ones the subcommand itself considers well-formed.
#[test]
fn activity_subcommand_exits_zero_with_no_output_for_every_argument_error() {
    let cases: [(&str, &[&str]); 5] = [
        ("unsupported vendor", &["--vendor", "gemini"]),
        ("unknown flag", &["--vendor", "claude", "--oops"]),
        ("no arguments", &[]),
        ("--vendor with no value", &["--vendor"]),
        (
            "--socket with no value",
            &["--vendor", "claude", "--socket"],
        ),
    ];
    for (name, args) in cases {
        let output = invoke_activity_with_args(args);
        assert!(output.status.success(), "{name}: {output:?}");
        assert!(output.stdout.is_empty(), "{name}: unexpected stdout");
        assert!(output.stderr.is_empty(), "{name}: unexpected stderr");
    }
}

#[tokio::test]
async fn activity_subcommand_writes_nothing_for_malformed_input() {
    let path = socket_path("malformed");
    let listener = UnixListener::bind(&path).expect("bind test listener");
    let output = tokio::task::spawn_blocking({
        let path = path.clone();
        move || invoke_activity("{ malformed", &path)
    })
    .await
    .expect("activity process task completes");
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    assert!(
        recv_frame(&listener).await.is_none(),
        "malformed input must not write a frame"
    );
    let _ = std::fs::remove_file(&path);
}
