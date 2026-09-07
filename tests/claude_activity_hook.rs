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
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
        .args(["activity", "--vendor", "claude", "--socket"])
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
        ("unsupported vendor", &["--vendor", "codex"]),
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
