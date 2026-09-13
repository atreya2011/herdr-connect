use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const SINGLE_SELECT_FIXTURE: &str = include_str!("fixtures/claude-ask-question/single-select.json");
const MULTI_SELECT_FIXTURE: &str = include_str!("fixtures/claude-ask-question/multi-select.json");

fn socket_path(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "herdr-connect-rs-{label}-{}-{}.sock",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after unix epoch")
            .as_nanos()
    ))
}

fn invoke_hook(payload: &str, socket: &Path) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
        .arg("hook")
        .env("HERDR_CLAUDE_BROKER_SOCKET", socket)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn hook subcommand");
    child
        .stdin
        .take()
        .expect("hook stdin is piped")
        .write_all(payload.as_bytes())
        .expect("write hook payload");
    child.wait_with_output().expect("wait for hook subcommand")
}

fn invoke_hook_without_socket(payload: &str) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
        .arg("hook")
        .env_remove("HERDR_CLAUDE_BROKER_SOCKET")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn hook subcommand");
    child
        .stdin
        .take()
        .expect("hook stdin is piped")
        .write_all(payload.as_bytes())
        .expect("write hook payload");
    child.wait_with_output().expect("wait for hook subcommand")
}

/// No socket configured: the hook must still exit 0 with no output, letting Claude's own dialog
/// appear, unlike the permission hook's fail-loud contract (a question has no safe deny).
#[test]
fn hook_subcommand_emits_nothing_for_a_question_with_no_broker_configured() {
    for payload in [SINGLE_SELECT_FIXTURE, MULTI_SELECT_FIXTURE] {
        let output = invoke_hook_without_socket(payload);
        assert!(output.status.success());
        assert!(output.stdout.is_empty());
    }
}

/// A broker that never answers (here: nothing is listening) must not hang or fail the tool call.
#[test]
fn hook_subcommand_falls_through_when_the_question_broker_is_unreachable() {
    let socket = socket_path("ask-question-unreachable");
    let output = invoke_hook(SINGLE_SELECT_FIXTURE, &socket);
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}

/// A peer that accepts the connection but never understands the question frame -- exactly the
/// shipped `broker` binary's behavior before it decodes question frames -- must not hang the hook
/// or crash the tool call: the frame is silently rejected as an unrecognized permission
/// [`Interaction`](herdr_connect_rs::Interaction), and the hook exits 0 with no output.
#[test]
fn hook_subcommand_falls_through_when_the_peer_does_not_understand_questions() {
    let socket = socket_path("ask-question-unrecognized-peer");
    let listener =
        std::os::unix::net::UnixListener::bind(&socket).expect("bind stub broker socket");
    let accept_thread = std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            drop(stream);
        }
    });
    let output = invoke_hook(SINGLE_SELECT_FIXTURE, &socket);
    accept_thread.join().expect("stub broker thread joins");
    let _ = std::fs::remove_file(&socket);
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}

#[test]
fn hook_subcommand_emits_nothing_for_malformed_question_input() {
    let output = invoke_hook(
        "{ malformed",
        Path::new("/tmp/herdr-connect-rs-no-such-question-broker.sock"),
    );
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}
