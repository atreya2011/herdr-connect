use herdr_connect_rs::{
    ClaudePermissionToolInput, Decision, DecisionBehavior, Interaction,
    decode_claude_permission_request, encode_claude_decision, request_decision,
    serve_tracer_broker,
};
use serde_json::Value;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::UnixListener;
use tokio::sync::oneshot;

const DEFAULT_FIXTURE: &str = include_str!("fixtures/claude-permission-request/default.json");
const ALLOW_FIXTURE: &str = include_str!("fixtures/claude-permission-request/allow.json");

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

fn start_broker(label: &str) -> (PathBuf, oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let path = socket_path(label);
    let listener = UnixListener::bind(&path).expect("bind real Unix broker socket");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        serve_tracer_broker(listener, shutdown_rx)
            .await
            .expect("real broker serves connections");
    });
    (path, shutdown_tx, task)
}

async fn stop_broker(
    path: &Path,
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
) {
    let _ = shutdown.send(());
    task.await.expect("broker task stops cleanly");
    std::fs::remove_file(path).expect("remove test broker socket");
}

fn interaction(session_id: &str, prompt_id: &str) -> Interaction {
    Interaction {
        session_id: session_id.to_owned(),
        prompt_id: prompt_id.to_owned(),
        tool_name: "Bash".to_owned(),
        tool_input: ClaudePermissionToolInput {
            command: format!("touch {prompt_id}"),
            description: format!("Create {prompt_id}"),
        },
    }
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

#[test]
fn permission_fixtures_decode_and_encode_allow_and_deny() {
    let cases = [
        ("default", DEFAULT_FIXTURE, Decision::allow(), None),
        (
            "allow",
            ALLOW_FIXTURE,
            Decision::deny(Some("operator denied this request".to_owned())),
            Some("operator denied this request"),
        ),
    ];
    for (name, payload, decision, message) in cases {
        let interaction = decode_claude_permission_request(payload.as_bytes())
            .unwrap_or_else(|error| panic!("{name} fixture decodes: {error}"));
        assert_eq!(interaction.tool_name, "Bash");
        let encoded = encode_claude_decision(&decision).expect("decision encodes");
        let value: Value = serde_json::from_slice(&encoded).expect("encoded decision is JSON");
        assert_eq!(
            value["hookSpecificOutput"]["hookEventName"],
            "PermissionRequest"
        );
        assert_eq!(
            value["hookSpecificOutput"]["decision"]["behavior"],
            match decision.behavior {
                DecisionBehavior::Allow => "allow",
                DecisionBehavior::Deny => "deny",
            }
        );
        assert_eq!(
            value["hookSpecificOutput"]["decision"]["message"].as_str(),
            message
        );
    }
}

#[tokio::test]
async fn real_unix_broker_round_trip_and_concurrent_requests_do_not_cross() {
    let (path, shutdown, task) = start_broker("round-trip");
    let requests = [
        interaction("session-a", "prompt-a"),
        interaction("session-b", "prompt-b"),
    ];
    let (first, second) = tokio::join!(
        request_decision(&requests[0], &path, Duration::from_secs(1)),
        request_decision(&requests[1], &path, Duration::from_secs(1)),
    );
    assert_eq!(first, Some(Decision::allow()));
    assert_eq!(second, Some(Decision::allow()));
    stop_broker(&path, shutdown, task).await;
}

#[tokio::test]
async fn timeout_and_malformed_broker_response_fall_through() {
    let cases = [
        ("timeout", Vec::new(), true),
        ("malformed", b"not json\n".to_vec(), false),
    ];
    for (label, response, should_hold_connection) in cases {
        let path = socket_path(label);
        let listener = UnixListener::bind(&path).expect("bind failure-path Unix socket");
        let accept_task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept hook connection");
            if should_hold_connection {
                tokio::time::sleep(Duration::from_millis(100)).await;
            } else {
                tokio::io::AsyncWriteExt::write_all(&mut stream, &response)
                    .await
                    .expect("write malformed broker response");
            }
        });
        let decision = request_decision(
            &interaction("failure-session", label),
            &path,
            Duration::from_millis(20),
        )
        .await;
        assert_eq!(decision, None, "{label} must fall through");
        accept_task.await.expect("failure-path socket task stops");
        std::fs::remove_file(path).expect("remove failure-path socket");
    }
}

#[tokio::test]
async fn hook_subcommand_emits_nothing_for_malformed_input() {
    let output = tokio::task::spawn_blocking(|| {
        invoke_hook(
            "{ malformed",
            Path::new("/tmp/herdr-connect-rs-no-such-broker.sock"),
        )
    })
    .await
    .expect("malformed hook process task completes");
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
}

#[tokio::test]
async fn hook_subcommand_uses_real_broker_round_trip() {
    let (path, shutdown, task) = start_broker("hook-process");
    let output = tokio::task::spawn_blocking({
        let path = path.clone();
        move || invoke_hook(DEFAULT_FIXTURE, &path)
    })
    .await
    .expect("hook process task completes");
    assert!(output.status.success());
    let value: Value = serde_json::from_slice(&output.stdout).expect("hook output is JSON");
    assert_eq!(value["hookSpecificOutput"]["decision"]["behavior"], "allow");
    stop_broker(&path, shutdown, task).await;
}
