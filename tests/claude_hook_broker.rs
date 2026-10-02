use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::UnixStream;

use herdr_connect_rs::{
    ClaudePermissionToolInput, Decision, DecisionBehavior, Interaction, PermissionVendor,
    decode_claude_permission_request, encode_claude_decision, request_decision,
};

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

fn interaction(session_id: &str, prompt_id: &str) -> Interaction {
    Interaction {
        session_id: session_id.to_owned(),
        prompt_id: prompt_id.to_owned(),
        tool_name: "Bash".to_owned(),
        tool_input: ClaudePermissionToolInput {
            command: format!("touch {prompt_id}"),
            description: format!("Create {prompt_id}"),
        },
        vendor: PermissionVendor::Claude,
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

fn invoke_hook_without_socket(payload: &str) -> std::process::Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
        .arg("hook")
        .env_remove("HERDR_CLAUDE_BROKER_SOCKET")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
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

struct BrokerProcess {
    child: Child,
    path: PathBuf,
}

impl BrokerProcess {
    fn start(label: &str) -> Self {
        let path = socket_path(label);
        let child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
            .args(["broker", "--socket"])
            .arg(&path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn shipped broker");
        Self { child, path }
    }

    async fn wait_until_ready(&self) {
        for _ in 0..100 {
            if self.path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("shipped broker did not create its socket");
    }

    fn signal(&self, signal: &str) {
        let status = Command::new("kill")
            .args([&format!("-{signal}"), &self.child.id().to_string()])
            .status()
            .expect("send broker signal");
        assert!(status.success(), "broker signal failed: {status}");
    }

    fn terminate(mut self) -> ExitStatus {
        self.signal("CONT");
        self.signal("TERM");
        let status = self.child.wait().expect("wait for shipped broker");
        assert!(!self.path.exists(), "broker socket was not cleaned up");
        status
    }

    fn terminate_with_stderr(mut self) -> (ExitStatus, String) {
        self.signal("CONT");
        self.signal("TERM");
        let mut stderr = self.child.stderr.take().expect("broker stderr is piped");
        let status = self.child.wait().expect("wait for shipped broker");
        let mut stderr_contents = String::new();
        stderr
            .read_to_string(&mut stderr_contents)
            .expect("read broker stderr");
        assert!(!self.path.exists(), "broker socket was not cleaned up");
        (status, stderr_contents)
    }
}

impl Drop for BrokerProcess {
    fn drop(&mut self) {
        if self
            .child
            .try_wait()
            .expect("check broker status")
            .is_none()
        {
            let _ = Command::new("kill")
                .args(["-CONT", &self.child.id().to_string()])
                .status();
            let _ = Command::new("kill")
                .args(["-TERM", &self.child.id().to_string()])
                .status();
            let _ = self.child.wait();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

#[test]
fn permission_fixtures_decode_and_encode_allow_and_deny() {
    let cases = [
        ("default", DEFAULT_FIXTURE, Decision::allow(), None),
        (
            "allow",
            ALLOW_FIXTURE,
            Decision::deny("operator denied this request".to_owned()),
            Some("operator denied this request"),
        ),
    ];
    for (name, payload, decision, message) in cases {
        let interaction = decode_claude_permission_request(payload.as_bytes())
            .unwrap_or_else(|error| panic!("{name} fixture decodes: {error}"));
        assert_eq!(interaction.tool_name, "Bash");
        let encoded = encode_claude_decision(&decision);
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
async fn real_broker_failure_cases_fall_through() {
    let broker = BrokerProcess::start("timeout");
    broker.wait_until_ready().await;
    broker.signal("STOP");
    let decision = request_decision(
        &interaction("failure-session", "timeout"),
        &broker.path,
        Duration::from_millis(20),
    )
    .await;
    assert_eq!(decision, None, "a stopped real broker must time out");
    broker.terminate();

    let broker = BrokerProcess::start("malformed-request");
    broker.wait_until_ready().await;
    let decision = request_decision(
        &Interaction {
            session_id: "failure-session".to_owned(),
            prompt_id: "malformed".to_owned(),
            tool_name: String::new(),
            tool_input: ClaudePermissionToolInput {
                command: "command".to_owned(),
                description: "description".to_owned(),
            },
            vendor: PermissionVendor::Claude,
        },
        &broker.path,
        Duration::from_millis(20),
    )
    .await;
    assert_eq!(decision, None, "a malformed request must fall through");
    broker.terminate();
}

#[tokio::test]
async fn oversized_frame_is_rejected_before_the_peer_closes() {
    let broker = BrokerProcess::start("oversized-frame");
    broker.wait_until_ready().await;
    let mut stream = UnixStream::connect(&broker.path)
        .await
        .expect("connect broker");
    stream
        .write_all(&vec![b'x'; 64 * 1024 + 1])
        .await
        .expect("write oversized frame");
    let mut byte = [0; 1];
    let read = tokio::time::timeout(Duration::from_millis(100), stream.read(&mut byte))
        .await
        .expect("broker must reject an oversized frame promptly")
        .expect("read broker close");
    assert_eq!(read, 0, "broker must close the oversized request");
    broker.terminate();
}

#[tokio::test]
async fn real_broker_drops_a_partial_initial_frame_within_the_deadline() {
    let broker = BrokerProcess::start("partial-initial-frame");
    broker.wait_until_ready().await;
    let mut stream = UnixStream::connect(&broker.path)
        .await
        .expect("connect broker");
    stream.write_all(b"{").await.expect("write partial frame");
    let mut byte = [0; 1];
    let read = tokio::time::timeout(Duration::from_secs(11), stream.read(&mut byte))
        .await
        .expect("broker must drop an incomplete frame within its deadline")
        .expect("read broker close");
    assert_eq!(read, 0, "broker must close the incomplete request");
    broker.terminate();
}

#[tokio::test]
async fn real_broker_logs_initial_frame_rejection_reasons() {
    let cases = [
        (
            "oversized-frame-log",
            vec![b'x'; 64 * 1024 + 1],
            "oversized broker frame",
        ),
        (
            "malformed-frame-log",
            b"not json\n".to_vec(),
            "malformed broker frame",
        ),
        ("empty-frame-log", b"\n".to_vec(), "empty broker frame"),
    ];
    for (label, frame, reason) in cases {
        let broker = BrokerProcess::start(label);
        broker.wait_until_ready().await;
        let mut stream = UnixStream::connect(&broker.path)
            .await
            .expect("connect broker");
        stream
            .write_all(&frame)
            .await
            .expect("write rejected frame");
        let mut byte = [0; 1];
        let read = tokio::time::timeout(Duration::from_millis(100), stream.read(&mut byte))
            .await
            .expect("broker must reject the frame promptly")
            .expect("read broker close");
        assert_eq!(read, 0, "broker must close the rejected request");
        let (status, stderr) = broker.terminate_with_stderr();
        assert!(status.success(), "broker did not stop cleanly: {status}");
        assert!(
            stderr
                .lines()
                .any(|line| line.contains(&format!("broker rejected initial frame: {reason}")),),
            "broker stderr did not name {reason}: {stderr}"
        );
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
async fn hook_subcommand_fails_loudly_when_socket_configuration_is_missing() {
    let output = tokio::task::spawn_blocking(|| invoke_hook_without_socket(DEFAULT_FIXTURE))
        .await
        .expect("misconfigured hook process task completes");
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}

#[tokio::test]
async fn broker_restarts_after_sigterm() {
    let broker = BrokerProcess::start("sigterm-first");
    broker.wait_until_ready().await;
    let status = broker.terminate();
    assert!(status.success(), "broker did not stop cleanly: {status}");

    let broker = BrokerProcess::start("sigterm-second");
    broker.wait_until_ready().await;
    let status = broker.terminate();
    assert!(
        status.success(),
        "restarted broker did not stop cleanly: {status}"
    );
}

#[tokio::test]
async fn hook_subcommand_falls_through_when_broker_cannot_map_the_session() {
    let broker = BrokerProcess::start("hook-process");
    broker.wait_until_ready().await;
    let output = tokio::task::spawn_blocking({
        let path = broker.path.clone();
        move || invoke_hook(DEFAULT_FIXTURE, &path)
    })
    .await
    .expect("hook process task completes");
    assert!(output.status.success());
    assert!(output.stdout.is_empty());
    broker.terminate();
}
