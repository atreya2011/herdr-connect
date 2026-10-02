use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};

/// Runs the built binary with an empty environment and a Herdr socket path that does not exist,
/// so a process that wrongly starts the bridge cannot reach a real Herdr session. A process still
/// running after the bound is killed and the test fails.
fn run_with_empty_environment(args: &[&str]) -> Output {
    let missing_socket: PathBuf = std::env::temp_dir().join(format!(
        "herdr-connect-rs-startup-{}-missing.sock",
        std::process::id()
    ));
    let mut child = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"))
        .args(args)
        .env_clear()
        .env("HERDR_SOCKET_PATH", &missing_socket)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn herdr-connect-rs");
    let deadline = Instant::now() + Duration::from_secs(3);
    while child.try_wait().expect("poll child").is_none() {
        if Instant::now() >= deadline {
            child.kill().expect("kill child that did not exit");
            child.wait().expect("reap killed child");
            panic!("herdr-connect-rs {args:?} was still running after 3 seconds");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    child.wait_with_output().expect("collect child output")
}

#[test]
fn bridge_fails_at_startup_when_discord_variables_are_missing() {
    let output = run_with_empty_environment(&[]);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(
            "Missing required environment variables: DISCORD_TOKEN, DISCORD_GUILD_ID, DISCORD_OWNER_ID"
        ),
        "stderr was: {stderr}"
    );
}
