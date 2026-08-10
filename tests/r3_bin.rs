use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::process::{Command, Stdio};

/// Answers every `agent.list` request on a fresh socket, echoing the request id.
fn serve(name: &str, result: &'static str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("r3-bin-{}-{name}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).unwrap();
    std::thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request = String::new();
            if reader.read_line(&mut request).is_err() || request.is_empty() {
                continue;
            }
            let id = serde_json::from_str::<serde_json::Value>(&request)
                .ok()
                .and_then(|value| value["id"].as_str().map(str::to_owned))
                .unwrap_or_default();
            let mut stream = stream;
            let _ =
                stream.write_all(format!("{{\"id\":\"{id}\",\"result\":{result}}}\n").as_bytes());
            let _ = stream.flush();
        }
    });
    path
}

fn run(
    socket: &std::path::Path,
    discord: bool,
    millis: u64,
) -> (String, String, Option<std::process::ExitStatus>) {
    let mut command = Command::new(env!("CARGO_BIN_EXE_herdr-connect-rs"));
    command
        .env("HERDR_SOCKET_PATH", socket)
        .env("HERDR_POLL_INTERVAL_MS", "200")
        .env_remove("DISCORD_TOKEN")
        .env_remove("DISCORD_GUILD_ID")
        .env_remove("DISCORD_OWNER_ID")
        .env_remove("DISCORD_CHANNEL_ID")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if discord {
        command
            .env("DISCORD_TOKEN", "token")
            .env("DISCORD_GUILD_ID", "1")
            .env("DISCORD_OWNER_ID", "2");
    }
    let mut child = command.spawn().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(millis));
    let status = child.try_wait().unwrap();
    let _ = child.kill();
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        status,
    )
}

const ONE_AGENT: &str =
    r#"{"agents":[{"agent":"claude","terminal_id":"t1","agent_status":"working"}]}"#;
const MALFORMED: &str = r#"{"agent_list":[]}"#;

// B1: src/main.rs:50 — PARITY 38: the console watcher must run without Discord configuration.
#[test]
fn b1_runs_without_discord_configuration() {
    let socket = serve("nodiscord", ONE_AGENT);
    let (stdout, stderr, status) = run(&socket, false, 1_200);
    let _ = std::fs::remove_file(&socket);
    assert!(
        status.is_none(),
        "exited without Discord config: status={status:?} stdout={stdout:?} stderr={stderr:?}"
    );
}

// B2: src/main.rs:56/67 — the process must survive startup and one malformed poll.
#[test]
fn b2_survives_startup_and_a_malformed_poll() {
    let socket = serve("malformed", MALFORMED);
    let (stdout, stderr, status) = run(&socket, true, 1_200);
    let _ = std::fs::remove_file(&socket);
    assert!(
        status.is_none(),
        "process died: status={status:?} stdout={stdout:?} stderr={stderr:?}"
    );
}

// B3: src/main.rs — PARITY 37: the watcher prints `<agent> <terminal_id>: <from> -> <to>`.
#[test]
fn b3_prints_watch_lines() {
    let socket = serve("print", ONE_AGENT);
    let (stdout, stderr, _status) = run(&socket, true, 1_200);
    let _ = std::fs::remove_file(&socket);
    assert!(
        !stdout.is_empty(),
        "the watcher printed nothing; stderr was: {stderr}"
    );
}
