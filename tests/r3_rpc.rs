use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;

use herdr_connect_rs::{list_agents, tab_list};

/// Answers exactly one newline-delimited JSON-RPC request, echoing the request id.
fn serve_once(name: &str, result: &'static str) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("r3-rpc-{}-{name}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).unwrap();
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request = String::new();
        reader.read_line(&mut request).unwrap();
        let id = serde_json::from_str::<serde_json::Value>(&request).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned();
        let mut stream = stream;
        stream
            .write_all(format!("{{\"id\":\"{id}\",\"result\":{result}}}\n").as_bytes())
            .unwrap();
        stream.flush().unwrap();
    });
    path
}

// P1: src/lib.rs:722 — PARITY 5: tab ID, working directory and vendor session identity are dropped.
#[test]
fn p1_agent_snapshot_preserves_tab_cwd_and_session() {
    let socket = serve_once(
        "fields",
        concat!(
            r#"{"agents":[{"agent":"claude","terminal_id":"t1","agent_status":"working","#,
            r#""tab_id":"tab-7","cwd":"/home/dev/project","session":{"agent":"claude","value":"sess-1"}}]}"#
        ),
    );
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", &socket) };
    let agents = list_agents().unwrap();
    let _ = std::fs::remove_file(&socket);
    let rendered = format!("{agents:?}");
    assert!(
        rendered.contains("tab-7")
            && rendered.contains("/home/dev/project")
            && rendered.contains("sess-1"),
        "snapshot lost tab/cwd/session, got: {rendered}"
    );
}

// P2: src/lib.rs:731 — PARITY 4: an agent entry missing `agent_status` is silently discarded.
#[test]
fn p2_malformed_agent_entry_is_an_error() {
    let socket = serve_once(
        "malformed",
        r#"{"agents":[{"agent":"claude","terminal_id":"t1"}]}"#,
    );
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", &socket) };
    let agents = list_agents();
    let _ = std::fs::remove_file(&socket);
    assert!(
        agents.is_err(),
        "a malformed agent entry was dropped instead of failing: {agents:?}"
    );
}

// P3: src/lib.rs:698 — PARITY 3: an unreachable Herdr yields a fabricated tab instead of an error.
#[test]
fn p3_unreachable_herdr_does_not_fabricate_a_tab() {
    unsafe {
        std::env::set_var(
            "HERDR_SOCKET_PATH",
            std::env::temp_dir().join("r3-rpc-absent.sock"),
        );
    };
    let tabs = tab_list();
    assert!(
        tabs.is_empty(),
        "invented tabs while Herdr was unreachable: {tabs:?}"
    );
}
