use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::sync::mpsc::Receiver;

use herdr_connect_rs::{
    agent_prompt, list_agents, request_rpc_result_with_params, tab_list, tab_list_result,
};
use serde_json::{Value, json};
use serial_test::serial;

/// Answers exactly one newline-delimited JSON-RPC request, echoing the request id.
fn serve_once(name: &str, result: impl Into<String>) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("r3-rpc-{}-{name}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).unwrap();
    let result = result.into();
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

fn captured_result(path: &str) -> String {
    serde_json::from_str::<serde_json::Value>(path).unwrap()["result"].to_string()
}

/// Captures one newline-delimited JSON-RPC request and returns a supplied envelope.
fn serve_capturing_request(
    name: &str,
    mut response: Value,
) -> (std::path::PathBuf, Receiver<Value>) {
    let path = std::env::temp_dir().join(format!("r3-rpc-{}-{name}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request = String::new();
        reader.read_line(&mut request).unwrap();
        let request: Value = serde_json::from_str(&request).unwrap();
        sender.send(request.clone()).unwrap();
        response["id"] = request["id"].clone();
        let mut stream = stream;
        writeln!(stream, "{response}").unwrap();
        stream.flush().unwrap();
    });
    (path, receiver)
}

/// Captures one request and returns a response without changing its envelope id.
fn serve_fixed_response(name: &str, response: Value) -> (std::path::PathBuf, Receiver<Value>) {
    let path = std::env::temp_dir().join(format!("r3-rpc-{}-{name}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request = String::new();
        reader.read_line(&mut request).unwrap();
        let request: Value = serde_json::from_str(&request).unwrap();
        sender.send(request).unwrap();
        let mut stream = stream;
        writeln!(stream, "{response}").unwrap();
        stream.flush().unwrap();
    });
    (path, receiver)
}

/// Validates the installed Herdr agent.prompt wait shape before returning its result.
fn serve_prompt_wait_contract(name: &str) -> (std::path::PathBuf, Receiver<Value>) {
    let path = std::env::temp_dir().join(format!("r3-rpc-{}-{name}", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path).unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request_line = String::new();
        reader.read_line(&mut request_line).unwrap();
        let request: Value = serde_json::from_str(&request_line).unwrap();
        sender.send(request.clone()).unwrap();
        let response = if request["params"]["wait"] == true {
            json!({
                "id": request["id"],
                "error": {
                    "code": "invalid_request",
                    "message": "invalid request: invalid type: boolean `true`, expected struct AgentPromptWaitOptions at line 1 column 137",
                },
            })
        } else if request["params"]["wait"].is_object() {
            json!({
                "id": request["id"],
                "result": {"status": "agent_prompted"},
            })
        } else {
            json!({
                "id": request["id"],
                "error": {
                    "code": "invalid_request",
                    "message": "agent.prompt wait must be an object",
                },
            })
        };
        let mut stream = stream;
        writeln!(stream, "{response}").unwrap();
        stream.flush().unwrap();
    });
    (path, receiver)
}

#[test]
#[serial]
fn t1_captured_herdr_responses_preserve_topology_identity() {
    let agent_socket = serve_once(
        "task1-agent",
        captured_result(include_str!("fixtures/herdr-agent-list-task1.json")),
    );
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", &agent_socket) };
    let agents = list_agents().unwrap();
    let _ = std::fs::remove_file(&agent_socket);
    assert_eq!(agents[0].workspace_id.as_deref(), Some("wC"));
    assert_eq!(agents[0].tab_id.as_deref(), Some("wC:tG"));
    assert_eq!(agents[0].pane_id.as_deref(), Some("wC:pQ"));

    let tab_socket = serve_once(
        "task1-tab",
        captured_result(include_str!("fixtures/herdr-tab-list-task1.json")),
    );
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", &tab_socket) };
    let tabs = tab_list();
    let _ = std::fs::remove_file(&tab_socket);
    assert_eq!(tabs[0].workspace_id, "wC");
    assert_eq!(tabs[0].tab_id, "wC:tG");
    assert_eq!(tabs[0].label, "captured-tab");
}

// P1: src/lib.rs:722 — PARITY 5: tab ID, working directory and vendor session identity are dropped.
#[test]
#[serial]
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
#[serial]
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
#[serial]
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

#[test]
#[serial]
fn tab_list_result_propagates_rpc_errors() {
    let socket = std::env::temp_dir().join(format!("r3-rpc-{}-absent", std::process::id()));
    let _ = std::fs::remove_file(&socket);
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", socket) };
    assert!(tab_list_result().unwrap_err().contains("connect failed"));
}

#[test]
#[serial]
fn tab_list_result_rejects_any_malformed_tab() {
    let socket = serve_once(
        "malformed-tab",
        r#"{"tabs":[{"tab_id":"tab-7","workspace_id":"ws"}]}"#,
    );
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", &socket) };
    let error = tab_list_result().unwrap_err();
    let _ = std::fs::remove_file(&socket);
    assert!(error.contains("missing field `label`"), "{error}");
}

#[test]
#[serial]
fn request_rpc_result_with_params_sends_json_object_unchanged() {
    let params = json!({
        "target": "pane-7",
        "nested": {"keep": ["ordering", 3]},
        "enabled": true,
    });
    let (socket, requests) =
        serve_capturing_request("params", json!({"result": {"accepted": true}}));
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", &socket) };

    let result = request_rpc_result_with_params("custom.method", &params).unwrap();
    let request = requests.recv().unwrap();
    let _ = std::fs::remove_file(&socket);

    assert_eq!(request["method"], "custom.method");
    assert_eq!(request["params"], params);
    assert_eq!(result, r#"{"accepted":true}"#);
}

#[test]
#[serial]
fn agent_prompt_sends_wait_and_surfaces_stalled_error() {
    let (socket, requests) = serve_capturing_request(
        "prompt-stalled",
        json!({
            "error": {
                "code": "agent_prompt_stalled",
                "message": "no observed state change within 5000ms",
            },
        }),
    );
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", &socket) };

    let error = agent_prompt("pane-7", "inspect the failing test").unwrap_err();
    let request = requests.recv().unwrap();
    let _ = std::fs::remove_file(&socket);

    assert_eq!(request["method"], "agent.prompt");
    assert_eq!(
        request["params"],
        json!({"target": "pane-7", "text": "inspect the failing test", "wait": {}})
    );
    assert!(error.contains("agent_prompt_stalled"), "{error}");
}

#[test]
#[serial]
fn agent_prompt_uses_wait_options_object() {
    let (socket, requests) = serve_prompt_wait_contract("prompt-wait-shape");
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", &socket) };

    let result = agent_prompt("pane-7", "inspect the failing test");
    let request = requests.recv().unwrap();
    let _ = std::fs::remove_file(&socket);

    assert_eq!(request["method"], "agent.prompt");
    assert_eq!(request["params"]["wait"], json!({}));
    assert_eq!(result.unwrap(), r#"{"status":"agent_prompted"}"#);
}

#[test]
#[serial]
fn agent_prompt_surfaces_error_with_mismatched_empty_id() {
    let (socket, requests) = serve_fixed_response(
        "prompt-error-empty-id",
        json!({
            "id": "",
            "error": {
                "code": "invalid_request",
                "message": "prompt envelope sentinel",
            },
        }),
    );
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", &socket) };

    let error = agent_prompt("pane-7", "inspect the failing test").unwrap_err();
    let _ = requests.recv().unwrap();
    let _ = std::fs::remove_file(&socket);

    assert!(error.contains("invalid_request"), "{error}");
    assert!(error.contains("prompt envelope sentinel"), "{error}");
}

#[test]
#[serial]
fn request_rpc_result_with_params_reports_both_mismatched_ids() {
    let (socket, requests) = serve_fixed_response(
        "response-id-mismatch",
        json!({"id": "returned-id", "result": {"accepted": true}}),
    );
    unsafe { std::env::set_var("HERDR_SOCKET_PATH", &socket) };

    let error = request_rpc_result_with_params("custom.method", &json!({})).unwrap_err();
    let request = requests.recv().unwrap();
    let _ = std::fs::remove_file(&socket);
    let expected_id = request["id"].as_str().unwrap();

    assert!(error.contains(expected_id), "{error}");
    assert!(error.contains("returned-id"), "{error}");
}
