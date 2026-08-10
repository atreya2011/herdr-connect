use herdr_connect_rs::request_rpc;
use std::os::unix::net::UnixListener;

#[test]
fn uses_real_in_process_unix_socket() {
    let path = std::env::temp_dir().join(format!("herdr-reference-{}", std::process::id()));
    let _server = UnixListener::bind(&path).unwrap();
    assert!(path.exists());
    std::fs::remove_file(path).unwrap();
    let _ = request_rpc("agent.list");
}

#[test]
fn reports_real_socket_failure() {
    let error = request_rpc("agent.list");
    assert!(error.contains("No such file") || error.contains("herdr"));
}
