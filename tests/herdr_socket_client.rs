use serde_json::Value;

use herdr_connect_rs::request_rpc_result;

#[test]
fn real_socket_read_only_contract() {
    if std::env::var_os("HERDR_SOCKET_PATH").is_none() {
        eprintln!("skipped: HERDR_SOCKET_PATH is not configured");
        return;
    }
    let cases = [("agent.list", "agents"), ("tab.list", "tabs")];
    for (method, key) in cases {
        let value: Value = serde_json::from_str(
            &request_rpc_result(method).expect("real Herdr read-only method succeeds"),
        )
        .expect("real Herdr result is JSON");
        assert!(value.get(key).is_some_and(Value::is_array));
    }
    let error = request_rpc_result("method.invalid.for.test")
        .expect_err("invalid method returns the real error envelope");
    assert!(error.contains("herdr method.invalid.for.test failed"));
}
