use std::fs;

#[test]
fn discord_contracts_are_real_and_main_wires_library() {
    let delivery_src = fs::read_to_string("src/delivery.rs").unwrap();
    let status_src = fs::read_to_string("src/live_status.rs").unwrap();
    let topology_src = fs::read_to_string("src/topology.rs").unwrap();
    let main = fs::read_to_string("src/main.rs").unwrap();
    for entry in fs::read_dir("src").unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|ext| ext == "rs") {
            let src = fs::read_to_string(&path).unwrap();
            assert!(!src.contains("#![allow(clippy::"), "{}", path.display());
            assert!(!src.contains("block_on("), "{}", path.display());
        }
    }
    let delivery = &delivery_src[delivery_src
        .find("pub async fn deliver_transition")
        .unwrap()..];
    assert!(delivery.contains("nonce"));
    let status = &status_src[status_src.find("pub async fn update_live_status").unwrap()..];
    assert!(status.contains("update_message"));
    assert!(topology_src.contains("topic("));
    assert!(main.contains("create_transition_messages") || main.contains("sync_topology"));
}

#[test]
fn delivery_errors_advance_the_recorded_transition_state() {
    let main = fs::read_to_string("src/main.rs").unwrap();
    let delivery = &main[main.find("deliver_to_route(client").unwrap()..];
    let error_branch = &delivery[..delivery.find("continue;").unwrap()];
    assert!(error_branch.contains(
        "previous.insert(terminal.clone(), (status.clone(), agent));"
    ));
}
