use std::fs;

#[test]
fn discord_contracts_are_real_and_main_wires_library() {
    let lib = fs::read_to_string("src/lib.rs").unwrap();
    let main = fs::read_to_string("src/main.rs").unwrap();
    assert!(!lib.contains("#![allow(clippy::"));
    assert!(!lib.contains("block_on("));
    let delivery = &lib[lib.find("pub async fn deliver_transition").unwrap()..];
    assert!(
        delivery[..delivery.find("pub async fn update_live_status").unwrap()].contains("nonce")
    );
    let status = &lib[lib.find("pub async fn update_live_status").unwrap()..];
    assert!(status.contains("update_message"));
    assert!(lib.contains("topic("));
    assert!(main.contains("create_transition_messages") || main.contains("sync_topology"));
}
