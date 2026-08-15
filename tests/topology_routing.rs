use herdr_connect_rs::{AgentSnapshot, HerdrTab, route_topology};
use serde_json::Value;

fn snapshot<T: serde::de::DeserializeOwned>(name: &str, key: &str) -> T {
    let value: Value = serde_json::from_str(match name {
        "agents" => include_str!("fixtures/herdr-agent-list.json"),
        "tabs" => include_str!("fixtures/herdr-tab-list.json"),
        _ => unreachable!(),
    })
    .expect("captured snapshot is JSON");
    serde_json::from_value(value["result"][key].clone()).expect("captured shape is valid")
}

#[test]
fn route_captured_herdr_snapshots() {
    let agents: Vec<AgentSnapshot> = snapshot("agents", "agents");
    let tabs: Vec<HerdrTab> = snapshot("tabs", "tabs");
    let cases = [
        ("term-real-1", "real-workspace:pane-1"),
        ("term-real-2", "real-workspace:pane-2"),
    ];
    for (terminal, pane) in cases {
        let route = route_topology(&agents, &tabs, terminal).unwrap();
        assert_eq!(route.workspace_id, "real-workspace");
        assert_eq!(route.tab_id, "real-workspace:tab-1");
        assert_eq!(route.pane_id, pane);
        assert_eq!(route.channel_name, "bridge-real-workspace");
        assert_eq!(route.thread_name, "bridge [real-workspace:tab-1]");
    }
}
