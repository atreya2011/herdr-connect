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
    let tabs: Vec<HerdrTab> = agents
        .iter()
        .take(2)
        .map(|agent| HerdrTab {
            tab_id: agent.tab_id.clone().expect("captured tab id"),
            workspace_id: agent.workspace_id.clone().expect("captured workspace id"),
            label: "bridge".to_owned(),
        })
        .collect();
    let cases = agents.iter().take(2).collect::<Vec<_>>();
    for agent in cases {
        let route = route_topology(&agents, &tabs, &agent.terminal_id).unwrap();
        assert_eq!(route.workspace_id, agent.workspace_id.as_deref().unwrap());
        assert_eq!(route.tab_id, agent.tab_id.as_deref().unwrap());
        assert_eq!(route.pane_id, agent.pane_id.as_deref().unwrap());
        assert_eq!(route.channel_name, "project-4-wc");
        assert_eq!(
            route.thread_name,
            format!("bridge [{}]", agent.tab_id.as_deref().unwrap())
        );
    }
}

#[test]
fn captured_agent_list_preserves_live_socket_shape_statistics() {
    let value: Value = serde_json::from_str(include_str!("fixtures/herdr-agent-list.json"))
        .expect("captured snapshot is JSON");
    let agents = value["result"]["agents"]
        .as_array()
        .expect("captured agents are an array");
    let cases = [
        ("agent count", agents.len(), 43),
        (
            "foreground cwd count",
            agents
                .iter()
                .filter(|agent| agent.get("foreground_cwd").is_some())
                .count(),
            39,
        ),
        (
            "agent session count",
            agents
                .iter()
                .filter(|agent| agent.get("agent_session").is_some())
                .count(),
            30,
        ),
    ];
    for (description, actual, expected) in cases {
        assert_eq!(actual, expected, "{description}");
    }
    assert_eq!(value["result"]["type"], "agent_list");
}
