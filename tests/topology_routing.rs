use serde_json::Value;

use herdr_connect_rs::{AgentSnapshot, HerdrTab, route_topology};

#[test]
fn route_captured_herdr_snapshots() {
    let value: Value = serde_json::from_str(include_str!("fixtures/herdr-agent-list.json"))
        .expect("captured snapshot is JSON");
    let agents: Vec<AgentSnapshot> =
        serde_json::from_value(value["result"]["agents"].clone()).expect("captured shape is valid");
    let tabs: Vec<HerdrTab> = agents
        .iter()
        .take(2)
        .map(|agent| HerdrTab {
            tab_id: agent.tab_id.clone(),
            workspace_id: agent.workspace_id.clone(),
            label: "bridge".to_owned(),
        })
        .collect();
    let cases = agents.iter().take(2).collect::<Vec<_>>();
    for agent in cases {
        let route = route_topology(&agents, &tabs, &agent.terminal_id).unwrap();
        assert_eq!(route.workspace_id, agent.workspace_id);
        assert_eq!(route.tab_id, agent.tab_id);
        assert_eq!(route.pane_id, agent.pane_id);
        assert_eq!(route.channel_name, "project-4-wc");
        assert_eq!(route.thread_name, format!("bridge [{}]", agent.tab_id));
    }
}

/// Herdr 0.9.0 may omit `agent` entirely while a pane's agent is still being detected. That entry
/// must still deserialize (not fail the whole `agent.list`).
#[test]
fn agent_list_entry_missing_agent_parses_as_unmirrored() {
    let value: Value = serde_json::from_str(include_str!("fixtures/herdr-agent-list.json"))
        .expect("captured snapshot is JSON");
    let agents: Vec<AgentSnapshot> =
        serde_json::from_value(value["result"]["agents"].clone()).expect("captured shape is valid");
    let detecting = agents
        .iter()
        .find(|agent| agent.agent.is_none())
        .expect("fixture contains an entry with no detected agent");
    assert_eq!(detecting.agent_status, "unknown");
    assert!(
        detecting.session.is_none(),
        "an undetected agent must not carry a session"
    );
}
