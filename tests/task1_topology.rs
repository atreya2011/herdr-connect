use herdr_connect_rs::{AgentSnapshot, HerdrTab, format_thread_name, route_topology};
use serde_json::Value;

fn fixture_agents() -> Vec<AgentSnapshot> {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/herdr-agent-list-task1.json")).unwrap();
    serde_json::from_value(fixture["result"]["agents"].clone()).unwrap()
}

fn fixture_tabs() -> Vec<HerdrTab> {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/herdr-tab-list-task1.json")).unwrap();
    serde_json::from_value(fixture["result"]["tabs"].clone()).unwrap()
}

#[test]
fn routes_the_captured_herdr_snapshot_to_its_workspace_tab_and_pane() {
    let route = route_topology(&fixture_agents(), &fixture_tabs(), "term_657e35d8c8ea11").unwrap();
    assert_eq!(route.workspace_id, "wC");
    assert_eq!(route.tab_id, "wC:tG");
    assert_eq!(route.pane_id, "wC:pQ");
    assert_eq!(route.channel_name, "project-4-wc");
    assert_eq!(route.thread_name, "captured-tab [wC:tG]");
}

#[test]
fn routes_two_agents_in_one_tab_to_the_same_thread() {
    let mut agents = fixture_agents();
    let mut duplicate = fixture_agents().pop().unwrap();
    duplicate.pane_id = Some("wC:pR".to_owned());
    duplicate.terminal_id = "term_657e35d8c8ea12".to_owned();
    agents.push(duplicate);
    let first = route_topology(&agents, &fixture_tabs(), "term_657e35d8c8ea11").unwrap();
    let second = route_topology(&agents, &fixture_tabs(), "term_657e35d8c8ea12").unwrap();
    assert_eq!(first.thread_name, second.thread_name);
    assert_eq!(first.pane_id, "wC:pQ");
    assert_eq!(second.pane_id, "wC:pR");
}

#[test]
fn preserves_the_reference_numeric_label_error() {
    assert_eq!(
        format_thread_name("7", "", "w1:t7").unwrap_err(),
        "herdr tab w1:t7 has numeric label 7 without a terminal title"
    );
}
