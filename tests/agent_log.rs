use herdr_connect_rs::{AgentSession, read_agent_log};

#[test]
fn captures_reference_fixture_expectations_table() {
    let cases = [
        ("claude", "typed slash command response", 0),
        ("claude-answered", "answered final", 3),
        ("claude-plan-files", "finished", 3),
        ("codex", "final answer", 4),
        ("codex-147", "final 0.147 answer", 2),
        ("codex-147-final-stop", "Acknowledged", 0),
        ("cursor", "final cursor", 4),
    ];
    for (fixture, expected_message, expected_tools) in cases {
        let actual = read_agent_log(Some(AgentSession {
            agent: fixture.into(),
            value: fixture.into(),
        }))
        .unwrap();
        assert_eq!(actual.message, expected_message);
        assert_eq!(actual.tool_calls, expected_tools);
    }
}

#[test]
fn reports_pointer_for_missing_and_malformed_logs() {
    let actual = read_agent_log(None).unwrap_err();
    assert_eq!(actual, "agent stopped, no log available");
}
