use std::path::Path;

use herdr_connect_rs::{AgentSession, read_agent_log};

#[test]
fn captures_reference_fixture_expectations_table() {
    let cases = [
        (
            "claude",
            "claude-session",
            "agent-log-claude.jsonl",
            "typed slash command response",
            0,
        ),
        (
            "claude",
            "claude-answered-session",
            "agent-log-claude-answered.jsonl",
            "answered final",
            3,
        ),
        (
            "claude",
            "claude-plan-files-session",
            "agent-log-claude-plan-files.jsonl",
            "finished",
            3,
        ),
        (
            "codex",
            "codex-session",
            "agent-log-codex.jsonl",
            "final answer",
            4,
        ),
        (
            "codex",
            "codex-147-session",
            "agent-log-codex-147.jsonl",
            "final 0.147 answer",
            2,
        ),
        (
            "codex",
            "codex-147-final-stop-session",
            "agent-log-codex-147-final-stop.jsonl",
            "Acknowledged",
            0,
        ),
        (
            "cursor",
            "cursor-session",
            "agent-log-cursor.json",
            "final cursor",
            4,
        ),
    ];
    for (agent, value, fixture, expected_message, expected_tools) in cases {
        let actual = read_agent_log(
            Some(AgentSession {
                agent: agent.into(),
                value: value.into(),
            }),
            Path::new("tests/fixtures").join(fixture).as_path(),
        )
        .unwrap();
        assert_eq!(actual.message, expected_message);
        assert_eq!(actual.tool_calls, expected_tools);
    }
}

#[test]
fn reports_pointer_for_missing_and_malformed_logs() {
    let actual = read_agent_log(None, Path::new("tests/fixtures/bad.jsonl")).unwrap_err();
    assert_eq!(actual, "agent stopped, no log available");
}
