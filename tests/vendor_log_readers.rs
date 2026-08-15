use herdr_connect_rs::{AgentSession, read_agent_log};
use std::path::Path;

#[test]
fn read_captured_vendor_logs() {
    let cases = [
        (
            AgentSession {
                agent: "claude".into(),
                value: "session".into(),
            },
            "tests/fixtures/claude-session.jsonl",
            "final answer",
        ),
        (
            AgentSession {
                agent: "codex".into(),
                value: "session".into(),
            },
            "tests/fixtures/codex-session.jsonl",
            "final answer",
        ),
        (
            AgentSession {
                agent: "cursor".into(),
                value: "session".into(),
            },
            "tests/fixtures/cursor-session.json",
            "final cursor",
        ),
    ];
    for (session, path, expected) in cases {
        assert_eq!(
            read_agent_log(Some(session), Path::new(path))
                .unwrap()
                .message,
            expected
        );
    }
}
