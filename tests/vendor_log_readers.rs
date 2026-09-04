use std::{fs, path::Path};

use herdr_connect_rs::{AgentSession, read_agent_log};

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
            read_agent_log(Some(&session), Path::new(path))
                .unwrap()
                .message,
            expected
        );
    }
}

#[test]
fn read_agent_log_tolerates_one_incomplete_trailing_line() {
    let cases = [
        (
            AgentSession {
                agent: "claude".into(),
                value: "session".into(),
            },
            "tests/fixtures/claude-session-mid-write.jsonl",
        ),
        (
            AgentSession {
                agent: "codex".into(),
                value: "session".into(),
            },
            "tests/fixtures/codex-session-mid-write.jsonl",
        ),
    ];
    for (session, path) in cases {
        let agent = session.agent.clone();
        let result = read_agent_log(Some(&session), Path::new(path));
        assert!(
            result.is_ok(),
            "{agent}: expected a truncated trailing line to be tolerated, got {result:?}"
        );
    }

    let claude = read_agent_log(
        Some(&AgentSession {
            agent: "claude".into(),
            value: "session".into(),
        }),
        Path::new("tests/fixtures/claude-session-mid-write.jsonl"),
    )
    .unwrap();
    assert_eq!(
        claude.question.as_deref(),
        Some("Which approach should we take?\n1. rewrite\n2. patch")
    );
}

#[test]
fn read_agent_log_rejects_non_trailing_corruption() {
    let result = read_agent_log(
        Some(&AgentSession {
            agent: "claude".into(),
            value: "session".into(),
        }),
        Path::new("tests/fixtures/claude-session-mid-corrupt.jsonl"),
    );
    assert!(
        result.is_err(),
        "a malformed non-final line must still fail the whole read"
    );
}

#[test]
fn missing_cursor_store_is_not_created() {
    let cases = [("cursor", "vendor-log")];
    for (agent, suffix) in cases {
        let path = std::env::temp_dir().join(format!(
            "herdr-connect-rs-missing-cursor-store-{}-{suffix}.db",
            std::process::id(),
        ));
        let _ = fs::remove_file(&path);

        let result = read_agent_log(
            Some(&AgentSession {
                agent: agent.into(),
                value: "session".into(),
            }),
            &path,
        );
        let created = path.exists();
        let _ = fs::remove_file(&path);

        assert!(result.is_err(), "{agent} missing store should fail");
        assert!(!created, "{agent} missing store should not be created");
    }
}
