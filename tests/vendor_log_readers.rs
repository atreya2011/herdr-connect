use std::{fs, path::Path};

use herdr_connect_rs::{
    AgentSession, read_agent_log, read_claude_prompts_incremental, read_codex_prompts_incremental,
};

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
                agent: "codex".into(),
                value: "session".into(),
            },
            "tests/fixtures/codex-session-response-item.jsonl",
            "gamma",
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
fn read_captured_claude_terminal_prompt_with_position() {
    let cases = [(
        "tests/fixtures/claude-session.jsonl",
        273_u64,
        "current",
        335_u64,
        1_177_u64,
    )];
    for (path, start_offset, expected_prompt, expected_prompt_position, expected_checkpoint) in
        cases
    {
        let (records, new_offset) =
            read_claude_prompts_incremental(Path::new(path), start_offset).unwrap();
        assert_eq!(
            records,
            vec![(expected_prompt.to_owned(), expected_prompt_position)]
        );
        assert_eq!(
            new_offset, expected_checkpoint,
            "checkpoint must advance through every parsed line, not stop at the last prompt"
        );
    }
}

#[test]
fn read_captured_codex_terminal_prompts_once_with_position() {
    let cases = [
        (
            "tests/fixtures/codex-session.jsonl",
            236_u64,
            "current",
            311_u64,
            892_u64,
        ),
        (
            "tests/fixtures/codex-session-response-item.jsonl",
            771_u64,
            "current",
            846_u64,
            1_848_u64,
        ),
    ];
    for (path, start_offset, expected_prompt, expected_prompt_position, expected_checkpoint) in
        cases
    {
        let (records, new_offset) =
            read_codex_prompts_incremental(Path::new(path), start_offset).unwrap();
        assert_eq!(
            records,
            vec![(expected_prompt.to_owned(), expected_prompt_position)]
        );
        assert_eq!(
            new_offset, expected_checkpoint,
            "checkpoint must advance through every parsed line, not stop at the last prompt"
        );

        let (repeated_records, repeated_offset) =
            read_codex_prompts_incremental(Path::new(path), new_offset).unwrap();
        assert!(repeated_records.is_empty());
        assert_eq!(repeated_offset, new_offset);
    }
}

#[test]
fn read_claude_prompts_incremental_rejects_injected_and_compact_summary_records() {
    let (prompts, _) = read_claude_prompts_incremental(
        Path::new("tests/fixtures/claude-injected-prompts.jsonl"),
        0,
    )
    .expect("read Claude injected-content fixture");
    assert_eq!(
        prompts
            .into_iter()
            .map(|(text, _)| text)
            .collect::<Vec<_>>(),
        vec!["fix the build".to_owned()],
        "a compaction summary, an interruption notice, and every wrapped or echoed harness form \
         must be rejected; only the genuine typed prompt is returned"
    );
}

#[test]
fn read_codex_prompts_incremental_ignores_the_response_item_twin_and_injected_content() {
    let (prompts, _) =
        read_codex_prompts_incremental(Path::new("tests/fixtures/codex-injected-prompts.jsonl"), 0)
            .expect("read Codex injected-content fixture");
    assert_eq!(
        prompts
            .into_iter()
            .map(|(text, _)| text)
            .collect::<Vec<_>>(),
        vec!["fix the build".to_owned()],
        "an injected <environment_context> record written only as a response_item must be \
         rejected; only the genuine event_msg/user_message prompt is returned"
    );
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
