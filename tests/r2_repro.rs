use std::path::Path;

use herdr_connect_rs::{
    AgentLogCapture, AgentSession, Transition, create_transition_messages, format_thread_name,
    list_agents, read_agent_log, tab_list, watch_transitions,
};

fn capture(message: &str) -> AgentLogCapture {
    AgentLogCapture {
        message: message.into(),
        failure: None,
        question: None,
    }
}
fn done() -> Transition {
    Transition {
        from: "working".into(),
        to: "done".into(),
        terminal_id: "t1".into(),
        agent: "claude".into(),
    }
}
fn bodies(parts: &[herdr_connect_rs::TransitionMessage]) -> Vec<String> {
    parts
        .iter()
        .map(|m| {
            m.description
                .split_once('\n')
                .map_or_else(|| m.description.clone(), |(_, r)| r.to_owned())
        })
        .collect()
}

#[test]
fn plain_split_is_lossless_and_line_aware() {
    let body = (0..40)
        .map(|i| format!("line {i:02} {}", "y".repeat(52)))
        .collect::<Vec<_>>()
        .join("\n");
    let parts = create_transition_messages(&done(), &capture(&body), "owner");
    assert!(parts.len() > 1);
    assert_eq!(bodies(&parts).join("\n"), body);
}

#[test]
fn short_fence_is_unchanged_and_oversized_fences_are_bounded() {
    let short = "intro\n```\ncode\n```";
    let parts = create_transition_messages(&done(), &capture(short), "owner");
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].description, short);
    let large = format!("```rust\n{}\n```", "x".repeat(4_000));
    assert!(
        create_transition_messages(&done(), &capture(&large), "owner")
            .iter()
            .all(|p| p.description.len() <= 1_900)
    );
}

#[test]
fn multi_fences_remain_balanced() {
    let body = format!(
        "```js\n{}\n```\n{}\n```py\n{}\n```",
        "A".repeat(1_000),
        "P".repeat(1_000),
        "B".repeat(1_000)
    );
    assert!(
        create_transition_messages(&done(), &capture(&body), "owner")
            .iter()
            .all(|p| p.description.matches("```").count() % 2 == 0)
    );
}

#[test]
fn claude_question_and_empty_response_contracts() {
    let question = concat!(
        r#"{"type":"user","message":{"content":[{"type":"text","text":"go"}]}}"#,
        "\n",
        r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"AskUserQuestion","id":"q1","input":{"questions":[{"question":"Ship it?","options":[{"label":"yes"},{"label":"no"}]}]}}]}}"#,
        "\n"
    );
    let path = std::env::temp_dir().join(format!("r2-question-{}", std::process::id()));
    std::fs::write(&path, question).unwrap();
    let log = read_agent_log(
        Some(AgentSession {
            agent: "claude".into(),
            value: "q".into(),
        }),
        &path,
    )
    .unwrap();
    assert_eq!(log.question.as_deref(), Some("Ship it?\n1. yes\n2. no"));
    std::fs::write(
        &path,
        concat!(
            r#"{"type":"user","message":{"content":[{"type":"text","text":"go"}]}}"#,
            "\n",
            r#"{"type":"assistant","message":{"content":[{"type":"text","text":""}]}}"#,
            "\n"
        ),
    )
    .unwrap();
    assert!(
        read_agent_log(
            Some(AgentSession {
                agent: "claude".into(),
                value: "q".into()
            }),
            &path
        )
        .is_err()
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn thread_names_reject_empty_base_and_suffix_only_names() {
    assert!(format_thread_name("", "title-should-not-be-used", "tab-1").is_err());
    assert!(
        format_thread_name("123", "   ", "tab-9")
            .unwrap_err()
            .contains("tab-9")
    );
    assert!(format_thread_name("build", "title", &"z".repeat(97)).is_err());
}

#[test]
fn watcher_forgets_disappeared_agents_and_preserves_identity() {
    let snapshots = [
        [("t1", "working")].as_slice(),
        [].as_slice(),
        [("t1", "idle")].as_slice(),
    ];
    assert!(watch_transitions(&snapshots).is_empty());
    let snapshots = [[("t1", "working")].as_slice(), [("t1", "idle")].as_slice()];
    assert_ne!(watch_transitions(&snapshots)[0].agent, "unknown");
}

#[test]
fn details_are_populated_and_malformed_agents_are_errors() {
    let log = read_agent_log(
        Some(AgentSession {
            agent: "codex".into(),
            value: "codex-session".into(),
        }),
        Path::new("tests/fixtures/agent-log-codex.jsonl"),
    )
    .unwrap();
    assert!(log.details.is_some());
    let path = std::env::temp_dir().join(format!("r2-malformed-{}", std::process::id()));
    unsafe {
        std::env::set_var("HERDR_SOCKET_PATH", &path);
    }
    let _ = std::fs::remove_file(&path);
    assert!(list_agents().is_err());
    assert!(!tab_list().is_empty());
}
