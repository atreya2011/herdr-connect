use std::path::Path;

use herdr_connect_rs::{
    AgentLogCapture, AgentSession, Transition, create_transition_messages, format_thread_name,
    read_activity_fixture, read_agent_log, request_rpc, tab_list,
};

#[test]
fn utf8_split_does_not_panic() {
    let message = format!("{}{}", "a".repeat(1899), "é");
    let result = std::panic::catch_unwind(|| {
        create_transition_messages(
            &Transition {
                from: "working".into(),
                to: "done".into(),
                terminal_id: "t".into(),
                agent: "unknown".into(),
            },
            &AgentLogCapture {
                message,
                failure: None,
                question: None,
            },
            "owner",
        )
    });
    assert!(result.is_ok());
}

#[test]
fn log_reader_uses_file_contents_and_identity() {
    assert_eq!(
        read_agent_log(
            Some(AgentSession {
                agent: "codex".into(),
                value: "any".into()
            }),
            Path::new("/missing")
        ),
        Err("agent stopped, no log available".into())
    );
    let path = std::env::temp_dir().join(format!("agent-log-{}", std::process::id()));
    std::fs::write(&path, "not json\n").unwrap();
    assert!(
        read_agent_log(
            Some(AgentSession {
                agent: "claude".into(),
                value: "s".into()
            }),
            &path
        )
        .is_err()
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn rpc_and_tabs_are_not_constants() {
    assert_ne!(request_rpc("totally.bogus.method"), "herdr RPC error");
    assert_ne!(
        tab_list()
            .iter()
            .map(|tab| tab.tab_id.as_str())
            .collect::<Vec<_>>(),
        vec!["tab.list"]
    );
}

#[test]
fn blocked_mention_is_first_part_only_and_parts_are_numbered() {
    let messages = create_transition_messages(
        &Transition {
            from: "working".into(),
            to: "blocked".into(),
            terminal_id: "t".into(),
            agent: "unknown".into(),
        },
        &AgentLogCapture {
            message: "x".repeat(4_000),
            failure: None,
            question: None,
        },
        "owner",
    );
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].mention.as_deref(), Some("<@owner>"));
    assert!(messages[1..].iter().all(|m| m.mention.is_none()));
    assert_eq!(
        messages
            .iter()
            .map(|m| m.description.lines().next().unwrap())
            .collect::<Vec<_>>(),
        ["1/3", "2/3", "3/3"]
    );
}

#[test]
fn thread_names_trim_and_truncate() {
    assert_eq!(
        format_thread_name(&"a".repeat(200), "title", "tab-1").unwrap(),
        format!("{} [tab-1]", "a".repeat(92))
    );
    assert_eq!(
        format_thread_name("  build  ", "title", "tab-1").unwrap(),
        "build [tab-1]"
    );
    assert!(format_thread_name("123", "   ", "tab-1").is_err());
}

#[test]
fn activity_read_errors_are_not_empty() {
    assert_ne!(
        read_activity_fixture("/nonexistent/directory/activity.jsonl"),
        ""
    );
}

#[test]
fn fence_split_keeps_fences_balanced() {
    let messages = create_transition_messages(
        &Transition {
            from: "working".into(),
            to: "done".into(),
            terminal_id: "t".into(),
            agent: "unknown".into(),
        },
        &AgentLogCapture {
            message: format!("```js\n{}\n```", "x".repeat(2_000)),
            failure: None,
            question: None,
        },
        "owner",
    );
    assert!(
        messages
            .iter()
            .all(|m| m.description.matches("```").count() % 2 == 0)
    );
}
