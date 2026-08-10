use std::path::Path;

use herdr_connect_rs::{
    AgentLogCapture, AgentSession, Transition, create_transition_messages, read_agent_log,
    watch_transitions,
};

// L1: src/lib.rs:610 — the agent identity is decided by the literal terminal name `term_a`.
#[test]
fn l1_agent_identity_does_not_depend_on_the_fixture_terminal_name() {
    let a = watch_transitions(&[
        [("term_a", "working")].as_slice(),
        [("term_a", "idle")].as_slice(),
    ])[0]
        .agent
        .clone();
    let b = watch_transitions(&[
        [("term_b", "working")].as_slice(),
        [("term_b", "idle")].as_slice(),
    ])[0]
        .agent
        .clone();
    assert_eq!(
        a, b,
        "same snapshot shape, different terminal name, different agent identity"
    );
}

// L3: src/lib.rs:153 — an answered AskUserQuestion is still reported as the pending question.
#[test]
fn l3_answered_question_is_not_reposted() {
    let log = concat!(
        r#"{"type":"user","message":{"content":[{"type":"text","text":"go"}]}}"#,
        "\n",
        r#"{"type":"assistant","message":{"content":[{"type":"tool_use","name":"AskUserQuestion","id":"q1","input":{"questions":[{"question":"Ship it?","options":[{"label":"yes"}]}]}}]}}"#,
        "\n",
        r#"{"type":"user","message":{"content":[{"type":"tool_result","tool_use_id":"q1","content":"yes"}]}}"#,
        "\n",
        r#"{"type":"assistant","message":{"content":[{"type":"text","text":"final answer"}]}}"#,
        "\n"
    );
    let path = std::env::temp_dir().join(format!("r3-answered-{}", std::process::id()));
    std::fs::write(&path, log).unwrap();
    let parsed = read_agent_log(
        Some(AgentSession {
            agent: "claude".into(),
            value: "s".into(),
        }),
        &path,
    )
    .unwrap();
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        parsed.question, None,
        "question q1 was answered by a tool_result and must not be pending"
    );
}

// L4: src/lib.rs:443 — PARITY 27: failure text and tool metadata never reach the rendered card.
#[test]
fn l4_failure_text_and_metadata_reach_the_card() {
    let messages = create_transition_messages(
        &Transition {
            from: "working".into(),
            to: "done".into(),
            terminal_id: "t1".into(),
            agent: "claude".into(),
        },
        &AgentLogCapture {
            message: "final".into(),
            failure: Some("Bash: exit 2".into()),
            question: None,
        },
        "owner",
    );
    let rendered = messages
        .iter()
        .map(|m| m.description.clone())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        rendered.contains("Bash: exit 2"),
        "failure text is dropped, card was: {rendered}"
    );
}

// L5: src/lib.rs:89 — a Cursor store without a `meta` table is rejected by a query whose result is discarded.
#[test]
fn l5_cursor_store_without_meta_table_is_readable() {
    let path = std::env::temp_dir().join(format!("r3-cursor-{}.sqlite", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let script = format!(
        "import sqlite3,json\nc=sqlite3.connect({:?})\nc.execute('CREATE TABLE blobs (id INTEGER PRIMARY KEY, data BLOB)')\nrows=[{{'data':{{'role':'user','content':[{{'type':'text','text':'go'}}]}}}},{{'data':{{'role':'assistant','content':[{{'type':'text','text':'final cursor'}}]}}}}]\nfor i,r in enumerate(rows): c.execute('INSERT INTO blobs VALUES (?,?)',(i,json.dumps(r).encode()))\nc.commit()\n",
        path.to_string_lossy()
    );
    let status = std::process::Command::new("python3")
        .arg("-c")
        .arg(script)
        .status()
        .unwrap();
    assert!(status.success(), "fixture build failed");
    let parsed = read_agent_log(
        Some(AgentSession {
            agent: "cursor".into(),
            value: "s".into(),
        }),
        Path::new(&path),
    );
    let _ = std::fs::remove_file(&path);
    assert_eq!(
        parsed.map(|log| log.message),
        Ok("final cursor".to_owned()),
        "a store carrying readable blobs was rejected"
    );
}
