use herdr_connect_rs::{
    AgentLogCapture, AgentSession, Transition, create_transition_messages, is_postable_transition,
    read_agent_log, watch_transitions,
};

#[test]
fn binary_matches_reference_runtime_surface() {
    let main = std::fs::read_to_string("src/main.rs").unwrap();
    for needle in [
        "load_config",
        "tab_list",
        "format_thread_name",
        "is_postable_transition",
    ] {
        assert!(
            main.contains(needle),
            "main missing runtime wiring for {needle}"
        );
    }
}

#[test]
fn main_requires_postable_filter() {
    let changes = watch_transitions(&[&[("t1", "idle")], &[("t1", "working")]]);
    assert!(!is_postable_transition(&changes[0]));
    assert!(
        std::fs::read_to_string("src/main.rs")
            .unwrap()
            .contains("is_postable_transition")
    );
}

#[test]
fn main_resolves_vendor_session_logs_not_terminal_paths() {
    let main = std::fs::read_to_string("src/main.rs").unwrap();
    assert!(!main.contains("PathBuf::from(terminal)") && main.contains("agent_session"));
}

#[test]
fn main_persists_and_reuses_live_status_message_ids() {
    let main = std::fs::read_to_string("src/main.rs").unwrap();
    assert!(main.contains("live_messages.insert") || main.contains("live_messages.entry"));
}

#[test]
fn main_delivers_mention_content_for_blocked() {
    let messages = create_transition_messages(
        &Transition {
            from: "working".into(),
            to: "blocked".into(),
            terminal_id: "t".into(),
            agent: "claude".into(),
        },
        &AgentLogCapture {
            message: "final".into(),
            failure: None,
            question: Some("choose?".into()),
        },
        "owner",
    );
    assert_eq!(messages[0].mention.as_deref(), Some("<@owner>"));
    assert!(
        std::fs::read_to_string("src/main.rs")
            .unwrap()
            .contains("mention")
    );
}

#[test]
fn sync_topology_creates_threads_not_guild_text_for_tabs() {
    let lib = std::fs::read_to_string("src/lib.rs").unwrap();
    let body = &lib[lib.find("pub async fn sync_topology").unwrap()
        ..lib.find("pub async fn deliver_transition").unwrap()];
    assert!(body.contains("create_thread") || body.contains("CreateThread"));
}

#[test]
fn sync_topology_matches_workspace_by_topic_not_channel_name() {
    let lib = std::fs::read_to_string("src/lib.rs").unwrap();
    let body = &lib[lib.find("pub async fn sync_topology").unwrap()
        ..lib.find("pub async fn deliver_transition").unwrap()];
    assert!(!body.contains("channel.name.as_deref() == Some(workspace_name.as_str())"));
}

#[test]
fn answered_ask_user_question_is_not_kept_as_question() {
    let path = std::env::temp_dir().join(format!("r3-ans-{}", std::process::id()));
    std::fs::write(&path, concat!(
        r#"{"type":"user","message":{"role":"user","content":"hi"}}"#, "\n",
        r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","name":"AskUserQuestion","id":"q1","input":{"questions":[{"question":"Ship?","options":[{"label":"yes"}]}]}}]}}"#, "\n",
        r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"q1","content":"yes"}]}}"#, "\n",
        r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"done"}]}}"#, "\n",
    )).unwrap();
    let log = read_agent_log(
        Some(AgentSession {
            agent: "claude".into(),
            value: "s".into(),
        }),
        &path,
    )
    .unwrap();
    assert_eq!(log.question, None);
    let _ = std::fs::remove_file(path);
}

#[test]
fn watch_transitions_must_not_hardcode_term_a_agent() {
    assert!(
        !std::fs::read_to_string("src/lib.rs")
            .unwrap()
            .contains("terminal == \"term_a\"")
    );
}

#[test]
fn main_installs_rustls_provider_and_allows_console_mode() {
    let main = std::fs::read_to_string("src/main.rs").unwrap();
    assert!(main.contains("install_default"));
    assert!(!main.contains("DISCORD_TOKEN") || main.contains("load_config"));
}

fn cursor_db(name: &str, wrapped: bool) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("r3-{name}-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute("CREATE TABLE meta (key TEXT, value TEXT)", [])
        .unwrap();
    connection
        .execute("CREATE TABLE blobs (id TEXT, data BLOB)", [])
        .unwrap();
    let row = |role: &str, message: &str| {
        if wrapped {
            serde_json::json!({"data":{"role":role,"content":[{"type":"text","text":message}]}})
        } else {
            serde_json::json!({"role":role,"content":[{"type":"text","text":message}]})
        }
    };
    for (id, value) in [
        ("m-user", row("user", "hi")),
        ("z-old", row("assistant", "old-by-rowid")),
        ("a-new", row("assistant", "new-by-rowid")),
    ] {
        connection
            .execute(
                "INSERT INTO blobs VALUES (?1, ?2)",
                rusqlite::params![id, serde_json::to_vec(&value).unwrap()],
            )
            .unwrap();
    }
    connection.close().unwrap();
    path
}

#[test]
fn cursor_sqlite_reference_shape_and_rowid() {
    let path = cursor_db("real", false);
    let log = read_agent_log(
        Some(AgentSession {
            agent: "cursor".into(),
            value: "s".into(),
        }),
        &path,
    );
    assert_eq!(log.unwrap().message, "new-by-rowid");
    let _ = std::fs::remove_file(path);
}

#[test]
fn cursor_sqlite_order_by_id_picks_storage_order() {
    let path = cursor_db("wrapped", true);
    let log = read_agent_log(
        Some(AgentSession {
            agent: "cursor".into(),
            value: "s".into(),
        }),
        &path,
    )
    .unwrap();
    assert_eq!(log.message, "new-by-rowid");
    let _ = std::fs::remove_file(path);
}
