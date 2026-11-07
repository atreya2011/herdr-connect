use std::fs;

use rusqlite::Connection;

use herdr_connect_rs::{AgentSession, read_agent_log};

#[test]
fn unreadable_cursor_row_fails_the_read() {
    let path = std::env::temp_dir().join(format!(
        "herdr-connect-rs-unreadable-cursor-row-{}.db",
        std::process::id()
    ));
    let _ = fs::remove_file(&path);
    let connection = Connection::open(&path).expect("create cursor store");
    connection
        .execute("CREATE TABLE blobs (data BLOB)", [])
        .expect("create blobs table");
    connection
        .execute(
            "INSERT INTO blobs (data) VALUES (?1)",
            [br#"{"role":"assistant","content":[{"type":"text","text":"answer"}]}"#.as_slice()],
        )
        .expect("insert Cursor row");
    connection
        .execute("INSERT INTO blobs (data) VALUES ('not a blob')", [])
        .expect("insert text row");
    drop(connection);

    let result = read_agent_log(
        &AgentSession {
            agent: "cursor".into(),
            value: "session".into(),
        },
        &path,
    );
    let _ = fs::remove_file(&path);

    assert!(
        result.is_err(),
        "a row that cannot be read must fail the read, got {result:?}"
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
            &AgentSession {
                agent: agent.into(),
                value: "session".into(),
            },
            &path,
        );
        let created = path.exists();
        let _ = fs::remove_file(&path);

        let error = result.expect_err("a missing store should fail");
        assert!(
            error.contains(path.to_str().expect("utf-8 temp path"))
                && error.contains("unable to open database file"),
            "{agent}: the error must name the store and the SQLite failure, got {error:?}"
        );
        assert!(!created, "{agent} missing store should not be created");
    }
}
