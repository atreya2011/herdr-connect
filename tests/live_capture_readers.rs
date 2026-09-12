use std::fs;
use std::path::PathBuf;

use herdr_connect_rs::{
    claude_turn_start_position, codex_turn_start_position, read_claude_incremental,
    read_codex_incremental, read_cursor_incremental, read_cursor_prompts_incremental,
};
use rusqlite::Connection;

fn temp_path(suffix: &str, extension: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "herdr-connect-rs-live-capture-readers-{}-{suffix}.{extension}",
        std::process::id()
    ))
}

#[test]
fn read_claude_incremental_returns_only_new_records_since_an_offset() {
    let path = temp_path("offset", "jsonl");
    let first_line = "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"first\"}]}}\n";
    fs::write(&path, first_line).expect("write claude fixture");

    let (first_pass, offset) = read_claude_incremental(&path, 0).expect("first read succeeds");
    assert_eq!(
        first_pass
            .into_iter()
            .map(|(text, _)| text)
            .collect::<Vec<_>>(),
        vec!["first".to_owned()]
    );

    let second_line = "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"second\"}]}}\n";
    fs::write(&path, format!("{first_line}{second_line}")).expect("append second record");
    let (second_pass, _) = read_claude_incremental(&path, offset).expect("second read succeeds");
    assert_eq!(
        second_pass
            .into_iter()
            .map(|(text, _)| text)
            .collect::<Vec<_>>(),
        vec!["second".to_owned()]
    );

    fs::remove_file(&path).expect("remove claude fixture");
}

#[test]
fn read_claude_incremental_defers_a_half_written_trailing_line() {
    let path = temp_path("torn", "jsonl");
    fs::write(
        &path,
        "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"first\"}]}}\n\
         {\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"second\"}]}}",
    )
    .expect("write torn claude fixture");

    let (texts, offset) = read_claude_incremental(&path, 0).expect("read tolerates torn tail");
    assert_eq!(texts, vec![("first".to_owned(), offset)]);

    fs::remove_file(&path).expect("remove torn claude fixture");
}

#[test]
fn read_claude_incremental_recovers_a_completed_record_at_the_same_offset() {
    let path = temp_path("recovers", "jsonl");
    let head = "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"first\"}]}}\n";
    let torn_tail = "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":[{\"type\":\"text\",\"text\":\"second\"}]}}";
    fs::write(&path, format!("{head}{torn_tail}")).expect("write torn claude fixture");
    let (_, deferred_offset) = read_claude_incremental(&path, 0).expect("first read defers tail");

    fs::write(&path, format!("{head}{torn_tail}\n")).expect("complete the trailing line");
    let (recovered, _) =
        read_claude_incremental(&path, deferred_offset).expect("second read recovers tail");
    assert_eq!(
        recovered,
        vec![("second".to_owned(), fs::metadata(&path).unwrap().len())]
    );

    fs::remove_file(&path).expect("remove claude fixture");
}

#[test]
fn claude_turn_start_position_resumes_at_the_reply_on_a_log_that_already_holds_one() {
    let path = "tests/fixtures/claude-session.jsonl";
    let start = claude_turn_start_position(path.as_ref()).expect("turn start resolves");
    let (texts, _) = read_claude_incremental(path.as_ref(), start).expect("read from turn start");
    assert_eq!(
        texts.into_iter().map(|(text, _)| text).collect::<Vec<_>>(),
        vec!["narration".to_owned(), "final answer".to_owned()]
    );
}

#[test]
fn read_codex_incremental_returns_current_turn_assistant_messages() {
    let cases = [(
        "tests/fixtures/codex-session-response-item.jsonl",
        695_u64,
        vec![
            ("priorreply".to_owned(), 433_u64),
            ("alpha".to_owned(), 958_u64),
            ("gamma".to_owned(), 1593_u64),
        ],
        vec![
            ("alpha".to_owned(), 958_u64),
            ("gamma".to_owned(), 1593_u64),
        ],
        1697_u64,
    )];
    for (path, expected_start, expected_all, expected_current, expected_end) in cases {
        let (all_messages, complete_offset) =
            read_codex_incremental(path.as_ref(), 0).expect("read complete Codex capture");
        assert_eq!(all_messages, expected_all);
        assert_eq!(complete_offset, expected_end);

        let start = codex_turn_start_position(path.as_ref()).expect("Codex turn start resolves");
        assert_eq!(start, expected_start);
        let (current_messages, current_offset) =
            read_codex_incremental(path.as_ref(), start).expect("read current Codex turn");
        assert_eq!(current_messages, expected_current);
        assert_eq!(current_offset, expected_end);
    }
}

#[test]
fn read_cursor_incremental_returns_only_new_rows_since_a_rowid() {
    let path = temp_path("rowid", "db");
    let connection = Connection::open(&path).expect("create cursor store");
    connection
        .execute("CREATE TABLE blobs (data BLOB)", [])
        .expect("create blobs table");
    let insert = |text: &str| {
        let row = format!(
            "{{\"role\":\"assistant\",\"content\":[{{\"type\":\"text\",\"text\":\"{text}\"}}]}}"
        );
        connection
            .execute("INSERT INTO blobs (data) VALUES (?1)", [row.as_bytes()])
            .expect("insert cursor row");
    };

    insert("first");
    let (first_pass, last_rowid) = read_cursor_incremental(&path, 0).expect("first read succeeds");
    assert_eq!(first_pass, vec![("first".to_owned(), 1)]);

    insert("second");
    let (second_pass, _) =
        read_cursor_incremental(&path, last_rowid).expect("second read succeeds");
    assert_eq!(second_pass, vec![("second".to_owned(), 2)]);

    drop(connection);
    fs::remove_file(&path).expect("remove cursor store");
}

/// The checkpoint this test expects is the highest rowid [`read_cursor_prompts_incremental`]
/// itself scans through, not the rowid of the last prompt found in range: round-1 finding 6
/// measured checkpoint-at-last-prompt as a defect (a 21.9 MB turn re-read on every tick, 20 ticks
/// costing 3.26 s instead of scanning through once), and the frozen expectation of rowid `1`
/// encoded exactly that defect. The orchestrator ruled it a genuinely wrong test under the
/// AGENTS.md red-green law and authorized updating this expected checkpoint value alone.
#[test]
fn read_cursor_prompts_incremental_returns_new_user_rows_once() {
    let cases = [(
        "tests/fixtures/cursor-session.json",
        0_i64,
        "current",
        1_i64,
        6_i64,
    )];
    for (fixture_path, start_rowid, expected_prompt, expected_prompt_rowid, expected_checkpoint) in
        cases
    {
        let path = temp_path("prompt-rowid", "db");
        let connection = Connection::open(&path).expect("create cursor store");
        connection
            .execute("CREATE TABLE blobs (data BLOB)", [])
            .expect("create blobs table");
        let rows: Vec<serde_json::Value> =
            serde_json::from_str(&fs::read_to_string(fixture_path).expect("read Cursor fixture"))
                .expect("parse Cursor fixture");
        for row in rows {
            let bytes = serde_json::to_vec(&row).expect("encode Cursor row");
            connection
                .execute("INSERT INTO blobs (data) VALUES (?1)", [bytes])
                .expect("insert Cursor row");
        }
        drop(connection);

        let first_observation = read_cursor_prompts_incremental(&path, start_rowid).and_then(
            |(first_pass, checkpoint)| {
                read_cursor_prompts_incremental(&path, checkpoint).map(
                    |(second_pass, repeated_checkpoint)| {
                        (first_pass, checkpoint, second_pass, repeated_checkpoint)
                    },
                )
            },
        );
        fs::remove_file(&path).expect("remove cursor store");
        let (first_pass, checkpoint, second_pass, repeated_checkpoint) =
            first_observation.expect("read Cursor prompts");

        assert_eq!(
            first_pass,
            vec![(expected_prompt.to_owned(), expected_prompt_rowid)]
        );
        assert_eq!(checkpoint, expected_checkpoint);
        assert!(second_pass.is_empty());
        assert_eq!(repeated_checkpoint, checkpoint);
    }
}
