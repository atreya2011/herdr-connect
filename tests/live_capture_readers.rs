use std::fs;
use std::path::PathBuf;

use herdr_connect_rs::{claude_turn_start_position, read_claude_incremental};

fn temp_path(suffix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "herdr-connect-rs-live-capture-readers-{}-{suffix}.jsonl",
        std::process::id()
    ))
}

#[test]
fn read_claude_incremental_returns_only_new_records_since_an_offset() {
    let path = temp_path("offset");
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
    let path = temp_path("torn");
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
    let path = temp_path("recovers");
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
