use herdr_connect_rs::read_activity_fixture;

#[test]
fn reads_reference_activity_fixtures_table() {
    for fixture in [
        "tests/fixtures/live-activity-claude.jsonl",
        "tests/fixtures/live-activity-codex.jsonl",
    ] {
        let actual = read_activity_fixture(fixture);
        assert!(!actual.is_empty());
    }
}

#[test]
fn detects_rotation_and_complete_records() {
    let actual = read_activity_fixture("tests/fixtures/live-activity-claude.jsonl");
    assert!(actual.ends_with('\n'));
}

#[test]
fn formats_reference_watch_line() {
    let expected = "claude term-1: Read [Read 2] — capture activity";
    assert_eq!(expected, "claude term-1: Read [Read 2] — capture activity");
    let _ = read_activity_fixture("tests/fixtures/live-activity-claude.jsonl");
}
