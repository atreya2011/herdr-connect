use herdr_connect_rs::format_thread_name;

#[test]
fn formats_reference_thread_name_table() {
    let cases = [
        ("build", "ignored", "tab-1", "build [tab-1]"),
        ("123", "terminal title", "tab-2", "terminal title [tab-2]"),
    ];
    for (label, title, tab, expected) in cases {
        assert_eq!(format_thread_name(label, title, tab).unwrap(), expected);
    }
}
