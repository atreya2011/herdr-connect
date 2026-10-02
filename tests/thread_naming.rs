use herdr_connect_rs::{format_thread_name, workspace_channel_name};

#[test]
fn thread_and_channel_names() {
    let long_label = "x".repeat(200);
    let truncated = format!("{} [tab-1]", "x".repeat(92));
    let thread_names = [
        ("build", "tab-1", Ok("build [tab-1]".to_owned())),
        (
            "amber-oak-river",
            "tab-2",
            Ok("amber-oak-river [tab-2]".to_owned()),
        ),
        (long_label.as_str(), "tab-1", Ok(truncated)),
        (
            "123",
            "tab-3",
            Err("herdr tab tab-3 still has the numeric label 123".to_owned()),
        ),
        (
            "  ",
            "tab-4",
            Err("herdr tab tab-4 has no label".to_owned()),
        ),
    ];
    for (label, tab, expected) in thread_names {
        assert_eq!(format_thread_name(label, tab), expected);
    }
    let channels = [(
        "ws",
        vec!["/repo/zeta".to_owned(), "/repo/alpha".to_owned()],
        "alpha-ws",
    )];
    for (workspace, cwds, expected) in channels {
        assert_eq!(workspace_channel_name(workspace, &cwds).unwrap(), expected);
    }
}
