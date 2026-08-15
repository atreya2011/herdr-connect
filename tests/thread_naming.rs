use herdr_connect_rs::{format_thread_name, workspace_channel_name};

#[test]
fn thread_and_channel_names() {
    let thread_names = [
        ("build", "ignored", "tab-1", Ok("build [tab-1]".to_owned())),
        (
            "123",
            "terminal title",
            "tab-2",
            Ok("terminal title [tab-2]".to_owned()),
        ),
    ];
    for (label, title, tab, expected) in thread_names {
        assert_eq!(format_thread_name(label, title, tab), expected);
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
