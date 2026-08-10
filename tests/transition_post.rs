use herdr_connect_rs::{Transition, create_transition_messages};

#[test]
fn matches_reference_transition_colors_and_mentions() {
    let cases = [
        ("done", 0x57f287, None),
        ("blocked", 0xfee75c, Some("<@owner>")),
        ("idle", 0x57f287, None),
    ];
    for (to, color, mention) in cases {
        let messages = create_transition_messages(
            Transition {
                from: "working".into(),
                to: to.into(),
                terminal_id: "t".into(),
            },
            "final",
            8,
            "owner",
        );
        assert_eq!(messages[0].color, color);
        assert_eq!(messages[0].mention.as_deref(), mention);
    }
}

#[test]
fn preserves_reference_multipart_lengths_and_numbering() {
    let messages = create_transition_messages(
        Transition {
            from: "working".into(),
            to: "idle".into(),
            terminal_id: "t".into(),
        },
        &"x".repeat(4_000),
        1,
        "owner",
    );
    assert_eq!(messages.len(), 3);
    assert!(
        messages
            .iter()
            .all(|message| message.description.len() <= 2_000)
    );
}

#[test]
fn uses_failure_color() {
    let messages = create_transition_messages(
        Transition {
            from: "working".into(),
            to: "done".into(),
            terminal_id: "t".into(),
        },
        "final",
        0,
        "owner",
    );
    assert_eq!(messages[0].color, 0xed4245);
}
