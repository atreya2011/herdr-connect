use herdr_connect_rs::watch_transitions;

#[test]
fn diffs_reference_snapshots() {
    let snapshots = [
        [("term_a", "working")].as_slice(),
        [("term_a", "idle")].as_slice(),
    ];
    let actual = watch_transitions(&snapshots);
    assert_eq!(
        actual,
        vec![herdr_connect_rs::Transition {
            from: "working".into(),
            to: "idle".into(),
            terminal_id: "term_a".into()
        }]
    );
}
