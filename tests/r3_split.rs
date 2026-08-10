use herdr_connect_rs::{AgentLogCapture, Transition, create_transition_messages};

fn render(body: &str) -> Vec<String> {
    create_transition_messages(
        &Transition {
            from: "working".into(),
            to: "done".into(),
            terminal_id: "t1".into(),
            agent: "claude".into(),
        },
        &AgentLogCapture {
            message: body.into(),
            failure: None,
            question: None,
        },
        "owner",
    )
    .into_iter()
    .map(|m| m.description)
    .collect()
}

// S1: src/lib.rs:490 — a long fence info string drives the per-chunk budget to 1 and breaks fencing.
#[test]
fn s1_long_fence_info_string_keeps_parts_bounded_and_balanced() {
    let body = format!("```{}\ncode\n```", "x".repeat(2_000));
    let parts = render(&body);
    assert!(
        parts.iter().all(|p| p.len() <= 1_900),
        "part lengths: {:?}",
        parts.iter().map(String::len).collect::<Vec<_>>()
    );
    assert!(
        parts.iter().all(|p| p.matches("```").count() % 2 == 0),
        "unbalanced fences across {} parts",
        parts.len()
    );
}
