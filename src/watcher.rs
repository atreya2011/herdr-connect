#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Transition {
    pub from: String,
    pub to: String,
    pub terminal_id: String,
    pub agent: String,
}

#[must_use]
pub fn is_postable_transition(t: &Transition) -> bool {
    t.from == "working" && matches!(t.to.as_str(), "blocked" | "done" | "idle")
}
#[must_use]
pub fn watch_transitions(snapshots: &[&[(&str, &str)]]) -> Vec<Transition> {
    let Some(first) = snapshots.first() else {
        return Vec::new();
    };
    let mut prior: std::collections::HashMap<&str, &str> = first
        .iter()
        .copied()
        .collect::<std::collections::HashMap<_, _>>(
    );
    let mut changes = Vec::new();
    for snapshot in &snapshots[1..] {
        for (terminal, status) in snapshot.iter().copied() {
            if let Some(old) = prior.get(terminal).filter(|old| **old != status) {
                changes.push(Transition {
                    from: (*old).into(),
                    to: status.into(),
                    terminal_id: terminal.into(),
                    agent: if terminal.starts_with("term_") {
                        "unknown".into()
                    } else {
                        (*terminal).into()
                    },
                });
            }
        }
        prior = snapshot
            .iter()
            .copied()
            .collect::<std::collections::HashMap<_, _>>();
    }
    changes
}
