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
