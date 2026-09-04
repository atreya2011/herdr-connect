use crate::herdr::{STATUS_BLOCKED, STATUS_DONE, STATUS_IDLE, STATUS_WORKING};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Transition {
    pub from: String,
    pub to: String,
    pub terminal_id: String,
    pub agent: String,
}

#[must_use]
pub fn is_postable_transition(t: &Transition) -> bool {
    t.from == STATUS_WORKING && matches!(t.to.as_str(), STATUS_BLOCKED | STATUS_DONE | STATUS_IDLE)
}
