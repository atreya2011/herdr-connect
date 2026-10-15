use crate::herdr::{STATUS_BLOCKED, STATUS_WORKING};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Transition {
    pub from: String,
    pub to: String,
    pub terminal_id: String,
}

#[must_use]
pub fn is_postable_transition(t: &Transition) -> bool {
    t.from == STATUS_WORKING && t.to == STATUS_BLOCKED
}
