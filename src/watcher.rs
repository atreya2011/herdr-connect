use crate::herdr::{STATUS_BLOCKED, STATUS_WORKING};

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Transition {
    pub from: String,
    pub to: String,
    pub terminal_id: String,
}

/// A `working` -> `blocked` transition: the only transition that posts a card. Done and idle post
/// nothing; assistant text reaches Discord live from the pane's vendor-log watch instead.
#[must_use]
pub fn is_postable_transition(t: &Transition) -> bool {
    t.from == STATUS_WORKING && t.to == STATUS_BLOCKED
}
