use std::collections::HashMap;
use std::collections::hash_map::Entry as MapEntry;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use tokio::sync::oneshot;

use crate::{Decision, QuestionAnswer, QuestionOption};
const TOKEN_BYTES: usize = 24;

/// Generates one opaque, random hex token for a newly issued registry entry.
///
/// # Errors
///
/// Returns an error when `/dev/urandom` cannot be read.
fn generate_token() -> Result<String, String> {
    let mut token_bytes = [0_u8; TOKEN_BYTES];
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut token_bytes))
        .map_err(|error| format!("cannot create interaction token: {error}"))?;
    let mut token = String::with_capacity(TOKEN_BYTES * 2);
    for byte in token_bytes {
        write!(&mut token, "{byte:02x}").map_err(|_| "token formatting failed".to_owned())?;
    }
    Ok(token)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalRequest {
    pub channel_id: u64,
    pub session_id: String,
}
#[derive(Debug)]
pub struct IssuedApproval {
    pub token: String,
    pub receiver: oneshot::Receiver<Decision>,
    pub expiry: Instant,
}
#[derive(Debug)]
struct Entry {
    request: ApprovalRequest,
    expiry: Instant,
    hook_alive: Arc<AtomicBool>,
    sender: oneshot::Sender<Decision>,
}
#[derive(Debug, Eq, PartialEq)]
pub enum ResolveError {
    UnknownOrExpired,
    WrongChannel,
}
#[derive(Default, Debug)]
pub struct InteractionRegistry {
    entries: Mutex<HashMap<String, Entry>>,
}
impl InteractionRegistry {
    fn lock_entries(&self) -> MutexGuard<'_, HashMap<String, Entry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
    /// Issues one token for a pending permission card.
    ///
    /// # Errors
    ///
    /// Returns an error when a token cannot be generated.
    pub fn issue_with_liveness(
        &self,
        request: ApprovalRequest,
        expiry: Instant,
        hook_alive: Arc<AtomicBool>,
    ) -> Result<IssuedApproval, String> {
        let token = generate_token()?;
        Ok(self.issue_with_token_and_liveness(token, request, expiry, hook_alive))
    }
    /// Inserts a supplied token for deterministic state-machine tests.
    #[cfg(test)]
    fn issue_with_token(
        &self,
        token: String,
        request: ApprovalRequest,
        expiry: Instant,
    ) -> IssuedApproval {
        self.issue_with_token_and_liveness(token, request, expiry, Arc::new(AtomicBool::new(true)))
    }
    fn issue_with_token_and_liveness(
        &self,
        token: String,
        request: ApprovalRequest,
        expiry: Instant,
        hook_alive: Arc<AtomicBool>,
    ) -> IssuedApproval {
        let (sender, receiver) = oneshot::channel();
        self.lock_entries().insert(
            token.clone(),
            Entry {
                request,
                expiry,
                hook_alive,
                sender,
            },
        );
        IssuedApproval {
            token,
            receiver,
            expiry,
        }
    }
    /// Resolves one pending token exactly once.
    ///
    /// # Errors
    ///
    /// Returns the rejection reason for an unknown, expired, or wrong-channel interaction.
    pub fn resolve(
        &self,
        token: &str,
        channel_id: u64,
        decision: Decision,
        now: Instant,
    ) -> Result<(), ResolveError> {
        let mut entries = self.lock_entries();
        let MapEntry::Occupied(occupied) = entries.entry(token.to_owned()) else {
            return Err(ResolveError::UnknownOrExpired);
        };
        let entry = occupied.get();
        if now >= entry.expiry || !entry.hook_alive.load(Ordering::Acquire) {
            return Err(ResolveError::UnknownOrExpired);
        }
        if channel_id != entry.request.channel_id {
            return Err(ResolveError::WrongChannel);
        }
        let sender = occupied.remove().sender;
        drop(entries);
        let _ = sender.send(decision);
        Ok(())
    }
    pub fn has_pending_session(&self, session_id: &str) -> bool {
        self.lock_entries().values().any(|entry| {
            entry.request.session_id == session_id && entry.hook_alive.load(Ordering::Acquire)
        })
    }
    pub fn remove(&self, token: &str) -> bool {
        self.lock_entries().remove(token).is_some()
    }
}

#[derive(Debug)]
pub struct IssuedQuestion {
    pub token: String,
    pub receiver: oneshot::Receiver<QuestionAnswer>,
    pub expiry: Instant,
}
#[derive(Debug)]
struct QuestionEntry {
    request: ApprovalRequest,
    /// The card's own options, so a component tap naming an option by index can be resolved back
    /// to its label without the caller re-supplying the question.
    options: Vec<QuestionOption>,
    expiry: Instant,
    hook_alive: Arc<AtomicBool>,
    sender: oneshot::Sender<QuestionAnswer>,
}
/// Correlates one Discord question card per `AskUserQuestion` question with the hook awaiting its
/// answer, mirroring [`InteractionRegistry`] with a [`QuestionAnswer`] resolution instead of a
/// [`Decision`].
#[derive(Default, Debug)]
pub struct QuestionRegistry {
    entries: Mutex<HashMap<String, QuestionEntry>>,
}
impl QuestionRegistry {
    fn lock_entries(&self) -> MutexGuard<'_, HashMap<String, QuestionEntry>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
    /// Issues one token for a pending question card.
    ///
    /// # Errors
    ///
    /// Returns an error when a token cannot be generated or the lifetime is invalid.
    pub fn issue_with_liveness(
        &self,
        request: ApprovalRequest,
        options: Vec<QuestionOption>,
        created_at: Instant,
        expiry: Instant,
        hook_alive: Arc<AtomicBool>,
    ) -> Result<IssuedQuestion, String> {
        let token = generate_token()?;
        if expiry <= created_at {
            return Err("invalid question registry entry".to_owned());
        }
        let (sender, receiver) = oneshot::channel();
        self.lock_entries().insert(
            token.clone(),
            QuestionEntry {
                request,
                options,
                expiry,
                hook_alive,
                sender,
            },
        );
        Ok(IssuedQuestion {
            token,
            receiver,
            expiry,
        })
    }
    /// The label of `index` among the token's own options, if the token is still pending.
    pub fn option_label(&self, token: &str, index: usize) -> Option<String> {
        self.lock_entries()
            .get(token)
            .and_then(|entry| entry.options.get(index))
            .map(|option| option.label.clone())
    }
    /// Resolves one pending token exactly once.
    ///
    /// # Errors
    ///
    /// Returns the rejection reason for an unknown, expired, or wrong-channel question.
    pub fn resolve(
        &self,
        token: &str,
        channel_id: u64,
        answer: QuestionAnswer,
        now: Instant,
    ) -> Result<(), ResolveError> {
        let mut entries = self.lock_entries();
        let MapEntry::Occupied(occupied) = entries.entry(token.to_owned()) else {
            return Err(ResolveError::UnknownOrExpired);
        };
        let entry = occupied.get();
        if now >= entry.expiry || !entry.hook_alive.load(Ordering::Acquire) {
            return Err(ResolveError::UnknownOrExpired);
        }
        if channel_id != entry.request.channel_id {
            return Err(ResolveError::WrongChannel);
        }
        let sender = occupied.remove().sender;
        drop(entries);
        let _ = sender.send(answer);
        Ok(())
    }
    /// The token of the pending question card for `session_id`, if one is currently open: the one
    /// card a thread reply can be consumed against as a free-text answer, single-select or
    /// multiSelect alike.
    pub fn pending_question_token(&self, session_id: &str) -> Option<String> {
        self.lock_entries()
            .iter()
            .find(|(_, entry)| {
                entry.request.session_id == session_id && entry.hook_alive.load(Ordering::Acquire)
            })
            .map(|(token, _)| token.clone())
    }
    pub fn remove(&self, token: &str) -> bool {
        self.lock_entries().remove(token).is_some()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use super::{ApprovalRequest, InteractionRegistry, ResolveError};
    use crate::Decision;
    fn request(channel_id: u64, session_id: &str) -> ApprovalRequest {
        ApprovalRequest {
            channel_id,
            session_id: session_id.to_owned(),
        }
    }
    #[test]
    fn registry_state_machine_rejects_invalid_taps() {
        let now = Instant::now();
        let cases = [("wrong channel", 8, ResolveError::WrongChannel)];
        for (_, channel, expected) in cases {
            let registry = InteractionRegistry::default();
            let issued = registry.issue_with_token(
                "token".to_owned(),
                request(7, "session"),
                now + Duration::from_secs(30),
            );
            assert_eq!(
                registry.resolve(&issued.token, channel, Decision::allow(), now,),
                Err(expected)
            );
        }
    }
    #[test]
    fn pending_lookup_is_scoped_to_the_vendor_session() {
        let now = Instant::now();
        let registry = InteractionRegistry::default();
        registry.issue_with_token(
            "session-scoped-token".to_owned(),
            request(7, "session-a"),
            now + Duration::from_secs(30),
        );

        for (session_id, expected) in [("session-a", true), ("session-b", false)] {
            assert_eq!(registry.has_pending_session(session_id), expected);
        }
    }
    #[tokio::test]
    async fn registry_issues_and_resolves_exactly_once() {
        let now = Instant::now();
        let registry = InteractionRegistry::default();
        let issued = registry.issue_with_token(
            "opaque-token".to_owned(),
            request(7, "session"),
            now + Duration::from_secs(30),
        );
        registry
            .resolve(&issued.token, 7, Decision::deny("no".to_owned()), now)
            .expect("first tap resolves");
        assert_eq!(issued.receiver.await, Ok(Decision::deny("no".to_owned())));
        assert_eq!(
            registry.resolve("opaque-token", 7, Decision::allow(), now,),
            Err(ResolveError::UnknownOrExpired)
        );
    }
    #[test]
    fn disconnected_hook_cannot_resolve_a_pending_token() {
        let now = Instant::now();
        let hook_alive = Arc::new(AtomicBool::new(true));
        let registry = InteractionRegistry::default();
        let issued = registry.issue_with_token_and_liveness(
            "disconnected-token".to_owned(),
            request(7, "session"),
            now + Duration::from_secs(30),
            Arc::clone(&hook_alive),
        );

        hook_alive.store(false, Ordering::Release);
        assert_eq!(
            registry.resolve(&issued.token, 7, Decision::allow(), now,),
            Err(ResolveError::UnknownOrExpired)
        );
    }

    use super::QuestionRegistry;
    use crate::{QuestionAnswer, QuestionOption};

    fn option(label: &str) -> QuestionOption {
        QuestionOption {
            label: label.to_owned(),
            description: format!("{label} description"),
        }
    }

    #[test]
    fn option_label_resolves_by_index_and_is_none_once_removed() {
        let now = Instant::now();
        let registry = QuestionRegistry::default();
        let issued = registry
            .issue_with_liveness(
                request(7, "session"),
                vec![option("Red"), option("Blue")],
                now,
                now + Duration::from_secs(30),
                Arc::new(AtomicBool::new(true)),
            )
            .expect("issue token");
        assert_eq!(
            registry.option_label(&issued.token, 1),
            Some("Blue".to_owned())
        );
        assert_eq!(registry.option_label(&issued.token, 5), None);
        assert!(registry.remove(&issued.token));
        assert_eq!(registry.option_label(&issued.token, 1), None);
    }

    #[test]
    fn question_registry_state_machine_rejects_invalid_taps() {
        let now = Instant::now();
        let registry = QuestionRegistry::default();
        let issued = registry
            .issue_with_liveness(
                request(7, "session"),
                Vec::new(),
                now,
                now + Duration::from_secs(30),
                Arc::new(AtomicBool::new(true)),
            )
            .expect("issue token");
        assert_eq!(
            registry.resolve(
                &issued.token,
                8,
                QuestionAnswer::Single("Blue".to_owned()),
                now,
            ),
            Err(ResolveError::WrongChannel)
        );
    }

    #[test]
    fn pending_question_token_finds_a_multi_select_card_too() {
        let now = Instant::now();
        let registry = QuestionRegistry::default();
        let issued = registry
            .issue_with_liveness(
                request(7, "session-a"),
                Vec::new(),
                now,
                now + Duration::from_secs(30),
                Arc::new(AtomicBool::new(true)),
            )
            .expect("issue multiSelect token");
        assert_eq!(
            registry.pending_question_token("session-a"),
            Some(issued.token),
            "a thread reply must be able to answer a pending multiSelect card too"
        );
        assert_eq!(registry.pending_question_token("session-b"), None);
    }

    #[tokio::test]
    async fn question_registry_issues_and_resolves_exactly_once() {
        let now = Instant::now();
        let registry = QuestionRegistry::default();
        let issued = registry
            .issue_with_liveness(
                request(7, "session"),
                Vec::new(),
                now,
                now + Duration::from_secs(30),
                Arc::new(AtomicBool::new(true)),
            )
            .expect("issue token");
        registry
            .resolve(
                &issued.token,
                7,
                QuestionAnswer::Single("Blue".to_owned()),
                now,
            )
            .expect("first tap resolves");
        assert_eq!(
            issued.receiver.await,
            Ok(QuestionAnswer::Single("Blue".to_owned()))
        );
        assert_eq!(
            registry.resolve(
                &issued.token,
                7,
                QuestionAnswer::Single("Red".to_owned()),
                now,
            ),
            Err(ResolveError::UnknownOrExpired)
        );
    }

    #[test]
    fn disconnected_hook_cannot_resolve_a_pending_question_token() {
        let now = Instant::now();
        let registry = QuestionRegistry::default();
        let issued = registry
            .issue_with_liveness(
                request(7, "session"),
                Vec::new(),
                now,
                now + Duration::from_secs(30),
                Arc::new(AtomicBool::new(false)),
            )
            .expect("issue token");
        assert_eq!(
            registry.resolve(
                &issued.token,
                7,
                QuestionAnswer::Single("Blue".to_owned()),
                now,
            ),
            Err(ResolveError::UnknownOrExpired)
        );
    }
}
