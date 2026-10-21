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

use crate::{Decision, Question};
const TOKEN_BYTES: usize = 24;

pub fn generate_token() -> Result<String, String> {
    let mut token_bytes = [0_u8; TOKEN_BYTES];
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut token_bytes))
        .map_err(|error| format!("cannot create interaction token: {error}"))?;
    let mut token = String::with_capacity(TOKEN_BYTES * 2);
    for byte in token_bytes {
        let _ = write!(&mut token, "{byte:02x}");
    }
    Ok(token)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApprovalRequest<R> {
    pub channel_id: R,
    pub session_id: String,
}
#[derive(Debug)]
pub struct IssuedApproval {
    pub token: String,
    pub receiver: oneshot::Receiver<Decision>,
    pub expiry: Instant,
}
#[derive(Debug)]
struct Entry<R> {
    request: ApprovalRequest<R>,
    expiry: Instant,
    hook_alive: Arc<AtomicBool>,
    sender: oneshot::Sender<Decision>,
}
#[derive(Debug, Eq, PartialEq)]
pub enum ResolveError {
    UnknownOrExpired,
    WrongChannel,
}
#[derive(Debug)]
pub struct InteractionRegistry<R> {
    entries: Mutex<HashMap<String, Entry<R>>>,
}
impl<R> Default for InteractionRegistry<R> {
    fn default() -> Self {
        Self {
            entries: Mutex::default(),
        }
    }
}
impl<R: Eq> InteractionRegistry<R> {
    fn lock_entries(&self) -> MutexGuard<'_, HashMap<String, Entry<R>>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
    pub fn issue_with_liveness(
        &self,
        request: ApprovalRequest<R>,
        expiry: Instant,
        hook_alive: Arc<AtomicBool>,
    ) -> Result<IssuedApproval, String> {
        let token = generate_token()?;
        Ok(self.issue_with_token_and_liveness(token, request, expiry, hook_alive))
    }
    #[cfg(test)]
    fn issue_with_token(
        &self,
        token: String,
        request: ApprovalRequest<R>,
        expiry: Instant,
    ) -> IssuedApproval {
        self.issue_with_token_and_liveness(token, request, expiry, Arc::new(AtomicBool::new(true)))
    }
    fn issue_with_token_and_liveness(
        &self,
        token: String,
        request: ApprovalRequest<R>,
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
    pub fn resolve(
        &self,
        token: &str,
        channel_id: &R,
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
        if *channel_id != entry.request.channel_id {
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

#[derive(Clone, Debug)]
pub struct PendingQuestion<R, M> {
    pub channel: R,
    pub message: M,
    pub pane_id: String,
    pub questions: Vec<Question>,
    pub index: usize,
}

impl<R, M> PendingQuestion<R, M> {
    #[must_use]
    pub fn question(&self) -> &Question {
        &self.questions[self.index]
    }

    #[must_use]
    pub const fn is_last(&self) -> bool {
        self.index + 1 == self.questions.len()
    }
}

#[derive(Debug)]
pub struct QuestionRegistry<R, M> {
    entries: Mutex<HashMap<String, PendingQuestion<R, M>>>,
}
impl<R, M> Default for QuestionRegistry<R, M> {
    fn default() -> Self {
        Self {
            entries: Mutex::default(),
        }
    }
}
impl<R: Clone + Eq, M: Clone + Eq> QuestionRegistry<R, M> {
    fn lock_entries(&self) -> MutexGuard<'_, HashMap<String, PendingQuestion<R, M>>> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner)
    }
    pub fn insert(&self, token: String, pending: PendingQuestion<R, M>) {
        self.lock_entries().insert(token, pending);
    }
    pub fn get(&self, token: &str) -> Option<PendingQuestion<R, M>> {
        self.lock_entries().get(token).cloned()
    }
    pub fn take(&self, token: &str) -> Option<PendingQuestion<R, M>> {
        self.lock_entries().remove(token)
    }
    pub fn token_in_channel(&self, channel: &R) -> Option<String> {
        self.lock_entries()
            .iter()
            .find(|(_, pending)| pending.channel == *channel)
            .map(|(token, _)| token.clone())
    }
    pub fn forget_message(&self, message: &M) {
        self.lock_entries()
            .retain(|_, pending| pending.message != *message);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use super::{ApprovalRequest, InteractionRegistry, ResolveError};
    use crate::Decision;
    fn request(channel_id: u64, session_id: &str) -> ApprovalRequest<u64> {
        ApprovalRequest {
            channel_id,
            session_id: session_id.to_owned(),
        }
    }
    #[test]
    fn registry_state_machine_rejects_invalid_taps() {
        let now = Instant::now();
        let cases = [("wrong channel", 8_u64, ResolveError::WrongChannel)];
        for (_, channel, expected) in cases {
            let registry = InteractionRegistry::default();
            let issued = registry.issue_with_token(
                "token".to_owned(),
                request(7, "session"),
                now + Duration::from_secs(30),
            );
            assert_eq!(
                registry.resolve(&issued.token, &channel, Decision::allow(), now,),
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
            .resolve(&issued.token, &7, Decision::deny("no".to_owned()), now)
            .expect("first tap resolves");
        assert_eq!(issued.receiver.await, Ok(Decision::deny("no".to_owned())));
        assert_eq!(
            registry.resolve("opaque-token", &7, Decision::allow(), now,),
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
            registry.resolve(&issued.token, &7, Decision::allow(), now,),
            Err(ResolveError::UnknownOrExpired)
        );
    }
}
