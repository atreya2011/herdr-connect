use crate::Decision;
use std::collections::HashMap;
use std::fmt::Write as _;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;
use tokio::sync::oneshot;
const TOKEN_BYTES: usize = 24;

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
    created_at: Instant,
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
    pub fn issue_with_liveness(
        &self,
        request: ApprovalRequest,
        created_at: Instant,
        expiry: Instant,
        hook_alive: Arc<AtomicBool>,
    ) -> Result<IssuedApproval, String> {
        let mut token_bytes = [0_u8; TOKEN_BYTES];
        File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut token_bytes))
            .map_err(|error| format!("cannot create interaction token: {error}"))?;
        let mut token = String::with_capacity(TOKEN_BYTES * 2);
        for byte in token_bytes {
            write!(&mut token, "{byte:02x}").map_err(|_| "token formatting failed".to_owned())?;
        }
        self.issue_with_token_and_liveness(token, request, created_at, expiry, hook_alive)
    }
    /// Inserts a supplied token for deterministic state-machine tests.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid lifetimes, empty tokens, or collisions.
    #[cfg(test)]
    fn issue_with_token(
        &self,
        token: String,
        request: ApprovalRequest,
        created_at: Instant,
        expiry: Instant,
    ) -> Result<IssuedApproval, String> {
        self.issue_with_token_and_liveness(
            token,
            request,
            created_at,
            expiry,
            Arc::new(AtomicBool::new(true)),
        )
    }
    fn issue_with_token_and_liveness(
        &self,
        token: String,
        request: ApprovalRequest,
        created_at: Instant,
        expiry: Instant,
        hook_alive: Arc<AtomicBool>,
    ) -> Result<IssuedApproval, String> {
        if token.is_empty() || expiry <= created_at {
            return Err("invalid interaction registry entry".to_owned());
        }
        let (sender, receiver) = oneshot::channel();
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "interaction registry lock poisoned".to_owned())?;
        if entries.contains_key(&token) {
            return Err("interaction token collision".to_owned());
        }
        entries.insert(
            token.clone(),
            Entry {
                request,
                created_at,
                expiry,
                hook_alive,
                sender,
            },
        );
        drop(entries);
        Ok(IssuedApproval {
            token,
            receiver,
            expiry,
        })
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
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| ResolveError::UnknownOrExpired)?;
        let Some(entry) = entries.get(token) else {
            return Err(ResolveError::UnknownOrExpired);
        };
        if now < entry.created_at
            || now >= entry.expiry
            || !entry.hook_alive.load(Ordering::Acquire)
        {
            return Err(ResolveError::UnknownOrExpired);
        }
        if channel_id != entry.request.channel_id {
            return Err(ResolveError::WrongChannel);
        }
        let entry = entries
            .remove(token)
            .ok_or(ResolveError::UnknownOrExpired)?;
        if !entry.hook_alive.load(Ordering::Acquire) {
            return Err(ResolveError::UnknownOrExpired);
        }
        let sender = entry.sender;
        drop(entries);
        let _ = sender.send(decision);
        Ok(())
    }
    pub fn has_pending(&self, token: &str) -> bool {
        self.entries.lock().ok().is_some_and(|entries| {
            entries
                .get(token)
                .is_some_and(|entry| entry.hook_alive.load(Ordering::Acquire))
        })
    }
    pub fn has_pending_session(&self, session_id: &str) -> bool {
        self.entries.lock().ok().is_some_and(|entries| {
            entries.values().any(|entry| {
                entry.request.session_id == session_id && entry.hook_alive.load(Ordering::Acquire)
            })
        })
    }
    pub fn remove(&self, token: &str) -> bool {
        self.entries
            .lock()
            .ok()
            .and_then(|mut entries| entries.remove(token))
            .is_some()
    }
}
#[cfg(test)]
mod tests {
    use super::{ApprovalRequest, InteractionRegistry, ResolveError};
    use crate::Decision;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
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
            let issued = registry
                .issue_with_token(
                    "token".to_owned(),
                    request(7, "session"),
                    now,
                    now + Duration::from_secs(30),
                )
                .expect("issue token");
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
        registry
            .issue_with_token(
                "session-scoped-token".to_owned(),
                request(7, "session-a"),
                now,
                now + Duration::from_secs(30),
            )
            .expect("issue token");

        for (session_id, expected) in [("session-a", true), ("session-b", false)] {
            assert_eq!(registry.has_pending_session(session_id), expected);
        }
    }
    #[tokio::test]
    async fn registry_issues_and_resolves_exactly_once() {
        let now = Instant::now();
        let registry = InteractionRegistry::default();
        let issued = registry
            .issue_with_token(
                "opaque-token".to_owned(),
                request(7, "session"),
                now,
                now + Duration::from_secs(30),
            )
            .expect("issue token");
        registry
            .resolve(&issued.token, 7, Decision::deny(Some("no".to_owned())), now)
            .expect("first tap resolves");
        assert_eq!(
            issued.receiver.await,
            Ok(Decision::deny(Some("no".to_owned())))
        );
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
        let issued = registry
            .issue_with_token_and_liveness(
                "disconnected-token".to_owned(),
                request(7, "session"),
                now,
                now + Duration::from_secs(30),
                Arc::clone(&hook_alive),
            )
            .expect("issue token");

        hook_alive.store(false, Ordering::Release);
        assert_eq!(
            registry.resolve(&issued.token, 7, Decision::allow(), now,),
            Err(ResolveError::UnknownOrExpired)
        );
    }
}
