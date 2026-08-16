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
    pub owner_id: String,
    pub session_id: String,
    pub prompt_id: String,
    pub tool: String,
    pub channel_id: u64,
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
    Unauthorized,
    WrongSession,
    WrongChannel,
}
#[derive(Default, Debug)]
pub struct InteractionRegistry {
    entries: Mutex<HashMap<String, Entry>>,
}
impl InteractionRegistry {
    /// Issues an opaque token backed by the operating system random source.
    ///
    /// # Errors
    ///
    /// Returns an error if the random source or registry is unavailable.
    pub fn issue(
        &self,
        request: ApprovalRequest,
        created_at: Instant,
        expiry: Instant,
    ) -> Result<IssuedApproval, String> {
        self.issue_with_liveness(request, created_at, expiry, Arc::new(AtomicBool::new(true)))
    }
    pub(crate) fn issue_with_liveness(
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
    pub fn issue_with_token(
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
    pub(crate) fn issue_with_token_and_liveness(
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
    /// Returns the rejection reason for an unknown, expired, unauthorized, mismatched, or
    /// wrong-channel interaction.
    pub fn resolve(
        &self,
        token: &str,
        owner_id: &str,
        session_id: &str,
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
        if owner_id != entry.request.owner_id {
            return Err(ResolveError::Unauthorized);
        }
        if session_id != entry.request.session_id {
            return Err(ResolveError::WrongSession);
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
    pub fn session_id(&self, token: &str) -> Option<String> {
        self.entries
            .lock()
            .ok()?
            .get(token)
            .filter(|entry| entry.hook_alive.load(Ordering::Acquire))
            .map(|entry| entry.request.session_id.clone())
    }
    pub fn expire(&self, token: &str, now: Instant) -> bool {
        let Ok(mut entries) = self.entries.lock() else {
            return false;
        };
        let expired = entries.get(token).is_some_and(|entry| now >= entry.expiry);
        if expired {
            entries.remove(token);
        }
        expired
    }
    pub fn remove(&self, token: &str) -> bool {
        self.entries
            .lock()
            .ok()
            .and_then(|mut entries| entries.remove(token))
            .is_some()
    }
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.lock().map_or(0, |entries| entries.len())
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
#[cfg(test)]
mod tests {
    use super::{ApprovalRequest, InteractionRegistry, ResolveError};
    use crate::Decision;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};
    fn request(channel_id: u64) -> ApprovalRequest {
        ApprovalRequest {
            owner_id: "owner".to_owned(),
            session_id: "session".to_owned(),
            prompt_id: "prompt".to_owned(),
            tool: "Bash".to_owned(),
            channel_id,
        }
    }
    #[test]
    fn registry_state_machine_rejects_invalid_taps() {
        let now = Instant::now();
        let cases = [
            (
                "wrong owner",
                "other",
                "session",
                7,
                ResolveError::Unauthorized,
            ),
            (
                "wrong session",
                "owner",
                "other",
                7,
                ResolveError::WrongSession,
            ),
            (
                "wrong channel",
                "owner",
                "session",
                8,
                ResolveError::WrongChannel,
            ),
        ];
        for (_, owner, session, channel, expected) in cases {
            let registry = InteractionRegistry::default();
            let issued = registry
                .issue_with_token(
                    "token".to_owned(),
                    request(7),
                    now,
                    now + Duration::from_secs(30),
                )
                .expect("issue token");
            assert_eq!(
                registry.resolve(
                    &issued.token,
                    owner,
                    session,
                    channel,
                    Decision::allow(),
                    now,
                ),
                Err(expected)
            );
        }
    }
    #[tokio::test]
    async fn registry_issues_and_resolves_exactly_once() {
        let now = Instant::now();
        let registry = InteractionRegistry::default();
        let issued = registry
            .issue_with_token(
                "opaque-token".to_owned(),
                request(7),
                now,
                now + Duration::from_secs(30),
            )
            .expect("issue token");
        registry
            .resolve(
                &issued.token,
                "owner",
                "session",
                7,
                Decision::deny(Some("no".to_owned())),
                now,
            )
            .expect("first tap resolves");
        assert_eq!(
            issued.receiver.await,
            Ok(Decision::deny(Some("no".to_owned())))
        );
        assert_eq!(
            registry.resolve(
                "opaque-token",
                "owner",
                "session",
                7,
                Decision::allow(),
                now,
            ),
            Err(ResolveError::UnknownOrExpired)
        );
    }
    #[test]
    fn expired_token_is_removed_and_rejected() {
        let now = Instant::now();
        let registry = InteractionRegistry::default();
        registry
            .issue_with_token(
                "expired-token".to_owned(),
                request(7),
                now,
                now + Duration::from_secs(1),
            )
            .expect("issue token");
        assert!(registry.expire("expired-token", now + Duration::from_secs(1)));
        assert_eq!(
            registry.resolve(
                "expired-token",
                "owner",
                "session",
                7,
                Decision::allow(),
                now + Duration::from_secs(1),
            ),
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
                request(7),
                now,
                now + Duration::from_secs(30),
                Arc::clone(&hook_alive),
            )
            .expect("issue token");

        hook_alive.store(false, Ordering::Release);
        assert_eq!(
            registry.resolve(&issued.token, "owner", "session", 7, Decision::allow(), now,),
            Err(ResolveError::UnknownOrExpired)
        );
    }
}
