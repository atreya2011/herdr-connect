//! Contract surface for the red parity suite.

pub mod parity_stubs {
    /// Placeholder for the unimplemented bridge contract.
    pub fn unimplemented_behavior() {
        todo!()
    }
}

/// Minimal public topology input for the red Discord contract.
pub struct TopologySyncRequest<'a> {
    pub workspace_id: &'a str,
    pub tab_id: &'a str,
    pub thread_name: &'a str,
}

/// Minimal public transition input for the red Discord contract.
pub struct TransitionDelivery<'a> {
    pub tab_id: &'a str,
    pub nonce: &'a str,
    pub content: &'a str,
}

/// Minimal public live-status input for the red Discord contract.
pub struct LiveStatusUpdate<'a> {
    pub terminal_id: &'a str,
    pub status: &'a str,
    pub content: &'a str,
}

/// Synchronizes one topology item through Discord.
pub fn sync_topology(
    _client: &twilight_http::Client,
    _guild_id: twilight_model::id::Id<twilight_model::id::marker::GuildMarker>,
    _request: TopologySyncRequest<'_>,
) -> Result<(), String> {
    todo!()
}

/// Delivers one transition through Discord.
pub fn deliver_transition(
    _client: &twilight_http::Client,
    _request: TransitionDelivery<'_>,
) -> Result<(), String> {
    todo!()
}

/// Updates one live-status message through Discord.
pub fn update_live_status(
    _client: &twilight_http::Client,
    _request: LiveStatusUpdate<'_>,
) -> Result<(), String> {
    todo!()
}
