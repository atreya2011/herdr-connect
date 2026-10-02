use twilight_model::channel::Channel;
use twilight_model::id::{Id, marker::ChannelMarker};

use crate::{
    TopologyCache, take_owner_deleted_tab, take_owner_deleted_workspace, take_self_deletion,
};

/// A guild channel or thread the Discord gateway reported deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuildDeletion {
    /// A thread delete event, which carries no name or topic, only the id.
    Thread(Id<ChannelMarker>),
    /// A channel delete event, which carries the deleted channel's last state.
    Channel(Box<Channel>),
}

impl GuildDeletion {
    fn id(&self) -> Id<ChannelMarker> {
        match self {
            Self::Thread(id) => *id,
            Self::Channel(channel) => channel.id,
        }
    }
}

/// Closes the Herdr tab or workspace behind a Discord deletion the owner made.
///
/// A deletion the bridge made itself is ignored once. A deleted thread or channel that is not
/// bridge-owned is ignored. The topology cache lock is held until `herdr` has closed the tab or
/// workspace: delivery and sync take that lock before they can resolve or recreate a thread, so
/// by the time they run the Herdr tab is gone and nothing remains to recreate.
///
/// # Errors
///
/// Returns the `herdr` close failure.
pub async fn handle_guild_deletion(
    topology_cache: &TopologyCache,
    deletion: GuildDeletion,
) -> Result<(), String> {
    if take_self_deletion(deletion.id()) {
        return Ok(());
    }
    let mut guard = topology_cache.lock().await;
    let close = match &deletion {
        GuildDeletion::Thread(id) => guard
            .as_mut()
            .and_then(|(channels, threads)| take_owner_deleted_tab(channels, threads, *id))
            .map(|tab_id| ["tab", "close", &tab_id].map(ToOwned::to_owned)),
        GuildDeletion::Channel(channel) => match guard.as_mut() {
            Some((channels, threads)) => take_owner_deleted_workspace(channels, threads, channel),
            None => take_owner_deleted_workspace(&mut Vec::new(), &mut Vec::new(), channel),
        }
        .map(|workspace_id| ["workspace", "close", &workspace_id].map(ToOwned::to_owned)),
    };
    let Some(args) = close else {
        return Ok(());
    };
    let result = run_herdr_close(args).await;
    drop(guard);
    result
}

async fn run_herdr_close(args: [String; 3]) -> Result<(), String> {
    let output = tokio::task::spawn_blocking(move || {
        std::process::Command::new("herdr").args(&args).output()
    })
    .await
    .map_err(|error| error.to_string())?
    .map_err(|error| format!("herdr spawn failed: {error}"))?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "herdr close failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{GuildDeletion, handle_guild_deletion};
    use crate::TopologyCache;
    use crate::topology::{owner_deletion_tests::channel, record_self_deletion};

    #[tokio::test]
    async fn deletions_that_are_not_owner_deletions_of_bridge_topology_never_reach_herdr() {
        let thread = channel(30, "build [w1:t1]", None, Some(10));
        let cache: TopologyCache = Arc::new(tokio::sync::Mutex::new(Some((
            vec![channel(10, "work", Some("herdr workspace [w1]"), None)],
            vec![thread.clone()],
        ))));
        record_self_deletion(thread.id);
        let cases = [
            (
                "the bridge's own thread deletion",
                GuildDeletion::Thread(thread.id),
            ),
            (
                "a thread the cache never held",
                GuildDeletion::Thread(channel(31, "x", None, Some(10)).id),
            ),
            (
                "a channel with no workspace topic",
                GuildDeletion::Channel(Box::new(channel(40, "lounge", None, None))),
            ),
        ];
        for (name, deletion) in cases {
            assert_eq!(
                handle_guild_deletion(&cache, deletion).await,
                Ok(()),
                "{name}"
            );
        }
        let cached_threads = cache
            .lock()
            .await
            .as_ref()
            .map(|(_, threads)| threads.len());
        assert_eq!(
            cached_threads,
            Some(1),
            "the suppressed thread stays cached"
        );
    }
}
