use twilight_model::channel::Channel;
use twilight_model::id::{Id, marker::ChannelMarker};

use crate::{
    TopologyCache, forget_owned, resolve_owner_deleted_tab, resolve_owner_deleted_workspace,
    tab_close, take_self_deletion, workspace_close,
};

/// A guild channel or thread the Discord gateway reported deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuildDeletion {
    /// A thread delete event, which carries no name or topic, only the id and the parent channel.
    Thread {
        id: Id<ChannelMarker>,
        parent_id: Id<ChannelMarker>,
    },
    /// A channel delete event, which carries the deleted channel's last state.
    Channel(Box<Channel>),
}

impl GuildDeletion {
    fn id(&self) -> Id<ChannelMarker> {
        match self {
            Self::Thread { id, .. } => *id,
            Self::Channel(channel) => channel.id,
        }
    }
}

enum Close {
    Tab(String),
    Workspace(String),
}

/// Closes the Herdr tab or workspace behind a Discord deletion the owner made.
///
/// A deletion the bridge made itself is ignored once. A deleted thread or channel outside the
/// bridge's workspace channels is ignored. A thread is resolved to its tab from the durable
/// registry of bridge-owned threads, which survives topology-cache invalidation and refetches, so
/// a delivery that already saw Unknown Channel and cleared the cache cannot make the deletion
/// unresolvable. The topology cache lock is held until `herdr` has closed the tab or workspace and
/// the deleted ids have left the cache, so a delivery or sync that runs after this handler took
/// the lock finds the tab gone. A delivery that raced ahead of the gateway event can create one
/// replacement thread first; the close then ends the tab and the bridge's own tab-closed handling
/// deletes that replacement.
///
/// # Errors
///
/// Returns an error when a thread under a bridge workspace channel cannot be resolved to its tab,
/// or the `herdr` close failure.
pub async fn handle_guild_deletion(
    topology_cache: &TopologyCache,
    deletion: GuildDeletion,
) -> Result<(), String> {
    let id = deletion.id();
    if take_self_deletion(id) {
        forget_owned(id);
        return Ok(());
    }
    let close = match &deletion {
        GuildDeletion::Thread { id, parent_id } => {
            resolve_owner_deleted_tab(*id, *parent_id)?.map(Close::Tab)
        }
        GuildDeletion::Channel(channel) => {
            resolve_owner_deleted_workspace(channel).map(Close::Workspace)
        }
    };
    let Some(close) = close else {
        return Ok(());
    };
    let mut guard = topology_cache.lock().await;
    if let Some((channels, threads)) = guard.as_mut() {
        channels.retain(|channel| channel.id != id);
        threads.retain(|thread| thread.id != id && thread.parent_id != Some(id));
    }
    let result = tokio::task::spawn_blocking(move || match close {
        Close::Tab(tab_id) => tab_close(&tab_id),
        Close::Workspace(workspace_id) => workspace_close(&workspace_id),
    })
    .await
    .map_err(|error| error.to_string())?;
    drop(guard);
    result.map(|_| forget_owned(id))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{GuildDeletion, handle_guild_deletion};
    use crate::TopologyCache;
    use crate::topology::{
        owner_deletion_tests::channel, record_self_deletion, remember_tab_threads,
        remember_workspace_channels,
    };

    #[tokio::test]
    async fn deletions_that_are_not_owner_deletions_of_bridge_topology_never_reach_herdr() {
        let thread = channel(9_030, "build [w1:t1]", None, Some(9_010));
        remember_workspace_channels(&[channel(9_010, "work", Some("herdr workspace [w1]"), None)]);
        remember_tab_threads(std::slice::from_ref(&thread));
        let cache: TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        record_self_deletion(thread.id);
        let cases = [
            (
                "the bridge's own thread deletion",
                GuildDeletion::Thread {
                    id: thread.id,
                    parent_id: thread.parent_id.expect("thread has a parent"),
                },
            ),
            (
                "a thread under a channel the bridge does not own",
                GuildDeletion::Thread {
                    id: channel(9_031, "x", None, Some(9_099)).id,
                    parent_id: channel(9_099, "lounge", None, None).id,
                },
            ),
            (
                "a channel with no workspace topic",
                GuildDeletion::Channel(Box::new(channel(9_040, "lounge", None, None))),
            ),
        ];
        for (name, deletion) in cases {
            assert_eq!(
                handle_guild_deletion(&cache, deletion).await,
                Ok(()),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn an_unresolvable_thread_under_a_workspace_channel_is_an_error() {
        remember_workspace_channels(&[channel(9_110, "work", Some("herdr workspace [w1]"), None)]);
        let cache: TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let deletion = GuildDeletion::Thread {
            id: channel(9_131, "x", None, Some(9_110)).id,
            parent_id: channel(9_110, "work", None, None).id,
        };
        let result = handle_guild_deletion(&cache, deletion).await;
        assert!(
            result
                .as_ref()
                .is_err_and(|error| error.contains("unresolved")),
            "{result:?}"
        );
    }
}
