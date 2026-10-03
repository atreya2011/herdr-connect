use twilight_model::channel::Channel;
use twilight_model::id::{Id, marker::ChannelMarker};

use crate::{
    TopologyCache, delete_thread_created_message, forget_owned, owned_thread_parent,
    resolve_owner_deleted_tab, resolve_owner_deleted_workspace, tab_close, take_self_deletion,
    workspace_close,
};

/// A guild channel or thread the Discord gateway reported deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuildDeletion {
    /// A thread delete event, which carries no name or topic, only the id.
    Thread { id: Id<ChannelMarker> },
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

/// The Herdr object an owner deletion closes.
#[derive(Debug, PartialEq, Eq)]
pub enum Close {
    Tab(String),
    Workspace(String),
}

/// Decides what an owner deletion closes, without touching Herdr or Discord.
///
/// A deletion the bridge made itself is consumed here and decides nothing. A thread that is not
/// in the registry of live tab threads, and a channel without a workspace topic, decide nothing.
pub fn decide_close(deletion: &GuildDeletion) -> Option<Close> {
    let id = deletion.id();
    if take_self_deletion(id) {
        forget_owned(id);
        return None;
    }
    match deletion {
        GuildDeletion::Thread { id } => resolve_owner_deleted_tab(*id).map(Close::Tab),
        GuildDeletion::Channel(channel) => {
            resolve_owner_deleted_workspace(channel).map(Close::Workspace)
        }
    }
}

/// Closes the Herdr tab or workspace behind a Discord deletion the owner made.
///
/// A deletion the bridge made itself is ignored once. A deleted thread that is not a live tab's
/// thread, and a deleted channel that is not a workspace channel, are ignored. A thread is
/// resolved to its tab from the durable registry of bridge-owned threads, which survives
/// topology-cache invalidation and refetches. The topology cache lock is held until `herdr` has
/// closed the tab or workspace and the deleted ids have left the cache, so a delivery or sync
/// that runs after this handler took the lock finds the tab gone. A delivery that raced ahead of
/// the gateway event can create one replacement thread first; the close then ends the tab and the
/// bridge's own tab-closed handling deletes that replacement. A deleted tab thread also has its
/// "started a thread" system message deleted from its parent workspace channel, before the close.
///
/// # Errors
///
/// Returns the `herdr` close failure, or else the failure to delete the system message.
pub async fn handle_guild_deletion(
    client: &twilight_http::Client,
    topology_cache: &TopologyCache,
    deletion: GuildDeletion,
) -> Result<(), String> {
    let id = deletion.id();
    let parent = owned_thread_parent(id);
    let close = decide_close(&deletion);
    let Some(close) = close else {
        return Ok(());
    };
    let message = match (&close, parent) {
        (Close::Tab(_), Some(parent)) => delete_thread_created_message(client, parent, id).await,
        _ => Ok(()),
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
    result.map(|_| forget_owned(id))?;
    message
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{Close, GuildDeletion, decide_close, handle_guild_deletion};
    use crate::TopologyCache;
    use crate::topology::{
        owner_deletion_tests::channel, record_self_deletion, remember_tab_threads,
        remember_workspace_channels,
    };

    #[test]
    fn deletion_decisions_close_only_what_the_owner_deleted_of_the_bridge_topology() {
        let workspace = channel(9_010, "work", Some("herdr workspace [testrun-a]"), None);
        let thread = channel(9_030, "build [testrun-a:t1]", None, Some(9_010));
        let own = channel(9_032, "mine [testrun-a:t2]", None, Some(9_010));
        remember_workspace_channels(std::slice::from_ref(&workspace));
        remember_tab_threads(&[thread.clone(), own.clone()]);
        record_self_deletion(own.id);
        let cases = [
            (
                "owner deletes a tab thread",
                GuildDeletion::Thread { id: thread.id },
                Some(Close::Tab("testrun-a:t1".to_owned())),
            ),
            (
                "the bridge's own thread deletion",
                GuildDeletion::Thread { id: own.id },
                None,
            ),
            (
                "a thread the registry never held",
                GuildDeletion::Thread {
                    id: channel(9_031, "notes", None, Some(9_010)).id,
                },
                None,
            ),
            (
                "owner deletes a workspace channel",
                GuildDeletion::Channel(Box::new(channel(
                    9_041,
                    "work",
                    Some("herdr workspace [testrun-b]"),
                    None,
                ))),
                Some(Close::Workspace("testrun-b".to_owned())),
            ),
            (
                "a channel with no workspace topic",
                GuildDeletion::Channel(Box::new(channel(9_040, "lounge", None, None))),
                None,
            ),
        ];
        for (name, deletion, expected) in cases {
            assert_eq!(decide_close(&deletion), expected, "{name}");
        }
    }

    #[tokio::test]
    async fn deletions_that_decide_nothing_return_without_reaching_herdr() {
        let cache: TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let client = twilight_http::Client::new(String::new());
        let cases = [
            (
                "a thread the registry never held",
                GuildDeletion::Thread {
                    id: channel(9_131, "notes", None, Some(9_110)).id,
                },
            ),
            (
                "a channel with no workspace topic",
                GuildDeletion::Channel(Box::new(channel(9_140, "lounge", None, None))),
            ),
        ];
        for (name, deletion) in cases {
            assert_eq!(
                handle_guild_deletion(&client, &cache, deletion).await,
                Ok(()),
                "{name}"
            );
        }
    }
}
