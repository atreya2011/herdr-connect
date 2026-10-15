use twilight_model::channel::Channel;
use twilight_model::id::{Id, marker::ChannelMarker};

use crate::{
    TopologyCache, delete_thread_created_message, forget_owned, resolve_owner_deleted_tab,
    resolve_owner_deleted_workspace, tab_close, take_self_deletion, workspace_close,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuildDeletion {
    Thread { id: Id<ChannelMarker> },
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

#[derive(Debug, PartialEq, Eq)]
pub enum Close {
    Tab(String, Id<ChannelMarker>),
    Workspace(String),
}

pub fn decide_close(deletion: &GuildDeletion) -> Option<Close> {
    let id = deletion.id();
    if take_self_deletion(id) {
        forget_owned(id);
        return None;
    }
    match deletion {
        GuildDeletion::Thread { id } => {
            resolve_owner_deleted_tab(*id).map(|(tab, parent)| Close::Tab(tab, parent))
        }
        GuildDeletion::Channel(channel) => {
            resolve_owner_deleted_workspace(channel).map(Close::Workspace)
        }
    }
}

pub async fn handle_guild_deletion(
    client: &twilight_http::Client,
    topology_cache: &TopologyCache,
    deletion: GuildDeletion,
) -> Result<(), String> {
    let id = deletion.id();
    let close = decide_close(&deletion);
    let Some(close) = close else {
        return Ok(());
    };
    let message = match &close {
        Close::Tab(_, parent) => delete_thread_created_message(client, *parent, id).await,
        Close::Workspace(_) => Ok(()),
    };
    let mut guard = topology_cache.lock().await;
    if let Some((channels, threads)) = guard.as_mut() {
        channels.retain(|channel| channel.id != id);
        threads.retain(|thread| thread.id != id && thread.parent_id != Some(id));
    }
    let result = tokio::task::spawn_blocking(move || match close {
        Close::Tab(tab_id, _) => tab_close(&tab_id),
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
                Some(Close::Tab("testrun-a:t1".to_owned(), workspace.id)),
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
