use std::fmt;

use twilight_model::channel::Channel;
use twilight_model::guild::audit_log::{AuditLogEntry, AuditLogEventType};
use twilight_model::id::Id;
use twilight_model::id::marker::{ChannelMarker, GuildMarker, UserMarker};

use crate::{
    Closed, TopologyCache, delete_thread_created_message, forget_owned, resolve_owner_deleted_tab,
    resolve_owner_deleted_workspace,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuildDeletion {
    Thread { id: Id<ChannelMarker> },
    Channel(Box<Channel>),
}

impl fmt::Display for GuildDeletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Thread { .. } => "THREAD_DELETE",
            Self::Channel(_) => "CHANNEL_DELETE",
        })
    }
}

impl GuildDeletion {
    fn id(&self) -> Id<ChannelMarker> {
        match self {
            Self::Thread { id, .. } => *id,
            Self::Channel(channel) => channel.id,
        }
    }

    const fn audit_log_event(&self) -> AuditLogEventType {
        match self {
            Self::Thread { .. } => AuditLogEventType::ThreadDelete,
            Self::Channel(_) => AuditLogEventType::ChannelDelete,
        }
    }

    fn owned_close(&self) -> Option<Close> {
        match self {
            Self::Thread { id } => {
                resolve_owner_deleted_tab(*id).map(|(tab, parent)| Close::Tab(tab, parent))
            }
            Self::Channel(channel) => {
                resolve_owner_deleted_workspace(channel).map(Close::Workspace)
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Close {
    Tab(String, Id<ChannelMarker>),
    Workspace(String),
}

pub fn decide_close(
    deletion: &GuildDeletion,
    entries: &[AuditLogEntry],
    owner_id: &str,
) -> Option<Close> {
    let target = deletion.id().cast();
    let event = deletion.audit_log_event();
    entries
        .iter()
        .any(|entry| {
            entry.action_type == event
                && entry.target_id == Some(target)
                && entry
                    .user_id
                    .is_some_and(|user| user.to_string() == owner_id)
        })
        .then(|| deletion.owned_close())
        .flatten()
}

pub async fn handle_guild_deletion<F>(
    client: &twilight_http::Client,
    guild: Id<GuildMarker>,
    owner_id: &str,
    topology_cache: &TopologyCache,
    deletion: GuildDeletion,
    closer: F,
) -> Result<(), String>
where
    F: FnOnce(Closed) -> Result<(), String> + Send + 'static,
{
    let id = deletion.id();
    {
        let mut guard = topology_cache.lock().await;
        if let Some((channels, threads)) = guard.as_mut() {
            channels.retain(|channel| channel.id != id);
            threads.retain(|thread| thread.id != id && thread.parent_id != Some(id));
        }
    }
    if deletion.owned_close().is_none() {
        return Ok(());
    }
    let owner = owner_id
        .parse::<Id<UserMarker>>()
        .map_err(|error| format!("DISCORD_OWNER_ID is not numeric: {error}"))?;
    let entries = client
        .audit_log(guild)
        .action_type(deletion.audit_log_event())
        .user_id(owner)
        .await
        .map_err(|error| format!("guild audit log: {error}"))?
        .model()
        .await
        .map_err(|error| format!("guild audit log: {error}"))?
        .entries;
    let Some(close) = decide_close(&deletion, &entries, owner_id) else {
        return Ok(());
    };
    let message = match &close {
        Close::Tab(_, parent) => delete_thread_created_message(client, *parent, id).await,
        Close::Workspace(_) => Ok(()),
    };
    let guard = topology_cache.lock().await;
    let result = tokio::task::spawn_blocking(move || match close {
        Close::Tab(tab_id, _) => closer(Closed::Tab(tab_id)),
        Close::Workspace(workspace_id) => closer(Closed::Workspace(workspace_id)),
    })
    .await
    .map_err(|error| error.to_string())?;
    drop(guard);
    result.map(|()| forget_owned(id))?;
    message
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use twilight_model::id::Id;

    use super::{GuildDeletion, handle_guild_deletion};
    use crate::TopologyCache;
    use crate::gateway::close_in_herdr;
    use crate::topology::owner_deletion_tests::channel;

    const OWNER: u64 = 100_000_000_000_000_001;

    #[tokio::test]
    async fn deletions_outside_the_bridge_topology_return_without_reaching_discord_or_herdr() {
        let cache: TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let client = twilight_http::Client::new(String::new());
        let owner = OWNER.to_string();
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
                handle_guild_deletion(
                    &client,
                    Id::new(1),
                    &owner,
                    &cache,
                    deletion,
                    close_in_herdr
                )
                .await,
                Ok(()),
                "{name}"
            );
        }
    }
}
