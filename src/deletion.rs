use std::fmt;

use twilight_model::channel::Channel;
use twilight_model::guild::audit_log::{AuditLogEntry, AuditLogEventType};
use twilight_model::id::Id;
use twilight_model::id::marker::{ChannelMarker, GuildMarker};

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
    if deletion.owned_close().is_none() {
        return Ok(());
    }
    let entries = client
        .audit_log(guild)
        .action_type(deletion.audit_log_event())
        .await
        .map_err(|error| format!("guild audit log: {error}"))?
        .model()
        .await
        .map_err(|error| format!("guild audit log: {error}"))?
        .entries;
    let id = deletion.id();
    let Some(close) = decide_close(&deletion, &entries, owner_id) else {
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

    use twilight_model::guild::audit_log::{AuditLogEntry, AuditLogEventType};
    use twilight_model::id::Id;

    use super::{Close, GuildDeletion, decide_close, handle_guild_deletion};
    use crate::TopologyCache;
    use crate::gateway::close_in_herdr;
    use crate::topology::{
        owner_deletion_tests::channel, remember_tab_threads, remember_workspace_channels,
    };

    const OWNER: u64 = 4_242;
    const BOT: u64 = 4_343;

    fn entry(action_type: AuditLogEventType, target: u64, user: Option<u64>) -> AuditLogEntry {
        AuditLogEntry {
            action_type,
            changes: Vec::new(),
            guild_id: Some(Id::new(1)),
            id: Id::new(target + 1),
            options: None,
            reason: None,
            target_id: Some(Id::new(target)),
            user_id: user.map(Id::new),
        }
    }

    #[test]
    fn deletion_decisions_close_only_what_the_audit_log_shows_the_owner_deleted() {
        let workspace = channel(9_010, "work", Some("herdr workspace [testrun-a]"), None);
        let thread = channel(9_030, "build [testrun-a:t1]", None, Some(9_010));
        let workspace_channel = channel(9_041, "work", Some("herdr workspace [testrun-b]"), None);
        remember_workspace_channels(std::slice::from_ref(&workspace));
        remember_tab_threads(std::slice::from_ref(&thread));
        let on_thread = GuildDeletion::Thread { id: thread.id };
        let on_channel = GuildDeletion::Channel(Box::new(workspace_channel));
        let unregistered_thread = GuildDeletion::Thread {
            id: channel(9_031, "notes", None, Some(9_010)).id,
        };
        let plain_channel = GuildDeletion::Channel(Box::new(channel(9_040, "lounge", None, None)));
        let thread_delete = AuditLogEventType::ThreadDelete;
        let channel_delete = AuditLogEventType::ChannelDelete;
        let owner = OWNER.to_string();
        let cases = [
            (
                "the owner deletes a tab thread",
                &on_thread,
                vec![entry(thread_delete, 9_030, Some(OWNER))],
                Some(Close::Tab("testrun-a:t1".to_owned(), workspace.id)),
            ),
            (
                "the owner deletes a workspace channel",
                &on_channel,
                vec![entry(channel_delete, 9_041, Some(OWNER))],
                Some(Close::Workspace("testrun-b".to_owned())),
            ),
            (
                "the owner entry follows a bot entry",
                &on_channel,
                vec![
                    entry(channel_delete, 9_042, Some(BOT)),
                    entry(channel_delete, 9_041, Some(OWNER)),
                ],
                Some(Close::Workspace("testrun-b".to_owned())),
            ),
            (
                "the bot token deletes a tab thread",
                &on_thread,
                vec![entry(thread_delete, 9_030, Some(BOT))],
                None,
            ),
            (
                "the bot token deletes a workspace channel",
                &on_channel,
                vec![entry(channel_delete, 9_041, Some(BOT))],
                None,
            ),
            (
                "an entry without a user",
                &on_channel,
                vec![entry(channel_delete, 9_041, None)],
                None,
            ),
            (
                "the owner deleted another channel",
                &on_channel,
                vec![entry(channel_delete, 9_042, Some(OWNER))],
                None,
            ),
            (
                "the owner entry has another action type",
                &on_thread,
                vec![entry(channel_delete, 9_030, Some(OWNER))],
                None,
            ),
            ("no audit log entry", &on_channel, Vec::new(), None),
            (
                "the owner deletes a thread the registry never held",
                &unregistered_thread,
                vec![entry(thread_delete, 9_031, Some(OWNER))],
                None,
            ),
            (
                "the owner deletes a channel with no workspace topic",
                &plain_channel,
                vec![entry(channel_delete, 9_040, Some(OWNER))],
                None,
            ),
        ];
        for (name, deletion, entries, expected) in cases {
            assert_eq!(decide_close(deletion, &entries, &owner), expected, "{name}");
        }
    }

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
