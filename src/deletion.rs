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

    use twilight_model::channel::Channel;
    use twilight_model::guild::audit_log::{AuditLog, AuditLogEntry};
    use twilight_model::id::Id;

    use super::{Close, GuildDeletion, decide_close, handle_guild_deletion};
    use crate::TopologyCache;
    use crate::gateway::close_in_herdr;
    use crate::topology::{
        forget_owned, owner_deletion_tests::channel, remember_tab_threads,
        remember_workspace_channels,
    };

    const OWNER: u64 = 100_000_000_000_000_001;

    struct RecordedDeletionFixture {
        thread_entries: Vec<AuditLogEntry>,
        channel_entries: Vec<AuditLogEntry>,
        workspace: Channel,
        threads: [Channel; 2],
        on_thread: GuildDeletion,
        on_other_thread: GuildDeletion,
        on_channel: GuildDeletion,
        on_other_channel: GuildDeletion,
        no_entry_channel: GuildDeletion,
        wrong_action: GuildDeletion,
        plain_channel: GuildDeletion,
    }

    fn recorded_deletion_fixture() -> RecordedDeletionFixture {
        let mut recorded: std::collections::HashMap<String, AuditLog> = serde_json::from_str(
            include_str!("../tests/fixtures/discord_audit_log_deletions.json"),
        )
        .expect("recorded Discord audit responses deserialize");
        let thread_entries = recorded
            .remove("thread_delete")
            .expect("thread-delete response")
            .entries;
        let channel_entries = recorded
            .remove("channel_delete")
            .expect("channel-delete response")
            .entries;
        let thread_target = thread_entries[0]
            .target_id
            .expect("owner thread target")
            .get();
        let other_thread_target = thread_entries[1]
            .target_id
            .expect("other-user thread target")
            .get();
        let channel_target = channel_entries[0]
            .target_id
            .expect("owner channel target")
            .get();
        let other_channel_target = channel_entries[1]
            .target_id
            .expect("other-user channel target")
            .get();
        let workspace = channel(9_010, "work", Some("herdr workspace [testrun-a]"), None);
        let thread = channel(thread_target, "build [testrun-a:t1]", None, Some(9_010));
        let other_thread = channel(
            other_thread_target,
            "build [testrun-a:t2]",
            None,
            Some(9_010),
        );
        remember_workspace_channels(std::slice::from_ref(&workspace));
        let on_thread = GuildDeletion::Thread { id: thread.id };
        let on_other_thread = GuildDeletion::Thread {
            id: other_thread.id,
        };
        let on_channel = GuildDeletion::Channel(Box::new(channel(
            channel_target,
            "work",
            Some("herdr workspace [testrun-b]"),
            None,
        )));
        let on_other_channel = GuildDeletion::Channel(Box::new(channel(
            other_channel_target,
            "work",
            Some("herdr workspace [testrun-c]"),
            None,
        )));
        let no_entry_channel = GuildDeletion::Channel(Box::new(channel(
            9_099,
            "work",
            Some("herdr workspace [testrun-d]"),
            None,
        )));
        let wrong_action = GuildDeletion::Channel(Box::new(channel(
            thread_target,
            "work",
            Some("herdr workspace [testrun-a]"),
            None,
        )));
        let plain_channel =
            GuildDeletion::Channel(Box::new(channel(channel_target, "lounge", None, None)));
        RecordedDeletionFixture {
            thread_entries,
            channel_entries,
            workspace,
            threads: [thread, other_thread],
            on_thread,
            on_other_thread,
            on_channel,
            on_other_channel,
            no_entry_channel,
            wrong_action,
            plain_channel,
        }
    }

    #[test]
    fn deletion_decisions_close_only_what_the_audit_log_shows_the_owner_deleted() {
        let fixture = recorded_deletion_fixture();
        let thread_entries = &fixture.thread_entries;
        let channel_entries = &fixture.channel_entries;
        let mut without_user = channel_entries[0].clone();
        without_user.user_id = None;
        let owner = OWNER.to_string();
        let cases = [
            (
                "the owner deletes a tab thread",
                &fixture.on_thread,
                vec![thread_entries[0].clone()],
                Some(Close::Tab("testrun-a:t1".to_owned(), fixture.workspace.id)),
                true,
            ),
            (
                "the owner deletes a workspace channel",
                &fixture.on_channel,
                vec![channel_entries[0].clone()],
                Some(Close::Workspace("testrun-b".to_owned())),
                true,
            ),
            (
                "the owner entry follows a bot entry",
                &fixture.on_channel,
                vec![channel_entries[1].clone(), channel_entries[0].clone()],
                Some(Close::Workspace("testrun-b".to_owned())),
                true,
            ),
            (
                "the bot token deletes a tab thread",
                &fixture.on_other_thread,
                vec![thread_entries[1].clone()],
                None,
                true,
            ),
            (
                "the bot token deletes a workspace channel",
                &fixture.on_other_channel,
                vec![channel_entries[1].clone()],
                None,
                true,
            ),
            (
                "an entry without a user",
                &fixture.on_channel,
                vec![without_user],
                None,
                true,
            ),
            (
                "the owner deleted another channel",
                &fixture.on_other_channel,
                vec![channel_entries[0].clone()],
                None,
                true,
            ),
            (
                "the owner entry has another action type",
                &fixture.wrong_action,
                vec![thread_entries[0].clone()],
                None,
                true,
            ),
            (
                "no audit log entry",
                &fixture.on_channel,
                Vec::new(),
                None,
                true,
            ),
            (
                "the target has no audit log entry",
                &fixture.no_entry_channel,
                channel_entries.clone(),
                None,
                true,
            ),
            (
                "the owner deletes a thread the registry never held",
                &fixture.on_thread,
                vec![thread_entries[0].clone()],
                None,
                false,
            ),
            (
                "the owner deletes a channel with no workspace topic",
                &fixture.plain_channel,
                vec![channel_entries[0].clone()],
                None,
                true,
            ),
        ];
        for (name, deletion, entries, expected, registered) in cases {
            forget_owned(fixture.threads[0].id);
            forget_owned(fixture.threads[1].id);
            if registered {
                remember_tab_threads(&fixture.threads);
            }
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
