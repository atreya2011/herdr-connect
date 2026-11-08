use twilight_model::channel::Channel;
use twilight_model::guild::audit_log::{AuditLogChange, AuditLogEntry, AuditLogEventType};
use twilight_model::id::Id;
use twilight_model::id::marker::{ChannelMarker, GuildMarker, UserMarker};

use crate::{
    ChatScope, HerdrTab, HerdrWorkspace, Renamed, TEST_RUN_PREFIX, format_thread_name,
    resolve_owner_deleted_tab, resolve_owner_deleted_workspace, tab_list_result,
    workspace_channel_name, workspace_list_result,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuildRename {
    Thread {
        id: Id<ChannelMarker>,
        tab_id: String,
        name: String,
    },
    Channel {
        id: Id<ChannelMarker>,
        workspace_id: String,
        name: String,
    },
}

impl GuildRename {
    const fn id(&self) -> Id<ChannelMarker> {
        match self {
            Self::Thread { id, .. } | Self::Channel { id, .. } => *id,
        }
    }

    fn name(&self) -> &str {
        match self {
            Self::Thread { name, .. } | Self::Channel { name, .. } => name,
        }
    }

    const fn audit_log_event(&self) -> AuditLogEventType {
        match self {
            Self::Thread { .. } => AuditLogEventType::ThreadUpdate,
            Self::Channel { .. } => AuditLogEventType::ChannelUpdate,
        }
    }
}

#[must_use]
pub fn owned_rename(channel: Channel) -> Option<GuildRename> {
    if let Some((tab_id, _)) = resolve_owner_deleted_tab(channel.id) {
        return Some(GuildRename::Thread {
            id: channel.id,
            tab_id,
            name: channel.name?,
        });
    }
    let workspace_id = resolve_owner_deleted_workspace(&channel)?;
    Some(GuildRename::Channel {
        id: channel.id,
        workspace_id,
        name: channel.name?,
    })
}

#[must_use]
pub fn herdr_rename(
    rename: &GuildRename,
    workspaces: &[HerdrWorkspace],
    tabs: &[HerdrTab],
) -> Option<(Renamed, bool)> {
    match rename {
        GuildRename::Thread { tab_id, name, .. } => {
            let tab = tabs.iter().find(|tab| &tab.tab_id == tab_id)?;
            let workspace = workspaces
                .iter()
                .find(|workspace| workspace.workspace_id == tab.workspace_id)?;
            if format_thread_name(&tab.label, tab_id).is_ok_and(|bridge| &bridge == name) {
                return None;
            }
            let label = name
                .strip_suffix(&format!(" [{tab_id}]"))
                .unwrap_or(name)
                .to_owned();
            Some((
                Renamed::Tab {
                    tab_id: tab_id.clone(),
                    label,
                },
                workspace.label.starts_with(TEST_RUN_PREFIX)
                    || tab.label.starts_with(TEST_RUN_PREFIX),
            ))
        }
        GuildRename::Channel {
            workspace_id, name, ..
        } => {
            let workspace = workspaces
                .iter()
                .find(|workspace| &workspace.workspace_id == workspace_id)?;
            if workspace_channel_name(workspace_id, &workspace.label)
                .is_ok_and(|bridge| &bridge == name)
            {
                return None;
            }
            Some((
                Renamed::Workspace {
                    workspace_id: workspace_id.clone(),
                    label: name.clone(),
                },
                workspace.label.starts_with(TEST_RUN_PREFIX),
            ))
        }
    }
}

#[must_use]
pub fn owner_renamed(rename: &GuildRename, entries: &[AuditLogEntry], owner_id: &str) -> bool {
    let target = rename.id().cast();
    entries.iter().any(|entry| {
        entry.action_type == rename.audit_log_event()
            && entry.target_id == Some(target)
            && entry
                .user_id
                .is_some_and(|user| user.to_string() == owner_id)
            && entry.changes.iter().any(|change| {
                matches!(change, AuditLogChange::Name { new: Some(new), .. } if new == rename.name())
            })
    })
}

pub async fn handle_guild_rename<F>(
    client: &twilight_http::Client,
    guild: Id<GuildMarker>,
    owner_id: &str,
    scope: (ChatScope, bool),
    rename: GuildRename,
    apply: F,
) -> Result<(), String>
where
    F: FnOnce(Renamed) -> Result<(), String> + Send + 'static,
{
    let (workspaces, tabs) = tokio::task::spawn_blocking(|| {
        Ok::<_, String>((workspace_list_result()?, tab_list_result()?))
    })
    .await
    .map_err(|error| error.to_string())??;
    let Some((renamed, test_run)) = herdr_rename(&rename, &workspaces, &tabs) else {
        return Ok(());
    };
    let (chat_scope, ignore_test_runs) = scope;
    if (chat_scope == ChatScope::TestRuns && !test_run) || (ignore_test_runs && test_run) {
        return Ok(());
    }
    let owner = owner_id
        .parse::<Id<UserMarker>>()
        .map_err(|error| format!("DISCORD_OWNER_ID is not numeric: {error}"))?;
    let entries = client
        .audit_log(guild)
        .action_type(rename.audit_log_event())
        .user_id(owner)
        .await
        .map_err(|error| format!("guild audit log: {error}"))?
        .model()
        .await
        .map_err(|error| format!("guild audit log: {error}"))?
        .entries;
    if !owner_renamed(&rename, &entries, owner_id) {
        return Ok(());
    }
    tokio::task::spawn_blocking(move || apply(renamed))
        .await
        .map_err(|error| error.to_string())?
}

#[cfg(test)]
mod tests {
    use twilight_model::guild::audit_log::{AuditLogChange, AuditLogEntry, AuditLogEventType};
    use twilight_model::id::Id;

    use super::{GuildRename, herdr_rename, owner_renamed};
    use crate::{HerdrTab, HerdrWorkspace, Renamed};

    const OWNER: u64 = 100_000_000_000_000_001;
    const THREAD: u64 = 9_201;

    fn thread(name: &str) -> GuildRename {
        GuildRename::Thread {
            id: Id::new(THREAD),
            tab_id: "w1F9:t1".to_owned(),
            name: name.to_owned(),
        }
    }

    fn channel(name: &str) -> GuildRename {
        GuildRename::Channel {
            id: Id::new(9_200),
            workspace_id: "w1F9".to_owned(),
            name: name.to_owned(),
        }
    }

    fn tab(label: &str) -> (Renamed, bool) {
        (
            Renamed::Tab {
                tab_id: "w1F9:t1".to_owned(),
                label: label.to_owned(),
            },
            true,
        )
    }

    #[test]
    fn owner_names_become_herdr_labels_and_bridge_names_are_ignored() {
        let workspaces = [HerdrWorkspace {
            workspace_id: "w1F9".to_owned(),
            label: "testrun-owner-409426260".to_owned(),
        }];
        let tabs = [HerdrTab {
            tab_id: "w1F9:t1".to_owned(),
            workspace_id: "w1F9".to_owned(),
            label: "testrun-tab-409426260".to_owned(),
        }];
        let cases = [
            (
                "a thread name that keeps the tab suffix",
                thread("testrun-kept-409426260 [w1F9:t1]"),
                Some(tab("testrun-kept-409426260")),
            ),
            (
                "a thread name without the tab suffix",
                thread("testrun-whole-409426260"),
                Some(tab("testrun-whole-409426260")),
            ),
            (
                "the thread name the bridge sets",
                thread("testrun-tab-409426260 [w1F9:t1]"),
                None,
            ),
            (
                "a workspace channel name",
                channel("testrun-chan-409426260"),
                Some((
                    Renamed::Workspace {
                        workspace_id: "w1F9".to_owned(),
                        label: "testrun-chan-409426260".to_owned(),
                    },
                    true,
                )),
            ),
            (
                "the channel name the bridge sets",
                channel("testrun-owner-409426260"),
                None,
            ),
        ];
        for (name, rename, expected) in cases {
            assert_eq!(
                herdr_rename(&rename, &workspaces, &tabs),
                expected,
                "{name}"
            );
        }
    }

    #[test]
    fn only_an_owner_audit_entry_for_the_new_name_proves_the_rename() {
        let entry = |user: u64, target: u64, new: &str| AuditLogEntry {
            action_type: AuditLogEventType::ThreadUpdate,
            changes: vec![AuditLogChange::Name {
                new: Some(new.to_owned()),
                old: Some("testrun-tab-409426260 [w1F9:t1]".to_owned()),
            }],
            guild_id: None,
            id: Id::new(1),
            options: None,
            reason: None,
            target_id: Some(Id::new(target)),
            user_id: Some(Id::new(user)),
        };
        let renamed = "testrun-whole-409426260";
        let cases = [
            (
                "the owner renamed the thread",
                entry(OWNER, THREAD, renamed),
                true,
            ),
            (
                "another user renamed the thread",
                entry(OWNER + 1, THREAD, renamed),
                false,
            ),
            (
                "the owner renamed another thread",
                entry(OWNER, THREAD + 1, renamed),
                false,
            ),
            (
                "the owner set an earlier name",
                entry(OWNER, THREAD, "testrun-kept-409426260"),
                false,
            ),
        ];
        for (name, entry, expected) in cases {
            assert_eq!(
                owner_renamed(&thread(renamed), &[entry], &OWNER.to_string()),
                expected,
                "{name}"
            );
        }
    }
}
