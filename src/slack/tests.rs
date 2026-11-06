use std::collections::HashSet;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use serial_test::serial;
use slack_morphism::prelude::*;

use super::{Slack, SlackRoute, slack_channel_name};
use crate::{
    Chat, Question, QuestionOption, TopologyClosure, TopologyRoute, tab_list_result,
    workspace_list_result,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn herdr(args: &[&str]) -> Result<Value> {
    let output = Command::new("herdr").args(args).output()?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned().into());
    }
    Ok(serde_json::from_slice::<Value>(&output.stdout)?["result"].clone())
}

fn field(value: &Value, pointer: &str) -> Result<String> {
    Ok(value
        .pointer(pointer)
        .and_then(Value::as_str)
        .ok_or("missing Herdr result field")?
        .to_owned())
}

fn preserve_non_test_channels(channels: &[SlackChannelInfo]) -> Result<()> {
    let workspaces = workspace_list_result()?;
    let tabs = tab_list_result()?;
    for channel in channels {
        if channel
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with("testrun-"))
        {
            continue;
        }
        let Some(topic) = channel
            .topic
            .as_ref()
            .and_then(|topic| topic.value.strip_prefix("herdr workspace ["))
        else {
            continue;
        };
        let Some((workspace_id, tab_id)) = topic.split_once("] tab [") else {
            return Err(
                "sweep would archive a non-test legacy channel; refused before sweep".into(),
            );
        };
        let tab_id = tab_id
            .strip_suffix(']')
            .ok_or("invalid channel tab topic")?;
        let workspace = workspaces
            .iter()
            .find(|entry| entry.workspace_id == workspace_id)
            .ok_or("sweep would archive a non-test workspace channel; refused before sweep")?;
        let tab = tabs
            .iter()
            .find(|entry| entry.tab_id == tab_id && entry.workspace_id == workspace_id)
            .ok_or("sweep would archive a non-test tab channel; refused before sweep")?;
        let expected = slack_channel_name(
            &workspace.label,
            &workspace.workspace_id,
            &tab.label,
            &tab.tab_id,
        )?;
        if channel.name.as_deref() != Some(expected.as_str()) {
            return Err("sweep would rename a non-test channel; refused before sweep".into());
        }
    }
    Ok(())
}

struct Owned {
    chat: Arc<Slack>,
    workspace: String,
    label: String,
    primary: TopologyRoute,
    channels: Vec<SlackChannelId>,
    names: HashSet<String>,
    before: Vec<SlackChannelInfo>,
}

impl Owned {
    async fn new() -> Result<Self> {
        let chat = Arc::new(Slack::from_env().await?);
        let before = chat.channels().await?;
        if before.iter().any(|channel| {
            channel.flags.is_archived != Some(true)
                && channel
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("testrun-"))
        }) {
            return Err("preexisting testrun Slack channels; refused before sweep".into());
        }
        preserve_non_test_channels(&before)?;
        let label = format!(
            "testrun-review-{}",
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis()
        );
        let created = herdr(&[
            "workspace",
            "create",
            "--label",
            &label,
            "--cwd",
            "/tmp",
            "--no-focus",
        ])?;
        let workspace = field(&created, "/workspace/workspace_id")?;
        let primary = TopologyRoute {
            workspace_id: workspace.clone(),
            tab_id: field(&created, "/tab/tab_id")?,
            pane_id: field(&created, "/root_pane/pane_id")?,
            channel_name: label.clone(),
            thread_name: "開発".repeat(40),
        };
        herdr(&["tab", "rename", &primary.tab_id, &primary.thread_name])?;
        let names = HashSet::from([slack_channel_name(
            &label,
            &workspace,
            &primary.thread_name,
            &primary.tab_id,
        )?]);
        Ok(Self {
            chat,
            workspace,
            label,
            primary,
            channels: Vec::new(),
            names,
            before,
        })
    }

    fn tab(&self, label: &str) -> Result<TopologyRoute> {
        let created = herdr(&[
            "tab",
            "create",
            "--workspace",
            &self.workspace,
            "--label",
            label,
            "--cwd",
            "/tmp",
            "--no-focus",
        ])?;
        Ok(TopologyRoute {
            workspace_id: self.workspace.clone(),
            tab_id: field(&created, "/tab/tab_id")?,
            pane_id: field(&created, "/root_pane/pane_id")?,
            channel_name: self.label.clone(),
            thread_name: label.to_owned(),
        })
    }

    async fn create(&mut self, route: &TopologyRoute, topic: bool) -> Result<SlackChannelInfo> {
        let name = slack_channel_name(
            &self.label,
            &self.workspace,
            &route.thread_name,
            &route.tab_id,
        )?;
        self.names.insert(name.clone());
        let session = self.chat.state.client.open_session(&self.chat.state.token);
        let channel = session
            .conversations_create(
                &SlackApiConversationsCreateRequest::new(name).with_is_private(false),
            )
            .await?
            .channel;
        self.channels.push(channel.id.clone());
        if topic {
            session
                .conversations_set_topic(&SlackApiConversationsSetTopicRequest::new(
                    channel.id.clone(),
                    format!(
                        "herdr workspace [{}] tab [{}]",
                        self.workspace, route.tab_id
                    ),
                ))
                .await?;
        }
        Ok(channel)
    }

    async fn mapped(&mut self, route: &TopologyRoute) -> Result<SlackRoute> {
        self.names.insert(slack_channel_name(
            &self.label,
            &self.workspace,
            &route.thread_name,
            &route.tab_id,
        )?);
        let mapped = self.chat.sync(route, false).await?;
        if !self.channels.contains(&mapped.channel) {
            self.channels.push(mapped.channel.clone());
        }
        Ok(mapped)
    }

    async fn info(&self, channel: &SlackChannelId) -> Result<SlackChannelInfo> {
        Ok(self
            .chat
            .state
            .client
            .open_session(&self.chat.state.token)
            .conversations_info(&SlackApiConversationsInfoRequest::new(channel.clone()))
            .await?
            .channel)
    }

    async fn members(&self, channel: &SlackChannelId) -> Result<Vec<SlackUserId>> {
        Ok(self
            .chat
            .state
            .client
            .open_session(&self.chat.state.token)
            .conversations_members(
                &SlackApiConversationsMembersRequest::new().with_channel(channel.clone()),
            )
            .await?
            .members)
    }

    async fn wait_name(&self, channel: &SlackChannelId, expected: &str) -> Result<bool> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        loop {
            if self.info(channel).await?.name.as_deref() == Some(expected) {
                return Ok(true);
            }
            if tokio::time::Instant::now() >= deadline {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    async fn preserved(&self) -> Result<bool> {
        let after = self.chat.channels().await?;
        Ok(self
            .before
            .iter()
            .filter(|channel| {
                !channel
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("testrun-"))
            })
            .all(|channel| {
                after.iter().any(|entry| {
                    entry.id == channel.id
                        && entry.name == channel.name
                        && entry.flags.is_archived != Some(true)
                })
            }))
    }

    async fn cleanup(&self) -> Result<()> {
        let mut ids = self.channels.iter().cloned().collect::<HashSet<_>>();
        ids.extend(
            self.chat
                .channels()
                .await?
                .into_iter()
                .filter(|channel| {
                    channel.creator.as_ref() == Some(&self.chat.bot)
                        && channel
                            .name
                            .as_ref()
                            .is_some_and(|name| self.names.contains(name))
                })
                .map(|channel| channel.id),
        );
        let mut failure = None;
        for id in &ids {
            match self.info(id).await {
                Ok(channel) if channel.flags.is_archived != Some(true) => {
                    if let Err(error) = self.chat.archive(channel).await {
                        failure = Some(error.to_string());
                    }
                }
                Err(error) => failure = Some(error.to_string()),
                Ok(_) => {}
            }
        }
        herdr(&["workspace", "close", &self.workspace])?;
        let mut remaining = 0;
        for id in &ids {
            remaining += usize::from(self.info(id).await?.flags.is_archived != Some(true));
        }
        println!("named zero-leftover check: {remaining} owned unarchived Slack channels");
        if let Some(error) = failure {
            return Err(error.into());
        }
        assert_eq!(remaining, 0, "zero-leftover check: owned Slack channels");
        Ok(())
    }
}

fn live() -> std::result::Result<(HashSet<String>, HashSet<String>), String> {
    Ok((
        workspace_list_result()?
            .into_iter()
            .map(|workspace| workspace.workspace_id)
            .collect(),
        tab_list_result()?
            .into_iter()
            .map(|tab| tab.tab_id)
            .collect(),
    ))
}

async fn recover_mapping(owned: &mut Owned) -> Result<(Vec<(&'static str, bool)>, TopologyRoute)> {
    let primary = owned.primary.clone();
    let partial = owned.create(&primary, false).await?;
    let first = owned.mapped(&primary).await?;
    let secondary = owned.tab("testrun-delivery")?;
    let existing = owned.create(&secondary, true).await?;
    let second = owned.mapped(&secondary).await?;
    let mut checks = vec![
        ("partial channel adopted", first.channel == partial.id),
        (
            "partial channel completed",
            owned
                .info(&first.channel)
                .await?
                .topic
                .is_some_and(|topic| {
                    topic.value
                        == format!(
                            "herdr workspace [{}] tab [{}]",
                            owned.workspace, primary.tab_id
                        )
                }),
        ),
        (
            "owner invited after partial create",
            owned
                .members(&first.channel)
                .await?
                .contains(&owned.chat.owner),
        ),
        (
            "existing mapping invites owner",
            second.channel == existing.id
                && owned
                    .members(&second.channel)
                    .await?
                    .contains(&owned.chat.owner),
        ),
        (
            "Unicode labels fit 80 characters",
            owned
                .info(&first.channel)
                .await?
                .name
                .is_some_and(|name| name.contains("開発") && name.chars().count() <= 80),
        ),
    ];
    owned
        .chat
        .state
        .client
        .open_session(&owned.chat.state.token)
        .conversations_archive(&SlackApiConversationsArchiveRequest::new(
            first.channel.clone(),
        ))
        .await?;
    let restored = owned.chat.sync(&primary, true).await?;
    checks.push((
        "bot archive recovered",
        restored.channel == first.channel
            && owned.info(&first.channel).await?.flags.is_archived != Some(true),
    ));
    Ok((checks, secondary))
}

async fn recover_sweep(
    owned: &mut Owned,
    secondary: &TopologyRoute,
) -> Result<Vec<(&'static str, bool)>> {
    let primary = owned.primary.clone();
    let second = owned
        .chat
        .cached_route(secondary)
        .await?
        .ok_or("secondary route missing")?;
    let mut checks = Vec::new();
    let orphan = owned.tab("testrun-orphan")?;
    let orphan_channel = owned.mapped(&orphan).await?;
    herdr(&["tab", "close", &orphan.tab_id])?;
    herdr(&["tab", "rename", &primary.tab_id, "🚧"])?;
    herdr(&["tab", "rename", &secondary.tab_id, "testrun-renamed"])?;
    let invalid = owned.tab("🚧")?;
    owned.before = owned.chat.channels().await?;
    preserve_non_test_channels(&owned.before)?;
    let sweep = owned
        .chat
        .sweep(vec![Ok(invalid), Ok(secondary.clone())], live)
        .await;
    let expected = slack_channel_name(
        &owned.label,
        &owned.workspace,
        "testrun-renamed",
        &secondary.tab_id,
    )?;
    checks.extend([
        ("invalid route reported", sweep.is_err()),
        (
            "orphan archived despite invalid route",
            owned.info(&orphan_channel.channel).await?.flags.is_archived == Some(true),
        ),
        (
            "valid name reconciled despite invalid label",
            owned.wait_name(&second.channel, &expected).await?,
        ),
    ]);
    Ok(checks)
}

async fn check_cards(owned: &Owned, second: &SlackRoute) -> Result<Vec<(&'static str, bool)>> {
    let mut checks = Vec::new();
    let question = Question {
        header: "Color".into(),
        question: "Which color?".into(),
        options: vec![
            QuestionOption {
                label: "Red".into(),
                description: "The color red".into(),
            },
            QuestionOption {
                label: "Blue".into(),
                description: "The color blue".into(),
            },
        ],
        multi_select: false,
    };
    let question_message = owned
        .chat
        .question_card(second, &question, "testrun-card-details")
        .await?;
    let history = owned.chat.history(&second.channel).await?;
    let card = history
        .iter()
        .find(|message| message.origin.ts == question_message)
        .ok_or("question message missing")?;
    let rendered = serde_json::to_string(card)?;
    checks.push((
        "question header and descriptions",
        rendered.contains("Color")
            && rendered.contains("The color red")
            && rendered.contains("The color blue"),
    ));
    let mut question = question;
    question.multi_select = true;
    owned
        .chat
        .update_question(second, &question_message, &question, "testrun-card-details")
        .await?;
    let updated = owned.chat.history(&second.channel).await?;
    let card = updated
        .iter()
        .find(|message| message.origin.ts == question_message)
        .ok_or("updated question message missing")?;
    let blocks = serde_json::to_value(&card.content.blocks)?;
    checks.push((
        "multiple choice layout",
        blocks[3]["accessory"]["type"] == "multi_static_select"
            && blocks[4]["elements"][0]["action_id"] == "herdrask-multi:testrun-card-details",
    ));
    Ok(checks)
}

async fn check_profile(owned: &Owned, second: &SlackRoute) -> Result<Vec<(&'static str, bool)>> {
    let mut checks = Vec::new();
    owned
        .chat
        .mirror_prompt(second, "testrun-profile-mirror")
        .await?;
    let session = owned
        .chat
        .state
        .client
        .open_session(&owned.chat.state.token);
    let history: Value = session
        .http_session_api
        .http_get(
            "conversations.history",
            &vec![
                ("channel", Some(second.channel.to_string())),
                ("limit", Some("100".to_owned())),
            ],
            None,
        )
        .await?;
    let mirrored = history["messages"]
        .as_array()
        .ok_or("history messages missing")?
        .iter()
        .find(|message| message["text"] == "testrun-profile-mirror")
        .ok_or("mirrored prompt missing")?;
    let profile = session
        .users_info(&SlackApiUsersInfoRequest::new(owned.chat.bot.clone()))
        .await?
        .user
        .profile
        .ok_or("bot profile missing during verification")?;
    let images = profile
        .icon
        .and_then(|icon| icon.images)
        .ok_or("bot avatar images missing during verification")?;
    let avatar = mirrored["icons"].as_object().is_some_and(|icons| {
        !icons.is_empty()
            && icons.values().all(|url| {
                url.as_str()
                    .is_some_and(|url| !images.resolutions.iter().any(|(_, image)| image == url))
            })
    });
    checks.extend([
        (
            "prompt has owner display name",
            mirrored["username"].as_str() == Some(&owned.chat.owner_identity.display_name),
        ),
        ("prompt uses customized owner avatar", avatar),
    ]);
    Ok(checks)
}

async fn check_status_delivery(
    owned: &Owned,
    second: &SlackRoute,
) -> Result<Vec<(&'static str, bool)>> {
    let mut checks = Vec::new();
    let session = owned
        .chat
        .state
        .client
        .open_session(&owned.chat.state.token);
    let anchor = owned.chat.send(second, "testrun-status-anchor").await?;
    owned
        .chat
        .state
        .status_messages
        .lock()
        .await
        .insert(second.channel.clone(), anchor.clone());
    owned.chat.typing(second).await?;
    checks.push((
        "typing registered for clearing",
        owned
            .chat
            .state
            .working
            .lock()
            .await
            .get(second)
            .is_some_and(Option::is_some),
    ));
    session
        .chat_delete(&SlackApiChatDeleteRequest::new(
            second.channel.clone(),
            anchor,
        ))
        .await?;
    checks.push((
        "status error reproduced after anchor deletion",
        owned.chat.set_working(second, true).await.is_err(),
    ));
    let delivered = owned
        .chat
        .send(second, "testrun-post-survives-status-failure")
        .await;
    checks.push(("successful post survives status failure", delivered.is_ok()));
    checks.push((
        "post not duplicated",
        owned
            .chat
            .history(&second.channel)
            .await?
            .iter()
            .filter(|message| {
                message.content.text.as_deref() == Some("testrun-post-survives-status-failure")
            })
            .count()
            == 1,
    ));
    let task = owned.chat.state.working.lock().await.remove(second);
    if let Some(Some(task)) = task {
        task.abort();
    }
    owned
        .chat
        .state
        .status_messages
        .lock()
        .await
        .remove(&second.channel);
    Ok(checks)
}

async fn check_background_closure(
    owned: &Owned,
    second: &SlackRoute,
) -> Result<Vec<(&'static str, bool)>> {
    let primary = owned.primary.clone();
    let first = owned
        .chat
        .cached_route(&primary)
        .await?
        .ok_or("primary route missing")?;
    let mut checks = Vec::new();
    let rename_lock = owned.chat.state.renames.lock().await;
    herdr(&["tab", "close", &primary.tab_id])?;
    let queued = tokio::time::timeout(
        Duration::from_secs(1),
        owned.chat.close_topology(&TopologyClosure::Tab {
            workspace_id: owned.workspace.clone(),
            tab_id: primary.tab_id,
        }),
    )
    .await;
    checks.push((
        "closure queued while rename blocked",
        matches!(queued, Ok(Ok(()))),
    ));
    checks.push((
        "other tab delivers while closure blocked",
        owned
            .chat
            .send(second, "testrun-delivery-during-close")
            .await
            .is_ok(),
    ));
    drop(rename_lock);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
    while owned.info(&first.channel).await?.flags.is_archived != Some(true)
        && tokio::time::Instant::now() < deadline
    {
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    checks.push((
        "background closure finished",
        owned.info(&first.channel).await?.flags.is_archived == Some(true),
    ));
    Ok(checks)
}

async fn recovery(owned: &mut Owned) -> Result<Vec<(&'static str, bool)>> {
    let (mut checks, secondary) = recover_mapping(owned).await?;
    checks.extend(recover_sweep(owned, &secondary).await?);
    let second = owned
        .chat
        .cached_route(&secondary)
        .await?
        .ok_or("secondary route missing")?;
    checks.extend(check_cards(owned, &second).await?);
    checks.extend(check_profile(owned, &second).await?);
    checks.extend(check_status_delivery(owned, &second).await?);
    checks.extend(check_background_closure(owned, &second).await?);
    checks.push(("non-test channels preserved", owned.preserved().await?));
    Ok(checks)
}

#[tokio::test]
#[serial]
async fn slack_runtime_recovers_real_topology() -> Result<()> {
    let mut owned = Owned::new().await?;
    let outcome = recovery(&mut owned).await;
    owned.cleanup().await?;
    for (case, observed) in outcome? {
        println!("real Slack runtime: {case}: {observed}");
        assert!(observed, "{case}");
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn slack_startup_reads_live_after_route_sync() -> Result<()> {
    let mut owned = Owned::new().await?;
    let outcome = async {
        let called = Arc::new(AtomicBool::new(false));
        owned.before = owned.chat.channels().await?;
        preserve_non_test_channels(&owned.before)?;
        let cache = owned.chat.state.topology.clone();
        let lock = cache.lock().await;
        let chat = owned.chat.clone();
        let route = owned.primary.clone();
        let callback = called.clone();
        let (started, running) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = started.send(());
            chat.startup_sweep(vec![Ok(route)], || {
                callback.store(true, Ordering::SeqCst);
                live()
            }).await;
        });
        running.await?;
        tokio::task::yield_now().await;
        let early = called.load(Ordering::SeqCst);
        let late = owned.tab("testrun-late")?;
        let channel = owned.create(&late, true).await?;
        drop(lock);
        task.await?;
        let primary = owned.chat.cached_route(&owned.primary).await?.ok_or("primary route not mapped")?;
        owned.channels.push(primary.channel);
        let preserved = owned.info(&channel.id).await?.flags.is_archived != Some(true);
        println!("real Slack startup: callback_before_sync={early} late_channel_preserved={preserved} callback_completed={}", called.load(Ordering::SeqCst));
        let checks = vec![
            ("live callback follows route sync", !early),
            ("late channel remains unarchived", preserved),
            ("live callback completed", called.load(Ordering::SeqCst)),
            ("non-test channels preserved", owned.preserved().await?),
        ];
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(checks)
    }.await;
    owned.cleanup().await?;
    for (case, observed) in outcome? {
        assert!(observed, "{case}");
    }
    Ok(())
}
