use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use herdr_connect_rs::{
    Chat, ChatScope, Inbound, PermissionVendor, Question, QuestionOption, Slack, SlackRoute,
    TopologyClosure, TopologyRoute, slack_channel_name, tab_list_result, workspace_list_result,
};
use serde_json::Value;
use slack_morphism::prelude::*;

fn herdr(args: &[&str]) -> Result<Value, String> {
    let output = Command::new("herdr")
        .args(args)
        .output()
        .map_err(|error| error.to_string())?;
    if !output.status.success() {
        return Err(String::from_utf8_lossy(&output.stderr).into_owned());
    }
    serde_json::from_slice(&output.stdout).map_err(|error| error.to_string())
}

#[tokio::test]
async fn slack_topology_real_workspace() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("SLACK_BOT_TOKEN")?;
    let slack = Slack::from_env().await?;
    let label = format!(
        "testrun-slack-{}",
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
    let workspace = created["result"]["workspace"]["workspace_id"]
        .as_str()
        .ok_or("created workspace has no id")?;
    let tab = created["result"]["tab"]["tab_id"]
        .as_str()
        .ok_or("created tab has no id")?;
    herdr(&["tab", "rename", tab, "testrun-build"])?;
    let route = TopologyRoute {
        workspace_id: workspace.into(),
        tab_id: tab.into(),
        pane_id: created["result"]["root_pane"]["pane_id"]
            .as_str()
            .ok_or("created pane has no id")?
            .into(),
        channel_name: label.clone(),
        thread_name: format!("testrun-build [{tab}]"),
    };
    let result = exercise_topology(&slack, &route, token).await;
    let leftovers = cleanup(&slack, &label, workspace).await?;
    assert_eq!(
        leftovers, 0,
        "zero-leftover check: unarchived testrun- Slack channels"
    );
    for (name, passed) in result? {
        assert!(passed, "{name}");
    }
    Ok(())
}

async fn exercise_topology(
    slack: &Slack,
    route: &TopologyRoute,
    token: String,
) -> Result<Vec<(&'static str, bool)>, Box<dyn std::error::Error>> {
    let name = slack_channel_name(
        &route.channel_name,
        &route.workspace_id,
        "testrun-build",
        &route.tab_id,
    )?;
    let label = &name;
    let workspace = route.workspace_id.as_str();
    let tab = route.tab_id.as_str();
    let found = slack.sync(route, false).await?;
    let mut checks = Vec::new();
    let channels = slack.channels().await?;
    checks.push((
        "channel uses workspace and tab labels and ids",
        channels.iter().any(|channel| {
            channel.id == found.channel && channel.name.as_deref() == Some(label.as_str())
        }),
    ));
    let client = SlackClient::new(SlackClientHyperConnector::new()?);
    let api_token = SlackApiToken::new(token.into());
    let members = client
        .open_session(&api_token)
        .conversations_members(
            &SlackApiConversationsMembersRequest::new().with_channel(found.channel.clone()),
        )
        .await?;
    checks.push((
        "owner invited",
        members
            .members
            .contains(&SlackUserId::from(slack.owner_id())),
    ));
    checks.extend(exercise_rename(slack, route, &found).await?);
    herdr(&["tab", "close", tab])?;
    slack
        .close(&TopologyClosure::Tab {
            workspace_id: workspace.into(),
            tab_id: tab.into(),
        })
        .await?;
    checks.push((
        "tab channel archived",
        client
            .open_session(&api_token)
            .conversations_info(&SlackApiConversationsInfoRequest::new(
                found.channel.clone(),
            ))
            .await?
            .channel
            .flags
            .is_archived
            == Some(true),
    ));
    Ok::<_, Box<dyn std::error::Error>>(checks)
}

async fn exercise_rename(
    slack: &Slack,
    route: &TopologyRoute,
    found: &SlackRoute,
) -> Result<Vec<(&'static str, bool)>, Box<dyn std::error::Error>> {
    let workspace_label = format!("{}-renamed", route.channel_name);
    herdr(&["workspace", "rename", &route.workspace_id, &workspace_label])?;
    herdr(&["tab", "rename", &route.tab_id, "testrun-renamed"])?;
    let expected = slack_channel_name(
        &workspace_label,
        &route.workspace_id,
        "testrun-renamed",
        &route.tab_id,
    )?;
    let started = tokio::time::Instant::now();
    let delivery = async {
        let sent = slack.send(found, "testrun-delivery-during-rename").await?;
        let delivered_at = started.elapsed();
        let received = slack.history(&found.channel).await?.iter().any(|message| {
            message.origin.ts == sent
                && message.content.text.as_deref() == Some("testrun-delivery-during-rename")
        });
        println!(
            "real rename message: elapsed_ms={} server_ts={} history_received={received}",
            delivered_at.as_millis(),
            sent
        );
        Ok::<_, herdr_connect_rs::ChatError>((delivered_at, received))
    };
    let rename = async {
        slack.refresh_names().await?;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(120);
        loop {
            let renamed = slack.channels().await?.iter().any(|channel| {
                channel.id == found.channel && channel.name.as_deref() == Some(expected.as_str())
            });
            if renamed || tokio::time::Instant::now() >= deadline {
                println!(
                    "real rename channel: elapsed_ms={} matched={renamed}",
                    started.elapsed().as_millis()
                );
                return Ok::<_, herdr_connect_rs::ChatError>((started.elapsed(), renamed));
            }
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        }
    };
    let (delivered, renamed) = tokio::join!(delivery, rename);
    let (delivered_at, received) = delivered?;
    let (renamed_at, name_matches) = renamed?;
    println!(
        "real rename timing: message_ms={} name_observed_ms={}",
        delivered_at.as_millis(),
        renamed_at.as_millis()
    );
    Ok(vec![(
        "workspace and tab rename with message delivery",
        received && name_matches,
    )])
}

async fn cleanup(
    slack: &Slack,
    label: &str,
    workspace: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    for channel in slack.channels().await? {
        if channel
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with(label))
        {
            let client = SlackClient::new(SlackClientHyperConnector::new()?);
            let token = SlackApiToken::new(std::env::var("SLACK_BOT_TOKEN")?.into());
            client
                .open_session(&token)
                .conversations_archive(&SlackApiConversationsArchiveRequest::new(channel.id))
                .await?;
        }
    }
    if workspace_list_result()?
        .iter()
        .any(|entry| entry.workspace_id == workspace)
    {
        herdr(&["workspace", "close", workspace])?;
    }
    let leftovers = slack
        .channels()
        .await?
        .iter()
        .filter(|channel| {
            channel
                .name
                .as_deref()
                .is_some_and(|name| name.starts_with("testrun-"))
        })
        .count();
    println!("zero-leftover check: {leftovers} unarchived testrun- Slack channels");
    Ok::<_, Box<dyn std::error::Error>>(leftovers)
}

fn create_route() -> Result<TopologyRoute, Box<dyn std::error::Error>> {
    let label = format!(
        "testrun-slack-{}",
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
    let workspace = created["result"]["workspace"]["workspace_id"]
        .as_str()
        .ok_or("created workspace has no id")?;
    let tab = created["result"]["tab"]["tab_id"]
        .as_str()
        .ok_or("created tab has no id")?;
    herdr(&["tab", "rename", tab, "testrun-build"])?;
    Ok(TopologyRoute {
        workspace_id: workspace.into(),
        tab_id: tab.into(),
        pane_id: created["result"]["root_pane"]["pane_id"]
            .as_str()
            .ok_or("created pane has no id")?
            .into(),
        channel_name: label,
        thread_name: format!("testrun-build [{tab}]"),
    })
}

#[tokio::test]
async fn slack_delivery_real_channel() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("SLACK_BOT_TOKEN")?;
    let slack = Slack::from_env().await?;
    let route = create_route()?;
    let result = async {
        let (parent, found) = slack.sync_route_with_parent(&route).await?;
        slack
            .post_live(&found, "testrun agent reply <literal>", "testrun-reply")
            .await?;
        slack
            .post_as_owner(
                &parent,
                &found,
                slack.owner_identity(),
                "testrun terminal prompt",
            )
            .await?;
        let client = SlackClient::new(SlackClientHyperConnector::new()?);
        let token = SlackApiToken::new(token.into());
        let replies = client
            .open_session(&token)
            .conversations_history(
                &SlackApiConversationsHistoryRequest::new().with_channel(found.channel.clone()),
            )
            .await?;
        let checks = vec![
            (
                "agent reply in channel",
                replies.messages.iter().any(|message| {
                    message.origin.thread_ts.is_none()
                        && message.content.text.as_deref()
                            == Some("testrun agent reply &lt;literal&gt;")
                }),
            ),
            (
                "terminal prompt uses owner display name",
                replies.messages.iter().any(|message| {
                    message.sender.bot_id.is_some()
                        && message.sender.username.as_deref()
                            == Some(slack.owner_identity().display_name.as_str())
                        && message.content.text.as_deref() == Some("testrun terminal prompt")
                }),
            ),
        ];
        Ok::<_, Box<dyn std::error::Error>>(checks)
    }
    .await;
    let leftovers = cleanup(&slack, &route.channel_name, &route.workspace_id).await?;
    assert_eq!(
        leftovers, 0,
        "zero-leftover check: unarchived testrun- Slack channels"
    );
    for (name, passed) in result? {
        assert!(passed, "{name}");
    }
    Ok(())
}

#[tokio::test]
async fn slack_socket_real_events() -> Result<(), Box<dyn std::error::Error>> {
    let slack = Slack::from_env().await?;
    let route = create_route()?;
    let result = async {
        slack
            .connect_socket(std::env::var("SLACK_APP_TOKEN")?)
            .await?;
        let found = slack.sync(&route, false).await?;
        let sent = slack.send(&found, "testrun socket delivery").await?;
        let received = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(Ok(Inbound::Posted(message))) = slack.next_event().await
                    && message.origin.ts == sent
                {
                    break message;
                }
            }
        })
        .await?;
        let ignored = slack.owner_message(received).await?.is_none();
        Ok::<_, Box<dyn std::error::Error>>(vec![("bot message ignored as owner prompt", ignored)])
    }
    .await;
    let shutdown = slack.shutdown_socket().await;
    let leftovers = cleanup(&slack, &route.channel_name, &route.workspace_id).await?;
    shutdown?;
    assert_eq!(
        leftovers, 0,
        "zero-leftover check: unarchived testrun- Slack channels"
    );
    for (name, passed) in result? {
        assert!(passed, "{name}");
    }
    Ok(())
}

#[tokio::test]
async fn slack_cards_real_channel() -> Result<(), Box<dyn std::error::Error>> {
    let slack = Slack::from_env().await?;
    let route = create_route()?;
    let result = async {
        let found = slack.sync_route(&route).await?;
        let permission = slack
            .post_permission_card(
                &found,
                PermissionVendor::Claude,
                "Bash",
                "printf testrun",
                "testrun-permission",
            )
            .await?;
        let mut question = test_question();
        let card = slack
            .post_question_card(&found, &question, "testrun-question")
            .await?;
        let initial = slack.history(&found.channel).await?;
        let permission_blocks = message_blocks(&initial, &permission)?;
        let single_blocks = message_blocks(&initial, &card)?;
        question.multi_select = true;
        slack
            .edit_question_card(&found, &card, &question, "testrun-question")
            .await?;
        let multi = slack.history(&found.channel).await?;
        let multi_blocks = message_blocks(&multi, &card)?;
        slack
            .expire_permission_card(
                &found,
                &permission,
                "testrun-permission",
                "testrun resolved",
            )
            .await?;
        slack.expire_card(&found, &card, "testrun answered").await?;
        let retired = slack.history(&found.channel).await?;
        let checks = vec![
            (
                "permission allow button",
                permission_blocks[2]["elements"][0]["action_id"]
                    == "herdr:allow:testrun-permission",
            ),
            (
                "permission deny button",
                permission_blocks[2]["elements"][1]["action_id"] == "herdr:deny:testrun-permission",
            ),
            (
                "question header and option descriptions included",
                single_blocks[0]["text"]["text"]
                    .as_str()
                    .is_some_and(|text| text.contains(&question.header))
                    && question.options.iter().all(|option| {
                        single_blocks[2]["text"]["text"]
                            .as_str()
                            .is_some_and(|text| text.contains(&option.description))
                    }),
            ),
            (
                "single-choice button",
                single_blocks[3]["elements"][1]["action_id"] == "herdrask:testrun-question:1",
            ),
            (
                "multiple-choice menu",
                multi_blocks[3]["accessory"]["type"] == "multi_static_select",
            ),
            (
                "multiple-choice submit button",
                multi_blocks[4]["elements"][0]["action_id"] == "herdrask-multi:testrun-question",
            ),
            (
                "retired permission has no actions",
                !message_blocks(&retired, &permission)?
                    .to_string()
                    .contains("action_id"),
            ),
            (
                "retired question has no actions",
                !message_blocks(&retired, &card)?
                    .to_string()
                    .contains("action_id"),
            ),
        ];
        Ok::<_, Box<dyn std::error::Error>>(checks)
    }
    .await;
    let shutdown = slack.shutdown_socket().await;
    let leftovers = cleanup(&slack, &route.channel_name, &route.workspace_id).await?;
    shutdown?;
    assert_eq!(
        leftovers, 0,
        "zero-leftover check: unarchived testrun- Slack channels"
    );
    for (name, passed) in result? {
        assert!(passed, "{name}");
    }
    Ok(())
}

fn message_blocks(
    messages: &[SlackHistoryMessage],
    ts: &SlackTs,
) -> Result<Value, Box<dyn std::error::Error>> {
    let message = messages
        .iter()
        .find(|message| &message.origin.ts == ts)
        .ok_or("card missing from real Slack channel")?;
    Ok(serde_json::to_value(&message.content.blocks)?)
}

fn test_question() -> Question {
    Question {
        question: "testrun: which option?".into(),
        header: "Choice".into(),
        options: vec![
            QuestionOption {
                label: "First".into(),
                description: "first choice".into(),
            },
            QuestionOption {
                label: "Second".into(),
                description: "second choice".into(),
            },
        ],
        multi_select: false,
    }
}

fn check_sweep_channels(
    slack: &Slack,
    before: &[SlackChannelInfo],
) -> Result<(), Box<dyn std::error::Error>> {
    if slack.scope() == ChatScope::Service
        && before.iter().any(|channel| {
            !channel
                .name
                .as_deref()
                .is_some_and(|name| name.starts_with("testrun-"))
                && channel
                    .topic
                    .as_ref()
                    .is_some_and(|topic| topic.value.starts_with("herdr workspace ["))
        })
    {
        return Err("service sweep refused while a non-test bridge channel exists".into());
    }
    Ok(())
}

async fn sweep_preserving_channels(
    slack: &Slack,
    routes: Vec<Result<TopologyRoute, String>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let before = slack.channels().await?;
    check_sweep_channels(slack, &before)?;
    slack
        .startup_sweep(routes, || {
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
        })
        .await;
    let after = slack.channels().await?;
    for channel in before {
        if !channel
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with("testrun-"))
            && !after.iter().any(|entry| {
                entry.id == channel.id
                    && entry.name == channel.name
                    && entry.flags.is_archived != Some(true)
            })
        {
            return Err("non-test channel changed during startup sweep".into());
        }
    }
    Ok(())
}

async fn preserve_service_orphan(
    service: &Slack,
    route: &TopologyRoute,
    channel: &SlackChannelId,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = SlackClient::new(SlackClientHyperConnector::new()?);
    let token = SlackApiToken::new(std::env::var("SLACK_BOT_TOKEN")?.into());
    let session = client.open_session(&token);
    let request = SlackApiConversationsInfoRequest::new(channel.clone());
    let name = session
        .conversations_info(&request)
        .await?
        .channel
        .name
        .ok_or("owned Slack channel has no name")?;
    herdr(&["workspace", "close", &route.workspace_id])?;
    if workspace_list_result()?
        .iter()
        .any(|workspace| workspace.workspace_id == route.workspace_id)
        || tab_list_result()?
            .iter()
            .any(|tab| tab.tab_id == route.tab_id)
    {
        return Err("owned workspace/tab did not become an orphan".into());
    }
    sweep_preserving_channels(service, Vec::new()).await?;
    let after = session.conversations_info(&request).await?.channel;
    let same_name = after.name.as_deref() == Some(name.as_str());
    if after.flags.is_archived != Some(false) || !same_name {
        return Err(format!(
            "service sweep changed owned orphan: archived={:?} same_name={same_name}",
            after.flags.is_archived
        )
        .into());
    }
    println!("service sweep preserved orphan: unarchived=true same_name=true");
    Ok(())
}

#[tokio::test]
async fn slack_sweep_real_topology() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("SLACK_BOT_TOKEN")?;
    let slack = Slack::from_env().await?;
    let route = create_route()?;
    let result = async {
        let filtered = Slack::from_env()
            .await?
            .with_scope(ChatScope::Service)
            .with_ignore_test_runs(true);
        sweep_preserving_channels(&slack, vec![Ok(route.clone())]).await?;
        let found = slack
            .cached_route(&route)
            .await?
            .ok_or("startup did not create route")?;
        let created = herdr(&[
            "tab",
            "create",
            "--workspace",
            &route.workspace_id,
            "--label",
            "testrun-obsolete",
            "--cwd",
            "/tmp",
            "--no-focus",
        ])?;
        let tab = created["result"]["tab"]["tab_id"]
            .as_str()
            .ok_or("new tab has no id")?;
        let obsolete = TopologyRoute {
            tab_id: tab.into(),
            thread_name: format!("testrun-obsolete [{tab}]"),
            ..route.clone()
        };
        let old = slack.sync_route(&obsolete).await?;
        herdr(&["tab", "close", tab])?;
        herdr(&["tab", "rename", &route.tab_id, "testrun-renamed"])?;
        let restarted = Slack::from_env().await?;
        sweep_preserving_channels(&restarted, Vec::new()).await?;
        let mut checks = Vec::new();
        for (name, fresh) in [("cached route", false), ("restart recovery", true)] {
            checks.push((name, restarted.sync(&route, fresh).await? == found));
        }
        let tab_removed = !restarted
            .channels()
            .await?
            .iter()
            .any(|channel| channel.id == old.channel);
        preserve_service_orphan(&filtered, &route, &found.channel).await?;
        sweep_preserving_channels(&restarted, Vec::new()).await?;
        let client = SlackClient::new(SlackClientHyperConnector::new()?);
        let token = SlackApiToken::new(token.into());
        let archived = client
            .open_session(&token)
            .conversations_info(&SlackApiConversationsInfoRequest::new(found.channel))
            .await?
            .channel;

        let original_name = slack_channel_name(
            &route.channel_name,
            &route.workspace_id,
            "testrun-build",
            &route.tab_id,
        )?;
        checks.extend([
            ("startup archives closed tab channel", tab_removed),
            (
                "startup archives closed workspace",
                archived.flags.is_archived == Some(true),
            ),
            (
                "startup renames before archive",
                archived.name.as_deref() != Some(&original_name),
            ),
        ]);
        Ok::<_, Box<dyn std::error::Error>>(checks)
    }
    .await;
    let leftovers = cleanup(&slack, &route.channel_name, &route.workspace_id).await?;
    assert_eq!(
        leftovers, 0,
        "zero-leftover check: unarchived testrun- Slack channels"
    );
    for (name, passed) in result? {
        assert!(passed, "{name}");
    }
    Ok(())
}
