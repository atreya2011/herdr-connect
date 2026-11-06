use std::collections::HashSet;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use herdr_connect_rs::{
    Slack, SlackArchive, SlackSocketEvent, TopologyClosure, TopologyRoute, tab_list_result,
    workspace_list_result,
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
    let slack = Slack::new(token.clone(), "U0CEYEYSKT5".into(), "T0CF6JNT0KB").await?;
    let before = slack.channels().await?;
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
        pane_id: String::new(),
        channel_name: label.clone(),
        thread_name: format!("testrun-build [{tab}]"),
    };
    let result = exercise_topology(&slack, &route, &before, token).await;
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
    before: &[SlackChannelInfo],
    token: String,
) -> Result<Vec<(&'static str, bool)>, Box<dyn std::error::Error>> {
    let label = &route.channel_name;
    let workspace = route.workspace_id.as_str();
    let tab = route.tab_id.as_str();
    let found = slack.sync(route, false).await?;
    let mut checks = Vec::new();
    let channels = slack.channels().await?;
    checks.push((
        "channel uses workspace label and topic",
        channels.iter().any(|channel| {
            channel.id == found.channel
                && channel.name.as_deref() == Some(label.as_str())
                && channel
                    .topic
                    .as_ref()
                    .is_some_and(|topic| topic.value == format!("herdr workspace [{workspace}]"))
        }),
    ));
    checks.push((
        "parent text uses tab label and id",
        slack.history(&found.channel).await?.iter().any(|message| {
            Some(&message.origin.ts) == found.thread.as_ref()
                && message.content.text.as_deref() == Some(&route.thread_name)
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
        members.members.contains(&SlackUserId::from("U0CEYEYSKT5")),
    ));
    for (name, fresh) in [("cached route", false), ("restart recovery", true)] {
        checks.push((name, slack.sync(route, fresh).await? == found));
    }
    let live = (
        workspace_list_result()?
            .into_iter()
            .map(|entry| entry.workspace_id)
            .collect::<HashSet<_>>(),
        tab_list_result()?
            .into_iter()
            .map(|entry| entry.tab_id)
            .collect::<HashSet<_>>(),
    );
    slack.sweep(vec![Ok(route.clone())], live).await?;
    let after = slack.channels().await?;
    for channel in before {
        if !channel
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with("testrun-"))
            && !after
                .iter()
                .any(|entry| entry.id == channel.id && entry.flags.is_archived != Some(true))
        {
            return Err("non-test channel changed".into());
        }
    }
    slack
        .close(&TopologyClosure::Tab {
            workspace_id: workspace.into(),
            tab_id: tab.into(),
        })
        .await?;
    checks.push((
        "tab parent removed",
        slack
            .history(&found.channel)
            .await?
            .iter()
            .all(|message| Some(&message.origin.ts) != found.thread.as_ref()),
    ));
    slack
        .close(&TopologyClosure::Workspace {
            workspace_id: workspace.into(),
        })
        .await?;
    let archived = client
        .open_session(&api_token)
        .conversations_info(&SlackApiConversationsInfoRequest::new(found.channel))
        .await?
        .channel;
    checks.push((
        "workspace renamed before archive",
        archived.flags.is_archived == Some(true)
            && archived.name.as_deref() != Some(label.as_str()),
    ));
    Ok::<_, Box<dyn std::error::Error>>(checks)
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
    herdr(&["workspace", "close", workspace])?;
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
async fn slack_delivery_real_thread() -> Result<(), Box<dyn std::error::Error>> {
    let token = std::env::var("SLACK_BOT_TOKEN")?;
    let slack = Slack::new(token.clone(), "U0CEYEYSKT5".into(), "T0CF6JNT0KB").await?;
    let route = create_route()?;
    let result = async {
        let found = slack.sync(&route, false).await?;
        let reply = slack.send(&found, "testrun agent reply <literal>").await?;
        slack
            .mirror_prompt(&found, "testrun terminal prompt")
            .await?;
        let activity = slack.send(&found, "testrun activity").await?;
        slack
            .update(&found, &activity, "testrun activity updated")
            .await?;
        slack.set_working(&found, true).await?;
        slack.set_working(&found, false).await?;
        let client = SlackClient::new(SlackClientHyperConnector::new()?);
        let token = SlackApiToken::new(token.into());
        let replies = client
            .open_session(&token)
            .conversations_replies(&SlackApiConversationsRepliesRequest::new(
                found.channel.clone(),
                found.thread.clone().ok_or("no parent")?,
            ))
            .await?;
        let checks = vec![
            (
                "agent reply in thread",
                replies.messages.iter().any(|message| {
                    message.origin.ts == reply
                        && message.origin.thread_ts == found.thread
                        && message.content.text.as_deref()
                            == Some("testrun agent reply &lt;literal&gt;")
                }),
            ),
            (
                "terminal prompt from bot",
                replies.messages.iter().any(|message| {
                    message.sender.bot_id.is_some()
                        && message.content.text.as_deref()
                            == Some("Terminal prompt from <@U0CEYEYSKT5>:\ntestrun terminal prompt")
                }),
            ),
            (
                "activity updated in place",
                replies.messages.iter().any(|message| {
                    message.origin.ts == activity
                        && message.content.text.as_deref() == Some("testrun activity updated")
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
    let slack = Slack::new(
        std::env::var("SLACK_BOT_TOKEN")?,
        "U0CEYEYSKT5".into(),
        "T0CF6JNT0KB",
    )
    .await?;
    let route = create_route()?;
    let result = async {
        slack
            .connect_socket(std::env::var("SLACK_APP_TOKEN")?)
            .await?;
        let found = slack.sync(&route, false).await?;
        let sent = slack.send(&found, "testrun socket delivery").await?;
        let received = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(SlackSocketEvent::Push(event)) = slack.next_socket_event().await
                    && let SlackEventCallbackBody::Message(message) = event.event
                    && message.origin.ts == sent
                {
                    break message;
                }
            }
        })
        .await?;
        let ignored = slack.resolve_owner_message(received).await?.is_none();
        slack
            .close(&TopologyClosure::Workspace {
                workspace_id: route.workspace_id.clone(),
            })
            .await?;
        let archive = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(SlackSocketEvent::Push(event)) = slack.next_socket_event().await
                    && let SlackEventCallbackBody::ChannelArchive(archive) = event.event
                    && archive.channel == found.channel
                {
                    break archive;
                }
            }
        })
        .await?;
        let non_owner_archive = archive.user != SlackUserId::from("U0CEYEYSKT5");
        slack
            .close_archived(SlackArchive(archive), |_| {
                Err("bot archive must never close Herdr".into())
            })
            .await?;
        Ok::<_, Box<dyn std::error::Error>>(vec![
            ("bot message ignored as owner prompt", ignored),
            ("archive has actual non-owner actor", non_owner_archive),
            (
                "non-owner archive leaves Herdr alive",
                workspace_list_result()?
                    .iter()
                    .any(|entry| entry.workspace_id == route.workspace_id),
            ),
        ])
    }
    .await;
    slack.shutdown_socket().await;
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
