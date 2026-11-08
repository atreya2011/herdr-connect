use std::fs;
use std::io::Write as _;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use herdr_connect_rs::{
    AgentSnapshot, Chat, ChatError, Closed, Inbound, PermissionResponder, Slack, SlackRoute,
    handle_component, handle_owner_message, workspace_close, workspace_list_result,
};
use serde_json::{Value, json};
use serial_test::serial;
use slack_morphism::prelude::*;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio::task::{JoinHandle, JoinSet};

use super::{
    Follower, Workspace, activity_binary_path, herdr_json, matching_tab, passthrough_env_args,
    read_new_terminal_prompts, resolve_session_path, snapshot_for_pane, transcript_question_answer,
};
use crate::{
    BridgeState, ChatConnection, handle_activity_event, handle_live_event, process_snapshot,
    route_topology, tab_list_result,
};

const STEP_TIMEOUT: Duration = Duration::from_secs(20 * 60);
const PROMPT_ACTIVITY_PROMPT: &str = "Run exactly one Bash tool call: echo prompt. Use no other tools. Then reply with exactly PROMPT DONE.";

struct Owned {
    label: String,
    directory: PathBuf,
    chat: Arc<Slack>,
    workspace: Option<Workspace>,
    broker: Option<JoinHandle<std::io::Result<()>>>,
    reader: Option<JoinHandle<()>>,
}

enum Goal {
    Message,
    Working,
    WorkingCleared,
    Permission {
        command: String,
        behavior: &'static str,
        marker: PathBuf,
    },
    Question {
        text: String,
        expected: Vec<String>,
        tap: &'static str,
    },
    Archived,
    ReviewPrompt,
    ReviewAnswer(String),
    ReviewConfirmation(&'static str),
    MarkdownReply(Vec<SlackTs>),
    ActivityTurn,
    ActivityPosted,
    MainTurnEnded,
    BackgroundTurn,
    PromptTurn,
}

enum Exercise {
    Acceptance,
    Review,
    Markdown,
    Activity,
    BackgroundActivity,
    RestartActivity,
    PromptActivity,
}

struct Driver {
    connection: ChatConnection<Slack>,
    pane: String,
    workspace: String,
    tab: String,
    route: SlackRoute,
    snapshot: AgentSnapshot,
    state: BridgeState<Slack>,
    events: UnboundedReceiver<Result<Inbound<Slack>, ChatError>>,
    live: UnboundedReceiver<String>,
    activity: UnboundedReceiver<herdr_connect_rs::ActivityFrame>,
    activity_tx: UnboundedSender<herdr_connect_rs::ActivityFrame>,
    tasks: JoinSet<Result<(), String>>,
    clock: tokio::time::Interval,
    messages: Vec<SlackHistoryMessage>,
    owner_texts: Vec<String>,
    decoded_texts: Vec<(String, String)>,
    taps: Vec<String>,
    working_confirmed: bool,
    working_observed: bool,
    archived_by_owner: bool,
    activity_posted: Vec<SlackTs>,
    activity_snapshots: Vec<Value>,
    turn_ends: usize,
    goal: Goal,
    directory: PathBuf,
    team: String,
}

fn test_env(name: &str) -> Result<String, String> {
    std::env::var(name).map_err(|error| format!("{name}: {error}"))
}

fn action(text: &str) -> Result<(), String> {
    println!("OWNER ACTION: {text}");
    std::io::stdout().flush().map_err(|error| error.to_string())
}

fn hook_settings(directory: &Path) -> Result<PathBuf, String> {
    let script = directory.join("permission-hook.py");
    let binary =
        serde_json::to_string(&activity_binary_path()?).map_err(|error| error.to_string())?;
    let socket =
        serde_json::to_string(&directory.join("broker.sock")).map_err(|error| error.to_string())?;
    let records = directory.join("permission-responses");
    fs::create_dir(&records).map_err(|error| error.to_string())?;
    let records = serde_json::to_string(&records).map_err(|error| error.to_string())?;
    let source = format!(
        "import json,pathlib,subprocess,sys,time\npayload=sys.stdin.buffer.read()\nresult=subprocess.run([{binary},'hook','--vendor','claude','--socket',{socket}],input=payload,stdout=subprocess.PIPE,check=True)\nrecord={{'request':json.loads(payload),'response':json.loads(result.stdout)}}\ntarget=pathlib.Path({records},str(time.time_ns())+'.json')\nstaged=target.with_suffix('.tmp')\nstaged.write_text(json.dumps(record))\nstaged.replace(target)\nsys.stdout.buffer.write(result.stdout)\n"
    );
    fs::write(&script, source).map_err(|error| error.to_string())?;
    let command = format!(
        "python3 '{}'",
        script.to_string_lossy().replace('\'', "'\\''")
    );
    let quote = |path: &Path| format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"));
    let activity = format!(
        "{} activity --vendor claude --socket {}",
        quote(&activity_binary_path()?),
        quote(&directory.join("broker.sock"))
    );
    let settings = json!({
        "permissions": {"allow": ["Bash(python3 -c *)", "Bash(sleep *)", "Bash(echo *)"], "ask": ["Bash(touch *)"]},
        "hooks": {
            "PermissionRequest": [{"matcher": "Bash", "hooks": [{"type": "command", "command": command, "timeout": 1200}]}],
            "PreToolUse": [{"matcher": "", "hooks": [{"type": "command", "command": activity, "async": true}]}],
            "Stop": [{"hooks": [{"type": "command", "command": activity, "async": true}]}]
        }
    });
    let path = directory.join("settings.json");
    fs::write(
        &path,
        serde_json::to_vec(&settings).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    Ok(path)
}

impl Owned {
    fn create_workspace(&mut self) -> Result<(), String> {
        let cwd = self
            .directory
            .to_str()
            .ok_or("test directory is not UTF-8")?;
        let broker = format!("HERDR_CLAUDE_BROKER_SOCKET={cwd}/broker.sock");
        let mut args = vec![
            "workspace",
            "create",
            "--label",
            &self.label,
            "--cwd",
            cwd,
            "--no-focus",
            "--env",
            &broker,
        ];
        let inherited = passthrough_env_args();
        for value in &inherited {
            args.extend(["--env", value]);
        }
        let created = herdr_json(&args)?;
        let field = |pointer| {
            created
                .pointer(pointer)
                .and_then(Value::as_str)
                .map(str::to_owned)
                .ok_or_else(|| format!("Herdr creation missing {pointer}"))
        };
        self.workspace = Some(Workspace {
            id: field("/result/workspace/workspace_id")?,
            tab_id: field("/result/tab/tab_id")?,
            pane_id: field("/result/root_pane/pane_id")?,
        });
        let workspace = self.workspace.as_ref().ok_or("test workspace missing")?;
        herdr_json(&["tab", "rename", &workspace.tab_id, &self.label])?;
        herdr_json(&[
            "tab",
            "create",
            "--workspace",
            &workspace.id,
            "--label",
            "testrun-survivor",
            "--cwd",
            cwd,
            "--no-focus",
        ])?;
        Ok(())
    }

    async fn start(&mut self) -> Result<Driver, String> {
        let settings = hook_settings(&self.directory)?;
        let responder = Arc::new(PermissionResponder::new(self.chat.clone()));
        let (activity_tx, activity) = unbounded_channel();
        let socket = self.directory.join("broker.sock");
        let broker_responder = responder.clone();
        let broker_socket = socket.clone();
        let broker_activity_tx = activity_tx.clone();
        self.broker = Some(tokio::spawn(async move {
            herdr_connect_rs::run_broker(&broker_socket, broker_responder, broker_activity_tx).await
        }));
        tokio::time::timeout(Duration::from_secs(10), async {
            while !socket.exists() {
                if self.broker.as_ref().is_some_and(JoinHandle::is_finished) {
                    return Err("test broker stopped before readiness".to_owned());
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok(())
        })
        .await
        .map_err(|_| "test broker readiness timed out".to_owned())??;
        self.create_workspace()?;
        let workspace = self.workspace.as_ref().ok_or("test workspace missing")?;
        let pane = workspace.pane_id.clone();
        let name = format!("slack-owner-{}", std::process::id());
        tokio::task::spawn_blocking(move || start_agent(&name, &pane, &settings))
            .await
            .map_err(|error| error.to_string())??;
        let snapshot = snapshot_for_pane(&workspace.pane_id)?;
        if snapshot.session.is_none() {
            return Err("real Claude agent did not report a session".into());
        }
        self.chat
            .connect_socket(test_env("SLACK_TEST_APP_TOKEN")?)
            .await
            .map_err(String::from)?;
        let tab = matching_tab(&workspace.tab_id)?;
        let topology = route_topology(
            std::slice::from_ref(&snapshot),
            std::slice::from_ref(&tab),
            &snapshot.terminal_id,
        )?;
        let route = self
            .chat
            .sync_route(&topology)
            .await
            .map_err(String::from)?;
        let (sender, events) = unbounded_channel();
        let reader_chat = self.chat.clone();
        self.reader = Some(tokio::spawn(async move {
            while let Some(event) = reader_chat.next_event().await {
                if sender.send(event).is_err() {
                    break;
                }
            }
        }));
        let (live_tx, live) = unbounded_channel();
        Ok(Driver {
            connection: (
                self.chat.clone(),
                responder,
                self.chat.owner_identity().clone(),
            ),
            pane: workspace.pane_id.clone(),
            workspace: workspace.id.clone(),
            tab: workspace.tab_id.clone(),
            route,
            snapshot,
            state: BridgeState {
                live_tx: Some(live_tx),
                ..BridgeState::default()
            },
            events,
            live,
            activity,
            activity_tx,
            tasks: JoinSet::new(),
            clock: tokio::time::interval(Duration::from_secs(2)),
            messages: Vec::new(),
            owner_texts: Vec::new(),
            decoded_texts: Vec::new(),
            taps: Vec::new(),
            working_confirmed: false,
            working_observed: false,
            archived_by_owner: false,
            activity_posted: Vec::new(),
            activity_snapshots: Vec::new(),
            turn_ends: 0,
            goal: Goal::Message,
            directory: self.directory.clone(),
            team: self.chat.team_id().to_owned(),
        })
    }

    async fn cleanup(&mut self) -> Result<(), String> {
        if let Some(task) = self.reader.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = self.broker.take() {
            task.abort();
            let _ = task.await;
        }
        let mut errors = Vec::new();
        if let Err(error) = self.chat.shutdown_socket().await {
            errors.push(error.to_string());
        }
        if let Err(error) = self.cleanup_herdr() {
            errors.push(error);
        }
        if let Err(error) = self.cleanup_slack().await {
            errors.push(error);
        }
        if let Err(error) = fs::remove_dir_all(&self.directory) {
            errors.push(error.to_string());
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
    fn cleanup_herdr(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        for workspace in workspace_list_result()?.iter().filter(|workspace| {
            workspace.label == self.label
                || self
                    .workspace
                    .as_ref()
                    .is_some_and(|fixture| fixture.id == workspace.workspace_id)
        }) {
            if let Err(error) = workspace_close(&workspace.workspace_id) {
                errors.push(error);
            }
        }
        let remaining = workspace_list_result()?
            .iter()
            .filter(|workspace| {
                workspace.label == self.label
                    || self
                        .workspace
                        .as_ref()
                        .is_some_and(|fixture| fixture.id == workspace.workspace_id)
            })
            .count();
        println!("named zero-leftover check: {remaining} owned Herdr workspaces");
        if remaining != 0 {
            errors.push("owned Herdr workspace remains".into());
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    async fn cleanup_slack(&self) -> Result<(), String> {
        let mut errors = Vec::new();
        let client =
            SlackClient::new(SlackClientHyperConnector::new().map_err(|error| error.to_string())?);
        let token = SlackApiToken::new(test_env("SLACK_TEST_BOT_TOKEN")?.into());
        for channel in self
            .chat
            .channels()
            .await
            .map_err(String::from)?
            .iter()
            .filter(|channel| {
                channel.name.as_deref().is_some_and(|name| {
                    name.starts_with("testrun-")
                        && self.workspace.as_ref().is_some_and(|workspace| {
                            name.contains(&format!("-{}-", workspace.id.to_lowercase()))
                                || channel.topic.as_ref().is_some_and(|topic| {
                                    topic.value.starts_with(&format!(
                                        "herdr workspace [{}] tab [",
                                        workspace.id
                                    ))
                                })
                        })
                })
            })
        {
            if let Err(error) = client
                .open_session(&token)
                .conversations_archive(&SlackApiConversationsArchiveRequest::new(
                    channel.id.clone(),
                ))
                .await
            {
                errors.push(error.to_string());
            }
        }
        let remaining = self
            .chat
            .channels()
            .await
            .map_err(String::from)?
            .iter()
            .filter(|channel| {
                channel
                    .name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("testrun-"))
            })
            .count();
        println!("named zero-leftover check: {remaining} unarchived testrun- Slack channels");
        if remaining != 0 {
            errors.push("unarchived testrun- Slack channels remain".into());
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

fn start_agent(name: &str, pane: &str, settings: &Path) -> Result<(), String> {
    herdr_json(&[
        "pane",
        "wait-output",
        pane,
        "--match",
        "╰─",
        "--timeout",
        "15000",
    ])?;
    let settings = settings.to_str().ok_or("settings path is not UTF-8")?;
    herdr_json(&[
        "agent", "start", name, "--kind", "claude", "--pane", pane, "--timeout", "60000", "--",
        "--permission-mode", "manual", "--tools", "Bash,AskUserQuestion,Agent", "--settings", settings,
        "--append-system-prompt", "You are a real testrun Slack acceptance agent. Follow only the test prompts. Run only their exact commands in this temporary directory. Never inspect secrets or touch other workspaces, tabs, channels, or production data. Never add Co-Authored-By or tool attribution lines. Never commit or push. After a denied command, do not retry or choose another tool. Answer question tools exactly as requested.",
        "Reply with exactly READY and use no tools.",
    ]).map(drop)
}

impl Driver {
    async fn refresh(&mut self) -> Result<(), String> {
        if self.archived_by_owner {
            return Ok(());
        }
        self.snapshot = snapshot_for_pane(&self.pane)?;
        if self.snapshot.workspace_id != self.workspace || self.snapshot.tab_id != self.tab {
            return Err("test agent moved outside the owned workspace or tab".into());
        }
        let tab = matching_tab(&self.tab)?;
        process_snapshot(
            &self.snapshot,
            std::slice::from_ref(&self.snapshot),
            std::slice::from_ref(&tab),
            &self.connection,
            &mut self.state,
        )
        .await;
        self.working_observed |= self.snapshot.agent_status == "working";
        handle_live_event(
            &self.connection,
            &self.snapshot.terminal_id,
            &mut self.state,
        )
        .await;
        self.messages = self
            .connection
            .0
            .history(&self.route.channel)
            .await
            .map_err(String::from)?;
        Ok(())
    }

    async fn owner_posted(&mut self, posted: &SlackMessageEvent) -> Result<bool, String> {
        if posted
            .sender
            .user
            .as_ref()
            .is_none_or(|user| user.0 != self.connection.0.owner_id())
            || posted.sender.bot_id.is_some()
            || posted.subtype.is_some()
        {
            return Ok(false);
        }
        let text = posted
            .content
            .as_ref()
            .and_then(|content| content.text.clone())
            .ok_or("owner message has no text")?;
        self.owner_texts.push(text.clone());
        if let Some(message) = self
            .connection
            .0
            .owner_message(posted.clone())
            .await
            .map_err(String::from)?
        {
            self.decoded_texts.push((text.clone(), message.content));
        }
        if let Goal::ReviewConfirmation(expected) = self.goal
            && text.trim() == expected
        {
            return Ok(true);
        }
        if matches!(self.goal, Goal::Working) {
            match text.trim() {
                "STATUS VISIBLE" => {
                    self.working_confirmed = true;
                    return Ok(true);
                }
                "STATUS NOT VISIBLE" => {
                    return Err(
                        "owner reports working status is not visible in the channel view".into(),
                    );
                }
                "PROFILE CORRECT" => {
                    println!("OWNER OBSERVED: terminal prompt {}", text.trim());
                    return Ok(true);
                }
                "PROFILE INCORRECT" => {
                    return Err("owner reports terminal prompt name or avatar differs".into());
                }
                _ => {}
            }
        }
        if matches!(self.goal, Goal::WorkingCleared) {
            match text.trim() {
                "STATUS CLEARED" => {
                    println!("OWNER OBSERVED: after settling, {}", text.trim());
                    return Ok(true);
                }
                "STATUS STILL VISIBLE" => {
                    return Err("owner reports working status remains after settling".into());
                }
                _ => {}
            }
        }
        Ok(false)
    }

    async fn inbound(&mut self, event: Inbound<Slack>) -> Result<(), String> {
        match event {
            Inbound::Posted(posted)
                if posted.origin.channel.as_ref() == Some(&self.route.channel) =>
            {
                if self.owner_posted(&posted).await? {
                    return Ok(());
                }
                let responder = self.connection.1.clone();
                let activity = self.activity_tx.clone();
                self.tasks.spawn(async move {
                    handle_owner_message(&responder, posted, &activity).await
                });
            }
            Inbound::Component(component)
                if component.route == self.route && component.authorized =>
            {
                self.taps.push(component.custom_id.clone());
                let responder = self.connection.1.clone();
                self.tasks.spawn(async move {
                    handle_component(responder, component).await;
                    Ok(())
                });
            }
            Inbound::Deleted(archive) if archive.0.channel == self.route.channel => {
                let owner = archive.0.user.0 == self.connection.0.owner_id();
                let expected_tab = self.tab.clone();
                self.connection
                    .0
                    .handle_deletion(archive, move |closed| match closed {
                        Closed::Tab(id) if id == expected_tab => {
                            herdr_connect_rs::tab_close(&id).map(drop)
                        }
                        _ => Err("refused Herdr close outside the owned workspace".into()),
                    })
                    .await
                    .map_err(String::from)?;
                self.archived_by_owner |= owner;
            }
            _ => {}
        }
        Ok(())
    }

    async fn pump(&mut self) -> Result<(), String> {
        tokio::select! {
            event = self.events.recv() => {
                Box::pin(self.inbound(event.ok_or("Slack event reader stopped")?.map_err(String::from)?)).await?;
            }
            _ = self.clock.tick() => self.refresh().await?,
            Some(terminal) = self.live.recv() => {
                if !self.archived_by_owner { handle_live_event(&self.connection, &terminal, &mut self.state).await; }
            }
            Some(frame) = self.activity.recv() => {
                let turn_end = frame.turn_end && frame.pane_id == self.pane;
                let tool_call = !frame.turn_end && frame.pane_id == self.pane && !self.archived_by_owner;
                if frame.pane_id == self.pane && !self.archived_by_owner { handle_activity_event(&self.connection, frame, &mut self.state).await; }
                if let Some(activity) = self.state.activity_messages.get(&self.pane)
                    && self.activity_posted.last() != Some(&activity.message)
                {
                    self.activity_posted.push(activity.message.clone());
                }
                if tool_call
                    && let Some(message) = self.state.activity_messages.get(&self.pane).map(|activity| activity.message.clone())
                {
                    let history = self.connection.0.history(&self.route.channel).await.map_err(String::from)?;
                    if let Some(activity) = history.iter().find(|entry| entry.origin.ts == message) {
                        let blocks = serde_json::to_value(&activity.content.blocks).map_err(|error| error.to_string())?;
                        let snapshot = json!({"text": activity.content.text, "blocks": blocks});
                        let tasks = snapshot
                            .pointer("/blocks/0/tasks")
                            .and_then(Value::as_array)
                            .map(|tasks| {
                                tasks
                                    .iter()
                                    .filter_map(|task| {
                                        Some(format!(
                                            "{}={}",
                                            task["title"].as_str()?,
                                            task["status"].as_str()?
                                        ))
                                    })
                                    .collect::<Vec<_>>()
                            })
                            .unwrap_or_default();
                        let title = snapshot.pointer("/blocks/0/title");
                        self.activity_snapshots.push(snapshot.clone());
                        println!("OWNER OBSERVED: activity plan title {title:?}, tasks {tasks:?}, text {:?}", snapshot["text"]);
                    }
                }
                self.turn_ends += usize::from(turn_end);
            }
            Some(result) = self.tasks.join_next() => result.map_err(|error| error.to_string())??,
        }
        Ok(())
    }

    fn settled(&self) -> Result<bool, String> {
        Ok(matches!(
            snapshot_for_pane(&self.pane)?.agent_status.as_str(),
            "idle" | "done"
        ))
    }

    fn bot_said(&self, text: &str) -> bool {
        self.messages.iter().any(|message| {
            message.sender.bot_id.is_some()
                && message
                    .content
                    .text
                    .as_deref()
                    .is_some_and(|content| content.trim() == text)
        })
    }

    fn permission_observed(&self, command: &str, behavior: &str) -> Result<bool, String> {
        for path in fs::read_dir(self.directory.join("permission-responses"))
            .map_err(|error| error.to_string())?
        {
            let path = path.map_err(|error| error.to_string())?.path();
            if path.extension().is_none_or(|extension| extension != "json") {
                continue;
            }
            let record: Value =
                serde_json::from_slice(&fs::read(path).map_err(|error| error.to_string())?)
                    .map_err(|error| error.to_string())?;
            if record
                .pointer("/request/tool_input/command")
                .and_then(Value::as_str)
                == Some(command)
                && record
                    .pointer("/response/hookSpecificOutput/decision/behavior")
                    .and_then(Value::as_str)
                    == Some(behavior)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn observed(&self) -> Result<bool, String> {
        match &self.goal {
            Goal::Message => {
                Ok(self.owner_texts.iter().any(|text| {
                    text.trim() == "Reply with exactly SLACK OWNER OK and use no tools."
                }) && self.bot_said("SLACK OWNER OK")
                    && self.settled()?)
            }
            Goal::Working => Ok(self.working_observed
                && self.working_confirmed
                && self
                    .owner_texts
                    .iter()
                    .any(|text| text.trim() == "PROFILE CORRECT")
                && self.bot_said("WORKING DONE")
                && self.settled()?),
            Goal::WorkingCleared => Ok(self
                .owner_texts
                .iter()
                .any(|text| text.trim() == "STATUS CLEARED")),
            Goal::Permission {
                command,
                behavior,
                marker,
            } => Ok(self
                .taps
                .iter()
                .any(|tap| tap.starts_with(&format!("herdr:{behavior}:")))
                && self.permission_observed(command, behavior)?
                && marker.exists() == (*behavior == "allow")
                && self.settled()?),
            Goal::Question {
                text,
                expected,
                tap,
            } => {
                let home = std::env::var("HOME").map_err(|error| error.to_string())?;
                let answer = transcript_question_answer(Path::new(&home), &self.snapshot, text)?;
                let expected_text = expected.join(", ");
                let answered = answer.as_deref() == Some(expected_text.as_str());
                let input = if tap.is_empty() {
                    self.owner_texts.iter().any(|value| value == &expected[0])
                } else {
                    self.taps.iter().any(|value| value.starts_with(tap))
                };
                Ok(answered && input && self.settled()?)
            }
            Goal::ReviewPrompt => self.review_prompt_observed(),
            Goal::ReviewAnswer(question) => self.review_answer_observed(question),
            Goal::ReviewConfirmation(expected) => {
                Ok(self.owner_texts.iter().any(|text| text.trim() == *expected))
            }
            Goal::MarkdownReply(before) => {
                Ok(self.agent_messages(before).next().is_some() && self.settled()?)
            }
            Goal::ActivityTurn => Ok(self.activity_snapshots.len() >= 2
                && !self.activity_posted.is_empty()
                && self.bot_said("ACTIVITY DONE")
                && self.settled()?),
            Goal::ActivityPosted => Ok(!self.activity_posted.is_empty()),
            Goal::PromptTurn => Ok(self.activity_snapshots.len() >= 2
                && self.bot_said("PROMPT DONE")
                && self.settled()?),
            Goal::MainTurnEnded => Ok(!self.activity_posted.is_empty()
                && self.turn_ends > 0
                && self.bot_said("STARTED")),
            Goal::BackgroundTurn => Ok(self.activity_posted.len() > 1
                && self.bot_said("BACKGROUND DONE")
                && self.settled()?),
            Goal::Archived => Ok(self.archived_by_owner
                && tab_list_result()?.iter().all(|tab| tab.tab_id != self.tab)
                && workspace_list_result()?
                    .iter()
                    .any(|workspace| workspace.workspace_id == self.workspace)),
        }
    }

    fn agent_messages<'a>(
        &'a self,
        before: &'a [SlackTs],
    ) -> impl Iterator<Item = &'a SlackHistoryMessage> + 'a {
        self.messages.iter().filter(move |message| {
            message.sender.bot_id.is_some()
                && message.sender.username.as_deref()
                    != Some(self.connection.0.owner_identity().display_name.as_str())
                && !before.contains(&message.origin.ts)
        })
    }

    async fn markdown(&mut self) -> Result<(), String> {
        self.ready().await?;
        let before: Vec<SlackTs> = self
            .messages
            .iter()
            .map(|message| message.origin.ts.clone())
            .collect();
        self.prompt("Reply with a short Markdown status note about a fictional build. Use bold text, a bulleted list, and a fenced code block. Mention the comparison a < b && c > d once in plain text. Use no tools.")?;
        self.wait(Goal::MarkdownReply(before.clone())).await?;
        let reply = self
            .agent_messages(&before)
            .next()
            .ok_or("real agent reply missing from Slack")?;
        let prompt = self
            .messages
            .iter()
            .find(|message| {
                message.sender.username.as_deref()
                    == Some(self.connection.0.owner_identity().display_name.as_str())
                    && !before.contains(&message.origin.ts)
            })
            .ok_or("mirrored terminal prompt missing from Slack")?;
        let blocks =
            serde_json::to_value(&reply.content.blocks).map_err(|error| error.to_string())?;
        let prompt_blocks =
            serde_json::to_value(&prompt.content.blocks).map_err(|error| error.to_string())?;
        let nodes = rich_nodes(&blocks);
        let rendered: String = nodes
            .iter()
            .filter(|node| node["type"] == "text")
            .filter_map(|node| node["text"].as_str())
            .collect();
        let fallback = reply.content.text.as_deref().unwrap_or_default();
        let checks = [
            (
                "reply stored as rich text",
                blocks[0]["type"] == "rich_text",
            ),
            (
                "bold rendered",
                nodes.iter().any(|node| node["style"]["bold"] == true),
            ),
            (
                "bulleted list rendered",
                nodes
                    .iter()
                    .any(|node| node["type"] == "rich_text_list" && node["style"] == "bullet"),
            ),
            (
                "code block rendered",
                nodes
                    .iter()
                    .any(|node| node["type"] == "rich_text_preformatted"),
            ),
            ("no literal bold markers", !rendered.contains("**")),
            (
                "entities decoded in rendered text",
                rendered.contains('<')
                    && rendered.contains('&')
                    && !["&lt;", "&gt;", "&amp;"]
                        .iter()
                        .any(|entity| rendered.contains(entity)),
            ),
            (
                "notification fallback keeps escaped text",
                fallback.contains("**") && fallback.contains("&lt;"),
            ),
            (
                "mirrored prompt stored as rich text",
                prompt_blocks[0]["type"] == "rich_text",
            ),
        ];
        for (case, observed) in checks {
            if !observed {
                return Err(format!(
                    "real Slack Markdown observation failed: {case}; text={fallback:?}; blocks={blocks}; prompt={prompt_blocks}"
                ));
            }
        }
        println!("OWNER OBSERVED: real agent Markdown reply stored as Slack rich text");
        Ok(())
    }

    async fn activity_deleted(&mut self) -> Result<(), String> {
        self.ready().await?;
        self.prompt("Make exactly two separate Bash tool calls, in this order: run echo first, then run echo second. Use no other tools. Then reply with exactly ACTIVITY DONE.")?;
        self.wait(Goal::ActivityTurn).await?;
        if self.activity_snapshots.len() != 2 {
            return Err(format!(
                "expected two activity plan snapshots, received {}",
                self.activity_snapshots.len()
            ));
        }
        let first = &self.activity_snapshots[0];
        let second = &self.activity_snapshots[1];
        let first_plan = &first["blocks"][0];
        let second_plan = &second["blocks"][0];
        let first_text = first["text"].as_str();
        let second_text = second["text"].as_str();
        let first_expected = herdr_connect_rs::activity_message_text(1, "Bash", "echo first");
        let second_expected = herdr_connect_rs::activity_message_text(2, "Bash", "echo second");
        let first_shortcode = ":gear: 1 · Bash: echo first";
        let second_shortcode = ":gear: 2 · Bash: echo second";
        let checks = [
            (
                "first update has one in-progress task and the one-call title",
                first_plan["title"] == "1 tool call"
                    && first_plan["tasks"].as_array().is_some_and(|tasks| {
                        tasks.len() == 1
                            && tasks[0]["title"] == "Bash: echo first"
                            && tasks[0]["status"] == "in_progress"
                    }),
            ),
            (
                "second update completes the first task and marks the second in progress",
                second_plan["title"] == "2 tool calls"
                    && second_plan["tasks"].as_array().is_some_and(|tasks| {
                        tasks.len() == 2
                            && tasks[0]["title"] == "Bash: echo first"
                            && tasks[0]["status"] == "complete"
                            && tasks[1]["title"] == "Bash: echo second"
                            && tasks[1]["status"] == "in_progress"
                    }),
            ),
            (
                "Slack receives a fresh plan block id on update",
                first_plan["block_id"].as_str().is_some_and(|block_id| {
                    second_plan["block_id"]
                        .as_str()
                        .is_some_and(|updated| block_id != updated)
                }),
            ),
            (
                "text fields preserve the activity notification and cleanup format",
                first_text.is_some_and(|text| text == first_expected || text == first_shortcode)
                    && second_text
                        .is_some_and(|text| text == second_expected || text == second_shortcode)
                    && first_text.is_some_and(herdr_connect_rs::is_activity_message_text)
                    && second_text.is_some_and(herdr_connect_rs::is_activity_message_text),
            ),
        ];
        for (case, observed) in checks {
            if !observed {
                return Err(format!(
                    "real Slack activity plan observation failed: {case}; first={first}; second={second}"
                ));
            }
        }
        println!(
            "OWNER OBSERVED: plan block showed two tool calls with complete and in-progress task states"
        );
        let posted = self
            .activity_posted
            .last()
            .cloned()
            .ok_or("no activity message was posted during the turn")?;
        if self
            .messages
            .iter()
            .any(|message| message.origin.ts == posted)
        {
            return Err(format!(
                "activity message {posted} remains in the channel after the turn ended"
            ));
        }
        println!("OWNER OBSERVED: activity message deleted after the turn ended");
        Ok(())
    }

    async fn prompt_activity(&mut self) -> Result<(), String> {
        let channel_url = self.ready().await?;
        action(&format!(
            "prompt activity: open {channel_url} and post exactly: {PROMPT_ACTIVITY_PROMPT}"
        ))?;
        self.wait(Goal::PromptTurn).await?;
        action(&format!(
            "prompt reply: in {channel_url}, post NO PROMPT EPHEMERAL if no ephemeral accepted: prompt submitted reply appeared for your prompt."
        ))?;
        self.wait(Goal::ReviewConfirmation("NO PROMPT EPHEMERAL"))
            .await?;
        if self.activity_snapshots.len() != 2 {
            return Err(format!(
                "expected two activity plan snapshots, received {}",
                self.activity_snapshots.len()
            ));
        }
        let first = &self.activity_snapshots[0];
        let second = &self.activity_snapshots[1];
        let first_plan = &first["blocks"][0];
        let second_plan = &second["blocks"][0];
        let prompt_text = herdr_connect_rs::prompt_activity_text();
        let checks = [
            (
                "accepted prompt posts a plan block whose only task is Prompt submitted",
                first_plan["title"] == "Prompt submitted"
                    && first_plan["tasks"].as_array().is_some_and(|tasks| {
                        tasks.len() == 1
                            && tasks[0]["title"] == "Prompt submitted"
                            && tasks[0]["status"] == "in_progress"
                    })
                    && first["text"].as_str().is_some_and(|text| {
                        (text == prompt_text || text == ":gear: Prompt submitted")
                            && herdr_connect_rs::is_activity_message_text(text)
                    }),
            ),
            (
                "next tool call completes the prompt task and adds an in-progress tool task",
                second_plan["title"] == "1 tool call"
                    && second_plan["tasks"].as_array().is_some_and(|tasks| {
                        tasks.len() == 2
                            && tasks[0]["title"] == "Prompt submitted"
                            && tasks[0]["status"] == "complete"
                            && tasks[1]["title"] == "Bash: echo prompt"
                            && tasks[1]["status"] == "in_progress"
                    }),
            ),
            (
                "next tool call edits the same activity message",
                self.activity_posted.len() == 1,
            ),
        ];
        for (case, observed) in checks {
            if !observed {
                return Err(format!(
                    "real Slack prompt activity observation failed: {case}; first={first}; second={second}; posted={:?}",
                    self.activity_posted
                ));
            }
        }
        println!(
            "OWNER OBSERVED: accepted prompt showed in the plan block without an ephemeral reply, and the tool call edited that message"
        );
        Ok(())
    }

    fn activity_remaining(&self) -> Vec<&SlackTs> {
        self.activity_posted
            .iter()
            .filter(|posted| {
                self.messages
                    .iter()
                    .any(|message| &message.origin.ts == *posted)
            })
            .collect()
    }

    async fn background_activity_deleted(&mut self) -> Result<(), String> {
        self.ready().await?;
        self.prompt("Use the Agent tool with run_in_background set to true and subagent_type general-purpose to start one background agent with this exact task: 'Run these Bash commands one at a time, in order: sleep 10, then echo one, then sleep 10, then echo two. Then reply DONE.' Immediately after launching it, reply exactly STARTED and end your turn without waiting for it. When you are later told that the background agent finished, reply exactly BACKGROUND DONE and use no tools.")?;
        self.wait(Goal::MainTurnEnded).await?;
        let status = snapshot_for_pane(&self.pane)?.agent_status;
        if status != "working" {
            return Err(format!(
                "pane status was {status} at the main turn end, so background work did not keep it working"
            ));
        }
        let remaining = self.activity_remaining();
        if !remaining.is_empty() {
            return Err(format!(
                "activity messages {remaining:?} remain after the main turn ended"
            ));
        }
        println!(
            "OWNER OBSERVED: activity messages {:?} deleted at the main turn end while background work kept the pane working",
            self.activity_posted
        );
        let main_turn = self.activity_posted.len();
        self.wait(Goal::BackgroundTurn).await?;
        let remaining = self.activity_remaining();
        if !remaining.is_empty() {
            return Err(format!(
                "background activity messages {remaining:?} remain after the background work ended"
            ));
        }
        println!(
            "OWNER OBSERVED: background activity messages {:?} deleted after the background work ended",
            &self.activity_posted[main_turn..]
        );
        Ok(())
    }

    async fn restart_activity_deleted(&mut self) -> Result<(), String> {
        self.ready().await?;
        self.prompt("Use Bash to run exactly python3 -c 'import time; time.sleep(30)' in the foreground. Use no other tools. Then reply with exactly RESTART DONE.")?;
        self.wait(Goal::ActivityPosted).await?;
        let posted = self
            .activity_posted
            .last()
            .cloned()
            .ok_or("no activity message was posted during the turn")?;
        let restarted = Slack::new(
            test_env("SLACK_TEST_BOT_TOKEN")?,
            test_env("SLACK_TEST_OWNER_ID")?,
        )
        .await
        .map_err(String::from)?;
        restarted
            .delete_stale_activity(&self.route)
            .await
            .map_err(String::from)?;
        let history = restarted
            .history(&self.route.channel)
            .await
            .map_err(String::from)?;
        if history.iter().any(|message| message.origin.ts == posted) {
            return Err(format!(
                "activity message {posted} posted before the restart remains after the restarted bridge cleaned the channel"
            ));
        }
        println!(
            "OWNER OBSERVED: activity message {posted} posted before the restart deleted by the restarted bridge"
        );
        Ok(())
    }

    fn decoded_review_text(&self, prefix: &str, rich: bool) -> Result<Option<&str>, String> {
        let Some((raw, decoded)) = self
            .decoded_texts
            .iter()
            .find(|(_, decoded)| decoded.starts_with(prefix))
        else {
            return Ok(None);
        };
        let mut checks = vec![
            ("encoded ampersand", raw.contains("&amp;")),
            ("encoded less-than", raw.contains("&lt;")),
            ("encoded greater-than", raw.contains("&gt;")),
            ("decoded entities", decoded.contains("A & B < C > D")),
            (
                "no raw entities or link markup",
                !["&amp;", "&lt;", "&gt;", "<https://", "<@", "<#"]
                    .iter()
                    .any(|value| decoded.contains(value)),
            ),
        ];
        if rich {
            checks.extend([
                (
                    "plain URL delivered",
                    raw.contains("<https://example.com") && decoded.contains("https://example.com"),
                ),
                (
                    "labeled URL decoded",
                    raw.contains("|Example>") && decoded.contains("Example (https://example.org"),
                ),
                (
                    "owner mention decoded",
                    raw.contains(&format!("<@{}>", self.connection.0.owner_id()))
                        && decoded.contains(&format!("@{}", self.connection.0.owner_id())),
                ),
                (
                    "channel mention decoded",
                    raw.contains(&format!("<#{}", self.route.channel)) && decoded.contains('#'),
                ),
            ]);
        }
        for (case, observed) in checks {
            if !observed {
                return Err(format!(
                    "real Slack decoding observation failed: {case}; raw={raw:?}; decoded={decoded:?}"
                ));
            }
        }
        Ok(Some(decoded))
    }

    fn review_prompt_observed(&self) -> Result<bool, String> {
        if !self.bot_said("DECODE PROMPT OK") || !self.settled()? {
            return Ok(false);
        }
        let decoded = self
            .decoded_review_text("Reply with exactly DECODE PROMPT OK", true)?
            .ok_or("agent replied without the genuine owner encoding message")?;
        let home = std::env::var("HOME").map_err(|error| error.to_string())?;
        let session = self
            .snapshot
            .session
            .as_ref()
            .ok_or("agent has no reported session")?;
        let path = resolve_session_path(Path::new(&home), &self.snapshot, session)
            .map_err(|error| error.to_string())?;
        let (prompts, _) = read_new_terminal_prompts(&path, Follower::Claude(0))?;
        if !prompts.iter().any(|prompt| prompt == decoded) {
            return Err(
                "native Claude transcript does not contain the decoded owner prompt".into(),
            );
        }
        println!(
            "OWNER OBSERVED: native prompt contains decoded entities, plain and labeled URLs, owner and channel mentions"
        );
        Ok(true)
    }

    fn review_answer_observed(&self, question: &str) -> Result<bool, String> {
        if !self.bot_said("DECODE ANSWER OK") || !self.settled()? {
            return Ok(false);
        }
        let decoded = self
            .decoded_review_text("FREE ANSWER A & B", false)?
            .ok_or("question completed without the genuine encoded owner answer")?;
        let home = std::env::var("HOME").map_err(|error| error.to_string())?;
        if transcript_question_answer(Path::new(&home), &self.snapshot, question)?.as_deref()
            != Some(decoded)
        {
            return Err(
                "native Claude question answer differs from the decoded owner answer".into(),
            );
        }
        println!("OWNER OBSERVED: native free-text answer contains decoded entities");
        Ok(true)
    }

    async fn wait(&mut self, goal: Goal) -> Result<(), String> {
        self.goal = goal;
        let deadline = tokio::time::Instant::now() + STEP_TIMEOUT;
        loop {
            if self.observed()? {
                if !matches!(self.goal, Goal::Archived) {
                    self.refresh().await?;
                }
                println!("OWNER OBSERVED: step complete");
                return Ok(());
            }
            tokio::time::timeout_at(deadline, Box::pin(self.pump()))
                .await
                .map_err(|_| "owner action was not observed within 20 minutes".to_owned())??;
        }
    }

    fn prompt(&self, prompt: &str) -> Result<(), String> {
        herdr_json(&["agent", "prompt", &self.pane, prompt]).map(drop)
    }

    async fn questions(&mut self, channel_url: &str) -> Result<(), String> {
        let cases = [
            (
                "single choice",
                false,
                vec!["Blue".to_owned()],
                "herdrask:",
                "click Blue",
            ),
            (
                "multiple choice",
                true,
                vec!["Cheese".to_owned(), "Olives".to_owned()],
                "herdrask-multi:",
                "select Cheese and Olives, then click Submit choices",
            ),
            (
                "free text",
                false,
                vec![format!("OWNER FREE TEXT {}", self.tab)],
                "",
                "reply with the exact free text shown below",
            ),
        ];
        for (name, multi, expected, tap, instruction) in cases {
            let question = format!("Slack owner {name} {}?", self.tab);
            let options = if multi {
                json!([{"label":"Cheese","description":"Add cheese"},{"label":"Olives","description":"Add olives"},{"label":"Mushrooms","description":"Add mushrooms"}])
            } else {
                json!([{"label":"Red","description":"The color red"},{"label":"Blue","description":"The color blue"}])
            };
            action(&format!(
                "{name}: in {channel_url}, {instruction}; expected answer: {}. Wait for the next real question card.",
                expected.join(", ")
            ))?;
            self.prompt(&format!("Use AskUserQuestion now with exactly one question: {}. After its answer, reply only ANSWER RECEIVED. Use no other tools.", json!({"header":"Slack test","question":question,"multiSelect":multi,"options":options})))?;
            self.wait(Goal::Question {
                text: question,
                expected,
                tap,
            })
            .await?;
        }
        Ok(())
    }

    async fn ready(&mut self) -> Result<String, String> {
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        while !self.settled()? {
            tokio::time::timeout_at(ready_deadline, Box::pin(self.pump()))
                .await
                .map_err(|_| "real agent bootstrap did not settle".to_owned())??;
        }
        self.refresh().await?;
        Ok(format!(
            "https://app.slack.com/client/{}/{}",
            self.team, self.route.channel
        ))
    }

    async fn exercise(&mut self) -> Result<(), String> {
        let channel_url = self.ready().await?;
        action(&format!(
            "message: open {channel_url} and reply exactly: Reply with exactly SLACK OWNER OK and use no tools."
        ))?;
        self.wait(Goal::Message).await?;
        self.working_observed = false;
        self.goal = Goal::Working;
        action(&format!(
            "working status and terminal prompt profile: in {channel_url}, inspect the channel view during the next three-minute task without opening a thread. Reply STATUS VISIBLE if the working indicator is visible there, or STATUS NOT VISIBLE if it is absent. Inspect the mirrored terminal prompt for this task and reply PROFILE CORRECT if it uses your display name and avatar, or PROFILE INCORRECT if either differs. Wait for WORKING DONE before the next step."
        ))?;
        self.prompt("Use Bash to run exactly python3 -c 'import time; time.sleep(180)' in the foreground with timeout 240000. Do not use background execution or any other command. Then reply with exactly WORKING DONE.")?;
        self.wait(Goal::Working).await?;
        action(&format!(
            "working status cleared: after WORKING DONE, inspect the channel view in {channel_url} without opening a thread. Reply STATUS CLEARED if the working indicator is gone, or STATUS STILL VISIBLE if it remains."
        ))?;
        self.wait(Goal::WorkingCleared).await?;
        for (button, behavior, filename) in [
            ("Deny", "deny", "testrun-denied"),
            ("Allow", "allow", "testrun-allowed"),
        ] {
            let marker = self.directory.join(filename);
            let command = format!(
                "touch '{}'",
                marker.to_string_lossy().replace('\'', "'\\''")
            );
            action(&format!(
                "permission {button}: in {channel_url}, click {button} on the next real permission card within its normal 45-second lifetime. Do not answer in the terminal."
            ))?;
            self.prompt(&format!("Use Bash exactly once to run {command}. Set description to Create the test marker. Do not retry a denied command or use another tool. After the tool result, reply only PERMISSION FINISHED."))?;
            self.wait(Goal::Permission {
                command,
                behavior,
                marker,
            })
            .await?;
        }
        self.questions(&channel_url).await?;
        action(&format!(
            "channel archive: last, open {channel_url} and archive only this test tab channel. The test will verify that tab {} closes and its workspace remains open.",
            self.tab
        ))?;
        self.wait(Goal::Archived).await
    }

    async fn ask_review_question(&mut self, multi: bool) -> Result<String, String> {
        let question = format!(
            "Slack decoding {} {}?",
            if multi { "toppings" } else { "free text" },
            self.tab
        );
        let options = if multi {
            json!([{"label":"Cheese","description":"Add cheese"},{"label":"Olives","description":"Add olives"},{"label":"Mushrooms","description":"Add mushrooms"}])
        } else {
            json!([{"label":"Red","description":"The color red"},{"label":"Blue","description":"The color blue"}])
        };
        self.prompt(&format!("Use AskUserQuestion now with exactly one question: {}. After its answer, reply only {}. Use no other tools.", json!({"header":"Slack review","question":question,"multiSelect":multi,"options":options}), if multi { "CHOICES RECEIVED" } else { "DECODE ANSWER OK" }))?;
        let deadline = tokio::time::Instant::now() + STEP_TIMEOUT;
        while self
            .connection
            .1
            .pending_question_token(&self.route)
            .is_none()
        {
            tokio::time::timeout_at(deadline, Box::pin(self.pump()))
                .await
                .map_err(|_| {
                    "real review question did not appear within 20 minutes".to_owned()
                })??;
        }
        Ok(question)
    }

    fn review_question_open(&self, token: &str, case: &str) -> Result<(), String> {
        let pending = self.connection.1.pending_question_token(&self.route);
        let status = snapshot_for_pane(&self.pane)?.agent_status;
        if pending.as_deref() != Some(token) || status != "blocked" {
            return Err(format!(
                "{case}: native question left blocked or its pending card changed; status={status} pending={pending:?}"
            ));
        }
        println!("OWNER OBSERVED: {case}: same pending question and native agent remains blocked");
        Ok(())
    }

    async fn review(&mut self) -> Result<(), String> {
        let channel_url = self.ready().await?;
        action(&format!(
            "encoding prompt: open {channel_url}. Type: Reply with exactly DECODE PROMPT OK and use no tools. Encoding probe: A & B < C > D https://example.com ; then add a rich link labeled Example to https://example.org using the Slack link editor, mention yourself using @, and mention this channel using #. Send as one message."
        ))?;
        self.wait(Goal::ReviewPrompt).await?;
        let question = self.ask_review_question(false).await?;
        action(&format!(
            "free-text answer: in {channel_url}, reply exactly FREE ANSWER A & B < C > D to the real Slack review question card."
        ))?;
        self.wait(Goal::ReviewAnswer(question)).await?;
        let question = self.ask_review_question(true).await?;
        let token = self
            .connection
            .1
            .pending_question_token(&self.route)
            .ok_or("review question has no pending token")?;
        action(&format!(
            "empty selection: in {channel_url}, click Submit choices without selecting anything. After seeing the ephemeral refused: choose at least one option, post EMPTY REFUSAL VISIBLE. Leave the question unanswered."
        ))?;
        self.wait(Goal::ReviewConfirmation("EMPTY REFUSAL VISIBLE"))
            .await?;
        self.review_question_open(&token, "empty submission refused")?;
        action(&format!(
            "selection only: in {channel_url}, select Cheese and Olives, do not click Submit choices, then post SELECTION ONLY."
        ))?;
        self.wait(Goal::ReviewConfirmation("SELECTION ONLY"))
            .await?;
        self.review_question_open(&token, "selector change ignored")?;
        action(&format!(
            "submit selected choices: in {channel_url}, click Submit choices once. The test will verify the native answer is Cheese, Olives, then clean up only its own resources."
        ))?;
        self.wait(Goal::Question {
            text: question,
            expected: vec!["Cheese".into(), "Olives".into()],
            tap: "herdrask-multi:",
        })
        .await
    }
}

fn rich_nodes(value: &Value) -> Vec<&Value> {
    let mut nodes = vec![value];
    match value {
        Value::Array(items) => items.iter().for_each(|item| nodes.extend(rich_nodes(item))),
        Value::Object(fields) => fields
            .values()
            .for_each(|field| nodes.extend(rich_nodes(field))),
        _ => {}
    }
    nodes
}

async fn run_owner_exercise(exercise: Exercise) -> Result<(), String> {
    let label = format!(
        "testrun-slack-owner-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis()
    );
    let directory = std::env::temp_dir().join(&label);
    let chat = Arc::new(
        Slack::new(
            test_env("SLACK_TEST_BOT_TOKEN")?,
            test_env("SLACK_TEST_OWNER_ID")?,
        )
        .await
        .map_err(String::from)?,
    );
    fs::DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .map_err(|error| error.to_string())?;
    let mut owned = Owned {
        label,
        directory,
        chat,
        workspace: None,
        broker: None,
        reader: None,
    };
    let outcome = async {
        let mut driver = owned.start().await?;
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).map_err(|error| error.to_string())?;
        tokio::select! {
            result = async {
                match exercise {
                    Exercise::Acceptance => driver.exercise().await,
                    Exercise::Review => driver.review().await,
                    Exercise::Markdown => driver.markdown().await,
                    Exercise::Activity => driver.activity_deleted().await,
                    Exercise::BackgroundActivity => driver.background_activity_deleted().await,
                    Exercise::RestartActivity => driver.restart_activity_deleted().await,
                    Exercise::PromptActivity => driver.prompt_activity().await,
                }
            } => result,
            _ = tokio::signal::ctrl_c() => Err("owner acceptance interrupted; cleaning up owned fixtures".into()),
            _ = terminate.recv() => Err("owner acceptance terminated; cleaning up owned fixtures".into()),
        }
    }
    .await;
    let cleanup = owned.cleanup().await;
    cleanup?;
    outcome
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires the real Slack owner to perform each printed action"]
async fn owner_acceptance_runs_in_order() -> Result<(), String> {
    run_owner_exercise(Exercise::Acceptance).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires the real Slack owner to post encoded messages and use question cards"]
async fn owner_review_decodes_messages_and_refuses_empty_choices() -> Result<(), String> {
    run_owner_exercise(Exercise::Review).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn real_claude_markdown_reply_renders_in_slack() -> Result<(), String> {
    run_owner_exercise(Exercise::Markdown).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn real_claude_activity_message_is_deleted_after_the_turn() -> Result<(), String> {
    run_owner_exercise(Exercise::Activity).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn real_claude_activity_message_is_deleted_when_the_main_turn_ends_during_background_work()
-> Result<(), String> {
    run_owner_exercise(Exercise::BackgroundActivity).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn real_claude_activity_message_posted_before_a_restart_is_deleted_after_it()
-> Result<(), String> {
    run_owner_exercise(Exercise::RestartActivity).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires the real Slack owner to post the printed prompt and confirm the reply"]
async fn owner_prompt_shows_as_the_only_plan_task_and_the_next_tool_call_edits_that_message()
-> Result<(), String> {
    run_owner_exercise(Exercise::PromptActivity).await
}
