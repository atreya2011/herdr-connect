use std::collections::HashSet;
use std::fs;
use std::io::Write as _;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use herdr_connect_rs::{
    AgentSnapshot, Chat, ChatError, Closed, Inbound, OwnerIdentity, PermissionResponder, Slack,
    SlackRoute, handle_component, handle_owner_message, workspace_close, workspace_list_result,
};
use serde_json::{Value, json};
use serial_test::serial;
use slack_morphism::prelude::*;
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio::task::{JoinHandle, JoinSet};

use super::{
    Workspace, activity_binary_path, herdr_json, matching_tab, passthrough_env_args,
    snapshot_for_pane, transcript_question_answer,
};
use crate::{
    BridgeState, ChatConnection, handle_activity_event, handle_live_event, process_snapshot,
    route_topology, tab_list_result,
};

const OWNER: &str = "U0CEYEYSKT5";
const TEAM: &str = "T0CF6JNT0KB";
const STEP_TIMEOUT: Duration = Duration::from_secs(20 * 60);

struct Owned {
    label: String,
    directory: PathBuf,
    chat: Arc<Slack>,
    workspace: Option<Workspace>,
    broker: Option<JoinHandle<std::io::Result<()>>>,
    reader: Option<JoinHandle<()>>,
    before_channels: HashSet<SlackChannelId>,
}

enum Goal {
    Message,
    Working,
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
    ParentDeleted,
    Archived,
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
    tasks: JoinSet<Result<(), String>>,
    clock: tokio::time::Interval,
    client: SlackHyperClient,
    token: SlackApiToken,
    messages: Vec<SlackHistoryMessage>,
    owner_texts: Vec<String>,
    taps: Vec<String>,
    working_confirmed: bool,
    working_observed: bool,
    parent_deleted: Option<Instant>,
    archived_by_owner: bool,
    goal: Goal,
    directory: PathBuf,
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
    let settings = json!({
        "permissions": {"allow": ["Bash(sleep 180)"], "ask": ["Bash(touch *)"]},
        "hooks": {"PermissionRequest": [{"matcher": "Bash", "hooks": [{"type": "command", "command": command, "timeout": 1200}]}]}
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
        if workspace_list_result()?
            .iter()
            .any(|workspace| workspace.label == self.label)
        {
            return Err("test workspace label already exists".into());
        }
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
        Ok(())
    }

    async fn start(&mut self) -> Result<Driver, String> {
        let settings = hook_settings(&self.directory)?;
        let responder = Arc::new(PermissionResponder::new(self.chat.clone()));
        let (activity_tx, activity) = unbounded_channel();
        let socket = self.directory.join("broker.sock");
        let broker_responder = responder.clone();
        let broker_socket = socket.clone();
        self.broker = Some(tokio::spawn(async move {
            herdr_connect_rs::run_broker(&broker_socket, broker_responder, activity_tx).await
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
            .connect_socket(std::env::var("SLACK_APP_TOKEN").map_err(|error| error.to_string())?)
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
        let token = SlackApiToken::new(
            std::env::var("SLACK_BOT_TOKEN")
                .map_err(|error| error.to_string())?
                .into(),
        );
        Ok(Driver {
            connection: (
                self.chat.clone(),
                responder,
                OwnerIdentity {
                    display_name: OWNER.into(),
                    avatar_url: None,
                },
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
            tasks: JoinSet::new(),
            clock: tokio::time::interval(Duration::from_secs(2)),
            client: SlackClient::new(
                SlackClientHyperConnector::new().map_err(|error| error.to_string())?,
            ),
            token,
            messages: Vec::new(),
            owner_texts: Vec::new(),
            taps: Vec::new(),
            working_confirmed: false,
            working_observed: false,
            parent_deleted: None,
            archived_by_owner: false,
            goal: Goal::Message,
            directory: self.directory.clone(),
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
            if !workspace.label.starts_with("testrun-") {
                errors.push("owned workspace lost its testrun label; refusing cleanup".into());
                continue;
            }
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
        let token = SlackApiToken::new(
            std::env::var("SLACK_BOT_TOKEN")
                .map_err(|error| error.to_string())?
                .into(),
        );
        for channel in self
            .chat
            .channels()
            .await
            .map_err(String::from)?
            .iter()
            .filter(|channel| {
                !self.before_channels.contains(&channel.id)
                    && channel.name.as_deref().is_some_and(|name| {
                        name == self.label || name.starts_with(&format!("{}-archived-", self.label))
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
        "--permission-mode", "manual", "--tools", "Bash,AskUserQuestion", "--settings", settings,
        "--append-system-prompt", "You are a real testrun Slack acceptance agent. Follow only the test prompts. Run only their exact commands in this temporary directory. Never inspect secrets or touch other workspaces, tabs, channels, or production data. Never add Co-Authored-By or tool attribution lines. Never commit or push. After a denied command, do not retry or choose another tool. Answer question tools exactly as requested.",
        "Reply with exactly READY and use no tools.",
    ]).map(drop)
}

impl Driver {
    async fn refresh(&mut self) -> Result<(), String> {
        if self.parent_deleted.is_some() || self.archived_by_owner {
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
            .client
            .open_session(&self.token)
            .conversations_replies(&SlackApiConversationsRepliesRequest::new(
                self.route.channel.clone(),
                self.route
                    .thread
                    .clone()
                    .ok_or("test route has no parent")?,
            ))
            .await
            .map_err(|error| error.to_string())?
            .messages;
        Ok(())
    }

    async fn inbound(&mut self, event: Inbound<Slack>) -> Result<(), String> {
        match event {
            Inbound::Posted(posted)
                if posted.origin.channel.as_ref() == Some(&self.route.channel) =>
            {
                if matches!(posted.subtype, Some(SlackMessageEventType::MessageDeleted))
                    && posted.deleted_ts == self.route.thread
                {
                    self.parent_deleted = Some(Instant::now());
                    return Ok(());
                }
                if posted
                    .sender
                    .user
                    .as_ref()
                    .is_some_and(|user| user.0 == OWNER)
                    && posted.sender.bot_id.is_none()
                    && posted.origin.thread_ts == self.route.thread
                    && posted.subtype.is_none()
                {
                    let text = posted
                        .content
                        .as_ref()
                        .and_then(|content| content.text.clone())
                        .ok_or("owner message has no text")?;
                    self.owner_texts.push(text.clone());
                    if matches!(self.goal, Goal::Working) && text.trim() == "STATUS VISIBLE" {
                        self.working_confirmed = self.snapshot.agent_status == "working";
                        return Ok(());
                    }
                }
                let responder = self.connection.1.clone();
                self.tasks
                    .spawn(async move { handle_owner_message(&responder, posted).await });
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
                let owner = archive.0.user.0 == OWNER;
                let expected_workspace = self.workspace.clone();
                self.connection
                    .0
                    .handle_deletion(archive, move |closed| match closed {
                        Closed::Workspace(id) if id == expected_workspace => {
                            let permitted = workspace_list_result()?.iter().any(|workspace| {
                                workspace.workspace_id == id
                                    && workspace.label.starts_with("testrun-")
                            });
                            if !permitted {
                                return Err(
                                    "refused close of a workspace without its testrun label".into(),
                                );
                            }
                            workspace_close(&id).map(drop)
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
                self.inbound(event.ok_or("Slack event reader stopped")?.map_err(String::from)?).await?;
            }
            _ = self.clock.tick() => self.refresh().await?,
            Some(terminal) = self.live.recv() => {
                if self.parent_deleted.is_none() && !self.archived_by_owner { handle_live_event(&self.connection, &terminal, &mut self.state).await; }
            }
            Some(frame) = self.activity.recv() => {
                if frame.pane_id == self.pane && self.parent_deleted.is_none() && !self.archived_by_owner { handle_activity_event(&self.connection, frame, &mut self.state).await; }
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
                && self.route.thread.as_ref() != Some(&message.origin.ts)
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
                && self.bot_said("WORKING DONE")
                && self.settled()?),
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
            Goal::ParentDeleted => Ok(self
                .parent_deleted
                .is_some_and(|when| when.elapsed() >= Duration::from_secs(5))
                && tab_list_result()?
                    .iter()
                    .any(|tab| tab.tab_id == self.tab && tab.workspace_id == self.workspace)),
            Goal::Archived => Ok(self.archived_by_owner
                && workspace_list_result()?
                    .iter()
                    .all(|workspace| workspace.workspace_id != self.workspace)),
        }
    }

    async fn wait(&mut self, goal: Goal) -> Result<(), String> {
        self.goal = goal;
        let deadline = tokio::time::Instant::now() + STEP_TIMEOUT;
        loop {
            if self.observed()? {
                if !matches!(self.goal, Goal::ParentDeleted | Goal::Archived) {
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

    async fn questions(&mut self, thread_url: &str) -> Result<(), String> {
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
                "{name}: in {thread_url}, {instruction}; expected answer: {}. Wait for the next real question card.",
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

    async fn exercise(&mut self) -> Result<(), String> {
        let ready_deadline = tokio::time::Instant::now() + Duration::from_secs(120);
        while !self.settled()? {
            tokio::time::timeout_at(ready_deadline, Box::pin(self.pump()))
                .await
                .map_err(|_| "real agent bootstrap did not settle".to_owned())??;
        }
        self.refresh().await?;
        let parent = self
            .route
            .thread
            .clone()
            .ok_or("test route has no parent")?;
        let thread_url = self
            .client
            .open_session(&self.token)
            .chat_get_permalink(&SlackApiChatGetPermalinkRequest::new(
                self.route.channel.clone(),
                parent,
            ))
            .await
            .map_err(|error| error.to_string())?
            .permalink
            .to_string();
        action(&format!(
            "message: open {thread_url} and reply exactly: Reply with exactly SLACK OWNER OK and use no tools."
        ))?;
        self.wait(Goal::Message).await?;
        self.working_observed = false;
        self.goal = Goal::Working;
        action(&format!(
            "working status: in {thread_url}, watch the next three-minute task; while the working indicator is visible, reply exactly STATUS VISIBLE. Wait for WORKING DONE before the next step."
        ))?;
        self.prompt("Use Bash to run exactly sleep 180 in the foreground with timeout 240000. Do not use background execution or any other command. Then reply with exactly WORKING DONE.")?;
        self.wait(Goal::Working).await?;
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
                "permission {button}: in {thread_url}, click {button} on the next real permission card within its normal 45-second lifetime. Do not answer in the terminal."
            ))?;
            self.prompt(&format!("Use Bash exactly once to run {command}. Set description to Create the test marker. Do not retry a denied command or use another tool. After the tool result, reply only PERMISSION FINISHED."))?;
            self.wait(Goal::Permission {
                command,
                behavior,
                marker,
            })
            .await?;
        }
        self.questions(&thread_url).await?;
        action(&format!(
            "parent deletion: open {thread_url}, use the parent message menu to delete only the bot parent message; leave its channel unarchived. The test will verify that the Herdr tab remains open."
        ))?;
        self.wait(Goal::ParentDeleted).await?;
        action(&format!(
            "channel archive: last, open https://app.slack.com/client/{TEAM}/{} and archive only this test channel through channel details. The test will verify that only Herdr workspace {} closes.",
            self.route.channel, self.workspace
        ))?;
        self.wait(Goal::Archived).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
#[ignore = "requires the real Slack owner to perform each printed action"]
async fn owner_acceptance_runs_in_order() -> Result<(), String> {
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
            std::env::var("SLACK_BOT_TOKEN").map_err(|error| error.to_string())?,
            OWNER.into(),
            TEAM,
        )
        .await
        .map_err(String::from)?,
    );
    let before_channels = chat
        .channels()
        .await
        .map_err(String::from)?
        .into_iter()
        .map(|channel| channel.id)
        .collect();
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
        before_channels,
    };
    let outcome = async {
        let mut driver = owned.start().await?;
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).map_err(|error| error.to_string())?;
        tokio::select! {
            result = driver.exercise() => result,
            _ = tokio::signal::ctrl_c() => Err("owner acceptance interrupted; cleaning up owned fixtures".into()),
            _ = terminate.recv() => Err("owner acceptance terminated; cleaning up owned fixtures".into()),
        }
    }
    .await;
    let cleanup = owned.cleanup().await;
    cleanup?;
    outcome
}
