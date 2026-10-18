use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use notify::Watcher;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use twilight_http::Client;
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, MessageMarker, UserMarker, WebhookMarker},
};

use herdr_connect_rs::{
    ACTIVITY_KIND, ActivityFrame, activity_message_text, bridge_eprintln, bridge_println,
    decode_claude_activity_request, decode_codex_activity_request, decode_cursor_activity_request,
    deliver_activity_message, send_activity_frame, update_activity_message,
};
use herdr_connect_rs::{
    AgentLogCapture, AgentSession, AgentSnapshot, Chat, ComponentHandler, Discord,
    ENV_DISCORD_GUILD_ID, ENV_DISCORD_OWNER_ID, ENV_DISCORD_TOKEN, ENV_HOME, EVENT_KEY,
    GatewayContext, HerdrSubscription, HerdrTab, OwnerIdentity, Question, STATUS_BLOCKED,
    STATUS_DONE, STATUS_IDLE, STATUS_WORKING, TopologyCache, TopologyClosure, TopologyRoute,
    Transition, TransitionMessage, UNKNOWN_CHANNEL_DELIVERY_ERROR, UNKNOWN_WEBHOOK_DELIVERY_ERROR,
    agent_read_detection, create_transition_messages, deliver_live_message,
    deliver_transition_card, dialog_shows_question, drive_gateway_with_components,
    execute_terminal_prompt_webhook, expire_informational_card, fetch_owner_identity,
    forget_departed_owner_prompt_suppressions, format_detection_question, hook_timeout,
    is_postable_transition, lifecycle_subscriptions, list_agents, live_message_nonce,
    load_discord_config, name_unlabeled_tabs, read_claude_incremental,
    read_claude_prompts_incremental, read_codex_incremental, read_codex_prompts_incremental,
    read_cursor_incremental, read_cursor_prompts_incremental, resolve_terminal_prompt_webhook,
    route_topology, split_live_message, status_subscriptions, subscribe_herdr_events,
    tab_list_result, take_owner_prompt_suppression, transition_card_nonce, workspace_list_result,
};
use herdr_connect_rs::{
    Decision, Interaction, PermissionResponder, PermissionVendor, VENDOR_CLAUDE, VENDOR_CODEX,
    VENDOR_CURSOR, cursor_argv_forces_allow, decode_claude_permission_request,
    decode_codex_permission_request, decode_cursor_permission_request, encode_claude_decision,
    encode_codex_decision, encode_cursor_decision, handle_component, is_cursor_agent_argv,
    request_decision, run_broker as run_permission_broker,
};

type DiscordConnection = (
    Arc<Client>,
    Id<GuildMarker>,
    String,
    Arc<PermissionResponder<Discord>>,
    OwnerIdentity,
);
type GatewayTask = tokio::task::JoinHandle<Result<(), String>>;
type BrokerTask = tokio::task::JoinHandle<Result<(), String>>;

#[derive(Clone, Copy)]
struct InformationalCard {
    channel: Id<ChannelMarker>,
    message: Id<MessageMarker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Follower {
    Claude(u64),
    Codex(u64),
    Cursor(i64),
}

struct LiveWatch {
    _watcher: notify::RecommendedWatcher,
    path: PathBuf,
    follower: Follower,
    route: TopologyRoute,
}

#[derive(Default)]
struct BridgeState {
    previous: HashMap<String, String>,
    state_change_sequences: HashMap<String, u64>,
    informational_cards: HashMap<String, InformationalCard>,
    live_watches: HashMap<String, LiveWatch>,
    live_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    live_errors_reported: HashSet<(String, String)>,
    rename_errors_reported: HashSet<String>,
    activity_messages: HashMap<String, ActivityMessage>,
    activity_eligible_panes: HashSet<String>,
    terminal_prompt_positions: HashMap<String, (PathBuf, Follower, Follower)>,
    awaiting_first_log: HashMap<String, String>,
    seen_without_session: HashMap<String, SystemTime>,
    terminal_prompt_webhooks: HashMap<Id<ChannelMarker>, (Id<WebhookMarker>, String)>,
}

struct ActivityMessage {
    message: Id<MessageMarker>,
    count: u32,
}

struct BlockedCardContext<'a> {
    client: &'a Client,
    owner_id: &'a str,
    responder: &'a PermissionResponder<Discord>,
    topology_cache: &'a TopologyCache,
    route: &'a TopologyRoute,
    snapshot: &'a AgentSnapshot,
    session: &'a AgentSession,
    terminal: &'a str,
    from_status: &'a str,
    state_change_seq: u64,
    informational_cards: &'a mut HashMap<String, InformationalCard>,
}

const SUBSCRIBE_RETRY_INITIAL: Duration = Duration::from_millis(250);
const SUBSCRIBE_RETRY_MAX: Duration = Duration::from_secs(30);

const TERMINAL_PROMPT_WEBHOOK_NAME: &str = "herdr-connect owner";

#[must_use]
fn vendor_is_supported(agent: Option<&str>) -> bool {
    matches!(agent, Some(VENDOR_CLAUDE | VENDOR_CODEX))
}

async fn wait_for_gateway(gateway: &mut GatewayTask) -> Result<(), String> {
    gateway
        .await
        .map_err(|error| format!("discord gateway task failed: {error}"))?
}

async fn wait_for_broker(broker: Option<&mut BrokerTask>) -> Result<(), String> {
    match broker {
        Some(broker) => broker
            .await
            .map_err(|error| format!("permission broker task failed: {error}"))?,
        None => std::future::pending().await,
    }
}

async fn handle_blocked_card(context: BlockedCardContext<'_>) {
    let BlockedCardContext {
        client,
        owner_id,
        responder,
        topology_cache,
        route,
        snapshot,
        session,
        terminal,
        from_status,
        state_change_seq,
        informational_cards,
    } = context;
    let target = match responder.chat().sync_route(route).await {
        Ok(target) => target,
        Err(error) => {
            bridge_eprintln!("{error}");
            return;
        }
    };
    let supported_broker_pending = vendor_is_supported(snapshot.agent.as_deref())
        && responder.has_pending_session(&session.value);
    if supported_broker_pending {
        return;
    }
    let detection = (snapshot.agent.as_deref() == Some(VENDOR_CLAUDE))
        .then(|| agent_read_detection(&route.pane_id).ok())
        .flatten();
    if let Some((questions, index)) = shown_question(snapshot, session, detection.as_deref()).await
    {
        match responder
            .deliver_question_card(&target, &route.pane_id, questions, index)
            .await
        {
            Ok(message) => {
                informational_cards.insert(
                    terminal.to_owned(),
                    InformationalCard {
                        channel: target,
                        message,
                    },
                );
            }
            Err(error) => bridge_eprintln!("discord delivery error: {error}"),
        }
        return;
    }
    let detection_question = detection.as_deref().and_then(format_detection_question);
    let capture = detection_question.map_or_else(
        || capture_for_blocked(snapshot, session),
        |question| AgentLogCapture {
            message: question.clone(),
            question: Some(question),
            pending_questions: Vec::new(),
            failure: None,
        },
    );
    let transition = Transition {
        from: from_status.to_owned(),
        to: STATUS_BLOCKED.to_owned(),
        terminal_id: terminal.to_owned(),
    };
    let messages = create_transition_messages(&transition, &capture, owner_id);
    deliver_blocked_messages(
        &BlockedDeliveryRoute {
            client,
            topology_cache,
        },
        target,
        terminal,
        state_change_seq,
        &messages,
        informational_cards,
    )
    .await;
}

const PENDING_QUESTION_WAIT: Duration = Duration::from_secs(3);
const PENDING_QUESTION_POLL: Duration = Duration::from_millis(500);

const QUESTION_DIALOG_MARKER: &str = "Type something.";

async fn shown_question(
    snapshot: &AgentSnapshot,
    session: &AgentSession,
    detection: Option<&str>,
) -> Option<(Vec<Question>, usize)> {
    let detection = detection?;
    format_detection_question(detection)?;
    let wait = detection.contains(QUESTION_DIALOG_MARKER);
    let deadline = tokio::time::Instant::now() + PENDING_QUESTION_WAIT;
    let mut last_error: Option<String>;
    loop {
        match pending_questions(snapshot, session) {
            Ok(questions) => {
                last_error = None;
                if let Some(index) = questions
                    .iter()
                    .position(|question| dialog_shows_question(detection, question))
                {
                    return Some((questions, index));
                }
            }
            Err(error) => last_error = Some(error),
        }
        if !wait || tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(PENDING_QUESTION_POLL).await;
    }
    if let Some(error) = last_error {
        bridge_eprintln!("agent blocked-context capture error: {error}");
    }
    None
}

fn pending_questions(
    snapshot: &AgentSnapshot,
    session: &AgentSession,
) -> Result<Vec<Question>, String> {
    let home = std::env::var_os(ENV_HOME).ok_or("HOME is not configured")?;
    capture_for_with_search_root(snapshot, session, Path::new(&home))
        .map(|capture| capture.pending_questions)
}

#[derive(Clone, Copy)]
struct BlockedDeliveryRoute<'a> {
    client: &'a Client,
    topology_cache: &'a TopologyCache,
}

async fn deliver_blocked_messages(
    delivery: &BlockedDeliveryRoute<'_>,
    target: Id<ChannelMarker>,
    terminal: &str,
    state_change_seq: u64,
    messages: &[TransitionMessage],
    informational_cards: &mut HashMap<String, InformationalCard>,
) {
    let BlockedDeliveryRoute {
        client,
        topology_cache,
    } = *delivery;
    let mut last = None;
    for (index, message) in messages.iter().enumerate() {
        let nonce = transition_card_nonce(terminal, state_change_seq, index);
        match deliver_transition_card(client, target, message, &nonce).await {
            Ok(id) => last = Some(id),
            Err(error) => {
                if error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR) {
                    *topology_cache.lock().await = None;
                }
                bridge_eprintln!("discord delivery error: {error}");
                return;
            }
        }
    }
    if let Some(message) = last {
        informational_cards.insert(
            terminal.to_owned(),
            InformationalCard {
                channel: target,
                message,
            },
        );
    }
}

async fn expire_blocked_card(
    discord: &DiscordConnection,
    terminal: &str,
    informational_cards: &mut HashMap<String, InformationalCard>,
) {
    if let Some(card) = informational_cards.get(terminal).copied() {
        let (client, _, _, responder, _) = discord;
        responder.forget_question_card(&card.message);
        if let Err(error) = expire_informational_card(
            client.as_ref(),
            card.channel,
            card.message,
            "resolved: pane left blocked",
        )
        .await
        {
            bridge_eprintln!("discord blocked-card expiry error: {error}");
        } else {
            informational_cards.remove(terminal);
        }
    }
}

async fn expire_departed_card(
    discord: &DiscordConnection,
    terminal: &str,
    card: InformationalCard,
) {
    let (client, _, _, responder, _) = discord;
    responder.forget_question_card(&card.message);
    if let Err(error) = expire_informational_card(
        client.as_ref(),
        card.channel,
        card.message,
        "resolved: pane left blocked",
    )
    .await
    {
        bridge_eprintln!("discord blocked-card expiry error for {terminal}: {error}");
    }
}

fn update_activity_eligibility(state: &mut BridgeState, snapshot: &AgentSnapshot, status: &str) {
    let pane_id = snapshot.pane_id.as_str();
    if status == STATUS_WORKING && snapshot.session.is_some() {
        state.activity_eligible_panes.insert(pane_id.to_owned());
    } else {
        state.activity_eligible_panes.remove(pane_id);
    }
}

async fn maybe_sync_fresh_session_topology(
    snapshot: &AgentSnapshot,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    discord: &DiscordConnection,
    state: &mut BridgeState,
) {
    let previous_status = state
        .previous
        .get(&snapshot.terminal_id)
        .map(String::as_str);
    if snapshot.session.is_none()
        || previous_status.is_some_and(|previous| {
            previous != "unknown"
                || !matches!(snapshot.agent_status.as_str(), STATUS_IDLE | STATUS_DONE)
        })
    {
        return;
    }
    let route = match route_topology(agents, tabs, &snapshot.terminal_id) {
        Ok(route) => route,
        Err(error) => {
            if state
                .live_errors_reported
                .insert((snapshot.terminal_id.clone(), error.clone()))
            {
                bridge_eprintln!("topology route error for {}: {error}", snapshot.terminal_id);
            }
            return;
        }
    };
    let (_, _, _, responder, _) = discord;
    if let Err(error) = responder.chat().sync_route(&route).await {
        bridge_eprintln!("{error}");
    }
}

async fn process_snapshot(
    snapshot: &AgentSnapshot,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    discord: &DiscordConnection,
    state: &mut BridgeState,
) {
    let (terminal, status) = (snapshot.terminal_id.clone(), snapshot.agent_status.clone());
    update_activity_eligibility(state, snapshot, &status);
    maybe_establish_terminal_prompt_baseline(snapshot, state);
    ensure_live_watch_started(discord, snapshot, agents, tabs, state).await;
    maybe_sync_fresh_session_topology(snapshot, agents, tabs, discord, state).await;
    if let Some(old) = state.previous.get(&terminal).cloned()
        && old != status
    {
        let seq = next_state_change_sequence(&mut state.state_change_sequences, &terminal);
        let leaving_blocked = old == STATUS_BLOCKED && status != STATUS_BLOCKED;
        let transition = Transition {
            from: old,
            to: status.clone(),
            terminal_id: terminal.clone(),
        };
        update_blocked_lifecycle(discord, &terminal, leaving_blocked, state).await;
        deliver_transition_if_postable(
            PostableTransitionContext {
                snapshot,
                agents,
                tabs,
                discord,
                terminal: &terminal,
                transition: &transition,
                state_change_seq: seq,
            },
            state,
        )
        .await;
        if status == STATUS_WORKING {
            forget_activity_message(state, &snapshot.pane_id);
        }
    }
    remember_previous_status(state, &terminal, &status);
}

fn remember_previous_status(state: &mut BridgeState, terminal: &str, status: &str) {
    state
        .previous
        .insert(terminal.to_owned(), status.to_owned());
}

struct PostableTransitionContext<'a> {
    snapshot: &'a AgentSnapshot,
    agents: &'a [AgentSnapshot],
    tabs: &'a [HerdrTab],
    discord: &'a DiscordConnection,
    terminal: &'a str,
    transition: &'a Transition,
    state_change_seq: u64,
}

async fn deliver_postable_transition(
    context: PostableTransitionContext<'_>,
    state: &mut BridgeState,
) {
    let PostableTransitionContext {
        snapshot,
        agents,
        tabs,
        discord,
        terminal,
        transition,
        state_change_seq,
    } = context;
    let Some(session) = snapshot.session.as_ref() else {
        bridge_println!("{terminal}: no reported session, not mirrored");
        return;
    };
    let route = match route_topology(agents, tabs, terminal) {
        Ok(route) => route,
        Err(error) => {
            bridge_eprintln!("{error}");
            return;
        }
    };
    let (client, _, owner_id, responder, _) = discord;
    handle_blocked_card(BlockedCardContext {
        client: client.as_ref(),
        owner_id,
        responder: responder.as_ref(),
        topology_cache: responder.chat().topology_cache(),
        route: &route,
        snapshot,
        session,
        terminal,
        from_status: &transition.from,
        state_change_seq,
        informational_cards: &mut state.informational_cards,
    })
    .await;
}

async fn deliver_transition_if_postable(
    context: PostableTransitionContext<'_>,
    state: &mut BridgeState,
) {
    if is_postable_transition(context.transition) {
        deliver_postable_transition(context, state).await;
    }
}

async fn update_blocked_lifecycle(
    discord: &DiscordConnection,
    terminal: &str,
    leaving_blocked: bool,
    state: &mut BridgeState,
) {
    if leaving_blocked {
        expire_blocked_card(discord, terminal, &mut state.informational_cards).await;
    }
}

fn component_handler(responder: Arc<PermissionResponder<Discord>>) -> ComponentHandler {
    Arc::new(move |interaction| {
        let responder = Arc::clone(&responder);
        let component = responder.chat().component(interaction);
        Box::pin(async move {
            if let Some(component) = component {
                handle_component(responder, component).await;
            }
        })
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SessionPathError {
    NotFoundYet(String),
    Permanent(String),
}

impl std::fmt::Display for SessionPathError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFoundYet(message) | Self::Permanent(message) => write!(formatter, "{message}"),
        }
    }
}

fn capture_for_with_search_root(
    snapshot: &AgentSnapshot,
    session: &AgentSession,
    search_root: &Path,
) -> Result<AgentLogCapture, String> {
    let path =
        resolve_session_path(search_root, snapshot, session).map_err(|error| error.to_string())?;
    let log = herdr_connect_rs::read_agent_log(session, &path)?;
    Ok(AgentLogCapture {
        message: log.message,
        failure: log.failure,
        question: log.question,
        pending_questions: log.pending_questions,
    })
}

fn resolve_session_path(
    search_root: &Path,
    snapshot: &AgentSnapshot,
    session: &AgentSession,
) -> Result<PathBuf, SessionPathError> {
    match session.agent.as_str() {
        VENDOR_CLAUDE => {
            let cwd = snapshot
                .cwd
                .as_deref()
                .filter(|cwd| !cwd.trim().is_empty())
                .ok_or_else(|| {
                    SessionPathError::Permanent(
                        "claude session has no cwd for log resolution".to_owned(),
                    )
                })?;
            let cwd_slug = cwd
                .chars()
                .map(|character| {
                    if character.is_ascii_alphanumeric() {
                        character
                    } else {
                        '-'
                    }
                })
                .collect::<String>();
            let session_file = format!("{}.jsonl", session.value);
            let candidates = claude_config_roots(search_root)?
                .into_iter()
                .map(|root| root.join("projects").join(&cwd_slug).join(&session_file))
                .filter(|path| path.is_file())
                .collect::<Vec<_>>();
            unique_existing_path(&candidates, "claude session log")
        }
        VENDOR_CODEX => {
            let roots = read_directories(search_root, "Codex home directory")?
                .into_iter()
                .filter(|path| {
                    path.file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name == ".codex" || name.starts_with(".codex-"))
                })
                .collect::<Vec<_>>();
            if roots.is_empty() {
                return Err(SessionPathError::Permanent(format!(
                    "no Codex home (.codex or .codex-*) under {}",
                    search_root.display()
                )));
            }
            let mut candidates = Vec::new();
            for root in roots {
                collect_matching_paths(
                    &root.join("sessions"),
                    &session.value,
                    |path| {
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| {
                                name.starts_with("rollout-")
                                    && Path::new(name)
                                        .extension()
                                        .and_then(|extension| extension.to_str())
                                        .is_some_and(|extension| {
                                            extension.eq_ignore_ascii_case("jsonl")
                                        })
                            })
                    },
                    &mut candidates,
                )?;
            }
            candidates
                .into_iter()
                .max_by(|left, right| left.file_name().cmp(&right.file_name()))
                .ok_or_else(|| {
                    SessionPathError::NotFoundYet("codex session log was not found".to_owned())
                })
        }
        VENDOR_CURSOR => {
            let chats = search_root.join(".cursor/chats");
            let mut candidates = Vec::new();
            for workspace in read_directories(&chats, "Cursor chat directory")? {
                let store = workspace.join(&session.value).join("store.db");
                if store.is_file() {
                    candidates.push(store);
                }
            }
            unique_existing_path(&candidates, "Cursor session store")
        }
        agent => Err(SessionPathError::Permanent(format!(
            "unsupported vendor session agent: {agent}"
        ))),
    }
}

fn claude_config_roots(search_root: &Path) -> Result<Vec<PathBuf>, SessionPathError> {
    let mut roots = read_entries(search_root, "Claude config directory")?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| {
            path.is_dir()
                && path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name == ".claude" || name.starts_with(".claude-"))
                && path.join("projects").is_dir()
        })
        .collect::<Vec<_>>();
    roots.sort();
    Ok(roots)
}

fn dedupe_hard_links(candidates: &[PathBuf]) -> Result<Vec<PathBuf>, SessionPathError> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    for path in candidates {
        let metadata = fs::metadata(path)
            .map_err(|error| SessionPathError::Permanent(format!("{}: {error}", path.display())))?;
        if seen.insert((metadata.dev(), metadata.ino())) {
            unique.push(path.clone());
        }
    }
    Ok(unique)
}

fn unique_existing_path(
    candidates: &[PathBuf],
    description: &str,
) -> Result<PathBuf, SessionPathError> {
    match dedupe_hard_links(candidates)?.as_slice() {
        [path] => Ok(path.clone()),
        [] => Err(SessionPathError::NotFoundYet(format!(
            "{description} was not found"
        ))),
        _ => Err(SessionPathError::Permanent(format!(
            "multiple {description}s were found"
        ))),
    }
}

fn collect_matching_paths(
    directory: &Path,
    session_id: &str,
    matches: fn(&Path) -> bool,
    candidates: &mut Vec<PathBuf>,
) -> Result<(), SessionPathError> {
    for entry in read_entries(directory, "session search directory")? {
        let path = entry.path();
        if path.is_dir() {
            collect_matching_paths(&path, session_id, matches, candidates)?;
        } else if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.contains(session_id))
            && matches(&path)
        {
            candidates.push(path);
        }
    }
    Ok(())
}

fn read_directories(root: &Path, description: &str) -> Result<Vec<PathBuf>, SessionPathError> {
    Ok(read_entries(root, description)?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect())
}

fn read_entries(
    directory: &Path,
    description: &str,
) -> Result<Vec<fs::DirEntry>, SessionPathError> {
    let classify = |error: std::io::Error| {
        SessionPathError::Permanent(format!(
            "failed to read {description} {}: {error}",
            directory.display()
        ))
    };
    fs::read_dir(directory)
        .map_err(classify)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(classify)
}

fn live_log_path(
    snapshot: &AgentSnapshot,
    session: &AgentSession,
) -> Result<Option<PathBuf>, String> {
    let home = std::env::var_os(ENV_HOME).ok_or_else(|| "HOME is not configured".to_owned())?;
    match resolve_session_path(Path::new(&home), snapshot, session) {
        Ok(path) => Ok(Some(path)),
        Err(SessionPathError::NotFoundYet(_)) => Ok(None),
        Err(SessionPathError::Permanent(message)) => Err(message),
    }
}

fn start_notify_watcher(
    follower: Follower,
    path: &Path,
    terminal: String,
    tx: tokio::sync::mpsc::UnboundedSender<String>,
) -> Result<notify::RecommendedWatcher, String> {
    let mut watcher =
        notify::recommended_watcher(move |result: notify::Result<notify::Event>| match result {
            Ok(event)
                if matches!(
                    event.kind,
                    notify::EventKind::Modify(_) | notify::EventKind::Create(_)
                ) =>
            {
                let _ = tx.send(terminal.clone());
            }
            Ok(_) => {}
            Err(error) => bridge_eprintln!("live capture watch error for {terminal}: {error}"),
        })
        .map_err(|error| error.to_string())?;
    let target = if matches!(follower, Follower::Cursor(_)) {
        path.parent()
            .ok_or_else(|| format!("cursor store {} has no parent", path.display()))?
    } else {
        path
    };
    watcher
        .watch(target, notify::RecursiveMode::NonRecursive)
        .map_err(|error| error.to_string())?;
    Ok(watcher)
}

fn initial_live_position(start: Follower, path: &Path) -> Result<Follower, String> {
    match start {
        Follower::Claude(offset) => {
            read_claude_incremental(path, offset).map(|(_, end)| Follower::Claude(end))
        }
        Follower::Codex(offset) => {
            read_codex_incremental(path, offset).map(|(_, end)| Follower::Codex(end))
        }
        Follower::Cursor(rowid) => {
            read_cursor_incremental(path, rowid).map(|(_, end)| Follower::Cursor(end))
        }
    }
}

fn initial_terminal_prompt_position(start: Follower, path: &Path) -> Result<Follower, String> {
    match start {
        Follower::Claude(offset) => read_claude_prompts_incremental(path, offset)
            .map(|(_, checkpoint)| Follower::Claude(checkpoint)),
        Follower::Codex(offset) => read_codex_prompts_incremental(path, offset)
            .map(|(_, checkpoint)| Follower::Codex(checkpoint)),
        Follower::Cursor(rowid) => read_cursor_prompts_incremental(path, rowid)
            .map(|(_, checkpoint)| Follower::Cursor(checkpoint)),
    }
}

fn read_new_terminal_prompts(
    path: &Path,
    position: Follower,
) -> Result<(Vec<String>, Follower), String> {
    match position {
        Follower::Claude(offset) => {
            let (prompts, checkpoint) = read_claude_prompts_incremental(path, offset)?;
            let prompts = prompts.into_iter().map(|(text, _)| text).collect();
            Ok((prompts, Follower::Claude(checkpoint)))
        }
        Follower::Codex(offset) => {
            let (prompts, checkpoint) = read_codex_prompts_incremental(path, offset)?;
            let prompts = prompts.into_iter().map(|(text, _)| text).collect();
            Ok((prompts, Follower::Codex(checkpoint)))
        }
        Follower::Cursor(last_rowid) => {
            let (prompts, new_rowid) = read_cursor_prompts_incremental(path, last_rowid)?;
            let prompts = prompts.into_iter().map(|(text, _)| text).collect();
            Ok((prompts, Follower::Cursor(new_rowid)))
        }
    }
}

async fn ensure_live_watch_started(
    discord: &DiscordConnection,
    snapshot: &AgentSnapshot,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    state: &mut BridgeState,
) {
    let terminal = snapshot.terminal_id.clone();
    let Some(session) = snapshot.session.clone() else {
        return;
    };
    if !matches!(
        session.agent.as_str(),
        VENDOR_CLAUDE | VENDOR_CODEX | VENDOR_CURSOR
    ) {
        return;
    }
    let Some((path, _prompt_position, live_position)) =
        state.terminal_prompt_positions.get(&terminal).cloned()
    else {
        return;
    };
    let route = match route_topology(agents, tabs, &terminal) {
        Ok(route) => route,
        Err(error) => {
            if state
                .live_errors_reported
                .insert((terminal.clone(), error.clone()))
            {
                bridge_eprintln!("topology route error for {terminal}: {error}");
            }
            return;
        }
    };
    if let Some(watch) = state.live_watches.get_mut(&terminal)
        && watch.path == path
    {
        watch.route = route;
        return;
    }
    let Some(live_tx) = state.live_tx.clone() else {
        return;
    };
    let watcher = match start_notify_watcher(live_position, &path, terminal.clone(), live_tx) {
        Ok(watcher) => watcher,
        Err(error) => {
            bridge_eprintln!("live capture watch error for {terminal}: {error}");
            return;
        }
    };
    state.live_watches.insert(
        terminal.clone(),
        LiveWatch {
            _watcher: watcher,
            path,
            follower: live_position,
            route,
        },
    );
    handle_live_event(discord, &terminal, state).await;
}

fn read_new_live_texts(
    path: &Path,
    position: Follower,
) -> Result<(Vec<(String, Follower)>, Follower), String> {
    match position {
        Follower::Claude(offset) => {
            let (texts, new_offset) = read_claude_incremental(path, offset)?;
            let texts = texts
                .into_iter()
                .map(|(text, end)| (text, Follower::Claude(end)))
                .collect();
            Ok((texts, Follower::Claude(new_offset)))
        }
        Follower::Codex(offset) => {
            let (texts, new_offset) = read_codex_incremental(path, offset)?;
            let texts = texts
                .into_iter()
                .map(|(text, end)| (text, Follower::Codex(end)))
                .collect();
            Ok((texts, Follower::Codex(new_offset)))
        }
        Follower::Cursor(rowid) => {
            let (texts, new_rowid) = read_cursor_incremental(path, rowid)?;
            let texts = texts
                .into_iter()
                .map(|(text, end)| (text, Follower::Cursor(end)))
                .collect();
            Ok((texts, Follower::Cursor(new_rowid)))
        }
    }
}

async fn cached_terminal_prompt_webhook(
    client: &Client,
    workspace_channel: Id<ChannelMarker>,
    state: &mut BridgeState,
) -> Result<(Id<WebhookMarker>, String), String> {
    if let Some(webhook) = state.terminal_prompt_webhooks.get(&workspace_channel) {
        return Ok(webhook.clone());
    }
    let webhook =
        resolve_terminal_prompt_webhook(client, workspace_channel, TERMINAL_PROMPT_WEBHOOK_NAME)
            .await?;
    state
        .terminal_prompt_webhooks
        .insert(workspace_channel, webhook.clone());
    Ok(webhook)
}

#[derive(Clone, Copy)]
struct TerminalPromptTarget<'a> {
    client: &'a Client,
    topology_cache: &'a TopologyCache,
    workspace_channel: Id<ChannelMarker>,
    thread: Id<ChannelMarker>,
}

async fn deliver_terminal_prompt_part<'a>(
    target: TerminalPromptTarget<'a>,
    identity: &OwnerIdentity,
    part: &str,
    state: &mut BridgeState,
) -> Result<TerminalPromptTarget<'a>, String> {
    let (webhook_id, webhook_token) =
        cached_terminal_prompt_webhook(target.client, target.workspace_channel, state).await?;
    execute_terminal_prompt_webhook(
        target.client,
        webhook_id,
        &webhook_token,
        target.thread,
        &identity.display_name,
        identity.avatar_url.as_deref(),
        part,
    )
    .await?;
    Ok(target)
}

async fn mirror_one_terminal_prompt<'a>(
    target: TerminalPromptTarget<'a>,
    identity: &OwnerIdentity,
    content: &str,
    state: &mut BridgeState,
) -> Result<TerminalPromptTarget<'a>, String> {
    let mut current = target;
    for part in split_live_message(content) {
        match deliver_terminal_prompt_part(current, identity, &part, state).await {
            Ok(delivered) => current = delivered,
            Err(error) => {
                if error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR) {
                    *current.topology_cache.lock().await = None;
                } else if error.starts_with(UNKNOWN_WEBHOOK_DELIVERY_ERROR) {
                    state
                        .terminal_prompt_webhooks
                        .remove(&current.workspace_channel);
                }
                return Err(error);
            }
        }
    }
    Ok(current)
}

fn maybe_establish_terminal_prompt_baseline(snapshot: &AgentSnapshot, state: &mut BridgeState) {
    let terminal = &snapshot.terminal_id;
    let Some(session) = snapshot.session.as_ref() else {
        if snapshot.agent.is_some() {
            state
                .seen_without_session
                .entry(terminal.clone())
                .or_insert_with(SystemTime::now);
        } else {
            state.seen_without_session.remove(terminal);
        }
        return;
    };
    let zero = match session.agent.as_str() {
        VENDOR_CLAUDE => Follower::Claude(0),
        VENDOR_CODEX => Follower::Codex(0),
        VENDOR_CURSOR => Follower::Cursor(0),
        _ => return,
    };
    let path = match live_log_path(snapshot, session) {
        Ok(Some(path)) => path,
        Ok(None) => {
            state
                .awaiting_first_log
                .insert(terminal.clone(), session.value.clone());
            return;
        }
        Err(error) => {
            bridge_eprintln!("terminal prompt baseline error for {terminal}: {error}");
            return;
        }
    };
    if terminal_prompt_baseline_is_current(&state.terminal_prompt_positions, terminal, &path) {
        state.seen_without_session.remove(terminal);
        return;
    }
    let was_awaiting_this_session = state
        .awaiting_first_log
        .get(terminal)
        .is_some_and(|awaiting_session| awaiting_session == &session.value);
    let followed_by_another_terminal = state
        .terminal_prompt_positions
        .iter()
        .any(|(other, (other_path, _, _))| other != terminal && other_path == &path);
    let created_after_first_sighting = if was_awaiting_this_session || followed_by_another_terminal
    {
        false
    } else if let Some(first_seen) = state.seen_without_session.get(terminal) {
        match fs::metadata(&path).and_then(|metadata| metadata.created()) {
            Ok(created) => created > *first_seen,
            Err(error) => {
                bridge_eprintln!("terminal prompt baseline error for {terminal}: {error}");
                return;
            }
        }
    } else {
        false
    };
    let positions = if was_awaiting_this_session || created_after_first_sighting {
        initial_live_position(zero, &path).map(|_| (zero, zero))
    } else {
        initial_terminal_prompt_position(zero, &path)
            .and_then(|prompt| initial_live_position(zero, &path).map(|live| (prompt, live)))
    };
    match positions {
        Ok((prompt_position, live_position)) => {
            state.awaiting_first_log.remove(terminal);
            state.seen_without_session.remove(terminal);
            state
                .terminal_prompt_positions
                .insert(terminal.clone(), (path, prompt_position, live_position));
        }
        Err(error) => bridge_eprintln!("terminal prompt baseline error for {terminal}: {error}"),
    }
}

fn terminal_prompt_baseline_is_current(
    positions: &HashMap<String, (PathBuf, Follower, Follower)>,
    terminal: &str,
    path: &Path,
) -> bool {
    positions
        .get(terminal)
        .is_some_and(|(existing_path, _, _)| existing_path == path)
}

async fn mirror_terminal_prompts(
    connection: &DiscordConnection,
    terminal: &str,
    route: &TopologyRoute,
    state: &mut BridgeState,
) {
    let Some((path, prompt_position, live_position)) =
        state.terminal_prompt_positions.get(terminal).cloned()
    else {
        return;
    };
    let (prompts, new_position) = match read_new_terminal_prompts(&path, prompt_position) {
        Ok(result) => result,
        Err(error) => {
            bridge_eprintln!("terminal prompt read error for {terminal}: {error}");
            return;
        }
    };
    state
        .terminal_prompt_positions
        .insert(terminal.to_owned(), (path, new_position, live_position));
    if prompts.is_empty() {
        return;
    }
    let (client, _, _, responder, identity) = connection;
    let topology_cache = responder.chat().topology_cache();
    let (workspace_channel, thread) = match responder.chat().sync_route_with_parent(route).await {
        Ok(channels) => channels,
        Err(error) => {
            bridge_eprintln!("terminal prompt route error for {terminal}: {error}");
            return;
        }
    };
    let mut target = TerminalPromptTarget {
        client: client.as_ref(),
        topology_cache,
        workspace_channel,
        thread,
    };
    for text in prompts {
        if take_owner_prompt_suppression(&route.pane_id, &text) {
            continue;
        }
        match mirror_one_terminal_prompt(target, identity, &text, state).await {
            Ok(delivered) => target = delivered,
            Err(error) => {
                bridge_eprintln!("terminal prompt delivery error for {terminal}: {error}");
            }
        }
    }
}

async fn handle_live_event(
    connection: &DiscordConnection,
    terminal: &str,
    state: &mut BridgeState,
) {
    let (client, _, _, responder, _) = connection;
    let Some(watch) = state.live_watches.get(terminal) else {
        return;
    };
    let (path, start_position, route) = (watch.path.clone(), watch.follower, watch.route.clone());
    mirror_terminal_prompts(connection, terminal, &route, state).await;
    let (texts, read_position) = match read_new_live_texts(&path, start_position) {
        Ok(result) => result,
        Err(error) => {
            if state
                .live_errors_reported
                .insert((terminal.to_owned(), error.clone()))
            {
                bridge_eprintln!("live capture read error for {terminal}: {error}");
            }
            return;
        }
    };
    let topology_cache = responder.chat().topology_cache();
    let channel = match responder.chat().sync_route(&route).await {
        Ok(channel) => channel,
        Err(error) => {
            if state
                .live_errors_reported
                .insert((terminal.to_owned(), error.to_string()))
            {
                bridge_eprintln!("live capture route error for {terminal}: {error}");
            }
            return;
        }
    };
    let mut delivered_position = start_position;
    let mut all_delivered = true;
    for (text, position) in texts {
        let mut posted_all = true;
        let nonce_position = match position {
            Follower::Claude(offset) | Follower::Codex(offset) => i128::from(offset),
            Follower::Cursor(rowid) => i128::from(rowid),
        };
        for (part_index, part) in split_live_message(&text).into_iter().enumerate() {
            let nonce = live_message_nonce(terminal, nonce_position, part_index);
            if let Err(error) = deliver_live_message(client.as_ref(), channel, &part, &nonce).await
            {
                if error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR) {
                    *topology_cache.lock().await = None;
                }
                if state
                    .live_errors_reported
                    .insert((terminal.to_owned(), error.clone()))
                {
                    bridge_eprintln!("live capture delivery error for {terminal}: {error}");
                }
                posted_all = false;
                break;
            }
        }
        if !posted_all {
            all_delivered = false;
            break;
        }
        delivered_position = position;
    }
    if all_delivered {
        delivered_position = read_position;
        state
            .live_errors_reported
            .retain(|(reported_terminal, _)| reported_terminal != terminal);
    }
    if let Some(watch) = state.live_watches.get_mut(terminal) {
        watch.follower = delivered_position;
    }
}

fn forget_activity_message(state: &mut BridgeState, pane_id: &str) {
    state.activity_messages.remove(pane_id);
}

async fn handle_activity_event(
    discord: &DiscordConnection,
    frame: ActivityFrame,
    state: &mut BridgeState,
) {
    let (client, _, _, responder, _) = discord;
    let route = TopologyRoute {
        workspace_id: frame.workspace_id,
        tab_id: frame.tab_id,
        pane_id: frame.pane_id.clone(),
        channel_name: String::new(),
        thread_name: String::new(),
    };
    let topology_cache = responder.chat().topology_cache();
    let channel = match responder.chat().cached_route(&route).await {
        Ok(Some(channel)) => channel,
        Ok(None) => return,
        Err(error) => {
            bridge_eprintln!("activity route error for pane {}: {error}", frame.pane_id);
            return;
        }
    };
    if !state.activity_eligible_panes.contains(&frame.pane_id) {
        return;
    }
    if let Some(existing) = state.activity_messages.get_mut(&frame.pane_id) {
        let text = activity_message_text(existing.count + 1, &frame.tool, &frame.summary);
        match update_activity_message(client.as_ref(), channel, existing.message, &text).await {
            Ok(()) => existing.count += 1,
            Err(error) => {
                if error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR) {
                    *topology_cache.lock().await = None;
                }
                bridge_eprintln!(
                    "activity edit delivery error for pane {}: {error}",
                    frame.pane_id
                );
            }
        }
        return;
    }
    let text = activity_message_text(1, &frame.tool, &frame.summary);
    match deliver_activity_message(client.as_ref(), channel, &text).await {
        Ok(message) => {
            state
                .activity_messages
                .insert(frame.pane_id, ActivityMessage { message, count: 1 });
        }
        Err(error) => {
            if error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR) {
                *topology_cache.lock().await = None;
            }
            bridge_eprintln!(
                "activity create delivery error for pane {}: {error}",
                frame.pane_id
            );
        }
    }
}

fn capture_for_blocked(snapshot: &AgentSnapshot, session: &AgentSession) -> AgentLogCapture {
    std::env::var_os(ENV_HOME).map_or_else(
        || AgentLogCapture {
            message: "blocked context unavailable: HOME is not configured".to_owned(),
            failure: None,
            question: None,
            pending_questions: Vec::new(),
        },
        |home| capture_for_blocked_with_search_root(snapshot, session, Path::new(&home)),
    )
}

fn capture_for_blocked_with_search_root(
    snapshot: &AgentSnapshot,
    session: &AgentSession,
    search_root: &Path,
) -> AgentLogCapture {
    match capture_for_with_search_root(snapshot, session, search_root) {
        Ok(capture) => capture,
        Err(error) => {
            bridge_eprintln!("agent blocked-context capture error: {error}");
            AgentLogCapture {
                message: format!("blocked context unavailable: {error}"),
                failure: None,
                question: None,
                pending_questions: Vec::new(),
            }
        }
    }
}

async fn sync_startup_topology<C: Chat>(chat: &C, agents: &[AgentSnapshot], tabs: &[HerdrTab]) {
    let mut synced_tabs = HashSet::new();
    let routes = agents
        .iter()
        .filter(|agent| agent.session.is_some())
        .filter(|agent| synced_tabs.insert(agent.tab_id.clone()))
        .map(|agent| route_topology(agents, tabs, &agent.terminal_id))
        .collect();
    chat.startup_sweep(routes, || {
        match (workspace_list_result(), tab_list_result()) {
            (Ok(workspaces), Ok(live_tabs)) => Ok((
                workspaces
                    .into_iter()
                    .map(|workspace| workspace.workspace_id)
                    .collect(),
                live_tabs.into_iter().map(|tab| tab.tab_id).collect(),
            )),
            (Err(error), _) | (_, Err(error)) => Err(error),
        }
    })
    .await;
}

fn next_state_change_sequence(
    state_change_sequences: &mut HashMap<String, u64>,
    terminal: &str,
) -> u64 {
    *state_change_sequences
        .entry(terminal.to_owned())
        .and_modify(|sequence| *sequence += 1)
        .or_insert(1)
}

fn prune_departed_state(
    state: &mut BridgeState,
    current_terminals: &HashSet<String>,
    current_panes: &HashSet<String>,
    current_tabs: &HashSet<String>,
) -> Vec<(String, InformationalCard)> {
    state
        .live_watches
        .retain(|terminal, _| current_terminals.contains(terminal));
    state
        .live_errors_reported
        .retain(|(terminal, _)| current_terminals.contains(terminal));
    state
        .terminal_prompt_positions
        .retain(|terminal, _| current_terminals.contains(terminal));
    state
        .awaiting_first_log
        .retain(|terminal, _| current_terminals.contains(terminal));
    state
        .seen_without_session
        .retain(|terminal, _| current_terminals.contains(terminal));
    state
        .rename_errors_reported
        .retain(|tab_id| current_tabs.contains(tab_id));
    state
        .activity_messages
        .retain(|pane_id, _| current_panes.contains(pane_id));
    state
        .activity_eligible_panes
        .retain(|pane_id| current_panes.contains(pane_id));
    forget_departed_owner_prompt_suppressions(current_panes);
    let departed_cards = state
        .informational_cards
        .iter()
        .filter(|(terminal, _)| !current_terminals.contains(*terminal))
        .map(|(terminal, card)| (terminal.clone(), *card))
        .collect();
    state
        .informational_cards
        .retain(|terminal, _| current_terminals.contains(terminal));
    departed_cards
}

async fn discord_connection(
    topology_cache: TopologyCache,
) -> Result<(DiscordConnection, GatewayTask), Box<dyn std::error::Error>> {
    let mut environment: Vec<(&str, String)> = Vec::new();
    for name in [
        ENV_DISCORD_TOKEN,
        ENV_DISCORD_GUILD_ID,
        ENV_DISCORD_OWNER_ID,
    ] {
        match std::env::var(name) {
            Ok(value) => environment.push((name, value)),
            Err(std::env::VarError::NotPresent) => {}
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(format!("{name} is set but is not valid UTF-8").into());
            }
        }
    }
    let environment: Vec<(&str, &str)> = environment
        .iter()
        .map(|(name, value)| (*name, value.as_str()))
        .collect();
    let config = load_discord_config(&environment)?;
    let guild = Id::<GuildMarker>::new(config.guild_id.parse()?);
    let client = Arc::new(Client::builder().token(config.token.clone()).build());
    let identity = fetch_startup_owner_identity(&client, &config.owner_id).await?;
    let (notices_tx, notices_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        while let Ok(notice) = notices_rx.recv() {
            bridge_eprintln!("{notice}");
        }
    });
    let responder = Arc::new(PermissionResponder::new(Arc::new(Discord::new(
        Arc::clone(&client),
        guild,
        config.owner_id.clone(),
        topology_cache,
    ))));
    let gateway = tokio::spawn(drive_gateway_with_components(
        config.token,
        GatewayContext {
            client: Arc::clone(&client),
            guild,
            owner_id: config.owner_id.clone(),
            responder: Arc::clone(&responder),
        },
        notices_tx,
        component_handler(Arc::clone(&responder)),
    ));
    Ok((
        (client, guild, config.owner_id, responder, identity),
        gateway,
    ))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut args = std::env::args();
    let _program = args.next();
    match args.next().as_deref() {
        Some("hook") => {
            let args: Vec<String> = args.collect();
            return run_hook(&args).await;
        }
        Some("activity") => {
            let args: Vec<String> = args.collect();
            return run_activity(&args).await;
        }
        Some("broker") => {
            let args: Vec<String> = args.collect();
            return run_broker(&args).await;
        }
        None => {}
        Some(other) => return Err(format!("unknown subcommand: {other}").into()),
    }
    run_bridge().await
}

async fn run_hook(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let (explicit_vendor, requested_socket) = parse_hook_args(args)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let mut input = Vec::new();
    tokio::io::stdin().read_to_end(&mut input).await?;
    let Ok(interaction) = decode_hook_request(&input, explicit_vendor) else {
        if matches!(explicit_vendor, Some(PermissionVendor::Cursor)) {
            write_hook_decision(PermissionVendor::Cursor, None).await?;
        }
        return Ok(());
    };
    if matches!(interaction.vendor, PermissionVendor::Cursor)
        && cursor_agent_ancestor_argv().is_some_and(|argv| cursor_argv_forces_allow(&argv))
    {
        return write_hook_decision(PermissionVendor::Cursor, Some(&Decision::allow())).await;
    }
    let socket_path = match requested_socket
        .or_else(|| std::env::var_os("HERDR_CLAUDE_BROKER_SOCKET").map(std::path::PathBuf::from))
    {
        Some(path) => Some(path),
        None if matches!(interaction.vendor, PermissionVendor::Cursor) => None,
        None => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "hook requires HERDR_CLAUDE_BROKER_SOCKET or --socket <path>",
            )
            .into());
        }
    };
    let decision = match socket_path.as_deref() {
        Some(path) => request_decision(&interaction, path, hook_timeout()).await,
        None => None,
    };
    write_hook_decision(interaction.vendor, decision.as_ref()).await
}

fn cursor_agent_ancestor_argv() -> Option<Vec<String>> {
    let mut pid = proc_parent_pid("self")?;
    while pid > 1 {
        let entry = pid.to_string();
        let argv = proc_argv(&entry)?;
        if is_cursor_agent_argv(&argv) {
            return Some(argv);
        }
        pid = proc_parent_pid(&entry)?;
    }
    None
}

fn proc_parent_pid(entry: &str) -> Option<u32> {
    let status = std::fs::read_to_string(format!("/proc/{entry}/status")).ok()?;
    status
        .lines()
        .find_map(|line| line.strip_prefix("PPid:"))
        .and_then(|value| value.trim().parse().ok())
}

fn proc_argv(entry: &str) -> Option<Vec<String>> {
    let cmdline = std::fs::read(format!("/proc/{entry}/cmdline")).ok()?;
    Some(
        cmdline
            .split(|byte| *byte == 0)
            .filter(|arg| !arg.is_empty())
            .map(|arg| String::from_utf8_lossy(arg).into_owned())
            .collect(),
    )
}

async fn write_hook_decision(
    vendor: PermissionVendor,
    decision: Option<&Decision>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(output) = encode_hook_decision(vendor, decision)? else {
        return Ok(());
    };
    let mut stdout = tokio::io::stdout();
    stdout.write_all(&output).await?;
    stdout.flush().await?;
    Ok(())
}

fn decode_hook_request(
    input: &[u8],
    vendor: Option<PermissionVendor>,
) -> Result<Interaction, String> {
    match vendor {
        Some(PermissionVendor::Claude) => decode_claude_permission_request(input),
        Some(PermissionVendor::Codex) => decode_codex_permission_request(input),
        Some(PermissionVendor::Cursor) => decode_cursor_permission_request(input),
        None => decode_claude_permission_request(input)
            .or_else(|_| decode_codex_permission_request(input))
            .or_else(|_| decode_cursor_permission_request(input)),
    }
}

fn encode_hook_decision(
    vendor: PermissionVendor,
    decision: Option<&Decision>,
) -> Result<Option<Vec<u8>>, String> {
    let Some(decision) = decision else {
        return match vendor {
            PermissionVendor::Claude | PermissionVendor::Codex => Ok(None),
            PermissionVendor::Cursor => encode_cursor_decision(&Decision::deny(
                "permission broker did not return a decision; denying by default".to_owned(),
            ))
            .map(Some),
        };
    };
    let output = match vendor {
        PermissionVendor::Claude => encode_claude_decision(decision),
        PermissionVendor::Codex => encode_codex_decision(decision),
        PermissionVendor::Cursor => encode_cursor_decision(decision)?,
    };
    Ok(Some(output))
}

fn parse_hook_args(
    args: &[String],
) -> Result<(Option<PermissionVendor>, Option<std::path::PathBuf>), String> {
    let mut vendor = None;
    let mut socket = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--vendor" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or("--vendor requires claude, codex, or cursor")?;
                vendor = Some(match value.as_str() {
                    VENDOR_CLAUDE => PermissionVendor::Claude,
                    VENDOR_CODEX => PermissionVendor::Codex,
                    VENDOR_CURSOR => PermissionVendor::Cursor,
                    _ => return Err("--vendor requires claude, codex, or cursor".to_owned()),
                });
            }
            "--socket" => {
                index += 1;
                let value = args
                    .get(index)
                    .filter(|value| !value.is_empty())
                    .ok_or("--socket requires a path")?;
                socket = Some(std::path::PathBuf::from(value));
            }
            argument => return Err(format!("unknown hook argument: {argument}")),
        }
        index += 1;
    }
    Ok((vendor, socket))
}

async fn run_activity(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let Ok((vendor, requested_socket)) = parse_activity_args(args) else {
        return Ok(());
    };
    let mut input = Vec::new();
    tokio::io::stdin().read_to_end(&mut input).await?;
    let (decoded, vendor) = match vendor {
        PermissionVendor::Claude => (decode_claude_activity_request(&input), VENDOR_CLAUDE),
        PermissionVendor::Codex => (decode_codex_activity_request(&input), VENDOR_CODEX),
        PermissionVendor::Cursor => (decode_cursor_activity_request(&input), VENDOR_CURSOR),
    };
    let Ok(request) = decoded else {
        return Ok(());
    };
    let Some(socket_path) = requested_socket
        .or_else(|| std::env::var_os("HERDR_CLAUDE_BROKER_SOCKET").map(std::path::PathBuf::from))
    else {
        return Ok(());
    };
    let workspace_id = std::env::var("HERDR_WORKSPACE_ID")?;
    let tab_id = std::env::var("HERDR_TAB_ID")?;
    let pane_id = std::env::var("HERDR_PANE_ID")?;
    let frame = ActivityFrame {
        kind: ACTIVITY_KIND.to_owned(),
        vendor: vendor.to_owned(),
        workspace_id,
        tab_id,
        pane_id,
        session_id: request.session_id,
        tool: request.tool,
        summary: request.summary,
    };
    send_activity_frame(&frame, &socket_path, Duration::from_secs(1)).await;
    Ok(())
}

fn parse_activity_args(
    args: &[String],
) -> Result<(PermissionVendor, Option<std::path::PathBuf>), String> {
    let mut vendor = None;
    let mut socket = None;
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--vendor" => {
                index += 1;
                let value = args
                    .get(index)
                    .ok_or("--vendor requires claude, codex, or cursor")?;
                vendor = Some(match value.as_str() {
                    VENDOR_CLAUDE => PermissionVendor::Claude,
                    VENDOR_CODEX => PermissionVendor::Codex,
                    VENDOR_CURSOR => PermissionVendor::Cursor,
                    _ => return Err("--vendor requires claude, codex, or cursor".to_owned()),
                });
            }
            "--socket" => {
                index += 1;
                let value = args
                    .get(index)
                    .filter(|value| !value.is_empty())
                    .ok_or("--socket requires a path")?;
                socket = Some(std::path::PathBuf::from(value));
            }
            argument => return Err(format!("unknown activity argument: {argument}")),
        }
        index += 1;
    }
    Ok((
        vendor.ok_or("activity requires --vendor claude, codex, or cursor")?,
        socket,
    ))
}

async fn run_broker(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let socket_path =
        socket_path(args).ok_or("broker requires HERDR_CLAUDE_BROKER_SOCKET or --socket <path>")?;
    let token = std::env::var(ENV_DISCORD_TOKEN)?;
    let guild_id = std::env::var(ENV_DISCORD_GUILD_ID)?;
    let owner_id = std::env::var(ENV_DISCORD_OWNER_ID)?;
    let config = load_discord_config(&[
        (ENV_DISCORD_TOKEN, &token),
        (ENV_DISCORD_GUILD_ID, &guild_id),
        (ENV_DISCORD_OWNER_ID, &owner_id),
    ])?;
    let guild = Id::<GuildMarker>::new(config.guild_id.parse()?);
    let client = Arc::new(Client::builder().token(config.token.clone()).build());
    let topology_cache: TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
    let responder = Arc::new(PermissionResponder::new(Arc::new(Discord::new(
        Arc::clone(&client),
        guild,
        config.owner_id.clone(),
        topology_cache,
    ))));
    let (activity_tx, activity_rx) = tokio::sync::mpsc::unbounded_channel();
    drop(activity_rx);
    run_permission_broker(&socket_path, responder, activity_tx)
        .await
        .map_err(Into::into)
}

fn socket_path(args: &[String]) -> Option<std::path::PathBuf> {
    match args {
        [flag, path] if flag == "--socket" && !path.is_empty() => {
            Some(std::path::PathBuf::from(path))
        }
        [] => std::env::var_os("HERDR_CLAUDE_BROKER_SOCKET").map(std::path::PathBuf::from),
        _ => None,
    }
}

fn start_broker(
    connection: &DiscordConnection,
    activity_tx: tokio::sync::mpsc::UnboundedSender<ActivityFrame>,
) -> Option<BrokerTask> {
    socket_path(&[]).map(|socket| {
        let responder = Arc::clone(&connection.3);
        tokio::spawn(async move {
            run_permission_broker(&socket, responder, activity_tx)
                .await
                .map_err(|error| error.to_string())
        })
    })
}

fn abort_broker(broker: &mut Option<BrokerTask>) {
    if let Some(broker) = broker.as_mut() {
        broker.abort();
    }
}

fn pane_ids_from_agents(agents: &[AgentSnapshot]) -> Vec<String> {
    let mut ids: Vec<String> = agents.iter().map(|agent| agent.pane_id.clone()).collect();
    ids.sort();
    ids.dedup();
    ids
}

#[derive(Debug, PartialEq, Eq)]
enum Membership {
    Add(String),
    Remove(String),
}

fn lifecycle_membership(event: &serde_json::Value) -> Option<Membership> {
    match event.get(EVENT_KEY)?.as_str()? {
        "pane_created" => event
            .pointer("/data/pane/pane_id")
            .and_then(serde_json::Value::as_str)
            .map(|id| Membership::Add(id.to_owned())),
        "pane_closed" => event
            .pointer("/data/pane_id")
            .and_then(serde_json::Value::as_str)
            .map(|id| Membership::Remove(id.to_owned())),
        "pane_agent_detected" => {
            let pane_id = event
                .pointer("/data/pane_id")
                .and_then(serde_json::Value::as_str)?;
            if event
                .pointer("/data/released")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
            {
                Some(Membership::Remove(pane_id.to_owned()))
            } else {
                Some(Membership::Add(pane_id.to_owned()))
            }
        }
        _ => None,
    }
}

fn lifecycle_closure(event: &serde_json::Value) -> Option<TopologyClosure> {
    match event.get(EVENT_KEY)?.as_str()? {
        "tab_closed" => Some(TopologyClosure::Tab {
            workspace_id: event.pointer("/data/workspace_id")?.as_str()?.to_owned(),
            tab_id: event.pointer("/data/tab_id")?.as_str()?.to_owned(),
        }),
        "workspace_closed" => Some(TopologyClosure::Workspace {
            workspace_id: event.pointer("/data/workspace_id")?.as_str()?.to_owned(),
        }),
        _ => None,
    }
}

fn apply_membership(pane_ids: &mut Vec<String>, change: Membership) -> bool {
    match change {
        Membership::Add(id) => {
            if let Err(index) = pane_ids.binary_search(&id) {
                pane_ids.insert(index, id);
                true
            } else {
                false
            }
        }
        Membership::Remove(id) => pane_ids.binary_search(&id).is_ok_and(|index| {
            pane_ids.remove(index);
            true
        }),
    }
}

fn reconcile_pane_ids(current: &mut Vec<String>, from_snapshot: Vec<String>) -> bool {
    if *current == from_snapshot {
        false
    } else {
        *current = from_snapshot;
        true
    }
}

#[derive(Debug)]
struct BridgeInterrupt;

impl std::fmt::Display for BridgeInterrupt {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "bridge interrupted")
    }
}

impl std::error::Error for BridgeInterrupt {}

fn unwrap_or_shutdown<T>(
    result: Result<T, BridgeInterrupt>,
    broker: &mut Option<BrokerTask>,
) -> Option<T> {
    result.map_or_else(
        |_| {
            abort_broker(broker);
            None
        },
        Some,
    )
}

struct BridgeRuntime {
    lifecycle: HerdrSubscription,
    pane_ids: Vec<String>,
    status: Option<HerdrSubscription>,
    state: BridgeState,
    live_events: tokio::sync::mpsc::UnboundedReceiver<String>,
    activity_events: tokio::sync::mpsc::UnboundedReceiver<ActivityFrame>,
}

async fn doorbell_unless_shutdown(
    discord: &DiscordConnection,
    state: &mut BridgeState,
    pane_ids: &mut Vec<String>,
    status: &mut Option<HerdrSubscription>,
    stop: &mut tokio::signal::unix::Signal,
    broker: &mut Option<BrokerTask>,
) -> bool {
    unwrap_or_shutdown(
        doorbell_snapshot(discord, state, pane_ids, status, stop).await,
        broker,
    )
    .is_some()
}

async fn bridge_event_loop(
    discord: &DiscordConnection,
    gateway: &mut GatewayTask,
    broker: &mut Option<BrokerTask>,
    stop: &mut tokio::signal::unix::Signal,
    runtime: &mut BridgeRuntime,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        tokio::select! {
            result = runtime.lifecycle.next_event() => {
                if !handle_lifecycle_select_result(result, discord, stop, broker, runtime).await {
                    break;
                }
            }
            result = next_status_event(&mut runtime.status) => {
                if !handle_status_select_result(result, discord, stop, broker, runtime).await {
                    break;
                }
            }
            Some(terminal) = runtime.live_events.recv() => {
                handle_live_event(discord, &terminal, &mut runtime.state).await;
            }
            Some(frame) = runtime.activity_events.recv() => {
                handle_activity_event(discord, frame, &mut runtime.state).await;
            }
            _ = tokio::signal::ctrl_c() => break,
            _ = stop.recv() => break,
            result = wait_for_gateway(gateway) => {
                return result.map_err(Into::into);
            }
            result = wait_for_broker(broker.as_mut()) => {
                return result.map_err(Into::into);
            }
        }
    }
    Ok(())
}

async fn apply_lifecycle_event(
    event: &serde_json::Value,
    discord: &DiscordConnection,
    stop: &mut tokio::signal::unix::Signal,
    broker: &mut Option<BrokerTask>,
    runtime: &mut BridgeRuntime,
) -> bool {
    if let Some(change) = lifecycle_membership(event)
        && apply_membership(&mut runtime.pane_ids, change)
    {
        let Some(next_status) = unwrap_or_shutdown(
            subscribe_status_with_backoff(&mut runtime.pane_ids, stop).await,
            broker,
        ) else {
            return false;
        };
        runtime.status = next_status;
    }

    if let Some(closure) = lifecycle_closure(event)
        && let Err(error) = discord.3.chat().close_topology(&closure).await
    {
        bridge_eprintln!("herdr topology closure error: {error}");
    }

    doorbell_unless_shutdown(
        discord,
        &mut runtime.state,
        &mut runtime.pane_ids,
        &mut runtime.status,
        stop,
        broker,
    )
    .await
}

fn spawn_archived_thread_registration(discord: &DiscordConnection) {
    let chat = discord.3.chat().clone();
    tokio::spawn(async move {
        if let Err(error) = chat.register_archived().await {
            bridge_eprintln!(
                "herdr archived thread registration FAILED, owner deletion of an archived tab thread may be ignored: {error}"
            );
        }
    });
}

fn spawn_startup_topology_sweep(
    discord: &DiscordConnection,
    rename_errors_reported: &mut HashSet<String>,
) {
    match list_agents().and_then(|agents| tab_list_result().map(|tabs| (agents, tabs))) {
        Ok((agents, mut tabs)) => {
            for error in name_unlabeled_tabs(&agents, &mut tabs, rename_errors_reported) {
                bridge_eprintln!("herdr startup topology error: {error}");
            }
            let chat = discord.3.chat().clone();
            let startup_task =
                tokio::spawn(
                    async move { sync_startup_topology(chat.as_ref(), &agents, &tabs).await },
                );
            tokio::spawn(async move {
                if let Err(error) = startup_task.await {
                    bridge_eprintln!("herdr startup topology task error: {error}");
                }
            });
        }
        Err(error) => bridge_eprintln!("herdr startup topology snapshot error: {error}"),
    }
}

async fn handle_lifecycle_subscribe_error(
    error: String,
    discord: &DiscordConnection,
    stop: &mut tokio::signal::unix::Signal,
    broker: &mut Option<BrokerTask>,
    runtime: &mut BridgeRuntime,
) -> bool {
    bridge_eprintln!("herdr lifecycle subscribe error: {error}");
    let Some(next_lifecycle) = unwrap_or_shutdown(
        subscribe_herdr_events_with_backoff(&lifecycle_subscriptions(), stop).await,
        broker,
    ) else {
        return false;
    };
    runtime.lifecycle = next_lifecycle;
    doorbell_unless_shutdown(
        discord,
        &mut runtime.state,
        &mut runtime.pane_ids,
        &mut runtime.status,
        stop,
        broker,
    )
    .await
}

async fn handle_lifecycle_select_result(
    result: Result<serde_json::Value, String>,
    discord: &DiscordConnection,
    stop: &mut tokio::signal::unix::Signal,
    broker: &mut Option<BrokerTask>,
    runtime: &mut BridgeRuntime,
) -> bool {
    match result {
        Ok(event) => apply_lifecycle_event(&event, discord, stop, broker, runtime).await,
        Err(error) => handle_lifecycle_subscribe_error(error, discord, stop, broker, runtime).await,
    }
}

async fn handle_status_select_result(
    result: Result<serde_json::Value, String>,
    discord: &DiscordConnection,
    stop: &mut tokio::signal::unix::Signal,
    broker: &mut Option<BrokerTask>,
    runtime: &mut BridgeRuntime,
) -> bool {
    match result {
        Ok(_event) => {
            doorbell_unless_shutdown(
                discord,
                &mut runtime.state,
                &mut runtime.pane_ids,
                &mut runtime.status,
                stop,
                broker,
            )
            .await
        }
        Err(error) => {
            bridge_eprintln!("herdr status subscribe error: {error}");
            let Some(next_status) = unwrap_or_shutdown(
                subscribe_status_with_backoff(&mut runtime.pane_ids, stop).await,
                broker,
            ) else {
                return false;
            };
            runtime.status = next_status;
            doorbell_unless_shutdown(
                discord,
                &mut runtime.state,
                &mut runtime.pane_ids,
                &mut runtime.status,
                stop,
                broker,
            )
            .await
        }
    }
}

async fn subscribe_herdr_events_with_backoff(
    subscriptions: &[serde_json::Value],
    stop: &mut tokio::signal::unix::Signal,
) -> Result<HerdrSubscription, BridgeInterrupt> {
    let mut delay = SUBSCRIBE_RETRY_INITIAL;
    loop {
        match subscribe_herdr_events(subscriptions).await {
            Ok(subscription) => return Ok(subscription),
            Err(error) => {
                bridge_eprintln!("herdr subscribe error: {error}; retrying in {delay:?}");
                tokio::select! {
                    () = tokio::time::sleep(delay) => {
                        delay = delay.saturating_mul(2).min(SUBSCRIBE_RETRY_MAX);
                    }
                    _ = tokio::signal::ctrl_c() => return Err(BridgeInterrupt),
                    _ = stop.recv() => return Err(BridgeInterrupt),
                }
            }
        }
    }
}

async fn subscribe_status_with_backoff(
    pane_ids: &mut Vec<String>,
    stop: &mut tokio::signal::unix::Signal,
) -> Result<Option<HerdrSubscription>, BridgeInterrupt> {
    let mut delay = SUBSCRIBE_RETRY_INITIAL;
    loop {
        let outcome: Result<Option<HerdrSubscription>, String> = async {
            let agents = list_agents()?;
            reconcile_pane_ids(pane_ids, pane_ids_from_agents(&agents));
            if pane_ids.is_empty() {
                return Ok(None);
            }
            let subscription = subscribe_herdr_events(&status_subscriptions(pane_ids)).await?;
            Ok(Some(subscription))
        }
        .await;
        match outcome {
            Ok(result) => return Ok(result),
            Err(error) => {
                bridge_eprintln!("herdr status subscribe error: {error}; retrying in {delay:?}");
                tokio::select! {
                    () = tokio::time::sleep(delay) => {
                        delay = delay.saturating_mul(2).min(SUBSCRIBE_RETRY_MAX);
                    }
                    _ = tokio::signal::ctrl_c() => return Err(BridgeInterrupt),
                    _ = stop.recv() => return Err(BridgeInterrupt),
                }
            }
        }
    }
}

async fn apply_herdr_snapshot(
    discord: &DiscordConnection,
    state: &mut BridgeState,
) -> Result<Vec<String>, String> {
    let agents = list_agents()?;
    let mut tabs = tab_list_result()?;
    for error in name_unlabeled_tabs(&agents, &mut tabs, &mut state.rename_errors_reported) {
        bridge_eprintln!("{error}");
    }
    let current_terminals: HashSet<String> = agents.iter().map(|s| s.terminal_id.clone()).collect();
    let current_tabs: HashSet<String> = tabs.iter().map(|tab| tab.tab_id.clone()).collect();
    let current_panes: HashSet<String> = agents.iter().map(|s| s.pane_id.clone()).collect();
    state
        .previous
        .retain(|terminal, _| current_terminals.contains(terminal));
    let departed_cards =
        prune_departed_state(state, &current_terminals, &current_panes, &current_tabs);
    for (terminal, card) in departed_cards {
        expire_departed_card(discord, &terminal, card).await;
    }
    for snapshot in &agents {
        process_snapshot(snapshot, &agents, &tabs, discord, state).await;
    }
    Ok(pane_ids_from_agents(&agents))
}

async fn doorbell_snapshot(
    discord: &DiscordConnection,
    state: &mut BridgeState,
    pane_ids: &mut Vec<String>,
    status: &mut Option<HerdrSubscription>,
    stop: &mut tokio::signal::unix::Signal,
) -> Result<(), BridgeInterrupt> {
    match apply_herdr_snapshot(discord, state).await {
        Ok(from_snapshot) => {
            if reconcile_pane_ids(pane_ids, from_snapshot) {
                *status = subscribe_status_with_backoff(pane_ids, stop).await?;
            }
        }
        Err(error) => bridge_eprintln!("herdr snapshot error: {error}"),
    }
    Ok(())
}

async fn next_status_event(
    status: &mut Option<HerdrSubscription>,
) -> Result<serde_json::Value, String> {
    match status.as_mut() {
        Some(stream) => stream.next_event().await,
        None => std::future::pending().await,
    }
}

async fn fetch_startup_owner_identity(
    client: &Client,
    owner_id: &str,
) -> Result<OwnerIdentity, String> {
    let owner_id = owner_id.parse::<u64>().map_err(|error| {
        format!("owner identity fetch error: DISCORD_OWNER_ID is not numeric: {error}")
    })?;
    fetch_owner_identity(client, Id::<UserMarker>::new(owner_id))
        .await
        .map_err(|error| format!("owner identity fetch error: {error}"))
}

async fn run_bridge() -> Result<(), Box<dyn std::error::Error>> {
    let topology_cache: TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
    let (activity_tx, activity_events) = tokio::sync::mpsc::unbounded_channel();
    let (connection, mut gateway) = discord_connection(Arc::clone(&topology_cache)).await?;
    let mut broker = start_broker(&connection, activity_tx);
    let (live_tx, live_events) = tokio::sync::mpsc::unbounded_channel();
    let state = BridgeState {
        live_tx: Some(live_tx),
        ..BridgeState::default()
    };
    let mut stop = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let Some(lifecycle) = unwrap_or_shutdown(
        subscribe_herdr_events_with_backoff(&lifecycle_subscriptions(), &mut stop).await,
        &mut broker,
    ) else {
        return Ok(());
    };
    let mut pane_ids = Vec::new();
    let Some(status) = unwrap_or_shutdown(
        subscribe_status_with_backoff(&mut pane_ids, &mut stop).await,
        &mut broker,
    ) else {
        return Ok(());
    };
    let mut runtime = BridgeRuntime {
        lifecycle,
        pane_ids,
        status,
        state,
        live_events,
        activity_events,
    };
    if !doorbell_unless_shutdown(
        &connection,
        &mut runtime.state,
        &mut runtime.pane_ids,
        &mut runtime.status,
        &mut stop,
        &mut broker,
    )
    .await
    {
        return Ok(());
    }
    spawn_archived_thread_registration(&connection);
    spawn_startup_topology_sweep(&connection, &mut runtime.state.rename_errors_reported);
    bridge_event_loop(
        &connection,
        &mut gateway,
        &mut broker,
        &mut stop,
        &mut runtime,
    )
    .await?;
    abort_broker(&mut broker);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use rusqlite::Connection;
    use serde_json::{Value, json};
    use serial_test::serial;
    use twilight_model::id::{
        Id,
        marker::{ChannelMarker, GuildMarker, MessageMarker, UserMarker},
    };

    use super::{
        BridgeRuntime, BridgeState, BrokerTask, Client, Discord, Follower, Membership,
        PermissionResponder, SessionPathError, TopologyClosure, TopologyRoute,
        agent_read_detection, apply_membership, capture_for_with_search_root,
        create_transition_messages, doorbell_snapshot, fetch_startup_owner_identity,
        handle_lifecycle_select_result, handle_live_event, initial_terminal_prompt_position,
        lifecycle_closure, lifecycle_membership, list_agents, live_log_path,
        maybe_establish_terminal_prompt_baseline, next_state_change_sequence, process_snapshot,
        prune_departed_state, read_new_terminal_prompts, resolve_session_path, route_topology,
        subscribe_status_with_backoff, sync_startup_topology, tab_list_result,
        terminal_prompt_baseline_is_current, unique_existing_path,
    };
    use herdr_connect_rs::{
        AgentSession, AgentSnapshot, Chat, STATUS_DONE, STATUS_IDLE, STATUS_WORKING, Transition,
        VENDOR_CLAUDE, VENDOR_CODEX, VENDOR_CURSOR, fetch_topology_lists, lifecycle_subscriptions,
        name_unlabeled_tabs, read_claude_incremental, read_codex_incremental,
        read_cursor_incremental, status_subscriptions, submit_owner_prompt, subscribe_herdr_events,
        sync_route, transition_card_nonce, workspace_list_result,
    };

    #[test]
    fn claude_terminal_prompt_position_discards_existing_prompts_and_reads_one_append_once() {
        let path = std::env::temp_dir().join(format!(
            "herdr-connect-rs-terminal-prompts-{}.jsonl",
            std::process::id()
        ));
        let test_result = std::panic::catch_unwind(|| {
            fs::copy("tests/fixtures/claude-session.jsonl", &path)
                .expect("copy committed Claude fixture");

            let initial_position = initial_terminal_prompt_position(Follower::Claude(0), &path)
                .expect("initial Claude terminal prompt position resolves");
            assert!(matches!(initial_position, Follower::Claude(1_177)));

            fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .and_then(|mut file| {
                    file.write_all(
                        b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"terminal-direct\"}]}}\n",
                    )
                })
                .expect("append real-schema Claude user record");

            let (prompts, checkpoint) = read_new_terminal_prompts(&path, initial_position)
                .expect("read appended Claude terminal prompt");
            assert_eq!(prompts, vec!["terminal-direct".to_owned()]);
            assert!(matches!(checkpoint, Follower::Claude(1_272)));

            let (repeated_prompts, repeated_checkpoint) =
                read_new_terminal_prompts(&path, checkpoint)
                    .expect("repeat Claude terminal prompt read");
            assert!(repeated_prompts.is_empty());
            assert!(matches!(repeated_checkpoint, Follower::Claude(1_272)));
        });
        let cleanup = fs::remove_file(&path);
        assert!(
            cleanup.is_ok(),
            "remove temporary Claude fixture: {cleanup:?}"
        );
        if let Err(payload) = test_result {
            std::panic::resume_unwind(payload);
        }
    }

    #[test]
    fn codex_terminal_prompt_position_discards_existing_prompts_and_reads_one_append_once() {
        let path = std::env::temp_dir().join(format!(
            "herdr-connect-rs-codex-terminal-prompts-{}.jsonl",
            std::process::id()
        ));
        let test_result = std::panic::catch_unwind(|| {
            fs::copy("tests/fixtures/codex-session-prompt-twin.jsonl", &path)
                .expect("copy committed Codex fixture");

            let initial_position = initial_terminal_prompt_position(Follower::Codex(0), &path)
                .expect("initial Codex terminal prompt position resolves");
            assert!(matches!(initial_position, Follower::Codex(1_215)));

            fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .and_then(|mut file| {
                    file.write_all(
                        b"{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"terminal-direct\"}]}}\n\
                          {\"type\":\"event_msg\",\"payload\":{\"type\":\"user_message\",\"message\":\"terminal-direct\"}}\n",
                    )
                })
                .expect("append real-schema Codex twin user record");

            let (prompts, checkpoint) = read_new_terminal_prompts(&path, initial_position)
                .expect("read appended Codex terminal prompt");
            assert_eq!(prompts, vec!["terminal-direct".to_owned()]);
            assert!(matches!(checkpoint, Follower::Codex(1_425)));

            let (repeated_prompts, repeated_checkpoint) =
                read_new_terminal_prompts(&path, checkpoint)
                    .expect("repeat Codex terminal prompt read");
            assert!(repeated_prompts.is_empty());
            assert!(matches!(repeated_checkpoint, Follower::Codex(1_425)));
        });
        let cleanup = fs::remove_file(&path);
        assert!(
            cleanup.is_ok(),
            "remove temporary Codex fixture: {cleanup:?}"
        );
        if let Err(payload) = test_result {
            std::panic::resume_unwind(payload);
        }
    }

    #[test]
    fn cursor_terminal_prompt_position_discards_existing_prompts_and_reads_one_append_once() {
        let path = std::env::temp_dir().join(format!(
            "herdr-connect-rs-cursor-terminal-prompts-{}.db",
            std::process::id()
        ));
        let _ = fs::remove_file(&path);
        let connection = Connection::open(&path).expect("create cursor store");
        connection
            .execute("CREATE TABLE blobs (data BLOB)", [])
            .expect("create blobs table");
        let rows: Vec<Value> = serde_json::from_str(
            &fs::read_to_string("tests/fixtures/cursor-session.json")
                .expect("read committed Cursor fixture"),
        )
        .expect("parse committed Cursor fixture");
        for row in rows {
            let bytes = serde_json::to_vec(&row).expect("encode Cursor row");
            connection
                .execute("INSERT INTO blobs (data) VALUES (?1)", [bytes])
                .expect("insert Cursor row");
        }
        drop(connection);

        let test_result =
            std::panic::catch_unwind(|| {
                let initial_position = initial_terminal_prompt_position(Follower::Cursor(0), &path)
                    .expect("initial Cursor terminal prompt position resolves");
                assert!(matches!(initial_position, Follower::Cursor(6)));

                let connection = Connection::open(&path).expect("reopen cursor store");
                connection
                .execute(
                    "INSERT INTO blobs (data) VALUES (?1)",
                    [br#"{"role":"user","content":[{"type":"text","text":"terminal-direct"}]}"#
                        .as_slice()],
                )
                .expect("append real-schema Cursor user row");
                drop(connection);

                let (prompts, checkpoint) = read_new_terminal_prompts(&path, initial_position)
                    .expect("read appended Cursor terminal prompt");
                assert_eq!(prompts, vec!["terminal-direct".to_owned()]);
                assert!(matches!(checkpoint, Follower::Cursor(7)));

                let (repeated_prompts, repeated_checkpoint) =
                    read_new_terminal_prompts(&path, checkpoint)
                        .expect("repeat Cursor terminal prompt read");
                assert!(repeated_prompts.is_empty());
                assert!(matches!(repeated_checkpoint, Follower::Cursor(7)));
            });
        let cleanup = fs::remove_file(&path);
        assert!(
            cleanup.is_ok(),
            "remove temporary Cursor fixture: {cleanup:?}"
        );
        if let Err(payload) = test_result {
            std::panic::resume_unwind(payload);
        }
    }

    #[test]
    fn terminal_prompt_baseline_is_current_only_for_the_exact_stored_path() {
        let mut positions = HashMap::new();
        let terminal = "terminal-1";
        let first_path = PathBuf::from("/tmp/session-a.jsonl");
        let second_path = PathBuf::from("/tmp/session-b.jsonl");

        assert!(
            !terminal_prompt_baseline_is_current(&positions, terminal, &first_path),
            "no baseline yet must never read as current"
        );

        positions.insert(
            terminal.to_owned(),
            (
                first_path.clone(),
                Follower::Claude(42),
                Follower::Claude(42),
            ),
        );
        assert!(
            terminal_prompt_baseline_is_current(&positions, terminal, &first_path),
            "the exact path a baseline was stored for must read as current"
        );
        assert!(
            !terminal_prompt_baseline_is_current(&positions, terminal, &second_path),
            "a session change (a different resolved path) must not reuse the old baseline"
        );
        assert!(
            !terminal_prompt_baseline_is_current(&positions, "terminal-2", &first_path),
            "a different terminal's baseline must not be reused either"
        );
    }

    #[test]
    fn resumed_session_with_existing_log_is_never_replayed_after_a_fresh_session_was_pending() {
        let original_home = std::env::var_os("HOME");
        let temp_home = std::env::temp_dir().join(format!(
            "herdr-connect-rs-resume-replay-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&temp_home);
        fs::create_dir_all(&temp_home).expect("create temp HOME");
        unsafe {
            std::env::set_var("HOME", &temp_home);
        }

        let test_result = std::panic::catch_unwind(|| {
            let cwd_dir = temp_home.join("project");
            fs::create_dir_all(&cwd_dir).expect("create project cwd");
            let cwd = cwd_dir.to_str().expect("utf8 cwd").to_owned();
            let cwd_slug: String = cwd
                .chars()
                .map(|character| {
                    if character.is_ascii_alphanumeric() {
                        character
                    } else {
                        '-'
                    }
                })
                .collect();
            let session_dir = temp_home.join(".claude").join("projects").join(&cwd_slug);
            fs::create_dir_all(&session_dir).expect("create Claude session dir");

            let old_session_path = session_dir.join("old-session.jsonl");
            fs::write(
                &old_session_path,
                "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"history one\"}]}}\n\
                 {\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"history two\"}]}}\n\
                 {\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"history three\"}]}}\n",
            )
            .expect("write existing Claude session log");
            let expected_discard_position = fs::metadata(&old_session_path)
                .expect("stat existing Claude session log")
                .len();

            let snapshot_for = |session_value: &str| AgentSnapshot {
                agent: Some(VENDOR_CLAUDE.to_owned()),
                terminal_id: "terminal-resume".to_owned(),
                agent_status: STATUS_IDLE.to_owned(),
                tab_id: "w1:t1".to_owned(),
                workspace_id: "w1".to_owned(),
                pane_id: "w1:p1".to_owned(),
                cwd: Some(cwd.clone()),
                session: Some(AgentSession {
                    agent: VENDOR_CLAUDE.to_owned(),
                    value: session_value.to_owned(),
                }),
            };

            let mut replay_state = BridgeState::default();
            maybe_establish_terminal_prompt_baseline(&snapshot_for("fresh-b"), &mut replay_state);
            maybe_establish_terminal_prompt_baseline(
                &snapshot_for("old-session"),
                &mut replay_state,
            );
            let (replay_path, replay_prompt_position, replay_live_position) = replay_state
                .terminal_prompt_positions
                .get("terminal-resume")
                .expect("resumed session baselines once its log resolves");
            assert_eq!(replay_path, &old_session_path);
            assert_eq!(
                *replay_prompt_position,
                Follower::Claude(expected_discard_position),
                "a resumed session with prior history must discard it, not replay it"
            );
            assert_eq!(
                *replay_live_position,
                Follower::Claude(expected_discard_position),
                "the live watch start must discard the resumed history too, not replay it"
            );

            let mut control_state = BridgeState::default();
            maybe_establish_terminal_prompt_baseline(
                &snapshot_for("old-session"),
                &mut control_state,
            );
            let (control_path, control_prompt_position, control_live_position) = control_state
                .terminal_prompt_positions
                .get("terminal-resume")
                .expect("baseline established for the control case");
            assert_eq!(control_path, &old_session_path);
            assert_eq!(
                (replay_prompt_position, replay_live_position),
                (control_prompt_position, control_live_position),
                "case b and control case c must baseline identically"
            );
        });

        match original_home {
            Some(value) => unsafe { std::env::set_var("HOME", value) },
            None => unsafe { std::env::remove_var("HOME") },
        }
        let _ = fs::remove_dir_all(&temp_home);
        if let Err(payload) = test_result {
            std::panic::resume_unwind(payload);
        }
    }

    struct FirstTurnCase {
        name: &'static str,
        log_predates_first_sighting: bool,
        another_terminal_follows_the_log: bool,
        expect_mirrored: bool,
    }

    fn first_turn_cases() -> [FirstTurnCase; 3] {
        [
            FirstTurnCase {
                name: "log created after the sessionless sighting",
                log_predates_first_sighting: false,
                another_terminal_follows_the_log: false,
                expect_mirrored: true,
            },
            FirstTurnCase {
                name: "log created before the sessionless sighting",
                log_predates_first_sighting: true,
                another_terminal_follows_the_log: false,
                expect_mirrored: false,
            },
            FirstTurnCase {
                name: "log created after the sighting but followed by another terminal",
                log_predates_first_sighting: false,
                another_terminal_follows_the_log: true,
                expect_mirrored: false,
            },
        ]
    }

    fn read_both(
        path: &Path,
        prompt_position: Follower,
        live_position: Follower,
    ) -> (Vec<String>, Vec<String>) {
        let (prompts, _) = read_new_terminal_prompts(path, prompt_position).expect("read prompts");
        let (texts, _) = super::read_new_live_texts(path, live_position).expect("read texts");
        (prompts, texts.into_iter().map(|(text, _)| text).collect())
    }

    fn assert_later_codex_session_discards_history(
        state: &mut BridgeState,
        temp_home: &Path,
        later: &AgentSnapshot,
        name: &str,
    ) {
        let continuation = "rollout-2026-09-28T12-38-56-00000000-0000-7000-8000-000000000001_00000000-0000-7000-8000-000000000002.jsonl";
        let continuation_path = temp_home
            .join(".codex-one/sessions/2026/09/28")
            .join(continuation);
        fs::copy(
            Path::new("tests/fixtures/codex-rollouts").join(continuation),
            &continuation_path,
        )
        .expect("copy committed Codex continuation fixture");
        maybe_establish_terminal_prompt_baseline(later, state);
        let (path, prompt_position, live_position) = state
            .terminal_prompt_positions
            .get(&later.terminal_id)
            .unwrap_or_else(|| panic!("{name}: second baseline established"));
        assert_eq!(path, &continuation_path, "{name}");
        let (prompts, texts) = read_both(path, *prompt_position, *live_position);
        assert!(
            prompts.is_empty() && texts.is_empty(),
            "{name}: a later session must discard its history: {prompts:?} {texts:?}"
        );
    }

    #[test]
    fn codex_log_created_after_the_pane_was_first_seen_without_a_session_is_read_from_zero() {
        let cases = first_turn_cases();
        let original_home = std::env::var_os("HOME");
        let temp_home = std::env::temp_dir().join(format!(
            "herdr-connect-rs-codex-first-turn-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&temp_home);
        unsafe {
            std::env::set_var("HOME", &temp_home);
        }
        let test_result = std::panic::catch_unwind(|| {
            let rollout = "rollout-2026-09-28T09-50-11-00000000-0000-7000-8000-000000000001.jsonl";
            let log_path = temp_home
                .join(".codex-one/sessions/2026/09/28")
                .join(rollout);
            let write_log = || {
                fs::create_dir_all(log_path.parent().expect("rollout has a parent"))
                    .expect("create Codex sessions directory");
                fs::copy(
                    Path::new("tests/fixtures/codex-rollouts").join(rollout),
                    &log_path,
                )
                .expect("copy committed Codex rollout fixture");
            };
            let snapshot = |session: Option<AgentSession>| AgentSnapshot {
                agent: Some(VENDOR_CODEX.to_owned()),
                terminal_id: "terminal-codex-first-turn".to_owned(),
                agent_status: STATUS_IDLE.to_owned(),
                tab_id: "w1:t1".to_owned(),
                workspace_id: "w1".to_owned(),
                pane_id: "w1:p1".to_owned(),
                cwd: Some("/srv/bridge".to_owned()),
                session,
            };
            let session = AgentSession {
                agent: VENDOR_CODEX.to_owned(),
                value: "00000000-0000-7000-8000-000000000001".to_owned(),
            };
            let tick = std::time::Duration::from_millis(50);
            for case in &cases {
                let _ = fs::remove_dir_all(&temp_home);
                let mut state = BridgeState::default();
                if case.log_predates_first_sighting {
                    write_log();
                    std::thread::sleep(tick);
                }
                maybe_establish_terminal_prompt_baseline(&snapshot(None), &mut state);
                if !case.log_predates_first_sighting {
                    std::thread::sleep(tick);
                    write_log();
                }
                if case.another_terminal_follows_the_log {
                    state.terminal_prompt_positions.insert(
                        "terminal-other".to_owned(),
                        (log_path.clone(), Follower::Codex(0), Follower::Codex(0)),
                    );
                }
                maybe_establish_terminal_prompt_baseline(
                    &snapshot(Some(session.clone())),
                    &mut state,
                );
                let (path, prompt_position, live_position) = state
                    .terminal_prompt_positions
                    .get("terminal-codex-first-turn")
                    .unwrap_or_else(|| panic!("{}: baseline established", case.name));
                assert_eq!(path, &log_path, "{}", case.name);
                let (prompts, texts) = read_both(path, *prompt_position, *live_position);
                let (expected_prompts, expected_texts) = if case.expect_mirrored {
                    (vec!["original prompt"], vec!["original answer"])
                } else {
                    (vec![], vec![])
                };
                assert_eq!(prompts, expected_prompts, "{}: prompts", case.name);
                assert_eq!(texts, expected_texts, "{}: live texts", case.name);

                std::thread::sleep(tick);
                assert_later_codex_session_discards_history(
                    &mut state,
                    &temp_home,
                    &snapshot(Some(AgentSession {
                        agent: VENDOR_CODEX.to_owned(),
                        value: "00000000-0000-7000-8000-000000000002".to_owned(),
                    })),
                    case.name,
                );
            }
        });
        match original_home {
            Some(value) => unsafe { std::env::set_var("HOME", value) },
            None => unsafe { std::env::remove_var("HOME") },
        }
        let _ = fs::remove_dir_all(&temp_home);
        if let Err(payload) = test_result {
            std::panic::resume_unwind(payload);
        }
    }

    #[tokio::test]
    async fn owner_identity_fetch_fails_startup_when_the_owner_id_is_not_numeric() {
        let client = Client::builder().token("fake-token".to_owned()).build();
        let result = fetch_startup_owner_identity(&client, "not-a-number").await;
        assert!(
            result.is_err(),
            "a non-numeric DISCORD_OWNER_ID must fail startup, not silently run without an \
             identity: {result:?}"
        );
    }

    #[tokio::test]
    async fn non_utf8_discord_variable_fails_startup_naming_the_variable() {
        use std::os::unix::ffi::OsStringExt;

        let original = std::env::var_os("DISCORD_TOKEN");
        unsafe {
            std::env::set_var(
                "DISCORD_TOKEN",
                std::ffi::OsString::from_vec(b"secret-token\xff".to_vec()),
            );
        }
        let result = super::discord_connection(Arc::new(tokio::sync::Mutex::new(None))).await;
        unsafe {
            match original {
                Some(value) => std::env::set_var("DISCORD_TOKEN", value),
                None => std::env::remove_var("DISCORD_TOKEN"),
            }
        }
        let error = result
            .err()
            .expect("a non-UTF-8 token must fail startup")
            .to_string();
        assert!(
            error.contains("DISCORD_TOKEN") && !error.contains("Missing required"),
            "unexpected error: {error}"
        );
        assert!(
            !error.contains("secret-token"),
            "the error must not print the variable's value: {error}"
        );
    }

    #[test]
    fn cursor_hook_without_a_decision_emits_a_deny_object() {
        let output = super::encode_hook_decision(super::PermissionVendor::Cursor, None)
            .expect("a missing decision must encode")
            .expect("Cursor failures must produce output");
        let value: Value = serde_json::from_slice(&output).expect("Cursor failure output is JSON");
        assert_eq!(value["permission"], "deny");
        assert_eq!(
            value["agent_message"],
            "permission broker did not return a decision; denying by default"
        );
    }

    #[test]
    fn state_change_nonce_survives_terminal_departure_and_return() {
        let terminal = "terminal";
        let mut state = BridgeState::default();

        let pre_departure_nonce = transition_card_nonce(
            terminal,
            next_state_change_sequence(&mut state.state_change_sequences, terminal),
            0,
        );

        prune_departed_state(
            &mut state,
            &HashSet::new(),
            &HashSet::new(),
            &HashSet::new(),
        );
        let returned_nonce = transition_card_nonce(
            terminal,
            next_state_change_sequence(&mut state.state_change_sequences, terminal),
            0,
        );

        assert_ne!(pre_departure_nonce, returned_nonce);
    }

    #[test]
    fn lifecycle_membership_matches_real_payload_shapes() {
        use serde_json::json;

        let created = json!({"event": "pane_created", "data": {"pane": {"pane_id": "w1:p1"}}});
        assert_eq!(
            lifecycle_membership(&created),
            Some(Membership::Add("w1:p1".to_owned()))
        );

        let closed = json!({"event": "pane_closed", "data": {"pane_id": "w1:p2"}});
        assert_eq!(
            lifecycle_membership(&closed),
            Some(Membership::Remove("w1:p2".to_owned()))
        );

        let detected = json!({
            "event": "pane_agent_detected",
            "data": {"pane_id": "w1:p3", "released": false}
        });
        assert_eq!(
            lifecycle_membership(&detected),
            Some(Membership::Add("w1:p3".to_owned()))
        );

        let released = json!({
            "event": "pane_agent_detected",
            "data": {"pane_id": "w1:p4", "released": true}
        });
        assert_eq!(
            lifecycle_membership(&released),
            Some(Membership::Remove("w1:p4".to_owned()))
        );

        let mut pane_ids = vec!["w1:p1".to_owned()];
        assert!(!apply_membership(
            &mut pane_ids,
            Membership::Add("w1:p1".to_owned())
        ));
        assert!(apply_membership(
            &mut pane_ids,
            Membership::Add("w1:p2".to_owned())
        ));
        assert_eq!(pane_ids, vec!["w1:p1".to_owned(), "w1:p2".to_owned()]);

        let tab_closed = json!({
            "event": "tab_closed",
            "data": {"tab_id": "w32:t2", "type": "tab_closed", "workspace_id": "w32"}
        });
        assert_eq!(lifecycle_membership(&tab_closed), None);
        assert_eq!(
            lifecycle_closure(&tab_closed),
            Some(TopologyClosure::Tab {
                workspace_id: "w32".to_owned(),
                tab_id: "w32:t2".to_owned(),
            })
        );

        let workspace_closed = json!({
            "event": "workspace_closed",
            "data": {
                "type": "workspace_closed",
                "workspace_id": "w32",
                "workspace": {
                    "active_tab_id": "w32:t1",
                    "agent_status": "unknown",
                    "focused": false,
                    "label": "testrun-payload-capture-2",
                    "number": 16,
                    "pane_count": 1,
                    "tab_count": 1,
                    "workspace_id": "w32"
                }
            }
        });
        assert_eq!(lifecycle_membership(&workspace_closed), None);
        assert_eq!(
            lifecycle_closure(&workspace_closed),
            Some(TopologyClosure::Workspace {
                workspace_id: "w32".to_owned(),
            })
        );
    }

    fn claude_session_snapshot(cwd: &str, session_id: &str) -> (AgentSnapshot, AgentSession) {
        let session = AgentSession {
            agent: "claude".to_owned(),
            value: session_id.to_owned(),
        };
        let snapshot = AgentSnapshot {
            agent: Some("claude".to_owned()),
            terminal_id: "claude-search-root-terminal".to_owned(),
            agent_status: "done".to_owned(),
            tab_id: "w1:t1".to_owned(),
            workspace_id: "w1".to_owned(),
            pane_id: "w1:p1".to_owned(),
            cwd: Some(cwd.to_owned()),
            session: Some(session.clone()),
        };
        (snapshot, session)
    }

    fn claude_project_dir(root: &Path, vendor_root: &str, cwd: &str) -> PathBuf {
        let cwd_slug: String = cwd
            .chars()
            .map(|character| {
                if character.is_ascii_alphanumeric() {
                    character
                } else {
                    '-'
                }
            })
            .collect();
        root.join(vendor_root).join("projects").join(cwd_slug)
    }

    struct ClaudeSearchRootCase {
        name: &'static str,
        build: fn(&Path) -> (AgentSnapshot, AgentSession, PathBuf),
    }

    #[test]
    fn claude_search_roots_cover_every_claude_config_directory_under_home() {
        let cases = [
            ClaudeSearchRootCase {
                name: "two roots: the log lives under the second, .claude-one",
                build: |root| {
                    let cwd = "/tmp/agent_workspace_v2";
                    fs::create_dir_all(claude_project_dir(root, ".claude", cwd))
                        .expect("create empty .claude project directory");
                    let directory = claude_project_dir(root, ".claude-one", cwd);
                    fs::create_dir_all(&directory).expect("create Claude project directory");
                    let expected_path = directory.join("two-roots-session.jsonl");
                    fs::write(&expected_path, "").expect("create Claude session file");
                    let (snapshot, session) = claude_session_snapshot(cwd, "two-roots-session");
                    (snapshot, session, expected_path)
                },
            },
            ClaudeSearchRootCase {
                name: "three roots: the log lives under the third, .claude-two",
                build: |root| {
                    let cwd = "/home/user/src/herdr-connect-rs";
                    for vendor_root in [".claude", ".claude-one"] {
                        fs::create_dir_all(claude_project_dir(root, vendor_root, cwd))
                            .expect("create empty Claude project directory");
                    }
                    let directory = claude_project_dir(root, ".claude-two", cwd);
                    fs::create_dir_all(&directory).expect("create Claude project directory");
                    let expected_path = directory.join("three-roots-session.jsonl");
                    fs::write(&expected_path, "").expect("create Claude session file");
                    let (snapshot, session) = claude_session_snapshot(cwd, "three-roots-session");
                    (snapshot, session, expected_path)
                },
            },
            ClaudeSearchRootCase {
                name: "the same log hard-linked across two roots resolves as one file",
                build: |root| {
                    let cwd = "/tmp/linked-workspace";
                    let primary_dir = claude_project_dir(root, ".claude", cwd);
                    fs::create_dir_all(&primary_dir).expect("create primary project directory");
                    let primary_path = primary_dir.join("linked-session.jsonl");
                    fs::write(&primary_path, "").expect("create Claude session file");
                    let secondary_dir = claude_project_dir(root, ".claude-one", cwd);
                    fs::create_dir_all(&secondary_dir).expect("create secondary project directory");
                    let secondary_path = secondary_dir.join("linked-session.jsonl");
                    fs::hard_link(&primary_path, &secondary_path)
                        .expect("hard-link session log across roots");
                    let (snapshot, session) = claude_session_snapshot(cwd, "linked-session");
                    (snapshot, session, primary_path)
                },
            },
        ];

        for (index, case) in cases.iter().enumerate() {
            let root = std::env::temp_dir().join(format!(
                "herdr-connect-rs-claude-search-roots-{}-{index}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is after unix epoch")
                    .as_nanos()
            ));
            fs::create_dir_all(&root).expect("create synthetic HOME directory");
            let (snapshot, session, expected_path) = (case.build)(&root);
            let result = resolve_session_path(&root, &snapshot, &session);
            assert_eq!(result, Ok(expected_path), "{}", case.name);
            fs::remove_dir_all(&root).expect("remove synthetic HOME directory");
        }
    }

    #[test]
    fn codex_session_rollouts_resolve_to_newest_filename_timestamp() {
        const ORIGINAL: &str =
            "rollout-2026-09-28T09-50-11-00000000-0000-7000-8000-000000000001.jsonl";
        const CONTINUATION: &str = "rollout-2026-09-28T12-38-56-00000000-0000-7000-8000-000000000001_00000000-0000-7000-8000-000000000002.jsonl";
        const THIRD_CONTINUATION: &str = "rollout-2026-09-28T18-20-05-00000000-0000-7000-8000-000000000001_00000000-0000-7000-8000-000000000004.jsonl";
        const SECOND_CONTINUATION: &str = "rollout-2026-09-29T15-43-21-00000000-0000-7000-8000-000000000001_00000000-0000-7000-8000-000000000003.jsonl";
        let cases = [
            ("single file", vec![ORIGINAL], ORIGINAL, "original"),
            (
                "original plus continuation",
                vec![CONTINUATION, ORIGINAL],
                CONTINUATION,
                "continuation",
            ),
            (
                "continuation only",
                vec![CONTINUATION],
                CONTINUATION,
                "continuation",
            ),
            (
                "original plus two continuations across days",
                vec![ORIGINAL, CONTINUATION, SECOND_CONTINUATION],
                SECOND_CONTINUATION,
                "second continuation",
            ),
            (
                "original plus later-day continuation",
                vec![ORIGINAL, SECOND_CONTINUATION],
                SECOND_CONTINUATION,
                "second continuation",
            ),
            (
                "original plus two continuations in one day",
                vec![ORIGINAL, CONTINUATION, THIRD_CONTINUATION],
                THIRD_CONTINUATION,
                "third continuation",
            ),
            (
                "two continuations in one day",
                vec![CONTINUATION, THIRD_CONTINUATION],
                THIRD_CONTINUATION,
                "third continuation",
            ),
            (
                "two continuations",
                vec![SECOND_CONTINUATION, CONTINUATION],
                SECOND_CONTINUATION,
                "second continuation",
            ),
        ];
        for (index, (name, files, expected_file, label)) in cases.into_iter().enumerate() {
            assert_codex_rollout_choice(index, name, &files, expected_file, label);
        }
    }

    fn assert_codex_rollout_choice(
        index: usize,
        name: &str,
        files: &[&str],
        expected_file: &str,
        label: &str,
    ) {
        let root = std::env::temp_dir().join(format!(
            "herdr-connect-rs-codex-rollouts-{}-{index}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        let sessions = root.join(".codex-one/sessions");
        for file in files {
            let day = sessions.join(file[8..18].replace('-', "/"));
            fs::create_dir_all(&day).expect("create synthetic Codex date directory");
            fs::copy(
                Path::new("tests/fixtures/codex-rollouts").join(file),
                day.join(file),
            )
            .expect("copy committed Codex rollout fixture");
        }
        let session = AgentSession {
            agent: VENDOR_CODEX.to_owned(),
            value: "00000000-0000-7000-8000-000000000001".to_owned(),
        };
        let snapshot = AgentSnapshot {
            agent: Some(VENDOR_CODEX.to_owned()),
            terminal_id: "codex-rollout-terminal".to_owned(),
            agent_status: STATUS_DONE.to_owned(),
            tab_id: "w1:t1".to_owned(),
            workspace_id: "w1".to_owned(),
            pane_id: "w1:p1".to_owned(),
            cwd: Some("/srv/bridge".to_owned()),
            session: Some(session.clone()),
        };
        let resolved = resolve_session_path(&root, &snapshot, &session);
        let capture = capture_for_with_search_root(&snapshot, &session, &root);
        let prompts = resolved.as_ref().ok().map(|path| {
            herdr_connect_rs::read_codex_prompts_incremental(path, 0)
                .expect("read selected Codex rollout prompts")
                .0
                .into_iter()
                .map(|(prompt, _)| prompt)
                .collect::<Vec<_>>()
        });
        fs::remove_dir_all(&root).expect("remove synthetic Codex HOME directory");
        assert_eq!(
            resolved,
            Ok(sessions
                .join(expected_file[8..18].replace('-', "/"))
                .join(expected_file)),
            "{name}"
        );
        assert_eq!(
            capture.expect("selected Codex rollout resolves").message,
            format!("{label} answer"),
            "{name}"
        );
        assert_eq!(prompts, Some(vec![format!("{label} prompt")]), "{name}");
    }

    struct CodexSearchRootCase {
        name: &'static str,
        vendor_root: &'static str,
        expected_message: &'static str,
    }

    #[test]
    fn codex_session_log_is_discovered_under_custom_home_root() {
        let cases = [CodexSearchRootCase {
            name: "the log lives under .codex-one",
            vendor_root: ".codex-one",
            expected_message: "gamma",
        }];

        for (index, case) in cases.iter().enumerate() {
            let root = std::env::temp_dir().join(format!(
                "herdr-connect-rs-codex-search-roots-{}-{index}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is after unix epoch")
                    .as_nanos()
            ));
            fs::create_dir_all(root.join(".codex/sessions"))
                .expect("create default Codex sessions directory");
            let log_path = root
                .join(case.vendor_root)
                .join("sessions/2026/09/09")
                .join("rollout-2026-09-09T00-00-00-capture-session.jsonl");
            fs::create_dir_all(log_path.parent().expect("Codex log has a parent directory"))
                .expect("create custom Codex sessions directory");
            fs::write(
                &log_path,
                include_str!("../tests/fixtures/codex-session-response-item.jsonl"),
            )
            .expect("write committed Codex session fixture");

            let session = AgentSession {
                agent: VENDOR_CODEX.to_owned(),
                value: "capture-session".to_owned(),
            };
            let snapshot = AgentSnapshot {
                agent: Some(VENDOR_CODEX.to_owned()),
                terminal_id: "codex-custom-home-terminal".to_owned(),
                agent_status: STATUS_DONE.to_owned(),
                tab_id: "w1:t1".to_owned(),
                workspace_id: "w1".to_owned(),
                pane_id: "w1:p1".to_owned(),
                cwd: Some("/srv/bridge".to_owned()),
                session: Some(session.clone()),
            };

            let resolved_path = resolve_session_path(&root, &snapshot, &session);
            let capture = capture_for_with_search_root(&snapshot, &session, &root);

            fs::remove_dir_all(&root).expect("remove synthetic HOME directory");

            assert_eq!(
                resolved_path,
                Ok(log_path.clone()),
                "{}: resolve custom Codex session path",
                case.name
            );
            let capture = capture.expect("custom Codex session fixture resolves");
            assert_eq!(
                capture.message, case.expected_message,
                "{}: assistant text",
                case.name
            );
        }
    }

    #[test]
    fn codex_session_search_without_a_codex_home_names_the_missing_home() {
        let root = std::env::temp_dir().join(format!(
            "herdr-connect-rs-codex-no-home-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&root).expect("create synthetic HOME directory");
        let session = AgentSession {
            agent: VENDOR_CODEX.to_owned(),
            value: "no-home-session".to_owned(),
        };
        let snapshot = AgentSnapshot {
            agent: Some(VENDOR_CODEX.to_owned()),
            terminal_id: "codex-no-home-terminal".to_owned(),
            agent_status: STATUS_DONE.to_owned(),
            tab_id: "w1:t1".to_owned(),
            workspace_id: "w1".to_owned(),
            pane_id: "w1:p1".to_owned(),
            cwd: Some("/srv/bridge".to_owned()),
            session: Some(session.clone()),
        };

        let resolved = resolve_session_path(&root, &snapshot, &session);
        fs::remove_dir_all(&root).expect("remove synthetic HOME directory");

        assert_eq!(
            resolved,
            Err(SessionPathError::Permanent(format!(
                "no Codex home (.codex or .codex-*) under {}",
                root.display()
            )))
        );
    }

    enum UniqueExistingPathExpectation {
        Ok(usize),
        Permanent,
    }

    struct UniqueExistingPathCase {
        name: &'static str,
        build: fn(&Path) -> Vec<PathBuf>,
        expected: UniqueExistingPathExpectation,
    }

    #[test]
    fn unique_existing_path_collapses_hard_linked_candidates() {
        let cases = [
            UniqueExistingPathCase {
                name: "one file",
                build: |dir| {
                    let path = dir.join("a.jsonl");
                    fs::write(&path, "{}").expect("write test session file");
                    vec![path]
                },
                expected: UniqueExistingPathExpectation::Ok(0),
            },
            UniqueExistingPathCase {
                name: "a hard-linked pair",
                build: |dir| {
                    let original = dir.join("a.jsonl");
                    fs::write(&original, "{}").expect("write test session file");
                    let linked = dir.join("b.jsonl");
                    fs::hard_link(&original, &linked).expect("create hard link");
                    vec![original, linked]
                },
                expected: UniqueExistingPathExpectation::Ok(0),
            },
            UniqueExistingPathCase {
                name: "two distinct files",
                build: |dir| {
                    let a = dir.join("a.jsonl");
                    fs::write(&a, "{}").expect("write test session file");
                    let b = dir.join("b.jsonl");
                    fs::write(&b, "{}").expect("write test session file");
                    vec![a, b]
                },
                expected: UniqueExistingPathExpectation::Permanent,
            },
        ];

        for (index, case) in cases.iter().enumerate() {
            let dir = std::env::temp_dir().join(format!(
                "testrun-unique-existing-path-{}-{index}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is after unix epoch")
                    .as_nanos()
            ));
            fs::create_dir_all(&dir).expect("create unique_existing_path test dir");
            let candidates = (case.build)(&dir);

            let result = unique_existing_path(&candidates, "test session log");
            match case.expected {
                UniqueExistingPathExpectation::Ok(expected_index) => {
                    assert_eq!(
                        result,
                        Ok(candidates[expected_index].clone()),
                        "{}",
                        case.name
                    );
                }
                UniqueExistingPathExpectation::Permanent => {
                    assert!(
                        matches!(result, Err(SessionPathError::Permanent(_))),
                        "{}: expected Permanent, got {result:?}",
                        case.name
                    );
                }
            }
            let _ = fs::remove_dir_all(&dir);
        }
    }

    #[test]
    fn capture_for_with_search_root_errors_on_missing_log() {
        let response: Value =
            serde_json::from_str(include_str!("../tests/fixtures/herdr-agent-list.json"))
                .expect("captured agent.list fixture is JSON");
        let agents: Vec<AgentSnapshot> =
            serde_json::from_value(response["result"]["agents"].clone())
                .expect("captured agent.list fixture has typed agents");

        let mut missing_log = agents
            .iter()
            .find(|snapshot| snapshot.session.is_some())
            .expect("fixture contains a session")
            .clone();
        missing_log
            .session
            .as_mut()
            .expect("session fixture is present")
            .value = "missing-session.jsonl".to_owned();
        let session = missing_log.session.as_ref().expect("session is present");
        assert!(
            capture_for_with_search_root(&missing_log, session, Path::new("tests/fixtures"))
                .is_err(),
            "reader errors for a reported session whose log is missing must surface"
        );
    }

    #[test]
    fn claude_pending_question_fixture_yields_question_capture_and_card() {
        let snapshot = AgentSnapshot {
            agent: Some("claude".to_owned()),
            terminal_id: "question-terminal".to_owned(),
            agent_status: "blocked".to_owned(),
            tab_id: "w1:t1".to_owned(),
            workspace_id: "w1".to_owned(),
            pane_id: "w1:p1".to_owned(),
            cwd: Some("/srv/bridge".to_owned()),
            session: Some(AgentSession {
                agent: "claude".to_owned(),
                value: "9a11cafe-affe-4f5c-8bda-b10cb6a5cafe".to_owned(),
            }),
        };
        let session = snapshot.session.as_ref().expect("snapshot has a session");
        let capture = capture_for_with_search_root(&snapshot, session, Path::new("tests/fixtures"))
            .expect("fixture-backed claude session resolves");
        let expected_question =
            "Which environment should the fix target?\n1. staging\n2. production";
        assert_eq!(capture.question.as_deref(), Some(expected_question));

        let transition = Transition {
            from: "working".to_owned(),
            to: "blocked".to_owned(),
            terminal_id: snapshot.terminal_id,
        };
        let card = create_transition_messages(&transition, &capture, "42")
            .into_iter()
            .next()
            .expect("blocked transition produces a card");
        assert_eq!(card.description, expected_question);
        assert_eq!(card.mention_user.as_deref(), Some("42"));
    }

    #[cfg(unix)]
    struct BlockedCaptureGuild {
        client: Arc<Client>,
        id: Id<GuildMarker>,
    }

    #[cfg(unix)]
    fn blocked_capture_guild() -> Option<BlockedCaptureGuild> {
        let guild = BlockedCaptureGuild {
            client: Arc::new(
                Client::builder()
                    .token(std::env::var("DISCORD_TOKEN").ok()?)
                    .timeout(std::time::Duration::from_secs(30))
                    .build(),
            ),
            id: Id::new(std::env::var("DISCORD_GUILD_ID").ok()?.parse().ok()?),
        };
        Some(guild)
    }

    #[cfg(unix)]
    fn is_test_channel(channel: &twilight_model::channel::Channel) -> bool {
        channel
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with("testrun-"))
    }

    #[cfg(unix)]
    async fn blocked_capture_testrun_threads(
        guild: &BlockedCaptureGuild,
    ) -> Result<Vec<Id<ChannelMarker>>, String> {
        let channels = guild
            .client
            .guild_channels(guild.id)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        let mut threads = guild
            .client
            .active_threads(guild.id)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?
            .threads;
        for parent in channels.iter().filter(|channel| {
            channel
                .topic
                .as_deref()
                .is_some_and(|topic| topic.starts_with("herdr workspace ["))
        }) {
            threads.extend(
                herdr_connect_rs::archived_threads(guild.client.as_ref(), parent.id).await?,
            );
        }
        Ok(threads
            .into_iter()
            .filter(is_test_channel)
            .map(|thread| thread.id)
            .collect())
    }

    #[cfg(unix)]
    async fn blocked_capture_active_testrun_threads(
        guild: &BlockedCaptureGuild,
    ) -> Result<usize, String> {
        Ok(guild
            .client
            .active_threads(guild.id)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?
            .threads
            .iter()
            .filter(|thread| is_test_channel(thread))
            .count())
    }

    #[cfg(unix)]
    async fn blocked_capture_delete_thread(
        guild: &BlockedCaptureGuild,
        thread: Id<ChannelMarker>,
    ) -> Result<(), String> {
        match guild.client.delete_channel(thread).await {
            Ok(_) => Ok(()),
            Err(error)
                if matches!(
                    error.kind(),
                    twilight_http::error::ErrorType::Response {
                        status,
                        error: twilight_http::api_error::ApiError::General(api_error),
                        ..
                    } if *status == twilight_http::response::StatusCode::NOT_FOUND
                        && api_error.code == 10003
                ) =>
            {
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    #[cfg(unix)]
    async fn blocked_capture_cleanup(guild: &BlockedCaptureGuild) -> Result<usize, String> {
        let channels = guild
            .client
            .guild_channels(guild.id)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())?;
        for channel in channels.into_iter().filter(is_test_channel) {
            guild
                .client
                .delete_channel(channel.id)
                .await
                .map_err(|e| e.to_string())?;
        }
        for thread in blocked_capture_testrun_threads(guild).await? {
            blocked_capture_delete_thread(guild, thread).await?;
        }
        let mut attempt = 0;
        loop {
            let leftover = guild
                .client
                .guild_channels(guild.id)
                .await
                .map_err(|e| e.to_string())?
                .model()
                .await
                .map_err(|e| e.to_string())?
                .into_iter()
                .filter(is_test_channel)
                .count()
                + blocked_capture_active_testrun_threads(guild).await?;
            if leftover == 0 || attempt == 4 {
                return Ok(leftover);
            }
            tokio::time::sleep(std::time::Duration::from_millis(200) * (attempt + 1)).await;
            attempt += 1;
        }
    }

    #[cfg(unix)]
    struct Tab {
        tab_id: String,
        pane_id: String,
    }

    #[cfg(unix)]
    const SUBSCRIBE_LABEL: &str = "testrun-subscribe";

    #[cfg(unix)]
    const PASSTHROUGH_ENV_VARS: &[&str] = &["CLAUDE_CONFIG_DIR", "CODEX_HOME"];

    #[cfg(unix)]
    fn passthrough_env_args() -> Vec<String> {
        PASSTHROUGH_ENV_VARS
            .iter()
            .filter_map(|name| {
                std::env::var(name)
                    .ok()
                    .map(|value| format!("{name}={value}"))
            })
            .collect()
    }

    #[cfg(unix)]
    fn create_tab(label: &str, workspace_id: &str, cwd: &str) -> Result<Tab, String> {
        let mut args = vec![
            "tab",
            "create",
            "--workspace",
            workspace_id,
            "--cwd",
            cwd,
            "--label",
            label,
            "--no-focus",
        ];
        let env_args = passthrough_env_args();
        for env_arg in &env_args {
            args.push("--env");
            args.push(env_arg);
        }
        let created = herdr_json(&args)?;
        let tab_id = created["result"]["tab"]["tab_id"]
            .as_str()
            .ok_or("herdr tab create result missing tab_id")?
            .to_owned();
        let pane_id = created["result"]["root_pane"]["pane_id"]
            .as_str()
            .ok_or("herdr tab create result missing pane_id")?
            .to_owned();
        Ok(Tab { tab_id, pane_id })
    }

    #[cfg(unix)]
    fn close_tab(tab_id: &str) {
        let _ = Command::new("herdr")
            .args(["tab", "close", tab_id])
            .output();
    }

    #[cfg(unix)]
    fn claude_testrun_dir(home: &Path) -> PathBuf {
        home.join(".cache/herdr-connect-testrun/claude")
    }

    #[cfg(unix)]
    fn cursor_testrun_dir(home: &Path) -> PathBuf {
        home.join(".cache/herdr-connect-testrun/cursor")
    }

    #[cfg(unix)]
    fn codex_testrun_dir(home: &Path) -> PathBuf {
        home.join(".cache/herdr-connect-testrun/codex")
    }

    #[cfg(unix)]
    fn clear_directory_contents(directory: &Path) -> Result<(), String> {
        fs::create_dir_all(directory).map_err(|error| error.to_string())?;
        for entry in fs::read_dir(directory).map_err(|error| error.to_string())? {
            let path = entry.map_err(|error| error.to_string())?.path();
            if path.is_dir() {
                fs::remove_dir_all(&path).map_err(|error| error.to_string())?;
            } else {
                fs::remove_file(&path).map_err(|error| error.to_string())?;
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    fn report_agent_state(pane_id: &str, state: &str) -> Result<(), String> {
        let args = [
            "pane",
            "report-agent",
            pane_id,
            "--source",
            "herdr:claude",
            "--agent",
            SUBSCRIBE_LABEL,
            "--state",
            state,
        ];
        let output = Command::new("herdr")
            .args(args)
            .output()
            .map_err(|error| format!("herdr {args:?} spawn failed: {error}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "herdr {args:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }

    #[cfg(unix)]
    fn herdr_json(args: &[&str]) -> Result<Value, String> {
        let output = Command::new("herdr")
            .args(args)
            .output()
            .map_err(|error| format!("herdr {args:?} spawn failed: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "herdr {args:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        serde_json::from_slice(&output.stdout)
            .map_err(|error| format!("herdr {args:?} produced non-JSON stdout: {error}"))
    }

    #[cfg(unix)]
    fn remaining_tabs(label: &str) -> Result<usize, String> {
        Ok(tab_list_result()?
            .into_iter()
            .filter(|tab| tab.label == label)
            .count())
    }

    #[cfg(unix)]
    struct Workspace {
        id: String,
        tab_id: String,
        pane_id: String,
    }

    #[cfg(unix)]
    fn create_workspace(label: &str, cwd: &str) -> Result<Workspace, String> {
        let mut args = vec![
            "workspace",
            "create",
            "--cwd",
            cwd,
            "--label",
            label,
            "--no-focus",
        ];
        let env_args = passthrough_env_args();
        for env_arg in &env_args {
            args.push("--env");
            args.push(env_arg);
        }
        let created = herdr_json(&args)?;
        let workspace_id = created["result"]["workspace"]["workspace_id"]
            .as_str()
            .ok_or("herdr workspace create result missing workspace_id")?
            .to_owned();
        let tab_id = created["result"]["tab"]["tab_id"]
            .as_str()
            .ok_or("herdr workspace create result missing tab_id")?
            .to_owned();
        let pane_id = created["result"]["root_pane"]["pane_id"]
            .as_str()
            .ok_or("herdr workspace create result missing pane_id")?
            .to_owned();
        let output = Command::new("herdr")
            .args(["tab", "rename", &tab_id, label])
            .output()
            .map_err(|error| format!("herdr tab rename spawn failed: {error}"))?;
        if !output.status.success() {
            return Err(format!(
                "herdr tab rename failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        Ok(Workspace {
            id: workspace_id,
            tab_id,
            pane_id,
        })
    }

    #[cfg(unix)]
    fn close_workspace(workspace_id: &str) {
        let _ = Command::new("herdr")
            .args(["workspace", "close", workspace_id])
            .output();
    }

    #[cfg(unix)]
    fn remaining_workspaces(label: &str) -> Result<usize, String> {
        Ok(workspace_list_result()?
            .into_iter()
            .filter(|workspace| workspace.label == label)
            .count())
    }

    #[cfg(unix)]
    fn snapshot_for_pane(pane_id: &str) -> Result<AgentSnapshot, String> {
        list_agents()?
            .into_iter()
            .find(|agent| agent.pane_id == pane_id)
            .ok_or_else(|| format!("agent.list has no entry for pane {pane_id}"))
    }

    #[cfg(unix)]
    fn event_pane_id_at<'a>(event: &'a Value, pointer: &str) -> Option<&'a str> {
        event.pointer(pointer).and_then(Value::as_str)
    }

    #[cfg(unix)]
    async fn wait_for_event(
        sub: &mut herdr_connect_rs::HerdrSubscription,
        event_name: &str,
        pane_id: &str,
        pane_id_pointer: &str,
        agent_status: Option<&str>,
        bound: Duration,
    ) -> Result<Value, String> {
        let deadline = Instant::now() + bound;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "timed out waiting for {event_name} pane={pane_id} status={agent_status:?}"
                ));
            }
            let event = tokio::time::timeout(remaining, sub.next_event())
                .await
                .map_err(|_| {
                    format!(
                        "timed out waiting for {event_name} pane={pane_id} status={agent_status:?}"
                    )
                })??;
            let got = event.get("event").and_then(Value::as_str).unwrap_or("");
            if got != event_name {
                continue;
            }
            if event_pane_id_at(&event, pane_id_pointer) != Some(pane_id) {
                continue;
            }
            if let Some(status) = agent_status
                && event.pointer("/data/agent_status").and_then(Value::as_str) != Some(status)
            {
                continue;
            }
            return Ok(event);
        }
    }

    #[cfg(unix)]
    fn matching_tab(tab_id: &str) -> Result<herdr_connect_rs::HerdrTab, String> {
        tab_list_result()?
            .into_iter()
            .find(|candidate| candidate.tab_id == tab_id)
            .ok_or_else(|| format!("tab.list has no entry for {tab_id}"))
    }

    #[cfg(unix)]
    fn discord_tuple(guild: &BlockedCaptureGuild) -> super::DiscordConnection {
        discord_tuple_with_cache(guild, Arc::new(tokio::sync::Mutex::new(None)))
    }

    #[cfg(unix)]
    fn discord_tuple_with_cache(
        guild: &BlockedCaptureGuild,
        topology_cache: herdr_connect_rs::TopologyCache,
    ) -> super::DiscordConnection {
        let owner_id = std::env::var("DISCORD_OWNER_ID").expect("DISCORD_OWNER_ID is set");
        let responder = Arc::new(PermissionResponder::new(Arc::new(Discord::new(
            Arc::clone(&guild.client),
            guild.id,
            owner_id.clone(),
            topology_cache,
        ))));
        let identity = herdr_connect_rs::OwnerIdentity {
            display_name: owner_id.clone(),
            avatar_url: None,
        };
        (
            Arc::clone(&guild.client),
            guild.id,
            owner_id,
            responder,
            identity,
        )
    }

    #[cfg(unix)]
    fn subscribe_tab_fixture() -> Result<(Tab, PathBuf), String> {
        let workspace_id = std::env::var("HERDR_WORKSPACE_ID").map_err(|_| {
            "HERDR_WORKSPACE_ID is set by the real Herdr pane environment".to_owned()
        })?;
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .map_err(|_| "HOME is set by the real Herdr pane environment".to_owned())?;
        let cwd_dir = claude_testrun_dir(&home);
        clear_directory_contents(&cwd_dir)?;
        let cwd = cwd_dir
            .to_str()
            .ok_or_else(|| "temp cwd is valid UTF-8".to_owned())?;
        create_tab(SUBSCRIBE_LABEL, &workspace_id, cwd).map(|tab| (tab, cwd_dir))
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn subscribe_ack_then_status_event_doorbells_list() {
        assert_eq!(
            remaining_tabs(SUBSCRIBE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        let (tab, cwd_dir) = subscribe_tab_fixture().expect("create testrun tab");
        let result = async {
            report_agent_state(&tab.pane_id, "idle")?;
            let mut sub =
                subscribe_herdr_events(&status_subscriptions(std::slice::from_ref(&tab.pane_id)))
                    .await?;
            report_agent_state(&tab.pane_id, "working")?;
            let event = wait_for_event(
                &mut sub,
                "pane.agent_status_changed",
                &tab.pane_id,
                "/data/pane_id",
                Some("working"),
                Duration::from_secs(10),
            )
            .await?;
            assert!(
                event.get("cwd").is_none() && event.pointer("/data/cwd").is_none(),
                "status event must not carry cwd: {event}"
            );
            let snapshot = snapshot_for_pane(&tab.pane_id)?;
            assert_eq!(snapshot.agent_status, "working");
            assert!(
                snapshot.cwd.is_some() || snapshot.session.is_some(),
                "doorbell agent.list must carry cwd or a reported session: {snapshot:?}"
            );
            Ok::<(), String>(())
        }
        .await;
        close_tab(&tab.tab_id);
        let _ = clear_directory_contents(&cwd_dir);
        let tabs_left =
            remaining_tabs(SUBSCRIBE_LABEL).expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const DETECTION_LABEL: &str = "testrun-detection";

    #[cfg(unix)]
    fn detection_tab_fixture() -> Result<(Tab, PathBuf), String> {
        let workspace_id = std::env::var("HERDR_WORKSPACE_ID").map_err(|_| {
            "HERDR_WORKSPACE_ID is set by the real Herdr pane environment".to_owned()
        })?;
        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-detection-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).map_err(|error| error.to_string())?;
        let cwd = cwd_dir
            .to_str()
            .ok_or_else(|| "temp cwd is valid UTF-8".to_owned())?;
        match create_tab(DETECTION_LABEL, &workspace_id, cwd) {
            Ok(tab) => Ok((tab, cwd_dir)),
            Err(error) => {
                let _ = fs::remove_dir_all(&cwd_dir);
                Err(error)
            }
        }
    }

    #[cfg(unix)]
    fn pane_run(pane_id: &str, command: &str) -> Result<(), String> {
        let output = Command::new("herdr")
            .args(["pane", "run", pane_id, command])
            .output()
            .map_err(|error| format!("herdr pane run spawn failed: {error}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "herdr pane run failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }

    #[cfg(unix)]
    fn wait_for_pane_output(pane_id: &str, needle: &str, timeout_ms: &str) -> Result<(), String> {
        let output = Command::new("herdr")
            .args([
                "pane",
                "wait-output",
                pane_id,
                "--match",
                needle,
                "--timeout",
                timeout_ms,
            ])
            .output()
            .map_err(|error| format!("herdr pane wait-output spawn failed: {error}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "herdr pane wait-output failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }

    #[cfg(unix)]
    #[test]
    #[serial]
    fn detection_read_round_trips_fixture_text_from_a_real_pane() {
        assert_eq!(
            remaining_tabs(DETECTION_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        let (tab, cwd_dir) = detection_tab_fixture().expect("create testrun tab");
        let result = (|| -> Result<(), String> {
            report_agent_state(&tab.pane_id, "idle")?;
            let fixture_path = cwd_dir.join("fixture.txt");
            fs::write(
                &fixture_path,
                include_str!("../tests/fixtures/claude-detection-blocked-question.txt"),
            )
            .map_err(|error| error.to_string())?;
            let fixture_path_str = fixture_path
                .to_str()
                .ok_or_else(|| "fixture path is valid UTF-8".to_owned())?;
            pane_run(
                &tab.pane_id,
                &format!("printf '%s' \"$(cat '{fixture_path_str}')\""),
            )?;
            wait_for_pane_output(&tab.pane_id, "Which color do you prefer?", "10000")?;
            let text = agent_read_detection(&tab.pane_id)?;
            assert!(
                text.contains("Which color do you prefer?"),
                "detection read must round-trip the printed question: {text}"
            );
            assert!(
                text.contains("The color red") && text.contains("The color blue"),
                "detection read must round-trip the printed options: {text}"
            );
            Ok(())
        })();
        close_tab(&tab.tab_id);
        let _ = fs::remove_dir_all(&cwd_dir);
        let tabs_left =
            remaining_tabs(DETECTION_LABEL).expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn lifecycle_created_subscribes_status_and_records_the_pane_snapshot() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        let connection = discord_tuple(&guild);
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(SUBSCRIBE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        let lifecycle = subscribe_herdr_events(&lifecycle_subscriptions())
            .await
            .expect("lifecycle subscribe");
        let mut stop = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("terminate signal stream");
        let mut broker: Option<BrokerTask> = None;
        let (_live_tx, live_events) = tokio::sync::mpsc::unbounded_channel();
        let (_activity_tx, activity_events) = tokio::sync::mpsc::unbounded_channel();
        let mut runtime = BridgeRuntime {
            lifecycle,
            pane_ids: Vec::new(),
            status: None,
            state: BridgeState::default(),
            live_events,
            activity_events,
        };
        doorbell_snapshot(
            &connection,
            &mut runtime.state,
            &mut runtime.pane_ids,
            &mut runtime.status,
            &mut stop,
        )
        .await
        .unwrap_or_else(|_| panic!("snapshot doorbell was interrupted"));
        let (tab, cwd_dir) = subscribe_tab_fixture().expect("create testrun tab");
        let result = async {
            let pane_created = wait_for_event(
                &mut runtime.lifecycle,
                "pane_created",
                &tab.pane_id,
                "/data/pane/pane_id",
                None,
                Duration::from_secs(10),
            )
            .await?;
            report_agent_state(&tab.pane_id, "idle")?;
            let terminal_id = snapshot_for_pane(&tab.pane_id)?.terminal_id;
            handle_lifecycle_select_result(
                Ok(pane_created),
                &connection,
                &mut stop,
                &mut broker,
                &mut runtime,
            )
            .await;
            if !runtime.pane_ids.contains(&tab.pane_id) {
                return Err(format!(
                    "pane_created did not add the pane to the runtime: {:?}",
                    runtime.pane_ids
                ));
            }
            if !runtime.state.previous.contains_key(&terminal_id) {
                return Err("pane_created did not run the snapshot doorbell".to_owned());
            }
            let status = runtime
                .status
                .as_mut()
                .ok_or_else(|| "pane_created did not subscribe to status".to_owned())?;
            report_agent_state(&tab.pane_id, "working")?;
            wait_for_event(
                status,
                "pane.agent_status_changed",
                &tab.pane_id,
                "/data/pane_id",
                Some("working"),
                Duration::from_secs(10),
            )
            .await?;
            Ok::<(), String>(())
        }
        .await;
        close_tab(&tab.tab_id);
        let _ = clear_directory_contents(&cwd_dir);
        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left =
            remaining_tabs(SUBSCRIBE_LABEL).expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn restarted_bridge_reuses_the_workspace_webhook() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let result = async {
            let channel = guild
                .client
                .create_guild_channel(guild.id, "testrun-webhook-reuse")
                .await
                .map_err(|error| error.to_string())?
                .model()
                .await
                .map_err(|error| error.to_string())?;
            let first = super::cached_terminal_prompt_webhook(
                guild.client.as_ref(),
                channel.id,
                &mut BridgeState::default(),
            )
            .await?;
            let second = super::cached_terminal_prompt_webhook(
                guild.client.as_ref(),
                channel.id,
                &mut BridgeState::default(),
            )
            .await?;
            if first.0 != second.0 {
                return Err(format!("webhook was not reused: {first:?} then {second:?}"));
            }
            let owned = guild
                .client
                .channel_webhooks(channel.id)
                .await
                .map_err(|error| error.to_string())?
                .model()
                .await
                .map_err(|error| error.to_string())?
                .iter()
                .filter(|webhook| {
                    webhook.name.as_deref() == Some(super::TERMINAL_PROMPT_WEBHOOK_NAME)
                })
                .count();
            if owned != 1 {
                return Err(format!("expected one bridge-owned webhook, found {owned}"));
            }
            Ok::<(), String>(())
        }
        .await;
        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    struct StalePaneCase {
        name: &'static str,
        initial_pane_ids: fn(&str, &str) -> Vec<String>,
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn subscribe_status_with_backoff_recovers_from_stale_pane_ids() {
        let cases = [
            StalePaneCase {
                name: "closed pane id tracked alongside the live pane",
                initial_pane_ids: |closed, live| {
                    let mut ids = vec![closed.to_owned(), live.to_owned()];
                    ids.sort();
                    ids
                },
            },
            StalePaneCase {
                name: "pane_ids holds only closed ids; live pane is untracked",
                initial_pane_ids: |closed, _live| vec![closed.to_owned()],
            },
        ];

        for case in cases {
            assert_eq!(
                remaining_tabs(SUBSCRIBE_LABEL).expect("tab.list succeeds"),
                0,
                "named zero-leftover check: {}",
                case.name
            );
            let (live_tab, live_cwd_dir) = subscribe_tab_fixture().expect("create testrun tab");
            report_agent_state(&live_tab.pane_id, "idle")
                .expect("register the live pane as a herdr agent");
            let (closed_tab, closed_cwd_dir) = subscribe_tab_fixture().expect("create testrun tab");
            close_tab(&closed_tab.tab_id);
            let _ = clear_directory_contents(&closed_cwd_dir);

            let mut pane_ids = (case.initial_pane_ids)(&closed_tab.pane_id, &live_tab.pane_id);

            let mut stop =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("install SIGTERM handler");

            let outcome = tokio::time::timeout(
                Duration::from_secs(20),
                subscribe_status_with_backoff(&mut pane_ids, &mut stop),
            )
            .await;

            close_tab(&live_tab.tab_id);
            let _ = clear_directory_contents(&live_cwd_dir);
            let tabs_left = remaining_tabs(SUBSCRIBE_LABEL)
                .expect("tab.list succeeds for the zero-leftover check");

            match outcome {
                Ok(Ok(Some(_))) => {}
                Ok(Ok(None)) => panic!(
                    "{}: expected Some(subscription) for the live pane, got None",
                    case.name
                ),
                Ok(Err(super::BridgeInterrupt)) => panic!(
                    "{}: subscribe_status_with_backoff returned BridgeInterrupt",
                    case.name
                ),
                Err(elapsed) => panic!(
                    "{}: subscribe_status_with_backoff did not recover from the stale pane id within the timeout: {elapsed}",
                    case.name
                ),
            }
            assert!(
                !pane_ids.contains(&closed_tab.pane_id),
                "{}: closed pane id must be dropped from membership",
                case.name
            );
            assert!(
                pane_ids.contains(&live_tab.pane_id),
                "{}: live pane id must be present in the recovered membership",
                case.name
            );
            assert_eq!(tabs_left, 0, "named zero-leftover check: {}", case.name);
        }
    }

    #[cfg(unix)]
    async fn wait_for_status(
        pane_id: &str,
        statuses: &[&str],
        bound: Duration,
    ) -> Result<AgentSnapshot, String> {
        let start = Instant::now();
        loop {
            match snapshot_for_pane(pane_id) {
                Ok(snapshot) if statuses.contains(&snapshot.agent_status.as_str()) => {
                    return Ok(snapshot);
                }
                Ok(snapshot) => {
                    if start.elapsed() > bound {
                        return Err(format!(
                            "pane {pane_id} did not reach {statuses:?} within {bound:?}, last saw {}",
                            snapshot.agent_status
                        ));
                    }
                }
                Err(error) => {
                    if start.elapsed() > bound {
                        return Err(error);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    #[cfg(unix)]
    const LIVE_CAPTURE_LABEL: &str = "testrun-live-capture";

    #[cfg(unix)]
    const LIVE_CAPTURE_FORCE_PROMPT: &str = "Say the word alpha. Then run the shell command \
                                              `sleep 12 && echo beta`. Then say the word gamma.";

    #[cfg(unix)]
    fn start_live_capture_agent(kind: &str, agent_name: &str, pane_id: &str) -> Result<(), String> {
        let vendor_args: &[&str] = match kind {
            "claude" => &["--model", "haiku"],
            "codex" => &["--no-daemon", "--model", "gpt-5.6-luna"],
            "cursor" => &["--yolo"],
            other => return Err(format!("unsupported live-capture test kind: {other}")),
        };
        let bound = Duration::from_secs(10);
        let start = Instant::now();
        loop {
            let mut args: Vec<&str> = vec![
                "agent",
                "start",
                agent_name,
                "--kind",
                kind,
                "--pane",
                pane_id,
                "--timeout",
                "60000",
            ];
            if !vendor_args.is_empty() {
                args.push("--");
                args.extend_from_slice(vendor_args);
            }
            match herdr_json(&args) {
                Ok(_) => return Ok(()),
                Err(error) if is_agent_pane_busy(&error) && start.elapsed() < bound => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[cfg(unix)]
    fn live_capture_tab_fixture(kind: &str) -> Result<(Tab, PathBuf), String> {
        let workspace_id = std::env::var("HERDR_WORKSPACE_ID").map_err(|_| {
            "HERDR_WORKSPACE_ID is set by the real Herdr pane environment".to_owned()
        })?;
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .map_err(|_| "HOME is set by the real Herdr pane environment".to_owned())?;
        let label = format!("{LIVE_CAPTURE_LABEL}-{kind}");
        let cwd_dir = match kind {
            "codex" => codex_testrun_dir(&home),
            "cursor" => cursor_testrun_dir(&home),
            _ => claude_testrun_dir(&home),
        };
        clear_directory_contents(&cwd_dir)?;
        let cwd = cwd_dir
            .to_str()
            .ok_or_else(|| "temp cwd is valid UTF-8".to_owned())?;
        let tab = create_tab(&label, &workspace_id, cwd)?;
        Ok((tab, cwd_dir))
    }

    #[cfg(unix)]
    async fn thread_messages(
        guild: &BlockedCaptureGuild,
        thread: Id<ChannelMarker>,
    ) -> Result<Vec<(String, bool, Id<MessageMarker>)>, String> {
        let messages = guild
            .client
            .channel_messages(thread)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        Ok(messages
            .into_iter()
            .map(|message| (message.content, !message.embeds.is_empty(), message.id))
            .collect())
    }

    #[cfg(unix)]
    fn poll_snapshot(
        pane_id: &str,
        bound: Duration,
        done: impl Fn(&AgentSnapshot) -> bool,
    ) -> Result<AgentSnapshot, String> {
        let start = Instant::now();
        loop {
            let past_bound = start.elapsed() > bound;
            match snapshot_for_pane(pane_id) {
                Ok(snapshot) if done(&snapshot) || past_bound => return Ok(snapshot),
                Err(error) if past_bound => return Err(error),
                Ok(_) | Err(_) => {}
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[cfg(unix)]
    async fn own(
        snapshot: &AgentSnapshot,
        tabs: &[herdr_connect_rs::HerdrTab],
        connection: &super::DiscordConnection,
        state: &mut BridgeState,
    ) {
        process_snapshot(
            snapshot,
            std::slice::from_ref(snapshot),
            tabs,
            connection,
            state,
        )
        .await;
    }

    #[cfg(unix)]
    async fn live_capture_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        agent_name: &str,
        kind: &str,
    ) -> Result<(), String> {
        start_live_capture_agent(kind, agent_name, &tab.pane_id)?;
        let idle = snapshot_for_pane(&tab.pane_id)?;
        let terminal = idle.terminal_id.clone();

        let matching = matching_tab(&tab.tab_id)?;
        let (tabs, agents) = (std::slice::from_ref(&matching), std::slice::from_ref(&idle));
        let mut state = BridgeState::default();
        let (live_tx, mut live_events) = tokio::sync::mpsc::unbounded_channel();
        state.live_tx = Some(live_tx);
        let connection = discord_tuple(guild);

        own(&idle, tabs, &connection, &mut state).await;
        let route = route_topology(agents, tabs, &terminal)?;
        let topology_cache = Arc::new(tokio::sync::Mutex::new(None));
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;

        let subs = status_subscriptions(std::slice::from_ref(&tab.pane_id));
        let mut sub = subscribe_herdr_events(&subs).await?;
        submit_owner_prompt(&tab.pane_id, LIVE_CAPTURE_FORCE_PROMPT)?;
        wait_for_event(
            &mut sub,
            "pane.agent_status_changed",
            &tab.pane_id,
            "/data/pane_id",
            Some("working"),
            Duration::from_secs(15),
        )
        .await?;
        let working = poll_snapshot(&tab.pane_id, Duration::from_secs(10), |s| {
            s.session.is_some()
        })?;
        if working.agent_status != STATUS_WORKING
            || working.session.as_ref().is_none_or(|sn| sn.agent != kind)
        {
            return Err(format!("no confirmed {kind} working session: {working:?}"));
        }
        own(&working, tabs, &connection, &mut state).await;
        let watch_deadline = Instant::now() + Duration::from_secs(5);
        while !state.live_watches.contains_key(&terminal) && Instant::now() < watch_deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
            own(&working, tabs, &connection, &mut state).await;
        }

        let has_alpha = |messages: &[(String, bool, Id<MessageMarker>)]| {
            messages
                .iter()
                .any(|(content, _, _)| content.to_lowercase().contains("alpha"))
        };
        let mut alpha_before_settle = has_alpha(&thread_messages(guild, thread).await?);
        let settled = loop {
            tokio::select! {
                Some(terminal_id) = live_events.recv() => {
                    handle_live_event(&connection, &terminal_id, &mut state).await;
                    if !alpha_before_settle {
                        alpha_before_settle = has_alpha(&thread_messages(guild, thread).await?);
                    }
                }
                event = wait_for_event(
                    &mut sub, "pane.agent_status_changed", &tab.pane_id, "/data/pane_id", None,
                    Duration::from_secs(30),
                ) => {
                    let event = event?;
                    if matches!(
                        event.pointer("/data/agent_status").and_then(Value::as_str),
                        Some("done" | "idle")
                    ) {
                        break poll_snapshot(&tab.pane_id, Duration::from_secs(2), |_| true)?;
                    }
                }
            }
        };
        if !alpha_before_settle {
            return Err("'alpha' did not appear live before the pane settled".to_owned());
        }
        own(&settled, tabs, &connection, &mut state).await;
        while let Ok(terminal_id) = live_events.try_recv() {
            handle_live_event(&connection, &terminal_id, &mut state).await;
        }
        handle_live_event(&connection, &terminal, &mut state).await;

        let Some(session) = settled.session.clone() else {
            return Err("settled snapshot lost its session".to_owned());
        };
        let log_path = live_log_path(&settled, &session)?.ok_or("no log path yet")?;
        let expected_count = match kind {
            "claude" => read_claude_incremental(&log_path, 0)?.0.len(),
            "codex" => read_codex_incremental(&log_path, 0)?.0.len(),
            "cursor" => read_cursor_incremental(&log_path, 0)?.0.len(),
            other => return Err(format!("unsupported vendor for structural count: {other}")),
        };
        let messages = thread_messages(guild, thread).await?;
        live_texts_match_with_no_card(&messages, expected_count)
    }

    #[cfg(unix)]
    fn live_texts_match_with_no_card(
        messages: &[(String, bool, Id<MessageMarker>)],
        expected_count: usize,
    ) -> Result<(), String> {
        let live = messages.iter().filter(|(_, embed, _)| !embed).count();
        if live != expected_count {
            return Err(format!(
                "expected {expected_count} live messages, got {live} in {messages:?}"
            ));
        }
        if let Some((content, _, _)) = messages.iter().find(|(_, embed, _)| *embed) {
            return Err(format!("a card was posted after live text: {content:?}"));
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn run_live_capture_test(kind: &str) {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let label = format!("{LIVE_CAPTURE_LABEL}-{kind}");
        assert_eq!(
            remaining_tabs(&label).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let created = live_capture_tab_fixture(kind);
        let (tab_id, cwd_dir, result, session_cleanup) = match created {
            Ok((tab, cwd_dir)) => {
                let agent_name = format!(
                    "live-{kind}-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome = tokio::time::timeout(
                    Duration::from_secs(180),
                    live_capture_exercise(&guild, &tab, &agent_name, kind),
                )
                .await
                .unwrap_or_else(|_| Err(format!("{kind} live-capture exercise timed out")));
                let session_cleanup = if kind == "codex" {
                    snapshot_for_pane(&tab.pane_id)
                        .map_err(|error| {
                            format!("Codex log path cannot be resolved during cleanup: {error}")
                        })
                        .and_then(|snapshot| {
                            let session = snapshot.session.as_ref().ok_or_else(|| {
                                "Codex log path cannot be resolved during cleanup: no session"
                                    .to_owned()
                            })?;
                            let path = resolve_session_path(&home, &snapshot, session).map_err(
                                |error| {
                                    format!(
                                        "Codex log path cannot be resolved during cleanup: {error}"
                                    )
                                },
                            )?;
                            fs::remove_file(path)
                                .map_err(|error| format!("remove Codex session file: {error}"))
                        })
                } else {
                    if let Ok(snapshot) = snapshot_for_pane(&tab.pane_id)
                        && let Some(session) = snapshot.session.as_ref()
                        && let Ok(path) = resolve_session_path(&home, &snapshot, session)
                        && let Some(parent) = path.parent()
                    {
                        let _ = fs::remove_dir_all(parent);
                    }
                    Ok(())
                };
                (Some(tab.tab_id), Some(cwd_dir), outcome, session_cleanup)
            }
            Err(error) => (None, None, Err(error), Ok(())),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        if let Some(cwd_dir) = cwd_dir {
            let _ = clear_directory_contents(&cwd_dir);
        }

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left =
            remaining_tabs(&label).expect("tab.list succeeds for the zero-leftover check");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert!(session_cleanup.is_ok(), "{session_cleanup:?}");
        assert!(result.is_ok(), "{result:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn live_capture_posts_first_live_text_before_settle_for_each_vendor() {
        for kind in ["claude", "cursor"] {
            run_live_capture_test(kind).await;
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn live_capture_posts_first_live_text_before_settle_for_codex() {
        run_live_capture_test("codex").await;
    }

    #[cfg(unix)]
    fn append_claude_assistant_record(path: &Path, text: &str) -> Result<(), String> {
        let record = json!({
            "type": "assistant",
            "message": {"role": "assistant", "content": [{"type": "text", "text": text}]},
        });
        let mut file = fs::OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        writeln!(file, "{record}").map_err(|error| error.to_string())
    }

    #[cfg(unix)]
    async fn live_count_once_reply_is_posted(
        guild: &BlockedCaptureGuild,
        thread: Id<ChannelMarker>,
        connection: &super::DiscordConnection,
        terminal: &str,
        state: &mut BridgeState,
    ) -> Result<usize, String> {
        let reply_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            handle_live_event(connection, terminal, state).await;
            let live: Vec<_> = thread_messages(guild, thread)
                .await?
                .into_iter()
                .filter(|(_, embed, _)| !embed)
                .collect();
            if live.iter().any(|(content, _, _)| {
                content.to_lowercase().contains("gamma")
                    && !content.contains(LIVE_CAPTURE_FORCE_PROMPT)
            }) {
                return Ok(live.len());
            }
            if Instant::now() >= reply_deadline {
                return Err(format!("the reply never reached the thread: {live:?}"));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    #[cfg(unix)]
    async fn live_text_after_settle_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        agent_name: &str,
    ) -> Result<(), String> {
        start_live_capture_agent("claude", agent_name, &tab.pane_id)?;
        let idle = snapshot_for_pane(&tab.pane_id)?;
        let terminal = idle.terminal_id.clone();
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let mut state = BridgeState::default();
        let (live_tx, mut live_events) = tokio::sync::mpsc::unbounded_channel();
        state.live_tx = Some(live_tx);
        let connection = discord_tuple(guild);

        own(&idle, tabs, &connection, &mut state).await;
        let route = route_topology(std::slice::from_ref(&idle), tabs, &terminal)?;
        let topology_cache = Arc::new(tokio::sync::Mutex::new(None));
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;

        let subs = status_subscriptions(std::slice::from_ref(&tab.pane_id));
        let mut sub = subscribe_herdr_events(&subs).await?;
        submit_owner_prompt(&tab.pane_id, LIVE_CAPTURE_FORCE_PROMPT)?;
        wait_for_event(
            &mut sub,
            "pane.agent_status_changed",
            &tab.pane_id,
            "/data/pane_id",
            Some("working"),
            Duration::from_secs(15),
        )
        .await?;
        let working = poll_snapshot(&tab.pane_id, Duration::from_secs(10), |s| {
            s.session.is_some()
        })?;
        if working.agent_status != STATUS_WORKING
            || working
                .session
                .as_ref()
                .is_none_or(|sn| sn.agent != "claude")
        {
            return Err(format!("no confirmed claude working session: {working:?}"));
        }
        own(&working, tabs, &connection, &mut state).await;
        let watch_deadline = Instant::now() + Duration::from_secs(5);
        while !state.live_watches.contains_key(&terminal) && Instant::now() < watch_deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
            own(&working, tabs, &connection, &mut state).await;
        }

        let settled = loop {
            tokio::select! {
                Some(terminal_id) = live_events.recv() => {
                    handle_live_event(&connection, &terminal_id, &mut state).await;
                }
                event = wait_for_event(
                    &mut sub, "pane.agent_status_changed", &tab.pane_id, "/data/pane_id", None,
                    Duration::from_secs(30),
                ) => {
                    let event = event?;
                    if matches!(
                        event.pointer("/data/agent_status").and_then(Value::as_str),
                        Some("done" | "idle")
                    ) {
                        break poll_snapshot(&tab.pane_id, Duration::from_secs(2), |_| true)?;
                    }
                }
            }
        };
        own(&settled, tabs, &connection, &mut state).await;
        while let Ok(terminal_id) = live_events.try_recv() {
            handle_live_event(&connection, &terminal_id, &mut state).await;
        }
        let live_before =
            live_count_once_reply_is_posted(guild, thread, &connection, &terminal, &mut state)
                .await?;

        let Some(session) = settled.session.clone() else {
            return Err("settled snapshot lost its session".to_owned());
        };
        let log_path = live_log_path(&settled, &session)?.ok_or("no log path yet")?;
        let marker = format!("post-settle-{agent_name}");
        append_claude_assistant_record(&log_path, &marker)?;
        handle_live_event(&connection, &terminal, &mut state).await;

        let messages = thread_messages(guild, thread).await?;
        let live: Vec<_> = messages.iter().filter(|(_, embed, _)| !embed).collect();
        if !live.iter().any(|(content, _, _)| content.contains(&marker)) {
            return Err(format!(
                "assistant text appended after settle was not posted: {messages:?}"
            ));
        }
        if live.len() != live_before + 1 {
            return Err(format!(
                "expected exactly one new live message after settle, before={live_before} \
                 after={} in {messages:?}",
                live.len()
            ));
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn live_text_after_settle_posts_and_does_not_repeat() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let label = format!("{LIVE_CAPTURE_LABEL}-claude");
        assert_eq!(
            remaining_tabs(&label).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let created = live_capture_tab_fixture("claude");
        let (tab_id, cwd_dir, result) = match created {
            Ok((tab, cwd_dir)) => {
                let agent_name = format!(
                    "settle-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome = tokio::time::timeout(
                    Duration::from_secs(180),
                    live_text_after_settle_exercise(&guild, &tab, &agent_name),
                )
                .await
                .unwrap_or_else(|_| Err("live-text-after-settle exercise timed out".to_owned()));
                if let Ok(snapshot) = snapshot_for_pane(&tab.pane_id)
                    && let Some(session) = snapshot.session.as_ref()
                    && let Ok(path) = resolve_session_path(&home, &snapshot, session)
                    && let Some(parent) = path.parent()
                {
                    let _ = fs::remove_dir_all(parent);
                }
                (Some(tab.tab_id), Some(cwd_dir), outcome)
            }
            Err(error) => (None, None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        if let Some(cwd_dir) = cwd_dir {
            let _ = clear_directory_contents(&cwd_dir);
        }

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left =
            remaining_tabs(&label).expect("tab.list succeeds for the zero-leftover check");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
    }

    #[cfg(unix)]
    enum TerminalPromptSubmission {
        Terminal,
        Discord,
    }

    #[cfg(unix)]
    #[derive(Clone, Copy)]
    struct TerminalPromptFixture<'a> {
        tab: &'a Tab,
        agent_name: &'a str,
        vendor: &'static str,
        connection: &'a super::DiscordConnection,
        tabs: &'a [herdr_connect_rs::HerdrTab],
        terminal: &'a str,
    }

    #[cfg(unix)]
    async fn wait_for_readable_capture(settled: &AgentSnapshot) -> Result<(), String> {
        let home = std::env::var("HOME").map_err(|error| error.to_string())?;
        let capture_deadline = Instant::now() + Duration::from_secs(10);
        let session = settled
            .session
            .as_ref()
            .ok_or("settled pane has no session")?;
        while let Err(error) = capture_for_with_search_root(settled, session, Path::new(&home)) {
            if Instant::now() >= capture_deadline {
                eprintln!("reply capture never became readable before settle: {error}");
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn run_terminal_prompt_turn(
        fixture: &TerminalPromptFixture<'_>,
        state: &mut BridgeState,
        live_events: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
        submission: TerminalPromptSubmission,
        prompt: &str,
    ) -> Result<(), String> {
        let TerminalPromptFixture {
            tab,
            agent_name,
            vendor,
            connection,
            tabs,
            terminal,
        } = *fixture;
        let subs = status_subscriptions(std::slice::from_ref(&tab.pane_id));
        let mut sub = subscribe_herdr_events(&subs).await?;
        let submit_task: tokio::task::JoinHandle<Result<(), String>> = match submission {
            TerminalPromptSubmission::Terminal => {
                let agent_name = agent_name.to_owned();
                let prompt = prompt.to_owned();
                tokio::task::spawn_blocking(move || {
                    prompt_claude_agent_and_wait(&agent_name, &prompt)
                })
            }
            TerminalPromptSubmission::Discord => {
                let pane_id = tab.pane_id.clone();
                let prompt = prompt.to_owned();
                tokio::task::spawn_blocking(move || {
                    submit_owner_prompt(&pane_id, &prompt).map(drop)
                })
            }
        };
        wait_for_event(
            &mut sub,
            "pane.agent_status_changed",
            &tab.pane_id,
            "/data/pane_id",
            Some(STATUS_WORKING),
            Duration::from_secs(15),
        )
        .await?;
        let working = poll_snapshot(&tab.pane_id, Duration::from_secs(10), |snapshot| {
            snapshot.session.is_some()
        })?;
        if working.agent_status != STATUS_WORKING
            || working
                .session
                .as_ref()
                .is_none_or(|session| session.agent != vendor)
        {
            return Err(format!(
                "no confirmed {vendor} working session: {working:?}"
            ));
        }
        own(&working, tabs, connection, state).await;
        let watch_deadline = Instant::now() + Duration::from_secs(5);
        while !state.live_watches.contains_key(terminal) && Instant::now() < watch_deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
            own(&working, tabs, connection, state).await;
        }
        loop {
            tokio::select! {
                Some(event_terminal) = live_events.recv() => {
                    handle_live_event(connection, &event_terminal, state).await;
                }
                event = wait_for_event(
                    &mut sub,
                    "pane.agent_status_changed",
                    &tab.pane_id,
                    "/data/pane_id",
                    None,
                    Duration::from_secs(30),
                ) => {
                    let event = event?;
                    if matches!(
                        event.pointer("/data/agent_status").and_then(Value::as_str),
                        Some(STATUS_DONE | STATUS_IDLE)
                    ) {
                        break;
                    }
                }
            }
        }
        let settled = poll_snapshot(&tab.pane_id, Duration::from_secs(2), |_| true)?;
        submit_task
            .await
            .map_err(|error| format!("prompt task failed: {error}"))??;
        wait_for_readable_capture(&settled).await?;
        own(&settled, tabs, connection, state).await;
        while let Ok(event_terminal) = live_events.try_recv() {
            handle_live_event(connection, &event_terminal, state).await;
        }
        handle_live_event(connection, terminal, state).await;
        Ok(())
    }

    #[cfg(unix)]
    async fn run_queued_prompt_turns(
        fixture: &TerminalPromptFixture<'_>,
        guild: &BlockedCaptureGuild,
        thread: Id<ChannelMarker>,
        state: &mut BridgeState,
        live_events: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
        prompts: [&str; 2],
        queued_reply: &str,
    ) -> Result<(), String> {
        let [long_prompt, queued_prompt] = prompts;
        let TerminalPromptFixture {
            tab,
            vendor,
            connection,
            tabs,
            terminal,
            ..
        } = *fixture;
        let subs = status_subscriptions(std::slice::from_ref(&tab.pane_id));
        let mut sub = subscribe_herdr_events(&subs).await?;
        let first_pane = tab.pane_id.clone();
        let first_prompt = long_prompt.to_owned();
        let first_task = tokio::task::spawn_blocking(move || {
            submit_owner_prompt(&first_pane, &first_prompt).map(drop)
        });
        wait_for_event(
            &mut sub,
            "pane.agent_status_changed",
            &tab.pane_id,
            "/data/pane_id",
            Some(STATUS_WORKING),
            Duration::from_secs(15),
        )
        .await?;
        let working = poll_snapshot(&tab.pane_id, Duration::from_secs(10), |snapshot| {
            snapshot.session.is_some()
        })?;
        if working.agent_status != STATUS_WORKING
            || working
                .session
                .as_ref()
                .is_none_or(|session| session.agent != vendor)
        {
            return Err(format!(
                "no confirmed {vendor} working session: {working:?}"
            ));
        }
        own(&working, tabs, connection, state).await;
        let watch_deadline = Instant::now() + Duration::from_secs(5);
        while !state.live_watches.contains_key(terminal) && Instant::now() < watch_deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
            own(&working, tabs, connection, state).await;
        }
        let queued_pane = tab.pane_id.clone();
        let queued_text = queued_prompt.to_owned();
        let queued_task = tokio::task::spawn_blocking(move || {
            submit_owner_prompt(&queued_pane, &queued_text).map(drop)
        });
        loop {
            tokio::select! {
                Some(event_terminal) = live_events.recv() => {
                    handle_live_event(connection, &event_terminal, state).await;
                }
                event = wait_for_event(
                    &mut sub,
                    "pane.agent_status_changed",
                    &tab.pane_id,
                    "/data/pane_id",
                    None,
                    Duration::from_secs(90),
                ) => {
                    let event = event?;
                    if matches!(
                        event.pointer("/data/agent_status").and_then(Value::as_str),
                        Some(STATUS_DONE | STATUS_IDLE)
                    ) {
                        let settled = poll_snapshot(&tab.pane_id, Duration::from_secs(2), |_| true)?;
                        wait_for_readable_capture(&settled).await?;
                        own(&settled, tabs, connection, state).await;
                        while let Ok(event_terminal) = live_events.try_recv() {
                            handle_live_event(connection, &event_terminal, state).await;
                        }
                        handle_live_event(connection, terminal, state).await;
                        let messages = thread_full_messages(guild, thread).await?;
                        if messages.iter().any(|message| {
                            message.webhook_id.is_none() && message.content == queued_reply
                        }) {
                            break;
                        }
                    }
                }
            }
        }
        first_task
            .await
            .map_err(|error| format!("first prompt task failed: {error}"))??;
        queued_task
            .await
            .map_err(|error| format!("queued prompt task failed: {error}"))??;
        Ok(())
    }

    #[cfg(unix)]
    async fn queued_prompt_exercise(
        fixture: &TerminalPromptFixture<'_>,
        guild: &BlockedCaptureGuild,
        thread: Id<ChannelMarker>,
        state: &mut BridgeState,
        live_events: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
        nonce: &str,
    ) -> Result<(), String> {
        let vendor = fixture.vendor;
        let long_reply = format!("terminal-origin-{vendor}-long-{nonce}");
        let long_prompt = format!(
            "List the numbers from 1 to 300, one per line. On the last line write exactly: {long_reply}"
        );
        let queued_reply = format!("terminal-origin-{vendor}-queued-{nonce}");
        let queued_prompt = format!("Reply with exactly: {queued_reply}");
        run_queued_prompt_turns(
            fixture,
            guild,
            thread,
            state,
            live_events,
            [&long_prompt, &queued_prompt],
            &queued_reply,
        )
        .await?;
        let messages = thread_full_messages(guild, thread).await?;
        assert_prompt_was_not_mirrored(&messages, &long_prompt)?;
        assert_prompt_was_not_mirrored(&messages, &queued_prompt)?;
        assert_queued_reply_follows_first_reply(&messages, &long_reply, &queued_reply)
    }

    #[cfg(unix)]
    fn assert_queued_reply_follows_first_reply(
        messages: &[twilight_model::channel::Message],
        first_reply: &str,
        queued_reply: &str,
    ) -> Result<(), String> {
        let mut sorted: Vec<_> = messages.iter().collect();
        sorted.sort_by_key(|message| message.id);
        let plain_positions = |matches: &dyn Fn(&str) -> bool| -> Vec<usize> {
            sorted
                .iter()
                .enumerate()
                .filter(|(_, message)| message.webhook_id.is_none() && matches(&message.content))
                .map(|(position, _)| position)
                .collect()
        };
        let first = plain_positions(&|content| content.contains(first_reply));
        let queued = plain_positions(&|content| content == queued_reply);
        let ([first], [queued]) = (first.as_slice(), queued.as_slice()) else {
            let plain: Vec<_> = sorted
                .iter()
                .filter(|message| message.webhook_id.is_none())
                .map(|message| message.content.as_str())
                .collect();
            return Err(format!(
                "expected one first reply and one queued reply, found {} and {}; plain messages: {plain:?}",
                first.len(),
                queued.len()
            ));
        };
        if first >= queued {
            return Err(
                "the queued prompt's reply did not follow the first turn's reply".to_owned(),
            );
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn thread_full_messages(
        guild: &BlockedCaptureGuild,
        thread: Id<ChannelMarker>,
    ) -> Result<Vec<twilight_model::channel::Message>, String> {
        guild
            .client
            .channel_messages(thread)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())
    }

    #[cfg(unix)]
    fn assert_terminal_prompt_mirrored_before_reply(
        messages: &[twilight_model::channel::Message],
        prompt: &str,
        reply: &str,
        owner_display_name: &str,
    ) -> Result<(), String> {
        let mut sorted: Vec<_> = messages.iter().collect();
        sorted.sort_by_key(|message| message.id);
        let mirrored: Vec<_> = sorted
            .iter()
            .filter(|message| message.webhook_id.is_some() && message.content == prompt)
            .collect();
        let [mirrored] = mirrored.as_slice() else {
            return Err(format!(
                "expected exactly one mirrored prompt message, found {}",
                mirrored.len()
            ));
        };
        if mirrored.author.name != owner_display_name {
            return Err(format!(
                "mirrored prompt author was {:?}, expected {owner_display_name:?}",
                mirrored.author.name
            ));
        }
        let prompt_position = sorted
            .iter()
            .position(|message| message.id == mirrored.id)
            .ok_or("mirrored prompt disappeared from the sorted thread")?;
        let reply_position = sorted
            .iter()
            .position(|message| message.webhook_id.is_none() && message.content == reply)
            .ok_or_else(|| format!("assistant reply {reply:?} did not appear in the thread"))?;
        if prompt_position >= reply_position {
            return Err("mirrored prompt did not precede the assistant's reply".to_owned());
        }
        Ok(())
    }

    #[cfg(unix)]
    fn assert_prompt_was_not_mirrored(
        messages: &[twilight_model::channel::Message],
        prompt: &str,
    ) -> Result<(), String> {
        if messages
            .iter()
            .any(|message| message.webhook_id.is_some() && message.content == prompt)
        {
            return Err(format!(
                "prompt {prompt:?} submitted through the bridge's own Discord path was mirrored"
            ));
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn terminal_origin_prompt_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        agent_name: &str,
        vendor: &'static str,
    ) -> Result<(), String> {
        start_live_capture_agent(vendor, agent_name, &tab.pane_id)?;
        let idle = snapshot_for_pane(&tab.pane_id)?;
        let terminal = idle.terminal_id.clone();
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let (live_tx, mut live_events) = tokio::sync::mpsc::unbounded_channel();
        let mut state = BridgeState {
            live_tx: Some(live_tx),
            ..BridgeState::default()
        };
        let mut connection = discord_tuple(guild);
        own(&idle, tabs, &connection, &mut state).await;

        let route = route_topology(std::slice::from_ref(&idle), tabs, &terminal)?;
        let topology_cache = Arc::new(tokio::sync::Mutex::new(None));
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;

        let owner_id = Id::<UserMarker>::new(
            std::env::var("DISCORD_OWNER_ID")
                .map_err(|error| error.to_string())?
                .parse::<u64>()
                .map_err(|error| error.to_string())?,
        );
        let identity =
            herdr_connect_rs::fetch_owner_identity(guild.client.as_ref(), owner_id).await?;
        connection.4 = identity.clone();
        let nonce = agent_name_nonce()?;
        let fixture = TerminalPromptFixture {
            tab,
            agent_name,
            vendor,
            connection: &connection,
            tabs,
            terminal: &terminal,
        };

        let first_reply = format!("terminal-origin-{vendor}-first-{nonce}");
        let first_prompt = format!("Reply with exactly: {first_reply}");
        run_terminal_prompt_turn(
            &fixture,
            &mut state,
            &mut live_events,
            TerminalPromptSubmission::Terminal,
            &first_prompt,
        )
        .await?;
        let first_messages = thread_full_messages(guild, thread).await?;
        assert_terminal_prompt_mirrored_before_reply(
            &first_messages,
            &first_prompt,
            &first_reply,
            &identity.display_name,
        )?;

        let mirrored_reply = format!("terminal-origin-{vendor}-mirrored-{nonce}");
        let mirrored_prompt = format!("Reply with exactly: {mirrored_reply}");
        run_terminal_prompt_turn(
            &fixture,
            &mut state,
            &mut live_events,
            TerminalPromptSubmission::Terminal,
            &mirrored_prompt,
        )
        .await?;
        let mirrored_messages = thread_full_messages(guild, thread).await?;
        assert_terminal_prompt_mirrored_before_reply(
            &mirrored_messages,
            &mirrored_prompt,
            &mirrored_reply,
            &identity.display_name,
        )?;

        let suppressed_reply = format!("terminal-origin-{vendor}-suppressed-{nonce}");
        let suppressed_prompt = format!("Reply with exactly: {suppressed_reply}");
        run_terminal_prompt_turn(
            &fixture,
            &mut state,
            &mut live_events,
            TerminalPromptSubmission::Discord,
            &suppressed_prompt,
        )
        .await?;
        let after_suppressed = thread_full_messages(guild, thread).await?;
        assert_prompt_was_not_mirrored(&after_suppressed, &suppressed_prompt)?;

        queued_prompt_exercise(
            &fixture,
            guild,
            thread,
            &mut state,
            &mut live_events,
            &nonce,
        )
        .await?;
        Ok(())
    }

    #[cfg(unix)]
    async fn run_terminal_origin_prompt_test(vendor: &'static str) {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let label = format!("{LIVE_CAPTURE_LABEL}-{vendor}");
        assert_eq!(
            remaining_tabs(&label).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let created = live_capture_tab_fixture(vendor);
        let (tab_id, cwd_dir, result, session_cleanup) = match created {
            Ok((tab, cwd_dir)) => {
                let agent_name = format!(
                    "toc-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome =
                    terminal_origin_prompt_exercise(&guild, &tab, &agent_name, vendor).await;
                let session_cleanup = match snapshot_for_pane(&tab.pane_id) {
                    Ok(snapshot) => snapshot.session.as_ref().map_or(Ok(()), |session| {
                        resolve_session_path(&home, &snapshot, session)
                            .map_err(|error| error.to_string())
                            .and_then(|path| {
                                fs::remove_file(path).map_err(|error| error.to_string())
                            })
                    }),
                    Err(error) if error.contains("agent.list has no entry") => Ok(()),
                    Err(error) => Err(error),
                };
                (Some(tab.tab_id), Some(cwd_dir), outcome, session_cleanup)
            }
            Err(error) => (None, None, Err(error), Ok(())),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        if let Some(cwd_dir) = cwd_dir {
            let _ = clear_directory_contents(&cwd_dir);
        }
        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(&label).expect("tab.list succeeds");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert!(session_cleanup.is_ok(), "{session_cleanup:?}");
        assert!(result.is_ok(), "{result:?}");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn terminal_origin_claude_prompt_mirrors_new_prompts_and_suppresses_bridge_submitted_ones()
     {
        run_terminal_origin_prompt_test(VENDOR_CLAUDE).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn terminal_origin_codex_prompt_mirrors_new_prompts_and_suppresses_bridge_submitted_ones()
    {
        run_terminal_origin_prompt_test(VENDOR_CODEX).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn terminal_origin_cursor_prompt_mirrors_new_prompts_and_suppresses_bridge_submitted_ones()
     {
        run_terminal_origin_prompt_test(VENDOR_CURSOR).await;
    }

    #[cfg(unix)]
    const ACTIVITY_LABEL: &str = "testrun-activity";

    #[cfg(unix)]
    const ACTIVITY_FORCE_PROMPT: &str =
        "Run the shell command `echo activity-check`. Then say the word done.";

    #[cfg(unix)]
    fn activity_tab_fixture() -> Result<(Tab, PathBuf), String> {
        let workspace_id = std::env::var("HERDR_WORKSPACE_ID").map_err(|_| {
            "HERDR_WORKSPACE_ID is set by the real Herdr pane environment".to_owned()
        })?;
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .map_err(|_| "HOME is set by the real Herdr pane environment".to_owned())?;
        let label = format!("{ACTIVITY_LABEL}-claude");
        let cwd_dir = claude_testrun_dir(&home);
        clear_directory_contents(&cwd_dir)?;
        let cwd = cwd_dir
            .to_str()
            .ok_or_else(|| "temp cwd is valid UTF-8".to_owned())?;
        let tab = create_tab(&label, &workspace_id, cwd)?;
        Ok((tab, cwd_dir))
    }

    #[cfg(unix)]
    fn activity_binary_path() -> Result<PathBuf, String> {
        let current = std::env::current_exe().map_err(|error| error.to_string())?;
        let deps_dir = current
            .parent()
            .ok_or("test binary has no parent directory")?;
        let target_dir = deps_dir
            .parent()
            .ok_or("deps directory has no parent directory")?;
        let candidate = target_dir.join("herdr-connect-rs");
        if candidate.is_file() {
            Ok(candidate)
        } else {
            Err(format!(
                "built herdr-connect-rs binary not found at {}",
                candidate.display()
            ))
        }
    }

    #[cfg(unix)]
    fn write_activity_settings(broker_socket: &Path) -> Result<PathBuf, String> {
        let binary = activity_binary_path()?;
        let binary = binary
            .to_str()
            .ok_or_else(|| "built binary path is valid UTF-8".to_owned())?;
        let socket_arg = broker_socket
            .to_str()
            .ok_or_else(|| "broker socket path is valid UTF-8".to_owned())?;
        let command = format!("{binary} activity --vendor claude --socket {socket_arg}");
        let settings = json!({
            "hooks": {
                "PreToolUse": [
                    {
                        "matcher": "",
                        "hooks": [
                            {"type": "command", "command": command, "async": true}
                        ]
                    }
                ]
            }
        });
        let path = std::env::temp_dir().join(format!(
            "herdr-connect-rs-activity-settings-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|error| error.to_string())?
                .as_nanos()
        ));
        fs::write(
            &path,
            serde_json::to_vec(&settings).map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        Ok(path)
    }

    #[cfg(unix)]
    fn start_claude_haiku_agent_with_settings(
        name: &str,
        pane_id: &str,
        settings_path: &Path,
    ) -> Result<(), String> {
        herdr_json(&[
            "pane",
            "wait-output",
            pane_id,
            "--match",
            "╰─",
            "--timeout",
            "15000",
        ])?;
        let settings_arg = settings_path
            .to_str()
            .ok_or_else(|| "settings path is valid UTF-8".to_owned())?;
        let bound = Duration::from_secs(10);
        let start = Instant::now();
        loop {
            let args = [
                "agent",
                "start",
                name,
                "--kind",
                "claude",
                "--pane",
                pane_id,
                "--timeout",
                "60000",
                "--",
                "--model",
                "haiku",
                "--settings",
                settings_arg,
            ];
            match herdr_json(&args) {
                Ok(_) => return Ok(()),
                Err(error) if is_agent_pane_busy(&error) && start.elapsed() < bound => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[cfg(unix)]
    async fn drive_one_activity_turn(
        pane_id: &str,
        tabs: &[herdr_connect_rs::HerdrTab],
        sub: &mut herdr_connect_rs::HerdrSubscription,
        connection: &super::DiscordConnection,
        state: &mut BridgeState,
        (activity_rx, frames): (
            &mut tokio::sync::mpsc::UnboundedReceiver<herdr_connect_rs::ActivityFrame>,
            &mut Vec<String>,
        ),
        prompt: &str,
    ) -> Result<AgentSnapshot, String> {
        let started = Instant::now();
        submit_owner_prompt(pane_id, prompt)?;
        wait_for_event(
            sub,
            "pane.agent_status_changed",
            pane_id,
            "/data/pane_id",
            Some("working"),
            Duration::from_secs(15),
        )
        .await?;
        let working = poll_snapshot(pane_id, Duration::from_secs(10), |s| s.session.is_some())?;
        own(&working, tabs, connection, state).await;

        let settled = loop {
            tokio::select! {
                Some(frame) = activity_rx.recv() => {
                    frames.push(activity_frame_line(started, &frame, state));
                    super::handle_activity_event(connection, frame, state).await;
                }
                event = wait_for_event(
                    sub, "pane.agent_status_changed", pane_id, "/data/pane_id", None,
                    Duration::from_secs(30),
                ) => {
                    let event = event?;
                    if matches!(
                        event.pointer("/data/agent_status").and_then(Value::as_str),
                        Some("done" | "idle")
                    ) {
                        break poll_snapshot(pane_id, Duration::from_secs(2), |_| true)?;
                    }
                }
            }
        };
        own(&settled, tabs, connection, state).await;
        Ok(settled)
    }

    #[cfg(unix)]
    fn activity_frame_line(
        started: Instant,
        frame: &herdr_connect_rs::ActivityFrame,
        state: &BridgeState,
    ) -> String {
        format!(
            "+{}ms pane={} tool={} summary={:?} session={:?} eligible={}",
            started.elapsed().as_millis(),
            frame.pane_id,
            frame.tool,
            frame.summary.chars().take(40).collect::<String>(),
            frame.session_id.chars().take(16).collect::<String>(),
            state.activity_eligible_panes.contains(&frame.pane_id),
        )
    }

    #[cfg(unix)]
    fn activity_message_rows(
        messages: &[(String, bool, Id<MessageMarker>)],
    ) -> Vec<&(String, bool, Id<MessageMarker>)> {
        let mut rows: Vec<_> = messages
            .iter()
            .filter(|(content, embed, _)| !embed && content.starts_with('⚙'))
            .collect();
        rows.sort_by_key(|(_, _, id)| *id);
        rows
    }

    #[cfg(unix)]
    fn assert_first_turn_activity(
        messages: &[(String, bool, Id<MessageMarker>)],
        frames: &[String],
    ) -> Result<Id<MessageMarker>, String> {
        if let Some((content, _, _)) = messages.iter().find(|(_, embed, _)| *embed) {
            return Err(format!(
                "a card was posted for turn one: {content:?}; frames received: {frames:?}"
            ));
        }
        let rows = activity_message_rows(messages);
        let [(text, _, activity_id)] = rows.as_slice() else {
            return Err(format!(
                "expected exactly one activity message after turn one, thread has {messages:?}; frames received: {frames:?}"
            ));
        };
        if !text.contains("Bash") {
            return Err(format!(
                "activity message did not name Bash: {text}; frames received: {frames:?}"
            ));
        }
        Ok(*activity_id)
    }

    #[cfg(unix)]
    fn assert_second_turn_activity(
        messages: &[(String, bool, Id<MessageMarker>)],
        first_activity_id: Id<MessageMarker>,
        frames: &[String],
    ) -> Result<(), String> {
        let rows = activity_message_rows(messages);
        let [first_row, second_row] = rows.as_slice() else {
            return Err(format!(
                "expected exactly two activity messages after turn two, thread has {messages:?}; frames received: {frames:?}"
            ));
        };
        if first_row.2 != first_activity_id {
            return Err(format!(
                "turn one's activity message changed identity: {first_row:?}; frames received: {frames:?}"
            ));
        }
        if !second_row.0.starts_with("⚙️ 1 ·") {
            return Err(format!(
                "turn two's activity message did not start a fresh count: {}; frames received: {frames:?}",
                second_row.0
            ));
        }
        if !second_row.0.contains("Bash") {
            return Err(format!(
                "turn two's activity message did not name Bash: {}; frames received: {frames:?}",
                second_row.0
            ));
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn activity_hook_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        agent_name: &str,
        broker_socket: &Path,
        settings_path: &Path,
    ) -> Result<(), String> {
        let shared_cache: herdr_connect_rs::TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let connection = discord_tuple_with_cache(guild, Arc::clone(&shared_cache));
        let mut state = BridgeState::default();
        let mut frames = Vec::new();

        let (activity_tx, mut activity_rx) = tokio::sync::mpsc::unbounded_channel();
        let responder = Arc::clone(&connection.3);
        let broker_socket_owned = broker_socket.to_path_buf();
        let broker_task = tokio::spawn(async move {
            super::run_permission_broker(&broker_socket_owned, responder, activity_tx).await
        });
        let broker_deadline = Instant::now() + Duration::from_secs(2);
        while !broker_socket.exists() && Instant::now() < broker_deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if !broker_socket.exists() {
            broker_task.abort();
            return Err("test broker did not create its socket".to_owned());
        }

        start_claude_haiku_agent_with_settings(agent_name, &tab.pane_id, settings_path)?;
        let idle = snapshot_for_pane(&tab.pane_id)?;
        let terminal = idle.terminal_id.clone();
        let matching = matching_tab(&tab.tab_id)?;
        let (tabs, agents) = (std::slice::from_ref(&matching), std::slice::from_ref(&idle));

        own(&idle, tabs, &connection, &mut state).await;
        let route = route_topology(agents, tabs, &terminal)?;
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &shared_cache).await?;

        let subs = status_subscriptions(std::slice::from_ref(&tab.pane_id));
        let mut sub = subscribe_herdr_events(&subs).await?;

        drive_one_activity_turn(
            &tab.pane_id,
            tabs,
            &mut sub,
            &connection,
            &mut state,
            (&mut activity_rx, &mut frames),
            ACTIVITY_FORCE_PROMPT,
        )
        .await?;
        let after_first_turn = thread_messages(guild, thread).await?;
        let first_activity_id = assert_first_turn_activity(&after_first_turn, &frames)?;

        let late_frame = herdr_connect_rs::ActivityFrame {
            kind: herdr_connect_rs::ACTIVITY_KIND.to_owned(),
            vendor: VENDOR_CLAUDE.to_owned(),
            workspace_id: route.workspace_id.clone(),
            tab_id: route.tab_id.clone(),
            pane_id: route.pane_id.clone(),
            session_id: "late-frame-synthetic".to_owned(),
            tool: "LateGhost".to_owned(),
            summary: "late-frame-should-be-dropped".to_owned(),
        };
        let late_started = Instant::now();
        herdr_connect_rs::send_activity_frame(&late_frame, broker_socket, Duration::from_secs(1))
            .await;
        let received = tokio::time::timeout(Duration::from_secs(2), activity_rx.recv())
            .await
            .map_err(|_| "late synthetic frame was not forwarded by the broker".to_owned())?
            .ok_or_else(|| "activity channel closed before the late frame arrived".to_owned())?;
        frames.push(activity_frame_line(late_started, &received, &state));
        super::handle_activity_event(&connection, received, &mut state).await;
        let after_late_frame = thread_messages(guild, thread).await?;
        if after_late_frame != after_first_turn {
            return Err(format!(
                "late frame changed the thread: before {after_first_turn:?}, after {after_late_frame:?}"
            ));
        }

        drive_one_activity_turn(
            &tab.pane_id,
            tabs,
            &mut sub,
            &connection,
            &mut state,
            (&mut activity_rx, &mut frames),
            ACTIVITY_FORCE_PROMPT,
        )
        .await?;
        broker_task.abort();
        let _ = std::fs::remove_file(broker_socket);

        let after_second_turn = thread_messages(guild, thread).await?;
        assert_second_turn_activity(&after_second_turn, first_activity_id, &frames)
    }

    #[cfg(unix)]
    async fn run_activity_hook_test() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let label = format!("{ACTIVITY_LABEL}-claude");
        assert_eq!(
            remaining_tabs(&label).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let broker_socket = std::env::temp_dir().join(format!(
            "herdr-connect-rs-activity-broker-{}-{}.sock",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        let settings_path = write_activity_settings(&broker_socket)
            .expect("write activity settings for the exercise");
        let created = activity_tab_fixture();
        let (tab_id, cwd_dir, result) = match created {
            Ok((tab, cwd_dir)) => {
                let agent_name = format!(
                    "activity-claude-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome = tokio::time::timeout(
                    Duration::from_secs(300),
                    activity_hook_exercise(
                        &guild,
                        &tab,
                        &agent_name,
                        &broker_socket,
                        &settings_path,
                    ),
                )
                .await
                .unwrap_or_else(|_| Err("activity hook exercise timed out".to_owned()));
                cleanup_real_claude_session_dir(&home, &tab.pane_id);
                (Some(tab.tab_id), Some(cwd_dir), outcome)
            }
            Err(error) => (None, None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        if let Some(cwd_dir) = cwd_dir {
            let _ = clear_directory_contents(&cwd_dir);
        }
        let _ = std::fs::remove_file(&broker_socket);
        let _ = std::fs::remove_file(&settings_path);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left =
            remaining_tabs(&label).expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn activity_hook_keeps_messages_inside_their_turn() {
        run_activity_hook_test().await;
    }

    #[cfg(unix)]
    const QUESTION_LABEL: &str = "testrun-question";

    #[cfg(unix)]
    const QUESTION_FORCE_PROMPT: &str = "Use the AskUserQuestion tool right now. Ask exactly one \
        question with header \"Color\", question text \"Which color?\", and exactly two options: \
        label \"Red\" with description \"The color red\", and label \"Blue\" with description \
        \"The color blue\". multiSelect must be false. Do not do anything else and do not say \
        anything else.";

    #[cfg(unix)]
    const WRAPPED_QUESTION: &str = "Your message is only a pasted kickoff for gauntlet run gk1004b on issue #110 (it reads as if another agent wrote it). It asks for discovery, sizing and a dispatch plan before any code is written, so which mode should I run it in?";

    #[cfg(unix)]
    const WRAPPED_QUESTION_FORCE_PROMPT: &str = "Use the AskUserQuestion tool right now. Ask exactly one question with header \"Gauntlet mode\", question text \"Your message is only a pasted kickoff for gauntlet run gk1004b on issue #110 (it reads as if another agent wrote it). It asks for discovery, sizing and a dispatch plan before any code is written, so which mode should I run it in?\", and exactly two options: label \"Run it as written\" with description \"Execute the kickoff following its stated objectives for discovery, sizing, and dispatch plan\", and label \"Do not run\" with description \"Do not execute the gauntlet run\". multiSelect must be false. Do not do anything else and do not say anything else.";

    #[cfg(unix)]
    const MULTI_QUESTION_FORCE_PROMPT: &str = "Use the AskUserQuestion tool right now. Ask exactly \
        one question with header \"Toppings\", question text \"Which toppings?\", and exactly \
        three options: label \"Cheese\" with description \"Add cheese\", label \"Olives\" with \
        description \"Add olives\", and label \"Mushrooms\" with description \"Add mushrooms\". \
        multiSelect must be true. Do not do anything else and do not say anything else.";

    #[cfg(unix)]
    fn question_tab_fixture() -> Result<(Tab, PathBuf), String> {
        let workspace_id = std::env::var("HERDR_WORKSPACE_ID").map_err(|_| {
            "HERDR_WORKSPACE_ID is set by the real Herdr pane environment".to_owned()
        })?;
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .map_err(|_| "HOME is set by the real Herdr pane environment".to_owned())?;
        let label = format!("{QUESTION_LABEL}-claude");
        let cwd_dir = claude_testrun_dir(&home);
        clear_directory_contents(&cwd_dir)?;
        let cwd = cwd_dir
            .to_str()
            .ok_or_else(|| "temp cwd is valid UTF-8".to_owned())?;
        create_tab(&label, &workspace_id, cwd).map(|tab| (tab, cwd_dir))
    }

    #[cfg(unix)]
    fn synthetic_component_interaction(
        guild_id: Id<GuildMarker>,
        owner_id: &str,
        channel: &twilight_model::channel::Channel,
        custom_id: &str,
        values: &[&str],
    ) -> Result<twilight_model::application::interaction::Interaction, String> {
        let channel_value = serde_json::to_value(channel).map_err(|error| error.to_string())?;
        let component_type = if values.is_empty() { 2 } else { 3 };
        let payload = json!({
            "id": "1",
            "application_id": "1",
            "authorizing_integration_owners": {},
            "token": "synthetic-test-token",
            "type": 3,
            "guild_id": guild_id.to_string(),
            "channel": channel_value,
            "user": {"id": owner_id, "username": "owner", "discriminator": "0"},
            "data": {
                "custom_id": custom_id,
                "component_type": component_type,
                "values": values,
            },
        });
        serde_json::from_value(payload).map_err(|error| error.to_string())
    }

    #[cfg(unix)]
    fn transcript_question_answer(
        home: &Path,
        snapshot: &AgentSnapshot,
        question: &str,
    ) -> Result<Option<String>, String> {
        let session = snapshot
            .session
            .as_ref()
            .ok_or_else(|| "settled pane has no reported session".to_owned())?;
        let path =
            resolve_session_path(home, snapshot, session).map_err(|error| error.to_string())?;
        let contents = fs::read_to_string(&path).map_err(|error| error.to_string())?;
        Ok(contents.lines().rev().find_map(|line| {
            let record: Value = serde_json::from_str(line).ok()?;
            record
                .get("toolUseResult")?
                .get("answers")?
                .get(question)?
                .as_str()
                .map(str::to_owned)
        }))
    }

    #[cfg(unix)]
    const TWO_QUESTION_FORCE_PROMPT: &str = "Use the AskUserQuestion tool right now, in a single \
        call with exactly two questions. First: header \"Color\", question text \"Which color?\", \
        multiSelect false, and exactly two options: label \"Red\" with description \"The color \
        red\", and label \"Blue\" with description \"The color blue\". Second: header \
        \"Toppings\", question text \"Which toppings?\", multiSelect true, and exactly three \
        options: label \"Cheese\" with description \"Add cheese\", label \"Olives\" with \
        description \"Add olives\", and label \"Mushrooms\" with description \"Add mushrooms\". \
        Do not do anything else and do not say anything else.";

    #[cfg(unix)]
    const TWO_SINGLE_QUESTION_FORCE_PROMPT: &str = "Use the AskUserQuestion tool right now, in a \
        single call with exactly two questions. First: header \"Color\", question text \"Which \
        color?\", multiSelect false, and exactly two options: label \"Red\" with description \
        \"The color red\", and label \"Blue\" with description \"The color blue\". Second: header \
        \"Size\", question text \"Which size?\", multiSelect false, and exactly two options: label \
        \"Small\" with description \"A small one\", and label \"Large\" with description \"A large \
        one\". Do not do anything else and do not say anything else.";

    #[cfg(unix)]
    enum QuestionAction {
        Button(usize),
        Select(&'static [&'static str]),
        Text(&'static str),
        TwoQuestions(&'static QuestionAction),
        TerminalThenButton,
    }

    #[cfg(unix)]
    struct QuestionOutcome {
        status_before: String,
        card: Option<String>,
        answered: Vec<Option<String>>,
        messages: Vec<String>,
    }

    #[cfg(unix)]
    async fn question_dialog_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        agent_name: &str,
        prompt: &str,
        questions: &[&str],
        action: QuestionAction,
    ) -> Result<QuestionOutcome, String> {
        let shared_cache: herdr_connect_rs::TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let connection = discord_tuple_with_cache(guild, Arc::clone(&shared_cache));
        let responder = Arc::clone(&connection.3);

        start_claude_haiku_agent(agent_name, &tab.pane_id)?;
        let idle = snapshot_for_pane(&tab.pane_id)?;
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = [matching];
        let route = route_topology(std::slice::from_ref(&idle), &tabs, &idle.terminal_id)?;
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &shared_cache).await?;
        let mut state = BridgeState::default();
        own(&idle, &tabs, &connection, &mut state).await;

        submit_owner_prompt(&tab.pane_id, prompt)?;
        let blocked = poll_snapshot(&tab.pane_id, Duration::from_secs(90), |snapshot| {
            snapshot.agent_status == "blocked"
        })?;
        if blocked.agent_status != "blocked" {
            return Err(format!(
                "pane never blocked on its question dialog, status {}",
                blocked.agent_status
            ));
        }
        state
            .previous
            .insert(blocked.terminal_id.clone(), STATUS_WORKING.to_owned());
        own(&blocked, &tabs, &connection, &mut state).await;
        let token = responder
            .pending_question_token(&thread)
            .ok_or("the blocked-card path posted no question card")?;
        let status_before = snapshot_for_pane(&tab.pane_id)?.agent_status;

        let channel = guild
            .client
            .channel(thread)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        let terminal_answered = matches!(action, QuestionAction::TerminalThenButton);
        perform_question_action(guild, &connection, tab, &channel, &token, action).await?;

        let settled = poll_snapshot(&tab.pane_id, Duration::from_secs(60), |snapshot| {
            matches!(snapshot.agent_status.as_str(), "done" | "idle")
        })?;
        if !terminal_answered {
            own(&settled, &tabs, &connection, &mut state).await;
        }
        let messages = thread_messages(guild, thread).await?;
        let card = messages
            .iter()
            .find(|(content, _, _)| content.starts_with("resolved:"))
            .map(|(content, _, _)| content.clone());
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .map_err(|_| "HOME is set by the real Herdr pane environment".to_owned())?;
        let answered = questions
            .iter()
            .map(|question| transcript_question_answer(&home, &settled, question))
            .collect::<Result<_, _>>()?;
        Ok(QuestionOutcome {
            status_before,
            card,
            answered,
            messages: messages
                .into_iter()
                .map(|(content, _, _)| content)
                .collect(),
        })
    }

    #[cfg(unix)]
    async fn click_component(
        responder: &Arc<PermissionResponder<Discord>>,
        interaction: twilight_model::application::interaction::Interaction,
    ) -> Result<(), String> {
        let component = responder
            .chat()
            .component(interaction)
            .ok_or("not a component")?;
        Box::pin(super::handle_component(Arc::clone(responder), component)).await;
        Ok(())
    }

    #[cfg(unix)]
    async fn perform_question_action(
        guild: &BlockedCaptureGuild,
        connection: &super::DiscordConnection,
        tab: &Tab,
        channel: &twilight_model::channel::Channel,
        token: &str,
        action: QuestionAction,
    ) -> Result<(), String> {
        let responder = Arc::clone(&connection.3);
        let thread = channel.id;
        let click = |custom_id: String, values: &'static [&'static str]| {
            synthetic_component_interaction(guild.id, &connection.2, channel, &custom_id, values)
        };
        match action {
            QuestionAction::Button(index) => {
                let interaction = click(format!("herdrask:{token}:{index}"), &[])?;
                click_component(&responder, interaction).await?;
            }
            QuestionAction::Select(values) => {
                let interaction = click(format!("herdrask-multi:{token}"), values)?;
                click_component(&responder, interaction).await?;
            }
            QuestionAction::Text(text) => {
                let mut message = guild
                    .client
                    .create_message(thread)
                    .content(text)
                    .await
                    .map_err(|error| error.to_string())?
                    .model()
                    .await
                    .map_err(|error| error.to_string())?;
                message.author.id = Id::<UserMarker>::new(
                    connection
                        .2
                        .parse::<u64>()
                        .map_err(|error| error.to_string())?,
                );
                message.author.bot = false;
                message.guild_id = Some(guild.id);
                herdr_connect_rs::handle_owner_message(
                    Arc::clone(&guild.client),
                    guild.id,
                    &connection.2,
                    message,
                    &responder,
                )
                .await?;
            }
            QuestionAction::TwoQuestions(second) => {
                let card_before = card_message_ids(guild, thread).await?;
                let interaction = click(format!("herdrask:{token}:1"), &[])?;
                click_component(&responder, interaction).await?;
                let second_token = responder
                    .pending_question_token(&thread)
                    .filter(|next| next != token)
                    .ok_or("the card never moved to the second question")?;
                if card_message_ids(guild, thread).await? != card_before {
                    return Err("the second question was not edited into the same card".to_owned());
                }
                let interaction = match second {
                    QuestionAction::Button(index) => {
                        click(format!("herdrask:{second_token}:{index}"), &[])?
                    }
                    QuestionAction::Select(values) => {
                        click(format!("herdrask-multi:{second_token}"), values)?
                    }
                    _ => return Err("the second answer must be a button or a select".to_owned()),
                };
                click_component(&responder, interaction).await?;
            }
            QuestionAction::TerminalThenButton => {
                let status = Command::new("herdr")
                    .args(["agent", "send-keys", &tab.pane_id, "1"])
                    .status()
                    .map_err(|error| error.to_string())?;
                if !status.success() {
                    return Err("herdr agent send-keys failed".to_owned());
                }
                poll_snapshot(&tab.pane_id, Duration::from_secs(30), |snapshot| {
                    snapshot.agent_status != "blocked"
                })?;
                let interaction = click(format!("herdrask:{token}:1"), &[])?;
                click_component(&responder, interaction).await?;
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn card_message_ids(
        guild: &BlockedCaptureGuild,
        thread: Id<ChannelMarker>,
    ) -> Result<Vec<Id<MessageMarker>>, String> {
        Ok(thread_messages(guild, thread)
            .await?
            .into_iter()
            .filter(|(_, has_embed, _)| *has_embed)
            .map(|(_, _, id)| id)
            .collect())
    }

    #[cfg(unix)]
    async fn run_question_dialog_test(
        prompt: &str,
        questions: &[&str],
        action: QuestionAction,
    ) -> Option<QuestionOutcome> {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return None;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let label = format!("{QUESTION_LABEL}-claude");
        assert_eq!(
            remaining_tabs(&label).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let (tab_id, cwd_dir, result) = match question_tab_fixture() {
            Ok((tab, cwd_dir)) => {
                let agent_name = format!(
                    "question-claude-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome = tokio::time::timeout(
                    Duration::from_secs(240),
                    Box::pin(question_dialog_exercise(
                        &guild,
                        &tab,
                        &agent_name,
                        prompt,
                        questions,
                        action,
                    )),
                )
                .await
                .unwrap_or_else(|_| Err("question dialog exercise timed out".to_owned()));
                cleanup_real_claude_session_dir(&home, &tab.pane_id);
                (Some(tab.tab_id), Some(cwd_dir), outcome)
            }
            Err(error) => (None, None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        if let Some(cwd_dir) = cwd_dir {
            let _ = clear_directory_contents(&cwd_dir);
        }
        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left =
            remaining_tabs(&label).expect("tab.list succeeds for the zero-leftover check");
        let outcome = result.unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        Some(outcome)
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn question_card_tap_types_the_option_into_the_open_dialog() {
        let Some(outcome) = Box::pin(run_question_dialog_test(
            WRAPPED_QUESTION_FORCE_PROMPT,
            &[WRAPPED_QUESTION],
            QuestionAction::Button(1),
        ))
        .await
        else {
            return;
        };
        assert_eq!(outcome.status_before, "blocked");
        assert_eq!(outcome.answered, [Some("Do not run".to_owned())]);
        assert_eq!(outcome.card.as_deref(), Some("resolved: pane left blocked"));
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn question_card_answered_in_the_terminal_is_retired_without_typing() {
        let Some(outcome) = Box::pin(run_question_dialog_test(
            QUESTION_FORCE_PROMPT,
            &["Which color?"],
            QuestionAction::TerminalThenButton,
        ))
        .await
        else {
            return;
        };
        assert_eq!(outcome.status_before, "blocked");
        assert_eq!(outcome.answered, [Some("Red".to_owned())]);
        assert_eq!(
            outcome.card.as_deref(),
            Some("resolved: answered in the terminal")
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn multi_select_question_card_submits_every_picked_option() {
        let Some(outcome) = Box::pin(run_question_dialog_test(
            MULTI_QUESTION_FORCE_PROMPT,
            &["Which toppings?"],
            QuestionAction::Select(&["0", "2"]),
        ))
        .await
        else {
            return;
        };
        assert_eq!(outcome.status_before, "blocked");
        assert_eq!(outcome.answered, [Some("Cheese, Mushrooms".to_owned())]);
        assert_eq!(outcome.card.as_deref(), Some("resolved: pane left blocked"));
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn question_card_free_text_reply_is_typed_into_the_dialog() {
        let Some(outcome) = Box::pin(run_question_dialog_test(
            QUESTION_FORCE_PROMPT,
            &["Which color?"],
            QuestionAction::Text("purple"),
        ))
        .await
        else {
            return;
        };
        assert_eq!(outcome.status_before, "blocked");
        assert!(
            outcome.answered[0]
                .as_deref()
                .is_some_and(|answer| answer.contains("purple")),
            "transcript answer was {:?}",
            outcome.answered
        );
        assert!(
            outcome
                .messages
                .iter()
                .any(|content| content == "accepted: answer typed into the dialog"),
            "thread has {:?}",
            outcome.messages
        );
        assert_eq!(outcome.card.as_deref(), Some("resolved: pane left blocked"));
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn two_single_select_question_call_is_answered_from_discord_and_submitted() {
        let Some(outcome) = Box::pin(run_question_dialog_test(
            TWO_SINGLE_QUESTION_FORCE_PROMPT,
            &["Which color?", "Which size?"],
            QuestionAction::TwoQuestions(&QuestionAction::Button(1)),
        ))
        .await
        else {
            return;
        };
        assert_eq!(outcome.status_before, "blocked");
        assert_eq!(
            outcome.answered,
            [Some("Blue".to_owned()), Some("Large".to_owned())]
        );
        assert_eq!(outcome.card.as_deref(), Some("resolved: pane left blocked"));
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn two_question_call_is_answered_from_discord_on_one_card() {
        let Some(outcome) = Box::pin(run_question_dialog_test(
            TWO_QUESTION_FORCE_PROMPT,
            &["Which color?", "Which toppings?"],
            QuestionAction::TwoQuestions(&QuestionAction::Select(&["0", "2"])),
        ))
        .await
        else {
            return;
        };
        assert_eq!(outcome.status_before, "blocked");
        assert_eq!(
            outcome.answered,
            [
                Some("Blue".to_owned()),
                Some("Cheese, Mushrooms".to_owned())
            ]
        );
        assert_eq!(outcome.card.as_deref(), Some("resolved: pane left blocked"));
    }

    #[cfg(unix)]
    const CODEX_ACTIVITY_BROKER_SOCKET: &str = "/tmp/herdr-claude-broker.sock";

    #[cfg(unix)]
    fn assert_codex_activity_environment() -> Result<(), String> {
        let codex_home =
            std::env::var("CODEX_HOME").map_err(|_| "CODEX_HOME is not set".to_owned())?;

        let hooks_path = Path::new(&codex_home).join("hooks.json");
        let hooks_raw = std::fs::read_to_string(&hooks_path)
            .map_err(|error| format!("{}: {error}", hooks_path.display()))?;
        let hooks: serde_json::Value = serde_json::from_str(&hooks_raw)
            .map_err(|error| format!("{}: {error}", hooks_path.display()))?;
        let activity_socket_flag = format!("--socket {CODEX_ACTIVITY_BROKER_SOCKET}");
        let activity_command = hooks["hooks"]["PreToolUse"][0]["hooks"][0]["command"]
            .as_str()
            .ok_or_else(|| format!("{}: no PreToolUse activity hook", hooks_path.display()))?;
        if !activity_command.contains(&activity_socket_flag) {
            return Err(format!(
                "{}: PreToolUse hook does not target {CODEX_ACTIVITY_BROKER_SOCKET}: {activity_command}",
                hooks_path.display()
            ));
        }

        let config_path = Path::new(&codex_home).join("config.toml");
        let config_raw = std::fs::read_to_string(&config_path)
            .map_err(|error| format!("{}: {error}", config_path.display()))?;
        let hooks_path_str = hooks_path
            .to_str()
            .ok_or_else(|| "hooks.json path is valid UTF-8".to_owned())?;
        for (pascal_event, snake_event) in [
            ("SessionStart", "session_start"),
            ("PermissionRequest", "permission_request"),
            ("PreToolUse", "pre_tool_use"),
        ] {
            if hooks["hooks"][pascal_event].is_null() {
                continue;
            }
            let trust_key = format!("[hooks.state.\"{hooks_path_str}:{snake_event}:0:0\"]");
            if !config_raw.lines().any(|line| line.trim() == trust_key) {
                return Err(format!(
                    "{}: no trust record for {hooks_path_str}:{snake_event}:0:0",
                    config_path.display()
                ));
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    fn codex_activity_tab_fixture(label: &str) -> Result<(Tab, PathBuf), String> {
        let workspace_id = std::env::var("HERDR_WORKSPACE_ID").map_err(|_| {
            "HERDR_WORKSPACE_ID is set by the real Herdr pane environment".to_owned()
        })?;
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .map_err(|_| "HOME is set by the real Herdr pane environment".to_owned())?;
        let cwd_dir = codex_testrun_dir(&home);
        clear_directory_contents(&cwd_dir)?;
        let cwd = cwd_dir
            .to_str()
            .ok_or_else(|| "temp cwd is valid UTF-8".to_owned())?;
        let tab = create_tab(label, &workspace_id, cwd)?;
        Ok((tab, cwd_dir))
    }

    #[cfg(unix)]
    fn cleanup_real_codex_session_file(home: &Path, pane_id: &str) {
        let Ok(snapshot) = snapshot_for_pane(pane_id) else {
            return;
        };
        let Some(session) = snapshot.session.as_ref() else {
            return;
        };
        if let Ok(path) = resolve_session_path(home, &snapshot, session) {
            let _ = fs::remove_file(path);
        }
    }

    #[cfg(unix)]
    struct RemoveSocketOnDrop<'a>(&'a Path);

    #[cfg(unix)]
    impl Drop for RemoveSocketOnDrop<'_> {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(self.0);
        }
    }

    #[cfg(unix)]
    async fn codex_activity_hook_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        agent_name: &str,
        broker_socket: &Path,
    ) -> Result<(), String> {
        let shared_cache: herdr_connect_rs::TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let connection = discord_tuple_with_cache(guild, Arc::clone(&shared_cache));
        let mut state = BridgeState::default();
        let mut frames = Vec::new();

        let (activity_tx, mut activity_rx) = tokio::sync::mpsc::unbounded_channel();
        let responder = Arc::clone(&connection.3);
        let broker_socket_owned = broker_socket.to_path_buf();
        let mut broker_task = tokio::spawn(async move {
            super::run_permission_broker(&broker_socket_owned, responder, activity_tx).await
        });
        let broker_deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if broker_socket.exists() {
                break;
            }
            if Instant::now() >= broker_deadline {
                broker_task.abort();
                return Err("test broker did not create its socket".to_owned());
            }
            tokio::select! {
                () = tokio::time::sleep(Duration::from_millis(10)) => {}
                joined = &mut broker_task => {
                    return Err(format!("test broker exited before binding: {joined:?}"));
                }
            }
        }
        let _remove_socket_on_drop = RemoveSocketOnDrop(broker_socket);

        start_live_capture_agent("codex", agent_name, &tab.pane_id)?;
        let idle = snapshot_for_pane(&tab.pane_id)?;
        let terminal = idle.terminal_id.clone();
        let matching = matching_tab(&tab.tab_id)?;
        let (tabs, agents) = (std::slice::from_ref(&matching), std::slice::from_ref(&idle));

        own(&idle, tabs, &connection, &mut state).await;
        let route = route_topology(agents, tabs, &terminal)?;
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &shared_cache).await?;

        let subs = status_subscriptions(std::slice::from_ref(&tab.pane_id));
        let mut sub = subscribe_herdr_events(&subs).await?;

        drive_one_activity_turn(
            &tab.pane_id,
            tabs,
            &mut sub,
            &connection,
            &mut state,
            (&mut activity_rx, &mut frames),
            ACTIVITY_FORCE_PROMPT,
        )
        .await?;
        let after_first_turn = thread_messages(guild, thread).await?;
        let first_activity_id = assert_first_turn_activity(&after_first_turn, &frames)?;

        let late_frame = herdr_connect_rs::ActivityFrame {
            kind: herdr_connect_rs::ACTIVITY_KIND.to_owned(),
            vendor: VENDOR_CODEX.to_owned(),
            workspace_id: route.workspace_id.clone(),
            tab_id: route.tab_id.clone(),
            pane_id: route.pane_id.clone(),
            session_id: "late-frame-synthetic".to_owned(),
            tool: "LateGhost".to_owned(),
            summary: "late-frame-should-be-dropped".to_owned(),
        };
        let late_started = Instant::now();
        herdr_connect_rs::send_activity_frame(&late_frame, broker_socket, Duration::from_secs(1))
            .await;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let received = tokio::time::timeout_at(deadline, activity_rx.recv())
                .await
                .map_err(|_| "late synthetic frame was not forwarded by the broker".to_owned())?
                .ok_or_else(|| {
                    "activity channel closed before the late frame arrived".to_owned()
                })?;
            frames.push(activity_frame_line(late_started, &received, &state));
            let is_synthetic = received.session_id == "late-frame-synthetic";
            super::handle_activity_event(&connection, received, &mut state).await;
            if is_synthetic {
                break;
            }
        }
        let after_late_frame = thread_messages(guild, thread).await?;
        if after_late_frame != after_first_turn {
            return Err(format!(
                "late frame changed the thread: before {after_first_turn:?}, after {after_late_frame:?}"
            ));
        }

        drive_one_activity_turn(
            &tab.pane_id,
            tabs,
            &mut sub,
            &connection,
            &mut state,
            (&mut activity_rx, &mut frames),
            ACTIVITY_FORCE_PROMPT,
        )
        .await?;
        broker_task.abort();

        let after_second_turn = thread_messages(guild, thread).await?;
        assert_second_turn_activity(&after_second_turn, first_activity_id, &frames)
    }

    #[cfg(unix)]
    async fn run_codex_activity_hook_test() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let label = format!("{ACTIVITY_LABEL}-codex");
        assert_eq!(
            remaining_tabs(&label).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_codex_activity_environment().unwrap_or_else(|error| panic!("{error}"));

        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let broker_socket = Path::new(CODEX_ACTIVITY_BROKER_SOCKET);
        let created = codex_activity_tab_fixture(&label);
        let (tab_id, cwd_dir, result) = match created {
            Ok((tab, cwd_dir)) => {
                let agent_name = format!(
                    "activity-codex-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome = tokio::time::timeout(
                    Duration::from_secs(300),
                    codex_activity_hook_exercise(&guild, &tab, &agent_name, broker_socket),
                )
                .await
                .unwrap_or_else(|_| Err("codex activity hook exercise timed out".to_owned()));
                cleanup_real_codex_session_file(&home, &tab.pane_id);
                (Some(tab.tab_id), Some(cwd_dir), outcome)
            }
            Err(error) => (None, None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        if let Some(cwd_dir) = cwd_dir {
            let _ = clear_directory_contents(&cwd_dir);
        }

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left =
            remaining_tabs(&label).expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn codex_activity_hook_keeps_messages_inside_their_turn() {
        run_codex_activity_hook_test().await;
    }

    #[cfg(unix)]
    fn report_agent_session(pane_id: &str, session_id: &str) -> Result<(), String> {
        let args = [
            "pane",
            "report-agent-session",
            pane_id,
            "--source",
            "herdr:claude",
            "--agent",
            "claude",
            "--agent-session-id",
            session_id,
        ];
        let output = Command::new("herdr")
            .args(args)
            .output()
            .map_err(|error| format!("herdr {args:?} spawn failed: {error}"))?;
        if output.status.success() {
            Ok(())
        } else {
            Err(format!(
                "herdr {args:?} failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ))
        }
    }

    #[cfg(unix)]
    fn generate_claude_session_id() -> Result<String, String> {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_nanos();
        Ok(format!(
            "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
            std::process::id(),
            (nanos >> 48) & 0xffff,
            (nanos >> 36) & 0xfff,
            (nanos >> 24) & 0xfff,
            nanos & 0xffff_ffff_ffff,
        ))
    }

    #[cfg(unix)]
    fn report_idle_with_session(pane_id: &str) -> Result<(), String> {
        report_agent_state(pane_id, "idle")?;
        report_agent_session(pane_id, &generate_claude_session_id()?)
    }

    #[cfg(unix)]
    fn start_claude_haiku_agent(name: &str, pane_id: &str) -> Result<(), String> {
        herdr_json(&[
            "pane",
            "wait-output",
            pane_id,
            "--match",
            "╰─",
            "--timeout",
            "15000",
        ])?;
        let bound = Duration::from_secs(10);
        let start = Instant::now();
        loop {
            let args = [
                "agent",
                "start",
                name,
                "--kind",
                "claude",
                "--pane",
                pane_id,
                "--timeout",
                "60000",
                "--",
                "--model",
                "haiku",
            ];
            match herdr_json(&args) {
                Ok(_) => return Ok(()),
                Err(error) if is_agent_pane_busy(&error) && start.elapsed() < bound => {
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[cfg(unix)]
    fn is_agent_pane_busy(error: &str) -> bool {
        error.contains("\"code\":\"agent_pane_busy\"")
    }

    #[cfg(unix)]
    fn agent_name_nonce() -> Result<String, String> {
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis()
            .to_string())
    }

    #[cfg(unix)]
    fn prompt_claude_agent_and_wait(name: &str, text: &str) -> Result<(), String> {
        herdr_json(&[
            "agent",
            "prompt",
            name,
            text,
            "--wait",
            "--timeout",
            "60000",
        ])
        .map(|_| ())
    }

    #[cfg(unix)]
    fn cleanup_real_claude_session_dir(home: &Path, pane_id: &str) {
        let Ok(snapshot) = snapshot_for_pane(pane_id) else {
            return;
        };
        let Some(session) = snapshot.session.as_ref() else {
            return;
        };
        if let Ok(path) = resolve_session_path(home, &snapshot, session)
            && let Some(parent) = path.parent()
        {
            let _ = fs::remove_dir_all(parent);
        }
    }

    #[cfg(unix)]
    const STARTUP_TOPOLOGY_LABEL: &str = "testrun-startup-topology";

    #[cfg(unix)]
    async fn startup_topology_sync_exercise(
        guild: &BlockedCaptureGuild,
        workspace: &Workspace,
    ) -> Result<(), String> {
        report_idle_with_session(&workspace.pane_id)?;
        let listed = snapshot_for_pane(&workspace.pane_id)?;
        let matching = matching_tab(&workspace.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&listed);

        let route = route_topology(agents, tabs, &listed.terminal_id)?;
        let topic = format!("herdr workspace [{}]", route.workspace_id);
        if !channel_with_topic_is_absent(guild, &topic).await? {
            return Err("the workspace channel existed before the startup sync".to_owned());
        }
        let connection = discord_tuple(guild);
        sync_startup_topology(connection.3.chat().as_ref(), agents, tabs).await;

        let channel = guild
            .client
            .guild_channels(guild.id)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|channel| channel.topic.as_deref() == Some(topic.as_str()))
            .ok_or_else(|| "startup sync did not create the workspace channel".to_owned())?;

        let thread_suffix = format!(" [{}]", route.tab_id);
        let threads = guild
            .client
            .active_threads(guild.id)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .threads;
        let has_thread = threads.iter().any(|thread| {
            thread.parent_id == Some(channel.id)
                && thread
                    .name
                    .as_deref()
                    .is_some_and(|name| name.ends_with(&thread_suffix))
        });
        if has_thread {
            Ok(())
        } else {
            Err("startup sync did not create the tab thread".to_owned())
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn startup_topology_sync_creates_channel_and_thread() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(STARTUP_TOPOLOGY_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(STARTUP_TOPOLOGY_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-cwd-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create startup-topology test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_workspace(STARTUP_TOPOLOGY_LABEL, cwd);
        let (workspace_id, result) = match created {
            Ok(workspace) => {
                let outcome = startup_topology_sync_exercise(&guild, &workspace).await;
                (Some(workspace.id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(STARTUP_TOPOLOGY_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(STARTUP_TOPOLOGY_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const FRESH_IDLE_SESSION_LABEL: &str = "testrun-fresh-idle-session";

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn fresh_idle_session_discovery_is_mirrored_on_first_snapshot() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );

        let cases = [
            (
                "claude",
                "testrun-fresh-idle-claude",
                "testrun-fresh-idle-claude:t1",
            ),
            (
                "codex",
                "testrun-fresh-idle-codex",
                "testrun-fresh-idle-codex:t1",
            ),
        ];
        let result = async {
            for (agent, workspace_id, tab_id) in cases {
                let terminal_id = format!("{workspace_id}:terminal");
                let pane_id = format!("{workspace_id}:pane");
                let snapshot = AgentSnapshot {
                    agent: Some(agent.to_owned()),
                    terminal_id: terminal_id.clone(),
                    agent_status: STATUS_IDLE.to_owned(),
                    tab_id: tab_id.to_owned(),
                    workspace_id: workspace_id.to_owned(),
                    pane_id,
                    cwd: Some(format!("/tmp/{FRESH_IDLE_SESSION_LABEL}")),
                    session: Some(AgentSession {
                        agent: agent.to_owned(),
                        value: format!("{agent}-fresh-session"),
                    }),
                };
                let tabs = [herdr_connect_rs::HerdrTab {
                    tab_id: tab_id.to_owned(),
                    workspace_id: workspace_id.to_owned(),
                    label: "fresh".to_owned(),
                }];
                let agents = [snapshot.clone()];
                let connection = discord_tuple(&guild);
                let mut state = BridgeState::default();

                process_snapshot(&snapshot, &agents, &tabs, &connection, &mut state).await;

                let route = route_topology(&agents, &tabs, &terminal_id)?;
                let topic = format!("herdr workspace [{}]", route.workspace_id);
                let channel = guild_channel_with_topic(&guild, &topic).await?;
                let suffix = format!(" [{}]", route.tab_id);
                let has_thread = active_threads_for_guild(&guild)
                    .await?
                    .iter()
                    .any(|thread| {
                        thread.parent_id == Some(channel.id)
                            && thread
                                .name
                                .as_deref()
                                .is_some_and(|name| name.ends_with(&suffix))
                    });
                if !has_thread {
                    return Err(format!(
                        "{agent} fresh idle session did not create its tab thread"
                    ));
                }
            }
            Ok::<(), String>(())
        }
        .await;

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const PREFETCH_REUSE_LABEL: &str = "testrun-startup-prefetch-reuse";

    #[cfg(unix)]
    async fn startup_topology_prefetch_reuse_exercise(
        guild: &BlockedCaptureGuild,
        workspace: &Workspace,
        tab_b: &Tab,
    ) -> Result<(), String> {
        report_idle_with_session(&workspace.pane_id)?;
        report_idle_with_session(&tab_b.pane_id)?;
        let agent_a = snapshot_for_pane(&workspace.pane_id)?;
        let agent_b = snapshot_for_pane(&tab_b.pane_id)?;
        let tabs = [
            matching_tab(&workspace.tab_id)?,
            matching_tab(&tab_b.tab_id)?,
        ];
        let agents = [agent_a.clone(), agent_b.clone()];

        let route_a = route_topology(&agents, &tabs, &agent_a.terminal_id)?;
        let route_b = route_topology(&agents, &tabs, &agent_b.terminal_id)?;
        let topic = format!("herdr workspace [{}]", route_a.workspace_id);
        if !channel_with_topic_is_absent(guild, &topic).await? {
            return Err("the workspace channel existed before the startup sync".to_owned());
        }
        let connection = discord_tuple(guild);
        sync_startup_topology(connection.3.chat().as_ref(), &agents, &tabs).await;
        let matching_channels: Vec<_> = guild
            .client
            .guild_channels(guild.id)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|channel| channel.topic.as_deref() == Some(topic.as_str()))
            .collect();
        let [channel] = matching_channels.as_slice() else {
            return Err(format!(
                "expected exactly one prefetched workspace channel, found {}",
                matching_channels.len()
            ));
        };

        let threads = guild
            .client
            .active_threads(guild.id)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .threads;
        let suffix_a = format!(" [{}]", route_a.tab_id);
        let suffix_b = format!(" [{}]", route_b.tab_id);
        let thread_a = threads
            .iter()
            .find(|thread| {
                thread
                    .name
                    .as_deref()
                    .is_some_and(|name| name.ends_with(&suffix_a))
            })
            .ok_or_else(|| "startup sync did not create tab a's thread".to_owned())?;
        let thread_b = threads
            .iter()
            .find(|thread| {
                thread
                    .name
                    .as_deref()
                    .is_some_and(|name| name.ends_with(&suffix_b))
            })
            .ok_or_else(|| "startup sync did not create tab b's thread".to_owned())?;
        if thread_a.parent_id != Some(channel.id) || thread_b.parent_id != Some(channel.id) {
            return Err(
                "both tabs' threads must share the one prefetched workspace channel".to_owned(),
            );
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn startup_topology_prefetch_reuses_one_channel_across_two_tabs() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(PREFETCH_REUSE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(PREFETCH_REUSE_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-cwd-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create prefetch-reuse test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = match create_workspace(PREFETCH_REUSE_LABEL, cwd) {
            Ok(workspace) => match create_tab(PREFETCH_REUSE_LABEL, &workspace.id, cwd) {
                Ok(tab_b) => Ok((workspace, tab_b)),
                Err(error) => Err((Some(workspace.id), error)),
            },
            Err(error) => Err((None, error)),
        };
        let (workspace_id, result) = match created {
            Ok((workspace, tab_b)) => {
                let workspace_id = workspace.id.clone();
                let outcome =
                    startup_topology_prefetch_reuse_exercise(&guild, &workspace, &tab_b).await;
                (Some(workspace_id), outcome)
            }
            Err((workspace_id, error)) => (workspace_id, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(PREFETCH_REUSE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(PREFETCH_REUSE_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    fn create_unlabeled_tab(workspace_id: &str, cwd: &str) -> Result<Tab, String> {
        let created = herdr_json(&[
            "tab",
            "create",
            "--workspace",
            workspace_id,
            "--cwd",
            cwd,
            "--no-focus",
        ])?;
        let tab_id = created["result"]["tab"]["tab_id"]
            .as_str()
            .ok_or("herdr tab create result missing tab_id")?
            .to_owned();
        let pane_id = created["result"]["root_pane"]["pane_id"]
            .as_str()
            .ok_or("herdr tab create result missing pane_id")?
            .to_owned();
        Ok(Tab { tab_id, pane_id })
    }

    #[cfg(unix)]
    async fn unlabeled_tab_gets_a_generated_name_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
    ) -> Result<(), String> {
        report_idle_with_session(&tab.pane_id)?;
        let snapshot = snapshot_for_pane(&tab.pane_id)?;
        let listed = matching_tab(&tab.tab_id)?;
        if !herdr_connect_rs::is_numeric_label(&listed.label) {
            return Err(format!(
                "expected a numeric auto-assigned label for an unlabeled tab, herdr reported label {:?}",
                listed.label
            ));
        }
        let expected_name = herdr_connect_rs::generated_tab_name(&tab.tab_id);
        let agents = [snapshot.clone()];
        let mut tabs = vec![listed];

        let errors = name_unlabeled_tabs(&agents, &mut tabs, &mut HashSet::new());
        if !errors.is_empty() {
            return Err(format!("renaming the unlabeled tab failed: {errors:?}"));
        }
        let renamed = matching_tab(&tab.tab_id)?;
        if renamed.label != expected_name {
            return Err(format!(
                "expected herdr label {expected_name:?}, got {:?}",
                renamed.label
            ));
        }

        let connection = discord_tuple(guild);
        let mut state = BridgeState::default();
        process_snapshot(&snapshot, &agents, &tabs, &connection, &mut state).await;
        let topic = format!("herdr workspace [{}]", renamed.workspace_id);
        let channel = guild_channel_with_topic(guild, &topic).await?;
        let expected_thread = format!("{expected_name} [{}]", tab.tab_id);
        let threads = active_threads_for_guild(guild).await?;
        let thread_names: Vec<_> = threads
            .iter()
            .filter(|thread| thread.parent_id == Some(channel.id))
            .filter_map(|thread| thread.name.as_deref())
            .filter(|name| name.ends_with(&format!(" [{}]", tab.tab_id)))
            .collect();
        if thread_names != [expected_thread.as_str()] {
            return Err(format!(
                "expected exactly the thread {expected_thread:?}, found {thread_names:?}"
            ));
        }

        Ok(())
    }

    #[test]
    fn rename_errors_are_pruned_with_their_closed_tab() {
        let mut state = BridgeState::default();
        state.rename_errors_reported.insert("w-1:3".to_owned());
        state.rename_errors_reported.insert("w-1:4".to_owned());
        let current_tabs = HashSet::from(["w-1:4".to_owned()]);
        let none = HashSet::new();
        super::prune_departed_state(&mut state, &none, &none, &current_tabs);
        assert_eq!(
            state.rename_errors_reported,
            HashSet::from(["w-1:4".to_owned()])
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn unlabeled_tab_is_renamed_and_its_thread_takes_the_generated_name() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );

        let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
            .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-unlabeled-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create unlabeled-tab test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_unlabeled_tab(&workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let outcome = unlabeled_tab_gets_a_generated_name_exercise(&guild, &tab).await;
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        let topic = format!("herdr workspace [{workspace_id}]");
        let thread_cleanup = match &tab_id {
            Some(tab_id) => delete_tab_threads(&guild, &topic, tab_id).await,
            None => Ok(()),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert!(thread_cleanup.is_ok(), "{thread_cleanup:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        if let Some(tab_id) = &tab_id {
            if let Some(channel) = guild_channels_for_guild(&guild)
                .await
                .unwrap()
                .into_iter()
                .find(|channel| channel.topic.as_deref() == Some(topic.as_str()))
            {
                assert!(
                    !thread_with_suffix_survives(&guild, channel.id, &format!(" [{tab_id}]"))
                        .await
                        .unwrap(),
                    "named zero-leftover check"
                );
            }
            let tabs = tab_list_result().expect("tab.list succeeds for the zero-leftover check");
            assert!(
                !tabs.iter().any(|listed| &listed.tab_id == tab_id),
                "named zero-leftover check"
            );
        }
    }

    #[cfg(unix)]
    const STARTUP_RACE_LABEL: &str = "testrun-startup-race";

    #[cfg(unix)]
    async fn startup_and_delivery_race_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
    ) -> Result<(), String> {
        report_idle_with_session(&tab.pane_id)?;
        let listed = snapshot_for_pane(&tab.pane_id)?;
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&listed);
        let route = route_topology(agents, tabs, &listed.terminal_id)?;

        let shared_cache: herdr_connect_rs::TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let connection = discord_tuple_with_cache(guild, Arc::clone(&shared_cache));

        let startup = sync_startup_topology(connection.3.chat().as_ref(), agents, tabs);
        let delivery = sync_route(guild.client.as_ref(), guild.id, &route, &shared_cache);
        let ((), delivery_result) = tokio::join!(startup, delivery);
        delivery_result?;

        let threads = guild
            .client
            .active_threads(guild.id)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .threads;
        let suffix = format!(" [{}]", route.tab_id);
        let matching_threads: Vec<_> = threads
            .iter()
            .filter(|thread| {
                thread
                    .name
                    .as_deref()
                    .is_some_and(|name| name.ends_with(&suffix))
            })
            .collect();
        if matching_threads.len() == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected exactly one thread for tab {}, found {}",
                route.tab_id,
                matching_threads.len()
            ))
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn startup_and_delivery_sync_race_creates_one_thread() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(STARTUP_RACE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
            .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-cwd-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create startup-race test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_tab(STARTUP_RACE_LABEL, &workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let outcome = startup_and_delivery_race_exercise(&guild, &tab).await;
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(STARTUP_RACE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const STALE_CACHE_LABEL: &str = "testrun-stale-cache";

    #[cfg(unix)]
    async fn startup_sweep_stale_cache_exercise(
        guild: &BlockedCaptureGuild,
        workspace: &Workspace,
    ) -> Result<(), String> {
        report_idle_with_session(&workspace.pane_id)?;
        let listed = snapshot_for_pane(&workspace.pane_id)?;
        let matching = matching_tab(&workspace.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&listed);
        let route = route_topology(agents, tabs, &listed.terminal_id)?;
        let topic = format!("herdr workspace [{}]", route.workspace_id);
        if !channel_with_topic_is_absent(guild, &topic).await? {
            return Err("the workspace channel existed before the stale fetch".to_owned());
        }

        let stale = fetch_topology_lists(guild.client.as_ref(), guild.id).await?;
        sync_route(
            guild.client.as_ref(),
            guild.id,
            &route,
            &Arc::new(tokio::sync::Mutex::new(None)),
        )
        .await?;
        let shared: herdr_connect_rs::TopologyCache =
            Arc::new(tokio::sync::Mutex::new(Some(stale)));
        sync_startup_topology(
            discord_tuple_with_cache(guild, Arc::clone(&shared))
                .3
                .chat()
                .as_ref(),
            agents,
            tabs,
        )
        .await;

        let matching_channels = guild
            .client
            .guild_channels(guild.id)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|channel| channel.topic.as_deref() == Some(topic.as_str()))
            .count();
        let suffix = format!(" [{}]", route.tab_id);
        let matching_threads = guild
            .client
            .active_threads(guild.id)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .threads
            .into_iter()
            .filter(|thread| {
                thread
                    .name
                    .as_deref()
                    .is_some_and(|name| name.ends_with(&suffix))
            })
            .count();
        if matching_channels == 1 && matching_threads == 1 {
            Ok(())
        } else {
            Err(format!(
                "expected exactly one channel and one thread for workspace {} tab {}, \
                 found {matching_channels} channels and {matching_threads} threads",
                route.workspace_id, route.tab_id
            ))
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn startup_sweep_with_a_stale_cache_creates_one_channel_and_one_thread() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(STALE_CACHE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(STALE_CACHE_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-cwd-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create stale-cache test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_workspace(STALE_CACHE_LABEL, cwd);
        let (workspace_id, result) = match created {
            Ok(workspace) => {
                let outcome = startup_sweep_stale_cache_exercise(&guild, &workspace).await;
                (Some(workspace.id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(STALE_CACHE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(STALE_CACHE_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    async fn guild_channels_for_guild(
        guild: &BlockedCaptureGuild,
    ) -> Result<Vec<twilight_model::channel::Channel>, String> {
        guild
            .client
            .guild_channels(guild.id)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())
    }

    #[cfg(unix)]
    async fn guild_channel_with_topic(
        guild: &BlockedCaptureGuild,
        topic: &str,
    ) -> Result<twilight_model::channel::Channel, String> {
        guild_channels_for_guild(guild)
            .await?
            .into_iter()
            .find(|channel| channel.topic.as_deref() == Some(topic))
            .ok_or_else(|| format!("no channel found with topic {topic}"))
    }

    #[cfg(unix)]
    async fn channel_with_topic_is_absent(
        guild: &BlockedCaptureGuild,
        topic: &str,
    ) -> Result<bool, String> {
        Ok(!guild_channels_for_guild(guild)
            .await?
            .iter()
            .any(|channel| channel.topic.as_deref() == Some(topic)))
    }

    #[cfg(unix)]
    async fn create_guild_thread(
        guild: &BlockedCaptureGuild,
        channel_id: Id<ChannelMarker>,
        name: &str,
    ) -> Result<twilight_model::channel::Channel, String> {
        guild
            .client
            .create_thread(
                channel_id,
                name,
                twilight_model::channel::ChannelType::PublicThread,
            )
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())
    }

    #[cfg(unix)]
    async fn active_threads_for_guild(
        guild: &BlockedCaptureGuild,
    ) -> Result<Vec<twilight_model::channel::Channel>, String> {
        Ok(guild
            .client
            .active_threads(guild.id)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?
            .threads)
    }

    #[cfg(unix)]
    async fn thread_with_suffix_survives(
        guild: &BlockedCaptureGuild,
        channel_id: Id<ChannelMarker>,
        suffix: &str,
    ) -> Result<bool, String> {
        let has_suffix = |thread: &twilight_model::channel::Channel| {
            thread
                .name
                .as_deref()
                .is_some_and(|name| name.ends_with(suffix))
        };
        if active_threads_for_guild(guild)
            .await?
            .iter()
            .any(|thread| thread.parent_id == Some(channel_id) && has_suffix(thread))
        {
            return Ok(true);
        }
        let archived =
            herdr_connect_rs::archived_threads(guild.client.as_ref(), channel_id).await?;
        Ok(archived.iter().any(has_suffix))
    }

    #[cfg(unix)]
    async fn active_tab_thread(
        guild: &BlockedCaptureGuild,
        channel_id: Id<ChannelMarker>,
        suffix: &str,
    ) -> Result<twilight_model::channel::Channel, String> {
        active_threads_for_guild(guild)
            .await?
            .into_iter()
            .find(|thread| {
                thread.parent_id == Some(channel_id)
                    && thread
                        .name
                        .as_deref()
                        .is_some_and(|name| name.ends_with(suffix))
            })
            .ok_or_else(|| "sync did not create the second tab's thread".to_owned())
    }

    #[cfg(unix)]
    async fn expect_thread_created_message(
        guild: &BlockedCaptureGuild,
        parent: Id<ChannelMarker>,
        thread_id: Id<ChannelMarker>,
        present: bool,
    ) -> Result<(), String> {
        let found = thread_created_messages(guild, parent, thread_id).await?;
        let ids_match = found
            .iter()
            .all(|message| message.id.get() == thread_id.get());
        if found.is_empty() != present && ids_match {
            return Ok(());
        }
        let found = found
            .iter()
            .map(|message| (message.id, message.reference.clone()))
            .collect::<Vec<_>>();
        Err(format!(
            "started-a-thread message expected {}: thread id {thread_id}; found {found:?}",
            if present { "present" } else { "deleted" },
        ))
    }

    #[cfg(unix)]
    async fn thread_created_messages(
        guild: &BlockedCaptureGuild,
        parent: Id<ChannelMarker>,
        thread_id: Id<ChannelMarker>,
    ) -> Result<Vec<twilight_model::channel::Message>, String> {
        Ok(guild
            .client
            .channel_messages(parent)
            .limit(100)
            .await
            .map_err(|error| error.to_string())?
            .models()
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .filter(|message| {
                message.kind == twilight_model::channel::message::MessageType::ThreadCreated
                    && message
                        .reference
                        .as_ref()
                        .is_some_and(|reference| reference.channel_id == Some(thread_id))
            })
            .collect())
    }

    #[cfg(unix)]
    async fn delete_tab_threads(
        guild: &BlockedCaptureGuild,
        topic: &str,
        tab_id: &str,
    ) -> Result<(), String> {
        let Some(channel) = guild_channels_for_guild(guild)
            .await?
            .into_iter()
            .find(|channel| channel.topic.as_deref() == Some(topic))
        else {
            return Ok(());
        };
        let suffix = format!(" [{tab_id}]");
        let mut threads = active_threads_for_guild(guild)
            .await?
            .into_iter()
            .filter(|thread| thread.parent_id == Some(channel.id))
            .collect::<Vec<_>>();
        threads
            .extend(herdr_connect_rs::archived_threads(guild.client.as_ref(), channel.id).await?);
        for thread in threads
            .iter()
            .filter(|thread| thread.name.as_deref().is_some_and(|n| n.ends_with(&suffix)))
        {
            blocked_capture_delete_thread(guild, thread.id).await?;
        }
        Ok(())
    }

    #[cfg(unix)]
    const CLOSED_TOPOLOGY_LABEL: &str = "testrun-closed-topology";

    #[cfg(unix)]
    enum ClosureBatchPresence {
        Active,
        Archived,
        Absent,
    }

    #[cfg(unix)]
    struct ClosureBatchCase {
        name: &'static str,
        presence: ClosureBatchPresence,
        closed: bool,
        expect_survives: Option<bool>,
    }

    #[cfg(unix)]
    async fn closed_topology_exercise(guild: &BlockedCaptureGuild) -> Result<(), String> {
        let nonce = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        );
        let workspace_id = format!("{CLOSED_TOPOLOGY_LABEL}-{nonce}");
        let channel = guild
            .client
            .create_guild_channel(guild.id, &format!("{CLOSED_TOPOLOGY_LABEL}-{nonce}"))
            .topic(&format!("herdr workspace [{workspace_id}]"))
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;

        let cases = [
            ClosureBatchCase {
                name: "an active thread whose closure is in the batch is deleted",
                presence: ClosureBatchPresence::Active,
                closed: true,
                expect_survives: Some(false),
            },
            ClosureBatchCase {
                name: "an archived thread whose closure is in the batch is deleted",
                presence: ClosureBatchPresence::Archived,
                closed: true,
                expect_survives: Some(false),
            },
            ClosureBatchCase {
                name: "a closure with no matching thread is a harmless no-op",
                presence: ClosureBatchPresence::Absent,
                closed: true,
                expect_survives: None,
            },
            ClosureBatchCase {
                name: "a live tab outside the batch survives",
                presence: ClosureBatchPresence::Active,
                closed: false,
                expect_survives: Some(true),
            },
        ];

        let mut closures = Vec::new();
        let mut suffixes = Vec::new();
        for (index, case) in cases.iter().enumerate() {
            let tab_id = format!("{workspace_id}:t{index}");
            let suffix = format!(" [{tab_id}]");
            match case.presence {
                ClosureBatchPresence::Active => {
                    create_guild_thread(guild, channel.id, &format!("tab-{index}{suffix}")).await?;
                }
                ClosureBatchPresence::Archived => {
                    let thread =
                        create_guild_thread(guild, channel.id, &format!("tab-{index}{suffix}"))
                            .await?;
                    guild
                        .client
                        .update_thread(thread.id)
                        .archived(true)
                        .await
                        .map_err(|error| error.to_string())?;
                }
                ClosureBatchPresence::Absent => {}
            }
            if case.closed {
                closures.push(TopologyClosure::Tab {
                    workspace_id: workspace_id.clone(),
                    tab_id,
                });
            }
            suffixes.push(suffix);
        }

        let connection = discord_tuple(guild);
        for closure in &closures {
            connection.3.chat().close_topology(closure).await?;
        }

        for (case, suffix) in cases.iter().zip(suffixes.iter()) {
            let Some(expected) = case.expect_survives else {
                continue;
            };
            let survives = thread_with_suffix_survives(guild, channel.id, suffix).await?;
            if survives != expected {
                return Err(format!(
                    "{}: expected survives={expected}, got {survives}",
                    case.name
                ));
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn closed_topology_deletes_matching_threads_and_spares_live_ones() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );

        let result = closed_topology_exercise(&guild).await;

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const LIVE_CLOSE_LABEL: &str = "testrun-live-close";

    #[cfg(unix)]
    async fn live_close_exercise(
        guild: &BlockedCaptureGuild,
        workspace: &Workspace,
        second_tab: &Tab,
    ) -> Result<(), String> {
        report_idle_with_session(&workspace.pane_id)?;
        report_idle_with_session(&second_tab.pane_id)?;
        let root_agent = snapshot_for_pane(&workspace.pane_id)?;
        let second_agent = snapshot_for_pane(&second_tab.pane_id)?;
        let root_tab = matching_tab(&workspace.tab_id)?;
        let second_matching_tab = matching_tab(&second_tab.tab_id)?;
        let tabs = [root_tab, second_matching_tab];
        let agents = [root_agent.clone(), second_agent.clone()];

        let connection = discord_tuple(guild);
        sync_startup_topology(connection.3.chat().as_ref(), &agents, &tabs).await;
        let root_route = route_topology(&agents, &tabs, &root_agent.terminal_id)?;
        let second_route = route_topology(&agents, &tabs, &second_agent.terminal_id)?;

        let topic = format!("herdr workspace [{}]", root_route.workspace_id);
        let channel = guild_channel_with_topic(guild, &topic).await?;
        let root_suffix = format!(" [{}]", root_route.tab_id);
        let second_suffix = format!(" [{}]", second_route.tab_id);
        let second_thread_id = active_tab_thread(guild, channel.id, &second_suffix)
            .await?
            .id;

        let lifecycle = subscribe_herdr_events(&lifecycle_subscriptions()).await?;
        let mut stop = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .map_err(|error| error.to_string())?;
        let mut broker: Option<BrokerTask> = None;
        let (_live_tx, live_events) = tokio::sync::mpsc::unbounded_channel();
        let (_activity_tx, activity_events) = tokio::sync::mpsc::unbounded_channel();
        let mut runtime = BridgeRuntime {
            lifecycle,
            pane_ids: Vec::new(),
            status: None,
            state: BridgeState::default(),
            live_events,
            activity_events,
        };

        doorbell_snapshot(
            &connection,
            &mut runtime.state,
            &mut runtime.pane_ids,
            &mut runtime.status,
            &mut stop,
        )
        .await
        .map_err(|_| "snapshot doorbell was interrupted".to_owned())?;
        expect_thread_created_message(guild, channel.id, second_thread_id, true).await?;
        close_tab(&second_tab.tab_id);
        let tab_closed = wait_for_event(
            &mut runtime.lifecycle,
            "tab_closed",
            &second_tab.tab_id,
            "/data/tab_id",
            None,
            Duration::from_secs(15),
        )
        .await?;
        handle_lifecycle_select_result(
            Ok(tab_closed),
            &connection,
            &mut stop,
            &mut broker,
            &mut runtime,
        )
        .await;

        if thread_with_suffix_survives(guild, channel.id, &second_suffix).await? {
            return Err("tab close did not delete the tab's thread".to_owned());
        }
        expect_thread_created_message(guild, channel.id, second_thread_id, false).await?;
        if !thread_with_suffix_survives(guild, channel.id, &root_suffix).await? {
            return Err("tab close deleted the root tab's thread".to_owned());
        }

        close_workspace(&workspace.id);
        let workspace_closed = wait_for_event(
            &mut runtime.lifecycle,
            "workspace_closed",
            &workspace.id,
            "/data/workspace_id",
            None,
            Duration::from_secs(15),
        )
        .await?;
        handle_lifecycle_select_result(
            Ok(workspace_closed),
            &connection,
            &mut stop,
            &mut broker,
            &mut runtime,
        )
        .await;

        if guild_channel_with_topic(guild, &topic).await.is_ok() {
            return Err("workspace close did not delete the workspace channel".to_owned());
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn live_tab_close_deletes_thread_then_workspace_close_deletes_channel() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(LIVE_CLOSE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(LIVE_CLOSE_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-live-close-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create live-close test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = match create_workspace(LIVE_CLOSE_LABEL, cwd) {
            Ok(workspace) => match create_tab(LIVE_CLOSE_LABEL, &workspace.id, cwd) {
                Ok(second_tab) => Ok((workspace, second_tab)),
                Err(error) => Err((Some(workspace.id), error)),
            },
            Err(error) => Err((None, error)),
        };
        let (workspace_id, result) = match created {
            Ok((workspace, second_tab)) => {
                let workspace_id = workspace.id.clone();
                let outcome = live_close_exercise(&guild, &workspace, &second_tab).await;
                (Some(workspace_id), outcome)
            }
            Err((workspace_id, error)) => (workspace_id, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(LIVE_CLOSE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(LIVE_CLOSE_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const OWNER_DELETE_LABEL: &str = "testrun-owner-delete";

    #[cfg(unix)]
    const GATEWAY_READY_WAIT: Duration = Duration::from_secs(8);

    #[cfg(unix)]
    async fn start_owner_deletion_gateway(
        connection: &super::DiscordConnection,
    ) -> Result<
        (
            tokio::task::JoinHandle<Result<(), String>>,
            std::sync::mpsc::Receiver<String>,
        ),
        String,
    > {
        let (notices_tx, notices_rx) = std::sync::mpsc::channel();
        let task = tokio::spawn(herdr_connect_rs::drive_gateway_with_components(
            std::env::var("DISCORD_TOKEN").map_err(|error| error.to_string())?,
            herdr_connect_rs::GatewayContext {
                client: Arc::clone(&connection.0),
                guild: connection.1,
                owner_id: connection.2.clone(),
                responder: Arc::clone(&connection.3),
            },
            notices_tx,
            super::component_handler(Arc::clone(&connection.3)),
        ));
        tokio::time::sleep(GATEWAY_READY_WAIT).await;
        Ok((task, notices_rx))
    }

    #[cfg(unix)]
    fn deletion_errors(notices: &std::sync::mpsc::Receiver<String>) -> Vec<String> {
        notices
            .try_iter()
            .filter(|notice| notice.contains("error"))
            .collect()
    }

    #[cfg(unix)]
    async fn deletion_notices(
        notices: std::sync::mpsc::Receiver<String>,
        dispatch: &str,
    ) -> Result<(Vec<String>, Vec<String>), String> {
        let mut seen = Vec::new();
        let deadline = tokio::time::Instant::now() + DELETION_DISPATCH_WAIT;
        while !seen.iter().any(|notice| notice == dispatch) {
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("no `{dispatch}` notice arrived; saw {seen:?}"));
            }
            seen.extend(notices.try_iter());
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        tokio::time::sleep(DELETION_ERROR_WINDOW).await;
        seen.extend(notices.try_iter());
        let dispatched = seen
            .iter()
            .filter(|notice| notice.starts_with("discord gateway deletion:"))
            .cloned()
            .collect();
        let errors = seen
            .into_iter()
            .filter(|notice| notice.contains("error"))
            .collect();
        Ok((dispatched, errors))
    }

    #[cfg(unix)]
    async fn owner_thread_delete_exercise(
        guild: &BlockedCaptureGuild,
        workspace: &Workspace,
        second_tab: &Tab,
    ) -> Result<(), String> {
        report_idle_with_session(&workspace.pane_id)?;
        report_idle_with_session(&second_tab.pane_id)?;
        let root_agent = snapshot_for_pane(&workspace.pane_id)?;
        let second_agent = snapshot_for_pane(&second_tab.pane_id)?;
        let tabs = [
            matching_tab(&workspace.tab_id)?,
            matching_tab(&second_tab.tab_id)?,
        ];
        let agents = [root_agent.clone(), second_agent.clone()];

        let connection = discord_tuple(guild);
        sync_startup_topology(connection.3.chat().as_ref(), &agents, &tabs).await;
        let second_route = route_topology(&agents, &tabs, &second_agent.terminal_id)?;
        let root_route = route_topology(&agents, &tabs, &root_agent.terminal_id)?;
        let topic = format!("herdr workspace [{}]", second_route.workspace_id);
        let channel = guild_channel_with_topic(guild, &topic).await?;
        let second_suffix = format!(" [{}]", second_route.tab_id);
        let root_suffix = format!(" [{}]", root_route.tab_id);
        let thread = active_tab_thread(guild, channel.id, &second_suffix).await?;
        expect_thread_created_message(guild, channel.id, thread.id, true).await?;

        let mut lifecycle = subscribe_herdr_events(&lifecycle_subscriptions()).await?;
        let (gateway, notices) = start_owner_deletion_gateway(&connection).await?;
        *connection.3.chat().topology_cache().lock().await = None;
        let deleted = guild
            .client
            .delete_channel(thread.id)
            .await
            .map_err(|error| error.to_string());
        let closed = if deleted.is_ok() {
            wait_for_event(
                &mut lifecycle,
                "tab_closed",
                &second_tab.tab_id,
                "/data/tab_id",
                None,
                Duration::from_secs(20),
            )
            .await
            .map(|_| ())
        } else {
            Ok(())
        };
        gateway.abort();
        deleted?;
        closed.map_err(|error| format!("owner thread delete did not close the tab: {error}"))?;
        matching_tab(&workspace.tab_id)
            .map_err(|error| format!("the sibling tab did not survive: {error}"))?;
        let errors = deletion_errors(&notices);
        if !errors.is_empty() {
            return Err(format!("gateway reported errors: {errors:?}"));
        }
        expect_thread_created_message(guild, channel.id, thread.id, false).await?;

        if !thread_with_suffix_survives(guild, channel.id, &root_suffix).await? {
            return Err("the sibling tab's thread did not survive".to_owned());
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn owner_thread_delete_closes_the_herdr_tab() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(OWNER_DELETE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(OWNER_DELETE_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-owner-delete-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create owner-delete test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = match create_workspace(OWNER_DELETE_LABEL, cwd) {
            Ok(workspace) => match create_tab(OWNER_DELETE_LABEL, &workspace.id, cwd) {
                Ok(second_tab) => Ok((workspace, second_tab)),
                Err(error) => Err((Some(workspace.id), error)),
            },
            Err(error) => Err((None, error)),
        };
        let (workspace_id, result) = match created {
            Ok((workspace, second_tab)) => {
                let workspace_id = workspace.id.clone();
                let outcome = owner_thread_delete_exercise(&guild, &workspace, &second_tab).await;
                (Some(workspace_id), outcome)
            }
            Err((workspace_id, error)) => (workspace_id, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(OWNER_DELETE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(OWNER_DELETE_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    async fn owner_archived_thread_delete_exercise(
        guild: &BlockedCaptureGuild,
        workspace: &Workspace,
        second_tab: &Tab,
    ) -> Result<(), String> {
        let channel = guild
            .client
            .create_guild_channel(
                guild.id,
                &format!(
                    "testrun-owner-delete-archived-{}",
                    second_tab.tab_id.replace(':', "-")
                ),
            )
            .topic(&format!("herdr workspace [{}]", workspace.id))
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        let suffix = format!(" [{}]", second_tab.tab_id);
        let thread = create_guild_thread(guild, channel.id, &format!("archived{suffix}")).await?;
        guild
            .client
            .update_thread(thread.id)
            .archived(true)
            .await
            .map_err(|error| error.to_string())?;
        herdr_connect_rs::register_archived_tab_threads(guild.client.as_ref(), guild.id).await?;
        if herdr_connect_rs::resolve_owner_deleted_tab(thread.id).map(|(tab, _)| tab)
            != Some(second_tab.tab_id.clone())
        {
            return Err("the archived registration did not record the thread".to_owned());
        }

        let connection = discord_tuple(guild);
        let mut lifecycle = subscribe_herdr_events(&lifecycle_subscriptions()).await?;
        let (gateway, notices) = start_owner_deletion_gateway(&connection).await?;
        let deleted = guild
            .client
            .delete_channel(thread.id)
            .await
            .map_err(|error| error.to_string());
        let closed = if deleted.is_ok() {
            wait_for_event(
                &mut lifecycle,
                "tab_closed",
                &second_tab.tab_id,
                "/data/tab_id",
                None,
                Duration::from_secs(20),
            )
            .await
            .map(|_| ())
        } else {
            Ok(())
        };
        gateway.abort();
        deleted?;
        closed.map_err(|error| {
            format!("owner delete of an archived thread did not close the tab: {error}")
        })?;
        matching_tab(&workspace.tab_id)
            .map_err(|error| format!("the sibling tab did not survive: {error}"))?;
        let errors = deletion_errors(&notices);
        if !errors.is_empty() {
            return Err(format!("gateway reported errors: {errors:?}"));
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn owner_archived_thread_delete_closes_the_herdr_tab() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(OWNER_DELETE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(OWNER_DELETE_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-owner-delete-archived-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create owner-delete test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = match create_workspace(OWNER_DELETE_LABEL, cwd) {
            Ok(workspace) => match create_tab(OWNER_DELETE_LABEL, &workspace.id, cwd) {
                Ok(second_tab) => Ok((workspace, second_tab)),
                Err(error) => Err((Some(workspace.id), error)),
            },
            Err(error) => Err((None, error)),
        };
        let (workspace_id, result) = match created {
            Ok((workspace, second_tab)) => {
                let workspace_id = workspace.id.clone();
                let outcome =
                    owner_archived_thread_delete_exercise(&guild, &workspace, &second_tab).await;
                (Some(workspace_id), outcome)
            }
            Err((workspace_id, error)) => (workspace_id, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(OWNER_DELETE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(OWNER_DELETE_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    async fn owner_channel_delete_exercise(
        guild: &BlockedCaptureGuild,
        workspace: &Workspace,
    ) -> Result<(), String> {
        report_idle_with_session(&workspace.pane_id)?;
        let agent = snapshot_for_pane(&workspace.pane_id)?;
        let tabs = [matching_tab(&workspace.tab_id)?];
        let agents = [agent.clone()];

        let connection = discord_tuple(guild);
        sync_startup_topology(connection.3.chat().as_ref(), &agents, &tabs).await;
        let route = route_topology(&agents, &tabs, &agent.terminal_id)?;
        let topic = format!("herdr workspace [{}]", route.workspace_id);
        let channel = guild_channel_with_topic(guild, &topic).await?;

        let mut lifecycle = subscribe_herdr_events(&lifecycle_subscriptions()).await?;
        let (gateway, notices) = start_owner_deletion_gateway(&connection).await?;
        let deleted = guild
            .client
            .delete_channel(channel.id)
            .await
            .map_err(|error| error.to_string());
        let closed = if deleted.is_ok() {
            wait_for_event(
                &mut lifecycle,
                "workspace_closed",
                &workspace.id,
                "/data/workspace_id",
                None,
                Duration::from_secs(20),
            )
            .await
            .map(|_| ())
        } else {
            Ok(())
        };
        gateway.abort();
        deleted?;
        closed.map_err(|error| {
            format!("owner channel delete did not close the workspace: {error}")
        })?;
        let errors = deletion_errors(&notices);
        if !errors.is_empty() {
            return Err(format!("gateway reported errors: {errors:?}"));
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn owner_channel_delete_closes_the_herdr_workspace() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(OWNER_DELETE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(OWNER_DELETE_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-owner-delete-channel-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create owner-delete test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let (workspace_id, result) = match create_workspace(OWNER_DELETE_LABEL, cwd) {
            Ok(workspace) => {
                let outcome = owner_channel_delete_exercise(&guild, &workspace).await;
                (Some(workspace.id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(OWNER_DELETE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(OWNER_DELETE_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const SELF_DELETE_LABEL: &str = "testrun-self";

    #[cfg(unix)]
    const DELETION_DISPATCH_WAIT: Duration = Duration::from_secs(30);

    #[cfg(unix)]
    const DELETION_ERROR_WINDOW: Duration = Duration::from_secs(4);

    #[cfg(unix)]
    async fn bridge_deletion_gateway_errors(
        guild: &BlockedCaptureGuild,
        delete_thread: bool,
    ) -> Result<(Vec<String>, Vec<String>), String> {
        let nonce = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        );
        let workspace_id = format!("{SELF_DELETE_LABEL}-{nonce}");
        let tab_id = format!("{workspace_id}:t1");
        let channel = guild
            .client
            .create_guild_channel(guild.id, &workspace_id)
            .topic(&format!("herdr workspace [{workspace_id}]"))
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        create_guild_thread(guild, channel.id, &format!("bridge [{tab_id}]")).await?;
        let (mut channels, mut threads) =
            fetch_topology_lists(guild.client.as_ref(), guild.id).await?;

        let connection = discord_tuple(guild);
        let (gateway, notices) = start_owner_deletion_gateway(&connection).await?;
        let deleted = if delete_thread {
            herdr_connect_rs::delete_tab_thread(
                guild.client.as_ref(),
                &channels,
                &mut threads,
                &mut HashMap::new(),
                &workspace_id,
                &tab_id,
            )
            .await
        } else {
            herdr_connect_rs::delete_workspace_channel(
                guild.client.as_ref(),
                &mut channels,
                &workspace_id,
            )
            .await
        };
        let dispatch = if delete_thread {
            "discord gateway deletion: THREAD_DELETE"
        } else {
            "discord gateway deletion: CHANNEL_DELETE"
        };
        let seen = deletion_notices(notices, dispatch).await;
        gateway.abort();
        deleted?;
        seen
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn bridge_deletions_are_not_forwarded_to_herdr_as_owner_deletions() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let cases = [
            ("the bridge's tab thread deletion", true),
            ("the bridge's workspace channel deletion", false),
        ];
        let mut outcomes = Vec::new();
        for (name, delete_thread) in cases {
            outcomes.push((
                name,
                bridge_deletion_gateway_errors(&guild, delete_thread).await,
            ));
        }
        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        for ((name, delete_thread), (_, outcome)) in cases.into_iter().zip(outcomes) {
            let dispatch = if delete_thread {
                "discord gateway deletion: THREAD_DELETE"
            } else {
                "discord gateway deletion: CHANNEL_DELETE"
            };
            assert_eq!(
                outcome,
                Ok((vec![dispatch.to_owned()], Vec::new())),
                "{name}"
            );
        }
        assert_eq!(channels_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const STARTUP_RECONCILE_LABEL: &str = "testrun-startup-reconcile";

    #[cfg(unix)]
    async fn startup_reconciliation_exercise(
        guild: &BlockedCaptureGuild,
        workspace: &Workspace,
    ) -> Result<(), String> {
        report_idle_with_session(&workspace.pane_id)?;
        let listed = snapshot_for_pane(&workspace.pane_id)?;
        let matching = matching_tab(&workspace.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&listed);
        let route = route_topology(agents, tabs, &listed.terminal_id)?;

        let connection = discord_tuple(guild);
        sync_startup_topology(connection.3.chat().as_ref(), agents, tabs).await;

        let topic = format!("herdr workspace [{}]", route.workspace_id);
        let live_channel = guild_channel_with_topic(guild, &topic).await?;

        let nonce = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        );
        let orphan_channel = guild
            .client
            .create_guild_channel(guild.id, &format!("testrun-orphan-{nonce}"))
            .topic("herdr workspace [w9Z9]")
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        create_guild_thread(guild, orphan_channel.id, "testrun-orphan [w9Z9:t1]").await?;
        let live_orphan_thread = create_guild_thread(
            guild,
            live_channel.id,
            &format!("testrun-orphan [{}:t9Z]", route.workspace_id),
        )
        .await?;
        let bystander_thread =
            create_guild_thread(guild, live_channel.id, "testrun-notes [staging]").await?;

        sync_startup_topology(connection.3.chat().as_ref(), agents, tabs).await;

        let channels_after = guild_channels_for_guild(guild).await?;
        if channels_after
            .iter()
            .any(|channel| channel.id == orphan_channel.id)
        {
            return Err("startup sweep did not delete the orphan workspace channel".to_owned());
        }
        if !channels_after
            .iter()
            .any(|channel| channel.topic.as_deref() == Some(topic.as_str()))
        {
            return Err("startup sweep deleted the live workspace channel".to_owned());
        }

        let active_after = active_threads_for_guild(guild).await?;
        if active_after
            .iter()
            .any(|thread| thread.id == live_orphan_thread.id)
        {
            return Err(
                "startup sweep did not delete the orphaned thread under the live workspace channel"
                    .to_owned(),
            );
        }
        let suffix = format!(" [{}]", route.tab_id);
        if !active_after.iter().any(|thread| {
            thread.parent_id == Some(live_channel.id)
                && thread
                    .name
                    .as_deref()
                    .is_some_and(|name| name.ends_with(&suffix))
        }) {
            return Err("startup sweep deleted the live tab's thread".to_owned());
        }
        if !active_after
            .iter()
            .any(|thread| thread.id == bystander_thread.id)
        {
            return Err(
                "startup sweep deleted a non-tab thread that happened to end in brackets"
                    .to_owned(),
            );
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn startup_sweep_deletes_topology_absent_from_herdr() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(STARTUP_RECONCILE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(STARTUP_RECONCILE_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-startup-reconcile-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create startup-reconcile test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_workspace(STARTUP_RECONCILE_LABEL, cwd);
        let (workspace_id, result) = match created {
            Ok(workspace) => {
                let workspace_id = workspace.id.clone();
                let outcome = startup_reconciliation_exercise(&guild, &workspace).await;
                (Some(workspace_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(STARTUP_RECONCILE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(STARTUP_RECONCILE_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    struct ConcurrentClearCase {
        name: &'static str,
        race_the_clear: bool,
    }

    #[cfg(unix)]
    async fn startup_sweep_survives_concurrent_clear_exercise(
        guild: &BlockedCaptureGuild,
        race_the_clear: bool,
    ) -> Result<(), String> {
        use std::sync::atomic::{AtomicBool, Ordering};

        let nonce = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        );
        let orphan_channel = guild
            .client
            .create_guild_channel(guild.id, &format!("testrun-orphan-{nonce}"))
            .topic("herdr workspace [wCC9]")
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        create_guild_thread(guild, orphan_channel.id, "testrun-orphan [wCC9:tCC]").await?;

        let shared_cache: herdr_connect_rs::TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let connection = discord_tuple_with_cache(guild, Arc::clone(&shared_cache));

        let racer = if race_the_clear {
            let done = Arc::new(AtomicBool::new(false));
            let handle = tokio::spawn({
                let cache = Arc::clone(&shared_cache);
                let done = Arc::clone(&done);
                async move {
                    while !done.load(Ordering::Relaxed) {
                        *cache.lock().await = None;
                    }
                }
            });
            Some((handle, done))
        } else {
            None
        };

        sync_startup_topology(connection.3.chat().as_ref(), &[], &[]).await;

        if let Some((handle, done)) = racer {
            done.store(true, Ordering::Relaxed);
            handle.await.map_err(|error| error.to_string())?;
        }

        let channels_after = guild_channels_for_guild(guild).await?;
        if channels_after
            .iter()
            .any(|channel| channel.id == orphan_channel.id)
        {
            return Err("startup sweep did not delete the orphan workspace channel".to_owned());
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn startup_sweep_survives_a_concurrent_cache_clear() {
        let cases = [
            ConcurrentClearCase {
                name: "cache cleared between the create and delete pass",
                race_the_clear: true,
            },
            ConcurrentClearCase {
                name: "cache left untouched",
                race_the_clear: false,
            },
        ];

        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };

        for case in cases {
            assert_eq!(
                blocked_capture_cleanup(&guild).await.unwrap(),
                0,
                "named zero-leftover check: {}",
                case.name
            );

            let result =
                startup_sweep_survives_concurrent_clear_exercise(&guild, case.race_the_clear).await;

            let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
            assert!(result.is_ok(), "{}: {result:?}", case.name);
            assert_eq!(channels_left, 0, "named zero-leftover check: {}", case.name);
        }
    }

    #[cfg(unix)]
    const NO_SESSION_LABEL: &str = "testrun-no-session";

    #[cfg(unix)]
    async fn drive_working_then_settled(
        workspace: &Workspace,
        tabs: &[herdr_connect_rs::HerdrTab],
        connection: &super::DiscordConnection,
        state: &mut BridgeState,
    ) -> Result<AgentSnapshot, String> {
        report_agent_state(&workspace.pane_id, "working")?;
        let working =
            wait_for_status(&workspace.pane_id, &["working"], Duration::from_secs(10)).await?;
        process_snapshot(
            &working,
            std::slice::from_ref(&working),
            tabs,
            connection,
            state,
        )
        .await;

        report_agent_state(&workspace.pane_id, "idle")?;
        let settled = wait_for_status(
            &workspace.pane_id,
            &["done", "idle"],
            Duration::from_secs(10),
        )
        .await?;
        process_snapshot(
            &settled,
            std::slice::from_ref(&settled),
            tabs,
            connection,
            state,
        )
        .await;
        Ok(settled)
    }

    #[cfg(unix)]
    async fn drive_idle_working_done(
        workspace: &Workspace,
        connection: &super::DiscordConnection,
        state: &mut BridgeState,
    ) -> Result<(TopologyRoute, AgentSnapshot), String> {
        report_agent_state(&workspace.pane_id, "idle")?;
        let idle = wait_for_status(&workspace.pane_id, &["idle"], Duration::from_secs(10)).await?;
        let matching = matching_tab(&workspace.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let route = route_topology(std::slice::from_ref(&idle), tabs, &idle.terminal_id)?;
        process_snapshot(&idle, std::slice::from_ref(&idle), tabs, connection, state).await;

        let settled = drive_working_then_settled(workspace, tabs, connection, state).await?;
        Ok((route, settled))
    }

    #[cfg(unix)]
    async fn session_less_pane_exercise(
        guild: &BlockedCaptureGuild,
        workspace: &Workspace,
    ) -> Result<(), String> {
        let mut state = BridgeState::default();
        let connection = discord_tuple(guild);
        let (route, _settled) = drive_idle_working_done(workspace, &connection, &mut state).await?;
        let topic = format!("herdr workspace [{}]", route.workspace_id);
        let thread_suffix = format!(" [{}]", route.tab_id);

        if !channel_with_topic_is_absent(guild, &topic).await? {
            return Err("session-less pane's workspace channel was created".to_owned());
        }
        if active_threads_for_guild(guild).await?.iter().any(|thread| {
            thread
                .name
                .as_deref()
                .is_some_and(|name| name.ends_with(&thread_suffix))
        }) {
            return Err("session-less pane's tab thread was created".to_owned());
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn session_less_pane_idle_working_done_posts_nothing() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(NO_SESSION_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(NO_SESSION_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-no-session-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create no-session test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_workspace(NO_SESSION_LABEL, cwd);
        let (workspace_id, result) = match created {
            Ok(workspace) => {
                let workspace_id = workspace.id.clone();
                let outcome = session_less_pane_exercise(&guild, &workspace).await;
                (Some(workspace_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(NO_SESSION_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(NO_SESSION_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const STARTUP_SESSION_SKIP_LABEL: &str = "testrun-startup-session-skip";

    #[cfg(unix)]
    async fn startup_topology_session_skip_exercise(
        guild: &BlockedCaptureGuild,
        workspace: &Workspace,
        second_tab: &Tab,
    ) -> Result<(), String> {
        report_agent_state(&workspace.pane_id, "idle")?;
        let no_session_agent = snapshot_for_pane(&workspace.pane_id)?;

        report_agent_state(&second_tab.pane_id, "idle")?;
        let session_id = generate_claude_session_id()?;
        report_agent_session(&second_tab.pane_id, &session_id)?;
        let session_agent = snapshot_for_pane(&second_tab.pane_id)?;
        let expected_session = AgentSession {
            agent: "claude".to_owned(),
            value: session_id.clone(),
        };
        if session_agent.session.as_ref() != Some(&expected_session) {
            return Err(format!(
                "expected session {expected_session:?} on pane {}, agent.list reported {session_agent:?}",
                second_tab.pane_id
            ));
        }

        let tabs = [
            matching_tab(&workspace.tab_id)?,
            matching_tab(&second_tab.tab_id)?,
        ];
        let agents = [no_session_agent.clone(), session_agent.clone()];

        let connection = discord_tuple(guild);
        sync_startup_topology(connection.3.chat().as_ref(), &agents, &tabs).await;

        let no_session_route = route_topology(&agents, &tabs, &no_session_agent.terminal_id)?;
        let session_route = route_topology(&agents, &tabs, &session_agent.terminal_id)?;
        let no_session_suffix = format!(" [{}]", no_session_route.tab_id);
        let session_suffix = format!(" [{}]", session_route.tab_id);

        let threads = active_threads_for_guild(guild).await?;
        if threads.iter().any(|thread| {
            thread
                .name
                .as_deref()
                .is_some_and(|name| name.ends_with(&no_session_suffix))
        }) {
            return Err("startup sweep created a thread for the session-less agent".to_owned());
        }
        if !threads.iter().any(|thread| {
            thread
                .name
                .as_deref()
                .is_some_and(|name| name.ends_with(&session_suffix))
        }) {
            return Err(
                "startup sweep did not create a thread for the session-carrying agent".to_owned(),
            );
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn startup_sweep_skips_session_less_agent_creates_for_session_carrying_agent() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(STARTUP_SESSION_SKIP_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(STARTUP_SESSION_SKIP_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-startup-session-skip-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create startup-session-skip test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_workspace(STARTUP_SESSION_SKIP_LABEL, cwd).and_then(|workspace| {
            create_tab(STARTUP_SESSION_SKIP_LABEL, &workspace.id, cwd)
                .map(|second_tab| (workspace, second_tab))
        });
        let (workspace_id, result) = match created {
            Ok((workspace, second_tab)) => {
                let workspace_id = workspace.id.clone();
                let outcome =
                    startup_topology_session_skip_exercise(&guild, &workspace, &second_tab).await;
                (Some(workspace_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(STARTUP_SESSION_SKIP_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(STARTUP_SESSION_SKIP_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const LATE_SESSION_TOPOLOGY_LABEL: &str = "testrun-late-session-topology";

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn late_session_idle_retry_syncs_topology_once_after_unknown_observation() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(LATE_SESSION_TOPOLOGY_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(LATE_SESSION_TOPOLOGY_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after unix epoch")
            .as_nanos();
        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-late-session-topology-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&cwd_dir).expect("create late-session-topology test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_workspace(LATE_SESSION_TOPOLOGY_LABEL, cwd);
        let (workspace_id, result) = match created {
            Ok(workspace) => {
                let workspace_id = workspace.id.clone();
                let outcome = async {
                    let mut state = BridgeState::default();
                    let connection = discord_tuple(&guild);
                    report_agent_state(&workspace.pane_id, "unknown")?;
                    let unknown =
                        wait_for_status(&workspace.pane_id, &["unknown"], Duration::from_secs(10))
                            .await?;
                    if unknown.session.is_some() {
                        return Err("unknown snapshot unexpectedly carried a session".to_owned());
                    }
                    let matching = matching_tab(&workspace.tab_id)?;
                    let tabs = std::slice::from_ref(&matching);
                    process_snapshot(
                        &unknown,
                        std::slice::from_ref(&unknown),
                        tabs,
                        &connection,
                        &mut state,
                    )
                    .await;

                    report_agent_state(&workspace.pane_id, "idle")?;
                    let session_id = generate_claude_session_id()?;
                    report_agent_session(&workspace.pane_id, &session_id)?;
                    let idle = wait_for_status(
                        &workspace.pane_id,
                        &[STATUS_IDLE, STATUS_DONE],
                        Duration::from_secs(30),
                    )
                    .await?;
                    if idle.session.as_ref().map(|session| session.agent.as_str())
                        != Some(VENDOR_CLAUDE)
                    {
                        return Err(format!(
                            "idle snapshot did not carry a real Claude session: {:?}",
                            idle.session
                        ));
                    }

                    let route =
                        route_topology(std::slice::from_ref(&idle), tabs, &idle.terminal_id)?;
                    let topic = format!("herdr workspace [{}]", route.workspace_id);
                    if !channel_with_topic_is_absent(&guild, &topic).await? {
                        return Err(
                            "unknown session-less observation created the workspace channel"
                                .to_owned(),
                        );
                    }
                    process_snapshot(
                        &idle,
                        std::slice::from_ref(&idle),
                        tabs,
                        &connection,
                        &mut state,
                    )
                    .await;

                    let channel = guild_channel_with_topic(&guild, &topic).await?;
                    let thread_suffix = format!(" [{}]", route.tab_id);
                    let threads = active_threads_for_guild(&guild).await?;
                    let matching_threads: Vec<_> = threads
                        .into_iter()
                        .filter(|thread| {
                            thread.parent_id == Some(channel.id)
                                && thread
                                    .name
                                    .as_deref()
                                    .is_some_and(|name| name.ends_with(&thread_suffix))
                        })
                        .collect();
                    if matching_threads.len() != 1 {
                        return Err(format!(
                            "expected one late-session topology thread, found {}",
                            matching_threads.len()
                        ));
                    }
                    let messages = thread_messages(&guild, matching_threads[0].id).await?;
                    if !messages.is_empty() {
                        return Err(format!(
                            "late-session topology created unexpected messages: {messages:?}"
                        ));
                    }

                    guild
                        .client
                        .delete_channel(matching_threads[0].id)
                        .await
                        .map_err(|error| error.to_string())?;
                    *connection.3.chat().topology_cache().lock().await = None;
                    process_snapshot(
                        &idle,
                        std::slice::from_ref(&idle),
                        tabs,
                        &connection,
                        &mut state,
                    )
                    .await;
                    if thread_with_suffix_survives(&guild, channel.id, &thread_suffix).await? {
                        return Err(
                            "a repeated idle snapshot synced the topology a second time".to_owned()
                        );
                    }
                    Ok::<(), String>(())
                }
                .await;
                (Some(workspace_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(LATE_SESSION_TOPOLOGY_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(LATE_SESSION_TOPOLOGY_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const SESSION_REPORTED_LATER_LABEL: &str = "testrun-session-reported-later";

    #[cfg(unix)]
    async fn session_less_pane_stays_silent_while_session_pane_is_mirrored_exercise(
        guild: &BlockedCaptureGuild,
        silent_workspace: &Workspace,
        claude_workspace: &Workspace,
        agent_name: &str,
    ) -> Result<(), String> {
        let (live_tx, mut live_events) = tokio::sync::mpsc::unbounded_channel();
        let mut state = BridgeState {
            live_tx: Some(live_tx),
            ..BridgeState::default()
        };
        let connection = discord_tuple(guild);
        drive_idle_working_done(silent_workspace, &connection, &mut state).await?;
        let silent_matching = matching_tab(&silent_workspace.tab_id)?;
        let silent_snapshot = snapshot_for_pane(&silent_workspace.pane_id)?;
        let silent_route = route_topology(
            std::slice::from_ref(&silent_snapshot),
            std::slice::from_ref(&silent_matching),
            &silent_snapshot.terminal_id,
        )?;
        if !channel_with_topic_is_absent(
            guild,
            &format!("herdr workspace [{}]", silent_route.workspace_id),
        )
        .await?
        {
            return Err("session-less pane's workspace channel was created".to_owned());
        }

        start_claude_haiku_agent(agent_name, &claude_workspace.pane_id)?;
        let with_session = snapshot_for_pane(&claude_workspace.pane_id)?;
        let session = with_session.session.as_ref().ok_or_else(|| {
            format!(
                "pane {} has no reported session after agent start",
                claude_workspace.pane_id
            )
        })?;
        if session.agent != "claude" {
            return Err(format!(
                "expected a claude session on pane {}, agent.list reported {session:?}",
                claude_workspace.pane_id
            ));
        }
        let claude_terminal = with_session.terminal_id.clone();
        let claude_matching = matching_tab(&claude_workspace.tab_id)?;
        let claude_tabs = std::slice::from_ref(&claude_matching);
        let claude_route = route_topology(
            std::slice::from_ref(&with_session),
            claude_tabs,
            &claude_terminal,
        )?;
        let claude_topic = format!("herdr workspace [{}]", claude_route.workspace_id);
        let claude_suffix = format!(" [{}]", claude_route.tab_id);

        process_snapshot(
            &with_session,
            std::slice::from_ref(&with_session),
            claude_tabs,
            &connection,
            &mut state,
        )
        .await;

        prompt_claude_agent_and_wait(agent_name, "Reply with exactly the word ready.")?;
        let settled = snapshot_for_pane(&claude_workspace.pane_id)?;
        process_snapshot(
            &settled,
            std::slice::from_ref(&settled),
            claude_tabs,
            &connection,
            &mut state,
        )
        .await;
        while let Ok(terminal_id) = live_events.try_recv() {
            handle_live_event(&connection, &terminal_id, &mut state).await;
        }
        handle_live_event(&connection, &claude_terminal, &mut state).await;

        let channel = guild_channel_with_topic(guild, &claude_topic)
            .await
            .map_err(|_| "reporting a session did not create the workspace channel".to_owned())?;
        let thread = active_threads_for_guild(guild)
            .await?
            .into_iter()
            .find(|thread| {
                thread.parent_id == Some(channel.id)
                    && thread
                        .name
                        .as_deref()
                        .is_some_and(|name| name.ends_with(&claude_suffix))
            })
            .ok_or_else(|| "reporting a session did not create the tab thread".to_owned())?;
        let messages = thread_full_messages(guild, thread.id).await?;
        if messages.iter().any(|message| {
            message.webhook_id.is_none()
                && message.embeds.is_empty()
                && message.content.to_lowercase().contains("ready")
        }) {
            Ok(())
        } else {
            let contents: Vec<_> = messages.iter().map(|message| &message.content).collect();
            Err(format!(
                "reporting a session did not post the reply as live text, thread has {contents:?}"
            ))
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn session_less_pane_stays_silent_while_session_pane_is_mirrored() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_tabs(SESSION_REPORTED_LATER_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(SESSION_REPORTED_LATER_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after unix epoch")
            .as_nanos();
        let silent_cwd_dir = std::env::temp_dir().join(format!(
            "testrun-session-reported-later-silent-{}-{nanos}",
            std::process::id()
        ));
        fs::create_dir_all(&silent_cwd_dir).expect("create session-reported-later silent cwd");
        let silent_cwd = silent_cwd_dir.to_str().expect("temp cwd is valid UTF-8");
        let claude_cwd_dir = claude_testrun_dir(&home);
        clear_directory_contents(&claude_cwd_dir).expect("clear session-reported-later claude cwd");
        let claude_cwd = claude_cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let (workspace_ids, result) =
            match create_workspace(SESSION_REPORTED_LATER_LABEL, silent_cwd) {
                Ok(silent_workspace) => {
                    match create_workspace(SESSION_REPORTED_LATER_LABEL, claude_cwd) {
                        Ok(claude_workspace) => {
                            let workspace_ids =
                                vec![silent_workspace.id.clone(), claude_workspace.id.clone()];
                            let agent_name = format!(
                                "testrun-late-{}",
                                agent_name_nonce().expect("system clock is after unix epoch")
                            );
                            let outcome =
                            session_less_pane_stays_silent_while_session_pane_is_mirrored_exercise(
                                &guild,
                                &silent_workspace,
                                &claude_workspace,
                                &agent_name,
                            )
                            .await;
                            cleanup_real_claude_session_dir(&home, &claude_workspace.pane_id);
                            (workspace_ids, outcome)
                        }
                        Err(error) => (vec![silent_workspace.id.clone()], Err(error)),
                    }
                }
                Err(error) => (vec![], Err(error)),
            };
        for workspace_id in &workspace_ids {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&silent_cwd_dir);
        let _ = clear_directory_contents(&claude_cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(SESSION_REPORTED_LATER_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(SESSION_REPORTED_LATER_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
    }
}
