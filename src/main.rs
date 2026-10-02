use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

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
    AgentLogCapture, AgentSession, AgentSnapshot, ComponentHandler, ENV_DISCORD_GUILD_ID,
    ENV_DISCORD_OWNER_ID, ENV_DISCORD_TOKEN, ENV_HOME, EVENT_KEY, GatewayContext,
    HerdrSubscription, HerdrTab, OwnerIdentity, STATUS_BLOCKED, STATUS_DONE, STATUS_IDLE,
    STATUS_WORKING, TopologyCache, TopologyRoute, Transition, TransitionMessage,
    UNKNOWN_CHANNEL_DELIVERY_ERROR, UNKNOWN_WEBHOOK_DELIVERY_ERROR, agent_read_detection,
    cached_route, create_transition_messages, delete_tab_thread, delete_topology_absent_from_herdr,
    delete_workspace_channel, deliver_live_message, deliver_transition_card,
    drive_gateway_with_components, execute_terminal_prompt_webhook, expire_informational_card,
    fetch_owner_identity, fetch_topology_lists, format_detection_question, hook_timeout,
    is_postable_transition, lifecycle_subscriptions, list_agents, live_message_nonce,
    load_discord_config, name_unlabeled_tabs, read_claude_incremental,
    read_claude_prompts_incremental, read_codex_incremental, read_codex_prompts_incremental,
    read_cursor_incremental, read_cursor_prompts_incremental, reconcile_topology_cache,
    register_archived_tab_threads, resolve_terminal_prompt_webhook, route_topology,
    split_live_message, status_subscriptions, subscribe_herdr_events, sync_topology,
    tab_list_result, take_owner_prompt_suppression, transition_card_nonce, workspace_channel_id,
    workspace_list_result,
};
use herdr_connect_rs::{
    Decision, Interaction, PermissionResponder, PermissionVendor, VENDOR_CLAUDE, VENDOR_CODEX,
    VENDOR_CURSOR, cursor_argv_forces_allow, decode_claude_permission_request,
    decode_codex_permission_request, decode_cursor_permission_request, encode_claude_decision,
    encode_codex_decision, encode_cursor_decision, handle_component, is_cursor_agent_argv,
    request_decision, run_broker as run_permission_broker,
};
use herdr_connect_rs::{
    decode_claude_ask_question, encode_claude_question_decision, question_hook_timeout,
    request_question_answers,
};

/// The Discord client, the guild, the owner's id, the permission responder, and the owner's
/// mirrored identity, fetched once at startup.
type DiscordConnection = (
    Arc<Client>,
    Id<GuildMarker>,
    String,
    Arc<PermissionResponder>,
    OwnerIdentity,
);
type GatewayTask = tokio::task::JoinHandle<Result<(), String>>;
type BrokerTask = tokio::task::JoinHandle<Result<(), String>>;

#[derive(Clone, Copy)]
struct InformationalCard {
    channel: Id<ChannelMarker>,
    message: Id<MessageMarker>,
}

/// A vendor-log reader and where it resumes from: a byte offset into the Claude or Codex JSONL
/// log, a `rowid` of the Cursor sqlite store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Follower {
    Claude(u64),
    Codex(u64),
    Cursor(i64),
}

/// Dropping this stops its `notify` watcher.
struct LiveWatch {
    _watcher: notify::RecommendedWatcher,
    path: PathBuf,
    follower: Follower,
    /// Resolves the tab thread to post into on every event through the cache-first `sync_route`,
    /// so the live event loop needs no agent/tab snapshot in scope and a thread deleted outside the
    /// bridge's own tracking is re-resolved on the next event once the cache is cleared.
    route: TopologyRoute,
}

#[derive(Default)]
struct BridgeState {
    previous: HashMap<String, String>,
    state_change_sequences: HashMap<String, u64>,
    informational_cards: HashMap<String, InformationalCard>,
    live_watches: HashMap<String, LiveWatch>,
    /// `None` in tests that never wire live capture up.
    live_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    /// Live-capture errors -- a read failure, a route failure, or a failed post -- and topology
    /// route errors already logged for a terminal, keyed by the terminal and the error text
    /// together so the same error is not repeated on every later event while a different error
    /// still is. Every entry for a terminal is cleared once a full read-and-deliver for it
    /// succeeds, so a recurring error after a recovery is logged afresh.
    live_errors_reported: HashSet<(String, String)>,
    /// Tabs whose generated-name rename failure was already logged, so a persistent failure is
    /// logged once per tab rather than on every snapshot pass.
    rename_errors_reported: HashSet<String>,
    /// One turn's activity message per pane, keyed by pane id (the identity an activity frame
    /// carries; a live watch's terminal id is a different Herdr identity for the same pane).
    /// Forgotten -- not deleted -- when the pane next reports `working`, so the new turn starts a
    /// fresh message instead of editing the previous turn's.
    activity_messages: HashMap<String, ActivityMessage>,
    /// Pane ids the latest Herdr snapshot reports as `working` with a session, kept fresh by
    /// every [`process_snapshot`] call (whether from the doorbell's `agent.list` sweep or a
    /// single-pane update). `handle_activity_event` checks this before creating a message: an
    /// activity frame that outlives its turn, or that names a pane with no reported session, has
    /// nothing to gate its creation without it.
    activity_eligible_panes: HashSet<String>,
    /// Per-terminal read baseline, established the first time [`process_snapshot`] sees a
    /// session-carrying pane in any status: the resolved log path, the prompt reader's start
    /// position, and the live text watch's start position, all captured at the same moment so the
    /// two readers agree on where "past everything already there" is and no prompt written between
    /// them is mirrored without its reply. Existing prompts and replies already in the log at that
    /// first sight are never replayed, but everything recorded afterward is. Keyed by terminal id,
    /// re-baselined when a later session on the same terminal (`/clear`, resume, a relaunch, or a
    /// vendor starting a fresh file or store) resolves a different path, rather than reusing a stale
    /// position from the old file.
    terminal_prompt_positions: HashMap<String, (PathBuf, Follower, Follower)>,
    /// Panes seen with a session whose [`live_log_path`] returned `Ok(None)` -- the log does not
    /// exist on disk yet -- keyed by terminal, valued by that session's identity. A fresh pane, a
    /// new session after `/clear`, or a relaunch before its first write records its own session
    /// here. This is rule 1's position-0-when-absent tracker, shared by the prompt reader and the
    /// live text watch: when this pane's own session log finally appears, both baseline at 0 rather
    /// than at the current end, so the pane's own first prompt and reply are mirrored.
    ///
    /// A pane first seen *without* any session records nothing here: it may later resolve to an
    /// already-populated session (a relaunch straight into existing history, or a resume), whose
    /// history predates the bridge and must never be replayed. A pane can also switch to a
    /// *different*, already-populated session before its pending one's log ever appears; the
    /// recorded session identity is compared against the resolved one, so only the same session
    /// that was pending baselines at 0, and any other session gets the normal
    /// discard-what-already-exists baseline. Removed the moment a path resolves for this terminal.
    awaiting_first_log: HashMap<String, String>,
    /// The bridge-owned terminal-prompt webhook resolved for each workspace channel, so mirroring
    /// a prompt does not list the channel's webhooks on every call. Cleared for a channel whose
    /// cached webhook fails delivery with an unknown-channel error, forcing one fresh resolve.
    terminal_prompt_webhooks: HashMap<Id<ChannelMarker>, (Id<WebhookMarker>, String)>,
}

/// The Discord message tracking one pane's current turn of tool activity: `count` tool calls
/// edited into it so far.
struct ActivityMessage {
    message: Id<MessageMarker>,
    count: u32,
}

struct BlockedCardContext<'a> {
    client: &'a Client,
    guild: Id<GuildMarker>,
    owner_id: &'a str,
    responder: &'a PermissionResponder,
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

/// The bridge-owned webhook name every workspace channel's terminal-prompt webhook is looked up or
/// created under.
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

/// Posts one blocked card for a pane that just entered `blocked`, built from whatever context is
/// available at that moment: a Claude pane's Herdr detection snapshot for the pending question, or
/// the vendor log otherwise. A supported vendor whose broker already has a pending permission or
/// question request for this session is left to that interactive card and posts nothing here. The
/// card is posted once; there is no capture retry.
async fn handle_blocked_card(context: BlockedCardContext<'_>) {
    let BlockedCardContext {
        client,
        guild,
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
    let target = match sync_route(client, guild, route, topology_cache).await {
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
    let detection_question = (snapshot.agent.as_deref() == Some(VENDOR_CLAUDE))
        .then(|| match agent_read_detection(&route.pane_id) {
            Ok(text) => Some(text),
            Err(error) => {
                bridge_eprintln!("agent detection read error for {terminal}: {error}");
                None
            }
        })
        .flatten()
        .and_then(|text| format_detection_question(&text));
    let capture = detection_question.map_or_else(
        || capture_for_blocked(snapshot, session),
        |question| AgentLogCapture {
            message: question.clone(),
            question: Some(question),
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

/// What [`deliver_blocked_messages`] needs to deliver a blocked card, bundled to keep the function
/// under the argument-count lint.
#[derive(Clone, Copy)]
struct BlockedDeliveryRoute<'a> {
    client: &'a Client,
    topology_cache: &'a TopologyCache,
}

/// Delivers each blocked-card message to `target`. A send that finds the cached thread gone
/// (unknown channel, deleted outside the bridge's own tracking) clears the shared topology cache
/// and logs; the next blocked event resolves the route again through the cache-first path. It does
/// not re-resolve and retry inside this call.
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
        let (client, ..) = discord;
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
    let (client, ..) = discord;
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

/// Split out of [`process_snapshot`] to keep it under the line-count lint. Keeps
/// `state.activity_eligible_panes` in step with this snapshot's pane: eligible while it reports
/// `working` with a session, not otherwise.
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
    let (client, guild, _, responder, _) = discord;
    if let Err(error) =
        sync_route(client.as_ref(), *guild, &route, responder.topology_cache()).await
    {
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
    bridge_println!(
        "{} {terminal}: {status}",
        snapshot.agent.as_deref().unwrap_or("none")
    );
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
        // The activity message is forgotten when the pane next reports `working`, so the new turn's
        // first tool call starts a fresh message instead of editing the previous turn's.
        if status == STATUS_WORKING {
            forget_activity_message(state, &snapshot.pane_id);
        }
    }
    remember_previous_status(state, &terminal, &status);
}

/// Split out of [`process_snapshot`] to keep it under the line-count lint.
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

/// Delivers a blocked transition's card to Discord: an agent reporting no session is not mirrored
/// and returns before any topology is routed. A tab whose topology cannot be routed is logged and skipped. Working, done, and idle transitions post nothing --
/// assistant text reaches Discord live from the pane's vendor-log watch, not as a turn-end card,
/// and a failed turn shows as the agent's own text the same way.
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
    let (client, guild, owner_id, responder, _) = discord;
    handle_blocked_card(BlockedCardContext {
        client: client.as_ref(),
        guild: *guild,
        owner_id,
        responder: responder.as_ref(),
        topology_cache: responder.topology_cache(),
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

/// Split out of [`process_snapshot`] to keep it under the line-count lint: delivers a blocked
/// transition's card. Working, done, and idle transitions post nothing.
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

fn component_handler(responder: Arc<PermissionResponder>) -> ComponentHandler {
    Arc::new(move |interaction| {
        let responder = Arc::clone(&responder);
        Box::pin(async move { handle_component(responder, interaction).await })
    })
}

/// Why a Claude/Codex/Cursor session's on-disk log could not be resolved, classified by KIND
/// rather than by matching the rendered message text.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SessionPathError {
    /// The log itself does not exist yet (zero candidates matched). Not a real problem: the vendor
    /// may still write it once its first turn starts, so the caller retries on every later
    /// snapshot.
    NotFoundYet(String),
    /// A real, non-transient problem — an ambiguous session (multiple candidate logs), an
    /// unsupported vendor, or a search directory that cannot be read at all — surfaced as an error
    /// the caller logs, rather than silently awaited like a log that has simply not appeared yet.
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
            // Codex continues a session in a new rollout whose filename timestamp sorts later.
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

/// Every Claude config directory directly under `search_root`: named `.claude` or starting with
/// `.claude-`, and containing a `projects` directory. Covers `CLAUDE_CONFIG_DIR` overrides such
/// as `~/.claude-two` alongside the default `~/.claude` and `~/.claude-one`. Sorted so the
/// search order is deterministic.
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

/// Collapses candidates that are hard links to the same file (same device and inode) into one
/// path, so a session log hard-linked across Claude config directories (for example `~/.claude`
/// and `~/.claude-one`) is not mistaken for two distinct session logs.
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

/// `Ok(None)` means the log does not exist yet ([`SessionPathError::NotFoundYet`]): the caller
/// retries later rather than treating it as an error.
///
/// # Errors
///
/// Returns any [`SessionPathError::Permanent`] error, or a misconfigured `HOME`.
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

/// Registers a `notify` watch on a vendor log path, forwarding the terminal id on every modify
/// event. Cursor watches the chat directory that holds `store.db` instead of the file itself, so
/// both the creation of its `-wal` sibling and later writes to it wake the follower; a watch on the
/// file, set up before the sibling exists (the common case for a freshly started pane), would never
/// see it appear.
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

/// The position a freshly opened live-capture follower resumes from when its log already exists at
/// first sight: past every assistant text currently in it, so a bridge that discovers a pane
/// mid-conversation never replays its history. A follower whose log did not exist yet at first
/// sight starts at 0 instead (see [`ensure_live_watch_started`]), so the whole log it later writes
/// is posted. `start` selects the log format and the position the read starts from.
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

/// The position a terminal's terminal-prompt mirroring starts from the first time it is ever
/// established for that terminal: past every prompt already in the vendor log, so a bridge that
/// discovers a pane mid-conversation never replays its history. `start` selects the log format and
/// the position the read starts from.
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

/// Reads new complete owner prompts appended to a terminal's vendor log since `position`, and the
/// position immediately after the last one read.
///
/// # Errors
///
/// Returns the incremental reader's error for the follower's vendor.
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

/// Ensures one live-capture follower per session-carrying pane, opened the first time the bridge
/// sees the pane's log resolve -- in any status, not only `working` -- and kept open across turns
/// so every assistant text the log later gains is posted, regardless of status. The watch is
/// replaced (its `notify` watcher dropped) only when the pane's session resolves to a different log
/// path; a closed pane's watch is pruned by [`prune_departed_state`]. On every snapshot the watch's
/// route is refreshed from `route_topology`, before the path check, so a pane moved to a different
/// tab or workspace mid-session posts to its new thread even though its log path is unchanged. It
/// follows the same log path the prompt baseline resolved for this snapshot, so the two readers
/// agree on where "past everything already there" is: the live position baselines at 0 when the log
/// did not exist at first sight (the prompt baseline is 0 too) and past every existing assistant
/// text otherwise.
///
/// A bridge restart re-follows from the current end and so reposts nothing; a log that grows before
/// the restart's first read is still picked up from that end.
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
    // Live text exists only for Claude, Codex, and Cursor logs; other vendors are not followed.
    if !matches!(
        session.agent.as_str(),
        VENDOR_CLAUDE | VENDOR_CODEX | VENDOR_CURSOR
    ) {
        return;
    }
    // The prompt baseline runs first on this snapshot and resolves the log path; the live watch
    // follows that same path. No entry means the log is not on disk yet (awaiting first log), so
    // there is nothing to follow until a later snapshot.
    let Some((path, _prompt_position, live_position)) =
        state.terminal_prompt_positions.get(&terminal).cloned()
    else {
        return;
    };
    // Refresh the route on every snapshot, before the path check, so a pane moved to a different
    // tab or workspace mid-session posts to its new thread even though its log path is unchanged.
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
    // Read once immediately: a fresh watch that baselined at 0 posts the whole log the pane just
    // wrote, and the next `notify` tick may never come if nothing more is written.
    handle_live_event(discord, &terminal, state).await;
}

/// Does not touch the watch's stored position: the caller advances it only past text it actually
/// delivers, so a text this read returns but a later delivery attempt drops is re-read and
/// re-sent rather than skipped. Each text is paired with the position immediately after it, and
/// the last element is the position after everything read.
///
/// # Errors
///
/// Returns the incremental reader's error for the follower's vendor.
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

/// Resolves both a route's workspace channel and its tab thread, reusing [`sync_route`]'s
/// cache-first synchronization and then reading the now-populated cache for the workspace channel
/// alongside it.
///
/// # Errors
///
/// Returns [`sync_route`]'s error, or a topology error when the cache does not hold the workspace
/// channel immediately after a successful sync (a topology invariant `sync_route` itself relies
/// on).
async fn sync_route_channels(
    client: &Client,
    guild: Id<GuildMarker>,
    route: &TopologyRoute,
    topology_cache: &TopologyCache,
) -> Result<(Id<ChannelMarker>, Id<ChannelMarker>), String> {
    let thread = sync_route(client, guild, route, topology_cache).await?;
    let guard = topology_cache.lock().await;
    let found = guard
        .as_ref()
        .and_then(|(channels, _)| workspace_channel_id(channels, &route.workspace_id));
    drop(guard);
    let workspace_channel = found.ok_or_else(|| {
        "herdr topology error: workspace channel missing from cache after sync".to_owned()
    })?;
    Ok((workspace_channel, thread))
}

/// The bridge-owned terminal-prompt webhook for `workspace_channel`, from
/// [`BridgeState::terminal_prompt_webhooks`] when already resolved, otherwise resolved fresh and
/// cached.
///
/// # Errors
///
/// Returns [`resolve_terminal_prompt_webhook`]'s error.
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

/// Everything [`mirror_one_terminal_prompt`] needs to deliver into one already-resolved route,
/// bundled to stay under the argument-count lint; fixed for the whole mirrored batch.
#[derive(Clone, Copy)]
struct TerminalPromptTarget<'a> {
    client: &'a Client,
    topology_cache: &'a TopologyCache,
    workspace_channel: Id<ChannelMarker>,
    thread: Id<ChannelMarker>,
}

/// Resolves (from cache or fresh) the webhook for `target.workspace_channel` and executes one
/// message into `target.thread`, returning `target` unchanged on success so the caller can chain
/// further parts or prompts against the same resolved pair.
///
/// # Errors
///
/// Returns [`cached_terminal_prompt_webhook`]'s or [`execute_terminal_prompt_webhook`]'s error --
/// covering both webhook resolution and execution.
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

/// Delivers one mirrored terminal prompt through the bridge-owned webhook, split into parts the
/// same way live text is so Discord's 2000-character message limit never silently drops it, one
/// webhook message per part, in order.
///
/// A delivery failure stops the batch there rather than sending later parts out of order. When the
/// cached thread is gone (unknown channel) the shared topology cache is cleared, and when the
/// cached webhook is gone (unknown webhook) that cached webhook is dropped; either way the next
/// mirrored prompt resolves the route and webhook again through the cache-first path. It does not
/// re-resolve and retry inside this call.
///
/// # Errors
///
/// Returns a Discord request or response error.
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

/// Establishes (or re-establishes, on a session change) the terminal-prompt read baseline for
/// `snapshot`'s pane, the first time the bridge ever sees it with a session, in any status -- not
/// only `working`. A prompt typed while the pane is idle, before its turn even starts, is
/// therefore captured here rather than excluded by a baseline set too late (once the pane finally
/// reaches `working` and its own initiating prompt is already in the log).
///
/// Keyed by terminal id in [`BridgeState::terminal_prompt_positions`], but re-baselined whenever
/// the freshly resolved log path differs from the one already stored there: a later session on the
/// same terminal (`/clear`, resume, a relaunch, or a vendor starting a fresh file or store) must
/// never have the old file's position applied to the new one.
///
/// A pane seen with a session whose [`live_log_path`] returns `Ok(None)` (the log or store does not
/// exist on disk yet -- a fresh pane, or a new session before its first write) records that
/// session's identity in [`BridgeState::awaiting_first_log`] and retries on the next snapshot. The
/// first time a path then resolves for this terminal, the baseline is 0 -- not
/// [`initial_terminal_prompt_position`]'s discard-what-already-exists checkpoint -- only when the
/// resolved path's session is the SAME one that was recorded pending: its log was created after
/// this terminal was already being watched, so everything now in it, including the prompt that may
/// have just created it, postdates first sight and belongs to this bridge run, not a discarded
/// history. A path resolving for any OTHER session -- one that was never recorded pending (a pane
/// first seen without a session, then relaunched straight into existing history), or a different
/// one that superseded the pending session -- gets the normal discard-what-already-exists baseline
/// instead, and the stale pending record is dropped either way so it cannot later misapply to a
/// third session. A permanent resolution error is logged and retried too, since resolving it costs
/// only a directory read.
fn maybe_establish_terminal_prompt_baseline(snapshot: &AgentSnapshot, state: &mut BridgeState) {
    let terminal = &snapshot.terminal_id;
    let Some(session) = snapshot.session.as_ref() else {
        return;
    };
    // Claude, Codex, and Cursor are the vendors whose logs are followed; `zero` is the baseline for
    // a log this terminal is only now seeing resolve for the first time.
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
        return;
    }
    let was_awaiting_this_session = state
        .awaiting_first_log
        .get(terminal)
        .is_some_and(|awaiting_session| awaiting_session == &session.value);
    // Capture the prompt reader's and the live text watch's start positions together, from the one
    // log the pane resolves now, so no prompt written between two separate reads is mirrored
    // without its reply. When this pane's own session log has only just appeared, both start at 0
    // to catch everything the pane writes; otherwise both start past everything already there. A
    // read that errors (Cursor creates its message table lazily on the first write) defers the
    // whole baseline -- and with it the watch -- to the next snapshot rather than baselining a log
    // that cannot be read yet.
    let positions = if was_awaiting_this_session {
        initial_live_position(zero, &path).map(|_| (zero, zero))
    } else {
        initial_terminal_prompt_position(zero, &path)
            .and_then(|prompt| initial_live_position(zero, &path).map(|live| (prompt, live)))
    };
    match positions {
        Ok((prompt_position, live_position)) => {
            state.awaiting_first_log.remove(terminal);
            state
                .terminal_prompt_positions
                .insert(terminal.clone(), (path, prompt_position, live_position));
        }
        Err(error) => bridge_eprintln!("terminal prompt baseline error for {terminal}: {error}"),
    }
}

/// Whether `terminal` already has a terminal-prompt baseline for exactly `path`: `false` both when
/// there is no baseline yet and when there is one for a different path (a session change -- a
/// `/clear`, resume, relaunch, or a vendor starting a fresh file or store -- must re-baseline
/// against the new path rather than reuse the old file's position, which the new file may not even
/// be as long as).
fn terminal_prompt_baseline_is_current(
    positions: &HashMap<String, (PathBuf, Follower, Follower)>,
    terminal: &str,
    path: &Path,
) -> bool {
    positions
        .get(terminal)
        .is_some_and(|(existing_path, _, _)| existing_path == path)
}

/// Mirrors newly recorded owner prompts from one terminal's vendor log into its tab thread through
/// the bridge-owned webhook, in log order, ahead of any assistant text the same `notify` tick
/// delivers -- the caller runs this before reading live text.
///
/// Reads forward from the path and prompt position [`maybe_establish_terminal_prompt_baseline`]
/// last left in [`BridgeState::terminal_prompt_positions`] -- its own position, advanced only past
/// prompts, distinct from the live-text watch's position advanced past assistant texts, so each
/// reader tracks its own progress through the shared log. Does nothing if that baseline is not
/// established yet: `process_snapshot` always runs it first, for every status, before this function
/// ever has a `LiveWatch` to be called from.
///
/// A prompt equal to a pending [`take_owner_prompt_suppression`] marker is dropped once instead of
/// mirrored: it is the bridge's own Discord-originated prompt, already posted by the owner in the
/// thread it came from. No session or no route: dropped silently. A delivery
/// failure is logged once and the position still advances past it -- a stuck prompt does not block
/// mirroring later ones.
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
    let (client, guild, _, responder, identity) = connection;
    let topology_cache = responder.topology_cache();
    let (workspace_channel, thread) =
        match sync_route_channels(client.as_ref(), *guild, route, topology_cache).await {
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

/// Mirrors any new owner terminal prompts via [`mirror_terminal_prompts`] before reading this
/// tick's live text, so a prompt that started the current turn is posted to Discord ahead of the
/// assistant text it produced.
///
/// Each posted text's nonce is derived from the terminal id and log position, not a counter, so it
/// survives a watch restart that resumes at the same position. The tab thread is resolved fresh on
/// every event through the cache-first [`sync_route`], so no channel is cached on the watch.
///
/// A read failure, a route failure, or a failed post is logged once per terminal (deduped through
/// [`BridgeState::live_errors_reported`], cleared on the next full success) and leaves the follower
/// running at its unchanged position: the stored log position only advances past text that was
/// actually delivered, so a text that still fails, and everything read after it, is re-read and
/// re-sent on the next log change. A send that finds the thread gone (unknown channel) also clears
/// the shared topology cache, so that next event re-resolves the route.
async fn handle_live_event(
    connection: &DiscordConnection,
    terminal: &str,
    state: &mut BridgeState,
) {
    let (client, guild, _, responder, _) = connection;
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
    let topology_cache = responder.topology_cache();
    let channel = match sync_route(client.as_ref(), *guild, &route, topology_cache).await {
        Ok(channel) => channel,
        Err(error) => {
            if state
                .live_errors_reported
                .insert((terminal.to_owned(), error.clone()))
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

/// Forgets a pane's tracked activity message, if any, so the next turn's first activity frame
/// creates a fresh one instead of editing the previous turn's message.
fn forget_activity_message(state: &mut BridgeState, pane_id: &str) {
    state.activity_messages.remove(pane_id);
}

/// The route's tab thread from the cached topology only, issuing no Discord request. `Ok(None)`
/// when the cache is not yet populated or does not resolve the route.
///
/// # Errors
///
/// Returns the cache's error when it lists duplicate threads for the route's tab.
async fn cached_route_channel(
    topology_cache: &TopologyCache,
    route: &TopologyRoute,
) -> Result<Option<Id<ChannelMarker>>, String> {
    let guard = topology_cache.lock().await;
    let Some((channels, active_threads)) = guard.as_ref() else {
        return Ok(None);
    };
    let channel = cached_route(channels, active_threads, route);
    drop(guard);
    channel
}

/// Applies one activity frame: routes it to its tab's thread purely from the cached topology, then
/// posts or edits this turn's one activity message for the pane.
///
/// A cache not yet populated, or a route the cache does not resolve, drops the frame silently (a
/// cache that lists duplicate threads for the tab drops it and logs), and
/// so does a pane the latest snapshot does not report as `working` with a session -- the same
/// no-session rule every other card follows. A send that finds the cached thread gone (unknown
/// channel) clears the shared topology cache and logs; the next frame drops until the cache-first
/// route is repopulated by other traffic. It does not re-resolve and retry inside this call.
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
    let topology_cache = responder.topology_cache();
    let channel = match cached_route_channel(topology_cache, &route).await {
        Ok(Some(channel)) => channel,
        Ok(None) => return,
        Err(error) => {
            bridge_eprintln!("activity route error for pane {}: {error}", frame.pane_id);
            return;
        }
    };
    // The eligibility gate covers both editing and creating: a frame that arrives after the pane
    // left `working` (its activity message still lingering until the next turn) is dropped rather
    // than editing the settled turn's message.
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
            }
        }
    }
}

/// Resolves `route`'s tab thread, serving it straight from `topology_cache` when the cache
/// already holds both the route's workspace channel and its tab thread. Refetches -- one Discord
/// round trip instead of one per lifecycle event -- only when the cache is empty, does not yet
/// hold the route, or (via a caller invalidating it first) held a channel a send just rejected as
/// unknown. One guard is held for the whole call, across the refetch too, so a concurrent miss on
/// the same route blocks on the lock instead of racing its own fetch-and-create.
async fn sync_route(
    client: &Client,
    guild: Id<GuildMarker>,
    route: &TopologyRoute,
    topology_cache: &TopologyCache,
) -> Result<Id<ChannelMarker>, String> {
    let mut guard = topology_cache.lock().await;
    sync_route_locked(client, guild, route, &mut guard).await
}

/// [`sync_route`]'s body, run under a guard the caller acquired and holds for this whole call
/// (including the refetch): a second concurrent miss on the same route blocks on that same guard
/// instead of racing its own fetch-and-create. Split into its own function, taking the guard by
/// reference rather than owning it, purely so `sync_route` itself has one single, unbroken use of
/// its guard for lint purposes; the locking behavior is identical either way.
async fn sync_route_locked(
    client: &Client,
    guild: Id<GuildMarker>,
    route: &TopologyRoute,
    guard: &mut tokio::sync::MutexGuard<
        '_,
        Option<(
            Vec<twilight_model::channel::Channel>,
            Vec<twilight_model::channel::Channel>,
        )>,
    >,
) -> Result<Id<ChannelMarker>, String> {
    let cached = match guard.as_ref() {
        Some((channels, active_threads)) => cached_route(channels, active_threads, route)
            .map_err(|error| format!("discord topology error: {error}"))?,
        None => None,
    };
    if let Some(channel) = cached {
        return Ok(channel);
    }
    let fetched = fetch_topology_lists(client, guild)
        .await
        .map_err(|error| format!("discord topology error: {error}"))?;
    let (channels, active_threads) = reconcile_topology_cache(guard, fetched);
    sync_topology(client, guild, channels, active_threads, route)
        .await
        .map_err(|error| format!("discord topology error: {error}"))
}

/// Ensures every workspace channel and tab thread exists, then deletes every workspace channel
/// and tab thread Herdr no longer lists. Runs in a spawned task beside the event loop rather than
/// blocking it. An agent reporting no session is not mirrored: it is skipped in the create pass,
/// so no channel or thread is created for it until a later snapshot reports one. A per-tab
/// routing or naming error is logged and skipped; the lazy sync inside delivery
/// still covers that tab once a card is due. The sweep refetches both lists once at its start rather than
/// adopting whatever the shared cache already holds, so a cache that missed an earlier create
/// cannot make the sweep recreate an existing channel or thread. The reconciliation pass that
/// follows reuses this same cache rather than refetching per tab; an Ok but empty Herdr
/// workspace or tab list is authoritative and deletes accordingly, while an Err from either
/// Herdr call skips the whole delete pass with one logged error line. The delete pass is
/// unaffected by session: a live tab keeps any thread that already exists. Both passes tolerate
/// the shared cache going empty mid-sweep -- a concurrent stale-route recovery elsewhere clears
/// it for microseconds while it re-resolves -- by refetching in place rather than aborting: the
/// create pass skips just that one agent and continues, and the delete pass still runs.
async fn sync_startup_topology(
    discord: &DiscordConnection,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
) {
    let (client, guild, _, responder, _) = discord;
    let topology_cache = responder.topology_cache();
    let fetched = match fetch_topology_lists(client.as_ref(), *guild).await {
        Ok(lists) => lists,
        Err(error) => {
            bridge_eprintln!("herdr startup topology error: {error}");
            return;
        }
    };
    let mut guard = topology_cache.lock().await;
    reconcile_topology_cache(&mut guard, fetched);
    drop(guard);
    let mut synced_tabs = HashSet::new();
    for agent in agents {
        if agent.session.is_none() {
            continue;
        }
        if !synced_tabs.insert(agent.tab_id.clone()) {
            continue;
        }
        let route = match route_topology(agents, tabs, &agent.terminal_id) {
            Ok(route) => route,
            Err(error) => {
                bridge_eprintln!("herdr startup topology error: {error}");
                continue;
            }
        };
        let mut guard = topology_cache.lock().await;
        let (channels, active_threads) = if let Some((channels, active_threads)) = guard.as_mut() {
            (channels, active_threads)
        } else {
            let fetched = match fetch_topology_lists(client.as_ref(), *guild).await {
                Ok(fetched) => fetched,
                Err(error) => {
                    bridge_eprintln!("herdr startup topology error: {error}");
                    continue;
                }
            };
            reconcile_topology_cache(&mut guard, fetched)
        };
        let result = sync_topology(client.as_ref(), *guild, channels, active_threads, &route).await;
        drop(guard);
        if let Err(error) = result {
            bridge_eprintln!("herdr startup topology error: {error}");
        }
    }
    let (workspaces, live_tabs) = match (workspace_list_result(), tab_list_result()) {
        (Ok(workspaces), Ok(live_tabs)) => (workspaces, live_tabs),
        (Err(error), _) | (_, Err(error)) => {
            bridge_eprintln!("herdr startup topology reconciliation error: {error}");
            return;
        }
    };
    let live_workspace_ids: HashSet<&str> = workspaces
        .iter()
        .map(|workspace| workspace.workspace_id.as_str())
        .collect();
    let live_tab_ids: HashSet<&str> = live_tabs.iter().map(|tab| tab.tab_id.as_str()).collect();
    let mut guard = topology_cache.lock().await;
    let (channels, active_threads) = if let Some((channels, active_threads)) = guard.as_mut() {
        (channels, active_threads)
    } else {
        let fetched = match fetch_topology_lists(client.as_ref(), *guild).await {
            Ok(fetched) => fetched,
            Err(error) => {
                bridge_eprintln!("herdr startup topology error: {error}");
                return;
            }
        };
        reconcile_topology_cache(&mut guard, fetched)
    };
    let result = delete_topology_absent_from_herdr(
        client.as_ref(),
        channels,
        active_threads,
        &live_workspace_ids,
        &live_tab_ids,
    )
    .await;
    drop(guard);
    if let Err(error) = result {
        bridge_eprintln!("herdr startup topology reconciliation error: {error}");
    }
}

/// Applies one `tab.closed`/`workspace.closed` Herdr event to Discord from a fresh topology fetch,
/// deleting only the tab or workspace the fetched lists actually contain. A tab closure with no
/// match in the active list and no match in its workspace channel's archived listing makes no
/// further Discord request. The delete error is logged; only a failure of the fetch itself is
/// returned.
async fn delete_closed_topology(
    discord: &DiscordConnection,
    closure: &TopologyClosure,
) -> Result<(), String> {
    let (client, guild, _, responder, _) = discord;
    let topology_cache = responder.topology_cache();
    let fetched = fetch_topology_lists(client.as_ref(), *guild).await?;
    let mut guard = topology_cache.lock().await;
    let (channels, active_threads) = reconcile_topology_cache(&mut guard, fetched);
    let result = match closure {
        TopologyClosure::Tab {
            workspace_id,
            tab_id,
        } => {
            let mut archived_cache: HashMap<
                Id<ChannelMarker>,
                Vec<twilight_model::channel::Channel>,
            > = HashMap::new();
            delete_tab_thread(
                client.as_ref(),
                channels,
                active_threads,
                &mut archived_cache,
                workspace_id,
                tab_id,
            )
            .await
        }
        TopologyClosure::Workspace { workspace_id } => {
            delete_workspace_channel(client.as_ref(), channels, workspace_id).await
        }
    };
    if let Err(error) = result {
        bridge_eprintln!("herdr topology closure error: {error}");
    }
    Ok(())
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

/// Removes every terminal-keyed entry for a terminal absent from `current_terminals`, except
/// `state_change_sequences`, which keeps counting so a returning terminal never reuses a card
/// nonce. Also removes every tab-keyed entry (`rename_errors_reported`) for a tab absent from
/// `current_tabs`, and every pane-keyed entry (`activity_messages`, `activity_eligible_panes`)
/// for a pane absent from `current_panes`, returning the informational cards that departed so
/// callers can expire them.
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
        .rename_errors_reported
        .retain(|tab_id| current_tabs.contains(tab_id));
    state
        .activity_messages
        .retain(|pane_id, _| current_panes.contains(pane_id));
    state
        .activity_eligible_panes
        .retain(|pane_id| current_panes.contains(pane_id));
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
    let environment: Vec<(&str, String)> = [
        ENV_DISCORD_TOKEN,
        ENV_DISCORD_GUILD_ID,
        ENV_DISCORD_OWNER_ID,
    ]
    .into_iter()
    .filter_map(|name| std::env::var(name).ok().map(|value| (name, value)))
    .collect();
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
    let responder = Arc::new(PermissionResponder::new(
        Arc::clone(&client),
        guild,
        config.owner_id.clone(),
        topology_cache,
    ));
    let gateway = tokio::spawn(drive_gateway_with_components(
        config.token,
        None,
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
    let question_error = if matches!(explicit_vendor, None | Some(PermissionVendor::Claude)) {
        match decode_claude_ask_question(&input) {
            Ok(question) => return run_question_hook(&question, requested_socket).await,
            Err(error) => Some(error),
        }
    } else {
        None
    };
    let interaction = match decode_hook_request(&input, explicit_vendor) {
        Ok(interaction) => interaction,
        Err(error) => {
            if let Some(question_error) = question_error {
                bridge_eprintln!("hook question decode error: {question_error}");
            }
            bridge_eprintln!("hook payload decode error: {error}");
            if matches!(explicit_vendor, Some(PermissionVendor::Cursor)) {
                write_hook_decision(PermissionVendor::Cursor, None).await?;
            }
            return Ok(());
        }
    };
    // Cursor fires beforeShellExecution even under --force/--yolo and its payload carries no
    // run-mode field, so a card that times out would deny every command of a hands-off seat.
    // The cursor-agent process's own flags are the only signal; anything unreadable falls
    // through to the card.
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

/// Answers one decoded `AskUserQuestion` request over the broker socket and prints Claude's
/// `updatedInput` hook response, always exiting 0: with no socket configured, no broker reachable,
/// or no owner answer before the deadline, this prints nothing so Claude's own dialog appears,
/// matching the activity hook's best-effort contract rather than the permission hook's fail-loud
/// one (a question has no safe deny to fall back to).
async fn run_question_hook(
    question: &herdr_connect_rs::QuestionInteraction,
    requested_socket: Option<std::path::PathBuf>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(socket_path) = requested_socket
        .or_else(|| std::env::var_os("HERDR_CLAUDE_BROKER_SOCKET").map(std::path::PathBuf::from))
    else {
        return Ok(());
    };
    let Some(answers) =
        request_question_answers(question, &socket_path, question_hook_timeout()).await
    else {
        return Ok(());
    };
    let output = encode_claude_question_decision(&question.raw_tool_input, &answers)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut stdout = tokio::io::stdout();
    stdout.write_all(&output).await?;
    stdout.flush().await?;
    Ok(())
}

/// Returns the argv of the nearest `cursor-agent` ancestor of this hook process, walking the
/// `/proc` parent chain from the parent upward. Off Linux, at pid 1 or 0, on any `/proc` read
/// error, or with no such ancestor this returns `None` so the caller keeps the card flow.
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
        PermissionVendor::Claude => encode_claude_decision(decision)?,
        PermissionVendor::Codex => encode_codex_decision(decision)?,
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

/// Reads one harness `PreToolUse` hook payload from stdin and forwards it to the broker as an
/// activity frame, always exiting 0 with no output: activity display is best-effort and must
/// never fail the tool call it rides on. A hook run outside a Herdr pane has no workspace, tab or
/// pane id to name, so it sends nothing.
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
    let (Ok(workspace_id), Ok(tab_id), Ok(pane_id)) = (
        std::env::var("HERDR_WORKSPACE_ID"),
        std::env::var("HERDR_TAB_ID"),
        std::env::var("HERDR_PANE_ID"),
    ) else {
        return Ok(());
    };
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
    let responder = Arc::new(PermissionResponder::new(
        Arc::clone(&client),
        guild,
        config.owner_id.clone(),
        topology_cache,
    ));
    // The standalone `broker` subcommand has no bridge event loop to forward activity frames to:
    // dropping the receiver immediately makes every subsequent send a no-op.
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

/// A Herdr `tab.closed`/`workspace.closed` event, naming the Discord topology it deletes.
#[derive(Debug, PartialEq, Eq)]
enum TopologyClosure {
    Tab {
        workspace_id: String,
        tab_id: String,
    },
    Workspace {
        workspace_id: String,
    },
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

/// Applies one lifecycle event: its membership change first (a status resubscribe if a pane joined
/// or left), then its closure if it is one (deleting that tab or workspace), then one doorbell.
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
        && let Err(error) = delete_closed_topology(discord, &closure).await
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

/// Registers every workspace channel's archived tab threads in a task of its own, independent of
/// Herdr and of the startup sweep, so an owner deletion of an archived thread whose tab has no
/// session still resolves. A failure is logged loudly.
fn spawn_archived_thread_registration(discord: &DiscordConnection) {
    let (client, guild, ..) = discord;
    let (client, guild) = (client.clone(), *guild);
    tokio::spawn(async move {
        if let Err(error) = register_archived_tab_threads(client.as_ref(), guild).await {
            bridge_eprintln!(
                "herdr archived thread registration FAILED, owner deletion of an archived tab thread may be ignored: {error}"
            );
        }
    });
}

/// Spawns the startup topology sweep (one `list_agents`/`tab_list_result` snapshot, then
/// `sync_startup_topology`) beside the caller rather than blocking it. Runs once, at process
/// start, after the first doorbell, so it shares that pass's `rename_errors_reported` and logs
/// no rename failure twice.
fn spawn_startup_topology_sweep(
    discord: &DiscordConnection,
    rename_errors_reported: &mut HashSet<String>,
) {
    match list_agents().and_then(|agents| tab_list_result().map(|tabs| (agents, tabs))) {
        Ok((agents, mut tabs)) => {
            for error in name_unlabeled_tabs(&agents, &mut tabs, rename_errors_reported) {
                bridge_eprintln!("herdr startup topology error: {error}");
            }
            let discord = discord.clone();
            let startup_task =
                tokio::spawn(async move { sync_startup_topology(&discord, &agents, &tabs).await });
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

/// Resubscribes to per-pane status, rebuilding `pane_ids` from a fresh `list_agents` call before
/// every attempt so a pane closing between attempts can never leave the subscription frozen on a
/// stale membership list.
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

/// Fetches the owner's mirrored identity once at startup.
///
/// # Errors
///
/// Returns an error when `owner_id` is not numeric or the fetch itself fails. Terminal prompt
/// mirroring has no fallback identity to mirror under, so startup fails on this error rather than
/// running the rest of the process without it.
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
        BridgeRuntime, BridgeState, BrokerTask, Client, Follower, Membership, PermissionResponder,
        SessionPathError, TopologyClosure, TopologyRoute, agent_read_detection, apply_membership,
        capture_for_with_search_root, create_transition_messages, delete_closed_topology,
        fetch_startup_owner_identity, fetch_topology_lists, handle_lifecycle_select_result,
        handle_live_event, initial_terminal_prompt_position, lifecycle_closure,
        lifecycle_membership, list_agents, live_log_path, maybe_establish_terminal_prompt_baseline,
        next_state_change_sequence, process_snapshot, prune_departed_state,
        read_new_terminal_prompts, resolve_session_path, route_topology,
        subscribe_status_with_backoff, sync_route, sync_startup_topology, tab_list_result,
        terminal_prompt_baseline_is_current, unique_existing_path,
    };
    use herdr_connect_rs::{
        AgentSession, AgentSnapshot, STATUS_DONE, STATUS_IDLE, STATUS_WORKING, Transition,
        VENDOR_CLAUDE, VENDOR_CODEX, VENDOR_CURSOR, lifecycle_subscriptions, name_unlabeled_tabs,
        read_claude_incremental, read_codex_incremental, read_cursor_incremental,
        status_subscriptions, submit_owner_prompt, subscribe_herdr_events, transition_card_nonce,
        workspace_list_result,
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

            // Codex writes every typed prompt twice: a `response_item` user-message record and an
            // `event_msg`/`user_message` twin that follows it. Appending both and finding exactly
            // one prompt proves the reader counts it once, sourced from the `response_item` copy
            // alone, and ignores its later `event_msg` twin entirely.
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

    /// Repro case b (a fresh session's pending marker must not leak onto a later, unrelated,
    /// already-populated session) and control case c (the same existing session, discovered
    /// directly, with no intervening fresh session) side by side: both must baseline identically,
    /// discarding the existing history rather than replaying it.
    #[test]
    fn resumed_session_with_existing_log_is_never_replayed_after_a_fresh_session_was_pending() {
        let original_home = std::env::var_os("HOME");
        let temp_home = std::env::temp_dir().join(format!(
            "herdr-connect-rs-resume-replay-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&temp_home);
        fs::create_dir_all(&temp_home).expect("create temp HOME");
        // SAFETY: `cargo test -- --test-threads=1` (this crate's mandated invocation) serializes
        // every test in this binary, so no other test observes HOME mid-mutation.
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

            // The existing, already-resumable session: prior history already on disk.
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

            // Case b: a fresh session with no log yet is seen first (marks the terminal
            // "awaiting"), then the same terminal switches to the existing, already-populated
            // session before the fresh one's log ever appears.
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

            // Control case c: the same existing session, discovered directly, with no intervening
            // fresh session -- must baseline identically to case b.
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

        // Captured from the live Herdr socket: closing a tab that is not a workspace's last tab.
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

        // Captured from the live Herdr socket: closing a workspace with a tab still open in it.
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

    /// Builds an `AgentSnapshot`/`AgentSession` pair for a Claude session at `cwd`, for use in
    /// [`ClaudeSearchRootCase::build`] closures.
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

    /// The project directory a session log for `cwd` resolves under beneath `root/vendor_root`,
    /// mirroring `resolve_session_path`'s slug so a case can place a fixture where production
    /// code will read it.
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

    /// One synthetic `$HOME` layout in
    /// [`claude_search_roots_cover_every_claude_config_directory_under_home`].
    struct ClaudeSearchRootCase {
        name: &'static str,
        /// Builds the layout under a fresh temp `$HOME` and returns the session to resolve and,
        /// when a log should be found there, the path it must resolve to.
        build: fn(&Path) -> (AgentSnapshot, AgentSession, Option<PathBuf>),
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
                    (snapshot, session, Some(expected_path))
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
                    (snapshot, session, Some(expected_path))
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
                    (snapshot, session, Some(primary_path))
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
            let (snapshot, session, expected) = (case.build)(&root);
            let result = resolve_session_path(&root, &snapshot, &session);
            match expected {
                Some(expected_path) => {
                    assert_eq!(result, Ok(expected_path), "{}", case.name);
                }
                None => {
                    assert!(
                        matches!(result, Err(SessionPathError::NotFoundYet(_))),
                        "{}: expected NotFoundYet, got {result:?}",
                        case.name
                    );
                }
            }
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

    /// One case in [`unique_existing_path_collapses_hard_linked_candidates`].
    enum UniqueExistingPathExpectation {
        /// Resolves to the candidate at this index.
        Ok(usize),
        /// A real, non-transient ambiguity: candidates that are not the same file.
        Permanent,
    }

    struct UniqueExistingPathCase {
        name: &'static str,
        build: fn(&Path) -> Vec<PathBuf>,
        expected: UniqueExistingPathExpectation,
    }

    /// Session logs hard-linked under both `~/.claude` and `~/.claude-one` (real, observed
    /// on-disk shape) must resolve as the one file they are, not as two ambiguous candidates.
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

    /// The real guild every real-guild test runs against, or `None` when its environment is not
    /// configured. When `HERDR_CLAUDE_BROKER_SOCKET` is set, panics if a production bridge is
    /// listening on that socket, because a second bridge on the same guild and Herdr session can
    /// satisfy a test's assertions in place of the code under test.
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
        if let Some(socket) = std::env::var_os("HERDR_CLAUDE_BROKER_SOCKET") {
            assert!(
                std::os::unix::net::UnixStream::connect(&socket).is_err(),
                "production bridge is listening on {}; stop it before running the suite",
                Path::new(&socket).display()
            );
        }
        Some(guild)
    }

    #[cfg(unix)]
    fn is_test_channel(channel: &twilight_model::channel::Channel) -> bool {
        channel
            .name
            .as_deref()
            .is_some_and(|name| name.starts_with("testrun-"))
    }

    /// Every `testrun-` thread in the guild, active or archived, whichever channel parents it. Suite
    /// threads outlive their tests when they hang off a channel the prefix filter does not delete.
    ///
    /// Archived threads are listed only under channels the bridge marks as a herdr workspace, the
    /// only channels it ever creates a thread in. This is the delete pass, so it pays for the full
    /// reach.
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

    /// The `testrun-` threads a leftover recount has to see: the guild-wide active list only.
    ///
    /// The recount deliberately skips the per-channel archived listings the delete pass runs. A
    /// thread the delete pass just deleted cannot come back as an archived thread, and a thread the
    /// suite leaked is active, because the suite never archives one. So an archived listing here
    /// could only repeat what the active list already shows.
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

    /// Deletes one thread, treating an already-deleted thread as done.
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

    /// Environment variables passed through to every real-agent test tab and workspace when set
    /// in the caller's own environment, so an agent started in it authenticates with the same
    /// account as the test process rather than falling back to its harness's default config
    /// directory: `CLAUDE_CONFIG_DIR` for Claude, `CODEX_HOME` for Codex.
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

    /// Creates a testrun tab, passing the caller's own [`PASSTHROUGH_ENV_VARS`] through to it.
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

    /// Fixed, owner-pre-trusted cwd for every real-Claude fixture: a fresh directory would trip
    /// Claude Code's own first-run "trust this folder?" prompt under a personal `CLAUDE_CONFIG_DIR`,
    /// which blocks the pane from ever reaching ready.
    #[cfg(unix)]
    fn claude_testrun_dir(home: &Path) -> PathBuf {
        home.join(".cache/herdr-connect-testrun/claude")
    }

    /// Fixed, owner-pre-trusted cwd for every real-Cursor fixture, for the same reason as
    /// [`claude_testrun_dir`]: the Cursor CLI's own one-time "Trust this workspace" prompt has no
    /// recovery either.
    #[cfg(unix)]
    fn cursor_testrun_dir(home: &Path) -> PathBuf {
        home.join(".cache/herdr-connect-testrun/cursor")
    }

    /// Fixed, owner-pre-trusted cwd for every real-Codex fixture, for the same reason as
    /// [`claude_testrun_dir`]: the Codex CLI's own one-time trust prompt has no recovery either.
    #[cfg(unix)]
    fn codex_testrun_dir(home: &Path) -> PathBuf {
        home.join(".cache/herdr-connect-testrun/codex")
    }

    /// Empties `directory` without removing it: a fixture's shared, owner-pre-trusted cwd must
    /// always exist at the same path.
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

    /// Creates a workspace and renames its root tab to the same label, so both the workspace and
    /// its root tab are visible to a zero-leftover check by that one label. Passes the caller's
    /// own [`PASSTHROUGH_ENV_VARS`] through, for the same reason as [`create_tab`].
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
        let responder = Arc::new(PermissionResponder::new(
            Arc::clone(&guild.client),
            guild.id,
            owner_id.clone(),
            topology_cache,
        ));
        // Tests that mirror terminal prompts replace this with the owner's fetched identity.
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

    /// Records the current status of every pane in the real Herdr session except `own_terminals`
    /// as already seen, so the doorbell's fresh-session topology sync returns early for the
    /// owner's own panes instead of creating or unarchiving their Discord channels and threads.
    #[cfg(unix)]
    fn seed_previous_for_other_terminals(
        state: &mut BridgeState,
        own_terminals: &[&str],
    ) -> Result<(), String> {
        for agent in list_agents()? {
            if !own_terminals.contains(&agent.terminal_id.as_str()) {
                state.previous.insert(agent.terminal_id, agent.agent_status);
            }
        }
        Ok(())
    }

    /// Feeds a real `pane_created` event through the lifecycle handler. The snapshot doorbell that
    /// follows the event also subscribes to status when the pane list changed, so no outcome tells
    /// the membership resubscribe apart from the doorbell's own subscribe: the row pins the
    /// subscription as an outcome of the whole handler, and the recorded snapshot as the
    /// doorbell's own effect.
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
            seed_previous_for_other_terminals(&mut runtime.state, &[&terminal_id])?;
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

    /// `sleep 12`: an instant tool step lets `haiku` finish before `working` is ever confirmed.
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

    /// Testrun tab cwd, never the repository directory (holds `.env`): the fixed, owner-trusted
    /// `.../herdr-connect-testrun/{claude,cursor,codex}` — its trust prompt has no recovery.
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

    /// The bool is whether the message carries an embed (a card, never live text).
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

    /// Polls until `done` accepts a snapshot or `bound` elapses, tolerating a transient
    /// `agent.list` deserialization failure within `bound` rather than propagating it at once.
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

    /// `process_snapshot` with the two slice args always equal to a single snapshot.
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

    /// Drives one real agent idle -> working -> settled: asserts `working` was observed, `alpha`
    /// was live before settle, the live message count matches the log, and no card was posted.
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
        // The persistent watch has no settle-time read of its own, so read once more here to
        // deliver any assistant text written just before `done` that no `notify` tick reached.
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

    /// Asserts the thread carries exactly `expected_count` live (non-embed) messages and no card
    /// (embed) message at all: with the end card removed, a settled turn posts only live text.
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

    /// A real-schema Claude assistant record, mirroring the shape in
    /// `tests/fixtures/claude-session.jsonl`, appended to a live session log so the follower reads
    /// an assistant text the running agent itself did not write.
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

    /// Rule 2 past settle: drives one real Claude turn to `done`/`idle`, then appends a fresh
    /// assistant record to the pane's session log while it stays settled. The still-open watcher
    /// posts that text -- status plays no part -- and the turn's own live texts are not reposted.
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
        // One more read so any text written just before `done` is delivered before the baseline
        // count is taken, isolating the post-settle append as the only new message.
        handle_live_event(&connection, &terminal, &mut state).await;
        let live_before = thread_messages(guild, thread)
            .await?
            .into_iter()
            .filter(|(_, embed, _)| !embed)
            .count();

        let Some(session) = settled.session.clone() else {
            return Err("settled snapshot lost its session".to_owned());
        };
        let log_path = live_log_path(&settled, &session)?.ok_or("no log path yet")?;
        let marker = format!("post-settle-{agent_name}");
        append_claude_assistant_record(&log_path, &marker)?;
        // The pane is settled; rule 2 still posts the appended text through the open watcher.
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

    /// Which path a terminal-prompt-turn helper submits its prompt through: [`Terminal`] mimics
    /// the owner typing directly in the Herdr pane (via `herdr agent prompt`, terminal-origin from
    /// the bridge's point of view); [`Discord`] mimics the bridge's own owner-message path
    /// (`submit_owner_prompt`, exactly what the Discord gateway handler calls).
    ///
    /// [`Terminal`]: TerminalPromptSubmission::Terminal
    /// [`Discord`]: TerminalPromptSubmission::Discord
    #[cfg(unix)]
    enum TerminalPromptSubmission {
        Terminal,
        Discord,
    }

    /// Everything one [`run_terminal_prompt_turn`] call needs about the fixed pane under test,
    /// bundled to stay under the argument-count lint; only `submission` and `prompt` change between
    /// the turns a single exercise drives.
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

    /// Cursor's `SQLite` write can commit well after Herdr itself reports `done`; waits for the
    /// reply to actually be readable before the final live read, so live delivery does not race
    /// the write and miss the reply.
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

    /// Drives one real turn to settle, submitting `prompt` through `submission`, then leaves
    /// `state` ready for the caller to inspect: the turn's own live-capture watch has run at least
    /// once more after settling, so any terminal prompt or assistant text it produced has already
    /// been mirrored or delivered.
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
        // The persistent watch has no settle-time read of its own, so read once more here to
        // deliver the reply written just before `done` that no `notify` tick reached.
        handle_live_event(connection, terminal, state).await;
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

    /// Asserts the thread carries exactly one webhook-authored message with `prompt`'s content,
    /// under `owner_display_name`, positioned before the plain (non-webhook) message carrying the
    /// assistant's `reply`.
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

    /// Asserts no webhook-authored message carries `prompt`'s content: a prompt submitted through
    /// the bridge's own Discord path must not be mirrored back into the thread it came from.
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

    /// Drives one real pane through three turns for `vendor`: a first turn -- the new session's
    /// very first prompt, typed before its log even exists -- whose prompt is mirrored ahead of its
    /// assistant reply exactly like the second, ordinary terminal-origin turn's is, and a third turn
    /// submitted through the bridge's own Discord path (`submit_owner_prompt`) whose prompt must not
    /// be mirrored back.
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
        // `mirror_terminal_prompts` mirrors under the connection's identity, which only the real
        // startup path (`run_bridge`) fetches; this harness builds its own connection and must set
        // it too.
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
        if vendor == VENDOR_CODEX {
            // Codex reports its session only once the pane is already `working`, by which point its
            // log already holds this first prompt. Per rule 1 a prompt already in the log at the
            // bridge's first sight of the session is never replayed, so this first prompt is not
            // mirrored; the baseline this turn establishes lets the next terminal prompt mirror.
            assert_prompt_was_not_mirrored(&first_messages, &first_prompt)?;
        } else {
            assert_terminal_prompt_mirrored_before_reply(
                &first_messages,
                &first_prompt,
                &first_reply,
                &identity.display_name,
            )?;
        }

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
        Ok(())
    }

    /// Real-service coverage shared by every vendor's terminal-prompt exercise: fixture creation,
    /// the exercise itself, and the same zero-leftover cleanup regardless of outcome.
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

    /// `echo`: an instant tool step, so the turn settles almost immediately after the one activity
    /// frame it produces.
    #[cfg(unix)]
    const ACTIVITY_FORCE_PROMPT: &str =
        "Run the shell command `echo activity-check`. Then say the word done.";

    /// Testrun tab cwd fixture for the activity hook exercise, mirroring `live_capture_tab_fixture`
    /// under its own label so the two tests' zero-leftover checks never collide.
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

    /// The freshly built `herdr-connect-rs` binary, resolved at runtime: `CARGO_BIN_EXE_*` is not
    /// defined at compile time for a bin target's own test harness (only for a separate target
    /// that depends on it, such as an integration test under `tests/`), so this walks up from the
    /// running test binary's own path (`target/<profile>/deps/<test-binary>`) to its sibling bin
    /// artifact (`target/<profile>/herdr-connect-rs`) instead.
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

    /// Writes a Claude `--settings` file registering the `PreToolUse` activity hook against
    /// `broker_socket`, per [examples/claude-hooks.json](../examples/claude-hooks.json)'s shape.
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

    /// Mirrors `start_claude_haiku_agent`, additionally passing `--settings <settings_path>` so
    /// the spawned Claude process registers the activity hook under test.
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

    /// Submits `prompt`, drives the pane through `working` (marking it activity-eligible via
    /// `own`, the same as a real doorbell would), drains activity frames into `state` until the
    /// pane settles, then marks it settled via `own` (forgetting the turn's activity message and
    /// revoking eligibility, exactly as `process_snapshot` does in production). Returns the
    /// settled snapshot.
    #[cfg(unix)]
    async fn drive_one_activity_turn(
        pane_id: &str,
        tabs: &[herdr_connect_rs::HerdrTab],
        sub: &mut herdr_connect_rs::HerdrSubscription,
        connection: &super::DiscordConnection,
        state: &mut BridgeState,
        activity_rx: &mut tokio::sync::mpsc::UnboundedReceiver<herdr_connect_rs::ActivityFrame>,
        prompt: &str,
    ) -> Result<AgentSnapshot, String> {
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

    /// The thread's plain `⚙️`-prefixed messages, in post order.
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

    /// Row 1: asserts `messages` (the thread right after turn one settles) holds exactly one
    /// activity message naming `Bash` and no card (embed) at all -- a turn that does not block posts
    /// no card. The exercise wires no live capture, so the turn's own reply text is not in the
    /// thread; that live-text path is covered by the live-capture rows. Returns the message's id.
    #[cfg(unix)]
    fn assert_first_turn_activity(
        messages: &[(String, bool, Id<MessageMarker>)],
    ) -> Result<Id<MessageMarker>, String> {
        if let Some((content, _, _)) = messages.iter().find(|(_, embed, _)| *embed) {
            return Err(format!("a card was posted for turn one: {content:?}"));
        }
        let rows = activity_message_rows(messages);
        let [(text, _, activity_id)] = rows.as_slice() else {
            return Err(format!(
                "expected exactly one activity message after turn one, thread has {messages:?}"
            ));
        };
        if !text.contains("Bash") {
            return Err(format!("activity message did not name Bash: {text}"));
        }
        Ok(*activity_id)
    }

    /// Row 2 continued: asserts `messages` (the thread after turn two settles) holds turn one's
    /// unchanged activity message plus a second, distinct one that starts its own count at 1 and
    /// names `Bash` -- not an edit continuing turn one's count, and not the dropped late frame.
    #[cfg(unix)]
    fn assert_second_turn_activity(
        messages: &[(String, bool, Id<MessageMarker>)],
        first_activity_id: Id<MessageMarker>,
    ) -> Result<(), String> {
        let rows = activity_message_rows(messages);
        let [first_row, second_row] = rows.as_slice() else {
            return Err(format!(
                "expected exactly two activity messages after turn two, thread has {messages:?}"
            ));
        };
        if first_row.2 != first_activity_id {
            return Err(format!(
                "turn one's activity message changed identity: {first_row:?}"
            ));
        }
        if !second_row.0.starts_with("⚙️ 1 ·") {
            return Err(format!(
                "turn two's activity message did not start a fresh count: {}",
                second_row.0
            ));
        }
        if !second_row.0.contains("Bash") {
            return Err(format!(
                "turn two's activity message did not name Bash: {}",
                second_row.0
            ));
        }
        Ok(())
    }

    /// Drives one real `claude --model haiku` agent with the activity hook registered against a
    /// real bridge broker (bound at `broker_socket`) through two turns, exercising every row of
    /// the turn-boundary table in one continuous scenario: turn one posts one activity frame and no
    /// card; a frame injected on the broker socket after the turn settles is dropped (no new
    /// message, no edit of the settled one); turn two's first frame starts its own fresh message
    /// rather than continuing turn one's count.
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

        // The activity hook only connects once Claude registers it at agent start, so the
        // snapshot -- and everything routed from it -- is only meaningful once the agent exists.
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

        // Row 1: a frame during the turn posts as an activity message, with no card.
        drive_one_activity_turn(
            &tab.pane_id,
            tabs,
            &mut sub,
            &connection,
            &mut state,
            &mut activity_rx,
            ACTIVITY_FORCE_PROMPT,
        )
        .await?;
        let after_first_turn = thread_messages(guild, thread).await?;
        let first_activity_id = assert_first_turn_activity(&after_first_turn)?;

        // Row 2: a frame injected after the turn settled is dropped, not edited or recreated.
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
        herdr_connect_rs::send_activity_frame(&late_frame, broker_socket, Duration::from_secs(1))
            .await;
        let received = tokio::time::timeout(Duration::from_secs(2), activity_rx.recv())
            .await
            .map_err(|_| "late synthetic frame was not forwarded by the broker".to_owned())?
            .ok_or_else(|| "activity channel closed before the late frame arrived".to_owned())?;
        super::handle_activity_event(&connection, received, &mut state).await;
        let after_late_frame = thread_messages(guild, thread).await?;
        if after_late_frame != after_first_turn {
            return Err(format!(
                "late frame changed the thread: before {after_first_turn:?}, after {after_late_frame:?}"
            ));
        }

        // Row 2 continued: the next turn starts its own message with count 1, not a continuation
        // of the dropped late frame or turn one's count.
        drive_one_activity_turn(
            &tab.pane_id,
            tabs,
            &mut sub,
            &connection,
            &mut state,
            &mut activity_rx,
            ACTIVITY_FORCE_PROMPT,
        )
        .await?;
        broker_task.abort();
        let _ = std::fs::remove_file(broker_socket);

        let after_second_turn = thread_messages(guild, thread).await?;
        assert_second_turn_activity(&after_second_turn, first_activity_id)
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

    /// Instructs a real `claude --model haiku` agent to call `AskUserQuestion` with exactly the
    /// shape the exercise below expects to resolve.
    #[cfg(unix)]
    const QUESTION_FORCE_PROMPT: &str = "Use the AskUserQuestion tool right now. Ask exactly one \
        question with header \"Color\", question text \"Which color?\", and exactly two options: \
        label \"Red\" with description \"The color red\", and label \"Blue\" with description \
        \"The color blue\". multiSelect must be false. Do not do anything else and do not say \
        anything else.";

    /// Testrun tab cwd fixture for the question hook exercise, mirroring `activity_tab_fixture`
    /// under its own label so the two tests' zero-leftover checks never collide.
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

    /// Writes a Claude `--settings` file registering the `PreToolUse` `AskUserQuestion` hook
    /// against `broker_socket`, per [examples/claude-hooks.json](../examples/claude-hooks.json)'s
    /// shape.
    #[cfg(unix)]
    fn write_question_settings(broker_socket: &Path) -> Result<PathBuf, String> {
        let binary = activity_binary_path()?;
        let binary = binary
            .to_str()
            .ok_or_else(|| "built binary path is valid UTF-8".to_owned())?;
        let socket_arg = broker_socket
            .to_str()
            .ok_or_else(|| "broker socket path is valid UTF-8".to_owned())?;
        let command = format!("{binary} hook --vendor claude --socket {socket_arg}");
        let settings = json!({
            "hooks": {
                "PreToolUse": [
                    {
                        "matcher": "AskUserQuestion",
                        "hooks": [
                            {"type": "command", "command": command, "timeout": herdr_connect_rs::question_hook_timeout().as_secs()}
                        ]
                    }
                ]
            }
        });
        let path = std::env::temp_dir().join(format!(
            "herdr-connect-rs-question-settings-{}-{}.json",
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

    /// Builds a `MessageComponent` interaction JSON has no way to synthesize from a real Discord
    /// client (there is no bot API that simulates a human clicking a button), so this constructs
    /// the minimal wire shape [`handle_component`] actually reads: guild, a real channel (fetched
    /// live so its shape is never guessed), the owner as the invoking user, and the tapped
    /// component's `custom_id` plus any select-menu `values`. Every downstream effect this drives
    /// -- the registry resolution, the real Discord card edit, the attempted (and discarded)
    /// interaction response -- is real; only this upstream "a human tapped a button" event is
    /// synthetic, the same gap the project's own permission-card click path has not yet closed
    /// (see ROADMAP.md's Cursor permission viability item).
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

    /// Drives one real `claude --model haiku` agent with the `AskUserQuestion` hook registered
    /// against a real bridge broker through a forced single-select question, resolves it the way an
    /// owner's Discord button tap would (see [`synthetic_component_interaction`]), and returns the
    /// pane's settled status and every message the exercise left in its tab thread, alongside what
    /// Claude's own session transcript recorded for the question's answer.
    #[cfg(unix)]
    async fn question_hook_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        agent_name: &str,
        broker_socket: &Path,
        settings_path: &Path,
    ) -> Result<
        (
            String,
            Vec<(String, bool, Id<MessageMarker>)>,
            Option<String>,
        ),
        String,
    > {
        let shared_cache: herdr_connect_rs::TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let connection = discord_tuple_with_cache(guild, Arc::clone(&shared_cache));
        let responder = Arc::clone(&connection.3);
        let (activity_tx, activity_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(activity_rx);
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
        let route = route_topology(agents, tabs, &terminal)?;
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &shared_cache).await?;

        submit_owner_prompt(&tab.pane_id, QUESTION_FORCE_PROMPT)?;

        let session_id = poll_snapshot(&tab.pane_id, Duration::from_secs(15), |snapshot| {
            snapshot.session.is_some()
        })?
        .session
        .ok_or("pane never reported a session while asking its question")?
        .value;

        let token = {
            let deadline = Instant::now() + Duration::from_secs(60);
            loop {
                if let Some(token) = connection.3.pending_question_token(&session_id) {
                    break token;
                }
                if Instant::now() >= deadline {
                    broker_task.abort();
                    return Err(
                        "AskUserQuestion hook never reached the broker with a pending card"
                            .to_owned(),
                    );
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        };

        let channel = guild
            .client
            .channel(thread)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        let interaction = synthetic_component_interaction(
            guild.id,
            &connection.2,
            &channel,
            &format!("herdrask:{token}:1"),
            &[],
        )?;
        super::handle_component(Arc::clone(&connection.3), interaction).await;

        let settled = poll_snapshot(&tab.pane_id, Duration::from_secs(45), |snapshot| {
            matches!(snapshot.agent_status.as_str(), "done" | "idle")
        })?;
        broker_task.abort();
        let _ = std::fs::remove_file(broker_socket);

        // The card's own edit runs detached from the resolution the hook waited on (see
        // `return_value_before_card_edit`), so give it a bounded moment to land before reading.
        let mut messages = thread_messages(guild, thread).await?;
        let edit_deadline = Instant::now() + Duration::from_secs(5);
        while !messages
            .iter()
            .any(|(content, _, _)| content.starts_with("resolved:"))
            && Instant::now() < edit_deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            messages = thread_messages(guild, thread).await?;
        }
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .map_err(|_| "HOME is set by the real Herdr pane environment".to_owned())?;
        let transcript_answer = transcript_question_answer(&home, &settled, "Which color?")?;
        Ok((settled.agent_status, messages, transcript_answer))
    }

    /// Reads the resolved pane's own real Claude session transcript and returns what it recorded
    /// for the `AskUserQuestion` `toolUseResult.answers[question]`, proving Claude itself received
    /// the answer -- not only that the broker's own card text (written independently, from its own
    /// resolution) says so.
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
    async fn run_question_hook_test() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
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
        let broker_socket = std::env::temp_dir().join(format!(
            "herdr-connect-rs-question-broker-{}-{}.sock",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        let settings_path = write_question_settings(&broker_socket)
            .expect("write question settings for the exercise");
        let created = question_tab_fixture();
        let (tab_id, cwd_dir, result) = match created {
            Ok((tab, cwd_dir)) => {
                let agent_name = format!(
                    "question-claude-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome = tokio::time::timeout(
                    Duration::from_secs(120),
                    question_hook_exercise(
                        &guild,
                        &tab,
                        &agent_name,
                        &broker_socket,
                        &settings_path,
                    ),
                )
                .await
                .unwrap_or_else(|_| Err("question hook exercise timed out".to_owned()));
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
        let (status, messages, transcript_answer) =
            result.unwrap_or_else(|error| panic!("{error}"));
        assert_ne!(
            status, "blocked",
            "the pane must never block on Claude's own dialog once the hook answers it"
        );
        assert!(
            messages
                .iter()
                .any(|(content, _, _)| content == "resolved: Blue"),
            "question card must read resolved: Blue, thread has {messages:?}"
        );
        assert_eq!(
            transcript_answer.as_deref(),
            Some("Blue"),
            "Claude's own session transcript must record toolUseResult.answers[\"Which color?\"] as \
             Blue, not just the broker's own card text"
        );
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn question_hook_answers_from_discord_without_blocking_the_pane() {
        run_question_hook_test().await;
    }

    /// Slack over `question_hook_timeout()` for the expiry row's poll for the blocked fall-back.
    #[cfg(unix)]
    const QUESTION_EXPIRY_POLL_MARGIN: Duration = Duration::from_secs(15);

    /// Mirrors [`question_hook_exercise`] but never resolves the card: the hook's own
    /// `QUESTION_TIMEOUT` window (thirty real seconds -- this test does not fake the clock, per the
    /// real-services law) must elapse before the card reads `expired: no owner answer` and the pane
    /// falls back to blocking on Claude's own dialog.
    #[cfg(unix)]
    async fn question_hook_expiry_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        agent_name: &str,
        broker_socket: &Path,
        settings_path: &Path,
    ) -> Result<(String, Vec<(String, bool, Id<MessageMarker>)>), String> {
        let shared_cache: herdr_connect_rs::TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
        let connection = discord_tuple_with_cache(guild, Arc::clone(&shared_cache));
        let responder = Arc::clone(&connection.3);
        let (activity_tx, activity_rx) = tokio::sync::mpsc::unbounded_channel();
        drop(activity_rx);
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
        let route = route_topology(agents, tabs, &terminal)?;
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &shared_cache).await?;

        submit_owner_prompt(&tab.pane_id, QUESTION_FORCE_PROMPT)?;

        let session_id = poll_snapshot(&tab.pane_id, Duration::from_secs(15), |snapshot| {
            snapshot.session.is_some()
        })?
        .session
        .ok_or("pane never reported a session while asking its question")?
        .value;

        let deadline = Instant::now() + Duration::from_secs(60);
        while connection.3.pending_question_token(&session_id).is_none() {
            if Instant::now() >= deadline {
                broker_task.abort();
                return Err(
                    "AskUserQuestion hook never reached the broker with a pending card".to_owned(),
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // No resolution: wait out the real QUESTION_TIMEOUT window. The hook's own timeout is that
        // window plus a 5 s margin, so the fall-back must land inside `question_hook_timeout()`
        // plus a poll margin, and not before the window itself has run (a 10 s allowance below the
        // hook timeout covers the margin and the poll granularity).
        let waiting_since = Instant::now();
        let status = poll_snapshot(
            &tab.pane_id,
            herdr_connect_rs::question_hook_timeout() + QUESTION_EXPIRY_POLL_MARGIN,
            |snapshot| matches!(snapshot.agent_status.as_str(), "blocked"),
        )?
        .agent_status;
        let waited = waiting_since.elapsed();
        let earliest = herdr_connect_rs::question_hook_timeout() - Duration::from_secs(10);
        if waited < earliest {
            broker_task.abort();
            return Err(format!(
                "pane fell back after {waited:?}, before the question window of about {earliest:?}"
            ));
        }
        broker_task.abort();
        let _ = std::fs::remove_file(broker_socket);

        // The card's own expiry edit runs detached from the status transition the hook's own
        // timeout races against (see `return_value_before_card_edit`), so give it the same bounded
        // moment to land the resolve row gives its own edit before reading.
        let mut messages = thread_messages(guild, thread).await?;
        let edit_deadline = Instant::now() + Duration::from_secs(5);
        while !messages
            .iter()
            .any(|(content, _, _)| content.starts_with("expired:"))
            && Instant::now() < edit_deadline
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
            messages = thread_messages(guild, thread).await?;
        }
        Ok((status, messages))
    }

    #[cfg(unix)]
    async fn run_question_hook_expiry_test() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let label = format!("{QUESTION_LABEL}-expiry-claude");
        assert_eq!(
            remaining_tabs(&label).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let broker_socket = std::env::temp_dir().join(format!(
            "herdr-connect-rs-question-expiry-broker-{}-{}.sock",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
            .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
        let settings_path = write_question_settings(&broker_socket)
            .expect("write question settings for the exercise");
        let cwd_dir = claude_testrun_dir(&home);
        let created = clear_directory_contents(&cwd_dir).and_then(|()| {
            let cwd = cwd_dir
                .to_str()
                .ok_or_else(|| "temp cwd is valid UTF-8".to_owned())?;
            create_tab(&label, &workspace_id, cwd)
        });
        let (tab_id, result) = match created {
            Ok(tab) => {
                let agent_name = format!(
                    "q-expiry-claude-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome = tokio::time::timeout(
                    herdr_connect_rs::question_hook_timeout()
                        + QUESTION_EXPIRY_POLL_MARGIN
                        + Duration::from_secs(60),
                    question_hook_expiry_exercise(
                        &guild,
                        &tab,
                        &agent_name,
                        &broker_socket,
                        &settings_path,
                    ),
                )
                .await
                .unwrap_or_else(|_| Err("question hook expiry exercise timed out".to_owned()));
                cleanup_real_claude_session_dir(&home, &tab.pane_id);
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = clear_directory_contents(&cwd_dir);
        let _ = std::fs::remove_file(&broker_socket);
        let _ = std::fs::remove_file(&settings_path);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left =
            remaining_tabs(&label).expect("tab.list succeeds for the zero-leftover check");
        let (status, messages) = result.unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(
            status, "blocked",
            "an expired question card must fall back to Claude's own dialog"
        );
        assert!(
            messages
                .iter()
                .any(|(content, _, _)| content == "expired: no owner answer"),
            "question card must read expired: no owner answer, thread has {messages:?}"
        );
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    /// Multi-threaded like [`startup_sweep_survives_a_concurrent_cache_clear`], for the same
    /// reason: this test's own `poll_snapshot` wait blocks its thread with `std::thread::sleep`
    /// for the real ~30s `QUESTION_TIMEOUT` window, and on a single-threaded runtime that starves
    /// the broker's own concurrently-awaited `request_one_question` task, delaying the card's
    /// `"expired: ..."` edit until after this test has already read the thread.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn question_hook_expires_and_falls_back_to_the_dialog() {
        run_question_hook_expiry_test().await;
    }

    /// The real Codex account's own broker socket, matching its already-installed
    /// `CODEX_HOME/hooks.json` (`PreToolUse` activity hook and `PermissionRequest` hook, both
    /// `--socket /tmp/herdr-claude-broker.sock`), per [examples/codex-hooks.json](../examples/codex-hooks.json)'s
    /// shape. Environment precondition, the same way [`codex_testrun_dir`] is: Codex has no
    /// `--settings` flag like Claude's to inject a hook per run, and a temporary `CODEX_HOME`
    /// (even one symlinking every file from the real one) never gets a Codex session reported by
    /// Herdr, so this exercise requires `CODEX_HOME` to already be set to the real account's own
    /// config directory and binds the hook's own fixed socket instead of a private one. Second
    /// precondition: the account's hooks must already be trusted -- after `hooks.json` changes,
    /// Codex shows "Hooks need review" and runs no hooks at all until a pane trusts them, so an
    /// untrusted `SessionStart` hook silently stops Herdr from ever reporting a session for the
    /// account.
    #[cfg(unix)]
    const CODEX_ACTIVITY_BROKER_SOCKET: &str = "/tmp/herdr-claude-broker.sock";

    /// Fails fast, naming exactly what is missing, when the Codex activity row's environment
    /// preconditions are unmet: `CODEX_HOME` is not set, its `hooks.json` lacks the activity hook
    /// on [`CODEX_ACTIVITY_BROKER_SOCKET`], or a hook entry it declares has no trust record in the
    /// shared `config.toml`'s `[hooks.state]`.
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

    /// Testrun tab cwd fixture for the Codex activity hook exercise, mirroring `activity_tab_fixture`
    /// under its own label so the two tests' zero-leftover checks never collide.
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

    /// Removes the real on-disk Codex session file a live pane's reported session resolves to, if
    /// any. Best-effort, mirroring `cleanup_real_claude_session_dir` for Codex's flat session-file
    /// layout (a session id is one file, not a project directory), the same way
    /// `live_capture_exercise`'s own Codex cleanup branch does.
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

    /// Codex counterpart to `activity_hook_exercise`: drives one real `codex --no-daemon --model gpt-5.6-luna`
    /// agent, started (via the already vendor-generic `start_live_capture_agent`) in a pane on the
    /// real Codex account, through the same turn-boundary table -- reusing `drive_one_activity_turn`,
    /// `assert_first_turn_activity`, and `assert_second_turn_activity` verbatim, since none of them
    /// are vendor-specific. `broker_socket` is [`CODEX_ACTIVITY_BROKER_SOCKET`], the account's own
    /// pre-installed hook target, not a private socket.
    /// Deletes the broker socket file on drop, so the Codex activity exercise removes it only
    /// once this test has confirmed it bound it -- never on an early failure before or during bind.
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

        // The activity hook only connects once Codex registers it at agent start, so the
        // snapshot -- and everything routed from it -- is only meaningful once the agent exists.
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

        // Row 1: a frame during the turn posts as an activity message, with no card.
        drive_one_activity_turn(
            &tab.pane_id,
            tabs,
            &mut sub,
            &connection,
            &mut state,
            &mut activity_rx,
            ACTIVITY_FORCE_PROMPT,
        )
        .await?;
        let after_first_turn = thread_messages(guild, thread).await?;
        let first_activity_id = assert_first_turn_activity(&after_first_turn)?;

        // Row 2: a frame injected after the turn settled is dropped, not edited or recreated.
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
        herdr_connect_rs::send_activity_frame(&late_frame, broker_socket, Duration::from_secs(1))
            .await;
        let received = tokio::time::timeout(Duration::from_secs(2), activity_rx.recv())
            .await
            .map_err(|_| "late synthetic frame was not forwarded by the broker".to_owned())?
            .ok_or_else(|| "activity channel closed before the late frame arrived".to_owned())?;
        super::handle_activity_event(&connection, received, &mut state).await;
        let after_late_frame = thread_messages(guild, thread).await?;
        if after_late_frame != after_first_turn {
            return Err(format!(
                "late frame changed the thread: before {after_first_turn:?}, after {after_late_frame:?}"
            ));
        }

        // Row 2 continued: the next turn starts its own message with count 1, not a continuation
        // of the dropped late frame or turn one's count.
        drive_one_activity_turn(
            &tab.pane_id,
            tabs,
            &mut sub,
            &connection,
            &mut state,
            &mut activity_rx,
            ACTIVITY_FORCE_PROMPT,
        )
        .await?;
        broker_task.abort();

        let after_second_turn = thread_messages(guild, thread).await?;
        assert_second_turn_activity(&after_second_turn, first_activity_id)
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

    /// Reports a real Claude session identity on a pane through Herdr's own
    /// `pane.report_agent_session` RPC.
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

    /// Generates a session id shaped like a real Claude session UUID, unique enough for a single
    /// test run.
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

    /// Reports idle then attaches a fresh stub session to a shell pane, for topology/sweep
    /// fixtures that need a session-carrying agent but never drive a real transition.
    #[cfg(unix)]
    fn report_idle_with_session(pane_id: &str) -> Result<(), String> {
        report_agent_state(pane_id, "idle")?;
        report_agent_session(pane_id, &generate_claude_session_id()?)
    }

    /// Starts a real `claude --model haiku` agent on a pane already at its interactive shell
    /// prompt, retrying `agent_pane_busy` while a freshly created pane's shell settles (mirrors
    /// `tests/prompt_stall_followup.rs`'s `start_agent`). Success means Herdr detected the real
    /// agent and its real Claude-hook-reported session. The pane must never have carried a
    /// synthetic `report-agent`/`report-agent-session` call: Herdr then treats it as occupied and
    /// rejects a real `agent start` with `agent_pane_busy` regardless of how long this retries.
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

    /// A short, lowercase, digit-only nonce for building a Herdr agent name: names must match
    /// `[a-z][a-z0-9_-]{0,31}`, so this is built from the clock, never from a pane or tab id
    /// (which may contain uppercase letters).
    #[cfg(unix)]
    fn agent_name_nonce() -> Result<String, String> {
        Ok(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_millis()
            .to_string())
    }

    /// Submits one prompt to a real running agent and waits for the resulting settled state,
    /// through Herdr's own `agent prompt --wait`, not the bridge's prompt-submission path.
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

    /// Removes the real on-disk Claude project directory a live pane's reported session resolves
    /// under, if any. Best-effort: called from cleanup after a real agent has written its own
    /// transcript, so nothing is left behind for `resolve_session_path` to find on a later run.
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
        sync_startup_topology(&connection, agents, tabs).await;

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
        sync_startup_topology(&connection, &agents, &tabs).await;
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

    /// Creates a tab with no `--label`, so Herdr auto-assigns a numeric one. The tab has no
    /// dedicated label to filter a zero-leftover check by; callers track the returned `tab_id`
    /// instead.
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

    /// Real-Herdr exercise for an unlabeled tab whose agent has reported a session: the bridge's
    /// first pass renames the tab to its generated name in Herdr and creates the thread under that
    /// same name; a second pass renames nothing.
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

    /// A tab's recorded rename failure is dropped once Herdr no longer lists the tab, and kept
    /// while it does.
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

        let startup = sync_startup_topology(&connection, agents, tabs);
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
            &discord_tuple_with_cache(guild, Arc::clone(&shared)),
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

    /// Whether no channel with `topic` exists, distinguishing a successful empty lookup from a
    /// transport failure: the latter propagates as `Err` instead of being read as absence.
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

    /// Whether a thread whose name ends with `suffix` still exists under `channel_id`, active or
    /// archived.
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

    /// Deletes every thread named `... [<tab_id>]` under the channel whose topic is `topic`, active
    /// or archived. A tab in the shared real workspace mirrors into a channel that does not start
    /// with `testrun-`, so the prefix-based cleanup never reaches its thread.
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

    /// Whether a tab's thread exists, and where, before [`closed_topology_exercise`] runs.
    #[cfg(unix)]
    enum ClosureBatchPresence {
        Active,
        Archived,
        Absent,
    }

    /// One row in [`closed_topology_exercise`]'s table: a tab's thread presence going in, whether
    /// its closure is applied, and whether the thread is expected to survive (`None` when no
    /// thread exists to check).
    #[cfg(unix)]
    struct ClosureBatchCase {
        name: &'static str,
        presence: ClosureBatchPresence,
        closed: bool,
        expect_survives: Option<bool>,
    }

    /// Table-driven, against a real Discord guild: applying each `tab.closed` closure on its own
    /// (as the lifecycle loop now does, one event at a time) deletes only the closed tabs whose
    /// threads the fetched active list or that channel's archived listing actually contains, and a
    /// live tab's thread whose closure is never applied survives untouched.
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
            delete_closed_topology(&connection, closure).await?;
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

    /// Drives a real `tab.closed` then a real `workspace.closed` event through the real lifecycle
    /// handler and asserts the Discord effects the owner's deletion rule requires.
    ///
    /// Closing a workspace's last tab also closes the workspace, so this exercise keeps a second
    /// tab alive through the tab-close step to observe tab close and workspace close as the two
    /// separately-observable Discord effects.
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
        sync_startup_topology(&connection, &agents, &tabs).await;
        let root_route = route_topology(&agents, &tabs, &root_agent.terminal_id)?;
        let second_route = route_topology(&agents, &tabs, &second_agent.terminal_id)?;

        let topic = format!("herdr workspace [{}]", root_route.workspace_id);
        let channel = guild_channel_with_topic(guild, &topic).await?;
        let root_suffix = format!(" [{}]", root_route.tab_id);
        let second_suffix = format!(" [{}]", second_route.tab_id);
        if !thread_with_suffix_survives(guild, channel.id, &second_suffix).await? {
            return Err("sync did not create the second tab's thread".to_owned());
        }

        let lifecycle = subscribe_herdr_events(&lifecycle_subscriptions())
            .await
            .map_err(|error| error.to_string())?;
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

        seed_previous_for_other_terminals(
            &mut runtime.state,
            &[&root_agent.terminal_id, &second_agent.terminal_id],
        )?;
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

    /// How long the real gateway needs to identify and receive guild events before a deletion is
    /// issued; the gateway exposes no readiness signal, and a delete sent earlier is never seen.
    #[cfg(unix)]
    const GATEWAY_READY_WAIT: Duration = Duration::from_secs(8);

    /// Starts the real Discord gateway with the production owner-deletion handling against
    /// `connection`, returning its task and the notices it logs.
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
            None,
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

    /// Waits until the gateway logs `dispatch`, the notice it sends for every delete event it
    /// handles, then keeps listening for `DELETION_ERROR_WINDOW` and returns the deletion
    /// dispatch notices and the error notices seen. Errs when `dispatch` never arrives, so a
    /// missing event is not mistaken for a suppressed one.
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

    /// Deletes a tab's Discord thread as the owner would and asserts Herdr closes that tab, the
    /// sibling tab survives, and a later topology sync recreates no thread for the closed tab.
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
        sync_startup_topology(&connection, &agents, &tabs).await;
        let second_route = route_topology(&agents, &tabs, &second_agent.terminal_id)?;
        let root_route = route_topology(&agents, &tabs, &root_agent.terminal_id)?;
        let topic = format!("herdr workspace [{}]", second_route.workspace_id);
        let channel = guild_channel_with_topic(guild, &topic).await?;
        let second_suffix = format!(" [{}]", second_route.tab_id);
        let root_suffix = format!(" [{}]", root_route.tab_id);
        let thread = active_threads_for_guild(guild)
            .await?
            .into_iter()
            .find(|thread| {
                thread.parent_id == Some(channel.id)
                    && thread
                        .name
                        .as_deref()
                        .is_some_and(|name| name.ends_with(&second_suffix))
            })
            .ok_or("sync did not create the second tab's thread")?;

        let mut lifecycle = subscribe_herdr_events(&lifecycle_subscriptions())
            .await
            .map_err(|error| error.to_string())?;
        let (gateway, notices) = start_owner_deletion_gateway(&connection).await?;
        // A delivery that met Unknown Channel clears the topology cache before the gateway
        // handler runs; the handler must still resolve the tab without any cache.
        *connection.3.topology_cache().lock().await = None;
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

    /// Registers an archived tab thread through the production archived registration alone, then
    /// deletes it as the owner would. The thread and its channel are created straight through the
    /// Discord API, so no sync registers them, and the Herdr tab must close and stay closed.
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
        if herdr_connect_rs::resolve_owner_deleted_tab(thread.id).as_deref()
            != Some(second_tab.tab_id.as_str())
        {
            return Err("the archived registration did not record the thread".to_owned());
        }

        let connection = discord_tuple(guild);
        let mut lifecycle = subscribe_herdr_events(&lifecycle_subscriptions())
            .await
            .map_err(|error| error.to_string())?;
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

    /// Deletes a workspace's Discord channel as the owner would and asserts Herdr closes that
    /// workspace and no channel is recreated for it.
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
        sync_startup_topology(&connection, &agents, &tabs).await;
        let route = route_topology(&agents, &tabs, &agent.terminal_id)?;
        let topic = format!("herdr workspace [{}]", route.workspace_id);
        let channel = guild_channel_with_topic(guild, &topic).await?;

        let mut lifecycle = subscribe_herdr_events(&lifecycle_subscriptions())
            .await
            .map_err(|error| error.to_string())?;
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

    /// How long the gateway is given to dispatch the delete event of a bridge deletion.
    #[cfg(unix)]
    const DELETION_DISPATCH_WAIT: Duration = Duration::from_secs(30);

    /// How long after the dispatch notice a handler that fails to recognise the deletion as the
    /// bridge's own is given to log its error.
    #[cfg(unix)]
    const DELETION_ERROR_WINDOW: Duration = Duration::from_secs(4);

    /// Deletes a testrun workspace channel, or one of its tab threads, through the production
    /// delete path while the gateway runs, and returns the delete-event dispatch notices and the
    /// errors the gateway logged. The ids are
    /// testrun ids, so a deletion wrongly treated as the owner's makes Herdr refuse to close an id
    /// it never had; no real tab or workspace can be closed.
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

    /// The bridge's own deletions of a tab thread and of a workspace channel come back through the
    /// gateway as delete events and must not be forwarded to Herdr as owner deletions.
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

    /// Creates an orphan channel/thread pair and an orphan thread under a live workspace channel,
    /// then runs the startup sweep and asserts it deletes exactly the topology Herdr no longer
    /// lists.
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
        sync_startup_topology(&connection, agents, tabs).await;

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
        let orphan_thread =
            create_guild_thread(guild, orphan_channel.id, "testrun-orphan [w9Z9:t1]").await?;
        let live_orphan_thread = create_guild_thread(
            guild,
            live_channel.id,
            &format!("testrun-orphan [{}:t9Z]", route.workspace_id),
        )
        .await?;
        // An owner-made thread whose bracket suffix is not this workspace's own tab id must
        // survive: it is not a bridge-owned tab thread.
        let bystander_thread =
            create_guild_thread(guild, live_channel.id, "testrun-notes [staging]").await?;

        sync_startup_topology(&connection, agents, tabs).await;

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
            .any(|thread| thread.id == orphan_thread.id)
        {
            return Err("startup sweep did not delete the orphan channel's thread".to_owned());
        }
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

    /// Creates an orphan channel/thread pair Herdr does not list, then runs the startup sweep's
    /// delete pass against a `topology_cache` starting at `None`. When `race_the_clear` is set, a
    /// second task hammers that same cache back to `None` on a separate worker thread for the
    /// sweep's whole run -- mimicking a concurrent stale-route recovery (`deliver_to_route`,
    /// `deliver_blocked_messages`, `handle_live_event`) clearing it mid-sweep. Either way the
    /// orphan must still be deleted: the reconciliation pass must not abort just because it
    /// observed an empty cache.
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
        let orphan_thread =
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

        sync_startup_topology(&connection, &[], &[]).await;

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
        let active_after = active_threads_for_guild(guild).await?;
        if active_after
            .iter()
            .any(|thread| thread.id == orphan_thread.id)
        {
            return Err("startup sweep did not delete the orphan channel's thread".to_owned());
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

    /// Drives one pane through a working -> settled round-trip via `process_snapshot`, returning
    /// the settled snapshot. Used by `drive_idle_working_done`'s second half.
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

    /// Drives one pane through a full idle -> working -> done round-trip via `process_snapshot`,
    /// returning its route and the settled snapshot. Shared by `session_less_pane_exercise` and
    /// `session_less_pane_stays_silent_while_session_pane_is_mirrored_exercise`'s silent-pane
    /// half, which both start with this same silent phase.
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

    /// Drives a session-less pane through idle -> working -> done and asserts the owner's rule
    /// that a pane reporting no session is not mirrored: no tab thread and no card of any kind is
    /// created or posted for it (README's "Herdr -> Discord" bullet). This test's workspace is
    /// dedicated to the one pane, so its channel stays absent too, though the rule itself allows
    /// a workspace channel to exist because another pane in it has a session.
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

    /// Runs the startup sweep over one session-less agent and one session-carrying agent sharing
    /// a workspace, and asserts the create pass mirrors only the session-carrying one.
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
        sync_startup_topology(&connection, &agents, &tabs).await;

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

                    // Delete the synced thread and the cached topology so that a second sync
                    // would have to recreate the thread; a repeated idle snapshot is not a
                    // transition and must not sync again.
                    guild
                        .client
                        .delete_channel(matching_threads[0].id)
                        .await
                        .map_err(|error| error.to_string())?;
                    *connection.3.topology_cache().lock().await = None;
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

    /// Two-pane design, proving the owner's two rules in one exercise on two separate panes: a
    /// session-less shell pane drives idle -> working -> done (mirroring `session_less_pane_exercise`)
    /// to prove silence, while a separate, never-synthetically-reported pane starts a real
    /// `claude --model haiku` agent and drives one real turn to prove that a session-carrying
    /// pane's next qualifying transition creates the topology and posts a card. The two halves
    /// cannot share one pane: Herdr treats a pane it has been told carries a synthetic agent (via
    /// `report-agent`/`report-agent-session`) as occupied and rejects a real `agent start` on it
    /// with `agent_pane_busy`. Status on the Claude pane comes only from its own live
    /// `agent.list` snapshots; nothing is set by hand.
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
        // The persistent watch has no settle-time read of its own, so read once more to deliver the
        // reply written just before `done`.
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
        let messages = thread_messages(guild, thread.id).await?;
        if messages
            .iter()
            .any(|(content, embed, _)| !embed && content.trim().eq_ignore_ascii_case("ready"))
        {
            Ok(())
        } else {
            Err(format!(
                "reporting a session did not post the reply as live text, thread has {messages:?}"
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
