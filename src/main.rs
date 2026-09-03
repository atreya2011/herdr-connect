use herdr_connect_rs::{
    AgentLogCapture, AgentSession, AgentSnapshot, ComponentHandler, HerdrSubscription, HerdrTab,
    TopologyCache, TopologyRoute, Transition, TransitionMessage, agent_read_detection,
    create_transition_messages, create_unsupported_blocked_card, deliver_transition_card,
    drive_gateway_with_components, expire_informational_card, fetch_topology_lists,
    format_detection_question, hook_timeout, is_postable_transition, lifecycle_subscriptions,
    list_agents, load_discord_config, reconcile_topology_cache, route_topology,
    status_subscriptions, subscribe_herdr_events, sync_topology, tab_list_result,
    transition_card_nonce,
};
use herdr_connect_rs::{
    Decision, Interaction, PermissionResponder, PermissionVendor, decode_claude_permission_request,
    decode_codex_permission_request, decode_cursor_permission_request, encode_claude_decision,
    encode_codex_decision, encode_cursor_decision, handle_component, request_decision,
    run_broker as run_permission_broker,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use twilight_http::Client;
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, MessageMarker},
};

type DiscordConnection = (
    Arc<Client>,
    Id<GuildMarker>,
    String,
    Arc<PermissionResponder>,
);
type GatewayTask = tokio::task::JoinHandle<Result<(), String>>;
type BrokerTask = tokio::task::JoinHandle<Result<(), String>>;

#[derive(Clone, Copy)]
struct InformationalCard {
    channel: Id<ChannelMarker>,
    message: Id<MessageMarker>,
}

#[derive(Default)]
struct BridgeState {
    previous: HashMap<String, (String, String)>,
    state_change_sequences: HashMap<String, u64>,
    herdr_state_change_seq: HashMap<String, u64>,
    blocked_since: HashMap<String, Instant>,
    informational_cards: HashMap<String, InformationalCard>,
    blocked_capture_attempts: HashMap<String, u32>,
    /// Last reply card text delivered per terminal, for session-carrying panes only. A reply
    /// card whose captured text equals this entry is not posted again, whether it arrives on the
    /// status-change path or the seq-backstop path; a legitimately identical consecutive reply
    /// is intentionally not reposted.
    last_posted: HashMap<String, String>,
}

struct BlockedCardContext<'a> {
    client: &'a Client,
    guild: Id<GuildMarker>,
    owner_id: &'a str,
    responder: &'a PermissionResponder,
    topology_cache: &'a TopologyCache,
    route: &'a TopologyRoute,
    snapshot: &'a AgentSnapshot,
    terminal: &'a str,
    from_status: &'a str,
    blocked_since: Option<&'a Instant>,
    state_change_seq: u64,
    informational_cards: &'a mut HashMap<String, InformationalCard>,
    blocked_capture_attempts: &'a mut HashMap<String, u32>,
    search_root: Option<&'a Path>,
}

/// Bounded number of blocked-capture attempts before falling back to the
/// informational card. The herdr "blocked" status can flip before the vendor log
/// file is flushed with the pending question, so a single capture miss is not
/// treated as "no question" — it is retried on the next few snapshots instead.
const MAX_BLOCKED_CAPTURE_ATTEMPTS: u32 = 3;

/// Retries blocked-capture while a pane stays blocked without another status event.
const BLOCKED_CAPTURE_RETRY_INTERVAL: Duration = Duration::from_millis(1_500);

const SUBSCRIBE_RETRY_INITIAL: Duration = Duration::from_millis(250);
const SUBSCRIBE_RETRY_MAX: Duration = Duration::from_secs(30);

#[derive(Debug, PartialEq, Eq)]
enum BlockedResponse {
    Question,
    Retry,
    Unsupported,
}

const fn decide_blocked_response(
    vendor_supported: bool,
    question: Option<&str>,
    attempts_so_far: u32,
) -> BlockedResponse {
    if !vendor_supported {
        return BlockedResponse::Unsupported;
    }
    if question.is_some() {
        return BlockedResponse::Question;
    }
    if attempts_so_far + 1 < MAX_BLOCKED_CAPTURE_ATTEMPTS {
        BlockedResponse::Retry
    } else {
        BlockedResponse::Unsupported
    }
}

async fn wait_for_gateway(gateway: Option<&mut GatewayTask>) -> Result<(), String> {
    match gateway {
        Some(gateway) => gateway
            .await
            .map_err(|error| format!("discord gateway task failed: {error}"))?,
        None => std::future::pending().await,
    }
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
        guild,
        owner_id,
        responder,
        topology_cache,
        route,
        snapshot,
        terminal,
        from_status,
        blocked_since,
        state_change_seq,
        informational_cards,
        blocked_capture_attempts,
        search_root,
    } = context;
    let target = match sync_route(client, guild, route, topology_cache).await {
        Ok(target) => target,
        Err(error) => {
            eprintln!("{error}");
            return;
        }
    };
    let vendor_supported = matches!(snapshot.agent.as_str(), "claude" | "codex");
    let supported_broker_pending = vendor_supported
        && snapshot
            .session
            .as_ref()
            .is_some_and(|session| responder.has_pending_session(&session.value));
    if supported_broker_pending {
        return;
    }
    let detection_question = (snapshot.agent == "claude")
        .then(|| agent_read_detection(&route.pane_id).ok())
        .flatten()
        .and_then(|text| format_detection_question(&text));
    let capture = detection_question.map_or_else(
        || {
            search_root.map_or_else(
                || capture_for_blocked(snapshot),
                |root| capture_for_blocked_with_search_root(snapshot, root),
            )
        },
        |question| AgentLogCapture {
            message: question.clone(),
            question: Some(question),
            failure: None,
        },
    );
    let attempts_so_far = blocked_capture_attempts.get(terminal).copied().unwrap_or(0);
    let messages = match decide_blocked_response(
        vendor_supported,
        capture.question.as_deref(),
        attempts_so_far,
    ) {
        BlockedResponse::Retry => {
            blocked_capture_attempts.insert(terminal.to_owned(), attempts_so_far + 1);
            return;
        }
        BlockedResponse::Question => {
            blocked_capture_attempts.remove(terminal);
            let transition = Transition {
                from: from_status.to_owned(),
                to: "blocked".to_owned(),
                terminal_id: terminal.to_owned(),
                agent: snapshot.agent.clone(),
            };
            create_transition_messages(&transition, &capture, owner_id)
        }
        BlockedResponse::Unsupported => {
            blocked_capture_attempts.remove(terminal);
            let blocked_age = blocked_since.map_or(Duration::ZERO, |started| {
                Instant::now().saturating_duration_since(*started)
            });
            vec![create_unsupported_blocked_card(
                &snapshot.agent,
                &route.pane_id,
                capture.question.as_deref().unwrap_or(&capture.message),
                owner_id,
                blocked_age,
            )]
        }
    };
    deliver_blocked_messages(
        client,
        target,
        terminal,
        state_change_seq,
        &messages,
        informational_cards,
    )
    .await;
}

async fn deliver_blocked_messages(
    client: &Client,
    target: Id<ChannelMarker>,
    terminal: &str,
    state_change_seq: u64,
    messages: &[TransitionMessage],
    informational_cards: &mut HashMap<String, InformationalCard>,
) {
    let mut last = None;
    for (index, message) in messages.iter().enumerate() {
        let nonce = transition_card_nonce(terminal, state_change_seq, index);
        match deliver_transition_card(client, target, message, &nonce).await {
            Ok(id) => last = Some(id),
            Err(error) => {
                eprintln!("discord delivery error: {error}");
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
    discord: Option<&DiscordConnection>,
    terminal: &str,
    informational_cards: &mut HashMap<String, InformationalCard>,
) {
    if let Some(card) = informational_cards.get(terminal).copied()
        && let Some((client, _guild, _owner_id, _responder)) = discord
    {
        if let Err(error) = expire_informational_card(
            client.as_ref(),
            card.channel,
            card.message,
            "resolved: pane left blocked",
        )
        .await
        {
            eprintln!("discord blocked-card expiry error: {error}");
        } else {
            informational_cards.remove(terminal);
        }
    }
}

async fn expire_departed_card(
    discord: Option<&DiscordConnection>,
    terminal: &str,
    card: InformationalCard,
) {
    let Some((client, _guild, _owner_id, _responder)) = discord else {
        return;
    };
    if let Err(error) = expire_informational_card(
        client.as_ref(),
        card.channel,
        card.message,
        "resolved: pane left blocked",
    )
    .await
    {
        eprintln!("discord blocked-card expiry error for {terminal}: {error}");
    }
}

/// Herdr's `state_change_seq` advancing while the bridge only sees a settled status means a
/// `working` phase happened between snapshots; rewrite `from` so the card still posts.
#[must_use]
fn seq_backstop_rewrites_working_from(
    transition: &Transition,
    previous_herdr_seq: Option<u64>,
    current_herdr_seq: u64,
) -> bool {
    !is_postable_transition(transition)
        && matches!(transition.to.as_str(), "idle" | "done")
        && previous_herdr_seq.is_some_and(|previous| current_herdr_seq > previous)
}

/// Settled status unchanged between snapshots while Herdr's seq advanced: a full turn collapsed.
#[must_use]
fn seq_backstop_collapsed_settled_turn(
    status: &str,
    previous_herdr_seq: Option<u64>,
    current_herdr_seq: u64,
) -> bool {
    matches!(status, "idle" | "done")
        && previous_herdr_seq.is_some_and(|previous| current_herdr_seq > previous)
}

async fn process_snapshot(
    snapshot: &AgentSnapshot,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    discord: Option<&DiscordConnection>,
    state: &mut BridgeState,
) {
    let agent = snapshot.agent.clone();
    let terminal = snapshot.terminal_id.clone();
    let status = snapshot.agent_status.clone();
    println!("{agent} {terminal}: {status}");
    let previous_herdr_seq = state
        .herdr_state_change_seq
        .insert(terminal.clone(), snapshot.state_change_seq);
    if let Some((old, prior_agent)) = state.previous.get(&terminal).cloned() {
        if old != status {
            let state_change_seq =
                next_state_change_sequence(&mut state.state_change_sequences, &terminal);
            let prior_status = old.clone();
            let leaving_blocked = prior_status == "blocked" && status != "blocked";
            let mut transition = Transition {
                from: old,
                to: status.clone(),
                terminal_id: terminal.clone(),
                agent: prior_agent,
            };
            if seq_backstop_rewrites_working_from(
                &transition,
                previous_herdr_seq,
                snapshot.state_change_seq,
            ) {
                "working".clone_into(&mut transition.from);
            }
            update_blocked_lifecycle(
                discord,
                &terminal,
                leaving_blocked,
                status == "blocked",
                state,
            )
            .await;
            if is_postable_transition(&transition)
                && let Err(error) = deliver_postable_transition(
                    PostableTransitionContext {
                        snapshot,
                        agents,
                        tabs,
                        discord,
                        terminal: &terminal,
                        transition: &transition,
                        state_change_seq,
                    },
                    state,
                )
                .await
            {
                eprintln!("{error}");
            }
        } else if seq_backstop_collapsed_settled_turn(
            &status,
            previous_herdr_seq,
            snapshot.state_change_seq,
        ) {
            let state_change_seq =
                next_state_change_sequence(&mut state.state_change_sequences, &terminal);
            let transition = Transition {
                from: "working".to_owned(),
                to: status.clone(),
                terminal_id: terminal.clone(),
                agent: prior_agent,
            };
            if is_postable_transition(&transition)
                && let Err(error) = deliver_postable_transition(
                    PostableTransitionContext {
                        snapshot,
                        agents,
                        tabs,
                        discord,
                        terminal: &terminal,
                        transition: &transition,
                        state_change_seq,
                    },
                    state,
                )
                .await
            {
                eprintln!("{error}");
            }
        }
    } else if status == "blocked" && state.blocked_capture_attempts.contains_key(&terminal) {
        retry_pending_blocked_capture(snapshot, agents, tabs, discord, &terminal, state).await;
    }
    state
        .previous
        .insert(terminal.clone(), (status.clone(), agent));
}

struct PostableTransitionContext<'a> {
    snapshot: &'a AgentSnapshot,
    agents: &'a [AgentSnapshot],
    tabs: &'a [HerdrTab],
    discord: Option<&'a DiscordConnection>,
    terminal: &'a str,
    transition: &'a Transition,
    state_change_seq: u64,
}

/// Delivers one postable transition's card to Discord: a blocked transition goes through
/// `handle_blocked_card` unconditionally, while a reply-card transition is captured from the
/// vendor log and delivered. For a session-carrying snapshot, a reply card whose captured text
/// equals the last one delivered for this terminal is skipped instead of reposted. The check
/// applies on both the status-change path and the seq-backstop path, so a legitimately identical
/// consecutive reply is intentionally not reposted either. A snapshot without a session keeps
/// posting every capture, since it has no vendor log to compare against.
///
/// # Errors
///
/// Returns topology or Discord delivery errors.
async fn deliver_postable_transition(
    context: PostableTransitionContext<'_>,
    state: &mut BridgeState,
) -> Result<(), String> {
    let PostableTransitionContext {
        snapshot,
        agents,
        tabs,
        discord,
        terminal,
        transition,
        state_change_seq,
    } = context;
    let route = route_topology(agents, tabs, terminal)?;
    let Some(connection) = discord else {
        return Ok(());
    };
    let (client, guild, owner_id, responder) = connection;
    if transition.to == "blocked" {
        handle_blocked_card(BlockedCardContext {
            client: client.as_ref(),
            guild: *guild,
            owner_id,
            responder: responder.as_ref(),
            topology_cache: responder.topology_cache(),
            route: &route,
            snapshot,
            terminal,
            from_status: &transition.from,
            blocked_since: state.blocked_since.get(terminal),
            state_change_seq,
            informational_cards: &mut state.informational_cards,
            blocked_capture_attempts: &mut state.blocked_capture_attempts,
            search_root: None,
        })
        .await;
        return Ok(());
    }
    let Some(capture) = capture_for_or_report(snapshot) else {
        return Ok(());
    };
    if snapshot.session.is_some() && state.last_posted.get(terminal) == Some(&capture.message) {
        println!("{terminal}: skipped duplicate reply card");
        return Ok(());
    }
    deliver_to_route(connection, &route, transition, &capture, state_change_seq).await?;
    if snapshot.session.is_some() {
        state
            .last_posted
            .insert(terminal.to_owned(), capture.message);
    }
    Ok(())
}

async fn update_blocked_lifecycle(
    discord: Option<&DiscordConnection>,
    terminal: &str,
    leaving_blocked: bool,
    entering_blocked: bool,
    state: &mut BridgeState,
) {
    if leaving_blocked {
        state.blocked_since.remove(terminal);
        state.blocked_capture_attempts.remove(terminal);
        expire_blocked_card(discord, terminal, &mut state.informational_cards).await;
    }
    if entering_blocked {
        state
            .blocked_since
            .entry(terminal.to_owned())
            .or_insert_with(Instant::now);
    }
}

async fn retry_pending_blocked_capture(
    snapshot: &AgentSnapshot,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    discord: Option<&DiscordConnection>,
    terminal: &str,
    state: &mut BridgeState,
) {
    let route = match route_topology(agents, tabs, terminal) {
        Ok(route) => route,
        Err(error) => {
            eprintln!("{error}");
            return;
        }
    };
    let Some((client, guild, owner_id, responder)) = discord else {
        return;
    };
    let state_change_seq = state
        .state_change_sequences
        .get(terminal)
        .copied()
        .unwrap_or(1);
    handle_blocked_card(BlockedCardContext {
        client: client.as_ref(),
        guild: *guild,
        owner_id,
        responder: responder.as_ref(),
        topology_cache: responder.topology_cache(),
        route: &route,
        snapshot,
        terminal,
        from_status: "blocked",
        blocked_since: state.blocked_since.get(terminal),
        state_change_seq,
        informational_cards: &mut state.informational_cards,
        blocked_capture_attempts: &mut state.blocked_capture_attempts,
        search_root: None,
    })
    .await;
}

fn component_handler(responder: Arc<PermissionResponder>) -> ComponentHandler {
    Arc::new(move |interaction| {
        let responder = Arc::clone(&responder);
        Box::pin(async move { handle_component(responder, interaction).await })
    })
}

fn capture_for(snapshot: &AgentSnapshot) -> Result<AgentLogCapture, String> {
    let home = std::env::var_os("HOME").ok_or_else(|| "HOME is not configured".to_owned())?;
    capture_for_with_search_root(snapshot, Path::new(&home))
}

fn capture_for_with_search_root(
    snapshot: &AgentSnapshot,
    search_root: &Path,
) -> Result<AgentLogCapture, String> {
    let Some(session) = snapshot.session.clone() else {
        return Ok(AgentLogCapture {
            message: "agent stopped, no log available".to_owned(),
            failure: None,
            question: None,
        });
    };
    let path = resolve_session_path(search_root, snapshot, &session)?;
    let log = herdr_connect_rs::read_agent_log(Some(session), &path)?;
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
) -> Result<PathBuf, String> {
    match session.agent.as_str() {
        "claude" => {
            let cwd = snapshot
                .cwd
                .as_deref()
                .filter(|cwd| !cwd.trim().is_empty())
                .ok_or_else(|| "claude session has no cwd for log resolution".to_owned())?;
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
            let candidates = [
                search_root
                    .join(".claude/projects")
                    .join(&cwd_slug)
                    .join(format!("{}.jsonl", session.value)),
                search_root
                    .join(".claude-one/projects")
                    .join(&cwd_slug)
                    .join(format!("{}.jsonl", session.value)),
            ];
            let existing = candidates
                .into_iter()
                .filter(|path| path.is_file())
                .collect::<Vec<_>>();
            unique_existing_path(&existing, "claude session log")
        }
        "codex" => find_unique_session_path(
            &search_root.join(".codex/sessions"),
            &session.value,
            |path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| {
                        name.starts_with("rollout-")
                            && Path::new(name)
                                .extension()
                                .and_then(|extension| extension.to_str())
                                .is_some_and(|extension| extension.eq_ignore_ascii_case("jsonl"))
                    })
            },
            "codex session log",
        ),
        "cursor" => {
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
        agent => Err(format!("unsupported vendor session agent: {agent}")),
    }
}

fn unique_existing_path(candidates: &[PathBuf], description: &str) -> Result<PathBuf, String> {
    match candidates {
        [path] => Ok(path.clone()),
        [] => Err(format!("{description} was not found")),
        _ => Err(format!("multiple {description}s were found")),
    }
}

fn find_unique_session_path(
    root: &Path,
    session_id: &str,
    matches: fn(&Path) -> bool,
    description: &str,
) -> Result<PathBuf, String> {
    let mut candidates = Vec::new();
    collect_matching_paths(root, session_id, matches, &mut candidates)?;
    unique_existing_path(&candidates, description)
}

fn collect_matching_paths(
    directory: &Path,
    session_id: &str,
    matches: fn(&Path) -> bool,
    candidates: &mut Vec<PathBuf>,
) -> Result<(), String> {
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

fn read_directories(root: &Path, description: &str) -> Result<Vec<PathBuf>, String> {
    Ok(read_entries(root, description)?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect())
}

fn read_entries(directory: &Path, description: &str) -> Result<Vec<fs::DirEntry>, String> {
    fs::read_dir(directory)
        .map_err(|error| {
            format!(
                "failed to read {description} {}: {error}",
                directory.display()
            )
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            format!(
                "failed to read {description} {}: {error}",
                directory.display()
            )
        })
}

fn capture_for_or_report(snapshot: &AgentSnapshot) -> Option<AgentLogCapture> {
    match capture_for(snapshot) {
        Ok(capture) => Some(capture),
        Err(error) => {
            eprintln!("agent log capture error: {error}");
            None
        }
    }
}

fn capture_for_blocked(snapshot: &AgentSnapshot) -> AgentLogCapture {
    std::env::var_os("HOME").map_or_else(
        || AgentLogCapture {
            message: "blocked context unavailable: HOME is not configured".to_owned(),
            failure: None,
            question: None,
        },
        |home| capture_for_blocked_with_search_root(snapshot, Path::new(&home)),
    )
}

fn capture_for_blocked_with_search_root(
    snapshot: &AgentSnapshot,
    search_root: &Path,
) -> AgentLogCapture {
    match capture_for_with_search_root(snapshot, search_root) {
        Ok(capture) => capture,
        Err(error) => {
            eprintln!("agent blocked-context capture error: {error}");
            AgentLogCapture {
                message: format!("blocked context unavailable: {error}"),
                failure: None,
                question: None,
            }
        }
    }
}

/// Delivers a transition's cards to the route resolved from one Herdr snapshot, including its `format_thread_name` result.
///
/// # Errors
///
/// Returns Discord topology or card-delivery errors.
async fn deliver_to_route(
    discord: &DiscordConnection,
    route: &TopologyRoute,
    transition: &Transition,
    capture: &AgentLogCapture,
    state_change_seq: u64,
) -> Result<Id<MessageMarker>, String> {
    let (client, guild, owner_id, responder) = discord;
    let messages = create_transition_messages(transition, capture, owner_id);
    let target = sync_route(client.as_ref(), *guild, route, responder.topology_cache()).await?;
    let mut last_message_id = None;
    for (index, message) in messages.iter().enumerate() {
        let nonce = transition_card_nonce(&transition.terminal_id, state_change_seq, index);
        last_message_id = Some(
            deliver_transition_card(client.as_ref(), target, message, &nonce)
                .await
                .map_err(|error| format!("discord delivery error: {error}"))?,
        );
    }
    last_message_id.ok_or_else(|| "discord delivery produced no messages".to_owned())
}

async fn sync_route(
    client: &Client,
    guild: Id<GuildMarker>,
    route: &TopologyRoute,
    topology_cache: &TopologyCache,
) -> Result<Id<ChannelMarker>, String> {
    let fetched = fetch_topology_lists(client, guild)
        .await
        .map_err(|error| format!("discord topology error: {error}"))?;
    let mut guard = topology_cache.lock().await;
    let (channels, active_threads) = reconcile_topology_cache(&mut guard, fetched);
    sync_topology(client, guild, channels, active_threads, route)
        .await
        .map_err(|error| format!("discord topology error: {error}"))
}

/// Ensures every workspace channel and tab thread exists before the event loop starts. A
/// per-tab routing or naming error is logged and skipped; the lazy sync inside delivery still
/// covers that tab once a card is due. The sweep refetches both lists once at its start rather
/// than adopting whatever the shared cache already holds, so a cache that missed an earlier
/// create cannot make the sweep recreate an existing channel or thread.
async fn sync_startup_topology(
    discord: &DiscordConnection,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
) {
    let (client, guild, _, responder) = discord;
    let topology_cache = responder.topology_cache();
    let fetched = match fetch_topology_lists(client.as_ref(), *guild).await {
        Ok(lists) => lists,
        Err(error) => {
            eprintln!("herdr startup topology error: {error}");
            return;
        }
    };
    let mut guard = topology_cache.lock().await;
    reconcile_topology_cache(&mut guard, fetched);
    drop(guard);
    let mut synced_tabs = HashSet::new();
    for agent in agents {
        if let Some(tab_id) = agent.tab_id.as_deref()
            && !synced_tabs.insert(tab_id.to_owned())
        {
            continue;
        }
        let route = match route_topology(agents, tabs, &agent.terminal_id) {
            Ok(route) => route,
            Err(error) => {
                eprintln!("herdr startup topology error: {error}");
                continue;
            }
        };
        let mut guard = topology_cache.lock().await;
        let Some((channels, active_threads)) = guard.as_mut() else {
            eprintln!("herdr startup topology error: topology cache was cleared");
            return;
        };
        let result = sync_topology(client.as_ref(), *guild, channels, active_threads, &route).await;
        drop(guard);
        if let Err(error) = result {
            eprintln!("herdr startup topology error: {error}");
        }
    }
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
    current: &HashSet<String>,
) -> Vec<(String, InformationalCard)> {
    state
        .blocked_since
        .retain(|terminal, _| current.contains(terminal));
    state
        .state_change_sequences
        .retain(|terminal, _| current.contains(terminal));
    state
        .herdr_state_change_seq
        .retain(|terminal, _| current.contains(terminal));
    state
        .blocked_capture_attempts
        .retain(|terminal, _| current.contains(terminal));
    state
        .last_posted
        .retain(|terminal, _| current.contains(terminal));
    let departed_cards = state
        .informational_cards
        .iter()
        .filter(|(terminal, _)| !current.contains(*terminal))
        .map(|(terminal, card)| (terminal.clone(), *card))
        .collect();
    state
        .informational_cards
        .retain(|terminal, _| current.contains(terminal));
    departed_cards
}

fn discord_connection(
    topology_cache: TopologyCache,
) -> Result<Option<(DiscordConnection, GatewayTask)>, Box<dyn std::error::Error>> {
    match (
        std::env::var("DISCORD_TOKEN"),
        std::env::var("DISCORD_GUILD_ID"),
        std::env::var("DISCORD_OWNER_ID"),
    ) {
        (Ok(token), Ok(guild_id), Ok(owner_id)) => {
            let config = load_discord_config(&[
                ("DISCORD_TOKEN", &token),
                ("DISCORD_GUILD_ID", &guild_id),
                ("DISCORD_OWNER_ID", &owner_id),
            ])?;
            let guild = Id::<GuildMarker>::new(config.guild_id.parse()?);
            let client = Arc::new(Client::builder().token(config.token.clone()).build());
            let (notices_tx, notices_rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                while let Ok(notice) = notices_rx.recv() {
                    eprintln!("{notice}");
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
                Arc::clone(&client),
                guild,
                config.owner_id.clone(),
                notices_tx,
                component_handler(Arc::clone(&responder)),
            ));
            Ok(Some(((client, guild, config.owner_id, responder), gateway)))
        }
        _ => Ok(None),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut args = std::env::args();
    let _program = args.next();
    match args.next().as_deref() {
        Some("hook") => return run_hook(args.collect()).await,
        Some("broker") => return run_broker(args.collect()).await,
        _ => {}
    }
    run_bridge().await
}

async fn run_hook(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let (explicit_vendor, requested_socket) = parse_hook_args(&args)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let mut input = Vec::new();
    tokio::io::stdin().read_to_end(&mut input).await?;
    let Some(interaction) = decode_hook_request(&input, explicit_vendor) else {
        if matches!(explicit_vendor, Some(PermissionVendor::Cursor)) {
            write_hook_decision(PermissionVendor::Cursor, None).await?;
        }
        return Ok(());
    };
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

fn decode_hook_request(input: &[u8], vendor: Option<PermissionVendor>) -> Option<Interaction> {
    match vendor {
        Some(PermissionVendor::Claude) => decode_claude_permission_request(input).ok(),
        Some(PermissionVendor::Codex) => decode_codex_permission_request(input).ok(),
        Some(PermissionVendor::Cursor) => decode_cursor_permission_request(input).ok(),
        None => decode_claude_permission_request(input)
            .or_else(|_| decode_codex_permission_request(input))
            .or_else(|_| decode_cursor_permission_request(input))
            .ok(),
    }
}

fn encode_hook_decision(
    vendor: PermissionVendor,
    decision: Option<&Decision>,
) -> Result<Option<Vec<u8>>, String> {
    let Some(decision) = decision else {
        return match vendor {
            PermissionVendor::Claude | PermissionVendor::Codex => Ok(None),
            PermissionVendor::Cursor => encode_cursor_decision(&Decision::deny(Some(
                "permission broker did not return a decision; denying by default".to_owned(),
            )))
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
                    "claude" => PermissionVendor::Claude,
                    "codex" => PermissionVendor::Codex,
                    "cursor" => PermissionVendor::Cursor,
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

async fn run_broker(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let socket_path = socket_path(&args)
        .ok_or("broker requires HERDR_CLAUDE_BROKER_SOCKET or --socket <path>")?;
    let token = std::env::var("DISCORD_TOKEN")?;
    let guild_id = std::env::var("DISCORD_GUILD_ID")?;
    let owner_id = std::env::var("DISCORD_OWNER_ID")?;
    let config = load_discord_config(&[
        ("DISCORD_TOKEN", &token),
        ("DISCORD_GUILD_ID", &guild_id),
        ("DISCORD_OWNER_ID", &owner_id),
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
    run_permission_broker(&socket_path, responder)
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

fn start_broker(connection: &DiscordConnection) -> Option<BrokerTask> {
    socket_path(&[]).map(|socket| {
        let responder = Arc::clone(&connection.3);
        tokio::spawn(async move {
            run_permission_broker(&socket, responder)
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
    let mut ids: Vec<String> = agents
        .iter()
        .filter_map(|agent| agent.pane_id.clone())
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

#[derive(Debug, PartialEq, Eq)]
enum Membership {
    Add(String),
    Remove(String),
}

fn canonical_event_name(event: &str) -> String {
    event.replace('.', "_")
}

fn lifecycle_membership(event: &serde_json::Value) -> Option<Membership> {
    match canonical_event_name(event.get("event")?.as_str()?).as_str() {
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
}

async fn doorbell_unless_shutdown(
    discord: Option<&DiscordConnection>,
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
    discord: Option<&DiscordConnection>,
    gateway: &mut Option<GatewayTask>,
    broker: &mut Option<BrokerTask>,
    stop: &mut tokio::signal::unix::Signal,
    runtime: &mut BridgeRuntime,
) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let blocked_retry_pending = !runtime.state.blocked_capture_attempts.is_empty();
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
            () = tokio::time::sleep(BLOCKED_CAPTURE_RETRY_INTERVAL), if blocked_retry_pending => {
                if !doorbell_unless_shutdown(
                    discord,
                    &mut runtime.state,
                    &mut runtime.pane_ids,
                    &mut runtime.status,
                    stop,
                    broker,
                )
                .await
                {
                    break;
                }
            }
            _ = tokio::signal::ctrl_c() => break,
            _ = stop.recv() => break,
            result = wait_for_gateway(gateway.as_mut()) => {
                return result.map_err(Into::into);
            }
            result = wait_for_broker(broker.as_mut()) => {
                return result.map_err(Into::into);
            }
        }
    }
    Ok(())
}

async fn handle_lifecycle_select_result(
    result: Result<serde_json::Value, String>,
    discord: Option<&DiscordConnection>,
    stop: &mut tokio::signal::unix::Signal,
    broker: &mut Option<BrokerTask>,
    runtime: &mut BridgeRuntime,
) -> bool {
    match result {
        Ok(event) => {
            if let Some(change) = lifecycle_membership(&event)
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
            eprintln!("herdr lifecycle subscribe error: {error}");
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
    }
}

async fn handle_status_select_result(
    result: Result<serde_json::Value, String>,
    discord: Option<&DiscordConnection>,
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
            eprintln!("herdr status subscribe error: {error}");
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
                eprintln!("herdr subscribe error: {error}; retrying in {delay:?}");
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
                eprintln!("herdr status subscribe error: {error}; retrying in {delay:?}");
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

#[cfg(test)]
async fn subscribe_status(pane_ids: &[String]) -> Result<Option<HerdrSubscription>, String> {
    if pane_ids.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        subscribe_herdr_events(&status_subscriptions(pane_ids)).await?,
    ))
}

async fn apply_herdr_snapshot(
    discord: Option<&DiscordConnection>,
    state: &mut BridgeState,
) -> Result<Vec<String>, String> {
    let agents = list_agents()?;
    let tabs = tab_list_result()?;
    let current: HashSet<String> = agents.iter().map(|s| s.terminal_id.clone()).collect();
    state
        .previous
        .retain(|terminal, _| current.contains(terminal));
    let departed_cards = prune_departed_state(state, &current);
    for (terminal, card) in departed_cards {
        expire_departed_card(discord, &terminal, card).await;
    }
    for snapshot in &agents {
        process_snapshot(snapshot, &agents, &tabs, discord, state).await;
    }
    Ok(pane_ids_from_agents(&agents))
}

async fn doorbell_snapshot(
    discord: Option<&DiscordConnection>,
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
        Err(error) => eprintln!("herdr snapshot error: {error}"),
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

async fn run_bridge() -> Result<(), Box<dyn std::error::Error>> {
    let topology_cache: TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
    let (discord, mut gateway, mut broker) = match discord_connection(Arc::clone(&topology_cache))?
    {
        Some((connection, gateway)) => {
            let broker = start_broker(&connection);
            (Some(connection), Some(gateway), broker)
        }
        None => (None, None, None),
    };
    let state = BridgeState::default();
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
    };
    if !doorbell_unless_shutdown(
        discord.as_ref(),
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
    if let Some(discord) = discord.as_ref() {
        match list_agents().and_then(|agents| tab_list_result().map(|tabs| (agents, tabs))) {
            Ok((agents, tabs)) => {
                let discord = discord.clone();
                let startup_task =
                    tokio::spawn(
                        async move { sync_startup_topology(&discord, &agents, &tabs).await },
                    );
                tokio::spawn(async move {
                    if let Err(error) = startup_task.await {
                        eprintln!("herdr startup topology task error: {error}");
                    }
                });
            }
            Err(error) => eprintln!("herdr startup topology snapshot error: {error}"),
        }
    }
    bridge_event_loop(
        discord.as_ref(),
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
    use super::{
        BlockedCardContext, BlockedResponse, BridgeState, Client, InformationalCard, Membership,
        PermissionResponder, TopologyRoute, agent_read_detection, apply_membership,
        capture_for_with_search_root, create_transition_messages, decide_blocked_response,
        fetch_topology_lists, handle_blocked_card, lifecycle_membership, list_agents,
        next_state_change_sequence, process_snapshot, prune_departed_state, resolve_session_path,
        route_topology, seq_backstop_collapsed_settled_turn, seq_backstop_rewrites_working_from,
        subscribe_status, subscribe_status_with_backoff, sync_route, sync_startup_topology,
        tab_list_result,
    };
    use herdr_connect_rs::{
        AgentSession, AgentSnapshot, Transition, lifecycle_subscriptions, status_subscriptions,
        subscribe_herdr_events, transition_card_nonce,
    };
    use serde_json::{Value, json};
    use serial_test::serial;
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::io::Write;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::Arc;
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use twilight_model::id::{
        Id,
        marker::{ChannelMarker, GuildMarker, MessageMarker},
    };

    #[test]
    fn cursor_broker_failures_emit_deny_objects() {
        for failure in ["timeout", "malformed broker response", "unavailable"] {
            let output = super::encode_hook_decision(super::PermissionVendor::Cursor, None)
                .unwrap_or_else(|error| panic!("{failure} failure must encode: {error}"))
                .expect("Cursor failures must produce output");
            let value: Value =
                serde_json::from_slice(&output).expect("Cursor failure output is JSON");
            assert_eq!(value["permission"], "deny", "failure: {failure}");
            assert!(
                value["agent_message"].as_str().is_some(),
                "failure: {failure}"
            );
        }
    }

    #[test]
    fn state_change_nonce_survives_terminal_departure_and_return() {
        let terminal = "terminal";
        let mut state_change_sequences = HashMap::new();
        let mut current_terminals = HashSet::from([terminal.to_owned()]);

        let pre_departure_nonce = transition_card_nonce(
            terminal,
            next_state_change_sequence(&mut state_change_sequences, terminal),
            0,
        );

        assert!(current_terminals.remove(terminal));
        assert!(current_terminals.insert(terminal.to_owned()));
        let returned_nonce = transition_card_nonce(
            terminal,
            next_state_change_sequence(&mut state_change_sequences, terminal),
            0,
        );

        assert_ne!(pre_departure_nonce, returned_nonce);
    }

    #[test]
    fn departed_terminals_are_pruned_from_reconciled_state() {
        let departed = "departed";
        let current = "current";
        let mut state = BridgeState::default();
        for terminal in [departed, current] {
            state
                .blocked_since
                .insert(terminal.to_owned(), Instant::now());
            state.state_change_sequences.insert(terminal.to_owned(), 1);
            state.herdr_state_change_seq.insert(terminal.to_owned(), 7);
            state.informational_cards.insert(
                terminal.to_owned(),
                InformationalCard {
                    channel: Id::<ChannelMarker>::new(1),
                    message: Id::<MessageMarker>::new(2),
                },
            );
            state
                .last_posted
                .insert(terminal.to_owned(), format!("{terminal} reply"));
        }

        let current_terminals = HashSet::from([current.to_owned()]);
        let expired = prune_departed_state(&mut state, &current_terminals);

        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, departed);
        assert!(!state.blocked_since.contains_key(departed));
        assert!(!state.state_change_sequences.contains_key(departed));
        assert!(!state.herdr_state_change_seq.contains_key(departed));
        assert!(!state.informational_cards.contains_key(departed));
        assert!(!state.last_posted.contains_key(departed));
        assert!(state.blocked_since.contains_key(current));
        assert!(state.state_change_sequences.contains_key(current));
        assert!(state.herdr_state_change_seq.contains_key(current));
        assert!(state.informational_cards.contains_key(current));
        assert_eq!(
            state.last_posted.get(current).map(String::as_str),
            Some("current reply")
        );
    }

    #[test]
    fn seq_backstop_rewrites_settled_turn_only_when_herdr_seq_advanced() {
        let transition = |from: &str, to: &str| Transition {
            from: from.to_owned(),
            to: to.to_owned(),
            terminal_id: "terminal".to_owned(),
            agent: "claude".to_owned(),
        };
        let cases = [
            (
                "seq advanced, settled: rewrite",
                transition("idle", "done"),
                Some(10),
                11,
                true,
            ),
            (
                "seq unchanged: no rewrite",
                transition("idle", "done"),
                Some(10),
                10,
                false,
            ),
            (
                "already postable: no rewrite",
                transition("working", "done"),
                Some(10),
                11,
                false,
            ),
            (
                "settled but seq missing: no rewrite",
                transition("idle", "idle"),
                None,
                11,
                false,
            ),
        ];
        for (label, transition, previous_seq, current_seq, expected) in cases {
            assert_eq!(
                seq_backstop_rewrites_working_from(&transition, previous_seq, current_seq),
                expected,
                "{label}: {transition:?} previous_seq={previous_seq:?} current_seq={current_seq}"
            );
        }
    }

    #[test]
    fn seq_backstop_collapsed_settled_turn_when_status_unchanged() {
        let cases = [
            ("done unchanged, seq advanced", "done", Some(10), 11, true),
            ("idle unchanged, seq advanced", "idle", Some(4), 5, true),
            ("done unchanged, seq flat", "done", Some(10), 10, false),
            ("working unchanged", "working", Some(10), 11, false),
            ("done unchanged, no prior seq", "done", None, 11, false),
        ];
        for (label, status, previous_seq, current_seq, expected) in cases {
            assert_eq!(
                seq_backstop_collapsed_settled_turn(status, previous_seq, current_seq),
                expected,
                "{label}"
            );
        }
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
    }

    #[test]
    fn claude_project_slugs_match_dot_and_underscore_directory_names_in_both_roots() {
        let root = std::env::temp_dir().join(format!(
            "herdr-connect-rs-claude-slug-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        let cases = [
            (
                ".claude",
                "/home/user/src/herdr-connect-rs",
                "-home-user-src-herdr-connect-rs",
            ),
            (
                ".claude-one",
                "/tmp/agent_workspace_v2",
                "-tmp-agent-workspace-v2",
            ),
        ];
        for (vendor_root, cwd, expected_directory) in cases {
            let directory = root
                .join(vendor_root)
                .join("projects")
                .join(expected_directory);
            fs::create_dir_all(&directory).expect("create Claude project directory");
            let expected_path = directory.join("slug-session.jsonl");
            fs::write(&expected_path, "").expect("create Claude session file");
            let snapshot = AgentSnapshot {
                agent: "claude".to_owned(),
                terminal_id: "slug-terminal".to_owned(),
                agent_status: "done".to_owned(),
                tab_id: None,
                workspace_id: None,
                pane_id: None,
                cwd: Some(cwd.to_owned()),
                terminal_title_stripped: None,
                session: Some(AgentSession {
                    agent: "claude".to_owned(),
                    value: "slug-session".to_owned(),
                }),
                state_change_seq: 0,
            };
            assert_eq!(
                resolve_session_path(
                    &root,
                    &snapshot,
                    snapshot.session.as_ref().expect("session is present"),
                ),
                Ok(expected_path)
            );
        }
        fs::remove_dir_all(&root).expect("remove Claude slug test directory");
    }

    #[test]
    fn captured_sessions_drive_transition_cards_and_pointer_posts() {
        let response: Value =
            serde_json::from_str(include_str!("../tests/fixtures/herdr-agent-list.json"))
                .expect("captured agent.list fixture is JSON");
        let agents: Vec<AgentSnapshot> =
            serde_json::from_value(response["result"]["agents"].clone())
                .expect("captured agent.list fixture has typed agents");
        let cases = [(true, "agent stopped, no log available")];
        for (without_session, expected_body) in cases {
            let snapshot = agents
                .iter()
                .find(|snapshot| snapshot.session.is_none() == without_session)
                .expect("fixture contains the requested agent");
            let capture = capture_for_with_search_root(snapshot, Path::new("tests/fixtures"))
                .expect("capture succeeds for a real session or explicit no-session pointer");
            let transition = Transition {
                from: "working".to_owned(),
                to: "done".to_owned(),
                terminal_id: snapshot.terminal_id.clone(),
                agent: snapshot.agent.clone(),
            };
            let card = create_transition_messages(&transition, &capture, "owner")
                .into_iter()
                .next()
                .expect("transition produces a card");
            assert_eq!(card.description, expected_body);
        }

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
        assert!(
            capture_for_with_search_root(&missing_log, Path::new("tests/fixtures")).is_err(),
            "reader errors for a reported session must surface instead of posting the pointer"
        );
    }

    #[test]
    fn decide_blocked_response_follows_the_bounded_retry_spec() {
        let cases = [
            (false, None, 0, BlockedResponse::Unsupported),
            (
                false,
                Some("pending question"),
                0,
                BlockedResponse::Unsupported,
            ),
            (true, Some("pending question"), 0, BlockedResponse::Question),
            (true, Some("pending question"), 2, BlockedResponse::Question),
            (true, None, 0, BlockedResponse::Retry),
            (true, None, 1, BlockedResponse::Retry),
            (true, None, 2, BlockedResponse::Unsupported),
        ];
        for (vendor_supported, question, attempts_so_far, expected) in cases {
            assert_eq!(
                decide_blocked_response(vendor_supported, question, attempts_so_far),
                expected,
                "vendor_supported={vendor_supported} question={question:?} attempts_so_far={attempts_so_far}"
            );
        }
    }

    #[test]
    fn claude_pending_question_fixture_yields_question_capture_and_card() {
        let snapshot = AgentSnapshot {
            agent: "claude".to_owned(),
            terminal_id: "question-terminal".to_owned(),
            agent_status: "blocked".to_owned(),
            tab_id: None,
            workspace_id: None,
            pane_id: None,
            cwd: Some("/srv/bridge".to_owned()),
            terminal_title_stripped: None,
            session: Some(AgentSession {
                agent: "claude".to_owned(),
                value: "9a11cafe-affe-4f5c-8bda-b10cb6a5cafe".to_owned(),
            }),
            state_change_seq: 0,
        };
        let capture = capture_for_with_search_root(&snapshot, Path::new("tests/fixtures"))
            .expect("fixture-backed claude session resolves");
        let expected_question =
            "Which environment should the fix target?\n1. staging\n2. production";
        assert_eq!(capture.question.as_deref(), Some(expected_question));

        let transition = Transition {
            from: "working".to_owned(),
            to: "blocked".to_owned(),
            terminal_id: snapshot.terminal_id,
            agent: snapshot.agent,
        };
        let card = create_transition_messages(&transition, &capture, "42")
            .into_iter()
            .next()
            .expect("blocked transition produces a card");
        assert_eq!(card.description, expected_question);
        assert_eq!(card.mention.as_deref(), Some("<@42>"));
    }

    #[tokio::test]
    async fn leaving_blocked_clears_capture_retry_bookkeeping() {
        let terminal = "leaving-blocked-terminal".to_owned();
        let snapshot = AgentSnapshot {
            agent: "claude".to_owned(),
            terminal_id: terminal.clone(),
            agent_status: "idle".to_owned(),
            tab_id: None,
            workspace_id: None,
            pane_id: None,
            cwd: None,
            terminal_title_stripped: None,
            session: None,
            state_change_seq: 0,
        };
        let mut state = BridgeState::default();
        state.previous.insert(
            terminal.clone(),
            ("blocked".to_owned(), "claude".to_owned()),
        );
        state.blocked_since.insert(terminal.clone(), Instant::now());
        state.blocked_capture_attempts.insert(terminal.clone(), 1);

        process_snapshot(&snapshot, &[], &[], None, &mut state).await;

        assert!(!state.blocked_capture_attempts.contains_key(&terminal));
        assert!(!state.blocked_since.contains_key(&terminal));
    }

    #[cfg(unix)]
    struct BlockedCaptureGuild {
        client: Arc<Client>,
        id: Id<GuildMarker>,
    }

    #[cfg(unix)]
    fn blocked_capture_guild() -> Option<BlockedCaptureGuild> {
        Some(BlockedCaptureGuild {
            client: Arc::new(
                Client::builder()
                    .token(std::env::var("DISCORD_TOKEN").ok()?)
                    .timeout(std::time::Duration::from_secs(30))
                    .build(),
            ),
            id: Id::new(std::env::var("DISCORD_GUILD_ID").ok()?.parse().ok()?),
        })
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
    #[tokio::test]
    #[serial]
    async fn blocked_capture_retries_before_fallback_and_posts_question_immediately() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );
        let result = blocked_capture_exercise(&guild).await;
        let left = blocked_capture_cleanup(&guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    async fn blocked_capture_exercise(guild: &BlockedCaptureGuild) -> Result<(), String> {
        let owner_id = std::env::var("DISCORD_OWNER_ID").map_err(|e| e.to_string())?;
        let responder = Arc::new(PermissionResponder::new(
            guild.client.clone(),
            guild.id,
            owner_id.clone(),
            Arc::new(tokio::sync::Mutex::new(None)),
        ));
        let route = TopologyRoute {
            workspace_id: "testrun-blocked-capture-workspace".to_owned(),
            tab_id: "testrun-blocked-capture-tab".to_owned(),
            pane_id: "testrun-blocked-capture-pane".to_owned(),
            channel_name: "testrun-blocked-capture".to_owned(),
            thread_name: "testrun-blocked-capture [testrun-blocked-capture-tab]".to_owned(),
        };
        assert_blocked_capture_retries_then_falls_back(
            guild,
            &route,
            responder.as_ref(),
            &owner_id,
        )
        .await?;
        assert_blocked_capture_posts_question_immediately(
            guild,
            &route,
            responder.as_ref(),
            &owner_id,
        )
        .await?;
        Ok(())
    }

    #[cfg(unix)]
    async fn assert_blocked_capture_retries_then_falls_back(
        guild: &BlockedCaptureGuild,
        route: &TopologyRoute,
        responder: &PermissionResponder,
        owner_id: &str,
    ) -> Result<(), String> {
        let terminal = "testrun-blocked-capture-terminal".to_owned();
        let no_question_snapshot = AgentSnapshot {
            agent: "claude".to_owned(),
            terminal_id: terminal.clone(),
            agent_status: "blocked".to_owned(),
            tab_id: None,
            workspace_id: None,
            pane_id: None,
            cwd: None,
            terminal_title_stripped: None,
            session: None,
            state_change_seq: 0,
        };
        let mut informational_cards = HashMap::new();
        let mut blocked_capture_attempts = HashMap::new();

        for attempt in 0..2u32 {
            handle_blocked_card(BlockedCardContext {
                client: guild.client.as_ref(),
                guild: guild.id,
                owner_id,
                responder,
                topology_cache: responder.topology_cache(),
                route,
                snapshot: &no_question_snapshot,
                terminal: &terminal,
                from_status: "blocked",
                blocked_since: None,
                state_change_seq: 1,
                informational_cards: &mut informational_cards,
                blocked_capture_attempts: &mut blocked_capture_attempts,
                search_root: None,
            })
            .await;
            if blocked_capture_attempts.get(&terminal).copied() != Some(attempt + 1) {
                return Err(format!(
                    "attempt {attempt}: expected retry bookkeeping to advance to {}, got {:?}",
                    attempt + 1,
                    blocked_capture_attempts.get(&terminal)
                ));
            }
            if informational_cards.contains_key(&terminal) {
                return Err(format!("attempt {attempt}: retry must not post a card yet"));
            }
        }

        handle_blocked_card(BlockedCardContext {
            client: guild.client.as_ref(),
            guild: guild.id,
            owner_id,
            responder,
            topology_cache: responder.topology_cache(),
            route,
            snapshot: &no_question_snapshot,
            terminal: &terminal,
            from_status: "blocked",
            blocked_since: None,
            state_change_seq: 1,
            informational_cards: &mut informational_cards,
            blocked_capture_attempts: &mut blocked_capture_attempts,
            search_root: None,
        })
        .await;
        if blocked_capture_attempts.contains_key(&terminal) {
            return Err("bookkeeping must clear once the fallback card posts".to_owned());
        }
        let fallback_card = informational_cards
            .get(&terminal)
            .ok_or("third attempt must post the informational fallback card")?;
        let fallback_message =
            fetch_message(guild, fallback_card.channel, fallback_card.message).await?;
        if !fallback_message.components.is_empty() {
            return Err("fallback card unexpectedly had components".to_owned());
        }
        let expected_owner_mention = format!("<@{owner_id}>");
        if fallback_message.content != expected_owner_mention {
            return Err(format!(
                "fallback card content was {:?}, expected {expected_owner_mention:?}",
                fallback_message.content
            ));
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn assert_blocked_capture_posts_question_immediately(
        guild: &BlockedCaptureGuild,
        route: &TopologyRoute,
        responder: &PermissionResponder,
        owner_id: &str,
    ) -> Result<(), String> {
        let question_terminal = "testrun-blocked-capture-terminal-question".to_owned();
        let question_snapshot = AgentSnapshot {
            agent: "claude".to_owned(),
            terminal_id: question_terminal.clone(),
            agent_status: "blocked".to_owned(),
            tab_id: None,
            workspace_id: None,
            pane_id: None,
            cwd: Some("/srv/bridge".to_owned()),
            terminal_title_stripped: None,
            session: Some(AgentSession {
                agent: "claude".to_owned(),
                value: "9a11cafe-affe-4f5c-8bda-b10cb6a5cafe".to_owned(),
            }),
            state_change_seq: 0,
        };
        let mut informational_cards = HashMap::new();
        let mut blocked_capture_attempts = HashMap::new();
        handle_blocked_card(BlockedCardContext {
            client: guild.client.as_ref(),
            guild: guild.id,
            owner_id,
            responder,
            topology_cache: responder.topology_cache(),
            route,
            snapshot: &question_snapshot,
            terminal: &question_terminal,
            from_status: "working",
            blocked_since: None,
            state_change_seq: 2,
            informational_cards: &mut informational_cards,
            blocked_capture_attempts: &mut blocked_capture_attempts,
            search_root: Some(Path::new("tests/fixtures")),
        })
        .await;
        if blocked_capture_attempts.contains_key(&question_terminal) {
            return Err("question path must not create retry bookkeeping".to_owned());
        }
        let question_card = informational_cards
            .get(&question_terminal)
            .ok_or("question path must post a card")?;
        let question_message =
            fetch_message(guild, question_card.channel, question_card.message).await?;
        if !question_message.components.is_empty() {
            return Err("question card unexpectedly had Allow/Deny components".to_owned());
        }
        let expected_question =
            "Which environment should the fix target?\n1. staging\n2. production";
        let description = question_message
            .embeds
            .first()
            .ok_or("question card had no embed")?
            .description
            .as_deref()
            .ok_or("question card embed had no description")?;
        if description != expected_question {
            return Err(format!(
                "question card description was {description:?}, expected {expected_question:?}"
            ));
        }
        let expected_owner_mention = format!("<@{owner_id}>");
        if question_message.content != expected_owner_mention {
            return Err(format!(
                "question card content was {:?}, expected {expected_owner_mention:?}",
                question_message.content
            ));
        }
        Ok(())
    }

    #[cfg(unix)]
    async fn fetch_message(
        guild: &BlockedCaptureGuild,
        channel: Id<ChannelMarker>,
        message: Id<MessageMarker>,
    ) -> Result<twilight_model::channel::Message, String> {
        guild
            .client
            .message(channel, message)
            .await
            .map_err(|e| e.to_string())?
            .model()
            .await
            .map_err(|e| e.to_string())
    }

    #[cfg(unix)]
    struct Tab {
        tab_id: String,
        pane_id: String,
    }

    #[cfg(unix)]
    const SUBSCRIBE_LABEL: &str = "testrun-subscribe";

    #[cfg(unix)]
    const SEQ_BACKSTOP_LABEL: &str = "testrun-seq-backstop";

    #[cfg(unix)]
    fn create_tab(label: &str, workspace_id: &str, cwd: &str) -> Result<Tab, String> {
        let created = herdr_json(&[
            "tab",
            "create",
            "--workspace",
            workspace_id,
            "--cwd",
            cwd,
            "--label",
            label,
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
    fn close_tab(tab_id: &str) {
        let _ = Command::new("herdr")
            .args(["tab", "close", tab_id])
            .output();
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
    fn snapshot_for_pane(pane_id: &str) -> Result<AgentSnapshot, String> {
        list_agents()?
            .into_iter()
            .find(|agent| agent.pane_id.as_deref() == Some(pane_id))
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
        (Arc::clone(&guild.client), guild.id, owner_id, responder)
    }

    #[cfg(unix)]
    fn thread_has_stopped_card(messages: &[twilight_model::channel::Message]) -> bool {
        messages.iter().any(|message| {
            message
                .embeds
                .first()
                .and_then(|embed| embed.description.as_deref())
                == Some("agent stopped, no log available")
        })
    }

    #[cfg(unix)]
    fn subscribe_tab_fixture() -> Result<(Tab, PathBuf), String> {
        let workspace_id = std::env::var("HERDR_WORKSPACE_ID").map_err(|_| {
            "HERDR_WORKSPACE_ID is set by the real Herdr pane environment".to_owned()
        })?;
        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-subscribe-{}-{}",
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
        match create_tab(SUBSCRIBE_LABEL, &workspace_id, cwd) {
            Ok(tab) => Ok((tab, cwd_dir)),
            Err(error) => {
                let _ = fs::remove_dir_all(&cwd_dir);
                Err(error)
            }
        }
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
            let mut sub = subscribe_herdr_events(&status_subscriptions(std::slice::from_ref(&tab.pane_id)))
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
                snapshot.cwd.is_some() || snapshot.session.is_some() || snapshot.state_change_seq > 0,
                "doorbell agent.list must carry cwd, reported session, or state_change_seq: {snapshot:?}"
            );
            Ok::<(), String>(())
        }
        .await;
        close_tab(&tab.tab_id);
        let _ = fs::remove_dir_all(&cwd_dir);
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
    async fn lifecycle_created_resubscribes_status_and_doorbells() {
        assert_eq!(
            remaining_tabs(SUBSCRIBE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        let mut lifecycle = subscribe_herdr_events(&lifecycle_subscriptions())
            .await
            .expect("lifecycle subscribe");
        let (tab, cwd_dir) = subscribe_tab_fixture().expect("create testrun tab");
        let result = async {
            wait_for_event(
                &mut lifecycle,
                "pane_created",
                &tab.pane_id,
                "/data/pane/pane_id",
                None,
                Duration::from_secs(10),
            )
            .await?;
            let mut status = subscribe_status(std::slice::from_ref(&tab.pane_id))
                .await?
                .ok_or_else(|| "status subscribe requires pane ids".to_owned())?;
            report_agent_state(&tab.pane_id, "working")?;
            wait_for_event(
                &mut status,
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
        let _ = fs::remove_dir_all(&cwd_dir);
        let tabs_left =
            remaining_tabs(SUBSCRIBE_LABEL).expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
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
            let _ = fs::remove_dir_all(&closed_cwd_dir);

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
            let _ = fs::remove_dir_all(&live_cwd_dir);
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
    async fn seed_then_working_then_done_card(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
    ) -> Result<(), String> {
        report_agent_state(&tab.pane_id, "idle")?;
        wait_for_status(&tab.pane_id, &["idle", "done"], Duration::from_secs(10)).await?;
        let listed = snapshot_for_pane(&tab.pane_id)?;
        let terminal = listed.terminal_id.clone();
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&listed);
        let mut state = BridgeState::default();
        let connection = discord_tuple(guild);
        process_snapshot(&listed, agents, tabs, Some(&connection), &mut state).await;

        let route = route_topology(agents, tabs, &terminal)?;
        let topology_cache: herdr_connect_rs::TopologyCache =
            Arc::new(tokio::sync::Mutex::new(None));
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;
        let before = guild
            .client
            .channel_messages(thread)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        if thread_has_stopped_card(&before) {
            return Err("silent seed posted a transition card".to_owned());
        }

        let mut sub =
            subscribe_herdr_events(&status_subscriptions(std::slice::from_ref(&tab.pane_id)))
                .await?;
        report_agent_state(&tab.pane_id, "working")?;
        wait_for_event(
            &mut sub,
            "pane.agent_status_changed",
            &tab.pane_id,
            "/data/pane_id",
            Some("working"),
            Duration::from_secs(10),
        )
        .await?;
        let working = snapshot_for_pane(&tab.pane_id)?;
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&working);
        process_snapshot(&working, agents, tabs, Some(&connection), &mut state).await;

        report_agent_state(&tab.pane_id, "idle")?;
        loop {
            let event = wait_for_event(
                &mut sub,
                "pane.agent_status_changed",
                &tab.pane_id,
                "/data/pane_id",
                None,
                Duration::from_secs(10),
            )
            .await?;
            if matches!(
                event.pointer("/data/agent_status").and_then(Value::as_str),
                Some("done" | "idle")
            ) {
                break;
            }
        }
        let settled = snapshot_for_pane(&tab.pane_id)?;
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&settled);
        process_snapshot(&settled, agents, tabs, Some(&connection), &mut state).await;

        let messages = guild
            .client
            .channel_messages(thread)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        if thread_has_stopped_card(&messages) {
            Ok(())
        } else {
            Err("idle -> working -> settled via subscribe did not post a card".to_owned())
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
    #[tokio::test]
    #[serial]
    async fn subscribe_idle_working_done_posts_transition_card() {
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
            remaining_tabs(SUBSCRIBE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let created = subscribe_tab_fixture();
        let (tab_id, cwd_dir, result) = match created {
            Ok((tab, cwd_dir)) => {
                let outcome = seed_then_working_then_done_card(&guild, &tab).await;
                (Some(tab.tab_id), Some(cwd_dir), outcome)
            }
            Err(error) => (None, None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        if let Some(cwd_dir) = cwd_dir {
            let _ = fs::remove_dir_all(cwd_dir);
        }

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left =
            remaining_tabs(SUBSCRIBE_LABEL).expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    /// Drives a real pane through a settled round-trip fast enough that only the settled status
    /// is observed between snapshots; the seq backstop must still post a card. When
    /// `same_status_collapse` is set, the baseline is first driven to `done` so the round-trip
    /// collapses onto the same status instead of advancing from `idle`; `scenario` names the case
    /// for the failure message.
    #[cfg(unix)]
    async fn seq_backstop_round_trip(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        same_status_collapse: bool,
        scenario: &str,
    ) -> Result<(), String> {
        report_agent_state(&tab.pane_id, "idle")?;
        let idle_baseline =
            wait_for_status(&tab.pane_id, &["idle"], Duration::from_secs(10)).await?;
        let baseline = if same_status_collapse {
            report_agent_state(&tab.pane_id, "working")?;
            report_agent_state(&tab.pane_id, "idle")?;
            wait_for_status(&tab.pane_id, &["done"], Duration::from_secs(10)).await?
        } else {
            idle_baseline
        };
        let terminal = baseline.terminal_id.clone();

        let mut state = BridgeState::default();
        process_snapshot(
            &baseline,
            std::slice::from_ref(&baseline),
            &[],
            None,
            &mut state,
        )
        .await;

        report_agent_state(&tab.pane_id, "working")?;
        report_agent_state(&tab.pane_id, "idle")?;
        let settled = wait_for_status(&tab.pane_id, &["done"], Duration::from_secs(10)).await?;
        if same_status_collapse && settled.agent_status != baseline.agent_status {
            return Err(format!(
                "expected same-status collapse on {}, saw {} -> {}",
                baseline.agent_status, baseline.agent_status, settled.agent_status
            ));
        }
        if settled.state_change_seq <= baseline.state_change_seq {
            return Err(format!(
                "herdr state_change_seq did not advance between snapshots: baseline={} settled={}",
                baseline.state_change_seq, settled.state_change_seq
            ));
        }

        let matching_tab = tab_list_result()?
            .into_iter()
            .find(|candidate| candidate.tab_id == tab.tab_id)
            .ok_or_else(|| format!("tab.list has no entry for {}", tab.tab_id))?;
        let tabs = std::slice::from_ref(&matching_tab);
        let agents = std::slice::from_ref(&settled);
        let route = route_topology(agents, tabs, &terminal)?;

        let owner_id = std::env::var("DISCORD_OWNER_ID").map_err(|e| e.to_string())?;
        let responder = Arc::new(PermissionResponder::new(
            Arc::clone(&guild.client),
            guild.id,
            owner_id.clone(),
            Arc::new(tokio::sync::Mutex::new(None)),
        ));
        let connection = (Arc::clone(&guild.client), guild.id, owner_id, responder);

        process_snapshot(&settled, agents, tabs, Some(&connection), &mut state).await;

        let topology_cache: herdr_connect_rs::TopologyCache =
            Arc::new(tokio::sync::Mutex::new(None));
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;
        let messages = guild
            .client
            .channel_messages(thread)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        if thread_has_stopped_card(&messages) {
            Ok(())
        } else {
            Err(format!("seq backstop ({scenario}) did not post a card"))
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn seq_backstop_between_snapshots_still_posts_a_card() {
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
            remaining_tabs(SEQ_BACKSTOP_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
            .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-seq-backstop-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create seq-backstop test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_tab(SEQ_BACKSTOP_LABEL, &workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let outcome = seq_backstop_round_trip(
                    &guild,
                    &tab,
                    false,
                    "idle -> working -> done between snapshots",
                )
                .await;
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(SEQ_BACKSTOP_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn seq_backstop_same_status_collapse_still_posts_a_card() {
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
            remaining_tabs(SEQ_BACKSTOP_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
            .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-seq-same-status-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create seq-same-status test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_tab(SEQ_BACKSTOP_LABEL, &workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let outcome = seq_backstop_round_trip(
                    &guild,
                    &tab,
                    true,
                    "done -> working -> done same-status collapse",
                )
                .await;
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(SEQ_BACKSTOP_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const SEQ_DEDUP_LABEL: &str = "testrun-seq-dedup";

    /// Real on-disk Claude project directory a session log for `cwd` resolves under, mirroring
    /// `resolve_session_path`'s slug so the test can place a fixture where production code will
    /// read it.
    #[cfg(unix)]
    fn claude_session_project_dir(home: &Path, cwd: &str) -> PathBuf {
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
        home.join(".claude-one/projects").join(cwd_slug)
    }

    #[cfg(unix)]
    fn claude_session_log_path(home: &Path, cwd: &str, session_id: &str) -> PathBuf {
        claude_session_project_dir(home, cwd).join(format!("{session_id}.jsonl"))
    }

    /// Appends one user/assistant turn to a real Claude session log, in the same record shape as
    /// `tests/fixtures/claude-session.jsonl`.
    #[cfg(unix)]
    fn append_claude_turn(path: &Path, prompt: &str, reply: &str) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| error.to_string())?;
        for record in [
            json!({
                "type": "user",
                "message": {"role": "user", "content": [{"type": "text", "text": prompt}]},
            }),
            json!({
                "type": "assistant",
                "message": {"role": "assistant", "content": [{"type": "text", "text": reply}]},
            }),
        ] {
            writeln!(file, "{record}").map_err(|error| error.to_string())?;
        }
        Ok(())
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

    #[cfg(unix)]
    async fn thread_card_descriptions(
        guild: &BlockedCaptureGuild,
        thread: Id<ChannelMarker>,
    ) -> Result<Vec<String>, String> {
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
            .filter_map(|message| {
                message
                    .embeds
                    .into_iter()
                    .next()
                    .and_then(|embed| embed.description)
            })
            .collect())
    }

    /// Drives one real pane carrying a real reported Claude session through the seq backstop
    /// with `state_change_seq` set by hand: Herdr freezes its own counter against synthetic
    /// `report-agent` state reports once a pane carries a session, so the counter cannot be
    /// advanced through Herdr itself for this scenario. Everything else stays real: a real Herdr
    /// tab, a real reported session, a real on-disk session log, and a real Discord thread.
    #[cfg(unix)]
    async fn seq_backstop_session_dedup_round_trip(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        home: &Path,
        cwd: &str,
    ) -> Result<(), String> {
        report_agent_state(&tab.pane_id, "idle")?;

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| error.to_string())?
            .as_nanos();
        let session_id = format!(
            "{:08x}-{:04x}-4{:03x}-8{:03x}-{:012x}",
            std::process::id(),
            (nanos >> 48) & 0xffff,
            (nanos >> 36) & 0xfff,
            (nanos >> 24) & 0xfff,
            nanos & 0xffff_ffff_ffff,
        );
        report_agent_session(&tab.pane_id, &session_id)?;

        let confirmed = snapshot_for_pane(&tab.pane_id)?;
        let expected_session = AgentSession {
            agent: "claude".to_owned(),
            value: session_id.clone(),
        };
        if confirmed.session.as_ref() != Some(&expected_session) {
            return Err(format!(
                "expected session {expected_session:?} on pane {}, agent.list reported {confirmed:?}",
                tab.pane_id
            ));
        }
        let terminal = confirmed.terminal_id.clone();

        let log_path = claude_session_log_path(home, cwd, &session_id);
        append_claude_turn(&log_path, "first prompt", "reply one")?;

        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let route = route_topology(std::slice::from_ref(&confirmed), tabs, &terminal)?;
        let connection = discord_tuple(guild);
        let topology_cache: herdr_connect_rs::TopologyCache =
            Arc::new(tokio::sync::Mutex::new(None));
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;

        let base_seq = confirmed.state_change_seq;
        let mut state = BridgeState {
            previous: HashMap::from([(
                terminal.clone(),
                (confirmed.agent_status.clone(), confirmed.agent.clone()),
            )]),
            herdr_state_change_seq: HashMap::from([(terminal.clone(), base_seq.saturating_sub(1))]),
            ..Default::default()
        };
        let mut snapshot = confirmed.clone();

        snapshot.state_change_seq = base_seq;
        process_snapshot(
            &snapshot,
            std::slice::from_ref(&snapshot),
            tabs,
            Some(&connection),
            &mut state,
        )
        .await;
        let after_first = thread_card_descriptions(guild, thread).await?;
        if after_first.len() != 1 || after_first.first().map(String::as_str) != Some("reply one") {
            return Err(format!(
                "expected exactly one card carrying \"reply one\" after the baseline settled snapshot, thread has {after_first:?}"
            ));
        }

        snapshot.state_change_seq = base_seq + 1;
        process_snapshot(
            &snapshot,
            std::slice::from_ref(&snapshot),
            tabs,
            Some(&connection),
            &mut state,
        )
        .await;
        let after_duplicate = thread_card_descriptions(guild, thread).await?;
        if after_duplicate != after_first {
            return Err(format!(
                "expected the duplicate settled snapshot (unchanged session log) to post no new card, thread now has {after_duplicate:?}"
            ));
        }

        append_claude_turn(&log_path, "second prompt", "reply two")?;
        snapshot.state_change_seq = base_seq + 2;
        process_snapshot(
            &snapshot,
            std::slice::from_ref(&snapshot),
            tabs,
            Some(&connection),
            &mut state,
        )
        .await;
        let after_new_turn = thread_card_descriptions(guild, thread).await?;
        if after_new_turn.len() != 2 || !after_new_turn.contains(&"reply two".to_owned()) {
            return Err(format!(
                "expected exactly one new card carrying \"reply two\" after the new-turn settled snapshot, thread now has {after_new_turn:?}"
            ));
        }

        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn seq_backstop_session_dedup_suppresses_duplicate_and_posts_new_turn() {
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
            remaining_tabs(SEQ_DEDUP_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
            .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-seq-dedup-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create seq-dedup test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");
        let project_dir = claude_session_project_dir(&home, cwd);

        let created = create_tab(SEQ_DEDUP_LABEL, &workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let outcome = seq_backstop_session_dedup_round_trip(&guild, &tab, &home, cwd).await;
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);
        let _ = fs::remove_dir_all(&project_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left =
            remaining_tabs(SEQ_DEDUP_LABEL).expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert!(
            !project_dir.exists(),
            "named zero-leftover check: claude-one project directory removed"
        );
    }

    #[cfg(unix)]
    const STARTUP_TOPOLOGY_LABEL: &str = "testrun-startup-topology";

    #[cfg(unix)]
    async fn startup_topology_sync_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
    ) -> Result<(), String> {
        report_agent_state(&tab.pane_id, "idle")?;
        let listed = snapshot_for_pane(&tab.pane_id)?;
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&listed);

        let connection = discord_tuple(guild);
        sync_startup_topology(&connection, agents, tabs).await;

        let route = route_topology(agents, tabs, &listed.terminal_id)?;
        let topic = format!("herdr workspace [{}]", route.workspace_id);
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
    async fn startup_topology_sync_creates_channel_and_thread_before_event_loop() {
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
        fs::create_dir_all(&cwd_dir).expect("create startup-topology test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_tab(STARTUP_TOPOLOGY_LABEL, &workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let outcome = startup_topology_sync_exercise(&guild, &tab).await;
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(STARTUP_TOPOLOGY_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const PREFETCH_REUSE_LABEL: &str = "testrun-startup-prefetch-reuse";

    #[cfg(unix)]
    async fn startup_topology_prefetch_reuse_exercise(
        guild: &BlockedCaptureGuild,
        tab_a: &Tab,
        tab_b: &Tab,
    ) -> Result<(), String> {
        report_agent_state(&tab_a.pane_id, "idle")?;
        report_agent_state(&tab_b.pane_id, "idle")?;
        let agent_a = snapshot_for_pane(&tab_a.pane_id)?;
        let agent_b = snapshot_for_pane(&tab_b.pane_id)?;
        let tabs = [matching_tab(&tab_a.tab_id)?, matching_tab(&tab_b.tab_id)?];
        let agents = [agent_a.clone(), agent_b.clone()];

        let connection = discord_tuple(guild);
        sync_startup_topology(&connection, &agents, &tabs).await;

        let route_a = route_topology(&agents, &tabs, &agent_a.terminal_id)?;
        let route_b = route_topology(&agents, &tabs, &agent_b.terminal_id)?;
        let topic = format!("herdr workspace [{}]", route_a.workspace_id);
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

        let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
            .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
        let make_cwd = |suffix: &str| {
            let dir = std::env::temp_dir().join(format!(
                "testrun-cwd-{}-{}-{suffix}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is after unix epoch")
                    .as_nanos()
            ));
            fs::create_dir_all(&dir).expect("create prefetch-reuse test cwd");
            dir
        };
        let cwd_a_dir = make_cwd("a");
        let cwd_b_dir = make_cwd("b");
        let cwd_a = cwd_a_dir.to_str().expect("temp cwd is valid UTF-8");
        let cwd_b = cwd_b_dir.to_str().expect("temp cwd is valid UTF-8");

        let created_a = create_tab(PREFETCH_REUSE_LABEL, &workspace_id, cwd_a);
        let created_b = create_tab(PREFETCH_REUSE_LABEL, &workspace_id, cwd_b);
        let (tab_ids, result) = match (created_a, created_b) {
            (Ok(tab_a), Ok(tab_b)) => {
                let outcome =
                    startup_topology_prefetch_reuse_exercise(&guild, &tab_a, &tab_b).await;
                (vec![tab_a.tab_id, tab_b.tab_id], outcome)
            }
            (Ok(tab_a), Err(error)) => (vec![tab_a.tab_id], Err(error)),
            (Err(error), Ok(tab_b)) => (vec![tab_b.tab_id], Err(error)),
            (Err(error), Err(_)) => (vec![], Err(error)),
        };
        for tab_id in &tab_ids {
            close_tab(tab_id);
        }
        let _ = fs::remove_dir_all(&cwd_a_dir);
        let _ = fs::remove_dir_all(&cwd_b_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(PREFETCH_REUSE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const STARTUP_RACE_LABEL: &str = "testrun-startup-race";

    #[cfg(unix)]
    async fn startup_and_delivery_race_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
    ) -> Result<(), String> {
        report_agent_state(&tab.pane_id, "idle")?;
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
        tab: &Tab,
    ) -> Result<(), String> {
        report_agent_state(&tab.pane_id, "idle")?;
        let listed = snapshot_for_pane(&tab.pane_id)?;
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&listed);
        let route = route_topology(agents, tabs, &listed.terminal_id)?;

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

        let topic = format!("herdr workspace [{}]", route.workspace_id);
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
    async fn startup_sweep_duplicates_thread_after_dropped_write_back() {
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
        fs::create_dir_all(&cwd_dir).expect("create stale-cache test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_tab(STALE_CACHE_LABEL, &workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let outcome = startup_sweep_stale_cache_exercise(&guild, &tab).await;
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(STALE_CACHE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }
}
