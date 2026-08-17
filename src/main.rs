use herdr_connect_rs::{
    AgentLogCapture, AgentSession, AgentSnapshot, ComponentHandler, HerdrTab, TopologyRoute,
    Transition, TransitionMessage, create_transition_messages, create_unsupported_blocked_card,
    deliver_transition_card, drive_gateway_with_components, expire_informational_card,
    hook_timeout, is_postable_transition, list_agents, load_config, load_discord_config,
    route_topology, sync_topology, tab_list_result, transition_card_nonce,
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
    blocked_since: HashMap<String, Instant>,
    informational_cards: HashMap<String, InformationalCard>,
    blocked_capture_attempts: HashMap<String, u32>,
    herdr_state_change_seq: HashMap<String, u64>,
}

struct BlockedCardContext<'a> {
    client: &'a Client,
    guild: Id<GuildMarker>,
    owner_id: &'a str,
    responder: &'a PermissionResponder,
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

/// Bounded number of blocked-poll capture attempts before falling back to the
/// informational card. The herdr "blocked" status can flip before the vendor log
/// file is flushed with the pending question, so a single capture miss is not
/// treated as "no question" — it is retried on the next few polls instead.
const MAX_BLOCKED_CAPTURE_ATTEMPTS: u32 = 3;

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
    let target = match sync_route(client, guild, route).await {
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
    let capture = search_root.map_or_else(
        || capture_for_blocked(snapshot),
        |root| capture_for_blocked_with_search_root(snapshot, root),
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

/// Detects a completed turn Herdr's own poll cadence missed: the pane went `working` and back to
/// a settled status between two bridge polls, so the naive `old` to `status` transition never saw
/// `working` as its `from`. Herdr's own `state_change_seq` advancing past what the previous poll
/// recorded for this terminal is the only signal available to catch this without polling faster.
#[must_use]
fn is_missed_fast_turn(
    transition: &Transition,
    previous_herdr_seq: Option<u64>,
    current_herdr_seq: u64,
) -> bool {
    !is_postable_transition(transition)
        && matches!(transition.to.as_str(), "idle" | "done")
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
    if let Some((old, prior_agent)) = state.previous.get(&terminal).cloned()
        && old != status
    {
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
        if is_missed_fast_turn(&transition, previous_herdr_seq, snapshot.state_change_seq) {
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
    let Some((client, guild, owner_id, responder)) = discord else {
        return Ok(());
    };
    if transition.to == "blocked" {
        handle_blocked_card(BlockedCardContext {
            client: client.as_ref(),
            guild: *guild,
            owner_id,
            responder: responder.as_ref(),
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
    deliver_to_route(
        client.as_ref(),
        *guild,
        owner_id,
        &route,
        transition,
        &capture,
        state_change_seq,
    )
    .await?;
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
    client: &Client,
    guild: Id<GuildMarker>,
    owner_id: &str,
    route: &TopologyRoute,
    transition: &Transition,
    capture: &AgentLogCapture,
    state_change_seq: u64,
) -> Result<Id<MessageMarker>, String> {
    let messages = create_transition_messages(transition, capture, owner_id);
    let target = sync_route(client, guild, route).await?;
    let mut last_message_id = None;
    for (index, message) in messages.iter().enumerate() {
        let nonce = transition_card_nonce(&transition.terminal_id, state_change_seq, index);
        last_message_id = Some(
            deliver_transition_card(client, target, message, &nonce)
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
) -> Result<Id<ChannelMarker>, String> {
    sync_topology(
        client,
        guild,
        &route.workspace_id,
        &route.channel_name,
        &route.thread_name,
        &route.tab_id,
    )
    .await
    .map_err(|error| format!("discord topology error: {error}"))
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
        .blocked_capture_attempts
        .retain(|terminal, _| current.contains(terminal));
    state
        .herdr_state_change_seq
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

fn discord_connection()
-> Result<Option<(DiscordConnection, GatewayTask)>, Box<dyn std::error::Error>> {
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
    let responder = Arc::new(PermissionResponder::new(
        Arc::clone(&client),
        guild,
        config.owner_id.clone(),
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

async fn run_bridge() -> Result<(), Box<dyn std::error::Error>> {
    let (discord, mut gateway, mut broker) = match discord_connection()? {
        Some((connection, gateway)) => {
            let broker = start_broker(&connection);
            (Some(connection), Some(gateway), broker)
        }
        None => (None, None, None),
    };
    let interval = load_config().poll_interval_ms;
    let mut state = BridgeState::default();
    let mut stop = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    loop {
        let agents = match list_agents() {
            Ok(agents) => agents,
            Err(error) => {
                eprintln!("herdr poll error: {error}");
                tokio::time::sleep(Duration::from_millis(interval)).await;
                continue;
            }
        };
        let tabs = match tab_list_result() {
            Ok(tabs) => tabs,
            Err(error) => {
                eprintln!("herdr tab poll error: {error}");
                tokio::time::sleep(Duration::from_millis(interval)).await;
                continue;
            }
        };
        let current: HashSet<String> = agents.iter().map(|s| s.terminal_id.clone()).collect();
        state
            .previous
            .retain(|terminal, _| current.contains(terminal));
        let departed_cards = prune_departed_state(&mut state, &current);
        for (terminal, card) in departed_cards {
            expire_departed_card(discord.as_ref(), &terminal, card).await;
        }
        for snapshot in &agents {
            process_snapshot(snapshot, &agents, &tabs, discord.as_ref(), &mut state).await;
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(interval)) => {},
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
    abort_broker(&mut broker);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        BlockedCardContext, BlockedResponse, BridgeState, Client, InformationalCard,
        PermissionResponder, TopologyRoute, capture_for_with_search_root,
        create_transition_messages, decide_blocked_response, handle_blocked_card,
        is_missed_fast_turn, list_agents, next_state_change_sequence, process_snapshot,
        prune_departed_state, resolve_session_path, route_topology, sync_route, tab_list_result,
    };
    use herdr_connect_rs::{AgentSession, AgentSnapshot, Transition, transition_card_nonce};
    use serde_json::Value;
    use serial_test::serial;
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::path::Path;
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
        }

        let current_terminals = HashSet::from([current.to_owned()]);
        let expired = prune_departed_state(&mut state, &current_terminals);

        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, departed);
        assert!(!state.blocked_since.contains_key(departed));
        assert!(!state.state_change_sequences.contains_key(departed));
        assert!(!state.herdr_state_change_seq.contains_key(departed));
        assert!(!state.informational_cards.contains_key(departed));
        assert!(state.blocked_since.contains_key(current));
        assert!(state.state_change_sequences.contains_key(current));
        assert!(state.herdr_state_change_seq.contains_key(current));
        assert!(state.informational_cards.contains_key(current));
    }

    #[test]
    fn missed_fast_turn_is_detected_only_when_seq_advanced_and_status_settled() {
        let transition = |from: &str, to: &str| Transition {
            from: from.to_owned(),
            to: to.to_owned(),
            terminal_id: "terminal".to_owned(),
            agent: "claude".to_owned(),
        };
        let cases = [
            (
                "seq advanced, settled: missed",
                transition("idle", "done"),
                Some(10),
                11,
                true,
            ),
            (
                "seq unchanged: nothing missed",
                transition("idle", "done"),
                Some(11),
                11,
                false,
            ),
            (
                "no prior seq recorded: cannot tell",
                transition("idle", "done"),
                None,
                11,
                false,
            ),
            (
                "still blocked: not settled",
                transition("idle", "blocked"),
                Some(10),
                11,
                false,
            ),
            (
                "already caught normally",
                transition("working", "done"),
                Some(10),
                11,
                false,
            ),
        ];
        for (label, transition, previous_seq, current_seq, expected) in cases {
            assert_eq!(
                is_missed_fast_turn(&transition, previous_seq, current_seq),
                expected,
                "{label}: {transition:?} previous_seq={previous_seq:?} current_seq={current_seq}"
            );
        }
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
    async fn blocked_capture_cleanup(guild: &BlockedCaptureGuild) -> Result<usize, String> {
        let is_test_channel = |channel: &twilight_model::channel::Channel| {
            channel
                .name
                .as_deref()
                .is_some_and(|name| name.starts_with("testrun-"))
        };
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
                .count();
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
    struct MissedTurnTab {
        tab_id: String,
        pane_id: String,
    }

    #[cfg(unix)]
    const MISSED_TURN_LABEL: &str = "testrun-missed-turn";

    #[cfg(unix)]
    fn create_missed_turn_tab(workspace_id: &str, cwd: &str) -> Result<MissedTurnTab, String> {
        let created = herdr_json(&[
            "tab",
            "create",
            "--workspace",
            workspace_id,
            "--cwd",
            cwd,
            "--label",
            MISSED_TURN_LABEL,
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
        Ok(MissedTurnTab { tab_id, pane_id })
    }

    #[cfg(unix)]
    fn close_missed_turn_tab(tab_id: &str) {
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
            MISSED_TURN_LABEL,
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
    fn remaining_missed_turn_tabs() -> Result<usize, String> {
        Ok(tab_list_result()?
            .into_iter()
            .filter(|tab| tab.label == MISSED_TURN_LABEL)
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
    async fn wait_for_status(
        pane_id: &str,
        status: &str,
        bound: Duration,
    ) -> Result<AgentSnapshot, String> {
        let start = Instant::now();
        loop {
            let snapshot = snapshot_for_pane(pane_id)?;
            if snapshot.agent_status == status {
                return Ok(snapshot);
            }
            if start.elapsed() > bound {
                return Err(format!(
                    "pane {pane_id} did not reach status {status} within {bound:?}, last saw {}",
                    snapshot.agent_status
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Drives a real pane through `idle -> working -> idle` fast enough that both transitions land
    /// inside one `report_agent_state` round trip, so the bridge's own poll only ever observes the
    /// naive `idle -> done` transition; the missed-turn detection is what must still post a card.
    #[cfg(unix)]
    async fn missed_fast_turn_round_trip(
        guild: &BlockedCaptureGuild,
        tab: &MissedTurnTab,
    ) -> Result<(), String> {
        report_agent_state(&tab.pane_id, "idle")?;
        let baseline = wait_for_status(&tab.pane_id, "idle", Duration::from_secs(10)).await?;
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
        let missed = wait_for_status(&tab.pane_id, "done", Duration::from_secs(10)).await?;
        if missed.state_change_seq <= baseline.state_change_seq {
            return Err(format!(
                "herdr state_change_seq did not advance between polls: baseline={} missed={}",
                baseline.state_change_seq, missed.state_change_seq
            ));
        }

        let matching_tab = tab_list_result()?
            .into_iter()
            .find(|candidate| candidate.tab_id == tab.tab_id)
            .ok_or_else(|| format!("tab.list has no entry for {}", tab.tab_id))?;
        let tabs = std::slice::from_ref(&matching_tab);
        let agents = std::slice::from_ref(&missed);
        let route = route_topology(agents, tabs, &terminal)?;

        let owner_id = std::env::var("DISCORD_OWNER_ID").map_err(|e| e.to_string())?;
        let responder = Arc::new(PermissionResponder::new(
            Arc::clone(&guild.client),
            guild.id,
            owner_id.clone(),
        ));
        let connection = (Arc::clone(&guild.client), guild.id, owner_id, responder);

        process_snapshot(&missed, agents, tabs, Some(&connection), &mut state).await;

        let thread = sync_route(guild.client.as_ref(), guild.id, &route).await?;
        let messages = guild
            .client
            .channel_messages(thread)
            .await
            .map_err(|error| error.to_string())?
            .model()
            .await
            .map_err(|error| error.to_string())?;
        let posted = messages.iter().any(|message| {
            message
                .embeds
                .first()
                .and_then(|embed| embed.description.as_deref())
                == Some("agent stopped, no log available")
        });
        if posted {
            Ok(())
        } else {
            Err(
                "missed fast turn (idle -> working -> done inside one poll) did not post a card"
                    .to_owned(),
            )
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn missed_fast_turn_between_polls_still_posts_a_card() {
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
            remaining_missed_turn_tabs().expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
            .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-missed-turn-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create missed-turn test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_missed_turn_tab(&workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let outcome = missed_fast_turn_round_trip(&guild, &tab).await;
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_missed_turn_tab(tab_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left =
            remaining_missed_turn_tabs().expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
    }
}
