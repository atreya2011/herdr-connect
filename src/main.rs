use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use notify::Watcher;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use twilight_http::Client;
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, MessageMarker, UserMarker, WebhookMarker},
};

use herdr_connect_rs::{
    ACTIVITY_KIND, ActivityFrame, activity_message_text, decode_claude_activity_request,
    decode_codex_activity_request, decode_cursor_activity_request, deliver_activity_message,
    send_activity_frame, update_activity_message,
};
use herdr_connect_rs::{
    AgentLogCapture, AgentSession, AgentSnapshot, ComponentHandler, ENV_DISCORD_GUILD_ID,
    ENV_DISCORD_OWNER_ID, ENV_DISCORD_TOKEN, ENV_HOME, EVENT_KEY, HerdrSubscription, HerdrTab,
    OwnerIdentity, RouteError, STATUS_BLOCKED, STATUS_DONE, STATUS_IDLE, STATUS_WORKING,
    TopologyCache, TopologyRoute, Transition, TransitionMessage, UNKNOWN_CHANNEL_DELIVERY_ERROR,
    UNKNOWN_WEBHOOK_DELIVERY_ERROR, agent_read_detection, cached_route, claude_turn_start_position,
    codex_turn_start_position, create_transition_messages, create_unsupported_blocked_card,
    cursor_turn_start_rowid, delete_tab_thread, delete_topology_absent_from_herdr,
    delete_workspace_channel, deliver_live_message, deliver_transition_card,
    drive_gateway_with_components, execute_terminal_prompt_webhook, expire_informational_card,
    fetch_owner_identity, fetch_topology_lists, format_detection_question, hook_timeout,
    is_postable_transition, lifecycle_subscriptions, list_agents, live_message_nonce,
    load_discord_config, read_claude_incremental, read_claude_prompts_incremental,
    read_codex_incremental, read_codex_prompts_incremental, read_cursor_incremental,
    read_cursor_prompts_incremental, reconcile_topology_cache, resolve_terminal_prompt_webhook,
    route_topology, split_live_message, status_subscriptions, subscribe_herdr_events,
    sync_topology, tab_list_result, take_owner_prompt_suppression, transition_card_nonce,
    workspace_channel_id, workspace_list_result,
};
use herdr_connect_rs::{
    Decision, Interaction, PermissionResponder, PermissionVendor, VENDOR_CLAUDE, VENDOR_CODEX,
    VENDOR_CURSOR, decode_claude_permission_request, decode_codex_permission_request,
    decode_cursor_permission_request, encode_claude_decision, encode_codex_decision,
    encode_cursor_decision, handle_component, request_decision,
    run_broker as run_permission_broker,
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

/// Where an incremental vendor-log reader resumes from: a byte offset for the Claude JSONL log, a
/// `rowid` for the Cursor sqlite store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LivePosition {
    Bytes(u64),
    RowId(i64),
}

/// Dropping this stops its `notify` watcher.
struct LiveWatch {
    _watcher: notify::RecommendedWatcher,
    vendor: String,
    path: PathBuf,
    position: LivePosition,
    /// The tab thread this watch currently posts to. Re-resolved from `route` and updated in
    /// place when a delivery finds it gone (deleted outside the bridge's own tracking), so later
    /// events do not repeat that recovery.
    channel: Id<ChannelMarker>,
    /// Kept so a dead `channel` can be re-resolved without the caller having to supply fresh
    /// agent/tab snapshots (the live event loop that drives most deliveries has none in scope).
    route: TopologyRoute,
}

#[derive(Default)]
struct BridgeState {
    previous: HashMap<String, (String, String)>,
    state_change_sequences: HashMap<String, u64>,
    herdr_state_change_seq: HashMap<String, u64>,
    blocked_since: HashMap<String, Instant>,
    informational_cards: HashMap<String, InformationalCard>,
    blocked_capture_attempts: HashMap<String, u32>,
    /// Last reply card text delivered per terminal. A reply card whose captured text equals this
    /// entry is not posted again, whether it arrives on the status-change path or the
    /// seq-backstop path; a legitimately identical consecutive reply is intentionally not
    /// reposted.
    last_posted: HashMap<String, String>,
    /// Tab ids waiting on a cold-start terminal title: `route_topology` reported
    /// [`RouteError::TitlePending`] for them. `discover_pending_and_unusable_tabs` inserts one on
    /// every snapshot pass, independent of any status transition; a resolved delivery-path route
    /// or `sync_pending_titles` removes one once its title has arrived (the latter also creating
    /// its thread).
    title_pending: HashSet<String>,
    /// Tab ids already logged for an unusable Discord thread name, so a permanent
    /// [`RouteError::Unusable`] is surfaced once rather than on every later snapshot.
    unusable_reported: HashSet<String>,
    live_watches: HashMap<String, LiveWatch>,
    /// `None` in tests that never wire live capture up.
    live_tx: Option<tokio::sync::mpsc::UnboundedSender<String>>,
    /// Terminals to stop retrying a watch for: a non-transient `live_log_path` error, or a live
    /// delivery that kept failing for [`LIVE_DELIVERY_ATTEMPTS`] ticks in a row.
    live_unfollowable: HashSet<String>,
    /// Terminals whose live-capture read error was already logged once, so it is not repeated on
    /// every later event.
    live_read_errors_reported: HashSet<String>,
    /// Consecutive failed delivery ticks per terminal for the text currently stuck at the front of
    /// its live watch. Reset once that text is finally delivered; cleared once the terminal is
    /// marked unfollowable, since there is no longer a watch to retry.
    live_delivery_attempts: HashMap<String, u32>,
    /// One turn's activity message per pane, keyed by pane id (the identity an activity frame
    /// carries; a live watch's terminal id is a different Herdr identity for the same pane).
    /// Forgotten -- not deleted -- at the point the pane's transition card for that turn is
    /// posted, so the next turn starts a fresh message instead of editing the last one.
    activity_messages: HashMap<String, ActivityMessage>,
    /// Pane ids the latest Herdr snapshot reports as `working` with a session, kept fresh by
    /// every [`process_snapshot`] call (whether from the doorbell's `agent.list` sweep or a
    /// single-pane update). `handle_activity_event` checks this before creating a message: an
    /// activity frame that outlives its turn, or that names a pane with no reported session, has
    /// nothing to gate its creation without it.
    activity_eligible_panes: HashSet<String>,
    /// The owner's mirrored display name and avatar, fetched once at startup. `None` when Discord
    /// is not configured or the fetch failed; terminal-prompt mirroring drops silently without it.
    owner_identity: Option<OwnerIdentity>,
    /// Per-terminal read path and position for terminal-prompt mirroring, established the first
    /// time [`process_snapshot`] sees a session-carrying pane in any status -- not only `working`,
    /// and never reset by a later turn's fresh [`LiveWatch`] (unlike that watch's own turn-scoped
    /// `position`): existing prompts already in the log at that first sight are never replayed, but
    /// every prompt recorded afterward, across every later turn, is. Keyed by terminal id but
    /// valued by the resolved log path alongside the position, because a later session on the same
    /// terminal (`/clear`, resume, a relaunch, or a vendor starting a fresh file or store) resolves
    /// a different path; [`process_snapshot`] re-baselines against it rather than reusing a stale
    /// position from the old file.
    terminal_prompt_positions: HashMap<String, (PathBuf, LivePosition)>,
    /// Terminals first seen either with no session reported at all yet (`None`), or with a session
    /// whose [`live_log_path`] returned `Ok(None)` (`Some(session value)`: the log or store does not
    /// exist on disk yet) -- a fresh pane, a new session after `/clear` or a relaunch before its
    /// first write, or a vendor (Codex) that does not report a session identity until the pane is
    /// already `working`, by which point its log can already hold the very prompt that started the
    /// turn. A pane can switch to a *different*, already-populated session before the pending one's
    /// log ever appears (Claude `/resume`, or a relaunch straight into an existing session), and
    /// that resumed session's own, unrelated history must never be baselined at 0 just because this
    /// terminal happened to have an unrelated fresh session pending; the `None` entry carries no
    /// session to compare against, so it matches whichever session resolves first. Removed the
    /// moment a path resolves for this terminal, whichever session it belongs to:
    /// [`maybe_establish_terminal_prompt_baseline`] baselines at position 0 when the recorded entry
    /// is `None` or its session matches the resolved path's session; any other session (including
    /// one that was never recorded pending at all) gets the normal discard-what-already-exists
    /// baseline instead.
    terminal_prompt_awaiting_first_log: HashMap<String, Option<String>>,
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

/// Bounded number of consecutive failed delivery ticks for the same stuck live-capture text
/// before its terminal is marked [`BridgeState::live_unfollowable`] and its watch dropped. A
/// persistent failure (the bot loses `SEND_MESSAGES` in that thread; a thread archived rather
/// than deleted, which the unknown-channel recovery does not cover) would otherwise retry and log
/// once per `notify` tick for the rest of the turn.
const LIVE_DELIVERY_ATTEMPTS: u32 = 3;

/// Retries blocked-capture while a pane stays blocked without another status event.
const BLOCKED_CAPTURE_RETRY_INTERVAL: Duration = Duration::from_millis(1_500);

const SUBSCRIBE_RETRY_INITIAL: Duration = Duration::from_millis(250);
const SUBSCRIBE_RETRY_MAX: Duration = Duration::from_secs(30);

/// The bridge-owned webhook name every workspace channel's terminal-prompt webhook is looked up or
/// created under.
const TERMINAL_PROMPT_WEBHOOK_NAME: &str = "herdr-connect owner";

/// How long a lifecycle batch keeps draining after its most recent event before it is acted on. A
/// resubscribe replay burst pushes events back-to-back well inside this window, so the whole
/// burst is drained into one batch instead of triggering one doorbell and one topology fetch per
/// event.
const LIFECYCLE_BATCH_WINDOW: Duration = Duration::from_millis(100);
/// Upper bound on events drained into one lifecycle batch, so a pathological event storm still
/// yields control back to the rest of the event loop.
const LIFECYCLE_BATCH_CAP: usize = 1_000;

#[derive(Debug, PartialEq, Eq)]
enum BlockedResponse {
    Question,
    Retry,
    Unsupported,
}

#[must_use]
fn vendor_is_supported(agent: Option<&str>) -> bool {
    matches!(agent, Some(VENDOR_CLAUDE | VENDOR_CODEX))
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
    let vendor_supported = vendor_is_supported(snapshot.agent.as_deref());
    let supported_broker_pending = vendor_supported
        && snapshot
            .session
            .as_ref()
            .is_some_and(|session| responder.has_pending_session(&session.value));
    if supported_broker_pending {
        return;
    }
    let detection_question = (snapshot.agent.as_deref() == Some(VENDOR_CLAUDE))
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
                to: STATUS_BLOCKED.to_owned(),
                terminal_id: terminal.to_owned(),
                agent: snapshot.agent.clone().unwrap_or_default(),
            };
            create_transition_messages(&transition, &capture, owner_id)
        }
        BlockedResponse::Unsupported => {
            blocked_capture_attempts.remove(terminal);
            let blocked_age = blocked_since.map_or(Duration::ZERO, |started| {
                Instant::now().saturating_duration_since(*started)
            });
            vec![create_unsupported_blocked_card(
                snapshot.agent.as_deref().unwrap_or("none"),
                &route.pane_id,
                capture.question.as_deref().unwrap_or(&capture.message),
                owner_id,
                blocked_age,
            )]
        }
    };
    deliver_blocked_messages(
        &BlockedDeliveryRoute {
            client,
            guild,
            route,
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

/// What [`deliver_blocked_messages`] needs to re-resolve its route on a stale-thread retry,
/// bundled to keep the function under the argument-count lint.
#[derive(Clone, Copy)]
struct BlockedDeliveryRoute<'a> {
    client: &'a Client,
    guild: Id<GuildMarker>,
    route: &'a TopologyRoute,
    topology_cache: &'a TopologyCache,
}

/// Delivers each blocked-card message to `target`, applying the same invalidate-and-retry as
/// [`deliver_to_route`] when a send finds the cached thread gone: the shared topology cache is
/// cleared, the route is re-resolved once, and that message is retried at the recreated thread
/// before later messages reuse it too.
async fn deliver_blocked_messages(
    delivery: &BlockedDeliveryRoute<'_>,
    mut target: Id<ChannelMarker>,
    terminal: &str,
    state_change_seq: u64,
    messages: &[TransitionMessage],
    informational_cards: &mut HashMap<String, InformationalCard>,
) {
    let BlockedDeliveryRoute {
        client,
        guild,
        route,
        topology_cache,
    } = *delivery;
    let mut last = None;
    for (index, message) in messages.iter().enumerate() {
        let nonce = transition_card_nonce(terminal, state_change_seq, index);
        let mut sent = deliver_transition_card(client, target, message, &nonce).await;
        if let Err(error) = &sent
            && error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR)
        {
            *topology_cache.lock().await = None;
            sent = match sync_route(client, guild, route, topology_cache).await {
                Ok(resolved) => {
                    target = resolved;
                    deliver_transition_card(client, target, message, &nonce).await
                }
                Err(error) => Err(error),
            };
        }
        match sent {
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
        && matches!(transition.to.as_str(), STATUS_IDLE | STATUS_DONE)
        && previous_herdr_seq.is_some_and(|previous| current_herdr_seq > previous)
}

/// Settled status unchanged between snapshots while Herdr's seq advanced: a full turn collapsed.
#[must_use]
fn seq_backstop_collapsed_settled_turn(
    status: &str,
    previous_herdr_seq: Option<u64>,
    current_herdr_seq: u64,
) -> bool {
    matches!(status, STATUS_IDLE | STATUS_DONE)
        && previous_herdr_seq.is_some_and(|previous| current_herdr_seq > previous)
}

/// Split out of [`process_snapshot`] to keep it under the line-count lint.
async fn maybe_start_live_watch(
    discord: Option<&DiscordConnection>,
    snapshot: &AgentSnapshot,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    state: &mut BridgeState,
) {
    if snapshot.agent_status == STATUS_WORKING {
        ensure_live_watch_started(discord, snapshot, agents, tabs, state).await;
    }
}

/// Split out of [`process_snapshot`] to keep it under the line-count lint. Keeps
/// `state.activity_eligible_panes` in step with this snapshot's pane: eligible while it reports
/// `working` with a session, not otherwise.
fn update_activity_eligibility(state: &mut BridgeState, snapshot: &AgentSnapshot, status: &str) {
    let Some(pane_id) = snapshot.pane_id.as_deref() else {
        return;
    };
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
    discord: Option<&DiscordConnection>,
    state: &mut BridgeState,
) {
    let previous_status = state
        .previous
        .get(&snapshot.terminal_id)
        .map(|(previous, _)| previous.as_str());
    if snapshot.session.is_none()
        || previous_status.is_some_and(|previous| {
            previous != "unknown"
                || !matches!(snapshot.agent_status.as_str(), STATUS_IDLE | STATUS_DONE)
        })
    {
        return;
    }
    let Ok(route) = route_topology(agents, tabs, &snapshot.terminal_id) else {
        return;
    };
    state.title_pending.remove(&route.tab_id);
    let Some((client, guild, _, responder)) = discord else {
        return;
    };
    if let Err(error) =
        sync_route(client.as_ref(), *guild, &route, responder.topology_cache()).await
    {
        eprintln!("{error}");
    }
}

/// Runs the terminal-prompt mirror for a turn the bridge never observed as `working` (either seq
/// backstop branch of [`process_snapshot`]): no [`LiveWatch`] ever existed for it, so
/// [`handle_live_event`] never ran, and its prompt would otherwise wait for the next turn's first
/// tick and land after this turn's own reply card. Does nothing without a session or a resolvable
/// route; the transition-card delivery that follows reports a route failure on its own.
async fn mirror_missed_turn_prompt(
    snapshot: &AgentSnapshot,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    discord: Option<&DiscordConnection>,
    state: &mut BridgeState,
) {
    let Some(session) = snapshot.session.as_ref() else {
        return;
    };
    let Ok(route) = route_topology(agents, tabs, &snapshot.terminal_id) else {
        return;
    };
    mirror_terminal_prompts(
        discord,
        &snapshot.terminal_id,
        &session.agent,
        &route,
        state,
    )
    .await;
}

async fn process_snapshot(
    snapshot: &AgentSnapshot,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    discord: Option<&DiscordConnection>,
    state: &mut BridgeState,
) {
    let (terminal, status) = (snapshot.terminal_id.clone(), snapshot.agent_status.clone());
    println!(
        "{} {terminal}: {status}",
        snapshot.agent.as_deref().unwrap_or("none")
    );
    update_activity_eligibility(state, snapshot, &status);
    maybe_establish_terminal_prompt_baseline(snapshot, state);
    maybe_start_live_watch(discord, snapshot, agents, tabs, state).await;
    maybe_sync_fresh_session_topology(snapshot, agents, tabs, discord, state).await;
    let previous_herdr_seq = state
        .herdr_state_change_seq
        .insert(terminal.clone(), snapshot.state_change_seq);
    if let Some((old, prior_agent)) = state.previous.get(&terminal).cloned() {
        if old != status {
            let old_was_working = old == STATUS_WORKING;
            let seq = next_state_change_sequence(&mut state.state_change_sequences, &terminal);
            let leaving_blocked = old == STATUS_BLOCKED && status != STATUS_BLOCKED;
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
                STATUS_WORKING.clone_into(&mut transition.from);
                mirror_missed_turn_prompt(snapshot, agents, tabs, discord, state).await;
            }
            if old_was_working {
                settle_live_watch(discord, &terminal, state).await;
            }
            update_blocked_lifecycle(
                discord,
                &terminal,
                leaving_blocked,
                status == STATUS_BLOCKED,
                state,
            )
            .await;
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
            if old_was_working {
                forget_activity_message(state, snapshot.pane_id.as_deref());
            }
        } else if seq_backstop_collapsed_settled_turn(
            &status,
            previous_herdr_seq,
            snapshot.state_change_seq,
        ) {
            mirror_missed_turn_prompt(snapshot, agents, tabs, discord, state).await;
            let state_change_seq =
                next_state_change_sequence(&mut state.state_change_sequences, &terminal);
            let transition = Transition {
                from: STATUS_WORKING.to_owned(),
                to: status.clone(),
                terminal_id: terminal.clone(),
                agent: prior_agent,
            };
            deliver_transition_if_postable(
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
            .await;
            forget_activity_message(state, snapshot.pane_id.as_deref());
        }
    } else if status == STATUS_BLOCKED && state.blocked_capture_attempts.contains_key(&terminal) {
        retry_pending_blocked_capture(snapshot, agents, tabs, discord, &terminal, state).await;
    }
    remember_previous_status(state, &terminal, &status, snapshot.agent.as_deref());
}

/// Split out of [`process_snapshot`] to keep it under the line-count lint.
fn remember_previous_status(
    state: &mut BridgeState,
    terminal: &str,
    status: &str,
    agent: Option<&str>,
) {
    state.previous.insert(
        terminal.to_owned(),
        (status.to_owned(), agent.unwrap_or_default().to_owned()),
    );
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

/// Applies the outcome of a failed `route_topology` call, returning `true` when this call logged
/// something. A pending cold-start title is remembered silently in `state.title_pending` for
/// `sync_pending_titles` to retry on a later snapshot; an unusable name is logged once per tab id
/// via `state.unusable_reported` and skipped on every later occurrence; any other routing error is
/// logged on every occurrence.
fn report_route_error(error: RouteError, state: &mut BridgeState) -> bool {
    match error {
        RouteError::TitlePending { tab_id } => {
            state.title_pending.insert(tab_id);
            false
        }
        RouteError::Unusable { tab_id, message } => {
            let is_new = state.unusable_reported.insert(tab_id);
            if is_new {
                eprintln!("{message}");
            }
            is_new
        }
        RouteError::Other(message) => {
            eprintln!("{message}");
            true
        }
    }
}

/// Delivers one postable transition's card to Discord: an agent reporting no session is not
/// mirrored and returns before any topology is routed, blocked cards included. A tab whose
/// topology cannot be routed is handled by `report_route_error` and otherwise skipped. Otherwise,
/// a blocked transition goes through `handle_blocked_card`, while a reply-card transition is
/// captured from the vendor log and delivered. A reply card whose captured text equals the last
/// one delivered for this terminal is skipped instead of reposted. The check applies on both the
/// status-change path and the seq-backstop path, so a legitimately identical consecutive reply is
/// intentionally not reposted either.
///
/// # Errors
///
/// Returns Discord delivery errors.
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
    if snapshot.session.is_none() {
        println!("{terminal}: no reported session, not mirrored");
        return Ok(());
    }
    let route = match route_topology(agents, tabs, terminal) {
        Ok(route) => route,
        Err(error) => {
            report_route_error(error, state);
            return Ok(());
        }
    };
    // A tab this call just resolved is no longer pending; without this, `sync_pending_titles`
    // would see the same tab still in the set later in the same snapshot pass and sync it again.
    state.title_pending.remove(&route.tab_id);
    let Some(connection) = discord else {
        return Ok(());
    };
    let (client, guild, owner_id, responder) = connection;
    if transition.to == STATUS_BLOCKED {
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
    let last_posted = state.last_posted.get(terminal).map(String::as_str);
    if capture.failure.is_none() && repeats_last_live_text(&capture, last_posted) {
        println!("{terminal}: skipped duplicate reply card");
        return Ok(());
    }
    let card_capture = card_capture_for_delivery(&capture, last_posted);
    deliver_to_route(
        connection,
        &route,
        transition,
        &card_capture,
        state_change_seq,
    )
    .await?;
    state
        .last_posted
        .insert(terminal.to_owned(), capture.message);
    Ok(())
}

/// Split out of [`process_snapshot`] to keep it under the line-count lint: delivers `context`'s
/// transition when postable and logs any delivery error, shared by the status-change and
/// seq-backstop paths.
async fn deliver_transition_if_postable(
    context: PostableTransitionContext<'_>,
    state: &mut BridgeState,
) {
    if is_postable_transition(context.transition)
        && let Err(error) = deliver_postable_transition(context, state).await
    {
        eprintln!("{error}");
    }
}

/// Whether a reply card would only repeat a text already shown live: the caller's dedup guard
/// skips posting in that case unless the turn also failed and needs reporting.
#[must_use]
fn repeats_last_live_text(capture: &AgentLogCapture, last_posted: Option<&str>) -> bool {
    last_posted == Some(capture.message.as_str())
}

/// The capture used to build a reply card's content. Unchanged, unless this turn's message
/// already appeared live and the turn also failed: then the message portion is cleared so the
/// card shows only the failure instead of repeating text already posted live.
#[must_use]
fn card_capture_for_delivery(
    capture: &AgentLogCapture,
    last_posted: Option<&str>,
) -> AgentLogCapture {
    if capture.failure.is_some() && repeats_last_live_text(capture, last_posted) {
        AgentLogCapture {
            message: String::new(),
            failure: capture.failure.clone(),
            question: capture.question.clone(),
        }
    } else {
        capture.clone()
    }
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
    if snapshot.session.is_none() {
        println!("{terminal}: no reported session, not mirrored");
        return;
    }
    let route = match route_topology(agents, tabs, terminal) {
        Ok(route) => route,
        Err(error) => {
            report_route_error(error, state);
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
        from_status: STATUS_BLOCKED,
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
    let home = std::env::var_os(ENV_HOME).ok_or_else(|| "HOME is not configured".to_owned())?;
    capture_for_with_search_root(snapshot, Path::new(&home))
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
    /// unsupported vendor, or a search directory that cannot be read at all — that a caller should
    /// stop retrying and mark unfollowable instead.
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
    search_root: &Path,
) -> Result<AgentLogCapture, String> {
    let session = snapshot.session.as_ref().ok_or_else(|| {
        format!(
            "{}: no reported session, no log available",
            snapshot.terminal_id
        )
    })?;
    let path =
        resolve_session_path(search_root, snapshot, session).map_err(|error| error.to_string())?;
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
            let roots = if roots.is_empty() {
                vec![search_root.join(".codex")]
            } else {
                roots
            };
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
            unique_existing_path(&candidates, "codex session log")
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

/// The Cursor session's chat directory: the parent of `store.db`, watched instead of the file
/// itself so both the creation of its `-wal` sibling and later writes to it wake the follower. A
/// watch set up before the sibling exists (the common case for a freshly started pane) would
/// otherwise never see it appear.
fn cursor_watch_target(store_path: &Path) -> &Path {
    store_path.parent().unwrap_or(store_path)
}

/// Registers a `notify` watch on a vendor log path, forwarding the terminal id on every modify
/// event. Cursor watches its chat directory (see [`cursor_watch_target`]) instead of the file.
fn start_notify_watcher(
    vendor: &str,
    path: &Path,
    terminal: String,
    tx: tokio::sync::mpsc::UnboundedSender<String>,
) -> Result<notify::RecommendedWatcher, String> {
    let mut watcher = notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
        if let Ok(event) = result
            && matches!(
                event.kind,
                notify::EventKind::Modify(_) | notify::EventKind::Create(_)
            )
        {
            let _ = tx.send(terminal.clone());
        }
    })
    .map_err(|error| error.to_string())?;
    let target = if vendor == VENDOR_CURSOR {
        cursor_watch_target(path)
    } else {
        path
    };
    watcher
        .watch(target, notify::RecursiveMode::NonRecursive)
        .map_err(|error| error.to_string())?;
    Ok(watcher)
}

/// The position a freshly started live-capture follower resumes from: the start of the current
/// turn. A watch that attaches mid-turn still posts every assistant text the turn already wrote,
/// since `ensure_live_watch_started` performs one immediate read from this position before
/// returning.
fn initial_live_position(vendor: &str, path: &Path) -> Result<LivePosition, String> {
    match vendor {
        VENDOR_CLAUDE => claude_turn_start_position(path).map(LivePosition::Bytes),
        VENDOR_CODEX => codex_turn_start_position(path).map(LivePosition::Bytes),
        VENDOR_CURSOR => cursor_turn_start_rowid(path).map(LivePosition::RowId),
        other => Err(format!(
            "live capture: unsupported vendor for initial position: {other}"
        )),
    }
}

/// The position a terminal's terminal-prompt mirroring starts from the first time it is ever
/// established for that terminal: past every prompt already in the vendor log, so a bridge that
/// discovers a pane mid-conversation never replays its history.
fn initial_terminal_prompt_position(vendor: &str, path: &Path) -> Result<LivePosition, String> {
    match vendor {
        VENDOR_CLAUDE => read_claude_prompts_incremental(path, 0)
            .map(|(_, checkpoint)| LivePosition::Bytes(checkpoint)),
        VENDOR_CODEX => read_codex_prompts_incremental(path, 0)
            .map(|(_, checkpoint)| LivePosition::Bytes(checkpoint)),
        VENDOR_CURSOR => read_cursor_prompts_incremental(path, 0)
            .map(|(_, checkpoint)| LivePosition::RowId(checkpoint)),
        other => Err(format!(
            "terminal prompt mirroring: unsupported vendor {other}"
        )),
    }
}

/// Reads new complete owner prompts appended to a terminal's vendor log since `position`, paired
/// with the byte offset (Claude) or `rowid` (Cursor) immediately after each.
///
/// # Errors
///
/// Returns the incremental reader's error for the follower's vendor, or a mismatch error when
/// `position`'s shape does not match the vendor's own (Claude/Codex track a byte offset, Cursor a
/// `rowid`).
fn read_new_terminal_prompts(
    vendor: &str,
    path: &Path,
    position: LivePosition,
) -> Result<(Vec<(String, i64)>, LivePosition), String> {
    match (vendor, position) {
        (VENDOR_CLAUDE, LivePosition::Bytes(offset)) => {
            let (prompts, checkpoint) = read_claude_prompts_incremental(path, offset)?;
            let prompts = prompts
                .into_iter()
                .map(|(text, position)| (text, i64::try_from(position).unwrap_or(i64::MAX)))
                .collect();
            Ok((prompts, LivePosition::Bytes(checkpoint)))
        }
        (VENDOR_CODEX, LivePosition::Bytes(offset)) => {
            let (prompts, checkpoint) = read_codex_prompts_incremental(path, offset)?;
            let prompts = prompts
                .into_iter()
                .map(|(text, position)| (text, i64::try_from(position).unwrap_or(i64::MAX)))
                .collect();
            Ok((prompts, LivePosition::Bytes(checkpoint)))
        }
        (VENDOR_CURSOR, LivePosition::RowId(last_rowid)) => {
            let (prompts, new_rowid) = read_cursor_prompts_incremental(path, last_rowid)?;
            Ok((prompts, LivePosition::RowId(new_rowid)))
        }
        (vendor, position) => Err(format!(
            "terminal prompt mirroring: unsupported vendor {vendor} with position {position:?}"
        )),
    }
}

/// Starts a live-capture follower for a terminal newly observed as `working`, unless one is
/// already running or the terminal was already marked unfollowable. Retried on every later
/// snapshot while the pane stays `working`, except that a non-transient `live_log_path` error, or
/// [`LIVE_DELIVERY_ATTEMPTS`] consecutive delivery failures inside [`handle_live_event`], marks
/// the terminal unfollowable so no further attempt is made for it.
///
/// A bridge restart re-follows an already-in-progress turn from its start. Discord's nonce dedupe
/// lasts only a few minutes, so this reposts only the turn's texts older than that window; a
/// closely-timed restart has its nonce still recognized and the repost suppressed.
async fn ensure_live_watch_started(
    discord: Option<&DiscordConnection>,
    snapshot: &AgentSnapshot,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    state: &mut BridgeState,
) {
    let terminal = snapshot.terminal_id.clone();
    if state.live_watches.contains_key(&terminal) || state.live_unfollowable.contains(&terminal) {
        return;
    }
    let Some(session) = snapshot.session.clone() else {
        return;
    };
    // Live text exists only for Claude, Codex, and Cursor logs; other vendors still get end cards,
    // just not live text.
    if !matches!(
        session.agent.as_str(),
        VENDOR_CLAUDE | VENDOR_CODEX | VENDOR_CURSOR
    ) {
        return;
    }
    let path = match live_log_path(snapshot, &session) {
        Ok(Some(path)) => path,
        Ok(None) => return,
        Err(error) => {
            eprintln!("live capture unfollowable for {terminal}: {error}");
            state.live_unfollowable.insert(terminal);
            return;
        }
    };
    let Some(live_tx) = state.live_tx.clone() else {
        return;
    };
    let position = match initial_live_position(&session.agent, &path) {
        Ok(position) => position,
        Err(error) => {
            eprintln!("live capture watch error for {terminal}: {error}");
            return;
        }
    };
    let watcher = match start_notify_watcher(&session.agent, &path, terminal.clone(), live_tx) {
        Ok(watcher) => watcher,
        Err(error) => {
            eprintln!("live capture watch error for {terminal}: {error}");
            return;
        }
    };
    let Some((client, guild, _owner_id, responder)) = discord else {
        return;
    };
    let Ok(route) = route_topology(agents, tabs, &terminal) else {
        return;
    };
    let Ok(channel) = sync_route(client.as_ref(), *guild, &route, responder.topology_cache()).await
    else {
        return;
    };
    state.live_watches.insert(
        terminal.clone(),
        LiveWatch {
            _watcher: watcher,
            vendor: session.agent,
            path,
            position,
            channel,
            route,
        },
    );
    // Read once immediately: the next `notify` tick may never come if the turn is already near
    // done.
    handle_live_event(discord, &terminal, state).await;
}

/// Does not touch the watch's stored position: the caller advances it only past text it actually
/// delivers, so a text this read returns but a later delivery attempt drops is re-read and
/// re-sent rather than skipped.
///
/// # Errors
///
/// Returns the incremental reader's error for the follower's vendor.
fn read_new_live_texts(watch: &LiveWatch) -> Result<(Vec<(String, i64)>, i64), String> {
    match (watch.vendor.as_str(), watch.position) {
        (VENDOR_CLAUDE, LivePosition::Bytes(offset)) => {
            let (texts, new_offset) = read_claude_incremental(&watch.path, offset)?;
            Ok((
                texts
                    .into_iter()
                    .map(|(text, position)| (text, i64::try_from(position).unwrap_or(i64::MAX)))
                    .collect(),
                i64::try_from(new_offset).unwrap_or(i64::MAX),
            ))
        }
        (VENDOR_CODEX, LivePosition::Bytes(offset)) => {
            let (texts, new_offset) = read_codex_incremental(&watch.path, offset)?;
            Ok((
                texts
                    .into_iter()
                    .map(|(text, position)| (text, i64::try_from(position).unwrap_or(i64::MAX)))
                    .collect(),
                i64::try_from(new_offset).unwrap_or(i64::MAX),
            ))
        }
        (VENDOR_CURSOR, LivePosition::RowId(rowid)) => {
            let (texts, new_rowid) = read_cursor_incremental(&watch.path, rowid)?;
            Ok((texts, new_rowid))
        }
        (vendor, _) => Err(format!("live capture: unsupported vendor {vendor}")),
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
/// bundled to stay under the argument-count lint; `workspace_channel`/`thread` are re-resolved and
/// replaced on an unknown-channel retry, the rest stays fixed for the whole mirrored batch.
#[derive(Clone, Copy)]
struct TerminalPromptTarget<'a> {
    client: &'a Client,
    guild: Id<GuildMarker>,
    route: &'a TopologyRoute,
    topology_cache: &'a TopologyCache,
    workspace_channel: Id<ChannelMarker>,
    thread: Id<ChannelMarker>,
}

/// Whether a terminal-prompt delivery failure is worth one retry against a freshly resolved
/// workspace channel, thread, and webhook: the cached thread or the cached webhook is stale
/// (deleted outside the bridge's own tracking -- deleting a channel deletes its webhooks with it,
/// so either can go stale independently of the other).
fn is_retriable_terminal_prompt_error(error: &str) -> bool {
    error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR)
        || error.starts_with(UNKNOWN_WEBHOOK_DELIVERY_ERROR)
}

/// Clears the shared topology cache and the cached webhook for `target`'s stale workspace channel,
/// re-resolves both the route and the webhook, and returns the refreshed target.
///
/// # Errors
///
/// Returns [`sync_route_channels`]'s error.
async fn refresh_terminal_prompt_target<'a>(
    target: TerminalPromptTarget<'a>,
    state: &mut BridgeState,
) -> Result<TerminalPromptTarget<'a>, String> {
    *target.topology_cache.lock().await = None;
    state
        .terminal_prompt_webhooks
        .remove(&target.workspace_channel);
    let (workspace_channel, thread) = sync_route_channels(
        target.client,
        target.guild,
        target.route,
        target.topology_cache,
    )
    .await?;
    Ok(TerminalPromptTarget {
        workspace_channel,
        thread,
        ..target
    })
}

/// Resolves (from cache or fresh) the webhook for `target.workspace_channel` and executes one
/// message into `target.thread`, returning `target` unchanged on success so the caller can chain
/// further parts or prompts against the same resolved pair.
///
/// # Errors
///
/// Returns [`cached_terminal_prompt_webhook`]'s or [`execute_terminal_prompt_webhook`]'s error --
/// covering both webhook resolution and execution, so [`mirror_one_terminal_prompt`]'s retry runs
/// the same [`is_retriable_terminal_prompt_error`] check against either failure.
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
/// A retriable failure ([`is_retriable_terminal_prompt_error`]) resolving or executing any part --
/// the cached thread or the cached webhook is stale -- invalidates the shared topology cache and
/// the cached webhook, re-resolves both the route and the webhook, and retries that one part once
/// against the fresh pair; any other failure, or a repeat failure after the retry, stops the batch
/// there rather than sending later parts out of order. Returns the resolved target reached by the
/// last part actually sent, so the caller carries a refreshed target forward to later prompts in
/// the same batch instead of retrying each one from the stale pair independently.
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
        current = match deliver_terminal_prompt_part(current, identity, &part, state).await {
            Ok(delivered) => delivered,
            Err(error) if is_retriable_terminal_prompt_error(&error) => {
                let refreshed = refresh_terminal_prompt_target(current, state).await?;
                deliver_terminal_prompt_part(refreshed, identity, &part, state).await?
            }
            Err(error) => return Err(error),
        };
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
/// A terminal seen with no session at all yet (Codex does not report one until the pane is already
/// `working`, by which point its log can already hold the prompt that started the turn) records
/// `None` in [`BridgeState::terminal_prompt_awaiting_first_log`]; one seen with a session whose
/// [`live_log_path`] returns `Ok(None)` (the log or store does not exist on disk yet -- a fresh
/// pane, or a new session before its first write) records `Some` of the session's current value.
/// Either way the terminal retries on the next snapshot. The first time a path then resolves for
/// this terminal, the baseline is 0 -- not [`initial_terminal_prompt_position`]'s
/// discard-what-already-exists checkpoint -- when the recorded entry is `None` (no session was ever
/// visible before this one, so it cannot be a history that predates this terminal being watched) or
/// `Some` of the SAME session that was recorded pending: its log was created after this terminal was
/// already being watched, so everything now in it, including the prompt that may have just created
/// it, postdates first sight and belongs to this bridge run, not a discarded history. A path
/// resolving for any OTHER session (one that was never recorded pending, or a different one that
/// superseded it -- a `/resume` onto an existing session before the pending one's log ever
/// appeared) gets the normal discard-what-already-exists baseline instead, and the stale pending
/// record is dropped either way so it cannot later misapply to a third session. A permanent
/// resolution error is logged and retried too, since resolving it costs only a directory read.
fn maybe_establish_terminal_prompt_baseline(snapshot: &AgentSnapshot, state: &mut BridgeState) {
    let terminal = &snapshot.terminal_id;
    let Some(session) = snapshot.session.as_ref() else {
        state
            .terminal_prompt_awaiting_first_log
            .entry(terminal.clone())
            .or_insert(None);
        return;
    };
    let vendor = session.agent.as_str();
    if !matches!(vendor, VENDOR_CLAUDE | VENDOR_CODEX | VENDOR_CURSOR) {
        return;
    }
    let path = match live_log_path(snapshot, session) {
        Ok(Some(path)) => path,
        Ok(None) => {
            state
                .terminal_prompt_awaiting_first_log
                .insert(terminal.clone(), Some(session.value.clone()));
            return;
        }
        Err(error) => {
            eprintln!("terminal prompt baseline error for {terminal}: {error}");
            return;
        }
    };
    if terminal_prompt_baseline_is_current(&state.terminal_prompt_positions, terminal, &path) {
        return;
    }
    let was_awaiting_this_session = match state.terminal_prompt_awaiting_first_log.remove(terminal)
    {
        Some(None) => true,
        Some(Some(awaiting_session)) => awaiting_session == session.value,
        None => false,
    };
    let position = if was_awaiting_this_session {
        Ok(zero_terminal_prompt_position(vendor))
    } else {
        initial_terminal_prompt_position(vendor, &path)
    };
    match position {
        Ok(position) => {
            state
                .terminal_prompt_positions
                .insert(terminal.clone(), (path, position));
        }
        Err(error) => eprintln!("terminal prompt baseline error for {terminal}: {error}"),
    }
}

/// The terminal-prompt baseline for a log a terminal is only now seeing resolve for the first time:
/// a byte offset for Claude and Codex, a `rowid` for Cursor.
fn zero_terminal_prompt_position(vendor: &str) -> LivePosition {
    if vendor == VENDOR_CURSOR {
        LivePosition::RowId(0)
    } else {
        LivePosition::Bytes(0)
    }
}

/// Whether `terminal` already has a terminal-prompt baseline for exactly `path`: `false` both when
/// there is no baseline yet and when there is one for a different path (a session change -- a
/// `/clear`, resume, relaunch, or a vendor starting a fresh file or store -- must re-baseline
/// against the new path rather than reuse the old file's position, which the new file may not even
/// be as long as).
fn terminal_prompt_baseline_is_current(
    positions: &HashMap<String, (PathBuf, LivePosition)>,
    terminal: &str,
    path: &Path,
) -> bool {
    positions
        .get(terminal)
        .is_some_and(|(existing_path, _)| existing_path == path)
}

/// Mirrors newly recorded owner prompts from one terminal's vendor log into its tab thread through
/// the bridge-owned webhook, in log order, ahead of any assistant text the same `notify` tick
/// delivers -- the caller runs this before reading live text.
///
/// Reads forward from the path and position [`maybe_establish_terminal_prompt_baseline`] last left
/// in [`BridgeState::terminal_prompt_positions`] -- independent of the live-text watch's own
/// turn-scoped position, so a later turn's own initiating prompt is mirrored rather than treated as
/// pre-existing. Does nothing if that baseline is not established yet: `process_snapshot` always
/// runs it first, for every status, before this function ever has a `LiveWatch` to be called from.
///
/// A prompt equal to a pending [`take_owner_prompt_suppression`] marker is dropped once instead of
/// mirrored: it is the bridge's own Discord-originated prompt, already posted by the owner in the
/// thread it came from. No session, no route, or no owner identity: dropped silently. A delivery
/// failure is logged once and the position still advances past it -- a stuck prompt does not block
/// mirroring later ones.
async fn mirror_terminal_prompts(
    discord: Option<&DiscordConnection>,
    terminal: &str,
    vendor: &str,
    route: &TopologyRoute,
    state: &mut BridgeState,
) {
    if !matches!(vendor, VENDOR_CLAUDE | VENDOR_CODEX | VENDOR_CURSOR) {
        return;
    }
    let Some((path, position)) = state.terminal_prompt_positions.get(terminal).cloned() else {
        return;
    };
    let (prompts, new_position) = match read_new_terminal_prompts(vendor, &path, position) {
        Ok(result) => result,
        Err(error) => {
            eprintln!("terminal prompt read error for {terminal}: {error}");
            return;
        }
    };
    state
        .terminal_prompt_positions
        .insert(terminal.to_owned(), (path, new_position));
    if prompts.is_empty() {
        return;
    }
    let Some((client, guild, _owner_id, responder)) = discord else {
        return;
    };
    let Some(identity) = state.owner_identity.clone() else {
        return;
    };
    let topology_cache = responder.topology_cache();
    let Ok((workspace_channel, thread)) =
        sync_route_channels(client.as_ref(), *guild, route, topology_cache).await
    else {
        return;
    };
    let mut target = TerminalPromptTarget {
        client: client.as_ref(),
        guild: *guild,
        route,
        topology_cache,
        workspace_channel,
        thread,
    };
    for (text, _position) in prompts {
        if take_owner_prompt_suppression(&route.pane_id, &text) {
            continue;
        }
        match mirror_one_terminal_prompt(target, &identity, &text, state).await {
            Ok(delivered) => target = delivered,
            Err(error) => eprintln!("terminal prompt delivery error for {terminal}: {error}"),
        }
    }
}

/// Mirrors any new owner terminal prompts via [`mirror_terminal_prompts`] before reading this
/// tick's live text, so a prompt that started the current turn is posted to Discord ahead of the
/// assistant text it produced.
///
/// Updates `state.last_posted` per fully delivered text so a turn-end card repeating it is
/// skipped. The nonce is derived from the terminal id and log position, not a counter, so it
/// survives a watch restart that resumes at the same position. A read failure logs once per
/// terminal and leaves the follower running at its unchanged position, to retry on the next event.
///
/// A delivery that fails against the watch's cached channel because the thread is gone (deleted
/// outside the bridge's own tracking) invalidates the shared topology cache, re-resolves the
/// route once, and retries that same text at the recreated thread, exactly like
/// [`deliver_to_route`]; the watch's `channel` is updated on a successful recovery so later events
/// do not repeat the round trip. The stored log position only advances past text that was
/// actually delivered: a text that still fails after the retry stops the batch there, so it and
/// everything read after it are re-read and re-sent on the next event instead of being lost.
///
/// That retry-on-the-next-tick behavior is only safe because it is bounded:
/// [`BridgeState::live_delivery_attempts`] counts consecutive failed ticks for whatever text is
/// currently stuck at the front, and once that reaches [`LIVE_DELIVERY_ATTEMPTS`] (a persistent
/// failure the one unknown-channel recovery does not fix -- lost `SEND_MESSAGES`, or a thread
/// archived rather than deleted) the terminal is logged once, added to
/// [`BridgeState::live_unfollowable`], and its watch is dropped, rather than retrying forever.
async fn handle_live_event(
    discord: Option<&DiscordConnection>,
    terminal: &str,
    state: &mut BridgeState,
) {
    let Some((client, guild, _owner_id, responder)) = discord else {
        return;
    };
    let Some(watch) = state.live_watches.get(terminal) else {
        return;
    };
    let start_position = match watch.position {
        LivePosition::Bytes(offset) => i64::try_from(offset).unwrap_or(i64::MAX),
        LivePosition::RowId(rowid) => rowid,
    };
    let mut channel = watch.channel;
    let route = watch.route.clone();
    let vendor = watch.vendor.clone();
    // `watch`'s borrow of `state.live_watches` ends here (its last use above); mirroring needs
    // `&mut state`, so it runs before `state.live_watches` is borrowed again below for live text.
    mirror_terminal_prompts(discord, terminal, &vendor, &route, state).await;
    let Some(watch) = state.live_watches.get(terminal) else {
        return;
    };
    let (texts, read_position) = match read_new_live_texts(watch) {
        Ok(result) => {
            state.live_read_errors_reported.remove(terminal);
            result
        }
        Err(error) => {
            if state.live_read_errors_reported.insert(terminal.to_owned()) {
                eprintln!("live capture read error for {terminal}: {error}");
            }
            return;
        }
    };
    let topology_cache = responder.topology_cache();
    let mut delivered_position = start_position;
    let mut all_delivered = true;
    for (text, position) in texts {
        let mut posted_all = true;
        for (part_index, part) in split_live_message(&text).into_iter().enumerate() {
            let nonce = live_message_nonce(terminal, position, part_index);
            let mut sent = deliver_live_message(client.as_ref(), channel, &part, &nonce).await;
            if let Err(error) = &sent
                && error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR)
            {
                *topology_cache.lock().await = None;
                sent = match sync_route(client.as_ref(), *guild, &route, topology_cache).await {
                    Ok(resolved) => {
                        channel = resolved;
                        deliver_live_message(client.as_ref(), channel, &part, &nonce).await
                    }
                    Err(error) => Err(error),
                };
            }
            if let Err(error) = sent {
                eprintln!("live capture delivery error for {terminal}: {error}");
                posted_all = false;
                break;
            }
        }
        if !posted_all {
            all_delivered = false;
            break;
        }
        state.last_posted.insert(terminal.to_owned(), text);
        delivered_position = position;
    }
    if all_delivered {
        delivered_position = read_position;
        state.live_delivery_attempts.remove(terminal);
    } else {
        let attempts_so_far = state
            .live_delivery_attempts
            .get(terminal)
            .copied()
            .unwrap_or(0);
        if attempts_so_far + 1 >= LIVE_DELIVERY_ATTEMPTS {
            eprintln!(
                "live capture unfollowable for {terminal}: delivery failed {LIVE_DELIVERY_ATTEMPTS} times in a row"
            );
            state.live_delivery_attempts.remove(terminal);
            state.live_watches.remove(terminal);
            state.live_unfollowable.insert(terminal.to_owned());
            return;
        }
        state
            .live_delivery_attempts
            .insert(terminal.to_owned(), attempts_so_far + 1);
    }
    if let Some(watch) = state.live_watches.get_mut(terminal) {
        watch.channel = channel;
        watch.position = match watch.vendor.as_str() {
            VENDOR_CURSOR => LivePosition::RowId(delivered_position),
            _ => LivePosition::Bytes(u64::try_from(delivered_position).unwrap_or(u64::MAX)),
        };
    }
}

/// Reads once more first so nothing written just before the pane left `working` is lost.
async fn settle_live_watch(
    discord: Option<&DiscordConnection>,
    terminal: &str,
    state: &mut BridgeState,
) {
    if !state.live_watches.contains_key(terminal) {
        return;
    }
    handle_live_event(discord, terminal, state).await;
    state.live_watches.remove(terminal);
    state.live_read_errors_reported.remove(terminal);
}

/// Forgets a pane's tracked activity message, if any, so the next turn's first activity frame
/// creates a fresh one instead of editing the settled turn's message.
fn forget_activity_message(state: &mut BridgeState, pane_id: Option<&str>) {
    if let Some(pane_id) = pane_id {
        state.activity_messages.remove(pane_id);
    }
}

/// The route's tab thread from the cached topology only, issuing no Discord request. `None` when
/// the cache is not yet populated or does not resolve the route.
async fn cached_route_channel(
    topology_cache: &TopologyCache,
    route: &TopologyRoute,
) -> Option<Id<ChannelMarker>> {
    let guard = topology_cache.lock().await;
    let (channels, active_threads) = guard.as_ref()?;
    let channel = cached_route(channels, active_threads, route).ok().flatten();
    drop(guard);
    channel
}

/// Re-resolves `route`'s tab thread from a fresh topology fetch, without creating anything: an
/// activity frame's empty `channel_name`/`thread_name` (it has no agent/tab snapshot to derive a
/// real name from) would corrupt a genuine create, so unlike [`sync_route`] a route the fresh
/// fetch still does not resolve returns `None` rather than falling through to `sync_topology`.
async fn refresh_activity_route_channel(
    client: &Client,
    guild: Id<GuildMarker>,
    route: &TopologyRoute,
    topology_cache: &TopologyCache,
) -> Option<Id<ChannelMarker>> {
    let fetched = fetch_topology_lists(client, guild).await.ok()?;
    let mut guard = topology_cache.lock().await;
    let (channels, active_threads) = reconcile_topology_cache(&mut guard, fetched);
    cached_route(channels, active_threads, route).ok().flatten()
}

/// Applies one activity frame: routes it to its tab's thread purely from the cached topology, then
/// posts or edits this turn's one activity message for the pane.
///
/// A cache not yet populated, or a route the cache does not resolve, drops the frame silently, and
/// so does a pane the latest snapshot does not report as `working` with a session -- the same
/// no-session rule every other card follows.
async fn handle_activity_event(
    discord: Option<&DiscordConnection>,
    frame: ActivityFrame,
    state: &mut BridgeState,
) {
    let Some((client, guild, _owner_id, responder)) = discord else {
        return;
    };
    let route = TopologyRoute {
        workspace_id: frame.workspace_id,
        tab_id: frame.tab_id,
        pane_id: frame.pane_id.clone(),
        channel_name: String::new(),
        thread_name: String::new(),
    };
    let topology_cache = responder.topology_cache();
    let Some(mut channel) = cached_route_channel(topology_cache, &route).await else {
        return;
    };
    if let Some(existing) = state.activity_messages.get_mut(&frame.pane_id) {
        let text = activity_message_text(existing.count + 1, &frame.tool, &frame.summary);
        let mut result =
            update_activity_message(client.as_ref(), channel, existing.message, &text).await;
        if let Err(error) = &result
            && error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR)
            && let Some(resolved) =
                refresh_activity_route_channel(client.as_ref(), *guild, &route, topology_cache)
                    .await
        {
            channel = resolved;
            result =
                update_activity_message(client.as_ref(), channel, existing.message, &text).await;
        }
        match result {
            Ok(()) => existing.count += 1,
            Err(error) => {
                eprintln!(
                    "activity edit delivery error for pane {}: {error}",
                    frame.pane_id
                );
            }
        }
        return;
    }
    if !state.activity_eligible_panes.contains(&frame.pane_id) {
        return;
    }
    let text = activity_message_text(1, &frame.tool, &frame.summary);
    let mut result = deliver_activity_message(client.as_ref(), channel, &text).await;
    if let Err(error) = &result
        && error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR)
        && let Some(resolved) =
            refresh_activity_route_channel(client.as_ref(), *guild, &route, topology_cache).await
    {
        channel = resolved;
        result = deliver_activity_message(client.as_ref(), channel, &text).await;
    }
    match result {
        Ok(message) => {
            state
                .activity_messages
                .insert(frame.pane_id, ActivityMessage { message, count: 1 });
        }
        Err(error) => {
            eprintln!(
                "activity create delivery error for pane {}: {error}",
                frame.pane_id
            );
        }
    }
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
    std::env::var_os(ENV_HOME).map_or_else(
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
    let topology_cache = responder.topology_cache();
    let messages = create_transition_messages(transition, capture, owner_id);
    let mut target = sync_route(client.as_ref(), *guild, route, topology_cache).await?;
    let mut last_message_id = None;
    for (index, message) in messages.iter().enumerate() {
        let nonce = transition_card_nonce(&transition.terminal_id, state_change_seq, index);
        let mut sent = deliver_transition_card(client.as_ref(), target, message, &nonce).await;
        if let Err(error) = &sent
            && error.starts_with(UNKNOWN_CHANNEL_DELIVERY_ERROR)
        {
            // The cached route no longer exists on Discord (deleted outside the bridge's own
            // tracking, since every deletion the bridge itself performs already keeps this same
            // cache in sync): drop it and resolve fresh before retrying once, rather than
            // repeating a send that can only fail again against the same stale id.
            *topology_cache.lock().await = None;
            target = sync_route(client.as_ref(), *guild, route, topology_cache).await?;
            sent = deliver_transition_card(client.as_ref(), target, message, &nonce).await;
        }
        last_message_id = Some(sent.map_err(|error| format!("discord delivery error: {error}"))?);
    }
    last_message_id.ok_or_else(|| "discord delivery produced no messages".to_owned())
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
/// so no channel or thread is created for it until a later snapshot reports one. A tab whose
/// cold-start title has not arrived yet is skipped quietly, with no log line; the ongoing event
/// loop's own snapshot passes (`discover_pending_and_unusable_tabs`, `sync_pending_titles`)
/// record it as pending and create its thread once a title arrives, independent of this sweep.
/// Any other per-tab routing or naming error is logged and skipped; the lazy sync inside delivery
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
        if agent.session.is_none() {
            continue;
        }
        if let Some(tab_id) = agent.tab_id.as_deref()
            && !synced_tabs.insert(tab_id.to_owned())
        {
            continue;
        }
        let route = match route_topology(agents, tabs, &agent.terminal_id) {
            Ok(route) => route,
            Err(RouteError::TitlePending { .. }) => continue,
            Err(error) => {
                eprintln!("herdr startup topology error: {error}");
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
                    eprintln!("herdr startup topology error: {error}");
                    continue;
                }
            };
            reconcile_topology_cache(&mut guard, fetched)
        };
        let result = sync_topology(client.as_ref(), *guild, channels, active_threads, &route).await;
        drop(guard);
        if let Err(error) = result {
            eprintln!("herdr startup topology error: {error}");
        }
    }
    let (workspaces, live_tabs) = match (workspace_list_result(), tab_list_result()) {
        (Ok(workspaces), Ok(live_tabs)) => (workspaces, live_tabs),
        (Err(error), _) | (_, Err(error)) => {
            eprintln!("herdr startup topology reconciliation error: {error}");
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
                eprintln!("herdr startup topology error: {error}");
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
        eprintln!("herdr startup topology reconciliation error: {error}");
    }
}

/// Applies every `tab.closed`/`workspace.closed` Herdr event in a batch to Discord with ONE
/// topology fetch shared across the whole batch, deleting only the tabs and workspaces the
/// fetched lists actually contain. A closure with no match in the active list and no match in that
/// closure's workspace channel's archived listing makes no further Discord request: the archived
/// listing itself is fetched at most once per distinct workspace channel in the batch and reused
/// by every closure that channel contains. A per-closure delete error is logged and does not stop
/// the remaining closures in the batch; only a failure of the shared fetch itself aborts the batch.
async fn delete_closed_topology_batch(
    discord: Option<&DiscordConnection>,
    closures: &[TopologyClosure],
) -> Result<(), String> {
    let Some((client, guild, _, responder)) = discord else {
        return Ok(());
    };
    if closures.is_empty() {
        return Ok(());
    }
    let topology_cache = responder.topology_cache();
    let fetched = fetch_topology_lists(client.as_ref(), *guild).await?;
    let mut guard = topology_cache.lock().await;
    let (channels, active_threads) = reconcile_topology_cache(&mut guard, fetched);
    let mut archived_cache: HashMap<Id<ChannelMarker>, Vec<twilight_model::channel::Channel>> =
        HashMap::new();
    for closure in closures {
        let result = match closure {
            TopologyClosure::Tab {
                workspace_id,
                tab_id,
            } => {
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
            eprintln!("herdr topology closure error: {error}");
        }
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

/// Removes every terminal-keyed entry for a terminal absent from `current_terminals`, every
/// tab-keyed entry (`title_pending`, `unusable_reported`) for a tab absent from `current_tabs`,
/// and every pane-keyed entry (`activity_messages`, `activity_eligible_panes`) for a pane absent
/// from `current_panes`, returning the informational cards that departed so callers can expire
/// them.
fn prune_departed_state(
    state: &mut BridgeState,
    current_terminals: &HashSet<String>,
    current_tabs: &HashSet<String>,
    current_panes: &HashSet<String>,
) -> Vec<(String, InformationalCard)> {
    state
        .blocked_since
        .retain(|terminal, _| current_terminals.contains(terminal));
    state
        .state_change_sequences
        .retain(|terminal, _| current_terminals.contains(terminal));
    state
        .herdr_state_change_seq
        .retain(|terminal, _| current_terminals.contains(terminal));
    state
        .blocked_capture_attempts
        .retain(|terminal, _| current_terminals.contains(terminal));
    state
        .last_posted
        .retain(|terminal, _| current_terminals.contains(terminal));
    state
        .live_watches
        .retain(|terminal, _| current_terminals.contains(terminal));
    state
        .live_unfollowable
        .retain(|terminal| current_terminals.contains(terminal));
    state
        .live_read_errors_reported
        .retain(|terminal| current_terminals.contains(terminal));
    state
        .title_pending
        .retain(|tab_id| current_tabs.contains(tab_id));
    state
        .unusable_reported
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

fn discord_connection(
    topology_cache: TopologyCache,
) -> Result<Option<(DiscordConnection, GatewayTask)>, Box<dyn std::error::Error>> {
    match (
        std::env::var(ENV_DISCORD_TOKEN),
        std::env::var(ENV_DISCORD_GUILD_ID),
        std::env::var(ENV_DISCORD_OWNER_ID),
    ) {
        (Ok(token), Ok(guild_id), Ok(owner_id)) => {
            let config = load_discord_config(&[
                (ENV_DISCORD_TOKEN, &token),
                (ENV_DISCORD_GUILD_ID, &guild_id),
                (ENV_DISCORD_OWNER_ID, &owner_id),
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
        _ => {}
    }
    run_bridge().await
}

async fn run_hook(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let (explicit_vendor, requested_socket) = parse_hook_args(args)
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
/// never fail the tool call it rides on.
async fn run_activity(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let Ok((vendor, requested_socket)) = parse_activity_args(args) else {
        return Ok(());
    };
    let mut input = Vec::new();
    tokio::io::stdin().read_to_end(&mut input).await?;
    let Ok(request) = (match vendor {
        VENDOR_CLAUDE => decode_claude_activity_request(&input),
        VENDOR_CODEX => decode_codex_activity_request(&input),
        VENDOR_CURSOR => decode_cursor_activity_request(&input),
        _ => return Ok(()),
    }) else {
        return Ok(());
    };
    let Some(socket_path) = requested_socket
        .or_else(|| std::env::var_os("HERDR_CLAUDE_BROKER_SOCKET").map(std::path::PathBuf::from))
    else {
        return Ok(());
    };
    let frame = ActivityFrame {
        kind: ACTIVITY_KIND.to_owned(),
        vendor: vendor.to_owned(),
        workspace_id: std::env::var("HERDR_WORKSPACE_ID").unwrap_or_default(),
        tab_id: std::env::var("HERDR_TAB_ID").unwrap_or_default(),
        pane_id: std::env::var("HERDR_PANE_ID").unwrap_or_default(),
        session_id: request.session_id,
        tool: request.tool,
        summary: request.summary,
    };
    send_activity_frame(&frame, &socket_path, Duration::from_secs(1)).await;
    Ok(())
}

fn parse_activity_args(
    args: &[String],
) -> Result<(&'static str, Option<std::path::PathBuf>), String> {
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
                    VENDOR_CLAUDE => VENDOR_CLAUDE,
                    VENDOR_CODEX => VENDOR_CODEX,
                    VENDOR_CURSOR => VENDOR_CURSOR,
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
    match canonical_event_name(event.get(EVENT_KEY)?.as_str()?).as_str() {
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
    match canonical_event_name(event.get(EVENT_KEY)?.as_str()?).as_str() {
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

/// Whether a `pane_updated` lifecycle event names a tab in `title_pending` and now carries a
/// non-empty terminal title. `pane.updated` has no server-side pane filter (see
/// `lifecycle_subscriptions`), so every pane's update reaches this check; only one naming a
/// pending tab's fresh title is worth a snapshot.
#[must_use]
fn pane_update_reports_a_pending_title(
    event: &serde_json::Value,
    title_pending: &HashSet<String>,
) -> bool {
    let Some(tab_id) = event
        .pointer("/data/pane/tab_id")
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    title_pending.contains(tab_id)
        && event
            .pointer("/data/pane/terminal_title_stripped")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|title| !title.trim().is_empty())
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
            Some(terminal) = runtime.live_events.recv() => {
                handle_live_event(discord, &terminal, &mut runtime.state).await;
            }
            Some(frame) = runtime.activity_events.recv() => {
                handle_activity_event(discord, frame, &mut runtime.state).await;
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

/// Keeps draining further lifecycle events into `batch` as long as each new one arrives within
/// [`LIFECYCLE_BATCH_WINDOW`] of the previous one, up to [`LIFECYCLE_BATCH_CAP`] events total
/// (including the seed event already in `batch`). Returns the terminating subscribe error when
/// draining stopped because the stream closed, rather than because the window elapsed or the cap
/// was reached.
async fn drain_lifecycle_batch(
    lifecycle: &mut HerdrSubscription,
    batch: &mut Vec<serde_json::Value>,
) -> Option<String> {
    while batch.len() < LIFECYCLE_BATCH_CAP {
        tokio::select! {
            result = lifecycle.next_event() => {
                match result {
                    Ok(event) => batch.push(event),
                    Err(error) => return Some(error),
                }
            }
            () = tokio::time::sleep(LIFECYCLE_BATCH_WINDOW) => return None,
        }
    }
    None
}

/// Applies one drained batch of lifecycle events: every membership change first (one status
/// resubscribe if any pane joined or left), then every closure with one shared topology fetch,
/// then one doorbell. A batch made only of `pane.updated` events that report no pending title
/// keeps the existing early-return rule and skips the doorbell.
async fn apply_lifecycle_batch(
    batch: &[serde_json::Value],
    discord: Option<&DiscordConnection>,
    stop: &mut tokio::signal::unix::Signal,
    broker: &mut Option<BrokerTask>,
    runtime: &mut BridgeRuntime,
) -> bool {
    let mut membership_changed = false;
    for event in batch {
        if let Some(change) = lifecycle_membership(event)
            && apply_membership(&mut runtime.pane_ids, change)
        {
            membership_changed = true;
        }
    }
    if membership_changed {
        let Some(next_status) = unwrap_or_shutdown(
            subscribe_status_with_backoff(&mut runtime.pane_ids, stop).await,
            broker,
        ) else {
            return false;
        };
        runtime.status = next_status;
    }

    let closures: Vec<TopologyClosure> = batch.iter().filter_map(lifecycle_closure).collect();
    if let Err(error) = delete_closed_topology_batch(discord, &closures).await {
        eprintln!("herdr topology closure error: {error}");
    }

    let worth_doorbell = batch.iter().any(|event| {
        let is_pane_updated = canonical_event_name(
            event
                .get(EVENT_KEY)
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default(),
        ) == "pane_updated";
        !is_pane_updated || pane_update_reports_a_pending_title(event, &runtime.state.title_pending)
    });
    if !worth_doorbell {
        return true;
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

/// Spawns the startup-style topology sweep (one `list_agents`/`tab_list_result` snapshot, then
/// `sync_startup_topology`) beside the caller rather than blocking it, exactly as `run_bridge`
/// does at process start. Run again after every successful lifecycle resubscribe: a replay gap
/// while the subscribe stream was down can otherwise leave a closed tab's thread or a closed
/// workspace's channel undeleted until some later, unrelated event happens to touch it.
fn spawn_startup_topology_sweep(discord: &DiscordConnection) {
    match list_agents().and_then(|agents| tab_list_result().map(|tabs| (agents, tabs))) {
        Ok((agents, tabs)) => {
            let discord = discord.clone();
            let startup_task =
                tokio::spawn(async move { sync_startup_topology(&discord, &agents, &tabs).await });
            tokio::spawn(async move {
                if let Err(error) = startup_task.await {
                    eprintln!("herdr startup topology task error: {error}");
                }
            });
        }
        Err(error) => eprintln!("herdr startup topology snapshot error: {error}"),
    }
}

async fn handle_lifecycle_subscribe_error(
    error: String,
    discord: Option<&DiscordConnection>,
    stop: &mut tokio::signal::unix::Signal,
    broker: &mut Option<BrokerTask>,
    runtime: &mut BridgeRuntime,
) -> bool {
    eprintln!("herdr lifecycle subscribe error: {error}");
    let Some(next_lifecycle) = unwrap_or_shutdown(
        subscribe_herdr_events_with_backoff(&lifecycle_subscriptions(), stop).await,
        broker,
    ) else {
        return false;
    };
    runtime.lifecycle = next_lifecycle;
    let alive = doorbell_unless_shutdown(
        discord,
        &mut runtime.state,
        &mut runtime.pane_ids,
        &mut runtime.status,
        stop,
        broker,
    )
    .await;
    if alive && let Some(discord) = discord {
        spawn_startup_topology_sweep(discord);
    }
    alive
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
            let mut batch = vec![event];
            let drain_error = drain_lifecycle_batch(&mut runtime.lifecycle, &mut batch).await;
            if !apply_lifecycle_batch(&batch, discord, stop, broker, runtime).await {
                return false;
            }
            match drain_error {
                Some(error) => {
                    handle_lifecycle_subscribe_error(error, discord, stop, broker, runtime).await
                }
                None => true,
            }
        }
        Err(error) => handle_lifecycle_subscribe_error(error, discord, stop, broker, runtime).await,
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
    let current_terminals: HashSet<String> = agents.iter().map(|s| s.terminal_id.clone()).collect();
    let current_tabs: HashSet<String> = tabs.iter().map(|tab| tab.tab_id.clone()).collect();
    let current_panes: HashSet<String> = agents.iter().filter_map(|s| s.pane_id.clone()).collect();
    state
        .previous
        .retain(|terminal, _| current_terminals.contains(terminal));
    let departed_cards =
        prune_departed_state(state, &current_terminals, &current_tabs, &current_panes);
    for (terminal, card) in departed_cards {
        expire_departed_card(discord, &terminal, card).await;
    }
    for snapshot in &agents {
        process_snapshot(snapshot, &agents, &tabs, discord, state).await;
    }
    discover_pending_and_unusable_tabs(&agents, &tabs, state);
    sync_pending_titles(discord, &agents, &tabs, state).await;
    Ok(pane_ids_from_agents(&agents))
}

/// Routes every session-carrying agent in the current snapshot with the pure, IO-free
/// `route_topology`, recording each failure via `report_route_error`. A status transition is not
/// required for this: it is what lets a numeric-label tab with no terminal title be discovered as
/// pending (and a permanently unusable name be logged) from a silent snapshot pass alone, so a
/// later `pane.updated` doorbell has a populated `title_pending` to check even when the agent's
/// status never changes.
fn discover_pending_and_unusable_tabs(
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    state: &mut BridgeState,
) {
    for agent in agents {
        if agent.session.is_none() {
            continue;
        }
        if let Err(error) = route_topology(agents, tabs, &agent.terminal_id) {
            report_route_error(error, state);
        }
    }
}

/// Removes each tab in `state.title_pending` whose current snapshot now reports a non-empty
/// terminal title on a session-carrying agent, and, when a Discord connection is available,
/// creates its thread. A session-less agent's title does not resolve the tab: the no-session rule
/// forbids mirroring it, so a tab whose only titled pane has no reported session stays pending.
/// The set is cleared for a resolved route even with `discord: None`, matching
/// `deliver_postable_transition`: a route that no longer needs a title is not pending, whether or
/// not this call can act on it. Runs `sync_route` directly rather than
/// `deliver_postable_transition`: there is no transition or reply card to deliver here, only the
/// thread itself needs to exist once the cold-start title arrives.
async fn sync_pending_titles(
    discord: Option<&DiscordConnection>,
    agents: &[AgentSnapshot],
    tabs: &[HerdrTab],
    state: &mut BridgeState,
) {
    if state.title_pending.is_empty() {
        return;
    }
    let ready_terminals: Vec<String> = agents
        .iter()
        .filter(|agent| {
            agent.session.is_some()
                && agent
                    .tab_id
                    .as_deref()
                    .is_some_and(|tab_id| state.title_pending.contains(tab_id))
                && agent
                    .terminal_title_stripped
                    .as_deref()
                    .is_some_and(|title| !title.trim().is_empty())
        })
        .map(|agent| agent.terminal_id.clone())
        .collect();
    for terminal_id in ready_terminals {
        match route_topology(agents, tabs, &terminal_id) {
            Ok(route) => {
                state.title_pending.remove(&route.tab_id);
                let Some((client, guild, _, responder)) = discord else {
                    continue;
                };
                if let Err(error) =
                    sync_route(client.as_ref(), *guild, &route, responder.topology_cache()).await
                {
                    eprintln!("{error}");
                }
            }
            Err(error) => {
                report_route_error(error, state);
            }
        }
    }
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

/// Fetches the owner's mirrored identity once at startup, when Discord is configured: `Ok(None)`
/// when it is not, since the rest of the bridge runs perfectly well without Discord at all.
///
/// # Errors
///
/// Returns an error when `DISCORD_OWNER_ID` is not numeric or the fetch itself fails. Terminal
/// prompt mirroring has no fallback identity to mirror under, so [`run_bridge`] fails startup on
/// this error rather than silently running the rest of the process without it.
async fn fetch_startup_owner_identity(
    discord: Option<&DiscordConnection>,
) -> Result<Option<OwnerIdentity>, String> {
    let Some((client, _guild, owner_id, _responder)) = discord else {
        return Ok(None);
    };
    let owner_id = owner_id.parse::<u64>().map_err(|error| {
        format!("owner identity fetch error: DISCORD_OWNER_ID is not numeric: {error}")
    })?;
    fetch_owner_identity(client.as_ref(), Id::<UserMarker>::new(owner_id))
        .await
        .map(Some)
        .map_err(|error| format!("owner identity fetch error: {error}"))
}

async fn run_bridge() -> Result<(), Box<dyn std::error::Error>> {
    let topology_cache: TopologyCache = Arc::new(tokio::sync::Mutex::new(None));
    let (activity_tx, activity_events) = tokio::sync::mpsc::unbounded_channel();
    let (discord, mut gateway, mut broker) = match discord_connection(Arc::clone(&topology_cache))?
    {
        Some((connection, gateway)) => {
            let broker = start_broker(&connection, activity_tx);
            (Some(connection), Some(gateway), broker)
        }
        None => (None, None, None),
    };
    let owner_identity = fetch_startup_owner_identity(discord.as_ref()).await?;
    let (live_tx, live_events) = tokio::sync::mpsc::unbounded_channel();
    let state = BridgeState {
        live_tx: Some(live_tx),
        owner_identity,
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
        spawn_startup_topology_sweep(discord);
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
        BlockedCardContext, BlockedDeliveryRoute, BlockedResponse, BridgeRuntime, BridgeState,
        BrokerTask, Client, LIVE_DELIVERY_ATTEMPTS, LivePosition, LiveWatch, Membership,
        PermissionResponder, SessionPathError, TopologyClosure, TopologyRoute,
        agent_read_detection, apply_membership, capture_for_with_search_root,
        card_capture_for_delivery, create_transition_messages, decide_blocked_response,
        delete_closed_topology_batch, deliver_blocked_messages, deliver_to_route,
        discover_pending_and_unusable_tabs, drain_lifecycle_batch, fetch_startup_owner_identity,
        fetch_topology_lists, handle_blocked_card, handle_lifecycle_select_result,
        handle_live_event, initial_terminal_prompt_position, is_retriable_terminal_prompt_error,
        lifecycle_closure, lifecycle_membership, list_agents, live_log_path,
        maybe_establish_terminal_prompt_baseline, next_state_change_sequence, process_snapshot,
        read_new_terminal_prompts, repeats_last_live_text, resolve_session_path, route_topology,
        seq_backstop_collapsed_settled_turn, seq_backstop_rewrites_working_from,
        start_notify_watcher, subscribe_status, subscribe_status_with_backoff, sync_pending_titles,
        sync_route, sync_startup_topology, tab_list_result, terminal_prompt_baseline_is_current,
        unique_existing_path,
    };
    use herdr_connect_rs::{
        AgentLogCapture, AgentSession, AgentSnapshot, STATUS_DONE, STATUS_IDLE, STATUS_WORKING,
        Transition, UNKNOWN_CHANNEL_DELIVERY_ERROR, UNKNOWN_WEBHOOK_DELIVERY_ERROR, VENDOR_CLAUDE,
        VENDOR_CODEX, VENDOR_CURSOR, lifecycle_subscriptions, read_claude_incremental,
        read_codex_incremental, read_cursor_incremental, status_subscriptions, submit_owner_prompt,
        subscribe_herdr_events, transition_card_nonce, workspace_list_result,
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

            let initial_position = initial_terminal_prompt_position(VENDOR_CLAUDE, &path)
                .expect("initial Claude terminal prompt position resolves");
            assert!(matches!(initial_position, LivePosition::Bytes(1_177)));

            fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .and_then(|mut file| {
                    file.write_all(
                        b"{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":[{\"type\":\"text\",\"text\":\"terminal-direct\"}]}}\n",
                    )
                })
                .expect("append real-schema Claude user record");

            let (prompts, checkpoint) =
                read_new_terminal_prompts(VENDOR_CLAUDE, &path, initial_position)
                    .expect("read appended Claude terminal prompt");
            assert_eq!(prompts, vec![("terminal-direct".to_owned(), 1_272_i64)]);
            assert!(matches!(checkpoint, LivePosition::Bytes(1_272)));

            let (repeated_prompts, repeated_checkpoint) =
                read_new_terminal_prompts(VENDOR_CLAUDE, &path, checkpoint)
                    .expect("repeat Claude terminal prompt read");
            assert!(repeated_prompts.is_empty());
            assert!(matches!(repeated_checkpoint, LivePosition::Bytes(1_272)));
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

            let initial_position = initial_terminal_prompt_position(VENDOR_CODEX, &path)
                .expect("initial Codex terminal prompt position resolves");
            assert!(matches!(initial_position, LivePosition::Bytes(1_215)));

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

            let (prompts, checkpoint) =
                read_new_terminal_prompts(VENDOR_CODEX, &path, initial_position)
                    .expect("read appended Codex terminal prompt");
            assert_eq!(prompts, vec![("terminal-direct".to_owned(), 1_342_i64)]);
            assert!(matches!(checkpoint, LivePosition::Bytes(1_425)));

            let (repeated_prompts, repeated_checkpoint) =
                read_new_terminal_prompts(VENDOR_CODEX, &path, checkpoint)
                    .expect("repeat Codex terminal prompt read");
            assert!(repeated_prompts.is_empty());
            assert!(matches!(repeated_checkpoint, LivePosition::Bytes(1_425)));
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
                let initial_position = initial_terminal_prompt_position(VENDOR_CURSOR, &path)
                    .expect("initial Cursor terminal prompt position resolves");
                assert!(matches!(initial_position, LivePosition::RowId(6)));

                let connection = Connection::open(&path).expect("reopen cursor store");
                connection
                .execute(
                    "INSERT INTO blobs (data) VALUES (?1)",
                    [br#"{"role":"user","content":[{"type":"text","text":"terminal-direct"}]}"#
                        .as_slice()],
                )
                .expect("append real-schema Cursor user row");
                drop(connection);

                let (prompts, checkpoint) =
                    read_new_terminal_prompts(VENDOR_CURSOR, &path, initial_position)
                        .expect("read appended Cursor terminal prompt");
                assert_eq!(prompts, vec![("terminal-direct".to_owned(), 7_i64)]);
                assert!(matches!(checkpoint, LivePosition::RowId(7)));

                let (repeated_prompts, repeated_checkpoint) =
                    read_new_terminal_prompts(VENDOR_CURSOR, &path, checkpoint)
                        .expect("repeat Cursor terminal prompt read");
                assert!(repeated_prompts.is_empty());
                assert!(matches!(repeated_checkpoint, LivePosition::RowId(7)));
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
            (first_path.clone(), LivePosition::Bytes(42)),
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
                tab_id: None,
                workspace_id: None,
                pane_id: None,
                cwd: Some(cwd.clone()),
                terminal_title_stripped: None,
                session: Some(AgentSession {
                    agent: VENDOR_CLAUDE.to_owned(),
                    value: session_value.to_owned(),
                }),
                state_change_seq: 0,
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
            let (replay_path, replay_position) = replay_state
                .terminal_prompt_positions
                .get("terminal-resume")
                .expect("resumed session baselines once its log resolves");
            assert_eq!(replay_path, &old_session_path);
            assert_eq!(
                *replay_position,
                LivePosition::Bytes(expected_discard_position),
                "a resumed session with prior history must discard it, not replay it"
            );

            // Control case c: the same existing session, discovered directly, with no intervening
            // fresh session -- must baseline identically to case b.
            let mut control_state = BridgeState::default();
            maybe_establish_terminal_prompt_baseline(
                &snapshot_for("old-session"),
                &mut control_state,
            );
            let (control_path, control_position) = control_state
                .terminal_prompt_positions
                .get("terminal-resume")
                .expect("baseline established for the control case");
            assert_eq!(control_path, &old_session_path);
            assert_eq!(
                replay_position, control_position,
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

    #[test]
    fn terminal_prompt_delivery_retries_on_unknown_channel_or_unknown_webhook_only() {
        let cases = [
            (
                "the target channel or thread is gone",
                format!("{UNKNOWN_CHANNEL_DELIVERY_ERROR}: response error: status code 404"),
                true,
            ),
            (
                "the target webhook is gone",
                format!("{UNKNOWN_WEBHOOK_DELIVERY_ERROR}: response error: status code 404"),
                true,
            ),
            (
                "an unrelated failure",
                "response error: status code 500".to_owned(),
                false,
            ),
        ];
        for (label, error, expected) in cases {
            assert_eq!(
                is_retriable_terminal_prompt_error(&error),
                expected,
                "{label}"
            );
        }
    }

    #[tokio::test]
    async fn owner_identity_fetch_is_none_without_discord_configured() {
        assert_eq!(fetch_startup_owner_identity(None).await, Ok(None));
    }

    #[tokio::test]
    async fn owner_identity_fetch_fails_startup_when_the_owner_id_is_not_numeric() {
        let client = Arc::new(Client::builder().token("fake-token".to_owned()).build());
        let guild = Id::<GuildMarker>::new(1);
        let owner_id = "not-a-number".to_owned();
        let responder = Arc::new(PermissionResponder::new(
            Arc::clone(&client),
            guild,
            owner_id.clone(),
            Arc::new(tokio::sync::Mutex::new(None)),
        ));
        let connection: super::DiscordConnection = (client, guild, owner_id, responder);
        let result = fetch_startup_owner_identity(Some(&connection)).await;
        assert!(
            result.is_err(),
            "a non-numeric DISCORD_OWNER_ID must fail startup, not silently run without an \
             identity: {result:?}"
        );
    }

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
            tab_id: None,
            workspace_id: None,
            pane_id: None,
            cwd: Some(cwd.to_owned()),
            terminal_title_stripped: None,
            session: Some(session.clone()),
            state_change_seq: 0,
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
                name: "a .claude-foo directory without a projects directory is not a root",
                build: |root| {
                    fs::create_dir_all(root.join(".claude-foo"))
                        .expect("create non-root .claude-foo directory");
                    let (snapshot, session) =
                        claude_session_snapshot("/tmp/ignored-workspace", "ignored-session");
                    (snapshot, session, None)
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
                tab_id: None,
                workspace_id: None,
                pane_id: None,
                cwd: Some("/srv/bridge".to_owned()),
                terminal_title_stripped: None,
                session: Some(session.clone()),
                state_change_seq: 0,
            };

            let resolved_path = resolve_session_path(&root, &snapshot, &session);
            let capture = capture_for_with_search_root(&snapshot, &root);

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
    fn capture_for_with_search_root_errors_on_missing_session_or_log() {
        let response: Value =
            serde_json::from_str(include_str!("../tests/fixtures/herdr-agent-list.json"))
                .expect("captured agent.list fixture is JSON");
        let agents: Vec<AgentSnapshot> =
            serde_json::from_value(response["result"]["agents"].clone())
                .expect("captured agent.list fixture has typed agents");

        let session_less = agents
            .iter()
            .find(|snapshot| snapshot.session.is_none())
            .expect("fixture contains a session-less agent");
        let error = capture_for_with_search_root(session_less, Path::new("tests/fixtures"))
            .expect_err("a session-less snapshot must not resolve a capture");
        assert!(
            error.contains(&session_less.terminal_id),
            "error must name the terminal missing its reported session: {error}"
        );

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
            "reader errors for a reported session whose log is missing must surface"
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
    fn reply_card_dedup_only_suppresses_the_message_never_a_failure() {
        let capture = |message: &str, failure: Option<&str>| AgentLogCapture {
            message: message.to_owned(),
            failure: failure.map(str::to_owned),
            question: None,
        };
        let cases = [
            (
                "repeats last live text, no failure: dedup skips the card entirely",
                capture("gamma", None),
                true,
                "gamma",
            ),
            (
                "repeats last live text, with failure: card must still post, message-only",
                capture("gamma", Some("tool errored")),
                false,
                "",
            ),
        ];
        for (name, capture, expect_skip, expect_card_message) in cases {
            let last_posted = Some("gamma");
            assert_eq!(
                capture.failure.is_none() && repeats_last_live_text(&capture, last_posted),
                expect_skip,
                "{name}: skip decision"
            );
            assert_eq!(
                card_capture_for_delivery(&capture, last_posted).message,
                expect_card_message,
                "{name}: card message"
            );
        }
    }

    #[test]
    fn claude_pending_question_fixture_yields_question_capture_and_card() {
        let snapshot = AgentSnapshot {
            agent: Some("claude".to_owned()),
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
            agent: snapshot.agent.unwrap_or_default(),
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
            agent: Some("claude".to_owned()),
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
            agent: Some("claude".to_owned()),
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
            agent: Some("claude".to_owned()),
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
        let _ = clear_directory_contents(&cwd_dir);
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
    const LIFECYCLE_BATCH_LABEL: &str = "testrun-lifecycle-batch";

    /// A gap in `next_event` results longer than this means no event is currently pending: longer
    /// than `LIFECYCLE_BATCH_WINDOW` so an ordinary batch-ending gap between live events does not
    /// read as "nothing pending".
    #[cfg(unix)]
    const LIFECYCLE_BATCH_CATCH_UP_IDLE: Duration = Duration::from_millis(600);

    /// One case in [`lifecycle_batch_coalesces_a_live_closure_burst_then_isolates_a_later_closure`]:
    /// `seeded_closures` tabs are closed back-to-back, after the subscribe, well inside
    /// `LIFECYCLE_BATCH_WINDOW`, so the whole burst is expected in one drained batch. A batch with
    /// more than one event is only required when more than one closure was seeded together: a lone
    /// seeded closure is still expected as its own single-event batch.
    #[cfg(unix)]
    struct LifecycleBatchCase {
        name: &'static str,
        seeded_closures: usize,
    }

    /// Extracts the tab ids that `lifecycle_closure` reports as tab closures within a batch.
    #[cfg(unix)]
    fn batch_tab_closures(batch: &[Value]) -> HashSet<String> {
        batch
            .iter()
            .filter_map(lifecycle_closure)
            .filter_map(|closure| match closure {
                TopologyClosure::Tab { tab_id, .. } => Some(tab_id),
                TopologyClosure::Workspace { .. } => None,
            })
            .collect()
    }

    /// Pulls one more drained batch from `lifecycle`, seeded by the next available event.
    /// `Ok(None)` means no event arrived within `LIFECYCLE_BATCH_CATCH_UP_IDLE`.
    #[cfg(unix)]
    async fn next_lifecycle_batch(
        lifecycle: &mut herdr_connect_rs::HerdrSubscription,
    ) -> Result<Option<Vec<Value>>, String> {
        let seed = match tokio::time::timeout(LIFECYCLE_BATCH_CATCH_UP_IDLE, lifecycle.next_event())
            .await
        {
            Ok(Ok(event)) => event,
            Ok(Err(error)) => return Err(error),
            Err(_) => return Ok(None),
        };
        let mut batch = vec![seed];
        if let Some(error) = drain_lifecycle_batch(lifecycle, &mut batch).await {
            return Err(error);
        }
        Ok(Some(batch))
    }

    /// Table-driven, against the real Herdr socket: closing several tabs back-to-back right after
    /// subscribing coalesces the whole live burst into one drained batch rather than one event at
    /// a time, and a tab closed only afterward arrives promptly in its own later batch, isolated
    /// from the burst.
    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn lifecycle_batch_coalesces_a_live_closure_burst_then_isolates_a_later_closure() {
        let cases = [
            LifecycleBatchCase {
                name: "single seeded closure",
                seeded_closures: 1,
            },
            LifecycleBatchCase {
                name: "two seeded closures",
                seeded_closures: 2,
            },
        ];

        for case in cases {
            assert_eq!(
                remaining_tabs(LIFECYCLE_BATCH_LABEL).expect("tab.list succeeds"),
                0,
                "named zero-leftover check: {}",
                case.name
            );

            let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
                .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
            let mut seeded_tab_ids = Vec::new();
            let mut seeded_cwd_dirs = Vec::new();
            for _ in 0..case.seeded_closures {
                let cwd_dir = std::env::temp_dir().join(format!(
                    "testrun-lifecycle-batch-{}-{}-{}",
                    std::process::id(),
                    seeded_tab_ids.len(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("system clock is after unix epoch")
                        .as_nanos()
                ));
                fs::create_dir_all(&cwd_dir).expect("create lifecycle-batch seed cwd");
                let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");
                let tab =
                    create_tab(LIFECYCLE_BATCH_LABEL, &workspace_id, cwd).expect("create seed tab");
                seeded_tab_ids.push(tab.tab_id);
                seeded_cwd_dirs.push(cwd_dir);
            }

            let mut lifecycle = subscribe_herdr_events(&lifecycle_subscriptions())
                .await
                .expect("lifecycle subscribe");

            let result: Result<(), String> = async {
                // Close every seeded tab back-to-back, well inside `LIFECYCLE_BATCH_WINDOW`: a
                // live burst that the batching code must coalesce into one drained batch.
                for tab_id in &seeded_tab_ids {
                    close_tab(tab_id);
                }

                let burst_batch = tokio::time::timeout(
                    Duration::from_secs(15),
                    next_lifecycle_batch(&mut lifecycle),
                )
                .await
                .map_err(|_| "timed out waiting for the closure burst".to_owned())??
                .ok_or_else(|| "expected a batch for the closure burst, got none".to_owned())?;

                let burst_closures = batch_tab_closures(&burst_batch);
                let expected_closures: HashSet<String> = seeded_tab_ids.iter().cloned().collect();
                if burst_closures != expected_closures {
                    return Err(format!(
                        "expected the whole burst {expected_closures:?} in one batch, got {burst_closures:?} from {burst_batch:?}"
                    ));
                }
                if case.seeded_closures > 1 && burst_batch.len() <= 1 {
                    return Err(format!(
                        "expected one batch with more than one event while closing {} tabs together, got {burst_batch:?}",
                        case.seeded_closures
                    ));
                }

                // A tab closed only now must arrive promptly, as its own batch, isolated from the
                // burst.
                let later_cwd_dir = std::env::temp_dir().join(format!(
                    "testrun-lifecycle-batch-later-{}-{}",
                    std::process::id(),
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("system clock is after unix epoch")
                        .as_nanos()
                ));
                fs::create_dir_all(&later_cwd_dir).map_err(|error| error.to_string())?;
                let later_cwd = later_cwd_dir
                    .to_str()
                    .ok_or_else(|| "temp cwd is valid UTF-8".to_owned())?;
                let later_tab = create_tab(LIFECYCLE_BATCH_LABEL, &workspace_id, later_cwd)
                    .map_err(|error| format!("later tab: {error}"))?;
                close_tab(&later_tab.tab_id);
                let _ = fs::remove_dir_all(&later_cwd_dir);

                let later_batch = tokio::time::timeout(
                    Duration::from_secs(15),
                    next_lifecycle_batch(&mut lifecycle),
                )
                .await
                .map_err(|_| "timed out waiting for the later closure".to_owned())??
                .ok_or_else(|| "expected a batch for the later closure, got none".to_owned())?;
                let later_closures = batch_tab_closures(&later_batch);
                if !later_closures.contains(&later_tab.tab_id) {
                    return Err(format!(
                        "later batch must contain the tab closed after the burst, got {later_closures:?}"
                    ));
                }
                if seeded_tab_ids.iter().any(|tab_id| later_closures.contains(tab_id)) {
                    return Err(format!(
                        "later batch must be its own closure, isolated from the burst: {later_closures:?}"
                    ));
                }
                Ok(())
            }
            .await;

            for cwd_dir in &seeded_cwd_dirs {
                let _ = fs::remove_dir_all(cwd_dir);
            }
            let tabs_left = remaining_tabs(LIFECYCLE_BATCH_LABEL)
                .expect("tab.list succeeds for the zero-leftover check");
            assert!(result.is_ok(), "{}: {result:?}", case.name);
            assert_eq!(tabs_left, 0, "named zero-leftover check: {}", case.name);
        }
    }

    /// Drives one real `claude --model haiku` agent through a genuine idle -> working -> settled
    /// round-trip observed via real Herdr subscribe events (unlike `seq_backstop_round_trip`,
    /// which only polls `agent.list`), and asserts a card carrying the real captured reply is
    /// posted. Status comes only from live `agent.list` snapshots; nothing is set by hand.
    #[cfg(unix)]
    async fn seed_then_working_then_done_card(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        agent_name: &str,
    ) -> Result<(), String> {
        start_claude_haiku_agent(agent_name, &tab.pane_id)?;
        let idle = snapshot_for_pane(&tab.pane_id)?;
        let session = idle.session.as_ref().ok_or_else(|| {
            format!(
                "pane {} has no reported session after agent start",
                tab.pane_id
            )
        })?;
        if session.agent != "claude" {
            return Err(format!(
                "expected a claude session on pane {}, agent.list reported {session:?}",
                tab.pane_id
            ));
        }
        let terminal = idle.terminal_id.clone();

        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&idle);
        let mut state = BridgeState::default();
        let connection = discord_tuple(guild);
        process_snapshot(&idle, agents, tabs, Some(&connection), &mut state).await;

        let route = route_topology(agents, tabs, &terminal)?;
        let topology_cache: herdr_connect_rs::TopologyCache =
            Arc::new(tokio::sync::Mutex::new(None));
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;
        let before = thread_card_descriptions(guild, thread).await?;
        if before
            .iter()
            .any(|description| description.to_lowercase().contains("ready"))
        {
            return Err("silent seed posted a transition card".to_owned());
        }

        let mut sub =
            subscribe_herdr_events(&status_subscriptions(std::slice::from_ref(&tab.pane_id)))
                .await?;
        herdr_json(&[
            "agent",
            "prompt",
            agent_name,
            "Reply with exactly the word ready.",
        ])?;
        wait_for_event(
            &mut sub,
            "pane.agent_status_changed",
            &tab.pane_id,
            "/data/pane_id",
            Some("working"),
            Duration::from_secs(15),
        )
        .await?;
        let working = snapshot_for_pane(&tab.pane_id)?;
        process_snapshot(
            &working,
            std::slice::from_ref(&working),
            tabs,
            Some(&connection),
            &mut state,
        )
        .await;

        let settled = loop {
            let event = wait_for_event(
                &mut sub,
                "pane.agent_status_changed",
                &tab.pane_id,
                "/data/pane_id",
                None,
                Duration::from_secs(30),
            )
            .await?;
            if matches!(
                event.pointer("/data/agent_status").and_then(Value::as_str),
                Some("done" | "idle")
            ) {
                break snapshot_for_pane(&tab.pane_id)?;
            }
        };
        process_snapshot(
            &settled,
            std::slice::from_ref(&settled),
            tabs,
            Some(&connection),
            &mut state,
        )
        .await;

        let messages = thread_card_descriptions(guild, thread).await?;
        if messages
            .iter()
            .any(|description| description.to_lowercase().contains("ready"))
        {
            Ok(())
        } else {
            Err(format!(
                "idle -> working -> settled via subscribe did not post a card, thread has {messages:?}"
            ))
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

        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let created = subscribe_tab_fixture();
        let (tab_id, cwd_dir, result) = match created {
            Ok((tab, cwd_dir)) => {
                let agent_name = format!(
                    "testrun-subscribe-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome = seed_then_working_then_done_card(&guild, &tab, &agent_name).await;
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
            remaining_tabs(SUBSCRIBE_LABEL).expect("tab.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
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
            "codex" => &["--model", "gpt-5.6-luna"],
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
            Some(connection),
            state,
        )
        .await;
    }

    /// Drives one real agent idle -> working -> settled: asserts `working` was observed, `alpha`
    /// was live before settle, live count matches the log, and the end card skips a repeat.
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
                    handle_live_event(Some(&connection), &terminal_id, &mut state).await;
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
            handle_live_event(Some(&connection), &terminal_id, &mut state).await;
        }

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
        end_card_matches_last_live_text(&messages, expected_count)
    }

    /// Asserts the thread carries exactly `expected_count` live (non-embed) messages and that the
    /// end card never repeats the last one.
    #[cfg(unix)]
    fn end_card_matches_last_live_text(
        messages: &[(String, bool, Id<MessageMarker>)],
        expected_count: usize,
    ) -> Result<(), String> {
        let mut live: Vec<_> = messages.iter().filter(|(_, embed, _)| !embed).collect();
        if live.len() != expected_count {
            return Err(format!("expected {expected_count} live, got {live:?}"));
        }
        live.sort_by_key(|(_, _, id)| *id);
        // An aborted, refused, or tool-only turn writes no assistant text: fail instead of
        // panicking, so the caller still runs cleanup and the zero-leftover checks.
        let Some((last_text, ..)) = live.last() else {
            return Err(
                "real turn produced no live text to compare against the end card".to_owned(),
            );
        };
        let repeated = messages
            .iter()
            .any(|(content, embed, _)| *embed && content == last_text);
        if repeated {
            return Err(format!("end card repeated live text {last_text:?}"));
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
                    handle_live_event(Some(connection), &event_terminal, state).await;
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
        own(&settled, tabs, connection, state).await;
        while let Ok(event_terminal) = live_events.try_recv() {
            handle_live_event(Some(connection), &event_terminal, state).await;
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
        let connection = discord_tuple(guild);
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
                    super::handle_activity_event(Some(connection), frame, state).await;
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
    /// activity message naming `Bash`, posted before the end card. Returns that message's id.
    #[cfg(unix)]
    fn assert_first_turn_activity(
        messages: &[(String, bool, Id<MessageMarker>)],
    ) -> Result<Id<MessageMarker>, String> {
        let rows = activity_message_rows(messages);
        let [(text, _, activity_id)] = rows.as_slice() else {
            return Err(format!(
                "expected exactly one activity message after turn one, thread has {messages:?}"
            ));
        };
        if !text.contains("Bash") {
            return Err(format!("activity message did not name Bash: {text}"));
        }
        let Some((_, _, end_card_id)) = messages.iter().find(|(_, embed, _)| *embed) else {
            return Err("no end card was posted for turn one".to_owned());
        };
        if activity_id >= end_card_id {
            return Err("activity message was not posted before the end card".to_owned());
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
        if second_row.2 == first_activity_id {
            return Err(
                "turn two edited turn one's activity message instead of posting its own".to_owned(),
            );
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
    /// the turn-boundary table in one continuous scenario: turn one's activity frame posts before
    /// its end card; a frame injected on the broker socket after the turn settles is dropped (no
    /// new message, no edit of the settled one); turn two's first frame starts its own fresh
    /// message rather than continuing turn one's count.
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

        // Row 1: a frame during the turn posts before the end card.
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
        super::handle_activity_event(Some(&connection), received, &mut state).await;
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

    /// The real Codex account's own broker socket, matching its already-installed
    /// `CODEX_HOME/hooks.json` (`PreToolUse` activity hook and `PermissionRequest` hook, both
    /// `--socket /tmp/herdr-claude-broker.sock`), per [examples/codex-hooks.json](../examples/codex-hooks.json)'s
    /// shape. Environment precondition, the same way [`codex_testrun_dir`] is: Codex has no
    /// `--settings` flag like Claude's to inject a hook per run, and a temporary `CODEX_HOME`
    /// (even one symlinking every file from the real one) never gets a Codex session reported by
    /// Herdr, so this exercise runs `CODEX_HOME=/home/user/.codex-one` directly and binds the
    /// hook's own fixed socket instead of a private one. Second precondition: the account's hooks
    /// must already be trusted -- after `hooks.json` changes, Codex shows "Hooks need review" and
    /// runs no hooks at all until a pane trusts them, so an untrusted `SessionStart` hook silently
    /// stops Herdr from ever reporting a session for the account.
    #[cfg(unix)]
    const CODEX_ACTIVITY_BROKER_SOCKET: &str = "/tmp/herdr-claude-broker.sock";

    /// The `CODEX_HOME` this exercise must run under, per the doc comment above.
    #[cfg(unix)]
    const CODEX_ACTIVITY_HOME: &str = "/home/user/.codex-one";

    /// Fails fast, naming exactly what is missing, when the Codex activity row's environment
    /// preconditions are unmet: `CODEX_HOME` is not [`CODEX_ACTIVITY_HOME`], its `hooks.json`
    /// lacks the activity hook on [`CODEX_ACTIVITY_BROKER_SOCKET`], or a hook entry it declares
    /// has no trust record in the shared `config.toml`'s `[hooks.state]`.
    #[cfg(unix)]
    fn assert_codex_activity_environment() -> Result<(), String> {
        let codex_home = std::env::var("CODEX_HOME")
            .map_err(|_| format!("CODEX_HOME is not set; expected {CODEX_ACTIVITY_HOME}"))?;
        if codex_home != CODEX_ACTIVITY_HOME {
            return Err(format!(
                "CODEX_HOME is {codex_home}, expected {CODEX_ACTIVITY_HOME}"
            ));
        }

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

    /// Codex counterpart to `activity_hook_exercise`: drives one real `codex --model gpt-5.6-luna`
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
        if std::os::unix::net::UnixStream::connect(broker_socket).is_ok() {
            return Err(format!(
                "production bridge is listening on {}; stop it before running this exercise",
                broker_socket.display()
            ));
        }

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

        // Row 1: a frame during the turn posts before the end card.
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
        super::handle_activity_event(Some(&connection), received, &mut state).await;
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

    /// Drives one real `claude --model haiku` agent through a genuine settled round-trip and
    /// asserts the seq backstop still posts a card carrying the real captured reply. Status and
    /// the seq counter come only from live `agent.list` snapshots; nothing is set by hand (the
    /// one owner-approved exception to that rule is `seq_backstop_session_dedup_round_trip`'s
    /// counter, which is unrelated to this test). When `same_status_collapse` is set, a first
    /// real turn settles the baseline so the second turn's settled status repeats it, exercising
    /// `seq_backstop_collapsed_settled_turn`; otherwise the baseline is the agent's fresh
    /// post-start status and one real turn's settled status differs from it, exercising
    /// `seq_backstop_rewrites_working_from`. `scenario` names the case for the failure message.
    #[cfg(unix)]
    async fn seq_backstop_round_trip(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        agent_name: &str,
        same_status_collapse: bool,
        scenario: &str,
    ) -> Result<(), String> {
        start_claude_haiku_agent(agent_name, &tab.pane_id)?;
        if same_status_collapse {
            prompt_claude_agent_and_wait(agent_name, "Reply with exactly the word ready.")?;
        }
        let baseline = snapshot_for_pane(&tab.pane_id)?;
        let session = baseline.session.as_ref().ok_or_else(|| {
            format!(
                "pane {} has no reported session after agent start",
                tab.pane_id
            )
        })?;
        if session.agent != "claude" {
            return Err(format!(
                "expected a claude session on pane {}, agent.list reported {session:?}",
                tab.pane_id
            ));
        }
        if !matches!(baseline.agent_status.as_str(), STATUS_IDLE | STATUS_DONE) {
            return Err(format!(
                "expected an idle or done baseline so the backstop path is the one exercised, saw {}",
                baseline.agent_status
            ));
        }
        let terminal = baseline.terminal_id.clone();

        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let connection = discord_tuple(guild);
        let route = route_topology(std::slice::from_ref(&baseline), tabs, &terminal)?;
        let topology_cache: herdr_connect_rs::TopologyCache =
            Arc::new(tokio::sync::Mutex::new(None));
        let thread = sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;

        let mut state = BridgeState::default();
        process_snapshot(
            &baseline,
            std::slice::from_ref(&baseline),
            tabs,
            Some(&connection),
            &mut state,
        )
        .await;

        prompt_claude_agent_and_wait(agent_name, "Reply with exactly the word ready.")?;
        let settled = snapshot_for_pane(&tab.pane_id)?;
        if same_status_collapse && settled.agent_status != baseline.agent_status {
            return Err(format!(
                "expected same-status collapse on {}, saw {} -> {}",
                baseline.agent_status, baseline.agent_status, settled.agent_status
            ));
        }
        if !same_status_collapse && settled.agent_status == baseline.agent_status {
            return Err(format!(
                "expected the settled status to differ from the baseline, both were {}",
                baseline.agent_status
            ));
        }
        if settled.state_change_seq <= baseline.state_change_seq {
            return Err(format!(
                "herdr state_change_seq did not advance between snapshots: baseline={} settled={}",
                baseline.state_change_seq, settled.state_change_seq
            ));
        }
        process_snapshot(
            &settled,
            std::slice::from_ref(&settled),
            tabs,
            Some(&connection),
            &mut state,
        )
        .await;

        let messages = thread_card_descriptions(guild, thread).await?;
        if messages
            .iter()
            .any(|description| description.to_lowercase().contains("ready"))
        {
            Ok(())
        } else {
            Err(format!(
                "seq backstop ({scenario}) did not post a card, thread has {messages:?}"
            ))
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
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let cwd_dir = claude_testrun_dir(&home);
        clear_directory_contents(&cwd_dir).expect("clear seq-backstop test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_tab(SEQ_BACKSTOP_LABEL, &workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let agent_name = format!(
                    "testrun-seq-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome = seq_backstop_round_trip(
                    &guild,
                    &tab,
                    &agent_name,
                    false,
                    "idle -> working -> done between snapshots",
                )
                .await;
                cleanup_real_claude_session_dir(&home, &tab.pane_id);
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = clear_directory_contents(&cwd_dir);

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
        let home = std::env::var("HOME")
            .map(PathBuf::from)
            .expect("HOME is set by the real Herdr pane environment");
        let cwd_dir = claude_testrun_dir(&home);
        clear_directory_contents(&cwd_dir).expect("clear seq-same-status test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_tab(SEQ_BACKSTOP_LABEL, &workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let agent_name = format!(
                    "testrun-seqsame-{}",
                    agent_name_nonce().expect("system clock is after unix epoch")
                );
                let outcome = seq_backstop_round_trip(
                    &guild,
                    &tab,
                    &agent_name,
                    true,
                    "done -> working -> done same-status collapse",
                )
                .await;
                cleanup_real_claude_session_dir(&home, &tab.pane_id);
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = clear_directory_contents(&cwd_dir);

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

        let session_id = generate_claude_session_id()?;
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
                (
                    confirmed.agent_status.clone(),
                    confirmed.agent.clone().unwrap_or_default(),
                ),
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
        report_idle_with_session(&tab.pane_id)?;
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
                    tab_id: Some(tab_id.to_owned()),
                    workspace_id: Some(workspace_id.to_owned()),
                    pane_id: Some(pane_id),
                    cwd: Some(format!("/tmp/{FRESH_IDLE_SESSION_LABEL}")),
                    terminal_title_stripped: Some("fresh".to_owned()),
                    session: Some(AgentSession {
                        agent: agent.to_owned(),
                        value: format!("{agent}-fresh-session"),
                    }),
                    state_change_seq: 0,
                };
                let tabs = [herdr_connect_rs::HerdrTab {
                    tab_id: tab_id.to_owned(),
                    workspace_id: workspace_id.to_owned(),
                    label: "fresh".to_owned(),
                }];
                let agents = [snapshot.clone()];
                let connection = discord_tuple(&guild);
                let mut state = BridgeState::default();

                process_snapshot(&snapshot, &agents, &tabs, Some(&connection), &mut state).await;

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
        tab_a: &Tab,
        tab_b: &Tab,
    ) -> Result<(), String> {
        report_idle_with_session(&tab_a.pane_id)?;
        report_idle_with_session(&tab_b.pane_id)?;
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

    /// Submits a foreground shell command that clears the terminal title, then holds it there for
    /// `hold` by sleeping, so a shell prompt that would otherwise reassert its own title cannot
    /// run again until the hold ends. `pane run` submits and returns immediately (it does not wait
    /// for the command to finish), so the caller observes the empty title for the remainder of
    /// `hold`.
    #[cfg(unix)]
    fn clear_and_hold_terminal_title(pane_id: &str, hold: Duration) -> Result<(), String> {
        pane_run(
            pane_id,
            &format!("sh -c \"printf '\\033]2;\\007'; sleep {}\"", hold.as_secs()),
        )
    }

    /// Waits up to `bound` for `agent.list` to report an empty (or whitespace-only) stripped
    /// terminal title on a pane. A fire-and-forget `pane run` that clears the title (`pane run`
    /// submits and returns immediately, it does not wait for the command to run) takes some real,
    /// unbounded moment to actually reach the shell; a single read racing that moment is not
    /// deterministic. Fails with the last observed title on timeout.
    #[cfg(unix)]
    async fn wait_for_empty_terminal_title(
        pane_id: &str,
        bound: Duration,
    ) -> Result<AgentSnapshot, String> {
        let start = Instant::now();
        loop {
            let snapshot = snapshot_for_pane(pane_id)?;
            let title = snapshot.terminal_title_stripped.as_deref().unwrap_or("");
            if title.trim().is_empty() {
                return Ok(snapshot);
            }
            if start.elapsed() > bound {
                return Err(format!(
                    "pane {pane_id} did not report an empty terminal title within {bound:?}, last saw {title:?}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Real-Herdr exercise for the numeric-label/no-title bridge behavior, driven exactly as the
    /// ongoing event loop would: a silent snapshot pass (no status transition) discovers the tab
    /// is pending, a real session-wide `pane.updated` event reports the title, and the next
    /// snapshot pass creates the thread.
    #[cfg(unix)]
    async fn late_terminal_title_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
    ) -> Result<(), String> {
        report_idle_with_session(&tab.pane_id)?;

        let hold = Duration::from_secs(10);
        let hold_started = Instant::now();
        clear_and_hold_terminal_title(&tab.pane_id, hold)?;
        let pending_snapshot =
            wait_for_empty_terminal_title(&tab.pane_id, Duration::from_secs(10)).await?;

        let matching = matching_tab(&tab.tab_id)?;
        if matching.label.is_empty() || !matching.label.chars().all(|c| c.is_ascii_digit()) {
            return Err(format!(
                "expected a numeric auto-assigned label for an unlabeled tab, herdr reported label {:?}",
                matching.label
            ));
        }
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&pending_snapshot);
        // `HERDR_WORKSPACE_ID` is the shared real workspace this whole test session runs in, so
        // its `herdr workspace [...]` channel already exists from ordinary, non-test tabs; the
        // observable proof nothing was created for THIS tab is that no thread names it, not that
        // the shared channel is absent.
        let topic = format!("herdr workspace [{}]", matching.workspace_id);
        let thread_suffix = format!(" [{}]", matching.tab_id);

        // A silent snapshot pass, exactly as `apply_herdr_snapshot` runs on every doorbell: no
        // status transition happens here, so only the pure discovery step can record this tab as
        // pending. `state` is never seeded by hand.
        let mut state = BridgeState::default();
        discover_pending_and_unusable_tabs(agents, tabs, &mut state);
        if !state.title_pending.contains(&matching.tab_id) {
            return Err("a snapshot pass did not record the tab as title-pending".to_owned());
        }
        if !tab_thread_is_absent(guild, &topic, &thread_suffix).await? {
            return Err("a thread exists for a tab with no terminal title yet".to_owned());
        }

        let remaining = hold.saturating_sub(hold_started.elapsed());
        if !remaining.is_zero() {
            tokio::time::sleep(remaining + Duration::from_millis(500)).await;
        }
        // Subscribed right before it is read: `pane.updated` is unfiltered and session-wide (see
        // `lifecycle_subscriptions`), so this stream carries every pane's updates across the whole
        // real Herdr session. Opening it any earlier, then leaving it unread while other work runs,
        // risks Herdr treating an idle, backlogged subscriber as a slow consumer and closing it.
        let mut lifecycle_sub = subscribe_herdr_events(&lifecycle_subscriptions())
            .await
            .map_err(|error| error.to_string())?;
        pane_run(
            &tab.pane_id,
            "sh -c \"printf '\\033]2;late title\\007'; sleep 15\"",
        )?;
        wait_for_event(
            &mut lifecycle_sub,
            "pane_updated",
            &tab.pane_id,
            "/data/pane/pane_id",
            None,
            Duration::from_secs(15),
        )
        .await?;

        let titled_snapshot = snapshot_for_pane(&tab.pane_id)?;
        if titled_snapshot.terminal_title_stripped.as_deref() != Some("late title") {
            return Err(format!(
                "expected terminal_title_stripped \"late title\" after the pane.updated event, agent.list reported {:?}",
                titled_snapshot.terminal_title_stripped
            ));
        }

        // The next snapshot pass: discovery is a no-op now (the route resolves), and
        // `sync_pending_titles` is what actually creates the thread and clears the set.
        let titled_agents = [titled_snapshot];
        discover_pending_and_unusable_tabs(&titled_agents, tabs, &mut state);
        let connection = discord_tuple(guild);
        sync_pending_titles(Some(&connection), &titled_agents, tabs, &mut state).await;

        if state.title_pending.contains(&matching.tab_id) {
            return Err(
                "sync_pending_titles left the tab in title_pending after the title arrived"
                    .to_owned(),
            );
        }
        let expected_name = format!("late title [{}]", matching.tab_id);
        let channel = guild_channel_with_topic(guild, &topic).await?;
        let threads = active_threads_for_guild(guild).await?;
        let created = threads
            .iter()
            .find(|thread| thread.parent_id == Some(channel.id))
            .ok_or_else(|| {
                "sync_pending_titles did not create the tab thread once the title arrived"
                    .to_owned()
            })?;
        if created.name.as_deref() != Some(expected_name.as_str()) {
            return Err(format!(
                "expected thread name {expected_name:?}, got {:?}",
                created.name
            ));
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn late_terminal_title_creates_thread_once_pane_updated_reports_one() {
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
            "testrun-latetitle-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create late-title test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = create_unlabeled_tab(&workspace_id, cwd);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let outcome = late_terminal_title_exercise(&guild, &tab).await;
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        if let Some(tab_id) = &tab_id {
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
        tab: &Tab,
    ) -> Result<(), String> {
        report_idle_with_session(&tab.pane_id)?;
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

    /// Whether no thread ending in `thread_suffix` survives under the channel with `topic`. A
    /// missing channel counts as absent too: a channel this test did not itself create (for
    /// example, the shared real workspace's own long-lived channel) can legitimately already
    /// exist, so channel presence alone says nothing about this specific tab's thread.
    #[cfg(unix)]
    async fn tab_thread_is_absent(
        guild: &BlockedCaptureGuild,
        topic: &str,
        thread_suffix: &str,
    ) -> Result<bool, String> {
        match guild_channel_with_topic(guild, topic).await {
            Ok(channel) => {
                Ok(!thread_with_suffix_survives(guild, channel.id, thread_suffix).await?)
            }
            Err(_) => Ok(true),
        }
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

    #[cfg(unix)]
    const CLOSURE_BATCH_LABEL: &str = "testrun-closure-batch";

    /// Whether a tab's thread exists, and where, before [`closure_batch_exercise`] runs its batch.
    #[cfg(unix)]
    enum ClosureBatchPresence {
        Active,
        Archived,
        Absent,
    }

    /// One row in [`closure_batch_exercise`]'s table: a tab's thread presence going in, whether its
    /// closure is included in the batch, and whether the thread is expected to survive the batch.
    #[cfg(unix)]
    struct ClosureBatchCase {
        name: &'static str,
        presence: ClosureBatchPresence,
        closed: bool,
        expect_survives: bool,
    }

    /// Table-driven, against a real Discord guild: a batch of several `tab.closed` closures for
    /// tabs in the same workspace channel deletes only the closed tabs whose threads the fetched
    /// active list or that channel's one archived listing actually contains, and a live tab's
    /// thread outside the batch survives untouched.
    #[cfg(unix)]
    async fn closure_batch_exercise(guild: &BlockedCaptureGuild) -> Result<(), String> {
        let nonce = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        );
        let workspace_id = format!("{CLOSURE_BATCH_LABEL}-{nonce}");
        let channel = guild
            .client
            .create_guild_channel(guild.id, &format!("{CLOSURE_BATCH_LABEL}-{nonce}"))
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
                expect_survives: false,
            },
            ClosureBatchCase {
                name: "an archived thread whose closure is in the batch is deleted",
                presence: ClosureBatchPresence::Archived,
                closed: true,
                expect_survives: false,
            },
            ClosureBatchCase {
                name: "a closure with no matching thread is a harmless no-op",
                presence: ClosureBatchPresence::Absent,
                closed: true,
                expect_survives: false,
            },
            ClosureBatchCase {
                name: "a live tab outside the batch survives",
                presence: ClosureBatchPresence::Active,
                closed: false,
                expect_survives: true,
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
        delete_closed_topology_batch(Some(&connection), &closures).await?;

        for (case, suffix) in cases.iter().zip(suffixes.iter()) {
            let survives = thread_with_suffix_survives(guild, channel.id, suffix).await?;
            if survives != case.expect_survives {
                return Err(format!(
                    "{}: expected survives={}, got {survives}",
                    case.name, case.expect_survives
                ));
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn closure_batch_deletes_matching_threads_and_spares_live_ones() {
        let Some(guild) = blocked_capture_guild() else {
            eprintln!("skipped: Discord real-guild environment is not configured");
            return;
        };
        assert_eq!(
            blocked_capture_cleanup(&guild).await.unwrap(),
            0,
            "named zero-leftover check"
        );

        let result = closure_batch_exercise(&guild).await;

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
    }

    #[cfg(unix)]
    const CACHE_RECOVERY_LABEL: &str = "testrun-cache-recovery";

    /// One row of [`sync_route_serves_cache_hits_and_recovers_from_a_stale_send`]: which of the
    /// three `sync_route` callers delivers past a cache hit gone stale.
    #[cfg(unix)]
    enum CacheRecoveryCaller {
        TransitionCard,
        BlockedCard,
        LiveMessage,
    }

    #[cfg(unix)]
    struct CacheRecoveryCase {
        name: &'static str,
        caller: CacheRecoveryCaller,
    }

    /// Drives `sync_route`'s three refetch triggers against a real guild: a cache hit is served
    /// without any Discord request (so deleting the channel directly, out from under the cache,
    /// does not get noticed), and `caller`'s delivery then recovers by treating the resulting
    /// unknown-channel send failure as a signal to invalidate the cache, resolve the route fresh,
    /// and retry once.
    #[cfg(unix)]
    async fn sync_route_cache_recovery_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        caller: &CacheRecoveryCaller,
    ) -> Result<(), String> {
        report_idle_with_session(&tab.pane_id)?;
        let listed = snapshot_for_pane(&tab.pane_id)?;
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&listed);
        let route = route_topology(agents, tabs, &listed.terminal_id)?;
        let topic = format!("herdr workspace [{}]", route.workspace_id);
        let suffix = format!(" [{}]", route.tab_id);

        let topology_cache: herdr_connect_rs::TopologyCache =
            Arc::new(tokio::sync::Mutex::new(None));
        let created_thread =
            sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;
        let workspace_channel = guild_channel_with_topic(guild, &topic).await?.id;

        // Delete the real thread directly, bypassing the bridge entirely: the in-memory cache
        // still references it, standing in for an owner deleting a tab thread out from under the
        // bridge's own tracking. The workspace channel is untouched.
        guild
            .client
            .delete_channel(created_thread)
            .await
            .map_err(|error| error.to_string())?;

        // A cache hit is served without a Discord request: sync_route returns the same, now-gone
        // thread id rather than refetching and discovering it is missing.
        let cached_thread =
            sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;
        if cached_thread != created_thread {
            return Err(format!(
                "expected the cache hit to return the original thread {created_thread}, got {cached_thread}"
            ));
        }
        if thread_with_suffix_survives(guild, workspace_channel, &suffix).await? {
            return Err("the cache hit must not have recreated the deleted thread".to_owned());
        }

        // A real send against the stale cached thread fails as unknown channel; the caller under
        // test must recover by invalidating the cache, resolving the route fresh, and retrying
        // once.
        let connection = discord_tuple_with_cache(guild, Arc::clone(&topology_cache));
        let transition = Transition {
            from: STATUS_IDLE.to_owned(),
            to: STATUS_DONE.to_owned(),
            terminal_id: listed.terminal_id.clone(),
            agent: VENDOR_CLAUDE.to_owned(),
        };
        let capture = AgentLogCapture {
            message: "cache-recovery test reply".to_owned(),
            question: None,
            failure: None,
        };
        match caller {
            CacheRecoveryCaller::TransitionCard => {
                deliver_to_route(&connection, &route, &transition, &capture, 1).await?;
            }
            CacheRecoveryCaller::BlockedCard => {
                recover_blocked_card_delivery(
                    &connection,
                    &route,
                    &topology_cache,
                    &transition,
                    &capture,
                    cached_thread,
                    &listed.terminal_id,
                )
                .await?;
            }
            CacheRecoveryCaller::LiveMessage => {
                recover_live_message_delivery(
                    &connection,
                    &route,
                    cached_thread,
                    &listed.terminal_id,
                )
                .await?;
            }
        }

        if !thread_with_suffix_survives(guild, workspace_channel, &suffix).await? {
            return Err("recovery must have created a fresh thread for the route".to_owned());
        }
        Ok(())
    }

    /// The [`CacheRecoveryCaller::BlockedCard`] arm of
    /// [`sync_route_cache_recovery_exercise`], split out to keep that function under the
    /// line-count lint: delivers a blocked-card message set to the stale `target` and asserts
    /// [`deliver_blocked_messages`]'s invalidate-and-retry recorded an informational card.
    #[cfg(unix)]
    async fn recover_blocked_card_delivery(
        connection: &super::DiscordConnection,
        route: &TopologyRoute,
        topology_cache: &herdr_connect_rs::TopologyCache,
        transition: &Transition,
        capture: &AgentLogCapture,
        target: Id<ChannelMarker>,
        terminal: &str,
    ) -> Result<(), String> {
        let (client, guild_id, owner_id, _responder) = connection;
        let messages = create_transition_messages(transition, capture, owner_id);
        let mut informational_cards = HashMap::new();
        deliver_blocked_messages(
            &BlockedDeliveryRoute {
                client: client.as_ref(),
                guild: *guild_id,
                route,
                topology_cache,
            },
            target,
            terminal,
            1,
            &messages,
            &mut informational_cards,
        )
        .await;
        if informational_cards.is_empty() {
            return Err("blocked card delivery did not record an informational card".to_owned());
        }
        Ok(())
    }

    /// The [`CacheRecoveryCaller::LiveMessage`] arm of [`sync_route_cache_recovery_exercise`],
    /// split out to keep that function under the line-count lint: binds a live watch to a
    /// synthetic one-record Claude log and the stale `target`, drives one [`handle_live_event`],
    /// and asserts the recovered delivery landed.
    #[cfg(unix)]
    async fn recover_live_message_delivery(
        connection: &super::DiscordConnection,
        route: &TopologyRoute,
        target: Id<ChannelMarker>,
        terminal: &str,
    ) -> Result<(), String> {
        let live_path = std::env::temp_dir().join(format!(
            "testrun-cache-recovery-live-{}-{}.jsonl",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::write(
            &live_path,
            "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\
             [{\"type\":\"text\",\"text\":\"cache-recovery live text\"}]}}\n",
        )
        .map_err(|error| error.to_string())?;
        let (live_tx, _live_rx) = tokio::sync::mpsc::unbounded_channel();
        let watcher =
            start_notify_watcher(VENDOR_CLAUDE, &live_path, terminal.to_owned(), live_tx)?;
        let mut state = BridgeState::default();
        state.live_watches.insert(
            terminal.to_owned(),
            LiveWatch {
                _watcher: watcher,
                vendor: VENDOR_CLAUDE.to_owned(),
                path: live_path.clone(),
                position: LivePosition::Bytes(0),
                channel: target,
                route: route.clone(),
            },
        );
        handle_live_event(Some(connection), terminal, &mut state).await;
        let _ = fs::remove_file(&live_path);
        if !state.last_posted.contains_key(terminal) {
            return Err("live message recovery did not deliver the pending text".to_owned());
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn sync_route_serves_cache_hits_and_recovers_from_a_stale_send() {
        let cases = [
            CacheRecoveryCase {
                name: "transition card",
                caller: CacheRecoveryCaller::TransitionCard,
            },
            CacheRecoveryCase {
                name: "blocked card",
                caller: CacheRecoveryCaller::BlockedCard,
            },
            CacheRecoveryCase {
                name: "live message",
                caller: CacheRecoveryCaller::LiveMessage,
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
            assert_eq!(
                remaining_tabs(CACHE_RECOVERY_LABEL).expect("tab.list succeeds"),
                0,
                "named zero-leftover check: {}",
                case.name
            );

            let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
                .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
            let cwd_dir = std::env::temp_dir().join(format!(
                "testrun-cache-recovery-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is after unix epoch")
                    .as_nanos()
            ));
            fs::create_dir_all(&cwd_dir).expect("create cache-recovery test cwd");
            let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

            let created = create_tab(CACHE_RECOVERY_LABEL, &workspace_id, cwd);
            let (tab_id, result) = match created {
                Ok(tab) => {
                    let outcome =
                        sync_route_cache_recovery_exercise(&guild, &tab, &case.caller).await;
                    (Some(tab.tab_id), outcome)
                }
                Err(error) => (None, Err(error)),
            };
            if let Some(tab_id) = &tab_id {
                close_tab(tab_id);
            }
            let _ = fs::remove_dir_all(&cwd_dir);

            let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
            let tabs_left = remaining_tabs(CACHE_RECOVERY_LABEL)
                .expect("tab.list succeeds for the zero-leftover check");
            assert!(result.is_ok(), "{}: {result:?}", case.name);
            assert_eq!(channels_left, 0, "named zero-leftover check: {}", case.name);
            assert_eq!(tabs_left, 0, "named zero-leftover check: {}", case.name);
        }
    }

    #[cfg(unix)]
    const LIVE_ATTEMPTS_LABEL: &str = "testrun-live-attempts";

    /// One row of [`live_delivery_gives_up_after_bounded_attempts`]: how the stuck text's delivery
    /// fails.
    #[cfg(unix)]
    enum LiveDeliveryFailureMode {
        /// The watch is pointed at a real category channel: `create_message` against it always
        /// fails with a non-"unknown channel" error (categories can never hold messages), so the
        /// one recovery branch is never even triggered and every tick fails identically.
        Persistent,
        /// Only the tab thread is deleted; the workspace channel survives, so the one
        /// unknown-channel recovery attempt inside `handle_live_event` re-resolves and recreates
        /// the thread within the same tick.
        Transient,
    }

    #[cfg(unix)]
    struct LiveDeliveryAttemptsCase {
        name: &'static str,
        mode: LiveDeliveryFailureMode,
    }

    /// The [`LiveDeliveryFailureMode::Persistent`] arm of [`live_delivery_attempts_exercise`],
    /// split out to keep that function under the line-count lint: drives [`handle_live_event`]
    /// [`LIVE_DELIVERY_ATTEMPTS`] times and asserts the terminal is marked
    /// [`BridgeState::live_unfollowable`] and its watch dropped only on the final attempt, never
    /// before.
    #[cfg(unix)]
    async fn assert_persistent_delivery_gives_up(
        connection: &super::DiscordConnection,
        terminal: &str,
        state: &mut BridgeState,
    ) -> Result<(), String> {
        for attempt in 1..=LIVE_DELIVERY_ATTEMPTS {
            handle_live_event(Some(connection), terminal, state).await;
            let gave_up = attempt == LIVE_DELIVERY_ATTEMPTS;
            let is_unfollowable = state.live_unfollowable.contains(terminal);
            let has_watch = state.live_watches.contains_key(terminal);
            if gave_up != is_unfollowable || gave_up == has_watch {
                return Err(format!(
                    "attempt {attempt}: expected gave_up={gave_up}, got \
                     unfollowable={is_unfollowable} has_watch={has_watch}"
                ));
            }
        }
        Ok(())
    }

    /// The [`LiveDeliveryFailureMode::Transient`] arm of [`live_delivery_attempts_exercise`],
    /// split out to keep that function under the line-count lint: drives one
    /// [`handle_live_event`] and asserts the stuck text was redelivered, the terminal was never
    /// marked unfollowable, and its attempt counter was reset.
    #[cfg(unix)]
    async fn assert_transient_delivery_redelivers(
        connection: &super::DiscordConnection,
        terminal: &str,
        state: &mut BridgeState,
    ) -> Result<(), String> {
        handle_live_event(Some(connection), terminal, state).await;
        if !state.last_posted.contains_key(terminal) {
            Err("transient delivery failure did not redeliver once it cleared".to_owned())
        } else if state.live_unfollowable.contains(terminal) {
            Err("a single transient failure must not mark the terminal unfollowable".to_owned())
        } else if state.live_delivery_attempts.contains_key(terminal) {
            Err("a fully recovered delivery must reset the attempt counter".to_owned())
        } else {
            Ok(())
        }
    }

    /// Binds a live watch with one pending text to a channel that fails delivery in the shape
    /// `mode` describes, then dispatches to [`assert_persistent_delivery_gives_up`] or
    /// [`assert_transient_delivery_redelivers`].
    #[cfg(unix)]
    async fn live_delivery_attempts_exercise(
        guild: &BlockedCaptureGuild,
        tab: &Tab,
        mode: &LiveDeliveryFailureMode,
    ) -> Result<(), String> {
        report_idle_with_session(&tab.pane_id)?;
        let listed = snapshot_for_pane(&tab.pane_id)?;
        let matching = matching_tab(&tab.tab_id)?;
        let tabs = std::slice::from_ref(&matching);
        let agents = std::slice::from_ref(&listed);
        let route = route_topology(agents, tabs, &listed.terminal_id)?;
        let terminal = listed.terminal_id.clone();

        let topology_cache: herdr_connect_rs::TopologyCache =
            Arc::new(tokio::sync::Mutex::new(None));
        let created_thread =
            sync_route(guild.client.as_ref(), guild.id, &route, &topology_cache).await?;

        let nonce = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        );
        let stuck_channel = match mode {
            LiveDeliveryFailureMode::Persistent => {
                guild
                    .client
                    .create_guild_channel(guild.id, &format!("testrun-category-{nonce}"))
                    .kind(twilight_model::channel::ChannelType::GuildCategory)
                    .await
                    .map_err(|error| error.to_string())?
                    .model()
                    .await
                    .map_err(|error| error.to_string())?
                    .id
            }
            LiveDeliveryFailureMode::Transient => {
                guild
                    .client
                    .delete_channel(created_thread)
                    .await
                    .map_err(|error| error.to_string())?;
                created_thread
            }
        };

        let live_path = std::env::temp_dir().join(format!("testrun-live-attempts-{nonce}.jsonl"));
        fs::write(
            &live_path,
            "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\",\"content\":\
             [{\"type\":\"text\",\"text\":\"live-attempts stuck text\"}]}}\n",
        )
        .map_err(|error| error.to_string())?;
        let (live_tx, _live_rx) = tokio::sync::mpsc::unbounded_channel();
        let watcher = start_notify_watcher(VENDOR_CLAUDE, &live_path, terminal.clone(), live_tx)?;
        let mut state = BridgeState::default();
        state.live_watches.insert(
            terminal.clone(),
            LiveWatch {
                _watcher: watcher,
                vendor: VENDOR_CLAUDE.to_owned(),
                path: live_path.clone(),
                position: LivePosition::Bytes(0),
                channel: stuck_channel,
                route: route.clone(),
            },
        );
        let connection = discord_tuple_with_cache(guild, Arc::clone(&topology_cache));

        let result = match mode {
            LiveDeliveryFailureMode::Persistent => {
                assert_persistent_delivery_gives_up(&connection, &terminal, &mut state).await
            }
            LiveDeliveryFailureMode::Transient => {
                assert_transient_delivery_redelivers(&connection, &terminal, &mut state).await
            }
        };

        let _ = fs::remove_file(&live_path);
        if matches!(mode, LiveDeliveryFailureMode::Persistent) {
            let _ = guild.client.delete_channel(stuck_channel).await;
        }
        result
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn live_delivery_gives_up_after_bounded_attempts() {
        let cases = [
            LiveDeliveryAttemptsCase {
                name: "persistent failure gives up",
                mode: LiveDeliveryFailureMode::Persistent,
            },
            LiveDeliveryAttemptsCase {
                name: "transient failure redelivers",
                mode: LiveDeliveryFailureMode::Transient,
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
            assert_eq!(
                remaining_tabs(LIVE_ATTEMPTS_LABEL).expect("tab.list succeeds"),
                0,
                "named zero-leftover check: {}",
                case.name
            );

            let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
                .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
            let cwd_dir = std::env::temp_dir().join(format!(
                "testrun-live-attempts-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("system clock is after unix epoch")
                    .as_nanos()
            ));
            fs::create_dir_all(&cwd_dir).expect("create live-attempts test cwd");
            let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

            let created = create_tab(LIVE_ATTEMPTS_LABEL, &workspace_id, cwd);
            let (tab_id, result) = match created {
                Ok(tab) => {
                    let outcome = live_delivery_attempts_exercise(&guild, &tab, &case.mode).await;
                    (Some(tab.tab_id), outcome)
                }
                Err(error) => (None, Err(error)),
            };
            if let Some(tab_id) = &tab_id {
                close_tab(tab_id);
            }
            let _ = fs::remove_dir_all(&cwd_dir);

            let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
            let tabs_left = remaining_tabs(LIVE_ATTEMPTS_LABEL)
                .expect("tab.list succeeds for the zero-leftover check");
            assert!(result.is_ok(), "{}: {result:?}", case.name);
            assert_eq!(channels_left, 0, "named zero-leftover check: {}", case.name);
            assert_eq!(tabs_left, 0, "named zero-leftover check: {}", case.name);
        }
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
            Some(&connection),
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
            Some(&connection),
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
    const RESUBSCRIBE_RECONCILE_LABEL: &str = "testrun-resubscribe-reconcile";

    /// Polls until a tab's thread is gone or `bound` elapses: the resubscribe reconciliation
    /// sweep runs on a spawned task rather than being awaited inline (exactly like the startup
    /// sweep it reuses), so its effect on Discord is only eventually observable.
    #[cfg(unix)]
    async fn wait_until_thread_absent(
        guild: &BlockedCaptureGuild,
        channel_id: Id<ChannelMarker>,
        suffix: &str,
        bound: Duration,
    ) -> Result<(), String> {
        let deadline = Instant::now() + bound;
        loop {
            if !thread_with_suffix_survives(guild, channel_id, suffix).await? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "thread with suffix {suffix} was not deleted within {bound:?}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
    }

    /// One row in the table [`resubscribe_reconciliation_exercise`] checks after a forced
    /// resubscribe: whether the named tab's thread is expected to survive the reconciliation
    /// sweep.
    #[cfg(unix)]
    struct ResubscribeReconcileExpectation {
        name: &'static str,
        suffix: String,
        survives: bool,
    }

    /// Drives a real forced lifecycle-subscribe error through `handle_lifecycle_select_result` and
    /// asserts that the resubscribe's own reconciliation sweep -- not a live `tab.closed` event --
    /// deletes a thread whose tab was closed while the subscribe stream was down, while a live
    /// tab's thread survives.
    #[cfg(unix)]
    async fn resubscribe_reconciliation_exercise(
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

        // Close the second tab without ever running its tab.closed event through the lifecycle
        // event loop: this stands in for a closure that happened while the subscribe stream was
        // down, so only the resubscribe's own reconciliation sweep -- not a live event -- can
        // catch it.
        close_tab(&second_tab.tab_id);

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

        let alive = handle_lifecycle_select_result(
            Err("test-forced subscribe error".to_owned()),
            Some(&connection),
            &mut stop,
            &mut broker,
            &mut runtime,
        )
        .await;
        if !alive {
            return Err(
                "handle_lifecycle_select_result reported shutdown on a forced resubscribe error"
                    .to_owned(),
            );
        }

        let expectations = [
            ResubscribeReconcileExpectation {
                name: "a thread whose tab was closed before the resubscribe is deleted after it",
                suffix: second_suffix,
                survives: false,
            },
            ResubscribeReconcileExpectation {
                name: "a live tab keeps its thread",
                suffix: root_suffix,
                survives: true,
            },
        ];
        for expectation in expectations {
            if expectation.survives {
                if !thread_with_suffix_survives(guild, channel.id, &expectation.suffix).await? {
                    return Err(format!("{}: thread did not survive", expectation.name));
                }
            } else {
                wait_until_thread_absent(
                    guild,
                    channel.id,
                    &expectation.suffix,
                    Duration::from_secs(20),
                )
                .await
                .map_err(|error| format!("{}: {error}", expectation.name))?;
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    #[tokio::test]
    #[serial]
    async fn resubscribe_reconciles_topology_against_closures_missed_while_down() {
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
            remaining_tabs(RESUBSCRIBE_RECONCILE_LABEL).expect("tab.list succeeds"),
            0,
            "named zero-leftover check"
        );
        assert_eq!(
            remaining_workspaces(RESUBSCRIBE_RECONCILE_LABEL).expect("workspace.list succeeds"),
            0,
            "named zero-leftover check"
        );

        let cwd_dir = std::env::temp_dir().join(format!(
            "testrun-resubscribe-reconcile-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock is after unix epoch")
                .as_nanos()
        ));
        fs::create_dir_all(&cwd_dir).expect("create resubscribe-reconcile test cwd");
        let cwd = cwd_dir.to_str().expect("temp cwd is valid UTF-8");

        let created = match create_workspace(RESUBSCRIBE_RECONCILE_LABEL, cwd) {
            Ok(workspace) => match create_tab(RESUBSCRIBE_RECONCILE_LABEL, &workspace.id, cwd) {
                Ok(second_tab) => Ok((workspace, second_tab)),
                Err(error) => Err((Some(workspace.id), error)),
            },
            Err(error) => Err((None, error)),
        };
        let (workspace_id, result) = match created {
            Ok((workspace, second_tab)) => {
                let workspace_id = workspace.id.clone();
                let outcome =
                    resubscribe_reconciliation_exercise(&guild, &workspace, &second_tab).await;
                (Some(workspace_id), outcome)
            }
            Err((workspace_id, error)) => (workspace_id, Err(error)),
        };
        if let Some(workspace_id) = &workspace_id {
            close_workspace(workspace_id);
        }
        let _ = fs::remove_dir_all(&cwd_dir);

        let channels_left = blocked_capture_cleanup(&guild).await.unwrap();
        let tabs_left = remaining_tabs(RESUBSCRIBE_RECONCILE_LABEL)
            .expect("tab.list succeeds for the zero-leftover check");
        let workspaces_left = remaining_workspaces(RESUBSCRIBE_RECONCILE_LABEL)
            .expect("workspace.list succeeds for the zero-leftover check");
        assert!(result.is_ok(), "{result:?}");
        assert_eq!(channels_left, 0, "named zero-leftover check");
        assert_eq!(tabs_left, 0, "named zero-leftover check");
        assert_eq!(workspaces_left, 0, "named zero-leftover check");
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
            Some(connection),
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
            Some(connection),
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
        process_snapshot(
            &idle,
            std::slice::from_ref(&idle),
            tabs,
            Some(connection),
            state,
        )
        .await;

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
                    let unknown = wait_for_status(
                        &workspace.pane_id,
                        &["unknown"],
                        Duration::from_secs(10),
                    )
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
                        Some(&connection),
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

                    let route = route_topology(std::slice::from_ref(&idle), tabs, &idle.terminal_id)?;
                    let topic = format!("herdr workspace [{}]", route.workspace_id);
                    if !channel_with_topic_is_absent(&guild, &topic).await? {
                        return Err(
                            "unknown session-less observation created the workspace channel".to_owned(),
                        );
                    }
                    process_snapshot(
                        &idle,
                        std::slice::from_ref(&idle),
                        tabs,
                        Some(&connection),
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

                    process_snapshot(
                        &idle,
                        std::slice::from_ref(&idle),
                        tabs,
                        Some(&connection),
                        &mut state,
                    )
                    .await;
                    let repeated_threads = active_threads_for_guild(&guild)
                        .await?
                        .into_iter()
                        .filter(|thread| {
                            thread.parent_id == Some(channel.id)
                                && thread
                                    .name
                                    .as_deref()
                                    .is_some_and(|name| name.ends_with(&thread_suffix))
                        })
                        .count();
                    if repeated_threads != 1 {
                        return Err(format!(
                            "repeated idle snapshot changed late-session topology thread count to {repeated_threads}"
                        ));
                    }
                    let repeated_messages = thread_messages(&guild, matching_threads[0].id).await?;
                    if repeated_messages != messages {
                        return Err(format!(
                            "repeated idle snapshot changed topology messages from {messages:?} to {repeated_messages:?}"
                        ));
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
        let mut state = BridgeState::default();
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

        if !channel_with_topic_is_absent(guild, &claude_topic).await? {
            return Err(
                "reporting a session alone already created the workspace channel".to_owned(),
            );
        }
        process_snapshot(
            &with_session,
            std::slice::from_ref(&with_session),
            claude_tabs,
            Some(&connection),
            &mut state,
        )
        .await;

        prompt_claude_agent_and_wait(agent_name, "Reply with exactly the word ready.")?;
        let settled = snapshot_for_pane(&claude_workspace.pane_id)?;
        process_snapshot(
            &settled,
            std::slice::from_ref(&settled),
            claude_tabs,
            Some(&connection),
            &mut state,
        )
        .await;

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
        let messages = thread_card_descriptions(guild, thread.id).await?;
        if messages
            .iter()
            .any(|description| description.to_lowercase().contains("ready"))
        {
            Ok(())
        } else {
            Err(format!(
                "reporting a session did not post the new transition's card, thread has {messages:?}"
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
