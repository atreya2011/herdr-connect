use herdr_connect_rs::{
    AgentLogCapture, AgentSession, AgentSnapshot, ComponentHandler, TopologyRoute, Transition,
    create_transition_messages, deliver_transition_card, drive_gateway_with_components,
    hook_timeout, is_postable_transition, list_agents, load_config, load_discord_config,
    route_topology, sync_topology, tab_list_result, transition_card_nonce,
};
use herdr_connect_rs::{
    PermissionResponder, decode_claude_permission_request, encode_claude_decision,
    handle_component, request_decision, run_broker as run_permission_broker,
};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use twilight_http::Client;
use twilight_model::id::{
    Id,
    marker::{GuildMarker, MessageMarker},
};

type DiscordConnection = (
    Arc<Client>,
    Id<GuildMarker>,
    String,
    Arc<PermissionResponder>,
);
type GatewayTask = tokio::task::JoinHandle<Result<(), String>>;
type BrokerTask = tokio::task::JoinHandle<Result<(), String>>;

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
    let target = sync_topology(
        client,
        guild,
        &route.workspace_id,
        &route.channel_name,
        &route.thread_name,
        &route.tab_id,
    )
    .await
    .map_err(|error| format!("discord topology error: {error}"))?;
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

fn next_state_change_sequence(
    state_change_sequences: &mut HashMap<String, u64>,
    terminal: &str,
) -> u64 {
    *state_change_sequences
        .entry(terminal.to_owned())
        .and_modify(|sequence| *sequence += 1)
        .or_insert(1)
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
    let mut input = Vec::new();
    tokio::io::stdin().read_to_end(&mut input).await?;
    let Ok(interaction) = decode_claude_permission_request(&input) else {
        return Ok(());
    };
    let socket_path = socket_path(&args).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "hook requires HERDR_CLAUDE_BROKER_SOCKET or --socket <path>",
        )
    })?;
    let Some(decision) = request_decision(&interaction, &socket_path, hook_timeout()).await else {
        return Ok(());
    };
    let output = encode_claude_decision(&decision)?;
    tokio::io::stdout().write_all(&output).await?;
    Ok(())
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
    let (notices_tx, notices_rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        while let Ok(notice) = notices_rx.recv() {
            eprintln!("{notice}");
        }
    });
    let gateway = tokio::spawn(drive_gateway_with_components(
        config.token,
        None,
        client,
        guild,
        config.owner_id,
        notices_tx,
        component_handler(Arc::clone(&responder)),
    ));
    let result = run_permission_broker(&socket_path, responder).await;
    gateway.abort();
    result?;
    Ok(())
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
    let mut previous: HashMap<String, (String, String)> = HashMap::new();
    let mut state_change_sequences: HashMap<String, u64> = HashMap::new();
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
        previous.retain(|terminal, _| current.contains(terminal));
        for snapshot in &agents {
            let agent = snapshot.agent.clone();
            let terminal = snapshot.terminal_id.clone();
            let status = snapshot.agent_status.clone();
            println!("{agent} {terminal}: {status}");
            if let Some((old, prior_agent)) = previous.get(&terminal).cloned()
                && old != status
            {
                let state_change_seq =
                    next_state_change_sequence(&mut state_change_sequences, &terminal);
                let transition = Transition {
                    from: old,
                    to: status.clone(),
                    terminal_id: terminal.clone(),
                    agent: prior_agent,
                };
                if !is_postable_transition(&transition) {
                    previous.insert(terminal.clone(), (status.clone(), agent));
                    continue;
                }
                let Some(capture) = capture_for_or_report(snapshot) else {
                    previous.insert(terminal.clone(), (status.clone(), agent));
                    continue;
                };
                let route = match route_topology(&agents, &tabs, &terminal) {
                    Ok(route) => route,
                    Err(error) => {
                        eprintln!("{error}");
                        previous.insert(terminal.clone(), (status.clone(), agent));
                        continue;
                    }
                };
                let Some((client, guild, owner_id, _responder)) = discord.as_ref() else {
                    previous.insert(terminal.clone(), (status.clone(), agent));
                    continue;
                };
                if let Err(error) = deliver_to_route(
                    client.as_ref(),
                    *guild,
                    owner_id,
                    &route,
                    &transition,
                    &capture,
                    state_change_seq,
                )
                .await
                {
                    eprintln!("{error}");
                    previous.insert(terminal.clone(), (status.clone(), agent));
                    continue;
                }
            }
            previous.insert(terminal.clone(), (status.clone(), agent));
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
        capture_for_with_search_root, create_transition_messages, next_state_change_sequence,
        resolve_session_path,
    };
    use herdr_connect_rs::{AgentSession, AgentSnapshot, Transition, transition_card_nonce};
    use serde_json::Value;
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::path::Path;
    use std::time::{SystemTime, UNIX_EPOCH};

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
}
