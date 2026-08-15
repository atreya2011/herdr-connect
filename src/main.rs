use herdr_connect_rs::{
    AgentLogCapture, AgentSession, TopologyRoute, Transition, create_transition_messages,
    deliver_transition_card, drive_gateway_with_owner_prompt, is_postable_transition, list_agents,
    load_config, load_discord_config, route_topology, sync_topology, tab_list_result,
    transition_card_nonce,
};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use twilight_http::Client;
use twilight_model::id::{
    Id,
    marker::{GuildMarker, MessageMarker},
};

type DiscordConnection = (Arc<Client>, Id<GuildMarker>, String);

fn capture_for(agent: &str, terminal: &str) -> AgentLogCapture {
    let agent_session = AgentSession {
        agent: agent.to_owned(),
        value: terminal.to_owned(),
    };
    let path = std::env::var("HERDR_LOG_DIR").map_or_else(
        |_| PathBuf::from(&agent_session.value),
        |directory| PathBuf::from(directory).join(terminal),
    );
    herdr_connect_rs::read_agent_log(Some(agent_session), &path).map_or_else(
        |error| AgentLogCapture {
            message: error,
            failure: None,
            question: None,
        },
        |log| AgentLogCapture {
            message: log.message,
            failure: log.failure,
            question: log.question,
        },
    )
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

fn discord_connection() -> Result<Option<DiscordConnection>, Box<dyn std::error::Error>> {
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
            tokio::spawn(drive_gateway_with_owner_prompt(
                config.token,
                None,
                Arc::clone(&client),
                guild,
                config.owner_id.clone(),
                notices_tx,
            ));
            Ok(Some((client, guild, config.owner_id)))
        }
        _ => Ok(None),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let home = std::env::var("HOME").unwrap_or_default();
    let app_config = load_config(&[], &home);
    let discord = discord_connection()?;
    let interval = app_config.poll_interval_ms;
    let mut previous: HashMap<String, (String, String)> = HashMap::new();
    let mut state_change_sequences: HashMap<String, u64> = HashMap::new();
    let mut stop = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut live_messages: HashMap<String, Id<MessageMarker>> = HashMap::new();

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
        state_change_sequences.retain(|terminal, _| current.contains(terminal));
        for snapshot in &agents {
            let agent = snapshot.agent.clone();
            let terminal = snapshot.terminal_id.clone();
            let status = snapshot.agent_status.clone();
            println!("{agent} {terminal}: {status}");
            if let Some((old, prior_agent)) = previous.get(&terminal).cloned()
                && old != status
            {
                let state_change_seq = state_change_sequences
                    .entry(terminal.clone())
                    .and_modify(|sequence| *sequence += 1)
                    .or_insert(1);
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
                let capture = capture_for(&agent, &terminal);
                let route = match route_topology(&agents, &tabs, &terminal) {
                    Ok(route) => route,
                    Err(error) => {
                        eprintln!("{error}");
                        previous.insert(terminal.clone(), (status.clone(), agent));
                        continue;
                    }
                };
                let Some((client, guild, owner_id)) = discord.as_ref() else {
                    previous.insert(terminal.clone(), (status.clone(), agent));
                    continue;
                };
                let message_id =
                    match deliver_to_route(
                        client.as_ref(),
                        *guild,
                        owner_id,
                        &route,
                        &transition,
                        &capture,
                        *state_change_seq,
                    )
                    .await
                    {
                        Ok(message_id) => message_id,
                        Err(error) => {
                            eprintln!("{error}");
                            previous.insert(terminal.clone(), (status.clone(), agent));
                            continue;
                        }
                    };
                live_messages.insert(terminal.clone(), message_id);
                let _ = live_messages.get(&terminal);
            }
            previous.insert(terminal.clone(), (status.clone(), agent));
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(interval)) => {},
            _ = tokio::signal::ctrl_c() => break,
            _ = stop.recv() => break,
        }
    }
    Ok(())
}
