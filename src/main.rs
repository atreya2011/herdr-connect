use herdr_connect_rs::{
    AgentLogCapture, AgentSnapshot, TopologyRoute, Transition, create_transition_messages,
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
type GatewayTask = tokio::task::JoinHandle<Result<(), String>>;

async fn wait_for_gateway(gateway: Option<&mut GatewayTask>) -> Result<(), String> {
    match gateway {
        Some(gateway) => gateway
            .await
            .map_err(|error| format!("discord gateway task failed: {error}"))?,
        None => std::future::pending().await,
    }
}

fn capture_for(snapshot: &AgentSnapshot) -> Result<AgentLogCapture, String> {
    let log_dir = std::env::var_os("HERDR_LOG_DIR");
    capture_for_with_log_dir(snapshot, log_dir.as_deref().map(std::path::Path::new))
}

fn capture_for_with_log_dir(
    snapshot: &AgentSnapshot,
    log_dir: Option<&std::path::Path>,
) -> Result<AgentLogCapture, String> {
    let Some(session) = snapshot.session.clone() else {
        return Ok(AgentLogCapture {
            message: "agent stopped, no log available".to_owned(),
            failure: None,
            question: None,
        });
    };
    let path = log_dir.map_or_else(
        || PathBuf::from(&session.value),
        |directory| directory.join(&session.value),
    );
    let log = herdr_connect_rs::read_agent_log(Some(session), &path)?;
    Ok(AgentLogCapture {
        message: log.message,
        failure: log.failure,
        question: log.question,
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
            let gateway = tokio::spawn(drive_gateway_with_owner_prompt(
                config.token,
                None,
                Arc::clone(&client),
                guild,
                config.owner_id.clone(),
                notices_tx,
            ));
            Ok(Some(((client, guild, config.owner_id), gateway)))
        }
        _ => Ok(None),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let home = std::env::var("HOME").unwrap_or_default();
    let app_config = load_config(&[], &home);
    let (discord, mut gateway) = match discord_connection()? {
        Some((connection, gateway)) => (Some(connection), Some(gateway)),
        None => (None, None),
    };
    let interval = app_config.poll_interval_ms;
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
                let Some((client, guild, owner_id)) = discord.as_ref() else {
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
                    *state_change_seq,
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
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{capture_for_with_log_dir, create_transition_messages};
    use herdr_connect_rs::{AgentSnapshot, Transition};
    use serde_json::Value;
    use std::path::Path;

    #[test]
    fn captured_sessions_drive_transition_cards_and_pointer_posts() {
        let response: Value =
            serde_json::from_str(include_str!("../tests/fixtures/herdr-agent-list.json"))
                .expect("captured agent.list fixture is JSON");
        let agents: Vec<AgentSnapshot> =
            serde_json::from_value(response["result"]["agents"].clone())
                .expect("captured agent.list fixture has typed agents");
        let cases = [
            ("term-real-1", "final answer"),
            ("term-no-session", "agent stopped, no log available"),
        ];
        for (terminal, expected_body) in cases {
            let snapshot = agents
                .iter()
                .find(|snapshot| snapshot.terminal_id == terminal)
                .expect("fixture contains the requested agent");
            let capture = capture_for_with_log_dir(snapshot, Some(Path::new("tests/fixtures")))
                .expect("capture succeeds for a real session or explicit no-session pointer");
            let transition = Transition {
                from: "working".to_owned(),
                to: "done".to_owned(),
                terminal_id: terminal.to_owned(),
                agent: snapshot.agent.clone(),
            };
            let card = create_transition_messages(&transition, &capture, "owner")
                .into_iter()
                .next()
                .expect("transition produces a card");
            assert_eq!(card.description, expected_body);
        }

        let mut missing_log = agents[0].clone();
        missing_log
            .session
            .as_mut()
            .expect("session fixture is present")
            .value = "missing-session.jsonl".to_owned();
        assert!(
            capture_for_with_log_dir(&missing_log, Some(Path::new("tests/fixtures"))).is_err(),
            "reader errors for a reported session must surface instead of posting the pointer"
        );
    }
}
