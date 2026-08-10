use herdr_connect_rs::{
    AgentLogCapture, AgentSession, Transition, create_transition_messages, deliver_transition,
    is_postable_transition, list_agents, load_config, load_discord_config, sync_topology,
    update_live_status,
};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;
use twilight_http::Client;
use twilight_model::id::{
    Id,
    marker::{ChannelMarker, GuildMarker, MessageMarker},
};

fn capture_for(agent: &str, terminal: &str) -> AgentLogCapture {
    let path = std::env::var("HERDR_LOG_DIR").map_or_else(
        |_| PathBuf::from(terminal),
        |directory| PathBuf::from(directory).join(terminal),
    );
    herdr_connect_rs::read_agent_log(
        Some(AgentSession {
            agent: agent.to_owned(),
            value: terminal.to_owned(),
        }),
        &path,
    )
    .map_or_else(
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

fn configured_channel() -> Option<Id<ChannelMarker>> {
    std::env::var("DISCORD_CHANNEL_ID")
        .ok()?
        .parse()
        .ok()
        .map(Id::new)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let home = std::env::var("HOME").unwrap_or_default();
    let app_config = load_config(&[], &home);
    let discord = match (
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
            let client = Client::builder().token(config.token).build();
            Some((client, guild, config.owner_id))
        }
        _ => None,
    };
    let channel = configured_channel();
    let interval = app_config.poll_interval_ms;
    let mut previous: HashMap<String, (String, String)> = HashMap::new();
    let live_messages: HashMap<String, Id<MessageMarker>> = HashMap::new();
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
        let current: HashSet<String> = agents
            .iter()
            .map(|(_, terminal, _)| terminal.clone())
            .collect();
        previous.retain(|terminal, _| current.contains(terminal));
        for (agent, terminal, status) in agents {
            println!("{agent} {terminal}: {status}");
            if let Some((old, prior_agent)) = previous.get(&terminal).cloned()
                && old != status
            {
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
                let Some((client, guild, owner_id)) = discord.as_ref() else {
                    previous.insert(terminal.clone(), (status.clone(), agent));
                    continue;
                };
                let messages = create_transition_messages(&transition, &capture, owner_id);
                if let Err(error) = sync_topology(client, *guild, "workspace", &terminal).await {
                    eprintln!("discord topology error: {error}");
                    continue;
                }
                if let Some(channel) = channel {
                    for (index, message) in messages.iter().enumerate() {
                        let nonce = format!("{terminal}-{index}");
                        if let Err(error) =
                            deliver_transition(client, channel, &message.description, &nonce).await
                        {
                            eprintln!("discord delivery error: {error}");
                            break;
                        }
                    }
                }
            }
            previous.insert(terminal.clone(), (status.clone(), agent));
            if let Some((client, _, _)) = discord.as_ref()
                && let Some(channel) = channel
                && let Err(error) = update_live_status(
                    client,
                    channel,
                    &terminal,
                    live_messages.get(&terminal).copied(),
                )
                .await
            {
                eprintln!("discord live-status error: {error}");
            }
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(interval)) => {},
            _ = tokio::signal::ctrl_c() => break,
            _ = stop.recv() => break,
        }
    }
    Ok(())
}
