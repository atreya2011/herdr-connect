use herdr_connect_rs::{
    AgentLogCapture, AgentSession, Transition, create_transition_messages, deliver_transition,
    list_agents, load_discord_config, sync_topology, update_live_status,
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
    let config = load_discord_config(&[
        ("DISCORD_TOKEN", &std::env::var("DISCORD_TOKEN")?),
        ("DISCORD_GUILD_ID", &std::env::var("DISCORD_GUILD_ID")?),
        ("DISCORD_OWNER_ID", &std::env::var("DISCORD_OWNER_ID")?),
    ])?;
    let guild = Id::<GuildMarker>::new(config.guild_id.parse()?);
    let client = Client::builder().token(config.token).build();
    let channel = configured_channel();
    let interval = std::env::var("HERDR_POLL_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_500);
    let mut previous: HashMap<String, (String, String)> = HashMap::new();
    let live_messages: HashMap<String, Id<MessageMarker>> = HashMap::new();
    let mut stop = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    loop {
        let agents = list_agents()?;
        let current: HashSet<String> = agents
            .iter()
            .map(|(_, terminal, _)| terminal.clone())
            .collect();
        previous.retain(|terminal, _| current.contains(terminal));
        for (agent, terminal, status) in agents {
            if let Some((old, prior_agent)) = previous.get(&terminal).cloned()
                && old != status
            {
                let transition = Transition {
                    from: old,
                    to: status.clone(),
                    terminal_id: terminal.clone(),
                    agent: prior_agent,
                };
                let capture = capture_for(&agent, &terminal);
                let messages = create_transition_messages(&transition, &capture, &config.owner_id);
                sync_topology(&client, guild, "workspace", &terminal).await?;
                if let Some(channel) = channel {
                    for (index, message) in messages.iter().enumerate() {
                        let nonce = format!("{terminal}-{index}");
                        deliver_transition(&client, channel, &message.description, &nonce).await?;
                    }
                }
            }
            previous.insert(terminal.clone(), (status.clone(), agent));
            if let Some(channel) = channel {
                update_live_status(
                    &client,
                    channel,
                    &terminal,
                    live_messages.get(&terminal).copied(),
                )
                .await?;
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
