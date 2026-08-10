use herdr_connect_rs::list_agents;
use std::collections::HashMap;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let interval = std::env::var("HERDR_POLL_INTERVAL_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_500);
    let mut previous = HashMap::new();
    loop {
        let agents = list_agents()?;
        for (agent, terminal, status) in agents {
            if let Some(old) = previous.insert(terminal.clone(), status.clone())
                && old != status
            {
                println!("{agent} {terminal}: {old} -> {status}");
            }
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(interval)) => {},
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    Ok(())
}
