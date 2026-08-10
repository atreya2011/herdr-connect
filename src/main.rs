use std::env;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let home = env::var("HOME")?;
    let socket_path = env::var("HERDR_SOCKET_PATH")
        .unwrap_or_else(|_| format!("{home}/.config/herdr/herdr.sock"));
    let mut stream = UnixStream::connect(socket_path).await?;
    stream
        .write_all(b"{\"id\":\"herdr-connect:watch\",\"method\":\"agent.list\",\"params\":{}}\n")
        .await?;
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).await?;
    let parsed = serde_json::from_str::<Value>(&response)?;
    let agents = parsed
        .get("result")
        .and_then(|result| result.get("agents"))
        .and_then(Value::as_array)
        .ok_or("agent.list response did not contain agents")?;
    for agent in agents.iter().take(3) {
        let kind = agent
            .get("agent")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let terminal = agent
            .get("terminal_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let status = agent
            .get("agent_status")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        println!("{kind} {terminal}: {status}");
    }
    Ok(())
}
