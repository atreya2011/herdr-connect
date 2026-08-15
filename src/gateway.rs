use std::sync::mpsc::Sender;
use std::{future::Future, sync::Arc};
use tokio::sync::Mutex;
use twilight_gateway::{
    ConfigBuilder, Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _,
};
use twilight_http::Client;
use twilight_model::id::{Id, marker::GuildMarker};

async fn with_owner_prompt_gate<F, Fut>(gate: &Mutex<()>, action: F) -> Result<(), String>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<(), String>>,
{
    let _guard = gate.lock().await;
    action().await
}

/// Connects the Discord gateway and dispatches owner prompts without blocking gateway progress.
///
/// # Errors
///
/// Returns an error when the Discord gateway terminates.
pub async fn drive_gateway_with_owner_prompt(
    token: String,
    gateway_url: Option<String>,
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: String,
    notices: Sender<String>,
) -> Result<(), String> {
    let intents = Intents::GUILDS | Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;
    let builder = ConfigBuilder::new(token, intents);
    let config = match gateway_url {
        Some(url) => builder.proxy_url(url).build(),
        None => builder.build(),
    };
    let mut shard = Shard::with_config(ShardId::ONE, config);
    let owner_prompt_gate = Arc::new(Mutex::new(()));
    while let Some(item) = shard.next_event(EventTypeFlags::MESSAGE_CREATE).await {
        let notice = match item {
            Ok(Event::MessageCreate(message)) => {
                let client = Arc::clone(&client);
                let owner_prompt_gate = Arc::clone(&owner_prompt_gate);
                let notices = notices.clone();
                let owner_id = owner_id.clone();
                tokio::spawn(async move {
                    let result = with_owner_prompt_gate(&owner_prompt_gate, || async {
                        crate::prompting::handle_owner_message(client, guild, &owner_id, message.0)
                            .await
                    })
                    .await;
                    if let Err(error) = result {
                        let _ = notices.send(format!("discord owner prompt error: {error}"));
                    }
                });
                "discord gateway message: MESSAGE_CREATE".to_owned()
            }
            Ok(_) => continue,
            Err(error) => format!("discord gateway error: {error}"),
        };
        if notices.send(notice).is_err() {
            break;
        }
    }
    gateway_closed_result()
}

fn gateway_closed_result() -> Result<(), String> {
    Err("discord gateway fatally closed; owner prompts are no longer received".to_owned())
}

#[cfg(test)]
mod tests {
    use super::{gateway_closed_result, with_owner_prompt_gate};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    #[tokio::test]
    async fn owner_prompt_dispatch_serializes_gate_and_send() {
        let gate = Arc::new(tokio::sync::Mutex::new(()));
        let active = Arc::new(AtomicUsize::new(0));
        let maximum = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..2 {
            let gate = Arc::clone(&gate);
            let active = Arc::clone(&active);
            let maximum = Arc::clone(&maximum);
            tasks.push(tokio::spawn(async move {
                with_owner_prompt_gate(&gate, || async {
                    let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                    maximum.fetch_max(current, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
            }));
        }
        for task in tasks {
            task.await
                .expect("owner prompt task joins")
                .expect("prompt succeeds");
        }
        assert_eq!(maximum.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn closed_gateway_returns_terminal_error() {
        assert_eq!(
            gateway_closed_result(),
            Err("discord gateway fatally closed; owner prompts are no longer received".to_owned())
        );
    }
}
