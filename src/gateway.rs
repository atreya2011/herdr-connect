use std::pin::Pin;
use std::sync::mpsc::Sender;
use std::{future::Future, sync::Arc};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use twilight_gateway::{
    ConfigBuilder, Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _,
};
use twilight_http::Client;
use twilight_model::id::{Id, marker::GuildMarker};

pub type ComponentHandler = Arc<
    dyn Fn(
            twilight_model::application::interaction::Interaction,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

struct OwnerPromptRequest {
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: String,
    message: twilight_model::channel::Message,
    notices: Sender<String>,
}

async fn consume_in_order<T, F, Fut>(mut receiver: UnboundedReceiver<T>, mut process: F)
where
    T: Send + 'static,
    F: FnMut(T) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    while let Some(item) = receiver.recv().await {
        process(item).await;
    }
}

fn spawn_owner_prompt_consumer() -> UnboundedSender<OwnerPromptRequest> {
    let (sender, receiver) = unbounded_channel();
    tokio::spawn(consume_in_order(receiver, process_owner_prompt));
    sender
}

async fn process_owner_prompt(request: OwnerPromptRequest) {
    let result = crate::prompting::handle_owner_message(
        request.client,
        request.guild,
        &request.owner_id,
        request.message,
    )
    .await;
    if let Err(error) = result {
        let _ = request
            .notices
            .send(format!("discord owner prompt error: {error}"));
    }
}

/// Connects the Discord gateway and dispatches owner prompts and component taps.
///
/// # Errors
///
/// Returns an error when the Discord gateway terminates.
pub async fn drive_gateway_with_components(
    token: String,
    gateway_url: Option<String>,
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: String,
    notices: Sender<String>,
    components: ComponentHandler,
) -> Result<(), String> {
    drive_gateway(
        token,
        gateway_url,
        client,
        guild,
        owner_id,
        notices,
        components,
    )
    .await
}

async fn drive_gateway(
    token: String,
    gateway_url: Option<String>,
    client: Arc<Client>,
    guild: Id<GuildMarker>,
    owner_id: String,
    notices: Sender<String>,
    components: ComponentHandler,
) -> Result<(), String> {
    let intents = Intents::GUILDS | Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;
    let builder = ConfigBuilder::new(token, intents);
    let config = match gateway_url {
        Some(url) => builder.proxy_url(url).build(),
        None => builder.build(),
    };
    let mut shard = Shard::with_config(ShardId::ONE, config);
    let owner_prompt_sender = spawn_owner_prompt_consumer();
    while let Some(item) = shard
        .next_event(EventTypeFlags::MESSAGE_CREATE | EventTypeFlags::INTERACTION_CREATE)
        .await
    {
        let notice = match item {
            Ok(Event::MessageCreate(message)) => {
                let request = OwnerPromptRequest {
                    client: Arc::clone(&client),
                    guild,
                    owner_id: owner_id.clone(),
                    message: message.0,
                    notices: notices.clone(),
                };
                if owner_prompt_sender.send(request).is_err() {
                    "discord owner prompt error: owner prompt queue closed".to_owned()
                } else {
                    "discord gateway message: MESSAGE_CREATE".to_owned()
                }
            }
            Ok(Event::InteractionCreate(interaction)) => {
                let handler = Arc::clone(&components);
                tokio::spawn(async move { handler(interaction.0).await });
                "discord gateway interaction: INTERACTION_CREATE".to_owned()
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
    use std::sync::Arc;
    use std::time::Duration;

    use super::{consume_in_order, gateway_closed_result};

    #[tokio::test]
    async fn owner_prompt_queue_consumes_messages_in_receive_order() {
        let cases = [vec![1_u8, 2, 3], vec![3_u8, 1, 2]];
        for received in cases {
            let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
            let observed = Arc::new(tokio::sync::Mutex::new(Vec::new()));
            let consumer = tokio::spawn(consume_in_order(receiver, {
                let observed = Arc::clone(&observed);
                move |message| {
                    let observed = Arc::clone(&observed);
                    async move {
                        if message == 1 {
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                        observed.lock().await.push(message);
                    }
                }
            }));
            for message in received.iter().copied() {
                sender
                    .send(message)
                    .expect("owner prompt queue accepts message");
            }
            drop(sender);
            consumer.await.expect("owner prompt consumer joins");

            assert_eq!(*observed.lock().await, received);
        }
    }

    #[test]
    fn closed_gateway_returns_terminal_error() {
        assert_eq!(
            gateway_closed_result(),
            Err("discord gateway fatally closed; owner prompts are no longer received".to_owned())
        );
    }
}
