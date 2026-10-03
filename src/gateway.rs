use std::pin::Pin;
use std::sync::mpsc::Sender;
use std::{future::Future, sync::Arc};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use twilight_gateway::{
    ConfigBuilder, Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _,
};
use twilight_http::Client;
use twilight_model::id::{Id, marker::GuildMarker};

use crate::broker::PermissionResponder;
use crate::deletion::{GuildDeletion, handle_guild_deletion};

pub type ComponentHandler = Arc<
    dyn Fn(
            twilight_model::application::interaction::Interaction,
        ) -> Pin<Box<dyn Future<Output = ()> + Send>>
        + Send
        + Sync,
>;

/// The Discord identity and permission state shared by every owner message the gateway dispatches,
/// bundled so [`drive_gateway_with_components`] stays under the arity lint.
#[derive(Clone)]
pub struct GatewayContext {
    pub client: Arc<Client>,
    pub guild: Id<GuildMarker>,
    pub owner_id: String,
    pub responder: Arc<PermissionResponder>,
}

struct OwnerPromptRequest {
    context: GatewayContext,
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
        request.context.client,
        request.context.guild,
        &request.context.owner_id,
        request.message,
        request.context.responder.as_ref(),
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
    context: GatewayContext,
    notices: Sender<String>,
    components: ComponentHandler,
) -> Result<(), String> {
    let intents = Intents::GUILDS | Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT;
    let config = ConfigBuilder::new(token, intents).build();
    let mut shard = Shard::with_config(ShardId::ONE, config);
    let owner_prompt_sender = spawn_owner_prompt_consumer();
    while let Some(item) = shard
        .next_event(
            EventTypeFlags::MESSAGE_CREATE
                | EventTypeFlags::INTERACTION_CREATE
                | EventTypeFlags::THREAD_DELETE
                | EventTypeFlags::CHANNEL_DELETE,
        )
        .await
    {
        let notice = match item {
            Ok(Event::MessageCreate(message)) => {
                let request = OwnerPromptRequest {
                    context: context.clone(),
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
            Ok(Event::ThreadDelete(thread)) if thread.guild_id == context.guild => {
                spawn_deletion(&context, &notices, GuildDeletion::Thread { id: thread.id });
                "discord gateway deletion: THREAD_DELETE".to_owned()
            }
            Ok(Event::ChannelDelete(channel)) if channel.guild_id == Some(context.guild) => {
                spawn_deletion(
                    &context,
                    &notices,
                    GuildDeletion::Channel(Box::new(channel.0)),
                );
                "discord gateway deletion: CHANNEL_DELETE".to_owned()
            }
            Ok(_) => continue,
            Err(error) => format!("discord gateway error: {error}"),
        };
        if notices.send(notice).is_err() {
            break;
        }
    }
    Err("discord gateway fatally closed; owner prompts are no longer received".to_owned())
}

fn spawn_deletion(context: &GatewayContext, notices: &Sender<String>, deletion: GuildDeletion) {
    let client = Arc::clone(&context.client);
    let responder = Arc::clone(&context.responder);
    let notices = notices.clone();
    tokio::spawn(async move {
        if let Err(error) =
            handle_guild_deletion(&client, responder.topology_cache(), deletion).await
        {
            let _ = notices.send(format!("discord owner deletion error: {error}"));
        }
    });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::consume_in_order;

    #[tokio::test]
    async fn owner_prompt_queue_consumes_messages_in_receive_order() {
        let messages = vec![1_u8, 2, 3];
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
        for message in messages.iter().copied() {
            sender
                .send(message)
                .expect("owner prompt queue accepts message");
        }
        drop(sender);
        consumer.await.expect("owner prompt consumer joins");

        assert_eq!(*observed.lock().await, messages);
    }
}
