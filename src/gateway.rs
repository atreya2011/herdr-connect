use std::future::Future;
use std::sync::Arc;
use std::sync::mpsc::Sender;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use crate::broker::{PermissionResponder, handle_component};
use crate::chat::{Chat, Closed, Inbound};
use crate::{tab_close, workspace_close};

struct OwnerPromptRequest<C: Chat> {
    responder: Arc<PermissionResponder<C>>,
    posted: C::Posted,
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

fn spawn_owner_prompt_consumer<C: Chat>() -> UnboundedSender<OwnerPromptRequest<C>> {
    let (sender, receiver) = unbounded_channel();
    tokio::spawn(consume_in_order(receiver, process_owner_prompt::<C>));
    sender
}

async fn process_owner_prompt<C: Chat>(request: OwnerPromptRequest<C>) {
    let result =
        crate::prompting::handle_owner_message(request.responder.as_ref(), request.posted).await;
    if let Err(error) = result {
        let _ = request
            .notices
            .send(format!("discord owner prompt error: {error}"));
    }
}

pub fn close_in_herdr(closed: Closed) -> Result<(), String> {
    match closed {
        Closed::Tab(tab_id) => tab_close(&tab_id),
        Closed::Workspace(workspace_id) => workspace_close(&workspace_id),
    }
    .map(drop)
}

pub async fn drive_gateway<C: Chat>(
    responder: Arc<PermissionResponder<C>>,
    notices: Sender<String>,
) -> Result<(), String> {
    let owner_prompt_sender = spawn_owner_prompt_consumer::<C>();
    while let Some(item) = responder.chat().next_event().await {
        let notice = match item {
            Ok(Inbound::Posted(posted)) => {
                let request = OwnerPromptRequest {
                    responder: Arc::clone(&responder),
                    posted,
                    notices: notices.clone(),
                };
                if owner_prompt_sender.send(request).is_err() {
                    "discord owner prompt error: owner prompt queue closed".to_owned()
                } else {
                    continue;
                }
            }
            Ok(Inbound::Component(component)) => {
                tokio::spawn(handle_component(Arc::clone(&responder), component));
                continue;
            }
            Ok(Inbound::Deleted(deletion)) => {
                let label = deletion.to_string();
                spawn_deletion(&responder, &notices, deletion);
                format!("discord gateway deletion: {label}")
            }
            Err(error) => format!("discord gateway error: {error}"),
        };
        if notices.send(notice).is_err() {
            break;
        }
    }
    Err("discord gateway fatally closed; owner prompts are no longer received".to_owned())
}

fn spawn_deletion<C: Chat>(
    responder: &Arc<PermissionResponder<C>>,
    notices: &Sender<String>,
    deletion: C::Deletion,
) {
    let responder = Arc::clone(responder);
    let notices = notices.clone();
    tokio::spawn(async move {
        if let Err(error) = responder
            .chat()
            .handle_deletion(deletion, close_in_herdr)
            .await
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
