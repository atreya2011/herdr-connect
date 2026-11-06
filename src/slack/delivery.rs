use slack_morphism::errors::SlackClientError;
use slack_morphism::prelude::*;

use super::{Slack, SlackRoute, SlackSocketEvent, error};
use crate::ChatError;

pub(super) fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

impl Slack {
    pub async fn send(&self, route: &SlackRoute, text: &str) -> Result<SlackTs, ChatError> {
        self.send_content(route, SlackMessageContent::new().with_text(escape(text)))
            .await
    }

    pub(super) async fn send_content(
        &self,
        route: &SlackRoute,
        content: SlackMessageContent,
    ) -> Result<SlackTs, ChatError> {
        let request = SlackApiChatPostMessageRequest::new(route.channel.clone(), content)
            .opt_thread_ts(route.thread.clone())
            .with_parse("none".into())
            .with_unfurl_links(false)
            .with_unfurl_media(false);
        let result = self
            .client
            .open_session(&self.token)
            .chat_post_message(&request)
            .await;
        match result {
            Ok(response) => {
                if self
                    .working
                    .lock()
                    .await
                    .get(route)
                    .is_some_and(Option::is_some)
                {
                    self.set_working(route, true).await?;
                }
                Ok(response.ts)
            }
            Err(failure) => {
                self.invalidate_if_gone(&failure).await;
                Err(error(failure))
            }
        }
    }

    pub async fn update(
        &self,
        route: &SlackRoute,
        message: &SlackTs,
        text: &str,
    ) -> Result<(), ChatError> {
        self.update_content(
            route,
            message,
            SlackMessageContent::new()
                .with_text(escape(text))
                .with_blocks(Vec::new()),
        )
        .await
    }

    pub(super) async fn update_content(
        &self,
        route: &SlackRoute,
        message: &SlackTs,
        content: SlackMessageContent,
    ) -> Result<(), ChatError> {
        self.client
            .open_session(&self.token)
            .chat_update(
                &SlackApiChatUpdateRequest::new(route.channel.clone(), content, message.clone())
                    .with_parse("none".into()),
            )
            .await
            .map_err(error)?;
        Ok(())
    }

    async fn invalidate_if_gone(&self, failure: &SlackClientError) {
        if let SlackClientError::ApiError(api) = failure
            && matches!(
                api.code.as_str(),
                "channel_not_found" | "is_archived" | "thread_not_found"
            )
        {
            let mut topology = self.topology.lock().await;
            topology.loaded = false;
            topology.threads.clear();
        }
    }

    pub async fn mirror_prompt(&self, route: &SlackRoute, text: &str) -> Result<(), ChatError> {
        self.send_content(
            route,
            SlackMessageContent::new().with_text(format!(
                "Terminal prompt from <@{}>:\n{}",
                self.owner,
                escape(text)
            )),
        )
        .await?;
        Ok(())
    }

    pub async fn set_working(&self, route: &SlackRoute, working: bool) -> Result<(), ChatError> {
        let thread = route
            .thread
            .clone()
            .ok_or_else(|| error("working status needs a tab thread"))?;
        self.client
            .open_session(&self.token)
            .assistant_threads_set_status(&SlackApiAssistantThreadsSetStatusRequest::new(
                route.channel.clone(),
                if working { "is working…" } else { "" }.into(),
                thread,
            ))
            .await
            .map_err(error)?;
        Ok(())
    }

    pub async fn track_working(&self, route: &SlackRoute, working: bool) -> Result<(), ChatError> {
        let mut tasks = self.working.lock().await;
        if tasks
            .get(route)
            .is_some_and(|task| task.is_some() == working)
        {
            return Ok(());
        }
        if let Some(Some(task)) = tasks.remove(route) {
            task.abort();
        }
        self.set_working(route, working).await?;
        if working {
            let (client, token, route, events) = (
                self.client.clone(),
                self.token.clone(),
                route.clone(),
                self.events_tx.clone(),
            );
            let thread = route
                .thread
                .clone()
                .ok_or_else(|| error("working status needs a tab thread"))?;
            let key = route.clone();
            tasks.insert(
                key,
                Some(tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                        if let Err(failure) = client
                            .open_session(&token)
                            .assistant_threads_set_status(
                                &SlackApiAssistantThreadsSetStatusRequest::new(
                                    route.channel.clone(),
                                    "is working…".into(),
                                    thread.clone(),
                                ),
                            )
                            .await
                        {
                            let _ = events.send(SlackSocketEvent::Failure(error(failure)));
                            return;
                        }
                    }
                })),
            );
        } else {
            tasks.insert(route.clone(), None);
        }
        drop(tasks);
        Ok(())
    }
}

impl Drop for Slack {
    fn drop(&mut self) {
        for task in self.working.get_mut().values().flatten() {
            task.abort();
        }
    }
}
