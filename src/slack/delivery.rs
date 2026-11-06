use slack_morphism::errors::SlackClientError;
use slack_morphism::prelude::*;

use super::{Slack, SlackRoute, error};
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
            Ok(response) => Ok(response.ts),
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
}
