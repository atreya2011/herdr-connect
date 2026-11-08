use slack_morphism::errors::SlackClientError;
use slack_morphism::prelude::*;

use super::{Slack, SlackRoute, SlackSocketEvent, SlackState, error};
use crate::{ActivityPlan, ChatError};

pub(super) fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn markdown(text: &str) -> SlackMessageContent {
    SlackMessageContent::new()
        .with_text(escape(text))
        .with_blocks(vec![SlackMarkdownBlock::new(escape(text)).into()])
}

pub(super) fn activity_content(
    text: &str,
    plan: &ActivityPlan,
    block_id: Option<String>,
) -> SlackMessageContent {
    let tasks = plan
        .tasks
        .iter()
        .enumerate()
        .map(|(index, task)| SlackTaskCardBlock {
            task_id: SlackTaskId(task.id.clone()),
            title: task.title.clone(),
            block_id: None,
            status: Some(if index + 1 == plan.tasks.len() {
                SlackTaskCardStatus::InProgress
            } else {
                SlackTaskCardStatus::Complete
            }),
            icon: None,
            hide_title: None,
            details: None,
            output: None,
            sources: None,
        })
        .collect();
    let title = match plan.count {
        0 => crate::PROMPT_TASK_TITLE.to_owned(),
        1 => "1 tool call".to_owned(),
        count => format!("{count} tool calls"),
    };
    let plan = SlackPlanBlock {
        title,
        tasks,
        block_id: block_id.map(SlackBlockId),
    };
    SlackMessageContent::new()
        .with_text(escape(text))
        .with_blocks(vec![plan.into()])
}

impl Slack {
    pub async fn send(&self, route: &SlackRoute, text: &str) -> Result<SlackTs, ChatError> {
        self.send_content(route, SlackMessageContent::new().with_text(escape(text)))
            .await
    }

    pub(super) async fn send_markdown(
        &self,
        route: &SlackRoute,
        text: &str,
    ) -> Result<SlackTs, ChatError> {
        self.send_content(route, markdown(text)).await
    }

    pub(super) async fn send_content(
        &self,
        route: &SlackRoute,
        content: SlackMessageContent,
    ) -> Result<SlackTs, ChatError> {
        self.require_route(route).await?;
        let request = SlackApiChatPostMessageRequest::new(route.channel.clone(), content)
            .with_parse("none".into())
            .with_unfurl_links(false)
            .with_unfurl_media(false);
        let result = self
            .state
            .client
            .open_session(&self.state.token)
            .chat_post_message(&request)
            .await;
        match result {
            Ok(response) => {
                let refresh_status = self
                    .state
                    .working
                    .lock()
                    .await
                    .get(route)
                    .is_some_and(Option::is_some);
                if refresh_status && let Err(failure) = self.set_working(route, true).await {
                    crate::bridge_eprintln!("Slack status refresh error after post: {failure}");
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
        self.require_route(route).await?;
        self.state
            .client
            .open_session(&self.state.token)
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
            let mut topology = self.state.topology.lock().await;
            topology.loaded = false;
            topology.tabs.clear();
        }
    }

    pub async fn mirror_prompt(&self, route: &SlackRoute, text: &str) -> Result<(), ChatError> {
        self.require_route(route).await?;
        let request = SlackApiChatPostMessageRequest::new(route.channel.clone(), markdown(text))
            .with_username(self.owner_identity.display_name.clone())
            .opt_icon_url(self.owner_identity.avatar_url.clone())
            .with_parse("none".into())
            .with_unfurl_links(false)
            .with_unfurl_media(false);
        self.state
            .client
            .open_session(&self.state.token)
            .chat_post_message(&request)
            .await
            .map_err(error)?;
        Ok(())
    }

    pub(super) async fn use_status_message(&self, route: &SlackRoute, message: &SlackTs) {
        let previous = self
            .state
            .status_messages
            .lock()
            .await
            .insert(route.channel.clone(), message.clone());
        if previous
            .as_ref()
            .is_some_and(|previous| previous != message)
        {
            let task = self.state.working.lock().await.remove(route);
            if let Some(Some(task)) = task {
                task.abort();
            }
            if let Some(previous) = previous
                && let Err(failure) = self
                    .state
                    .client
                    .open_session(&self.state.token)
                    .assistant_threads_set_status(&SlackApiAssistantThreadsSetStatusRequest::new(
                        route.channel.clone(),
                        String::new(),
                        previous,
                    ))
                    .await
            {
                crate::bridge_eprintln!("Slack previous-message status clear error: {failure}");
            }
        }
    }

    pub async fn set_working(&self, route: &SlackRoute, working: bool) -> Result<(), ChatError> {
        self.require_route(route).await?;
        self.state.set_working(route, working).await
    }
    pub async fn track_working(&self, route: &SlackRoute, working: bool) -> Result<(), ChatError> {
        self.require_route(route).await?;
        self.state.track_working(route, working).await
    }
}

impl Drop for SlackState {
    fn drop(&mut self) {
        for task in self.working.get_mut().values().flatten() {
            task.abort();
        }
        for (_, task) in self.renames.get_mut().values() {
            task.abort();
        }
    }
}

impl SlackState {
    pub async fn set_working(&self, route: &SlackRoute, working: bool) -> Result<(), ChatError> {
        let thread = self
            .status_messages
            .lock()
            .await
            .get(&route.channel)
            .cloned()
            .ok_or_else(|| error("working status needs an owner message"))?;
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
        if !self
            .status_messages
            .lock()
            .await
            .contains_key(&route.channel)
        {
            return Ok(());
        }
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
        drop(tasks);
        self.set_working(route, working).await?;
        if working {
            let (client, token, route, events) = (
                self.client.clone(),
                self.token.clone(),
                route.clone(),
                self.events_tx.clone(),
            );
            let thread = self
                .status_messages
                .lock()
                .await
                .get(&route.channel)
                .cloned()
                .ok_or_else(|| error("working status needs an owner message"))?;
            let key = route.clone();
            self.working.lock().await.insert(
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
            self.working.lock().await.insert(route.clone(), None);
        }
        Ok(())
    }
}
