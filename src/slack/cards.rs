use serde_json::{Value, json};
use slack_morphism::prelude::*;

use super::{Slack, SlackRoute, delivery::escape, error};
use crate::{ChatError, PermissionVendor, Question};

fn content(text: String, blocks: Value) -> Result<SlackMessageContent, ChatError> {
    Ok(SlackMessageContent::new()
        .with_text(text)
        .with_blocks(serde_json::from_value(blocks).map_err(error)?))
}

fn question_content(
    question: &Question,
    token: &str,
    owner: &SlackUserId,
) -> Result<SlackMessageContent, ChatError> {
    let mut blocks = vec![
        json!({"type":"section","text":{"type":"mrkdwn","text":format!("<@{owner}> Claude question")}}),
        json!({"type":"section","text":{"type":"plain_text","text":question.question.chars().take(3000).collect::<String>()}}),
    ];
    if question.multi_select {
        let options: Vec<_> = question.options.iter().enumerate().map(|(index, option)| json!({"text":{"type":"plain_text","text":option.label.chars().take(75).collect::<String>()},"value":index.to_string()})).collect();
        blocks.push(json!({"type":"section","text":{"type":"plain_text","text":"Choose options, then submit."},"accessory":{"type":"multi_static_select","action_id":format!("herdrask-picks:{token}"),"placeholder":{"type":"plain_text","text":"Choose options"},"options":options}}));
        blocks.push(json!({"type":"actions","elements":[{"type":"button","action_id":format!("herdrask-multi:{token}"),"text":{"type":"plain_text","text":"Submit choices"}}]}));
    } else {
        let buttons: Vec<_> = question.options.iter().enumerate().map(|(index, option)| json!({"type":"button","action_id":format!("herdrask:{token}:{index}"),"text":{"type":"plain_text","text":option.label.chars().take(75).collect::<String>()}})).collect();
        blocks.push(json!({"type":"actions","elements":buttons}));
    }
    content(
        format!("<@{owner}> {}", escape(&question.question)),
        json!(blocks),
    )
}

impl Slack {
    pub async fn permission_card(
        &self,
        route: &SlackRoute,
        vendor: PermissionVendor,
        tool: &str,
        command: &str,
        token: &str,
    ) -> Result<SlackTs, ChatError> {
        let title = format!("<@{}> {vendor:?} permission: {}", self.owner, escape(tool));
        let blocks = json!([
            {"type":"section","text":{"type":"mrkdwn","text":title}},
            {"type":"section","text":{"type":"plain_text","text":command.chars().take(3000).collect::<String>()}},
            {"type":"actions","elements":[
                {"type":"button","action_id":format!("herdr:allow:{token}"),"text":{"type":"plain_text","text":"Allow"},"style":"primary"},
                {"type":"button","action_id":format!("herdr:deny:{token}"),"text":{"type":"plain_text","text":"Deny"},"style":"danger"}
            ]}
        ]);
        self.send_content(
            route,
            content(format!("{title}\n{}", escape(command)), blocks)?,
        )
        .await
    }

    pub async fn question_card(
        &self,
        route: &SlackRoute,
        question: &Question,
        token: &str,
    ) -> Result<SlackTs, ChatError> {
        self.send_content(route, question_content(question, token, &self.owner)?)
            .await
    }

    pub async fn update_question(
        &self,
        route: &SlackRoute,
        message: &SlackTs,
        question: &Question,
        token: &str,
    ) -> Result<(), ChatError> {
        self.update_content(
            route,
            message,
            question_content(question, token, &self.owner)?,
        )
        .await
    }
}
