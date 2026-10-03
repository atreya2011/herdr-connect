use serde::{Deserialize, Serialize};

/// One option Claude offered for a single question.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct QuestionOption {
    pub label: String,
    pub description: String,
}

/// One question from an `AskUserQuestion` tool call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Question {
    pub question: String,
    pub header: String,
    pub options: Vec<QuestionOption>,
    #[serde(rename = "multiSelect")]
    pub multi_select: bool,
}
