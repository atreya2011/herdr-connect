use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// The Claude hook event a question request is decoded from.
const ASK_QUESTION_EVENT: &str = "PreToolUse";
/// The Claude tool name that identifies a question request on the shared `PreToolUse` matcher.
pub const ASK_QUESTION_TOOL: &str = "AskUserQuestion";

/// Discriminates a question frame from a permission [`Interaction`](crate::permission::Interaction).
///
/// Used the same way [`ACTIVITY_KIND`](crate::activity::ACTIVITY_KIND) discriminates an activity
/// frame on the shared broker socket.
pub const QUESTION_KIND: &str = "question";

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
    #[serde(rename = "multiSelect", default)]
    pub multi_select: bool,
}

/// One resolved answer: a single label for a single-select question, or several for multiSelect.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum QuestionAnswer {
    Single(String),
    Multiple(Vec<String>),
}

/// One decoded `AskUserQuestion` `PreToolUse` hook payload, ready to forward to the broker.
///
/// `raw_tool_input` is the original `tool_input` object, kept so the hook can splice the broker's
/// resolved `answers` into it verbatim (preserving fields this crate does not model) when it
/// encodes the final `updatedInput` response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuestionInteraction {
    pub session_id: String,
    pub request_id: String,
    pub questions: Vec<Question>,
    pub raw_tool_input: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct AskUserQuestionToolInput {
    questions: Vec<Question>,
}

#[derive(Debug, Deserialize)]
struct AskUserQuestionRequest {
    session_id: String,
    hook_event_name: String,
    tool_name: String,
    tool_input: serde_json::Value,
    tool_use_id: String,
}

/// Decodes one Claude `AskUserQuestion` `PreToolUse` hook payload into a [`QuestionInteraction`].
///
/// `tool_use_id` becomes `request_id`: unlike a permission request, an `AskUserQuestion` call has
/// no `prompt_id` correlate a decision needs, and `tool_use_id` is already the vendor-supplied,
/// per-call identifier.
///
/// # Errors
///
/// Returns an error when the payload is not JSON, does not name the `AskUserQuestion` tool on a
/// `PreToolUse` event, or its `tool_input.questions` does not parse.
pub fn decode_claude_ask_question(input: &[u8]) -> Result<QuestionInteraction, String> {
    let request: AskUserQuestionRequest =
        serde_json::from_slice(input).map_err(|error| error.to_string())?;
    if request.hook_event_name != ASK_QUESTION_EVENT {
        return Err("unexpected Claude hook event".to_owned());
    }
    if request.tool_name != ASK_QUESTION_TOOL {
        return Err("unexpected Claude tool name".to_owned());
    }
    let parsed: AskUserQuestionToolInput = serde_json::from_value(request.tool_input.clone())
        .map_err(|error| format!("AskUserQuestion tool_input does not parse: {error}"))?;
    if parsed.questions.is_empty()
        || parsed
            .questions
            .iter()
            .any(|question| question.options.is_empty())
    {
        return Err("AskUserQuestion request has no usable questions".to_owned());
    }
    Ok(QuestionInteraction {
        session_id: request.session_id,
        request_id: request.tool_use_id,
        questions: parsed.questions,
        raw_tool_input: request.tool_input,
    })
}

/// Encodes the broker's resolved answers as Claude's `PreToolUse` `updatedInput` hook response,
/// splicing `answers` into the original `tool_input` so every other field it carried round-trips
/// unchanged.
///
/// # Errors
///
/// Returns an error if `raw_tool_input` is not a JSON object or the response cannot be serialized.
pub fn encode_claude_question_decision(
    raw_tool_input: &serde_json::Value,
    answers: &BTreeMap<String, QuestionAnswer>,
) -> Result<Vec<u8>, String> {
    let mut updated_input = raw_tool_input
        .as_object()
        .cloned()
        .ok_or_else(|| "AskUserQuestion tool_input is not a JSON object".to_owned())?;
    let answers = serde_json::to_value(answers).map_err(|error| error.to_string())?;
    updated_input.insert("answers".to_owned(), answers);
    serde_json::to_vec(&serde_json::json!({
        "hookSpecificOutput": {
            "hookEventName": ASK_QUESTION_EVENT,
            "permissionDecision": "allow",
            "updatedInput": updated_input,
        },
    }))
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{
        ASK_QUESTION_TOOL, Question, QuestionAnswer, QuestionOption, decode_claude_ask_question,
        encode_claude_question_decision,
    };
    use std::collections::BTreeMap;

    const SINGLE_SELECT_FIXTURE: &str =
        include_str!("../tests/fixtures/claude-ask-question/single-select.json");
    const MULTI_SELECT_FIXTURE: &str =
        include_str!("../tests/fixtures/claude-ask-question/multi-select.json");

    #[test]
    fn decodes_single_and_multi_select_fixtures() {
        let cases = [
            (
                "single-select",
                SINGLE_SELECT_FIXTURE,
                vec![Question {
                    question: "Which color?".to_owned(),
                    header: "Color".to_owned(),
                    options: vec![
                        QuestionOption {
                            label: "Red".to_owned(),
                            description: "The color red".to_owned(),
                        },
                        QuestionOption {
                            label: "Blue".to_owned(),
                            description: "The color blue".to_owned(),
                        },
                    ],
                    multi_select: false,
                }],
            ),
            (
                "multi-select",
                MULTI_SELECT_FIXTURE,
                vec![Question {
                    question: "Which toppings?".to_owned(),
                    header: "Toppings".to_owned(),
                    options: vec![
                        QuestionOption {
                            label: "Cheese".to_owned(),
                            description: "Add cheese".to_owned(),
                        },
                        QuestionOption {
                            label: "Olives".to_owned(),
                            description: "Add olives".to_owned(),
                        },
                        QuestionOption {
                            label: "Mushrooms".to_owned(),
                            description: "Add mushrooms".to_owned(),
                        },
                    ],
                    multi_select: true,
                }],
            ),
        ];
        for (name, payload, expected_questions) in cases {
            let interaction = decode_claude_ask_question(payload.as_bytes())
                .unwrap_or_else(|error| panic!("{name} fixture decodes: {error}"));
            assert_eq!(interaction.questions, expected_questions, "case={name}");
            assert!(!interaction.session_id.is_empty(), "case={name}");
            assert!(!interaction.request_id.is_empty(), "case={name}");
        }
    }

    #[test]
    fn rejects_a_different_hook_event_or_tool() {
        let cases = [
            r#"{"session_id":"s","hook_event_name":"PermissionRequest","tool_name":"AskUserQuestion","tool_input":{"questions":[]},"tool_use_id":"t"}"#,
            r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"Bash","tool_input":{"questions":[]},"tool_use_id":"t"}"#,
        ];
        for payload in cases {
            assert!(decode_claude_ask_question(payload.as_bytes()).is_err());
        }
    }

    #[test]
    fn rejects_a_request_with_no_usable_questions() {
        let cases = [
            r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"AskUserQuestion","tool_input":{"questions":[]},"tool_use_id":"t"}"#,
            r#"{"session_id":"s","hook_event_name":"PreToolUse","tool_name":"AskUserQuestion","tool_input":{"questions":[{"question":"q","header":"h","options":[],"multiSelect":false}]},"tool_use_id":"t"}"#,
        ];
        for payload in cases {
            assert!(decode_claude_ask_question(payload.as_bytes()).is_err());
        }
    }

    #[test]
    fn rejects_malformed_json() {
        assert!(decode_claude_ask_question(b"{ malformed").is_err());
    }

    #[test]
    fn encodes_single_and_multi_select_answers_into_the_original_tool_input() {
        let raw_tool_input = serde_json::json!({
            "questions": [{"question": "Which color?", "header": "Color", "options": [], "multiSelect": false}],
        });
        let cases = [
            (
                "single",
                BTreeMap::from([(
                    "Which color?".to_owned(),
                    QuestionAnswer::Single("Blue".to_owned()),
                )]),
                serde_json::json!("Blue"),
            ),
            (
                "multiple",
                BTreeMap::from([(
                    "Which toppings?".to_owned(),
                    QuestionAnswer::Multiple(vec!["Cheese".to_owned(), "Olives".to_owned()]),
                )]),
                serde_json::json!(["Cheese", "Olives"]),
            ),
        ];
        for (name, answers, expected_value) in cases {
            let encoded =
                encode_claude_question_decision(&raw_tool_input, &answers).expect("encodes");
            let value: serde_json::Value =
                serde_json::from_slice(&encoded).expect("encoded decision is JSON");
            assert_eq!(
                value["hookSpecificOutput"]["hookEventName"], "PreToolUse",
                "case={name}"
            );
            assert_eq!(
                value["hookSpecificOutput"]["permissionDecision"], "allow",
                "case={name}"
            );
            assert_eq!(
                value["hookSpecificOutput"]["updatedInput"]["questions"],
                raw_tool_input["questions"],
                "case={name}: original tool_input fields round-trip"
            );
            let (key, _) = answers.iter().next().expect("one answer");
            assert_eq!(
                value["hookSpecificOutput"]["updatedInput"]["answers"][key], expected_value,
                "case={name}"
            );
        }
    }

    #[test]
    fn encode_rejects_a_non_object_tool_input() {
        assert!(
            encode_claude_question_decision(&serde_json::json!([1, 2]), &BTreeMap::new()).is_err()
        );
    }

    #[test]
    fn ask_question_tool_name_constant_matches_the_vendor_string() {
        assert_eq!(ASK_QUESTION_TOOL, "AskUserQuestion");
    }
}
