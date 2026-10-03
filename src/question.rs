use serde::Deserialize;

use crate::readers::format_detection_question;

/// One option Claude offered for a single question.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct QuestionOption {
    pub label: String,
    pub description: String,
}

/// One question from an `AskUserQuestion` tool call.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct Question {
    pub question: String,
    pub header: String,
    pub options: Vec<QuestionOption>,
    #[serde(rename = "multiSelect")]
    pub multi_select: bool,
}

/// Whether Herdr's detection snapshot of a pane still shows Claude's dialog for `question`.
#[must_use]
pub fn dialog_shows_question(detection: &str, question: &Question) -> bool {
    format_detection_question(detection).is_some_and(|dialog| dialog.contains(&question.question))
}

/// What the owner chose on a question card.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Answer {
    /// The zero-based indices of the chosen options: one for a single-select question, any number
    /// for a multiSelect question.
    Options(Vec<usize>),
    /// A free-text answer typed into the dialog's "Type something" entry.
    Text(String),
}

/// One input to the terminal dialog: key presses, or literal text typed into its free-text entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AnswerStep {
    Keys(Vec<String>),
    Text(String),
}

/// The inputs that give `answer` to Claude's own terminal dialog for `question`, the same ones the
/// owner would press. `is_last` is whether `question` is the last of its `AskUserQuestion` call.
///
/// Every dialog entry has a number key. A single-select option's number selects and confirms at
/// once. A multiSelect option's number toggles its checkbox; `right` then moves to the next tab,
/// which is the next question or, after the last question, the Submit tab, where `enter` submits.
/// The free-text entry is the one numbered after the listed options: its number focuses it, the
/// text is typed, and `enter` confirms. Free text is only mapped for a single-select question; how
/// a multiSelect dialog takes it has not been probed.
///
/// # Errors
///
/// Returns an error for a free-text answer to a multiSelect question.
pub fn answer_steps(
    question: &Question,
    is_last: bool,
    answer: &Answer,
) -> Result<Vec<AnswerStep>, String> {
    let number = |index: usize| (index + 1).to_string();
    match answer {
        Answer::Options(indices) => {
            let mut keys: Vec<String> = indices.iter().map(|&index| number(index)).collect();
            if question.multi_select {
                keys.push("right".to_owned());
                if is_last {
                    keys.push("enter".to_owned());
                }
            }
            Ok(vec![AnswerStep::Keys(keys)])
        }
        Answer::Text(_) if question.multi_select => {
            Err("free text is not supported for a multiSelect question".to_owned())
        }
        Answer::Text(text) => Ok(vec![
            AnswerStep::Keys(vec![number(question.options.len())]),
            AnswerStep::Text(text.clone()),
            AnswerStep::Keys(vec!["enter".to_owned()]),
        ]),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Answer, AnswerStep, Question, QuestionOption, answer_steps, dialog_shows_question,
    };

    fn question(multi_select: bool) -> Question {
        Question {
            question: "Which?".to_owned(),
            header: "Pick".to_owned(),
            options: ["Red", "Blue", "Green"]
                .map(|label| QuestionOption {
                    label: label.to_owned(),
                    description: String::new(),
                })
                .to_vec(),
            multi_select,
        }
    }

    fn keys(keys: &[&str]) -> AnswerStep {
        AnswerStep::Keys(keys.iter().map(|key| (*key).to_owned()).collect())
    }

    #[test]
    fn a_single_select_choice_is_its_number_key() {
        for is_last in [false, true] {
            assert_eq!(
                answer_steps(&question(false), is_last, &Answer::Options(vec![1])),
                Ok(vec![keys(&["2"])]),
                "is_last={is_last}"
            );
        }
    }

    #[test]
    fn the_last_multi_select_question_toggles_each_pick_then_submits() {
        assert_eq!(
            answer_steps(&question(true), true, &Answer::Options(vec![0, 2])),
            Ok(vec![keys(&["1", "3", "right", "enter"])])
        );
    }

    #[test]
    fn an_earlier_multi_select_question_toggles_each_pick_then_moves_to_the_next_tab() {
        assert_eq!(
            answer_steps(&question(true), false, &Answer::Options(vec![0, 2])),
            Ok(vec![keys(&["1", "3", "right"])])
        );
    }

    #[test]
    fn free_text_on_a_single_select_question_focuses_the_entry_after_the_options_types_and_confirms()
     {
        assert_eq!(
            answer_steps(&question(false), true, &Answer::Text("purple".to_owned())),
            Ok(vec![
                keys(&["4"]),
                AnswerStep::Text("purple".to_owned()),
                keys(&["enter"])
            ])
        );
    }

    #[test]
    fn free_text_on_a_multi_select_question_is_refused() {
        assert!(answer_steps(&question(true), true, &Answer::Text("purple".to_owned())).is_err());
    }

    #[test]
    fn the_dialog_check_matches_the_question_text_in_the_detection_snapshot() {
        let dialog = include_str!("../tests/fixtures/claude-detection-blocked-question.txt");
        let no_dialog = include_str!("../tests/fixtures/claude-detection-no-dialog.txt");
        let mut asked = question(false);
        asked.question = "Which color do you prefer?".to_owned();
        assert!(dialog_shows_question(dialog, &asked));
        assert!(!dialog_shows_question(dialog, &question(false)));
        assert!(!dialog_shows_question(no_dialog, &asked));
    }
}
