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

/// Whether Herdr's detection snapshot still shows Claude's dialog for `question`.
///
/// The screen wraps the question at the pane width and prefixes lines with a box character, so
/// those prefixes are removed before whitespace is collapsed.
#[must_use]
pub fn dialog_shows_question(detection: &str, question: &Question) -> bool {
    let normalize_dialog = |text: &str| {
        text.lines()
            .map(|line| {
                line.strip_prefix("│ ")
                    .or_else(|| line.strip_prefix('│'))
                    .unwrap_or(line)
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let collapse = |text: &str| text.split_whitespace().collect::<Vec<_>>().join(" ");
    format_detection_question(detection).is_some_and(|dialog| {
        collapse(&normalize_dialog(&dialog)).contains(&collapse(&question.question))
    })
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
/// owner would press. `question` is number `index` of the `total` questions of its
/// `AskUserQuestion` call.
///
/// Every dialog entry has a number key. In a one-question call a single-select option's number
/// confirms at once, and a multiSelect question takes its numbers (each toggles a checkbox), then
/// `right` to the Submit tab and `enter`. In a call with several questions a single-select number
/// answers the question and advances to the next tab by itself, a multiSelect question needs
/// `right` to advance, and after the last question the dialog shows a review screen whose
/// "Submit answers" `enter` confirms. The free-text entry is the one numbered after the listed
/// options: its number focuses it, the text is typed, and `enter` confirms. Free text is only
/// mapped for a single-select question; how a multiSelect dialog takes it has not been probed.
///
/// # Errors
///
/// Returns an error for a free-text answer to a multiSelect question.
pub fn answer_steps(
    question: &Question,
    index: usize,
    total: usize,
    answer: &Answer,
) -> Result<Vec<AnswerStep>, String> {
    let number = |index: usize| (index + 1).to_string();
    let review_follows = total > 1 && index + 1 == total;
    match answer {
        Answer::Options(indices) => {
            let mut keys: Vec<String> = indices.iter().map(|&index| number(index)).collect();
            if question.multi_select {
                keys.push("right".to_owned());
                if index + 1 == total {
                    keys.push("enter".to_owned());
                }
            } else if review_follows {
                keys.push("enter".to_owned());
            }
            Ok(vec![AnswerStep::Keys(keys)])
        }
        Answer::Text(_) if question.multi_select => {
            Err("free text is not supported for a multiSelect question".to_owned())
        }
        Answer::Text(text) => {
            let mut confirm = vec!["enter".to_owned()];
            if review_follows {
                confirm.push("enter".to_owned());
            }
            Ok(vec![
                AnswerStep::Keys(vec![number(question.options.len())]),
                AnswerStep::Text(text.clone()),
                AnswerStep::Keys(confirm),
            ])
        }
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
    fn a_single_question_single_select_choice_is_its_number_key() {
        assert_eq!(
            answer_steps(&question(false), 0, 1, &Answer::Options(vec![1])),
            Ok(vec![keys(&["2"])])
        );
    }

    #[test]
    fn a_single_question_multi_select_choice_toggles_then_moves_to_submit_and_submits() {
        assert_eq!(
            answer_steps(&question(true), 0, 1, &Answer::Options(vec![0, 2])),
            Ok(vec![keys(&["1", "3", "right", "enter"])])
        );
    }

    #[test]
    fn an_earlier_single_select_question_of_a_call_is_only_its_number_key() {
        assert_eq!(
            answer_steps(&question(false), 0, 2, &Answer::Options(vec![1])),
            Ok(vec![keys(&["2"])])
        );
    }

    #[test]
    fn the_last_single_select_question_of_a_call_adds_enter_on_the_review_screen() {
        assert_eq!(
            answer_steps(&question(false), 1, 2, &Answer::Options(vec![1])),
            Ok(vec![keys(&["2", "enter"])])
        );
    }

    #[test]
    fn an_earlier_multi_select_question_of_a_call_moves_to_the_next_tab_without_enter() {
        assert_eq!(
            answer_steps(&question(true), 0, 2, &Answer::Options(vec![0, 2])),
            Ok(vec![keys(&["1", "3", "right"])])
        );
    }

    #[test]
    fn the_last_multi_select_question_of_a_call_moves_to_review_and_submits() {
        assert_eq!(
            answer_steps(&question(true), 1, 2, &Answer::Options(vec![0, 2])),
            Ok(vec![keys(&["1", "3", "right", "enter"])])
        );
    }

    #[test]
    fn free_text_focuses_the_entry_after_the_options_types_and_confirms() {
        assert_eq!(
            answer_steps(&question(false), 0, 1, &Answer::Text("purple".to_owned())),
            Ok(vec![
                keys(&["4"]),
                AnswerStep::Text("purple".to_owned()),
                keys(&["enter"])
            ])
        );
        assert_eq!(
            answer_steps(&question(false), 0, 2, &Answer::Text("purple".to_owned())),
            Ok(vec![
                keys(&["4"]),
                AnswerStep::Text("purple".to_owned()),
                keys(&["enter"])
            ])
        );
    }

    #[test]
    fn free_text_on_the_last_question_of_a_call_adds_enter_on_the_review_screen() {
        assert_eq!(
            answer_steps(&question(false), 1, 2, &Answer::Text("purple".to_owned())),
            Ok(vec![
                keys(&["4"]),
                AnswerStep::Text("purple".to_owned()),
                keys(&["enter", "enter"])
            ])
        );
    }

    #[test]
    fn free_text_on_a_multi_select_question_is_refused() {
        assert!(answer_steps(&question(true), 0, 1, &Answer::Text("purple".to_owned())).is_err());
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

    #[test]
    fn the_dialog_check_matches_a_question_wrapped_with_box_prefixes() {
        let dialog = concat!(
            "────\n",
            " ☐ Gauntlet mode\n",
            "\n",
            "│ Your message is only a pasted kickoff for gauntlet run gk1004b on issue #110 (it reads as if another agent wrote it). It asks for discovery, sizing and a dispatch\n",
            "│ plan before any code is written, so which mode should I run it in?\n",
            "\n",
            "❯ 1. Run it as written\n",
            "  2. Discovery only\n",
            "────\n",
        );
        let mut asked = question(false);
        asked.question = "Your message is only a pasted kickoff for gauntlet run gk1004b on issue #110 (it reads as if another agent wrote it). It asks for discovery, sizing and a dispatch plan before any code is written, so which mode should I run it in?".to_owned();
        assert!(dialog_shows_question(dialog, &asked));
    }
}
