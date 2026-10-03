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
/// owner would press.
///
/// Every dialog entry has a number key. A single-select option's number selects and confirms at
/// once. A multiSelect option's number toggles its checkbox; `right` then moves to the Submit tab
/// and `enter` submits. The free-text entry is the one numbered after the listed options: its
/// number focuses it, the text is typed, and `enter` confirms.
#[must_use]
pub fn answer_steps(question: &Question, answer: &Answer) -> Vec<AnswerStep> {
    let number = |index: usize| (index + 1).to_string();
    match answer {
        Answer::Options(indices) => {
            let mut keys: Vec<String> = indices.iter().map(|&index| number(index)).collect();
            if question.multi_select {
                keys.extend(["right".to_owned(), "enter".to_owned()]);
            }
            vec![AnswerStep::Keys(keys)]
        }
        Answer::Text(text) => vec![
            AnswerStep::Keys(vec![number(question.options.len())]),
            AnswerStep::Text(text.clone()),
            AnswerStep::Keys(vec!["enter".to_owned()]),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::{Answer, AnswerStep, Question, QuestionOption, answer_steps};

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
        assert_eq!(
            answer_steps(&question(false), &Answer::Options(vec![1])),
            vec![keys(&["2"])]
        );
    }

    #[test]
    fn a_multi_select_choice_toggles_each_pick_then_submits() {
        assert_eq!(
            answer_steps(&question(true), &Answer::Options(vec![0, 2])),
            vec![keys(&["1", "3", "right", "enter"])]
        );
    }

    #[test]
    fn free_text_focuses_the_entry_after_the_options_types_and_confirms() {
        for multi_select in [false, true] {
            assert_eq!(
                answer_steps(&question(multi_select), &Answer::Text("purple".to_owned())),
                vec![
                    keys(&["4"]),
                    AnswerStep::Text("purple".to_owned()),
                    keys(&["enter"])
                ],
                "multi_select={multi_select}"
            );
        }
    }
}
