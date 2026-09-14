use crate::herdr::STATUS_BLOCKED;
use crate::watcher::Transition;

const MAX_PART_LENGTH: usize = 1_900;
const MAX_THREAD_NAME_LENGTH: usize = 100;

#[derive(Debug, PartialEq, Eq)]
pub struct TransitionMessage {
    pub description: String,
    pub color: u32,
    pub mention: Option<String>,
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct AgentLogCapture {
    pub message: String,
    pub failure: Option<String>,
    pub question: Option<String>,
}

#[must_use]
pub fn create_transition_messages(
    transition: &Transition,
    capture: &AgentLogCapture,
    owner: &str,
) -> Vec<TransitionMessage> {
    let mut body = capture
        .question
        .as_deref()
        .unwrap_or(&capture.message)
        .to_owned();
    if let Some(failure) = &capture.failure {
        if !body.is_empty() {
            body.push_str("\n\n");
        }
        body.push_str(failure);
    }
    let color = if capture.failure.is_some() {
        0x00ed_4245
    } else {
        0x00fe_e75c
    };
    let mut parts = split_body(&body);
    let total = parts.len();
    if total > 1 {
        for (i, part) in parts.iter_mut().enumerate() {
            *part = format!("{}/{}\n{}", i + 1, total, part);
        }
    }
    parts
        .into_iter()
        .enumerate()
        .map(|(i, description)| TransitionMessage {
            description,
            color,
            mention: (transition.to == STATUS_BLOCKED && i == 0).then(|| format!("<@{owner}>")),
        })
        .collect()
}
/// Splits a live-captured assistant text at the same boundary transition cards use, with no part
/// numbering: each part is posted as its own plain message.
#[must_use]
pub fn split_live_message(text: &str) -> Vec<String> {
    split_body(text)
}

fn split_body(body: &str) -> Vec<String> {
    const PART_BUDGET: usize = MAX_PART_LENGTH - 10;
    let mut atoms = Vec::new();
    let mut index = 0;
    let lines: Vec<&str> = body.split('\n').collect();
    while index < lines.len() {
        if lines[index].starts_with("```") {
            let open = lines[index];
            let mut end = index + 1;
            while end < lines.len() && !lines[end].starts_with("```") {
                end += 1;
            }
            let inner = lines[index + 1..end].join("\n");
            let limit = PART_BUDGET.saturating_sub(open.len() + 7);
            if open.len() + 7 >= PART_BUDGET {
                atoms.extend(chars_chunks(
                    &body_without_fences(open, &inner, end < lines.len()),
                    PART_BUDGET,
                ));
            } else if end >= lines.len() || format!("{open}\n{inner}\n```").len() > PART_BUDGET {
                atoms.extend(
                    chars_chunks(&inner, limit.max(1))
                        .into_iter()
                        .map(|part| format!("{open}\n{part}\n```")),
                );
            } else {
                atoms.push(format!("{open}\n{inner}\n```"));
            }
            index = if end < lines.len() {
                end + 1
            } else {
                lines.len()
            };
        } else {
            atoms.push(lines[index].to_owned());
            index += 1;
        }
    }
    let mut out = Vec::new();
    let mut current = String::new();
    for atom in atoms {
        let next = if current.is_empty() {
            atom.clone()
        } else {
            format!("{current}\n{atom}")
        };
        if next.len() <= PART_BUDGET {
            current = next;
        } else {
            if !current.is_empty() {
                out.push(current);
            }
            if atom.len() <= PART_BUDGET {
                current = atom;
            } else {
                out.extend(chars_chunks(&atom, PART_BUDGET));
                current = String::new();
            }
        }
    }
    if !current.is_empty() {
        out.push(current);
    }
    if out.is_empty() {
        out.push(String::new());
    }
    out
}

fn body_without_fences(open: &str, inner: &str, terminated: bool) -> String {
    let close = if terminated { "\n```" } else { "" };
    format!("{open}\n{inner}{close}").replace("```", "")
}
fn chars_chunks(text: &str, limit: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    for c in text.chars() {
        if current.len() + c.len_utf8() > limit && !current.is_empty() {
            out.push(current);
            current = String::new();
        }
        current.push(c);
    }
    if !current.is_empty() {
        out.push(current);
    }
    out
}

/// Why [`format_thread_name`] could not produce a name.
#[derive(Debug, PartialEq, Eq)]
pub enum ThreadNameError {
    /// A numeric tab label has no terminal title yet. Not a failure: a cold-start tab is titled
    /// by Herdr shortly after the pane starts, so the caller waits for a later snapshot.
    TitlePending,
    /// The name can never be produced from this tab's identity (empty label, or a tab id too
    /// long for a Discord thread name).
    Unusable(String),
}

/// Formats a bounded Discord thread name.
///
/// # Errors
///
/// Returns [`ThreadNameError::TitlePending`] when a numeric label has no terminal title yet, or
/// [`ThreadNameError::Unusable`] when the label is empty or the suffix cannot fit.
pub fn format_thread_name(
    label: &str,
    title: &str,
    tab_id: &str,
) -> Result<String, ThreadNameError> {
    let label = label.trim();
    let title = title.trim();
    let numeric_label = !label.is_empty() && label.chars().all(|c| c.is_ascii_digit());
    let base = if !label.is_empty() && !numeric_label {
        label
    } else if numeric_label && !title.is_empty() {
        title
    } else if numeric_label {
        return Err(ThreadNameError::TitlePending);
    } else {
        return Err(ThreadNameError::Unusable(format!(
            "herdr tab {tab_id} has no label"
        )));
    };
    let suffix = format!(" [{tab_id}]");
    if suffix.chars().count() > MAX_THREAD_NAME_LENGTH {
        return Err(ThreadNameError::Unusable(format!(
            "herdr tab id {tab_id} is too long for a Discord thread"
        )));
    }
    let capacity = MAX_THREAD_NAME_LENGTH - suffix.chars().count();
    if capacity == 0 {
        return Err(ThreadNameError::Unusable(format!(
            "herdr tab id {tab_id} is too long for a Discord thread"
        )));
    }
    Ok(format!(
        "{}{}",
        base.chars().take(capacity).collect::<String>(),
        suffix
    ))
}
