use crate::watcher::Transition;
use std::time::Duration;

const MAX_PART_LENGTH: usize = 1_900;
const MAX_THREAD_NAME_LENGTH: usize = 100;
const MAX_UNSUPPORTED_BLOCKED_DESCRIPTION_LENGTH: usize = 3_800;
const MAX_DISCORD_CONTENT_LENGTH: usize = 2_000;

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
pub fn create_unsupported_blocked_card(
    agent_kind: &str,
    pane_id: &str,
    context: &str,
    owner: &str,
    blocked_age: Duration,
) -> TransitionMessage {
    let mention = format!("<@{owner}>");
    TransitionMessage {
        description: unsupported_blocked_description(
            agent_kind,
            pane_id,
            context,
            blocked_age,
            mention.chars().count(),
        ),
        color: 0x00fe_e75c,
        mention: Some(mention),
    }
}

fn unsupported_blocked_description(
    agent_kind: &str,
    pane_id: &str,
    context: &str,
    blocked_age: Duration,
    mention_length: usize,
) -> String {
    let agent_kind = bounded_inline(agent_kind, 128, true);
    let pane_id = bounded_inline(pane_id, 128, true);
    let prefix = format!(
        "Unsupported blocked pane\nVendor/agent: `{agent_kind}`\nPane: `{pane_id}`\nBlocked for: {}\nContext:\n```\n",
        format_blocked_age(blocked_age)
    );
    let suffix = "\n```\nAction: OPEN/FOCUS the pane in Herdr.";
    let description_limit = MAX_UNSUPPORTED_BLOCKED_DESCRIPTION_LENGTH
        .min(MAX_DISCORD_CONTENT_LENGTH.saturating_sub(mention_length.saturating_add(1)));
    let context_limit =
        description_limit.saturating_sub(prefix.chars().count() + suffix.chars().count());
    let context = bounded_inline(context, context_limit, false);
    format!("{prefix}{context}{suffix}")
}

fn bounded_inline(value: &str, limit: usize, strip_control_characters: bool) -> String {
    let sanitized = value
        .chars()
        .filter(|character| !strip_control_characters || !character.is_control())
        .map(|character| if character == '`' { 'ˋ' } else { character })
        .collect::<String>();
    if sanitized.chars().count() <= limit {
        return sanitized;
    }
    let mut bounded = sanitized
        .chars()
        .take(limit.saturating_sub(1))
        .collect::<String>();
    bounded.push('…');
    bounded
}

fn format_blocked_age(age: Duration) -> String {
    let seconds = age.as_secs();
    let minutes = seconds / 60;
    let seconds = seconds % 60;
    if minutes == 0 {
        format!("{seconds}s")
    } else {
        format!("{minutes}m {seconds}s")
    }
}

#[must_use]
pub fn create_transition_messages(
    transition: &Transition,
    capture: &AgentLogCapture,
    owner: &str,
) -> Vec<TransitionMessage> {
    let mut body = if transition.to == "blocked" {
        capture
            .question
            .as_deref()
            .unwrap_or(&capture.message)
            .to_owned()
    } else {
        capture.message.clone()
    };
    if let Some(failure) = &capture.failure {
        body.push_str("\n\n");
        body.push_str(failure);
    }
    let color = if capture.failure.is_some() {
        0x00ed_4245
    } else if transition.to == "blocked" {
        0x00fe_e75c
    } else {
        0x0057_f287
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
            mention: (transition.to == "blocked" && i == 0).then(|| format!("<@{owner}>")),
        })
        .collect()
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

/// Formats a bounded Discord thread name.
///
/// # Errors
///
/// Returns an error when the label has no usable base or the suffix cannot fit.
pub fn format_thread_name(label: &str, title: &str, tab_id: &str) -> Result<String, String> {
    let label = label.trim();
    let title = title.trim();
    let base = if !label.is_empty() && !label.chars().all(|c| c.is_ascii_digit()) {
        label
    } else if !label.is_empty() && label.chars().all(|c| c.is_ascii_digit()) && !title.is_empty() {
        title
    } else {
        return Err(format!(
            "herdr tab {tab_id} has numeric label {label} without a terminal title"
        ));
    };
    let suffix = format!(" [{tab_id}]");
    if suffix.chars().count() > MAX_THREAD_NAME_LENGTH {
        return Err(format!(
            "herdr tab id {tab_id} is too long for a Discord thread"
        ));
    }
    let capacity = MAX_THREAD_NAME_LENGTH - suffix.chars().count();
    if capacity == 0 {
        return Err(format!(
            "herdr tab id {tab_id} is too long for a Discord thread"
        ));
    }
    Ok(format!(
        "{}{}",
        base.chars().take(capacity).collect::<String>(),
        suffix
    ))
}

#[cfg(test)]
mod tests {
    use super::{MAX_UNSUPPORTED_BLOCKED_DESCRIPTION_LENGTH, create_unsupported_blocked_card};
    use std::time::Duration;

    #[test]
    fn unsupported_blocked_card_is_informational_and_bounded() {
        let cases = vec![
            (
                "cursor",
                "pane-1",
                "login prompt with ``` unsafe context".to_owned(),
                Duration::from_secs(3),
                "3s",
            ),
            (
                "claude",
                "pane-2",
                "x".repeat(4_000),
                Duration::from_secs(123),
                "2m 3s",
            ),
        ];
        for (agent, pane, context, age, expected_age) in cases {
            let card = create_unsupported_blocked_card(agent, pane, &context, "42", age);
            assert!(card.description.chars().count() <= MAX_UNSUPPORTED_BLOCKED_DESCRIPTION_LENGTH);
            assert!(
                card.description
                    .contains(&format!("Vendor/agent: `{agent}`"))
            );
            assert!(card.description.contains(&format!("Pane: `{pane}`")));
            assert!(
                card.description
                    .contains(&format!("Blocked for: {expected_age}"))
            );
            assert!(
                card.description
                    .contains("Action: OPEN/FOCUS the pane in Herdr.")
            );
            assert!(!card.description.contains("Allow"));
            assert!(!card.description.contains("Deny"));
            assert_eq!(card.description.matches("```").count(), 2);
            assert_eq!(card.mention.as_deref(), Some("<@42>"));
        }
    }

    #[test]
    fn unsupported_blocked_card_content_fits_discord_limit() {
        let card = create_unsupported_blocked_card(
            "cursor",
            "pane-1",
            &"x".repeat(4_000),
            "42",
            Duration::from_secs(3),
        );
        let content = format!("{} {}", card.mention.as_deref().unwrap(), card.description);

        assert!(content.chars().count() <= 2_000);
    }

    #[test]
    fn unsupported_blocked_card_inline_identity_stays_single_line() {
        let card = create_unsupported_blocked_card(
            "cursor\n**Allow**",
            "pane\n**Allow**",
            "first\nsecond",
            "42",
            Duration::from_secs(3),
        );

        assert!(!card.description.contains("\n**Allow**"));
        assert!(card.description.contains("first\nsecond"));
        assert_eq!(card.description.matches("```").count(), 2);
    }
}
