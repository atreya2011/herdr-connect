use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::Value;

use herdr_connect_rs::{list_agents, submit_owner_prompt, tab_list_result};

const LABEL_PREFIX: &str = "testrun-stall";
const PROMPT_TEXT: &str = "Reply with exactly the word acknowledged and nothing else.";
const STALL_SENTENCE: &str = "Reply with exactly the word acknowledged and nothing else. ";
const STALL_SENTENCE_REPEATS: usize = 60;

struct Case {
    label: &'static str,
    kind: &'static str,
    agent_args: &'static [&'static str],
}

const CASES: &[Case] = &[
    Case {
        label: "testrun-stall-cursor",
        kind: "cursor",
        agent_args: &[],
    },
    Case {
        label: "testrun-stall-claude",
        kind: "claude",
        agent_args: &["--model", "haiku"],
    },
];

#[test]
fn stalled_prompt_submission_recovers_and_the_turn_starts() {
    let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
        .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
    let cwd = env!("CARGO_MANIFEST_DIR");

    assert_eq!(
        remaining_testrun_tabs().expect("tab.list succeeds"),
        0,
        "named zero-leftover check"
    );

    for case in CASES {
        let created = create_tab(&workspace_id, cwd, case.label);
        let (tab_id, result) = match created {
            Ok(tab) => {
                let outcome = start_agent(&tab.pane_id, case.label, case.kind, case.agent_args)
                    .and_then(|()| exercise(case.kind, &tab.pane_id));
                (Some(tab.tab_id), outcome)
            }
            Err(error) => (None, Err(error)),
        };
        if let Some(tab_id) = &tab_id {
            close_tab(tab_id);
        }
        if let Err(error) = result {
            panic!("case {}: {error}", case.kind);
        }
    }

    let leftover = remaining_testrun_tabs().expect("tab.list succeeds for the zero-leftover check");
    assert_eq!(leftover, 0, "named zero-leftover check");
}

fn exercise(kind: &str, pane_id: &str) -> Result<(), String> {
    let text = prompt_text_for(kind);
    submit_owner_prompt(pane_id, &text)?;
    wait_until_not_idle(pane_id, Duration::from_secs(20))
}

/// A multi-kilobyte prompt reliably lands as an unsubmitted bracketed-paste block in a freshly
/// started claude pane's composer, so Herdr reports `agent_prompt_stalled`; the cursor case keeps
/// the original short prompt since it is only a regression check for the generalized path.
fn prompt_text_for(kind: &str) -> String {
    if kind == "claude" {
        STALL_SENTENCE.repeat(STALL_SENTENCE_REPEATS)
    } else {
        PROMPT_TEXT.to_owned()
    }
}

struct TestTab {
    tab_id: String,
    pane_id: String,
}

fn create_tab(workspace_id: &str, cwd: &str, label: &str) -> Result<TestTab, String> {
    let created = herdr_json(&[
        "tab",
        "create",
        "--workspace",
        workspace_id,
        "--cwd",
        cwd,
        "--label",
        label,
        "--no-focus",
    ])?;
    let tab_id = created["result"]["tab"]["tab_id"]
        .as_str()
        .ok_or("herdr tab create result missing tab_id")?
        .to_owned();
    let pane_id = created["result"]["root_pane"]["pane_id"]
        .as_str()
        .ok_or("herdr tab create result missing pane_id")?
        .to_owned();
    Ok(TestTab { tab_id, pane_id })
}

/// Starts an agent on a freshly created pane, polling against real `agent.start` rejections until
/// Herdr reports the pane as an available shell.
///
/// A freshly created pane's shell briefly forks startup-script subprocesses (oh-my-zsh compinit,
/// profile hooks, ...) before settling; `agent start` rejects with `agent_pane_busy` while that
/// window is open, and a plain pre-check of pane process state cannot close the gap because the
/// state can change again between the check and the follow-up `agent start` call. Retrying the
/// actual `agent start` call is Herdr's own authoritative answer to "is this pane available".
fn start_agent(pane_id: &str, name: &str, kind: &str, agent_args: &[&str]) -> Result<(), String> {
    let bound = Duration::from_secs(10);
    let start = Instant::now();
    loop {
        let mut args: Vec<&str> = vec![
            "agent",
            "start",
            name,
            "--kind",
            kind,
            "--pane",
            pane_id,
            "--timeout",
            "60000",
        ];
        if !agent_args.is_empty() {
            args.push("--");
            args.extend_from_slice(agent_args);
        }
        match herdr_json(&args) {
            Ok(_) => return Ok(()),
            Err(error) if is_agent_pane_busy(&error) && start.elapsed() < bound => {
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(error) => return Err(error),
        }
    }
}

fn is_agent_pane_busy(error: &str) -> bool {
    error.contains("\"code\":\"agent_pane_busy\"")
}

fn close_tab(tab_id: &str) {
    let _ = Command::new("herdr")
        .args(["tab", "close", tab_id])
        .output();
}

fn wait_until_not_idle(pane_id: &str, bound: Duration) -> Result<(), String> {
    let start = Instant::now();
    loop {
        let status = list_agents()?
            .into_iter()
            .find(|agent| agent.pane_id.as_deref() == Some(pane_id))
            .map(|agent| agent.agent_status);
        match status {
            Some(status) if status != "idle" => return Ok(()),
            _ if start.elapsed() > bound => {
                return Err(format!(
                    "pane {pane_id} did not leave idle within {bound:?}"
                ));
            }
            _ => std::thread::sleep(Duration::from_millis(250)),
        }
    }
}

fn remaining_testrun_tabs() -> Result<usize, String> {
    Ok(tab_list_result()?
        .into_iter()
        .filter(|tab| tab.label.starts_with(LABEL_PREFIX))
        .count())
}

fn herdr_json(args: &[&str]) -> Result<Value, String> {
    let output = Command::new("herdr")
        .args(args)
        .output()
        .map_err(|error| format!("herdr {args:?} spawn failed: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "herdr {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("herdr {args:?} produced non-JSON stdout: {error}"))
}
