use herdr_connect_rs::{list_agents, submit_owner_prompt, tab_list_result};
use serde_json::Value;
use std::process::Command;
use std::time::{Duration, Instant};

const CURSOR_KIND: &str = "cursor";
const LABEL: &str = "testrun-cursor-stall-followup";
const PROMPT_TEXT: &str = "Reply with exactly the word acknowledged and nothing else.";

#[test]
fn cursor_pane_stall_is_followed_by_enter_and_the_turn_starts() {
    let workspace_id = std::env::var("HERDR_WORKSPACE_ID")
        .expect("HERDR_WORKSPACE_ID is set by the real Herdr pane environment");
    let cwd = env!("CARGO_MANIFEST_DIR");

    assert_eq!(
        remaining_testrun_tabs().expect("tab.list succeeds"),
        0,
        "named zero-leftover check"
    );

    let created = create_cursor_tab(&workspace_id, cwd);
    let (tab_id, result) = match created {
        Ok(tab) => {
            let outcome = exercise(&tab.pane_id);
            (Some(tab.tab_id), outcome)
        }
        Err(error) => (None, Err(error)),
    };
    if let Some(tab_id) = &tab_id {
        close_tab(tab_id);
    }
    let leftover = remaining_testrun_tabs().expect("tab.list succeeds for the zero-leftover check");

    let status = result.expect("cursor pane leaves idle after the bridge's stall follow-up");
    assert_ne!(
        status, "idle",
        "pane must leave idle once the prompt is actually submitted"
    );
    assert_eq!(leftover, 0, "named zero-leftover check");
}

fn exercise(pane_id: &str) -> Result<String, String> {
    submit_owner_prompt(CURSOR_KIND, pane_id, PROMPT_TEXT)?;
    wait_until_not_idle(pane_id, Duration::from_secs(20))
}

struct TestTab {
    tab_id: String,
    pane_id: String,
}

fn create_cursor_tab(workspace_id: &str, cwd: &str) -> Result<TestTab, String> {
    let created = herdr_json(&[
        "tab",
        "create",
        "--workspace",
        workspace_id,
        "--cwd",
        cwd,
        "--label",
        LABEL,
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
    herdr_json(&[
        "agent",
        "start",
        LABEL,
        "--kind",
        CURSOR_KIND,
        "--pane",
        &pane_id,
        "--timeout",
        "60000",
    ])?;
    Ok(TestTab { tab_id, pane_id })
}

fn close_tab(tab_id: &str) {
    let _ = Command::new("herdr")
        .args(["tab", "close", tab_id])
        .output();
}

fn wait_until_not_idle(pane_id: &str, bound: Duration) -> Result<String, String> {
    let start = Instant::now();
    loop {
        let status = list_agents()?
            .into_iter()
            .find(|agent| agent.pane_id.as_deref() == Some(pane_id))
            .map(|agent| agent.agent_status);
        match status {
            Some(status) if status != "idle" => return Ok(status),
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
        .filter(|tab| tab.label == LABEL)
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
