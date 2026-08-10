//! Red, table-driven parity contracts. Implementation is intentionally absent in this phase.

use std::os::unix::net::UnixListener;

fn red_cases(module: &str, cases: &[&str]) {
    let failures: Vec<_> = cases
        .iter()
        .map(|case| format!("{module}: {case}"))
        .collect();
    assert!(
        failures.is_empty(),
        "unimplemented parity behaviors: {failures:?}"
    );
}

#[test]
fn herdr_socket_client() {
    let path = std::env::temp_dir().join(format!("herdr-connect-red-{}", std::process::id()));
    let _server = UnixListener::bind(&path).expect("real Unix socket server must start");
    red_cases(
        "herdr socket client",
        &[
            "connect and exchange newline-delimited JSON-RPC",
            "agent.list",
            "tab.list",
            "response IDs and RPC errors",
            "agent identity fields",
        ],
    );
    let _ = std::fs::remove_file(path);
}

#[test]
fn transition_watcher() {
    red_cases(
        "transition watcher",
        &[
            "immediate polling",
            "first-snapshot priming",
            "changed existing agents",
            "disappeared terminal eviction",
            "restart generation safety",
            "error surface",
        ],
    );
}

#[test]
fn discovery_loop() {
    red_cases(
        "discovery loop",
        &[
            "workspace and tab synchronization",
            "numeric-label title substitution",
            "frozen tab names",
            "thread-name length rejection",
            "serialized synchronization",
            "stop scheduling",
            "failure continuation",
        ],
    );
}

#[test]
fn topology_mapping() {
    red_cases(
        "topology mapping",
        &[
            "workspace topic mapping",
            "workspace reuse and creation",
            "tab suffix mapping",
            "active and paginated archived search",
            "thread creation and owner membership",
            "mapping cache",
        ],
    );
}

#[test]
fn transition_cards_and_embeds() {
    red_cases(
        "transition cards and embeds",
        &[
            "postable transitions",
            "final session-log capture",
            "blocked question selection",
            "bounded status embeds",
            "changed-file summaries and diffs",
            "safe multipart splitting",
            "blocked mention exactly once",
            "silent non-blocked posts",
        ],
    );
}

#[test]
fn delivery_queue() {
    red_cases(
        "delivery queue",
        &[
            "observation-time capture",
            "ordered nonce delivery",
            "same-tab sibling suppression",
            "five-attempt eviction",
            "missing-tab eviction",
            "bounded queue",
            "primary failure preservation",
        ],
    );
}

#[test]
fn live_status() {
    red_cases(
        "live status",
        &[
            "console transition formatting",
            "Discord working status message",
            "coalesced edits",
            "durable restart store",
            "final-card and stop clearing",
            "edit and delete failure budgets",
            "startup URL-marker stray sweep",
            "SIGINT and SIGTERM stop",
        ],
    );
}

#[test]
fn live_activity_watch() {
    red_cases(
        "live activity/watch",
        &[
            "complete JSONL boundary and rotation",
            "Cursor row IDs",
            "incremental feed merge",
            "current tool and task counts",
            "session identity reset",
            "non-working removal and advancement",
        ],
    );
}

#[test]
fn vendor_log_readers() {
    for fixture in [
        "agent-log-claude.jsonl",
        "agent-log-claude-answered.jsonl",
        "agent-log-claude-plan-files.jsonl",
        "agent-log-codex.jsonl",
        "agent-log-codex-147.jsonl",
        "agent-log-codex-147-final-stop.jsonl",
        "agent-log-cursor.json",
    ] {
        assert!(
            std::path::Path::new("tests/fixtures")
                .join(fixture)
                .exists(),
            "missing fixture {fixture}"
        );
    }
    red_cases(
        "vendor log readers",
        &[
            "vendor log location",
            "Claude final turn",
            "Codex rollout formats",
            "Cursor trailing reminder",
            "record and usage deduplication",
            "stable no-log failure",
        ],
    );
}

#[test]
fn config_and_allowlist() {
    red_cases(
        "config/allowlist",
        &[
            "required Discord configuration",
            "Herdr socket default",
            "poll interval validation",
            "owner-only allowlist",
            "gateway intents and no REST retries",
        ],
    );
}
