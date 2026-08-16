# herdr-connect-rs

Rust Discord bridge for Herdr agents.

To install the Cursor shell hook, copy [`examples/cursor-hooks.json`](examples/cursor-hooks.json) to Cursor's hooks configuration. Cursor defaults to fail-open and allow responses are unreliable when an allowlist takes precedence; `failClosed: true` plus an explicit deny is the reliable closed state.
