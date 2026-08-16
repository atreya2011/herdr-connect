# Cursor `beforeShellExecution` fixture

This fixture is a sanitized copy of the real Cursor hook payload captured under Herdr in
`/tmp/herdr-connect-rs-gauntlet-20260815-102236/t8-capture.jsonl`. `/tmp` paths are represented as
`<tmp>`.

Observed live behavior:

- `session_id == herdr agent_session.value`, so it identifies the Herdr agent session.
- `generation_id` is the per-request vendor-neutral request ID and maps to the broker interaction's
  `prompt_id`.
- Cursor's `allow` result is an unresolved bug because the built-in allowlist takes precedence;
  `deny` is reliable.
- The adapter is therefore fail-closed: broker timeout, malformed response, or unavailable broker
  emits a Cursor denial. Silence or allow is unsafe because Cursor may continue to require the
  shell approval dialog.
