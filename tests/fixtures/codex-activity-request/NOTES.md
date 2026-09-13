# Codex `PreToolUse` activity fixtures

`command.json` is constructed from the same `session_id`/`turn_id`/`tool_input` shape as the
live-captured [`codex-permission-request/default.json`](../codex-permission-request/default.json)
fixture, with `hook_event_name` changed to `PreToolUse`: unlike that fixture, no live Codex
`PreToolUse` payload has been captured under Herdr yet.

No other Codex `tool_input` shape has live evidence, so no other fixture exists here.
