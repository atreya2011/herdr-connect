# Codex `PreToolUse` activity fixtures

`command.json` is constructed from the same `session_id`/`turn_id`/`tool_input` shape as the
live-captured [`codex-permission-request/default.json`](../codex-permission-request/default.json)
fixture, with `hook_event_name` changed to `PreToolUse`: unlike that fixture, no live Codex
`PreToolUse` payload has been captured under Herdr yet.

Codex's embedded hook schema declares `tool_input` as any JSON value, so non-Bash tools reach the hook without a `command`; the decoder gives such a payload an empty summary, covered by a unit test rather than a fixture.
