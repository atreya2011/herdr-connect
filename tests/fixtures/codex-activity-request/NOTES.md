# Codex `PreToolUse` activity fixtures

These fixtures are constructed from the same `session_id`/`turn_id`/`tool_input` shape as the
live-captured [`codex-permission-request/default.json`](../codex-permission-request/default.json)
fixture, with `hook_event_name` changed to `PreToolUse`: unlike that fixture, no live Codex
`PreToolUse` payload has been captured under Herdr yet.

## Shape

- `hook_event_name` is `PreToolUse`.
- `command.json` carries `tool_input.command`, mirroring the live-captured `Bash` shape.
- `other-field.json` carries no `command`; `tool_input.file_path` comes before `description` in
  the object, exercising the "first string field" fallback the command field lacks -- Codex's
  `tool_input` shape varies per tool with no fixed fallback field list like Claude's.
- `empty-tool-input.json` carries an empty `tool_input` object.
