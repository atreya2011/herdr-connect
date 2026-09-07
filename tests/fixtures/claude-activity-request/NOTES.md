# Claude `PreToolUse` activity fixtures

These fixtures are constructed from the documented `PreToolUse` command-hook stdin schema
(https://code.claude.com/docs/en/hooks.md), not captured live: unlike the `PermissionRequest`
fixtures beside `claude-permission-request/`, no live `PreToolUse` payload has been captured under
Herdr yet.

## Shape

- `hook_event_name` is `PreToolUse`.
- `tool_input` varies by tool: `command.json` carries `command` (and a longer-than-80-character one,
  to exercise summary truncation), `file-path.json` carries `file_path` with no `command`, and
  `empty-tool-input.json` carries an empty `tool_input` object.
- `session_id` is the only field the activity decoder reads besides `tool_name`/`tool_input`; unlike
  `PermissionRequest`, `PreToolUse` carries no `prompt_id` field the decoder needs.
