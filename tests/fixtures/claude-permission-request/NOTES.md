# Claude `PermissionRequest` fixtures

These fixtures are sanitized copies of live Claude 2.1.x Haiku PermissionRequest hook stdin payloads captured under Herdr with `permission_mode` set to `default`.

## Observed live evidence

- CORRELATION: The payload carries `session_id` and `prompt_id`. It does not carry `tool_use_id`. The broker must correlate on `(session_id, prompt_id)` and must not assume that `tool_use_id` exists for `PermissionRequest`.
- `hook_event_name` is `PermissionRequest`.
- `permission_mode` is `default`.

## Decision and suppression

- ACCEPTED SCHEMA (Claude 2.1.233): The allow decision is `{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"allow"}}}`. The deny decision is `{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":{"behavior":"deny","message":"..."}}}`. `decision` is an object with a `behavior` field, not a bare string.
- SUPPRESSION PROVEN: A well-formed allow decision returned synchronously from the `PermissionRequest` hook suppresses the interactive TUI dialog and the tool executes with no prompt. This was live-verified on Claude 2.1.233 Haiku under Herdr: the `touch` command ran, no dialog rendered, and the agent settled in the done state.
- GOTCHA: A malformed decision such as `{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":"allow"}}` is discarded and the flow falls through to the interactive dialog under the documented malformed-output rule. The broker MUST emit the object form.

- Remaining task-4 unknowns are hook-timeout behavior, which is undetermined, and whether a running session adopts a newly added hook.
