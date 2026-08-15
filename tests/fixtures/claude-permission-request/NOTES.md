# Claude `PermissionRequest` fixtures

These fixtures are sanitized copies of live Claude 2.1.x Haiku PermissionRequest hook stdin payloads captured under Herdr with `permission_mode` set to `default`.

## Observed live evidence

- CORRELATION: The payload carries `session_id` and `prompt_id`. It does not carry `tool_use_id`. The broker must correlate on `(session_id, prompt_id)` and must not assume that `tool_use_id` exists for `PermissionRequest`.
- `hook_event_name` is `PermissionRequest`.
- DECISION SUPPRESSION: The allow-variant hook emitted the exact documented decision JSON `{"hookSpecificOutput":{"hookEventName":"PermissionRequest","decision":"allow"}}`. The interactive TUI dialog still rendered, and the tool did not run.
- The default capture also fell through to the interactive dialog. The captures provide no evidence of a hook timeout; they do provide evidence that the interactive flow was not suppressed.
- The observed allow result differs from the documented interactive outcome. Two explanations remain undistinguished without Anthropic's exact accepted schema: `allow` may not be honored in the interactive TUI, or the accepted field/shape may differ subtly from the documented shape and the output fell through under the documented malformed-output rule.

## Repro-gate

This is a repro-gate for tasks 4/5. The hook-broker approval design depends on interactive suppression working, and that behavior is currently UNPROVEN on this Claude version.
