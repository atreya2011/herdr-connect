# Codex `PermissionRequest` fixture

This fixture is a sanitized copy of a real Codex 0.147 `gpt-5.6-luna` `PermissionRequest` hook stdin payload captured under Herdr with `approval_policy=untrusted` and `permission_mode=default`.

## Observed live evidence

- CORRELATION: The payload carries `session_id` and `turn_id`. `session_id == herdr agent_session.value` was live-proven for the pane, so correlate on `session_id` for pane mapping; `turn_id` distinguishes requests within a session. Unlike Claude (`session_id` + `prompt_id`, with no `tool_use_id`), Codex gives `session_id` + `turn_id`.
- The Codex approval hook fires only under a prompting `approval_policy` such as `untrusted`. With `on-request`, the model can self-approve safe commands; `touch` ran with no prompt. The broker path is reached only when Codex actually prompts.
- DECISION: Codex accepts the documented allow/deny decision object. The allow/deny encoding shape is task 7's job; this fixture does not claim a decision schema beyond the captured request.
