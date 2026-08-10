# Parity checklist

## Herdr socket client

1. Connect to the configured Herdr Unix socket and exchange newline-delimited JSON-RPC messages.
2. Issue `agent.list` and return the contained agent snapshot.
3. Issue `tab.list` and return the contained tab snapshot.
4. Match JSON-RPC response IDs, surface RPC error envelopes, and fail malformed responses.
5. Preserve agent status, terminal ID, tab ID, pane ID, working directory, title, and vendor session identity.

## Transition watcher

6. Poll `agent.list` immediately and then after each completed polling interval.
7. Prime from the first successful snapshot without emitting transitions.
8. Emit changes only for existing terminal IDs whose status changed.
9. Replace the snapshot so disappeared terminal IDs are forgotten.
10. Stop polling idempotently and ignore in-flight results from an older lifecycle generation.
11. Report polling and transition-handler failures through the configured error surface.

## Discovery loop

12. Poll agents and tabs and synchronize every visible workspace and tab.
13. Derive tab names from labels, replacing purely numeric labels with the terminal title.
14. Keep tab names frozen and suffix every name with its tab ID.
15. Reject a tab ID that cannot fit the Discord thread-name limit.
16. Serialize overlapping discovery synchronizations and stop scheduling after shutdown.
17. Continue discovery after one synchronization failure and report the failure.

## Topology mapping

18. Map each Herdr workspace to one Discord text channel identified by a workspace topic marker.
19. Reuse matching workspace channels and create missing channels with the expected topic.
20. Map each tab to a thread identified by the tab-ID name suffix.
21. Search active and paginated archived public threads, reuse matches, and unarchive reused threads.
22. Create missing public threads with the frozen name and add the owner as a member.
23. Cache workspace and tab mappings for transition delivery.

## Transition cards + embeds

24. Post only `working` to `blocked`, `done`, or `idle` transitions.
25. Read the final response and interaction details from the agent’s own session log.
26. Use the blocked question when present; otherwise use the captured final response.
27. Render status-colored embeds with bounded descriptions, metadata fields, tool failures, and failure text.
28. Include bounded changed-file summaries and inline diffs when available.
29. Split long content into numbered complete Discord messages without breaking fenced code blocks where possible.
30. Mention the owner exactly once for blocked transitions and suppress mentions for other statuses.

## Delivery queue

31. Capture transition content at observation time before enqueueing delivery.
32. Deliver queued messages in order with a stable bounded nonce and Discord nonce deduplication enabled.
33. Suppress later same-tab siblings within one delivery cycle after a delivery is selected or blocked.
34. Retry Discord delivery failures and evict entries after five attempts.
35. Evict pending deliveries whose source tab no longer exists and bound the queue to 100 entries.
36. Preserve the primary delivery failure while continuing independent queue cleanup.

## Live status

37. Format watcher output as `<agent> <terminal_id>: <from> -> <to>`.
38. Run the console watcher without requiring Discord configuration.
39. Stop status and activity polling on SIGINT and SIGTERM.

## Live activity/watch

40. Read only complete new JSONL records after a persisted byte position and detect truncation or rotation.
41. Read Cursor activity rows from SQLite by increasing row ID.
42. Merge incremental activity with prior tool counts, task subjects, and plan state.
43. Track the current tool, tool counts, task subjects, and latest plan step for working agents.
44. Reset activity position and feed when a working agent’s session identity changes.
45. Remove non-working agents from the live activity set and report only advancing or rotated activity.

## Vendor log readers

46. Locate Claude, Codex, and Cursor session logs from the vendor-specific session identity.
47. Read the final Claude turn after the latest prompt and extract response text, tools, failures, model, tokens, plan, branch, and changed files.
48. Read the final Codex turn from rollout JSONL and extract response text, tools, failures, model, effort, tokens, plan, and changed files.
49. Read Cursor SQLite blobs, normalize tool calls and results, and extract the final response, failures, plan, and changed files.
50. Deduplicate split vendor records and API usage while preserving parallel tool counts.
51. Return the stable no-log failure when a session or log is unavailable or unreadable.

## Config/allowlist

52. Require non-empty trimmed Discord token, guild ID, and owner ID before gateway login.
53. Read the Herdr socket path from configuration and default it to the standard Herdr socket location.
54. Read and validate the polling interval configuration.
55. Ignore every Discord user except the configured owner, and deny all users when the allowlist is empty.
56. Use the configured Discord guild and gateway intents and do not retry REST requests automatically.
