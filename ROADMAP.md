# Rust implementation resume

This file preserves the product information needed after the historical TypeScript worktrees are removed. Current Rust code, tests, fixtures, and laws remain authoritative.

## Resume gate

Before new implementation work:

1. Keep the Discord service running with its `testrun-` filter enabled; test bridge instances disable filtering and own only their `testrun-` resources. Ensure that no other Socket Mode connection uses the Slack test app before a Slack test connects. The service stays Discord-only.
2. Run formatting and strict lint gates.
3. Run the full real-service suite serially with `.env` exported without printing it.
4. Confirm that no `testrun-` Discord channels or threads and no unarchived `testrun-` Slack channels remain. Slack tests clean up by archiving; archived channels do not count as leftovers.

The full suite was not re-run at the TypeScript-to-Rust handoff. Formatting and strict linting were green. Do not infer a current full-suite green result.

## Immediate work

### Cursor permission viability

The Cursor decoder, decision encoder, fail-closed hook behavior, broker adapter, and hook registration example exist. Prove the complete live path with a real Cursor permission request, broker, Discord interaction, and visible deny when the broker is unavailable. Treat an allowlist that bypasses hook output as a vendor limitation, not a bridge success.

### Discord topology synchronization cost

The startup sweep fetches guild channels and active threads once at its start into a mutex-guarded `TopologyCache`, reusing that pair across every tab it syncs instead of refetching per tab. The delivery path (`sync_route`) now serves a route straight from that cache when it already holds the route's workspace channel and tab thread, and refetches only when the cache is empty or does not yet hold the route. A send against a cached thread that fails as an unknown channel clears the cache and logs; the next event resolves the route again through the cache-first path, rather than re-resolving and retrying inside the same call. The lock is held for the whole call so concurrent requests still serialize correctly instead of racing a duplicate create. The permission path (`sync_channel`) still fetches a fresh pair on every call; port the same cache-first behavior there only with its own real-service tests, not by assuming this note is enough. Preserve the outcome of [historical issue #27](https://github.com/atreya2011/herdr-connect/issues/27): ordinary checks read the fetched lists, while archived-thread listing is a miss-path HTTP read, skipped whenever the active list already resolves the tab. Port the behavior, not the old discord.js implementation details.

Slack caches tab channels separately and queues name reconciliation outside its routing lock.

## Unported product backlog

These behaviors existed as TypeScript product decisions or reviewed branches but are absent from the current Rust implementation. They are not part of the immediate handoff unless the owner prioritizes them.

- [Dashboard and read commands](https://github.com/atreya2011/herdr-connect/issues/7): pinned cross-workspace status, ephemeral `/agents`, and on-demand `/read <tab_id>`.
- [Operations](https://github.com/atreya2011/herdr-connect/issues/8): the `doctor` preflight remains unported. A [systemd user service example](examples/herdr-connect.service) runs the no-argument Discord bridge; the service stays up during tests. This repository ignores `.env`.
- [Rich result details](https://github.com/atreya2011/herdr-connect/issues/29): vendor-recorded model, effort, tool, token, plan, branch, changed-file, and bounded diff details, omitting fields the vendor does not record.
- [Durable live status](https://github.com/atreya2011/herdr-connect/issues/36): one bounded status card per active turn with persisted lifecycle and orphan cleanup. Prior dead Rust live-status surfaces were deliberately removed; reintroduce this only as a newly approved feature with real-service tests.

## Do not port

- Raw `!keys` and generic pane key injection from historical issues #4 and #6. Owner text uses semantic `agent.prompt`; supported permissions use vendor-native hooks and Discord or Slack interactions. The one exception is answering a Claude question card, which types the answer into Claude's own dialog.
- The TypeScript delivery scheduler race in historical issue #16. The Rust implementation does not contain that scheduler; require a Rust reproducer before changing Rust delivery.
- Bun, discord.js, fake-client, pull-request, or TypeScript lint conventions. This repository is Rust, local-only, direct to `main`, and real-services-only.
- Historical topology counts, pane identifiers, bridge process identifiers, or agent locations. They were observations, not product contracts.

## Preserved product contracts

- Discord uses one workspace channel per mirrored Herdr workspace and one tab thread per mirrored tab. Slack uses one public channel per mirrored tab, with no workspace channel or bot thread-parent message; the owner is invited to each new channel.
- A pane whose Herdr integration reports no session identity has no tab conversation or cards until a later snapshot reports one. Its Discord workspace channel may exist for another pane with a session; Slack creates no channel for that session-less tab.
- Discord identity is the workspace channel topic and tab thread suffix. Existing Discord channel names and frozen thread names are not reconciled after creation. Slack identity is the workspace and full tab IDs in its channel topic. Slack channel names combine workspace label, workspace ID, tab label, and short tab ID; workspace and tab renames and startup reconcile those names. Labels are shortened to fit Slack's 80-character limit; IDs are preserved. Rate-limited renames run outside the routing lock so messages continue.
- Reported vendor sessions provide content; pane scraping and guessed session paths are forbidden, except for reading a Claude pane's Herdr detection snapshot to extract a pending blocked question and to confirm the dialog is still showing before an answer is typed.
- Only blocked, permission, and question cards mention the owner. Discord uses explicit mention allowlists; Slack escapes message text and emits explicit mentions in its cards. Working and done post no cards; Slack shows assistant working status on the latest owner message and clears it when work stops.
- Owner-only semantic prompts inside mapped Discord tab threads or Slack tab channels; unrelated chat surfaces and bot messages are ignored. Slack prompt-submission and free-text-answer acknowledgements are ephemeral to the owner.
- Permission decisions are correlated, expiring, exactly once, and invalid when the requesting hook disconnects.
- Discord deletion is two-way. A closed Herdr tab's thread is deleted; a closed Herdr workspace's channel is deleted (Discord removes its threads with it). An owner-deleted tab thread closes its Herdr tab (`herdr tab close`), and an owner-deleted workspace channel closes its Herdr workspace (`herdr workspace close`). Owner deletion is proved only by a guild audit-log entry of the matching type whose target is that thread or channel and whose user is the owner; any other deletion, no matching entry, or a failed audit-log request closes nothing. The gateway's thread-delete event carries no name, so the tab is resolved from a durable registry of bridge-owned thread ids (filled from active and archived listings and creates, never cleared by cache invalidation); a thread the registry does not hold is not a live tab's thread and is ignored with no Herdr call and no notice. Archived threads are registered at startup per channel, independent of the delete pass; a failed listing is logged. Startup deletes workspace channels and tab threads absent from Herdr, preserving `testrun-` resources in the service. Discord deletes obsolete topology rather than archiving it.
- Slack closes a Herdr tab only when its channel-archive event identifies the owner. Other actors and deletion events without owner proof close nothing; no Slack channel event closes a workspace. Closing a Herdr tab renames and archives its channel; closing a workspace does so for all its tab channels. Startup also renames and archives orphaned tab channels and legacy workspace channels. Renaming before archive releases the original name for reuse; archived channels remain in Slack.
- A tab with a numeric label is renamed once to a generated `word-word-word` label before it is routed; a rename that fails is logged and the tab is not mirrored until it succeeds.
- One watcher per pane, four rules: (1) a session-carrying pane appears -> create its Discord tab thread or Slack tab channel and follow its vendor log from the current end, or from 0 if the log does not exist yet; (2) the log gains complete assistant text -> post it as a plain message (never a tool call), per log position, whatever the pane's status; (3) the pane goes blocked -> post the blocked, permission, or question card, while working and done post no cards; (4) the pane closes or its session resolves to a different log -> close the watcher, the only time one closes. A restart follows from the current end and reposts nothing. Discord additionally suppresses exact repeats within its nonce window using a position-derived nonce; Slack does not use that nonce.
