# Rust implementation resume

This file preserves the product information needed after the historical TypeScript worktrees are removed. Current Rust code, tests, fixtures, and owner-locked laws remain authoritative.

## Resume gate

Before new implementation work:

1. Verify that this Rust process will be the only bridge using the Discord guild and token.
2. Run formatting and strict lint gates.
3. Run the full real-service suite serially with `.env` exported without printing it.
4. Confirm that no `testrun-` Discord channels remain.

The full suite was not re-run at the TypeScript-to-Rust handoff. Formatting and strict linting were green. Do not infer a current full-suite green result.

## Immediate work

### Cursor permission viability

The Cursor decoder, decision encoder, fail-closed hook behavior, broker adapter, and hook registration example exist. Prove the complete live path with a real Cursor permission request, broker, Discord interaction, and visible deny when the broker is unavailable. Treat an allowlist that bypasses hook output as a vendor limitation, not a bridge success.

### Topology synchronization cost

The startup sweep fetches guild channels and active threads once at its start into a mutex-guarded `TopologyCache`, reusing that pair across every tab it syncs instead of refetching per tab. The delivery path (`sync_route`) now serves a route straight from that cache when it already holds the route's workspace channel and tab thread, and refetches only when the cache is empty or does not yet hold the route. A send against a cached thread that fails as an unknown channel clears the cache and logs; the next event resolves the route again through the cache-first path, rather than re-resolving and retrying inside the same call. The lock is held for the whole call so concurrent requests still serialize correctly instead of racing a duplicate create. The permission path (`sync_channel`) still fetches a fresh pair on every call; port the same cache-first behavior there only with its own real-service tests, not by assuming this note is enough. Preserve the outcome of [historical issue #27](https://github.com/atreya2011/herdr-connect/issues/27): ordinary checks read the fetched lists, while archived-thread listing is a miss-path HTTP read, skipped whenever the active list already resolves the tab. Port the behavior, not the old discord.js implementation details.

## Unported product backlog

These behaviors existed as TypeScript product decisions or reviewed branches but are absent from the current Rust implementation. They are not part of the immediate handoff unless the owner prioritizes them.

- [Dashboard and read commands](https://github.com/atreya2011/herdr-connect/issues/7): pinned cross-workspace status, ephemeral `/agents`, and on-demand `/read <tab_id>`.
- [Operations](https://github.com/atreya2011/herdr-connect/issues/8): a systemd user service and a `doctor` preflight. The historical environment-file path is obsolete; this repository ignores `.env`.
- [Rich result details](https://github.com/atreya2011/herdr-connect/issues/29): vendor-recorded model, effort, tool, token, plan, branch, changed-file, and bounded diff details, omitting fields the vendor does not record.
- [Durable live status](https://github.com/atreya2011/herdr-connect/issues/36): one bounded status card per active turn with persisted lifecycle and orphan cleanup. Prior dead Rust live-status surfaces were deliberately removed; reintroduce this only as a newly approved feature with real-service tests.

## Do not port

- Raw `!keys`, pane key injection, or numbered menu buttons that type terminal input from historical issues #4 and #6. Owner text uses semantic `agent.prompt`; supported permissions use vendor-native hooks and Discord interactions.
- The TypeScript delivery scheduler race in historical issue #16. The Rust implementation does not contain that scheduler; require a Rust reproducer before changing Rust delivery.
- Bun, discord.js, fake-client, pull-request, or TypeScript lint conventions. This repository is Rust, local-only, direct to `main`, and real-services-only.
- Historical topology counts, pane identifiers, bridge process identifiers, or agent locations. They were observations, not product contracts.

## Preserved product contracts

- One workspace channel per Herdr workspace and one tab thread per Herdr tab.
- A pane whose Herdr integration reports no session identity is not mirrored — no tab thread and no card of any kind — until a later snapshot reports one (its workspace channel is per workspace and may still exist because another pane in it has a session).
- Topic and tab suffix are identity. Existing channel names and frozen thread names are not reconciled after creation.
- Reported vendor sessions provide content; pane scraping and guessed session paths are forbidden, except for reading a Claude pane's Herdr detection snapshot to extract a pending blocked question.
- Blocked-only owner mentions; explicit mention allowlists; working and done post nothing.
- Owner-only semantic prompts inside qualifying mapped threads; all unrelated Discord surfaces remain silent.
- Permission decisions are correlated, expiring, exactly once, and invalid when the requesting hook disconnects.
- A closed Herdr tab's thread is deleted; a closed Herdr workspace's channel is deleted (Discord removes its threads with it). Startup deletes every workspace channel and tab thread Herdr no longer lists. Deletion is the rule; nothing is archived.
- A tab with a numeric label is renamed once to a generated `word-word-word` label before it is routed; a rename that fails is logged and the tab is not mirrored until it succeeds.
- One watcher per pane, four rules: (1) a session-carrying pane appears -> create its tab thread and open one watcher on its vendor log at the current end, or at 0 if the log does not exist yet; (2) the log gains a complete assistant text -> post it as a plain message (never a tool call), per log position, whatever the pane's status; (3) the pane goes blocked -> post the blocked, permission, or question card, while working and done post nothing; (4) the pane closes or its session resolves to a different log -> close the watcher, the only time one closes. A restart re-follows from the current end and reposts nothing; an exact repeat is suppressed by Discord's position-derived nonce.
