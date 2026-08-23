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

### Unsupported blocked-card closeout

The informational blocked card is implemented and bounded, but its handoff task was still in progress. Complete the required adversarial review against current code and retain the distinction between an informational blocked pane and a live permission request.

### Topology synchronization cost

The current Rust delivery and permission paths call `sync_topology`, which downloads guild channels plus active and archived threads for each route. Preserve the outcome of [historical issue #27](https://github.com/atreya2011/herdr-connect/issues/27): ordinary checks read gateway-maintained local state, while archived-thread listing remains a miss-path HTTP read. Port the behavior, not the old discord.js implementation details.

### Late terminal titles

`format_thread_name` currently errors every time a numeric tab label has no terminal title. Preserve the outcome of [historical issue #28](https://github.com/atreya2011/herdr-connect/issues/28): wait quietly while a cold-start title is absent, create the thread when it arrives, and surface a permanently unusable name once rather than once per snapshot.

## Unported product backlog

These behaviors existed as TypeScript product decisions or reviewed branches but are absent from the current Rust implementation. They are not part of the immediate handoff unless the owner prioritizes them.

- [Close behavior](https://github.com/atreya2011/herdr-connect/issues/5): post one tab-closing note; let Discord archive the thread; move a closed workspace channel to an Archived category; never delete Discord topology.
- [Dashboard and read commands](https://github.com/atreya2011/herdr-connect/issues/7): pinned cross-workspace status, ephemeral `/agents`, and on-demand `/read <tab_id>`.
- [Operations](https://github.com/atreya2011/herdr-connect/issues/8): a systemd user service and a `doctor` preflight. The historical environment-file path is obsolete; this repository ignores `.env`.
- [Rich result details](https://github.com/atreya2011/herdr-connect/issues/29): vendor-recorded model, effort, tool, token, plan, branch, changed-file, and bounded diff details, omitting fields the vendor does not record.
- [Live activity capture](https://github.com/atreya2011/herdr-connect/issues/40): incremental, complete-record log following for working agents.
- [Durable live status](https://github.com/atreya2011/herdr-connect/issues/36): one bounded status card per active turn with persisted lifecycle and orphan cleanup. Prior dead Rust live-status surfaces were deliberately removed; reintroduce this only as a newly approved feature with real-service tests.

## Do not port

- Raw `!keys`, pane key injection, or numbered menu buttons that type terminal input from historical issues #4 and #6. Owner text uses semantic `agent.prompt`; supported permissions use vendor-native hooks and Discord interactions.
- The TypeScript delivery scheduler race in historical issue #16. The Rust implementation does not contain that scheduler; require a Rust reproducer before changing Rust delivery.
- Bun, discord.js, fake-client, pull-request, or TypeScript lint conventions. This repository is Rust, local-only, direct to `main`, and real-services-only.
- Historical topology counts, pane identifiers, bridge process identifiers, or agent locations. They were observations, not product contracts.

## Preserved product contracts

- One workspace channel per Herdr workspace and one tab thread per Herdr tab.
- Topic and tab suffix are identity. Existing channel names and frozen thread names are not reconciled after creation.
- Reported vendor sessions provide transition content; pane scraping and guessed session paths are forbidden, except for reading a Claude pane's Herdr detection snapshot to extract a pending blocked question.
- Blocked-only owner mentions; explicit mention allowlists; all other transitions are silent.
- Owner-only semantic prompts inside qualifying mapped threads; all unrelated Discord surfaces remain silent.
- Permission decisions are correlated, expiring, exactly once, and invalid when the requesting hook disconnects.
- Discord topology is preserved. Archived threads may be revived; bridge-owned channels and threads are not deleted as normal lifecycle behavior.
