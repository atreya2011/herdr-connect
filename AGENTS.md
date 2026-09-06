# Agent instructions

How to change this crate. Architecture is in [README.md](README.md). Terms are in [CONTEXT.md](CONTEXT.md). Remaining work is in [ROADMAP.md](ROADMAP.md). Current Rust tests override historical TypeScript issues.

## Commands

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
set -a; . ./.env >/dev/null 2>&1; set +a; cargo test -- --test-threads=1
```

Never print or commit `.env`. Real-guild tests create only `testrun-` channels, clean up on every path, and end with a named zero-leftover check.

## Laws (locked by the owner)

- **Red-green**: a failing test is written before any behavior. Tests are the contract — assertions, inputs, expected values, and fixtures are frozen; adapting a test to force green is forbidden. A genuinely wrong test stops work and gets reported, not edited.
- **Real services only**: no mocked, stubbed, faked, or emulated service clients or responses, and no test-run servers posing as a service. Socket tests connect to the real running herdr socket. Discord tests run against the real guild. Vendor-log tests use committed fixtures mirroring real on-disk records.
- **Barebones**: overengineering is evil. No abstractions beyond what tests force, no speculative configurability.
- **No fallbacks**: mandatory behavior that fails must fail fast through the error surface. Placeholder values, invented defaults, and silent recovery are forbidden.
- **No reinvented wheels**: use battle-tested crates where they replace real hand-rolled complexity — and only then. A crate earns its place; speculative dependencies are overengineering.
- **Strictest lints**: no lint `allow` anywhere in source. The single exception is `multiple_crate_versions` in the manifest, justified by a comment.
- **Idiomatic Rust**: `Result` errors, no panics in library paths, no `block_on` in library code, references where callers keep using values.
- **Reproduction or rejection**: a bug claim without a reproducing failing test or exact command evidence is rejected unread.
- **Honest reporting**: no green claim without pasting the run that proves it. The orchestrator independently re-runs all gates; false green claims are treated as defects.
- **Bounded change**: one commit changes at most 1,000 hand-written lines; generated files such as `Cargo.lock` are excluded from the count.
- **No ghost cases**: code and tests exist only for behavior with evidence — a reproduced defect, a real log, or a stated requirement. Hypothetical situations get neither code nor tests.

## Repo conventions

- Commits go to `main` in small atomic chunks.
- Source is modules under `src/` with crate-root re-exports.
- Prompts may stall at the Herdr level for any vendor. The bridge acknowledges submission with state unconfirmed and recovers with a two-rung ladder: Enter first; only if the pane is still idle, Ctrl+U then one fresh resubmission.
- Refusal replies exist only inside threads with a `[tab_id]` suffix whose parent topic is `herdr workspace [workspace_id]`.
- Use the terms in `CONTEXT.md`. Read `ROADMAP.md` before selecting new implementation work.
