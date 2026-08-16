# CLAUDE.md

Rust rewrite of herdr-connect: a two-way bridge between Discord and agents running in herdr panes. The TypeScript reference implementation lives in the herdr-connect repo; its merged main is the semantic authority when behavior is underdetermined here.

## Commands

```bash
cargo fmt --check                          # formatting gate
cargo clippy --all-targets -- -D warnings  # lint gate (pedantic, nursery, cargo at deny)
set -a; . ~/.config/herdr-discord/env; set +a; cargo test   # full suite incl. real guild
```

Never print or commit the env file's contents. Real-guild tests create only `testrun-` prefixed channels, clean up on every path, and end with a named zero-leftover check.

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

## Repo conventions

- Commits go straight to `main` in small atomic chunks. No PRs, no remote.
- Source is organized as modules under `src/` with crate-root re-exports (post folder-split).
- Cursor prompts may stall at the Herdr level; the bridge acknowledges submission with state unconfirmed and never injects keys.
- Refusal replies exist only inside threads with a `[tab_id]` suffix whose parent topic is `herdr workspace [workspace_id]`; all other surfaces stay silent.
