# herdr-connect-rs

Two-way Rust bridge between one Discord guild and agents in local Herdr panes.

## Architecture

Discord and Herdr do not share a socket. This process sits between them.

```mermaid
flowchart LR
  owner[Owner]
  discord[Discord]
  bridge[herdr-connect-rs]
  herdr[Herdr]
  panes[Agent panes]

  owner -->|message in a tab thread| discord
  discord -->|"Gateway WebSocket<br/>MESSAGE_CREATE"| bridge
  bridge -->|HTTP: cards, channels, threads| discord
  bridge -->|"Unix JSON-RPC events.subscribe doorbell"| herdr
  bridge -->|"Unix JSON-RPC: agent.list, tab.list, agent.prompt"| herdr
  herdr --> panes
```

- **Herdr → Discord:** at startup, once Discord is configured, the bridge lists agents and tabs once and, in a spawned task beside the event loop, syncs every workspace channel and tab thread, then deletes every workspace channel and tab thread Herdr no longer lists. After that, a Herdr subscribe event wakes the bridge; it lists agents and tabs and diffs snapshots. The mirror is four rules, one watcher per pane:

  1. **A session-carrying pane appears:** create its tab thread and open one watcher on its vendor log at the current end -- or at position 0 if the log does not exist on disk yet, so the whole log it later writes is posted. A pane whose Herdr integration reports no session identity is not mirrored: no tab thread and no card of any kind, until a later snapshot reports one (its workspace channel is per workspace and may still exist because another pane in it has a session). A tab with a numeric label and no terminal title yet is skipped quietly and recorded as pending on every snapshot pass; a `pane.updated` event naming it with a fresh title, or any status transition, wakes the next pass to create its thread, while a name that can never be produced (a tab id too long for Discord) is logged once rather than on every snapshot. The first snapshot after start is silent.
  2. **The log gains a complete assistant text:** post it as its own plain message (never a tool call), in log order. This is per log position, and status plays no part -- a text written while the pane is `working`, `done`, or `idle` is posted the same way. A bridge restart re-follows from the current end, so it reposts nothing; separately, each message's nonce is derived from its log position, so if the same position were ever posted twice within Discord's nonce window it would be suppressed as an exact repeat. Content comes from the vendor session Herdr reported, not from scraping the pane.
  3. **The pane goes `blocked`:** post the blocked, permission, or question card. `working` and `done` post nothing -- a settled turn's reply, and a failed turn's error, both reach Discord as the pane's own live text under rule 2, not as a turn-end card. The blocked card is built once from whatever is available at that moment: a Claude pane's Herdr detection snapshot for the pending question, or the vendor log otherwise.
  4. **The pane closes, or its session resolves to a different log:** close the watcher. A closed Herdr tab deletes its Discord thread; a closed Herdr workspace deletes its Discord channel (Discord removes its threads with it). That is the only time a watcher closes.
- **Terminal prompts → Discord:** the owner's display name and avatar are fetched once at startup from `GET /users/{DISCORD_OWNER_ID}`. For every session-carrying pane with a resolved tab thread, following its vendor log for new user prompts starts the moment the bridge first sees the pane, using the same `notify` watch that drives live text: each new prompt is posted once into the tab thread, in log order, before any assistant text it produced, through a bridge-owned webhook (`herdr-connect owner`, one per workspace channel, found by name or created) executed with the owner's identity and `thread_id`. A prompt already in the log at that first sight is never replayed. A prompt the bridge itself just submitted from Discord is recognized once and dropped rather than mirrored back into the thread it came from. No thread or no session: dropped silently; a delivery error against an unknown channel or unknown webhook clears the cached route or webhook and logs, and the next mirrored prompt resolves both again through the cache-first path -- there is no in-call retry.
- **Discord → Herdr:** gateway events. An owner message in a thread named `… [tab_id]` under topic `herdr workspace [workspace_id]` becomes `agent.prompt` when exactly one matching pane is `idle` or `done`. Other surfaces stay silent.
- **Permissions (optional):** `herdr-connect-rs hook` speaks to a second Unix socket, `HERDR_CLAUDE_BROKER_SOCKET`. That path is not on by default; install [examples/cursor-hooks.json](examples/cursor-hooks.json) (or the Claude/Codex equivalent) if you want Allow/Deny cards.
- **Activity (optional):** `herdr-connect-rs activity --vendor claude`, `--vendor codex`, and `--vendor cursor` speak to the same `HERDR_CLAUDE_BROKER_SOCKET`; install [examples/claude-hooks.json](examples/claude-hooks.json) for Claude, [examples/codex-hooks.json](examples/codex-hooks.json) for Codex, or [examples/cursor-hooks.json](examples/cursor-hooks.json) for Cursor to enable the matching vendor. Each tool call it reports is folded into one plain message per pane per turn -- the first call posts it, later calls in the same turn edit it in place -- and the message is forgotten when the pane next reports `working`, so the new turn starts a new one. A pane whose tab has no thread yet drops the frame silently, and so does a pane the latest Herdr snapshot does not report as `working` with a session: the same no-session rule the mirror follows.
- **Questions (optional, Claude only):** the same `herdr-connect-rs hook --vendor claude` also answers a Claude `AskUserQuestion` tool call matched by the `PreToolUse` entry in [examples/claude-hooks.json](examples/claude-hooks.json), so the owner answers from Discord instead of the pane blocking on Claude's own TUI dialog. Each question in the call gets its own card, in order, with a "Type an answer" hint either way: a single-select question gets one button per option, a multiSelect question gets a Discord select menu instead, and replying in the pane's own thread while any card is open -- single-select or multiSelect -- resolves it with the reply's text instead of submitting the reply as a prompt. Every question in the call must resolve before the hook answers -- the whole call shares one five-minute deadline, and the first card to hit it falls the whole call through to Claude's own dialog, that card reading `expired: no owner answer`. A resolved card reads `resolved: <answer>`, a multiSelect answer joined with `, `.

Identity is the topic and the `[tab_id]` suffix. Existing names are not renamed.

## Configuration

`.env` (mode `600`, gitignored):

- `DISCORD_TOKEN`
- `DISCORD_GUILD_ID`
- `DISCORD_OWNER_ID`

`HERDR_SOCKET_PATH` if the Herdr socket is not the default. Never print or commit `.env`. Run only one bridge against a guild.

```bash
set -a; . ./.env >/dev/null 2>&1; set +a; cargo run
```

## Docs

- [AGENTS.md](AGENTS.md) — how to work in this repo
- [CONTEXT.md](CONTEXT.md) — terms
- [ROADMAP.md](ROADMAP.md) — remaining work
