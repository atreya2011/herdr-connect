# Herdr Chat Bridge

The bridge connects local Herdr agents to Discord or Slack for a single owner. Discord represents workspaces as channels and tabs as threads. Slack represents each mirrored tab as a channel, with no workspace channel or bot thread-parent message.

## Language

**Workspace channel**:
The Discord channel representing exactly one Herdr workspace.
Slack has no workspace channels.
_Avoid_: Project channel, repository channel

**Tab thread**:
The Discord thread representing exactly one Herdr tab and its agent conversation.
Slack uses a tab channel instead.
_Avoid_: Pane thread, agent thread

**Tab channel**:
The public Slack channel representing one mirrored Herdr tab and its agent conversation. Its topic stores the workspace and tab IDs, and its name combines their labels and IDs. Messages and cards go directly in this channel; no bot thread-parent message is created. Discord uses a tab thread instead.
_Avoid_: Workspace channel, tab thread

**Owner**:
The single person authorized to prompt agents and resolve permission requests, identified by `DISCORD_OWNER_ID` on Discord and `SLACK_OWNER_ID` on Slack.
_Avoid_: Admin, operator, allowed user

**Agent transition**:
An observed change in an agent's Herdr lifecycle state.
_Avoid_: Status event, pane event

**Blocked card**:
The non-interactive Discord card or Slack message posted when a pane goes blocked and no live permission request or question card covers it, carrying the pending question or the pane's last context and mentioning the owner. Working and done post no card; Slack working status is separate.
_Avoid_: Transition card, end card, notification, log dump

**Reported session**:
The vendor session identity supplied by the agent's Herdr integration. Codex supplies it correctly only when the pane runs `codex --no-daemon`; the shared daemon runs hooks with another pane's environment. Codex reports it only when the first prompt is submitted, at the same moment it creates the session log.
_Avoid_: Terminal session, inferred session

**Owner prompt**:
Text authored by the owner in a Discord tab thread or Slack tab channel for semantic submission to its mapped agent. Submitted the same way to an idle, done, or working pane; the agent queues one sent mid-turn.
_Avoid_: Key injection, terminal input

**Terminal prompt**:
Text the owner types directly into a Herdr pane, mirrored into its Discord tab thread through the workspace webhook under the owner's identity, or into its Slack tab channel as a labeled bot message without impersonating or notifying the owner. A pane's echo of an owner prompt the bridge just submitted is suppressed.
_Avoid_: Owner prompt, terminal input, key injection

**Permission request**:
A synchronous vendor request that requires an allow or deny decision before the vendor continues.
_Avoid_: Blocked pane, menu prompt

**Permission card**:
The interactive Discord or Slack representation of a live permission request, with Allow and Deny buttons. Slack uses Block Kit.
_Avoid_: Blocked card, approval message

**Question card**:
The interactive representation of the `AskUserQuestion` question Claude's terminal dialog shows: option buttons for a single-select question, a Discord select menu or Slack menu with Submit choices for a multiSelect question. Answering it types into that dialog; the owner can equally answer in the terminal.
_Avoid_: Permission card, blocked card, poll

**Live message**:
A plain message posted in the Discord tab thread or Slack tab channel for new complete assistant text in a pane's vendor log, whatever the pane's status. Assistant replies arrive this way; there is no turn-end card.
_Avoid_: Streaming message, partial reply, live card
