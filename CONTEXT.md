# Herdr Discord Bridge

This context describes the local, single-owner bridge between Herdr agent activity and its Discord representation.

## Language

**Workspace channel**:
The Discord channel representing exactly one Herdr workspace.
_Avoid_: Project channel, repository channel

**Tab thread**:
The Discord thread representing exactly one Herdr tab and its agent conversation.
_Avoid_: Pane thread, agent thread

**Owner**:
The single Discord user authorized to prompt agents and resolve permission requests.
_Avoid_: Admin, operator, allowed user

**Agent transition**:
An observed change in an agent's Herdr lifecycle state.
_Avoid_: Status event, pane event

**Blocked card**:
The non-interactive Discord card posted when a pane goes blocked and no live permission or question request covers it, carrying the pending question or the pane's last context and mentioning the owner. Working and done post no card.
_Avoid_: Transition card, end card, notification, log dump

**Reported session**:
The vendor session identity supplied by the agent's Herdr integration. Codex supplies it correctly only when the pane runs `codex --no-daemon`; the shared daemon runs hooks with another pane's environment. Codex reports it only when the first prompt is submitted, at the same moment it creates the session log.
_Avoid_: Terminal session, inferred session

**Owner prompt**:
Text authored by the owner in a tab thread for semantic submission to its mapped agent. Submitted the same way to an idle, done, or working pane; the agent queues one sent mid-turn.
_Avoid_: Key injection, terminal input

**Terminal prompt**:
Text the owner types directly into a Herdr pane, mirrored into its tab thread through the bridge-owned workspace webhook under the owner's identity, unless it is the pane's echo of an owner prompt the bridge itself just submitted.
_Avoid_: Owner prompt, terminal input, key injection

**Permission request**:
A synchronous vendor request that requires an allow or deny decision before the vendor continues.
_Avoid_: Blocked pane, menu prompt

**Permission card**:
The interactive Discord representation of a live permission request.
_Avoid_: Blocked card, approval message

**Question card**:
The interactive Discord representation of one pending `AskUserQuestion` question: buttons for a single-select question, a select menu for a multiSelect question.
_Avoid_: Permission card, blocked card, poll

**Live message**:
A plain, content-only Discord message posted for one new complete assistant text a pane's vendor log records, whatever the pane's status. It is the only way a reply reaches Discord; there is no turn-end card.
_Avoid_: Streaming message, partial reply, live card
