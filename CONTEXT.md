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

**Transition card**:
A Discord result posted for a qualifying agent transition.
_Avoid_: Notification, log dump

**Reported session**:
The vendor session identity supplied by the agent's Herdr integration.
_Avoid_: Terminal session, inferred session

**Owner prompt**:
Text authored by the owner in a tab thread for semantic submission to its mapped agent.
_Avoid_: Key injection, terminal input

**Permission request**:
A synchronous vendor request that requires an allow or deny decision before the vendor continues.
_Avoid_: Blocked pane, menu prompt

**Permission card**:
The interactive Discord representation of a live permission request.
_Avoid_: Blocked card, approval message

**Informational blocked card**:
A non-interactive Discord card reporting that an unsupported pane is blocked and must be handled in Herdr.
_Avoid_: Permission card, fallback approval

**Live message**:
A plain, content-only Discord message posted for one new complete assistant text a working pane's vendor log records, ahead of its turn's transition card.
_Avoid_: Streaming message, partial reply, live card
