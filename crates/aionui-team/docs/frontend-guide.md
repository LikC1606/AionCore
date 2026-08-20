# Team frontend guide

The browser manages presentation and user intent. It does not reproduce agent
authorization, work transitions, Git integration, or wake scheduling.

## Read model

Render canonical WorkItems from Team query responses. Useful fields include:

- state and revision;
- subject, assignee, controller, and reviewer;
- delivery requirement and submitted evidence;
- server-computed allowed actions.

Treat allowed actions as the basis for controls. A stale revision response
means the client must refresh before retrying.

## Commands

Public UI mutations should map to canonical commands with a stable idempotency
key. Do not write work state optimistically as a second source of truth. Refresh
from the returned receipt or query snapshot after a successful command.

Git delivery controls must show the exact repository, base, branch, and head
coordinates. Integration conflict and retry state comes from durable query
state, not from parsing chat messages.

## Conversation and runtime

- User-to-agent chat continues through the normal conversation API.
- Agent-to-agent mailbox traffic is an internal coordination mechanism.
- Roster and runtime status use existing Team HTTP responses and WebSocket
  events such as `team.agentStatusChanged`, `team.agentSpawned`, and
  `team.agentRemoved`.
- MCP is backend-to-agent transport; the browser does not call it directly.

## Avoid

- inferring work completion from message text;
- exposing internal merge/conflict/recovery commands as UI actions;
- accepting a client-supplied actor identity;
- maintaining a parallel task list;
- treating an idle member as failed.
