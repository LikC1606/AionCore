# aionui-team

`aionui-team` provides a small collaboration kernel for one lead and its team
members. Canonical `WorkItem` state is the source of truth for work; the
mailbox carries context, questions, and result summaries.

## Runtime shape

```text
TeamSession
|- member runtime and scheduler
|- mailbox
|- TeamMcpServer
`- canonical TeamCommand / TeamQuery services
   `- WorkItem + delivery persistence
```

The collaboration model is directional:

- superior: may delegate bounded work and review it;
- peer: may exchange context, but cannot control the other's work;
- subordinate: may execute and submit work assigned by a direct superior.

Authorization comes from the member credential bound by the MCP server and
the conversation principal resolved by the service. Tool payloads never choose
the actor or team.

## Interfaces

- [HTTP API](./api.md): team lifecycle and public routes.
- [MCP](./mcp.md): the native agent collaboration surface.
- [Internals](./internals.md): state ownership and wake behavior.
- [Prompts](./team-prompts.md): short role and wake context.
- [Frontend guide](./frontend-guide.md): client integration boundary.

Historical SQLite migrations may still contain retired tables so old databases
upgrade safely. They are not runtime work state.
