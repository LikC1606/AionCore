# Team MCP

Team MCP is the native agent-facing collaboration surface. Each active team
session exposes one local MCP server. A member authenticates with a credential
bound to one slot; the server derives the caller identity from that binding.

## Work tools

| Tool | Purpose |
|------|---------|
| `team_inspect` | Read visible WorkItems, exact revisions, and server-computed allowed actions. |
| `team_delegate` | Create and queue bounded work for a direct subordinate. |
| `team_progress` | Start, block, or resume work assigned to the caller. |
| `team_submit` | Submit an inline result or Git content revision plus immutable head. |
| `team_review` | Accept, request changes, or reject a submitted result. |
| `team_cancel` | Cancel work controlled by the caller. |

These tools are adapters over canonical `TeamCommand` and `TeamQuery`; they do
not maintain a second work model. Mutation calls require an idempotency key and
the current revision. The kernel validates the caller's relation and role.

The work lifecycle is:

```text
delegate -> queued -> running <-> blocked -> submitted -> reviewed
                                               |            |
                                               `-- changes --'
```

Terminal outcomes are completed, rejected, or cancelled. Internal integration
transitions and recovery operations are not public MCP tools.

## Coordination tools

| Tool | Access | Purpose |
|------|--------|---------|
| `team_send_message` | member | Send mailbox context to a slot, `leader`, or `*`. |
| `team_members` | member | Read the current roster and runtime status. |
| `team_list_assistants` | member | List assistants eligible for staffing. |
| `team_describe_assistant` | member | Inspect one assistant before staffing. |
| `team_spawn_agent` | lead | Add a user-approved teammate. |
| `team_rename_agent` | lead | Rename a member. |
| `team_shutdown_agent` | lead | Start graceful teammate shutdown. |

Mailbox messages are not work state. A message may explain a WorkItem, report a
blocker, or summarize a result, but status changes must use the work tools.

## Identity and errors

- The initial MCP credential selects the member slot.
- The live conversation must resolve to the same member.
- `actor`, `member_id`, `team_id`, and equivalent identity fields are not
  accepted in work-tool payloads.
- Missing service composition returns an explicit transport-unavailable error;
  there is no silent fallback or success response.
- Stale revisions and disallowed transitions fail without mutation.

## Delivery modes

`team_submit` supports:

- `inline`: work whose result is communicated through the mailbox and recorded
  as an inline delivery;
- `git`: work bound at delegation time to a server-owned repository, base
  commit, and branch assignment. Submission supplies only `content_revision`
  and the immutable `head_commit`; the service resolves all assignment
  coordinates from the WorkItem. Review acceptance does not itself forge merged
  evidence; integration remains an internal, controlled service.

## Tool discovery

The tool descriptors in `aionui-api-types::team_tools` are authoritative for
names and JSON schemas. Lead-only tools are filtered from teammate discovery.
MCP and CLI adapters consume the same descriptors.
