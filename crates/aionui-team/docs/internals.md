# Team internals

## State ownership

The subsystem separates coordination from durable work:

| Concern | Owner |
|---------|-------|
| Work lifecycle, revision, assignee, reviewer, delivery | canonical Team kernel and repository |
| Queries and allowed actions | `TeamQueryService` |
| Mutations and authorization | `TeamCommandService` |
| Agent-to-agent context | `Mailbox` |
| Runtime status and wake scheduling | `TeammateManager` / `TeamSession` |
| MCP adaptation | `TeamWorkRuntimeService` |

There is no scheduler-owned work database. Historical schema is migration-only.

## Relationships

Authorization uses directional member relations:

- a superior may delegate to a direct subordinate and control that WorkItem;
- an assignee may start, block, resume, and submit its assigned WorkItem;
- the designated reviewer may review a submission;
- peers can communicate but do not gain assignment or review authority;
- unrelated members fail closed.

The authenticated MCP credential and conversation principal must identify the
same member before a command reaches the kernel.

## Wake path

```text
mailbox enqueue / runtime event
  -> TeamSession reconcile
  -> TeammateManager.try_wake
  -> unread mailbox projection + bounded WorkItem summary
  -> agent turn
  -> mailbox actions and canonical commands
  -> finalize / idle
```

The wake payload contains unread messages plus a short dynamic summary of
relevant WorkItems and allowed actions. Agents use `team_inspect` for exact
state and revisions.

## Invariants

- One member credential maps to one slot.
- Payload data cannot select the actor.
- Durable command idempotency prevents duplicate mutations.
- Exact revisions prevent stale transitions.
- Git work is bound to repository, base, branch, and submitted head.
- Public tools cannot record merge, conflict, or recovery state.
- Mailbox messages never substitute for WorkItem transitions.
- Lead cannot shut itself down.
- Removing a member clears its runtime wake state.

## Composition

The application composition root installs canonical work services once:

```rust
pub fn configure_team_work(
    &self,
    work_coordinator: Arc<TeamWorkCoordinator>,
    command_service: Arc<TeamCommandService>,
    query_service: Arc<TeamQueryService>,
) -> Result<(), TeamError>
```

Calling a canonical work tool before this configuration returns an explicit
error. The adapter does not construct repositories or alternate services on
demand.
