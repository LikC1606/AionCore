# Team prompts

Team prompts are short role constraints plus current runtime context. They do
not teach the entire protocol and do not tell agents to ignore native tools.

## Lead

The lead owns decomposition, delegation, review, and final synthesis:

1. inspect current work when state may have changed;
2. delegate bounded WorkItems to direct subordinates;
3. exchange concise context through the mailbox;
4. review submitted evidence and create follow-up work when needed;
5. end the turn while waiting.

Roster changes require user intent. Staffing tools do not replace work
delegation.

## Teammate

A teammate owns only WorkItems assigned to it:

1. inspect exact state and allowed actions;
2. start, block, or resume assigned work;
3. send concise blocker or result context through the mailbox;
4. submit inline or exact Git evidence when ready;
5. end the turn when no action is available.

A teammate does not review, cancel, or reassign work unless the current query
explicitly permits that action.

## Dynamic wake context

Each wake adds:

- unread mailbox messages;
- a bounded summary of relevant non-terminal WorkItems;
- exact WorkItem identifiers, revisions, and allowed actions;
- a reminder to use `team_inspect` for full current state.

The summary is derived from `TeamQueryService`. If the canonical adapter is not
configured, the prompt omits the summary rather than inventing state.

## Tool surface

Role prompts name the currently discoverable tools from shared descriptors.
They emphasize three rules only:

- identity and team scope come from the authenticated runtime;
- mutations use stable idempotency keys and exact revisions;
- mailbox messages carry context, while canonical commands change work state.
