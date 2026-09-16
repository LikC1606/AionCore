-- SQLx applies this additive migration once, inside its migration transaction.
ALTER TABLE teams ADD COLUMN coordination_protocol TEXT;

-- Old managed mathematics sessions need an explicit, audited binding migration.
-- An unsupported protocol is intentional: readers fail closed rather than
-- silently exposing native coordination after an upgrade.
UPDATE teams
SET coordination_protocol = '{"kind":"legacy_managed_migration_required"}'
WHERE EXISTS (
    SELECT 1
    FROM json_each(CASE WHEN json_valid(teams.agents) THEN teams.agents ELSE '[]' END) AS agent
    JOIN conversations AS conversation
      ON conversation.id = json_extract(CASE WHEN json_valid(agent.value) THEN agent.value ELSE '{}' END, '$.conversation_id')
      AND conversation.user_id = teams.user_id
    WHERE json_extract(
        CASE WHEN json_valid(conversation.extra) THEN conversation.extra ELSE '{}' END,
        '$.research_team_profile_id'
    ) = 'mathematics-research-team'
);
