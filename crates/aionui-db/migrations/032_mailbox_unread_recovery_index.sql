CREATE INDEX IF NOT EXISTS idx_mailbox_recoverable_unread_team
ON mailbox(team_id)
WHERE read = 0 AND from_agent_id <> to_agent_id;
