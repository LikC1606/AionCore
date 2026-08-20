ALTER TABLE mailbox ADD COLUMN idempotency_scope TEXT;
ALTER TABLE mailbox ADD COLUMN idempotency_key TEXT;
ALTER TABLE mailbox ADD COLUMN request_fingerprint TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mailbox_idempotency
    ON mailbox(team_id, idempotency_scope, idempotency_key)
    WHERE idempotency_scope IS NOT NULL AND idempotency_key IS NOT NULL;
