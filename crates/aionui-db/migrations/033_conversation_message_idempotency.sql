CREATE TABLE IF NOT EXISTS conversation_message_receipts (
    conversation_id     TEXT    NOT NULL,
    idempotency_key     TEXT    NOT NULL,
    request_fingerprint TEXT    NOT NULL,
    message_id          TEXT    NOT NULL,
    turn_id             TEXT    NOT NULL,
    created_at          INTEGER NOT NULL,
    PRIMARY KEY (conversation_id, idempotency_key),
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE,
    FOREIGN KEY (message_id) REFERENCES messages(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_conversation_message_receipts_message
    ON conversation_message_receipts(message_id);
