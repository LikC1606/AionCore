CREATE TABLE IF NOT EXISTS team_coordination_migrations (
    team_id TEXT PRIMARY KEY NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL,
    migration_id TEXT NOT NULL,
    proof_digest TEXT NOT NULL,
    proof_json TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    applied_at INTEGER NOT NULL,
    UNIQUE (user_id, migration_id)
);
