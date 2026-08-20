-- Durable, exact intent for Team Mode Git delivery integration.
--
-- An attempt is inserted in the same transaction that moves its delivery to
-- `integrating`, then resolved in the same transaction as the resulting Team
-- command. The immutable coordinates make an interrupted Git side effect
-- safely reconcilable after process restart.

CREATE TABLE IF NOT EXISTS team_git_integration_attempts (
    attempt_id            TEXT    PRIMARY KEY NOT NULL,
    team_id               TEXT    NOT NULL,
    work_item_id          TEXT    NOT NULL,
    delivery_id           TEXT    NOT NULL,
    repository_id         TEXT    NOT NULL,
    base_commit           TEXT    NOT NULL,
    source_ref            TEXT    NOT NULL,
    source_head           TEXT    NOT NULL,
    target_ref            TEXT    NOT NULL,
    target_head           TEXT    NOT NULL,
    state                 TEXT    NOT NULL
                                  CHECK (state IN ('pending', 'merged', 'conflicted', 'retryable')),
    merged_commit         TEXT,
    observed_target_head  TEXT,
    recovery_reason       TEXT
                                  CHECK (recovery_reason IS NULL OR recovery_reason IN (
                                      'interrupted', 'retryable_infrastructure', 'precondition_changed'
                                  )),
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL,
    UNIQUE (team_id, attempt_id),
    FOREIGN KEY (team_id, work_item_id, delivery_id)
        REFERENCES team_git_deliveries(team_id, work_item_id, id),
    CHECK (
        (state = 'pending' AND merged_commit IS NULL AND recovery_reason IS NULL)
        OR (
            state = 'merged' AND length(merged_commit) > 0
            AND length(observed_target_head) > 0 AND recovery_reason IS NULL
        )
        OR (
            state = 'conflicted' AND merged_commit IS NULL
            AND length(observed_target_head) > 0 AND recovery_reason IS NULL
        )
        OR (
            state = 'retryable' AND merged_commit IS NULL AND recovery_reason IS NOT NULL
            AND (
                recovery_reason <> 'precondition_changed'
                OR length(observed_target_head) > 0
            )
        )
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_team_git_integration_one_pending
    ON team_git_integration_attempts(team_id, delivery_id)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS idx_team_git_integration_pending_team
    ON team_git_integration_attempts(team_id, created_at ASC, attempt_id ASC)
    WHERE state = 'pending';
