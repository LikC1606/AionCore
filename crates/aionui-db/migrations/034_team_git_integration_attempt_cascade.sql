-- Team deletion owns the complete Team Mode aggregate. Integration attempts
-- must follow their immutable delivery when the aggregate is removed.

CREATE TABLE team_git_integration_attempts_new (
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
        REFERENCES team_git_deliveries(team_id, work_item_id, id) ON DELETE CASCADE,
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

INSERT INTO team_git_integration_attempts_new (
    attempt_id, team_id, work_item_id, delivery_id, repository_id,
    base_commit, source_ref, source_head, target_ref, target_head,
    state, merged_commit, observed_target_head, recovery_reason,
    created_at, updated_at
)
SELECT
    attempt_id, team_id, work_item_id, delivery_id, repository_id,
    base_commit, source_ref, source_head, target_ref, target_head,
    state, merged_commit, observed_target_head, recovery_reason,
    created_at, updated_at
FROM team_git_integration_attempts;

DROP TABLE team_git_integration_attempts;
ALTER TABLE team_git_integration_attempts_new RENAME TO team_git_integration_attempts;

CREATE UNIQUE INDEX idx_team_git_integration_one_pending
    ON team_git_integration_attempts(team_id, delivery_id)
    WHERE state = 'pending';

CREATE INDEX idx_team_git_integration_pending_team
    ON team_git_integration_attempts(team_id, created_at ASC, attempt_id ASC)
    WHERE state = 'pending';
