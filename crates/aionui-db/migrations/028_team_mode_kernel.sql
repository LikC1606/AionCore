-- Team Mode v2 durable kernel state.
--
-- These tables intentionally live beside the legacy team_tasks/mailbox tables
-- while the command path is migrated. They are the canonical persistence
-- boundary for versioned WorkItems, immutable Git delivery identities, and
-- idempotent command receipts.

CREATE TABLE IF NOT EXISTS team_work_items (
    id                         TEXT    PRIMARY KEY NOT NULL,
    team_id                    TEXT    NOT NULL,
    parent_work_item_id        TEXT,
    subject                    TEXT    NOT NULL,
    description                TEXT,
    controller_member_id       TEXT    NOT NULL,
    assignee_member_id         TEXT    NOT NULL,
    reviewer_member_id         TEXT    NOT NULL,
    integrator_member_id       TEXT,
    delivery_requirement       TEXT    NOT NULL
                                      CHECK (delivery_requirement IN ('none', 'git')),
    state                      TEXT    NOT NULL
                                      CHECK (state IN (
                                          'draft', 'queued', 'running', 'blocked',
                                          'submitted', 'reviewing', 'changes_requested',
                                          'accepted', 'completed', 'rejected', 'failed',
                                          'cancelled'
                                      )),
    current_submission_json    TEXT,
    accepted_delivery_json     TEXT,
    revision                   INTEGER NOT NULL CHECK (revision >= 0),
    created_at                 INTEGER NOT NULL,
    updated_at                 INTEGER NOT NULL,
    UNIQUE (team_id, id),
    FOREIGN KEY (team_id) REFERENCES teams(id) ON DELETE CASCADE,
    FOREIGN KEY (team_id, parent_work_item_id)
        REFERENCES team_work_items(team_id, id)
);
CREATE INDEX IF NOT EXISTS idx_team_work_items_team_updated
    ON team_work_items(team_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_team_work_items_parent
    ON team_work_items(team_id, parent_work_item_id);

CREATE TABLE IF NOT EXISTS team_git_deliveries (
    id                    TEXT    PRIMARY KEY NOT NULL,
    team_id               TEXT    NOT NULL,
    work_item_id          TEXT    NOT NULL,
    producer_member_id    TEXT    NOT NULL,
    repository_id         TEXT    NOT NULL,
    content_revision      INTEGER NOT NULL CHECK (content_revision >= 1),
    base_commit           TEXT    NOT NULL,
    branch_ref            TEXT    NOT NULL,
    head_commit           TEXT    NOT NULL,
    state                 TEXT    NOT NULL
                                  CHECK (state IN (
                                      'submitted', 'accepted', 'integrating',
                                      'conflicted', 'merged', 'superseded',
                                      'rejected', 'abandoned'
                                  )),
    revision              INTEGER NOT NULL CHECK (revision >= 0),
    merged_commit         TEXT,
    created_at            INTEGER NOT NULL,
    updated_at            INTEGER NOT NULL,
    UNIQUE (team_id, id),
    UNIQUE (team_id, work_item_id, id),
    UNIQUE (team_id, work_item_id, content_revision),
    FOREIGN KEY (team_id, work_item_id)
        REFERENCES team_work_items(team_id, id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_team_git_deliveries_work_item_updated
    ON team_git_deliveries(team_id, work_item_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS team_work_events (
    sequence                    INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id                    TEXT    NOT NULL UNIQUE,
    team_id                     TEXT    NOT NULL,
    work_item_id                TEXT    NOT NULL,
    delivery_id                 TEXT,
    actor_member_id             TEXT    NOT NULL,
    command_name                TEXT    NOT NULL,
    idempotency_key             TEXT    NOT NULL,
    request_fingerprint         TEXT    NOT NULL,
    result_json                 TEXT    NOT NULL,
    expected_work_item_revision INTEGER CHECK (expected_work_item_revision >= 0),
    expected_delivery_revision  INTEGER CHECK (expected_delivery_revision >= 0),
    work_item_revision          INTEGER NOT NULL CHECK (work_item_revision >= 0),
    delivery_revision           INTEGER CHECK (delivery_revision >= 0),
    created_at                  INTEGER NOT NULL,
    UNIQUE (team_id, actor_member_id, idempotency_key),
    CHECK (
        (delivery_id IS NULL AND delivery_revision IS NULL)
        OR (delivery_id IS NOT NULL AND delivery_revision IS NOT NULL)
    ),
    CHECK (expected_delivery_revision IS NULL OR delivery_id IS NOT NULL),
    FOREIGN KEY (team_id, work_item_id)
        REFERENCES team_work_items(team_id, id) ON DELETE CASCADE,
    FOREIGN KEY (team_id, work_item_id, delivery_id)
        REFERENCES team_git_deliveries(team_id, work_item_id, id)
);
CREATE INDEX IF NOT EXISTS idx_team_work_events_work_item_sequence
    ON team_work_events(team_id, work_item_id, sequence);
