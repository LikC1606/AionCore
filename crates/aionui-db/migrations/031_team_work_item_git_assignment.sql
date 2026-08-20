-- Immutable Git coordinates assigned when a WorkItem is delegated.
--
-- SQLite treats this column-level CHECK as a row CHECK. Keeping it on the
-- final added column lets the migration extend the existing table without a
-- destructive rebuild while still enforcing the all-or-none invariant.

ALTER TABLE team_work_items ADD COLUMN git_repository_id TEXT;
ALTER TABLE team_work_items ADD COLUMN git_base_commit TEXT;
ALTER TABLE team_work_items ADD COLUMN git_branch_ref TEXT
    CHECK (
        (
            delivery_requirement = 'none'
            AND git_repository_id IS NULL
            AND git_base_commit IS NULL
            AND git_branch_ref IS NULL
        )
        OR (
            delivery_requirement = 'git'
            AND git_repository_id IS NOT NULL AND length(trim(git_repository_id)) > 0
            AND git_base_commit IS NOT NULL AND length(trim(git_base_commit)) > 0
            AND git_branch_ref IS NOT NULL AND length(trim(git_branch_ref)) > 0
        )
    );
