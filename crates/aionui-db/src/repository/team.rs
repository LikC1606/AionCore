use crate::error::DbError;
use crate::models::{MailboxMessageRow, TeamRow};

/// Parameters for updating a team record.
#[derive(Debug, Clone, Default)]
pub struct UpdateTeamParams {
    pub name: Option<String>,
    pub workspace: Option<String>,
    pub agents: Option<String>,
    pub lead_agent_id: Option<String>,
    pub session_mode: Option<String>,
}

/// Atomically persisted authorization and outcome for the legacy protocol upgrade.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TeamCoordinationMigrationRow {
    pub team_id: String,
    pub user_id: String,
    pub migration_id: String,
    pub proof_digest: String,
    pub proof_json: String,
    pub receipt_json: String,
    pub applied_at: aionui_common::TimestampMs,
}

/// Durable identity for a caller-retryable mailbox write.
#[derive(Debug, Clone, Copy)]
pub struct MailboxIdempotencyParams<'a> {
    pub scope: &'a str,
    pub key: &'a str,
    pub request_fingerprint: &'a str,
}

/// Result of an atomic idempotent mailbox insert.
#[derive(Debug, Clone)]
pub enum MailboxWriteResult {
    Inserted,
    Existing(MailboxMessageRow),
    IdempotencyConflict { existing_request_fingerprint: String },
}

/// Data access abstraction for team collaboration tables.
///
/// Covers the active `teams` and `mailbox` collaboration tables.
///
/// Object-safe via `async_trait` to support `Arc<dyn ITeamRepository>`.
#[async_trait::async_trait]
pub trait ITeamRepository: Send + Sync {
    // ── Team CRUD ────────────────────────────────────────────────────

    /// Inserts a new team record.
    async fn create_team(&self, row: &TeamRow) -> Result<(), DbError>;

    /// Returns all teams ordered by creation time ascending.
    async fn list_teams(&self) -> Result<Vec<TeamRow>, DbError>;

    /// Returns teams owned by `user_id`, ordered by creation time ascending.
    async fn list_teams_by_user(&self, user_id: &str) -> Result<Vec<TeamRow>, DbError>;

    /// Returns a single team by id, or `None` if not found.
    async fn get_team(&self, team_id: &str) -> Result<Option<TeamRow>, DbError>;

    /// Updates a team by id with the provided fields.
    /// Returns `DbError::NotFound` if absent.
    async fn update_team(&self, team_id: &str, params: &UpdateTeamParams) -> Result<(), DbError>;

    async fn get_coordination_migration(
        &self,
        _team_id: &str,
    ) -> Result<Option<TeamCoordinationMigrationRow>, DbError> {
        Err(DbError::Init(
            "Team coordination migration storage is unavailable".into(),
        ))
    }

    /// Compare the complete expected snapshot, then commit protocol and audit together.
    /// Implementations must never emulate this with separate update/insert calls.
    async fn migrate_coordination_protocol(
        &self,
        _expected: &TeamRow,
        _audit: &TeamCoordinationMigrationRow,
    ) -> Result<TeamCoordinationMigrationRow, DbError> {
        Err(DbError::Init(
            "Team coordination migration storage is unavailable".into(),
        ))
    }

    /// Atomically deletes a Team and all Team-owned mailbox and work state.
    /// Returns `DbError::NotFound` if absent.
    async fn delete_team(&self, team_id: &str) -> Result<(), DbError>;

    // ── Mailbox ──────────────────────────────────────────────────────

    /// Writes a message to the mailbox.
    async fn write_message(&self, row: &MailboxMessageRow) -> Result<(), DbError>;

    /// Atomically inserts a keyed mailbox message or returns the row previously
    /// inserted for the same `(team_id, scope, key)` identity.
    async fn write_message_idempotent(
        &self,
        row: &MailboxMessageRow,
        idempotency: &MailboxIdempotencyParams<'_>,
    ) -> Result<MailboxWriteResult, DbError>;

    /// Atomically reads all unread messages for `to_agent_id` in a team
    /// and marks them as read. Uses `BEGIN IMMEDIATE` for atomicity.
    async fn read_unread_and_mark(&self, team_id: &str, to_agent_id: &str) -> Result<Vec<MailboxMessageRow>, DbError>;

    /// Reads all unread messages for `to_agent_id` without marking them as read.
    async fn peek_unread(&self, team_id: &str, to_agent_id: &str) -> Result<Vec<MailboxMessageRow>, DbError>;

    /// Returns Team ids with recoverable unread mailbox demand, ordered by id.
    /// Self-addressed rows and mailbox rows for deleted Teams are excluded.
    async fn list_team_ids_with_recoverable_unread_mailbox(
        &self,
        after_team_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<String>, DbError>;

    /// Marks the given message IDs as read. IDs that don't exist are silently ignored.
    async fn mark_read_batch(&self, ids: &[String]) -> Result<(), DbError>;

    /// Returns message history for an agent, optionally limited.
    /// Messages are ordered by `created_at` ascending.
    async fn get_history(
        &self,
        team_id: &str,
        to_agent_id: &str,
        limit: Option<i64>,
    ) -> Result<Vec<MailboxMessageRow>, DbError>;

    /// Deletes all mailbox messages belonging to a team.
    async fn delete_mailbox_by_team(&self, team_id: &str) -> Result<(), DbError>;
}
