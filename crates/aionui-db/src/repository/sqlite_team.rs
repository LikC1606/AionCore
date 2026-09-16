use aionui_common::now_ms;
use sqlx::SqlitePool;

use crate::error::DbError;
use crate::models::{MailboxMessageRow, TeamRow};
use crate::repository::team::{
    ITeamRepository, MailboxIdempotencyParams, MailboxWriteResult, TeamCoordinationMigrationRow, UpdateTeamParams,
};

/// SQLite-backed implementation of [`ITeamRepository`].
#[derive(Clone, Debug)]
pub struct SqliteTeamRepository {
    pool: SqlitePool,
}

impl SqliteTeamRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl ITeamRepository for SqliteTeamRepository {
    // ── Team CRUD ────────────────────────────────────────────────────

    async fn create_team(&self, row: &TeamRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO teams (coordination_protocol, id, user_id, name, workspace, workspace_mode, agents, lead_agent_id, session_mode, agents_version, created_at, updated_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.coordination_protocol)
        .bind(&row.id)
        .bind(&row.user_id)
        .bind(&row.name)
        .bind(&row.workspace)
        .bind(&row.workspace_mode)
        .bind(&row.agents)
        .bind(&row.lead_agent_id)
        .bind(&row.session_mode)
        .bind(&row.agents_version)
        .bind(row.created_at)
        .bind(row.updated_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn list_teams(&self) -> Result<Vec<TeamRow>, DbError> {
        let rows = sqlx::query_as::<_, TeamRow>("SELECT * FROM teams ORDER BY created_at ASC")
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    async fn list_teams_by_user(&self, user_id: &str) -> Result<Vec<TeamRow>, DbError> {
        let rows = sqlx::query_as::<_, TeamRow>("SELECT * FROM teams WHERE user_id = ? ORDER BY created_at ASC")
            .bind(user_id)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows)
    }

    async fn get_team(&self, team_id: &str) -> Result<Option<TeamRow>, DbError> {
        let row = sqlx::query_as::<_, TeamRow>("SELECT * FROM teams WHERE id = ?")
            .bind(team_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn update_team(&self, team_id: &str, params: &UpdateTeamParams) -> Result<(), DbError> {
        let mut set_clauses = Vec::new();
        if params.name.is_some() {
            set_clauses.push("name = ?");
        }
        if params.workspace.is_some() {
            set_clauses.push("workspace = ?");
        }
        if params.agents.is_some() {
            set_clauses.push("agents = ?");
        }
        if params.lead_agent_id.is_some() {
            set_clauses.push("lead_agent_id = ?");
        }
        if params.session_mode.is_some() {
            set_clauses.push("session_mode = ?");
        }

        if set_clauses.is_empty() {
            return Ok(());
        }

        set_clauses.push("updated_at = ?");
        let sql = format!("UPDATE teams SET {} WHERE id = ?", set_clauses.join(", "));

        let mut query = sqlx::query(&sql);
        if let Some(ref name) = params.name {
            query = query.bind(name);
        }
        if let Some(ref workspace) = params.workspace {
            query = query.bind(workspace);
        }
        if let Some(ref agents) = params.agents {
            query = query.bind(agents);
        }
        if let Some(ref lead_agent_id) = params.lead_agent_id {
            query = query.bind(lead_agent_id);
        }
        if let Some(ref session_mode) = params.session_mode {
            query = query.bind(session_mode);
        }
        query = query.bind(now_ms());
        query = query.bind(team_id);

        let result = query.execute(&self.pool).await?;
        if result.rows_affected() == 0 {
            return Err(DbError::NotFound(format!("team {team_id}")));
        }
        Ok(())
    }

    async fn delete_team(&self, team_id: &str) -> Result<(), DbError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let exists = sqlx::query_scalar::<_, i64>("SELECT 1 FROM teams WHERE id = ?")
            .bind(team_id)
            .fetch_optional(&mut *transaction)
            .await?;
        if exists.is_none() {
            transaction.rollback().await?;
            return Err(DbError::NotFound(format!("team {team_id}")));
        }

        // Mailbox and the pre-kernel task table have no foreign key to teams.
        // Canonical WorkItems, Deliveries, Events, and integration attempts are
        // removed by the Team aggregate's ON DELETE CASCADE graph.
        sqlx::query("DELETE FROM mailbox WHERE team_id = ?")
            .bind(team_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query("DELETE FROM team_tasks WHERE team_id = ?")
            .bind(team_id)
            .execute(&mut *transaction)
            .await?;
        let result = sqlx::query("DELETE FROM teams WHERE id = ?")
            .bind(team_id)
            .execute(&mut *transaction)
            .await?;
        if result.rows_affected() != 1 {
            transaction.rollback().await?;
            return Err(DbError::NotFound(format!("team {team_id}")));
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn get_coordination_migration(&self, team_id: &str) -> Result<Option<TeamCoordinationMigrationRow>, DbError> {
        Ok(
            sqlx::query_as("SELECT * FROM team_coordination_migrations WHERE team_id = ?")
                .bind(team_id)
                .fetch_optional(&self.pool)
                .await?,
        )
    }

    async fn migrate_coordination_protocol(
        &self,
        expected: &TeamRow,
        audit: &TeamCoordinationMigrationRow,
    ) -> Result<TeamCoordinationMigrationRow, DbError> {
        const LEGACY: &str = r#"{"kind":"legacy_managed_migration_required"}"#;
        const MANAGED: &str = r#"{"kind":"managed_mcp","logicalTool":"research_team"}"#;
        if expected.id != audit.team_id || expected.user_id != audit.user_id {
            return Err(DbError::Conflict(
                "Team coordination migration identity mismatch".into(),
            ));
        }
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let current: TeamRow = sqlx::query_as("SELECT * FROM teams WHERE id = ?")
            .bind(&expected.id)
            .fetch_optional(&mut *transaction)
            .await?
            .ok_or_else(|| DbError::NotFound("Team coordination migration target not found".into()))?;
        let existing: Option<TeamCoordinationMigrationRow> =
            sqlx::query_as("SELECT * FROM team_coordination_migrations WHERE team_id = ?")
                .bind(&expected.id)
                .fetch_optional(&mut *transaction)
                .await?;
        if let Some(existing) = existing {
            if existing.user_id != audit.user_id
                || existing.migration_id != audit.migration_id
                || existing.proof_digest != audit.proof_digest
                || existing.proof_json != audit.proof_json
                || current.user_id != audit.user_id
                || current.coordination_protocol.as_deref() != Some(MANAGED)
            {
                return Err(DbError::Conflict("Team coordination migration replay conflict".into()));
            }
            transaction.commit().await?;
            return Ok(existing);
        }
        let snapshot = |row: &TeamRow| {
            serde_json::to_value(row)
                .map_err(|_| DbError::Init("Cannot encode Team coordination migration snapshot".into()))
        };
        if current.coordination_protocol.as_deref() != Some(LEGACY) || snapshot(&current)? != snapshot(expected)? {
            return Err(DbError::Conflict("Team coordination migration snapshot changed".into()));
        }
        let reused_id: Option<i64> =
            sqlx::query_scalar("SELECT 1 FROM team_coordination_migrations WHERE user_id = ? AND migration_id = ?")
                .bind(&audit.user_id)
                .bind(&audit.migration_id)
                .fetch_optional(&mut *transaction)
                .await?;
        if reused_id.is_some() {
            return Err(DbError::Conflict("Team coordination migration id already used".into()));
        }
        // Both writes share the transaction, so an interruption cannot leave an
        // upgraded Team without the authorization that justified the upgrade.
        sqlx::query("INSERT INTO team_coordination_migrations (team_id, user_id, migration_id, proof_digest, proof_json, receipt_json, applied_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
            .bind(&audit.team_id).bind(&audit.user_id).bind(&audit.migration_id)
            .bind(&audit.proof_digest).bind(&audit.proof_json).bind(&audit.receipt_json).bind(audit.applied_at)
            .execute(&mut *transaction).await?;
        sqlx::query("UPDATE teams SET coordination_protocol = ?, updated_at = ? WHERE id = ?")
            .bind(MANAGED)
            .bind(audit.applied_at)
            .bind(&expected.id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;
        Ok(audit.clone())
    }

    // ── Mailbox ──────────────────────────────────────────────────────

    async fn write_message(&self, row: &MailboxMessageRow) -> Result<(), DbError> {
        sqlx::query(
            "INSERT INTO mailbox \
                (id, team_id, to_agent_id, from_agent_id, type, content, summary, files, read, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&row.id)
        .bind(&row.team_id)
        .bind(&row.to_agent_id)
        .bind(&row.from_agent_id)
        .bind(&row.msg_type)
        .bind(&row.content)
        .bind(&row.summary)
        .bind(&row.files)
        .bind(row.read)
        .bind(row.created_at)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn write_message_idempotent(
        &self,
        row: &MailboxMessageRow,
        idempotency: &MailboxIdempotencyParams<'_>,
    ) -> Result<MailboxWriteResult, DbError> {
        if idempotency.scope.is_empty() || idempotency.key.is_empty() || idempotency.request_fingerprint.is_empty() {
            return Err(DbError::Conflict(
                "Mailbox idempotency scope, key, and request fingerprint must not be empty".into(),
            ));
        }

        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;
        let outcome = async {
            let existing = sqlx::query_as::<_, (String, String)>(
                "SELECT id, request_fingerprint FROM mailbox \
                 WHERE team_id = ? AND idempotency_scope = ? AND idempotency_key = ?",
            )
            .bind(&row.team_id)
            .bind(idempotency.scope)
            .bind(idempotency.key)
            .fetch_optional(&mut *transaction)
            .await?;

            if let Some((existing_id, existing_fingerprint)) = existing {
                if existing_fingerprint != idempotency.request_fingerprint {
                    return Ok(MailboxWriteResult::IdempotencyConflict {
                        existing_request_fingerprint: existing_fingerprint,
                    });
                }
                let existing_row = sqlx::query_as::<_, MailboxMessageRow>(
                    "SELECT id, team_id, to_agent_id, from_agent_id, \
                            type, content, summary, files, read, created_at \
                     FROM mailbox WHERE id = ?",
                )
                .bind(existing_id)
                .fetch_one(&mut *transaction)
                .await?;
                return Ok(MailboxWriteResult::Existing(existing_row));
            }

            sqlx::query(
                "INSERT INTO mailbox \
                    (id, team_id, to_agent_id, from_agent_id, type, content, summary, files, read, created_at, \
                     idempotency_scope, idempotency_key, request_fingerprint) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&row.id)
            .bind(&row.team_id)
            .bind(&row.to_agent_id)
            .bind(&row.from_agent_id)
            .bind(&row.msg_type)
            .bind(&row.content)
            .bind(&row.summary)
            .bind(&row.files)
            .bind(row.read)
            .bind(row.created_at)
            .bind(idempotency.scope)
            .bind(idempotency.key)
            .bind(idempotency.request_fingerprint)
            .execute(&mut *transaction)
            .await?;
            Ok(MailboxWriteResult::Inserted)
        }
        .await;

        match outcome {
            Ok(MailboxWriteResult::Inserted) => {
                transaction.commit().await?;
                Ok(MailboxWriteResult::Inserted)
            }
            Ok(outcome) => {
                transaction.rollback().await?;
                Ok(outcome)
            }
            Err(error) => {
                let _ = transaction.rollback().await;
                Err(error)
            }
        }
    }

    async fn read_unread_and_mark(&self, team_id: &str, to_agent_id: &str) -> Result<Vec<MailboxMessageRow>, DbError> {
        // Use BEGIN IMMEDIATE for atomicity: prevents concurrent readers
        // from seeing the same unread messages.
        let mut tx = self.pool.begin().await?;

        // SQLite does not support RETURNING on UPDATE, so we use a
        // two-step approach within the same IMMEDIATE transaction.
        sqlx::query("PRAGMA read_uncommitted = false").execute(&mut *tx).await?;

        let rows = sqlx::query_as::<_, MailboxMessageRow>(
            "SELECT id, team_id, to_agent_id, from_agent_id, \
                    type, content, summary, files, read, created_at \
             FROM mailbox \
             WHERE team_id = ? AND to_agent_id = ? AND read = 0 \
             ORDER BY created_at ASC",
        )
        .bind(team_id)
        .bind(to_agent_id)
        .fetch_all(&mut *tx)
        .await?;

        if !rows.is_empty() {
            sqlx::query(
                "UPDATE mailbox SET read = 1 \
                 WHERE team_id = ? AND to_agent_id = ? AND read = 0",
            )
            .bind(team_id)
            .bind(to_agent_id)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(rows)
    }

    async fn peek_unread(&self, team_id: &str, to_agent_id: &str) -> Result<Vec<MailboxMessageRow>, DbError> {
        let rows = sqlx::query_as::<_, MailboxMessageRow>(
            "SELECT id, team_id, to_agent_id, from_agent_id, \
                    type, content, summary, files, read, created_at \
             FROM mailbox \
             WHERE team_id = ? AND to_agent_id = ? AND read = 0 \
             ORDER BY created_at ASC",
        )
        .bind(team_id)
        .bind(to_agent_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn list_team_ids_with_recoverable_unread_mailbox(
        &self,
        after_team_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<String>, DbError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let rows = match after_team_id {
            Some(after_team_id) => {
                sqlx::query_scalar::<_, String>(
                    "SELECT DISTINCT m.team_id \
                     FROM mailbox AS m \
                     INNER JOIN teams AS t ON t.id = m.team_id \
                     WHERE m.read = 0 \
                       AND m.from_agent_id <> m.to_agent_id \
                       AND m.team_id > ? \
                     ORDER BY m.team_id ASC \
                     LIMIT ?",
                )
                .bind(after_team_id)
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_scalar::<_, String>(
                    "SELECT DISTINCT m.team_id \
                     FROM mailbox AS m \
                     INNER JOIN teams AS t ON t.id = m.team_id \
                     WHERE m.read = 0 \
                       AND m.from_agent_id <> m.to_agent_id \
                     ORDER BY m.team_id ASC \
                     LIMIT ?",
                )
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows)
    }

    async fn mark_read_batch(&self, ids: &[String]) -> Result<(), DbError> {
        if ids.is_empty() {
            return Ok(());
        }
        // SQLite placeholder limit is 999; batch if needed.
        for chunk in ids.chunks(500) {
            let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let sql = format!("UPDATE mailbox SET read = 1 WHERE id IN ({placeholders})");
            let mut query = sqlx::query(&sql);
            for id in chunk {
                query = query.bind(id);
            }
            query.execute(&self.pool).await?;
        }
        Ok(())
    }

    async fn get_history(
        &self,
        team_id: &str,
        to_agent_id: &str,
        limit: Option<i64>,
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        let rows = if let Some(limit) = limit {
            sqlx::query_as::<_, MailboxMessageRow>(
                "SELECT id, team_id, to_agent_id, from_agent_id, \
                        type, content, summary, files, read, created_at \
                 FROM mailbox \
                 WHERE team_id = ? AND to_agent_id = ? \
                 ORDER BY created_at ASC \
                 LIMIT ?",
            )
            .bind(team_id)
            .bind(to_agent_id)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as::<_, MailboxMessageRow>(
                "SELECT id, team_id, to_agent_id, from_agent_id, \
                        type, content, summary, files, read, created_at \
                 FROM mailbox \
                 WHERE team_id = ? AND to_agent_id = ? \
                 ORDER BY created_at ASC",
            )
            .bind(team_id)
            .bind(to_agent_id)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows)
    }

    async fn delete_mailbox_by_team(&self, team_id: &str) -> Result<(), DbError> {
        sqlx::query("DELETE FROM mailbox WHERE team_id = ?")
            .bind(team_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
