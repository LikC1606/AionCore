use sqlx::{SqliteConnection, SqlitePool};

use crate::error::DbError;
use crate::models::{TeamGitDeliveryRow, TeamGitIntegrationAttemptRow, TeamWorkEventRow, TeamWorkItemRow};
use crate::repository::team_mode::{
    CommitTeamCommandParams, ITeamModeRepository, NewTeamMailboxNotification, NewTeamWorkEvent,
    TEAM_WORK_EVENT_NOTIFICATION_SCOPE, TeamCommandCommitResult, TeamCommandReceiptLookupResult,
    TeamGitDeliveryMutation, TeamGitIntegrationMutation, TeamGitIntegrationResolution, TeamRosterGuard,
    TeamWorkItemMutation,
};

/// SQLite-backed Team Mode v2 command repository.
#[derive(Clone, Debug)]
pub struct SqliteTeamModeRepository {
    pool: SqlitePool,
}

impl SqliteTeamModeRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn commit_on_connection(
        connection: &mut SqliteConnection,
        params: &CommitTeamCommandParams,
    ) -> Result<TeamCommandCommitResult, DbError> {
        validate_roster_guard(params.roster_guard.as_ref())?;
        if let Some(guard) = params.roster_guard.as_ref()
            && !team_roster_matches(connection, &params.event.team_id, guard).await?
        {
            return Ok(TeamCommandCommitResult::TeamRosterConflict {
                team_id: params.event.team_id.clone(),
            });
        }

        if let Some(existing) = find_receipt_on_connection(
            connection,
            &params.event.team_id,
            &params.event.actor_member_id,
            &params.event.idempotency_key,
        )
        .await?
        {
            return Ok(if existing.request_fingerprint == params.event.request_fingerprint {
                TeamCommandCommitResult::Replayed(existing)
            } else {
                TeamCommandCommitResult::IdempotencyConflict {
                    existing_request_fingerprint: existing.request_fingerprint,
                }
            });
        }

        validate_commit_params(params)?;

        for guard in &params.work_item_guards {
            let actual_revision =
                find_work_item_revision(connection, &params.event.team_id, &guard.work_item_id).await?;
            if actual_revision != Some(guard.expected_revision) {
                return Ok(TeamCommandCommitResult::WorkItemGuardConflict {
                    work_item_id: guard.work_item_id.clone(),
                    expected_revision: guard.expected_revision,
                    actual_revision,
                });
            }
        }

        if params.work_item.is_none()
            && let Some(expected_revision) = params.event.expected_work_item_revision
        {
            let actual_revision =
                find_work_item_revision(connection, &params.event.team_id, &params.event.work_item_id).await?;
            if actual_revision != Some(expected_revision) {
                return Ok(TeamCommandCommitResult::WorkItemRevisionConflict {
                    expected_revision,
                    actual_revision,
                });
            }
        }

        if params.delivery.is_none()
            && let Some(expected_revision) = params.event.expected_delivery_revision
        {
            let delivery_id = params
                .event
                .delivery_id
                .as_deref()
                .expect("validated expected delivery revision requires an ID");
            let Some(delivery) =
                find_git_delivery_on_connection(connection, &params.event.team_id, delivery_id).await?
            else {
                return Ok(TeamCommandCommitResult::DeliveryRevisionConflict {
                    expected_revision,
                    actual_revision: None,
                });
            };
            if delivery.work_item_id != params.event.work_item_id {
                return Err(DbError::Conflict(
                    "Expected delivery does not belong to the command WorkItem".into(),
                ));
            }
            if delivery.revision != expected_revision {
                return Ok(TeamCommandCommitResult::DeliveryRevisionConflict {
                    expected_revision,
                    actual_revision: Some(delivery.revision),
                });
            }
        }

        if let Some(mutation) = params.work_item.as_ref() {
            match mutation {
                TeamWorkItemMutation::Insert(row) => insert_work_item(connection, row).await?,
                TeamWorkItemMutation::CompareAndSwap { expected_revision, row } => {
                    let Some(existing) = find_work_item_on_connection(connection, &row.team_id, &row.id).await? else {
                        return Ok(TeamCommandCommitResult::WorkItemRevisionConflict {
                            expected_revision: *expected_revision,
                            actual_revision: None,
                        });
                    };
                    ensure_same_work_item_assignment(&existing, row)?;
                    if existing.revision != *expected_revision {
                        return Ok(TeamCommandCommitResult::WorkItemRevisionConflict {
                            expected_revision: *expected_revision,
                            actual_revision: Some(existing.revision),
                        });
                    }
                    let updated = update_work_item_cas(connection, *expected_revision, row).await?;
                    if !updated {
                        let actual_revision = find_work_item_revision(connection, &row.team_id, &row.id).await?;
                        return Ok(TeamCommandCommitResult::WorkItemRevisionConflict {
                            expected_revision: *expected_revision,
                            actual_revision,
                        });
                    }
                }
            }
        }

        if let Some(mutation) = params.delivery.as_ref() {
            match mutation {
                TeamGitDeliveryMutation::Insert(row) => insert_git_delivery(connection, row).await?,
                TeamGitDeliveryMutation::CompareAndSwap { expected_revision, row } => {
                    let Some(existing) = find_git_delivery_on_connection(connection, &row.team_id, &row.id).await?
                    else {
                        return Ok(TeamCommandCommitResult::DeliveryRevisionConflict {
                            expected_revision: *expected_revision,
                            actual_revision: None,
                        });
                    };
                    ensure_same_delivery_identity(&existing, row)?;
                    ensure_integration_transition_is_coupled(&existing, row, false)?;
                    if existing.revision != *expected_revision {
                        return Ok(TeamCommandCommitResult::DeliveryRevisionConflict {
                            expected_revision: *expected_revision,
                            actual_revision: Some(existing.revision),
                        });
                    }
                    let updated = update_git_delivery_cas(connection, *expected_revision, row).await?;
                    if !updated {
                        let actual_revision = find_git_delivery_revision(connection, &row.team_id, &row.id).await?;
                        return Ok(TeamCommandCommitResult::DeliveryRevisionConflict {
                            expected_revision: *expected_revision,
                            actual_revision,
                        });
                    }
                }
                TeamGitDeliveryMutation::CompareAndSwapWithIntegration {
                    expected_revision,
                    row,
                    integration,
                } => {
                    let Some(existing) = find_git_delivery_on_connection(connection, &row.team_id, &row.id).await?
                    else {
                        return Ok(TeamCommandCommitResult::DeliveryRevisionConflict {
                            expected_revision: *expected_revision,
                            actual_revision: None,
                        });
                    };
                    ensure_same_delivery_identity(&existing, row)?;
                    ensure_integration_transition_is_coupled(&existing, row, true)?;
                    if existing.revision != *expected_revision {
                        return Ok(TeamCommandCommitResult::DeliveryRevisionConflict {
                            expected_revision: *expected_revision,
                            actual_revision: Some(existing.revision),
                        });
                    }
                    let updated = update_git_delivery_cas(connection, *expected_revision, row).await?;
                    if !updated {
                        let actual_revision = find_git_delivery_revision(connection, &row.team_id, &row.id).await?;
                        return Ok(TeamCommandCommitResult::DeliveryRevisionConflict {
                            expected_revision: *expected_revision,
                            actual_revision,
                        });
                    }
                    apply_integration_mutation(connection, row, integration).await?;
                }
            }
        }

        validate_event_post_state(connection, &params.event).await?;

        let receipt = insert_event(connection, &params.event).await?;
        insert_notifications(connection, &params.event.team_id, &params.notifications).await?;
        Ok(TeamCommandCommitResult::Applied(receipt))
    }
}

#[async_trait::async_trait]
impl ITeamModeRepository for SqliteTeamModeRepository {
    async fn list_work_items(&self, team_id: &str) -> Result<Vec<TeamWorkItemRow>, DbError> {
        let rows = sqlx::query_as::<_, TeamWorkItemRow>(
            "SELECT * FROM team_work_items WHERE team_id = ? ORDER BY created_at ASC, id ASC",
        )
        .bind(team_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn get_work_item(&self, team_id: &str, work_item_id: &str) -> Result<Option<TeamWorkItemRow>, DbError> {
        let row = sqlx::query_as::<_, TeamWorkItemRow>("SELECT * FROM team_work_items WHERE team_id = ? AND id = ?")
            .bind(team_id)
            .bind(work_item_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row)
    }

    async fn list_git_deliveries(
        &self,
        team_id: &str,
        work_item_id: Option<&str>,
    ) -> Result<Vec<TeamGitDeliveryRow>, DbError> {
        let rows = match work_item_id {
            Some(work_item_id) => {
                sqlx::query_as::<_, TeamGitDeliveryRow>(
                    "SELECT * FROM team_git_deliveries \
                 WHERE team_id = ? AND work_item_id = ? \
                 ORDER BY updated_at DESC, id ASC",
                )
                .bind(team_id)
                .bind(work_item_id)
                .fetch_all(&self.pool)
                .await?
            }
            None => {
                sqlx::query_as::<_, TeamGitDeliveryRow>(
                    "SELECT * FROM team_git_deliveries \
                 WHERE team_id = ? ORDER BY updated_at DESC, id ASC",
                )
                .bind(team_id)
                .fetch_all(&self.pool)
                .await?
            }
        };
        Ok(rows)
    }

    async fn get_git_delivery(&self, team_id: &str, delivery_id: &str) -> Result<Option<TeamGitDeliveryRow>, DbError> {
        let row =
            sqlx::query_as::<_, TeamGitDeliveryRow>("SELECT * FROM team_git_deliveries WHERE team_id = ? AND id = ?")
                .bind(team_id)
                .bind(delivery_id)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row)
    }

    async fn get_git_integration_attempt(
        &self,
        team_id: &str,
        attempt_id: &str,
    ) -> Result<Option<TeamGitIntegrationAttemptRow>, DbError> {
        let row = sqlx::query_as::<_, TeamGitIntegrationAttemptRow>(
            "SELECT * FROM team_git_integration_attempts WHERE team_id = ? AND attempt_id = ?",
        )
        .bind(team_id)
        .bind(attempt_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn get_pending_git_integration_attempt(
        &self,
        team_id: &str,
        delivery_id: &str,
    ) -> Result<Option<TeamGitIntegrationAttemptRow>, DbError> {
        let row = sqlx::query_as::<_, TeamGitIntegrationAttemptRow>(
            "SELECT * FROM team_git_integration_attempts \
             WHERE team_id = ? AND delivery_id = ? AND state = 'pending'",
        )
        .bind(team_id)
        .bind(delivery_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn list_pending_git_integration_attempts(
        &self,
        team_id: &str,
    ) -> Result<Vec<TeamGitIntegrationAttemptRow>, DbError> {
        let rows = sqlx::query_as::<_, TeamGitIntegrationAttemptRow>(
            "SELECT * FROM team_git_integration_attempts \
             WHERE team_id = ? AND state = 'pending' ORDER BY created_at ASC, attempt_id ASC",
        )
        .bind(team_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn find_git_delivery(
        &self,
        team_id: &str,
        work_item_id: &str,
        content_revision: i64,
    ) -> Result<Option<TeamGitDeliveryRow>, DbError> {
        let row = sqlx::query_as::<_, TeamGitDeliveryRow>(
            "SELECT * FROM team_git_deliveries \
             WHERE team_id = ? AND work_item_id = ? AND content_revision = ?",
        )
        .bind(team_id)
        .bind(work_item_id)
        .bind(content_revision)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn find_command_receipt(
        &self,
        team_id: &str,
        actor_member_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<TeamWorkEventRow>, DbError> {
        let row = sqlx::query_as::<_, TeamWorkEventRow>(
            "SELECT * FROM team_work_events \
             WHERE team_id = ? AND actor_member_id = ? AND idempotency_key = ?",
        )
        .bind(team_id)
        .bind(actor_member_id)
        .bind(idempotency_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    async fn find_command_receipt_guarded(
        &self,
        team_id: &str,
        actor_member_id: &str,
        idempotency_key: &str,
        roster_guard: &TeamRosterGuard,
    ) -> Result<TeamCommandReceiptLookupResult, DbError> {
        validate_roster_guard(Some(roster_guard))?;
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let result = async {
            if !team_roster_matches(&mut transaction, team_id, roster_guard).await? {
                return Ok(TeamCommandReceiptLookupResult::TeamRosterConflict {
                    team_id: team_id.to_owned(),
                });
            }
            Ok(
                match find_receipt_on_connection(&mut transaction, team_id, actor_member_id, idempotency_key).await? {
                    Some(receipt) => TeamCommandReceiptLookupResult::Found(Box::new(receipt)),
                    None => TeamCommandReceiptLookupResult::NotFound,
                },
            )
        }
        .await;

        match result {
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

    async fn list_work_events(&self, team_id: &str, work_item_id: &str) -> Result<Vec<TeamWorkEventRow>, DbError> {
        let rows = sqlx::query_as::<_, TeamWorkEventRow>(
            "SELECT * FROM team_work_events \
             WHERE team_id = ? AND work_item_id = ? ORDER BY sequence ASC",
        )
        .bind(team_id)
        .bind(work_item_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    async fn commit_command(&self, params: &CommitTeamCommandParams) -> Result<TeamCommandCommitResult, DbError> {
        let mut transaction = self.pool.begin_with("BEGIN IMMEDIATE").await?;

        let result = Self::commit_on_connection(&mut transaction, params).await;
        match result {
            Ok(TeamCommandCommitResult::Applied(receipt)) => {
                transaction.commit().await?;
                Ok(TeamCommandCommitResult::Applied(receipt))
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
}

fn validate_commit_params(params: &CommitTeamCommandParams) -> Result<(), DbError> {
    let event = &params.event;
    if event.event_id.is_empty()
        || event.team_id.is_empty()
        || event.work_item_id.is_empty()
        || event.actor_member_id.is_empty()
        || event.command_name.is_empty()
        || event.idempotency_key.is_empty()
        || event.request_fingerprint.is_empty()
    {
        return Err(DbError::Conflict(
            "Team command identity fields must not be empty".into(),
        ));
    }
    if params.work_item.is_none() && params.delivery.is_none() {
        return Err(DbError::Conflict(
            "Team command must mutate a WorkItem or Git delivery".into(),
        ));
    }
    if event.delivery_id.is_some() != event.delivery_revision.is_some() {
        return Err(DbError::Conflict(
            "Team command event delivery ID and revision must be present together".into(),
        ));
    }
    if event.expected_delivery_revision.is_some() && event.delivery_id.is_none() {
        return Err(DbError::Conflict(
            "Expected delivery revision requires a delivery ID".into(),
        ));
    }
    if event.expected_work_item_revision.is_some_and(|revision| revision < 0)
        || event.expected_delivery_revision.is_some_and(|revision| revision < 0)
        || event.work_item_revision < 0
        || event.delivery_revision.is_some_and(|revision| revision < 0)
    {
        return Err(DbError::Conflict(
            "Team command event revisions must be non-negative".into(),
        ));
    }

    if params
        .work_item_guards
        .iter()
        .any(|guard| guard.work_item_id.is_empty() || guard.expected_revision < 0)
    {
        return Err(DbError::Conflict(
            "WorkItem revision guards require a non-empty ID and non-negative revision".into(),
        ));
    }

    validate_notifications(&params.notifications)?;

    if let Some(mutation) = params.work_item.as_ref() {
        let row = mutation.row();
        ensure_scope("WorkItem", &event.team_id, &event.work_item_id, &row.team_id, &row.id)?;
        validate_work_item_mutation(mutation)?;
        let expected_revision = match mutation {
            TeamWorkItemMutation::Insert(_) => None,
            TeamWorkItemMutation::CompareAndSwap { expected_revision, .. } => Some(*expected_revision),
        };
        if event.expected_work_item_revision != expected_revision {
            return Err(DbError::Conflict(
                "Event expected WorkItem revision must match the mutation pre-state".into(),
            ));
        }
        if event.work_item_revision != row.revision {
            return Err(DbError::Conflict(
                "Event WorkItem revision must match the committed snapshot".into(),
            ));
        }
    }

    if let Some(mutation) = params.delivery.as_ref() {
        let row = mutation.row();
        ensure_scope(
            "Git delivery",
            &event.team_id,
            &event.work_item_id,
            &row.team_id,
            &row.work_item_id,
        )?;
        if event.delivery_id.as_deref() != Some(row.id.as_str()) {
            return Err(DbError::Conflict(
                "Event delivery ID must match the committed delivery snapshot".into(),
            ));
        }
        validate_git_delivery_mutation(mutation)?;
        let expected_revision = mutation.expected_revision();
        if event.expected_delivery_revision != expected_revision {
            return Err(DbError::Conflict(
                "Event expected delivery revision must match the mutation pre-state".into(),
            ));
        }
        if event.delivery_revision != Some(row.revision) {
            return Err(DbError::Conflict(
                "Event delivery revision must match the committed snapshot".into(),
            ));
        }
    }

    Ok(())
}

fn validate_notifications(notifications: &[NewTeamMailboxNotification]) -> Result<(), DbError> {
    let mut message_ids = std::collections::HashSet::with_capacity(notifications.len());
    let mut idempotency_keys = std::collections::HashSet::with_capacity(notifications.len());

    for notification in notifications {
        if notification.message_id.trim().is_empty()
            || notification.to_agent_id.trim().is_empty()
            || notification.from_agent_id.trim().is_empty()
            || notification.content.trim().is_empty()
            || notification.idempotency_key.trim().is_empty()
            || notification.request_fingerprint.trim().is_empty()
        {
            return Err(DbError::Conflict(
                "Team command notification identity and content fields must not be empty".into(),
            ));
        }
        if notification.idempotency_scope != TEAM_WORK_EVENT_NOTIFICATION_SCOPE {
            return Err(DbError::Conflict(format!(
                "Team command notification idempotency scope must be {TEAM_WORK_EVENT_NOTIFICATION_SCOPE}"
            )));
        }
        if notification.to_agent_id == notification.from_agent_id {
            return Err(DbError::Conflict(
                "Team command notification sender and recipient must differ".into(),
            ));
        }
        if notification.created_at < 0 {
            return Err(DbError::Conflict(
                "Team command notification timestamp must be non-negative".into(),
            ));
        }
        if notification
            .summary
            .as_deref()
            .is_some_and(|summary| summary.trim().is_empty())
        {
            return Err(DbError::Conflict(
                "Team command notification summary must not be empty when present".into(),
            ));
        }
        if let Some(files_json) = notification.files_json.as_deref() {
            let files: Vec<String> = serde_json::from_str(files_json)
                .map_err(|_| DbError::Conflict("Team command notification files must be a JSON string array".into()))?;
            if files.is_empty() || files.iter().any(|file| file.trim().is_empty()) {
                return Err(DbError::Conflict(
                    "Team command notification files must contain non-empty paths".into(),
                ));
            }
        }
        if !message_ids.insert(notification.message_id.as_str()) {
            return Err(DbError::Conflict(
                "Team command notification message IDs must be unique".into(),
            ));
        }
        if !idempotency_keys.insert(notification.idempotency_key.as_str()) {
            return Err(DbError::Conflict(
                "Team command notification idempotency keys must be unique".into(),
            ));
        }
    }
    Ok(())
}

fn validate_roster_guard(guard: Option<&TeamRosterGuard>) -> Result<(), DbError> {
    if guard.is_some_and(|guard| guard.expected_user_id.is_empty() || guard.expected_agents_json.is_empty()) {
        return Err(DbError::Conflict(
            "Team roster guard identity fields must not be empty".into(),
        ));
    }
    Ok(())
}

fn ensure_scope(
    resource: &str,
    expected_team_id: &str,
    expected_work_item_id: &str,
    actual_team_id: &str,
    actual_work_item_id: &str,
) -> Result<(), DbError> {
    if expected_team_id != actual_team_id || expected_work_item_id != actual_work_item_id {
        return Err(DbError::Conflict(format!(
            "{resource} scope does not match the command event"
        )));
    }
    Ok(())
}

fn validate_work_item_mutation(mutation: &TeamWorkItemMutation) -> Result<(), DbError> {
    let row = mutation.row();
    validate_work_item_assignment(row)?;
    match mutation {
        TeamWorkItemMutation::Insert(row) if row.revision != 0 => {
            Err(DbError::Conflict("A new WorkItem must start at revision 0".into()))
        }
        TeamWorkItemMutation::CompareAndSwap { expected_revision, row } => {
            ensure_next_revision("WorkItem", *expected_revision, row.revision)
        }
        TeamWorkItemMutation::Insert(_) => Ok(()),
    }
}

fn validate_work_item_assignment(row: &TeamWorkItemRow) -> Result<(), DbError> {
    let complete = row
        .git_repository_id
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        && row
            .git_base_commit
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
        && row
            .git_branch_ref
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty());
    let absent = row.git_repository_id.is_none() && row.git_base_commit.is_none() && row.git_branch_ref.is_none();
    if (row.delivery_requirement == "none" && absent) || (row.delivery_requirement == "git" && complete) {
        Ok(())
    } else {
        Err(DbError::Conflict(
            "WorkItem delivery requirement and immutable Git assignment do not match".into(),
        ))
    }
}

fn validate_git_delivery_mutation(mutation: &TeamGitDeliveryMutation) -> Result<(), DbError> {
    match mutation {
        TeamGitDeliveryMutation::Insert(row) if row.revision != 0 => {
            Err(DbError::Conflict("A new Git delivery must start at revision 0".into()))
        }
        TeamGitDeliveryMutation::CompareAndSwap { expected_revision, row }
        | TeamGitDeliveryMutation::CompareAndSwapWithIntegration {
            expected_revision, row, ..
        } => ensure_next_revision("Git delivery", *expected_revision, row.revision),
        TeamGitDeliveryMutation::Insert(_) => Ok(()),
    }?;
    if let TeamGitDeliveryMutation::CompareAndSwapWithIntegration { row, integration, .. } = mutation {
        validate_integration_mutation(row, integration)?;
    }
    Ok(())
}

fn ensure_next_revision(resource: &str, expected_revision: i64, next_revision: i64) -> Result<(), DbError> {
    let expected_next = expected_revision
        .checked_add(1)
        .ok_or_else(|| DbError::Conflict(format!("{resource} revision overflow")))?;
    if expected_revision < 0 || next_revision != expected_next {
        return Err(DbError::Conflict(format!(
            "{resource} CAS snapshot revision must equal expected revision + 1"
        )));
    }
    Ok(())
}

async fn insert_work_item(connection: &mut SqliteConnection, row: &TeamWorkItemRow) -> Result<(), DbError> {
    sqlx::query(
        "INSERT INTO team_work_items (\
            id, team_id, parent_work_item_id, subject, description, \
            controller_member_id, assignee_member_id, reviewer_member_id, integrator_member_id, \
            delivery_requirement, git_repository_id, git_base_commit, git_branch_ref, \
            state, current_submission_json, accepted_delivery_json, revision, created_at, updated_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&row.id)
    .bind(&row.team_id)
    .bind(&row.parent_work_item_id)
    .bind(&row.subject)
    .bind(&row.description)
    .bind(&row.controller_member_id)
    .bind(&row.assignee_member_id)
    .bind(&row.reviewer_member_id)
    .bind(&row.integrator_member_id)
    .bind(&row.delivery_requirement)
    .bind(&row.git_repository_id)
    .bind(&row.git_base_commit)
    .bind(&row.git_branch_ref)
    .bind(&row.state)
    .bind(&row.current_submission_json)
    .bind(&row.accepted_delivery_json)
    .bind(row.revision)
    .bind(row.created_at)
    .bind(row.updated_at)
    .execute(connection)
    .await?;
    Ok(())
}

async fn update_work_item_cas(
    connection: &mut SqliteConnection,
    expected_revision: i64,
    row: &TeamWorkItemRow,
) -> Result<bool, DbError> {
    let result = sqlx::query(
        "UPDATE team_work_items SET \
            parent_work_item_id = ?, subject = ?, description = ?, controller_member_id = ?, \
            assignee_member_id = ?, reviewer_member_id = ?, integrator_member_id = ?, \
            state = ?, current_submission_json = ?, accepted_delivery_json = ?, \
            revision = ?, updated_at = ? \
         WHERE team_id = ? AND id = ? AND revision = ?",
    )
    .bind(&row.parent_work_item_id)
    .bind(&row.subject)
    .bind(&row.description)
    .bind(&row.controller_member_id)
    .bind(&row.assignee_member_id)
    .bind(&row.reviewer_member_id)
    .bind(&row.integrator_member_id)
    .bind(&row.state)
    .bind(&row.current_submission_json)
    .bind(&row.accepted_delivery_json)
    .bind(row.revision)
    .bind(row.updated_at)
    .bind(&row.team_id)
    .bind(&row.id)
    .bind(expected_revision)
    .execute(connection)
    .await?;
    Ok(result.rows_affected() == 1)
}

async fn insert_git_delivery(connection: &mut SqliteConnection, row: &TeamGitDeliveryRow) -> Result<(), DbError> {
    sqlx::query(
        "INSERT INTO team_git_deliveries (\
            id, team_id, work_item_id, producer_member_id, repository_id, content_revision, \
            base_commit, branch_ref, head_commit, state, revision, merged_commit, created_at, updated_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&row.id)
    .bind(&row.team_id)
    .bind(&row.work_item_id)
    .bind(&row.producer_member_id)
    .bind(&row.repository_id)
    .bind(row.content_revision)
    .bind(&row.base_commit)
    .bind(&row.branch_ref)
    .bind(&row.head_commit)
    .bind(&row.state)
    .bind(row.revision)
    .bind(&row.merged_commit)
    .bind(row.created_at)
    .bind(row.updated_at)
    .execute(connection)
    .await?;
    Ok(())
}

async fn update_git_delivery_cas(
    connection: &mut SqliteConnection,
    expected_revision: i64,
    row: &TeamGitDeliveryRow,
) -> Result<bool, DbError> {
    let result = sqlx::query(
        "UPDATE team_git_deliveries \
         SET state = ?, revision = ?, merged_commit = ?, updated_at = ? \
         WHERE team_id = ? AND id = ? AND revision = ?",
    )
    .bind(&row.state)
    .bind(row.revision)
    .bind(&row.merged_commit)
    .bind(row.updated_at)
    .bind(&row.team_id)
    .bind(&row.id)
    .bind(expected_revision)
    .execute(connection)
    .await?;
    Ok(result.rows_affected() == 1)
}

fn ensure_integration_transition_is_coupled(
    existing: &TeamGitDeliveryRow,
    replacement: &TeamGitDeliveryRow,
    has_integration_mutation: bool,
) -> Result<(), DbError> {
    let touches_integration = existing.state == "integrating"
        || replacement.state == "integrating"
        || replacement.state == "merged"
        || replacement.state == "conflicted";
    if touches_integration != has_integration_mutation {
        return Err(DbError::Conflict(
            "Git integration lifecycle transitions require a coupled durable attempt mutation".into(),
        ));
    }
    Ok(())
}

fn validate_integration_mutation(
    delivery: &TeamGitDeliveryRow,
    mutation: &TeamGitIntegrationMutation,
) -> Result<(), DbError> {
    match mutation {
        TeamGitIntegrationMutation::Begin(attempt) => {
            for (field, value) in [
                ("attempt_id", attempt.attempt_id.as_str()),
                ("repository_id", attempt.repository_id.as_str()),
                ("base_commit", attempt.base_commit.as_str()),
                ("source_ref", attempt.source_ref.as_str()),
                ("source_head", attempt.source_head.as_str()),
                ("target_ref", attempt.target_ref.as_str()),
                ("target_head", attempt.target_head.as_str()),
            ] {
                if value.trim().is_empty() {
                    return Err(DbError::Conflict(format!(
                        "Git integration attempt {field} must not be empty"
                    )));
                }
            }
            if attempt.team_id != delivery.team_id
                || attempt.work_item_id != delivery.work_item_id
                || attempt.delivery_id != delivery.id
                || attempt.repository_id != delivery.repository_id
                || attempt.base_commit != delivery.base_commit
                || attempt.source_ref != delivery.branch_ref
                || attempt.source_head != delivery.head_commit
            {
                return Err(DbError::Conflict(
                    "Git integration attempt is not bound to the exact delivery identity".into(),
                ));
            }
            if delivery.state != "integrating"
                || attempt.state != "pending"
                || attempt.merged_commit.is_some()
                || attempt.observed_target_head.is_some()
                || attempt.recovery_reason.is_some()
                || attempt.updated_at != attempt.created_at
            {
                return Err(DbError::Conflict(
                    "A new Git integration attempt must be a clean pending intent".into(),
                ));
            }
        }
        TeamGitIntegrationMutation::Resolve {
            attempt_id, resolution, ..
        } => {
            if attempt_id.trim().is_empty() {
                return Err(DbError::Conflict(
                    "Resolved Git integration attempt ID must not be empty".into(),
                ));
            }
            match resolution {
                TeamGitIntegrationResolution::Merged {
                    merged_commit,
                    observed_target_head,
                } if delivery.state == "merged"
                    && delivery.merged_commit.as_deref() == Some(merged_commit)
                    && !merged_commit.trim().is_empty()
                    && !observed_target_head.trim().is_empty() => {}
                TeamGitIntegrationResolution::Conflicted { observed_target_head }
                    if delivery.state == "conflicted"
                        && delivery.merged_commit.is_none()
                        && !observed_target_head.trim().is_empty() => {}
                TeamGitIntegrationResolution::Retryable {
                    recovery_reason,
                    observed_target_head,
                } if delivery.state == "accepted"
                    && delivery.merged_commit.is_none()
                    && matches!(
                        recovery_reason.as_str(),
                        "interrupted" | "retryable_infrastructure" | "precondition_changed"
                    )
                    && observed_target_head
                        .as_deref()
                        .is_none_or(|head| !head.trim().is_empty())
                    && (*recovery_reason != "precondition_changed" || observed_target_head.is_some()) => {}
                _ => {
                    return Err(DbError::Conflict(
                        "Git integration resolution does not match the delivery post-state".into(),
                    ));
                }
            }
        }
    }
    Ok(())
}

async fn apply_integration_mutation(
    connection: &mut SqliteConnection,
    delivery: &TeamGitDeliveryRow,
    mutation: &TeamGitIntegrationMutation,
) -> Result<(), DbError> {
    match mutation {
        TeamGitIntegrationMutation::Begin(attempt) => {
            sqlx::query(
                "INSERT INTO team_git_integration_attempts (\
                    attempt_id, team_id, work_item_id, delivery_id, repository_id, base_commit, \
                    source_ref, source_head, target_ref, target_head, state, merged_commit, \
                    observed_target_head, recovery_reason, created_at, updated_at\
                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(&attempt.attempt_id)
            .bind(&attempt.team_id)
            .bind(&attempt.work_item_id)
            .bind(&attempt.delivery_id)
            .bind(&attempt.repository_id)
            .bind(&attempt.base_commit)
            .bind(&attempt.source_ref)
            .bind(&attempt.source_head)
            .bind(&attempt.target_ref)
            .bind(&attempt.target_head)
            .bind(&attempt.state)
            .bind(&attempt.merged_commit)
            .bind(&attempt.observed_target_head)
            .bind(&attempt.recovery_reason)
            .bind(attempt.created_at)
            .bind(attempt.updated_at)
            .execute(connection)
            .await?;
        }
        TeamGitIntegrationMutation::Resolve {
            attempt_id,
            resolution,
            updated_at,
        } => {
            let attempt = find_integration_attempt_on_connection(connection, &delivery.team_id, attempt_id)
                .await?
                .ok_or_else(|| DbError::Conflict("Git integration attempt does not exist".into()))?;
            if attempt.state != "pending"
                || attempt.team_id != delivery.team_id
                || attempt.work_item_id != delivery.work_item_id
                || attempt.delivery_id != delivery.id
                || attempt.repository_id != delivery.repository_id
                || attempt.base_commit != delivery.base_commit
                || attempt.source_ref != delivery.branch_ref
                || attempt.source_head != delivery.head_commit
            {
                return Err(DbError::Conflict(
                    "Git integration resolution does not match the pending exact intent".into(),
                ));
            }
            let (state, merged_commit, observed_target_head, recovery_reason) = match resolution {
                TeamGitIntegrationResolution::Merged {
                    merged_commit,
                    observed_target_head,
                } => (
                    "merged",
                    Some(merged_commit.as_str()),
                    Some(observed_target_head.as_str()),
                    None,
                ),
                TeamGitIntegrationResolution::Conflicted { observed_target_head } => {
                    ("conflicted", None, Some(observed_target_head.as_str()), None)
                }
                TeamGitIntegrationResolution::Retryable {
                    recovery_reason,
                    observed_target_head,
                } => (
                    "retryable",
                    None,
                    observed_target_head.as_deref(),
                    Some(recovery_reason.as_str()),
                ),
            };
            let result = sqlx::query(
                "UPDATE team_git_integration_attempts SET \
                    state = ?, merged_commit = ?, observed_target_head = ?, recovery_reason = ?, updated_at = ? \
                 WHERE team_id = ? AND attempt_id = ? AND state = 'pending'",
            )
            .bind(state)
            .bind(merged_commit)
            .bind(observed_target_head)
            .bind(recovery_reason)
            .bind(updated_at)
            .bind(&delivery.team_id)
            .bind(attempt_id)
            .execute(connection)
            .await?;
            if result.rows_affected() != 1 {
                return Err(DbError::Conflict(
                    "Git integration attempt was resolved concurrently".into(),
                ));
            }
        }
    }
    Ok(())
}

async fn validate_event_post_state(connection: &mut SqliteConnection, event: &NewTeamWorkEvent) -> Result<(), DbError> {
    let actual_work_item_revision = find_work_item_revision(connection, &event.team_id, &event.work_item_id).await?;
    if actual_work_item_revision != Some(event.work_item_revision) {
        return Err(DbError::Conflict(
            "Event WorkItem revision does not match durable post-state".into(),
        ));
    }

    let Some(delivery_id) = event.delivery_id.as_deref() else {
        if event.delivery_revision.is_some() {
            return Err(DbError::Conflict(
                "Event delivery revision requires a delivery ID".into(),
            ));
        }
        return Ok(());
    };
    let expected_delivery_revision = event
        .delivery_revision
        .ok_or_else(|| DbError::Conflict("Event delivery ID requires a delivery revision".into()))?;
    let delivery = find_git_delivery_on_connection(connection, &event.team_id, delivery_id)
        .await?
        .ok_or_else(|| DbError::Conflict("Event delivery does not exist in the command Team".into()))?;
    if delivery.work_item_id != event.work_item_id {
        return Err(DbError::Conflict(
            "Event delivery does not belong to the event WorkItem".into(),
        ));
    }
    if delivery.revision != expected_delivery_revision {
        return Err(DbError::Conflict(
            "Event delivery revision does not match durable post-state".into(),
        ));
    }
    Ok(())
}

async fn insert_event(
    connection: &mut SqliteConnection,
    event: &NewTeamWorkEvent,
) -> Result<TeamWorkEventRow, DbError> {
    let result = sqlx::query(
        "INSERT INTO team_work_events (\
            event_id, team_id, work_item_id, delivery_id, actor_member_id, command_name, \
            idempotency_key, request_fingerprint, result_json, expected_work_item_revision, \
            expected_delivery_revision, work_item_revision, delivery_revision, created_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(&event.event_id)
    .bind(&event.team_id)
    .bind(&event.work_item_id)
    .bind(&event.delivery_id)
    .bind(&event.actor_member_id)
    .bind(&event.command_name)
    .bind(&event.idempotency_key)
    .bind(&event.request_fingerprint)
    .bind(&event.result_json)
    .bind(event.expected_work_item_revision)
    .bind(event.expected_delivery_revision)
    .bind(event.work_item_revision)
    .bind(event.delivery_revision)
    .bind(event.created_at)
    .execute(connection)
    .await?;

    Ok(TeamWorkEventRow {
        sequence: result.last_insert_rowid(),
        event_id: event.event_id.clone(),
        team_id: event.team_id.clone(),
        work_item_id: event.work_item_id.clone(),
        delivery_id: event.delivery_id.clone(),
        actor_member_id: event.actor_member_id.clone(),
        command_name: event.command_name.clone(),
        idempotency_key: event.idempotency_key.clone(),
        request_fingerprint: event.request_fingerprint.clone(),
        result_json: event.result_json.clone(),
        expected_work_item_revision: event.expected_work_item_revision,
        expected_delivery_revision: event.expected_delivery_revision,
        work_item_revision: event.work_item_revision,
        delivery_revision: event.delivery_revision,
        created_at: event.created_at,
    })
}

async fn insert_notifications(
    connection: &mut SqliteConnection,
    team_id: &str,
    notifications: &[NewTeamMailboxNotification],
) -> Result<(), DbError> {
    for notification in notifications {
        let existing = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT id, request_fingerprint FROM mailbox \
             WHERE team_id = ? AND idempotency_scope = ? AND idempotency_key = ?",
        )
        .bind(team_id)
        .bind(&notification.idempotency_scope)
        .bind(&notification.idempotency_key)
        .fetch_optional(&mut *connection)
        .await?;

        if let Some((_, existing_fingerprint)) = existing {
            let message = if existing_fingerprint.as_deref() == Some(&notification.request_fingerprint) {
                "Team command notification idempotency key already exists without its event receipt".into()
            } else {
                format!(
                    "Team command notification idempotency key was already used with a different request fingerprint: {}",
                    existing_fingerprint.as_deref().unwrap_or("<missing>")
                )
            };
            return Err(DbError::Conflict(message));
        }

        sqlx::query(
            "INSERT INTO mailbox (\
                id, team_id, to_agent_id, from_agent_id, type, content, summary, files, read, created_at, \
                idempotency_scope, idempotency_key, request_fingerprint\
             ) VALUES (?, ?, ?, ?, 'message', ?, ?, ?, 0, ?, ?, ?, ?)",
        )
        .bind(&notification.message_id)
        .bind(team_id)
        .bind(&notification.to_agent_id)
        .bind(&notification.from_agent_id)
        .bind(&notification.content)
        .bind(&notification.summary)
        .bind(&notification.files_json)
        .bind(notification.created_at)
        .bind(&notification.idempotency_scope)
        .bind(&notification.idempotency_key)
        .bind(&notification.request_fingerprint)
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

async fn find_receipt_on_connection(
    connection: &mut SqliteConnection,
    team_id: &str,
    actor_member_id: &str,
    idempotency_key: &str,
) -> Result<Option<TeamWorkEventRow>, DbError> {
    let row = sqlx::query_as::<_, TeamWorkEventRow>(
        "SELECT * FROM team_work_events \
         WHERE team_id = ? AND actor_member_id = ? AND idempotency_key = ?",
    )
    .bind(team_id)
    .bind(actor_member_id)
    .bind(idempotency_key)
    .fetch_optional(connection)
    .await?;
    Ok(row)
}

async fn find_work_item_revision(
    connection: &mut SqliteConnection,
    team_id: &str,
    work_item_id: &str,
) -> Result<Option<i64>, DbError> {
    let revision = sqlx::query_scalar::<_, i64>("SELECT revision FROM team_work_items WHERE team_id = ? AND id = ?")
        .bind(team_id)
        .bind(work_item_id)
        .fetch_optional(connection)
        .await?;
    Ok(revision)
}

async fn find_work_item_on_connection(
    connection: &mut SqliteConnection,
    team_id: &str,
    work_item_id: &str,
) -> Result<Option<TeamWorkItemRow>, DbError> {
    let row = sqlx::query_as::<_, TeamWorkItemRow>("SELECT * FROM team_work_items WHERE team_id = ? AND id = ?")
        .bind(team_id)
        .bind(work_item_id)
        .fetch_optional(connection)
        .await?;
    Ok(row)
}

async fn team_roster_matches(
    connection: &mut SqliteConnection,
    team_id: &str,
    guard: &TeamRosterGuard,
) -> Result<bool, DbError> {
    let actual = sqlx::query_as::<_, (String, String, Option<String>)>(
        "SELECT user_id, agents, lead_agent_id FROM teams WHERE id = ?",
    )
    .bind(team_id)
    .fetch_optional(connection)
    .await?;
    Ok(actual.is_some_and(|(user_id, agents_json, lead_agent_id)| {
        user_id == guard.expected_user_id
            && agents_json == guard.expected_agents_json
            && lead_agent_id == guard.expected_lead_agent_id
    }))
}

async fn find_git_delivery_revision(
    connection: &mut SqliteConnection,
    team_id: &str,
    delivery_id: &str,
) -> Result<Option<i64>, DbError> {
    let revision =
        sqlx::query_scalar::<_, i64>("SELECT revision FROM team_git_deliveries WHERE team_id = ? AND id = ?")
            .bind(team_id)
            .bind(delivery_id)
            .fetch_optional(connection)
            .await?;
    Ok(revision)
}

async fn find_git_delivery_on_connection(
    connection: &mut SqliteConnection,
    team_id: &str,
    delivery_id: &str,
) -> Result<Option<TeamGitDeliveryRow>, DbError> {
    let row = sqlx::query_as::<_, TeamGitDeliveryRow>("SELECT * FROM team_git_deliveries WHERE team_id = ? AND id = ?")
        .bind(team_id)
        .bind(delivery_id)
        .fetch_optional(connection)
        .await?;
    Ok(row)
}

async fn find_integration_attempt_on_connection(
    connection: &mut SqliteConnection,
    team_id: &str,
    attempt_id: &str,
) -> Result<Option<TeamGitIntegrationAttemptRow>, DbError> {
    let row = sqlx::query_as::<_, TeamGitIntegrationAttemptRow>(
        "SELECT * FROM team_git_integration_attempts WHERE team_id = ? AND attempt_id = ?",
    )
    .bind(team_id)
    .bind(attempt_id)
    .fetch_optional(connection)
    .await?;
    Ok(row)
}

fn ensure_same_delivery_identity(
    existing: &TeamGitDeliveryRow,
    replacement: &TeamGitDeliveryRow,
) -> Result<(), DbError> {
    let same_identity = existing.id == replacement.id
        && existing.team_id == replacement.team_id
        && existing.work_item_id == replacement.work_item_id
        && existing.producer_member_id == replacement.producer_member_id
        && existing.repository_id == replacement.repository_id
        && existing.content_revision == replacement.content_revision
        && existing.base_commit == replacement.base_commit
        && existing.branch_ref == replacement.branch_ref
        && existing.head_commit == replacement.head_commit
        && existing.created_at == replacement.created_at;
    if !same_identity {
        return Err(DbError::Conflict(format!(
            "Git delivery '{}' identity is immutable",
            existing.id
        )));
    }
    Ok(())
}

fn ensure_same_work_item_assignment(existing: &TeamWorkItemRow, replacement: &TeamWorkItemRow) -> Result<(), DbError> {
    let same_assignment = existing.delivery_requirement == replacement.delivery_requirement
        && existing.git_repository_id == replacement.git_repository_id
        && existing.git_base_commit == replacement.git_base_commit
        && existing.git_branch_ref == replacement.git_branch_ref;
    if !same_assignment {
        return Err(DbError::Conflict(format!(
            "WorkItem '{}' Git assignment is immutable",
            existing.id
        )));
    }
    Ok(())
}
