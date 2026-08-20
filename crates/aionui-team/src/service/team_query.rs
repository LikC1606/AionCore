use std::sync::Arc;

use aionui_api_types::{
    TeamGitDeliveryResponse, TeamGitWorkAssignmentResponse, TeamWorkEventResponse, TeamWorkItemResponse,
    TeamWorkItemSnapshotResponse, TeamWorkSubmissionResponse,
};
use aionui_db::{DbError, ITeamModeRepository, ITeamRepository, TeamGitDeliveryRow, TeamWorkEventRow, TeamWorkItemRow};
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::kernel::{
    DeliveryRequirement, GitDeliveryLifecycle, GitDeliveryRef, GitDeliveryState, GitWorkAssignment, WorkItemLifecycle,
    WorkItemState, WorkSubmission,
};

const STABLE_SNAPSHOT_ATTEMPTS: usize = 3;

/// Authenticated, read-only access to canonical Team Mode state.
pub struct TeamQueryService {
    team_repo: Arc<dyn ITeamRepository>,
    mode_repo: Arc<dyn ITeamModeRepository>,
}

impl TeamQueryService {
    pub fn new(team_repo: Arc<dyn ITeamRepository>, mode_repo: Arc<dyn ITeamModeRepository>) -> Self {
        Self { team_repo, mode_repo }
    }

    pub async fn list_work_items(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
    ) -> Result<Vec<TeamWorkItemResponse>, TeamQueryError> {
        self.require_owner(authenticated_user_id, team_id).await?;
        self.mode_repo
            .list_work_items(team_id)
            .await?
            .into_iter()
            .map(work_item_response)
            .collect()
    }

    pub async fn get_work_item_snapshot(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
        work_item_id: &str,
    ) -> Result<TeamWorkItemSnapshotResponse, TeamQueryError> {
        self.require_owner(authenticated_user_id, team_id).await?;

        for _ in 0..STABLE_SNAPSHOT_ATTEMPTS {
            let work_item = self.load_work_item(team_id, work_item_id).await?;
            let deliveries = self.mode_repo.list_git_deliveries(team_id, Some(work_item_id)).await?;
            let observed_again = self.mode_repo.get_work_item(team_id, work_item_id).await?;
            if observed_again.as_ref() == Some(&work_item) {
                return Ok(TeamWorkItemSnapshotResponse {
                    work_item: work_item_response(work_item)?,
                    deliveries: deliveries
                        .into_iter()
                        .map(git_delivery_response)
                        .collect::<Result<_, _>>()?,
                });
            }
        }

        Err(TeamQueryError::ConcurrentSnapshotChange)
    }

    pub async fn list_work_item_events(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
        work_item_id: &str,
    ) -> Result<Vec<TeamWorkEventResponse>, TeamQueryError> {
        self.require_owner(authenticated_user_id, team_id).await?;
        self.load_work_item(team_id, work_item_id).await?;
        self.mode_repo
            .list_work_events(team_id, work_item_id)
            .await?
            .into_iter()
            .map(work_event_response)
            .collect()
    }

    async fn require_owner(&self, authenticated_user_id: &str, team_id: &str) -> Result<(), TeamQueryError> {
        let team = self
            .team_repo
            .get_team(team_id)
            .await?
            .ok_or_else(|| TeamQueryError::TeamNotFound(team_id.to_owned()))?;
        if team.user_id != authenticated_user_id {
            return Err(TeamQueryError::ForbiddenTeam);
        }
        Ok(())
    }

    async fn load_work_item(&self, team_id: &str, work_item_id: &str) -> Result<TeamWorkItemRow, TeamQueryError> {
        self.mode_repo
            .get_work_item(team_id, work_item_id)
            .await?
            .ok_or_else(|| TeamQueryError::WorkItemNotFound(work_item_id.to_owned()))
    }
}

#[derive(Debug, Error)]
pub enum TeamQueryError {
    #[error("Team not found: {0}")]
    TeamNotFound(String),
    #[error("authenticated user does not own this Team")]
    ForbiddenTeam,
    #[error("WorkItem not found: {0}")]
    WorkItemNotFound(String),
    #[error("Team Mode state changed repeatedly while it was being read")]
    ConcurrentSnapshotChange,
    #[error("stored {aggregate} is invalid: {reason}")]
    CorruptStoredState { aggregate: &'static str, reason: String },
    #[error(transparent)]
    Database(#[from] DbError),
}

fn work_item_response(row: TeamWorkItemRow) -> Result<TeamWorkItemResponse, TeamQueryError> {
    let state = decode_string_enum::<WorkItemState>(&row.state, "WorkItem state")?;
    let delivery_requirement =
        decode_string_enum::<DeliveryRequirement>(&row.delivery_requirement, "delivery requirement")?;
    let git_assignment = decode_git_assignment(&row)?;
    let current_submission =
        decode_optional::<WorkSubmission>(row.current_submission_json.as_deref(), "WorkItem current submission")?;
    let accepted_delivery =
        decode_optional::<GitDeliveryRef>(row.accepted_delivery_json.as_deref(), "WorkItem accepted delivery")?;
    let revision = nonnegative_u64(row.revision, "WorkItem revision")?;
    let lifecycle = WorkItemLifecycle::restore(
        row.team_id.clone(),
        row.id.clone(),
        state,
        delivery_requirement,
        git_assignment,
        current_submission,
        accepted_delivery,
        revision,
    )
    .map_err(|error| corrupt("WorkItem", error))?;

    let current_submission = lifecycle.current_submission().map(|submission| {
        let delivery = submission.git_delivery();
        TeamWorkSubmissionResponse {
            kind: if delivery.is_some() { "git" } else { "inline" }.to_owned(),
            producer_member_id: submission.producer_member_id().to_owned(),
            delivery_id: delivery.map(|value| value.delivery_id().to_owned()),
        }
    });

    Ok(TeamWorkItemResponse {
        id: row.id,
        team_id: row.team_id,
        parent_work_item_id: row.parent_work_item_id,
        subject: row.subject,
        description: row.description,
        controller_member_id: row.controller_member_id,
        assignee_member_id: row.assignee_member_id,
        reviewer_member_id: row.reviewer_member_id,
        integrator_member_id: row.integrator_member_id,
        delivery_requirement: row.delivery_requirement,
        git_assignment: lifecycle
            .git_assignment()
            .map(|assignment| TeamGitWorkAssignmentResponse {
                base_commit: assignment.base_commit().to_owned(),
                branch_ref: assignment.branch_ref().to_owned(),
            }),
        state: row.state,
        current_submission,
        accepted_delivery_id: lifecycle
            .accepted_delivery()
            .map(|delivery| delivery.delivery_id().to_owned()),
        revision: lifecycle.revision(),
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn decode_git_assignment(row: &TeamWorkItemRow) -> Result<Option<GitWorkAssignment>, TeamQueryError> {
    match (
        row.git_repository_id.as_deref(),
        row.git_base_commit.as_deref(),
        row.git_branch_ref.as_deref(),
    ) {
        (None, None, None) => Ok(None),
        (Some(repository_id), Some(base_commit), Some(branch_ref)) => {
            Ok(Some(GitWorkAssignment::new(repository_id, base_commit, branch_ref)))
        }
        _ => Err(corrupt("WorkItem", "Git assignment fields must be present together")),
    }
}

fn git_delivery_response(row: TeamGitDeliveryRow) -> Result<TeamGitDeliveryResponse, TeamQueryError> {
    let content_revision = positive_u64(row.content_revision, "Git delivery content revision")?;
    let state = decode_string_enum::<GitDeliveryState>(&row.state, "Git delivery state")?;
    let revision = nonnegative_u64(row.revision, "Git delivery revision")?;
    let delivery = GitDeliveryRef::new(
        row.id.clone(),
        row.team_id.clone(),
        row.work_item_id.clone(),
        row.producer_member_id.clone(),
        row.repository_id.clone(),
        content_revision,
        row.base_commit.clone(),
        row.branch_ref.clone(),
        row.head_commit.clone(),
    );
    let lifecycle = GitDeliveryLifecycle::restore(delivery, state, revision, row.merged_commit.clone())
        .map_err(|error| corrupt("Git delivery", error))?;

    Ok(TeamGitDeliveryResponse {
        id: row.id,
        team_id: row.team_id,
        work_item_id: row.work_item_id,
        producer_member_id: row.producer_member_id,
        content_revision,
        base_commit: row.base_commit,
        branch_ref: row.branch_ref,
        head_commit: row.head_commit,
        state: row.state,
        revision: lifecycle.revision(),
        merged_commit: row.merged_commit,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn work_event_response(row: TeamWorkEventRow) -> Result<TeamWorkEventResponse, TeamQueryError> {
    if row.delivery_id.is_some() != row.delivery_revision.is_some() {
        return Err(corrupt(
            "WorkItem event",
            "delivery identity and revision must be present together",
        ));
    }

    Ok(TeamWorkEventResponse {
        sequence: positive_u64(row.sequence, "WorkItem event sequence")?,
        event_id: row.event_id,
        team_id: row.team_id,
        work_item_id: row.work_item_id,
        delivery_id: row.delivery_id,
        actor_member_id: row.actor_member_id,
        command_name: row.command_name,
        work_item_revision: nonnegative_u64(row.work_item_revision, "WorkItem event revision")?,
        delivery_revision: row
            .delivery_revision
            .map(|revision| nonnegative_u64(revision, "Git delivery event revision"))
            .transpose()?,
        created_at: row.created_at,
    })
}

fn decode_optional<T: DeserializeOwned>(
    raw: Option<&str>,
    aggregate: &'static str,
) -> Result<Option<T>, TeamQueryError> {
    raw.map(|value| serde_json::from_str(value).map_err(|error| corrupt(aggregate, error)))
        .transpose()
}

fn decode_string_enum<T: DeserializeOwned>(raw: &str, aggregate: &'static str) -> Result<T, TeamQueryError> {
    serde_json::from_value(serde_json::Value::String(raw.to_owned())).map_err(|error| corrupt(aggregate, error))
}

fn nonnegative_u64(value: i64, field: &'static str) -> Result<u64, TeamQueryError> {
    u64::try_from(value).map_err(|_| corrupt(field, format!("negative value {value}")))
}

fn positive_u64(value: i64, field: &'static str) -> Result<u64, TeamQueryError> {
    let value = nonnegative_u64(value, field)?;
    if value == 0 {
        return Err(corrupt(field, "value must be positive"));
    }
    Ok(value)
}

fn corrupt(aggregate: &'static str, reason: impl std::fmt::Display) -> TeamQueryError {
    TeamQueryError::CorruptStoredState {
        aggregate,
        reason: reason.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aionui_db::models::TeamRow;
    use aionui_db::{ITeamRepository, SqliteTeamModeRepository, SqliteTeamRepository, init_database_memory};

    async fn query_service() -> (TeamQueryService, aionui_db::Database) {
        let database = init_database_memory().await.expect("initialize database");
        let team_repo = Arc::new(SqliteTeamRepository::new(database.pool().clone()));
        team_repo
            .create_team(&TeamRow {
                id: "team-1".into(),
                user_id: "owner".into(),
                name: "Canonical Team".into(),
                workspace: String::new(),
                workspace_mode: "shared".into(),
                agents: "[]".into(),
                lead_agent_id: None,
                session_mode: None,
                agents_version: "1.0.1".into(),
                created_at: 1,
                updated_at: 1,
            })
            .await
            .unwrap();
        let mode_repo = Arc::new(SqliteTeamModeRepository::new(database.pool().clone()));
        (TeamQueryService::new(team_repo, mode_repo), database)
    }

    async fn seed_submitted_work_item(database: &aionui_db::Database) {
        let submission = serde_json::json!({
            "kind": "git",
            "delivery": {
                "delivery_id": "delivery-1",
                "team_id": "team-1",
                "work_item_id": "work-1",
                "producer_member_id": "worker",
                "repository_id": "repo-1",
                "content_revision": 1,
                "base_commit": "base",
                "branch_ref": "refs/heads/ds/work-1/worker",
                "head_commit": "head"
            }
        })
        .to_string();
        sqlx::query(
            "INSERT INTO team_work_items \
             (id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
              integrator_member_id, delivery_requirement, git_repository_id, git_base_commit, git_branch_ref, \
              state, current_submission_json, revision, created_at, updated_at) \
             VALUES ('work-1', 'team-1', 'Implement query', 'lead', 'worker', 'reviewer', \
                     'integrator', 'git', 'repo-1', 'base', 'refs/heads/ds/work-1/worker', \
                     'submitted', ?, 3, 10, 20)",
        )
        .bind(submission)
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO team_git_deliveries \
             (id, team_id, work_item_id, producer_member_id, repository_id, content_revision, base_commit, \
              branch_ref, head_commit, state, revision, created_at, updated_at) \
             VALUES ('delivery-1', 'team-1', 'work-1', 'worker', 'repo-1', 1, 'base', \
                     'refs/heads/ds/work-1/worker', 'head', 'submitted', 0, 15, 15)",
        )
        .execute(database.pool())
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO team_work_events \
             (event_id, team_id, work_item_id, delivery_id, actor_member_id, command_name, idempotency_key, \
              request_fingerprint, result_json, expected_work_item_revision, expected_delivery_revision, \
              work_item_revision, delivery_revision, created_at) \
             VALUES ('event-1', 'team-1', 'work-1', 'delivery-1', 'worker', 'submit_git', \
                     'private-key', 'private-fingerprint', '{\"private\":true}', 2, NULL, 3, 0, 15)",
        )
        .execute(database.pool())
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn owner_receives_validated_snapshot_and_sanitized_events() {
        let (service, database) = query_service().await;
        seed_submitted_work_item(&database).await;

        let snapshot = service
            .get_work_item_snapshot("owner", "team-1", "work-1")
            .await
            .unwrap();
        assert_eq!(snapshot.work_item.state, "submitted");
        assert_eq!(
            snapshot.work_item.git_assignment,
            Some(TeamGitWorkAssignmentResponse {
                base_commit: "base".into(),
                branch_ref: "refs/heads/ds/work-1/worker".into(),
            })
        );
        assert_eq!(
            snapshot
                .work_item
                .current_submission
                .as_ref()
                .unwrap()
                .delivery_id
                .as_deref(),
            Some("delivery-1")
        );
        assert_eq!(snapshot.deliveries[0].head_commit, "head");
        let snapshot_json = serde_json::to_value(&snapshot).unwrap();
        assert!(
            snapshot_json["work_item"]["git_assignment"]
                .get("repository_id")
                .is_none()
        );
        assert!(snapshot_json["deliveries"][0].get("repository_id").is_none());

        let events = service
            .list_work_item_events("owner", "team-1", "work-1")
            .await
            .unwrap();
        let event_json = serde_json::to_value(&events[0]).unwrap();
        assert!(event_json.get("idempotency_key").is_none());
        assert!(event_json.get("request_fingerprint").is_none());
        assert!(event_json.get("result_json").is_none());
    }

    #[tokio::test]
    async fn ownership_is_required_before_team_mode_state_is_returned() {
        let (service, database) = query_service().await;
        seed_submitted_work_item(&database).await;

        let error = service.list_work_items("other-user", "team-1").await.unwrap_err();

        assert!(matches!(error, TeamQueryError::ForbiddenTeam));
    }

    #[tokio::test]
    async fn missing_work_item_is_not_reported_as_an_empty_snapshot_or_history() {
        let (service, _database) = query_service().await;

        let snapshot_error = service
            .get_work_item_snapshot("owner", "team-1", "missing")
            .await
            .unwrap_err();
        assert!(matches!(snapshot_error, TeamQueryError::WorkItemNotFound(id) if id == "missing"));

        let event_error = service
            .list_work_item_events("owner", "team-1", "missing")
            .await
            .unwrap_err();
        assert!(matches!(event_error, TeamQueryError::WorkItemNotFound(id) if id == "missing"));
    }

    #[tokio::test]
    async fn malformed_persisted_submission_is_rejected_instead_of_exposed() {
        let (service, database) = query_service().await;
        sqlx::query(
            "INSERT INTO team_work_items \
             (id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
              delivery_requirement, state, current_submission_json, revision, created_at, updated_at) \
             VALUES ('work-bad', 'team-1', 'Bad snapshot', 'lead', 'worker', 'reviewer', \
                     'none', 'submitted', 'not-json', 3, 10, 10)",
        )
        .execute(database.pool())
        .await
        .unwrap();

        let error = service.list_work_items("owner", "team-1").await.unwrap_err();

        assert!(matches!(error, TeamQueryError::CorruptStoredState { .. }));
    }
}
