use std::sync::Arc;

use aionui_common::now_ms;
use aionui_db::models::TeamRow;
use aionui_db::{
    CommitTeamCommandParams, ITeamModeRepository, ITeamRepository, NewTeamWorkEvent, SqliteTeamModeRepository,
    SqliteTeamRepository, TeamCommandCommitResult, TeamGitDeliveryMutation, TeamGitDeliveryRow,
    TeamGitIntegrationAttemptRow, TeamGitIntegrationMutation, init_database_memory,
};

async fn setup() -> (aionui_db::Database, Arc<SqliteTeamModeRepository>) {
    let db = init_database_memory().await.unwrap();
    let team_repo = SqliteTeamRepository::new(db.pool().clone());
    team_repo
        .create_team(&TeamRow {
            coordination_protocol: None,
            id: "team-1".into(),
            user_id: "user-1".into(),
            name: "Integration Team".into(),
            workspace: "/tmp/integration-team".into(),
            workspace_mode: "shared".into(),
            agents: "[]".into(),
            lead_agent_id: None,
            session_mode: None,
            agents_version: "1".into(),
            created_at: now_ms(),
            updated_at: now_ms(),
        })
        .await
        .unwrap();
    let now = now_ms();
    sqlx::query(
        "INSERT INTO team_work_items (\
            id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
            integrator_member_id, delivery_requirement, git_repository_id, git_base_commit, git_branch_ref, \
            state, revision, created_at, updated_at\
         ) VALUES ('work-1', 'team-1', 'Integrate', 'lead', 'worker', 'lead', 'lead', \
                   'git', 'repo-1', 'base-1', 'refs/heads/work', 'accepted', 5, ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO team_git_deliveries (\
            id, team_id, work_item_id, producer_member_id, repository_id, content_revision, \
            base_commit, branch_ref, head_commit, state, revision, created_at, updated_at\
         ) VALUES ('delivery-1', 'team-1', 'work-1', 'worker', 'repo-1', 1, 'base-1', \
                   'refs/heads/work', 'source-1', 'accepted', 1, ?, ?)",
    )
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();
    let repository = Arc::new(SqliteTeamModeRepository::new(db.pool().clone()));
    (db, repository)
}

fn attempt(repository_id: &str) -> TeamGitIntegrationAttemptRow {
    let now = now_ms();
    TeamGitIntegrationAttemptRow {
        attempt_id: "attempt-1".into(),
        team_id: "team-1".into(),
        work_item_id: "work-1".into(),
        delivery_id: "delivery-1".into(),
        repository_id: repository_id.into(),
        base_commit: "base-1".into(),
        source_ref: "refs/heads/work".into(),
        source_head: "source-1".into(),
        target_ref: "refs/heads/main".into(),
        target_head: "target-1".into(),
        state: "pending".into(),
        merged_commit: None,
        observed_target_head: None,
        recovery_reason: None,
        created_at: now,
        updated_at: now,
    }
}

fn integrating_delivery() -> TeamGitDeliveryRow {
    TeamGitDeliveryRow {
        id: "delivery-1".into(),
        team_id: "team-1".into(),
        work_item_id: "work-1".into(),
        producer_member_id: "worker".into(),
        repository_id: "repo-1".into(),
        content_revision: 1,
        base_commit: "base-1".into(),
        branch_ref: "refs/heads/work".into(),
        head_commit: "source-1".into(),
        state: "integrating".into(),
        revision: 2,
        merged_commit: None,
        created_at: 0,
        updated_at: now_ms(),
    }
}

fn event(id: &str) -> NewTeamWorkEvent {
    NewTeamWorkEvent {
        event_id: id.into(),
        team_id: "team-1".into(),
        work_item_id: "work-1".into(),
        delivery_id: Some("delivery-1".into()),
        actor_member_id: "lead".into(),
        command_name: "begin_integration".into(),
        idempotency_key: id.into(),
        request_fingerprint: format!("fingerprint-{id}"),
        result_json: "{}".into(),
        expected_work_item_revision: Some(5),
        expected_delivery_revision: Some(1),
        work_item_revision: 5,
        delivery_revision: Some(2),
        created_at: now_ms(),
    }
}

#[tokio::test]
async fn exact_attempt_is_committed_with_integrating_delivery_and_event() {
    let (_db, repository) = setup().await;
    let mut delivery = integrating_delivery();
    delivery.created_at = repository
        .get_git_delivery("team-1", "delivery-1")
        .await
        .unwrap()
        .unwrap()
        .created_at;
    let attempt = attempt("repo-1");
    let outcome = repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: vec![],
            notifications: Vec::new(),
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::CompareAndSwapWithIntegration {
                expected_revision: 1,
                row: delivery,
                integration: TeamGitIntegrationMutation::Begin(Box::new(attempt.clone())),
            }),
            event: event("begin-1"),
        })
        .await
        .unwrap();
    assert!(matches!(outcome, TeamCommandCommitResult::Applied(_)));
    assert_eq!(
        repository
            .get_pending_git_integration_attempt("team-1", "delivery-1")
            .await
            .unwrap(),
        Some(attempt)
    );
    assert_eq!(
        repository
            .get_git_delivery("team-1", "delivery-1")
            .await
            .unwrap()
            .unwrap()
            .state,
        "integrating"
    );
    assert_eq!(repository.list_work_events("team-1", "work-1").await.unwrap().len(), 1);
}

#[tokio::test]
async fn mismatched_attempt_identity_rolls_back_the_delivery_transition() {
    let (_db, repository) = setup().await;
    let original = repository
        .get_git_delivery("team-1", "delivery-1")
        .await
        .unwrap()
        .unwrap();
    let mut delivery = integrating_delivery();
    delivery.created_at = original.created_at;
    let result = repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: vec![],
            notifications: Vec::new(),
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::CompareAndSwapWithIntegration {
                expected_revision: 1,
                row: delivery,
                integration: TeamGitIntegrationMutation::Begin(Box::new(attempt("another-repo"))),
            }),
            event: event("begin-invalid"),
        })
        .await;
    assert!(result.is_err());
    assert_eq!(
        repository.get_git_delivery("team-1", "delivery-1").await.unwrap(),
        Some(original)
    );
    assert!(
        repository
            .list_work_events("team-1", "work-1")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        repository
            .list_pending_git_integration_attempts("team-1")
            .await
            .unwrap()
            .is_empty()
    );
}
