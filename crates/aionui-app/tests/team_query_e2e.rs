mod common;

use aionui_db::models::TeamRow;
use aionui_db::{ITeamRepository, SqliteTeamRepository};
use axum::http::StatusCode;
use tower::ServiceExt;

use common::{body_json, build_app, get_request, get_with_token, setup_and_login};

async fn seed_team(services: &aionui_app::AppServices, id: &str, user_id: &str) {
    SqliteTeamRepository::new(services.database.pool().clone())
        .create_team(&TeamRow {
            id: id.to_owned(),
            user_id: user_id.to_owned(),
            name: format!("Team {id}"),
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
        .expect("seed Team");
}

async fn seed_work_item_history(services: &aionui_app::AppServices, team_id: &str) {
    let submission = serde_json::json!({
        "kind": "git",
        "delivery": {
            "delivery_id": "delivery-1",
            "team_id": team_id,
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
         VALUES ('work-1', ?, 'Implement query API', 'lead', 'worker', 'reviewer', \
                 'integrator', 'git', 'repo-1', 'base', 'refs/heads/ds/work-1/worker', \
                 'submitted', ?, 3, 10, 20)",
    )
    .bind(team_id)
    .bind(submission)
    .execute(services.database.pool())
    .await
    .expect("seed WorkItem");
    sqlx::query(
        "INSERT INTO team_git_deliveries \
         (id, team_id, work_item_id, producer_member_id, repository_id, content_revision, base_commit, \
          branch_ref, head_commit, state, revision, created_at, updated_at) \
         VALUES ('delivery-1', ?, 'work-1', 'worker', 'repo-1', 1, 'base', \
                 'refs/heads/ds/work-1/worker', 'head', 'submitted', 0, 15, 15)",
    )
    .bind(team_id)
    .execute(services.database.pool())
    .await
    .expect("seed Git delivery");
    sqlx::query(
        "INSERT INTO team_work_events \
         (event_id, team_id, work_item_id, delivery_id, actor_member_id, command_name, idempotency_key, \
          request_fingerprint, result_json, expected_work_item_revision, expected_delivery_revision, \
          work_item_revision, delivery_revision, created_at) \
         VALUES ('event-1', ?, 'work-1', 'delivery-1', 'worker', 'submit_git', \
                 'secret-idempotency', 'secret-fingerprint', '{\"secret_result\":true}', 2, NULL, 3, 0, 15)",
    )
    .bind(team_id)
    .execute(services.database.pool())
    .await
    .expect("seed WorkItem event");
}

#[tokio::test]
async fn authenticated_owner_can_query_safe_team_mode_views() {
    let (mut app, services) = build_app().await;
    let (token, _csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_team(&services, "team-query", "system_default_user").await;
    seed_work_item_history(&services, "team-query").await;

    let list_response = app
        .clone()
        .oneshot(get_with_token("/api/teams/team-query/work-items", &token))
        .await
        .unwrap();
    assert_eq!(list_response.status(), StatusCode::OK);
    let list_json = body_json(list_response).await;
    assert_eq!(list_json["data"][0]["id"], "work-1");

    let snapshot_response = app
        .clone()
        .oneshot(get_with_token("/api/teams/team-query/work-items/work-1", &token))
        .await
        .unwrap();
    assert_eq!(snapshot_response.status(), StatusCode::OK);
    let snapshot_json = body_json(snapshot_response).await;
    assert_eq!(snapshot_json["data"]["work_item"]["state"], "submitted");
    assert_eq!(snapshot_json["data"]["deliveries"][0]["head_commit"], "head");
    assert!(snapshot_json["data"]["work_item"]["git_assignment"]["repository_id"].is_null());
    assert!(snapshot_json["data"]["deliveries"][0]["repository_id"].is_null());

    let events_response = app
        .oneshot(get_with_token("/api/teams/team-query/work-items/work-1/events", &token))
        .await
        .unwrap();
    assert_eq!(events_response.status(), StatusCode::OK);
    let events_json = body_json(events_response).await;
    assert_eq!(events_json["data"][0]["command_name"], "submit_git");
    let serialized = serde_json::to_string(&events_json).unwrap();
    for forbidden in [
        "idempotency_key",
        "request_fingerprint",
        "result_json",
        "secret-idempotency",
        "secret-fingerprint",
        "secret_result",
    ] {
        assert!(!serialized.contains(forbidden), "response leaked {forbidden}");
    }
}

#[tokio::test]
async fn team_mode_queries_reject_unauthenticated_cross_user_and_missing_resources() {
    let (mut app, services) = build_app().await;

    let unauthenticated = app
        .clone()
        .oneshot(get_request("/api/teams/team-owned/work-items"))
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);

    let (token, _csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_team(&services, "team-owned", "system_default_user").await;
    seed_team(&services, "team-foreign", "different-user").await;

    let cross_user = app
        .clone()
        .oneshot(get_with_token("/api/teams/team-foreign/work-items", &token))
        .await
        .unwrap();
    assert_eq!(cross_user.status(), StatusCode::FORBIDDEN);

    let missing = app
        .oneshot(get_with_token(
            "/api/teams/team-owned/work-items/missing/events",
            &token,
        ))
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
}
