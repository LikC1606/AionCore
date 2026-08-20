mod common;

use std::time::Duration;

use aionui_app::{AppConfig, AppServices, create_router_with_runtime};
use aionui_db::models::TeamRow;
use aionui_db::{ITeamRepository, SqliteTeamRepository};
use axum::http::StatusCode;
use serde_json::{Value, json};
use tower::ServiceExt;

use common::{body_json, build_app, get_with_token, json_with_token, setup_and_login};

const TEAM_ID: &str = "team-command-http";

async fn seed_command_team(services: &aionui_app::AppServices, team_id: &str, user_id: &str) {
    let agents = json!([
        {
            "slot_id": "lead",
            "name": "Lead",
            "role": "lead",
            "conversation_id": "conv-lead",
            "backend": "mock",
            "model": ""
        },
        {
            "slot_id": "worker",
            "name": "Worker",
            "role": "teammate",
            "conversation_id": "conv-worker",
            "backend": "mock",
            "model": ""
        }
    ]);
    SqliteTeamRepository::new(services.database.pool().clone())
        .create_team(&TeamRow {
            id: team_id.to_owned(),
            user_id: user_id.to_owned(),
            name: "Command Team".into(),
            workspace: String::new(),
            workspace_mode: "shared".into(),
            agents: serde_json::to_string(&agents).unwrap(),
            lead_agent_id: Some("lead".into()),
            session_mode: None,
            agents_version: "1.0.1".into(),
            created_at: 1,
            updated_at: 1,
        })
        .await
        .expect("seed command Team");
}

async fn seed_inline_submission(services: &aionui_app::AppServices, team_id: &str, work_item_id: &str) {
    seed_inline_state(services, team_id, work_item_id, "submitted", 3).await;
}

async fn seed_inline_state(
    services: &aionui_app::AppServices,
    team_id: &str,
    work_item_id: &str,
    state: &str,
    revision: i64,
) {
    let submission = json!({
        "kind": "inline",
        "producer_member_id": "worker"
    })
    .to_string();
    sqlx::query(
        "INSERT INTO team_work_items \
         (id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
          delivery_requirement, state, current_submission_json, revision, created_at, updated_at) \
         VALUES (?, ?, 'Review this result', 'lead', 'worker', 'lead', \
                 'none', ?, ?, ?, 10, 20)",
    )
    .bind(work_item_id)
    .bind(team_id)
    .bind(state)
    .bind(submission)
    .bind(revision)
    .execute(services.database.pool())
    .await
    .expect("seed inline submission");
}

fn delegate_body() -> Value {
    json!({
        "idempotency_key": "delegate-http-1",
        "subject": "Implement the scoped change",
        "description": "Return a reviewable result",
        "assignee_member_id": "worker",
        "delivery_requirement": "none"
    })
}

#[tokio::test]
async fn owner_delegate_and_cancel_are_idempotent_public_commands() {
    let database = aionui_db::init_database_memory().await.unwrap();
    let services = AppServices::from_config(database, &AppConfig::default()).await.unwrap();
    let (mut app, _runtime) = create_router_with_runtime(&services)
        .await
        .expect("build router with runtime");
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_command_team(&services, TEAM_ID, "system_default_user").await;

    let endpoint = format!("/api/teams/{TEAM_ID}/work-items/delegate");
    let first = tokio::time::timeout(
        Duration::from_secs(5),
        app.clone()
            .oneshot(json_with_token("POST", &endpoint, delegate_body(), &token, &csrf)),
    )
    .await
    .expect("delegation must not wait for Team runtime startup")
    .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first = body_json(first).await;
    assert_eq!(first["data"]["state"], "queued");
    assert_eq!(first["data"]["revision"], 1);
    assert_eq!(first["data"]["replayed"], false);
    let work_item_id = first["data"]["work_item_id"].as_str().unwrap().to_owned();
    let notification: (String, String, String) = sqlx::query_as(
        "SELECT to_agent_id, from_agent_id, content FROM mailbox \
         WHERE team_id = ? AND idempotency_scope = 'team_work_event' \
           AND to_agent_id = 'worker' AND content LIKE '%is queued for you%'",
    )
    .bind(TEAM_ID)
    .fetch_one(services.database.pool())
    .await
    .expect("delegation notification is durable before the response");
    assert_eq!(notification.0, "worker");
    assert_eq!(notification.1, "lead");
    assert!(notification.2.contains(&work_item_id));

    let replay = app
        .clone()
        .oneshot(json_with_token("POST", &endpoint, delegate_body(), &token, &csrf))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let replay = body_json(replay).await;
    assert_eq!(replay["data"]["work_item_id"], work_item_id);
    assert_eq!(replay["data"]["replayed"], true);
    let notification_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM mailbox \
         WHERE team_id = ? AND idempotency_scope = 'team_work_event' \
           AND to_agent_id = 'worker' AND content LIKE '%is queued for you%'",
    )
    .bind(TEAM_ID)
    .fetch_one(services.database.pool())
    .await
    .unwrap();
    assert_eq!(notification_count, 1);

    let listed = app
        .clone()
        .oneshot(get_with_token(&format!("/api/teams/{TEAM_ID}/work-items"), &token))
        .await
        .unwrap();
    let listed = body_json(listed).await;
    assert_eq!(listed["data"].as_array().unwrap().len(), 1);

    let cancel_endpoint = format!("/api/teams/{TEAM_ID}/work-items/{work_item_id}/cancel");
    let cancel_body = json!({
        "idempotency_key": "cancel-http-1",
        "expected_work_revision": 1
    });
    let cancelled = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &cancel_endpoint,
            cancel_body.clone(),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(cancelled.status(), StatusCode::OK);
    let cancelled = body_json(cancelled).await;
    assert_eq!(cancelled["data"]["state"], "cancelled");
    let cancel_notification: String = sqlx::query_scalar(
        "SELECT content FROM mailbox \
         WHERE team_id = ? AND idempotency_scope = 'team_work_event' \
           AND to_agent_id = 'worker' AND content LIKE '%was cancelled%'",
    )
    .bind(TEAM_ID)
    .fetch_one(services.database.pool())
    .await
    .expect("cancel notification is durable");
    assert!(cancel_notification.contains(&work_item_id));

    let replayed_cancel = app
        .oneshot(json_with_token("POST", &cancel_endpoint, cancel_body, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(replayed_cancel.status(), StatusCode::OK);
    assert_eq!(body_json(replayed_cancel).await["data"]["replayed"], true);
}

#[tokio::test]
async fn request_changes_durably_notifies_the_assignee_once() {
    let (mut app, services) = build_app().await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_command_team(&services, TEAM_ID, "system_default_user").await;
    seed_inline_submission(&services, TEAM_ID, "work-changes").await;

    let endpoint = format!("/api/teams/{TEAM_ID}/work-items/work-changes/review");
    let request = json!({
        "idempotency_key": "request-changes-http-1",
        "expected_work_revision": 3,
        "decision": "request_changes",
        "feedback": "  Tighten the evidence table and rerun the focused check.  "
    });
    let changed = app
        .clone()
        .oneshot(json_with_token("POST", &endpoint, request.clone(), &token, &csrf))
        .await
        .unwrap();
    assert_eq!(changed.status(), StatusCode::OK);
    let changed = body_json(changed).await;
    assert_eq!(changed["data"]["state"], "changes_requested");

    let notification: (String, String, String) = sqlx::query_as(
        "SELECT to_agent_id, from_agent_id, content FROM mailbox \
         WHERE team_id = ? AND idempotency_scope = 'team_work_event' \
           AND to_agent_id = 'worker' AND content LIKE '%work-changes%'",
    )
    .bind(TEAM_ID)
    .fetch_one(services.database.pool())
    .await
    .expect("request_changes notification is durable");
    assert_eq!(notification.0, "worker");
    assert_eq!(notification.1, "lead");
    assert!(notification.2.contains("work-changes"));
    assert!(
        notification
            .2
            .contains("Reviewer feedback:\nTighten the evidence table and rerun the focused check.")
    );
    assert!(!notification.2.contains("  Tighten"));

    let replay = app
        .clone()
        .oneshot(json_with_token("POST", &endpoint, request.clone(), &token, &csrf))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let changed_feedback = app
        .oneshot(json_with_token(
            "POST",
            &endpoint,
            json!({
                "idempotency_key": "request-changes-http-1",
                "expected_work_revision": 3,
                "decision": "request_changes",
                "feedback": "A different message cannot reuse this request key."
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(changed_feedback.status(), StatusCode::CONFLICT);
    let notification_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM mailbox \
         WHERE team_id = ? AND idempotency_scope = 'team_work_event' \
           AND to_agent_id = 'worker' AND content LIKE '%work-changes%'",
    )
    .bind(TEAM_ID)
    .fetch_one(services.database.pool())
    .await
    .unwrap();
    assert_eq!(notification_count, 1);
}

#[tokio::test]
async fn review_feedback_contract_rejects_invalid_payloads_before_mutation() {
    let (mut app, services) = build_app().await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_command_team(&services, TEAM_ID, "system_default_user").await;
    seed_inline_submission(&services, TEAM_ID, "work-feedback-validation").await;

    let endpoint = format!("/api/teams/{TEAM_ID}/work-items/work-feedback-validation/review");
    let invalid_requests = [
        json!({
            "idempotency_key": "feedback-missing",
            "expected_work_revision": 3,
            "decision": "request_changes"
        }),
        json!({
            "idempotency_key": "feedback-blank",
            "expected_work_revision": 3,
            "decision": "request_changes",
            "feedback": " \n "
        }),
        json!({
            "idempotency_key": "feedback-too-long",
            "expected_work_revision": 3,
            "decision": "request_changes",
            "feedback": "x".repeat(4001)
        }),
        json!({
            "idempotency_key": "feedback-on-accept",
            "expected_work_revision": 3,
            "decision": "accept",
            "feedback": "not allowed"
        }),
        json!({
            "idempotency_key": "feedback-on-reject",
            "expected_work_revision": 3,
            "decision": "reject",
            "feedback": "not allowed"
        }),
    ];
    for request in invalid_requests {
        let response = app
            .clone()
            .oneshot(json_with_token("POST", &endpoint, request, &token, &csrf))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    let state: (String, i64) = sqlx::query_as("SELECT state, revision FROM team_work_items WHERE id = ?")
        .bind("work-feedback-validation")
        .fetch_one(services.database.pool())
        .await
        .unwrap();
    assert_eq!(state, ("submitted".to_owned(), 3));

    seed_inline_submission(&services, TEAM_ID, "work-accept-blank-feedback").await;
    let accepted = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/teams/{TEAM_ID}/work-items/work-accept-blank-feedback/review"),
            json!({
                "idempotency_key": "accept-blank-feedback",
                "expected_work_revision": 3,
                "decision": "accept",
                "feedback": " \n "
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
    assert_eq!(body_json(accepted).await["data"]["state"], "completed");

    seed_inline_submission(&services, TEAM_ID, "work-reject-blank-feedback").await;
    let rejected = app
        .oneshot(json_with_token(
            "POST",
            &format!("/api/teams/{TEAM_ID}/work-items/work-reject-blank-feedback/review"),
            json!({
                "idempotency_key": "reject-blank-feedback",
                "expected_work_revision": 3,
                "decision": "reject",
                "feedback": " \n "
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::OK);
    assert_eq!(body_json(rejected).await["data"]["state"], "rejected");
}

#[tokio::test]
async fn public_commands_reject_identity_fields_and_cross_user_access() {
    let (mut app, services) = build_app().await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_command_team(&services, TEAM_ID, "system_default_user").await;
    seed_command_team(&services, "foreign-team", "another-user").await;

    let forged = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &format!("/api/teams/{TEAM_ID}/work-items/delegate"),
            json!({
                "idempotency_key": "forged-1",
                "subject": "Try to impersonate",
                "assignee_member_id": "worker",
                "delivery_requirement": "none",
                "actor_member_id": "worker"
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(forged.status(), StatusCode::BAD_REQUEST);

    let foreign = app
        .oneshot(json_with_token(
            "POST",
            "/api/teams/foreign-team/work-items/delegate",
            delegate_body(),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(foreign.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn one_public_review_completes_an_inline_submission_and_replays() {
    let (mut app, services) = build_app().await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_command_team(&services, TEAM_ID, "system_default_user").await;
    seed_inline_submission(&services, TEAM_ID, "work-review").await;

    let endpoint = format!("/api/teams/{TEAM_ID}/work-items/work-review/review");
    let request = json!({
        "idempotency_key": "review-http-1",
        "expected_work_revision": 3,
        "decision": "accept"
    });
    let completed = app
        .clone()
        .oneshot(json_with_token("POST", &endpoint, request.clone(), &token, &csrf))
        .await
        .unwrap();
    assert_eq!(completed.status(), StatusCode::OK);
    let completed = body_json(completed).await;
    assert_eq!(completed["data"]["state"], "completed");
    assert_eq!(completed["data"]["revision"], 6);

    let replay = app
        .oneshot(json_with_token("POST", &endpoint, request, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let replay = body_json(replay).await;
    assert_eq!(replay["data"]["state"], "completed");
    assert_eq!(replay["data"]["replayed"], true);
}

#[tokio::test]
async fn public_review_recovers_after_begin_or_decision_process_loss() {
    let (mut app, services) = build_app().await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_command_team(&services, TEAM_ID, "system_default_user").await;
    seed_inline_state(&services, TEAM_ID, "work-reviewing", "reviewing", 4).await;
    seed_inline_state(&services, TEAM_ID, "work-accepted", "accepted", 5).await;

    let reviewing_endpoint = format!("/api/teams/{TEAM_ID}/work-items/work-reviewing/review");
    let reviewing = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &reviewing_endpoint,
            json!({
                "idempotency_key": "review-recovering-1",
                "expected_work_revision": 4,
                "decision": "accept"
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(reviewing.status(), StatusCode::OK);
    assert_eq!(body_json(reviewing).await["data"]["state"], "completed");

    let accepted_endpoint = format!("/api/teams/{TEAM_ID}/work-items/work-accepted/review");
    let request_changes = app
        .clone()
        .oneshot(json_with_token(
            "POST",
            &accepted_endpoint,
            json!({
                "idempotency_key": "review-recovering-request-changes",
                "expected_work_revision": 5,
                "decision": "request_changes",
                "feedback": "This recovery state must remain Accept-only."
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(request_changes.status(), StatusCode::CONFLICT);

    let accepted = app
        .oneshot(json_with_token(
            "POST",
            &accepted_endpoint,
            json!({
                "idempotency_key": "review-recovering-2",
                "expected_work_revision": 5,
                "decision": "accept"
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(accepted.status(), StatusCode::OK);
    assert_eq!(body_json(accepted).await["data"]["state"], "completed");
}
