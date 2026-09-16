mod common;

use aionui_app::{AppConfig, AppServices, build_module_states, create_router_with_states};
use aionui_db::{ITeamRepository, SqliteTeamRepository, init_database_memory, models::TeamRow};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use common::{body_json, get_request, get_with_token, json_with_token, setup_and_login};
use ed25519_dalek::{Signer, SigningKey};
use serde_json::{Value, json};
use tower::ServiceExt;

const ENDPOINT: &str = "/api/teams/legacy/coordination-protocol/migration";

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

async fn app() -> (Router, AppServices, SigningKey, tempfile::TempDir) {
    let root = tempfile::tempdir().unwrap();
    let config = AppConfig {
        data_dir: root.path().into(),
        work_dir: root.path().into(),
        ..Default::default()
    };
    let services = AppServices::from_config(init_database_memory().await.unwrap(), &config)
        .await
        .unwrap();
    let (states, _) = build_module_states(&services).await.unwrap();
    let key = SigningKey::from_bytes(&[19; 32]);
    states
        .team
        .service
        .configure_coordination_migration_keys(
            &json!([{
                "user_id":"system_default_user", "key_id":"trusted", "public_key":hex(key.verifying_key().as_bytes()),
            }])
            .to_string(),
        )
        .unwrap();
    let app = create_router_with_states(&services, states);
    (app, services, key, root)
}

async fn seed(services: &AppServices, owner: &str) {
    SqliteTeamRepository::new(services.database.pool().clone()).create_team(&TeamRow {
        coordination_protocol: Some(r#"{"kind":"legacy_managed_migration_required"}"#.into()),
        id: "legacy".into(), user_id: owner.into(), name: "Legacy".into(), workspace: String::new(),
        workspace_mode: "shared".into(),
        agents: r#"[{"slot_id":"lead","name":"Lead","role":"lead","conversation_id":"conversation-1","backend":"codex","model":"test"}]"#.into(),
        lead_agent_id: Some("lead".into()), session_mode: None, agents_version: "1.0.1".into(), created_at: 1, updated_at: 1,
    }).await.unwrap();
}

fn signed_request(snapshot: &Value, key: &SigningKey) -> Value {
    let mut request = json!({
        "schema":"aionui.team.coordination-migration.v1", "migration_id":"migration-1",
        "team_id":"legacy", "user_id":"system_default_user", "profile_id":"mathematics-research-team",
        "snapshot_digest": snapshot["data"]["snapshot_digest"], "source_binding_digest":"b".repeat(64),
        "key_id":"trusted",
    });
    let fields = [
        "schema",
        "migration_id",
        "team_id",
        "user_id",
        "profile_id",
        "snapshot_digest",
        "source_binding_digest",
        "key_id",
    ];
    let mut payload = b"aionui.team.coordination-migration-proof.v1\0".to_vec();
    payload.extend(serde_json::to_vec(&fields.map(|field| request[field].as_str().unwrap())).unwrap());
    request["signature"] = json!(hex(&key.sign(&payload).to_bytes()));
    request
}

#[tokio::test]
async fn owner_can_apply_a_signed_migration_and_replay_the_exact_receipt() {
    let (mut app, services, key, _root) = app().await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed(&services, "system_default_user").await;
    let response = app.clone().oneshot(get_with_token(ENDPOINT, &token)).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let request = signed_request(&body_json(response).await, &key);
    let response = app
        .clone()
        .oneshot(json_with_token("POST", ENDPOINT, request.clone(), &token, &csrf))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let receipt = body_json(response).await;
    let replay = app
        .oneshot(json_with_token("POST", ENDPOINT, request, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    assert_eq!(body_json(replay).await, receipt);
    assert_eq!(
        receipt["data"]["coordination_protocol"],
        json!({"kind":"managed_mcp","logicalTool":"research_team"})
    );
}

#[tokio::test]
async fn migration_requires_authentication_csrf_and_team_ownership() {
    let (mut app, services, key, _root) = app().await;
    let response = app.clone().oneshot(get_request(ENDPOINT)).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let without_csrf = Request::builder()
        .method("POST")
        .uri(ENDPOINT)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from("{}"))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(without_csrf).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );
    seed(&services, "another-owner").await;
    assert_eq!(
        app.clone()
            .oneshot(get_with_token(ENDPOINT, &token))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let request = signed_request(&json!({"data":{"snapshot_digest":"a".repeat(64)}}), &key);
    let response = app
        .oneshot(json_with_token("POST", ENDPOINT, request, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let error = body_json(response).await;
    assert_eq!(error["code"], "FORBIDDEN");
    assert_eq!(error["error"], "Forbidden.");
}

#[tokio::test]
async fn missing_team_and_oversized_identity_are_rejected_without_an_audit() {
    let (mut app, services, key, _root) = app().await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    assert_eq!(
        app.clone()
            .oneshot(get_with_token(ENDPOINT, &token))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    let request = signed_request(&json!({"data":{"snapshot_digest":"a".repeat(64)}}), &key);
    assert_eq!(
        app.clone()
            .oneshot(json_with_token("POST", ENDPOINT, request, &token, &csrf))
            .await
            .unwrap()
            .status(),
        StatusCode::NOT_FOUND
    );
    seed(&services, "system_default_user").await;
    let snapshot = body_json(app.clone().oneshot(get_with_token(ENDPOINT, &token)).await.unwrap()).await;
    let mut oversized = signed_request(&snapshot, &key);
    oversized["migration_id"] = json!("x".repeat(129));
    let response = app
        .oneshot(json_with_token("POST", ENDPOINT, oversized, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(
        body_json(response)
            .await
            .to_string()
            .contains("TEAM_COORDINATION_MIGRATION_INVALID")
    );
    assert!(
        SqliteTeamRepository::new(services.database.pool().clone())
            .get_coordination_migration("legacy")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn malformed_and_forged_proofs_leave_the_legacy_team_quarantined() {
    let (mut app, services, key, _root) = app().await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed(&services, "system_default_user").await;
    let snapshot = body_json(app.clone().oneshot(get_with_token(ENDPOINT, &token)).await.unwrap()).await;
    let mut forged = signed_request(&snapshot, &key);
    forged["signature"] = json!("0".repeat(128));
    assert_eq!(
        app.clone()
            .oneshot(json_with_token("POST", ENDPOINT, forged, &token, &csrf))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let mut unknown_field = signed_request(&snapshot, &key);
    unknown_field["public_key"] = json!("request-supplied trust is prohibited");
    assert_eq!(
        app.clone()
            .oneshot(json_with_token("POST", ENDPOINT, unknown_field, &token, &csrf))
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    let repo = SqliteTeamRepository::new(services.database.pool().clone());
    assert!(repo.get_coordination_migration("legacy").await.unwrap().is_none());
}
