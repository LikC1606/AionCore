mod common;

use std::sync::Arc;

use aionui_ai_agent::WorkerTaskManagerImpl;
use aionui_app::{AppConfig, AppServices, create_router};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use tower::ServiceExt;

use common::{body_json, json_with_token, setup_and_login};

const HOST_HEADER: &str = "x-deepscientist-math-host-secret";

async fn fixture(local: bool) -> (Router, AppServices, tempfile::TempDir) {
    let root = tempfile::tempdir().unwrap();
    let db = aionui_db::init_database_memory().await.unwrap();
    let config = AppConfig {
        data_dir: root.path().join("data"),
        work_dir: root.path().join("work"),
        local,
        ..Default::default()
    };
    std::fs::create_dir_all(&config.work_dir).unwrap();
    let tasks = Arc::new(WorkerTaskManagerImpl::new(Arc::new(|_| {
        panic!("registration must not launch an agent")
    })));
    let mut services = AppServices::from_config(db, &config)
        .await
        .unwrap()
        .with_worker_task_manager(tasks);
    services.conversation_service = services
        .conversation_service
        .clone()
        .with_math_budget_host_secret(Some("a".repeat(64)))
        .unwrap();
    let router = create_router(&services).await.unwrap();
    (router, services, root)
}

fn binding() -> Value {
    json!({
        "run_id": "test-run", "input_digest": "b".repeat(64), "actor_id": "lead",
        "socket_path": "/tmp/math-budget-route-fixture.sock", "secret": "c".repeat(64),
        "output_tokens": 1024, "provider": "WestlakeHPC", "model": "deepseek-flash"
    })
}

fn with_host(mut request: Request<Body>) -> Request<Body> {
    request
        .headers_mut()
        .insert(HOST_HEADER, "a".repeat(64).parse().unwrap());
    request
}

async fn create_conversation(services: &AppServices) -> String {
    let request = serde_json::from_value(json!({
        "type": "acp", "extra": { "workspace": services.work_dir.to_string_lossy() }
    }))
    .unwrap();
    services
        .conversation_service
        .create("system_default_user", request)
        .await
        .unwrap()
        .id
}

#[tokio::test]
async fn math_budget_route_register_duplicate_revoke_never_echoes_secrets() {
    let (mut app, services, _root) = fixture(false).await;
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let id = create_conversation(&services).await;
    let path = format!("/api/conversations/{id}/math-budget-binding");

    let response = app
        .clone()
        .oneshot(with_host(json_with_token("POST", &path, binding(), &token, &csrf)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        body_json(response).await,
        json!({"data": {"ok": true}, "success": true})
    );
    let stored = serde_json::to_string(
        &services
            .conversation_service
            .get("system_default_user", &id)
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(!stored.contains(&"c".repeat(64)));
    assert!(!stored.contains("math-budget-route-fixture.sock"));

    let response = app
        .clone()
        .oneshot(with_host(json_with_token("POST", &path, binding(), &token, &csrf)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(
        body_json(response)
            .await
            .to_string()
            .contains("math_budget_binding_already_registered")
    );

    let response = app
        .clone()
        .oneshot(with_host(json_with_token("DELETE", &path, Value::Null, &token, &csrf)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .oneshot(with_host(json_with_token("POST", &path, binding(), &token, &csrf)))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    assert!(
        body_json(response)
            .await
            .to_string()
            .contains("math_budget_binding_conversation_not_registerable")
    );
}

#[tokio::test]
async fn math_budget_route_requires_login_host_auth_and_csrf() {
    let (mut app, services, _root) = fixture(false).await;
    let id = create_conversation(&services).await;
    let path = format!("/api/conversations/{id}/math-budget-binding");
    let request = Request::builder()
        .method("POST")
        .uri(&path)
        .header("content-type", "application/json")
        .body(Body::from(binding().to_string()))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(with_host(request)).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );

    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    let response = app
        .clone()
        .oneshot(json_with_token("POST", &path, binding(), &token, &csrf))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        body_json(response)
            .await
            .to_string()
            .contains("math_budget_host_unauthorized")
    );

    let mut request = with_host(json_with_token("POST", &path, binding(), &token, &csrf));
    request.headers_mut().remove("x-csrf-token");
    assert_eq!(app.oneshot(request).await.unwrap().status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn math_budget_route_hides_other_users_conversations_and_checks_host_in_local_mode() {
    let (mut app, services, _root) = fixture(false).await;
    let id = create_conversation(&services).await;
    let path = format!("/api/conversations/{id}/math-budget-binding");
    let (token, csrf) = setup_and_login(&mut app, &services, "other", "StrongP@ss1").await;
    for method in ["POST", "DELETE"] {
        let response = app
            .clone()
            .oneshot(with_host(json_with_token(method, &path, binding(), &token, &csrf)))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    let (app, services, _root) = fixture(true).await;
    let id = create_conversation(&services).await;
    let request = Request::builder()
        .method("POST")
        .uri(format!("/api/conversations/{id}/math-budget-binding"))
        .header("content-type", "application/json")
        .body(Body::from(binding().to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        body_json(response)
            .await
            .to_string()
            .contains("math_budget_host_unauthorized")
    );
}
