mod common;

use std::fs;

use aionui_app::{AppConfig, AppServices, create_router};
use aionui_common::now_ms;
use aionui_db::models::TeamRow;
use aionui_db::{ITeamRepository, SqliteTeamRepository};
use aionui_file::git_delivery::{ExactRepository, GitDeliveryAdapter, GitDeliveryError, PrepareMemberWorktree};
use aionui_team::kernel::{GitDeliveryRef, WorkSubmission};
use aionui_team::{TeamAgent, TeammateRole};
use axum::http::StatusCode;
use git2::{IndexAddOption, Oid, Repository, Signature};
use serde_json::json;
use tempfile::TempDir;
use tower::ServiceExt;

use common::{body_json, json_with_token, setup_and_login};

const TEAM_ID: &str = "team-git-http";
const WORK_ID: &str = "work-git-http";
const DELIVERY_ID: &str = "delivery-git-http";

#[tokio::test]
async fn owner_integrates_exact_accepted_delivery_and_replay_is_idempotent() {
    let repository_dir = TempDir::new().unwrap();
    let member_dir = TempDir::new().unwrap();
    let app_dir = TempDir::new().unwrap();
    let repository = Repository::init(repository_dir.path()).unwrap();
    fs::write(repository_dir.path().join("base.txt"), "base\n").unwrap();
    let base = commit_all(&repository, "base");

    let adapter = GitDeliveryAdapter::new();
    let (identity, target) = adapter.resolve_current_worktree(repository_dir.path()).unwrap();
    let source_ref = "refs/heads/ds/team/http-worker".to_owned();
    let source_path = member_dir.path().join("worker");
    adapter
        .prepare_member_worktree(&PrepareMemberWorktree {
            repository: ExactRepository::from(&identity),
            worktree_name: "ds-http-worker".into(),
            worktree_path: source_path.clone(),
            branch_ref: source_ref.clone(),
            base_head: base,
        })
        .unwrap();
    let source_repository = Repository::open(&source_path).unwrap();
    fs::write(source_path.join("worker.txt"), "worker result\n").unwrap();
    let source_head = commit_all(&source_repository, "worker result");

    let database = aionui_db::init_database_memory().await.unwrap();
    let services = AppServices::from_config(
        database,
        &AppConfig {
            data_dir: app_dir.path().join("data"),
            work_dir: app_dir.path().join("work"),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    #[cfg(unix)]
    let runtime_skill_source = {
        let source = services.skill_paths.user_skills_dir.join("team-runtime");
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("SKILL.md"), "# Team runtime Skill\n").unwrap();
        source
    };

    let mut app = create_router(&services).await.unwrap();
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_team(&services, identity.repository_id()).await;
    seed_accepted_delivery(&services, identity.repository_id(), base, &source_ref, source_head).await;

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        for worktree in [repository_dir.path(), source_path.as_path()] {
            let link = worktree.join(".codex/skills/team-runtime");
            fs::create_dir_all(link.parent().unwrap()).unwrap();
            symlink(&runtime_skill_source, link).unwrap();
            assert!(matches!(
                GitDeliveryAdapter::new()
                    .resolve_current_worktree(worktree)
                    .unwrap_err(),
                GitDeliveryError::DirtyWorktree { .. }
            ));
        }
    }

    let endpoint = format!("/api/teams/{TEAM_ID}/work-items/{WORK_ID}/integrate");
    let request = json!({
        "idempotency_key": "integrate-http-1",
        "expected_work_revision": 5,
        "expected_delivery_revision": 1
    });
    let first = app
        .clone()
        .oneshot(json_with_token("POST", &endpoint, request.clone(), &token, &csrf))
        .await
        .unwrap();
    assert_eq!(first.status(), StatusCode::OK);
    let first = body_json(first).await;
    assert_eq!(first["data"]["resolution"], "merged");
    let merge_head = Oid::from_str(first["data"]["merged_commit"].as_str().unwrap()).unwrap();

    let target_repository = Repository::open(repository_dir.path()).unwrap();
    assert_eq!(target_repository.refname_to_id(&target.branch_ref).unwrap(), merge_head);
    let merge_commit = target_repository.find_commit(merge_head).unwrap();
    assert_eq!(merge_commit.parent_count(), 2);
    assert_eq!(merge_commit.parent_id(0).unwrap(), target.head);
    assert_eq!(merge_commit.parent_id(1).unwrap(), source_head);

    let replay = app
        .oneshot(json_with_token("POST", &endpoint, request, &token, &csrf))
        .await
        .unwrap();
    assert_eq!(replay.status(), StatusCode::OK);
    let replay = body_json(replay).await;
    assert_eq!(replay["data"]["attempt_id"], first["data"]["attempt_id"]);
    assert_eq!(replay["data"]["merged_commit"], first["data"]["merged_commit"]);
    assert_eq!(target_repository.refname_to_id(&target.branch_ref).unwrap(), merge_head);
}

#[cfg(unix)]
#[tokio::test]
async fn owner_delegates_git_work_with_managed_codex_skill_links_present() {
    use std::os::unix::fs::symlink;

    let repository_dir = TempDir::new().unwrap();
    let app_dir = TempDir::new().unwrap();
    let repository = Repository::init(repository_dir.path()).unwrap();
    fs::write(repository_dir.path().join("base.txt"), "base\n").unwrap();
    let base = commit_all(&repository, "base");
    let (identity, _) = GitDeliveryAdapter::new()
        .resolve_current_worktree(repository_dir.path())
        .unwrap();

    let database = aionui_db::init_database_memory().await.unwrap();
    let services = AppServices::from_config(
        database,
        &AppConfig {
            data_dir: app_dir.path().join("data"),
            work_dir: app_dir.path().join("work"),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let runtime_skill_source = services.skill_paths.user_skills_dir.join("team-runtime");
    fs::create_dir_all(&runtime_skill_source).unwrap();
    fs::write(runtime_skill_source.join("SKILL.md"), "# Team runtime Skill\n").unwrap();
    let skill_link = repository_dir.path().join(".codex/skills/team-runtime");
    fs::create_dir_all(skill_link.parent().unwrap()).unwrap();
    symlink(&runtime_skill_source, &skill_link).unwrap();
    assert!(matches!(
        GitDeliveryAdapter::new()
            .resolve_current_worktree(repository_dir.path())
            .unwrap_err(),
        GitDeliveryError::DirtyWorktree { .. }
    ));

    let mut app = create_router(&services).await.unwrap();
    let (token, csrf) = setup_and_login(&mut app, &services, "admin", "StrongP@ss1").await;
    seed_team(&services, identity.repository_id()).await;
    let response = app
        .oneshot(json_with_token(
            "POST",
            &format!("/api/teams/{TEAM_ID}/work-items/delegate"),
            json!({
                "idempotency_key": "delegate-git-runtime-skills",
                "subject": "Implement in an isolated Team worktree",
                "assignee_member_id": "worker",
                "delivery_requirement": "git"
            }),
            &token,
            &csrf,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = body_json(response).await;
    assert_eq!(response["data"]["state"], "queued");

    let assignment: (String, String, String) =
        sqlx::query_as("SELECT git_repository_id, git_base_commit, git_branch_ref FROM team_work_items WHERE id = ?")
            .bind(response["data"]["work_item_id"].as_str().unwrap())
            .fetch_one(services.database.pool())
            .await
            .unwrap();
    assert_eq!(assignment.0, identity.repository_id());
    assert_eq!(assignment.1, base.to_string());
    assert!(assignment.2.starts_with("refs/heads/ds/team/"));

    let notification: (String, String, String, String) = sqlx::query_as(
        "SELECT content, idempotency_scope, idempotency_key, request_fingerprint \
         FROM mailbox WHERE team_id = ? AND to_agent_id = ?",
    )
    .bind(TEAM_ID)
    .bind("worker")
    .fetch_one(services.database.pool())
    .await
    .unwrap();
    assert!(notification.0.contains("prepared Git workspace"));
    assert_eq!(notification.1, "team_work_event");
    assert!(notification.2.ends_with(":0"));
    assert!(!notification.3.is_empty());
}

async fn seed_team(services: &aionui_app::AppServices, workspace: &str) {
    let agents = vec![
        member("lead", "conv-lead", TeammateRole::Lead),
        member("worker", "conv-worker", TeammateRole::Teammate),
    ];
    SqliteTeamRepository::new(services.database.pool().clone())
        .create_team(&TeamRow {
            coordination_protocol: None,
            id: TEAM_ID.into(),
            user_id: "system_default_user".into(),
            name: "Git HTTP Team".into(),
            workspace: workspace.into(),
            workspace_mode: "shared".into(),
            agents: serde_json::to_string(&agents).unwrap(),
            lead_agent_id: Some("lead".into()),
            session_mode: None,
            agents_version: "1".into(),
            created_at: now_ms(),
            updated_at: now_ms(),
        })
        .await
        .unwrap();
}

async fn seed_accepted_delivery(
    services: &aionui_app::AppServices,
    repository_id: &str,
    base: Oid,
    branch_ref: &str,
    head: Oid,
) {
    let delivery = GitDeliveryRef::new(
        DELIVERY_ID,
        TEAM_ID,
        WORK_ID,
        "worker",
        repository_id,
        1,
        base.to_string(),
        branch_ref,
        head.to_string(),
    );
    let submission = serde_json::to_string(&WorkSubmission::git(delivery.clone())).unwrap();
    let accepted = serde_json::to_string(&delivery).unwrap();
    let now = now_ms();
    sqlx::query(
        "INSERT INTO team_work_items (\
            id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
            integrator_member_id, delivery_requirement, git_repository_id, git_base_commit, git_branch_ref, \
            state, current_submission_json, accepted_delivery_json, revision, created_at, updated_at\
         ) VALUES (?, ?, 'Integrate exact HTTP delivery', 'lead', 'worker', 'lead', 'lead', 'git', ?, ?, ?, \
                   'accepted', ?, ?, 5, ?, ?)",
    )
    .bind(WORK_ID)
    .bind(TEAM_ID)
    .bind(repository_id)
    .bind(base.to_string())
    .bind(branch_ref)
    .bind(submission)
    .bind(accepted)
    .bind(now)
    .bind(now)
    .execute(services.database.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO team_git_deliveries (\
            id, team_id, work_item_id, producer_member_id, repository_id, content_revision, base_commit, \
            branch_ref, head_commit, state, revision, created_at, updated_at\
         ) VALUES (?, ?, ?, 'worker', ?, 1, ?, ?, ?, 'accepted', 1, ?, ?)",
    )
    .bind(DELIVERY_ID)
    .bind(TEAM_ID)
    .bind(WORK_ID)
    .bind(repository_id)
    .bind(base.to_string())
    .bind(branch_ref)
    .bind(head.to_string())
    .bind(now)
    .bind(now)
    .execute(services.database.pool())
    .await
    .unwrap();
}

fn member(slot_id: &str, conversation_id: &str, role: TeammateRole) -> TeamAgent {
    TeamAgent {
        slot_id: slot_id.into(),
        name: slot_id.into(),
        role,
        conversation_id: conversation_id.into(),
        backend: "mock".into(),
        model: String::new(),
        assistant_id: None,
        status: None,
        conversation_type: None,
        cli_path: None,
    }
}

fn commit_all(repository: &Repository, message: &str) -> Oid {
    let mut index = repository.index().unwrap();
    index.add_all(["*"], IndexAddOption::DEFAULT, None).unwrap();
    index.write().unwrap();
    let tree_id = index.write_tree().unwrap();
    let tree = repository.find_tree(tree_id).unwrap();
    let signature = Signature::now("Team Test", "team-test@example.invalid").unwrap();
    let parents = repository
        .head()
        .ok()
        .and_then(|head| head.target())
        .map(|head| repository.find_commit(head).unwrap());
    let parent_refs = parents.iter().collect::<Vec<_>>();
    repository
        .commit(Some("HEAD"), &signature, &signature, message, &tree, &parent_refs)
        .unwrap()
}
