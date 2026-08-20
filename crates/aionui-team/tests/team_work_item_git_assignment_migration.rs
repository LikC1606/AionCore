use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;

use aionui_api_types::TeamGitWorkAssignmentResponse;
use aionui_db::{SqliteTeamModeRepository, SqliteTeamRepository};
use aionui_team::TeamQueryService;
use sqlx::migrate::Migrator;
use sqlx::sqlite::SqlitePoolOptions;

const USER_ID: &str = "migration-owner";
const TEAM_ID: &str = "migration-team";
const MIGRATION_031_SHA384: &str =
    "9369ec9d21a86ce8306cc9e56f701d2ee74abd7782bff9f27c700c074c98252c96d6f5b8cfbb5157ff9679a0989cd26b";

async fn migrator() -> Migrator {
    let migrations_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../aionui-db/migrations");
    Migrator::new(migrations_path).await.unwrap()
}

async fn run_migrations_through(pool: &sqlx::SqlitePool, max_version: i64) {
    let full = migrator().await;
    let migrations = full
        .migrations
        .iter()
        .filter(|migration| migration.version <= max_version)
        .cloned()
        .collect::<Vec<_>>();
    let migrator = Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: false,
        locking: true,
        no_tx: false,
    };

    migrator.run(pool).await.unwrap();
}

async fn seed_owner_and_team(pool: &sqlx::SqlitePool) {
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, created_at, updated_at) \
         VALUES (?, 'migration-owner', '', 1, 1)",
    )
    .bind(USER_ID)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO teams \
            (id, user_id, name, workspace, workspace_mode, agents, agents_version, created_at, updated_at) \
         VALUES (?, ?, 'Migration Team', '', 'shared', '[]', '1.0.1', 1, 1)",
    )
    .bind(TEAM_ID)
    .bind(USER_ID)
    .execute(pool)
    .await
    .unwrap();
}

fn query_service(pool: &sqlx::SqlitePool) -> TeamQueryService {
    TeamQueryService::new(
        Arc::new(SqliteTeamRepository::new(pool.clone())),
        Arc::new(SqliteTeamModeRepository::new(pool.clone())),
    )
}

#[tokio::test]
async fn migration_031_keeps_the_published_checksum() {
    let migration = migrator()
        .await
        .migrations
        .iter()
        .find(|migration| migration.version == 31)
        .cloned()
        .expect("migration 031 must exist");
    let checksum = migration
        .checksum
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    assert_eq!(checksum, MIGRATION_031_SHA384);
}

#[tokio::test]
async fn database_persisted_at_migration_031_upgrades_without_checksum_failure() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();

    run_migrations_through(&pool, 31).await;
    run_migrations_through(&pool, i64::MAX).await;

    let latest: i64 = sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(latest >= 34);
}

#[tokio::test]
async fn fresh_database_enforces_assignment_constraint_and_restores_both_delivery_modes() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    run_migrations_through(&pool, i64::MAX).await;
    seed_owner_and_team(&pool).await;

    sqlx::query(
        "INSERT INTO team_work_items (\
            id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
            delivery_requirement, state, revision, created_at, updated_at\
         ) VALUES ('fresh-inline', ?, 'Inline', 'lead', 'worker', 'lead', \
                   'none', 'draft', 0, 1, 1)",
    )
    .bind(TEAM_ID)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO team_work_items (\
            id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
            integrator_member_id, delivery_requirement, git_repository_id, git_base_commit, \
            git_branch_ref, state, revision, created_at, updated_at\
         ) VALUES ('fresh-git', ?, 'Git', 'lead', 'worker', 'lead', 'lead', 'git', \
                   'repo-fresh', 'base-fresh', 'refs/heads/fresh', 'draft', 0, 1, 1)",
    )
    .bind(TEAM_ID)
    .execute(&pool)
    .await
    .unwrap();

    let work_items = query_service(&pool).list_work_items(USER_ID, TEAM_ID).await.unwrap();
    assert_eq!(work_items.len(), 2);
    let inline = work_items.iter().find(|work| work.id == "fresh-inline").unwrap();
    assert_eq!(inline.delivery_requirement, "none");
    assert!(inline.git_assignment.is_none());
    let git = work_items.iter().find(|work| work.id == "fresh-git").unwrap();
    assert_eq!(git.delivery_requirement, "git");
    assert_eq!(
        git.git_assignment,
        Some(TeamGitWorkAssignmentResponse {
            base_commit: "base-fresh".into(),
            branch_ref: "refs/heads/fresh".into(),
        })
    );

    let invalid_none_with_assignment = sqlx::query(
        "INSERT INTO team_work_items (\
            id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
            delivery_requirement, git_repository_id, state, revision, created_at, updated_at\
         ) VALUES ('invalid-none', ?, 'Invalid', 'lead', 'worker', 'lead', \
                   'none', 'invented-repository', 'draft', 0, 1, 1)",
    )
    .bind(TEAM_ID)
    .execute(&pool)
    .await;
    assert!(invalid_none_with_assignment.is_err());
    let invalid_git_without_assignment = sqlx::query(
        "INSERT INTO team_work_items (\
            id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
            delivery_requirement, state, revision, created_at, updated_at\
         ) VALUES ('invalid-git', ?, 'Invalid', 'lead', 'worker', 'lead', \
                   'git', 'draft', 0, 1, 1)",
    )
    .bind(TEAM_ID)
    .execute(&pool)
    .await;
    assert!(invalid_git_without_assignment.is_err());

    let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info('team_work_items')")
        .fetch_all(&pool)
        .await
        .unwrap();
    for expected in ["git_repository_id", "git_base_commit", "git_branch_ref"] {
        assert!(columns.iter().any(|column| column == expected));
    }
    assert!(!columns.iter().any(|column| column == "git_assignment_guard"));

    let foreign_key_violations: Vec<(String, i64, String, i64)> = sqlx::query_as("PRAGMA foreign_key_check")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert!(foreign_key_violations.is_empty());
}
