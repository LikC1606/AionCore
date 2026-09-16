use std::borrow::Cow;
use std::path::Path;

use aionui_db::models::{MailboxMessageRow, TeamRow};
use aionui_db::{
    ITeamRepository, SqliteTeamRepository, TeamCoordinationMigrationRow, UpdateTeamParams, init_database,
    init_database_memory,
};
use sqlx::migrate::Migrator;
use sqlx::sqlite::SqlitePoolOptions;

async fn migrate(pool: &sqlx::SqlitePool, version: i64) {
    let full = Migrator::new(Path::new("migrations")).await.unwrap();
    let migrator = Migrator {
        migrations: Cow::Owned(
            full.migrations
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ignore_missing: false,
        locking: true,
        no_tx: false,
    };
    let mut connection = pool.acquire().await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF; PRAGMA legacy_alter_table = ON")
        .execute(&mut *connection)
        .await
        .unwrap();
    migrator.run(&mut *connection).await.unwrap();
    sqlx::query("PRAGMA foreign_keys = ON; PRAGMA legacy_alter_table = OFF")
        .execute(&mut *connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn upgrade_quarantines_legacy_math_but_preserves_ordinary_teams_and_mailbox() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    migrate(&pool, 34).await;
    sqlx::query(
        "INSERT INTO users (id, username, password_hash, created_at, updated_at) VALUES ('u1', 'u1', '', 1, 1)",
    )
    .execute(&pool)
    .await
    .unwrap();
    for (id, extra, agents) in [
        (
            "math",
            r#"{"research_team_profile_id":"mathematics-research-team"}"#,
            r#"[{"conversation_id":"math"}]"#,
        ),
        ("ordinary", "{}", r#"[{"conversation_id":"ordinary"}]"#),
        ("damaged", "{}", r#"["damaged", {}]"#),
    ] {
        sqlx::query("INSERT INTO conversations (id, user_id, name, type, extra, created_at, updated_at) VALUES (?, 'u1', ?, 'acp', ?, 1, 1)")
            .bind(id).bind(id).bind(extra).execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO teams (id, user_id, name, workspace, workspace_mode, agents, agents_version, created_at, updated_at) VALUES (?, 'u1', ?, '', 'shared', ?, '1.0.1', 1, 1)")
            .bind(id).bind(id).bind(agents).execute(&pool).await.unwrap();
    }
    sqlx::query("INSERT INTO mailbox (id, team_id, to_agent_id, from_agent_id, type, content, read, created_at) VALUES ('m1', 'math', 'lead', 'user', 'message', 'retained work', 0, 1)")
        .execute(&pool).await.unwrap();

    migrate(&pool, 35).await;
    migrate(&pool, 35).await;

    let rows: Vec<(String, Option<String>)> = sqlx::query_as("SELECT id, coordination_protocol FROM teams ORDER BY id")
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("damaged".into(), None),
            (
                "math".into(),
                Some(r#"{"kind":"legacy_managed_migration_required"}"#.into())
            ),
            ("ordinary".into(), None),
        ]
    );
    let content: String = sqlx::query_scalar("SELECT content FROM mailbox WHERE id = 'm1'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(content, "retained work");
}

fn legacy_row() -> TeamRow {
    TeamRow {
        coordination_protocol: Some(r#"{"kind":"legacy_managed_migration_required"}"#.into()),
        id: "legacy".into(),
        user_id: "system_default_user".into(),
        name: "Legacy".into(),
        workspace: "/unchanged/worktree".into(),
        workspace_mode: "shared".into(),
        agents: "[]".into(),
        lead_agent_id: None,
        session_mode: None,
        agents_version: "1.0.1".into(),
        created_at: 1,
        updated_at: 1,
    }
}

fn migration_audit() -> TeamCoordinationMigrationRow {
    TeamCoordinationMigrationRow {
        team_id: "legacy".into(),
        user_id: "system_default_user".into(),
        migration_id: "migration-1".into(),
        proof_digest: "a".repeat(64),
        proof_json: r#"{"signed":"proof-1"}"#.into(),
        receipt_json: r#"{"receipt":"receipt-1"}"#.into(),
        applied_at: 2,
    }
}

#[tokio::test]
async fn explicit_upgrade_is_atomic_when_commit_effect_fails() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    let expected = legacy_row();
    repo.create_team(&expected).await.unwrap();
    sqlx::query("CREATE TRIGGER interrupt_protocol_update BEFORE UPDATE OF coordination_protocol ON teams BEGIN SELECT RAISE(ABORT, 'simulated interruption'); END")
        .execute(db.pool()).await.unwrap();
    let error = repo
        .migrate_coordination_protocol(&expected, &migration_audit())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("simulated interruption"));
    assert!(repo.get_coordination_migration("legacy").await.unwrap().is_none());
    assert_eq!(
        repo.get_team("legacy").await.unwrap().unwrap().coordination_protocol,
        expected.coordination_protocol
    );

    sqlx::query("DROP TRIGGER interrupt_protocol_update")
        .execute(db.pool())
        .await
        .unwrap();
    let result = repo
        .migrate_coordination_protocol(&expected, &migration_audit())
        .await
        .unwrap();
    assert_eq!(result.receipt_json, migration_audit().receipt_json);
}

#[tokio::test]
async fn committed_upgrade_survives_database_reopen_and_preserves_mailbox() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("migration.db");
    let expected = legacy_row();
    {
        let db = init_database(&file).await.unwrap();
        let repo = SqliteTeamRepository::new(db.pool().clone());
        repo.create_team(&expected).await.unwrap();
        repo.write_message(&MailboxMessageRow {
            id: "retained-message".into(),
            team_id: "legacy".into(),
            to_agent_id: "lead".into(),
            from_agent_id: "worker".into(),
            msg_type: "message".into(),
            content: "Retained delivery".into(),
            summary: None,
            files: None,
            read: false,
            created_at: 1,
        })
        .await
        .unwrap();
        repo.migrate_coordination_protocol(&expected, &migration_audit())
            .await
            .unwrap();
        db.pool().close().await;
    }
    let db = init_database(&file).await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    let replay = repo
        .migrate_coordination_protocol(&expected, &migration_audit())
        .await
        .unwrap();
    assert_eq!(replay.applied_at, 2);
    assert_eq!(
        repo.peek_unread("legacy", "lead").await.unwrap()[0].content,
        "Retained delivery"
    );
    assert_eq!(
        repo.get_team("legacy").await.unwrap().unwrap().workspace,
        expected.workspace
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_migrations_from_separate_pools_commit_once() {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("concurrent.db");
    let first = init_database(&file).await.unwrap();
    let first_repo = SqliteTeamRepository::new(first.pool().clone());
    let row = legacy_row();
    first_repo.create_team(&row).await.unwrap();
    let second = init_database(&file).await.unwrap();
    let second_repo = SqliteTeamRepository::new(second.pool().clone());
    let audit = migration_audit();
    let (left, right) = tokio::join!(
        first_repo.migrate_coordination_protocol(&row, &audit),
        second_repo.migrate_coordination_protocol(&row, &audit)
    );
    assert_eq!(left.unwrap().receipt_json, right.unwrap().receipt_json);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM team_coordination_migrations")
        .fetch_one(first.pool())
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn stale_or_cross_user_snapshots_and_native_teams_are_not_upgraded() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    let expected = legacy_row();
    repo.create_team(&expected).await.unwrap();
    let mut other = migration_audit();
    other.user_id = "other-user".into();
    assert!(
        matches!(repo.migrate_coordination_protocol(&expected, &other).await, Err(aionui_db::DbError::Conflict(message)) if message == "Team coordination migration identity mismatch")
    );
    repo.update_team(
        "legacy",
        &UpdateTeamParams {
            name: Some("Changed".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(
        matches!(repo.migrate_coordination_protocol(&expected, &migration_audit()).await, Err(aionui_db::DbError::Conflict(message)) if message == "Team coordination migration snapshot changed")
    );
    assert!(repo.get_coordination_migration("legacy").await.unwrap().is_none());

    let mut ordinary = legacy_row();
    ordinary.id = "ordinary".into();
    ordinary.coordination_protocol = None;
    repo.create_team(&ordinary).await.unwrap();
    let mut audit = migration_audit();
    audit.team_id = "ordinary".into();
    assert!(matches!(
        repo.migrate_coordination_protocol(&ordinary, &audit).await,
        Err(aionui_db::DbError::Conflict(_))
    ));
}

#[tokio::test]
async fn a_migration_id_cannot_be_reused_for_another_team() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    let first = legacy_row();
    repo.create_team(&first).await.unwrap();
    repo.migrate_coordination_protocol(&first, &migration_audit())
        .await
        .unwrap();
    let mut second = legacy_row();
    second.id = "other-team".into();
    repo.create_team(&second).await.unwrap();
    let mut audit = migration_audit();
    audit.team_id = second.id.clone();
    assert!(
        matches!(repo.migrate_coordination_protocol(&second, &audit).await, Err(aionui_db::DbError::Conflict(message)) if message == "Team coordination migration id already used")
    );
    assert_eq!(
        repo.get_team(&second.id).await.unwrap().unwrap().coordination_protocol,
        second.coordination_protocol
    );
}
