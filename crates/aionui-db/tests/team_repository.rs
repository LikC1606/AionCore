//! Black-box integration tests for `ITeamRepository`.
//!
//! Tests exercise the repository trait interface without knowledge of
//! the underlying SQLite implementation details.
//!
//! Covers test-plan items from Phase 11 test-plan:
//! - Section 1 (Team CRUD): TC-1..TC-7, TL-1..TL-3, TG-1..TG-2, TD-1..TD-6, TR-1..TR-4
//! - Section 4 (Mailbox): MW-1..MW-3, MR-1..MR-4, MH-1..MH-3, MD-1..MD-2

use std::sync::Arc;

use aionui_common::now_ms;
use aionui_db::models::{MailboxMessageRow, TeamRow};
use aionui_db::{
    DbError, ITeamRepository, MailboxIdempotencyParams, MailboxWriteResult, SqliteTeamRepository, UpdateTeamParams,
    init_database, init_database_memory,
};

async fn repo() -> (Arc<dyn ITeamRepository>, aionui_db::Database) {
    let db = init_database_memory().await.unwrap();
    let r = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    (r as Arc<dyn ITeamRepository>, db)
}

fn make_team(id: &str, name: &str) -> TeamRow {
    make_team_for_user(id, "system_default_user", name)
}

fn make_team_for_user(id: &str, user_id: &str, name: &str) -> TeamRow {
    let now = now_ms();
    TeamRow {
        id: id.into(),
        user_id: user_id.into(),
        name: name.into(),
        workspace: String::new(),
        workspace_mode: "shared".into(),
        agents:
            r#"[{"slot_id":"a1","name":"Lead","role":"lead","conversation_id":"conv-1","backend":"claude","model":""}]"#
                .into(),
        lead_agent_id: Some("a1".into()),
        session_mode: None,
        agents_version: "1.0.1".into(),
        created_at: now,
        updated_at: now,
    }
}

fn make_mailbox_msg(id: &str, team_id: &str, to: &str, from: &str, msg_type: &str) -> MailboxMessageRow {
    MailboxMessageRow {
        id: id.into(),
        team_id: team_id.into(),
        to_agent_id: to.into(),
        from_agent_id: from.into(),
        msg_type: msg_type.into(),
        content: format!("content-{id}"),
        summary: None,
        files: None,
        read: false,
        created_at: now_ms(),
    }
}

// ── Team CRUD Tests ──────────────────────────────────────────────────

#[tokio::test]
async fn create_and_get_team() {
    let (repo, _db) = repo().await;
    let team = make_team("t1", "Team Alpha");
    repo.create_team(&team).await.unwrap();

    let fetched = repo.get_team("t1").await.unwrap().expect("team exists");
    assert_eq!(fetched.id, "t1");
    assert_eq!(fetched.name, "Team Alpha");
    assert_eq!(fetched.lead_agent_id, Some("a1".into()));
}

#[tokio::test]
async fn get_nonexistent_team_returns_none() {
    let (repo, _db) = repo().await;
    let result = repo.get_team("nonexistent").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn list_teams_empty() {
    let (repo, _db) = repo().await;
    let teams = repo.list_teams().await.unwrap();
    assert!(teams.is_empty());
}

#[tokio::test]
async fn list_teams_multiple() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Alpha")).await.unwrap();
    repo.create_team(&make_team("t2", "Beta")).await.unwrap();

    let teams = repo.list_teams().await.unwrap();
    assert_eq!(teams.len(), 2);
    assert_eq!(teams[0].id, "t1");
    assert_eq!(teams[1].id, "t2");
}

#[tokio::test]
async fn list_teams_by_user_filters_to_owner() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team_for_user("t1", "user-a", "Alpha"))
        .await
        .unwrap();
    repo.create_team(&make_team_for_user("t2", "user-b", "Beta"))
        .await
        .unwrap();
    repo.create_team(&make_team_for_user("t3", "user-a", "Gamma"))
        .await
        .unwrap();

    let teams = repo.list_teams_by_user("user-a").await.unwrap();

    assert_eq!(teams.len(), 2);
    assert_eq!(teams[0].id, "t1");
    assert_eq!(teams[1].id, "t3");
    assert!(teams.iter().all(|team| team.user_id == "user-a"));
}

#[tokio::test]
async fn update_team_name() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Old Name")).await.unwrap();

    repo.update_team(
        "t1",
        &UpdateTeamParams {
            name: Some("New Name".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let team = repo.get_team("t1").await.unwrap().unwrap();
    assert_eq!(team.name, "New Name");
}

#[tokio::test]
async fn update_team_agents_json() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    let new_agents = r#"[{"slotId":"a1"},{"slotId":"a2"}]"#;
    repo.update_team(
        "t1",
        &UpdateTeamParams {
            agents: Some(new_agents.into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let team = repo.get_team("t1").await.unwrap().unwrap();
    assert_eq!(team.agents, new_agents);
}

#[tokio::test]
async fn update_team_can_patch_workspace() {
    let db = init_database_memory().await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    repo.update_team(
        "t1",
        &UpdateTeamParams {
            workspace: Some("/tmp/aionui-team-shared-workspace".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let updated = repo.get_team("t1").await.unwrap().unwrap();
    assert_eq!(updated.workspace, "/tmp/aionui-team-shared-workspace");
}

#[tokio::test]
async fn update_team_can_patch_session_mode() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    repo.update_team(
        "t1",
        &UpdateTeamParams {
            session_mode: Some("full_auto".into()),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let updated = repo.get_team("t1").await.unwrap().unwrap();
    assert_eq!(updated.session_mode.as_deref(), Some("full_auto"));
}

#[tokio::test]
async fn update_nonexistent_team_returns_not_found() {
    let (repo, _db) = repo().await;
    let result = repo
        .update_team(
            "nonexistent",
            &UpdateTeamParams {
                name: Some("X".into()),
                ..Default::default()
            },
        )
        .await;
    assert!(matches!(result, Err(DbError::NotFound(_))));
}

#[tokio::test]
async fn delete_team() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();
    repo.delete_team("t1").await.unwrap();

    let result = repo.get_team("t1").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn delete_nonexistent_team_returns_not_found() {
    let (repo, _db) = repo().await;
    let result = repo.delete_team("nonexistent").await;
    assert!(matches!(result, Err(DbError::NotFound(_))));
}

async fn seed_team_mode_aggregate(repo: &Arc<dyn ITeamRepository>, db: &aionui_db::Database, team_id: &str) {
    repo.create_team(&make_team(team_id, "Git Team")).await.unwrap();
    repo.write_message(&make_mailbox_msg("mail-1", team_id, "a1", "worker", "message"))
        .await
        .unwrap();
    let now = now_ms();
    sqlx::query(
        "INSERT INTO team_tasks \
            (id, team_id, subject, status, blocked_by, blocks, created_at, updated_at) \
         VALUES ('legacy-task-1', ?, 'legacy', 'pending', '[]', '[]', ?, ?)",
    )
    .bind(team_id)
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO team_work_items (\
            id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
            integrator_member_id, delivery_requirement, state, revision, created_at, updated_at, \
            git_repository_id, git_base_commit, git_branch_ref\
         ) VALUES ('work-1', ?, 'Integrate', 'a1', 'worker', 'a1', 'a1', 'git', \
                   'accepted', 5, ?, ?, 'repo-1', 'base-1', 'refs/heads/work')",
    )
    .bind(team_id)
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO team_git_deliveries (\
            id, team_id, work_item_id, producer_member_id, repository_id, content_revision, \
            base_commit, branch_ref, head_commit, state, revision, created_at, updated_at\
         ) VALUES ('delivery-1', ?, 'work-1', 'worker', 'repo-1', 1, 'base-1', \
                   'refs/heads/work', 'source-1', 'integrating', 2, ?, ?)",
    )
    .bind(team_id)
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO team_work_events (\
            event_id, team_id, work_item_id, delivery_id, actor_member_id, command_name, \
            idempotency_key, request_fingerprint, result_json, expected_work_item_revision, \
            expected_delivery_revision, work_item_revision, delivery_revision, created_at\
         ) VALUES ('event-1', ?, 'work-1', 'delivery-1', 'a1', 'begin_integration', \
                   'begin-1', 'fingerprint-1', '{}', 5, 1, 5, 2, ?)",
    )
    .bind(team_id)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO team_git_integration_attempts (\
            attempt_id, team_id, work_item_id, delivery_id, repository_id, base_commit, \
            source_ref, source_head, target_ref, target_head, state, created_at, updated_at\
         ) VALUES ('attempt-1', ?, 'work-1', 'delivery-1', 'repo-1', 'base-1', \
                   'refs/heads/work', 'source-1', 'refs/heads/main', 'target-1', 'pending', ?, ?)",
    )
    .bind(team_id)
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();
}

async fn team_domain_row_count(db: &aionui_db::Database, table: &str, team_id: &str) -> i64 {
    let query = format!("SELECT COUNT(*) FROM {table} WHERE team_id = ?");
    sqlx::query_scalar(&query)
        .bind(team_id)
        .fetch_one(db.pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn delete_team_atomically_removes_git_integration_aggregate_and_mailbox() {
    let (repo, db) = repo().await;
    seed_team_mode_aggregate(&repo, &db, "t1").await;

    repo.delete_team("t1").await.unwrap();

    assert!(repo.get_team("t1").await.unwrap().is_none());
    for table in [
        "mailbox",
        "team_tasks",
        "team_work_items",
        "team_git_deliveries",
        "team_work_events",
        "team_git_integration_attempts",
    ] {
        assert_eq!(
            team_domain_row_count(&db, table, "t1").await,
            0,
            "{table} was not deleted"
        );
    }
    let attempt_delete_actions: Vec<String> =
        sqlx::query_scalar("SELECT on_delete FROM pragma_foreign_key_list('team_git_integration_attempts')")
            .fetch_all(db.pool())
            .await
            .unwrap();
    assert!(attempt_delete_actions.iter().any(|action| action == "CASCADE"));
}

#[tokio::test]
async fn delete_team_failure_rolls_back_mailbox_and_complete_work_aggregate() {
    let (repo, db) = repo().await;
    seed_team_mode_aggregate(&repo, &db, "t1").await;
    sqlx::query(
        "CREATE TRIGGER fail_team_delete BEFORE DELETE ON teams \
         WHEN OLD.id = 't1' BEGIN SELECT RAISE(ABORT, 'forced Team delete failure'); END",
    )
    .execute(db.pool())
    .await
    .unwrap();

    assert!(repo.delete_team("t1").await.is_err());

    assert!(repo.get_team("t1").await.unwrap().is_some());
    for table in [
        "mailbox",
        "team_tasks",
        "team_work_items",
        "team_git_deliveries",
        "team_work_events",
        "team_git_integration_attempts",
    ] {
        assert_eq!(
            team_domain_row_count(&db, table, "t1").await,
            1,
            "{table} was partially deleted"
        );
    }
}

// ── Mailbox Tests ────────────────────────────────────────────────────

#[tokio::test]
async fn write_and_read_unread_messages() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    // Write 3 messages to agent a1
    for i in 1..=3 {
        let msg = make_mailbox_msg(&format!("m{i}"), "t1", "a1", "a2", "message");
        repo.write_message(&msg).await.unwrap();
    }

    // Read unread: should return 3
    let unread = repo.read_unread_and_mark("t1", "a1").await.unwrap();
    assert_eq!(unread.len(), 3);
    assert!(!unread[0].read); // returned rows reflect pre-mark state
    assert_eq!(unread[0].msg_type, "message");

    // Read again: should return 0 (all marked read)
    let unread2 = repo.read_unread_and_mark("t1", "a1").await.unwrap();
    assert!(unread2.is_empty());
}

#[tokio::test]
async fn read_unread_no_messages() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    let unread = repo.read_unread_and_mark("t1", "a1").await.unwrap();
    assert!(unread.is_empty());
}

#[tokio::test]
async fn write_idle_notification_with_summary() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    let mut msg = make_mailbox_msg("m1", "t1", "a1", "a2", "idle_notification");
    msg.summary = Some("Task completed".into());
    repo.write_message(&msg).await.unwrap();

    let history = repo.get_history("t1", "a1", None).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].msg_type, "idle_notification");
    assert_eq!(history[0].summary.as_deref(), Some("Task completed"));
}

#[tokio::test]
async fn write_shutdown_request() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    let msg = make_mailbox_msg("m1", "t1", "a1", "a2", "shutdown_request");
    repo.write_message(&msg).await.unwrap();

    let history = repo.get_history("t1", "a1", None).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].msg_type, "shutdown_request");
}

#[tokio::test]
async fn get_history_with_limit() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    for i in 1..=10 {
        let msg = make_mailbox_msg(&format!("m{i}"), "t1", "a1", "a2", "message");
        repo.write_message(&msg).await.unwrap();
    }

    let history = repo.get_history("t1", "a1", Some(5)).await.unwrap();
    assert_eq!(history.len(), 5);
}

#[tokio::test]
async fn get_history_no_limit() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    for i in 1..=3 {
        let msg = make_mailbox_msg(&format!("m{i}"), "t1", "a1", "a2", "message");
        repo.write_message(&msg).await.unwrap();
    }

    let history = repo.get_history("t1", "a1", None).await.unwrap();
    assert_eq!(history.len(), 3);
}

#[tokio::test]
async fn get_history_empty() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    let history = repo.get_history("t1", "a1", None).await.unwrap();
    assert!(history.is_empty());
}

#[tokio::test]
async fn get_history_includes_read_messages() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();

    let msg = make_mailbox_msg("m1", "t1", "a1", "a2", "message");
    repo.write_message(&msg).await.unwrap();

    // Read and mark
    repo.read_unread_and_mark("t1", "a1").await.unwrap();

    // History should still return the message
    let history = repo.get_history("t1", "a1", None).await.unwrap();
    assert_eq!(history.len(), 1);
    assert!(history[0].read);
}

#[tokio::test]
async fn delete_mailbox_by_team() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team1")).await.unwrap();
    repo.create_team(&make_team("t2", "Team2")).await.unwrap();

    // Write messages to both teams
    let msg1 = make_mailbox_msg("m1", "t1", "a1", "a2", "message");
    let msg2 = make_mailbox_msg("m2", "t2", "a1", "a2", "message");
    repo.write_message(&msg1).await.unwrap();
    repo.write_message(&msg2).await.unwrap();

    // Delete team1 mailbox
    repo.delete_mailbox_by_team("t1").await.unwrap();

    // Team1 mailbox empty
    let h1 = repo.get_history("t1", "a1", None).await.unwrap();
    assert!(h1.is_empty());

    // Team2 mailbox intact
    let h2 = repo.get_history("t2", "a1", None).await.unwrap();
    assert_eq!(h2.len(), 1);
}

#[tokio::test]
async fn recoverable_unread_mailbox_team_ids_are_filtered_deduplicated_and_paginated() {
    let (repo, _db) = repo().await;
    for team_id in ["team-a", "team-b", "team-c", "team-read", "team-self"] {
        repo.create_team(&make_team(team_id, team_id)).await.unwrap();
    }

    for message in [
        make_mailbox_msg("a-1", "team-a", "a1", "a2", "message"),
        make_mailbox_msg("a-2", "team-a", "a1", "a3", "message"),
        make_mailbox_msg("b-1", "team-b", "a1", "a2", "message"),
        make_mailbox_msg("c-1", "team-c", "a1", "a2", "message"),
        make_mailbox_msg("self-1", "team-self", "a1", "a1", "message"),
        make_mailbox_msg("read-1", "team-read", "a1", "a2", "message"),
        make_mailbox_msg("orphan-1", "team-deleted", "a1", "a2", "message"),
    ] {
        repo.write_message(&message).await.unwrap();
    }
    repo.read_unread_and_mark("team-read", "a1").await.unwrap();

    let first_page = repo
        .list_team_ids_with_recoverable_unread_mailbox(None, 2)
        .await
        .unwrap();
    assert_eq!(first_page, ["team-a", "team-b"]);

    let second_page = repo
        .list_team_ids_with_recoverable_unread_mailbox(Some("team-b"), 2)
        .await
        .unwrap();
    assert_eq!(second_page, ["team-c"]);
    assert!(
        repo.list_team_ids_with_recoverable_unread_mailbox(Some("team-c"), 2)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        repo.list_team_ids_with_recoverable_unread_mailbox(None, 0)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn recoverable_unread_mailbox_index_is_partial() {
    let (_repo, db) = repo().await;
    let index: (i64, i64) = sqlx::query_as(
        "SELECT [unique], partial FROM pragma_index_list('mailbox') \
         WHERE name = 'idx_mailbox_recoverable_unread_team'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(index, (0, 1));
}

#[tokio::test]
async fn mailbox_schema_has_partial_idempotency_identity() {
    let (_repo, db) = repo().await;
    let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info('mailbox')")
        .fetch_all(db.pool())
        .await
        .unwrap();
    assert!(columns.contains(&"idempotency_scope".to_owned()));
    assert!(columns.contains(&"idempotency_key".to_owned()));
    assert!(columns.contains(&"request_fingerprint".to_owned()));

    let index: (i64, i64) = sqlx::query_as(
        "SELECT [unique], partial FROM pragma_index_list('mailbox') WHERE name = 'idx_mailbox_idempotency'",
    )
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(index, (1, 1));
}

#[tokio::test]
async fn idempotent_mailbox_write_returns_existing_row_and_rejects_changed_payload() {
    let (repo, _db) = repo().await;
    repo.create_team(&make_team("t1", "Team")).await.unwrap();
    let first = make_mailbox_msg("m-first", "t1", "a1", "user", "message");
    let repeated = make_mailbox_msg("m-repeated", "t1", "a1", "user", "message");
    let identity = MailboxIdempotencyParams {
        scope: "lead",
        key: "turn-1",
        request_fingerprint: "fingerprint-1",
    };

    assert!(matches!(
        repo.write_message_idempotent(&first, &identity).await.unwrap(),
        MailboxWriteResult::Inserted
    ));
    let replay = repo.write_message_idempotent(&repeated, &identity).await.unwrap();
    let MailboxWriteResult::Existing(existing) = replay else {
        panic!("expected existing mailbox row");
    };
    assert_eq!(existing.id, first.id);

    let conflict = repo
        .write_message_idempotent(
            &repeated,
            &MailboxIdempotencyParams {
                request_fingerprint: "different-fingerprint",
                ..identity
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        conflict,
        MailboxWriteResult::IdempotencyConflict {
            existing_request_fingerprint
        } if existing_request_fingerprint == "fingerprint-1"
    ));
    assert_eq!(repo.get_history("t1", "a1", None).await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_idempotent_mailbox_writes_across_pools_insert_once() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mailbox-concurrent.db");
    let first_db = init_database(&path).await.unwrap();
    let first_repo = SqliteTeamRepository::new(first_db.pool().clone());
    first_repo.create_team(&make_team("t1", "Team")).await.unwrap();
    let second_db = init_database(&path).await.unwrap();
    let second_repo = SqliteTeamRepository::new(second_db.pool().clone());
    let first_row = make_mailbox_msg("m-first", "t1", "a1", "user", "message");
    let second_row = make_mailbox_msg("m-second", "t1", "a1", "user", "message");
    let identity = MailboxIdempotencyParams {
        scope: "a1",
        key: "turn-concurrent",
        request_fingerprint: "fingerprint-concurrent",
    };

    let (first, second) = tokio::join!(
        first_repo.write_message_idempotent(&first_row, &identity),
        second_repo.write_message_idempotent(&second_row, &identity)
    );
    let outcomes = [first.unwrap(), second.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, MailboxWriteResult::Inserted))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, MailboxWriteResult::Existing(_)))
            .count(),
        1
    );
    assert_eq!(first_repo.get_history("t1", "a1", None).await.unwrap().len(), 1);

    drop(first_repo);
    drop(second_repo);
    first_db.close().await;
    second_db.close().await;
}

#[tokio::test]
async fn idempotent_mailbox_write_replays_after_dropped_result_and_database_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mailbox-restart.db");
    let identity = MailboxIdempotencyParams {
        scope: "lead",
        key: "turn-after-restart",
        request_fingerprint: "fingerprint-restart",
    };

    {
        let db = init_database(&path).await.unwrap();
        let repo = SqliteTeamRepository::new(db.pool().clone());
        repo.create_team(&make_team("t1", "Team")).await.unwrap();
        let first = make_mailbox_msg("m-before-restart", "t1", "a1", "user", "message");
        let _dropped_result = repo.write_message_idempotent(&first, &identity).await.unwrap();
        drop(repo);
        db.close().await;
    }

    let db = init_database(&path).await.unwrap();
    let repo = SqliteTeamRepository::new(db.pool().clone());
    let retry = make_mailbox_msg("m-after-restart", "t1", "a1", "user", "message");
    let result = repo.write_message_idempotent(&retry, &identity).await.unwrap();
    let MailboxWriteResult::Existing(existing) = result else {
        panic!("expected persisted mailbox replay");
    };
    assert_eq!(existing.id, "m-before-restart");
    assert_eq!(repo.get_history("t1", "a1", None).await.unwrap().len(), 1);

    drop(repo);
    db.close().await;
}
