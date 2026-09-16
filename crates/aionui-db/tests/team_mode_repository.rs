use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::time::Duration;

use aionui_common::now_ms;
use aionui_db::models::{MailboxMessageRow, TeamRow};
use aionui_db::{
    CommitTeamCommandParams, DbError, ITeamModeRepository, ITeamRepository, MailboxIdempotencyParams,
    NewTeamMailboxNotification, NewTeamWorkEvent, SqliteTeamModeRepository, SqliteTeamRepository,
    TEAM_WORK_EVENT_NOTIFICATION_SCOPE, TeamCommandCommitResult, TeamCommandReceiptLookupResult,
    TeamGitDeliveryMutation, TeamGitDeliveryRow, TeamRosterGuard, TeamWorkItemMutation, TeamWorkItemRevisionGuard,
    TeamWorkItemRow, UpdateTeamParams, init_database, init_database_memory,
};

async fn repository() -> (SqliteTeamModeRepository, aionui_db::Database) {
    let database = init_database_memory().await.expect("initialize database");
    let legacy_team_repository = SqliteTeamRepository::new(database.pool().clone());
    legacy_team_repository
        .create_team(&team("team-1"))
        .await
        .expect("create owning team");
    (SqliteTeamModeRepository::new(database.pool().clone()), database)
}

fn team(id: &str) -> TeamRow {
    let timestamp = now_ms();
    TeamRow {
        coordination_protocol: None,
        id: id.into(),
        user_id: "system_default_user".into(),
        name: format!("Team {id}"),
        workspace: String::new(),
        workspace_mode: "shared".into(),
        agents: "[]".into(),
        lead_agent_id: Some("controller".into()),
        session_mode: None,
        agents_version: "1.0.1".into(),
        created_at: timestamp,
        updated_at: timestamp,
    }
}

fn work_item(id: &str, requirement: &str) -> TeamWorkItemRow {
    let timestamp = now_ms();
    TeamWorkItemRow {
        id: id.into(),
        team_id: "team-1".into(),
        parent_work_item_id: None,
        subject: "Implement durable Team Mode".into(),
        description: Some("Exercise the command repository".into()),
        controller_member_id: "controller".into(),
        assignee_member_id: "producer".into(),
        reviewer_member_id: "reviewer".into(),
        integrator_member_id: (requirement == "git").then(|| "integrator".into()),
        delivery_requirement: requirement.into(),
        git_repository_id: (requirement == "git").then(|| "repo-1".into()),
        git_base_commit: (requirement == "git").then(|| "base-commit".into()),
        git_branch_ref: (requirement == "git").then(|| format!("refs/heads/team/{id}")),
        state: "draft".into(),
        current_submission_json: None,
        accepted_delivery_json: None,
        revision: 0,
        created_at: timestamp,
        updated_at: timestamp,
    }
}

fn delivery(id: &str, work_item_id: &str, content_revision: i64) -> TeamGitDeliveryRow {
    let timestamp = now_ms();
    TeamGitDeliveryRow {
        id: id.into(),
        team_id: "team-1".into(),
        work_item_id: work_item_id.into(),
        producer_member_id: "producer".into(),
        repository_id: "repo-1".into(),
        content_revision,
        base_commit: "base-commit".into(),
        branch_ref: format!("refs/heads/team/{work_item_id}/{content_revision}"),
        head_commit: format!("head-commit-{content_revision}"),
        state: "submitted".into(),
        revision: 0,
        merged_commit: None,
        created_at: timestamp,
        updated_at: timestamp,
    }
}

fn notification(message_id: &str, event_id: &str, fingerprint: &str) -> NewTeamMailboxNotification {
    NewTeamMailboxNotification {
        message_id: message_id.into(),
        to_agent_id: "producer".into(),
        from_agent_id: "controller".into(),
        content: "Start the assigned WorkItem".into(),
        summary: Some("WorkItem assigned".into()),
        files_json: Some(r#"["/tmp/worktree"]"#.into()),
        idempotency_scope: TEAM_WORK_EVENT_NOTIFICATION_SCOPE.into(),
        idempotency_key: format!("{event_id}:0"),
        request_fingerprint: fingerprint.into(),
        created_at: now_ms(),
    }
}

#[allow(clippy::too_many_arguments)]
fn event(
    event_id: &str,
    work_item_id: &str,
    actor_member_id: &str,
    command_name: &str,
    idempotency_key: &str,
    request_fingerprint: &str,
    work_item_revision: Option<i64>,
    delivery: Option<(&str, i64)>,
) -> NewTeamWorkEvent {
    NewTeamWorkEvent {
        event_id: event_id.into(),
        team_id: "team-1".into(),
        work_item_id: work_item_id.into(),
        delivery_id: delivery.map(|(id, _)| id.into()),
        actor_member_id: actor_member_id.into(),
        command_name: command_name.into(),
        idempotency_key: idempotency_key.into(),
        request_fingerprint: request_fingerprint.into(),
        result_json: format!(r#"{{"command":"{command_name}"}}"#),
        expected_work_item_revision: work_item_revision.and_then(|revision| (revision > 0).then_some(revision - 1)),
        expected_delivery_revision: delivery.and_then(|(_, revision)| (revision > 0).then_some(revision - 1)),
        work_item_revision: work_item_revision.expect("test event requires a WorkItem post-state revision"),
        delivery_revision: delivery.map(|(_, revision)| revision),
        created_at: now_ms(),
    }
}

fn insert_work_command(row: TeamWorkItemRow, event_id: &str, idempotency_key: &str) -> CommitTeamCommandParams {
    CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            event_id,
            &row.id,
            "controller",
            "create_work_item",
            idempotency_key,
            "fingerprint-create",
            Some(row.revision),
            None,
        ),
        work_item: Some(TeamWorkItemMutation::Insert(row)),
        delivery: None,
    }
}

#[tokio::test]
async fn commit_inserts_work_item_and_ordered_receipt_atomically() {
    let (repository, _database) = repository().await;
    let row = work_item("work-1", "none");

    let outcome = repository
        .commit_command(&insert_work_command(row.clone(), "event-1", "key-1"))
        .await
        .expect("commit command");

    let TeamCommandCommitResult::Applied(receipt) = outcome else {
        panic!("expected applied command");
    };
    assert_eq!(receipt.sequence, 1);
    assert_eq!(receipt.work_item_revision, 0);
    assert_eq!(repository.get_work_item("team-1", "work-1").await.unwrap(), Some(row));
    assert_eq!(
        repository
            .find_command_receipt("team-1", "controller", "key-1")
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        repository.list_work_events("team-1", "work-1").await.unwrap(),
        vec![receipt]
    );
}

#[tokio::test]
async fn work_item_git_assignment_is_required_and_immutable() {
    let (repository, _database) = repository().await;
    let mut partial = work_item("work-partial", "git");
    partial.git_base_commit = None;
    let error = repository
        .commit_command(&insert_work_command(partial, "event-partial", "key-partial"))
        .await
        .unwrap_err();
    assert!(matches!(error, DbError::Conflict(message) if message.contains("Git assignment")));
    assert!(
        repository
            .get_work_item("team-1", "work-partial")
            .await
            .unwrap()
            .is_none()
    );

    let original = work_item("work-assigned", "git");
    repository
        .commit_command(&insert_work_command(original.clone(), "event-assigned", "key-assigned"))
        .await
        .unwrap();
    let mut replacement = original.clone();
    replacement.git_branch_ref = Some("refs/heads/team/other".into());
    replacement.state = "queued".into();
    replacement.revision = 1;
    replacement.updated_at += 1;
    let mutation = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-mutated-assignment",
            "work-assigned",
            "controller",
            "queue",
            "key-mutated-assignment",
            "fingerprint-mutated-assignment",
            Some(1),
            None,
        ),
        work_item: Some(TeamWorkItemMutation::CompareAndSwap {
            expected_revision: 0,
            row: replacement,
        }),
        delivery: None,
    };

    let error = repository.commit_command(&mutation).await.unwrap_err();
    assert!(matches!(error, DbError::Conflict(message) if message.contains("assignment is immutable")));
    assert_eq!(
        repository.get_work_item("team-1", "work-assigned").await.unwrap(),
        Some(original)
    );
    assert!(
        repository
            .find_command_receipt("team-1", "controller", "key-mutated-assignment")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn migration_check_enforces_assignment_all_or_none_for_direct_writes() {
    let (_repository, database) = repository().await;
    for (id, requirement, repository_id, base_commit, branch_ref) in [
        ("work-partial", "git", Some("repo-1"), None, Some("refs/heads/work")),
        (
            "work-unexpected",
            "none",
            Some("repo-1"),
            Some("base-1"),
            Some("refs/heads/work"),
        ),
    ] {
        let result = sqlx::query(
            "INSERT INTO team_work_items (\
                id, team_id, subject, controller_member_id, assignee_member_id, reviewer_member_id, \
                delivery_requirement, git_repository_id, git_base_commit, git_branch_ref, \
                state, revision, created_at, updated_at\
             ) VALUES (?, 'team-1', 'Invalid assignment', 'controller', 'producer', 'reviewer', \
                       ?, ?, ?, ?, 'draft', 0, 1, 1)",
        )
        .bind(id)
        .bind(requirement)
        .bind(repository_id)
        .bind(base_commit)
        .bind(branch_ref)
        .execute(database.pool())
        .await;
        assert!(result.is_err(), "invalid assignment row {id} must fail its CHECK");
    }
}

#[tokio::test]
async fn event_insert_failure_rolls_back_the_aggregate_insert() {
    let (repository, _database) = repository().await;
    repository
        .commit_command(&insert_work_command(work_item("work-1", "none"), "event-1", "key-1"))
        .await
        .unwrap();
    let duplicate_event_id = insert_work_command(work_item("work-2", "none"), "event-1", "key-2");

    let error = repository.commit_command(&duplicate_event_id).await.unwrap_err();

    assert!(error.is_unique_violation(), "expected unique violation, got {error:?}");
    assert!(repository.get_work_item("team-1", "work-2").await.unwrap().is_none());
    assert!(
        repository
            .find_command_receipt("team-1", "controller", "key-2")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn mailbox_insert_failure_rolls_back_work_item_event_and_all_new_notifications() {
    let (repository, database) = repository().await;
    let mailbox = SqliteTeamRepository::new(database.pool().clone());
    mailbox
        .write_message(&MailboxMessageRow {
            id: "duplicate-message".into(),
            team_id: "team-1".into(),
            to_agent_id: "producer".into(),
            from_agent_id: "controller".into(),
            msg_type: "message".into(),
            content: "existing".into(),
            summary: None,
            files: None,
            read: false,
            created_at: now_ms(),
        })
        .await
        .unwrap();
    let mut command = insert_work_command(
        work_item("work-mailbox-failure", "none"),
        "event-mailbox-failure",
        "key-mailbox-failure",
    );
    command.notifications = vec![
        notification("new-message", "event-mailbox-failure", "new-fingerprint"),
        notification(
            "duplicate-message",
            "event-mailbox-failure-second",
            "second-fingerprint",
        ),
    ];

    let error = repository.commit_command(&command).await.unwrap_err();

    assert!(error.is_unique_violation(), "expected unique violation, got {error:?}");
    assert!(
        repository
            .get_work_item("team-1", "work-mailbox-failure")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        repository
            .find_command_receipt("team-1", "controller", "key-mailbox-failure")
            .await
            .unwrap()
            .is_none()
    );
    let history = mailbox.get_history("team-1", "producer", None).await.unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].id, "duplicate-message");
}

#[tokio::test]
async fn mailbox_idempotency_key_with_different_fingerprint_rolls_back_command() {
    let (repository, database) = repository().await;
    let mailbox = SqliteTeamRepository::new(database.pool().clone());
    let existing = MailboxMessageRow {
        id: "existing-message".into(),
        team_id: "team-1".into(),
        to_agent_id: "producer".into(),
        from_agent_id: "controller".into(),
        msg_type: "message".into(),
        content: "old notification".into(),
        summary: None,
        files: None,
        read: false,
        created_at: now_ms(),
    };
    mailbox
        .write_message_idempotent(
            &existing,
            &MailboxIdempotencyParams {
                scope: TEAM_WORK_EVENT_NOTIFICATION_SCOPE,
                key: "event-mailbox-conflict:0",
                request_fingerprint: "old-fingerprint",
            },
        )
        .await
        .unwrap();
    let mut command = insert_work_command(
        work_item("work-mailbox-conflict", "none"),
        "event-mailbox-conflict",
        "key-mailbox-conflict",
    );
    command.notifications = vec![notification(
        "new-message",
        "event-mailbox-conflict",
        "different-fingerprint",
    )];

    let error = repository.commit_command(&command).await.unwrap_err();

    assert!(matches!(error, DbError::Conflict(message) if message.contains("different request fingerprint")));
    assert!(
        repository
            .get_work_item("team-1", "work-mailbox-conflict")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        repository
            .find_command_receipt("team-1", "controller", "key-mailbox-conflict")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(mailbox.get_history("team-1", "producer", None).await.unwrap().len(), 1);
}

#[tokio::test]
async fn atomic_work_event_and_unread_notification_survive_database_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("team-command-notification.db");

    {
        let database = init_database(&path).await.unwrap();
        let mailbox = SqliteTeamRepository::new(database.pool().clone());
        mailbox.create_team(&team("team-1")).await.unwrap();
        let repository = SqliteTeamModeRepository::new(database.pool().clone());
        let mut command = insert_work_command(work_item("work-restart", "none"), "event-restart", "key-restart");
        command.notifications = vec![notification("message-restart", "event-restart", "notification-restart")];
        assert!(matches!(
            repository.commit_command(&command).await.unwrap(),
            TeamCommandCommitResult::Applied(_)
        ));
        drop(repository);
        drop(mailbox);
        database.close().await;
    }

    let database = init_database(&path).await.unwrap();
    let repository = SqliteTeamModeRepository::new(database.pool().clone());
    let mailbox = SqliteTeamRepository::new(database.pool().clone());

    assert!(
        repository
            .get_work_item("team-1", "work-restart")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        repository
            .find_command_receipt("team-1", "controller", "key-restart")
            .await
            .unwrap()
            .is_some()
    );
    let unread = mailbox.peek_unread("team-1", "producer").await.unwrap();
    assert_eq!(unread.len(), 1);
    assert_eq!(unread[0].id, "message-restart");
    assert_eq!(unread[0].team_id, "team-1");
    assert_eq!(unread[0].msg_type, "message");
    assert!(!unread[0].read);
    assert_eq!(
        mailbox
            .list_team_ids_with_recoverable_unread_mailbox(None, 10)
            .await
            .unwrap(),
        vec!["team-1"]
    );

    drop(repository);
    drop(mailbox);
    database.close().await;
}

#[tokio::test]
async fn same_idempotency_scope_and_fingerprint_replays_without_writing() {
    let (repository, database) = repository().await;
    let mut command = insert_work_command(work_item("work-1", "none"), "event-1", "key-1");
    command.notifications = vec![notification("message-1", "event-1", "notification-fingerprint")];
    let first = repository.commit_command(&command).await.unwrap();

    let replay = repository.commit_command(&command).await.unwrap();

    let (TeamCommandCommitResult::Applied(first), TeamCommandCommitResult::Replayed(replay)) = (first, replay) else {
        panic!("expected applied then replayed outcomes");
    };
    assert_eq!(replay, first);
    assert_eq!(repository.list_work_events("team-1", "work-1").await.unwrap().len(), 1);
    assert_eq!(
        SqliteTeamRepository::new(database.pool().clone())
            .peek_unread("team-1", "producer")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn changed_roster_blocks_replay_of_an_existing_receipt() {
    let (repository, database) = repository().await;
    let mut original = insert_work_command(work_item("work-1", "none"), "event-1", "key-1");
    original.roster_guard = Some(TeamRosterGuard {
        expected_user_id: "system_default_user".into(),
        expected_agents_json: "[]".into(),
        expected_lead_agent_id: Some("controller".into()),
    });
    assert!(matches!(
        repository.commit_command(&original).await.unwrap(),
        TeamCommandCommitResult::Applied(_)
    ));
    SqliteTeamRepository::new(database.pool().clone())
        .update_team(
            "team-1",
            &UpdateTeamParams {
                agents: Some(r#"[{"slot_id":"replacement"}]"#.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let outcome = repository.commit_command(&original).await.unwrap();

    assert_eq!(
        outcome,
        TeamCommandCommitResult::TeamRosterConflict {
            team_id: "team-1".into()
        }
    );
    assert_eq!(repository.list_work_events("team-1", "work-1").await.unwrap().len(), 1);
}

#[tokio::test]
async fn receipt_replay_precedes_validation_of_a_stale_materialized_snapshot() {
    let (repository, _database) = repository().await;
    let original = insert_work_command(work_item("work-1", "none"), "event-1", "key-1");
    let applied = repository.commit_command(&original).await.unwrap();
    let mut retry = original;
    let Some(TeamWorkItemMutation::Insert(row)) = retry.work_item.as_mut() else {
        panic!("expected insert mutation");
    };
    row.revision = 99;
    retry.event.work_item_revision = 99;

    let replay = repository.commit_command(&retry).await.unwrap();

    let (TeamCommandCommitResult::Applied(applied), TeamCommandCommitResult::Replayed(replay)) = (applied, replay)
    else {
        panic!("expected applied then replayed outcomes");
    };
    assert_eq!(replay, applied);
    assert_eq!(repository.list_work_events("team-1", "work-1").await.unwrap().len(), 1);
}

#[tokio::test]
async fn changed_roster_guard_rolls_back_work_item_and_event() {
    let (repository, database) = repository().await;
    let mut command = insert_work_command(work_item("work-1", "none"), "event-1", "key-1");
    command.roster_guard = Some(TeamRosterGuard {
        expected_user_id: "system_default_user".into(),
        expected_agents_json: "[]".into(),
        expected_lead_agent_id: Some("controller".into()),
    });
    SqliteTeamRepository::new(database.pool().clone())
        .update_team(
            "team-1",
            &UpdateTeamParams {
                agents: Some(r#"[{"slot_id":"replacement","role":"teammate"}]"#.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let outcome = repository.commit_command(&command).await.unwrap();

    assert_eq!(
        outcome,
        TeamCommandCommitResult::TeamRosterConflict {
            team_id: "team-1".into()
        }
    );
    assert!(repository.get_work_item("team-1", "work-1").await.unwrap().is_none());
    assert!(
        repository
            .find_command_receipt("team-1", "controller", "key-1")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn guarded_receipt_lookup_never_returns_data_after_roster_change() {
    let (repository, database) = repository().await;
    let guard = TeamRosterGuard {
        expected_user_id: "system_default_user".into(),
        expected_agents_json: "[]".into(),
        expected_lead_agent_id: Some("controller".into()),
    };
    let mut command = insert_work_command(work_item("work-1", "none"), "event-1", "key-1");
    command.roster_guard = Some(guard.clone());
    let TeamCommandCommitResult::Applied(receipt) = repository.commit_command(&command).await.unwrap() else {
        panic!("expected applied command");
    };

    assert_eq!(
        repository
            .find_command_receipt_guarded("team-1", "controller", "missing-key", &guard)
            .await
            .unwrap(),
        TeamCommandReceiptLookupResult::NotFound
    );
    assert_eq!(
        repository
            .find_command_receipt_guarded("team-1", "controller", "key-1", &guard)
            .await
            .unwrap(),
        TeamCommandReceiptLookupResult::Found(Box::new(receipt))
    );

    SqliteTeamRepository::new(database.pool().clone())
        .update_team(
            "team-1",
            &UpdateTeamParams {
                agents: Some(r#"[{"slot_id":"replacement"}]"#.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert_eq!(
        repository
            .find_command_receipt_guarded("team-1", "controller", "key-1", &guard)
            .await
            .unwrap(),
        TeamCommandReceiptLookupResult::TeamRosterConflict {
            team_id: "team-1".into()
        }
    );
}

#[tokio::test]
async fn matching_parent_revision_guard_allows_child_insert() {
    let (repository, _database) = repository().await;
    repository
        .commit_command(&insert_work_command(
            work_item("parent", "none"),
            "event-parent",
            "key-parent",
        ))
        .await
        .unwrap();
    let mut child = work_item("child", "none");
    child.parent_work_item_id = Some("parent".into());
    let mut command = insert_work_command(child, "event-child", "key-child");
    command.work_item_guards = vec![TeamWorkItemRevisionGuard {
        work_item_id: "parent".into(),
        expected_revision: 0,
    }];

    assert!(matches!(
        repository.commit_command(&command).await.unwrap(),
        TeamCommandCommitResult::Applied(_)
    ));
    assert!(repository.get_work_item("team-1", "child").await.unwrap().is_some());
}

#[tokio::test]
async fn existing_child_receipt_replays_before_a_now_stale_parent_guard() {
    let (repository, _database) = repository().await;
    let parent = work_item("parent", "none");
    repository
        .commit_command(&insert_work_command(parent.clone(), "event-parent", "key-parent"))
        .await
        .unwrap();
    let mut child = work_item("child", "none");
    child.parent_work_item_id = Some("parent".into());
    let mut create_child = insert_work_command(child, "event-child", "key-child");
    create_child.work_item_guards = vec![TeamWorkItemRevisionGuard {
        work_item_id: "parent".into(),
        expected_revision: 0,
    }];
    let applied = repository.commit_command(&create_child).await.unwrap();

    let mut changed_parent = parent;
    changed_parent.state = "queued".into();
    changed_parent.revision = 1;
    changed_parent.updated_at += 1;
    repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: event(
                "event-parent-queued",
                "parent",
                "controller",
                "queue_work_item",
                "key-parent-queued",
                "fingerprint-parent-queued",
                Some(1),
                None,
            ),
            work_item: Some(TeamWorkItemMutation::CompareAndSwap {
                expected_revision: 0,
                row: changed_parent,
            }),
            delivery: None,
        })
        .await
        .unwrap();

    let replay = repository.commit_command(&create_child).await.unwrap();

    let (TeamCommandCommitResult::Applied(applied), TeamCommandCommitResult::Replayed(replay)) = (applied, replay)
    else {
        panic!("expected applied then replayed outcomes");
    };
    assert_eq!(replay, applied);
    assert_eq!(repository.list_work_events("team-1", "child").await.unwrap().len(), 1);
}

#[tokio::test]
async fn changed_parent_revision_guard_rejects_child_without_an_event() {
    let (repository, _database) = repository().await;
    let parent = work_item("parent", "none");
    repository
        .commit_command(&insert_work_command(parent.clone(), "event-parent", "key-parent"))
        .await
        .unwrap();
    let mut child = work_item("child", "none");
    child.parent_work_item_id = Some("parent".into());
    let mut create_child = insert_work_command(child, "event-child", "key-child");
    create_child.work_item_guards = vec![TeamWorkItemRevisionGuard {
        work_item_id: "parent".into(),
        expected_revision: 0,
    }];

    let mut changed_parent = parent;
    changed_parent.state = "queued".into();
    changed_parent.revision = 1;
    changed_parent.updated_at += 1;
    repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: event(
                "event-parent-queued",
                "parent",
                "controller",
                "queue_work_item",
                "key-parent-queued",
                "fingerprint-parent-queued",
                Some(1),
                None,
            ),
            work_item: Some(TeamWorkItemMutation::CompareAndSwap {
                expected_revision: 0,
                row: changed_parent,
            }),
            delivery: None,
        })
        .await
        .unwrap();

    let outcome = repository.commit_command(&create_child).await.unwrap();

    assert_eq!(
        outcome,
        TeamCommandCommitResult::WorkItemGuardConflict {
            work_item_id: "parent".into(),
            expected_revision: 0,
            actual_revision: Some(1)
        }
    );
    assert!(repository.get_work_item("team-1", "child").await.unwrap().is_none());
    assert!(
        repository
            .find_command_receipt("team-1", "controller", "key-child")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn work_item_guard_does_not_resolve_a_parent_from_another_team() {
    let (repository, database) = repository().await;
    SqliteTeamRepository::new(database.pool().clone())
        .create_team(&team("team-2"))
        .await
        .unwrap();
    repository
        .commit_command(&insert_work_command(
            work_item("parent", "none"),
            "event-parent",
            "key-parent",
        ))
        .await
        .unwrap();

    let mut child = work_item("child", "none");
    child.team_id = "team-2".into();
    let mut command = insert_work_command(child, "event-child", "key-child");
    command.event.team_id = "team-2".into();
    command.work_item_guards = vec![TeamWorkItemRevisionGuard {
        work_item_id: "parent".into(),
        expected_revision: 0,
    }];

    let outcome = repository.commit_command(&command).await.unwrap();

    assert_eq!(
        outcome,
        TeamCommandCommitResult::WorkItemGuardConflict {
            work_item_id: "parent".into(),
            expected_revision: 0,
            actual_revision: None
        }
    );
    assert!(repository.get_work_item("team-2", "child").await.unwrap().is_none());
    assert!(
        repository
            .find_command_receipt("team-2", "controller", "key-child")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn same_idempotency_scope_with_different_fingerprint_conflicts() {
    let (repository, _database) = repository().await;
    let command = insert_work_command(work_item("work-1", "none"), "event-1", "key-1");
    repository.commit_command(&command).await.unwrap();
    let mut conflicting = command;
    conflicting.event.event_id = "event-2".into();
    conflicting.event.request_fingerprint = "different-fingerprint".into();

    let outcome = repository.commit_command(&conflicting).await.unwrap();

    assert_eq!(
        outcome,
        TeamCommandCommitResult::IdempotencyConflict {
            existing_request_fingerprint: "fingerprint-create".into()
        }
    );
    assert_eq!(repository.list_work_events("team-1", "work-1").await.unwrap().len(), 1);
}

#[tokio::test]
async fn same_idempotency_key_is_independent_for_different_actors() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "none");
    repository
        .commit_command(&insert_work_command(created.clone(), "event-1", "shared-key"))
        .await
        .unwrap();

    let mut queued = created;
    queued.state = "queued".into();
    queued.revision = 1;
    queued.updated_at += 1;
    let command = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-2",
            "work-1",
            "producer",
            "queue_work_item",
            "shared-key",
            "fingerprint-queue",
            Some(1),
            None,
        ),
        work_item: Some(TeamWorkItemMutation::CompareAndSwap {
            expected_revision: 0,
            row: queued,
        }),
        delivery: None,
    };

    assert!(matches!(
        repository.commit_command(&command).await.unwrap(),
        TeamCommandCommitResult::Applied(_)
    ));
    let events = repository.list_work_events("team-1", "work-1").await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events.iter().map(|event| event.sequence).collect::<Vec<_>>(),
        vec![1, 2]
    );
    assert_eq!(events[0].expected_work_item_revision, None);
    assert_eq!(events[1].expected_work_item_revision, Some(0));
}

#[tokio::test]
async fn successful_delivery_cas_persists_both_expected_revisions() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "git");
    repository
        .commit_command(&insert_work_command(created, "event-1", "key-1"))
        .await
        .unwrap();
    let submitted = delivery("delivery-1", "work-1", 1);
    repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: event(
                "event-2",
                "work-1",
                "producer",
                "submit_git_delivery",
                "key-2",
                "fingerprint-submit",
                Some(0),
                Some(("delivery-1", 0)),
            ),
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::Insert(submitted.clone())),
        })
        .await
        .unwrap();

    let mut accepted = submitted;
    accepted.state = "accepted".into();
    accepted.revision = 1;
    accepted.updated_at += 1;
    let mut accept_event = event(
        "event-3",
        "work-1",
        "reviewer",
        "accept_delivery",
        "key-3",
        "fingerprint-accept",
        Some(0),
        Some(("delivery-1", 1)),
    );
    accept_event.expected_work_item_revision = Some(0);
    let outcome = repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: accept_event,
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::CompareAndSwap {
                expected_revision: 0,
                row: accepted,
            }),
        })
        .await
        .unwrap();

    let TeamCommandCommitResult::Applied(receipt) = outcome else {
        panic!("expected applied command");
    };
    assert_eq!(receipt.expected_work_item_revision, Some(0));
    assert_eq!(receipt.expected_delivery_revision, Some(0));
    assert_eq!(receipt.work_item_revision, 0);
    assert_eq!(receipt.delivery_revision, Some(1));
    assert_eq!(
        repository
            .find_command_receipt("team-1", "reviewer", "key-3")
            .await
            .unwrap(),
        Some(receipt)
    );
}

#[tokio::test]
async fn expected_work_item_revision_guards_delivery_only_commands() {
    let (repository, _database) = repository().await;
    repository
        .commit_command(&insert_work_command(work_item("work-1", "git"), "event-1", "key-1"))
        .await
        .unwrap();
    let submitted = delivery("delivery-1", "work-1", 1);
    repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: event(
                "event-2",
                "work-1",
                "producer",
                "submit_git_delivery",
                "key-2",
                "fingerprint-submit",
                Some(0),
                Some(("delivery-1", 0)),
            ),
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::Insert(submitted.clone())),
        })
        .await
        .unwrap();

    let mut accepted = submitted.clone();
    accepted.state = "accepted".into();
    accepted.revision = 1;
    accepted.updated_at += 1;
    let mut guarded_event = event(
        "event-3",
        "work-1",
        "reviewer",
        "accept_delivery",
        "key-3",
        "fingerprint-accept",
        Some(0),
        Some(("delivery-1", 1)),
    );
    guarded_event.expected_work_item_revision = Some(9);
    let outcome = repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: guarded_event,
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::CompareAndSwap {
                expected_revision: 0,
                row: accepted,
            }),
        })
        .await
        .unwrap();

    assert_eq!(
        outcome,
        TeamCommandCommitResult::WorkItemRevisionConflict {
            expected_revision: 9,
            actual_revision: Some(0),
        }
    );
    assert_eq!(
        repository.get_git_delivery("team-1", "delivery-1").await.unwrap(),
        Some(submitted)
    );
    assert!(
        repository
            .find_command_receipt("team-1", "reviewer", "key-3")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn expected_delivery_revision_guards_work_only_commands() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "git");
    repository
        .commit_command(&insert_work_command(created.clone(), "event-1", "key-1"))
        .await
        .unwrap();
    let submitted = delivery("delivery-1", "work-1", 1);
    repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: event(
                "event-2",
                "work-1",
                "producer",
                "submit_git_delivery",
                "key-2",
                "fingerprint-submit",
                Some(0),
                Some(("delivery-1", 0)),
            ),
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::Insert(submitted)),
        })
        .await
        .unwrap();

    let mut queued = created.clone();
    queued.state = "queued".into();
    queued.revision = 1;
    queued.updated_at += 1;
    let mut guarded_event = event(
        "event-3",
        "work-1",
        "controller",
        "queue_work_item",
        "key-3",
        "fingerprint-queue",
        Some(1),
        Some(("delivery-1", 0)),
    );
    guarded_event.expected_delivery_revision = Some(9);
    let outcome = repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: guarded_event,
            work_item: Some(TeamWorkItemMutation::CompareAndSwap {
                expected_revision: 0,
                row: queued,
            }),
            delivery: None,
        })
        .await
        .unwrap();

    assert_eq!(
        outcome,
        TeamCommandCommitResult::DeliveryRevisionConflict {
            expected_revision: 9,
            actual_revision: Some(0),
        }
    );
    assert_eq!(
        repository.get_work_item("team-1", "work-1").await.unwrap(),
        Some(created)
    );
    assert!(
        repository
            .find_command_receipt("team-1", "controller", "key-3")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn stale_work_item_cas_returns_actual_revision_without_an_event() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "none");
    repository
        .commit_command(&insert_work_command(created.clone(), "event-1", "key-1"))
        .await
        .unwrap();

    let mut queued = created.clone();
    queued.state = "queued".into();
    queued.revision = 1;
    queued.updated_at += 1;
    let queue = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-2",
            "work-1",
            "controller",
            "queue_work_item",
            "key-2",
            "fingerprint-queue",
            Some(1),
            None,
        ),
        work_item: Some(TeamWorkItemMutation::CompareAndSwap {
            expected_revision: 0,
            row: queued.clone(),
        }),
        delivery: None,
    };
    repository.commit_command(&queue).await.unwrap();

    let mut stale = created;
    stale.state = "running".into();
    stale.revision = 1;
    stale.updated_at += 2;
    let stale_command = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-3",
            "work-1",
            "producer",
            "start_work_item",
            "key-3",
            "fingerprint-start",
            Some(1),
            None,
        ),
        work_item: Some(TeamWorkItemMutation::CompareAndSwap {
            expected_revision: 0,
            row: stale,
        }),
        delivery: None,
    };

    let outcome = repository.commit_command(&stale_command).await.unwrap();

    assert_eq!(
        outcome,
        TeamCommandCommitResult::WorkItemRevisionConflict {
            expected_revision: 0,
            actual_revision: Some(1)
        }
    );
    assert_eq!(
        repository.get_work_item("team-1", "work-1").await.unwrap(),
        Some(queued)
    );
    assert_eq!(repository.list_work_events("team-1", "work-1").await.unwrap().len(), 2);
    assert!(
        repository
            .find_command_receipt("team-1", "producer", "key-3")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delivery_cas_conflict_rolls_back_earlier_work_item_cas() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "git");
    repository
        .commit_command(&insert_work_command(created.clone(), "event-1", "key-1"))
        .await
        .unwrap();

    let mut queued = created.clone();
    queued.state = "queued".into();
    queued.revision = 1;
    queued.updated_at += 1;
    let submitted_delivery = delivery("delivery-1", "work-1", 1);
    let submit = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-2",
            "work-1",
            "producer",
            "submit_git_delivery",
            "key-2",
            "fingerprint-submit",
            Some(1),
            Some(("delivery-1", 0)),
        ),
        work_item: Some(TeamWorkItemMutation::CompareAndSwap {
            expected_revision: 0,
            row: queued.clone(),
        }),
        delivery: Some(TeamGitDeliveryMutation::Insert(submitted_delivery.clone())),
    };
    repository.commit_command(&submit).await.unwrap();

    let mut running = queued.clone();
    running.state = "running".into();
    running.revision = 2;
    running.updated_at += 1;
    let mut impossible_delivery = submitted_delivery.clone();
    impossible_delivery.state = "accepted".into();
    impossible_delivery.revision = 100;
    impossible_delivery.updated_at += 1;
    let conflicting = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-3",
            "work-1",
            "reviewer",
            "accept_delivery",
            "key-3",
            "fingerprint-accept",
            Some(2),
            Some(("delivery-1", 100)),
        ),
        work_item: Some(TeamWorkItemMutation::CompareAndSwap {
            expected_revision: 1,
            row: running,
        }),
        delivery: Some(TeamGitDeliveryMutation::CompareAndSwap {
            expected_revision: 99,
            row: impossible_delivery,
        }),
    };

    let outcome = repository.commit_command(&conflicting).await.unwrap();

    assert_eq!(
        outcome,
        TeamCommandCommitResult::DeliveryRevisionConflict {
            expected_revision: 99,
            actual_revision: Some(0)
        }
    );
    assert_eq!(
        repository.get_work_item("team-1", "work-1").await.unwrap(),
        Some(queued)
    );
    assert_eq!(
        repository.get_git_delivery("team-1", "delivery-1").await.unwrap(),
        Some(submitted_delivery)
    );
    assert_eq!(repository.list_work_events("team-1", "work-1").await.unwrap().len(), 2);
}

#[tokio::test]
async fn git_delivery_identity_is_insert_only() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "git");
    repository
        .commit_command(&insert_work_command(created.clone(), "event-1", "key-1"))
        .await
        .unwrap();
    let original = delivery("delivery-1", "work-1", 1);
    let insert = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-2",
            "work-1",
            "producer",
            "submit_git_delivery",
            "key-2",
            "fingerprint-submit",
            Some(0),
            Some(("delivery-1", 0)),
        ),
        work_item: None,
        delivery: Some(TeamGitDeliveryMutation::Insert(original.clone())),
    };
    repository.commit_command(&insert).await.unwrap();

    let mut changed_identity = original.clone();
    changed_identity.head_commit = "different-head".into();
    changed_identity.state = "accepted".into();
    changed_identity.revision = 1;
    changed_identity.updated_at += 1;
    let update = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-3",
            "work-1",
            "reviewer",
            "accept_delivery",
            "key-3",
            "fingerprint-accept",
            Some(0),
            Some(("delivery-1", 1)),
        ),
        work_item: None,
        delivery: Some(TeamGitDeliveryMutation::CompareAndSwap {
            expected_revision: 0,
            row: changed_identity,
        }),
    };

    let error = repository.commit_command(&update).await.unwrap_err();

    assert!(matches!(error, DbError::Conflict(message) if message.contains("identity is immutable")));
    assert_eq!(
        repository.get_git_delivery("team-1", "delivery-1").await.unwrap(),
        Some(original)
    );
    assert!(
        repository
            .find_command_receipt("team-1", "reviewer", "key-3")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn delivery_only_mutation_cannot_forge_work_item_post_revision() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "git");
    repository
        .commit_command(&insert_work_command(created, "event-1", "key-1"))
        .await
        .unwrap();
    let submitted = delivery("delivery-1", "work-1", 1);
    repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: event(
                "event-2",
                "work-1",
                "producer",
                "submit_git_delivery",
                "key-2",
                "fingerprint-submit",
                Some(0),
                Some(("delivery-1", 0)),
            ),
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::Insert(submitted.clone())),
        })
        .await
        .unwrap();
    let mut accepted = submitted.clone();
    accepted.state = "accepted".into();
    accepted.revision = 1;
    accepted.updated_at += 1;
    let command = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-3",
            "work-1",
            "reviewer",
            "accept_delivery",
            "key-3",
            "fingerprint-accept",
            Some(99),
            Some(("delivery-1", 1)),
        ),
        work_item: None,
        delivery: Some(TeamGitDeliveryMutation::CompareAndSwap {
            expected_revision: 0,
            row: accepted,
        }),
    };
    let mut command = command;
    command.event.expected_work_item_revision = None;

    let error = repository.commit_command(&command).await.unwrap_err();

    assert!(
        matches!(error, DbError::Conflict(message) if message.contains("WorkItem revision does not match durable post-state"))
    );
    assert_eq!(
        repository.get_git_delivery("team-1", "delivery-1").await.unwrap(),
        Some(submitted)
    );
    assert!(
        repository
            .find_command_receipt("team-1", "reviewer", "key-3")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn work_only_mutation_cannot_forge_existing_delivery_post_revision() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "git");
    repository
        .commit_command(&insert_work_command(created.clone(), "event-1", "key-1"))
        .await
        .unwrap();
    let submitted = delivery("delivery-1", "work-1", 1);
    repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: event(
                "event-2",
                "work-1",
                "producer",
                "submit_git_delivery",
                "key-2",
                "fingerprint-submit",
                Some(0),
                Some(("delivery-1", 0)),
            ),
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::Insert(submitted)),
        })
        .await
        .unwrap();
    let mut queued = created.clone();
    queued.state = "queued".into();
    queued.revision = 1;
    queued.updated_at += 1;
    let command = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-3",
            "work-1",
            "controller",
            "queue_work_item",
            "key-3",
            "fingerprint-queue-with-delivery",
            Some(1),
            Some(("delivery-1", 99)),
        ),
        work_item: Some(TeamWorkItemMutation::CompareAndSwap {
            expected_revision: 0,
            row: queued,
        }),
        delivery: None,
    };
    let mut command = command;
    command.event.expected_delivery_revision = None;

    let error = repository.commit_command(&command).await.unwrap_err();

    assert!(
        matches!(error, DbError::Conflict(message) if message.contains("delivery revision does not match durable post-state"))
    );
    assert_eq!(
        repository.get_work_item("team-1", "work-1").await.unwrap(),
        Some(created)
    );
    assert!(
        repository
            .find_command_receipt("team-1", "controller", "key-3")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn event_delivery_must_belong_to_the_event_work_item() {
    let (repository, _database) = repository().await;
    let work_one = work_item("work-1", "git");
    let work_two = work_item("work-2", "git");
    repository
        .commit_command(&insert_work_command(work_one.clone(), "event-1", "key-1"))
        .await
        .unwrap();
    repository
        .commit_command(&insert_work_command(work_two, "event-2", "key-2"))
        .await
        .unwrap();
    let delivery_two = delivery("delivery-2", "work-2", 1);
    repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: event(
                "event-3",
                "work-2",
                "producer",
                "submit_git_delivery",
                "key-3",
                "fingerprint-submit-work-2",
                Some(0),
                Some(("delivery-2", 0)),
            ),
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::Insert(delivery_two)),
        })
        .await
        .unwrap();
    let mut queued = work_one.clone();
    queued.state = "queued".into();
    queued.revision = 1;
    queued.updated_at += 1;
    let command = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-4",
            "work-1",
            "controller",
            "queue_work_item",
            "key-4",
            "fingerprint-cross-work-delivery",
            Some(1),
            Some(("delivery-2", 0)),
        ),
        work_item: Some(TeamWorkItemMutation::CompareAndSwap {
            expected_revision: 0,
            row: queued,
        }),
        delivery: None,
    };
    let mut command = command;
    command.event.expected_delivery_revision = Some(0);

    let error = repository.commit_command(&command).await.unwrap_err();

    assert!(matches!(error, DbError::Conflict(message) if message.contains("delivery does not belong")));
    assert_eq!(
        repository.get_work_item("team-1", "work-1").await.unwrap(),
        Some(work_one)
    );
    assert!(
        repository
            .find_command_receipt("team-1", "controller", "key-4")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn content_revision_is_unique_per_team_and_work_item() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "git");
    repository
        .commit_command(&insert_work_command(created, "event-1", "key-1"))
        .await
        .unwrap();

    let first = delivery("delivery-1", "work-1", 1);
    repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: event(
                "event-2",
                "work-1",
                "producer",
                "submit_git_delivery",
                "key-2",
                "fingerprint-submit-1",
                Some(0),
                Some(("delivery-1", 0)),
            ),
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::Insert(first)),
        })
        .await
        .unwrap();

    let duplicate_revision = delivery("delivery-2", "work-1", 1);
    let duplicate = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-3",
            "work-1",
            "producer",
            "submit_git_delivery",
            "key-3",
            "fingerprint-submit-2",
            Some(0),
            Some(("delivery-2", 0)),
        ),
        work_item: None,
        delivery: Some(TeamGitDeliveryMutation::Insert(duplicate_revision)),
    };

    let error = repository.commit_command(&duplicate).await.unwrap_err();

    assert!(error.is_unique_violation(), "expected unique violation, got {error:?}");
    assert!(
        repository
            .find_git_delivery("team-1", "work-1", 1)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        repository
            .find_command_receipt("team-1", "producer", "key-3")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn parent_work_item_cannot_cross_team_scope() {
    let (repository, database) = repository().await;
    let teams = SqliteTeamRepository::new(database.pool().clone());
    teams.create_team(&team("team-2")).await.unwrap();
    repository
        .commit_command(&insert_work_command(work_item("parent", "none"), "event-1", "key-1"))
        .await
        .unwrap();

    let mut child = work_item("child", "none");
    child.team_id = "team-2".into();
    child.parent_work_item_id = Some("parent".into());
    let mut command = insert_work_command(child, "event-2", "key-2");
    command.event.team_id = "team-2".into();

    let error = repository.commit_command(&command).await.unwrap_err();

    assert!(matches!(error, DbError::Query(_)));
    assert!(repository.get_work_item("team-2", "child").await.unwrap().is_none());
}

#[tokio::test]
async fn invalid_revision_step_is_rejected_before_any_write() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "none");
    repository
        .commit_command(&insert_work_command(created.clone(), "event-1", "key-1"))
        .await
        .unwrap();
    let mut skipped = created.clone();
    skipped.state = "running".into();
    skipped.revision = 2;
    skipped.updated_at += 1;
    let command = CommitTeamCommandParams {
        roster_guard: None,
        work_item_guards: Vec::new(),
        notifications: Vec::new(),
        event: event(
            "event-2",
            "work-1",
            "producer",
            "start_work_item",
            "key-2",
            "fingerprint-start",
            Some(2),
            None,
        ),
        work_item: Some(TeamWorkItemMutation::CompareAndSwap {
            expected_revision: 0,
            row: skipped,
        }),
        delivery: None,
    };

    let error = repository.commit_command(&command).await.unwrap_err();

    assert!(matches!(error, DbError::Conflict(message) if message.contains("expected revision + 1")));
    assert_eq!(
        repository.get_work_item("team-1", "work-1").await.unwrap(),
        Some(created)
    );
    assert_eq!(repository.list_work_events("team-1", "work-1").await.unwrap().len(), 1);
}

#[tokio::test]
async fn event_expected_work_item_revision_must_describe_the_mutation_pre_state() {
    let (repository, _database) = repository().await;
    let created = work_item("work-1", "none");
    let mut invalid_insert = insert_work_command(created.clone(), "event-1", "key-1");
    invalid_insert.event.expected_work_item_revision = Some(0);

    let error = repository.commit_command(&invalid_insert).await.unwrap_err();

    assert!(matches!(error, DbError::Conflict(message) if message.contains("expected WorkItem revision")));
    assert!(repository.get_work_item("team-1", "work-1").await.unwrap().is_none());

    repository
        .commit_command(&insert_work_command(created.clone(), "event-2", "key-2"))
        .await
        .unwrap();
    let mut queued = created.clone();
    queued.state = "queued".into();
    queued.revision = 1;
    queued.updated_at += 1;
    let mut invalid_cas_event = event(
        "event-3",
        "work-1",
        "controller",
        "queue_work_item",
        "key-3",
        "fingerprint-queue",
        Some(1),
        None,
    );
    invalid_cas_event.expected_work_item_revision = Some(1);
    let error = repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: invalid_cas_event,
            work_item: Some(TeamWorkItemMutation::CompareAndSwap {
                expected_revision: 0,
                row: queued,
            }),
            delivery: None,
        })
        .await
        .unwrap_err();

    assert!(matches!(error, DbError::Conflict(message) if message.contains("expected WorkItem revision")));
    assert_eq!(
        repository.get_work_item("team-1", "work-1").await.unwrap(),
        Some(created)
    );
}

#[tokio::test]
async fn event_expected_delivery_revision_must_describe_the_mutation_pre_state() {
    let (repository, _database) = repository().await;
    repository
        .commit_command(&insert_work_command(work_item("work-1", "git"), "event-1", "key-1"))
        .await
        .unwrap();
    let submitted = delivery("delivery-1", "work-1", 1);
    let mut invalid_insert_event = event(
        "event-2",
        "work-1",
        "producer",
        "submit_git_delivery",
        "key-2",
        "fingerprint-submit-invalid",
        Some(0),
        Some(("delivery-1", 0)),
    );
    invalid_insert_event.expected_delivery_revision = Some(0);
    let error = repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: invalid_insert_event,
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::Insert(submitted.clone())),
        })
        .await
        .unwrap_err();

    assert!(matches!(error, DbError::Conflict(message) if message.contains("expected delivery revision")));
    assert!(
        repository
            .get_git_delivery("team-1", "delivery-1")
            .await
            .unwrap()
            .is_none()
    );

    repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: event(
                "event-3",
                "work-1",
                "producer",
                "submit_git_delivery",
                "key-3",
                "fingerprint-submit",
                Some(0),
                Some(("delivery-1", 0)),
            ),
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::Insert(submitted.clone())),
        })
        .await
        .unwrap();
    let mut accepted = submitted.clone();
    accepted.state = "accepted".into();
    accepted.revision = 1;
    accepted.updated_at += 1;
    let mut invalid_cas_event = event(
        "event-4",
        "work-1",
        "reviewer",
        "accept_delivery",
        "key-4",
        "fingerprint-accept",
        Some(0),
        Some(("delivery-1", 1)),
    );
    invalid_cas_event.expected_delivery_revision = Some(1);
    let error = repository
        .commit_command(&CommitTeamCommandParams {
            roster_guard: None,
            work_item_guards: Vec::new(),
            notifications: Vec::new(),
            event: invalid_cas_event,
            work_item: None,
            delivery: Some(TeamGitDeliveryMutation::CompareAndSwap {
                expected_revision: 0,
                row: accepted,
            }),
        })
        .await
        .unwrap_err();

    assert!(matches!(error, DbError::Conflict(message) if message.contains("expected delivery revision")));
    assert_eq!(
        repository.get_git_delivery("team-1", "delivery-1").await.unwrap(),
        Some(submitted)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_a_command_mid_write_releases_the_managed_immediate_transaction() {
    let (repository, database) = repository().await;
    let (write_started_tx, write_started_rx) = mpsc::channel();
    let release_write = Arc::new((Mutex::new(false), Condvar::new()));
    let release_write_from_hook = Arc::clone(&release_write);
    let block_once = Arc::new(AtomicBool::new(true));
    let block_once_from_hook = Arc::clone(&block_once);
    let mut connection = database.pool().acquire().await.unwrap();
    connection.lock_handle().await.unwrap().set_update_hook(move |update| {
        if update.table == "team_work_items" && block_once_from_hook.swap(false, Ordering::SeqCst) {
            write_started_tx.send(()).unwrap();
            let (released, notification) = &*release_write_from_hook;
            let mut released = released.lock().unwrap();
            while !*released {
                released = notification.wait(released).unwrap();
            }
        }
    });
    drop(connection);

    let cancelled_repository = repository.clone();
    let cancelled_command = insert_work_command(work_item("work-cancelled", "none"), "event-1", "key-1");
    let cancelled = tokio::spawn(async move { cancelled_repository.commit_command(&cancelled_command).await });
    write_started_rx
        .recv_timeout(Duration::from_secs(2))
        .expect("cancelled command should reach the SQLite write hook");
    cancelled.abort();
    {
        let (released, notification) = &*release_write;
        *released.lock().unwrap() = true;
        notification.notify_all();
    }
    let join_error = tokio::time::timeout(Duration::from_secs(2), cancelled)
        .await
        .expect("cancelled command task should stop")
        .unwrap_err();
    assert!(join_error.is_cancelled());

    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        repository.commit_command(&insert_work_command(
            work_item("work-after-cancel", "none"),
            "event-2",
            "key-2",
        )),
    )
    .await
    .expect("managed transaction drop should release the SQLite writer lock")
    .unwrap();

    assert!(matches!(outcome, TeamCommandCommitResult::Applied(_)));
    assert!(
        repository
            .get_work_item("team-1", "work-cancelled")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        repository
            .get_work_item("team-1", "work-after-cancel")
            .await
            .unwrap()
            .is_some()
    );
}
