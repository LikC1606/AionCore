use std::sync::Arc;

use aionui_common::now_ms;
use aionui_db::models::TeamRow;
use aionui_db::{
    ITeamModeRepository, ITeamRepository, SqliteTeamModeRepository, SqliteTeamRepository, init_database,
    init_database_memory,
};
use aionui_realtime::BroadcastEventBus;

use super::*;
use crate::kernel::{
    DeliveryRequirement, GitDeliveryLifecycle, GitDeliveryRef, GitDeliveryState, GitWorkAssignment,
    IntegrationRecoveryReason, WorkItemLifecycle, WorkItemState, WorkSubmission,
};
use crate::types::{TeamAgent, TeammateRole};

const USER_ID: &str = "system_default_user";
const TEAM_ID: &str = "team-command-test";

struct Harness {
    service: TeamCommandService,
    team_repo: Arc<SqliteTeamRepository>,
    mode_repo: Arc<SqliteTeamModeRepository>,
    lead: TeamCommandPrincipal,
    worker: TeamCommandPrincipal,
    peer: TeamCommandPrincipal,
}

async fn setup() -> Harness {
    let db = init_database_memory().await.unwrap();
    let team_repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    let mode_repo = Arc::new(SqliteTeamModeRepository::new(db.pool().clone()));
    team_repo.create_team(&team_row()).await.unwrap();

    Harness {
        service: TeamCommandService::new(team_repo.clone(), mode_repo.clone()),
        team_repo,
        mode_repo,
        lead: TeamCommandPrincipal::from_runtime(USER_ID, TEAM_ID, "conv-lead"),
        worker: TeamCommandPrincipal::from_runtime(USER_ID, TEAM_ID, "conv-worker"),
        peer: TeamCommandPrincipal::from_runtime(USER_ID, TEAM_ID, "conv-peer"),
    }
}

#[tokio::test]
async fn applied_command_emits_one_invalidation_and_replay_emits_none() {
    let db = init_database_memory().await.unwrap();
    let team_repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    let mode_repo = Arc::new(SqliteTeamModeRepository::new(db.pool().clone()));
    team_repo.create_team(&team_row()).await.unwrap();
    let event_bus = Arc::new(BroadcastEventBus::new(8));
    let mut events = event_bus.subscribe();
    let service = TeamCommandService::new_with_event_broadcaster(team_repo, mode_repo, event_bus);
    let lead = TeamCommandPrincipal::from_runtime(USER_ID, TEAM_ID, "conv-lead");

    let applied = service
        .execute(
            &lead,
            "emit-work-invalidation",
            create_command(DeliveryRequirement::None),
        )
        .await
        .unwrap();
    assert!(!applied.replayed);
    let event = events.recv().await.unwrap();
    assert_eq!(event.name, crate::events::TEAM_WORK_CHANGED_EVENT);
    assert_eq!(event.data["team_id"], TEAM_ID);
    assert_eq!(event.data["work_item_id"], applied.result.work_item_id);
    assert_eq!(event.data["event_sequence"], applied.event_sequence);

    let replayed = service
        .execute(
            &lead,
            "emit-work-invalidation",
            create_command(DeliveryRequirement::None),
        )
        .await
        .unwrap();
    assert!(replayed.replayed);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

fn team_row() -> TeamRow {
    let agents = vec![
        member("lead", "conv-lead", TeammateRole::Lead),
        member("worker", "conv-worker", TeammateRole::Teammate),
        member("peer", "conv-peer", TeammateRole::Teammate),
    ];
    TeamRow {
        id: TEAM_ID.into(),
        user_id: USER_ID.into(),
        name: "Command Team".into(),
        workspace: "/tmp/team-command".into(),
        workspace_mode: "shared".into(),
        agents: serde_json::to_string(&agents).unwrap(),
        lead_agent_id: Some("lead".into()),
        session_mode: None,
        agents_version: "1".into(),
        created_at: now_ms(),
        updated_at: now_ms(),
    }
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

fn create_command(requirement: DeliveryRequirement) -> TeamCommand {
    TeamCommand::CreateWorkItem {
        parent_work_item_id: None,
        subject: "Implement the scoped change".into(),
        description: Some("Acceptance is covered by the command lifecycle".into()),
        assignee_member_id: "worker".into(),
        delivery_requirement: requirement,
        git_assignment: (requirement == DeliveryRequirement::Git)
            .then(|| GitWorkAssignment::new("repo-1", "base-1", "refs/heads/ds/worker-assignment")),
    }
}

async fn create_and_start(harness: &Harness, requirement: DeliveryRequirement, key_prefix: &str) -> String {
    let created = harness
        .service
        .execute(
            &harness.lead,
            &format!("{key_prefix}-create"),
            create_command(requirement),
        )
        .await
        .unwrap();
    let work_item_id = created.result.work_item_id;
    harness
        .service
        .execute(
            &harness.lead,
            &format!("{key_prefix}-queue"),
            TeamCommand::Queue {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 0,
                prepared_workspace: (requirement == DeliveryRequirement::Git).then(|| "/repo/.worktrees/worker".into()),
            },
        )
        .await
        .unwrap();
    harness
        .service
        .execute(
            &harness.worker,
            &format!("{key_prefix}-start"),
            TeamCommand::Start {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 1,
            },
        )
        .await
        .unwrap();
    work_item_id
}

async fn submit_and_accept_git(harness: &Harness, work_item_id: &str, key_prefix: &str) -> String {
    let submitted = harness
        .service
        .execute(
            &harness.worker,
            &format!("{key_prefix}-submit"),
            TeamCommand::SubmitGit {
                work_item_id: work_item_id.into(),
                expected_work_revision: 2,
                content_revision: 1,
                head_commit: "head-1".into(),
                evidence: "Git delivery is ready for review.".into(),
            },
        )
        .await
        .unwrap();
    let delivery_id = submitted.result.delivery.unwrap().delivery_id;
    harness
        .service
        .execute(
            &harness.lead,
            &format!("{key_prefix}-review"),
            TeamCommand::BeginReview {
                work_item_id: work_item_id.into(),
                expected_work_revision: 3,
                expected_delivery_revision: Some(0),
            },
        )
        .await
        .unwrap();
    harness
        .service
        .execute(
            &harness.lead,
            &format!("{key_prefix}-accept"),
            TeamCommand::Accept {
                work_item_id: work_item_id.into(),
                expected_work_revision: 4,
                expected_delivery_revision: Some(0),
            },
        )
        .await
        .unwrap();
    delivery_id
}

#[tokio::test]
async fn non_git_flow_completes_and_replays_after_state_advanced() {
    let harness = setup().await;
    let create = create_command(DeliveryRequirement::None);
    let created = harness
        .service
        .execute(&harness.lead, "create-1", create.clone())
        .await
        .unwrap();
    let replayed = harness
        .service
        .execute(&harness.lead, "create-1", create)
        .await
        .unwrap();
    assert!(replayed.replayed);
    assert_eq!(replayed.event_sequence, created.event_sequence);
    assert_eq!(replayed.result, created.result);

    let conflict = harness
        .service
        .execute(
            &harness.lead,
            "create-1",
            TeamCommand::CreateWorkItem {
                parent_work_item_id: None,
                subject: "Different command".into(),
                description: Some("Acceptance is covered by the command lifecycle".into()),
                assignee_member_id: "worker".into(),
                delivery_requirement: DeliveryRequirement::None,
                git_assignment: None,
            },
        )
        .await;
    assert!(matches!(conflict, Err(TeamCommandError::IdempotencyConflict)));

    let work_item_id = created.result.work_item_id;
    for (principal, key, command) in [
        (
            &harness.lead,
            "queue-1",
            TeamCommand::Queue {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 0,
                prepared_workspace: None,
            },
        ),
        (
            &harness.worker,
            "start-1",
            TeamCommand::Start {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 1,
            },
        ),
        (
            &harness.worker,
            "submit-1",
            TeamCommand::SubmitInline {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 2,
                evidence: "Inline result is ready for review.".into(),
            },
        ),
        (
            &harness.lead,
            "review-1",
            TeamCommand::BeginReview {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 3,
                expected_delivery_revision: None,
            },
        ),
        (
            &harness.lead,
            "accept-1",
            TeamCommand::Accept {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 4,
                expected_delivery_revision: None,
            },
        ),
        (
            &harness.lead,
            "complete-1",
            TeamCommand::CompleteWithoutDelivery {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 5,
            },
        ),
    ] {
        harness.service.execute(principal, key, command).await.unwrap();
    }

    let row = harness
        .mode_repo
        .get_work_item(TEAM_ID, &work_item_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.state, "completed");
    assert_eq!(row.revision, 6);
    let events = harness
        .mode_repo
        .list_work_events(TEAM_ID, &work_item_id)
        .await
        .unwrap();
    assert_eq!(events.len(), 7);
    assert_eq!(events[0].expected_work_item_revision, None);
    assert_eq!(
        events[1..]
            .iter()
            .map(|event| event.expected_work_item_revision)
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)]
    );

    let replay = harness
        .service
        .execute(
            &harness.lead,
            "accept-1",
            TeamCommand::Accept {
                work_item_id,
                expected_work_revision: 4,
                expected_delivery_revision: None,
            },
        )
        .await
        .unwrap();
    assert!(replay.replayed);
    assert_eq!(replay.result.work_item_state, WorkItemState::Accepted);
}

#[tokio::test]
async fn actor_identity_and_peer_relationship_fail_closed() {
    let harness = setup().await;
    let peer_delegate = harness
        .service
        .execute(
            &harness.worker,
            "peer-create",
            TeamCommand::CreateWorkItem {
                parent_work_item_id: None,
                subject: "Implement the scoped change".into(),
                description: None,
                assignee_member_id: "peer".into(),
                delivery_requirement: DeliveryRequirement::None,
                git_assignment: None,
            },
        )
        .await;
    assert!(matches!(peer_delegate, Err(TeamCommandError::RelationPolicy(_))));

    let wrong_user = TeamCommandPrincipal::from_runtime("other-user", TEAM_ID, "conv-lead");
    assert!(matches!(
        harness
            .service
            .execute(&wrong_user, "wrong-user", create_command(DeliveryRequirement::None))
            .await,
        Err(TeamCommandError::ForbiddenTeam)
    ));

    let unknown_caller = TeamCommandPrincipal::from_runtime(USER_ID, TEAM_ID, "conv-missing");
    assert!(matches!(
        harness
            .service
            .execute(
                &unknown_caller,
                "unknown-caller",
                create_command(DeliveryRequirement::None),
            )
            .await,
        Err(TeamCommandError::CallerNotMember)
    ));

    let work_item_id = create_and_start(&harness, DeliveryRequirement::None, "auth").await;
    assert!(matches!(
        harness
            .service
            .execute(
                &harness.peer,
                "peer-start",
                TeamCommand::Start {
                    work_item_id,
                    expected_work_revision: 2,
                },
            )
            .await,
        Err(TeamCommandError::WorkItemPolicy(_))
    ));
}

#[tokio::test]
async fn authenticated_owner_is_bound_to_lead_identity() {
    let harness = setup().await;
    let receipt = harness
        .service
        .execute_as_owner(
            USER_ID,
            TEAM_ID,
            "owner-create",
            create_command(DeliveryRequirement::None),
        )
        .await
        .unwrap();
    let events = harness
        .mode_repo
        .list_work_events(TEAM_ID, &receipt.result.work_item_id)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].actor_member_id, "lead");

    let forbidden = harness
        .service
        .execute_as_owner(
            "other-user",
            TEAM_ID,
            "owner-forbidden",
            create_command(DeliveryRequirement::None),
        )
        .await;
    assert!(matches!(forbidden, Err(TeamCommandError::ForbiddenTeam)));
}

#[tokio::test]
async fn git_merge_is_exact_atomic_and_integrating_cannot_be_cancelled() {
    let harness = setup().await;
    let work_item_id = create_and_start(&harness, DeliveryRequirement::Git, "git").await;
    let delivery_id = submit_and_accept_git(&harness, &work_item_id, "git").await;
    let integrating = harness
        .service
        .execute(
            &harness.lead,
            "git-integrate",
            TeamCommand::BeginIntegration {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 5,
                expected_delivery_revision: 1,
                target_ref: "refs/heads/main".into(),
                target_head: "target-1".into(),
            },
        )
        .await
        .unwrap();
    let attempt_id = integrating
        .result
        .delivery
        .as_ref()
        .and_then(|delivery| delivery.integration_attempt_id.clone())
        .unwrap();

    let cancel = harness
        .service
        .execute(
            &harness.lead,
            "git-cancel-integrating",
            TeamCommand::Cancel {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 5,
                expected_delivery_revision: Some(2),
            },
        )
        .await;
    assert!(matches!(cancel, Err(TeamCommandError::GitDeliveryTransition(_))));
    let before_merge = harness
        .mode_repo
        .get_git_delivery(TEAM_ID, &delivery_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before_merge.state, "integrating");
    assert_eq!(before_merge.revision, 2);

    let merged = harness
        .service
        .execute(
            &harness.lead,
            "git-merged",
            TeamCommand::ResolveIntegrationMerged {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 5,
                expected_delivery_revision: 2,
                evidence: MergedIntegrationEvidence {
                    attempt_id,
                    merged_commit: "merge-1".into(),
                    observed_target_head: "merge-1".into(),
                },
            },
        )
        .await
        .unwrap();
    assert_eq!(merged.result.work_item_state, WorkItemState::Completed);
    assert_eq!(merged.result.delivery.as_ref().unwrap().state, GitDeliveryState::Merged);

    let work = harness
        .mode_repo
        .get_work_item(TEAM_ID, &work_item_id)
        .await
        .unwrap()
        .unwrap();
    let delivery = harness
        .mode_repo
        .get_git_delivery(TEAM_ID, &delivery_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((work.state.as_str(), work.revision), ("completed", 6));
    assert_eq!((delivery.state.as_str(), delivery.revision), ("merged", 3));
    assert_eq!(delivery.merged_commit.as_deref(), Some("merge-1"));
    let events = harness
        .mode_repo
        .list_work_events(TEAM_ID, &work_item_id)
        .await
        .unwrap();
    let review_event = events
        .iter()
        .find(|event| event.command_name == "begin_review")
        .unwrap();
    assert_eq!(review_event.expected_work_item_revision, Some(3));
    assert_eq!(review_event.expected_delivery_revision, Some(0));
    let merge_event = events
        .into_iter()
        .find(|event| event.command_name == "resolve_integration_merged")
        .unwrap();
    assert_eq!(merge_event.expected_work_item_revision, Some(5));
    assert_eq!(merge_event.expected_delivery_revision, Some(2));
}

#[tokio::test]
async fn git_conflict_atomically_reopens_work_item() {
    let harness = setup().await;
    let work_item_id = create_and_start(&harness, DeliveryRequirement::Git, "conflict").await;
    let delivery_id = submit_and_accept_git(&harness, &work_item_id, "conflict").await;
    let integrating = harness
        .service
        .execute(
            &harness.lead,
            "conflict-integrate",
            TeamCommand::BeginIntegration {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 5,
                expected_delivery_revision: 1,
                target_ref: "refs/heads/main".into(),
                target_head: "target-1".into(),
            },
        )
        .await
        .unwrap();
    let attempt_id = integrating
        .result
        .delivery
        .as_ref()
        .and_then(|delivery| delivery.integration_attempt_id.clone())
        .unwrap();
    let conflict_command = TeamCommand::ResolveIntegrationConflict {
        work_item_id: work_item_id.clone(),
        expected_work_revision: 5,
        expected_delivery_revision: 2,
        evidence: ConflictedIntegrationEvidence {
            attempt_id,
            observed_target_head: "target-1".into(),
        },
    };
    harness
        .service
        .execute(&harness.lead, "conflict-record", conflict_command.clone())
        .await
        .unwrap();
    let replayed = harness
        .service
        .execute(&harness.lead, "conflict-record", conflict_command)
        .await
        .unwrap();
    assert!(replayed.replayed);

    let work = harness
        .mode_repo
        .get_work_item(TEAM_ID, &work_item_id)
        .await
        .unwrap()
        .unwrap();
    let delivery = harness
        .mode_repo
        .get_git_delivery(TEAM_ID, &delivery_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((work.state.as_str(), work.revision), ("changes_requested", 6));
    assert!(work.current_submission_json.is_none());
    assert_eq!((delivery.state.as_str(), delivery.revision), ("conflicted", 3));
    let notifications = harness
        .team_repo
        .get_history(TEAM_ID, "worker", None)
        .await
        .unwrap()
        .into_iter()
        .filter(|message| message.content.contains("Git integration conflicted"))
        .collect::<Vec<_>>();
    assert_eq!(notifications.len(), 1);
    assert!(notifications[0].content.contains(&work_item_id));
    assert!(notifications[0].content.contains("target-1"));
    assert!(!notifications[0].content.contains("repo-1"));
}

#[tokio::test]
async fn git_submission_uses_the_persisted_assignment_coordinates() {
    let harness = setup().await;
    let work_item_id = create_and_start(&harness, DeliveryRequirement::Git, "assignment-derived").await;
    let submitted = harness
        .service
        .execute(
            &harness.worker,
            "assignment-derived-submit",
            TeamCommand::SubmitGit {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 2,
                content_revision: 1,
                head_commit: "head-1".into(),
                evidence: "Persisted assignment delivery is ready.".into(),
            },
        )
        .await
        .unwrap();
    let delivery_id = submitted.result.delivery.unwrap().delivery_id;
    let delivery = harness
        .mode_repo
        .get_git_delivery(TEAM_ID, &delivery_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.repository_id, "repo-1");
    assert_eq!(delivery.base_commit, "base-1");
    assert_eq!(delivery.branch_ref, "refs/heads/ds/worker-assignment");
    assert_eq!(delivery.head_commit, "head-1");
}

#[tokio::test]
async fn changes_requested_resubmit_keeps_the_original_git_assignment() {
    let harness = setup().await;
    let work_item_id = create_and_start(&harness, DeliveryRequirement::Git, "assignment-resubmit").await;
    let first = harness
        .service
        .execute(
            &harness.worker,
            "assignment-resubmit-submit-1",
            TeamCommand::SubmitGit {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 2,
                content_revision: 1,
                head_commit: "head-1".into(),
                evidence: "First delivery is ready.".into(),
            },
        )
        .await
        .unwrap();
    let first_delivery_id = first.result.delivery.unwrap().delivery_id;
    harness
        .service
        .execute(
            &harness.lead,
            "assignment-resubmit-review",
            TeamCommand::BeginReview {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 3,
                expected_delivery_revision: Some(0),
            },
        )
        .await
        .unwrap();
    harness
        .service
        .execute(
            &harness.lead,
            "assignment-resubmit-changes",
            TeamCommand::RequestChanges {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 4,
                expected_delivery_revision: Some(0),
                feedback: "Address the requested changes.".into(),
            },
        )
        .await
        .unwrap();
    harness
        .service
        .execute(
            &harness.worker,
            "assignment-resubmit-restart",
            TeamCommand::Start {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 5,
            },
        )
        .await
        .unwrap();
    let reused_revision = harness
        .service
        .execute(
            &harness.worker,
            "assignment-resubmit-reuse-content-revision",
            TeamCommand::SubmitGit {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 6,
                content_revision: 1,
                head_commit: "head-2".into(),
                evidence: "Attempted replacement delivery.".into(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        reused_revision,
        TeamCommandError::GitContentRevisionAlreadyUsed(1)
    ));
    let second = harness
        .service
        .execute(
            &harness.worker,
            "assignment-resubmit-submit-2",
            TeamCommand::SubmitGit {
                work_item_id: work_item_id.clone(),
                expected_work_revision: 6,
                content_revision: 2,
                head_commit: "head-2".into(),
                evidence: "Revised delivery is ready.".into(),
            },
        )
        .await
        .unwrap();

    assert_ne!(second.result.delivery.as_ref().unwrap().delivery_id, first_delivery_id);
    let stored = harness
        .mode_repo
        .get_work_item(TEAM_ID, &work_item_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.git_repository_id.as_deref(), Some("repo-1"));
    assert_eq!(stored.git_base_commit.as_deref(), Some("base-1"));
    assert_eq!(
        stored.git_branch_ref.as_deref(),
        Some("refs/heads/ds/worker-assignment")
    );
    let deliveries = harness
        .mode_repo
        .list_git_deliveries(TEAM_ID, Some(&work_item_id))
        .await
        .unwrap();
    assert_eq!(deliveries.len(), 2);
    assert!(deliveries.iter().all(|delivery| {
        delivery.repository_id == "repo-1"
            && delivery.base_commit == "base-1"
            && delivery.branch_ref == "refs/heads/ds/worker-assignment"
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_same_key_creates_one_work_item_and_one_event() {
    let harness = setup().await;
    let Harness {
        service,
        mode_repo,
        lead,
        ..
    } = harness;
    let service = Arc::new(service);
    let lead_a = lead.clone();
    let lead_b = lead;
    let command = create_command(DeliveryRequirement::None);
    let command_b = command.clone();

    let first_call = service.execute(&lead_a, "concurrent-create", command);
    let second_call = service.execute(&lead_b, "concurrent-create", command_b);
    let (first, second) = tokio::join!(first_call, second_call);
    let first = first.unwrap();
    let second = second.unwrap();

    assert_ne!(first.replayed, second.replayed);
    assert_eq!(first.event_sequence, second.event_sequence);
    assert_eq!(first.result, second.result);
    let events = mode_repo
        .list_work_events(TEAM_ID, &first.result.work_item_id)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
}

#[tokio::test]
async fn receipt_replays_after_database_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("team-mode-restart.db");
    let command = create_command(DeliveryRequirement::None);

    let first = {
        let db = init_database(&path).await.unwrap();
        let team_repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        let mode_repo = Arc::new(SqliteTeamModeRepository::new(db.pool().clone()));
        team_repo.create_team(&team_row()).await.unwrap();
        let service = TeamCommandService::new(team_repo.clone(), mode_repo.clone());
        let lead = TeamCommandPrincipal::from_runtime(USER_ID, TEAM_ID, "conv-lead");
        let receipt = service.execute(&lead, "restart-create", command.clone()).await.unwrap();
        drop(service);
        drop(mode_repo);
        drop(team_repo);
        db.close().await;
        receipt
    };

    let db = init_database(&path).await.unwrap();
    let team_repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    let mode_repo = Arc::new(SqliteTeamModeRepository::new(db.pool().clone()));
    let service = TeamCommandService::new(team_repo.clone(), mode_repo.clone());
    let lead = TeamCommandPrincipal::from_runtime(USER_ID, TEAM_ID, "conv-lead");
    let replay = service.execute(&lead, "restart-create", command).await.unwrap();

    assert!(replay.replayed);
    assert_eq!(replay.event_sequence, first.event_sequence);
    assert_eq!(replay.result, first.result);
    let events = mode_repo
        .list_work_events(TEAM_ID, &first.result.work_item_id)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);

    drop(service);
    drop(mode_repo);
    drop(team_repo);
    db.close().await;
}

#[test]
fn adapter_command_names_are_stable() {
    let commands = [
        TeamCommand::Block {
            work_item_id: "work".into(),
            expected_work_revision: 2,
            context: "Blocked on a missing dependency.".into(),
        },
        TeamCommand::Resume {
            work_item_id: "work".into(),
            expected_work_revision: 3,
        },
        TeamCommand::RequestChanges {
            work_item_id: "work".into(),
            expected_work_revision: 4,
            expected_delivery_revision: None,
            feedback: "Add the missing validation.".into(),
        },
        TeamCommand::Reject {
            work_item_id: "work".into(),
            expected_work_revision: 4,
            expected_delivery_revision: None,
        },
        TeamCommand::ResolveIntegrationRetryable {
            work_item_id: "work".into(),
            expected_work_revision: 5,
            expected_delivery_revision: 2,
            evidence: RetryableIntegrationEvidence {
                attempt_id: "attempt".into(),
                reason: IntegrationRecoveryReason::Interrupted,
                observed_target_head: None,
            },
        },
    ];
    assert_eq!(
        commands.map(|command| command.name()),
        [
            "block",
            "resume",
            "request_changes",
            "reject",
            "resolve_integration_retryable"
        ]
    );
}

#[test]
fn cancelled_pair_preserves_whether_delivery_was_accepted() {
    let delivery_ref = GitDeliveryRef::new(
        "delivery-cancelled",
        TEAM_ID,
        "work-cancelled",
        "worker",
        "repo-1",
        1,
        "base-1",
        "refs/heads/ds/work-cancelled/worker",
        "head-1",
    );
    let submission = Some(WorkSubmission::git(delivery_ref.clone()));
    let cancelled_before_accept = WorkItemLifecycle::restore(
        TEAM_ID,
        "work-cancelled",
        WorkItemState::Cancelled,
        DeliveryRequirement::Git,
        Some(GitWorkAssignment::new(
            "repo-1",
            "base-1",
            "refs/heads/ds/work-cancelled/worker",
        )),
        submission.clone(),
        None,
        4,
    )
    .unwrap();
    let cancelled_after_accept = WorkItemLifecycle::restore(
        TEAM_ID,
        "work-cancelled",
        WorkItemState::Cancelled,
        DeliveryRequirement::Git,
        Some(GitWorkAssignment::new(
            "repo-1",
            "base-1",
            "refs/heads/ds/work-cancelled/worker",
        )),
        submission,
        Some(delivery_ref.clone()),
        6,
    )
    .unwrap();
    let abandoned_before_accept =
        GitDeliveryLifecycle::restore(delivery_ref.clone(), GitDeliveryState::Abandoned, 1, None).unwrap();
    let abandoned_after_accept =
        GitDeliveryLifecycle::restore(delivery_ref, GitDeliveryState::Abandoned, 2, None).unwrap();

    assert!(super::service::validate_pair_state(&cancelled_before_accept, &abandoned_before_accept).is_ok());
    assert!(super::service::validate_pair_state(&cancelled_after_accept, &abandoned_after_accept).is_ok());
    assert!(super::service::validate_pair_state(&cancelled_after_accept, &abandoned_before_accept).is_err());
    assert!(super::service::validate_pair_state(&cancelled_before_accept, &abandoned_after_accept).is_err());
}
