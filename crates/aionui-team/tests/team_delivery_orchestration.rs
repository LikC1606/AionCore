use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use aionui_common::now_ms;
use aionui_db::models::TeamRow;
use aionui_db::{
    Database, ITeamModeRepository, ITeamRepository, SqliteTeamModeRepository, SqliteTeamRepository,
    init_database_memory,
};
use aionui_realtime::BroadcastEventBus;
use aionui_team::kernel::{GitDeliveryRef, GitWorkAssignment, IntegrationRecoveryReason, WorkSubmission};
use aionui_team::{
    BeginGitIntegrationRequest, GitDeliveryPortError, GitDeliveryPortOutcome, GitDeliveryReconciliation,
    GitIntegrationIntent, GitIntegrationTarget, GitWorkAssignmentPlan, GitWorkspacePortError,
    PreparedGitWorkAssignment, TeamAgent, TeamCommandService, TeamDeliveryError, TeamDeliveryService,
    TeamGitDeliveryPort, TeamGitWorkspacePort, TeamIntegrationResolution, TeammateRole,
};
use tokio::sync::Notify;

const USER_ID: &str = "system_default_user";
const TEAM_ID: &str = "team-delivery-test";
const WORK_ID: &str = "work-1";
const DELIVERY_ID: &str = "delivery-1";

struct Harness {
    _db: Database,
    team_repo: Arc<SqliteTeamRepository>,
    mode_repo: Arc<SqliteTeamModeRepository>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SeenIntent {
    attempt_id: String,
    repository_id: String,
    base_commit: String,
    source_ref: String,
    source_head: String,
    target_ref: String,
    target_head: String,
}

struct ScriptedPort {
    integrate_results: Mutex<VecDeque<Result<GitDeliveryPortOutcome, GitDeliveryPortError>>>,
    reconcile_results: Mutex<VecDeque<Result<GitDeliveryReconciliation, GitDeliveryPortError>>>,
    integrate_calls: AtomicUsize,
    reconcile_calls: AtomicUsize,
    seen: Mutex<Vec<SeenIntent>>,
}

struct BlockingPort {
    integrate_started: Notify,
    release_integrate: Notify,
    integrate_calls: AtomicUsize,
    reconcile_calls: AtomicUsize,
}

impl BlockingPort {
    fn new() -> Self {
        Self {
            integrate_started: Notify::new(),
            release_integrate: Notify::new(),
            integrate_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
        }
    }
}

impl ScriptedPort {
    fn new(
        integrate_results: Vec<Result<GitDeliveryPortOutcome, GitDeliveryPortError>>,
        reconcile_results: Vec<Result<GitDeliveryReconciliation, GitDeliveryPortError>>,
    ) -> Self {
        Self {
            integrate_results: Mutex::new(integrate_results.into()),
            reconcile_results: Mutex::new(reconcile_results.into()),
            integrate_calls: AtomicUsize::new(0),
            reconcile_calls: AtomicUsize::new(0),
            seen: Mutex::new(Vec::new()),
        }
    }

    fn record(&self, intent: &GitIntegrationIntent) {
        self.seen.lock().unwrap().push(SeenIntent {
            attempt_id: intent.attempt_id().to_owned(),
            repository_id: intent.repository_id().to_owned(),
            base_commit: intent.base_commit().to_owned(),
            source_ref: intent.source_ref().to_owned(),
            source_head: intent.source_head().to_owned(),
            target_ref: intent.target_ref().to_owned(),
            target_head: intent.target_head().to_owned(),
        });
    }
}

#[async_trait::async_trait]
impl TeamGitDeliveryPort for ScriptedPort {
    async fn integrate(&self, intent: &GitIntegrationIntent) -> Result<GitDeliveryPortOutcome, GitDeliveryPortError> {
        self.integrate_calls.fetch_add(1, Ordering::SeqCst);
        self.record(intent);
        self.integrate_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected integrate call")
    }

    async fn reconcile(
        &self,
        intent: &GitIntegrationIntent,
    ) -> Result<GitDeliveryReconciliation, GitDeliveryPortError> {
        self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
        self.record(intent);
        self.reconcile_results
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected reconcile call")
    }
}

#[async_trait::async_trait]
impl TeamGitDeliveryPort for BlockingPort {
    async fn integrate(&self, _intent: &GitIntegrationIntent) -> Result<GitDeliveryPortOutcome, GitDeliveryPortError> {
        self.integrate_calls.fetch_add(1, Ordering::SeqCst);
        self.integrate_started.notify_one();
        self.release_integrate.notified().await;
        Ok(GitDeliveryPortOutcome::Merged {
            merged_commit: "merge-1".into(),
            observed_target_head: "merge-1".into(),
        })
    }

    async fn reconcile(
        &self,
        _intent: &GitIntegrationIntent,
    ) -> Result<GitDeliveryReconciliation, GitDeliveryPortError> {
        self.reconcile_calls.fetch_add(1, Ordering::SeqCst);
        Ok(GitDeliveryReconciliation::ReadyToIntegrate)
    }
}

#[async_trait::async_trait]
impl TeamGitWorkspacePort for ScriptedPort {
    async fn plan_assignment(&self, _plan: &GitWorkAssignmentPlan) -> Result<GitWorkAssignment, GitWorkspacePortError> {
        Err(GitWorkspacePortError::InvalidRequest(
            "assignment planning is not used by delivery orchestration tests".into(),
        ))
    }

    async fn prepare_assignment(
        &self,
        _plan: &GitWorkAssignmentPlan,
        _assignment: &GitWorkAssignment,
    ) -> Result<PreparedGitWorkAssignment, GitWorkspacePortError> {
        Err(GitWorkspacePortError::InvalidRequest(
            "assignment preparation is not used by delivery orchestration tests".into(),
        ))
    }

    async fn resolve_integration_target(
        &self,
        _workspace: &str,
        expected_repository_id: &str,
    ) -> Result<GitIntegrationTarget, GitWorkspacePortError> {
        assert_eq!(expected_repository_id, "repo-1");
        Ok(GitIntegrationTarget::new("refs/heads/main", "target-1"))
    }
}

#[async_trait::async_trait]
impl TeamGitWorkspacePort for BlockingPort {
    async fn plan_assignment(&self, _plan: &GitWorkAssignmentPlan) -> Result<GitWorkAssignment, GitWorkspacePortError> {
        Err(GitWorkspacePortError::InvalidRequest(
            "assignment planning is not used by delivery orchestration tests".into(),
        ))
    }

    async fn prepare_assignment(
        &self,
        _plan: &GitWorkAssignmentPlan,
        _assignment: &GitWorkAssignment,
    ) -> Result<PreparedGitWorkAssignment, GitWorkspacePortError> {
        Err(GitWorkspacePortError::InvalidRequest(
            "assignment preparation is not used by delivery orchestration tests".into(),
        ))
    }

    async fn resolve_integration_target(
        &self,
        _workspace: &str,
        expected_repository_id: &str,
    ) -> Result<GitIntegrationTarget, GitWorkspacePortError> {
        assert_eq!(expected_repository_id, "repo-1");
        Ok(GitIntegrationTarget::new("refs/heads/main", "target-1"))
    }
}

async fn setup() -> Harness {
    let db = init_database_memory().await.unwrap();
    let team_repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
    let mode_repo = Arc::new(SqliteTeamModeRepository::new(db.pool().clone()));
    team_repo.create_team(&team_row()).await.unwrap();

    let delivery = GitDeliveryRef::new(
        DELIVERY_ID,
        TEAM_ID,
        WORK_ID,
        "worker",
        "repo-1",
        1,
        "base-1",
        "refs/heads/ds/work-1/worker",
        "source-1",
    );
    let submission = serde_json::to_string(&WorkSubmission::git(delivery.clone())).unwrap();
    let accepted = serde_json::to_string(&delivery).unwrap();
    let now = now_ms();
    sqlx::query(
        "INSERT INTO team_work_items (\
            id, team_id, parent_work_item_id, subject, description, controller_member_id, \
            assignee_member_id, reviewer_member_id, integrator_member_id, delivery_requirement, \
            git_repository_id, git_base_commit, git_branch_ref, state, current_submission_json, \
            accepted_delivery_json, revision, created_at, updated_at\
         ) VALUES (?, ?, NULL, 'Integrate exact delivery', NULL, 'lead', 'worker', 'lead', 'lead', \
                   'git', 'repo-1', 'base-1', 'refs/heads/ds/work-1/worker', 'accepted', ?, ?, 5, ?, ?)",
    )
    .bind(WORK_ID)
    .bind(TEAM_ID)
    .bind(submission)
    .bind(accepted)
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO team_git_deliveries (\
            id, team_id, work_item_id, producer_member_id, repository_id, content_revision, \
            base_commit, branch_ref, head_commit, state, revision, merged_commit, created_at, updated_at\
         ) VALUES (?, ?, ?, 'worker', 'repo-1', 1, 'base-1', \
                   'refs/heads/ds/work-1/worker', 'source-1', 'accepted', 1, NULL, ?, ?)",
    )
    .bind(DELIVERY_ID)
    .bind(TEAM_ID)
    .bind(WORK_ID)
    .bind(now)
    .bind(now)
    .execute(db.pool())
    .await
    .unwrap();

    Harness {
        _db: db,
        team_repo,
        mode_repo,
    }
}

fn team_row() -> TeamRow {
    let agents = vec![
        member("lead", "conv-lead", TeammateRole::Lead),
        member("worker", "conv-worker", TeammateRole::Teammate),
    ];
    TeamRow {
        coordination_protocol: None,
        id: TEAM_ID.into(),
        user_id: USER_ID.into(),
        name: "Delivery Team".into(),
        workspace: "/tmp/team-delivery".into(),
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

fn request() -> BeginGitIntegrationRequest {
    BeginGitIntegrationRequest {
        work_item_id: WORK_ID.into(),
        expected_work_revision: 5,
        expected_delivery_revision: 1,
    }
}

fn service(harness: &Harness, port: Arc<ScriptedPort>) -> TeamDeliveryService {
    let command_service = Arc::new(TeamCommandService::new(
        harness.team_repo.clone(),
        harness.mode_repo.clone(),
    ));
    TeamDeliveryService::new(
        harness.team_repo.clone(),
        harness.mode_repo.clone(),
        command_service,
        port.clone(),
        port,
    )
}

#[tokio::test]
async fn integration_broadcasts_begin_and_resolution_but_not_replay() {
    let harness = setup().await;
    let event_bus = Arc::new(BroadcastEventBus::new(8));
    let mut events = event_bus.subscribe();
    let command_service = Arc::new(TeamCommandService::new_with_event_broadcaster(
        harness.team_repo.clone(),
        harness.mode_repo.clone(),
        event_bus,
    ));
    let port = Arc::new(ScriptedPort::new(
        vec![Ok(GitDeliveryPortOutcome::Merged {
            merged_commit: "merge-1".into(),
            observed_target_head: "merge-1".into(),
        })],
        vec![],
    ));
    let delivery_service = TeamDeliveryService::new(
        harness.team_repo.clone(),
        harness.mode_repo.clone(),
        command_service,
        port.clone(),
        port,
    );

    let first = delivery_service
        .integrate_as_owner(USER_ID, TEAM_ID, "broadcast-integration", request())
        .await
        .unwrap();
    assert_eq!(first.resolution, TeamIntegrationResolution::Merged);
    let begin = events.recv().await.unwrap();
    let resolved = events.recv().await.unwrap();
    for event in [&begin, &resolved] {
        assert_eq!(event.name, aionui_team::events::TEAM_WORK_CHANGED_EVENT);
        assert_eq!(event.data["team_id"], TEAM_ID);
        assert_eq!(event.data["work_item_id"], WORK_ID);
    }
    assert!(begin.data["event_sequence"].as_i64() < resolved.data["event_sequence"].as_i64());

    let replayed = delivery_service
        .integrate_as_owner(USER_ID, TEAM_ID, "broadcast-integration", request())
        .await
        .unwrap();
    assert_eq!(replayed, first);
    assert!(matches!(
        events.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn recovery_scanner_does_not_race_an_in_flight_integration_attempt() {
    let harness = setup().await;
    let port = Arc::new(BlockingPort::new());
    let command_service = Arc::new(TeamCommandService::new(
        harness.team_repo.clone(),
        harness.mode_repo.clone(),
    ));
    let delivery_service = Arc::new(TeamDeliveryService::new(
        harness.team_repo.clone(),
        harness.mode_repo.clone(),
        command_service,
        port.clone(),
        port.clone(),
    ));

    let foreground = tokio::spawn({
        let delivery_service = delivery_service.clone();
        async move {
            delivery_service
                .integrate_as_owner(USER_ID, TEAM_ID, "foreground-integration", request())
                .await
        }
    });
    tokio::time::timeout(std::time::Duration::from_secs(2), port.integrate_started.notified())
        .await
        .expect("foreground integration should persist Begin before entering the port");
    assert_eq!(
        harness
            .mode_repo
            .list_pending_git_integration_attempts(TEAM_ID)
            .await
            .unwrap()
            .len(),
        1
    );

    let mut recovery = tokio::spawn({
        let delivery_service = delivery_service.clone();
        async move { delivery_service.reconcile_pending_once().await }
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), &mut recovery)
            .await
            .is_err(),
        "scanner should wait for the foreground owner of the attempt lock"
    );
    assert_eq!(port.integrate_calls.load(Ordering::SeqCst), 1);
    assert_eq!(port.reconcile_calls.load(Ordering::SeqCst), 0);

    port.release_integrate.notify_one();
    let foreground_receipt = foreground.await.unwrap().unwrap();
    assert_eq!(foreground_receipt.resolution, TeamIntegrationResolution::Merged);
    let report = recovery.await.unwrap().unwrap();
    assert_eq!(report.pending_attempt_count, 1);
    assert_eq!(report.merged_attempt_count, 1);
    assert_eq!(port.integrate_calls.load(Ordering::SeqCst), 1);
    assert_eq!(port.reconcile_calls.load(Ordering::SeqCst), 0);

    let events = harness.mode_repo.list_work_events(TEAM_ID, WORK_ID).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.command_name.starts_with("resolve_integration_"))
            .count(),
        1
    );
}

#[tokio::test]
async fn crash_after_git_effect_reconciles_the_same_durable_intent() {
    let harness = setup().await;
    let crashing_port = Arc::new(ScriptedPort::new(
        vec![Err(GitDeliveryPortError::Unavailable("simulated crash window".into()))],
        vec![],
    ));
    let first = service(&harness, crashing_port.clone())
        .integrate_as_owner(USER_ID, TEAM_ID, "crash-window", request())
        .await;
    assert!(matches!(first, Err(TeamDeliveryError::Port(_))));

    let pending = harness
        .mode_repo
        .list_pending_git_integration_attempts(TEAM_ID)
        .await
        .unwrap();
    assert_eq!(pending.len(), 1);
    let delivery = harness
        .mode_repo
        .get_git_delivery(TEAM_ID, DELIVERY_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((delivery.state.as_str(), delivery.revision), ("integrating", 2));

    let restarted_port = Arc::new(ScriptedPort::new(
        vec![],
        vec![Ok(GitDeliveryReconciliation::Resolved(
            GitDeliveryPortOutcome::Merged {
                merged_commit: "merge-1".into(),
                observed_target_head: "merge-1".into(),
            },
        ))],
    ));
    let restarted_service = service(&harness, restarted_port.clone());
    let pending_ids = restarted_service
        .pending_attempt_ids_as_owner(USER_ID, TEAM_ID)
        .await
        .unwrap();
    assert_eq!(pending_ids, vec![pending[0].attempt_id.clone()]);
    let report = restarted_service.reconcile_pending_once().await.unwrap();
    assert_eq!(report.pending_attempt_count, 1);
    assert_eq!(report.merged_attempt_count, 1);
    assert_eq!(report.resolved_attempt_count(), 1);
    assert_eq!(restarted_port.reconcile_calls.load(Ordering::SeqCst), 1);
    assert_eq!(restarted_port.integrate_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        restarted_port.seen.lock().unwrap()[0],
        crashing_port.seen.lock().unwrap()[0]
    );

    let second_report = restarted_service.reconcile_pending_once().await.unwrap();
    assert_eq!(second_report.pending_attempt_count, 0);
    assert_eq!(second_report.resolved_attempt_count(), 0);
    assert_eq!(restarted_port.reconcile_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn restart_recovery_returns_target_moved_attempt_to_retryable_state() {
    let harness = setup().await;
    let crashing_port = Arc::new(ScriptedPort::new(
        vec![Err(GitDeliveryPortError::Unavailable("simulated crash window".into()))],
        vec![],
    ));
    assert!(
        service(&harness, crashing_port)
            .integrate_as_owner(USER_ID, TEAM_ID, "target-moved-crash", request())
            .await
            .is_err()
    );

    let restarted_port = Arc::new(ScriptedPort::new(
        vec![],
        vec![Ok(GitDeliveryReconciliation::Resolved(
            GitDeliveryPortOutcome::Retryable {
                reason: IntegrationRecoveryReason::PreconditionChanged,
                observed_target_head: Some("target-2".into()),
            },
        ))],
    ));
    let report = service(&harness, restarted_port.clone())
        .reconcile_pending_once()
        .await
        .unwrap();
    assert_eq!(report.pending_attempt_count, 1);
    assert_eq!(report.retryable_attempt_count, 1);
    assert_eq!(report.failed_attempt_count, 0);
    assert_eq!(restarted_port.reconcile_calls.load(Ordering::SeqCst), 1);

    let delivery = harness
        .mode_repo
        .get_git_delivery(TEAM_ID, DELIVERY_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((delivery.state.as_str(), delivery.revision), ("accepted", 3));
}

#[tokio::test]
async fn restart_recovery_commits_conflict_and_stops_retrying_the_attempt() {
    let harness = setup().await;
    let crashing_port = Arc::new(ScriptedPort::new(
        vec![Err(GitDeliveryPortError::Unavailable("simulated crash window".into()))],
        vec![],
    ));
    assert!(
        service(&harness, crashing_port)
            .integrate_as_owner(USER_ID, TEAM_ID, "conflict-crash", request())
            .await
            .is_err()
    );

    let restarted_port = Arc::new(ScriptedPort::new(
        vec![],
        vec![Ok(GitDeliveryReconciliation::Resolved(
            GitDeliveryPortOutcome::Conflicted {
                observed_target_head: "target-1".into(),
            },
        ))],
    ));
    let restarted_service = service(&harness, restarted_port.clone());
    let report = restarted_service.reconcile_pending_once().await.unwrap();
    assert_eq!(report.conflicted_attempt_count, 1);

    let work = harness
        .mode_repo
        .get_work_item(TEAM_ID, WORK_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!((work.state.as_str(), work.revision), ("changes_requested", 6));
    assert!(work.current_submission_json.is_none());

    let second_report = restarted_service.reconcile_pending_once().await.unwrap();
    assert_eq!(second_report.pending_attempt_count, 0);
    assert_eq!(restarted_port.reconcile_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn periodic_reconciler_scans_immediately_and_joins_on_shutdown() {
    let harness = setup().await;
    let crashing_port = Arc::new(ScriptedPort::new(
        vec![Err(GitDeliveryPortError::Unavailable("simulated crash window".into()))],
        vec![],
    ));
    assert!(
        service(&harness, crashing_port)
            .integrate_as_owner(USER_ID, TEAM_ID, "periodic-crash", request())
            .await
            .is_err()
    );

    let restarted_port = Arc::new(ScriptedPort::new(
        vec![],
        vec![Ok(GitDeliveryReconciliation::Resolved(
            GitDeliveryPortOutcome::Merged {
                merged_commit: "merge-1".into(),
                observed_target_head: "merge-1".into(),
            },
        ))],
    ));
    let restarted_service = Arc::new(service(&harness, restarted_port.clone()));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let handle =
        restarted_service.start_pending_integration_reconciler(shutdown_rx, std::time::Duration::from_secs(60));

    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while restarted_port.reconcile_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("startup recovery scan should run immediately");
    shutdown_tx.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), handle)
        .await
        .expect("reconciler should observe shutdown")
        .expect("reconciler task should join");
    assert_eq!(restarted_port.reconcile_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn target_moved_returns_delivery_to_retryable_accepted_state() {
    let harness = setup().await;
    let port = Arc::new(ScriptedPort::new(
        vec![Ok(GitDeliveryPortOutcome::Retryable {
            reason: IntegrationRecoveryReason::PreconditionChanged,
            observed_target_head: Some("target-2".into()),
        })],
        vec![],
    ));
    let receipt = service(&harness, port.clone())
        .integrate_as_owner(USER_ID, TEAM_ID, "target-moved", request())
        .await
        .unwrap();
    assert_eq!(receipt.resolution, TeamIntegrationResolution::Retryable);
    assert_eq!(
        receipt.recovery_reason,
        Some(IntegrationRecoveryReason::PreconditionChanged)
    );
    assert_eq!(receipt.observed_target_head.as_deref(), Some("target-2"));

    let attempt = harness
        .mode_repo
        .get_git_integration_attempt(TEAM_ID, &receipt.attempt_id)
        .await
        .unwrap()
        .unwrap();
    let delivery = harness
        .mode_repo
        .get_git_delivery(TEAM_ID, DELIVERY_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt.state, "retryable");
    assert_eq!((delivery.state.as_str(), delivery.revision), ("accepted", 3));
}

#[tokio::test]
async fn completed_attempt_replays_without_touching_the_port() {
    let harness = setup().await;
    let port = Arc::new(ScriptedPort::new(
        vec![Ok(GitDeliveryPortOutcome::Merged {
            merged_commit: "merge-1".into(),
            observed_target_head: "merge-1".into(),
        })],
        vec![],
    ));
    let delivery_service = service(&harness, port.clone());
    let first = delivery_service
        .integrate_as_owner(USER_ID, TEAM_ID, "idempotent", request())
        .await
        .unwrap();
    let replay = delivery_service
        .integrate_as_owner(USER_ID, TEAM_ID, "idempotent", request())
        .await
        .unwrap();
    assert_eq!(replay, first);
    assert_eq!(port.integrate_calls.load(Ordering::SeqCst), 1);
    assert_eq!(port.reconcile_calls.load(Ordering::SeqCst), 0);
    let events = harness.mode_repo.list_work_events(TEAM_ID, WORK_ID).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.command_name.starts_with("resolve_integration_"))
            .count(),
        1
    );
}

#[tokio::test]
async fn conflict_atomically_resolves_attempt_delivery_and_work_item() {
    let harness = setup().await;
    let port = Arc::new(ScriptedPort::new(
        vec![Ok(GitDeliveryPortOutcome::Conflicted {
            observed_target_head: "target-1".into(),
        })],
        vec![],
    ));
    let receipt = service(&harness, port)
        .integrate_as_owner(USER_ID, TEAM_ID, "conflict", request())
        .await
        .unwrap();
    assert_eq!(receipt.resolution, TeamIntegrationResolution::Conflicted);

    let attempt = harness
        .mode_repo
        .get_git_integration_attempt(TEAM_ID, &receipt.attempt_id)
        .await
        .unwrap()
        .unwrap();
    let delivery = harness
        .mode_repo
        .get_git_delivery(TEAM_ID, DELIVERY_ID)
        .await
        .unwrap()
        .unwrap();
    let work = harness
        .mode_repo
        .get_work_item(TEAM_ID, WORK_ID)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt.state, "conflicted");
    assert_eq!((delivery.state.as_str(), delivery.revision), ("conflicted", 3));
    assert_eq!((work.state.as_str(), work.revision), ("changes_requested", 6));
    assert!(work.current_submission_json.is_none());
}
