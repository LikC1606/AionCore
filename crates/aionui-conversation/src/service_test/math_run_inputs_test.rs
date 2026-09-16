use super::*;
use crate::service::math_run_inputs::{INTEGRITY_MESSAGE, tests::Fixture};
use aionui_db::SqliteConversationRepository;

const OWNER: &str = "system_default_user";

async fn sqlite_service(
    task_manager: Arc<dyn IWorkerTaskManager>,
) -> (ConversationService, Arc<SqliteConversationRepository>) {
    let db = init_database_memory().await.unwrap();
    let repo = Arc::new(SqliteConversationRepository::new(db.pool().clone()));
    let service = service_from_repo(repo.clone(), task_manager);
    (service, repo)
}

fn service_from_repo(
    repo: Arc<SqliteConversationRepository>,
    task_manager: Arc<dyn IWorkerTaskManager>,
) -> ConversationService {
    ConversationService::new(
        std::env::temp_dir(),
        Arc::new(MockBroadcaster::new()),
        Arc::new(FixedSkillResolver { names: vec![] }),
        task_manager,
        repo,
        Arc::new(StubAgentMetadataRepo),
        Arc::new(StubAcpSessionRepo::default()),
    )
}

fn create_request(fixture: &Fixture) -> CreateConversationRequest {
    let mut request = make_create_req();
    for (key, value) in fixture.extra.as_object().unwrap() {
        request.extra[key] = value.clone();
    }
    request
}

fn turn_request(id: &str, callback: ConversationAgentTurnStartedCallback) -> ConversationAgentTurnRequest {
    let mut request = make_agent_turn_request(id.to_owned(), callback);
    request.user_id = OWNER.into();
    request
}

fn scripted_agent(id: &str) -> Arc<ScriptedAgent> {
    let mut agent = ScriptedAgent::new(id, vec![]);
    agent.workspace_override = Some(ensure_test_workspace_path());
    Arc::new(agent)
}

#[tokio::test]
async fn math_inputs_are_persisted_and_rechecked_by_a_reconstructed_service() {
    let fixture = Fixture::new();
    let tasks = Arc::new(MockTaskManager::new());
    let (service, repo) = sqlite_service(tasks.clone()).await;
    let conversation = service.create(OWNER, create_request(&fixture)).await.unwrap();
    assert_eq!(
        conversation.extra["mathematics_runtime"],
        fixture.extra["mathematics_runtime"]
    );
    let agent = scripted_agent(&conversation.id);
    tasks.insert_agent(&conversation.id, AgentInstance::Mock(agent.clone()));
    let reconstructed = service_from_repo(repo, tasks);
    let (accepted, callback) = prompt_acceptance_counter();
    let result = reconstructed
        .run_agent_turn(turn_request(&conversation.id, callback.clone()))
        .await
        .unwrap();
    assert_eq!(result.status, ConversationAgentTurnStatus::Completed);
    fixture.change_task();
    let result = reconstructed
        .run_agent_turn(turn_request(&conversation.id, callback))
        .await
        .unwrap();
    assert_eq!(result.error_message.as_deref(), Some(INTEGRITY_MESSAGE));
    assert_eq!(
        result.failure_kind,
        Some(crate::ConversationAgentTurnFailureKind::RunInputsIntegrity)
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert_eq!(agent.sent_contents().len(), 1);
    assert!(!reconstructed.runtime_state().is_claimed(&conversation.id));
}

#[tokio::test]
async fn math_inputs_reject_changes_before_task_construction() {
    let fixture = Fixture::new();
    let tasks = Arc::new(RebuildingScriptedTaskManager::new(vec![]));
    let (service, _repo) = sqlite_service(tasks.clone()).await;
    let conversation = service.create(OWNER, create_request(&fixture)).await.unwrap();
    fixture.change_task();
    let (accepted, callback) = prompt_acceptance_counter();
    let result = service
        .run_agent_turn(turn_request(&conversation.id, callback))
        .await
        .unwrap();
    assert_eq!(result.error_message.as_deref(), Some(INTEGRITY_MESSAGE));
    assert_eq!(tasks.build_count(), 0);
    assert_eq!(accepted.load(Ordering::SeqCst), 0);
}

#[cfg(unix)]
#[tokio::test]
async fn frozen_retrieval_is_rechecked_before_model_turn_after_service_reconstruction() {
    use std::os::unix::fs::PermissionsExt;
    let mut fixture = Fixture::new();
    let corpus = fixture.bind_retrieval_corpus();
    let tasks = Arc::new(MockTaskManager::new());
    let (service, repo) = sqlite_service(tasks.clone()).await;
    let conversation = service.create(OWNER, create_request(&fixture)).await.unwrap();
    let agent = scripted_agent(&conversation.id);
    tasks.insert_agent(&conversation.id, AgentInstance::Mock(agent.clone()));
    let reconstructed = service_from_repo(repo, tasks);
    let (accepted, callback) = prompt_acceptance_counter();
    let result = reconstructed
        .run_agent_turn(turn_request(&conversation.id, callback.clone()))
        .await
        .unwrap();
    assert_eq!(result.status, ConversationAgentTurnStatus::Completed);
    std::fs::set_permissions(&corpus, std::fs::Permissions::from_mode(0o600)).unwrap();
    std::fs::write(&corpus, "changed corpus").unwrap();
    std::fs::set_permissions(&corpus, std::fs::Permissions::from_mode(0o400)).unwrap();
    let result = reconstructed
        .run_agent_turn(turn_request(&conversation.id, callback))
        .await
        .unwrap();
    assert_eq!(result.error_message.as_deref(), Some(INTEGRITY_MESSAGE));
    assert_eq!(
        result.failure_kind,
        Some(crate::ConversationAgentTurnFailureKind::RunInputsIntegrity)
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1);
    assert_eq!(agent.sent_contents().len(), 1);
    assert!(!reconstructed.runtime_state().is_claimed(&conversation.id));
}

struct GatedTasks {
    inner: MockTaskManager,
    started: Notify,
    release: Notify,
    builds: AtomicUsize,
}

#[async_trait::async_trait]
impl IWorkerTaskManager for GatedTasks {
    fn get_task(&self, id: &str) -> Option<AgentInstance> {
        self.inner.get_task(id)
    }
    async fn get_or_build_task(&self, id: &str, options: BuildTaskOptions) -> Result<AgentInstance, AgentError> {
        self.builds.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        self.release.notified().await;
        self.inner.get_or_build_task(id, options).await
    }
    fn kill(&self, id: &str, reason: Option<AgentKillReason>) -> Result<(), AgentError> {
        self.inner.kill(id, reason)
    }
    fn kill_and_wait(
        &self,
        id: &str,
        reason: Option<AgentKillReason>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
        self.inner.kill_and_wait(id, reason)
    }
    async fn clear(&self) {
        self.inner.clear().await;
    }
    fn active_count(&self) -> usize {
        self.inner.active_count()
    }
    fn collect_idle(&self, threshold: TimestampMs) -> Vec<String> {
        self.inner.collect_idle(threshold)
    }
}

#[tokio::test]
async fn math_inputs_changed_during_cold_build_never_reach_agent_or_replay() {
    let fixture = Fixture::new();
    let tasks = Arc::new(GatedTasks {
        inner: MockTaskManager::new(),
        started: Notify::new(),
        release: Notify::new(),
        builds: AtomicUsize::new(0),
    });
    let (service, repo) = sqlite_service(tasks.clone()).await;
    let conversation = service.create(OWNER, create_request(&fixture)).await.unwrap();
    let agent = scripted_agent(&conversation.id);
    tasks
        .inner
        .insert_agent(&conversation.id, AgentInstance::Mock(agent.clone()));
    let (accepted, callback) = prompt_acceptance_counter();
    let request = turn_request(&conversation.id, callback);
    let running_service = service.clone();
    let running = tokio::spawn(async move { running_service.run_agent_turn(request).await });
    tokio::time::timeout(Duration::from_secs(5), tasks.started.notified())
        .await
        .unwrap();
    fixture.change_task();
    tasks.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(result.error_message.as_deref(), Some(INTEGRITY_MESSAGE));
    assert_eq!(
        result.failure_kind,
        Some(crate::ConversationAgentTurnFailureKind::RunInputsIntegrity)
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 0);
    assert!(agent.sent_contents().is_empty());
    assert_eq!(tasks.builds.load(Ordering::SeqCst), 1);
    assert!(!service.runtime_state().is_claimed(&conversation.id));
    let messages = repo
        .list_messages_page(
            &conversation.id,
            &MessagePageParams {
                limit: 30,
                direction: MessagePageDirection::InitialLatest,
            },
        )
        .await
        .unwrap();
    let content = messages
        .items
        .iter()
        .map(|message| message.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(content.contains("math_run_inputs_integrity_failed"));
    assert!(!content.contains("Changed task contents"));
}

#[tokio::test]
async fn math_inputs_are_immutable_through_public_and_internal_updates() {
    let fixture = Fixture::new();
    let tasks: Arc<dyn IWorkerTaskManager> = Arc::new(MockTaskManager::new());
    let (service, repo) = sqlite_service(tasks.clone()).await;
    let conversation = service.create(OWNER, create_request(&fixture)).await.unwrap();
    let patch = json!({"mathematics_runtime": null});
    let error = service
        .update(
            OWNER,
            &conversation.id,
            serde_json::from_value(json!({"extra": patch})).unwrap(),
            &tasks,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Bad request: Mathematics run inputs are immutable after initialization"
    );
    let error = service.update_extra(&conversation.id, patch).await.unwrap_err();
    assert_eq!(
        error.to_string(),
        "Bad request: Mathematics run inputs are immutable after initialization"
    );
    service
        .update_extra(&conversation.id, json!({"workspace": ensure_test_workspace_path()}))
        .await
        .unwrap();
    let extra: serde_json::Value =
        serde_json::from_str(&repo.get(&conversation.id).await.unwrap().unwrap().extra).unwrap();
    assert_eq!(extra["mathematics_runtime"], fixture.extra["mathematics_runtime"]);
    let foreign = service
        .update(
            "another-user",
            &conversation.id,
            serde_json::from_value(json!({"extra": fixture.extra})).unwrap(),
            &tasks,
        )
        .await
        .unwrap_err();
    assert!(matches!(foreign, ConversationError::NotFound { .. }));
}

#[tokio::test]
async fn math_inputs_install_only_during_initial_core_team_bootstrap() {
    let fixture = Fixture::new();
    let (service, repo, tasks, conversation) = make_core_bound_team_conversation().await;
    let patch: UpdateConversationRequest = serde_json::from_value(json!({"extra": fixture.extra})).unwrap();
    let updated = service.update("u", &conversation.id, patch, &tasks).await.unwrap();
    assert_eq!(
        updated.extra["mathematics_runtime"],
        fixture.extra["mathematics_runtime"]
    );
    assert!(updated.extra.get("_team_snapshot_bootstrap_pending").is_none());
    assert!(
        repo.get(&conversation.id)
            .await
            .unwrap()
            .unwrap()
            .extra
            .contains("run_inputs")
    );
    // Provisioning may repeat the same identity while updating the final role prompt.
    service
        .update(
            "u",
            &conversation.id,
            serde_json::from_value(json!({"extra": fixture.extra})).unwrap(),
            &tasks,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn math_inputs_cannot_be_installed_into_a_claimed_team_turn() {
    let fixture = Fixture::new();
    let (service, _repo, tasks, conversation) = make_core_bound_team_conversation().await;
    let claim = service
        .runtime_state()
        .try_claim_turn(&conversation.id, "turn-being-built")
        .unwrap();
    let error = service
        .update(
            "u",
            &conversation.id,
            serde_json::from_value(json!({"extra": fixture.extra})).unwrap(),
            &tasks,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Bad request: Mathematics run inputs require a new conversation or initial Team bootstrap"
    );
    drop(claim);
}

#[tokio::test]
async fn math_inputs_cannot_be_attached_to_an_existing_non_team_conversation() {
    let fixture = Fixture::new();
    let tasks: Arc<dyn IWorkerTaskManager> = Arc::new(MockTaskManager::new());
    let (service, _repo) = sqlite_service(tasks.clone()).await;
    let conversation = service.create(OWNER, make_create_req()).await.unwrap();
    let error = service
        .update(
            OWNER,
            &conversation.id,
            serde_json::from_value(json!({"extra": fixture.extra})).unwrap(),
            &tasks,
        )
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "Bad request: Mathematics run inputs require a new conversation or initial Team bootstrap"
    );
}
