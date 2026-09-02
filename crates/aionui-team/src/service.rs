mod describe_support;
mod response_builder;
pub(crate) mod spawn_support;
pub(crate) mod team_command;
mod team_delivery;
mod team_query;
mod team_work;
mod team_work_runtime;

pub use team_command::TeamCommandService;
pub use team_delivery::{
    BeginGitIntegrationRequest, GitIntegrationRecoveryReport, TeamDeliveryError, TeamDeliveryService,
    TeamIntegrationReceipt, TeamIntegrationResolution,
};
pub use team_query::{TeamQueryError, TeamQueryService};
pub use team_work::TeamWorkCoordinator;
pub(crate) use team_work::{DelegateWorkItem, TeamWorkCoordinatorError};

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

use aionui_ai_agent::{ActiveLeaseRegistry, AgentError, AgentInstance, IWorkerTaskManager, IdleCleanupCoordinator};
use aionui_api_types::{
    AddAgentRequest, CreateTeamRequest, GetConfigOptionsResponse, TeamAgentResponse, TeamAgentRuntimeStatus,
    TeamResponse, TeamRunAckResponse, TeamRunStateResponse, TeamSendMessageQueuedResponse, TeamSessionBinding,
    TeamSessionPhase, TeamSessionStatus, TeamSessionStatusPayload, TeamToolCall, TeamToolContextResponse,
    TeamToolErrorCode, TeamToolErrorPayload, TeamToolTransport, WebSocketMessage,
};
use aionui_common::{AgentKillReason, ConversationStatus, TimestampMs, generate_id, now_ms};
use aionui_db::models::TeamRow;
use aionui_db::{
    IAgentMetadataRepository, IAssistantDefinitionRepository, IAssistantOverlayRepository, IProviderRepository,
    ITeamRepository, UpdateTeamParams,
};
use aionui_realtime::EventBroadcaster;
use dashmap::{DashMap, DashSet};
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, RwLock, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, info, warn};

use crate::error::TeamError;
use crate::event_loop::{AgentLoopContext, EventLoopRegistrationError};
use crate::events::{
    TEAM_CREATED_EVENT, TEAM_REMOVED_EVENT, TEAM_RENAMED_EVENT, TEAM_SESSION_STATUS_CHANGED_EVENT, TeamEventEmitter,
};
use crate::mailbox::user_message_fingerprint;
use crate::mcp::TeamMcpStdioConfig;
use crate::member_runtime::{
    AttachLease, AttachOutcome, AttachWaiter, BeginRemove, MemberRuntimeFailure, MemberRuntimeSnapshot, ReserveAttach,
};
use crate::message_projection::TeamProjectionMessageStore;
use crate::ports::{AgentTurnCancellationPort, AgentTurnExecutionPort, TeamAssistantCatalogPort};
use crate::prompt_dump::TeamPromptDumpConfig;
use crate::provisioning::{TeamAgentProvisioner, TeamConversationProvisioningPort};
use crate::runtime_tools::{
    ResolvedTeamToolContext, agent_for_conversation, error_payload, execute_with_scheduler, role_to_tool_role,
};
use crate::session::{AgentMessageQueueResult, TeamSession, attach_member_runtime, spawn_attach_agent_process_bg};
use crate::team_run::{TeamRunManager, target_role_for};
use crate::types::{Team, TeamAgent, TeammateRole};
use crate::work_coordinator::RuntimeConstraint;
use crate::work_source::WorkSource;
use crate::workspace::validate_create_workspace_path;

use self::team_work_runtime::TeamWorkRuntimeService;

pub(crate) fn inherit_team_workspace(extra: &mut serde_json::Value, workspace: &str) {
    if !workspace.trim().is_empty() {
        extra["workspace"] = serde_json::Value::String(workspace.to_owned());
    }
}

struct SessionEntry {
    session: Arc<TeamSession>,
    slow_monitor_handle: tokio::task::JoinHandle<()>,
}

/// A receipt for a caller supplied idempotency key.  This lives on the
/// service, rather than on `TeamSession`, because a session can be rebuilt
/// while the Team remains healthy (for example after a missing Codex rollout).
struct IdempotencyReceipt {
    created_at: Instant,
    fingerprint: String,
    result: Arc<AsyncMutex<Option<TeamRunAckResponse>>>,
}

const IDEMPOTENCY_RECEIPT_LIMIT: usize = 2_048;
const IDEMPOTENCY_RECEIPT_RETENTION: Duration = Duration::from_secs(15 * 60);
const IDEMPOTENCY_KEY_MAX_LEN: usize = 240;
const MAILBOX_RECOVERY_BATCH_SIZE: u32 = 100;
const ORPHAN_TEAM_CONVERSATION_GRACE_MS: TimestampMs = 5 * 60 * 1_000;

struct ActiveTeamProvisioning {
    team_ids: Arc<DashSet<String>>,
    team_id: String,
}

impl ActiveTeamProvisioning {
    fn new(team_ids: Arc<DashSet<String>>, team_id: String) -> Self {
        team_ids.insert(team_id.clone());
        Self { team_ids, team_id }
    }
}

impl Drop for ActiveTeamProvisioning {
    fn drop(&mut self) {
        self.team_ids.remove(&self.team_id);
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MailboxRecoveryReport {
    pub scanned_team_count: usize,
    pub started_session_count: usize,
    pub notified_session_count: usize,
    pub notified_slot_count: usize,
    pub failed_team_count: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OrphanTeamConversationRecoveryReport {
    pub scanned_count: usize,
    pub skipped_active_count: usize,
    pub skipped_fresh_count: usize,
    pub removed_count: usize,
    pub failed_count: usize,
}

pub struct TeamIdleCleanupCoordinator {
    service: Arc<TeamSessionService>,
    active_leases: Arc<ActiveLeaseRegistry>,
}

impl TeamIdleCleanupCoordinator {
    pub fn new(service: Arc<TeamSessionService>, active_leases: Arc<ActiveLeaseRegistry>) -> Self {
        Self { service, active_leases }
    }
}

#[async_trait::async_trait]
impl IdleCleanupCoordinator for TeamIdleCleanupCoordinator {
    async fn cleanup_idle_conversations(
        &self,
        idle_conversation_ids: Vec<String>,
        idle_threshold_ms: TimestampMs,
    ) -> Vec<String> {
        self.service
            .cleanup_idle_team_runtime_tasks(idle_conversation_ids, &self.active_leases, idle_threshold_ms)
            .await
    }
}

struct TeamAgentRebuildOutcome {
    agent: TeamAgent,
    duration_ms: u128,
    result: Result<(), TeamError>,
}

struct MemberRuntimeReconcileWork {
    agent: TeamAgent,
    waiter: AttachWaiter,
    owner: Option<AttachLease>,
}

fn host_team_runtime_start_concurrency() -> usize {
    // Runtime startup is already bounded by the host scheduler and by the
    // per-team semaphore.  Use the host's actual parallelism instead of a
    // fixed three-worker ceiling; a multi-worker mathematics run should not
    // spend several seconds waiting for an artificial admission interval.
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(1)
}

struct TeamRuntimeStartGate {
    semaphore: Arc<Semaphore>,
}

impl TeamRuntimeStartGate {
    fn new() -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(host_team_runtime_start_concurrency())),
        }
    }

    async fn acquire(&self) -> Result<OwnedSemaphorePermit, TeamError> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TeamError::InvalidRequest("team runtime start gate closed".to_owned()))?;
        Ok(permit)
    }
}

fn format_rebuild_agent_identity(agent: &TeamAgent) -> String {
    format!(
        "{} (backend={}, model={}, role={}, slot_id={}, conversation_id={})",
        agent.name, agent.backend, agent.model, agent.role, agent.slot_id, agent.conversation_id
    )
}

fn spawn_rebuild_agent_process(
    jobs: &mut JoinSet<TeamAgentRebuildOutcome>,
    provisioner: TeamAgentProvisioner,
    task_manager: Arc<dyn IWorkerTaskManager>,
    user_id: String,
    agent: TeamAgent,
    cfg: TeamMcpStdioConfig,
) {
    jobs.spawn(async move {
        let team_id = cfg.team_id.clone();
        info!(
            team_id = %team_id,
            slot_id = %agent.slot_id,
            agent_name = %agent.name,
            conversation_id = %agent.conversation_id,
            backend = %agent.backend,
            model = %agent.model,
            role = %agent.role,
            "team agent rebuild attach started"
        );
        let attach_started_at = Instant::now();
        let result = provisioner
            .attach_agent_process(&user_id, &agent, cfg, &task_manager)
            .await;
        let duration_ms = attach_started_at.elapsed().as_millis();
        match &result {
            Ok(()) => info!(
                team_id = %team_id,
                slot_id = %agent.slot_id,
                agent_name = %agent.name,
                conversation_id = %agent.conversation_id,
                backend = %agent.backend,
                model = %agent.model,
                role = %agent.role,
                duration_ms,
                "team agent rebuild attach finished"
            ),
            Err(error) => warn!(
                team_id = %team_id,
                slot_id = %agent.slot_id,
                agent_name = %agent.name,
                conversation_id = %agent.conversation_id,
                backend = %agent.backend,
                model = %agent.model,
                role = %agent.role,
                duration_ms,
                error = %error,
                "team agent rebuild attach failed"
            ),
        }
        TeamAgentRebuildOutcome {
            agent,
            duration_ms,
            result,
        }
    });
}

async fn join_next_rebuild_outcome(
    jobs: &mut JoinSet<TeamAgentRebuildOutcome>,
) -> Result<Option<TeamAgentRebuildOutcome>, TeamError> {
    match jobs.join_next().await {
        Some(Ok(outcome)) => Ok(Some(outcome)),
        Some(Err(error)) => Err(TeamError::InvalidRequest(format!(
            "team agent rebuild task failed: {error}"
        ))),
        None => Ok(None),
    }
}

pub struct TeamSessionService {
    repo: Arc<dyn ITeamRepository>,
    agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
    assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
    assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
    assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
    provider_repo: Arc<dyn IProviderRepository>,
    conversation_port: Arc<dyn TeamConversationProvisioningPort>,
    projection_store: Arc<dyn TeamProjectionMessageStore>,
    broadcaster: Arc<dyn EventBroadcaster>,
    task_manager: Arc<dyn IWorkerTaskManager>,
    turn_port: Arc<dyn AgentTurnExecutionPort>,
    cancellation_port: Arc<dyn AgentTurnCancellationPort>,
    backend_binary_path: Arc<PathBuf>,
    prompt_dump: TeamPromptDumpConfig,
    sessions: Arc<DashMap<String, SessionEntry>>,
    /// Per-team mutex serializing membership mutations with session startup so
    /// callers cannot read-modify-write the `agents` JSON or rebuild a runtime
    /// session from a stale roster snapshot.
    add_agent_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Per-team mutex serializing `ensure_session` so concurrent callers cannot
    /// race and start two sessions for the same team.
    ensure_session_locks: Arc<DashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    /// Per-team lifecycle gate. Normal runtime operations hold a read lease;
    /// deletion holds the write lease while it quiesces the runtime and
    /// commits the durable aggregate delete.
    lifecycle_locks: Arc<DashMap<String, Arc<RwLock<()>>>>,
    /// Team IDs whose conversations are being provisioned before the Team row
    /// is committed. Orphan recovery must never collect these bindings.
    active_provisioning_team_ids: Arc<DashSet<String>>,
    /// Per-team admission control for on-demand member runtime starts. It
    /// caps concurrent handshakes while spacing process launches just enough
    /// to avoid CPU and MCP initialization spikes.
    runtime_start_gates: Arc<DashMap<String, Arc<TeamRuntimeStartGate>>>,
    /// Service-lifetime receipts for foreground/retry enqueue requests. A
    /// retry must remain idempotent even when the published TeamSession is
    /// replaced during runtime recovery.
    idempotency_receipts: Arc<AsyncMutex<HashMap<String, IdempotencyReceipt>>>,
    /// Canonical Team work adapter. AppServices installs it once after
    /// constructing the shared command and query services.
    team_work_runtime: OnceLock<Arc<TeamWorkRuntimeService>>,
    /// Back-pointer used by [`TeamSession::spawn_agent`] to reach DB-facing
    /// orchestration without threading the service through every session method.
    /// Stored as `Weak` so the session map does not create a strong cycle with
    /// the service that owns it. Set once during [`TeamSessionService::new`]
    /// via [`Arc::new_cyclic`].
    self_ref: Weak<TeamSessionService>,
}

impl TeamSessionService {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        repo: Arc<dyn ITeamRepository>,
        agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
        assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
        assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
        assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
        provider_repo: Arc<dyn IProviderRepository>,
        conversation_port: Arc<dyn TeamConversationProvisioningPort>,
        projection_store: Arc<dyn TeamProjectionMessageStore>,
        broadcaster: Arc<dyn EventBroadcaster>,
        task_manager: Arc<dyn IWorkerTaskManager>,
        turn_port: Arc<dyn AgentTurnExecutionPort>,
        cancellation_port: Arc<dyn AgentTurnCancellationPort>,
        backend_binary_path: Arc<PathBuf>,
    ) -> Arc<Self> {
        Self::new_with_prompt_dump(
            repo,
            agent_metadata_repo,
            assistant_catalog,
            assistant_definition_repo,
            assistant_overlay_repo,
            provider_repo,
            conversation_port,
            projection_store,
            broadcaster,
            task_manager,
            turn_port,
            cancellation_port,
            backend_binary_path,
            TeamPromptDumpConfig::disabled(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_prompt_dump(
        repo: Arc<dyn ITeamRepository>,
        agent_metadata_repo: Arc<dyn IAgentMetadataRepository>,
        assistant_catalog: Arc<dyn TeamAssistantCatalogPort>,
        assistant_definition_repo: Arc<dyn IAssistantDefinitionRepository>,
        assistant_overlay_repo: Arc<dyn IAssistantOverlayRepository>,
        provider_repo: Arc<dyn IProviderRepository>,
        conversation_port: Arc<dyn TeamConversationProvisioningPort>,
        projection_store: Arc<dyn TeamProjectionMessageStore>,
        broadcaster: Arc<dyn EventBroadcaster>,
        task_manager: Arc<dyn IWorkerTaskManager>,
        turn_port: Arc<dyn AgentTurnExecutionPort>,
        cancellation_port: Arc<dyn AgentTurnCancellationPort>,
        backend_binary_path: Arc<PathBuf>,
        prompt_dump: TeamPromptDumpConfig,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak| Self {
            repo,
            agent_metadata_repo,
            assistant_catalog,
            assistant_definition_repo,
            assistant_overlay_repo,
            provider_repo,
            conversation_port,
            projection_store,
            broadcaster,
            task_manager,
            turn_port,
            cancellation_port,
            backend_binary_path,
            prompt_dump,
            sessions: Arc::new(DashMap::new()),
            add_agent_locks: Arc::new(DashMap::new()),
            ensure_session_locks: Arc::new(DashMap::new()),
            lifecycle_locks: Arc::new(DashMap::new()),
            active_provisioning_team_ids: Arc::new(DashSet::new()),
            runtime_start_gates: Arc::new(DashMap::new()),
            idempotency_receipts: Arc::new(AsyncMutex::new(HashMap::new())),
            team_work_runtime: OnceLock::new(),
            self_ref: weak.clone(),
        })
    }

    pub(crate) async fn acquire_runtime_start_permit(&self, team_id: &str) -> Result<OwnedSemaphorePermit, TeamError> {
        let gate = self
            .runtime_start_gates
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(TeamRuntimeStartGate::new()))
            .clone();
        let permit = gate.acquire().await?;
        debug!(
            team_id,
            max_concurrency = host_team_runtime_start_concurrency(),
            "team runtime start admitted"
        );
        Ok(permit)
    }

    pub(crate) fn provisioner(&self) -> TeamAgentProvisioner {
        TeamAgentProvisioner::new(
            self.repo.clone(),
            self.agent_metadata_repo.clone(),
            self.assistant_catalog.clone(),
            self.provider_repo.clone(),
            self.conversation_port.clone(),
        )
    }

    fn lifecycle_lock(&self, team_id: &str) -> Arc<RwLock<()>> {
        self.lifecycle_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    async fn delete_team_conversation_ids(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_ids: &[String],
    ) -> Result<(), TeamError> {
        let mut failed = Vec::new();
        for conversation_id in conversation_ids {
            if let Err(error) = self
                .conversation_port
                .delete_team_conversation(user_id, conversation_id)
                .await
            {
                warn!(team_id, conversation_id, error = %error, "Team conversation cleanup will require a retry");
                failed.push(conversation_id.clone());
            }
        }
        if failed.is_empty() {
            Ok(())
        } else {
            Err(TeamError::InvalidRequest(format!(
                "Team conversation cleanup is incomplete; retry Team removal ({})",
                failed.join(", ")
            )))
        }
    }

    async fn load_owned_team(&self, user_id: &str, team_id: &str) -> Result<Team, TeamError> {
        let row = self
            .repo
            .get_team(team_id)
            .await?
            .ok_or_else(|| TeamError::TeamNotFound(team_id.into()))?;
        if row.user_id != user_id {
            return Err(TeamError::Forbidden(format!(
                "team {team_id} is not owned by current user"
            )));
        }
        Ok(Team::from_row(&row)?)
    }

    pub async fn renew_active_lease(
        &self,
        user_id: &str,
        team_id: &str,
        active_leases: &ActiveLeaseRegistry,
    ) -> Result<(), TeamError> {
        let team = match self.load_owned_team(user_id, team_id).await {
            Ok(team) => team,
            Err(error @ (TeamError::TeamNotFound(_) | TeamError::Forbidden(_))) => {
                debug!(
                    kind = "team",
                    team_id,
                    user_id,
                    error = %error,
                    "Team active lease renew rejected"
                );
                return Err(error);
            }
            Err(error) => {
                warn!(
                    kind = "team",
                    team_id,
                    user_id,
                    error = %error,
                    "Team active lease renew failed"
                );
                return Err(error);
            }
        };

        let conversation_ids = team
            .agents
            .iter()
            .map(|agent| agent.conversation_id.as_str())
            .filter(|conversation_id| !conversation_id.trim().is_empty());
        let (covered_count, expires_at) = active_leases.renew_many(conversation_ids);

        debug!(
            kind = "team",
            team_id, covered_count, expires_at, "Team active lease renewed"
        );
        Ok(())
    }

    pub async fn create_team(&self, user_id: &str, req: CreateTeamRequest) -> Result<TeamResponse, TeamError> {
        if req.agents.is_empty() {
            return Err(TeamError::InvalidRequest("at least one agent is required".into()));
        }
        if req
            .agents
            .iter()
            .any(|agent| agent.conversation_id.as_deref().is_some_and(|id| !id.trim().is_empty()))
        {
            return Err(TeamError::InvalidRequest(
                "creating Team agents from existing conversations are no longer supported; omit agents[].conversation_id"
                    .into(),
            ));
        }

        let shared_workspace = match req.workspace.as_deref() {
            Some(workspace) if !workspace.is_empty() => Some(validate_create_workspace_path(workspace)?),
            _ => None,
        };

        let team_id = generate_id();
        let _active_provisioning =
            ActiveTeamProvisioning::new(self.active_provisioning_team_ids.clone(), team_id.clone());
        let now = now_ms();

        let provisioned = self
            .provisioner()
            .provision_initial_agents(user_id, &team_id, &req.agents, shared_workspace.as_deref())
            .await?;
        let agents = provisioned.agents;
        let lead_agent_id = provisioned.lead_agent_id;
        let team_workspace = provisioned.team_workspace;
        let conversation_ids = agents
            .iter()
            .map(|agent| agent.conversation_id.clone())
            .collect::<Vec<_>>();
        let agents_json = match serde_json::to_string(&agents) {
            Ok(agents_json) => agents_json,
            Err(error) => {
                let _ = self
                    .delete_team_conversation_ids(user_id, &team_id, &conversation_ids)
                    .await;
                return Err(error.into());
            }
        };

        let row = TeamRow {
            id: team_id.clone(),
            user_id: user_id.to_owned(),
            name: req.name.clone(),
            workspace: team_workspace.clone(),
            workspace_mode: "shared".into(),
            agents: agents_json,
            lead_agent_id: lead_agent_id.clone(),
            session_mode: None,
            agents_version: "1.0.1".into(),
            created_at: now,
            updated_at: now,
        };
        if let Err(error) = self.repo.create_team(&row).await {
            let _ = self
                .delete_team_conversation_ids(user_id, &team_id, &conversation_ids)
                .await;
            return Err(error.into());
        }

        let team = Team {
            id: team_id,
            user_id: user_id.to_owned(),
            name: req.name,
            workspace: team_workspace,
            workspace_mode: "shared".into(),
            agents,
            lead_agent_id,
            session_mode: None,
            created_at: now,
            updated_at: now,
        };

        let response = match self.build_team_response(&team).await {
            Ok(response) => response,
            Err(error) => {
                match self.repo.delete_team(&team.id).await {
                    Ok(()) => {
                        let _ = self
                            .delete_team_conversation_ids(user_id, &team.id, &conversation_ids)
                            .await;
                    }
                    Err(rollback_error) => warn!(
                        team_id = %team.id,
                        error = %rollback_error,
                        "failed to roll back Team after response construction failed"
                    ),
                }
                return Err(error);
            }
        };

        info!(
            team_id = %team.id,
            workspace_source = if shared_workspace.is_some() {
                "user_supplied"
            } else {
                "auto_from_leader"
            },
            agent_count = team.agents.len(),
            "Team created"
        );

        self.broadcast_team_created(&team.id, &team.name);
        Ok(response)
    }

    pub async fn list_teams(&self, user_id: &str) -> Result<Vec<TeamResponse>, TeamError> {
        let rows = self.repo.list_teams_by_user(user_id).await?;
        let mut teams = Vec::with_capacity(rows.len());
        for row in &rows {
            match Team::from_row(row) {
                Ok(team) => match self.build_team_response(&team).await {
                    Ok(resp) => teams.push(resp),
                    Err(e) => {
                        tracing::warn!(team_id = %row.id, error = %e, "skipping team with build error");
                    }
                },
                Err(e) => {
                    tracing::warn!(team_id = %row.id, error = %e, "skipping team with invalid agents JSON");
                }
            }
        }
        Ok(teams)
    }

    pub async fn get_team(&self, user_id: &str, team_id: &str) -> Result<TeamResponse, TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        self.build_team_response(&team).await
    }

    pub async fn remove_team(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        let lifecycle_lock = self.lifecycle_lock(team_id);
        let _lifecycle_guard = lifecycle_lock.write().await;
        let membership_lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _membership_guard = membership_lock.lock().await;
        let team = match self.load_owned_team(user_id, team_id).await {
            Ok(team) => team,
            Err(TeamError::TeamNotFound(_)) => {
                let orphaned = self
                    .conversation_port
                    .list_team_conversation_ids(user_id, team_id)
                    .await?;
                if orphaned.is_empty() {
                    return Err(TeamError::TeamNotFound(team_id.to_owned()));
                }
                self.delete_team_conversation_ids(user_id, team_id, &orphaned).await?;
                info!(team_id, "replayed orphaned Team conversation cleanup");
                self.broadcast_team_removed(team_id);
                return Ok(());
            }
            Err(error) => return Err(error),
        };

        // Stop every in-process writer before committing the durable delete.
        // The lifecycle write lease waits for already admitted sends/tools and
        // prevents new session ensures until this operation finishes.
        self.stop_session_unchecked(team_id);

        let kill_futures: Vec<_> = team
            .agents
            .iter()
            .map(|agent| {
                self.task_manager
                    .kill_and_wait(&agent.conversation_id, Some(AgentKillReason::TeamDeleted))
            })
            .collect();

        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(3),
            futures_util::future::join_all(kill_futures),
        )
        .await;

        // Team, mailbox, WorkItem, Delivery, event, and integration-attempt
        // rows are removed by the repository in one transaction. A failure
        // leaves the durable Team and all conversations intact; the stopped
        // runtime can be rebuilt on the next ensure.
        self.repo.delete_team(team_id).await?;

        let conversation_ids = team
            .agents
            .iter()
            .map(|agent| agent.conversation_id.clone())
            .collect::<Vec<_>>();
        let conversation_cleanup = self
            .delete_team_conversation_ids(user_id, team_id, &conversation_ids)
            .await;

        self.add_agent_locks.remove(team_id);
        self.ensure_session_locks.remove(team_id);
        self.runtime_start_gates.remove(team_id);

        info!(team_id = %team_id, "Team removed");
        self.broadcast_team_removed(team_id);
        conversation_cleanup
    }

    pub async fn rename_team(&self, user_id: &str, team_id: &str, name: &str) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;

        self.repo
            .update_team(
                team_id,
                &UpdateTeamParams {
                    name: Some(name.to_owned()),
                    ..Default::default()
                },
            )
            .await?;
        self.broadcast_team_renamed(team_id, name);
        Ok(())
    }

    pub async fn add_agent(
        &self,
        user_id: &str,
        team_id: &str,
        req: AddAgentRequest,
    ) -> Result<TeamAgentResponse, TeamError> {
        let lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        let row = self
            .repo
            .get_team(team_id)
            .await?
            .ok_or_else(|| TeamError::TeamNotFound(team_id.into()))?;
        if row.user_id != user_id {
            return Err(TeamError::Forbidden(format!(
                "team {team_id} is not owned by current user"
            )));
        }
        let mut team = Team::from_row(&row)?;
        let agent = self.provisioner().add_agent(user_id, &row, &mut team, req).await?;

        if let Some(session) = self.sessions.get(team_id).map(|e| Arc::clone(&e.session)) {
            let reservation = session.reserve_dynamic_member_attach(&agent);
            session.add_manual_agent(&agent).await?;
            let service = self
                .self_ref
                .upgrade()
                .ok_or_else(|| TeamError::InvalidRequest("add_agent requires a live TeamSessionService".into()))?;
            spawn_attach_agent_process_bg(
                service,
                session,
                user_id.to_owned(),
                agent.clone(),
                self.task_manager.clone(),
                reservation,
            );
            info!(
                team_id = %team_id,
                slot_id = %agent.slot_id,
                assistant_id = %agent.assistant_id.as_deref().unwrap_or(""),
                role = %agent.role,
                notification_written = true,
                wake_requested = true,
                "manual teammate added"
            );
        } else {
            TeamEventEmitter::new(team_id.to_owned(), self.broadcaster.clone()).broadcast_agent_spawned(&agent);
            info!(
                team_id = %team_id,
                slot_id = %agent.slot_id,
                assistant_id = %agent.assistant_id.as_deref().unwrap_or(""),
                role = %agent.role,
                notification_written = false,
                wake_requested = false,
                "manual teammate added"
            );
        }

        self.build_agent_response(&agent).await
    }

    pub async fn remove_agent(&self, user_id: &str, team_id: &str, slot_id: &str) -> Result<(), TeamError> {
        let lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let (removed, session, removal_lease) = {
            let _guard = lock.lock().await;
            let team = self.load_owned_team(user_id, team_id).await?;
            let removed = team
                .agents
                .iter()
                .find(|agent| agent.slot_id == slot_id)
                .cloned()
                .ok_or_else(|| TeamError::AgentNotFound(slot_id.into()))?;
            if removed.role == crate::types::TeammateRole::Lead {
                return Err(TeamError::InvalidRequest("cannot remove the team lead".into()));
            }
            let session = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session));
            let removal = session
                .as_ref()
                .map(|session| session.member_runtimes().begin_remove(slot_id));
            let removal_lease = match removal {
                Some(BeginRemove::Start(lease)) => Some(lease),
                Some(BeginRemove::Join(waiter)) => {
                    drop(_guard);
                    return match waiter.wait().await {
                        AttachOutcome::Removed => Ok(()),
                        AttachOutcome::Failed(failure) => Err(TeamError::MemberRuntimeFailed {
                            team_id: team_id.to_owned(),
                            slot_id: removed.slot_id,
                            conversation_id: removed.conversation_id,
                            public_reason: failure.public_reason,
                        }),
                        AttachOutcome::Ready | AttachOutcome::SessionStopped => {
                            Err(TeamError::SessionNotFound(team_id.to_owned()))
                        }
                    };
                }
                Some(BeginRemove::Absent | BeginRemove::SessionStopped) | None => None,
            };
            (removed, session, removal_lease)
        };

        // Cancellation and process cleanup intentionally happen without the
        // membership lock. Concurrent ensure calls observe Removing and join
        // the same registry operation instead of starting a replacement.
        if let Some(session) = &session {
            session.event_loops().remove(slot_id);
        }
        self.task_manager
            .kill_and_wait(&removed.conversation_id, Some(AgentKillReason::TeamDeleted))
            .await;

        let persist_result = {
            let _guard = lock.lock().await;
            let mut current = self.load_owned_team(user_id, team_id).await?;
            current.agents.retain(|agent| agent.slot_id != slot_id);
            let agents_json = serde_json::to_string(&current.agents)?;
            self.repo
                .update_team(
                    team_id,
                    &UpdateTeamParams {
                        agents: Some(agents_json),
                        ..Default::default()
                    },
                )
                .await
        };

        if let Err(error) = persist_result {
            if let (Some(session), Some(lease)) = (&session, removal_lease.as_ref()) {
                session
                    .member_runtimes()
                    .restore_attach_required_after_remove_persist_error(
                        lease,
                        MemberRuntimeFailure {
                            classification: "membership_persist_failed",
                            public_reason: "Agent runtime needs to restart after membership update failed".to_owned(),
                        },
                    );
                self.refresh_member_runtime_status(session).await;
            }
            return Err(error.into());
        }

        let published_session = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session));
        let active_session = if let Some(current) = published_session {
            let current_removal_lease = if session.as_ref().is_some_and(|captured| Arc::ptr_eq(captured, &current)) {
                removal_lease
            } else {
                match current.member_runtimes().begin_remove(slot_id) {
                    BeginRemove::Start(lease) => Some(lease),
                    BeginRemove::Join(waiter) => {
                        let _ = waiter.wait().await;
                        None
                    }
                    BeginRemove::Absent | BeginRemove::SessionStopped => None,
                }
            };
            current.event_loops().remove(slot_id);
            self.task_manager
                .kill_and_wait(&removed.conversation_id, Some(AgentKillReason::TeamDeleted))
                .await;
            match current.scheduler().remove_agent(slot_id).await {
                Ok(_) | Err(TeamError::AgentNotFound(_)) => {}
                Err(error) => return Err(error),
            }
            if let Some(lease) = current_removal_lease.as_ref() {
                current.member_runtimes().finish_remove(lease);
            }
            Some(current)
        } else {
            None
        };

        if let Err(error) = self
            .conversation_port
            .delete_team_conversation(user_id, &removed.conversation_id)
            .await
        {
            warn!(
                team_id,
                slot_id,
                conversation_id = %removed.conversation_id,
                error = %error,
                "removed team member conversation cleanup failed"
            );
        }

        if let Some(session) = active_session.filter(|session| self.capture_published_session(session).is_some()) {
            session.notify_leader_membership_removed(&removed).await?;
            self.refresh_member_runtime_status(&session).await;
            info!(
                team_id = %team_id,
                slot_id = %removed.slot_id,
                assistant_id = %removed.assistant_id.as_deref().unwrap_or(""),
                role = %removed.role,
                notification_written = true,
                wake_requested = true,
                "manual teammate removed"
            );
        } else {
            TeamEventEmitter::new(team_id.to_owned(), self.broadcaster.clone()).broadcast_agent_removed(slot_id);
            info!(
                team_id = %team_id,
                slot_id = %removed.slot_id,
                assistant_id = %removed.assistant_id.as_deref().unwrap_or(""),
                role = %removed.role,
                notification_written = false,
                wake_requested = false,
                "manual teammate removed"
            );
        }

        Ok(())
    }

    pub async fn rename_agent(&self, user_id: &str, team_id: &str, slot_id: &str, name: &str) -> Result<(), TeamError> {
        let lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;

        let mut team = self.load_owned_team(user_id, team_id).await?;

        let normalized = crate::scheduler::normalize_name(name);
        if normalized.is_empty() {
            return Err(TeamError::InvalidRequest(
                "rename_agent.name is empty after normalization".into(),
            ));
        }

        // Uniqueness check against all other agents in the team.
        let has_conflict = team
            .agents
            .iter()
            .any(|a| a.slot_id != slot_id && crate::scheduler::normalize_name(&a.name) == normalized);
        if has_conflict {
            return Err(TeamError::DuplicateAgentName(name.to_owned()));
        }

        let agent = team
            .agents
            .iter_mut()
            .find(|a| a.slot_id == slot_id)
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.into()))?;
        agent.name = name.to_owned();

        let agents_json = serde_json::to_string(&team.agents)?;
        self.repo
            .update_team(
                team_id,
                &UpdateTeamParams {
                    agents: Some(agents_json),
                    ..Default::default()
                },
            )
            .await?;

        if let Some(session) = self.sessions.get(team_id).map(|e| Arc::clone(&e.session)) {
            let _ = session.rename_agent(slot_id, name).await;
        }

        Ok(())
    }

    /// Start the team's MCP server and rebuild every agent process so it
    /// carries a fresh `team_mcp_stdio_config` pointing at the new server.
    ///
    /// Flow (mcp.md §4.3):
    /// 1. Start `TeamSession` (opens the MCP TCP server).
    /// 2. For each agent: persist `team_mcp_stdio_config` into
    ///    `conversation.extra` → `task_manager.kill_and_wait(conv_id, TeamMcpRebuild)`
    ///    → `TeamConversationProvisioningPort::warmup_agent_process(...)`
    ///    rebuilds the ACP process with
    ///    the new extra.
    /// 3. Spawn per-agent event loops that drain the mailbox whenever notified.
    /// 4. Only insert into `sessions` after every step above succeeds — on
    ///    any failure, stop the session and leave the map untouched so a
    ///    retry can start cleanly.
    pub async fn ensure_session(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        let row = match self.repo.get_team(team_id).await {
            Ok(Some(row)) => row,
            Ok(None) | Err(_) => return self.ensure_session_inner(team_id).await,
        };
        if row.user_id != user_id {
            return Err(TeamError::Forbidden(format!(
                "team {team_id} is not owned by current user"
            )));
        }
        self.ensure_session_inner(team_id).await
    }

    /// Reconciles durable mailbox demand into live Team event loops. Cold
    /// sessions are restored; existing sessions are explicitly re-notified so
    /// a lost in-memory wake cannot strand an unread row.
    pub async fn reconcile_unread_mailboxes_once(&self) -> Result<MailboxRecoveryReport, TeamError> {
        let mut report = MailboxRecoveryReport::default();
        let mut after_team_id = None;

        loop {
            let team_ids = self
                .repo
                .list_team_ids_with_recoverable_unread_mailbox(after_team_id.as_deref(), MAILBOX_RECOVERY_BATCH_SIZE)
                .await?;
            if team_ids.is_empty() {
                break;
            }
            let page_len = team_ids.len();
            after_team_id = team_ids.last().cloned();

            for team_id in team_ids {
                report.scanned_team_count += 1;
                let live_session = self.sessions.get(&team_id).map(|entry| Arc::clone(&entry.session));
                let result = match live_session {
                    Some(session) => session
                        .notify_recoverable_unread_mailbox("mailbox_reconciler")
                        .await
                        .map(|slots| {
                            if !slots.is_empty() {
                                report.notified_session_count += 1;
                                report.notified_slot_count += slots.len();
                            }
                        }),
                    None => self.ensure_session_inner(&team_id).await.map(|()| {
                        report.started_session_count += 1;
                    }),
                };
                if let Err(error) = result {
                    report.failed_team_count += 1;
                    warn!(
                        team_id,
                        error = %error,
                        "recoverable unread Team mailbox reconciliation failed; retrying on the next scan"
                    );
                }
            }

            if page_len < MAILBOX_RECOVERY_BATCH_SIZE as usize {
                break;
            }
        }

        if report.scanned_team_count > 0 {
            info!(
                scanned_team_count = report.scanned_team_count,
                started_session_count = report.started_session_count,
                notified_session_count = report.notified_session_count,
                notified_slot_count = report.notified_slot_count,
                failed_team_count = report.failed_team_count,
                "recoverable unread Team mailbox reconciliation completed"
            );
        }
        Ok(report)
    }

    /// Removes stale Team-bound conversations whose Team aggregate no longer
    /// exists. Fresh bindings and in-process provisioning are protected so a
    /// slow create cannot be mistaken for an orphan.
    pub async fn reconcile_orphan_team_conversations_once(
        &self,
    ) -> Result<OrphanTeamConversationRecoveryReport, TeamError> {
        let candidates = self
            .conversation_port
            .list_team_conversation_cleanup_candidates()
            .await?;
        let now = now_ms();
        let mut team_exists = HashMap::<String, bool>::new();
        let mut report = OrphanTeamConversationRecoveryReport::default();
        for candidate in candidates {
            report.scanned_count += 1;
            if self.active_provisioning_team_ids.contains(&candidate.team_id) {
                report.skipped_active_count += 1;
                continue;
            }
            if now.saturating_sub(candidate.created_at) < ORPHAN_TEAM_CONVERSATION_GRACE_MS {
                report.skipped_fresh_count += 1;
                continue;
            }
            let exists = match team_exists.get(&candidate.team_id) {
                Some(exists) => *exists,
                None => {
                    let exists = self.repo.get_team(&candidate.team_id).await?.is_some();
                    team_exists.insert(candidate.team_id.clone(), exists);
                    exists
                }
            };
            if exists {
                continue;
            }
            match self
                .conversation_port
                .delete_team_conversation(&candidate.user_id, &candidate.conversation_id)
                .await
            {
                Ok(()) => report.removed_count += 1,
                Err(error) => {
                    report.failed_count += 1;
                    warn!(
                        team_id = %candidate.team_id,
                        conversation_id = %candidate.conversation_id,
                        error = %error,
                        "orphaned Team conversation cleanup failed; retrying on the next recovery scan"
                    );
                }
            }
        }
        Ok(report)
    }

    /// Starts an immediate-then-periodic recovery scan tied to application
    /// shutdown. The single loop awaits each scan, so scans never overlap.
    pub fn start_unread_mailbox_reconciler(
        self: &Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
        scan_interval: Duration,
    ) -> JoinHandle<()> {
        let service = Arc::downgrade(self);
        tokio::spawn(async move {
            if scan_interval.is_zero() {
                warn!("unread Team mailbox reconciler disabled because scan interval is zero");
                return;
            }
            if *shutdown_rx.borrow() {
                return;
            }

            let mut interval = tokio::time::interval(scan_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            info!(
                scan_interval_secs = scan_interval.as_secs(),
                "unread Team mailbox reconciler started"
            );

            loop {
                tokio::select! {
                    biased;
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                        let Some(service) = service.upgrade() else {
                            break;
                        };
                        if let Err(error) = service.reconcile_orphan_team_conversations_once().await {
                            warn!(
                                error = %error,
                                "orphaned Team conversation recovery scan failed; retrying on the next scan"
                            );
                        }
                        if let Err(error) = service.reconcile_unread_mailboxes_once().await {
                            warn!(
                                error = %error,
                                "unread Team mailbox recovery scan failed; retrying on the next scan"
                            );
                        }
                    }
                }
            }
            info!("unread Team mailbox reconciler stopped");
        })
    }

    async fn ensure_session_inner(&self, team_id: &str) -> Result<(), TeamError> {
        let lifecycle_lock = self.lifecycle_lock(team_id);
        let _lifecycle_guard = lifecycle_lock.read().await;
        self.ensure_session_inner_unlocked(team_id).await
    }

    async fn ensure_session_inner_unlocked(&self, team_id: &str) -> Result<(), TeamError> {
        let membership_lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let membership_guard = membership_lock.lock().await;

        let row = match self.repo.get_team(team_id).await {
            Ok(Some(row)) => row,
            Ok(None) => {
                self.broadcast_session_status(
                    team_id,
                    TeamSessionStatus::Failed,
                    Some(TeamSessionPhase::LoadingTeam),
                    |p| {
                        p.error = Some(format!("team not found: {team_id}"));
                    },
                );
                return Err(TeamError::TeamNotFound(team_id.into()));
            }
            Err(e) => {
                self.broadcast_session_status(
                    team_id,
                    TeamSessionStatus::Failed,
                    Some(TeamSessionPhase::LoadingTeam),
                    |p| {
                        p.error = Some(e.to_string());
                    },
                );
                return Err(e.into());
            }
        };
        let user_id = row.user_id.clone();
        let team = Team::from_row(&row)?;
        let agents_snapshot: Vec<TeamAgent> = team.agents.clone();
        let lead_agent = agents_snapshot
            .iter()
            .find(|agent| agent.role == TeammateRole::Lead)
            .cloned()
            .ok_or_else(|| TeamError::InvalidRequest("team lead not found".to_owned()))?;

        if let Some(session) = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session)) {
            let work = self
                .reserve_member_runtime_reconciliation(&session, std::slice::from_ref(&lead_agent))
                .await?;
            drop(membership_guard);
            return self
                .complete_member_runtime_reconciliation(team_id, &user_id, session, work)
                .await;
        }

        let lock = self
            .ensure_session_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let ensure_guard = lock.lock().await;

        if let Some(session) = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session)) {
            let work = self
                .reserve_member_runtime_reconciliation(&session, std::slice::from_ref(&lead_agent))
                .await?;
            drop(membership_guard);
            drop(ensure_guard);
            return self
                .complete_member_runtime_reconciliation(team_id, &user_id, session, work)
                .await;
        }

        self.broadcast_session_status(
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::LoadingTeam),
            |_| {},
        );

        self.broadcast_session_status(
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::StartingBridge),
            |_| {},
        );

        let session = match TeamSession::start_with_prompt_dump(
            team,
            self.repo.clone(),
            self.broadcaster.clone(),
            self.backend_binary_path.clone(),
            self.task_manager.clone(),
            self.turn_port.clone(),
            self.cancellation_port.clone(),
            self.projection_store.clone(),
            user_id.clone(),
            self.self_ref.clone(),
            self.prompt_dump.clone(),
        )
        .await
        {
            Ok(session) => Arc::new(session),
            Err(e) => {
                self.broadcast_session_status(
                    team_id,
                    TeamSessionStatus::Failed,
                    Some(TeamSessionPhase::StartingBridge),
                    |p| {
                        p.error = Some(e.to_string());
                    },
                );
                return Err(e);
            }
        };

        self.broadcast_session_status(
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::AttachingAgents),
            |_| {},
        );

        if let Err(e) = self
            .rebuild_agent_processes(team_id, &session, &user_id, std::slice::from_ref(&lead_agent))
            .await
        {
            self.broadcast_session_status(
                team_id,
                TeamSessionStatus::Failed,
                Some(TeamSessionPhase::AttachingAgents),
                |p| {
                    p.error = Some(e.to_string());
                },
            );
            session.stop();
            self.cleanup_bootstrap_runtime_tasks(&agents_snapshot).await;
            return Err(e);
        }

        if let Err(error) = self.spawn_event_loops(&session, &user_id, &agents_snapshot) {
            warn!(
                team_id,
                generation = session.generation(),
                error = %error,
                "team bootstrap event loop registration failed"
            );
            session.stop();
            self.cleanup_bootstrap_runtime_tasks(&agents_snapshot).await;
            self.broadcast_session_status(
                team_id,
                TeamSessionStatus::Failed,
                Some(TeamSessionPhase::AttachingAgents),
                |payload| payload.error = Some(error.to_string()),
            );
            return Err(error);
        }

        if !session.member_runtimes().seed_ready(lead_agent.slot_id.clone()) {
            let error = TeamError::InvalidRequest("failed to seed Lead runtime registry".to_owned());
            session.stop();
            self.cleanup_bootstrap_runtime_tasks(&agents_snapshot).await;
            return Err(error);
        }
        for agent in &agents_snapshot {
            let constraint = if agent.slot_id == lead_agent.slot_id {
                RuntimeConstraint::Ready
            } else {
                RuntimeConstraint::Starting { operation_id: 0 }
            };
            session
                .work_coordinator()
                .set_runtime_constraint(&agent.slot_id, target_role_for(agent.role), constraint);
        }

        let slow_monitor_handle = Self::spawn_slow_monitor(session.clone());
        let entry = SessionEntry {
            session: session.clone(),
            slow_monitor_handle,
        };
        self.sessions.insert(team_id.to_owned(), entry);
        drop(membership_guard);

        for agent in &agents_snapshot {
            let status = if agent.slot_id == lead_agent.slot_id {
                TeamAgentRuntimeStatus::Ready
            } else {
                TeamAgentRuntimeStatus::Pending
            };
            self.broadcast_agent_runtime_status(team_id, agent, status, None);
        }

        self.broadcast_session_status(
            team_id,
            TeamSessionStatus::Starting,
            Some(TeamSessionPhase::Recovering),
            |_| {},
        );

        if let Err(err) = session.try_start_recovery_drain("ensure_session_ready").await {
            warn!(
                team_id,
                error = %err,
                "team recovery scan failed after session ensure"
            );
        }

        self.broadcast_session_status(team_id, TeamSessionStatus::Ready, None, |p| {
            p.server_count = Some(1);
        });

        Ok(())
    }

    async fn reserve_member_runtime_reconciliation(
        &self,
        session: &Arc<TeamSession>,
        agents: &[TeamAgent],
    ) -> Result<Vec<MemberRuntimeReconcileWork>, TeamError> {
        let scheduler_slots = session
            .scheduler()
            .list_agents()
            .await
            .into_iter()
            .map(|agent| agent.slot_id)
            .collect::<HashSet<_>>();
        let mut work = Vec::new();

        for agent in agents {
            if !scheduler_slots.contains(&agent.slot_id) {
                session.scheduler().add_agent(agent).await;
            }
            let reservation = match session.member_runtimes().snapshot(&agent.slot_id) {
                MemberRuntimeSnapshot::Ready if self.task_manager.get_task(&agent.conversation_id).is_none() => {
                    session.member_runtimes().reserve_repair(&agent.slot_id)
                }
                MemberRuntimeSnapshot::Ready => ReserveAttach::AlreadyReady,
                _ => session.member_runtimes().reserve_attach(&agent.slot_id, true),
            };

            match reservation {
                ReserveAttach::Start(owner) => {
                    self.broadcast_agent_runtime_status(
                        session.team_id(),
                        agent,
                        TeamAgentRuntimeStatus::Pending,
                        None,
                    );
                    work.push(MemberRuntimeReconcileWork {
                        agent: agent.clone(),
                        waiter: owner.waiter(),
                        owner: Some(owner),
                    });
                }
                ReserveAttach::Join(waiter) | ReserveAttach::Removing(waiter) => {
                    info!(
                        team_id = session.team_id(),
                        slot_id = agent.slot_id,
                        conversation_id = agent.conversation_id,
                        operation_id = waiter.operation_id(),
                        generation = session.generation(),
                        duration_ms = 0,
                        error_classification = "none",
                        "team member runtime reconciliation waiting"
                    );
                    work.push(MemberRuntimeReconcileWork {
                        agent: agent.clone(),
                        waiter,
                        owner: None,
                    });
                }
                ReserveAttach::AlreadyReady => {}
                ReserveAttach::SessionStopped => {
                    return Err(TeamError::InvalidRequest(
                        "team session stopped during reconciliation reservation".to_owned(),
                    ));
                }
            }
        }
        Ok(work)
    }

    async fn complete_member_runtime_reconciliation(
        &self,
        team_id: &str,
        user_id: &str,
        session: Arc<TeamSession>,
        work: Vec<MemberRuntimeReconcileWork>,
    ) -> Result<(), TeamError> {
        if work.iter().any(|item| item.agent.role == TeammateRole::Lead) {
            self.publish_member_runtime_starting_if_current(&session);
        }
        let mut waiters = Vec::with_capacity(work.len());
        for item in work {
            let waiter = item.waiter;
            if let Some(owner) = item.owner {
                tokio::spawn(attach_member_runtime(
                    self.self_ref
                        .upgrade()
                        .ok_or_else(|| TeamError::InvalidRequest("team service is shutting down".to_owned()))?,
                    Arc::clone(&session),
                    user_id.to_owned(),
                    item.agent.clone(),
                    self.task_manager.clone(),
                    owner,
                ));
            }
            waiters.push((item.agent, waiter));
        }

        let outcomes = futures_util::future::join_all(
            waiters
                .into_iter()
                .map(|(agent, waiter)| async move { (agent, waiter.wait().await) }),
        )
        .await;

        let membership_lock = self
            .add_agent_locks
            .entry(team_id.to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _membership_guard = membership_lock.lock().await;
        let current_agents = match self.repo.get_team(team_id).await? {
            Some(row) => Team::from_row(&row)?.agents,
            None => return Err(TeamError::TeamNotFound(team_id.to_owned())),
        };
        let current_slots = current_agents
            .iter()
            .map(|agent| agent.slot_id.as_str())
            .collect::<HashSet<_>>();
        let current_session = self
            .sessions
            .get(team_id)
            .ok_or_else(|| TeamError::SessionNotFound(team_id.to_owned()))?;
        if !Arc::ptr_eq(&current_session.session, &session) {
            return Err(TeamError::SessionNotFound(team_id.to_owned()));
        }

        for (agent, outcome) in outcomes {
            if !current_slots.contains(agent.slot_id.as_str()) {
                continue;
            }
            match outcome {
                AttachOutcome::Ready | AttachOutcome::Removed => {}
                AttachOutcome::Failed(failure) => {
                    // A Worker rollout is independently recoverable. Keep the
                    // healthy Lead session usable and expose the failed slot
                    // through its agent-runtime event; only a failed Lead is
                    // fatal to the Team lifecycle.
                    if agent.role == TeammateRole::Lead {
                        self.broadcast_session_status(
                            team_id,
                            TeamSessionStatus::Failed,
                            Some(TeamSessionPhase::AttachingAgents),
                            |payload| payload.error = Some(failure.public_reason.clone()),
                        );
                    } else {
                        // refresh_member_runtime_status takes this team's
                        // membership lock. Release the reconciliation guard
                        // first: Tokio mutexes are not re-entrant, and keeping
                        // it here would make a failed Worker hang forever.
                        drop(current_session);
                        drop(_membership_guard);
                        self.refresh_member_runtime_status(&session).await;
                    }
                    return Err(TeamError::MemberRuntimeFailed {
                        team_id: team_id.to_owned(),
                        slot_id: agent.slot_id,
                        conversation_id: agent.conversation_id,
                        public_reason: failure.public_reason,
                    });
                }
                AttachOutcome::SessionStopped => {
                    return Err(TeamError::InvalidRequest(
                        "team session stopped during reconciliation".to_owned(),
                    ));
                }
            }
        }

        let ready_count = current_agents
            .iter()
            .filter(|agent| {
                matches!(
                    session.member_runtimes().snapshot(&agent.slot_id),
                    MemberRuntimeSnapshot::Ready
                )
            })
            .count();
        self.broadcast_session_status(team_id, TeamSessionStatus::Ready, None, |payload| {
            payload.server_count = Some(ready_count);
        });
        Ok(())
    }

    async fn cleanup_bootstrap_runtime_tasks(&self, agents: &[TeamAgent]) {
        for agent in agents {
            self.task_manager
                .kill_and_wait(&agent.conversation_id, Some(AgentKillReason::TeamMcpRebuild))
                .await;
        }
    }

    pub(crate) async fn cleanup_stale_member_runtime_task(
        &self,
        captured_session: &TeamSession,
        conversation_id: &str,
    ) {
        let lock = self
            .ensure_session_locks
            .entry(captured_session.team_id().to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _guard = lock.lock().await;
        if self
            .sessions
            .get(captured_session.team_id())
            .is_some_and(|entry| !std::ptr::eq(entry.session.as_ref(), captured_session))
        {
            return;
        }
        self.task_manager
            .kill_and_wait(conversation_id, Some(AgentKillReason::TeamMcpRebuild))
            .await;
    }

    pub async fn get_conversation_config_options(
        &self,
        user_id: &str,
        team_id: &str,
        conversation_id: &str,
    ) -> Result<GetConfigOptionsResponse, TeamError> {
        let row = self
            .repo
            .get_team(team_id)
            .await?
            .ok_or_else(|| TeamError::TeamNotFound(team_id.to_owned()))?;
        if row.user_id != user_id {
            return Err(TeamError::Forbidden(format!(
                "team {team_id} is not owned by current user"
            )));
        }

        let team = Team::from_row(&row)?;
        let member = team.agents.iter().any(|agent| agent.conversation_id == conversation_id);
        if !member {
            return Err(TeamError::AgentNotFound(conversation_id.to_owned()));
        }

        self.conversation_port.get_config_options(conversation_id).await
    }

    fn broadcast_session_status<F>(
        &self,
        team_id: &str,
        status: TeamSessionStatus,
        phase: Option<TeamSessionPhase>,
        customize: F,
    ) where
        F: FnOnce(&mut TeamSessionStatusPayload),
    {
        let mut payload = TeamSessionStatusPayload {
            team_id: team_id.to_owned(),
            status,
            phase,
            server_count: None,
            error: None,
        };
        customize(&mut payload);
        let event = WebSocketMessage::new(
            TEAM_SESSION_STATUS_CHANGED_EVENT,
            serde_json::to_value(payload).expect("serialize team session status payload"),
        );
        self.broadcaster.broadcast(event);
    }

    fn spawn_slow_monitor(session: Arc<TeamSession>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let snapshot = session.work_snapshot().await;
                session.team_run_manager().publish_snapshot_update(&snapshot);
                // Slow notifications are independent per Worker.  Handle them
                // together so one slow mailbox/database operation cannot delay
                // notifications for every other slot in the same minute tick.
                futures_util::future::join_all(snapshot.slots.iter().map(|slot| async {
                    if let Err(error) = session.notify_leader_slow_turn(slot).await {
                        warn!(
                            team_id = %session.team_id(),
                            slot_id = %slot.slot_id,
                            error = %error,
                            "slow Worker turn notification failed"
                        );
                    }
                }))
                .await;
            }
        })
    }

    fn broadcast_team_created(&self, team_id: &str, team_name: &str) {
        info!(team_id = %team_id, event_name = TEAM_CREATED_EVENT, "team event broadcast");
        self.broadcaster.broadcast(WebSocketMessage::new(
            TEAM_CREATED_EVENT,
            serde_json::json!({ "team_id": team_id, "team_name": team_name }),
        ));
        self.broadcast_team_list_changed(team_id, "created");
    }

    fn broadcast_team_removed(&self, team_id: &str) {
        info!(team_id = %team_id, event_name = TEAM_REMOVED_EVENT, "team event broadcast");
        self.broadcaster.broadcast(WebSocketMessage::new(
            TEAM_REMOVED_EVENT,
            serde_json::json!({ "team_id": team_id }),
        ));
        self.broadcast_team_list_changed(team_id, "removed");
    }

    fn broadcast_team_renamed(&self, team_id: &str, team_name: &str) {
        info!(team_id = %team_id, event_name = TEAM_RENAMED_EVENT, "team event broadcast");
        self.broadcaster.broadcast(WebSocketMessage::new(
            TEAM_RENAMED_EVENT,
            serde_json::json!({ "team_id": team_id, "team_name": team_name }),
        ));
        self.broadcast_team_list_changed(team_id, "renamed");
    }

    fn broadcast_team_list_changed(&self, team_id: &str, action: &str) {
        info!(team_id = %team_id, event_name = crate::events::TEAM_LIST_CHANGED_EVENT, action, "team event broadcast");
        self.broadcaster.broadcast(WebSocketMessage::new(
            crate::events::TEAM_LIST_CHANGED_EVENT,
            serde_json::json!({ "team_id": team_id, "action": action }),
        ));
    }

    pub(crate) fn broadcast_agent_runtime_status(
        &self,
        team_id: &str,
        agent: &TeamAgent,
        status: TeamAgentRuntimeStatus,
        error: Option<String>,
    ) {
        TeamEventEmitter::new(team_id.to_owned(), self.broadcaster.clone())
            .broadcast_agent_runtime_status(agent, status, error);
    }

    async fn rebuild_agent_processes(
        &self,
        team_id: &str,
        session: &TeamSession,
        user_id: &str,
        agents: &[TeamAgent],
    ) -> Result<(), TeamError> {
        let provisioner = self.provisioner();
        let task_manager = self.task_manager.clone();
        let started_at = Instant::now();
        let mut rebuild_jobs: Vec<TeamAgent> = agents.to_vec();
        rebuild_jobs.sort_by_key(|agent| match agent.role {
            TeammateRole::Lead => 0,
            TeammateRole::Teammate => 1,
        });

        info!(
            team_id,
            agent_count = agents.len(),
            max_concurrency = host_team_runtime_start_concurrency(),
            start_stagger_ms = 0u128,
            "team agent rebuild started"
        );

        let mut outcomes = Vec::new();
        let mut jobs = JoinSet::new();

        for agent in rebuild_jobs {
            let cfg = session.mcp_stdio_config(&agent.slot_id);
            self.broadcast_agent_runtime_status(team_id, &agent, TeamAgentRuntimeStatus::Pending, None);
            spawn_rebuild_agent_process(
                &mut jobs,
                provisioner.clone(),
                task_manager.clone(),
                user_id.to_owned(),
                agent,
                cfg,
            );
        }

        while let Some(outcome) = join_next_rebuild_outcome(&mut jobs).await? {
            outcomes.push(outcome);
        }

        let mut success_count = 0usize;
        let mut failures: Vec<&TeamAgentRebuildOutcome> = Vec::new();
        for outcome in &outcomes {
            match &outcome.result {
                Ok(()) => success_count += 1,
                Err(_) => failures.push(outcome),
            }
        }

        info!(
            team_id,
            agent_count = agents.len(),
            success_count,
            failure_count = failures.len(),
            duration_ms = started_at.elapsed().as_millis(),
            max_concurrency = host_team_runtime_start_concurrency(),
            start_stagger_ms = 0u128,
            "team agent rebuild completed"
        );

        if failures.is_empty() {
            return Ok(());
        }

        let first_error = failures
            .first()
            .map(|outcome| {
                let error = outcome
                    .result
                    .as_ref()
                    .err()
                    .map(ToString::to_string)
                    .unwrap_or_else(|| "unknown rebuild failure".to_owned());
                format!("{}: {error}", format_rebuild_agent_identity(&outcome.agent))
            })
            .unwrap_or_else(|| "unknown rebuild failure".to_owned());

        for failure in &failures {
            let error = failure
                .result
                .as_ref()
                .err()
                .map(ToString::to_string)
                .unwrap_or_else(|| "unknown rebuild failure".to_owned());
            self.broadcast_agent_runtime_status(
                team_id,
                &failure.agent,
                TeamAgentRuntimeStatus::Failed,
                Some(error.clone()),
            );
            warn!(
                team_id,
                slot_id = %failure.agent.slot_id,
                agent_name = %failure.agent.name,
                conversation_id = %failure.agent.conversation_id,
                backend = %failure.agent.backend,
                model = %failure.agent.model,
                role = %failure.agent.role,
                duration_ms = failure.duration_ms,
                error = %error,
                "warmup failed during rebuild"
            );
        }

        for success in outcomes.iter().filter(|outcome| outcome.result.is_ok()) {
            info!(
                team_id,
                slot_id = %success.agent.slot_id,
                agent_name = %success.agent.name,
                conversation_id = %success.agent.conversation_id,
                backend = %success.agent.backend,
                model = %success.agent.model,
                role = %success.agent.role,
                "cleaning up successfully attached agent after rebuild failure"
            );
            self.task_manager
                .kill_and_wait(&success.agent.conversation_id, Some(AgentKillReason::TeamMcpRebuild))
                .await;
        }

        Err(TeamError::InvalidRequest(format!(
            "failed to attach rebuilt agent: {first_error}"
        )))
    }

    /// Spawn per-agent event loops that drain the mailbox whenever notified.
    /// Each agent gets its own tokio task that runs until the session shuts down.
    fn spawn_event_loops(
        &self,
        session: &Arc<TeamSession>,
        user_id: &str,
        agents: &[TeamAgent],
    ) -> Result<(), TeamError> {
        let registry = session.event_loops();

        for agent in agents {
            let ctx = AgentLoopContext {
                team_id: session.team_id().to_owned(),
                slot_id: agent.slot_id.clone(),
                user_id: user_id.to_owned(),
                session: session.clone(),
                scheduler: session.scheduler().clone(),
                mailbox: session.mailbox().clone(),
                turn_port: self.turn_port.clone(),
                registry: registry.clone(),
            };
            registry.spawn(&agent.slot_id, ctx).map_err(|error| {
                TeamError::InvalidRequest(format!(
                    "failed to register bootstrap event loop for slot {}: {error:?}",
                    agent.slot_id
                ))
            })?;
        }
        Ok(())
    }

    /// Register an event loop for a dynamically spawned agent.
    ///
    /// Called by [`TeamSession::spawn_agent`] after `attach_spawned_agent_process`
    /// succeeds so the newly booted agent gets its own drain loop — exactly as
    /// `spawn_event_loops` does for the initial members during `ensure_session`.
    pub(crate) fn register_event_loop(
        &self,
        session: &Arc<TeamSession>,
        slot_id: &str,
    ) -> Result<bool, EventLoopRegistrationError> {
        let registry = session.event_loops();

        let ctx = AgentLoopContext {
            team_id: session.team_id().to_owned(),
            slot_id: slot_id.to_owned(),
            user_id: session.user_id().to_owned(),
            session: session.clone(),
            scheduler: session.scheduler().clone(),
            mailbox: session.mailbox().clone(),
            turn_port: self.turn_port.clone(),
            registry: registry.clone(),
        };
        match registry.spawn(slot_id, ctx) {
            Ok(()) => {
                info!(
                    team_id = session.team_id(),
                    slot_id,
                    generation = session.generation(),
                    "agent event loop registered"
                );
                Ok(true)
            }
            Err(EventLoopRegistrationError::Duplicate) => Ok(false),
            Err(error) => Err(error),
        }
    }

    pub async fn get_session_user_id(&self, team_id: &str) -> Option<String> {
        self.sessions.get(team_id).map(|e| e.session.user_id().to_owned())
    }

    pub(crate) fn capture_published_session(&self, expected: &TeamSession) -> Option<Arc<TeamSession>> {
        self.sessions
            .get(expected.team_id())
            .and_then(|entry| std::ptr::eq(entry.session.as_ref(), expected).then(|| Arc::clone(&entry.session)))
    }

    /// Run a synchronous side effect only while `expected` is still the
    /// published session. Keeping the map guard alive through `action`
    /// serializes the effect with session removal/replacement.
    pub(crate) fn with_published_session<R>(
        &self,
        expected: &TeamSession,
        action: impl FnOnce(&TeamSession) -> R,
    ) -> Option<R> {
        let entry = self.sessions.get(expected.team_id())?;
        std::ptr::eq(entry.session.as_ref(), expected).then(|| action(&entry.session))
    }

    pub(crate) fn publish_member_runtime_ready_if_current(&self, expected: &TeamSession, agent: &TeamAgent) -> bool {
        self.with_published_session(expected, |_| {
            self.broadcast_agent_runtime_status(expected.team_id(), agent, TeamAgentRuntimeStatus::Ready, None);
        })
        .is_some()
    }

    pub(crate) fn publish_member_runtime_starting_if_current(&self, expected: &TeamSession) -> bool {
        self.with_published_session(expected, |_| {
            self.broadcast_session_status(
                expected.team_id(),
                TeamSessionStatus::Starting,
                Some(TeamSessionPhase::AttachingAgents),
                |_| {},
            );
        })
        .is_some()
    }

    pub(crate) fn publish_member_runtime_failed_if_current(&self, expected: &TeamSession, reason: &str) -> bool {
        self.with_published_session(expected, |_| {
            self.broadcast_session_status(
                expected.team_id(),
                TeamSessionStatus::Failed,
                Some(TeamSessionPhase::AttachingAgents),
                |payload| payload.error = Some(reason.to_owned()),
            );
        })
        .is_some()
    }

    pub(crate) async fn refresh_member_runtime_status(&self, expected: &TeamSession) {
        let membership_lock = self
            .add_agent_locks
            .entry(expected.team_id().to_owned())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone();
        let _membership_guard = membership_lock.lock().await;
        let Ok(Some(row)) = self.repo.get_team(expected.team_id()).await else {
            return;
        };
        let Ok(team) = Team::from_row(&row) else {
            return;
        };

        let lead = team.agents.iter().find(|agent| agent.role == TeammateRole::Lead);
        let Some(lead) = lead else {
            return;
        };
        let lead_status = expected.member_runtimes().snapshot(&lead.slot_id);
        if let MemberRuntimeSnapshot::Failed { failure, .. } = lead_status {
            self.publish_member_runtime_failed_if_current(expected, &failure.public_reason);
        } else if !matches!(lead_status, MemberRuntimeSnapshot::Ready) {
            self.publish_member_runtime_starting_if_current(expected);
        } else {
            let ready_count = team
                .agents
                .iter()
                .filter(|agent| {
                    matches!(
                        expected.member_runtimes().snapshot(&agent.slot_id),
                        MemberRuntimeSnapshot::Ready
                    )
                })
                .count();
            let _ = self.with_published_session(expected, |_| {
                self.broadcast_session_status(expected.team_id(), TeamSessionStatus::Ready, None, |payload| {
                    payload.server_count = Some(ready_count);
                });
            });
        }
    }

    pub async fn get_run_state(&self, user_id: &str, team_id: &str) -> Result<TeamRunStateResponse, TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        let session = self.sessions.get(team_id).map(|entry| Arc::clone(&entry.session));
        let Some(session) = session else {
            return Ok(TeamRunStateResponse {
                session_generation: None,
                active_run: None,
                slot_work: Vec::new(),
            });
        };
        let snapshot = session.work_snapshot().await;
        let active_run = session.team_run_manager().current_payload(&snapshot).filter(|run| {
            matches!(
                run.status,
                aionui_api_types::TeamRunStatus::Accepted
                    | aionui_api_types::TeamRunStatus::Running
                    | aionui_api_types::TeamRunStatus::Cancelling
            )
        });
        let slot_work = snapshot.slots.iter().map(TeamRunManager::slot_payload).collect();
        Ok(TeamRunStateResponse {
            session_generation: Some(snapshot.session_generation),
            active_run,
            slot_work,
        })
    }

    pub fn get_session_scheduler(&self, team_id: &str) -> Option<Arc<crate::scheduler::TeammateManager>> {
        self.sessions.get(team_id).map(|e| e.session.scheduler().clone())
    }

    pub async fn resolve_team_tool_context(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<ResolvedTeamToolContext, TeamToolErrorPayload> {
        let Some(binding_lookup) = self
            .conversation_port
            .lookup_team_binding_by_conversation(conversation_id)
            .await
            .map_err(|error| error_payload(TeamToolErrorCode::RuntimeContextMissing, error.to_string()))?
        else {
            return Err(error_payload(
                TeamToolErrorCode::ConversationNotFound,
                "conversation not found",
            ));
        };

        if binding_lookup.user_id != user_id {
            return Err(error_payload(
                TeamToolErrorCode::PermissionDenied,
                "conversation does not belong to user",
            ));
        }

        let Some(team_id) = binding_lookup.team_id.clone() else {
            return Ok(ResolvedTeamToolContext {
                response: TeamToolContextResponse {
                    in_team: false,
                    conversation_id: conversation_id.to_owned(),
                    team_id: None,
                    team_name: None,
                    slot_id: None,
                    role: None,
                    agent_name: None,
                    transport: None,
                    allowed_tools: Vec::new(),
                },
                context: None,
            });
        };

        let team_row = self
            .repo
            .get_team(&team_id)
            .await
            .map_err(|error| error_payload(TeamToolErrorCode::RuntimeContextMissing, error.to_string()))?
            .ok_or_else(|| error_payload(TeamToolErrorCode::TeamNotFound, "team not found"))?;
        if team_row.user_id != user_id {
            return Err(error_payload(
                TeamToolErrorCode::PermissionDenied,
                "team does not belong to user",
            ));
        }

        let binding = TeamSessionBinding {
            team_id: team_id.clone(),
            slot_id: binding_lookup.slot_id,
            role: binding_lookup.role,
            runtime_seed: Default::default(),
            mcp: None,
        };
        let agents: Vec<crate::types::TeamAgent> = serde_json::from_str(&team_row.agents)
            .map_err(|error| error_payload(TeamToolErrorCode::RuntimeContextMissing, error.to_string()))?;
        let agent = agent_for_conversation(&agents, conversation_id, &binding)?;
        let context = crate::tool_executor::TeamToolContext {
            team_id: team_id.clone(),
            caller_slot_id: agent.slot_id.clone(),
            caller_role: agent.role,
            user_id: Some(user_id.to_owned()),
            conversation_id: Some(conversation_id.to_owned()),
            transport: TeamToolTransport::CliAssumed,
        };
        let allowed_tools = aionui_api_types::team_tool_descriptors_for_role(role_to_tool_role(agent.role))
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect::<Vec<_>>();
        Ok(ResolvedTeamToolContext {
            response: TeamToolContextResponse {
                in_team: true,
                conversation_id: conversation_id.to_owned(),
                team_id: Some(team_id),
                team_name: Some(team_row.name),
                slot_id: Some(agent.slot_id.clone()),
                role: Some(role_to_tool_role(agent.role)),
                agent_name: Some(agent.name.clone()),
                transport: Some(TeamToolTransport::CliAssumed),
                allowed_tools,
            },
            context: Some(context),
        })
    }

    pub async fn execute_team_tool(
        &self,
        context: &crate::tool_executor::TeamToolContext,
        call: TeamToolCall,
    ) -> Result<serde_json::Value, TeamToolErrorPayload> {
        let lifecycle_lock = self.lifecycle_lock(&context.team_id);
        let _lifecycle_guard = lifecycle_lock.read().await;
        let scheduler = self
            .get_session_scheduler(&context.team_id)
            .ok_or_else(|| error_payload(TeamToolErrorCode::TeamNotFound, "active team session not found"))?;
        execute_with_scheduler(&scheduler, &self.self_ref, context, call).await
    }

    #[cfg(test)]
    fn session_has_slow_monitor(&self, team_id: &str) -> bool {
        self.sessions
            .get(team_id)
            .map(|entry| !entry.slow_monitor_handle.is_finished())
            .unwrap_or(false)
    }

    #[cfg(test)]
    fn session_count_for_test(&self) -> usize {
        self.sessions.len()
    }

    pub async fn stop_session(&self, user_id: &str, team_id: &str) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.stop_session_unchecked(team_id);
        Ok(())
    }

    fn stop_session_unchecked(&self, team_id: &str) {
        if let Some((_, entry)) = self.sessions.remove(team_id) {
            entry.slow_monitor_handle.abort();
            entry.session.stop();
        }
    }

    pub async fn cleanup_idle_team_runtime_tasks(
        &self,
        idle_conversation_ids: Vec<String>,
        active_leases: &ActiveLeaseRegistry,
        idle_threshold_ms: TimestampMs,
    ) -> Vec<String> {
        if idle_conversation_ids.is_empty() {
            return Vec::new();
        }

        let idle_conversation_set: HashSet<String> = idle_conversation_ids.iter().cloned().collect();
        let now = now_ms();
        let mut handled_conversations = HashSet::new();
        let mut cleanup_teams = Vec::new();

        for entry in self.sessions.iter() {
            let team_id = entry.key().clone();
            let session = Arc::clone(&entry.session);
            let agents = session.scheduler().list_agents().await;
            let matched_idle_count = agents
                .iter()
                .filter(|agent| idle_conversation_set.contains(&agent.conversation_id))
                .count();
            if matched_idle_count == 0 {
                continue;
            }

            for agent in &agents {
                handled_conversations.insert(agent.conversation_id.clone());
            }

            if session.team_run_manager().current_active_run_id().is_some() {
                debug!(
                    team_id,
                    matched_idle_count, "team idle cleanup skipped because team run is active"
                );
                continue;
            }

            if agents
                .iter()
                .any(|agent| active_leases.active_until(&agent.conversation_id).is_some())
            {
                debug!(
                    team_id,
                    matched_idle_count, "team idle cleanup skipped because at least one member has an active lease"
                );
                continue;
            }

            if !agents.iter().all(|agent| {
                self.task_manager
                    .get_task(&agent.conversation_id)
                    .map(|task| is_idle_collectable_team_member(&task, now, idle_threshold_ms))
                    .unwrap_or(true)
            }) {
                debug!(
                    team_id,
                    matched_idle_count, "team idle cleanup skipped because at least one member runtime task is active"
                );
                continue;
            }

            cleanup_teams.push((team_id, agents, matched_idle_count));
        }

        for (team_id, agents, matched_idle_count) in cleanup_teams {
            info!(
                team_id,
                matched_idle_count,
                member_count = agents.len(),
                "team idle cleanup stopping idle team session"
            );
            info!(team_id, reason = "idle_cleanup", "broadcasting team session stopped");
            self.broadcast_session_status(&team_id, TeamSessionStatus::Stopped, None, |_| {});
            self.stop_session_unchecked(&team_id);
            for agent in agents {
                self.task_manager
                    .kill_and_wait(&agent.conversation_id, Some(AgentKillReason::IdleTimeout))
                    .await;
            }
        }

        idle_conversation_ids
            .into_iter()
            .filter(|conversation_id| !handled_conversations.contains(conversation_id))
            .collect()
    }

    fn normalize_idempotency_key(value: Option<&str>) -> Result<Option<String>, TeamError> {
        let Some(value) = value else {
            return Ok(None);
        };
        let value = value.trim();
        if value.is_empty() {
            return Err(TeamError::InvalidRequest("idempotency_key cannot be empty".into()));
        }
        if value.len() > IDEMPOTENCY_KEY_MAX_LEN || value.chars().any(char::is_control) {
            return Err(TeamError::InvalidRequest(
                "idempotency_key must be at most 240 characters and contain no control characters".into(),
            ));
        }
        Ok(Some(value.to_owned()))
    }

    fn idempotency_receipt_key(team_id: &str, slot_id: &str, key: &str) -> String {
        format!("{team_id}\0{slot_id}\0{key}")
    }

    async fn discard_failed_idempotency_receipt(
        &self,
        team_id: &str,
        slot_id: &str,
        key: &str,
        result: &Arc<AsyncMutex<Option<TeamRunAckResponse>>>,
    ) {
        let cache_key = Self::idempotency_receipt_key(team_id, slot_id, key);
        let mut receipts = self.idempotency_receipts.lock().await;
        let should_remove = receipts
            .get(&cache_key)
            .is_some_and(|receipt| Arc::ptr_eq(&receipt.result, result));
        if should_remove {
            receipts.remove(&cache_key);
        }
    }

    async fn idempotency_gate(
        &self,
        team_id: &str,
        slot_id: &str,
        key: Option<&str>,
        content: &str,
        files: Option<&Vec<String>>,
    ) -> Result<Option<Arc<AsyncMutex<Option<TeamRunAckResponse>>>>, TeamError> {
        let Some(key) = key else {
            return Ok(None);
        };
        let fingerprint = user_message_fingerprint(content, files.map(Vec::as_slice));
        let cache_key = Self::idempotency_receipt_key(team_id, slot_id, key);
        let now = Instant::now();
        let mut receipts = self.idempotency_receipts.lock().await;
        receipts.retain(|_, receipt| {
            now.duration_since(receipt.created_at) <= IDEMPOTENCY_RECEIPT_RETENTION
                || Arc::strong_count(&receipt.result) > 1
        });
        if let Some(receipt) = receipts.get(&cache_key) {
            if receipt.fingerprint != fingerprint {
                return Err(TeamError::InvalidRequest(
                    "idempotency_key was already used for a different Team message".into(),
                ));
            }
            return Ok(Some(receipt.result.clone()));
        }
        if receipts.len() >= IDEMPOTENCY_RECEIPT_LIMIT {
            let oldest = receipts
                .iter()
                .filter(|(_, receipt)| Arc::strong_count(&receipt.result) == 1)
                .min_by_key(|(_, receipt)| receipt.created_at)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                receipts.remove(&oldest);
            } else {
                return Err(TeamError::InvalidRequest(
                    "too many in-flight idempotent Team messages".into(),
                ));
            }
        }
        let result = Arc::new(AsyncMutex::new(None));
        receipts.insert(
            cache_key,
            IdempotencyReceipt {
                created_at: now,
                fingerprint,
                result: result.clone(),
            },
        );
        Ok(Some(result))
    }

    /// Backwards-compatible send path for internal callers that do not carry
    /// a caller receipt key.
    pub async fn send_message(
        &self,
        user_id: &str,
        team_id: &str,
        content: &str,
        files: Option<Vec<String>>,
    ) -> Result<TeamRunAckResponse, TeamError> {
        self.send_message_with_idempotency(user_id, team_id, content, files, None)
            .await
    }

    pub async fn send_message_with_idempotency(
        &self,
        user_id: &str,
        team_id: &str,
        content: &str,
        files: Option<Vec<String>>,
        idempotency_key: Option<String>,
    ) -> Result<TeamRunAckResponse, TeamError> {
        let lifecycle_lock = self.lifecycle_lock(team_id);
        let _lifecycle_guard = lifecycle_lock.read().await;
        self.load_owned_team(user_id, team_id).await?;
        let idempotency_key = Self::normalize_idempotency_key(idempotency_key.as_deref())?;
        let gate = self
            .idempotency_gate(team_id, "lead", idempotency_key.as_deref(), content, files.as_ref())
            .await?;
        if let Some(gate) = gate {
            let mut result = gate.lock().await;
            if let Some(ack) = result.as_ref() {
                return Ok(ack.clone());
            }
            let operation = async {
                self.ensure_session_inner_unlocked(team_id).await?;
                let session = {
                    let entry = self
                        .sessions
                        .get(team_id)
                        .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
                    Arc::clone(&entry.session)
                };
                session
                    .send_message_with_idempotency(content, files, idempotency_key.as_deref())
                    .await
            }
            .await;
            match operation {
                Ok(ack) => {
                    *result = Some(ack.clone());
                    Ok(ack)
                }
                Err(error) => {
                    self.discard_failed_idempotency_receipt(
                        team_id,
                        "lead",
                        idempotency_key.as_deref().expect("keyed gate requires a key"),
                        &gate,
                    )
                    .await;
                    Err(error)
                }
            }
        } else {
            self.ensure_session_inner_unlocked(team_id).await?;
            let session = {
                let entry = self
                    .sessions
                    .get(team_id)
                    .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
                Arc::clone(&entry.session)
            };
            session
                .send_message_with_idempotency(content, files, idempotency_key.as_deref())
                .await
        }
    }

    pub async fn send_message_to_agent(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        content: &str,
        files: Option<Vec<String>>,
        idempotency_key: Option<String>,
    ) -> Result<TeamRunAckResponse, TeamError> {
        self.send_message_to_agent_in_workspace(user_id, team_id, slot_id, content, files, idempotency_key, None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_message_to_agent_in_workspace(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        content: &str,
        files: Option<Vec<String>>,
        idempotency_key: Option<String>,
        workspace: Option<String>,
    ) -> Result<TeamRunAckResponse, TeamError> {
        let lifecycle_lock = self.lifecycle_lock(team_id);
        let _lifecycle_guard = lifecycle_lock.read().await;
        self.load_owned_team(user_id, team_id).await?;
        let workspace = workspace.as_deref().map(validate_create_workspace_path).transpose()?;
        let idempotency_key = Self::normalize_idempotency_key(idempotency_key.as_deref())?;
        let gate = self
            .idempotency_gate(team_id, slot_id, idempotency_key.as_deref(), content, files.as_ref())
            .await?;
        if let Some(gate) = gate {
            let mut result = gate.lock().await;
            if let Some(ack) = result.as_ref() {
                return Ok(ack.clone());
            }
            let operation = async {
                self.ensure_session_inner_unlocked(team_id).await?;
                let session = {
                    let entry = self
                        .sessions
                        .get(team_id)
                        .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
                    Arc::clone(&entry.session)
                };
                self.bind_idle_agent_workspace(user_id, team_id, slot_id, &session, workspace.as_deref())
                    .await?;
                session
                    .send_message_to_agent(slot_id, content, files, idempotency_key.as_deref())
                    .await
            }
            .await;
            match operation {
                Ok(ack) => {
                    *result = Some(ack.clone());
                    Ok(ack)
                }
                Err(error) => {
                    self.discard_failed_idempotency_receipt(
                        team_id,
                        slot_id,
                        idempotency_key.as_deref().expect("keyed gate requires a key"),
                        &gate,
                    )
                    .await;
                    Err(error)
                }
            }
        } else {
            self.ensure_session_inner_unlocked(team_id).await?;
            let session = {
                let entry = self
                    .sessions
                    .get(team_id)
                    .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
                Arc::clone(&entry.session)
            };
            self.bind_idle_agent_workspace(user_id, team_id, slot_id, &session, workspace.as_deref())
                .await?;
            session
                .send_message_to_agent(slot_id, content, files, idempotency_key.as_deref())
                .await
        }
    }

    /// Persist a provenance-preserving Worker delivery to this Team's Lead.
    /// Unlike a user-authored Agent message, this path records the active
    /// Worker turn's successful Lead delivery so finalization does not enqueue
    /// a redundant idle notification.
    pub async fn send_agent_delivery_to_lead(
        &self,
        user_id: &str,
        team_id: &str,
        source_slot_id: &str,
        content: &str,
        files: Option<Vec<String>>,
        idempotency_key: Option<String>,
    ) -> Result<TeamSendMessageQueuedResponse, TeamError> {
        let lifecycle_lock = self.lifecycle_lock(team_id);
        let _lifecycle_guard = lifecycle_lock.read().await;
        self.load_owned_team(user_id, team_id).await?;
        let idempotency_key = Self::normalize_idempotency_key(idempotency_key.as_deref())?;
        self.ensure_session_inner_unlocked(team_id).await?;
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        let lead_slot_id = session
            .scheduler()
            .find_lead_slot_id()
            .await
            .ok_or_else(|| TeamError::AgentNotFound("no lead agent in team".into()))?;
        let result = session
            .send_agent_message_from_agent_with_idempotency(
                source_slot_id,
                &lead_slot_id,
                content,
                files,
                idempotency_key.as_deref(),
            )
            .await?;
        Ok(TeamSendMessageQueuedResponse {
            team_run_id: result.team_run_id,
            target: result.target,
        })
    }

    async fn bind_idle_agent_workspace(
        &self,
        user_id: &str,
        team_id: &str,
        slot_id: &str,
        session: &TeamSession,
        workspace: Option<&str>,
    ) -> Result<(), TeamError> {
        let Some(workspace) = workspace else {
            return Ok(());
        };
        let agent = session.scheduler().get_agent(slot_id).await?;
        let status = session.scheduler().get_status(slot_id).await?;
        if !crate::scheduler::is_settled(status) {
            return Err(TeamError::InvalidRequest(format!(
                "cannot change workspace for active Team agent {slot_id}"
            )));
        }
        if self
            .conversation_port
            .conversation_workspace(&agent.conversation_id)
            .await?
            .as_deref()
            == Some(workspace)
        {
            return Ok(());
        }
        self.conversation_port
            .patch_runtime_config(&agent.conversation_id, serde_json::json!({ "workspace": workspace }))
            .await?;
        self.rebuild_agent_processes(team_id, session, user_id, std::slice::from_ref(&agent))
            .await
    }

    /// Best-effort runtime wake for mailbox rows already committed by the
    /// canonical WorkItem transaction. Durable unread reconciliation closes
    /// the process-interruption window after the transaction commits.
    pub(crate) fn wake_work_notifications(self: &Arc<Self>, user_id: &str, team_id: &str) {
        let service = Arc::clone(self);
        let user_id = user_id.to_owned();
        let team_id = team_id.to_owned();
        std::mem::drop(tokio::spawn(async move {
            if let Err(error) = service.ensure_session(&user_id, &team_id).await {
                warn!(
                    team_id,
                    error = %error,
                    "canonical WorkItem notification remains queued after session wake failed"
                );
                return;
            }
            let session = service.sessions.get(&team_id).map(|entry| Arc::clone(&entry.session));
            match session {
                Some(session) => {
                    if let Err(error) = session.notify_recoverable_unread_mailbox("team_work_command").await {
                        warn!(
                            team_id,
                            error = %error,
                            "canonical WorkItem notification remains queued after event-loop wake failed"
                        );
                    }
                }
                None => warn!(
                    team_id,
                    "canonical WorkItem notification remains queued because its Team session is unavailable"
                ),
            }
        }));
    }

    pub async fn ensure_agent_runtime(&self, user_id: &str, team_id: &str, slot_id: &str) -> Result<(), TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        let agent = team
            .agents
            .iter()
            .find(|agent| agent.slot_id == slot_id)
            .cloned()
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.to_owned()))?;
        self.ensure_session_inner(team_id).await?;
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        let work = self
            .reserve_member_runtime_reconciliation(&session, std::slice::from_ref(&agent))
            .await?;
        self.complete_member_runtime_reconciliation(team_id, user_id, session, work)
            .await
    }

    pub async fn cancel_run(
        &self,
        user_id: &str,
        team_id: &str,
        team_run_id: &str,
        target_slot_id: Option<String>,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id).await?;
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        session.cancel_run(team_run_id, target_slot_id, reason).await
    }

    pub async fn cancel_child_turn(
        &self,
        user_id: &str,
        team_id: &str,
        team_run_id: &str,
        slot_id: &str,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id).await?;
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        session.cancel_child_turn(team_run_id, slot_id, reason).await
    }

    pub async fn pause_slot_work(
        &self,
        user_id: &str,
        team_id: &str,
        team_run_id: &str,
        slot_id: &str,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        self.load_owned_team(user_id, team_id).await?;
        self.ensure_session_inner(team_id).await?;
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        session.pause_slot_work(team_run_id, slot_id, reason).await
    }

    pub async fn set_session_mode(&self, user_id: &str, team_id: &str, mode: &str) -> Result<(), TeamError> {
        let team = self.load_owned_team(user_id, team_id).await?;
        let provisioner = self.provisioner();
        self.repo
            .update_team(
                team_id,
                &UpdateTeamParams {
                    session_mode: Some(mode.to_owned()),
                    ..Default::default()
                },
            )
            .await?;

        for agent in &team.agents {
            let mode_applied = match self.task_manager.get_task(&agent.conversation_id) {
                Some(instance) => match set_active_agent_session_mode(&instance, mode).await {
                    Ok(()) => true,
                    Err(e) => {
                        warn!(
                            team_id,
                            slot_id = %agent.slot_id,
                            conversation_id = %agent.conversation_id,
                            error = %e,
                            "failed to set session mode on agent"
                        );
                        false
                    }
                },
                None => true,
            };
            if mode_applied && let Err(e) = provisioner.update_session_mode_seed(agent, mode).await {
                warn!(
                    team_id,
                    slot_id = %agent.slot_id,
                    conversation_id = %agent.conversation_id,
                    error = %e,
                    "failed to persist team session mode seed"
                );
            }
        }

        Ok(())
    }

    pub async fn send_agent_message_from_agent(
        &self,
        team_id: &str,
        from_slot_id: &str,
        to_slot_id: &str,
        content: &str,
        files: Option<Vec<String>>,
    ) -> Result<AgentMessageQueueResult, TeamError> {
        self.send_agent_message_from_agent_with_idempotency(team_id, from_slot_id, to_slot_id, content, files, None)
            .await
    }

    pub async fn send_agent_message_from_agent_with_idempotency(
        &self,
        team_id: &str,
        from_slot_id: &str,
        to_slot_id: &str,
        content: &str,
        files: Option<Vec<String>>,
        idempotency_key: Option<String>,
    ) -> Result<AgentMessageQueueResult, TeamError> {
        if from_slot_id == to_slot_id {
            return Err(TeamError::InvalidRequest(
                "team_send_message cannot target the authenticated caller".into(),
            ));
        }
        let idempotency_key = Self::normalize_idempotency_key(idempotency_key.as_deref())?;
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        session
            .send_agent_message_from_agent_with_idempotency(
                from_slot_id,
                to_slot_id,
                content,
                files,
                idempotency_key.as_deref(),
            )
            .await
    }

    pub async fn shutdown_agent_in_session(
        &self,
        team_id: &str,
        caller_slot_id: &str,
        target_slot_id: &str,
        reason: Option<String>,
    ) -> Result<(), TeamError> {
        let session = {
            let entry = self
                .sessions
                .get(team_id)
                .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
            Arc::clone(&entry.session)
        };
        session.shutdown_agent(caller_slot_id, target_slot_id, reason).await
    }

    pub(crate) async fn wake_leader_after_recovery_message(
        &self,
        team_id: &str,
        source_slot_id: &str,
        source: WorkSource,
    ) -> Result<(), TeamError> {
        let entry = self
            .sessions
            .get(team_id)
            .ok_or_else(|| TeamError::SessionNotFound(team_id.into()))?;
        entry
            .session
            .wake_leader_after_recovery_message(source_slot_id, source)
            .await
    }
}

async fn set_active_agent_session_mode(instance: &AgentInstance, mode: &str) -> Result<(), AgentError> {
    #[allow(unreachable_patterns)]
    match instance {
        AgentInstance::Acp(_) => instance.set_config_option("mode", mode).await.map(|_| ()),
        AgentInstance::Aionrs(manager) => manager.set_mode(mode).await,
        _ => instance.set_config_option("mode", mode).await.map(|_| ()),
    }
}

fn is_idle_collectable_team_member(task: &AgentInstance, now: TimestampMs, idle_threshold_ms: TimestampMs) -> bool {
    if !matches!(
        task.status(),
        None | Some(ConversationStatus::Pending | ConversationStatus::Finished)
    ) {
        return false;
    }
    now.saturating_sub(task.last_activity_at()) > idle_threshold_ms
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use aionui_ai_agent::types::{BuildTaskOptions, SendMessageData};
    use aionui_ai_agent::{
        ActiveLeaseRegistry, AgentError, AgentInstance, AgentSendError, AgentStreamEvent, IAgentTask, IMockAgent,
        IWorkerTaskManager, IdleCleanupCoordinator,
    };
    use aionui_api_types::{AddAgentRequest, ConfigOptionConfirmation, SetConfigOptionResponse, TeamRunTargetRole};
    use aionui_common::{AgentKillReason, AgentType, ConversationStatus, TimestampMs, now_ms};
    use aionui_db::{IConversationRepository, ITeamRepository};
    use tokio::sync::broadcast;

    use super::{TeamIdleCleanupCoordinator, TeamRuntimeStartGate};
    use crate::test_utils::workspace_harness::{
        setup_with_factory_metadata_team_repo_and_conversation_repo,
        setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster,
        setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager,
        single_agent_team_request,
    };

    #[test]
    fn team_runtime_start_concurrency_follows_host_parallelism() {
        assert!(super::host_team_runtime_start_concurrency() >= 1);
    }

    #[test]
    fn idempotency_key_rejects_empty_oversized_and_control_values() {
        assert!(super::TeamSessionService::normalize_idempotency_key(Some(" ")).is_err());
        assert!(super::TeamSessionService::normalize_idempotency_key(Some(&"x".repeat(241))).is_err());
        assert!(super::TeamSessionService::normalize_idempotency_key(Some("turn\n1")).is_err());
        assert_eq!(
            super::TeamSessionService::normalize_idempotency_key(Some(" turn-1 "))
                .unwrap()
                .as_deref(),
            Some("turn-1")
        );
    }

    #[tokio::test]
    async fn team_runtime_start_gate_has_no_artificial_admission_interval() {
        let gate = TeamRuntimeStartGate::new();
        let first = gate.acquire().await.unwrap();
        drop(first);

        let started_at = std::time::Instant::now();
        let second = gate.acquire().await.unwrap();
        let elapsed = started_at.elapsed();
        drop(second);

        assert!(
            elapsed < std::time::Duration::from_millis(100),
            "runtime admission must not wait on a fixed stagger: {elapsed:?}"
        );
    }

    struct ModeSettingAgent {
        conversation_id: String,
        agent_type: AgentType,
        mode_result: Mutex<Result<(), String>>,
        event_tx: broadcast::Sender<AgentStreamEvent>,
        status: Option<ConversationStatus>,
        last_activity_at: TimestampMs,
    }

    impl ModeSettingAgent {
        fn accepts_mode(conversation_id: &str) -> Self {
            Self::new(conversation_id, Ok(()))
        }

        fn rejects_mode(conversation_id: &str, message: &str) -> Self {
            Self::new(conversation_id, Err(message.to_owned()))
        }

        fn new(conversation_id: &str, mode_result: Result<(), String>) -> Self {
            let (event_tx, _) = broadcast::channel(1);
            Self {
                conversation_id: conversation_id.to_owned(),
                agent_type: AgentType::Acp,
                mode_result: Mutex::new(mode_result),
                event_tx,
                status: None,
                last_activity_at: now_ms(),
            }
        }

        fn idle_finished(conversation_id: &str) -> Self {
            Self::accepts_mode(conversation_id)
                .with_status(Some(ConversationStatus::Finished))
                .with_last_activity(now_ms() - 600_000)
        }

        fn idle_pending_aionrs(conversation_id: &str) -> Self {
            Self::accepts_mode(conversation_id)
                .with_agent_type(AgentType::Aionrs)
                .with_status(Some(ConversationStatus::Pending))
                .with_last_activity(now_ms() - 600_000)
        }

        fn with_agent_type(mut self, agent_type: AgentType) -> Self {
            self.agent_type = agent_type;
            self
        }

        fn with_status(mut self, status: Option<ConversationStatus>) -> Self {
            self.status = status;
            self
        }

        fn with_last_activity(mut self, last_activity_at: TimestampMs) -> Self {
            self.last_activity_at = last_activity_at;
            self
        }
    }

    #[async_trait::async_trait]
    impl IAgentTask for ModeSettingAgent {
        fn agent_type(&self) -> AgentType {
            self.agent_type
        }

        fn conversation_id(&self) -> &str {
            &self.conversation_id
        }

        fn workspace(&self) -> &str {
            "/tmp/aioncore-team-mode-test"
        }

        fn status(&self) -> Option<ConversationStatus> {
            self.status
        }

        fn last_activity_at(&self) -> TimestampMs {
            self.last_activity_at
        }

        fn subscribe(&self) -> broadcast::Receiver<AgentStreamEvent> {
            self.event_tx.subscribe()
        }

        async fn send_message(&self, _data: SendMessageData) -> Result<(), AgentSendError> {
            Ok(())
        }

        async fn cancel(&self) -> Result<(), AgentError> {
            Ok(())
        }

        fn kill(&self, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl IMockAgent for ModeSettingAgent {
        async fn set_config_option(&self, option_id: &str, value: &str) -> Result<SetConfigOptionResponse, AgentError> {
            assert_eq!(option_id, "mode");
            assert_eq!(value, "read-only");
            match self.mode_result.lock().unwrap().clone() {
                Ok(()) => Ok(SetConfigOptionResponse {
                    confirmation: ConfigOptionConfirmation::Observed,
                    config_options: None,
                }),
                Err(message) => Err(AgentError::bad_request(message)),
            }
        }
    }

    struct StaticTaskManager {
        tasks: HashMap<String, AgentInstance>,
    }

    impl StaticTaskManager {
        fn new(tasks: HashMap<String, AgentInstance>) -> Self {
            Self { tasks }
        }
    }

    #[async_trait::async_trait]
    impl IWorkerTaskManager for StaticTaskManager {
        fn get_task(&self, conversation_id: &str) -> Option<AgentInstance> {
            self.tasks.get(conversation_id).cloned()
        }

        async fn get_or_build_task(
            &self,
            _conversation_id: &str,
            _options: BuildTaskOptions,
        ) -> Result<AgentInstance, AgentError> {
            Err(AgentError::internal("static task manager does not build tasks"))
        }

        fn kill(&self, _conversation_id: &str, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
            Ok(())
        }

        fn kill_and_wait(
            &self,
            _conversation_id: &str,
            _reason: Option<AgentKillReason>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            Box::pin(std::future::ready(()))
        }

        async fn clear(&self) {}

        fn active_count(&self) -> usize {
            self.tasks.len()
        }

        fn collect_idle(&self, _idle_threshold_ms: TimestampMs) -> Vec<String> {
            Vec::new()
        }
    }

    struct MutableTaskManager {
        tasks: Mutex<HashMap<String, AgentInstance>>,
        kills: Mutex<Vec<String>>,
    }

    impl MutableTaskManager {
        fn new() -> Self {
            Self {
                tasks: Mutex::new(HashMap::new()),
                kills: Mutex::new(Vec::new()),
            }
        }

        fn insert_mode_agent(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().insert(
                conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::accepts_mode(conversation_id))),
            );
        }

        fn insert_idle_finished_agent(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().insert(
                conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::idle_finished(conversation_id))),
            );
        }

        fn insert_idle_pending_aionrs_agent(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().insert(
                conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::idle_pending_aionrs(conversation_id))),
            );
        }

        fn remove(&self, conversation_id: &str) {
            self.tasks.lock().unwrap().remove(conversation_id);
        }

        fn reset_kills(&self) {
            self.kills.lock().unwrap().clear();
        }

        fn kills(&self) -> Vec<String> {
            self.kills.lock().unwrap().clone()
        }
    }

    fn two_agent_team_request(name: &str) -> aionui_api_types::CreateTeamRequest {
        aionui_api_types::CreateTeamRequest {
            name: name.into(),
            agents: vec![
                aionui_api_types::TeamAgentInput {
                    name: "Lead".into(),
                    role: "lead".into(),
                    backend: Some("acp".into()),
                    model: "claude".into(),
                    assistant_id: None,
                    conversation_id: None,
                },
                aionui_api_types::TeamAgentInput {
                    name: "Worker".into(),
                    role: "teammate".into(),
                    backend: Some("acp".into()),
                    model: "claude".into(),
                    assistant_id: None,
                    conversation_id: None,
                },
            ],
            workspace: None,
        }
    }

    fn team_with_aionrs_worker_request(name: &str) -> aionui_api_types::CreateTeamRequest {
        let mut request = two_agent_team_request(name);
        request.agents.push(aionui_api_types::TeamAgentInput {
            name: "Butler".into(),
            role: "teammate".into(),
            backend: Some("aionrs".into()),
            model: "claude-sonnet".into(),
            assistant_id: None,
            conversation_id: None,
        });
        request
    }

    #[async_trait::async_trait]
    impl IWorkerTaskManager for MutableTaskManager {
        fn get_task(&self, conversation_id: &str) -> Option<AgentInstance> {
            self.tasks.lock().unwrap().get(conversation_id).cloned()
        }

        async fn get_or_build_task(
            &self,
            _conversation_id: &str,
            _options: BuildTaskOptions,
        ) -> Result<AgentInstance, AgentError> {
            Err(AgentError::internal("mutable task manager does not build tasks"))
        }

        fn kill(&self, conversation_id: &str, _reason: Option<AgentKillReason>) -> Result<(), AgentError> {
            self.kills.lock().unwrap().push(conversation_id.to_owned());
            self.remove(conversation_id);
            Ok(())
        }

        fn kill_and_wait(
            &self,
            conversation_id: &str,
            reason: Option<AgentKillReason>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
            let _ = self.kill(conversation_id, reason);
            Box::pin(std::future::ready(()))
        }

        async fn clear(&self) {
            self.tasks.lock().unwrap().clear();
        }

        fn active_count(&self) -> usize {
            self.tasks.lock().unwrap().len()
        }

        fn collect_idle(&self, _idle_threshold_ms: TimestampMs) -> Vec<String> {
            Vec::new()
        }
    }

    #[tokio::test]
    async fn session_has_slow_monitor() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Slow Monitor"))
            .await
            .unwrap();

        svc.ensure_session("user-test", &created.id).await.unwrap();

        assert!(svc.session_has_slow_monitor(&created.id));
        svc.stop_session("user-test", &created.id).await.unwrap();
    }

    #[tokio::test]
    async fn ensure_session_emits_agent_runtime_ready_after_member_warmup() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime Events"))
            .await
            .unwrap();
        let assistant = created.assistants.first().expect("team assistant");

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let events = broadcaster.events_by_name("team.agentRuntimeStatusChanged");
        let statuses: Vec<&str> = events
            .iter()
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();

        assert_eq!(statuses, vec!["pending", "ready"]);
        assert_eq!(
            events[0].data.get("team_id").and_then(serde_json::Value::as_str),
            Some(created.id.as_str())
        );
        assert_eq!(
            events[0].data.get("slot_id").and_then(serde_json::Value::as_str),
            Some(assistant.slot_id.as_str())
        );
        assert_eq!(
            events[0]
                .data
                .get("conversation_id")
                .and_then(serde_json::Value::as_str),
            Some(assistant.conversation_id.as_str())
        );
    }

    #[tokio::test]
    async fn ensure_session_keeps_worker_dormant_until_first_message() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Runtime Repair"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();

        svc.ensure_session("user-test", &created.id).await.unwrap();
        task_manager.insert_mode_agent(&lead.conversation_id);
        task_manager.reset_kills();
        let original_session = Arc::clone(&svc.sessions.get(&created.id).expect("session").session);
        let original_generation = original_session.generation();

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let current_session = Arc::clone(&svc.sessions.get(&created.id).expect("session").session);
        assert!(Arc::ptr_eq(&original_session, &current_session));
        assert_eq!(current_session.generation(), original_generation);
        assert!(task_manager.kills().is_empty());
        assert!(current_session.event_loops().has(&lead.slot_id));
        assert!(current_session.event_loops().has(&worker.slot_id));
        assert_eq!(
            current_session.member_runtimes().snapshot(&worker.slot_id),
            crate::member_runtime::MemberRuntimeSnapshot::Absent
        );

        let task_manager_for_ready = task_manager.clone();
        let worker_conversation_id = worker.conversation_id.clone();
        broadcaster.set_observer(Arc::new(move |event| {
            if event.name == "team.agentRuntimeStatusChanged"
                && event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready")
                && event.data.get("conversation_id").and_then(serde_json::Value::as_str)
                    == Some(worker_conversation_id.as_str())
            {
                task_manager_for_ready.insert_mode_agent(&worker_conversation_id);
            }
        }));

        let ack = current_session
            .send_message_to_agent(&worker.slot_id, "start on demand", None, None)
            .await
            .unwrap();
        assert_eq!(
            ack.enqueue_status,
            aionui_api_types::TeamMessageEnqueueStatus::BlockedRuntimeStarting
        );

        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                match current_session.member_runtimes().snapshot(&worker.slot_id) {
                    crate::member_runtime::MemberRuntimeSnapshot::Ready => break,
                    crate::member_runtime::MemberRuntimeSnapshot::Failed { failure, .. } => {
                        panic!("worker runtime failed to start on demand: {failure:?}")
                    }
                    _ => tokio::task::yield_now().await,
                }
            }
        })
        .await
        .expect("worker runtime should start after its first queued message");
        assert_eq!(task_manager.kills(), vec![worker.conversation_id.clone()]);

        let events = broadcaster.events_by_name("team.agentRuntimeStatusChanged");
        let statuses: Vec<&str> = events
            .iter()
            .filter(|event| {
                event.data.get("slot_id").and_then(serde_json::Value::as_str) == Some(worker.slot_id.as_str())
            })
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();

        assert_eq!(statuses, vec!["pending", "pending", "ready"]);
    }

    #[tokio::test]
    async fn ensure_agent_runtime_starts_a_dormant_worker_without_enqueuing_work() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Explicit Runtime Ensure"))
            .await
            .unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();

        svc.ensure_session("user-test", &created.id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).expect("session").session);
        assert_eq!(
            session.member_runtimes().snapshot(&worker.slot_id),
            crate::member_runtime::MemberRuntimeSnapshot::Absent
        );
        let task_manager_for_ready = task_manager.clone();
        let worker_conversation_id = worker.conversation_id.clone();
        broadcaster.set_observer(Arc::new(move |event| {
            if event.name == "team.agentRuntimeStatusChanged"
                && event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready")
                && event.data.get("conversation_id").and_then(serde_json::Value::as_str)
                    == Some(worker_conversation_id.as_str())
            {
                task_manager_for_ready.insert_mode_agent(&worker_conversation_id);
            }
        }));

        svc.ensure_agent_runtime("user-test", &created.id, &worker.slot_id)
            .await
            .unwrap();

        assert_eq!(
            session.member_runtimes().snapshot(&worker.slot_id),
            crate::member_runtime::MemberRuntimeSnapshot::Ready
        );
        let state = svc.get_run_state("user-test", &created.id).await.unwrap();
        assert!(state.active_run.is_none());
        assert!(state.slot_work.iter().all(|slot| slot.team_run_id.is_none()));
    }

    #[tokio::test]
    async fn no_work_reconciliation_rejects_replaced_session() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("No work replacement"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let old = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        svc.stop_session("user-test", &created.id).await.unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();

        let result = svc
            .complete_member_runtime_reconciliation(&created.id, "user-test", old, Vec::new())
            .await;
        assert!(matches!(result, Err(crate::TeamError::SessionNotFound(_))));
    }

    #[tokio::test]
    async fn replaced_session_cannot_publish_dynamic_runtime_ready() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Runtime ready generation fence"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let old = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        let agent = old.scheduler().list_agents().await.remove(0);

        svc.stop_session("user-test", &created.id).await.unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let ready_before = broadcaster
            .events_by_name("team.agentRuntimeStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready"))
            .count();

        assert!(!svc.publish_member_runtime_ready_if_current(&old, &agent));
        let ready_after = broadcaster
            .events_by_name("team.agentRuntimeStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready"))
            .count();
        assert_eq!(ready_after, ready_before, "old generations must not publish Ready");
    }

    #[tokio::test]
    async fn stale_cleanup_does_not_kill_replacement_runtime() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Cleanup fence"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let old = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        svc.stop_session("user-test", &created.id).await.unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();
        task_manager.insert_mode_agent(&conversation_id);

        svc.cleanup_stale_member_runtime_task(&old, &conversation_id).await;

        assert!(task_manager.get_task(&conversation_id).is_some());
    }

    #[tokio::test]
    async fn idle_cleanup_stops_team_session_and_kills_all_members_when_team_is_collectable() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Idle Cleanup"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        task_manager.insert_idle_finished_agent(&lead.conversation_id);
        task_manager.insert_idle_finished_agent(&worker.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let unhandled = svc
            .cleanup_idle_team_runtime_tasks(vec![lead.conversation_id.clone()], &ActiveLeaseRegistry::new(), 300_000)
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);
    }

    #[tokio::test]
    async fn idle_cleanup_broadcasts_team_session_stopped() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Idle Cleanup Stopped Broadcast"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let worker = created
            .assistants
            .iter()
            .find(|agent| agent.role == "teammate")
            .unwrap();
        task_manager.insert_idle_finished_agent(&lead.conversation_id);
        task_manager.insert_idle_finished_agent(&worker.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let unhandled = svc
            .cleanup_idle_team_runtime_tasks(vec![lead.conversation_id.clone()], &ActiveLeaseRegistry::new(), 300_000)
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);

        let stopped_events: Vec<_> = broadcaster
            .events_by_name("team.sessionStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("stopped"))
            .collect();
        assert_eq!(
            stopped_events.len(),
            1,
            "idle cleanup must broadcast exactly one stopped status"
        );
        assert_eq!(
            stopped_events[0]
                .data
                .get("team_id")
                .and_then(serde_json::Value::as_str),
            Some(created.id.as_str())
        );
    }

    #[tokio::test]
    async fn explicit_stop_session_does_not_broadcast_team_session_stopped() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team(
                "user-test",
                single_agent_team_request("Explicit Stop No Stopped Broadcast"),
            )
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();

        svc.stop_session("user-test", &created.id).await.unwrap();

        let stopped_count = broadcaster
            .events_by_name("team.sessionStatusChanged")
            .into_iter()
            .filter(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("stopped"))
            .count();
        assert_eq!(stopped_count, 0, "explicit stop must not broadcast a stopped status");
    }

    #[test]
    fn idle_collectable_team_member_accepts_idle_pending_aionrs_runtime() {
        let task = AgentInstance::Mock(Arc::new(ModeSettingAgent::idle_pending_aionrs("aionrs-idle")));

        assert!(super::is_idle_collectable_team_member(&task, now_ms(), 300_000));
    }

    #[test]
    fn idle_collectable_team_member_rejects_running_aionrs_runtime() {
        let task = AgentInstance::Mock(Arc::new(
            ModeSettingAgent::accepts_mode("aionrs-running")
                .with_agent_type(AgentType::Aionrs)
                .with_status(Some(ConversationStatus::Running))
                .with_last_activity(now_ms() - 600_000),
        ));

        assert!(!super::is_idle_collectable_team_member(&task, now_ms(), 300_000));
    }

    #[tokio::test]
    async fn idle_cleanup_stops_team_session_when_aionrs_member_is_idle_pending() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", team_with_aionrs_worker_request("Idle Aionrs Cleanup"))
            .await
            .unwrap();
        let lead = created.assistants.iter().find(|agent| agent.role == "lead").unwrap();
        let acp_worker = created.assistants.iter().find(|agent| agent.name == "Worker").unwrap();
        let aionrs_worker = created.assistants.iter().find(|agent| agent.name == "Butler").unwrap();
        task_manager.insert_idle_finished_agent(&lead.conversation_id);
        task_manager.insert_idle_finished_agent(&acp_worker.conversation_id);
        task_manager.insert_idle_pending_aionrs_agent(&aionrs_worker.conversation_id);

        svc.ensure_session("user-test", &created.id).await.unwrap();

        let unhandled = svc
            .cleanup_idle_team_runtime_tasks(
                vec![lead.conversation_id.clone(), acp_worker.conversation_id.clone()],
                &ActiveLeaseRegistry::new(),
                300_000,
            )
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);
    }

    #[tokio::test]
    async fn team_idle_cleanup_coordinator_delegates_to_team_service() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", two_agent_team_request("Idle Coordinator"))
            .await
            .unwrap();
        for agent in &created.assistants {
            task_manager.insert_idle_finished_agent(&agent.conversation_id);
        }

        svc.ensure_session("user-test", &created.id).await.unwrap();
        let coordinator = TeamIdleCleanupCoordinator::new(svc.clone(), Arc::new(ActiveLeaseRegistry::new()));

        let unhandled = coordinator
            .cleanup_idle_conversations(vec![created.assistants[0].conversation_id.clone()], 300_000)
            .await;

        assert!(unhandled.is_empty());
        assert_eq!(svc.session_count_for_test(), 0);
        assert_eq!(task_manager.active_count(), 0);
    }

    #[tokio::test]
    async fn manual_add_agent_in_active_session_emits_runtime_ready_after_background_attach() {
        let task_manager = Arc::new(MutableTaskManager::new());
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager.clone());
        let created = svc
            .create_team("user-test", single_agent_team_request("Manual Runtime Events"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let task_manager_for_ready = task_manager.clone();
        broadcaster.set_observer(Arc::new(move |event| {
            if event.name == "team.agentRuntimeStatusChanged"
                && event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready")
                && let Some(conversation_id) = event.data.get("conversation_id").and_then(serde_json::Value::as_str)
            {
                task_manager_for_ready.insert_mode_agent(conversation_id);
            }
        }));

        let added = svc
            .add_agent(
                "user-test",
                &created.id,
                AddAgentRequest {
                    name: "Worker".to_owned(),
                    role: "teammate".to_owned(),
                    backend: Some("acp".to_owned()),
                    model: "claude".to_owned(),
                    assistant_id: None,
                },
            )
            .await
            .unwrap();

        let events = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                let events = broadcaster.events_by_name("team.agentRuntimeStatusChanged");
                let added_events: Vec<_> = events
                    .into_iter()
                    .filter(|event| {
                        event.data.get("slot_id").and_then(serde_json::Value::as_str) == Some(added.slot_id.as_str())
                    })
                    .collect();
                if added_events
                    .iter()
                    .any(|event| event.data.get("status").and_then(serde_json::Value::as_str) == Some("ready"))
                {
                    break added_events;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("runtime ready event should be emitted");
        let statuses: Vec<&str> = events
            .iter()
            .map(|event| event.data.get("status").and_then(serde_json::Value::as_str).unwrap())
            .collect();

        assert_eq!(statuses, vec!["pending", "ready"]);
        let session = Arc::clone(&svc.sessions.get(&created.id).expect("session").session);
        assert_eq!(session.event_loops().len(), 2, "new slot must own one event loop");
        assert!(session.event_loops().has(&added.slot_id));
        assert_eq!(
            session.member_runtimes().snapshot(&added.slot_id),
            crate::member_runtime::MemberRuntimeSnapshot::Ready
        );
    }

    #[tokio::test]
    async fn dynamic_member_is_reserved_before_membership_event() {
        let (svc, _repo, _task_manager, _conv_repo, broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_and_broadcaster();
        let created = svc
            .create_team("user-test", single_agent_team_request("Reservation ordering"))
            .await
            .unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let session = Arc::clone(&svc.sessions.get(&created.id).unwrap().session);
        let observed = Arc::new(std::sync::Mutex::new(None));
        let observed_for_event = Arc::clone(&observed);
        broadcaster.set_observer(Arc::new(move |event| {
            if event.name != crate::events::TEAM_AGENT_SPAWNED_EVENT {
                return;
            }
            let slot_id = event
                .data
                .get("assistant")
                .and_then(|assistant| assistant.get("slot_id"))
                .and_then(serde_json::Value::as_str);
            if let Some(slot_id) = slot_id {
                *observed_for_event.lock().unwrap() = Some(session.member_runtimes().snapshot(slot_id));
            }
        }));

        svc.add_agent(
            "user-test",
            &created.id,
            AddAgentRequest {
                name: "Worker".to_owned(),
                role: "teammate".to_owned(),
                backend: Some("acp".to_owned()),
                model: "claude".to_owned(),
                assistant_id: None,
            },
        )
        .await
        .unwrap();

        assert!(matches!(
            observed.lock().unwrap().as_ref(),
            Some(
                crate::member_runtime::MemberRuntimeSnapshot::Attaching { .. }
                    | crate::member_runtime::MemberRuntimeSnapshot::Ready
            )
        ));
    }

    #[tokio::test]
    async fn set_session_mode_persists_team_mode_and_new_agents_inherit_it() {
        let (svc, repo, _task_manager, conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Mode Seed"))
            .await
            .unwrap();

        svc.set_session_mode("user-test", &created.id, "full_auto")
            .await
            .unwrap();

        let row = repo.get_team(&created.id).await.unwrap().expect("team row");
        assert_eq!(row.session_mode.as_deref(), Some("full_auto"));

        let added = svc
            .add_agent(
                "user-test",
                &created.id,
                AddAgentRequest {
                    name: "Worker".to_owned(),
                    role: "teammate".to_owned(),
                    backend: Some("acp".to_owned()),
                    model: "claude".to_owned(),
                    assistant_id: None,
                },
            )
            .await
            .unwrap();
        let extra = conv_repo
            .get_extra(&added.conversation_id)
            .expect("added conversation extra");

        assert_eq!(
            extra.get("session_mode").and_then(serde_json::Value::as_str),
            Some("full_auto")
        );
    }

    #[tokio::test]
    async fn set_session_mode_does_not_persist_agent_seed_when_active_runtime_rejects_mode() {
        let accepting_conversation_id = "conv-accepts";
        let rejecting_conversation_id = "conv-rejects";
        let task_manager = Arc::new(StaticTaskManager::new(HashMap::from([
            (
                accepting_conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::accepts_mode(accepting_conversation_id))),
            ),
            (
                rejecting_conversation_id.to_owned(),
                AgentInstance::Mock(Arc::new(ModeSettingAgent::rejects_mode(
                    rejecting_conversation_id,
                    "Value 'read-only' is not selectable for config option 'mode'",
                ))),
            ),
        ])));
        let (svc, repo, _task_manager, conv_repo, _broadcaster) =
            setup_with_factory_metadata_team_repo_conversation_repo_broadcaster_and_task_manager(task_manager);
        let created = svc
            .create_team("user-test", single_agent_team_request("Partial Mode Seed"))
            .await
            .unwrap();
        let mut row = repo.get_team(&created.id).await.unwrap().expect("team row");
        row.agents = serde_json::json!([
            {
                "slot_id": "slot-accepts",
                "name": "Codex CLI",
                "role": "lead",
                "conversation_id": accepting_conversation_id,
                "backend": "codex",
                "model": "openai.gpt-5.5",
                "assistant_id": "bare:codex"
            },
            {
                "slot_id": "slot-rejects",
                "name": "Claude Code",
                "role": "teammate",
                "conversation_id": rejecting_conversation_id,
                "backend": "claude",
                "model": "global.anthropic.claude-opus-4-8",
                "assistant_id": "bare:claude"
            }
        ])
        .to_string();
        repo.update_team(
            &created.id,
            &aionui_db::UpdateTeamParams {
                agents: Some(row.agents),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        conv_repo
            .create(&aionui_db::models::ConversationRow {
                id: accepting_conversation_id.to_owned(),
                user_id: "user-test".to_owned(),
                name: "Codex CLI".to_owned(),
                r#type: AgentType::Acp.serde_name().to_owned(),
                extra: serde_json::json!({
                    "current_mode_id": "default",
                    "session_mode": "default"
                })
                .to_string(),
                model: None,
                status: Some("pending".to_owned()),
                source: None,
                channel_chat_id: None,
                pinned: false,
                pinned_at: None,
                created_at: now_ms(),
                updated_at: now_ms(),
            })
            .await
            .unwrap();
        conv_repo
            .create(&aionui_db::models::ConversationRow {
                id: rejecting_conversation_id.to_owned(),
                user_id: "user-test".to_owned(),
                name: "Claude Code".to_owned(),
                r#type: AgentType::Acp.serde_name().to_owned(),
                extra: serde_json::json!({
                    "current_mode_id": "default",
                    "session_mode": "default"
                })
                .to_string(),
                model: None,
                status: Some("pending".to_owned()),
                source: None,
                channel_chat_id: None,
                pinned: false,
                pinned_at: None,
                created_at: now_ms(),
                updated_at: now_ms(),
            })
            .await
            .unwrap();

        svc.set_session_mode("user-test", &created.id, "read-only")
            .await
            .unwrap();

        let team = repo.get_team(&created.id).await.unwrap().expect("team row");
        assert_eq!(team.session_mode.as_deref(), Some("read-only"));

        let accepting_extra = conv_repo.get_extra(accepting_conversation_id).unwrap();
        assert_eq!(
            accepting_extra.get("session_mode").and_then(serde_json::Value::as_str),
            Some("read-only")
        );

        let rejecting_extra = conv_repo.get_extra(rejecting_conversation_id).unwrap();
        assert_eq!(
            rejecting_extra.get("session_mode").and_then(serde_json::Value::as_str),
            Some("default")
        );
    }

    #[tokio::test]
    async fn run_state_returns_none_without_session_and_does_not_create_session() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Run State"))
            .await
            .unwrap();
        svc.stop_session("user-test", &created.id).await.unwrap();

        assert_eq!(svc.session_count_for_test(), 0);

        let state = svc.get_run_state("user-test", &created.id).await.unwrap();

        assert!(state.active_run.is_none());
        assert_eq!(svc.session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn cold_ensure_and_restart_keep_idle_lead_role() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Idle Lead Role"))
            .await
            .unwrap();
        let lead_slot_id = created.assistants[0].slot_id.clone();

        svc.ensure_session("user-test", &created.id).await.unwrap();
        let cold_state = svc.get_run_state("user-test", &created.id).await.unwrap();
        assert!(cold_state.active_run.is_none());
        assert_eq!(cold_state.slot_work.len(), 1);
        assert_eq!(cold_state.slot_work[0].slot_id, lead_slot_id);
        assert_eq!(cold_state.slot_work[0].role, TeamRunTargetRole::Lead);

        svc.stop_session("user-test", &created.id).await.unwrap();
        svc.ensure_session("user-test", &created.id).await.unwrap();
        let restarted_state = svc.get_run_state("user-test", &created.id).await.unwrap();
        assert!(restarted_state.active_run.is_none());
        assert_eq!(restarted_state.slot_work.len(), 1);
        assert_eq!(restarted_state.slot_work[0].slot_id, lead_slot_id);
        assert_eq!(restarted_state.slot_work[0].role, TeamRunTargetRole::Lead);
    }

    #[tokio::test]
    async fn config_options_returns_snapshot_without_creating_team_session() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Config Options"))
            .await
            .unwrap();
        let conversation_id = &created.assistants[0].conversation_id;

        assert_eq!(svc.session_count_for_test(), 0);

        let options = svc
            .get_conversation_config_options("user-test", &created.id, conversation_id)
            .await
            .unwrap();

        assert_eq!(options.config_options[0].id, "model");
        assert_eq!(svc.session_count_for_test(), 0);
    }

    #[tokio::test]
    async fn run_state_returns_current_active_payload() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Active Run State"))
            .await
            .unwrap();

        let ack = svc.send_message("user-test", &created.id, "hello", None).await.unwrap();
        let state = svc.get_run_state("user-test", &created.id).await.unwrap();
        let active_run = state.active_run.expect("active run state");

        assert_eq!(active_run.team_id, created.id);
        assert_eq!(active_run.team_run_id, ack.run.team_run_id);
        assert_eq!(active_run.status, ack.run.status);
        assert_eq!(active_run.target_slot_id, ack.run.target_slot_id);
        assert_eq!(active_run.target_role, ack.run.target_role);
        assert_eq!(active_run.target_role, TeamRunTargetRole::Lead);
        assert_eq!(active_run.queued_intent_count, 1);
        assert_eq!(active_run.slot_work.len(), 1);
        assert_eq!(active_run.slot_work[0].slot_id, ack.run.slot_work[0].slot_id);
        assert_eq!(active_run.slot_work[0].role, TeamRunTargetRole::Lead);
    }

    #[tokio::test]
    async fn unknown_pause_does_not_pollute_run_state() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Unknown Pause"))
            .await
            .unwrap();
        let ack = svc.send_message("user-test", &created.id, "hello", None).await.unwrap();
        let before = svc.get_run_state("user-test", &created.id).await.unwrap();

        let error = svc
            .pause_slot_work(
                "user-test",
                &created.id,
                &ack.run.team_run_id,
                "missing-slot",
                Some("not a member".to_owned()),
            )
            .await
            .unwrap_err();

        assert!(matches!(error, crate::error::TeamError::AgentNotFound(ref slot_id) if slot_id == "missing-slot"));
        let after = svc.get_run_state("user-test", &created.id).await.unwrap();
        assert_eq!(after.session_generation, before.session_generation);
        assert_eq!(
            after.active_run.as_ref().map(|run| run.team_run_id.as_str()),
            Some(ack.run.team_run_id.as_str())
        );
        assert_eq!(after.slot_work.len(), before.slot_work.len());
        assert!(
            after
                .slot_work
                .iter()
                .all(|slot| { slot.slot_id != "missing-slot" && slot.role == TeamRunTargetRole::Lead })
        );
        assert!(
            after
                .active_run
                .into_iter()
                .flat_map(|run| run.slot_work)
                .all(|slot| { slot.slot_id != "missing-slot" && slot.role == TeamRunTargetRole::Lead })
        );
    }

    #[tokio::test]
    async fn config_options_return_member_runtime_snapshot() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config"))
            .await
            .unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();

        let response = svc
            .get_conversation_config_options("user-test", &created.id, &conversation_id)
            .await
            .unwrap();

        let model = response
            .config_options
            .iter()
            .find(|option| option.id == "model")
            .expect("model config option");
        assert_eq!(model.current_value.as_deref(), Some("claude"));
    }

    #[tokio::test]
    async fn config_options_reports_runtime_not_ready_for_member_conversation() {
        let (svc, _repo, _task_manager, conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config Pending"))
            .await
            .unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();
        conv_repo.mark_runtime_not_ready(&conversation_id);

        let err = svc
            .get_conversation_config_options("user-test", &created.id, &conversation_id)
            .await
            .expect_err("member runtime readiness should be reported distinctly");

        assert!(matches!(
            err,
            crate::error::TeamError::RuntimeNotReady {
                conversation_id: ref id
            } if id == &conversation_id
        ));
    }

    #[tokio::test]
    async fn config_options_reject_non_member_conversation() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config Reject"))
            .await
            .unwrap();

        let err = svc
            .get_conversation_config_options("user-test", &created.id, "other-conversation")
            .await
            .expect_err("non-member conversation must be rejected");

        assert!(matches!(err, crate::error::TeamError::AgentNotFound(_)));
    }

    #[tokio::test]
    async fn config_options_reject_cross_user_access() {
        let (svc, _repo, _task_manager, _conv_repo) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        let created = svc
            .create_team("user-test", single_agent_team_request("Team Config Owner"))
            .await
            .unwrap();
        let conversation_id = created.assistants[0].conversation_id.clone();

        let err = svc
            .get_conversation_config_options("other-user", &created.id, &conversation_id)
            .await
            .expect_err("team config options must reject cross-user access");

        assert!(matches!(err, crate::error::TeamError::Forbidden(_)));
    }
}
