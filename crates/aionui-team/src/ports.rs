use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use aionui_api_types::{ConversationRuntimeSummary, TeamRunTargetRole};
use async_trait::async_trait;

use crate::error::TeamError;
use crate::kernel::{GitWorkAssignment, IntegrationRecoveryReason};

/// Stable inputs used to plan a Git assignment before the WorkItem is queued.
///
/// `assignment_key` is an opaque, idempotent command key. Adapters must derive
/// deterministic branch/worktree coordinates from it and must never expose it
/// as a filesystem path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitWorkAssignmentPlan {
    team_id: String,
    assignee_member_id: String,
    workspace: String,
    assignment_key: String,
}

impl GitWorkAssignmentPlan {
    pub(crate) fn new(
        team_id: impl Into<String>,
        assignee_member_id: impl Into<String>,
        workspace: impl Into<String>,
        assignment_key: impl Into<String>,
    ) -> Self {
        Self {
            team_id: team_id.into(),
            assignee_member_id: assignee_member_id.into(),
            workspace: workspace.into(),
            assignment_key: assignment_key.into(),
        }
    }

    pub fn team_id(&self) -> &str {
        &self.team_id
    }

    pub fn assignee_member_id(&self) -> &str {
        &self.assignee_member_id
    }

    pub fn workspace(&self) -> &str {
        &self.workspace
    }

    pub fn assignment_key(&self) -> &str {
        &self.assignment_key
    }
}

/// Exact persisted assignment plus its deterministic local worktree key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedGitWorkAssignment {
    assignment: GitWorkAssignment,
    workspace_path: String,
}

impl PreparedGitWorkAssignment {
    pub fn new(assignment: GitWorkAssignment, workspace_path: impl Into<String>) -> Self {
        Self {
            assignment,
            workspace_path: workspace_path.into(),
        }
    }

    pub fn assignment(&self) -> &GitWorkAssignment {
        &self.assignment
    }

    pub fn workspace_path(&self) -> &str {
        &self.workspace_path
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitIntegrationTarget {
    branch_ref: String,
    head_commit: String,
}

impl GitIntegrationTarget {
    pub fn new(branch_ref: impl Into<String>, head_commit: impl Into<String>) -> Self {
        Self {
            branch_ref: branch_ref.into(),
            head_commit: head_commit.into(),
        }
    }

    pub fn branch_ref(&self) -> &str {
        &self.branch_ref
    }

    pub fn head_commit(&self) -> &str {
        &self.head_commit
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GitWorkspacePortError {
    #[error("Git workspace adapter is temporarily unavailable: {0}")]
    Unavailable(String),
    #[error("Git workspace adapter rejected the request: {0}")]
    InvalidRequest(String),
}

#[async_trait]
pub trait TeamGitWorkspacePort: Send + Sync {
    /// Resolve a clean target and choose immutable repository/base/source-ref
    /// coordinates. This method is read-only.
    async fn plan_assignment(&self, plan: &GitWorkAssignmentPlan) -> Result<GitWorkAssignment, GitWorkspacePortError>;

    /// Materialize or verify the exact assignment after it is durable. A
    /// successful retry must return the same registered worktree.
    async fn prepare_assignment(
        &self,
        plan: &GitWorkAssignmentPlan,
        assignment: &GitWorkAssignment,
    ) -> Result<PreparedGitWorkAssignment, GitWorkspacePortError>;

    /// Resolve the current clean integration target from the Team workspace.
    async fn resolve_integration_target(
        &self,
        workspace: &str,
        expected_repository_id: &str,
    ) -> Result<GitIntegrationTarget, GitWorkspacePortError>;
}

/// Exact, durable coordinates for one Git integration attempt.
///
/// The adapter may resolve local paths from `repository_id`, but it must not
/// substitute refs or commits. Retries and restart reconciliation receive the
/// same value loaded from SQLite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitIntegrationIntent {
    attempt_id: String,
    team_id: String,
    work_item_id: String,
    delivery_id: String,
    repository_id: String,
    base_commit: String,
    source_ref: String,
    source_head: String,
    target_ref: String,
    target_head: String,
}

impl GitIntegrationIntent {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        attempt_id: impl Into<String>,
        team_id: impl Into<String>,
        work_item_id: impl Into<String>,
        delivery_id: impl Into<String>,
        repository_id: impl Into<String>,
        base_commit: impl Into<String>,
        source_ref: impl Into<String>,
        source_head: impl Into<String>,
        target_ref: impl Into<String>,
        target_head: impl Into<String>,
    ) -> Self {
        Self {
            attempt_id: attempt_id.into(),
            team_id: team_id.into(),
            work_item_id: work_item_id.into(),
            delivery_id: delivery_id.into(),
            repository_id: repository_id.into(),
            base_commit: base_commit.into(),
            source_ref: source_ref.into(),
            source_head: source_head.into(),
            target_ref: target_ref.into(),
            target_head: target_head.into(),
        }
    }

    pub fn attempt_id(&self) -> &str {
        &self.attempt_id
    }

    pub fn team_id(&self) -> &str {
        &self.team_id
    }

    pub fn work_item_id(&self) -> &str {
        &self.work_item_id
    }

    pub fn delivery_id(&self) -> &str {
        &self.delivery_id
    }

    pub fn repository_id(&self) -> &str {
        &self.repository_id
    }

    pub fn base_commit(&self) -> &str {
        &self.base_commit
    }

    pub fn source_ref(&self) -> &str {
        &self.source_ref
    }

    pub fn source_head(&self) -> &str {
        &self.source_head
    }

    pub fn target_ref(&self) -> &str {
        &self.target_ref
    }

    pub fn target_head(&self) -> &str {
        &self.target_head
    }
}

/// Trusted evidence returned by the Git adapter for the exact input intent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitDeliveryPortOutcome {
    Merged {
        merged_commit: String,
        observed_target_head: String,
    },
    Conflicted {
        observed_target_head: String,
    },
    Retryable {
        reason: IntegrationRecoveryReason,
        observed_target_head: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitDeliveryReconciliation {
    Resolved(GitDeliveryPortOutcome),
    ReadyToIntegrate,
}

#[derive(Debug, thiserror::Error)]
pub enum GitDeliveryPortError {
    #[error("Git delivery adapter is temporarily unavailable: {0}")]
    Unavailable(String),
    #[error("Git delivery adapter rejected the durable intent: {0}")]
    InvalidIntent(String),
}

#[async_trait]
pub trait TeamGitDeliveryPort: Send + Sync {
    async fn integrate(&self, intent: &GitIntegrationIntent) -> Result<GitDeliveryPortOutcome, GitDeliveryPortError>;

    async fn reconcile(&self, intent: &GitIntegrationIntent)
    -> Result<GitDeliveryReconciliation, GitDeliveryPortError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamConversationBindingLookup {
    pub conversation_id: String,
    pub user_id: String,
    pub team_id: Option<String>,
    pub slot_id: Option<String>,
    pub role: Option<String>,
}

#[async_trait]
pub trait TeamConversationLookupPort: Send + Sync {
    async fn lookup_team_binding_by_conversation(
        &self,
        conversation_id: &str,
    ) -> Result<Option<TeamConversationBindingLookup>, TeamError>;
}

#[async_trait]
pub trait TeamAssistantCatalogPort: Send + Sync {
    async fn list_team_selectable_assistants(&self) -> Result<Vec<TeamAssistantCatalogEntry>, TeamError>;

    async fn resolve_team_selectable_assistant(
        &self,
        assistant_id: &str,
    ) -> Result<Option<TeamAssistantCatalogEntry>, TeamError> {
        Ok(self
            .list_team_selectable_assistants()
            .await?
            .into_iter()
            .find(|assistant| assistant.assistant_id == assistant_id))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamAssistantCatalogEntry {
    pub assistant_id: String,
    pub name: String,
    pub backend: String,
    pub description: String,
    pub skills: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentTurnSource {
    Mailbox {
        unread_message_ids: Vec<String>,
        unread_count: usize,
    },
}

#[derive(Clone)]
pub struct AgentTurnRequest {
    pub team_run_id: Option<String>,
    pub team_id: String,
    pub slot_id: String,
    pub role: TeamRunTargetRole,
    pub conversation_id: String,
    pub user_id: String,
    pub content: String,
    pub files: Vec<String>,
    pub source: AgentTurnSource,
    /// Fires exactly once after the Agent task accepts the first prompt. A
    /// caller must not interpret task preparation or task construction as
    /// delivery.
    pub on_started: Option<AgentTurnStartedCallback>,
}

impl fmt::Debug for AgentTurnRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentTurnRequest")
            .field("team_run_id", &self.team_run_id)
            .field("team_id", &self.team_id)
            .field("slot_id", &self.slot_id)
            .field("role", &self.role)
            .field("conversation_id", &self.conversation_id)
            .field("user_id", &self.user_id)
            .field("files", &self.files)
            .field("source", &self.source)
            .field("has_on_started", &self.on_started.is_some())
            .finish_non_exhaustive()
    }
}

pub type AgentTurnStartedCallback =
    Arc<dyn Fn(AgentTurnStarted) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTurnStarted {
    pub team_run_id: Option<String>,
    pub slot_id: String,
    pub role: TeamRunTargetRole,
    pub conversation_id: String,
    pub turn_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentTurnStatus {
    Completed,
    Failed,
    Skipped,
}

impl AgentTurnStatus {
    pub fn is_success(self) -> bool {
        matches!(self, Self::Completed)
    }
}

#[derive(Debug, Clone)]
pub struct AgentTurnOutcome {
    pub conversation_id: String,
    pub turn_id: String,
    pub status: AgentTurnStatus,
    pub runtime: Option<ConversationRuntimeSummary>,
}

#[derive(Debug, thiserror::Error)]
pub enum AgentTurnExecutionError {
    #[error("agent turn skipped: {reason}")]
    Skipped { reason: String },
    #[error("agent turn failed: {reason}")]
    Failed { reason: String },
}

#[async_trait]
pub trait AgentTurnExecutionPort: Send + Sync {
    async fn run_agent_turn(&self, request: AgentTurnRequest) -> Result<AgentTurnOutcome, AgentTurnExecutionError>;
}

#[async_trait]
pub trait AgentTurnCancellationPort: Send + Sync {
    async fn cancel_agent_turn(
        &self,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
    ) -> Result<(), AgentTurnExecutionError>;
}
