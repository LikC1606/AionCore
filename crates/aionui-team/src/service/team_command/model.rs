use aionui_db::DbError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::actor::RosterIntegrityError;
use super::snapshot::SnapshotError;
use crate::kernel::{
    DeliveryRequirement, GitDeliveryState, GitDeliveryTransitionError, GitWorkAssignment, IntegrationRecoveryReason,
    RelationPolicyError, WorkItemPolicyError, WorkItemState, WorkItemTransitionError,
};

pub(super) const RESULT_SCHEMA_VERSION: u8 = 1;

/// Authenticated runtime identity. The caller conversation comes from a
/// trusted runtime adapter and is deliberately absent from command payloads.
#[derive(Debug, Clone)]
pub(crate) struct TeamCommandPrincipal {
    pub(super) authenticated_user_id: String,
    pub(super) team_id: String,
    pub(super) trusted_caller_conversation_id: String,
}

impl TeamCommandPrincipal {
    pub(crate) fn from_runtime(
        authenticated_user_id: impl Into<String>,
        team_id: impl Into<String>,
        trusted_caller_conversation_id: impl Into<String>,
    ) -> Self {
        Self {
            authenticated_user_id: authenticated_user_id.into(),
            team_id: team_id.into(),
            trusted_caller_conversation_id: trusted_caller_conversation_id.into(),
        }
    }

    pub(crate) fn authenticated_user_id(&self) -> &str {
        &self.authenticated_user_id
    }

    pub(crate) fn team_id(&self) -> &str {
        &self.team_id
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct MergedIntegrationEvidence {
    pub(crate) attempt_id: String,
    pub(crate) merged_commit: String,
    pub(crate) observed_target_head: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ConflictedIntegrationEvidence {
    pub(crate) attempt_id: String,
    pub(crate) observed_target_head: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct RetryableIntegrationEvidence {
    pub(crate) attempt_id: String,
    pub(crate) reason: IntegrationRecoveryReason,
    pub(crate) observed_target_head: Option<String>,
}

/// Typed Team Mode write surface. Transport-specific fields stay in adapters.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub(crate) enum TeamCommand {
    CreateWorkItem {
        parent_work_item_id: Option<String>,
        subject: String,
        description: Option<String>,
        assignee_member_id: String,
        delivery_requirement: DeliveryRequirement,
        git_assignment: Option<GitWorkAssignment>,
    },
    Queue {
        work_item_id: String,
        expected_work_revision: u64,
        /// Trusted path prepared by `TeamWorkCoordinator`. Transport payloads
        /// never expose this field for callers to choose.
        prepared_workspace: Option<String>,
    },
    Start {
        work_item_id: String,
        expected_work_revision: u64,
    },
    Block {
        work_item_id: String,
        expected_work_revision: u64,
        context: String,
    },
    Resume {
        work_item_id: String,
        expected_work_revision: u64,
    },
    SubmitInline {
        work_item_id: String,
        expected_work_revision: u64,
        evidence: String,
    },
    SubmitGit {
        work_item_id: String,
        expected_work_revision: u64,
        content_revision: u64,
        head_commit: String,
        evidence: String,
    },
    BeginReview {
        work_item_id: String,
        expected_work_revision: u64,
        expected_delivery_revision: Option<u64>,
    },
    RequestChanges {
        work_item_id: String,
        expected_work_revision: u64,
        expected_delivery_revision: Option<u64>,
        feedback: String,
    },
    Accept {
        work_item_id: String,
        expected_work_revision: u64,
        expected_delivery_revision: Option<u64>,
    },
    Reject {
        work_item_id: String,
        expected_work_revision: u64,
        expected_delivery_revision: Option<u64>,
    },
    Cancel {
        work_item_id: String,
        expected_work_revision: u64,
        expected_delivery_revision: Option<u64>,
    },
    CompleteWithoutDelivery {
        work_item_id: String,
        expected_work_revision: u64,
    },
    BeginIntegration {
        work_item_id: String,
        expected_work_revision: u64,
        expected_delivery_revision: u64,
        target_ref: String,
        target_head: String,
    },
    ResolveIntegrationRetryable {
        work_item_id: String,
        expected_work_revision: u64,
        expected_delivery_revision: u64,
        evidence: RetryableIntegrationEvidence,
    },
    ResolveIntegrationMerged {
        work_item_id: String,
        expected_work_revision: u64,
        expected_delivery_revision: u64,
        evidence: MergedIntegrationEvidence,
    },
    ResolveIntegrationConflict {
        work_item_id: String,
        expected_work_revision: u64,
        expected_delivery_revision: u64,
        evidence: ConflictedIntegrationEvidence,
    },
}

impl TeamCommand {
    pub(super) fn name(&self) -> &'static str {
        match self {
            Self::CreateWorkItem { .. } => "create_work_item",
            Self::Queue { .. } => "queue",
            Self::Start { .. } => "start",
            Self::Block { .. } => "block",
            Self::Resume { .. } => "resume",
            Self::SubmitInline { .. } => "submit_inline",
            Self::SubmitGit { .. } => "submit_git",
            Self::BeginReview { .. } => "begin_review",
            Self::RequestChanges { .. } => "request_changes",
            Self::Accept { .. } => "accept",
            Self::Reject { .. } => "reject",
            Self::Cancel { .. } => "cancel",
            Self::CompleteWithoutDelivery { .. } => "complete_without_delivery",
            Self::BeginIntegration { .. } => "begin_integration",
            Self::ResolveIntegrationRetryable { .. } => "resolve_integration_retryable",
            Self::ResolveIntegrationMerged { .. } => "resolve_integration_merged",
            Self::ResolveIntegrationConflict { .. } => "resolve_integration_conflict",
        }
    }

    pub(super) fn work_item_id(&self) -> Option<&str> {
        match self {
            Self::CreateWorkItem { .. } => None,
            Self::Queue { work_item_id, .. }
            | Self::Start { work_item_id, .. }
            | Self::Block { work_item_id, .. }
            | Self::Resume { work_item_id, .. }
            | Self::SubmitInline { work_item_id, .. }
            | Self::SubmitGit { work_item_id, .. }
            | Self::BeginReview { work_item_id, .. }
            | Self::RequestChanges { work_item_id, .. }
            | Self::Accept { work_item_id, .. }
            | Self::Reject { work_item_id, .. }
            | Self::Cancel { work_item_id, .. }
            | Self::CompleteWithoutDelivery { work_item_id, .. }
            | Self::BeginIntegration { work_item_id, .. }
            | Self::ResolveIntegrationRetryable { work_item_id, .. }
            | Self::ResolveIntegrationMerged { work_item_id, .. }
            | Self::ResolveIntegrationConflict { work_item_id, .. } => Some(work_item_id),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TeamCommandDeliveryResult {
    pub delivery_id: String,
    pub content_revision: u64,
    pub head_commit: String,
    pub state: GitDeliveryState,
    pub revision: u64,
    pub merged_commit: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub integration_attempt_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TeamCommandResult {
    pub schema_version: u8,
    pub event_id: String,
    pub team_id: String,
    pub work_item_id: String,
    pub work_item_state: WorkItemState,
    pub work_item_revision: u64,
    pub delivery: Option<TeamCommandDeliveryResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TeamCommandReceipt {
    pub event_sequence: i64,
    pub replayed: bool,
    pub result: TeamCommandResult,
}

#[derive(Debug, Error)]
pub(crate) enum TeamCommandError {
    #[error("Team not found: {0}")]
    TeamNotFound(String),
    #[error("authenticated user does not own this Team")]
    ForbiddenTeam,
    #[error("trusted caller conversation is not an active Team member")]
    CallerNotMember,
    #[error("Team member not found: {0}")]
    MemberNotFound(String),
    #[error("WorkItem not found: {0}")]
    WorkItemNotFound(String),
    #[error("invalid parent WorkItem: {0}")]
    InvalidParentWorkItem(String),
    #[error("invalid Team command: {0}")]
    InvalidCommand(String),
    #[error("Git command requires expected_delivery_revision")]
    ExpectedDeliveryRevisionRequired,
    #[error("non-Git command cannot include expected_delivery_revision")]
    UnexpectedDeliveryRevision,
    #[error("Git delivery is required")]
    GitDeliveryRequired,
    #[error("Git content_revision {0} has already been used for this WorkItem")]
    GitContentRevisionAlreadyUsed(u64),
    #[error("idempotency key was already used for a different command")]
    IdempotencyConflict,
    #[error("Team roster changed while the command was being authorized")]
    RosterChanged,
    #[error("Team Mode aggregate changed repeatedly while it was being read; retry the command")]
    ConcurrentSnapshotChange,
    #[error("{aggregate} revision conflict: expected {expected_revision}, actual {actual_revision:?}")]
    RevisionConflict {
        aggregate: &'static str,
        expected_revision: i64,
        actual_revision: Option<i64>,
    },
    #[error("Team Mode aggregate is corrupt: {0}")]
    CorruptAggregate(String),
    #[error("Team Mode receipt is corrupt: {0}")]
    CorruptReceipt(String),
    #[error(transparent)]
    Roster(#[from] RosterIntegrityError),
    #[error(transparent)]
    Snapshot(#[from] SnapshotError),
    #[error(transparent)]
    RelationPolicy(#[from] RelationPolicyError),
    #[error(transparent)]
    WorkItemPolicy(#[from] WorkItemPolicyError),
    #[error(transparent)]
    WorkItemTransition(#[from] WorkItemTransitionError),
    #[error(transparent)]
    GitDeliveryTransition(#[from] GitDeliveryTransitionError),
    #[error(transparent)]
    Database(#[from] DbError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
