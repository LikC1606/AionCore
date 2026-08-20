use aionui_common::TimestampMs;

use crate::error::DbError;
use crate::models::{TeamGitDeliveryRow, TeamGitIntegrationAttemptRow, TeamWorkEventRow, TeamWorkItemRow};

/// Mailbox idempotency scope reserved for notifications committed with a
/// Team WorkItem event.
pub const TEAM_WORK_EVENT_NOTIFICATION_SCOPE: &str = "team_work_event";

/// Durable resolution written beside the delivery and WorkItem transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamGitIntegrationResolution {
    Merged {
        merged_commit: String,
        observed_target_head: String,
    },
    Conflicted {
        observed_target_head: String,
    },
    Retryable {
        recovery_reason: String,
        observed_target_head: Option<String>,
    },
}

/// Attempt mutation coupled to a Git delivery compare-and-swap.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamGitIntegrationMutation {
    Begin(Box<TeamGitIntegrationAttemptRow>),
    Resolve {
        attempt_id: String,
        resolution: TeamGitIntegrationResolution,
        updated_at: TimestampMs,
    },
}

/// A WorkItem snapshot mutation committed with one command receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamWorkItemMutation {
    Insert(TeamWorkItemRow),
    CompareAndSwap {
        expected_revision: i64,
        row: TeamWorkItemRow,
    },
}

impl TeamWorkItemMutation {
    pub fn row(&self) -> &TeamWorkItemRow {
        match self {
            Self::Insert(row) | Self::CompareAndSwap { row, .. } => row,
        }
    }
}

/// A Git delivery state mutation committed with one command receipt.
///
/// `CompareAndSwap` may only change lifecycle state, revision, merged commit,
/// and update time. The repository rejects changes to delivery identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamGitDeliveryMutation {
    Insert(TeamGitDeliveryRow),
    CompareAndSwap {
        expected_revision: i64,
        row: TeamGitDeliveryRow,
    },
    CompareAndSwapWithIntegration {
        expected_revision: i64,
        row: TeamGitDeliveryRow,
        integration: TeamGitIntegrationMutation,
    },
}

impl TeamGitDeliveryMutation {
    pub fn row(&self) -> &TeamGitDeliveryRow {
        match self {
            Self::Insert(row) | Self::CompareAndSwap { row, .. } | Self::CompareAndSwapWithIntegration { row, .. } => {
                row
            }
        }
    }

    pub fn expected_revision(&self) -> Option<i64> {
        match self {
            Self::Insert(_) => None,
            Self::CompareAndSwap { expected_revision, .. }
            | Self::CompareAndSwapWithIntegration { expected_revision, .. } => Some(*expected_revision),
        }
    }
}

/// Input for the ordered event that also acts as a command receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTeamWorkEvent {
    pub event_id: String,
    pub team_id: String,
    pub work_item_id: String,
    pub delivery_id: Option<String>,
    pub actor_member_id: String,
    pub command_name: String,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    pub result_json: String,
    /// WorkItem revision observed before this command, if the command depends
    /// on an existing WorkItem snapshot.
    pub expected_work_item_revision: Option<i64>,
    /// Git delivery revision observed before this command, if the command
    /// depends on an existing delivery snapshot.
    pub expected_delivery_revision: Option<i64>,
    pub work_item_revision: i64,
    pub delivery_revision: Option<i64>,
    pub created_at: TimestampMs,
}

/// An unread inter-agent notification committed atomically with a Team
/// WorkItem event.
///
/// `team_id`, mailbox type, and read state are deliberately absent: the
/// repository derives the Team from the event and persists every command
/// notification as an unread `message`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewTeamMailboxNotification {
    pub message_id: String,
    pub to_agent_id: String,
    pub from_agent_id: String,
    pub content: String,
    pub summary: Option<String>,
    pub files_json: Option<String>,
    pub idempotency_scope: String,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    pub created_at: TimestampMs,
}

/// Optimistic guard for the authenticated Team roster snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamRosterGuard {
    pub expected_user_id: String,
    pub expected_agents_json: String,
    pub expected_lead_agent_id: Option<String>,
}

/// Read-only WorkItem revision precondition for a related aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamWorkItemRevisionGuard {
    pub work_item_id: String,
    pub expected_revision: i64,
}

/// All durable changes produced by one Team Mode command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitTeamCommandParams {
    pub roster_guard: Option<TeamRosterGuard>,
    pub work_item_guards: Vec<TeamWorkItemRevisionGuard>,
    pub work_item: Option<TeamWorkItemMutation>,
    pub delivery: Option<TeamGitDeliveryMutation>,
    pub notifications: Vec<NewTeamMailboxNotification>,
    pub event: NewTeamWorkEvent,
}

/// Outcome of an atomic Team Mode command commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamCommandCommitResult {
    Applied(TeamWorkEventRow),
    Replayed(TeamWorkEventRow),
    TeamRosterConflict {
        team_id: String,
    },
    WorkItemGuardConflict {
        work_item_id: String,
        expected_revision: i64,
        actual_revision: Option<i64>,
    },
    WorkItemRevisionConflict {
        expected_revision: i64,
        actual_revision: Option<i64>,
    },
    DeliveryRevisionConflict {
        expected_revision: i64,
        actual_revision: Option<i64>,
    },
    IdempotencyConflict {
        existing_request_fingerprint: String,
    },
}

/// Roster-authorized lookup result for an existing command receipt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TeamCommandReceiptLookupResult {
    Found(Box<TeamWorkEventRow>),
    NotFound,
    TeamRosterConflict { team_id: String },
}

/// Persistence boundary for the Team Mode v2 command model.
#[async_trait::async_trait]
pub trait ITeamModeRepository: Send + Sync {
    async fn list_work_items(&self, team_id: &str) -> Result<Vec<TeamWorkItemRow>, DbError>;

    async fn get_work_item(&self, team_id: &str, work_item_id: &str) -> Result<Option<TeamWorkItemRow>, DbError>;

    async fn list_git_deliveries(
        &self,
        team_id: &str,
        work_item_id: Option<&str>,
    ) -> Result<Vec<TeamGitDeliveryRow>, DbError>;

    async fn get_git_delivery(&self, team_id: &str, delivery_id: &str) -> Result<Option<TeamGitDeliveryRow>, DbError>;

    async fn get_git_integration_attempt(
        &self,
        team_id: &str,
        attempt_id: &str,
    ) -> Result<Option<TeamGitIntegrationAttemptRow>, DbError>;

    async fn get_pending_git_integration_attempt(
        &self,
        team_id: &str,
        delivery_id: &str,
    ) -> Result<Option<TeamGitIntegrationAttemptRow>, DbError>;

    async fn list_pending_git_integration_attempts(
        &self,
        team_id: &str,
    ) -> Result<Vec<TeamGitIntegrationAttemptRow>, DbError>;

    async fn find_git_delivery(
        &self,
        team_id: &str,
        work_item_id: &str,
        content_revision: i64,
    ) -> Result<Option<TeamGitDeliveryRow>, DbError>;

    async fn find_command_receipt(
        &self,
        team_id: &str,
        actor_member_id: &str,
        idempotency_key: &str,
    ) -> Result<Option<TeamWorkEventRow>, DbError>;

    /// Looks up a receipt only while the authenticated roster snapshot still
    /// matches. The roster check and receipt read share one transaction.
    async fn find_command_receipt_guarded(
        &self,
        team_id: &str,
        actor_member_id: &str,
        idempotency_key: &str,
        roster_guard: &TeamRosterGuard,
    ) -> Result<TeamCommandReceiptLookupResult, DbError>;

    async fn list_work_events(&self, team_id: &str, work_item_id: &str) -> Result<Vec<TeamWorkEventRow>, DbError>;

    /// Atomically applies aggregate snapshots, appends their event receipt,
    /// and inserts all unread mailbox notifications.
    ///
    /// The receipt scope is `(team_id, actor_member_id, idempotency_key)`.
    /// Reusing that scope with the same fingerprint replays the stored result;
    /// reusing it with a different fingerprint is an idempotency conflict.
    /// The roster guard is authorization and therefore precedes replay. Related
    /// WorkItem guards are concurrency preconditions and follow replay.
    async fn commit_command(&self, params: &CommitTeamCommandParams) -> Result<TeamCommandCommitResult, DbError>;
}
