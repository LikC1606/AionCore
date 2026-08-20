use aionui_common::TimestampMs;
use serde::{Deserialize, Serialize};

/// Durable snapshot for one Team Mode WorkItem aggregate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct TeamWorkItemRow {
    pub id: String,
    pub team_id: String,
    pub parent_work_item_id: Option<String>,
    pub subject: String,
    pub description: Option<String>,
    pub controller_member_id: String,
    pub assignee_member_id: String,
    pub reviewer_member_id: String,
    pub integrator_member_id: Option<String>,
    pub delivery_requirement: String,
    pub git_repository_id: Option<String>,
    pub git_base_commit: Option<String>,
    pub git_branch_ref: Option<String>,
    pub state: String,
    pub current_submission_json: Option<String>,
    pub accepted_delivery_json: Option<String>,
    pub revision: i64,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}

/// Durable state for one immutable Git delivery identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct TeamGitDeliveryRow {
    pub id: String,
    pub team_id: String,
    pub work_item_id: String,
    pub producer_member_id: String,
    pub repository_id: String,
    pub content_revision: i64,
    pub base_commit: String,
    pub branch_ref: String,
    pub head_commit: String,
    pub state: String,
    pub revision: i64,
    pub merged_commit: Option<String>,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}

/// Durable intent and outcome for one exact Git integration attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct TeamGitIntegrationAttemptRow {
    pub attempt_id: String,
    pub team_id: String,
    pub work_item_id: String,
    pub delivery_id: String,
    pub repository_id: String,
    pub base_commit: String,
    pub source_ref: String,
    pub source_head: String,
    pub target_ref: String,
    pub target_head: String,
    pub state: String,
    pub merged_commit: Option<String>,
    pub observed_target_head: Option<String>,
    pub recovery_reason: Option<String>,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}

/// Ordered Team Mode command event and idempotency receipt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, sqlx::FromRow)]
pub struct TeamWorkEventRow {
    pub sequence: i64,
    pub event_id: String,
    pub team_id: String,
    pub work_item_id: String,
    pub delivery_id: Option<String>,
    pub actor_member_id: String,
    pub command_name: String,
    pub idempotency_key: String,
    pub request_fingerprint: String,
    pub result_json: String,
    pub expected_work_item_revision: Option<i64>,
    pub expected_delivery_revision: Option<i64>,
    pub work_item_revision: i64,
    pub delivery_revision: Option<i64>,
    pub created_at: TimestampMs,
}
