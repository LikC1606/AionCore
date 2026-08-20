use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamWorkDeliveryRequirement {
    None,
    Git,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegateTeamWorkRequest {
    pub idempotency_key: String,
    #[serde(default)]
    pub parent_work_item_id: Option<String>,
    pub subject: String,
    #[serde(default)]
    pub description: Option<String>,
    pub assignee_member_id: String,
    pub delivery_requirement: TeamWorkDeliveryRequirement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamWorkReviewDecision {
    Accept,
    RequestChanges,
    Reject,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewTeamWorkRequest {
    pub idempotency_key: String,
    pub expected_work_revision: u64,
    #[serde(default)]
    pub expected_delivery_revision: Option<u64>,
    pub decision: TeamWorkReviewDecision,
    #[serde(default)]
    pub feedback: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelTeamWorkRequest {
    pub idempotency_key: String,
    pub expected_work_revision: u64,
    #[serde(default)]
    pub expected_delivery_revision: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IntegrateTeamWorkRequest {
    pub idempotency_key: String,
    pub expected_work_revision: u64,
    pub expected_delivery_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IntegrateTeamWorkResponse {
    pub attempt_id: String,
    pub work_item_id: String,
    pub delivery_id: String,
    pub resolution: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merged_commit: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_target_head: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recovery_reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamWorkCommandDeliveryResponse {
    pub delivery_id: String,
    pub content_revision: u64,
    pub head_commit: String,
    pub state: String,
    pub revision: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub merged_commit: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamWorkCommandResponse {
    pub event_sequence: i64,
    pub event_id: String,
    pub work_item_id: String,
    pub state: String,
    pub revision: u64,
    pub replayed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delivery: Option<TeamWorkCommandDeliveryResponse>,
}

/// Invalidation-only event emitted after a canonical Team command commits.
/// Consumers must refetch the SQLite-backed projection instead of treating
/// this payload as aggregate state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TeamWorkChangedPayload {
    pub team_id: String,
    pub work_item_id: String,
    pub event_sequence: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delegate_request_has_one_small_transport_contract() {
        let request: DelegateTeamWorkRequest = serde_json::from_value(serde_json::json!({
            "idempotency_key": "delegate-1",
            "subject": "Audit the implementation",
            "assignee_member_id": "worker-1",
            "delivery_requirement": "git"
        }))
        .unwrap();
        assert_eq!(request.delivery_requirement, TeamWorkDeliveryRequirement::Git);
        assert!(request.parent_work_item_id.is_none());
        assert!(request.description.is_none());
    }

    #[test]
    fn delegate_request_rejects_client_supplied_git_assignment() {
        let request = serde_json::from_value::<DelegateTeamWorkRequest>(serde_json::json!({
            "idempotency_key": "delegate-1",
            "subject": "Audit",
            "assignee_member_id": "worker-1",
            "delivery_requirement": "git",
            "git_assignment": {
                "repository_id": "/private/local/repository",
                "base_commit": "base-1",
                "branch_ref": "refs/heads/ds/worker-1"
            }
        }));

        assert!(request.is_err());
    }

    #[test]
    fn review_decision_uses_the_small_public_contract() {
        let request: ReviewTeamWorkRequest = serde_json::from_value(serde_json::json!({
            "idempotency_key": "review-1",
            "expected_work_revision": 3,
            "expected_delivery_revision": 0,
            "decision": "request_changes",
            "feedback": "Tighten the evidence table."
        }))
        .unwrap();
        assert_eq!(request.decision, TeamWorkReviewDecision::RequestChanges);
        assert_eq!(request.feedback.as_deref(), Some("Tighten the evidence table."));

        let unknown = serde_json::from_value::<ReviewTeamWorkRequest>(serde_json::json!({
            "idempotency_key": "review-1",
            "expected_work_revision": 3,
            "decision": "accept",
            "actor_member_id": "forged-worker"
        }));
        assert!(unknown.is_err());
    }

    #[test]
    fn integration_request_does_not_accept_client_git_coordinates() {
        let request: IntegrateTeamWorkRequest = serde_json::from_value(serde_json::json!({
            "idempotency_key": "integrate-1",
            "expected_work_revision": 5,
            "expected_delivery_revision": 1
        }))
        .unwrap();
        assert_eq!(request.expected_delivery_revision, 1);

        let forged = serde_json::from_value::<IntegrateTeamWorkRequest>(serde_json::json!({
            "idempotency_key": "integrate-1",
            "expected_work_revision": 5,
            "expected_delivery_revision": 1,
            "target_ref": "refs/heads/main",
            "target_head": "attacker-selected"
        }));
        assert!(forged.is_err());
    }
}
