use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, Json, Path, State};

use aionui_api_types::{
    ApiResponse, CancelTeamWorkRequest, DelegateTeamWorkRequest, IntegrateTeamWorkRequest, IntegrateTeamWorkResponse,
    ReviewTeamWorkRequest, TeamWorkCommandDeliveryResponse, TeamWorkCommandResponse, TeamWorkDeliveryRequirement,
    TeamWorkReviewDecision,
};
use aionui_auth::CurrentUser;
use aionui_common::ApiError;

use super::TeamRouterState;
use crate::kernel::DeliveryRequirement;
use crate::service::team_command::{TeamCommand, TeamCommandError, TeamCommandReceipt};
use crate::service::{
    BeginGitIntegrationRequest, DelegateWorkItem, TeamDeliveryError, TeamIntegrationResolution,
    TeamWorkCoordinatorError,
};

const PUBLIC_IDEMPOTENCY_KEY_MAX_LEN: usize = 220;
const REVIEW_FEEDBACK_MAX_CHARS: usize = 4000;

#[derive(serde::Deserialize)]
pub(super) struct WorkItemPath {
    pub(super) id: String,
    pub(super) work_item_id: String,
}

pub(super) async fn delegate_work(
    State(state): State<TeamRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(team_id): Path<String>,
    body: Result<Json<DelegateTeamWorkRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<TeamWorkCommandResponse>>, ApiError> {
    let Json(request) = body.map_err(ApiError::from)?;
    validate_public_key(&request.idempotency_key)?;
    let delegated = state
        .work_coordinator
        .delegate_as_owner(
            &user.id,
            &team_id,
            &request.idempotency_key,
            DelegateWorkItem {
                parent_work_item_id: request.parent_work_item_id,
                subject: request.subject,
                description: request.description,
                assignee_member_id: request.assignee_member_id,
                delivery_requirement: match request.delivery_requirement {
                    TeamWorkDeliveryRequirement::None => DeliveryRequirement::None,
                    TeamWorkDeliveryRequirement::Git => DeliveryRequirement::Git,
                },
            },
        )
        .await
        .map_err(coordinator_error_to_api)?;
    state.service.wake_work_notifications(&user.id, &team_id);
    Ok(Json(ApiResponse::ok(command_response(delegated.command))))
}

pub(super) async fn review_work(
    State(state): State<TeamRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(path): Path<WorkItemPath>,
    body: Result<Json<ReviewTeamWorkRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<TeamWorkCommandResponse>>, ApiError> {
    let Json(request) = body.map_err(ApiError::from)?;
    validate_public_key(&request.idempotency_key)?;
    let feedback = validate_review_feedback(request.decision, request.feedback.as_deref())?;
    let requests_changes = request.decision == TeamWorkReviewDecision::RequestChanges;
    let mut review_revision = request.expected_work_revision;
    let mut recovered_inline_accept = false;
    let begin = state
        .command_service
        .execute_as_owner(
            &user.id,
            &path.id,
            &derived_key(&request.idempotency_key, "begin"),
            TeamCommand::BeginReview {
                work_item_id: path.work_item_id.clone(),
                expected_work_revision: request.expected_work_revision,
                expected_delivery_revision: request.expected_delivery_revision,
            },
        )
        .await;
    match begin {
        Err(begin_error) => {
            // Review is intentionally a small saga because each step has its own
            // CAS/event receipt. If the process stopped after BeginReview (or
            // after accepting an inline result), resume only from the exact
            // revision the caller supplied. This keeps recovery bounded and does
            // not turn a stale request into an authorization bypass.
            let snapshot = match state
                .query_service
                .get_work_item_snapshot(&user.id, &path.id, &path.work_item_id)
                .await
            {
                Ok(snapshot) => snapshot,
                Err(_) => return Err(command_error_to_api(begin_error)),
            };
            let observed_delivery_revision = snapshot
                .work_item
                .current_submission
                .as_ref()
                .and_then(|submission| submission.delivery_id.as_ref())
                .and_then(|delivery_id| {
                    snapshot
                        .deliveries
                        .iter()
                        .find(|delivery| &delivery.id == delivery_id)
                        .map(|delivery| delivery.revision)
                });
            if snapshot.work_item.revision != request.expected_work_revision
                || observed_delivery_revision != request.expected_delivery_revision
            {
                return Err(command_error_to_api(begin_error));
            }
            match (
                snapshot.work_item.state.as_str(),
                request.decision,
                snapshot.work_item.delivery_requirement.as_str(),
            ) {
                ("reviewing", _, _) => {}
                ("accepted", TeamWorkReviewDecision::Accept, "none") => {
                    recovered_inline_accept = true;
                }
                _ => return Err(command_error_to_api(begin_error)),
            }
        }
        Ok(begin) => {
            review_revision = begin.result.work_item_revision;
        }
    }

    if recovered_inline_accept {
        let completed = state
            .command_service
            .execute_as_owner(
                &user.id,
                &path.id,
                &derived_key(&request.idempotency_key, "complete"),
                TeamCommand::CompleteWithoutDelivery {
                    work_item_id: path.work_item_id,
                    expected_work_revision: review_revision,
                },
            )
            .await
            .map_err(command_error_to_api)?;
        return Ok(Json(ApiResponse::ok(command_response(completed))));
    }

    let decision_command = match request.decision {
        TeamWorkReviewDecision::Accept => TeamCommand::Accept {
            work_item_id: path.work_item_id.clone(),
            expected_work_revision: review_revision,
            expected_delivery_revision: request.expected_delivery_revision,
        },
        TeamWorkReviewDecision::RequestChanges => TeamCommand::RequestChanges {
            work_item_id: path.work_item_id.clone(),
            expected_work_revision: review_revision,
            expected_delivery_revision: request.expected_delivery_revision,
            feedback: feedback.expect("request_changes feedback was validated before the command"),
        },
        TeamWorkReviewDecision::Reject => TeamCommand::Reject {
            work_item_id: path.work_item_id.clone(),
            expected_work_revision: review_revision,
            expected_delivery_revision: request.expected_delivery_revision,
        },
    };
    let decided = state
        .command_service
        .execute_as_owner(
            &user.id,
            &path.id,
            &derived_key(&request.idempotency_key, "decision"),
            decision_command,
        )
        .await
        .map_err(command_error_to_api)?;

    if requests_changes || (request.decision == TeamWorkReviewDecision::Accept && decided.result.delivery.is_some()) {
        state.service.wake_work_notifications(&user.id, &path.id);
    }

    if request.decision == TeamWorkReviewDecision::Accept && decided.result.delivery.is_none() {
        let completed = state
            .command_service
            .execute_as_owner(
                &user.id,
                &path.id,
                &derived_key(&request.idempotency_key, "complete"),
                TeamCommand::CompleteWithoutDelivery {
                    work_item_id: path.work_item_id,
                    expected_work_revision: decided.result.work_item_revision,
                },
            )
            .await
            .map_err(command_error_to_api)?;
        return Ok(Json(ApiResponse::ok(command_response(completed))));
    }

    Ok(Json(ApiResponse::ok(command_response(decided))))
}

pub(super) async fn cancel_work(
    State(state): State<TeamRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(path): Path<WorkItemPath>,
    body: Result<Json<CancelTeamWorkRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<TeamWorkCommandResponse>>, ApiError> {
    let Json(request) = body.map_err(ApiError::from)?;
    validate_public_key(&request.idempotency_key)?;
    let receipt = state
        .command_service
        .execute_as_owner(
            &user.id,
            &path.id,
            &request.idempotency_key,
            TeamCommand::Cancel {
                work_item_id: path.work_item_id.clone(),
                expected_work_revision: request.expected_work_revision,
                expected_delivery_revision: request.expected_delivery_revision,
            },
        )
        .await
        .map_err(command_error_to_api)?;
    state.service.wake_work_notifications(&user.id, &path.id);
    Ok(Json(ApiResponse::ok(command_response(receipt))))
}

pub(super) async fn integrate_work(
    State(state): State<TeamRouterState>,
    Extension(user): Extension<CurrentUser>,
    Path(path): Path<WorkItemPath>,
    body: Result<Json<IntegrateTeamWorkRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<IntegrateTeamWorkResponse>>, ApiError> {
    let Json(request) = body.map_err(ApiError::from)?;
    validate_public_key(&request.idempotency_key)?;
    let receipt = state
        .delivery_service
        .integrate_as_owner(
            &user.id,
            &path.id,
            &request.idempotency_key,
            BeginGitIntegrationRequest {
                work_item_id: path.work_item_id,
                expected_work_revision: request.expected_work_revision,
                expected_delivery_revision: request.expected_delivery_revision,
            },
        )
        .await
        .map_err(delivery_error_to_api)?;
    if receipt.resolution == TeamIntegrationResolution::Conflicted {
        state.service.wake_work_notifications(&user.id, &path.id);
    }
    let resolution = match receipt.resolution {
        TeamIntegrationResolution::Merged => "merged",
        TeamIntegrationResolution::Conflicted => "conflicted",
        TeamIntegrationResolution::Retryable => "retryable",
    };
    Ok(Json(ApiResponse::ok(IntegrateTeamWorkResponse {
        attempt_id: receipt.attempt_id,
        work_item_id: receipt.work_item_id,
        delivery_id: receipt.delivery_id,
        resolution: resolution.to_owned(),
        merged_commit: receipt.merged_commit,
        observed_target_head: receipt.observed_target_head,
        recovery_reason: receipt.recovery_reason.map(|reason| reason.as_str().to_owned()),
    })))
}

fn validate_public_key(key: &str) -> Result<(), ApiError> {
    if key.trim().is_empty() || key.len() > PUBLIC_IDEMPOTENCY_KEY_MAX_LEN {
        return Err(ApiError::BadRequest(format!(
            "idempotency_key must contain 1..={PUBLIC_IDEMPOTENCY_KEY_MAX_LEN} bytes"
        )));
    }
    Ok(())
}

fn validate_review_feedback(
    decision: TeamWorkReviewDecision,
    feedback: Option<&str>,
) -> Result<Option<String>, ApiError> {
    let feedback = feedback.unwrap_or_default().trim();
    match decision {
        TeamWorkReviewDecision::RequestChanges => {
            let count = feedback.chars().count();
            if !(1..=REVIEW_FEEDBACK_MAX_CHARS).contains(&count) {
                return Err(ApiError::BadRequest(format!(
                    "feedback must contain 1..={REVIEW_FEEDBACK_MAX_CHARS} characters for request_changes"
                )));
            }
            Ok(Some(feedback.to_owned()))
        }
        TeamWorkReviewDecision::Accept | TeamWorkReviewDecision::Reject if !feedback.is_empty() => Err(
            ApiError::BadRequest("feedback is only allowed for request_changes".to_owned()),
        ),
        TeamWorkReviewDecision::Accept | TeamWorkReviewDecision::Reject => Ok(None),
    }
}

fn derived_key(key: &str, step: &str) -> String {
    format!("{key}:{step}")
}

fn command_response(receipt: TeamCommandReceipt) -> TeamWorkCommandResponse {
    TeamWorkCommandResponse {
        event_sequence: receipt.event_sequence,
        event_id: receipt.result.event_id,
        work_item_id: receipt.result.work_item_id,
        state: receipt.result.work_item_state.as_str().to_owned(),
        revision: receipt.result.work_item_revision,
        replayed: receipt.replayed,
        delivery: receipt.result.delivery.map(|delivery| TeamWorkCommandDeliveryResponse {
            delivery_id: delivery.delivery_id,
            content_revision: delivery.content_revision,
            head_commit: delivery.head_commit,
            state: delivery.state.as_str().to_owned(),
            revision: delivery.revision,
            merged_commit: delivery.merged_commit,
        }),
    }
}

fn command_error_to_api(error: TeamCommandError) -> ApiError {
    match error {
        TeamCommandError::TeamNotFound(id) | TeamCommandError::WorkItemNotFound(id) => ApiError::NotFound(id),
        TeamCommandError::ForbiddenTeam
        | TeamCommandError::CallerNotMember
        | TeamCommandError::MemberNotFound(_)
        | TeamCommandError::RelationPolicy(_)
        | TeamCommandError::WorkItemPolicy(_) => ApiError::Forbidden(error.to_string()),
        TeamCommandError::InvalidParentWorkItem(_)
        | TeamCommandError::InvalidCommand(_)
        | TeamCommandError::ExpectedDeliveryRevisionRequired
        | TeamCommandError::UnexpectedDeliveryRevision
        | TeamCommandError::GitDeliveryRequired => ApiError::BadRequest(error.to_string()),
        TeamCommandError::IdempotencyConflict
        | TeamCommandError::GitContentRevisionAlreadyUsed(_)
        | TeamCommandError::RosterChanged
        | TeamCommandError::ConcurrentSnapshotChange
        | TeamCommandError::RevisionConflict { .. }
        | TeamCommandError::WorkItemTransition(_)
        | TeamCommandError::GitDeliveryTransition(_) => ApiError::Conflict(error.to_string()),
        TeamCommandError::CorruptAggregate(_)
        | TeamCommandError::CorruptReceipt(_)
        | TeamCommandError::Roster(_)
        | TeamCommandError::Snapshot(_)
        | TeamCommandError::Database(_)
        | TeamCommandError::Json(_) => ApiError::Internal("Team Mode command failed".to_owned()),
    }
}

fn coordinator_error_to_api(error: TeamWorkCoordinatorError) -> ApiError {
    match error {
        TeamWorkCoordinatorError::TeamNotFound(id) => ApiError::NotFound(id),
        TeamWorkCoordinatorError::ForbiddenTeam => ApiError::Forbidden("Team is not owned by current user".into()),
        TeamWorkCoordinatorError::GitWorkspaceRequired => {
            ApiError::BadRequest("Team workspace is not a Git repository".into())
        }
        TeamWorkCoordinatorError::Git(crate::ports::GitWorkspacePortError::InvalidRequest(message)) => {
            ApiError::BadRequest(message)
        }
        TeamWorkCoordinatorError::Git(crate::ports::GitWorkspacePortError::Unavailable(message)) => {
            ApiError::Conflict(message)
        }
        TeamWorkCoordinatorError::Command(command) => command_error_to_api(command),
        TeamWorkCoordinatorError::CorruptState | TeamWorkCoordinatorError::Database(_) => {
            ApiError::Internal("Team work delegation failed".to_owned())
        }
    }
}

fn delivery_error_to_api(error: TeamDeliveryError) -> ApiError {
    match error {
        TeamDeliveryError::TeamNotFound(id) | TeamDeliveryError::AttemptNotFound(id) => ApiError::NotFound(id),
        TeamDeliveryError::ForbiddenTeam => ApiError::Forbidden("Team is not owned by current user".into()),
        TeamDeliveryError::ForbiddenActor(message) => ApiError::Forbidden(message),
        TeamDeliveryError::InvalidRequest(message) => ApiError::BadRequest(message),
        TeamDeliveryError::Conflict(message) => ApiError::Conflict(message),
        TeamDeliveryError::Port(crate::ports::GitDeliveryPortError::InvalidIntent(message))
        | TeamDeliveryError::Workspace(crate::ports::GitWorkspacePortError::InvalidRequest(message)) => {
            ApiError::BadRequest(message)
        }
        TeamDeliveryError::Port(crate::ports::GitDeliveryPortError::Unavailable(message))
        | TeamDeliveryError::Workspace(crate::ports::GitWorkspacePortError::Unavailable(message)) => {
            ApiError::Conflict(message)
        }
        TeamDeliveryError::CorruptState | TeamDeliveryError::Database(_) => {
            ApiError::Internal("Team Git integration failed".to_owned())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_idempotency_keys_reserve_space_for_saga_steps() {
        assert!(validate_public_key("delegate-1").is_ok());
        assert!(validate_public_key("").is_err());
        assert!(validate_public_key(&"x".repeat(PUBLIC_IDEMPOTENCY_KEY_MAX_LEN + 1)).is_err());
        assert!(derived_key(&"x".repeat(PUBLIC_IDEMPOTENCY_KEY_MAX_LEN), "decision").len() <= 240);
    }

    #[test]
    fn review_feedback_is_decision_bound_and_normalized() {
        assert_eq!(
            validate_review_feedback(TeamWorkReviewDecision::RequestChanges, Some("  concise feedback  ")).unwrap(),
            Some("concise feedback".to_owned())
        );
        assert!(validate_review_feedback(TeamWorkReviewDecision::RequestChanges, None).is_err());
        assert!(validate_review_feedback(TeamWorkReviewDecision::RequestChanges, Some(" \n ")).is_err());
        assert!(
            validate_review_feedback(
                TeamWorkReviewDecision::RequestChanges,
                Some(&"x".repeat(REVIEW_FEEDBACK_MAX_CHARS + 1)),
            )
            .is_err()
        );
        assert_eq!(
            validate_review_feedback(
                TeamWorkReviewDecision::RequestChanges,
                Some(&"x".repeat(REVIEW_FEEDBACK_MAX_CHARS)),
            )
            .unwrap()
            .unwrap()
            .chars()
            .count(),
            REVIEW_FEEDBACK_MAX_CHARS
        );
        assert_eq!(
            validate_review_feedback(TeamWorkReviewDecision::Accept, Some(" \n ")).unwrap(),
            None
        );
        assert!(validate_review_feedback(TeamWorkReviewDecision::Accept, Some("not allowed")).is_err());
        assert!(validate_review_feedback(TeamWorkReviewDecision::Reject, Some("not allowed")).is_err());
    }
}
