use thiserror::Error;

use super::{DeliveryRequirement, MemberRelation, RelationAction};

/// Relationship-level authorization shared by all Team Mode transports.
pub struct RelationPolicy;

impl RelationPolicy {
    /// Check only the relationship portion of an authorization decision.
    pub const fn allows(relation: MemberRelation, action: RelationAction) -> bool {
        match relation {
            MemberRelation::SelfMember => matches!(action, RelationAction::InspectScopedWork),
            MemberRelation::Superior => matches!(
                action,
                RelationAction::InspectScopedWork
                    | RelationAction::DirectMessage
                    | RelationAction::SubmitResult
                    | RelationAction::ReportBlocked
                    | RelationAction::RequestDecision
            ),
            MemberRelation::Peer => matches!(
                action,
                RelationAction::InspectScopedWork
                    | RelationAction::DirectMessage
                    | RelationAction::ProposeCollaboration
            ),
            MemberRelation::Subordinate => matches!(
                action,
                RelationAction::InspectScopedWork
                    | RelationAction::DirectMessage
                    | RelationAction::DelegateWork
                    | RelationAction::ReviewResult
                    | RelationAction::RequestChanges
                    | RelationAction::AcceptResult
                    | RelationAction::RejectResult
                    | RelationAction::IntegrateDelivery
                    | RelationAction::CancelControlledWork
            ),
            MemberRelation::Unrelated => false,
        }
    }

    pub fn authorize(relation: MemberRelation, action: RelationAction) -> Result<(), RelationPolicyError> {
        if Self::allows(relation, action) {
            return Ok(());
        }
        if relation == MemberRelation::Unrelated {
            return Err(RelationPolicyError::Unrelated);
        }
        Err(RelationPolicyError::ActionNotAllowed { relation, action })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum RelationPolicyError {
    #[error("the members do not have an active relationship in this scope")]
    Unrelated,
    #[error("{action:?} is not allowed when the target is {relation:?}")]
    ActionNotAllowed {
        relation: MemberRelation,
        action: RelationAction,
    },
}

/// A member command whose authorization depends on a WorkItem responsibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkItemAction {
    Queue,
    Start,
    Block,
    Resume,
    Submit,
    BeginReview,
    RequestChanges,
    Accept,
    Reject,
    Integrate,
    ReopenAfterConflict,
    Complete,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkItemRole {
    Controller,
    Assignee,
    Reviewer,
    Integrator,
}

/// The minimum responsibility projection needed for WorkItem authorization.
///
/// State and revision checks deliberately live in the lifecycle and command
/// handler. This view answers only whether the actor owns the required role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkItemPolicyView<'a> {
    pub controller_member_id: &'a str,
    pub assignee_member_id: Option<&'a str>,
    pub submission_producer_member_id: Option<&'a str>,
    pub reviewer_member_id: Option<&'a str>,
    pub integrator_member_id: Option<&'a str>,
    pub delivery_requirement: DeliveryRequirement,
}

/// Resource-level authorization for a single WorkItem.
pub struct WorkItemPolicy;

impl WorkItemPolicy {
    pub fn authorize(
        actor_member_id: &str,
        action: WorkItemAction,
        view: WorkItemPolicyView<'_>,
    ) -> Result<(), WorkItemPolicyError> {
        if matches!(action, WorkItemAction::Integrate | WorkItemAction::ReopenAfterConflict)
            && view.delivery_requirement != DeliveryRequirement::Git
        {
            return Err(WorkItemPolicyError::GitDeliveryNotRequired { action });
        }

        if action == WorkItemAction::Submit && view.submission_producer_member_id != Some(actor_member_id) {
            return Err(WorkItemPolicyError::SubmissionProducerMismatch { action });
        }

        if view.delivery_requirement == DeliveryRequirement::Git
            && matches!(
                action,
                WorkItemAction::Accept
                    | WorkItemAction::Integrate
                    | WorkItemAction::ReopenAfterConflict
                    | WorkItemAction::Complete
            )
            && view.integrator_member_id.is_none()
        {
            return Err(WorkItemPolicyError::IntegratorRequired { action });
        }

        let required_role = match action {
            WorkItemAction::Queue | WorkItemAction::Cancel => WorkItemRole::Controller,
            WorkItemAction::Start | WorkItemAction::Block | WorkItemAction::Resume | WorkItemAction::Submit => {
                WorkItemRole::Assignee
            }
            WorkItemAction::BeginReview
            | WorkItemAction::RequestChanges
            | WorkItemAction::Accept
            | WorkItemAction::Reject => WorkItemRole::Reviewer,
            WorkItemAction::Integrate | WorkItemAction::ReopenAfterConflict => WorkItemRole::Integrator,
            WorkItemAction::Complete => match view.delivery_requirement {
                DeliveryRequirement::None => WorkItemRole::Controller,
                DeliveryRequirement::Git => WorkItemRole::Integrator,
            },
        };

        let required_member_id = match required_role {
            WorkItemRole::Controller => Some(view.controller_member_id),
            WorkItemRole::Assignee => view.assignee_member_id,
            WorkItemRole::Reviewer => view.reviewer_member_id,
            WorkItemRole::Integrator => view.integrator_member_id,
        };

        if required_member_id != Some(actor_member_id) {
            return Err(WorkItemPolicyError::RoleRequired { action, required_role });
        }

        if required_role == WorkItemRole::Reviewer {
            let producer_member_id = view
                .submission_producer_member_id
                .ok_or(WorkItemPolicyError::SubmissionProducerRequired { action })?;
            if producer_member_id == actor_member_id {
                return Err(WorkItemPolicyError::SelfReviewNotAllowed);
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum WorkItemPolicyError {
    #[error("{action:?} requires the {required_role:?} WorkItem role")]
    RoleRequired {
        action: WorkItemAction,
        required_role: WorkItemRole,
    },
    #[error("a submission producer cannot review its own result")]
    SelfReviewNotAllowed,
    #[error("{action:?} requires the current submission producer")]
    SubmissionProducerRequired { action: WorkItemAction },
    #[error("{action:?} requires the authenticated actor to be the submission producer")]
    SubmissionProducerMismatch { action: WorkItemAction },
    #[error("{action:?} requires an assigned Git integrator")]
    IntegratorRequired { action: WorkItemAction },
    #[error("{action:?} requires a Git delivery")]
    GitDeliveryNotRequired { action: WorkItemAction },
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONTROLLER: &str = "controller";
    const ASSIGNEE: &str = "assignee";
    const REVIEWER: &str = "reviewer";
    const INTEGRATOR: &str = "integrator";

    fn git_view() -> WorkItemPolicyView<'static> {
        WorkItemPolicyView {
            controller_member_id: CONTROLLER,
            assignee_member_id: Some(ASSIGNEE),
            submission_producer_member_id: Some(ASSIGNEE),
            reviewer_member_id: Some(REVIEWER),
            integrator_member_id: Some(INTEGRATOR),
            delivery_requirement: DeliveryRequirement::Git,
        }
    }

    #[test]
    fn relation_policy_is_directional() {
        assert!(RelationPolicy::allows(
            MemberRelation::Subordinate,
            RelationAction::DelegateWork
        ));
        assert!(!RelationPolicy::allows(
            MemberRelation::Superior,
            RelationAction::DelegateWork
        ));
        assert!(RelationPolicy::allows(
            MemberRelation::Superior,
            RelationAction::SubmitResult
        ));
        assert!(!RelationPolicy::allows(
            MemberRelation::Subordinate,
            RelationAction::SubmitResult
        ));
    }

    #[test]
    fn peers_cannot_assign_review_or_integrate() {
        for action in [
            RelationAction::DelegateWork,
            RelationAction::ReviewResult,
            RelationAction::IntegrateDelivery,
            RelationAction::CancelControlledWork,
        ] {
            assert!(!RelationPolicy::allows(MemberRelation::Peer, action));
        }
        assert!(RelationPolicy::allows(
            MemberRelation::Peer,
            RelationAction::ProposeCollaboration
        ));
    }

    #[test]
    fn unrelated_members_are_denied() {
        assert_eq!(
            RelationPolicy::authorize(MemberRelation::Unrelated, RelationAction::DirectMessage),
            Err(RelationPolicyError::Unrelated)
        );
    }

    #[test]
    fn assignee_can_drive_execution_but_cannot_review() {
        let view = git_view();
        for action in [
            WorkItemAction::Start,
            WorkItemAction::Block,
            WorkItemAction::Resume,
            WorkItemAction::Submit,
        ] {
            assert_eq!(WorkItemPolicy::authorize(ASSIGNEE, action, view), Ok(()));
        }
        assert!(WorkItemPolicy::authorize(ASSIGNEE, WorkItemAction::Accept, view).is_err());
    }

    #[test]
    fn reviewer_cannot_review_own_result() {
        let view = WorkItemPolicyView {
            assignee_member_id: Some("replacement-assignee"),
            submission_producer_member_id: Some(REVIEWER),
            ..git_view()
        };
        assert_eq!(
            WorkItemPolicy::authorize(REVIEWER, WorkItemAction::Accept, view),
            Err(WorkItemPolicyError::SelfReviewNotAllowed)
        );
    }

    #[test]
    fn assignee_cannot_attribute_its_submission_to_another_member() {
        let view = WorkItemPolicyView {
            submission_producer_member_id: Some("another-member"),
            ..git_view()
        };
        assert_eq!(
            WorkItemPolicy::authorize(ASSIGNEE, WorkItemAction::Submit, view),
            Err(WorkItemPolicyError::SubmissionProducerMismatch {
                action: WorkItemAction::Submit,
            })
        );
    }

    #[test]
    fn git_work_cannot_be_accepted_without_an_integrator() {
        let view = WorkItemPolicyView {
            integrator_member_id: None,
            ..git_view()
        };
        assert_eq!(
            WorkItemPolicy::authorize(REVIEWER, WorkItemAction::Accept, view),
            Err(WorkItemPolicyError::IntegratorRequired {
                action: WorkItemAction::Accept,
            })
        );
    }

    #[test]
    fn controller_relationship_does_not_grant_integration() {
        let view = git_view();
        assert_eq!(
            WorkItemPolicy::authorize(CONTROLLER, WorkItemAction::Integrate, view),
            Err(WorkItemPolicyError::RoleRequired {
                action: WorkItemAction::Integrate,
                required_role: WorkItemRole::Integrator,
            })
        );
        assert_eq!(
            WorkItemPolicy::authorize(INTEGRATOR, WorkItemAction::Integrate, view),
            Ok(())
        );
    }

    #[test]
    fn non_git_work_cannot_be_integrated() {
        let view = WorkItemPolicyView {
            delivery_requirement: DeliveryRequirement::None,
            ..git_view()
        };
        assert_eq!(
            WorkItemPolicy::authorize(INTEGRATOR, WorkItemAction::Integrate, view),
            Err(WorkItemPolicyError::GitDeliveryNotRequired {
                action: WorkItemAction::Integrate,
            })
        );
        assert_eq!(
            WorkItemPolicy::authorize(CONTROLLER, WorkItemAction::Complete, view),
            Ok(())
        );
    }
}
