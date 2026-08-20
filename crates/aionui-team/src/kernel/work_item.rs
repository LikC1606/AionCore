use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    ConflictedDeliveryEvidence, DeliveryRequirement, GitDeliveryRef, GitWorkAssignment, MergedDeliveryEvidence,
    WorkSubmission,
};

/// The execution and review lifecycle of a WorkItem.
///
/// Git integration is tracked by `GitDeliveryLifecycle`. `Completed` is the
/// only successful terminal WorkItem state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkItemState {
    Draft,
    Queued,
    Running,
    Blocked,
    Submitted,
    Reviewing,
    ChangesRequested,
    Accepted,
    Completed,
    Rejected,
    Failed,
    Cancelled,
}

impl WorkItemState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Blocked => "blocked",
            Self::Submitted => "submitted",
            Self::Reviewing => "reviewing",
            Self::ChangesRequested => "changes_requested",
            Self::Accepted => "accepted",
            Self::Completed => "completed",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Rejected | Self::Failed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkItemTransition {
    Queue,
    Start,
    Block,
    Resume,
    Submit { submission: WorkSubmission },
    BeginReview,
    RequestChanges,
    Accept,
    Reject,
    ReopenAfterConflict { evidence: ConflictedDeliveryEvidence },
    CompleteWithoutDelivery,
    CompleteWithDelivery { evidence: MergedDeliveryEvidence },
    Fail,
    Cancel,
}

/// Versioned WorkItem lifecycle with an exact submission and acceptance bind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct WorkItemLifecycle {
    team_id: String,
    work_item_id: String,
    state: WorkItemState,
    delivery_requirement: DeliveryRequirement,
    git_assignment: Option<GitWorkAssignment>,
    current_submission: Option<WorkSubmission>,
    accepted_delivery: Option<GitDeliveryRef>,
    revision: u64,
}

// Phase 2 command services will become the first non-test lifecycle driver.
#[cfg_attr(not(test), allow(dead_code))]
impl WorkItemLifecycle {
    pub(crate) fn new(
        team_id: impl Into<String>,
        work_item_id: impl Into<String>,
        delivery_requirement: DeliveryRequirement,
        git_assignment: Option<GitWorkAssignment>,
    ) -> Result<Self, WorkItemRestoreError> {
        validate_git_assignment(delivery_requirement, git_assignment.as_ref())?;
        Ok(Self {
            team_id: team_id.into(),
            work_item_id: work_item_id.into(),
            state: WorkItemState::Draft,
            delivery_requirement,
            git_assignment,
            current_submission: None,
            accepted_delivery: None,
            revision: 0,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn restore(
        team_id: impl Into<String>,
        work_item_id: impl Into<String>,
        state: WorkItemState,
        delivery_requirement: DeliveryRequirement,
        git_assignment: Option<GitWorkAssignment>,
        current_submission: Option<WorkSubmission>,
        accepted_delivery: Option<GitDeliveryRef>,
        revision: u64,
    ) -> Result<Self, WorkItemRestoreError> {
        let team_id = team_id.into();
        let work_item_id = work_item_id.into();
        validate_git_assignment(delivery_requirement, git_assignment.as_ref())?;

        match (&current_submission, delivery_requirement) {
            (Some(WorkSubmission::Inline { .. }), DeliveryRequirement::None) | (None, _) => {}
            (Some(WorkSubmission::Git { delivery }), DeliveryRequirement::Git)
                if delivery.team_id() == team_id && delivery.work_item_id() == work_item_id =>
            {
                if let Some(field) = git_assignment
                    .as_ref()
                    .expect("validated Git assignment")
                    .submission_mismatch(delivery)
                {
                    return Err(WorkItemRestoreError::SubmissionAssignmentMismatch { field });
                }
            }
            (Some(WorkSubmission::Git { delivery }), DeliveryRequirement::Git) => {
                return Err(WorkItemRestoreError::SubmissionScopeMismatch {
                    expected_team_id: team_id,
                    expected_work_item_id: work_item_id,
                    actual_team_id: delivery.team_id().to_owned(),
                    actual_work_item_id: delivery.work_item_id().to_owned(),
                });
            }
            (Some(_), requirement) => {
                return Err(WorkItemRestoreError::SubmissionRequirementMismatch { requirement });
            }
        }

        let state_requires_submission = matches!(
            state,
            WorkItemState::Submitted
                | WorkItemState::Reviewing
                | WorkItemState::Accepted
                | WorkItemState::Completed
                | WorkItemState::Rejected
        );
        let state_forbids_submission = matches!(
            state,
            WorkItemState::Draft
                | WorkItemState::Queued
                | WorkItemState::Running
                | WorkItemState::Blocked
                | WorkItemState::ChangesRequested
                | WorkItemState::Failed
        );
        if state_requires_submission && current_submission.is_none() {
            return Err(WorkItemRestoreError::SubmissionMissing { state });
        }
        if state_forbids_submission && current_submission.is_some() {
            return Err(WorkItemRestoreError::UnexpectedSubmission { state });
        }

        if delivery_requirement == DeliveryRequirement::None && accepted_delivery.is_some() {
            return Err(WorkItemRestoreError::AcceptedDeliveryNotAllowed);
        }
        if accepted_delivery.is_some()
            && !matches!(
                state,
                WorkItemState::Accepted | WorkItemState::Completed | WorkItemState::Cancelled
            )
        {
            return Err(WorkItemRestoreError::UnexpectedAcceptedDelivery { state });
        }
        if delivery_requirement == DeliveryRequirement::Git
            && matches!(state, WorkItemState::Accepted | WorkItemState::Completed)
            && accepted_delivery.is_none()
        {
            return Err(WorkItemRestoreError::AcceptedDeliveryMissing { state });
        }
        if let Some(accepted) = accepted_delivery.as_ref() {
            let submitted = current_submission.as_ref().and_then(WorkSubmission::git_delivery);
            if submitted != Some(accepted) {
                return Err(WorkItemRestoreError::AcceptedDeliveryMismatch);
            }
        }
        if !is_reachable_snapshot(
            state,
            delivery_requirement,
            current_submission.is_some(),
            accepted_delivery.is_some(),
            revision,
        ) {
            return Err(WorkItemRestoreError::InvalidRevision {
                state,
                actual: revision,
            });
        }

        Ok(Self {
            team_id,
            work_item_id,
            state,
            delivery_requirement,
            git_assignment,
            current_submission,
            accepted_delivery,
            revision,
        })
    }

    pub fn work_item_id(&self) -> &str {
        &self.work_item_id
    }

    pub fn team_id(&self) -> &str {
        &self.team_id
    }

    pub const fn state(&self) -> WorkItemState {
        self.state
    }

    pub const fn delivery_requirement(&self) -> DeliveryRequirement {
        self.delivery_requirement
    }

    pub const fn git_assignment(&self) -> Option<&GitWorkAssignment> {
        self.git_assignment.as_ref()
    }

    pub const fn current_submission(&self) -> Option<&WorkSubmission> {
        self.current_submission.as_ref()
    }

    pub fn submission_producer_member_id(&self) -> Option<&str> {
        self.current_submission.as_ref().map(WorkSubmission::producer_member_id)
    }

    pub const fn accepted_delivery(&self) -> Option<&GitDeliveryRef> {
        self.accepted_delivery.as_ref()
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn apply(
        &mut self,
        expected_revision: u64,
        transition: WorkItemTransition,
    ) -> Result<WorkItemState, WorkItemTransitionError> {
        if expected_revision != self.revision {
            return Err(WorkItemTransitionError::RevisionConflict {
                expected: expected_revision,
                actual: self.revision,
            });
        }

        let mut next_submission = self.current_submission.clone();
        let mut next_accepted_delivery = self.accepted_delivery.clone();
        let next_state = match (self.state, transition.clone()) {
            (WorkItemState::Draft | WorkItemState::ChangesRequested, WorkItemTransition::Queue) => {
                WorkItemState::Queued
            }
            (WorkItemState::Queued | WorkItemState::ChangesRequested, WorkItemTransition::Start) => {
                WorkItemState::Running
            }
            (WorkItemState::Running, WorkItemTransition::Block) => WorkItemState::Blocked,
            (WorkItemState::Blocked, WorkItemTransition::Resume) => WorkItemState::Running,
            (WorkItemState::Running, WorkItemTransition::Submit { submission }) => {
                self.validate_submission(&submission)?;
                next_submission = Some(submission);
                next_accepted_delivery = None;
                WorkItemState::Submitted
            }
            (WorkItemState::Submitted, WorkItemTransition::BeginReview) => WorkItemState::Reviewing,
            (WorkItemState::Reviewing, WorkItemTransition::RequestChanges) => {
                next_submission = None;
                next_accepted_delivery = None;
                WorkItemState::ChangesRequested
            }
            (WorkItemState::Reviewing, WorkItemTransition::Accept) => {
                let submission = self
                    .current_submission
                    .as_ref()
                    .ok_or(WorkItemTransitionError::SubmissionMissing)?;
                next_accepted_delivery = submission.git_delivery().cloned();
                WorkItemState::Accepted
            }
            (WorkItemState::Reviewing, WorkItemTransition::Reject) => WorkItemState::Rejected,
            (WorkItemState::Accepted, WorkItemTransition::ReopenAfterConflict { evidence }) => {
                self.validate_delivery_evidence(evidence.delivery())?;
                next_submission = None;
                next_accepted_delivery = None;
                WorkItemState::ChangesRequested
            }
            (WorkItemState::Accepted, WorkItemTransition::CompleteWithoutDelivery) => {
                if self.delivery_requirement != DeliveryRequirement::None {
                    return Err(WorkItemTransitionError::GitDeliveryRequired);
                }
                WorkItemState::Completed
            }
            (WorkItemState::Accepted, WorkItemTransition::CompleteWithDelivery { evidence }) => {
                if self.delivery_requirement != DeliveryRequirement::Git {
                    return Err(WorkItemTransitionError::GitDeliveryNotRequired);
                }
                self.validate_delivery_evidence(evidence.delivery())?;
                WorkItemState::Completed
            }
            (
                WorkItemState::Queued
                | WorkItemState::Running
                | WorkItemState::Blocked
                | WorkItemState::ChangesRequested,
                WorkItemTransition::Fail,
            ) => WorkItemState::Failed,
            (
                WorkItemState::Draft
                | WorkItemState::Queued
                | WorkItemState::Running
                | WorkItemState::Blocked
                | WorkItemState::Submitted
                | WorkItemState::Reviewing
                | WorkItemState::ChangesRequested
                | WorkItemState::Accepted,
                WorkItemTransition::Cancel,
            ) => WorkItemState::Cancelled,
            (state, _) => {
                return Err(WorkItemTransitionError::InvalidTransition {
                    state,
                    transition: Box::new(transition),
                });
            }
        };

        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or(WorkItemTransitionError::RevisionOverflow)?;
        self.state = next_state;
        self.current_submission = next_submission;
        self.accepted_delivery = next_accepted_delivery;
        self.revision = next_revision;
        Ok(next_state)
    }

    fn validate_submission(&self, submission: &WorkSubmission) -> Result<(), WorkItemTransitionError> {
        match (self.delivery_requirement, submission) {
            (DeliveryRequirement::None, WorkSubmission::Inline { .. }) => Ok(()),
            (DeliveryRequirement::Git, WorkSubmission::Git { delivery })
                if delivery.team_id() == self.team_id && delivery.work_item_id() == self.work_item_id =>
            {
                let assignment = self
                    .git_assignment
                    .as_ref()
                    .ok_or(WorkItemTransitionError::GitAssignmentMissing)?;
                match assignment.submission_mismatch(delivery) {
                    Some(field) => Err(WorkItemTransitionError::SubmissionAssignmentMismatch { field }),
                    None => Ok(()),
                }
            }
            (DeliveryRequirement::Git, WorkSubmission::Git { delivery }) => {
                Err(WorkItemTransitionError::SubmissionScopeMismatch {
                    expected_team_id: self.team_id.clone(),
                    expected_work_item_id: self.work_item_id.clone(),
                    actual_team_id: delivery.team_id().to_owned(),
                    actual_work_item_id: delivery.work_item_id().to_owned(),
                })
            }
            _ => Err(WorkItemTransitionError::SubmissionRequirementMismatch {
                requirement: self.delivery_requirement,
            }),
        }
    }

    fn validate_delivery_evidence(&self, actual: &GitDeliveryRef) -> Result<(), WorkItemTransitionError> {
        let expected = self
            .accepted_delivery
            .as_ref()
            .ok_or(WorkItemTransitionError::AcceptedDeliveryMissing)?;
        if expected == actual {
            return Ok(());
        }
        Err(WorkItemTransitionError::DeliveryEvidenceMismatch {
            expected_delivery_id: expected.delivery_id().to_owned(),
            expected_content_revision: expected.content_revision(),
            expected_head_commit: expected.head_commit().to_owned(),
            actual_delivery_id: actual.delivery_id().to_owned(),
            actual_content_revision: actual.content_revision(),
            actual_head_commit: actual.head_commit().to_owned(),
        })
    }
}

fn validate_git_assignment(
    requirement: DeliveryRequirement,
    assignment: Option<&GitWorkAssignment>,
) -> Result<(), WorkItemRestoreError> {
    match (requirement, assignment) {
        (DeliveryRequirement::None, None) => Ok(()),
        (DeliveryRequirement::None, Some(_)) => Err(WorkItemRestoreError::GitAssignmentNotAllowed),
        (DeliveryRequirement::Git, None) => Err(WorkItemRestoreError::GitAssignmentMissing),
        (DeliveryRequirement::Git, Some(assignment)) => match assignment.empty_field() {
            Some(field) => Err(WorkItemRestoreError::GitAssignmentFieldEmpty { field }),
            None => Ok(()),
        },
    }
}

fn is_reachable_snapshot(
    state: WorkItemState,
    delivery_requirement: DeliveryRequirement,
    has_submission: bool,
    has_accepted_delivery: bool,
    revision: u64,
) -> bool {
    match state {
        WorkItemState::Draft => revision == 0,
        WorkItemState::Queued => match delivery_requirement {
            DeliveryRequirement::None => matches!(revision, 1 | 6 | 8) || revision >= 10,
            DeliveryRequirement::Git => revision == 1 || revision >= 6,
        },
        WorkItemState::Running => matches!(revision, 2 | 4) || revision >= 6,
        WorkItemState::Blocked | WorkItemState::Submitted => matches!(revision, 3 | 5) || revision >= 7,
        WorkItemState::Reviewing => matches!(revision, 4 | 6) || revision >= 8,
        WorkItemState::ChangesRequested => match delivery_requirement {
            DeliveryRequirement::None => matches!(revision, 5 | 7) || revision >= 9,
            DeliveryRequirement::Git => revision >= 5,
        },
        WorkItemState::Accepted | WorkItemState::Rejected => matches!(revision, 5 | 7) || revision >= 9,
        WorkItemState::Completed => matches!(revision, 6 | 8) || revision >= 10,
        WorkItemState::Failed => revision >= 2,
        WorkItemState::Cancelled if has_accepted_delivery => matches!(revision, 6 | 8) || revision >= 10,
        WorkItemState::Cancelled if has_submission => revision >= 4,
        WorkItemState::Cancelled => revision >= 1,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WorkItemTransitionError {
    #[error("expected WorkItem revision {expected}, but current revision is {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error("cannot apply {transition:?} while WorkItem is {state:?}")]
    InvalidTransition {
        state: WorkItemState,
        transition: Box<WorkItemTransition>,
    },
    #[error("submitted result does not match the {requirement:?} delivery requirement")]
    SubmissionRequirementMismatch { requirement: DeliveryRequirement },
    #[error(
        "delivery belongs to {actual_team_id}/{actual_work_item_id}, expected {expected_team_id}/{expected_work_item_id}"
    )]
    SubmissionScopeMismatch {
        expected_team_id: String,
        expected_work_item_id: String,
        actual_team_id: String,
        actual_work_item_id: String,
    },
    #[error("Git WorkItem is missing its immutable assignment")]
    GitAssignmentMissing,
    #[error("Git submission {field} does not match the immutable WorkItem assignment")]
    SubmissionAssignmentMismatch { field: &'static str },
    #[error("cannot accept a WorkItem without a current submission")]
    SubmissionMissing,
    #[error("accepted Git delivery is missing")]
    AcceptedDeliveryMissing,
    #[error(
        "delivery evidence mismatch: expected {expected_delivery_id}@{expected_content_revision}:{expected_head_commit}, got {actual_delivery_id}@{actual_content_revision}:{actual_head_commit}"
    )]
    DeliveryEvidenceMismatch {
        expected_delivery_id: String,
        expected_content_revision: u64,
        expected_head_commit: String,
        actual_delivery_id: String,
        actual_content_revision: u64,
        actual_head_commit: String,
    },
    #[error("Git WorkItem requires merged delivery evidence")]
    GitDeliveryRequired,
    #[error("non-Git WorkItem cannot consume Git delivery evidence")]
    GitDeliveryNotRequired,
    #[error("WorkItem revision overflow")]
    RevisionOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum WorkItemRestoreError {
    #[error("{state:?} WorkItem has invalid lifecycle revision {actual}")]
    InvalidRevision { state: WorkItemState, actual: u64 },
    #[error("restored submission does not match the {requirement:?} delivery requirement")]
    SubmissionRequirementMismatch { requirement: DeliveryRequirement },
    #[error(
        "restored delivery belongs to {actual_team_id}/{actual_work_item_id}, expected {expected_team_id}/{expected_work_item_id}"
    )]
    SubmissionScopeMismatch {
        expected_team_id: String,
        expected_work_item_id: String,
        actual_team_id: String,
        actual_work_item_id: String,
    },
    #[error("Git WorkItem snapshot is missing its immutable assignment")]
    GitAssignmentMissing,
    #[error("non-Git WorkItem snapshot cannot contain a Git assignment")]
    GitAssignmentNotAllowed,
    #[error("Git WorkItem assignment {field} must not be empty")]
    GitAssignmentFieldEmpty { field: &'static str },
    #[error("restored Git submission {field} does not match the immutable WorkItem assignment")]
    SubmissionAssignmentMismatch { field: &'static str },
    #[error("{state:?} WorkItem snapshot requires a current submission")]
    SubmissionMissing { state: WorkItemState },
    #[error("{state:?} WorkItem snapshot cannot contain a current submission")]
    UnexpectedSubmission { state: WorkItemState },
    #[error("non-Git WorkItem snapshot cannot contain an accepted delivery")]
    AcceptedDeliveryNotAllowed,
    #[error("{state:?} WorkItem snapshot cannot contain an accepted delivery")]
    UnexpectedAcceptedDelivery { state: WorkItemState },
    #[error("{state:?} Git WorkItem snapshot requires an accepted delivery")]
    AcceptedDeliveryMissing { state: WorkItemState },
    #[error("accepted delivery does not match the current Git submission")]
    AcceptedDeliveryMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::{GitDeliveryLifecycle, GitDeliveryTransition};

    const TEAM_ID: &str = "team-1";
    const WORK_ITEM_ID: &str = "work-1";

    fn git_assignment() -> GitWorkAssignment {
        GitWorkAssignment::new("repo-1", "base", "refs/heads/ds/work-1/worker")
    }

    fn new_work(work_item_id: &str, requirement: DeliveryRequirement) -> WorkItemLifecycle {
        let assignment = (requirement == DeliveryRequirement::Git).then(git_assignment);
        WorkItemLifecycle::new(TEAM_ID, work_item_id, requirement, assignment).unwrap()
    }

    fn delivery_ref(delivery_id: &str, work_item_id: &str, head: &str) -> GitDeliveryRef {
        GitDeliveryRef::new(
            delivery_id,
            TEAM_ID,
            work_item_id,
            "worker",
            "repo-1",
            1,
            "base",
            "refs/heads/ds/work-1/worker",
            head,
        )
    }

    fn apply_work(lifecycle: &mut WorkItemLifecycle, transition: WorkItemTransition) -> WorkItemState {
        lifecycle.apply(lifecycle.revision(), transition).unwrap()
    }

    fn accepted_work(requirement: DeliveryRequirement, delivery: Option<GitDeliveryRef>) -> WorkItemLifecycle {
        let mut lifecycle = new_work(WORK_ITEM_ID, requirement);
        apply_work(&mut lifecycle, WorkItemTransition::Queue);
        apply_work(&mut lifecycle, WorkItemTransition::Start);
        let submission = match delivery {
            Some(delivery) => WorkSubmission::git(delivery),
            None => WorkSubmission::inline("worker"),
        };
        apply_work(&mut lifecycle, WorkItemTransition::Submit { submission });
        apply_work(&mut lifecycle, WorkItemTransition::BeginReview);
        apply_work(&mut lifecycle, WorkItemTransition::Accept);
        lifecycle
    }

    fn merged_delivery(reference: GitDeliveryRef) -> GitDeliveryLifecycle {
        let mut delivery = GitDeliveryLifecycle::submitted(reference);
        delivery.apply(0, GitDeliveryTransition::Accept).unwrap();
        delivery.apply(1, GitDeliveryTransition::BeginIntegration).unwrap();
        delivery
            .apply(
                2,
                GitDeliveryTransition::MarkMerged {
                    merged_commit: "merge-1".into(),
                },
            )
            .unwrap();
        delivery
    }

    fn conflicted_delivery(reference: GitDeliveryRef) -> GitDeliveryLifecycle {
        let mut delivery = GitDeliveryLifecycle::submitted(reference);
        delivery.apply(0, GitDeliveryTransition::Accept).unwrap();
        delivery.apply(1, GitDeliveryTransition::BeginIntegration).unwrap();
        delivery.apply(2, GitDeliveryTransition::MarkConflicted).unwrap();
        delivery
    }

    #[test]
    fn non_git_work_completes_without_delivery() {
        let mut work = accepted_work(DeliveryRequirement::None, None);
        assert_eq!(
            apply_work(&mut work, WorkItemTransition::CompleteWithoutDelivery),
            WorkItemState::Completed
        );
        assert!(work.state().is_terminal());
    }

    #[test]
    fn git_acceptance_binds_exact_delivery_and_head() {
        let reference = delivery_ref("delivery-1", WORK_ITEM_ID, "head-1");
        let work = accepted_work(DeliveryRequirement::Git, Some(reference.clone()));
        assert_eq!(work.accepted_delivery(), Some(&reference));
    }

    #[test]
    fn git_work_completes_only_with_matching_merged_evidence() {
        let reference = delivery_ref("delivery-1", WORK_ITEM_ID, "head-1");
        let mut work = accepted_work(DeliveryRequirement::Git, Some(reference.clone()));
        let merged = merged_delivery(reference);
        assert_eq!(
            apply_work(
                &mut work,
                WorkItemTransition::CompleteWithDelivery {
                    evidence: merged.merged_evidence().unwrap(),
                }
            ),
            WorkItemState::Completed
        );
    }

    #[test]
    fn changed_head_invalidates_acceptance() {
        let accepted = delivery_ref("delivery-1", WORK_ITEM_ID, "head-1");
        let mut work = accepted_work(DeliveryRequirement::Git, Some(accepted));
        let changed = delivery_ref("delivery-2", WORK_ITEM_ID, "head-2");
        let merged = merged_delivery(changed);

        assert!(matches!(
            work.apply(
                work.revision(),
                WorkItemTransition::CompleteWithDelivery {
                    evidence: merged.merged_evidence().unwrap(),
                }
            ),
            Err(WorkItemTransitionError::DeliveryEvidenceMismatch { .. })
        ));
        assert_eq!(work.state(), WorkItemState::Accepted);
    }

    #[test]
    fn delivery_for_another_work_item_is_rejected_at_submit() {
        let mut work = new_work(WORK_ITEM_ID, DeliveryRequirement::Git);
        apply_work(&mut work, WorkItemTransition::Queue);
        apply_work(&mut work, WorkItemTransition::Start);
        let submission = WorkSubmission::git(delivery_ref("delivery-1", "work-2", "head-1"));
        assert!(matches!(
            work.apply(work.revision(), WorkItemTransition::Submit { submission }),
            Err(WorkItemTransitionError::SubmissionScopeMismatch { .. })
        ));
        assert_eq!(work.state(), WorkItemState::Running);
    }

    #[test]
    fn conflict_reopens_only_the_work_item_that_accepted_that_delivery() {
        let reference = delivery_ref("delivery-1", WORK_ITEM_ID, "head-1");
        let mut work = accepted_work(DeliveryRequirement::Git, Some(reference.clone()));
        let conflicted = conflicted_delivery(reference);
        assert_eq!(
            apply_work(
                &mut work,
                WorkItemTransition::ReopenAfterConflict {
                    evidence: conflicted.conflicted_evidence().unwrap(),
                }
            ),
            WorkItemState::ChangesRequested
        );
        assert!(work.current_submission().is_none());
        assert!(work.accepted_delivery().is_none());
    }

    #[test]
    fn accepted_work_can_be_cancelled_before_delivery_integration() {
        let reference = delivery_ref("delivery-1", WORK_ITEM_ID, "head-1");
        let mut work = accepted_work(DeliveryRequirement::Git, Some(reference));
        assert_eq!(
            apply_work(&mut work, WorkItemTransition::Cancel),
            WorkItemState::Cancelled
        );
        assert!(work.state().is_terminal());
    }

    #[test]
    fn blocked_work_only_resumes_to_running() {
        let mut work = new_work(WORK_ITEM_ID, DeliveryRequirement::None);
        apply_work(&mut work, WorkItemTransition::Queue);
        apply_work(&mut work, WorkItemTransition::Start);
        assert_eq!(apply_work(&mut work, WorkItemTransition::Block), WorkItemState::Blocked);
        assert_eq!(
            apply_work(&mut work, WorkItemTransition::Resume),
            WorkItemState::Running
        );
    }

    #[test]
    fn requested_changes_require_a_new_submission_and_review() {
        let mut work = new_work(WORK_ITEM_ID, DeliveryRequirement::None);
        apply_work(&mut work, WorkItemTransition::Queue);
        apply_work(&mut work, WorkItemTransition::Start);
        apply_work(
            &mut work,
            WorkItemTransition::Submit {
                submission: WorkSubmission::inline("worker"),
            },
        );
        apply_work(&mut work, WorkItemTransition::BeginReview);
        apply_work(&mut work, WorkItemTransition::RequestChanges);
        assert!(work.current_submission().is_none());
        apply_work(&mut work, WorkItemTransition::Start);
        apply_work(
            &mut work,
            WorkItemTransition::Submit {
                submission: WorkSubmission::inline("worker"),
            },
        );
        apply_work(&mut work, WorkItemTransition::BeginReview);
        assert_eq!(
            apply_work(&mut work, WorkItemTransition::Accept),
            WorkItemState::Accepted
        );
    }

    #[test]
    fn stale_revision_is_rejected_without_mutating_state() {
        let mut work = new_work(WORK_ITEM_ID, DeliveryRequirement::None);
        apply_work(&mut work, WorkItemTransition::Queue);
        assert_eq!(
            work.apply(0, WorkItemTransition::Start),
            Err(WorkItemTransitionError::RevisionConflict { expected: 0, actual: 1 })
        );
        assert_eq!(work.state(), WorkItemState::Queued);
        assert_eq!(work.revision(), 1);
    }

    #[test]
    fn terminal_work_states_reject_further_transitions() {
        let mut completed = accepted_work(DeliveryRequirement::None, None);
        apply_work(&mut completed, WorkItemTransition::CompleteWithoutDelivery);

        let mut rejected = new_work("work-rejected", DeliveryRequirement::None);
        apply_work(&mut rejected, WorkItemTransition::Queue);
        apply_work(&mut rejected, WorkItemTransition::Start);
        apply_work(
            &mut rejected,
            WorkItemTransition::Submit {
                submission: WorkSubmission::inline("worker"),
            },
        );
        apply_work(&mut rejected, WorkItemTransition::BeginReview);
        apply_work(&mut rejected, WorkItemTransition::Reject);

        let mut failed = new_work("work-failed", DeliveryRequirement::None);
        apply_work(&mut failed, WorkItemTransition::Queue);
        apply_work(&mut failed, WorkItemTransition::Fail);

        let mut cancelled = new_work("work-cancelled", DeliveryRequirement::None);
        apply_work(&mut cancelled, WorkItemTransition::Cancel);

        for mut work in [completed, rejected, failed, cancelled] {
            let terminal = work.state();
            let revision = work.revision();
            assert!(matches!(
                work.apply(revision, WorkItemTransition::Start),
                Err(WorkItemTransitionError::InvalidTransition { state, .. }) if state == terminal
            ));
            assert_eq!(work.revision(), revision);
        }
    }

    #[test]
    fn restore_round_trips_a_valid_accepted_git_work_item() {
        let reference = delivery_ref("delivery-1", WORK_ITEM_ID, "head-1");
        let work = accepted_work(DeliveryRequirement::Git, Some(reference));

        let restored = WorkItemLifecycle::restore(
            work.team_id().to_owned(),
            work.work_item_id().to_owned(),
            work.state(),
            work.delivery_requirement(),
            work.git_assignment().cloned(),
            work.current_submission().cloned(),
            work.accepted_delivery().cloned(),
            work.revision(),
        )
        .unwrap();

        assert_eq!(restored, work);
    }

    #[test]
    fn creation_and_restore_require_a_complete_correlated_git_assignment() {
        assert_eq!(
            WorkItemLifecycle::new(TEAM_ID, WORK_ITEM_ID, DeliveryRequirement::Git, None),
            Err(WorkItemRestoreError::GitAssignmentMissing)
        );
        assert_eq!(
            WorkItemLifecycle::new(TEAM_ID, WORK_ITEM_ID, DeliveryRequirement::None, Some(git_assignment()),),
            Err(WorkItemRestoreError::GitAssignmentNotAllowed)
        );
        assert_eq!(
            WorkItemLifecycle::new(
                TEAM_ID,
                WORK_ITEM_ID,
                DeliveryRequirement::Git,
                Some(GitWorkAssignment::new("repo-1", "", "refs/heads/work")),
            ),
            Err(WorkItemRestoreError::GitAssignmentFieldEmpty { field: "base_commit" })
        );

        let mismatched = GitDeliveryRef::new(
            "delivery-1",
            TEAM_ID,
            WORK_ITEM_ID,
            "worker",
            "repo-other",
            1,
            "base",
            "refs/heads/ds/work-1/worker",
            "head-1",
        );
        assert_eq!(
            WorkItemLifecycle::restore(
                TEAM_ID,
                WORK_ITEM_ID,
                WorkItemState::Submitted,
                DeliveryRequirement::Git,
                Some(git_assignment()),
                Some(WorkSubmission::git(mismatched)),
                None,
                3,
            ),
            Err(WorkItemRestoreError::SubmissionAssignmentMismatch { field: "repository_id" })
        );
    }

    #[test]
    fn restore_rejects_impossible_or_cross_scope_snapshots() {
        let reference = delivery_ref("delivery-1", WORK_ITEM_ID, "head-1");
        assert_eq!(
            WorkItemLifecycle::restore(
                TEAM_ID,
                WORK_ITEM_ID,
                WorkItemState::Completed,
                DeliveryRequirement::Git,
                Some(git_assignment()),
                Some(WorkSubmission::git(reference)),
                None,
                6,
            ),
            Err(WorkItemRestoreError::AcceptedDeliveryMissing {
                state: WorkItemState::Completed,
            })
        );

        let cross_scope = delivery_ref("delivery-2", "work-2", "head-2");
        assert!(matches!(
            WorkItemLifecycle::restore(
                TEAM_ID,
                WORK_ITEM_ID,
                WorkItemState::Submitted,
                DeliveryRequirement::Git,
                Some(git_assignment()),
                Some(WorkSubmission::git(cross_scope)),
                None,
                3,
            ),
            Err(WorkItemRestoreError::SubmissionScopeMismatch { .. })
        ));

        assert_eq!(
            WorkItemLifecycle::restore(
                TEAM_ID,
                WORK_ITEM_ID,
                WorkItemState::Running,
                DeliveryRequirement::None,
                None,
                Some(WorkSubmission::inline("worker")),
                None,
                2,
            ),
            Err(WorkItemRestoreError::UnexpectedSubmission {
                state: WorkItemState::Running,
            })
        );
    }

    #[test]
    fn restore_rejects_unreachable_revision_holes() {
        let inline = || Some(WorkSubmission::inline("worker"));
        for (state, requirement, submission, accepted, revision) in [
            (WorkItemState::Draft, DeliveryRequirement::None, None, None, 1),
            (WorkItemState::Queued, DeliveryRequirement::None, None, None, 7),
            (WorkItemState::Running, DeliveryRequirement::None, None, None, 5),
            (WorkItemState::Submitted, DeliveryRequirement::None, inline(), None, 6),
            (WorkItemState::Reviewing, DeliveryRequirement::None, inline(), None, 7),
            (WorkItemState::Accepted, DeliveryRequirement::None, inline(), None, 1),
            (WorkItemState::Completed, DeliveryRequirement::None, inline(), None, 7),
            (WorkItemState::Cancelled, DeliveryRequirement::None, inline(), None, 3),
        ] {
            assert_eq!(
                WorkItemLifecycle::restore(
                    TEAM_ID,
                    WORK_ITEM_ID,
                    state,
                    requirement,
                    None,
                    submission,
                    accepted,
                    revision,
                ),
                Err(WorkItemRestoreError::InvalidRevision {
                    state,
                    actual: revision,
                })
            );
        }
    }

    #[test]
    fn restore_accepts_revisions_reached_after_review_cycles() {
        for revision in [1, 6, 8, 10, 11] {
            assert!(
                WorkItemLifecycle::restore(
                    TEAM_ID,
                    WORK_ITEM_ID,
                    WorkItemState::Queued,
                    DeliveryRequirement::None,
                    None,
                    None,
                    None,
                    revision,
                )
                .is_ok(),
                "queued revision {revision} should be reachable"
            );
        }
        for revision in [5, 6, 7, 8, 9] {
            assert!(
                WorkItemLifecycle::restore(
                    TEAM_ID,
                    WORK_ITEM_ID,
                    WorkItemState::ChangesRequested,
                    DeliveryRequirement::Git,
                    Some(git_assignment()),
                    None,
                    None,
                    revision,
                )
                .is_ok(),
                "Git changes-requested revision {revision} should be reachable"
            );
        }
    }
}
