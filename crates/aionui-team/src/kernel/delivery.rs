use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Whether a WorkItem needs a Git delivery before it can complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryRequirement {
    None,
    Git,
}

/// Exact repository coordinates assigned to a Git WorkItem.
///
/// The assignment is chosen when work is delegated and remains immutable for
/// the lifetime of the WorkItem. A submission may advance `head_commit`, but
/// it cannot switch repositories, bases, or source branches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitWorkAssignment {
    repository_id: String,
    base_commit: String,
    branch_ref: String,
}

impl GitWorkAssignment {
    pub fn new(
        repository_id: impl Into<String>,
        base_commit: impl Into<String>,
        branch_ref: impl Into<String>,
    ) -> Self {
        Self {
            repository_id: repository_id.into(),
            base_commit: base_commit.into(),
            branch_ref: branch_ref.into(),
        }
    }

    pub fn repository_id(&self) -> &str {
        &self.repository_id
    }

    pub fn base_commit(&self) -> &str {
        &self.base_commit
    }

    pub fn branch_ref(&self) -> &str {
        &self.branch_ref
    }

    pub(crate) fn empty_field(&self) -> Option<&'static str> {
        [
            ("repository_id", self.repository_id.as_str()),
            ("base_commit", self.base_commit.as_str()),
            ("branch_ref", self.branch_ref.as_str()),
        ]
        .into_iter()
        .find_map(|(field, value)| value.trim().is_empty().then_some(field))
    }

    pub(crate) fn submission_mismatch(&self, delivery: &GitDeliveryRef) -> Option<&'static str> {
        if self.repository_id != delivery.repository_id {
            Some("repository_id")
        } else if self.base_commit != delivery.base_commit {
            Some("base_commit")
        } else if self.branch_ref != delivery.branch_ref {
            Some("branch_ref")
        } else {
            None
        }
    }
}

/// Immutable content identity for one submitted Git delivery attempt.
///
/// A changed head commit creates a new delivery attempt instead of mutating
/// this reference. Review acceptance therefore remains bound to exact content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GitDeliveryRef {
    delivery_id: String,
    team_id: String,
    work_item_id: String,
    producer_member_id: String,
    repository_id: String,
    content_revision: u64,
    base_commit: String,
    branch_ref: String,
    head_commit: String,
}

impl GitDeliveryRef {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        delivery_id: impl Into<String>,
        team_id: impl Into<String>,
        work_item_id: impl Into<String>,
        producer_member_id: impl Into<String>,
        repository_id: impl Into<String>,
        content_revision: u64,
        base_commit: impl Into<String>,
        branch_ref: impl Into<String>,
        head_commit: impl Into<String>,
    ) -> Self {
        Self {
            delivery_id: delivery_id.into(),
            team_id: team_id.into(),
            work_item_id: work_item_id.into(),
            producer_member_id: producer_member_id.into(),
            repository_id: repository_id.into(),
            content_revision,
            base_commit: base_commit.into(),
            branch_ref: branch_ref.into(),
            head_commit: head_commit.into(),
        }
    }

    pub fn delivery_id(&self) -> &str {
        &self.delivery_id
    }

    pub fn work_item_id(&self) -> &str {
        &self.work_item_id
    }

    pub fn team_id(&self) -> &str {
        &self.team_id
    }

    pub fn producer_member_id(&self) -> &str {
        &self.producer_member_id
    }

    pub fn repository_id(&self) -> &str {
        &self.repository_id
    }

    pub const fn content_revision(&self) -> u64 {
        self.content_revision
    }

    pub fn base_commit(&self) -> &str {
        &self.base_commit
    }

    pub fn branch_ref(&self) -> &str {
        &self.branch_ref
    }

    pub fn head_commit(&self) -> &str {
        &self.head_commit
    }
}

/// The result currently submitted for WorkItem review.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WorkSubmission {
    Inline { producer_member_id: String },
    Git { delivery: GitDeliveryRef },
}

impl WorkSubmission {
    pub fn inline(producer_member_id: impl Into<String>) -> Self {
        Self::Inline {
            producer_member_id: producer_member_id.into(),
        }
    }

    pub const fn git(delivery: GitDeliveryRef) -> Self {
        Self::Git { delivery }
    }

    pub fn producer_member_id(&self) -> &str {
        match self {
            Self::Inline { producer_member_id } => producer_member_id,
            Self::Git { delivery } => delivery.producer_member_id(),
        }
    }

    pub const fn git_delivery(&self) -> Option<&GitDeliveryRef> {
        match self {
            Self::Inline { .. } => None,
            Self::Git { delivery } => Some(delivery),
        }
    }
}

/// Lifecycle of one immutable Git delivery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GitDeliveryState {
    Submitted,
    Accepted,
    Integrating,
    Conflicted,
    Merged,
    Superseded,
    Rejected,
    Abandoned,
}

impl GitDeliveryState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Submitted => "submitted",
            Self::Accepted => "accepted",
            Self::Integrating => "integrating",
            Self::Conflicted => "conflicted",
            Self::Merged => "merged",
            Self::Superseded => "superseded",
            Self::Rejected => "rejected",
            Self::Abandoned => "abandoned",
        }
    }

    pub const fn is_terminal_attempt(self) -> bool {
        matches!(
            self,
            Self::Conflicted | Self::Merged | Self::Superseded | Self::Rejected | Self::Abandoned
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GitDeliveryTransition {
    Accept,
    Supersede,
    Reject,
    Abandon,
    BeginIntegration,
    ReturnToAccepted { reason: IntegrationRecoveryReason },
    MarkMerged { merged_commit: String },
    MarkConflicted,
}

/// Why an integration lease returned to the accepted, retryable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegrationRecoveryReason {
    Interrupted,
    RetryableInfrastructure,
    PreconditionChanged,
}

impl IntegrationRecoveryReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Interrupted => "interrupted",
            Self::RetryableInfrastructure => "retryable_infrastructure",
            Self::PreconditionChanged => "precondition_changed",
        }
    }
}

/// Versioned state for a single immutable delivery reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct GitDeliveryLifecycle {
    delivery: GitDeliveryRef,
    state: GitDeliveryState,
    revision: u64,
    merged_commit: Option<String>,
}

// Phase 2 command services will become the first non-test lifecycle driver.
#[cfg_attr(not(test), allow(dead_code))]
impl GitDeliveryLifecycle {
    pub(crate) const fn submitted(delivery: GitDeliveryRef) -> Self {
        Self {
            delivery,
            state: GitDeliveryState::Submitted,
            revision: 0,
            merged_commit: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn restore(
        delivery: GitDeliveryRef,
        state: GitDeliveryState,
        revision: u64,
        merged_commit: Option<String>,
    ) -> Result<Self, GitDeliveryRestoreError> {
        let revision_is_valid = match state {
            GitDeliveryState::Submitted => revision == 0,
            GitDeliveryState::Accepted => revision >= 1 && !revision.is_multiple_of(2),
            GitDeliveryState::Integrating => revision >= 2 && revision.is_multiple_of(2),
            GitDeliveryState::Conflicted | GitDeliveryState::Merged => revision >= 3 && !revision.is_multiple_of(2),
            GitDeliveryState::Superseded | GitDeliveryState::Rejected => revision == 1,
            GitDeliveryState::Abandoned => revision == 1 || (revision >= 2 && revision.is_multiple_of(2)),
        };
        if !revision_is_valid {
            return Err(GitDeliveryRestoreError::InvalidRevision {
                state,
                actual: revision,
            });
        }
        match (state, merged_commit.as_deref()) {
            (GitDeliveryState::Merged, Some(commit)) if !commit.is_empty() => {}
            (GitDeliveryState::Merged, _) => return Err(GitDeliveryRestoreError::MergedCommitMissing),
            (_, Some(_)) => return Err(GitDeliveryRestoreError::UnexpectedMergedCommit { state }),
            (_, None) => {}
        }
        Ok(Self {
            delivery,
            state,
            revision,
            merged_commit,
        })
    }

    pub const fn delivery(&self) -> &GitDeliveryRef {
        &self.delivery
    }

    pub const fn state(&self) -> GitDeliveryState {
        self.state
    }

    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub fn merged_commit(&self) -> Option<&str> {
        self.merged_commit.as_deref()
    }

    pub(crate) fn apply(
        &mut self,
        expected_revision: u64,
        transition: GitDeliveryTransition,
    ) -> Result<GitDeliveryState, GitDeliveryTransitionError> {
        if expected_revision != self.revision {
            return Err(GitDeliveryTransitionError::RevisionConflict {
                expected: expected_revision,
                actual: self.revision,
            });
        }

        let (next_state, merged_commit) = match (self.state, transition.clone()) {
            (GitDeliveryState::Submitted, GitDeliveryTransition::Accept) => (GitDeliveryState::Accepted, None),
            (GitDeliveryState::Submitted, GitDeliveryTransition::Supersede) => (GitDeliveryState::Superseded, None),
            (GitDeliveryState::Submitted, GitDeliveryTransition::Reject) => (GitDeliveryState::Rejected, None),
            (GitDeliveryState::Submitted, GitDeliveryTransition::Abandon) => (GitDeliveryState::Abandoned, None),
            (GitDeliveryState::Accepted, GitDeliveryTransition::Abandon) => (GitDeliveryState::Abandoned, None),
            (GitDeliveryState::Accepted, GitDeliveryTransition::BeginIntegration) => {
                (GitDeliveryState::Integrating, None)
            }
            (GitDeliveryState::Integrating, GitDeliveryTransition::ReturnToAccepted { .. }) => {
                (GitDeliveryState::Accepted, None)
            }
            (GitDeliveryState::Integrating, GitDeliveryTransition::MarkMerged { merged_commit })
                if merged_commit.is_empty() =>
            {
                return Err(GitDeliveryTransitionError::MergedCommitMissing);
            }
            (GitDeliveryState::Integrating, GitDeliveryTransition::MarkMerged { merged_commit }) => {
                (GitDeliveryState::Merged, Some(merged_commit))
            }
            (GitDeliveryState::Integrating, GitDeliveryTransition::MarkConflicted) => {
                (GitDeliveryState::Conflicted, None)
            }
            (state, _) => {
                return Err(GitDeliveryTransitionError::InvalidTransition { state, transition });
            }
        };

        let next_revision = self
            .revision
            .checked_add(1)
            .ok_or(GitDeliveryTransitionError::RevisionOverflow)?;
        self.state = next_state;
        self.revision = next_revision;
        self.merged_commit = merged_commit;
        Ok(next_state)
    }

    /// Produce completion evidence only after this exact delivery is merged.
    pub(crate) fn merged_evidence(&self) -> Option<MergedDeliveryEvidence> {
        if self.state != GitDeliveryState::Merged {
            return None;
        }
        Some(MergedDeliveryEvidence {
            delivery: self.delivery.clone(),
            delivery_revision: self.revision,
            merged_commit: self.merged_commit.clone()?,
        })
    }

    /// Produce conflict evidence only after this exact delivery conflicts.
    pub(crate) fn conflicted_evidence(&self) -> Option<ConflictedDeliveryEvidence> {
        (self.state == GitDeliveryState::Conflicted).then(|| ConflictedDeliveryEvidence {
            delivery: self.delivery.clone(),
            delivery_revision: self.revision,
        })
    }
}

/// Opaque proof created only from a merged `GitDeliveryLifecycle`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergedDeliveryEvidence {
    delivery: GitDeliveryRef,
    delivery_revision: u64,
    merged_commit: String,
}

impl MergedDeliveryEvidence {
    pub const fn delivery(&self) -> &GitDeliveryRef {
        &self.delivery
    }

    pub const fn delivery_revision(&self) -> u64 {
        self.delivery_revision
    }

    pub fn merged_commit(&self) -> &str {
        &self.merged_commit
    }
}

/// Opaque proof created only from a conflicted `GitDeliveryLifecycle`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictedDeliveryEvidence {
    delivery: GitDeliveryRef,
    delivery_revision: u64,
}

impl ConflictedDeliveryEvidence {
    pub const fn delivery(&self) -> &GitDeliveryRef {
        &self.delivery
    }

    pub const fn delivery_revision(&self) -> u64 {
        self.delivery_revision
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GitDeliveryTransitionError {
    #[error("expected delivery revision {expected}, but current revision is {actual}")]
    RevisionConflict { expected: u64, actual: u64 },
    #[error("cannot apply {transition:?} while delivery is {state:?}")]
    InvalidTransition {
        state: GitDeliveryState,
        transition: GitDeliveryTransition,
    },
    #[error("merged delivery requires a non-empty merged_commit")]
    MergedCommitMissing,
    #[error("delivery revision overflow")]
    RevisionOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum GitDeliveryRestoreError {
    #[error("{state:?} delivery has invalid lifecycle revision {actual}")]
    InvalidRevision { state: GitDeliveryState, actual: u64 },
    #[error("merged delivery snapshot is missing merged_commit")]
    MergedCommitMissing,
    #[error("{state:?} delivery snapshot cannot contain merged_commit")]
    UnexpectedMergedCommit { state: GitDeliveryState },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delivery_ref(id: &str, head: &str) -> GitDeliveryRef {
        GitDeliveryRef::new(
            id,
            "team-1",
            "work-1",
            "worker",
            "repo-1",
            1,
            "base",
            "refs/heads/ds/work-1/worker",
            head,
        )
    }

    fn apply(lifecycle: &mut GitDeliveryLifecycle, transition: GitDeliveryTransition) -> GitDeliveryState {
        lifecycle.apply(lifecycle.revision(), transition).unwrap()
    }

    #[test]
    fn merged_evidence_is_bound_to_exact_delivery_content() {
        let reference = delivery_ref("delivery-1", "head-1");
        let mut delivery = GitDeliveryLifecycle::submitted(reference.clone());
        apply(&mut delivery, GitDeliveryTransition::Accept);
        apply(&mut delivery, GitDeliveryTransition::BeginIntegration);
        apply(
            &mut delivery,
            GitDeliveryTransition::MarkMerged {
                merged_commit: "merge-1".into(),
            },
        );

        let evidence = delivery.merged_evidence().unwrap();
        assert_eq!(evidence.delivery(), &reference);
        assert_eq!(evidence.delivery_revision(), 3);
        assert_eq!(evidence.merged_commit(), "merge-1");
    }

    #[test]
    fn conflict_is_terminal_for_one_delivery_attempt() {
        let mut delivery = GitDeliveryLifecycle::submitted(delivery_ref("delivery-1", "head-1"));
        apply(&mut delivery, GitDeliveryTransition::Accept);
        apply(&mut delivery, GitDeliveryTransition::BeginIntegration);
        apply(&mut delivery, GitDeliveryTransition::MarkConflicted);

        assert!(delivery.state().is_terminal_attempt());
        assert!(delivery.conflicted_evidence().is_some());
        assert!(delivery.merged_evidence().is_none());
    }

    #[test]
    fn review_outcomes_close_unintegrated_delivery_attempts() {
        for (transition, terminal) in [
            (GitDeliveryTransition::Supersede, GitDeliveryState::Superseded),
            (GitDeliveryTransition::Reject, GitDeliveryState::Rejected),
            (GitDeliveryTransition::Abandon, GitDeliveryState::Abandoned),
        ] {
            let mut delivery = GitDeliveryLifecycle::submitted(delivery_ref("delivery-1", "head-1"));
            assert_eq!(apply(&mut delivery, transition), terminal);
            assert!(delivery.state().is_terminal_attempt());
            assert!(matches!(
                delivery.apply(delivery.revision(), GitDeliveryTransition::Accept),
                Err(GitDeliveryTransitionError::InvalidTransition { .. })
            ));
        }
    }

    #[test]
    fn accepted_delivery_can_be_abandoned_before_integration() {
        let mut delivery = GitDeliveryLifecycle::submitted(delivery_ref("delivery-1", "head-1"));
        apply(&mut delivery, GitDeliveryTransition::Accept);
        assert_eq!(
            apply(&mut delivery, GitDeliveryTransition::Abandon),
            GitDeliveryState::Abandoned
        );
        assert!(delivery.state().is_terminal_attempt());

        let restored =
            GitDeliveryLifecycle::restore(delivery.delivery().clone(), delivery.state(), delivery.revision(), None)
                .unwrap();
        assert_eq!(restored, delivery);
    }

    #[test]
    fn stale_delivery_revision_does_not_mutate_state() {
        let mut delivery = GitDeliveryLifecycle::submitted(delivery_ref("delivery-1", "head-1"));
        apply(&mut delivery, GitDeliveryTransition::Accept);
        assert_eq!(
            delivery.apply(0, GitDeliveryTransition::BeginIntegration),
            Err(GitDeliveryTransitionError::RevisionConflict { expected: 0, actual: 1 })
        );
        assert_eq!(delivery.state(), GitDeliveryState::Accepted);
    }

    #[test]
    fn retryable_integration_failure_returns_to_accepted() {
        let mut delivery = GitDeliveryLifecycle::submitted(delivery_ref("delivery-1", "head-1"));
        apply(&mut delivery, GitDeliveryTransition::Accept);
        apply(&mut delivery, GitDeliveryTransition::BeginIntegration);
        assert_eq!(
            apply(
                &mut delivery,
                GitDeliveryTransition::ReturnToAccepted {
                    reason: IntegrationRecoveryReason::RetryableInfrastructure,
                }
            ),
            GitDeliveryState::Accepted
        );
        assert_eq!(
            apply(&mut delivery, GitDeliveryTransition::BeginIntegration),
            GitDeliveryState::Integrating
        );
    }

    #[test]
    fn empty_merge_commit_cannot_generate_completion_evidence() {
        let mut delivery = GitDeliveryLifecycle::submitted(delivery_ref("delivery-1", "head-1"));
        apply(&mut delivery, GitDeliveryTransition::Accept);
        apply(&mut delivery, GitDeliveryTransition::BeginIntegration);
        assert_eq!(
            delivery.apply(
                delivery.revision(),
                GitDeliveryTransition::MarkMerged {
                    merged_commit: String::new(),
                }
            ),
            Err(GitDeliveryTransitionError::MergedCommitMissing)
        );
        assert_eq!(delivery.state(), GitDeliveryState::Integrating);
        assert!(delivery.merged_evidence().is_none());
    }

    #[test]
    fn restore_rejects_forged_merged_snapshot() {
        assert_eq!(
            GitDeliveryLifecycle::restore(
                delivery_ref("delivery-1", "head-1"),
                GitDeliveryState::Merged,
                2,
                Some("merge-1".into()),
            ),
            Err(GitDeliveryRestoreError::InvalidRevision {
                state: GitDeliveryState::Merged,
                actual: 2,
            })
        );
        assert_eq!(
            GitDeliveryLifecycle::restore(delivery_ref("delivery-1", "head-1"), GitDeliveryState::Merged, 3, None,),
            Err(GitDeliveryRestoreError::MergedCommitMissing)
        );
    }
}
