use serde::{Deserialize, Serialize};

/// The target member's relation to the acting member.
///
/// For example, `Subordinate` means that the target is subordinate to the
/// actor. `SelfMember` and `Unrelated` are projections and must not be stored as
/// relation records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberRelation {
    SelfMember,
    Superior,
    Peer,
    Subordinate,
    Unrelated,
}

impl MemberRelation {
    /// Return the same relationship from the other member's perspective.
    pub const fn inverse(self) -> Self {
        match self {
            Self::SelfMember => Self::SelfMember,
            Self::Superior => Self::Subordinate,
            Self::Peer => Self::Peer,
            Self::Subordinate => Self::Superior,
            Self::Unrelated => Self::Unrelated,
        }
    }
}

/// An interaction whose eligibility can be narrowed by a member relationship.
///
/// A positive relationship decision is necessary but not sufficient. Resource
/// ownership, lifecycle, revision, and delivery checks still apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationAction {
    InspectScopedWork,
    DirectMessage,
    DelegateWork,
    SubmitResult,
    ReportBlocked,
    RequestDecision,
    ProposeCollaboration,
    ReviewResult,
    RequestChanges,
    AcceptResult,
    RejectResult,
    IntegrateDelivery,
    CancelControlledWork,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inverse_preserves_symmetric_relations() {
        assert_eq!(MemberRelation::SelfMember.inverse(), MemberRelation::SelfMember);
        assert_eq!(MemberRelation::Peer.inverse(), MemberRelation::Peer);
        assert_eq!(MemberRelation::Unrelated.inverse(), MemberRelation::Unrelated);
    }

    #[test]
    fn inverse_reverses_supervision_direction() {
        assert_eq!(MemberRelation::Superior.inverse(), MemberRelation::Subordinate);
        assert_eq!(MemberRelation::Subordinate.inverse(), MemberRelation::Superior);
    }
}
