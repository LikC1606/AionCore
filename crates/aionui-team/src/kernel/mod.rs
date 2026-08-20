//! Stable collaboration semantics shared by Team Mode adapters.
//!
//! This module intentionally contains no database, transport, runtime, or Git
//! implementation. Those integrations may change independently, while the
//! relation policy and lifecycle rules remain deterministic and testable.

mod delivery;
mod policy;
mod relation;
mod work_item;

pub use delivery::{
    ConflictedDeliveryEvidence, DeliveryRequirement, GitDeliveryLifecycle, GitDeliveryRef, GitDeliveryRestoreError,
    GitDeliveryState, GitDeliveryTransition, GitDeliveryTransitionError, GitWorkAssignment, IntegrationRecoveryReason,
    MergedDeliveryEvidence, WorkSubmission,
};
pub use policy::{
    RelationPolicy, RelationPolicyError, WorkItemAction, WorkItemPolicy, WorkItemPolicyError, WorkItemPolicyView,
    WorkItemRole,
};
pub use relation::{MemberRelation, RelationAction};
pub use work_item::{
    WorkItemLifecycle, WorkItemRestoreError, WorkItemState, WorkItemTransition, WorkItemTransitionError,
};
