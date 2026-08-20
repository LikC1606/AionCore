use aionui_common::TimestampMs;
use aionui_db::{TeamGitDeliveryRow, TeamWorkItemRow};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::kernel::{
    DeliveryRequirement, GitDeliveryLifecycle, GitDeliveryRef, GitDeliveryState, GitWorkAssignment, WorkItemLifecycle,
    WorkItemState, WorkSubmission,
};

pub(super) struct NewWorkItemRecord {
    pub id: String,
    pub team_id: String,
    pub parent_work_item_id: Option<String>,
    pub subject: String,
    pub description: Option<String>,
    pub controller_member_id: String,
    pub assignee_member_id: String,
    pub reviewer_member_id: String,
    pub integrator_member_id: Option<String>,
    pub created_at: TimestampMs,
}

pub(super) fn create_work_item_row(
    record: NewWorkItemRecord,
    lifecycle: &WorkItemLifecycle,
) -> Result<TeamWorkItemRow, SnapshotError> {
    if record.id != lifecycle.work_item_id() || record.team_id != lifecycle.team_id() {
        return Err(SnapshotError::IdentityMismatch("new WorkItem metadata"));
    }
    Ok(TeamWorkItemRow {
        id: record.id,
        team_id: record.team_id,
        parent_work_item_id: record.parent_work_item_id,
        subject: record.subject,
        description: record.description,
        controller_member_id: record.controller_member_id,
        assignee_member_id: record.assignee_member_id,
        reviewer_member_id: record.reviewer_member_id,
        integrator_member_id: record.integrator_member_id,
        delivery_requirement: encode_string_enum(&lifecycle.delivery_requirement())?,
        git_repository_id: lifecycle
            .git_assignment()
            .map(|assignment| assignment.repository_id().to_owned()),
        git_base_commit: lifecycle
            .git_assignment()
            .map(|assignment| assignment.base_commit().to_owned()),
        git_branch_ref: lifecycle
            .git_assignment()
            .map(|assignment| assignment.branch_ref().to_owned()),
        state: encode_string_enum(&lifecycle.state())?,
        current_submission_json: encode_optional(lifecycle.current_submission())?,
        accepted_delivery_json: encode_optional(lifecycle.accepted_delivery())?,
        revision: encode_revision(lifecycle.revision())?,
        created_at: record.created_at,
        updated_at: record.created_at,
    })
}

pub(super) fn update_work_item_row(
    mut row: TeamWorkItemRow,
    lifecycle: &WorkItemLifecycle,
    updated_at: TimestampMs,
) -> Result<TeamWorkItemRow, SnapshotError> {
    if row.id != lifecycle.work_item_id() || row.team_id != lifecycle.team_id() {
        return Err(SnapshotError::IdentityMismatch("updated WorkItem snapshot"));
    }
    row.delivery_requirement = encode_string_enum(&lifecycle.delivery_requirement())?;
    row.git_repository_id = lifecycle
        .git_assignment()
        .map(|assignment| assignment.repository_id().to_owned());
    row.git_base_commit = lifecycle
        .git_assignment()
        .map(|assignment| assignment.base_commit().to_owned());
    row.git_branch_ref = lifecycle
        .git_assignment()
        .map(|assignment| assignment.branch_ref().to_owned());
    row.state = encode_string_enum(&lifecycle.state())?;
    row.current_submission_json = encode_optional(lifecycle.current_submission())?;
    row.accepted_delivery_json = encode_optional(lifecycle.accepted_delivery())?;
    row.revision = encode_revision(lifecycle.revision())?;
    row.updated_at = updated_at;
    Ok(row)
}

pub(super) fn restore_work_item(row: &TeamWorkItemRow) -> Result<WorkItemLifecycle, SnapshotError> {
    let state = decode_string_enum::<WorkItemState>(&row.state, "WorkItem state")?;
    let requirement = decode_string_enum::<DeliveryRequirement>(&row.delivery_requirement, "delivery requirement")?;
    let git_assignment = decode_git_assignment(row)?;
    let current_submission = decode_optional::<WorkSubmission>(row.current_submission_json.as_deref(), "submission")?;
    let accepted_delivery =
        decode_optional::<GitDeliveryRef>(row.accepted_delivery_json.as_deref(), "accepted delivery")?;
    let revision = decode_revision(row.revision, "WorkItem")?;
    WorkItemLifecycle::restore(
        row.team_id.clone(),
        row.id.clone(),
        state,
        requirement,
        git_assignment,
        current_submission,
        accepted_delivery,
        revision,
    )
    .map_err(|error| SnapshotError::InvalidAggregate {
        aggregate: "WorkItem",
        reason: error.to_string(),
    })
}

fn decode_git_assignment(row: &TeamWorkItemRow) -> Result<Option<GitWorkAssignment>, SnapshotError> {
    match (
        row.git_repository_id.as_deref(),
        row.git_base_commit.as_deref(),
        row.git_branch_ref.as_deref(),
    ) {
        (None, None, None) => Ok(None),
        (Some(repository_id), Some(base_commit), Some(branch_ref)) => {
            Ok(Some(GitWorkAssignment::new(repository_id, base_commit, branch_ref)))
        }
        _ => Err(SnapshotError::InvalidAggregate {
            aggregate: "WorkItem",
            reason: "Git assignment fields must be present together".into(),
        }),
    }
}

pub(super) fn create_delivery_row(
    lifecycle: &GitDeliveryLifecycle,
    created_at: TimestampMs,
) -> Result<TeamGitDeliveryRow, SnapshotError> {
    let delivery = lifecycle.delivery();
    validate_delivery_identity(delivery)?;
    Ok(TeamGitDeliveryRow {
        id: delivery.delivery_id().to_owned(),
        team_id: delivery.team_id().to_owned(),
        work_item_id: delivery.work_item_id().to_owned(),
        producer_member_id: delivery.producer_member_id().to_owned(),
        repository_id: delivery.repository_id().to_owned(),
        content_revision: encode_content_revision(delivery.content_revision())?,
        base_commit: delivery.base_commit().to_owned(),
        branch_ref: delivery.branch_ref().to_owned(),
        head_commit: delivery.head_commit().to_owned(),
        state: encode_string_enum(&lifecycle.state())?,
        revision: encode_revision(lifecycle.revision())?,
        merged_commit: lifecycle.merged_commit().map(str::to_owned),
        created_at,
        updated_at: created_at,
    })
}

pub(super) fn update_delivery_row(
    mut row: TeamGitDeliveryRow,
    lifecycle: &GitDeliveryLifecycle,
    updated_at: TimestampMs,
) -> Result<TeamGitDeliveryRow, SnapshotError> {
    let delivery = lifecycle.delivery();
    if !row_identity_matches_delivery(&row, delivery)? {
        return Err(SnapshotError::IdentityMismatch("updated Git delivery snapshot"));
    }
    row.state = encode_string_enum(&lifecycle.state())?;
    row.revision = encode_revision(lifecycle.revision())?;
    row.merged_commit = lifecycle.merged_commit().map(str::to_owned);
    row.updated_at = updated_at;
    Ok(row)
}

pub(super) fn restore_delivery(row: &TeamGitDeliveryRow) -> Result<GitDeliveryLifecycle, SnapshotError> {
    let content_revision = decode_content_revision(row.content_revision)?;
    let delivery = GitDeliveryRef::new(
        row.id.clone(),
        row.team_id.clone(),
        row.work_item_id.clone(),
        row.producer_member_id.clone(),
        row.repository_id.clone(),
        content_revision,
        row.base_commit.clone(),
        row.branch_ref.clone(),
        row.head_commit.clone(),
    );
    validate_delivery_identity(&delivery)?;
    let state = decode_string_enum::<GitDeliveryState>(&row.state, "Git delivery state")?;
    let revision = decode_revision(row.revision, "Git delivery")?;
    GitDeliveryLifecycle::restore(delivery, state, revision, row.merged_commit.clone()).map_err(|error| {
        SnapshotError::InvalidAggregate {
            aggregate: "Git delivery",
            reason: error.to_string(),
        }
    })
}

pub(super) fn row_identity_matches_delivery(
    row: &TeamGitDeliveryRow,
    delivery: &GitDeliveryRef,
) -> Result<bool, SnapshotError> {
    Ok(row.id == delivery.delivery_id()
        && row.team_id == delivery.team_id()
        && row.work_item_id == delivery.work_item_id()
        && row.producer_member_id == delivery.producer_member_id()
        && row.repository_id == delivery.repository_id()
        && decode_content_revision(row.content_revision)? == delivery.content_revision()
        && row.base_commit == delivery.base_commit()
        && row.branch_ref == delivery.branch_ref()
        && row.head_commit == delivery.head_commit())
}

fn validate_delivery_identity(delivery: &GitDeliveryRef) -> Result<(), SnapshotError> {
    for (field, value) in [
        ("delivery_id", delivery.delivery_id()),
        ("team_id", delivery.team_id()),
        ("work_item_id", delivery.work_item_id()),
        ("producer_member_id", delivery.producer_member_id()),
        ("repository_id", delivery.repository_id()),
        ("base_commit", delivery.base_commit()),
        ("branch_ref", delivery.branch_ref()),
        ("head_commit", delivery.head_commit()),
    ] {
        if value.trim().is_empty() {
            return Err(SnapshotError::EmptyDeliveryIdentity { field });
        }
    }
    if delivery.content_revision() == 0 {
        return Err(SnapshotError::InvalidContentRevision(0));
    }
    Ok(())
}

fn encode_optional<T: Serialize>(value: Option<&T>) -> Result<Option<String>, SnapshotError> {
    value
        .map(serde_json::to_string)
        .transpose()
        .map_err(|error| SnapshotError::Json(error.to_string()))
}

fn decode_optional<T: DeserializeOwned>(raw: Option<&str>, field: &'static str) -> Result<Option<T>, SnapshotError> {
    raw.map(|value| {
        serde_json::from_str(value).map_err(|error| SnapshotError::InvalidJsonField {
            field,
            reason: error.to_string(),
        })
    })
    .transpose()
}

fn encode_string_enum<T: Serialize>(value: &T) -> Result<String, SnapshotError> {
    match serde_json::to_value(value).map_err(|error| SnapshotError::Json(error.to_string()))? {
        serde_json::Value::String(value) => Ok(value),
        _ => Err(SnapshotError::EnumEncoding),
    }
}

fn decode_string_enum<T: DeserializeOwned>(raw: &str, field: &'static str) -> Result<T, SnapshotError> {
    serde_json::from_value(serde_json::Value::String(raw.to_owned())).map_err(|error| SnapshotError::InvalidEnum {
        field,
        value: raw.to_owned(),
        reason: error.to_string(),
    })
}

fn encode_revision(revision: u64) -> Result<i64, SnapshotError> {
    i64::try_from(revision).map_err(|_| SnapshotError::RevisionOverflow(revision))
}

fn decode_revision(revision: i64, aggregate: &'static str) -> Result<u64, SnapshotError> {
    u64::try_from(revision).map_err(|_| SnapshotError::NegativeRevision {
        aggregate,
        actual: revision,
    })
}

fn encode_content_revision(revision: u64) -> Result<i64, SnapshotError> {
    if revision == 0 {
        return Err(SnapshotError::InvalidContentRevision(revision));
    }
    i64::try_from(revision).map_err(|_| SnapshotError::ContentRevisionOverflow(revision))
}

fn decode_content_revision(revision: i64) -> Result<u64, SnapshotError> {
    let revision = u64::try_from(revision).map_err(|_| SnapshotError::InvalidContentRevision(0))?;
    if revision == 0 {
        return Err(SnapshotError::InvalidContentRevision(0));
    }
    Ok(revision)
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum SnapshotError {
    #[error("snapshot JSON encoding failed: {0}")]
    Json(String),
    #[error("{field} snapshot JSON is invalid: {reason}")]
    InvalidJsonField { field: &'static str, reason: String },
    #[error("snapshot enum must encode as a string")]
    EnumEncoding,
    #[error("{field} value {value:?} is invalid: {reason}")]
    InvalidEnum {
        field: &'static str,
        value: String,
        reason: String,
    },
    #[error("{aggregate} snapshot has negative revision {actual}")]
    NegativeRevision { aggregate: &'static str, actual: i64 },
    #[error("lifecycle revision {0} cannot be persisted")]
    RevisionOverflow(u64),
    #[error("Git content revision {0} is invalid")]
    InvalidContentRevision(u64),
    #[error("Git content revision {0} cannot be persisted")]
    ContentRevisionOverflow(u64),
    #[error("Git delivery identity field {field} is empty")]
    EmptyDeliveryIdentity { field: &'static str },
    #[error("{0} identity does not match its lifecycle")]
    IdentityMismatch(&'static str),
    #[error("{aggregate} snapshot is invalid: {reason}")]
    InvalidAggregate { aggregate: &'static str, reason: String },
}
