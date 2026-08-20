use aionui_common::{generate_prefixed_id, now_ms};
use aionui_db::{
    TeamGitDeliveryMutation, TeamGitIntegrationAttemptRow, TeamGitIntegrationMutation, TeamGitIntegrationResolution,
    TeamWorkItemMutation,
};

use super::model::{
    ConflictedIntegrationEvidence, MergedIntegrationEvidence, RetryableIntegrationEvidence, TeamCommand,
    TeamCommandError, TeamCommandReceipt,
};
use super::service::{
    LoadedCommandAggregate, LoadedDelivery, ResolvedCommandActor, TeamCommandService, TeamNotificationDraft,
    command_result, revision_to_i64,
};
use super::snapshot::{create_delivery_row, update_delivery_row, update_work_item_row};
use crate::kernel::{
    DeliveryRequirement, GitDeliveryLifecycle, GitDeliveryRef, GitDeliveryTransition, RelationAction, RelationPolicy,
    WorkItemAction, WorkItemLifecycle, WorkItemPolicy, WorkItemPolicyView, WorkItemTransition, WorkSubmission,
};

impl TeamCommandService {
    pub(super) async fn execute_existing_inner(
        &self,
        actor: &ResolvedCommandActor,
        idempotency_key: &str,
        fingerprint: String,
        command: TeamCommand,
    ) -> Result<TeamCommandReceipt, TeamCommandError> {
        let work_item_id = command
            .work_item_id()
            .ok_or_else(|| TeamCommandError::InvalidCommand("missing WorkItem id".into()))?;
        let mut aggregate = self.load_aggregate(&actor.team_id, work_item_id).await?;
        let original_work_revision = aggregate.work.revision();
        let original_delivery_revision = aggregate
            .delivery
            .as_ref()
            .map(|delivery| delivery.lifecycle.revision());
        let now = now_ms();
        let command_name = command.name();
        let mut work_changed = false;
        let mut delivery_changed = false;
        let mut delivery_inserted = false;
        let mut integration_mutation = None;
        let mut integration_attempt_id = None;
        let mut notification_drafts = Vec::new();

        match command {
            TeamCommand::Queue {
                expected_work_revision,
                prepared_workspace,
                ..
            } => {
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Queue,
                    None,
                    Some((aggregate.row.assignee_member_id.as_str(), RelationAction::DelegateWork)),
                )?;
                aggregate
                    .work
                    .apply(expected_work_revision, WorkItemTransition::Queue)?;
                let content = match (aggregate.work.delivery_requirement(), prepared_workspace) {
                    (DeliveryRequirement::Git, Some(workspace)) if !workspace.trim().is_empty() => format!(
                        "Canonical WorkItem `{}` is queued for you. Call `team_inspect`, then work only in the prepared Git workspace `{workspace}`.",
                        aggregate.work.work_item_id()
                    ),
                    (DeliveryRequirement::Git, Some(_)) => {
                        return Err(TeamCommandError::InvalidCommand(
                            "prepared Git workspace must not be empty".into(),
                        ));
                    }
                    (DeliveryRequirement::Git, None) => {
                        return Err(TeamCommandError::InvalidCommand(
                            "Git queue command requires the prepared workspace".into(),
                        ));
                    }
                    (DeliveryRequirement::None, Some(_)) => {
                        return Err(TeamCommandError::InvalidCommand(
                            "non-Git queue command cannot include a prepared workspace".into(),
                        ));
                    }
                    (DeliveryRequirement::None, None) => format!(
                        "Canonical WorkItem `{}` is queued for you. Call `team_inspect` for its scope and exact revision before acting.",
                        aggregate.work.work_item_id()
                    ),
                };
                notification_drafts.push(TeamNotificationDraft::new(
                    aggregate.row.assignee_member_id.clone(),
                    content,
                ));
                work_changed = true;
            }
            TeamCommand::Start {
                expected_work_revision, ..
            } => {
                authorize_work(actor, &aggregate, WorkItemAction::Start, None, None)?;
                aggregate
                    .work
                    .apply(expected_work_revision, WorkItemTransition::Start)?;
                work_changed = true;
            }
            TeamCommand::Block {
                expected_work_revision,
                context,
                ..
            } => {
                let context = normalize_required_text("block context", &context, 4_000)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Block,
                    None,
                    Some((
                        aggregate.row.controller_member_id.as_str(),
                        RelationAction::ReportBlocked,
                    )),
                )?;
                aggregate
                    .work
                    .apply(expected_work_revision, WorkItemTransition::Block)?;
                notification_drafts.push(TeamNotificationDraft::new(
                    aggregate.row.controller_member_id.clone(),
                    format!(
                        "Canonical WorkItem `{}` was blocked by its assignee at revision {}.\nBlocker context:\n{}\nCall `team_inspect` for the current state before deciding the next action.",
                        aggregate.work.work_item_id(),
                        aggregate.work.revision(),
                        context
                    ),
                ));
                work_changed = true;
            }
            TeamCommand::Resume {
                expected_work_revision, ..
            } => {
                authorize_work(actor, &aggregate, WorkItemAction::Resume, None, None)?;
                aggregate
                    .work
                    .apply(expected_work_revision, WorkItemTransition::Resume)?;
                work_changed = true;
            }
            TeamCommand::SubmitInline {
                expected_work_revision,
                evidence,
                ..
            } => {
                let evidence = normalize_required_text("submission evidence", &evidence, 8_000)?;
                ensure_requirement(&aggregate.work, DeliveryRequirement::None)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Submit,
                    Some(&actor.actor_member_id),
                    Some((aggregate.row.reviewer_member_id.as_str(), RelationAction::SubmitResult)),
                )?;
                aggregate.work.apply(
                    expected_work_revision,
                    WorkItemTransition::Submit {
                        submission: WorkSubmission::inline(&actor.actor_member_id),
                    },
                )?;
                notification_drafts.push(TeamNotificationDraft::new(
                    aggregate.row.reviewer_member_id.clone(),
                    format!(
                        "Canonical WorkItem `{}` was submitted by member `{}` at revision {}.\nSubmission evidence:\n{}\nCall `team_inspect` for the exact submission before `team_review`.",
                        aggregate.work.work_item_id(),
                        actor.actor_member_id,
                        aggregate.work.revision(),
                        evidence
                    ),
                ));
                work_changed = true;
            }
            TeamCommand::SubmitGit {
                expected_work_revision,
                content_revision,
                head_commit,
                evidence,
                ..
            } => {
                let evidence = normalize_required_text("submission evidence", &evidence, 8_000)?;
                ensure_requirement(&aggregate.work, DeliveryRequirement::Git)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Submit,
                    Some(&actor.actor_member_id),
                    Some((aggregate.row.reviewer_member_id.as_str(), RelationAction::SubmitResult)),
                )?;
                validate_git_submission(content_revision, &head_commit)?;
                if self
                    .find_git_delivery_for_content_revision(
                        &actor.team_id,
                        aggregate.work.work_item_id(),
                        revision_to_i64(content_revision)?,
                    )
                    .await?
                    .is_some()
                {
                    return Err(TeamCommandError::GitContentRevisionAlreadyUsed(content_revision));
                }
                let assignment =
                    aggregate.work.git_assignment().cloned().ok_or_else(|| {
                        TeamCommandError::CorruptAggregate("Git WorkItem assignment is missing".into())
                    })?;
                let reference = GitDeliveryRef::new(
                    generate_prefixed_id("delivery"),
                    &actor.team_id,
                    aggregate.work.work_item_id(),
                    &actor.actor_member_id,
                    assignment.repository_id(),
                    content_revision,
                    assignment.base_commit(),
                    assignment.branch_ref(),
                    head_commit,
                );
                let lifecycle = GitDeliveryLifecycle::submitted(reference.clone());
                let row = create_delivery_row(&lifecycle, now)?;
                aggregate.work.apply(
                    expected_work_revision,
                    WorkItemTransition::Submit {
                        submission: WorkSubmission::git(reference),
                    },
                )?;
                aggregate.delivery = Some(LoadedDelivery {
                    row,
                    lifecycle,
                    pending_attempt: None,
                });
                notification_drafts.push(TeamNotificationDraft::new(
                    aggregate.row.reviewer_member_id.clone(),
                    format!(
                        "Canonical WorkItem `{}` was submitted by member `{}` at work revision {}.\nSubmission evidence:\n{}\nCall `team_inspect` for the exact submission and delivery revision before `team_review`.",
                        aggregate.work.work_item_id(),
                        actor.actor_member_id,
                        aggregate.work.revision(),
                        evidence
                    ),
                ));
                work_changed = true;
                delivery_inserted = true;
            }
            TeamCommand::BeginReview {
                expected_work_revision,
                expected_delivery_revision,
                ..
            } => {
                let producer = required_producer(&aggregate.work)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::BeginReview,
                    None,
                    Some((producer, RelationAction::ReviewResult)),
                )?;
                ensure_optional_delivery_revision(&aggregate, expected_delivery_revision)?;
                aggregate
                    .work
                    .apply(expected_work_revision, WorkItemTransition::BeginReview)?;
                work_changed = true;
            }
            TeamCommand::RequestChanges {
                expected_work_revision,
                expected_delivery_revision,
                feedback,
                ..
            } => {
                let feedback = normalize_required_text("review feedback", &feedback, 4_000)?;
                let producer = required_producer(&aggregate.work)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::RequestChanges,
                    None,
                    Some((producer, RelationAction::RequestChanges)),
                )?;
                apply_optional_delivery(
                    &mut aggregate,
                    expected_delivery_revision,
                    GitDeliveryTransition::Supersede,
                )?;
                delivery_changed = aggregate.delivery.is_some();
                aggregate
                    .work
                    .apply(expected_work_revision, WorkItemTransition::RequestChanges)?;
                notification_drafts.push(TeamNotificationDraft::new(
                    aggregate.row.assignee_member_id.clone(),
                    format!(
                        "Changes were requested for canonical WorkItem `{}` at revision {}.\nReviewer feedback:\n{}\nCall `team_inspect` for the current revision and resume only that WorkItem.",
                        aggregate.work.work_item_id(),
                        aggregate.work.revision(),
                        feedback
                    ),
                ));
                work_changed = true;
            }
            TeamCommand::Accept {
                expected_work_revision,
                expected_delivery_revision,
                ..
            } => {
                let producer = required_producer(&aggregate.work)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Accept,
                    None,
                    Some((producer, RelationAction::AcceptResult)),
                )?;
                apply_optional_delivery(
                    &mut aggregate,
                    expected_delivery_revision,
                    GitDeliveryTransition::Accept,
                )?;
                delivery_changed = aggregate.delivery.is_some();
                aggregate
                    .work
                    .apply(expected_work_revision, WorkItemTransition::Accept)?;
                if let Some(delivery) = aggregate.delivery.as_ref() {
                    let integrator_member_id = aggregate.row.integrator_member_id.clone().ok_or_else(|| {
                        TeamCommandError::CorruptAggregate("Git WorkItem integrator is missing".into())
                    })?;
                    notification_drafts.push(TeamNotificationDraft::new(
                        integrator_member_id,
                        format!(
                            "Canonical WorkItem `{}` accepted Git delivery `{}` at work revision {} and delivery revision {}. Call `team_inspect`, then use only `team_integrate` for controlled integration.",
                            aggregate.work.work_item_id(),
                            delivery.lifecycle.delivery().delivery_id(),
                            aggregate.work.revision(),
                            delivery.lifecycle.revision()
                        ),
                    ));
                }
                work_changed = true;
            }
            TeamCommand::Reject {
                expected_work_revision,
                expected_delivery_revision,
                ..
            } => {
                let producer = required_producer(&aggregate.work)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Reject,
                    None,
                    Some((producer, RelationAction::RejectResult)),
                )?;
                apply_optional_delivery(
                    &mut aggregate,
                    expected_delivery_revision,
                    GitDeliveryTransition::Reject,
                )?;
                delivery_changed = aggregate.delivery.is_some();
                aggregate
                    .work
                    .apply(expected_work_revision, WorkItemTransition::Reject)?;
                work_changed = true;
            }
            TeamCommand::Cancel {
                expected_work_revision,
                expected_delivery_revision,
                ..
            } => {
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Cancel,
                    None,
                    Some((
                        aggregate.row.assignee_member_id.as_str(),
                        RelationAction::CancelControlledWork,
                    )),
                )?;
                apply_optional_delivery(
                    &mut aggregate,
                    expected_delivery_revision,
                    GitDeliveryTransition::Abandon,
                )?;
                delivery_changed = aggregate.delivery.is_some();
                aggregate
                    .work
                    .apply(expected_work_revision, WorkItemTransition::Cancel)?;
                notification_drafts.push(TeamNotificationDraft::new(
                    aggregate.row.assignee_member_id.clone(),
                    format!(
                        "Canonical WorkItem `{}` was cancelled at revision {}. Stop work on it and call `team_inspect` before taking another action.",
                        aggregate.work.work_item_id(),
                        aggregate.work.revision()
                    ),
                ));
                work_changed = true;
            }
            TeamCommand::CompleteWithoutDelivery {
                expected_work_revision, ..
            } => {
                let producer = required_producer(&aggregate.work)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Complete,
                    None,
                    Some((producer, RelationAction::AcceptResult)),
                )?;
                aggregate
                    .work
                    .apply(expected_work_revision, WorkItemTransition::CompleteWithoutDelivery)?;
                work_changed = true;
            }
            TeamCommand::BeginIntegration {
                expected_work_revision,
                expected_delivery_revision,
                target_ref,
                target_head,
                ..
            } => {
                ensure_expected_work_revision(&aggregate.work, expected_work_revision)?;
                let producer = required_producer(&aggregate.work)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Integrate,
                    None,
                    Some((producer, RelationAction::IntegrateDelivery)),
                )?;
                validate_integration_target(&target_ref, &target_head)?;
                let delivery = aggregate
                    .delivery
                    .as_ref()
                    .ok_or(TeamCommandError::GitDeliveryRequired)?;
                if delivery.lifecycle.delivery().branch_ref() == target_ref {
                    return Err(TeamCommandError::InvalidCommand(
                        "Git integration source and target refs must differ".into(),
                    ));
                }
                let reference = delivery.lifecycle.delivery().clone();
                let attempt_id = generate_prefixed_id("integration");
                let attempt = TeamGitIntegrationAttemptRow {
                    attempt_id: attempt_id.clone(),
                    team_id: reference.team_id().to_owned(),
                    work_item_id: reference.work_item_id().to_owned(),
                    delivery_id: reference.delivery_id().to_owned(),
                    repository_id: reference.repository_id().to_owned(),
                    base_commit: reference.base_commit().to_owned(),
                    source_ref: reference.branch_ref().to_owned(),
                    source_head: reference.head_commit().to_owned(),
                    target_ref,
                    target_head,
                    state: "pending".into(),
                    merged_commit: None,
                    observed_target_head: None,
                    recovery_reason: None,
                    created_at: now,
                    updated_at: now,
                };
                required_delivery_mut(&mut aggregate)?
                    .apply(expected_delivery_revision, GitDeliveryTransition::BeginIntegration)?;
                integration_attempt_id = Some(attempt_id);
                integration_mutation = Some(TeamGitIntegrationMutation::Begin(Box::new(attempt)));
                delivery_changed = true;
            }
            TeamCommand::ResolveIntegrationRetryable {
                expected_work_revision,
                expected_delivery_revision,
                evidence,
                ..
            } => {
                ensure_expected_work_revision(&aggregate.work, expected_work_revision)?;
                let producer = required_producer(&aggregate.work)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Integrate,
                    None,
                    Some((producer, RelationAction::IntegrateDelivery)),
                )?;
                let RetryableIntegrationEvidence {
                    attempt_id,
                    reason,
                    observed_target_head,
                } = evidence;
                require_pending_attempt(&aggregate, &attempt_id)?;
                required_delivery_mut(&mut aggregate)?.apply(
                    expected_delivery_revision,
                    GitDeliveryTransition::ReturnToAccepted { reason },
                )?;
                integration_attempt_id = Some(attempt_id.clone());
                integration_mutation = Some(TeamGitIntegrationMutation::Resolve {
                    attempt_id,
                    resolution: TeamGitIntegrationResolution::Retryable {
                        recovery_reason: reason.as_str().to_owned(),
                        observed_target_head,
                    },
                    updated_at: now,
                });
                delivery_changed = true;
            }
            TeamCommand::ResolveIntegrationMerged {
                expected_work_revision,
                expected_delivery_revision,
                evidence,
                ..
            } => {
                let producer = required_producer(&aggregate.work)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::Complete,
                    None,
                    Some((producer, RelationAction::IntegrateDelivery)),
                )?;
                let MergedIntegrationEvidence {
                    attempt_id,
                    merged_commit,
                    observed_target_head,
                } = evidence;
                require_pending_attempt(&aggregate, &attempt_id)?;
                let delivery = required_delivery_mut(&mut aggregate)?;
                delivery.apply(
                    expected_delivery_revision,
                    GitDeliveryTransition::MarkMerged {
                        merged_commit: merged_commit.clone(),
                    },
                )?;
                let completion_evidence = delivery
                    .merged_evidence()
                    .ok_or_else(|| TeamCommandError::CorruptAggregate("merged evidence missing".into()))?;
                aggregate.work.apply(
                    expected_work_revision,
                    WorkItemTransition::CompleteWithDelivery {
                        evidence: completion_evidence,
                    },
                )?;
                integration_attempt_id = Some(attempt_id.clone());
                integration_mutation = Some(TeamGitIntegrationMutation::Resolve {
                    attempt_id,
                    resolution: TeamGitIntegrationResolution::Merged {
                        merged_commit,
                        observed_target_head,
                    },
                    updated_at: now,
                });
                work_changed = true;
                delivery_changed = true;
            }
            TeamCommand::ResolveIntegrationConflict {
                expected_work_revision,
                expected_delivery_revision,
                evidence,
                ..
            } => {
                let producer = required_producer(&aggregate.work)?;
                authorize_work(
                    actor,
                    &aggregate,
                    WorkItemAction::ReopenAfterConflict,
                    None,
                    Some((producer, RelationAction::IntegrateDelivery)),
                )?;
                let ConflictedIntegrationEvidence {
                    attempt_id,
                    observed_target_head,
                } = evidence;
                require_pending_attempt(&aggregate, &attempt_id)?;
                let delivery = required_delivery_mut(&mut aggregate)?;
                delivery.apply(expected_delivery_revision, GitDeliveryTransition::MarkConflicted)?;
                let conflict_evidence = delivery
                    .conflicted_evidence()
                    .ok_or_else(|| TeamCommandError::CorruptAggregate("conflict evidence missing".into()))?;
                aggregate.work.apply(
                    expected_work_revision,
                    WorkItemTransition::ReopenAfterConflict {
                        evidence: conflict_evidence,
                    },
                )?;
                notification_drafts.push(TeamNotificationDraft::new(
                    aggregate.row.assignee_member_id.clone(),
                    format!(
                        "Git integration conflicted for canonical WorkItem `{}` at revision {} (observed target head `{}`). Call `team_inspect`, then resume that WorkItem and prepare a new submission.",
                        aggregate.work.work_item_id(),
                        aggregate.work.revision(),
                        observed_target_head
                    ),
                ));
                integration_attempt_id = Some(attempt_id.clone());
                integration_mutation = Some(TeamGitIntegrationMutation::Resolve {
                    attempt_id,
                    resolution: TeamGitIntegrationResolution::Conflicted { observed_target_head },
                    updated_at: now,
                });
                work_changed = true;
                delivery_changed = true;
            }
            TeamCommand::CreateWorkItem { .. } => {
                return Err(TeamCommandError::InvalidCommand(
                    "create command reached existing WorkItem dispatcher".into(),
                ));
            }
        }

        let work_mutation = if work_changed {
            let row = update_work_item_row(aggregate.row.clone(), &aggregate.work, now)?;
            Some(TeamWorkItemMutation::CompareAndSwap {
                expected_revision: revision_to_i64(original_work_revision)?,
                row,
            })
        } else {
            None
        };
        let delivery_mutation = if delivery_inserted {
            Some(TeamGitDeliveryMutation::Insert(
                aggregate
                    .delivery
                    .as_ref()
                    .ok_or_else(|| TeamCommandError::CorruptAggregate("new delivery missing".into()))?
                    .row
                    .clone(),
            ))
        } else if delivery_changed {
            let delivery = aggregate
                .delivery
                .as_ref()
                .ok_or_else(|| TeamCommandError::CorruptAggregate("updated delivery missing".into()))?;
            let row = update_delivery_row(delivery.row.clone(), &delivery.lifecycle, now)?;
            let expected_revision = revision_to_i64(
                original_delivery_revision
                    .ok_or_else(|| TeamCommandError::CorruptAggregate("delivery revision missing".into()))?,
            )?;
            Some(match integration_mutation {
                Some(integration) => TeamGitDeliveryMutation::CompareAndSwapWithIntegration {
                    expected_revision,
                    row,
                    integration,
                },
                None => TeamGitDeliveryMutation::CompareAndSwap { expected_revision, row },
            })
        } else {
            None
        };
        let result = command_result(
            &actor.team_id,
            generate_prefixed_id("event"),
            &aggregate.work,
            aggregate.delivery.as_ref().map(|delivery| &delivery.lifecycle),
            integration_attempt_id.as_deref(),
        );
        self.commit_plan(
            actor,
            idempotency_key,
            fingerprint,
            command_name,
            Vec::new(),
            work_mutation,
            delivery_mutation,
            notification_drafts,
            result,
            now,
        )
        .await
    }
}

fn normalize_required_text(field: &str, value: &str, max_chars: usize) -> Result<String, TeamCommandError> {
    let value = value.trim();
    let count = value.chars().count();
    if !(1..=max_chars).contains(&count) {
        return Err(TeamCommandError::InvalidCommand(format!(
            "{field} must contain 1..={max_chars} characters"
        )));
    }
    Ok(value.to_owned())
}

pub(super) fn authorize_work(
    actor: &ResolvedCommandActor,
    aggregate: &LoadedCommandAggregate,
    action: WorkItemAction,
    prospective_producer: Option<&str>,
    relation_check: Option<(&str, RelationAction)>,
) -> Result<(), TeamCommandError> {
    let submission_producer = prospective_producer.or_else(|| aggregate.work.submission_producer_member_id());
    WorkItemPolicy::authorize(
        &actor.actor_member_id,
        action,
        WorkItemPolicyView {
            controller_member_id: &aggregate.row.controller_member_id,
            assignee_member_id: Some(&aggregate.row.assignee_member_id),
            submission_producer_member_id: submission_producer,
            reviewer_member_id: Some(&aggregate.row.reviewer_member_id),
            integrator_member_id: aggregate.row.integrator_member_id.as_deref(),
            delivery_requirement: aggregate.work.delivery_requirement(),
        },
    )?;
    if let Some((target_member_id, relation_action)) = relation_check {
        let relation = actor.roster.relation(&actor.actor_member_id, target_member_id);
        RelationPolicy::authorize(relation, relation_action)?;
    }
    Ok(())
}

fn apply_optional_delivery(
    aggregate: &mut LoadedCommandAggregate,
    expected_revision: Option<u64>,
    transition: GitDeliveryTransition,
) -> Result<(), TeamCommandError> {
    match aggregate.delivery.as_mut() {
        Some(delivery) => {
            let expected_revision = expected_revision.ok_or(TeamCommandError::ExpectedDeliveryRevisionRequired)?;
            delivery.lifecycle.apply(expected_revision, transition)?;
        }
        None if expected_revision.is_some() => return Err(TeamCommandError::UnexpectedDeliveryRevision),
        None => {}
    }
    Ok(())
}

fn ensure_optional_delivery_revision(
    aggregate: &LoadedCommandAggregate,
    expected_revision: Option<u64>,
) -> Result<(), TeamCommandError> {
    match aggregate.delivery.as_ref() {
        Some(delivery) => {
            let expected_revision = expected_revision.ok_or(TeamCommandError::ExpectedDeliveryRevisionRequired)?;
            if delivery.lifecycle.revision() != expected_revision {
                return Err(TeamCommandError::RevisionConflict {
                    aggregate: "Git delivery",
                    expected_revision: revision_to_i64(expected_revision)?,
                    actual_revision: Some(revision_to_i64(delivery.lifecycle.revision())?),
                });
            }
        }
        None if expected_revision.is_some() => return Err(TeamCommandError::UnexpectedDeliveryRevision),
        None => {}
    }
    Ok(())
}

fn required_delivery_mut(
    aggregate: &mut LoadedCommandAggregate,
) -> Result<&mut GitDeliveryLifecycle, TeamCommandError> {
    aggregate
        .delivery
        .as_mut()
        .map(|delivery| &mut delivery.lifecycle)
        .ok_or(TeamCommandError::GitDeliveryRequired)
}

fn require_pending_attempt<'a>(
    aggregate: &'a LoadedCommandAggregate,
    attempt_id: &str,
) -> Result<&'a TeamGitIntegrationAttemptRow, TeamCommandError> {
    let attempt = aggregate
        .delivery
        .as_ref()
        .and_then(|delivery| delivery.pending_attempt.as_ref())
        .ok_or_else(|| TeamCommandError::CorruptAggregate("pending Git integration attempt is missing".into()))?;
    if attempt.attempt_id != attempt_id {
        return Err(TeamCommandError::InvalidCommand(
            "Git integration evidence does not match the pending attempt".into(),
        ));
    }
    Ok(attempt)
}

pub(super) fn required_producer(work: &WorkItemLifecycle) -> Result<&str, TeamCommandError> {
    work.submission_producer_member_id()
        .ok_or_else(|| TeamCommandError::CorruptAggregate("current submission producer is missing".into()))
}

fn ensure_requirement(work: &WorkItemLifecycle, expected: DeliveryRequirement) -> Result<(), TeamCommandError> {
    if work.delivery_requirement() != expected {
        return Err(TeamCommandError::InvalidCommand(format!(
            "command requires {expected:?} delivery policy"
        )));
    }
    Ok(())
}

fn ensure_expected_work_revision(work: &WorkItemLifecycle, expected: u64) -> Result<(), TeamCommandError> {
    if work.revision() != expected {
        return Err(TeamCommandError::RevisionConflict {
            aggregate: "WorkItem",
            expected_revision: revision_to_i64(expected)?,
            actual_revision: Some(revision_to_i64(work.revision())?),
        });
    }
    Ok(())
}

fn validate_git_submission(content_revision: u64, head_commit: &str) -> Result<(), TeamCommandError> {
    if content_revision == 0 {
        return Err(TeamCommandError::InvalidCommand(
            "Git content_revision must be at least 1".into(),
        ));
    }
    if head_commit.trim().is_empty() {
        return Err(TeamCommandError::InvalidCommand("head_commit must not be empty".into()));
    }
    Ok(())
}

fn validate_integration_target(target_ref: &str, target_head: &str) -> Result<(), TeamCommandError> {
    if !target_ref.starts_with("refs/heads/") || target_ref.trim() == "refs/heads/" {
        return Err(TeamCommandError::InvalidCommand(
            "Git integration target_ref must be a direct local branch ref".into(),
        ));
    }
    if target_head.trim().is_empty() {
        return Err(TeamCommandError::InvalidCommand(
            "Git integration target_head must not be empty".into(),
        ));
    }
    Ok(())
}
