use std::sync::Arc;

use aionui_api_types::{TeamWorkChangedPayload, WebSocketMessage};
use aionui_common::{generate_id, generate_prefixed_id, now_ms};
use aionui_db::{
    CommitTeamCommandParams, ITeamModeRepository, ITeamRepository, NewTeamMailboxNotification, NewTeamWorkEvent,
    TEAM_WORK_EVENT_NOTIFICATION_SCOPE, TeamCommandCommitResult, TeamCommandReceiptLookupResult,
    TeamGitDeliveryMutation, TeamGitDeliveryRow, TeamGitIntegrationAttemptRow, TeamRosterGuard, TeamWorkEventRow,
    TeamWorkItemMutation, TeamWorkItemRevisionGuard, TeamWorkItemRow,
};
use aionui_realtime::EventBroadcaster;
use serde::Serialize;
use sha2::{Digest, Sha256};
use tracing::{debug, warn};

use super::actor::LegacyTeamRoster;
use super::dispatch::{authorize_work, required_producer};
use super::model::{
    RESULT_SCHEMA_VERSION, TeamCommand, TeamCommandDeliveryResult, TeamCommandError, TeamCommandPrincipal,
    TeamCommandReceipt, TeamCommandResult,
};
use super::snapshot::{NewWorkItemRecord, create_work_item_row, restore_delivery, restore_work_item};
use crate::kernel::{
    DeliveryRequirement, GitDeliveryLifecycle, GitDeliveryState, GitWorkAssignment, RelationAction, RelationPolicy,
    WorkItemLifecycle, WorkItemState, WorkSubmission,
};

const FINGERPRINT_SCHEMA_VERSION: u8 = 1;
const IDEMPOTENCY_KEY_MAX_LEN: usize = 240;
const SUBJECT_MAX_LEN: usize = 500;
const DESCRIPTION_MAX_LEN: usize = 20_000;
const STABLE_SNAPSHOT_ATTEMPTS: usize = 3;
const TEAM_WORK_NOTIFICATION_SENDER: &str = "team_work";

pub struct TeamCommandService {
    team_repo: Arc<dyn ITeamRepository>,
    mode_repo: Arc<dyn ITeamModeRepository>,
    event_broadcaster: Option<Arc<dyn EventBroadcaster>>,
}

impl TeamCommandService {
    pub(super) async fn find_git_delivery_for_content_revision(
        &self,
        team_id: &str,
        work_item_id: &str,
        content_revision: i64,
    ) -> Result<Option<TeamGitDeliveryRow>, TeamCommandError> {
        Ok(self
            .mode_repo
            .find_git_delivery(team_id, work_item_id, content_revision)
            .await?)
    }

    pub fn new(team_repo: Arc<dyn ITeamRepository>, mode_repo: Arc<dyn ITeamModeRepository>) -> Self {
        Self {
            team_repo,
            mode_repo,
            event_broadcaster: None,
        }
    }

    pub fn new_with_event_broadcaster(
        team_repo: Arc<dyn ITeamRepository>,
        mode_repo: Arc<dyn ITeamModeRepository>,
        event_broadcaster: Arc<dyn EventBroadcaster>,
    ) -> Self {
        Self {
            team_repo,
            mode_repo,
            event_broadcaster: Some(event_broadcaster),
        }
    }

    /// Executes a command on behalf of the authenticated Team owner.
    ///
    /// The owner is deliberately projected to the validated Lead member. HTTP
    /// request bodies never choose an actor identity, so an authenticated user
    /// cannot accidentally or deliberately impersonate a worker.
    pub(crate) async fn execute_as_owner(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
        idempotency_key: &str,
        command: TeamCommand,
    ) -> Result<TeamCommandReceipt, TeamCommandError> {
        validate_idempotency_key(idempotency_key)?;
        let resolved = self.resolve_owner(authenticated_user_id, team_id).await?;
        self.execute_resolved(&resolved, idempotency_key, command).await
    }

    pub(crate) async fn execute(
        &self,
        principal: &TeamCommandPrincipal,
        idempotency_key: &str,
        command: TeamCommand,
    ) -> Result<TeamCommandReceipt, TeamCommandError> {
        validate_idempotency_key(idempotency_key)?;
        let resolved = self.resolve_actor(principal).await?;
        self.execute_resolved(&resolved, idempotency_key, command).await
    }

    /// Looks up a durable receipt only after authenticating the current actor
    /// and rechecking the roster guard. Coordinators use this before an
    /// external side effect so a retry does not re-plan already persisted
    /// repository coordinates.
    pub(crate) async fn find_receipt_as_owner(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
        idempotency_key: &str,
        expected_command_name: &str,
    ) -> Result<Option<TeamCommandReceipt>, TeamCommandError> {
        validate_idempotency_key(idempotency_key)?;
        let resolved = self.resolve_owner(authenticated_user_id, team_id).await?;
        self.find_receipt_resolved(&resolved, idempotency_key, expected_command_name)
            .await
    }

    pub(crate) async fn find_receipt(
        &self,
        principal: &TeamCommandPrincipal,
        idempotency_key: &str,
        expected_command_name: &str,
    ) -> Result<Option<TeamCommandReceipt>, TeamCommandError> {
        validate_idempotency_key(idempotency_key)?;
        let resolved = self.resolve_actor(principal).await?;
        self.find_receipt_resolved(&resolved, idempotency_key, expected_command_name)
            .await
    }

    pub(crate) async fn authorize_delegate_as_owner(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
        assignee_member_id: &str,
    ) -> Result<(), TeamCommandError> {
        let resolved = self.resolve_owner(authenticated_user_id, team_id).await?;
        Self::authorize_delegate_resolved(&resolved, assignee_member_id)
    }

    pub(crate) async fn authorize_delegate(
        &self,
        principal: &TeamCommandPrincipal,
        assignee_member_id: &str,
    ) -> Result<(), TeamCommandError> {
        let resolved = self.resolve_actor(principal).await?;
        Self::authorize_delegate_resolved(&resolved, assignee_member_id)
    }

    pub(crate) async fn authorize_integration_as_owner(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
        work_item_id: &str,
    ) -> Result<(), TeamCommandError> {
        let resolved = self.resolve_owner(authenticated_user_id, team_id).await?;
        self.authorize_integration_resolved(&resolved, work_item_id).await
    }

    pub(crate) async fn authorize_integration(
        &self,
        principal: &TeamCommandPrincipal,
        work_item_id: &str,
    ) -> Result<(), TeamCommandError> {
        let resolved = self.resolve_actor(principal).await?;
        self.authorize_integration_resolved(&resolved, work_item_id).await
    }

    async fn authorize_integration_resolved(
        &self,
        resolved: &ResolvedCommandActor,
        work_item_id: &str,
    ) -> Result<(), TeamCommandError> {
        let aggregate = self.load_aggregate(&resolved.team_id, work_item_id).await?;
        let producer = required_producer(&aggregate.work)?;
        authorize_work(
            resolved,
            &aggregate,
            crate::kernel::WorkItemAction::Integrate,
            None,
            Some((producer, RelationAction::IntegrateDelivery)),
        )
    }

    fn authorize_delegate_resolved(
        resolved: &ResolvedCommandActor,
        assignee_member_id: &str,
    ) -> Result<(), TeamCommandError> {
        if resolved.roster.member(assignee_member_id).is_none() {
            return Err(TeamCommandError::MemberNotFound(assignee_member_id.to_owned()));
        }
        let relation = resolved.roster.relation(&resolved.actor_member_id, assignee_member_id);
        RelationPolicy::authorize(relation, RelationAction::DelegateWork)?;
        Ok(())
    }

    async fn find_receipt_resolved(
        &self,
        resolved: &ResolvedCommandActor,
        idempotency_key: &str,
        expected_command_name: &str,
    ) -> Result<Option<TeamCommandReceipt>, TeamCommandError> {
        match self
            .mode_repo
            .find_command_receipt_guarded(
                &resolved.team_id,
                &resolved.actor_member_id,
                idempotency_key,
                &resolved.roster_guard,
            )
            .await?
        {
            TeamCommandReceiptLookupResult::Found(event) => {
                if event.command_name != expected_command_name {
                    return Err(TeamCommandError::IdempotencyConflict);
                }
                receipt_from_event(*event, true).map(Some)
            }
            TeamCommandReceiptLookupResult::NotFound => Ok(None),
            TeamCommandReceiptLookupResult::TeamRosterConflict { .. } => Err(TeamCommandError::RosterChanged),
        }
    }

    async fn execute_resolved(
        &self,
        resolved: &ResolvedCommandActor,
        idempotency_key: &str,
        command: TeamCommand,
    ) -> Result<TeamCommandReceipt, TeamCommandError> {
        let fingerprint = command_fingerprint(&resolved.team_id, &resolved.actor_member_id, &command)?;

        match self
            .mode_repo
            .find_command_receipt_guarded(
                &resolved.team_id,
                &resolved.actor_member_id,
                idempotency_key,
                &resolved.roster_guard,
            )
            .await?
        {
            TeamCommandReceiptLookupResult::Found(event) => return replay_event(*event, &fingerprint),
            TeamCommandReceiptLookupResult::NotFound => {}
            TeamCommandReceiptLookupResult::TeamRosterConflict { .. } => {
                return Err(TeamCommandError::RosterChanged);
            }
        }

        match command {
            TeamCommand::CreateWorkItem {
                parent_work_item_id,
                subject,
                description,
                assignee_member_id,
                delivery_requirement,
                git_assignment,
            } => {
                self.execute_create(
                    resolved,
                    idempotency_key,
                    fingerprint,
                    CreateCommand {
                        parent_work_item_id,
                        subject,
                        description,
                        assignee_member_id,
                        delivery_requirement,
                        git_assignment,
                    },
                )
                .await
            }
            command => {
                self.execute_existing(resolved, idempotency_key, fingerprint, command)
                    .await
            }
        }
    }

    async fn resolve_owner(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
    ) -> Result<ResolvedCommandActor, TeamCommandError> {
        let (roster, roster_guard) = self.load_roster(authenticated_user_id, team_id).await?;
        let actor_member_id = roster.lead().member_id.clone();
        Ok(ResolvedCommandActor {
            team_id: team_id.to_owned(),
            actor_member_id,
            roster,
            roster_guard,
        })
    }

    async fn resolve_actor(&self, principal: &TeamCommandPrincipal) -> Result<ResolvedCommandActor, TeamCommandError> {
        let (roster, roster_guard) = self
            .load_roster(&principal.authenticated_user_id, &principal.team_id)
            .await?;
        let actor_member_id = roster
            .resolve_conversation(&principal.trusted_caller_conversation_id)
            .ok_or(TeamCommandError::CallerNotMember)?
            .member_id
            .clone();
        Ok(ResolvedCommandActor {
            team_id: principal.team_id.clone(),
            actor_member_id,
            roster,
            roster_guard,
        })
    }

    async fn load_roster(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
    ) -> Result<(LegacyTeamRoster, TeamRosterGuard), TeamCommandError> {
        let team = self
            .team_repo
            .get_team(team_id)
            .await?
            .ok_or_else(|| TeamCommandError::TeamNotFound(team_id.to_owned()))?;
        if team.user_id != authenticated_user_id {
            return Err(TeamCommandError::ForbiddenTeam);
        }
        let roster = LegacyTeamRoster::from_row(&team)?;
        let guard = TeamRosterGuard {
            expected_user_id: team.user_id,
            expected_agents_json: team.agents,
            expected_lead_agent_id: team.lead_agent_id,
        };
        Ok((roster, guard))
    }

    async fn execute_create(
        &self,
        actor: &ResolvedCommandActor,
        idempotency_key: &str,
        fingerprint: String,
        command: CreateCommand,
    ) -> Result<TeamCommandReceipt, TeamCommandError> {
        let subject = command.subject.trim();
        if subject.is_empty() || subject.len() > SUBJECT_MAX_LEN {
            return Err(TeamCommandError::InvalidCommand(format!(
                "subject must contain 1..={SUBJECT_MAX_LEN} bytes"
            )));
        }
        if command
            .description
            .as_ref()
            .is_some_and(|description| description.len() > DESCRIPTION_MAX_LEN)
        {
            return Err(TeamCommandError::InvalidCommand(format!(
                "description exceeds {DESCRIPTION_MAX_LEN} bytes"
            )));
        }
        if actor.roster.member(&command.assignee_member_id).is_none() {
            return Err(TeamCommandError::MemberNotFound(command.assignee_member_id));
        }
        let relation = actor
            .roster
            .relation(&actor.actor_member_id, &command.assignee_member_id);
        RelationPolicy::authorize(relation, RelationAction::DelegateWork)?;

        let work_item_guards = if let Some(parent_id) = command.parent_work_item_id.as_deref() {
            let parent = self.load_aggregate(&actor.team_id, parent_id).await?;
            if parent.row.controller_member_id != actor.actor_member_id || parent.work.state().is_terminal() {
                return Err(TeamCommandError::InvalidParentWorkItem(parent_id.to_owned()));
            }
            vec![TeamWorkItemRevisionGuard {
                work_item_id: parent_id.to_owned(),
                expected_revision: parent.row.revision,
            }]
        } else {
            Vec::new()
        };

        let work_item_id = generate_prefixed_id("work");
        let lifecycle = WorkItemLifecycle::new(
            &actor.team_id,
            &work_item_id,
            command.delivery_requirement,
            command.git_assignment,
        )
        .map_err(|error| TeamCommandError::InvalidCommand(error.to_string()))?;
        let now = now_ms();
        let row = create_work_item_row(
            NewWorkItemRecord {
                id: work_item_id,
                team_id: actor.team_id.clone(),
                parent_work_item_id: command.parent_work_item_id,
                subject: subject.to_owned(),
                description: command.description,
                controller_member_id: actor.actor_member_id.clone(),
                assignee_member_id: command.assignee_member_id,
                reviewer_member_id: actor.actor_member_id.clone(),
                integrator_member_id: (command.delivery_requirement == DeliveryRequirement::Git)
                    .then(|| actor.actor_member_id.clone()),
                created_at: now,
            },
            &lifecycle,
        )?;
        let result = command_result(&actor.team_id, generate_prefixed_id("event"), &lifecycle, None, None);
        self.commit_plan(
            actor,
            idempotency_key,
            fingerprint,
            "create_work_item",
            work_item_guards,
            Some(TeamWorkItemMutation::Insert(row)),
            None,
            Vec::new(),
            result,
            now,
        )
        .await
    }

    // Existing aggregate command dispatch is kept in a dedicated method so
    // create/auth/replay remain easy to audit.
    async fn execute_existing(
        &self,
        actor: &ResolvedCommandActor,
        idempotency_key: &str,
        fingerprint: String,
        command: TeamCommand,
    ) -> Result<TeamCommandReceipt, TeamCommandError> {
        self.execute_existing_inner(actor, idempotency_key, fingerprint, command)
            .await
    }

    pub(super) async fn load_aggregate(
        &self,
        team_id: &str,
        work_item_id: &str,
    ) -> Result<LoadedCommandAggregate, TeamCommandError> {
        for _ in 0..STABLE_SNAPSHOT_ATTEMPTS {
            let row = self
                .mode_repo
                .get_work_item(team_id, work_item_id)
                .await?
                .ok_or_else(|| TeamCommandError::WorkItemNotFound(work_item_id.to_owned()))?;
            let work = restore_work_item(&row)?;
            let Some(reference) = work.current_submission().and_then(WorkSubmission::git_delivery) else {
                return Ok(LoadedCommandAggregate {
                    row,
                    work,
                    delivery: None,
                });
            };
            let delivery_row = self
                .mode_repo
                .get_git_delivery(team_id, reference.delivery_id())
                .await?
                .ok_or_else(|| TeamCommandError::CorruptAggregate("referenced Git delivery is missing".into()))?;
            let pending_attempt = self
                .mode_repo
                .get_pending_git_integration_attempt(team_id, reference.delivery_id())
                .await?;

            // The repository deliberately exposes small query methods. Re-read
            // the WorkItem around its delivery so a compound commit cannot be
            // mistaken for a corrupt split snapshot.
            if self.mode_repo.get_work_item(team_id, work_item_id).await?.as_ref() != Some(&row)
                || self
                    .mode_repo
                    .get_git_delivery(team_id, reference.delivery_id())
                    .await?
                    .as_ref()
                    != Some(&delivery_row)
            {
                continue;
            }
            let lifecycle = restore_delivery(&delivery_row)?;
            if lifecycle.delivery() != reference {
                return Err(TeamCommandError::CorruptAggregate(
                    "WorkItem delivery reference does not match immutable delivery row".into(),
                ));
            }
            if (lifecycle.state() == GitDeliveryState::Integrating) != pending_attempt.is_some() {
                return Err(TeamCommandError::CorruptAggregate(
                    "Integrating Git delivery and pending attempt are inconsistent".into(),
                ));
            }
            validate_pair_state(&work, &lifecycle)?;
            return Ok(LoadedCommandAggregate {
                row,
                work,
                delivery: Some(LoadedDelivery {
                    row: delivery_row,
                    lifecycle,
                    pending_attempt,
                }),
            });
        }
        Err(TeamCommandError::ConcurrentSnapshotChange)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn commit_plan(
        &self,
        actor: &ResolvedCommandActor,
        idempotency_key: &str,
        fingerprint: String,
        command_name: &'static str,
        work_item_guards: Vec<TeamWorkItemRevisionGuard>,
        work_item: Option<TeamWorkItemMutation>,
        delivery: Option<TeamGitDeliveryMutation>,
        notification_drafts: Vec<TeamNotificationDraft>,
        result: TeamCommandResult,
        created_at: i64,
    ) -> Result<TeamCommandReceipt, TeamCommandError> {
        let expected_work_item_revision = match work_item.as_ref() {
            Some(TeamWorkItemMutation::Insert(_)) => None,
            Some(TeamWorkItemMutation::CompareAndSwap { expected_revision, .. }) => Some(*expected_revision),
            None => Some(revision_to_i64(result.work_item_revision)?),
        };
        let expected_delivery_revision = match delivery.as_ref() {
            Some(mutation) => mutation.expected_revision(),
            None => result
                .delivery
                .as_ref()
                .map(|delivery| revision_to_i64(delivery.revision))
                .transpose()?,
        };
        let result_json = serde_json::to_string(&result)?;
        let event = NewTeamWorkEvent {
            event_id: result.event_id.clone(),
            team_id: actor.team_id.clone(),
            work_item_id: result.work_item_id.clone(),
            delivery_id: result.delivery.as_ref().map(|delivery| delivery.delivery_id.clone()),
            actor_member_id: actor.actor_member_id.clone(),
            command_name: command_name.to_owned(),
            idempotency_key: idempotency_key.to_owned(),
            request_fingerprint: fingerprint,
            result_json,
            expected_work_item_revision,
            expected_delivery_revision,
            work_item_revision: revision_to_i64(result.work_item_revision)?,
            delivery_revision: result
                .delivery
                .as_ref()
                .map(|delivery| revision_to_i64(delivery.revision))
                .transpose()?,
            created_at,
        };
        let expected_fingerprint = event.request_fingerprint.clone();
        let notifications = notification_drafts
            .into_iter()
            .enumerate()
            .map(|(ordinal, draft)| {
                notification_from_draft(&event.event_id, &event.actor_member_id, ordinal, draft, created_at)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let outcome = self
            .mode_repo
            .commit_command(&CommitTeamCommandParams {
                roster_guard: Some(actor.roster_guard.clone()),
                work_item_guards,
                work_item,
                delivery,
                notifications,
                event,
            })
            .await?;
        match outcome {
            TeamCommandCommitResult::Applied(event) => {
                debug!(
                    team_id = %event.team_id,
                    work_item_id = %event.work_item_id,
                    actor_member_id = %event.actor_member_id,
                    command = %event.command_name,
                    event_sequence = event.sequence,
                    "applied Team Mode command"
                );
                self.broadcast_work_changed(&event);
                receipt_from_event(event, false)
            }
            TeamCommandCommitResult::Replayed(event) => replay_event(event, &expected_fingerprint),
            TeamCommandCommitResult::TeamRosterConflict { .. } => Err(TeamCommandError::RosterChanged),
            TeamCommandCommitResult::WorkItemGuardConflict {
                expected_revision,
                actual_revision,
                ..
            } => Err(TeamCommandError::RevisionConflict {
                aggregate: "guarded WorkItem",
                expected_revision,
                actual_revision,
            }),
            TeamCommandCommitResult::WorkItemRevisionConflict {
                expected_revision,
                actual_revision,
            } => Err(TeamCommandError::RevisionConflict {
                aggregate: "WorkItem",
                expected_revision,
                actual_revision,
            }),
            TeamCommandCommitResult::DeliveryRevisionConflict {
                expected_revision,
                actual_revision,
            } => Err(TeamCommandError::RevisionConflict {
                aggregate: "Git delivery",
                expected_revision,
                actual_revision,
            }),
            TeamCommandCommitResult::IdempotencyConflict { .. } => Err(TeamCommandError::IdempotencyConflict),
        }
    }

    fn broadcast_work_changed(&self, event: &TeamWorkEventRow) {
        let Some(broadcaster) = self.event_broadcaster.as_ref() else {
            return;
        };
        let payload = TeamWorkChangedPayload {
            team_id: event.team_id.clone(),
            work_item_id: event.work_item_id.clone(),
            event_sequence: event.sequence,
        };
        broadcaster.broadcast(WebSocketMessage::new(
            crate::events::TEAM_WORK_CHANGED_EVENT,
            serde_json::to_value(payload).expect("serialize Team WorkItem invalidation"),
        ));
    }
}

pub(super) struct ResolvedCommandActor {
    pub(super) team_id: String,
    pub(super) actor_member_id: String,
    pub(super) roster: LegacyTeamRoster,
    pub(super) roster_guard: TeamRosterGuard,
}

pub(super) struct LoadedDelivery {
    pub(super) row: TeamGitDeliveryRow,
    pub(super) lifecycle: GitDeliveryLifecycle,
    pub(super) pending_attempt: Option<TeamGitIntegrationAttemptRow>,
}

pub(super) struct LoadedCommandAggregate {
    pub(super) row: TeamWorkItemRow,
    pub(super) work: WorkItemLifecycle,
    pub(super) delivery: Option<LoadedDelivery>,
}

pub(super) struct TeamNotificationDraft {
    pub(super) to_agent_id: String,
    pub(super) content: String,
}

impl TeamNotificationDraft {
    pub(super) fn new(to_agent_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            to_agent_id: to_agent_id.into(),
            content: content.into(),
        }
    }
}

struct CreateCommand {
    parent_work_item_id: Option<String>,
    subject: String,
    description: Option<String>,
    assignee_member_id: String,
    delivery_requirement: DeliveryRequirement,
    git_assignment: Option<GitWorkAssignment>,
}

pub(super) fn validate_pair_state(
    work: &WorkItemLifecycle,
    delivery: &GitDeliveryLifecycle,
) -> Result<(), TeamCommandError> {
    let valid = match work.state() {
        WorkItemState::Submitted | WorkItemState::Reviewing => delivery.state() == GitDeliveryState::Submitted,
        WorkItemState::Accepted => {
            matches!(
                delivery.state(),
                GitDeliveryState::Accepted | GitDeliveryState::Integrating
            )
        }
        WorkItemState::Completed => delivery.state() == GitDeliveryState::Merged,
        WorkItemState::Rejected => delivery.state() == GitDeliveryState::Rejected,
        WorkItemState::Cancelled => {
            delivery.state() == GitDeliveryState::Abandoned
                && match work.accepted_delivery() {
                    Some(_) => delivery.revision() >= 2 && delivery.revision().is_multiple_of(2),
                    None => delivery.revision() == 1,
                }
        }
        _ => false,
    };
    if !valid {
        warn!(
            team_id = %work.team_id(),
            work_item_id = %work.work_item_id(),
            work_state = ?work.state(),
            delivery_state = ?delivery.state(),
            "rejected inconsistent Team Mode aggregate snapshots"
        );
        return Err(TeamCommandError::CorruptAggregate(
            "WorkItem and Git delivery states are inconsistent".into(),
        ));
    }
    Ok(())
}

fn validate_idempotency_key(key: &str) -> Result<(), TeamCommandError> {
    if key.trim().is_empty() || key.len() > IDEMPOTENCY_KEY_MAX_LEN || key.chars().any(char::is_control) {
        return Err(TeamCommandError::InvalidCommand(format!(
            "idempotency key must contain 1..={IDEMPOTENCY_KEY_MAX_LEN} bytes and no control characters"
        )));
    }
    Ok(())
}

fn command_fingerprint(
    team_id: &str,
    actor_member_id: &str,
    command: &TeamCommand,
) -> Result<String, TeamCommandError> {
    #[derive(Serialize)]
    struct Fingerprint<'a> {
        schema_version: u8,
        team_id: &'a str,
        actor_member_id: &'a str,
        command: &'a TeamCommand,
    }
    let encoded = serde_json::to_vec(&Fingerprint {
        schema_version: FINGERPRINT_SCHEMA_VERSION,
        team_id,
        actor_member_id,
        command,
    })?;
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

fn notification_from_draft(
    event_id: &str,
    actor_member_id: &str,
    ordinal: usize,
    draft: TeamNotificationDraft,
    created_at: i64,
) -> Result<NewTeamMailboxNotification, TeamCommandError> {
    #[derive(Serialize)]
    struct NotificationFingerprint<'a> {
        schema_version: u8,
        event_id: &'a str,
        ordinal: usize,
        to_agent_id: &'a str,
        from_agent_id: &'a str,
        content: &'a str,
    }

    let from_agent_id = if draft.to_agent_id == actor_member_id {
        TEAM_WORK_NOTIFICATION_SENDER
    } else {
        actor_member_id
    };
    let fingerprint = NotificationFingerprint {
        schema_version: 1,
        event_id,
        ordinal,
        to_agent_id: &draft.to_agent_id,
        from_agent_id,
        content: &draft.content,
    };
    let encoded = serde_json::to_vec(&fingerprint)?;
    Ok(NewTeamMailboxNotification {
        message_id: generate_id(),
        to_agent_id: draft.to_agent_id,
        from_agent_id: from_agent_id.to_owned(),
        content: draft.content,
        summary: None,
        files_json: None,
        idempotency_scope: TEAM_WORK_EVENT_NOTIFICATION_SCOPE.to_owned(),
        idempotency_key: format!("{event_id}:{ordinal}"),
        request_fingerprint: format!("{:x}", Sha256::digest(encoded)),
        created_at,
    })
}

pub(super) fn command_result(
    team_id: &str,
    event_id: String,
    work: &WorkItemLifecycle,
    delivery: Option<&GitDeliveryLifecycle>,
    integration_attempt_id: Option<&str>,
) -> TeamCommandResult {
    TeamCommandResult {
        schema_version: RESULT_SCHEMA_VERSION,
        event_id,
        team_id: team_id.to_owned(),
        work_item_id: work.work_item_id().to_owned(),
        work_item_state: work.state(),
        work_item_revision: work.revision(),
        delivery: delivery.map(|delivery| TeamCommandDeliveryResult {
            delivery_id: delivery.delivery().delivery_id().to_owned(),
            content_revision: delivery.delivery().content_revision(),
            head_commit: delivery.delivery().head_commit().to_owned(),
            state: delivery.state(),
            revision: delivery.revision(),
            merged_commit: delivery.merged_commit().map(str::to_owned),
            integration_attempt_id: integration_attempt_id.map(str::to_owned),
        }),
    }
}

fn replay_event(event: TeamWorkEventRow, expected_fingerprint: &str) -> Result<TeamCommandReceipt, TeamCommandError> {
    if event.request_fingerprint != expected_fingerprint {
        return Err(TeamCommandError::IdempotencyConflict);
    }
    receipt_from_event(event, true)
}

fn receipt_from_event(event: TeamWorkEventRow, replayed: bool) -> Result<TeamCommandReceipt, TeamCommandError> {
    let result: TeamCommandResult = serde_json::from_str(&event.result_json)?;
    if result.schema_version != RESULT_SCHEMA_VERSION
        || result.event_id != event.event_id
        || result.team_id != event.team_id
        || result.work_item_id != event.work_item_id
        || revision_to_i64(result.work_item_revision)? != event.work_item_revision
        || result.delivery.as_ref().map(|delivery| delivery.delivery_id.as_str()) != event.delivery_id.as_deref()
        || result
            .delivery
            .as_ref()
            .map(|delivery| revision_to_i64(delivery.revision))
            .transpose()?
            != event.delivery_revision
    {
        return Err(TeamCommandError::CorruptReceipt(event.event_id));
    }
    debug!(
        team_id = %event.team_id,
        work_item_id = %event.work_item_id,
        command = %event.command_name,
        event_sequence = event.sequence,
        replayed,
        "loaded Team Mode command receipt"
    );
    Ok(TeamCommandReceipt {
        event_sequence: event.sequence,
        replayed,
        result,
    })
}

pub(super) fn revision_to_i64(revision: u64) -> Result<i64, TeamCommandError> {
    i64::try_from(revision).map_err(|_| TeamCommandError::InvalidCommand("revision exceeds SQLite range".into()))
}
