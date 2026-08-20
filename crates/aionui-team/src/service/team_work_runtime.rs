use std::sync::{Arc, Weak};

use aionui_api_types::{
    IntegrateTeamWorkResponse, TeamToolCall, TeamToolErrorCode, TeamToolErrorPayload, TeamToolName,
    TeamWorkCommandDeliveryResponse, TeamWorkCommandResponse, TeamWorkItemResponse, TeamWorkReviewDecision,
};
use aionui_db::ITeamRepository;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};

use super::team_command::{TeamCommand, TeamCommandError, TeamCommandPrincipal, TeamCommandReceipt};
use super::{
    BeginGitIntegrationRequest, DelegateWorkItem, TeamCommandService, TeamDeliveryError, TeamDeliveryService,
    TeamIntegrationResolution, TeamQueryError, TeamQueryService, TeamSessionService, TeamWorkCoordinator,
    TeamWorkCoordinatorError,
};
use crate::error::TeamError;
use crate::kernel::DeliveryRequirement;
use crate::tool_executor::TeamToolContext;
use crate::types::TeamAgent;

const PUBLIC_IDEMPOTENCY_KEY_MAX_LEN: usize = 220;
const PROMPT_WORK_ITEM_LIMIT: usize = 8;
const BLOCK_CONTEXT_MAX_CHARS: usize = 4_000;
const SUBMISSION_EVIDENCE_MAX_CHARS: usize = 8_000;
const REVIEW_FEEDBACK_MAX_CHARS: usize = 4_000;

impl TeamSessionService {
    /// Install the canonical work adapter exactly once. Keeping this separate
    /// from the legacy session constructor avoids duplicating repositories in
    /// tests while AppServices remains the composition root.
    pub fn configure_team_work(
        &self,
        work_coordinator: Arc<TeamWorkCoordinator>,
        command_service: Arc<TeamCommandService>,
        delivery_service: Arc<TeamDeliveryService>,
        query_service: Arc<TeamQueryService>,
    ) -> Result<(), TeamError> {
        self.team_work_runtime
            .set(Arc::new(TeamWorkRuntimeService::new(
                self.repo.clone(),
                work_coordinator,
                command_service,
                delivery_service,
                query_service,
                Arc::new(SessionWorkNotificationWake {
                    service: self.self_ref.clone(),
                }),
            )))
            .map_err(|_| TeamError::InvalidRequest("canonical Team work adapter is already configured".into()))
    }

    pub(crate) async fn execute_canonical_work_tool(
        &self,
        context: &TeamToolContext,
        call: TeamToolCall,
    ) -> Result<Value, TeamToolErrorPayload> {
        let runtime = self.team_work_runtime.get().ok_or_else(|| {
            tool_error(
                TeamToolErrorCode::TransportUnavailable,
                "canonical Team work adapter is not configured",
            )
        })?;
        runtime.execute_tool(context, call).await
    }

    pub(crate) async fn canonical_work_prompt_summary(
        &self,
        context: &TeamToolContext,
    ) -> Result<Option<String>, TeamToolErrorPayload> {
        let Some(runtime) = self.team_work_runtime.get() else {
            return Ok(None);
        };
        runtime.prompt_summary(context).await.map(Some)
    }
}

#[async_trait]
trait WorkNotificationWake: Send + Sync {
    async fn wake(&self, user_id: &str, team_id: &str);
}

struct SessionWorkNotificationWake {
    service: Weak<TeamSessionService>,
}

#[async_trait]
impl WorkNotificationWake for SessionWorkNotificationWake {
    async fn wake(&self, user_id: &str, team_id: &str) {
        if let Some(service) = self.service.upgrade() {
            service.wake_work_notifications(user_id, team_id);
        }
    }
}

pub(crate) struct TeamWorkRuntimeService {
    team_repo: Arc<dyn ITeamRepository>,
    work_coordinator: Arc<TeamWorkCoordinator>,
    command_service: Arc<TeamCommandService>,
    delivery_service: Arc<TeamDeliveryService>,
    query_service: Arc<TeamQueryService>,
    notification_wake: Arc<dyn WorkNotificationWake>,
}

impl TeamWorkRuntimeService {
    fn new(
        team_repo: Arc<dyn ITeamRepository>,
        work_coordinator: Arc<TeamWorkCoordinator>,
        command_service: Arc<TeamCommandService>,
        delivery_service: Arc<TeamDeliveryService>,
        query_service: Arc<TeamQueryService>,
        notification_wake: Arc<dyn WorkNotificationWake>,
    ) -> Self {
        Self {
            team_repo,
            work_coordinator,
            command_service,
            delivery_service,
            query_service,
            notification_wake,
        }
    }

    pub(crate) async fn execute_tool(
        &self,
        context: &TeamToolContext,
        call: TeamToolCall,
    ) -> Result<Value, TeamToolErrorPayload> {
        let identity = self.resolve_identity(context).await?;
        match call.tool {
            TeamToolName::TeamInspect => self.inspect(&identity, call.arguments).await,
            TeamToolName::TeamDelegate => self.delegate(&identity, call.arguments).await,
            TeamToolName::TeamProgress => self.progress(&identity, call.arguments).await,
            TeamToolName::TeamSubmit => self.submit(&identity, call.arguments).await,
            TeamToolName::TeamReview => self.review(&identity, call.arguments).await,
            TeamToolName::TeamIntegrate => self.integrate(&identity, call.arguments).await,
            TeamToolName::TeamCancel => self.cancel(&identity, call.arguments).await,
            _ => Err(tool_error(
                TeamToolErrorCode::UnknownTool,
                "tool is not part of the canonical Team work surface",
            )),
        }
    }

    pub(crate) async fn prompt_summary(&self, context: &TeamToolContext) -> Result<String, TeamToolErrorPayload> {
        let identity = self.resolve_identity(context).await?;
        let work_items = self
            .query_service
            .list_work_items(&identity.user_id, &identity.team_id)
            .await
            .map_err(map_query_error)?;
        let mut relevant = work_items
            .iter()
            .filter(|work| member_has_responsibility(work, &identity.member_id) && !is_terminal(&work.state))
            .take(PROMPT_WORK_ITEM_LIMIT)
            .peekable();

        let mut summary = String::from("## Current Team Work\n");
        if relevant.peek().is_none() {
            summary.push_str(
                "No active canonical WorkItem currently names you. Use `team_inspect` when state may have changed.\n",
            );
            return Ok(summary);
        }
        for work in relevant {
            let actions = allowed_actions(work, &identity.member_id, None);
            let actions = if actions.is_empty() {
                "none".to_owned()
            } else {
                actions.join(", ")
            };
            summary.push_str(&format!(
                "- `{}` | {} r{} | {} | allowed: {}\n",
                work.id,
                work.state,
                work.revision,
                single_line(&work.subject),
                actions,
            ));
        }
        summary.push_str("Use `team_inspect` for exact details and current revisions.\n");
        Ok(summary)
    }

    async fn inspect(&self, identity: &RuntimeIdentity, arguments: Value) -> Result<Value, TeamToolErrorPayload> {
        let input: InspectInput = parse_input(arguments)?;
        if let Some(work_item_id) = input.work_item_id {
            let snapshot = self
                .query_service
                .get_work_item_snapshot(&identity.user_id, &identity.team_id, &work_item_id)
                .await
                .map_err(map_query_error)?;
            let accepted_delivery_state =
                snapshot
                    .work_item
                    .accepted_delivery_id
                    .as_deref()
                    .and_then(|accepted_delivery_id| {
                        snapshot
                            .deliveries
                            .iter()
                            .find(|delivery| delivery.id == accepted_delivery_id)
                            .map(|delivery| delivery.state.clone())
                    });
            let work_item = work_item_value(
                snapshot.work_item,
                &identity.member_id,
                accepted_delivery_state.as_deref(),
            )?;
            return Ok(json!({
                "viewer_member_id": identity.member_id,
                "work_item": work_item,
                "deliveries": snapshot.deliveries,
            }));
        }

        let work_items = self
            .query_service
            .list_work_items(&identity.user_id, &identity.team_id)
            .await
            .map_err(map_query_error)?;
        let work_items = work_items
            .into_iter()
            .map(|work| work_item_value(work, &identity.member_id, None))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(json!({
            "viewer_member_id": identity.member_id,
            "work_items": work_items,
        }))
    }

    async fn delegate(&self, identity: &RuntimeIdentity, arguments: Value) -> Result<Value, TeamToolErrorPayload> {
        let input: DelegateInput = parse_input(arguments)?;
        validate_public_key(&input.idempotency_key)?;
        let delegated = self
            .work_coordinator
            .delegate_as_member(
                &identity.principal,
                &input.idempotency_key,
                DelegateWorkItem {
                    parent_work_item_id: input.parent_work_item_id,
                    subject: input.subject,
                    description: input.description,
                    assignee_member_id: input.assignee_member_id,
                    delivery_requirement: input.delivery_requirement.into(),
                },
            )
            .await
            .map_err(map_coordinator_error)?;
        self.wake_notifications(identity).await;
        let mut value = command_value(delegated.command)?;
        if let Some(workspace) = delegated.prepared_workspace {
            value["prepared_workspace"] = Value::String(workspace);
        }
        Ok(value)
    }

    async fn progress(&self, identity: &RuntimeIdentity, arguments: Value) -> Result<Value, TeamToolErrorPayload> {
        let input: ProgressInput = parse_input(arguments)?;
        validate_public_key(&input.idempotency_key)?;
        let context = validate_progress_context(input.action, input.context.as_deref())?;
        let blocked = matches!(input.action, ProgressAction::Block);
        let command = match input.action {
            ProgressAction::Start => TeamCommand::Start {
                work_item_id: input.work_item_id,
                expected_work_revision: input.expected_work_revision,
            },
            ProgressAction::Block => TeamCommand::Block {
                work_item_id: input.work_item_id,
                expected_work_revision: input.expected_work_revision,
                context: context.expect("block context was validated"),
            },
            ProgressAction::Resume => TeamCommand::Resume {
                work_item_id: input.work_item_id,
                expected_work_revision: input.expected_work_revision,
            },
        };
        let receipt = self.execute(identity, &input.idempotency_key, command).await?;
        if blocked {
            self.wake_notifications(identity).await;
        }
        command_value(receipt)
    }

    async fn submit(&self, identity: &RuntimeIdentity, arguments: Value) -> Result<Value, TeamToolErrorPayload> {
        let input: SubmitInput = parse_input(arguments)?;
        validate_public_key(&input.idempotency_key)?;
        let evidence = validate_required_text("evidence", &input.evidence, SUBMISSION_EVIDENCE_MAX_CHARS)?;
        let command = match (input.kind, input.git) {
            (SubmissionKind::Inline, None) => TeamCommand::SubmitInline {
                work_item_id: input.work_item_id,
                expected_work_revision: input.expected_work_revision,
                evidence: evidence.clone(),
            },
            (SubmissionKind::Git, Some(git)) => TeamCommand::SubmitGit {
                work_item_id: input.work_item_id,
                expected_work_revision: input.expected_work_revision,
                content_revision: git.content_revision,
                head_commit: git.head_commit,
                evidence,
            },
            (SubmissionKind::Inline, Some(_)) => {
                return Err(schema_error("inline submission must not include git"));
            }
            (SubmissionKind::Git, None) => return Err(schema_error("git submission requires git")),
        };
        let receipt = self.execute(identity, &input.idempotency_key, command).await?;
        self.wake_notifications(identity).await;
        command_value(receipt)
    }

    async fn review(&self, identity: &RuntimeIdentity, arguments: Value) -> Result<Value, TeamToolErrorPayload> {
        let input: ReviewInput = parse_input(arguments)?;
        validate_public_key(&input.idempotency_key)?;
        let feedback = validate_review_feedback(input.decision, input.feedback.as_deref())?;
        let mut review_revision = input.expected_work_revision;
        let mut recovered_inline_accept = false;
        let begin = self
            .execute(
                identity,
                &derived_key(&input.idempotency_key, "begin"),
                TeamCommand::BeginReview {
                    work_item_id: input.work_item_id.clone(),
                    expected_work_revision: input.expected_work_revision,
                    expected_delivery_revision: input.expected_delivery_revision,
                },
            )
            .await;
        match begin {
            Err(begin_error) => {
                // A process can stop between the durable BeginReview and decision
                // receipts. Resume only when the exact caller revision still
                // describes the expected intermediate state.
                let snapshot = match self
                    .query_service
                    .get_work_item_snapshot(&identity.user_id, &identity.team_id, &input.work_item_id)
                    .await
                {
                    Ok(snapshot) => snapshot,
                    Err(_) => return Err(begin_error),
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
                if snapshot.work_item.revision != input.expected_work_revision
                    || observed_delivery_revision != input.expected_delivery_revision
                {
                    return Err(begin_error);
                }
                match (
                    snapshot.work_item.state.as_str(),
                    input.decision,
                    snapshot.work_item.delivery_requirement.as_str(),
                ) {
                    ("reviewing", _, _) => {}
                    ("accepted", TeamWorkReviewDecision::Accept, "none") => {
                        recovered_inline_accept = true;
                    }
                    _ => return Err(begin_error),
                }
            }
            Ok(begin) => {
                review_revision = begin.result.work_item_revision;
            }
        }

        if recovered_inline_accept {
            return command_value(
                self.execute(
                    identity,
                    &derived_key(&input.idempotency_key, "complete"),
                    TeamCommand::CompleteWithoutDelivery {
                        work_item_id: input.work_item_id,
                        expected_work_revision: review_revision,
                    },
                )
                .await?,
            );
        }
        let decision = match input.decision {
            TeamWorkReviewDecision::Accept => TeamCommand::Accept {
                work_item_id: input.work_item_id.clone(),
                expected_work_revision: review_revision,
                expected_delivery_revision: input.expected_delivery_revision,
            },
            TeamWorkReviewDecision::RequestChanges => TeamCommand::RequestChanges {
                work_item_id: input.work_item_id.clone(),
                expected_work_revision: review_revision,
                expected_delivery_revision: input.expected_delivery_revision,
                feedback: feedback.expect("request_changes feedback was validated"),
            },
            TeamWorkReviewDecision::Reject => TeamCommand::Reject {
                work_item_id: input.work_item_id.clone(),
                expected_work_revision: review_revision,
                expected_delivery_revision: input.expected_delivery_revision,
            },
        };
        let decided = self
            .execute(identity, &derived_key(&input.idempotency_key, "decision"), decision)
            .await?;
        match input.decision {
            TeamWorkReviewDecision::RequestChanges => {
                self.wake_notifications(identity).await;
            }
            TeamWorkReviewDecision::Accept if decided.result.delivery.is_some() => {
                self.wake_notifications(identity).await;
            }
            TeamWorkReviewDecision::Accept | TeamWorkReviewDecision::Reject => {}
        }
        if input.decision == TeamWorkReviewDecision::Accept && decided.result.delivery.is_none() {
            return command_value(
                self.execute(
                    identity,
                    &derived_key(&input.idempotency_key, "complete"),
                    TeamCommand::CompleteWithoutDelivery {
                        work_item_id: input.work_item_id,
                        expected_work_revision: decided.result.work_item_revision,
                    },
                )
                .await?,
            );
        }
        command_value(decided)
    }

    async fn wake_notifications(&self, identity: &RuntimeIdentity) {
        self.notification_wake.wake(&identity.user_id, &identity.team_id).await;
    }

    async fn cancel(&self, identity: &RuntimeIdentity, arguments: Value) -> Result<Value, TeamToolErrorPayload> {
        let input: CancelInput = parse_input(arguments)?;
        validate_public_key(&input.idempotency_key)?;
        let receipt = self
            .execute(
                identity,
                &input.idempotency_key,
                TeamCommand::Cancel {
                    work_item_id: input.work_item_id,
                    expected_work_revision: input.expected_work_revision,
                    expected_delivery_revision: input.expected_delivery_revision,
                },
            )
            .await?;
        self.wake_notifications(identity).await;
        command_value(receipt)
    }

    async fn integrate(&self, identity: &RuntimeIdentity, arguments: Value) -> Result<Value, TeamToolErrorPayload> {
        let input: IntegrateInput = parse_input(arguments)?;
        validate_public_key(&input.idempotency_key)?;
        let receipt = self
            .delivery_service
            .integrate_as_member(
                &identity.principal,
                &input.idempotency_key,
                BeginGitIntegrationRequest {
                    work_item_id: input.work_item_id,
                    expected_work_revision: input.expected_work_revision,
                    expected_delivery_revision: input.expected_delivery_revision,
                },
            )
            .await
            .map_err(map_delivery_error)?;
        if receipt.resolution == TeamIntegrationResolution::Conflicted {
            self.wake_notifications(identity).await;
        }
        serde_json::to_value(IntegrateTeamWorkResponse {
            attempt_id: receipt.attempt_id,
            work_item_id: receipt.work_item_id,
            delivery_id: receipt.delivery_id,
            resolution: match receipt.resolution {
                TeamIntegrationResolution::Merged => "merged",
                TeamIntegrationResolution::Conflicted => "conflicted",
                TeamIntegrationResolution::Retryable => "retryable",
            }
            .to_owned(),
            merged_commit: receipt.merged_commit,
            observed_target_head: receipt.observed_target_head,
            recovery_reason: receipt.recovery_reason.map(|reason| reason.as_str().to_owned()),
        })
        .map_err(|_| internal_error())
    }

    async fn execute(
        &self,
        identity: &RuntimeIdentity,
        idempotency_key: &str,
        command: TeamCommand,
    ) -> Result<TeamCommandReceipt, TeamToolErrorPayload> {
        self.command_service
            .execute(&identity.principal, idempotency_key, command)
            .await
            .map_err(map_command_error)
    }

    async fn resolve_identity(&self, context: &TeamToolContext) -> Result<RuntimeIdentity, TeamToolErrorPayload> {
        let conversation_id = context
            .conversation_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                tool_error(
                    TeamToolErrorCode::RuntimeContextMissing,
                    "trusted caller conversation is unavailable",
                )
            })?;
        let team = self
            .team_repo
            .get_team(&context.team_id)
            .await
            .map_err(|_| internal_error())?
            .ok_or_else(|| tool_error(TeamToolErrorCode::TeamNotFound, "team not found"))?;
        if context
            .user_id
            .as_deref()
            .is_some_and(|user_id| user_id != team.user_id)
        {
            return Err(tool_error(
                TeamToolErrorCode::PermissionDenied,
                "authenticated user does not own this Team",
            ));
        }
        let members: Vec<TeamAgent> = serde_json::from_str(&team.agents).map_err(|_| internal_error())?;
        let member = members
            .iter()
            .find(|member| member.slot_id == context.caller_slot_id)
            .ok_or_else(|| {
                tool_error(
                    TeamToolErrorCode::AgentNotFound,
                    "authenticated member is not in the Team",
                )
            })?;
        if member.conversation_id != conversation_id {
            return Err(tool_error(
                TeamToolErrorCode::PermissionDenied,
                "authenticated member credential does not match the caller conversation",
            ));
        }
        Ok(RuntimeIdentity {
            user_id: team.user_id.clone(),
            team_id: team.id.clone(),
            member_id: member.slot_id.clone(),
            principal: TeamCommandPrincipal::from_runtime(team.user_id, team.id, conversation_id),
        })
    }
}

struct RuntimeIdentity {
    user_id: String,
    team_id: String,
    member_id: String,
    principal: TeamCommandPrincipal,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct InspectInput {
    #[serde(default)]
    work_item_id: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DeliveryRequirementInput {
    None,
    Git,
}

impl From<DeliveryRequirementInput> for DeliveryRequirement {
    fn from(value: DeliveryRequirementInput) -> Self {
        match value {
            DeliveryRequirementInput::None => Self::None,
            DeliveryRequirementInput::Git => Self::Git,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DelegateInput {
    idempotency_key: String,
    #[serde(default)]
    parent_work_item_id: Option<String>,
    subject: String,
    #[serde(default)]
    description: Option<String>,
    assignee_member_id: String,
    delivery_requirement: DeliveryRequirementInput,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProgressAction {
    Start,
    Block,
    Resume,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressInput {
    idempotency_key: String,
    work_item_id: String,
    expected_work_revision: u64,
    action: ProgressAction,
    #[serde(default)]
    context: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum SubmissionKind {
    Inline,
    Git,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GitSubmissionInput {
    content_revision: u64,
    head_commit: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitInput {
    idempotency_key: String,
    work_item_id: String,
    expected_work_revision: u64,
    kind: SubmissionKind,
    evidence: String,
    #[serde(default)]
    git: Option<GitSubmissionInput>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewInput {
    idempotency_key: String,
    work_item_id: String,
    expected_work_revision: u64,
    #[serde(default)]
    expected_delivery_revision: Option<u64>,
    decision: TeamWorkReviewDecision,
    #[serde(default)]
    feedback: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct IntegrateInput {
    idempotency_key: String,
    work_item_id: String,
    expected_work_revision: u64,
    expected_delivery_revision: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelInput {
    idempotency_key: String,
    work_item_id: String,
    expected_work_revision: u64,
    #[serde(default)]
    expected_delivery_revision: Option<u64>,
}

fn parse_input<T: for<'de> Deserialize<'de>>(arguments: Value) -> Result<T, TeamToolErrorPayload> {
    serde_json::from_value(arguments).map_err(|error| schema_error(error.to_string()))
}

fn validate_public_key(key: &str) -> Result<(), TeamToolErrorPayload> {
    if key.trim().is_empty() || key.len() > PUBLIC_IDEMPOTENCY_KEY_MAX_LEN || key.chars().any(char::is_control) {
        return Err(schema_error(format!(
            "idempotency_key must contain 1..={PUBLIC_IDEMPOTENCY_KEY_MAX_LEN} bytes and no control characters"
        )));
    }
    Ok(())
}

fn validate_required_text(field: &str, value: &str, max_chars: usize) -> Result<String, TeamToolErrorPayload> {
    let value = value.trim();
    let count = value.chars().count();
    if !(1..=max_chars).contains(&count) {
        return Err(schema_error(format!("{field} must contain 1..={max_chars} characters")));
    }
    Ok(value.to_owned())
}

fn validate_progress_context(
    action: ProgressAction,
    context: Option<&str>,
) -> Result<Option<String>, TeamToolErrorPayload> {
    match action {
        ProgressAction::Block => {
            validate_required_text("context", context.unwrap_or_default(), BLOCK_CONTEXT_MAX_CHARS).map(Some)
        }
        ProgressAction::Start | ProgressAction::Resume if context.is_some() => {
            Err(schema_error("context is only allowed for block"))
        }
        ProgressAction::Start | ProgressAction::Resume => Ok(None),
    }
}

fn validate_review_feedback(
    decision: TeamWorkReviewDecision,
    feedback: Option<&str>,
) -> Result<Option<String>, TeamToolErrorPayload> {
    match decision {
        TeamWorkReviewDecision::RequestChanges => {
            validate_required_text("feedback", feedback.unwrap_or_default(), REVIEW_FEEDBACK_MAX_CHARS).map(Some)
        }
        TeamWorkReviewDecision::Accept | TeamWorkReviewDecision::Reject if feedback.is_some() => {
            Err(schema_error("feedback is only allowed for request_changes"))
        }
        TeamWorkReviewDecision::Accept | TeamWorkReviewDecision::Reject => Ok(None),
    }
}

fn derived_key(key: &str, step: &str) -> String {
    format!("{key}:{step}")
}

fn command_value(receipt: TeamCommandReceipt) -> Result<Value, TeamToolErrorPayload> {
    serde_json::to_value(command_response(receipt)).map_err(|_| internal_error())
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

fn work_item_value(
    work: TeamWorkItemResponse,
    viewer_member_id: &str,
    accepted_delivery_state: Option<&str>,
) -> Result<Value, TeamToolErrorPayload> {
    let actions = allowed_actions(&work, viewer_member_id, accepted_delivery_state);
    let mut value = serde_json::to_value(work).map_err(|_| internal_error())?;
    value["allowed_actions"] = json!(actions);
    Ok(value)
}

fn allowed_actions(
    work: &TeamWorkItemResponse,
    viewer_member_id: &str,
    accepted_delivery_state: Option<&str>,
) -> Vec<&'static str> {
    let mut actions = Vec::new();
    if work.assignee_member_id == viewer_member_id {
        match work.state.as_str() {
            "queued" | "changes_requested" => actions.push("start"),
            "running" => {
                actions.push("block");
                actions.push("submit");
            }
            "blocked" => actions.push("resume"),
            _ => {}
        }
    }
    if work.reviewer_member_id == viewer_member_id
        && (matches!(work.state.as_str(), "submitted" | "reviewing")
            || (work.state == "accepted" && work.delivery_requirement == "none"))
    {
        actions.push("review");
    }
    if work.integrator_member_id.as_deref() == Some(viewer_member_id)
        && work.state == "accepted"
        && work.delivery_requirement == "git"
        && work.accepted_delivery_id.is_some()
        && accepted_delivery_state == Some("accepted")
    {
        actions.push("integrate");
    }
    let can_cancel = !is_terminal(&work.state)
        && (!(work.state == "accepted" && work.delivery_requirement == "git")
            || accepted_delivery_state == Some("accepted"));
    if work.controller_member_id == viewer_member_id && can_cancel {
        actions.push("cancel");
    }
    actions
}

fn member_has_responsibility(work: &TeamWorkItemResponse, member_id: &str) -> bool {
    work.controller_member_id == member_id
        || work.assignee_member_id == member_id
        || work.reviewer_member_id == member_id
        || work.integrator_member_id.as_deref() == Some(member_id)
}

fn is_terminal(state: &str) -> bool {
    matches!(state, "completed" | "rejected" | "failed" | "cancelled")
}

fn single_line(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn map_command_error(error: TeamCommandError) -> TeamToolErrorPayload {
    let code = match error {
        TeamCommandError::TeamNotFound(_) => TeamToolErrorCode::TeamNotFound,
        TeamCommandError::WorkItemNotFound(_) => TeamToolErrorCode::WorkItemNotFound,
        TeamCommandError::ForbiddenTeam
        | TeamCommandError::CallerNotMember
        | TeamCommandError::MemberNotFound(_)
        | TeamCommandError::RelationPolicy(_)
        | TeamCommandError::WorkItemPolicy(_) => TeamToolErrorCode::PermissionDenied,
        TeamCommandError::InvalidParentWorkItem(_)
        | TeamCommandError::InvalidCommand(_)
        | TeamCommandError::ExpectedDeliveryRevisionRequired
        | TeamCommandError::UnexpectedDeliveryRevision
        | TeamCommandError::GitDeliveryRequired => TeamToolErrorCode::SchemaValidationFailed,
        TeamCommandError::RevisionConflict { .. } => TeamToolErrorCode::RevisionConflict,
        TeamCommandError::IdempotencyConflict
        | TeamCommandError::GitContentRevisionAlreadyUsed(_)
        | TeamCommandError::RosterChanged
        | TeamCommandError::ConcurrentSnapshotChange
        | TeamCommandError::WorkItemTransition(_)
        | TeamCommandError::GitDeliveryTransition(_) => TeamToolErrorCode::BusinessRuleViolation,
        TeamCommandError::CorruptAggregate(_)
        | TeamCommandError::CorruptReceipt(_)
        | TeamCommandError::Roster(_)
        | TeamCommandError::Snapshot(_)
        | TeamCommandError::Database(_)
        | TeamCommandError::Json(_) => TeamToolErrorCode::Internal,
    };
    let message = if code == TeamToolErrorCode::Internal {
        "canonical Team work service failed".to_owned()
    } else {
        error.to_string()
    };
    tool_error(code, message)
}

fn map_query_error(error: TeamQueryError) -> TeamToolErrorPayload {
    let code = match error {
        TeamQueryError::TeamNotFound(_) => TeamToolErrorCode::TeamNotFound,
        TeamQueryError::WorkItemNotFound(_) => TeamToolErrorCode::WorkItemNotFound,
        TeamQueryError::ForbiddenTeam => TeamToolErrorCode::PermissionDenied,
        TeamQueryError::ConcurrentSnapshotChange => TeamToolErrorCode::BusinessRuleViolation,
        TeamQueryError::CorruptStoredState { .. } | TeamQueryError::Database(_) => TeamToolErrorCode::Internal,
    };
    let message = if code == TeamToolErrorCode::Internal {
        "canonical Team work query failed".to_owned()
    } else {
        error.to_string()
    };
    tool_error(code, message)
}

fn map_delivery_error(error: TeamDeliveryError) -> TeamToolErrorPayload {
    let code = match &error {
        TeamDeliveryError::TeamNotFound(_) => TeamToolErrorCode::TeamNotFound,
        TeamDeliveryError::ForbiddenTeam | TeamDeliveryError::ForbiddenActor(_) => TeamToolErrorCode::PermissionDenied,
        TeamDeliveryError::InvalidRequest(_) => TeamToolErrorCode::SchemaValidationFailed,
        TeamDeliveryError::Conflict(_) => TeamToolErrorCode::RevisionConflict,
        TeamDeliveryError::Port(crate::ports::GitDeliveryPortError::Unavailable(_))
        | TeamDeliveryError::Workspace(crate::ports::GitWorkspacePortError::Unavailable(_)) => {
            TeamToolErrorCode::TransportUnavailable
        }
        TeamDeliveryError::AttemptNotFound(_)
        | TeamDeliveryError::Port(crate::ports::GitDeliveryPortError::InvalidIntent(_))
        | TeamDeliveryError::Workspace(crate::ports::GitWorkspacePortError::InvalidRequest(_)) => {
            TeamToolErrorCode::BusinessRuleViolation
        }
        TeamDeliveryError::CorruptState | TeamDeliveryError::Database(_) => TeamToolErrorCode::Internal,
    };
    let message = if code == TeamToolErrorCode::Internal {
        "canonical Team delivery service failed".to_owned()
    } else {
        error.to_string()
    };
    tool_error(code, message)
}

fn map_coordinator_error(error: TeamWorkCoordinatorError) -> TeamToolErrorPayload {
    match error {
        TeamWorkCoordinatorError::TeamNotFound(team_id) => {
            tool_error(TeamToolErrorCode::TeamNotFound, format!("Team not found: {team_id}"))
        }
        TeamWorkCoordinatorError::ForbiddenTeam => tool_error(
            TeamToolErrorCode::PermissionDenied,
            "authenticated user does not own this Team",
        ),
        TeamWorkCoordinatorError::GitWorkspaceRequired => tool_error(
            TeamToolErrorCode::BusinessRuleViolation,
            "Team workspace is not a Git repository",
        ),
        TeamWorkCoordinatorError::Git(crate::ports::GitWorkspacePortError::InvalidRequest(message)) => {
            tool_error(TeamToolErrorCode::SchemaValidationFailed, message)
        }
        TeamWorkCoordinatorError::Git(crate::ports::GitWorkspacePortError::Unavailable(message)) => {
            tool_error(TeamToolErrorCode::TransportUnavailable, message)
        }
        TeamWorkCoordinatorError::Command(command) => map_command_error(command),
        TeamWorkCoordinatorError::CorruptState | TeamWorkCoordinatorError::Database(_) => internal_error(),
    }
}

fn schema_error(message: impl Into<String>) -> TeamToolErrorPayload {
    tool_error(TeamToolErrorCode::SchemaValidationFailed, message)
}

fn internal_error() -> TeamToolErrorPayload {
    tool_error(TeamToolErrorCode::Internal, "canonical Team work service failed")
}

fn tool_error(code: TeamToolErrorCode, message: impl Into<String>) -> TeamToolErrorPayload {
    TeamToolErrorPayload::new(code, message)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use aionui_db::models::{MailboxMessageRow, TeamRow};
    use aionui_db::{ITeamRepository, SqliteTeamModeRepository, SqliteTeamRepository, init_database_memory};
    use async_trait::async_trait;
    use serde_json::json;

    use super::*;
    use crate::kernel::GitWorkAssignment;
    use crate::ports::{
        GitDeliveryPortError, GitDeliveryPortOutcome, GitDeliveryReconciliation, GitIntegrationIntent,
        GitIntegrationTarget, GitWorkAssignmentPlan, GitWorkspacePortError, PreparedGitWorkAssignment,
        TeamGitDeliveryPort, TeamGitWorkspacePort,
    };
    use crate::types::TeammateRole;

    struct TestGitWorkspace;

    #[derive(Default)]
    struct TestGitDelivery {
        integrate_calls: AtomicUsize,
        conflicted: AtomicBool,
    }

    struct TestWorkNotificationWake {
        repo: Arc<dyn ITeamRepository>,
        calls: AtomicUsize,
    }

    impl TestWorkNotificationWake {
        fn new(repo: Arc<dyn ITeamRepository>) -> Self {
            Self {
                repo,
                calls: AtomicUsize::new(0),
            }
        }

        async fn history(&self, target_member_id: &str) -> Vec<MailboxMessageRow> {
            self.repo.get_history("team-1", target_member_id, None).await.unwrap()
        }
    }

    #[async_trait]
    impl WorkNotificationWake for TestWorkNotificationWake {
        async fn wake(&self, _user_id: &str, _team_id: &str) {
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl TeamGitDeliveryPort for TestGitDelivery {
        async fn integrate(
            &self,
            _intent: &GitIntegrationIntent,
        ) -> Result<GitDeliveryPortOutcome, GitDeliveryPortError> {
            self.integrate_calls.fetch_add(1, Ordering::SeqCst);
            if self.conflicted.load(Ordering::SeqCst) {
                return Ok(GitDeliveryPortOutcome::Conflicted {
                    observed_target_head: "target-conflicted".into(),
                });
            }
            Ok(GitDeliveryPortOutcome::Merged {
                merged_commit: "merge-1".into(),
                observed_target_head: "merge-1".into(),
            })
        }

        async fn reconcile(
            &self,
            _intent: &GitIntegrationIntent,
        ) -> Result<GitDeliveryReconciliation, GitDeliveryPortError> {
            panic!("completed runtime integration must replay without reconciliation")
        }
    }

    #[async_trait]
    impl TeamGitWorkspacePort for TestGitWorkspace {
        async fn plan_assignment(
            &self,
            _plan: &GitWorkAssignmentPlan,
        ) -> Result<GitWorkAssignment, GitWorkspacePortError> {
            Ok(GitWorkAssignment::new("repo-1", "base-1", "refs/heads/team/worker"))
        }

        async fn prepare_assignment(
            &self,
            _plan: &GitWorkAssignmentPlan,
            assignment: &GitWorkAssignment,
        ) -> Result<PreparedGitWorkAssignment, GitWorkspacePortError> {
            Ok(PreparedGitWorkAssignment::new(
                assignment.clone(),
                "/repo/.worktrees/worker",
            ))
        }

        async fn resolve_integration_target(
            &self,
            _workspace: &str,
            _expected_repository_id: &str,
        ) -> Result<GitIntegrationTarget, GitWorkspacePortError> {
            Ok(GitIntegrationTarget::new("refs/heads/main", "base-1"))
        }
    }

    fn member(slot_id: &str, conversation_id: &str, role: TeammateRole) -> TeamAgent {
        TeamAgent {
            slot_id: slot_id.into(),
            name: slot_id.into(),
            role,
            conversation_id: conversation_id.into(),
            backend: "mock".into(),
            model: String::new(),
            assistant_id: None,
            status: None,
            conversation_type: None,
            cli_path: None,
        }
    }

    async fn runtime_with_notifications() -> (
        TeamWorkRuntimeService,
        Arc<TeamCommandService>,
        Arc<TestGitDelivery>,
        Arc<TestWorkNotificationWake>,
    ) {
        let database = init_database_memory().await.unwrap();
        let team_repo = Arc::new(SqliteTeamRepository::new(database.pool().clone()));
        let agents = vec![
            member("lead", "conv-lead", TeammateRole::Lead),
            member("worker", "conv-worker", TeammateRole::Teammate),
        ];
        team_repo
            .create_team(&TeamRow {
                id: "team-1".into(),
                user_id: "owner".into(),
                name: "Team".into(),
                workspace: "/repo".into(),
                workspace_mode: "shared".into(),
                agents: serde_json::to_string(&agents).unwrap(),
                lead_agent_id: Some("lead".into()),
                session_mode: None,
                agents_version: "1".into(),
                created_at: 1,
                updated_at: 1,
            })
            .await
            .unwrap();
        let team_repo: Arc<dyn ITeamRepository> = team_repo;
        let notification_wake = Arc::new(TestWorkNotificationWake::new(team_repo.clone()));
        let mode_repo = Arc::new(SqliteTeamModeRepository::new(database.pool().clone()));
        let command_service = Arc::new(TeamCommandService::new(team_repo.clone(), mode_repo.clone()));
        let git_workspace = Arc::new(TestGitWorkspace);
        let git_delivery = Arc::new(TestGitDelivery::default());
        let work_coordinator = Arc::new(TeamWorkCoordinator::new(
            team_repo.clone(),
            mode_repo.clone(),
            command_service.clone(),
            git_workspace.clone(),
        ));
        let delivery_service = Arc::new(TeamDeliveryService::new(
            team_repo.clone(),
            mode_repo.clone(),
            command_service.clone(),
            git_delivery.clone(),
            git_workspace,
        ));
        let runtime = TeamWorkRuntimeService::new(
            team_repo.clone(),
            work_coordinator,
            command_service.clone(),
            delivery_service,
            Arc::new(TeamQueryService::new(team_repo, mode_repo)),
            notification_wake.clone(),
        );
        (runtime, command_service, git_delivery, notification_wake)
    }

    async fn runtime_with_commands() -> (TeamWorkRuntimeService, Arc<TeamCommandService>, Arc<TestGitDelivery>) {
        let (runtime, command_service, git_delivery, _) = runtime_with_notifications().await;
        (runtime, command_service, git_delivery)
    }

    async fn runtime() -> TeamWorkRuntimeService {
        runtime_with_commands().await.0
    }

    fn context(slot_id: &str, conversation_id: &str, role: TeammateRole) -> TeamToolContext {
        TeamToolContext {
            team_id: "team-1".into(),
            caller_slot_id: slot_id.into(),
            caller_role: role,
            user_id: Some("owner".into()),
            conversation_id: Some(conversation_id.into()),
            transport: aionui_api_types::TeamToolTransport::Mcp,
        }
    }

    async fn delegate(runtime: &TeamWorkRuntimeService) -> Value {
        runtime
            .execute_tool(
                &context("lead", "conv-lead", TeammateRole::Lead),
                TeamToolCall {
                    tool: TeamToolName::TeamDelegate,
                    arguments: json!({
                        "idempotency_key": "delegate-1",
                        "subject": "Implement",
                        "assignee_member_id": "worker",
                        "delivery_requirement": "none"
                    }),
                },
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn delegate_notification_contains_exact_git_workspace_and_replay_is_single_mailbox_row() {
        let (runtime, _, _, notifications) = runtime_with_notifications().await;
        let lead = context("lead", "conv-lead", TeammateRole::Lead);
        let call = TeamToolCall {
            tool: TeamToolName::TeamDelegate,
            arguments: json!({
                "idempotency_key": "notify-delegate-git",
                "subject": "Implement in exact workspace",
                "assignee_member_id": "worker",
                "delivery_requirement": "git"
            }),
        };

        let delegated = runtime.execute_tool(&lead, call.clone()).await.unwrap();
        let replayed = runtime.execute_tool(&lead, call).await.unwrap();

        assert_eq!(replayed["replayed"], true);
        let history = notifications.history("worker").await;
        assert_eq!(history.len(), 1);
        assert!(history[0].content.contains(delegated["work_item_id"].as_str().unwrap()));
        assert!(history[0].content.contains("/repo/.worktrees/worker"));
        assert!(history[0].content.contains("team_inspect"));
    }

    #[tokio::test]
    async fn submit_notification_is_queued_for_reviewer() {
        let (runtime, _, _, notifications) = runtime_with_notifications().await;
        let delegated = delegate(&runtime).await;
        let work_item_id = delegated["work_item_id"].as_str().unwrap();
        let worker = context("worker", "conv-worker", TeammateRole::Teammate);
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "notify-submit-start",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamSubmit,
                    arguments: json!({
                        "idempotency_key": "notify-submit",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 2,
                        "kind": "inline",
                        "evidence": "Validated inline result."
                    }),
                },
            )
            .await
            .unwrap();

        let history = notifications.history("lead").await;
        assert_eq!(history.len(), 1);
        assert!(history[0].content.contains(work_item_id));
        assert!(history[0].content.contains("was submitted by member `worker`"));
        assert!(history[0].content.contains("Validated inline result."));
        assert!(history[0].content.contains("team_inspect"));
    }

    #[tokio::test]
    async fn blocked_work_notifies_its_controller() {
        let (runtime, _, _, notifications) = runtime_with_notifications().await;
        let delegated = delegate(&runtime).await;
        let work_item_id = delegated["work_item_id"].as_str().unwrap();
        let worker = context("worker", "conv-worker", TeammateRole::Teammate);
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "notify-block-start",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "notify-block",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 2,
                        "action": "block",
                        "context": "Waiting for the required dataset."
                    }),
                },
            )
            .await
            .unwrap();

        let history = notifications.history("lead").await;
        assert_eq!(history.len(), 1);
        assert!(history[0].content.contains(work_item_id));
        assert!(history[0].content.contains("was blocked"));
        assert!(history[0].content.contains("Waiting for the required dataset."));
    }

    #[tokio::test]
    async fn request_changes_notification_is_queued_for_assignee() {
        let (runtime, _, _, notifications) = runtime_with_notifications().await;
        let delegated = delegate(&runtime).await;
        let work_item_id = delegated["work_item_id"].as_str().unwrap();
        let worker = context("worker", "conv-worker", TeammateRole::Teammate);
        let lead = context("lead", "conv-lead", TeammateRole::Lead);
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "notify-changes-start",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamSubmit,
                    arguments: json!({
                        "idempotency_key": "notify-changes-submit",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 2,
                        "kind": "inline",
                        "evidence": "Initial result for review."
                    }),
                },
            )
            .await
            .unwrap();
        runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamReview,
                    arguments: json!({
                        "idempotency_key": "notify-changes-review",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 3,
                        "decision": "request_changes",
                        "feedback": "Add the missing robustness check."
                    }),
                },
            )
            .await
            .unwrap();

        let history = notifications.history("worker").await;
        let changes: Vec<_> = history
            .iter()
            .filter(|message| message.content.contains("Changes were requested"))
            .collect();
        assert_eq!(changes.len(), 1);
        assert!(changes[0].content.contains(work_item_id));
        assert!(changes[0].content.contains("Add the missing robustness check."));
        assert!(changes[0].content.contains("team_inspect"));
    }

    #[tokio::test]
    async fn accepted_git_delivery_notifies_integrator() {
        let (runtime, _, _, notifications) = runtime_with_notifications().await;
        let lead = context("lead", "conv-lead", TeammateRole::Lead);
        let worker = context("worker", "conv-worker", TeammateRole::Teammate);
        let delegated = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamDelegate,
                    arguments: json!({
                        "idempotency_key": "notify-accept-delegate",
                        "subject": "Prepare integration",
                        "assignee_member_id": "worker",
                        "delivery_requirement": "git"
                    }),
                },
            )
            .await
            .unwrap();
        let work_item_id = delegated["work_item_id"].as_str().unwrap();
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "notify-accept-start",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamSubmit,
                    arguments: json!({
                        "idempotency_key": "notify-accept-submit",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 2,
                        "kind": "git",
                        "evidence": "Git delivery passes the focused checks.",
                        "git": { "content_revision": 1, "head_commit": "head-1" }
                    }),
                },
            )
            .await
            .unwrap();
        runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamReview,
                    arguments: json!({
                        "idempotency_key": "notify-accept-review",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 3,
                        "expected_delivery_revision": 0,
                        "decision": "accept"
                    }),
                },
            )
            .await
            .unwrap();

        let history = notifications.history("lead").await;
        let integration: Vec<_> = history
            .iter()
            .filter(|message| message.content.contains("accepted Git delivery"))
            .collect();
        assert_eq!(integration.len(), 1);
        assert!(integration[0].content.contains(work_item_id));
        assert!(integration[0].content.contains("team_integrate"));
        assert_eq!(integration[0].from_agent_id, "team_work");
    }

    #[tokio::test]
    async fn integration_conflict_notifies_and_wakes_the_assignee() {
        let (runtime, _, git_delivery, notifications) = runtime_with_notifications().await;
        git_delivery.conflicted.store(true, Ordering::SeqCst);
        let lead = context("lead", "conv-lead", TeammateRole::Lead);
        let worker = context("worker", "conv-worker", TeammateRole::Teammate);
        let delegated = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamDelegate,
                    arguments: json!({
                        "idempotency_key": "conflict-notify-delegate",
                        "subject": "Prepare a conflicting delivery",
                        "assignee_member_id": "worker",
                        "delivery_requirement": "git"
                    }),
                },
            )
            .await
            .unwrap();
        let work_item_id = delegated["work_item_id"].as_str().unwrap();
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "conflict-notify-start",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamSubmit,
                    arguments: json!({
                        "idempotency_key": "conflict-notify-submit",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 2,
                        "kind": "git",
                        "evidence": "Conflict candidate is ready.",
                        "git": { "content_revision": 1, "head_commit": "head-1" }
                    }),
                },
            )
            .await
            .unwrap();
        runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamReview,
                    arguments: json!({
                        "idempotency_key": "conflict-notify-review",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 3,
                        "expected_delivery_revision": 0,
                        "decision": "accept"
                    }),
                },
            )
            .await
            .unwrap();
        let wakes_before = notifications.calls.load(Ordering::SeqCst);
        let integrated = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamIntegrate,
                    arguments: json!({
                        "idempotency_key": "conflict-notify-integrate",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 5,
                        "expected_delivery_revision": 1
                    }),
                },
            )
            .await
            .unwrap();

        assert_eq!(integrated["resolution"], "conflicted");
        assert!(notifications.calls.load(Ordering::SeqCst) > wakes_before);
        let conflict_messages = notifications
            .history("worker")
            .await
            .into_iter()
            .filter(|message| message.content.contains("Git integration conflicted"))
            .collect::<Vec<_>>();
        assert_eq!(conflict_messages.len(), 1);
        assert!(conflict_messages[0].content.contains("target-conflicted"));
        assert!(conflict_messages[0].content.contains("team_inspect"));
    }

    #[tokio::test]
    async fn changed_submission_evidence_conflicts_without_duplicate_notification() {
        let (runtime, _, _, notifications) = runtime_with_notifications().await;
        let delegated = delegate(&runtime).await;
        let work_item_id = delegated["work_item_id"].as_str().unwrap();
        let worker = context("worker", "conv-worker", TeammateRole::Teammate);
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "evidence-conflict-start",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();
        let first = TeamToolCall {
            tool: TeamToolName::TeamSubmit,
            arguments: json!({
                "idempotency_key": "evidence-conflict-submit",
                "work_item_id": work_item_id,
                "expected_work_revision": 2,
                "kind": "inline",
                "evidence": "Exact evidence A"
            }),
        };
        runtime.execute_tool(&worker, first.clone()).await.unwrap();
        let mut changed = first;
        changed.arguments["evidence"] = json!("Different evidence B");

        let conflict = runtime.execute_tool(&worker, changed).await.unwrap_err();
        assert_eq!(conflict.code, TeamToolErrorCode::BusinessRuleViolation);
        assert!(conflict.message.contains("idempotency key"));
        let history = notifications.history("lead").await;
        assert_eq!(history.len(), 1);
        assert!(history[0].content.contains("Exact evidence A"));
        assert!(!history[0].content.contains("Different evidence B"));
    }

    #[tokio::test]
    async fn authenticated_conversation_is_the_only_actor_source() {
        let runtime = runtime().await;
        let delegated = delegate(&runtime).await;
        let work_item_id = delegated["work_item_id"].as_str().unwrap();

        let forged = runtime
            .execute_tool(
                &context("lead", "conv-lead", TeammateRole::Lead),
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "start-forged",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start",
                        "actor_member_id": "worker"
                    }),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(forged.code, TeamToolErrorCode::SchemaValidationFailed);

        let started = runtime
            .execute_tool(
                &context("worker", "conv-worker", TeammateRole::Teammate),
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "start-real",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();
        assert_eq!(started["state"], "running");
    }

    #[tokio::test]
    async fn inline_work_flows_through_delegate_progress_submit_and_review() {
        let runtime = runtime().await;
        let delegated = delegate(&runtime).await;
        let work_item_id = delegated["work_item_id"].as_str().unwrap();
        let worker = context("worker", "conv-worker", TeammateRole::Teammate);
        let lead = context("lead", "conv-lead", TeammateRole::Lead);

        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "start-1",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();
        let submitted = runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamSubmit,
                    arguments: json!({
                        "idempotency_key": "submit-1",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 2,
                        "kind": "inline",
                        "evidence": "Inline flow result is complete."
                    }),
                },
            )
            .await
            .unwrap();
        assert_eq!(submitted["state"], "submitted");

        let reviewed = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamReview,
                    arguments: json!({
                        "idempotency_key": "review-1",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 3,
                        "decision": "accept"
                    }),
                },
            )
            .await
            .unwrap();
        assert_eq!(reviewed["state"], "completed");

        let inspected = runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamInspect,
                    arguments: json!({ "work_item_id": work_item_id }),
                },
            )
            .await
            .unwrap();
        assert_eq!(inspected["viewer_member_id"], "worker");
        assert_eq!(inspected["work_item"]["state"], "completed");
    }

    #[tokio::test]
    async fn review_resumes_when_begin_review_was_already_committed() {
        let (runtime, command_service, _) = runtime_with_commands().await;
        let delegated = delegate(&runtime).await;
        let work_item_id = delegated["work_item_id"].as_str().unwrap().to_owned();
        let worker = context("worker", "conv-worker", TeammateRole::Teammate);
        let lead = context("lead", "conv-lead", TeammateRole::Lead);

        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "recover-start-1",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamSubmit,
                    arguments: json!({
                        "idempotency_key": "recover-submit-1",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 2,
                        "kind": "inline",
                        "evidence": "Recovery flow result is complete."
                    }),
                },
            )
            .await
            .unwrap();

        command_service
            .execute_as_owner(
                "owner",
                "team-1",
                "recover-begin-committed",
                TeamCommand::BeginReview {
                    work_item_id: work_item_id.clone(),
                    expected_work_revision: 3,
                    expected_delivery_revision: None,
                },
            )
            .await
            .unwrap();

        let recovered = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamReview,
                    arguments: json!({
                        "idempotency_key": "recover-review-1",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 4,
                        "decision": "accept"
                    }),
                },
            )
            .await
            .unwrap();
        assert_eq!(recovered["state"], "completed");
    }

    #[tokio::test]
    async fn git_delegation_uses_coordinator_and_returns_prepared_workspace() {
        let runtime = runtime().await;
        let lead = context("lead", "conv-lead", TeammateRole::Lead);
        let delegated = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamDelegate,
                    arguments: json!({
                        "idempotency_key": "delegate-git-1",
                        "subject": "Implement in branch",
                        "assignee_member_id": "worker",
                        "delivery_requirement": "git"
                    }),
                },
            )
            .await
            .unwrap();
        assert_eq!(delegated["state"], "queued");
        assert_eq!(delegated["prepared_workspace"], "/repo/.worktrees/worker");

        let replayed = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamDelegate,
                    arguments: json!({
                        "idempotency_key": "delegate-git-1",
                        "subject": "Implement in branch",
                        "assignee_member_id": "worker",
                        "delivery_requirement": "git"
                    }),
                },
            )
            .await
            .unwrap();
        assert_eq!(replayed["replayed"], true);
        assert_eq!(replayed["work_item_id"], delegated["work_item_id"]);
        assert_eq!(replayed["prepared_workspace"], "/repo/.worktrees/worker");

        let inspected = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamInspect,
                    arguments: json!({ "work_item_id": delegated["work_item_id"] }),
                },
            )
            .await
            .unwrap();
        assert_eq!(inspected["work_item"]["git_assignment"]["base_commit"], "base-1");
        assert_eq!(
            inspected["work_item"]["git_assignment"]["branch_ref"],
            "refs/heads/team/worker"
        );
        assert!(
            inspected["work_item"]["git_assignment"].get("repository_id").is_none(),
            "repository identity stays internal to the workspace adapter"
        );

        let worker = context("worker", "conv-worker", TeammateRole::Teammate);
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "start-git-1",
                        "work_item_id": delegated["work_item_id"],
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();

        let forged_assignment = runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamSubmit,
                    arguments: json!({
                        "idempotency_key": "submit-git-forged",
                        "work_item_id": delegated["work_item_id"],
                        "expected_work_revision": 2,
                        "kind": "git",
                        "evidence": "Forged assignment must be rejected.",
                        "git": {
                            "repository_id": "other-repository",
                            "content_revision": 1,
                            "base_commit": "other-base",
                            "branch_ref": "refs/heads/other",
                            "head_commit": "head-1"
                        }
                    }),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(forged_assignment.code, TeamToolErrorCode::SchemaValidationFailed);

        let submitted = runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamSubmit,
                    arguments: json!({
                        "idempotency_key": "submit-git-1",
                        "work_item_id": delegated["work_item_id"],
                        "expected_work_revision": 2,
                        "kind": "git",
                        "evidence": "Git implementation and tests are ready.",
                        "git": {
                            "content_revision": 1,
                            "head_commit": "head-1"
                        }
                    }),
                },
            )
            .await
            .unwrap();
        assert_eq!(submitted["state"], "submitted");
        assert_eq!(submitted["delivery"]["head_commit"], "head-1");
    }

    #[tokio::test]
    async fn accepted_git_delivery_integrates_only_as_bound_member_and_replays_without_side_effect() {
        let (runtime, _, git_delivery) = runtime_with_commands().await;
        let lead = context("lead", "conv-lead", TeammateRole::Lead);
        let worker = context("worker", "conv-worker", TeammateRole::Teammate);
        let delegated = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamDelegate,
                    arguments: json!({
                        "idempotency_key": "integrate-delegate",
                        "subject": "Integrate exact delivery",
                        "assignee_member_id": "worker",
                        "delivery_requirement": "git"
                    }),
                },
            )
            .await
            .unwrap();
        let work_item_id = delegated["work_item_id"].as_str().unwrap().to_owned();
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamProgress,
                    arguments: json!({
                        "idempotency_key": "integrate-start",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 1,
                        "action": "start"
                    }),
                },
            )
            .await
            .unwrap();
        runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamSubmit,
                    arguments: json!({
                        "idempotency_key": "integrate-submit",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 2,
                        "kind": "git",
                        "evidence": "Integration candidate is ready.",
                        "git": { "content_revision": 1, "head_commit": "head-1" }
                    }),
                },
            )
            .await
            .unwrap();
        let accepted = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamReview,
                    arguments: json!({
                        "idempotency_key": "integrate-review",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 3,
                        "expected_delivery_revision": 0,
                        "decision": "accept"
                    }),
                },
            )
            .await
            .unwrap();
        assert_eq!(
            (accepted["state"].as_str(), accepted["revision"].as_u64()),
            (Some("accepted"), Some(5))
        );
        assert_eq!(accepted["delivery"]["revision"], 1);

        let inspected = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamInspect,
                    arguments: json!({ "work_item_id": work_item_id }),
                },
            )
            .await
            .unwrap();
        assert!(
            inspected["work_item"]["allowed_actions"]
                .as_array()
                .unwrap()
                .contains(&json!("integrate"))
        );

        let wrong_actor = runtime
            .execute_tool(
                &worker,
                TeamToolCall {
                    tool: TeamToolName::TeamIntegrate,
                    arguments: json!({
                        "idempotency_key": "integrate-wrong-actor",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 5,
                        "expected_delivery_revision": 1
                    }),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(wrong_actor.code, TeamToolErrorCode::PermissionDenied);

        let wrong_revision = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamIntegrate,
                    arguments: json!({
                        "idempotency_key": "integrate-wrong-revision",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 4,
                        "expected_delivery_revision": 1
                    }),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(wrong_revision.code, TeamToolErrorCode::RevisionConflict);
        assert_eq!(git_delivery.integrate_calls.load(Ordering::SeqCst), 0);

        let forged_evidence = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamIntegrate,
                    arguments: json!({
                        "idempotency_key": "integrate-forged",
                        "work_item_id": work_item_id,
                        "expected_work_revision": 5,
                        "expected_delivery_revision": 1,
                        "merged_commit": "self-reported"
                    }),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(forged_evidence.code, TeamToolErrorCode::SchemaValidationFailed);

        let arguments = json!({
            "idempotency_key": "integrate-exact",
            "work_item_id": work_item_id,
            "expected_work_revision": 5,
            "expected_delivery_revision": 1
        });
        let merged = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamIntegrate,
                    arguments: arguments.clone(),
                },
            )
            .await
            .unwrap();
        assert_eq!(merged["resolution"], "merged");
        assert_eq!(merged["merged_commit"], "merge-1");
        assert!(merged["attempt_id"].as_str().is_some_and(|value| !value.is_empty()));
        assert_eq!(git_delivery.integrate_calls.load(Ordering::SeqCst), 1);

        let replayed = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamIntegrate,
                    arguments,
                },
            )
            .await
            .unwrap();
        assert_eq!(replayed, merged);
        assert_eq!(git_delivery.integrate_calls.load(Ordering::SeqCst), 1);

        let completed = runtime
            .execute_tool(
                &lead,
                TeamToolCall {
                    tool: TeamToolName::TeamInspect,
                    arguments: json!({ "work_item_id": work_item_id }),
                },
            )
            .await
            .unwrap();
        assert_eq!(completed["work_item"]["state"], "completed");
        assert_eq!(completed["deliveries"][0]["state"], "merged");
        assert_eq!(completed["deliveries"][0]["merged_commit"], "merge-1");
    }

    #[tokio::test]
    async fn credential_and_conversation_must_resolve_to_the_same_member() {
        let runtime = runtime().await;
        let error = runtime
            .execute_tool(
                &context("worker", "conv-lead", TeammateRole::Teammate),
                TeamToolCall {
                    tool: TeamToolName::TeamInspect,
                    arguments: json!({}),
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, TeamToolErrorCode::PermissionDenied);
    }

    #[test]
    fn accepted_git_cancel_is_advertised_only_for_an_explicitly_accepted_delivery() {
        let work = TeamWorkItemResponse {
            id: "work-1".into(),
            team_id: "team-1".into(),
            parent_work_item_id: None,
            subject: "Integrate exact delivery".into(),
            description: None,
            controller_member_id: "lead".into(),
            assignee_member_id: "worker".into(),
            reviewer_member_id: "lead".into(),
            integrator_member_id: Some("lead".into()),
            delivery_requirement: "git".into(),
            git_assignment: None,
            state: "accepted".into(),
            current_submission: None,
            accepted_delivery_id: Some("delivery-1".into()),
            revision: 5,
            created_at: 1,
            updated_at: 1,
        };

        assert!(!allowed_actions(&work, "lead", None).contains(&"cancel"));
        assert!(!allowed_actions(&work, "lead", None).contains(&"integrate"));
        assert!(allowed_actions(&work, "lead", Some("accepted")).contains(&"cancel"));
        assert!(allowed_actions(&work, "lead", Some("accepted")).contains(&"integrate"));
        assert!(!allowed_actions(&work, "lead", Some("integrating")).contains(&"cancel"));
        assert!(!allowed_actions(&work, "lead", Some("integrating")).contains(&"integrate"));
    }
}
