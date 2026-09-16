use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use aionui_ai_agent::types::{BuildTaskOptions, SendMessageData};
use aionui_ai_agent::{AgentError, AgentInstance, AgentSendError, AgentSessionKind, IWorkerTaskManager};
use aionui_common::{AgentType, ConversationStatus, ErrorChain, now_ms};
use aionui_db::models::ConversationRow;
use tokio::sync::oneshot;
use tracing::{debug, error, info, warn};

use crate::agent_health_policy::{AgentHealthAction, AgentHealthPolicy};
use crate::runtime_state::RuntimeLifecycleState;
use crate::runtime_state::TurnClaim;
use crate::service::math_run_inputs::{self, IntegrityError, MathRunInputs};
use crate::service::{
    ConversationAgentTurnFailureKind, ConversationAgentTurnStartedCallback, ConversationService,
    MAX_SYSTEM_RESPONSE_CONTINUATIONS_PER_TURN, agent_error_top_level_code, persist_session_key,
};
use crate::stream_relay::{RelayOutcome, StreamRelay, TurnAttemptSummary, is_transport_disconnect_message};
use crate::turn_continuation_policy::{ContinuationDecision, TurnContinuationPolicy};
use crate::turn_recovery_policy::{TurnRecoveryDecision, TurnRecoveryPolicy};
use aionui_api_types::{AgentErrorCode, SendMessageRequest};

fn acp_backend_from_build_options(options: &BuildTaskOptions) -> Option<&str> {
    match &options.context.kind {
        AgentSessionKind::Acp(ctx) => ctx.config.backend.as_deref(),
        AgentSessionKind::Aionrs(_) => None,
    }
}

pub(crate) struct TurnStartInput {
    pub user_id: String,
    pub conversation: ConversationRow,
    pub request: SendMessageRequest,
    pub required_runtime_mode: Option<String>,
    pub build_options: BuildTaskOptions,
    pub stored_workspace: String,
    pub turn_id: String,
    pub turn_claim: TurnClaim,
    /// Fires exactly once when the first stream event establishes that the
    /// Agent task has started the prompt. Task-option resolution, task build,
    /// workspace persistence, runtime-mode setup, and failures before a stream
    /// event are all pre-acceptance.
    pub on_started: Option<ConversationAgentTurnStartedCallback>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ConversationTurnStatus {
    Completed,
    Failed,
}

pub(crate) struct ConversationTurnResult {
    pub status: ConversationTurnStatus,
    pub error_message: Option<String>,
    pub failure_kind: Option<ConversationAgentTurnFailureKind>,
}

pub(crate) struct ConversationTurnOrchestrator {
    service: ConversationService,
    task_manager: Arc<dyn IWorkerTaskManager>,
}

struct TurnAttemptInput {
    run_inputs: Result<Option<MathRunInputs>, IntegrityError>,
    conv_id: String,
    turn_id: String,
    user_id: String,
    build_options: BuildTaskOptions,
    stored_workspace: String,
    send: SendMessageData,
    msg_id: String,
    allowed_skill_names: Vec<String>,
    required_runtime_mode: Option<String>,
    continuation_count: usize,
    defer_clean_terminal_errors: bool,
    on_started: Option<ConversationAgentTurnStartedCallback>,
    prompt_accepted: Arc<AtomicBool>,
}

struct TurnAttemptResult {
    outcome: RelayOutcome,
    summary: TurnAttemptSummary,
    agent_type: AgentType,
    backend: Option<String>,
}

impl ConversationTurnOrchestrator {
    pub fn new(service: ConversationService, task_manager: Arc<dyn IWorkerTaskManager>) -> Self {
        Self { service, task_manager }
    }

    pub fn spawn_user_turn(self, input: TurnStartInput) {
        tokio::spawn(async move {
            let _ = self.run_user_turn(input).await;
        });
    }

    async fn run_attempt(&self, input: TurnAttemptInput) -> Result<TurnAttemptResult, ConversationTurnResult> {
        let run_inputs = match input.run_inputs {
            Ok(inputs) => inputs,
            Err(_) => return Err(self.reject_math_inputs(&input.conv_id, &input.turn_id, true).await),
        };
        if math_run_inputs::verify_optional(run_inputs.as_ref()).is_err() {
            return Err(self.reject_math_inputs(&input.conv_id, &input.turn_id, true).await);
        }
        let build_started_at = now_ms();
        let availability_agent_id = availability_agent_id(&input.build_options);
        let backend = acp_backend_from_build_options(&input.build_options).map(str::to_owned);
        info!(
            conversation_id = %input.conv_id,
            turn_id = %input.turn_id,
            "Agent task build started"
        );

        let agent = match self
            .task_manager
            .get_or_build_task(&input.conv_id, input.build_options)
            .await
        {
            Ok(agent) => agent,
            Err(err) => {
                let top_level_code = agent_error_top_level_code(&err);
                let send_error = AgentSendError::from_agent_error_ref_for_backend(&err, backend.as_deref());
                let top_level_code = if send_error.is_openclaw_gateway_unreachable() {
                    "USER_AGENT_OPENCLAW_GATEWAY_UNREACHABLE"
                } else {
                    top_level_code
                };
                if send_error.is_openclaw_gateway_unreachable() {
                    warn!(
                        conversation_id = %input.conv_id,
                        turn_id = %input.turn_id,
                        backend = "openclaw",
                        error_kind = "openclaw_gateway_unreachable",
                        port = 18789_u16,
                        phase = "turn_build",
                        "OpenClaw Gateway unreachable during ACP startup"
                    );
                }
                error!(
                    conversation_id = %input.conv_id,
                    turn_id = %input.turn_id,
                    error_code = ?send_error.code(),
                    error = %ErrorChain(&err),
                    "Agent task build failed"
                );
                let failure_message = send_error_display_message(&send_error);
                record_agent_session_failure(
                    &self.service,
                    availability_agent_id.as_deref(),
                    "session_build_failed",
                    &failure_message,
                )
                .await;
                self.service
                    .persist_and_broadcast_send_failure_tip(
                        &input.conv_id,
                        &input.turn_id,
                        &send_error,
                        Some(top_level_code),
                    )
                    .await;
                return Err(ConversationTurnResult {
                    failure_kind: None,
                    status: ConversationTurnStatus::Failed,
                    error_message: Some(failure_message),
                });
            }
        };

        if let Err(err) = self
            .service
            .maybe_persist_workspace(&input.conv_id, &input.stored_workspace, agent.workspace())
            .await
        {
            let top_level_code = err.error_code();
            let failure_message = err.to_string();
            let send_error = AgentSendError::from_agent_error(err.to_agent_error());
            error!(
                conversation_id = %input.conv_id,
                turn_id = %input.turn_id,
                error_code = err.error_code(),
                error = %ErrorChain(&err),
                "Failed to persist resolved workspace"
            );
            self.service
                .persist_and_broadcast_send_failure_tip(
                    &input.conv_id,
                    &input.turn_id,
                    &send_error,
                    Some(top_level_code),
                )
                .await;
            return Err(ConversationTurnResult {
                failure_kind: None,
                status: ConversationTurnStatus::Failed,
                error_message: Some(failure_message),
            });
        }

        info!(
            conversation_id = %input.conv_id,
            turn_id = %input.turn_id,
            agent_type = ?agent.agent_type(),
            elapsed_ms = now_ms().saturating_sub(build_started_at),
            "Agent task ready"
        );

        let persistence = self.service.runtime_persistence();
        let runtime_state = self.service.runtime_state();
        let mut pending_send = Some((input.send, input.msg_id));
        let mut continuation_count = input.continuation_count;
        let continuation_policy = TurnContinuationPolicy::new(MAX_SYSTEM_RESPONSE_CONTINUATIONS_PER_TURN);
        let mut last_outcome = None;
        let mut aggregate_summary = TurnAttemptSummary::default();

        while let Some((current_send, msg_id)) = pending_send.take() {
            let lifecycle = runtime_state.lifecycle_for(&input.conv_id);
            let defer_clean_terminal_errors = input.defer_clean_terminal_errors
                && agent.agent_type() == AgentType::Acp
                && lifecycle == RuntimeLifecycleState::Active
                && aggregate_summary.safe_to_auto_replay();
            let relay = StreamRelay::new(
                input.conv_id.clone(),
                msg_id,
                input.turn_id.clone(),
                input.user_id.clone(),
                self.service.conversation_repo().clone(),
                self.service.broadcaster().clone(),
            )
            .with_skill_resolver(self.service.skill_resolver())
            .with_allowed_skill_names(input.allowed_skill_names.clone())
            .with_runtime_state(Arc::clone(&runtime_state))
            .with_persistence(persistence.clone())
            .with_turn_completion(false)
            .with_defer_clean_terminal_errors(defer_clean_terminal_errors)
            .with_started_callback(input.on_started.clone(), Arc::clone(&input.prompt_accepted));

            let rx = agent.subscribe();
            if let Some(mode) = input
                .required_runtime_mode
                .as_deref()
                .map(str::trim)
                .filter(|mode| !mode.is_empty())
            {
                match apply_required_runtime_mode(&agent, mode).await {
                    Ok(()) => {
                        info!(
                            conversation_id = %input.conv_id,
                            turn_id = %input.turn_id,
                            mode,
                            "Confirmed required runtime mode before agent turn"
                        );
                    }
                    Err(err) => {
                        let top_level_code = agent_error_top_level_code(&err);
                        let failure_message = err.to_string();
                        let send_error = AgentSendError::from_agent_error(err);
                        error!(
                            conversation_id = %input.conv_id,
                            turn_id = %input.turn_id,
                            mode,
                            error = %failure_message,
                            "Failed to apply required runtime mode before agent turn"
                        );
                        self.service
                            .persist_and_broadcast_send_failure_tip(
                                &input.conv_id,
                                &input.turn_id,
                                &send_error,
                                Some(top_level_code),
                            )
                            .await;
                        return Err(ConversationTurnResult {
                            failure_kind: None,
                            status: ConversationTurnStatus::Failed,
                            error_message: Some(failure_message),
                        });
                    }
                }
            }
            let send_agent = agent.clone();
            let conv_id_send = input.conv_id.clone();
            let turn_id_for_send = input.turn_id.clone();
            let feedback_service = self.service.clone();
            let feedback_agent_id = availability_agent_id.clone();
            let (send_error_tx, send_error_rx) = oneshot::channel();
            let send_inputs = run_inputs.clone();

            let send_task = tokio::spawn(async move {
                // Cold task build, runtime-mode confirmation and task scheduling
                // may all await. Verify in the sending task, immediately before
                // handing the prompt to the agent, including every continuation.
                if math_run_inputs::verify_optional(send_inputs.as_ref()).is_err() {
                    let _ = send_error_tx.send(AgentSendError::from_agent_error(AgentError::bad_request(
                        math_run_inputs::INTEGRITY_MESSAGE,
                    )));
                    return true;
                }
                match send_agent.send_message(current_send).await {
                    Ok(()) => {}
                    Err(e) => {
                        let failure_message = send_error_display_message(&e);
                        record_agent_session_failure(
                            &feedback_service,
                            feedback_agent_id.as_deref(),
                            "session_send_failed",
                            &failure_message,
                        )
                        .await;
                        let task_status = send_agent.status();
                        let agent_type = send_agent.agent_type();
                        error!(
                            conversation_id = %conv_id_send,
                            turn_id = %turn_id_for_send,
                            ?agent_type,
                            ?task_status,
                            error = %ErrorChain(&e),
                            "Agent send_message failed"
                        );
                        if task_status == Some(ConversationStatus::Finished) {
                            debug!(
                                conversation_id = %conv_id_send,
                                turn_id = %turn_id_for_send,
                                ?agent_type,
                                "Agent send_message failed on finished task; relay will prefer any runtime terminal before fallback"
                            );
                        }
                        warn!(
                            conversation_id = %conv_id_send,
                            turn_id = %turn_id_for_send,
                            ?agent_type,
                            code = ?e.code(),
                            ownership = ?e.ownership(),
                            "Agent send_message returned error; offering fallback stream error to relay"
                        );
                        let _ = send_error_tx.send(e);
                    }
                }
                false
            });

            let outcome = relay.consume_with_send_error(rx, send_error_rx).await;
            match send_task.await {
                Ok(true) => {
                    // Publish errors deferred by the ordinary retry policy, but
                    // bypass continuation and auto-replay for identity loss.
                    return Err(self
                        .reject_math_inputs(&input.conv_id, &input.turn_id, outcome.attempt.terminal_error_deferred)
                        .await);
                }
                Ok(false) => {}
                Err(error) => {
                    error!(
                        conversation_id = %input.conv_id,
                        turn_id = %input.turn_id,
                        error = %error,
                        "Agent send task terminated unexpectedly"
                    );
                }
            }
            aggregate_summary.merge(&outcome.attempt);

            if let Some(session_key) = agent.get_session_key() {
                persist_session_key(
                    self.service.conversation_repo(),
                    &persistence,
                    &input.conv_id,
                    &session_key,
                )
                .await;
            }

            match continuation_policy.decide(&input.conv_id, continuation_count, &outcome, lifecycle) {
                ContinuationDecision::Continue { content, next_count } => {
                    continuation_count = next_count;
                    let next_turn_msg_id = ConversationService::mint_msg_id();
                    pending_send = Some((
                        SendMessageData {
                            content,
                            msg_id: next_turn_msg_id.clone(),
                            turn_id: Some(input.turn_id.clone()),
                            files: vec![],
                            inject_skills: vec![],
                        },
                        next_turn_msg_id,
                    ));
                }
                ContinuationDecision::Stop(_) => {
                    last_outcome = Some(outcome);
                    break;
                }
            }
        }

        Ok(TurnAttemptResult {
            outcome: last_outcome.unwrap_or_default(),
            summary: aggregate_summary,
            agent_type: agent.agent_type(),
            backend,
        })
    }

    pub(crate) async fn run_user_turn(self, input: TurnStartInput) -> ConversationTurnResult {
        let mut turn_claim = input.turn_claim;
        let conv_id = input.conversation.id.clone();
        let turn_id = input.turn_id.clone();
        let runtime_state = self.service.runtime_state();
        let allowed_skill_names = input.build_options.context.skills.clone();
        let first_turn_msg_id = ConversationService::mint_msg_id();
        let initial_send = SendMessageData {
            content: input.request.content,
            msg_id: first_turn_msg_id.clone(),
            turn_id: Some(turn_id.clone()),
            files: input.request.files,
            inject_skills: input.request.inject_skills,
        };
        let mut replayed = false;
        let mut replay_started_at = None;
        let mut final_error_message;
        let mut auth_failure = false;
        let mut failure_kind = None;
        let prompt_accepted = Arc::new(AtomicBool::new(false));
        let run_inputs = math_run_inputs::from_persisted_extra(&input.conversation.extra);

        info!(conversation_id = %conv_id, turn_id = %turn_id, "conversation turn orchestrator started");

        let final_failed = loop {
            let attempt_number = if replayed { 2 } else { 1 };
            let attempt_result = match self
                .run_attempt(TurnAttemptInput {
                    run_inputs: run_inputs.clone(),
                    conv_id: conv_id.clone(),
                    turn_id: turn_id.clone(),
                    user_id: input.user_id.clone(),
                    build_options: input.build_options.clone(),
                    stored_workspace: input.stored_workspace.clone(),
                    send: initial_send.clone(),
                    msg_id: first_turn_msg_id.clone(),
                    allowed_skill_names: allowed_skill_names.clone(),
                    required_runtime_mode: input.required_runtime_mode.clone(),
                    continuation_count: 0,
                    defer_clean_terminal_errors: !replayed,
                    on_started: input.on_started.clone(),
                    prompt_accepted: Arc::clone(&prompt_accepted),
                })
                .await
            {
                Ok(result) => result,
                Err(result) => {
                    failure_kind = result.failure_kind;
                    final_error_message = result.error_message;
                    break result.status == ConversationTurnStatus::Failed;
                }
            };

            // Track the final attempt's auth signal so the post-loop availability
            // write-back can reflect "needs sign-in" (last iteration wins).
            auth_failure = terminal_is_auth_failure(&attempt_result.outcome);

            let lifecycle = runtime_state.lifecycle_for(&conv_id);
            if !attempt_result.outcome.terminal.is_error() {
                final_error_message = None;
                if replayed {
                    info!(
                        conversation_id = %conv_id,
                        turn_id = %turn_id,
                        attempt = attempt_number,
                        elapsed_ms = replay_started_at
                            .map(|started_at| now_ms().saturating_sub(started_at))
                            .unwrap_or_default(),
                        "conversation turn auto replay completed"
                    );
                }
                break false;
            }
            final_error_message = turn_attempt_error_message(&attempt_result.summary);
            if replayed {
                warn!(
                    conversation_id = %conv_id,
                    turn_id = %turn_id,
                    attempt = attempt_number,
                    error_code = ?attempt_result.outcome.terminal.code(),
                    retryable = ?attempt_result.outcome.terminal.retryable(),
                    "conversation turn auto replay failed"
                );
            }

            let mut recovery_outcome = attempt_result.outcome.clone();
            recovery_outcome.attempt = attempt_result.summary.clone();
            let decision = TurnRecoveryPolicy::decide(
                attempt_result.agent_type,
                attempt_result.backend.as_deref(),
                &recovery_outcome,
                lifecycle,
                replayed,
            );

            match decision {
                TurnRecoveryDecision::AutoReplayOnce { reason, .. } => {
                    replay_started_at = Some(now_ms());
                    info!(
                        conversation_id = %conv_id,
                        turn_id = %turn_id,
                        attempt = attempt_number,
                        next_attempt = attempt_number + 1,
                        backend = attempt_result.backend.as_deref().unwrap_or("unknown"),
                        error_code = ?attempt_result.outcome.terminal.code(),
                        retryable = ?attempt_result.outcome.terminal.retryable(),
                        ?reason,
                        "conversation turn auto replay starting"
                    );
                    self.service
                        .evict_acp_task_after_terminal_error(
                            &conv_id,
                            attempt_result.agent_type,
                            &attempt_result.outcome,
                            &self.task_manager,
                        )
                        .await;
                    replayed = true;
                    continue;
                }
                TurnRecoveryDecision::None => {
                    if attempt_result.outcome.attempt.terminal_error_deferred
                        && let Some(data) = attempt_result.outcome.attempt.terminal_error.clone()
                    {
                        let send_error = AgentSendError::from_stream_error_data(data);
                        self.service
                            .persist_and_broadcast_send_failure_tip(&conv_id, &turn_id, &send_error, None)
                            .await;
                    }

                    match AgentHealthPolicy::decide(attempt_result.agent_type, &attempt_result.outcome, lifecycle) {
                        AgentHealthAction::Keep => {}
                        AgentHealthAction::EvictAcpTask { .. } => {
                            self.service
                                .evict_acp_task_after_terminal_error(
                                    &conv_id,
                                    attempt_result.agent_type,
                                    &attempt_result.outcome,
                                    &self.task_manager,
                                )
                                .await;
                        }
                    }
                    break true;
                }
            }
        };

        if auth_failure {
            // The agent connected (detection saw it online) but a real turn hit
            // an explicit auth signal — write "needs sign-in" back to its
            // availability so the list stops showing it as plainly usable.
            record_agent_session_failure(
                &self.service,
                availability_agent_id(&input.build_options).as_deref(),
                "auth_required",
                final_error_message
                    .as_deref()
                    .unwrap_or("Agent requires sign-in to run."),
            )
            .await;
        } else if !final_failed {
            record_agent_session_success(&self.service, availability_agent_id(&input.build_options).as_deref()).await;
        }

        let was_deleting = turn_claim.release_for_turn(&turn_id);
        self.service
            .complete_released_turn(&conv_id, &turn_id, was_deleting)
            .await;

        ConversationTurnResult {
            failure_kind,
            status: if final_failed {
                ConversationTurnStatus::Failed
            } else {
                ConversationTurnStatus::Completed
            },
            error_message: if final_failed { final_error_message } else { None },
        }
    }

    async fn reject_math_inputs(
        &self,
        conversation_id: &str,
        turn_id: &str,
        persist_tip: bool,
    ) -> ConversationTurnResult {
        error!(
            conversation_id,
            turn_id,
            error_code = math_run_inputs::INTEGRITY_CODE,
            "Mathematics turn rejected because frozen run inputs failed integrity checks"
        );
        if persist_tip {
            self.service
                .persist_and_broadcast_send_failure_tip(
                    conversation_id,
                    turn_id,
                    &AgentSendError::from_agent_error(AgentError::bad_request(math_run_inputs::INTEGRITY_MESSAGE)),
                    Some(math_run_inputs::INTEGRITY_CODE),
                )
                .await;
        }
        ConversationTurnResult {
            failure_kind: Some(ConversationAgentTurnFailureKind::RunInputsIntegrity),
            status: ConversationTurnStatus::Failed,
            error_message: Some(math_run_inputs::INTEGRITY_MESSAGE.into()),
        }
    }
}

fn availability_agent_id(options: &BuildTaskOptions) -> Option<String> {
    match &options.context.kind {
        AgentSessionKind::Acp(context) => context
            .config
            .agent_id
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(str::to_owned),
        AgentSessionKind::Aionrs(_) => None,
    }
}

/// True when the turn's terminal is an explicit authentication signal: an
/// `ACP_EMPTY_TURN_NEEDS_AUTH` benign tip (the agent connected but isn't signed
/// in and returned an empty end_turn), or an Error terminal carrying an
/// auth/login error code. Used to reflect "needs sign-in" into the agent's
/// availability even when detection (initialize + session/new, no prompt)
/// showed it online. Non-auth outcomes — generic empty turns, billing,
/// rate-limit, context, network — are deliberately excluded so we don't flip an
/// agent to unavailable for transient or unrelated failures.
fn terminal_is_auth_failure(outcome: &RelayOutcome) -> bool {
    if outcome.attempt.needs_auth {
        return true;
    }
    matches!(
        outcome.terminal.code(),
        Some(
            AgentErrorCode::UserAgentAuthRequired
                | AgentErrorCode::UserLlmProviderAuthFailed
                | AgentErrorCode::UserLlmProviderAwsSsoExpired
        )
    )
}

async fn apply_required_runtime_mode(agent: &AgentInstance, mode: &str) -> Result<(), AgentError> {
    agent.set_config_option("mode", mode).await?;
    Ok(())
}

fn send_error_display_message(error: &AgentSendError) -> String {
    error
        .stream_error()
        .detail
        .clone()
        .unwrap_or_else(|| error.stream_error().message.clone())
}

fn turn_attempt_error_message(summary: &TurnAttemptSummary) -> Option<String> {
    summary.terminal_error.as_ref().map(|error| {
        if is_transport_disconnect_message(&error.message) {
            error.message.clone()
        } else {
            error
                .detail
                .as_deref()
                .filter(|detail| !detail.trim().is_empty())
                .unwrap_or(error.message.as_str())
                .to_owned()
        }
    })
}

async fn record_agent_session_failure(
    service: &ConversationService,
    agent_id: Option<&str>,
    code: &str,
    message: &str,
) {
    let Some(agent_id) = agent_id else {
        return;
    };
    let Some(feedback) = service.agent_availability_feedback() else {
        return;
    };
    if let Err(error) = feedback.record_session_failure(agent_id, code, message).await {
        warn!(
            agent_id,
            code,
            error = %ErrorChain(&error),
            "Failed to record agent availability session failure"
        );
    }
}

async fn record_agent_session_success(service: &ConversationService, agent_id: Option<&str>) {
    let Some(agent_id) = agent_id else {
        return;
    };
    let Some(feedback) = service.agent_availability_feedback() else {
        return;
    };
    if let Err(error) = feedback.record_session_success(agent_id).await {
        warn!(
            agent_id,
            error = %ErrorChain(&error),
            "Failed to record agent availability session success"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream_relay::RelayTerminal;

    fn finish_outcome(needs_auth: bool) -> RelayOutcome {
        RelayOutcome {
            system_responses: vec![],
            terminal: RelayTerminal::Finish,
            attempt: TurnAttemptSummary {
                needs_auth,
                ..Default::default()
            },
        }
    }

    fn error_outcome(code: AgentErrorCode) -> RelayOutcome {
        RelayOutcome {
            system_responses: vec![],
            terminal: RelayTerminal::Error {
                code: Some(code),
                retryable: None,
            },
            attempt: TurnAttemptSummary::default(),
        }
    }

    #[test]
    fn needs_auth_empty_turn_is_auth_failure() {
        assert!(terminal_is_auth_failure(&finish_outcome(true)));
    }

    #[test]
    fn plain_finish_is_not_auth_failure() {
        // A generic empty turn (or any normal finish) must NOT flip availability.
        assert!(!terminal_is_auth_failure(&finish_outcome(false)));
    }

    #[test]
    fn explicit_auth_error_codes_are_auth_failure() {
        assert!(terminal_is_auth_failure(&error_outcome(
            AgentErrorCode::UserAgentAuthRequired
        )));
        assert!(terminal_is_auth_failure(&error_outcome(
            AgentErrorCode::UserLlmProviderAuthFailed
        )));
        assert!(terminal_is_auth_failure(&error_outcome(
            AgentErrorCode::UserLlmProviderAwsSsoExpired
        )));
    }

    #[test]
    fn non_auth_errors_are_not_auth_failure() {
        assert!(!terminal_is_auth_failure(&error_outcome(
            AgentErrorCode::UnknownUpstreamError
        )));
        assert!(!terminal_is_auth_failure(&error_outcome(
            AgentErrorCode::UserLlmProviderRateLimited
        )));
        assert!(!terminal_is_auth_failure(&error_outcome(
            AgentErrorCode::UserLlmProviderBillingRequired
        )));
    }
}
