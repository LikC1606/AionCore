use std::sync::Arc;

use aionui_ai_agent::IWorkerTaskManager;
use aionui_api_types::{AssistantConversationRequest, CreateConversationRequest, GetConfigOptionsResponse};
use aionui_conversation::{
    ConversationAgentTurnFailureKind, ConversationAgentTurnOutcome, ConversationAgentTurnRequest,
    ConversationAgentTurnStarted, ConversationAgentTurnStatus, ConversationError, ConversationService,
};
use aionui_db::models::MessageRow;
use aionui_db::{ConversationFilters, IConversationRepository};
use aionui_team::{
    AgentTurnCancellationPort, AgentTurnExecutionError, AgentTurnExecutionPort, AgentTurnOutcome, AgentTurnRequest,
    AgentTurnStarted, AgentTurnStatus, TeamConversationBindingLookup, TeamConversationCleanupCandidate,
    TeamConversationCreateRequest, TeamConversationCreateResult, TeamConversationLookupPort,
    TeamConversationProvisioningPort, TeamError, TeamProjectionMessageStore,
};
use async_trait::async_trait;
use tracing::info;

pub struct TeamConversationAdapters {
    conversation_service: ConversationService,
    conversation_repo: Arc<dyn IConversationRepository>,
    task_manager: Arc<dyn IWorkerTaskManager>,
}

impl TeamConversationAdapters {
    pub fn new(
        conversation_service: ConversationService,
        conversation_repo: Arc<dyn IConversationRepository>,
        task_manager: Arc<dyn IWorkerTaskManager>,
    ) -> Self {
        Self {
            conversation_service,
            conversation_repo,
            task_manager,
        }
    }
}

#[async_trait]
impl AgentTurnExecutionPort for TeamConversationAdapters {
    async fn run_agent_turn(&self, request: AgentTurnRequest) -> Result<AgentTurnOutcome, AgentTurnExecutionError> {
        let team_started = request.on_started.clone();
        let team_run_id = request.team_run_id.clone();
        let slot_id = request.slot_id.clone();
        let role = request.role.clone();
        let on_started = team_started.map(|callback| {
            Arc::new(move |started: ConversationAgentTurnStarted| {
                let callback = callback.clone();
                let team_run_id = team_run_id.clone();
                let slot_id = slot_id.clone();
                let role = role.clone();
                Box::pin(async move {
                    callback(AgentTurnStarted {
                        team_run_id,
                        slot_id,
                        role,
                        conversation_id: started.conversation_id,
                        turn_id: started.turn_id,
                    })
                    .await;
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
            })
                as Arc<
                    dyn Fn(
                            ConversationAgentTurnStarted,
                        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>
                        + Send
                        + Sync,
                >
        });

        let conversation_id = request.conversation_id.clone();
        let outcome = loop {
            match self
                .conversation_service
                .run_agent_turn(ConversationAgentTurnRequest {
                    user_id: request.user_id.clone(),
                    conversation_id: conversation_id.clone(),
                    content: request.content.clone(),
                    files: request.files.clone(),
                    inject_skills: Vec::new(),
                    required_runtime_mode: None,
                    persist_user_message: false,
                    user_message_hidden: false,
                    on_started: on_started.clone(),
                })
                .await
            {
                Ok(outcome) => break outcome,
                Err(error) if is_retryable_conversation_busy(&error) => {
                    info!(
                        conversation_id = %conversation_id,
                        team_run_id = ?request.team_run_id,
                        slot_id = %request.slot_id,
                        "team conversation turn waiting for active conversation turn to release"
                    );
                    self.conversation_service
                        .runtime_state()
                        .wait_until_unclaimed(&conversation_id)
                        .await;
                    info!(
                        conversation_id = %conversation_id,
                        team_run_id = ?request.team_run_id,
                        slot_id = %request.slot_id,
                        "team conversation turn retrying after active conversation turn released"
                    );
                }
                Err(error) => return Err(map_conversation_turn_error(error)),
            }
        };

        map_conversation_turn_outcome(outcome)
    }
}

#[async_trait]
impl AgentTurnCancellationPort for TeamConversationAdapters {
    async fn cancel_agent_turn(
        &self,
        user_id: &str,
        conversation_id: &str,
        turn_id: &str,
    ) -> Result<(), AgentTurnExecutionError> {
        self.conversation_service
            .cancel(user_id, conversation_id, turn_id, &self.task_manager)
            .await
            .map(|_| ())
            .map_err(map_conversation_turn_error)
    }
}

#[async_trait]
impl TeamProjectionMessageStore for TeamConversationAdapters {
    fn mint_message_id(&self) -> String {
        ConversationService::mint_msg_id()
    }

    async fn find_projected_message(
        &self,
        conversation_id: &str,
        msg_id: &str,
        msg_type: &str,
    ) -> Result<Option<MessageRow>, TeamError> {
        Ok(self
            .conversation_repo
            .get_message_by_msg_id(conversation_id, msg_id, msg_type)
            .await?)
    }

    async fn insert_projected_message(&self, row: &MessageRow) -> Result<(), TeamError> {
        self.conversation_service
            .insert_raw_message(row)
            .await
            .map_err(map_conversation_update_error)
    }
}

#[async_trait]
impl TeamConversationProvisioningPort for TeamConversationAdapters {
    async fn create_team_conversation(
        &self,
        request: TeamConversationCreateRequest,
    ) -> Result<TeamConversationCreateResult, TeamError> {
        let response = self
            .conversation_service
            .create(
                &request.user_id,
                CreateConversationRequest {
                    r#type: request.agent_type,
                    name: Some(request.name),
                    model: request.top_level_model,
                    assistant: request.assistant_id.map(|assistant_id| AssistantConversationRequest {
                        id: assistant_id,
                        locale: None,
                        conversation_overrides: None,
                    }),
                    source: None,
                    channel_chat_id: None,
                    extra: request.extra,
                },
            )
            .await
            .map_err(map_conversation_create_error)?;
        let workspace = response
            .extra
            .get("workspace")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| TeamError::InvalidRequest("created team conversation did not resolve a workspace".into()))?
            .to_owned();
        Ok(TeamConversationCreateResult {
            conversation_id: response.id,
            workspace,
        })
    }

    async fn conversation_workspace(&self, conversation_id: &str) -> Result<Option<String>, TeamError> {
        let Some(row) = self.conversation_repo.get(conversation_id).await? else {
            return Ok(None);
        };
        let extra: serde_json::Value = serde_json::from_str(&row.extra).unwrap_or(serde_json::Value::Null);
        Ok(extra
            .get("workspace")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned))
    }

    async fn conversation_assistant_id(&self, conversation_id: &str) -> Result<Option<String>, TeamError> {
        if let Some(snapshot) = self.conversation_repo.get_assistant_snapshot(conversation_id).await? {
            let assistant_id = snapshot.assistant_id.trim();
            if !assistant_id.is_empty() {
                return Ok(Some(assistant_id.to_owned()));
            }
        }

        let Some(row) = self.conversation_repo.get(conversation_id).await? else {
            return Ok(None);
        };

        let extra: serde_json::Value = serde_json::from_str(&row.extra).unwrap_or(serde_json::Value::Null);
        Ok(extra
            .get("assistant_id")
            .or_else(|| extra.get("preset_assistant_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned))
    }

    async fn create_team_temp_workspace(&self, team_id: &str) -> Result<String, TeamError> {
        self.conversation_service
            .create_team_temp_workspace(team_id)
            .map_err(map_conversation_update_error)
    }

    async fn patch_runtime_config(&self, conversation_id: &str, patch: serde_json::Value) -> Result<(), TeamError> {
        self.conversation_service
            .update_extra(conversation_id, patch)
            .await
            .map_err(map_conversation_update_error)
    }

    async fn save_acp_runtime_mode(&self, conversation_id: &str, mode: &str) -> Result<(), TeamError> {
        self.conversation_service
            .save_acp_runtime_mode(conversation_id, mode)
            .await
            .map_err(map_conversation_update_error)
    }

    async fn get_config_options(&self, conversation_id: &str) -> Result<GetConfigOptionsResponse, TeamError> {
        self.conversation_service
            .get_config_options(conversation_id)
            .await
            .map_err(map_conversation_update_error)
    }

    async fn warmup_agent_process(
        &self,
        user_id: &str,
        conversation_id: &str,
        task_manager: &Arc<dyn IWorkerTaskManager>,
    ) -> Result<(), TeamError> {
        self.conversation_service
            .warmup(user_id, conversation_id, task_manager)
            .await
            .map_err(map_conversation_update_error)
    }

    async fn delete_team_conversation(&self, user_id: &str, conversation_id: &str) -> Result<(), TeamError> {
        self.conversation_service
            .delete(user_id, conversation_id)
            .await
            .map_err(map_conversation_update_error)
    }

    async fn list_team_conversation_ids(&self, user_id: &str, team_id: &str) -> Result<Vec<String>, TeamError> {
        let mut cursor = None;
        let mut conversation_ids = Vec::new();
        loop {
            let page = self
                .conversation_repo
                .list_paginated(
                    user_id,
                    &ConversationFilters {
                        cursor: cursor.clone(),
                        limit: 100,
                        ..Default::default()
                    },
                )
                .await?;
            for row in &page.items {
                if aionui_api_types::TeamSessionBinding::team_id_marker_from_extra_str(&row.extra).as_deref()
                    == Some(team_id)
                {
                    conversation_ids.push(row.id.clone());
                }
            }
            if !page.has_more {
                break;
            }
            cursor = page.items.last().map(|row| row.id.clone());
            if cursor.is_none() {
                break;
            }
        }
        Ok(conversation_ids)
    }

    async fn list_team_conversation_cleanup_candidates(
        &self,
    ) -> Result<Vec<TeamConversationCleanupCandidate>, TeamError> {
        Ok(self
            .conversation_repo
            .list_team_bound()
            .await?
            .into_iter()
            .filter_map(|row| {
                let team_id = aionui_api_types::TeamSessionBinding::team_id_marker_from_extra_str(&row.extra)?;
                Some(TeamConversationCleanupCandidate {
                    conversation_id: row.id,
                    user_id: row.user_id,
                    team_id,
                    created_at: row.created_at,
                })
            })
            .collect())
    }

    async fn lookup_team_binding_by_conversation(
        &self,
        conversation_id: &str,
    ) -> Result<Option<TeamConversationBindingLookup>, TeamError> {
        let Some(row) = self.conversation_repo.get(conversation_id).await? else {
            return Ok(None);
        };
        let extra: serde_json::Value = serde_json::from_str(&row.extra).unwrap_or(serde_json::Value::Null);
        Ok(Some(TeamConversationBindingLookup {
            conversation_id: row.id,
            user_id: row.user_id,
            team_id: extra
                .get("teamId")
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
            slot_id: extra
                .get("slot_id")
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
            role: extra
                .get("role")
                .and_then(serde_json::Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_owned),
        }))
    }
}

#[async_trait]
impl TeamConversationLookupPort for TeamConversationAdapters {
    async fn lookup_team_binding_by_conversation(
        &self,
        conversation_id: &str,
    ) -> Result<Option<TeamConversationBindingLookup>, TeamError> {
        <Self as TeamConversationProvisioningPort>::lookup_team_binding_by_conversation(self, conversation_id).await
    }
}

fn is_retryable_conversation_busy(error: &ConversationError) -> bool {
    matches!(error, ConversationError::Busy { reason } if reason.contains("already running"))
}

fn map_conversation_turn_outcome(
    outcome: ConversationAgentTurnOutcome,
) -> Result<AgentTurnOutcome, AgentTurnExecutionError> {
    match outcome.failure_kind {
        Some(ConversationAgentTurnFailureKind::RunInputsIntegrity) => Err(AgentTurnExecutionError::Rejected {
            reason: outcome
                .error_message
                .unwrap_or_else(|| "math_run_inputs_integrity_failed".into()),
        }),
        Some(ConversationAgentTurnFailureKind::TransportDisconnect) => Err(AgentTurnExecutionError::Transport {
            reason: outcome
                .error_message
                .unwrap_or_else(|| "model stream disconnected before completion".into()),
        }),
        None => Ok(AgentTurnOutcome {
            conversation_id: outcome.conversation_id,
            turn_id: outcome.turn_id,
            status: match outcome.status {
                ConversationAgentTurnStatus::Completed => AgentTurnStatus::Completed,
                ConversationAgentTurnStatus::Failed => AgentTurnStatus::Failed,
            },
            runtime: Some(outcome.runtime),
        }),
    }
}

fn map_conversation_create_error(error: ConversationError) -> TeamError {
    match error {
        ConversationError::WorkspacePathUnavailable { path } => TeamError::WorkspacePathUnavailable(path),
        ConversationError::WorkspacePathRuntimeUnavailable { path } => TeamError::WorkspacePathRuntimeUnavailable(path),
        other => TeamError::InvalidRequest(format!("failed to create conversation: {other}")),
    }
}

fn map_conversation_update_error(error: ConversationError) -> TeamError {
    match error {
        ConversationError::WorkspacePathUnavailable { path } => TeamError::WorkspacePathUnavailable(path),
        ConversationError::WorkspacePathRuntimeUnavailable { path } => TeamError::WorkspacePathRuntimeUnavailable(path),
        ConversationError::Forbidden { reason } => TeamError::Forbidden(reason),
        ConversationError::ActiveAgentNotFound { conversation_id } => TeamError::RuntimeNotReady { conversation_id },
        ConversationError::NotFound { id } => TeamError::InvalidRequest(format!("conversation not found: {id}")),
        ConversationError::NotFoundReason { reason } => TeamError::InvalidRequest(reason),
        other => TeamError::InvalidRequest(other.to_string()),
    }
}

fn map_conversation_turn_error(error: ConversationError) -> AgentTurnExecutionError {
    match error {
        ConversationError::Busy { reason } => AgentTurnExecutionError::Skipped { reason },
        other => AgentTurnExecutionError::Failed {
            reason: other.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn_outcome(kind: Option<ConversationAgentTurnFailureKind>) -> ConversationAgentTurnOutcome {
        ConversationAgentTurnOutcome {
            conversation_id: "conversation-test".into(),
            turn_id: "turn-test".into(),
            status: ConversationAgentTurnStatus::Failed,
            error_message: Some("static rejection".into()),
            failure_kind: kind,
            runtime: aionui_api_types::ConversationRuntimeSummary {
                state: aionui_api_types::ConversationRuntimeStateKind::Idle,
                can_send_message: true,
                has_task: false,
                task_status: None,
                is_processing: false,
                pending_confirmations: 0,
                turn_id: None,
            },
        }
    }

    #[test]
    fn run_input_integrity_is_terminal_not_transport_or_background_start_retry() {
        assert!(
            matches!(map_conversation_turn_outcome(turn_outcome(Some(ConversationAgentTurnFailureKind::RunInputsIntegrity))),
            Err(AgentTurnExecutionError::Rejected { reason }) if reason == "static rejection")
        );
        assert!(
            matches!(map_conversation_turn_outcome(turn_outcome(Some(ConversationAgentTurnFailureKind::TransportDisconnect))),
            Err(AgentTurnExecutionError::Transport { reason }) if reason == "static rejection")
        );
        assert_eq!(
            map_conversation_turn_outcome(turn_outcome(None)).unwrap().status,
            AgentTurnStatus::Failed
        );
    }

    #[test]
    fn active_agent_missing_maps_to_team_runtime_not_ready() {
        let err = map_conversation_update_error(ConversationError::ActiveAgentNotFound {
            conversation_id: "conv-1".into(),
        });

        assert!(matches!(
            err,
            TeamError::RuntimeNotReady {
                conversation_id: ref id
            } if id == "conv-1"
        ));
    }
}
