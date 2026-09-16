use aionui_ai_agent::math_budget_binding::MathBudgetBinding;
use aionui_api_types::MathBudgetBindingRequest;
use subtle::ConstantTimeEq;

use super::{ConversationError, ConversationService};

impl ConversationService {
    /// Captured by AppServices at startup, independent of browser/local authentication.
    pub fn with_math_budget_host_secret(mut self, secret: Option<String>) -> Result<Self, ConversationError> {
        if secret.as_ref().is_some_and(|value| !valid_host_secret(value)) {
            return Err(ConversationError::bad_request("math_budget_host_secret_invalid"));
        }
        self.math_budget_host_secret = secret.map(Into::into);
        Ok(self)
    }

    pub fn authorize_math_budget_host(&self, supplied: Option<&str>) -> Result<(), ConversationError> {
        let authorized = self
            .math_budget_host_secret
            .as_deref()
            .zip(supplied)
            .is_some_and(|(expected, supplied)| {
                valid_host_secret(supplied) && bool::from(expected.as_bytes().ct_eq(supplied.as_bytes()))
            });
        if !authorized {
            return Err(ConversationError::Unauthorized {
                reason: "math_budget_host_unauthorized".into(),
            });
        }
        Ok(())
    }

    pub async fn register_math_budget_binding(
        &self,
        user_id: &str,
        conversation_id: &str,
        req: MathBudgetBindingRequest,
    ) -> Result<(), ConversationError> {
        let row = self
            .conversation_repo
            .get(conversation_id)
            .await?
            .filter(|row| row.user_id == user_id)
            .ok_or_else(|| ConversationError::NotFound {
                id: conversation_id.into(),
            })?;
        if row.r#type != "acp" {
            return Err(ConversationError::bad_request("math_budget_codex_required"));
        }
        self.task_manager
            .register_math_budget_binding(
                conversation_id,
                MathBudgetBinding {
                    run_id: req.run_id,
                    input_digest: req.input_digest,
                    actor_id: req.actor_id,
                    socket_path: req.socket_path,
                    secret: req.secret,
                    output_tokens: req.output_tokens,
                    provider: req.provider,
                    model: req.model,
                },
            )
            .map_err(ConversationError::from)?;
        tracing::info!(conversation_id, "Registered ephemeral mathematics budget binding");
        Ok(())
    }

    pub async fn revoke_math_budget_binding(
        &self,
        user_id: &str,
        conversation_id: &str,
    ) -> Result<(), ConversationError> {
        self.conversation_repo
            .get(conversation_id)
            .await?
            .filter(|row| row.user_id == user_id)
            .ok_or_else(|| ConversationError::NotFound {
                id: conversation_id.into(),
            })?;
        self.task_manager
            .revoke_math_budget_binding(conversation_id)
            .await
            .map_err(ConversationError::from)?;
        tracing::info!(conversation_id, "Revoked ephemeral mathematics budget binding");
        Ok(())
    }
}

fn valid_host_secret(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
