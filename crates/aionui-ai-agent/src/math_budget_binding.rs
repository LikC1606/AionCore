//! Ephemeral host supplied budget binding for a conversation.

#[derive(Clone)]
pub struct MathBudgetBinding {
    pub run_id: String,
    pub input_digest: String,
    pub actor_id: String,
    pub socket_path: String,
    pub secret: String,
    pub output_tokens: u32,
    pub provider: String,
    pub model: String,
}

impl MathBudgetBinding {
    pub fn validate(&self) -> Result<(), String> {
        if !valid_id(&self.run_id) || !valid_id(&self.actor_id) || !lower_hex_digest(&self.input_digest) {
            return Err("math_budget_binding_identity_missing".into());
        }
        if !std::path::Path::new(&self.socket_path).is_absolute()
            || self.socket_path.len() > 103
            || self.socket_path.contains('\0')
            || !lower_hex_digest(&self.secret)
        {
            return Err("math_budget_binding_transport_invalid".into());
        }
        if self.output_tokens == 0 || self.output_tokens > 64_000 {
            return Err("math_budget_binding_output_cap_invalid".into());
        }
        if self.provider != "WestlakeHPC" || self.model != "deepseek-flash" {
            return Err("math_budget_binding_provider_model_invalid".into());
        }
        Ok(())
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
}

fn lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
