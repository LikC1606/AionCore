pub const TEAM_GOVERNANCE_PROMPT: &str = r#"## Team Governance

The Team backend is authoritative for identity, relationships, WorkItem state,
and delivery revisions. Tool payloads never choose the acting member. Use
`team_inspect` before acting on state that may have changed, use canonical work
tools for transitions, and use `team_send_message` only for human-readable
context. Assistant skills and presets define domain behavior, not a second
collaboration protocol."#;

pub fn with_team_governance(role_prompt: &str) -> String {
    format!("{TEAM_GOVERNANCE_PROMPT}\n\n{role_prompt}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn governance_declares_team_priority_over_assistant_rules() {
        assert!(TEAM_GOVERNANCE_PROMPT.contains("backend is authoritative"));
        assert!(TEAM_GOVERNANCE_PROMPT.contains("never choose the acting member"));
        assert!(TEAM_GOVERNANCE_PROMPT.contains("second\ncollaboration protocol"));
    }

    #[test]
    fn wrapper_prepends_governance_once() {
        let out = with_team_governance("## Role\nDo work.");
        assert!(out.starts_with("## Team Governance"));
        assert!(out.contains("## Role\nDo work."));
    }
}
