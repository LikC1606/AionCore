use aionui_api_types::{TeamToolDescriptor, TeamToolRole, TeamToolTransport};

use crate::role_prompt::TeamPromptRole;

pub fn build_team_tool_usage(role: TeamPromptRole, transport: TeamToolTransport) -> String {
    let tool_role = match role {
        TeamPromptRole::Lead => TeamToolRole::Lead,
        TeamPromptRole::Teammate => TeamToolRole::Teammate,
    };
    let descriptors = aionui_api_types::team_tool_descriptors_for_role(tool_role);
    match transport {
        TeamToolTransport::Mcp => render_mcp_usage(role, &descriptors),
        TeamToolTransport::CliAssumed => render_cli_usage(role, &descriptors),
    }
}

fn render_mcp_usage(role: TeamPromptRole, descriptors: &[TeamToolDescriptor]) -> String {
    let names = descriptors
        .iter()
        .map(|tool| format!("`{}`", tool.name))
        .collect::<Vec<_>>()
        .join(", ");
    let mut text = format!(
        "## Team Tools\nUse the native Team MCP tools. Current tools: {names}.\n\
Use exact revisions returned by `team_inspect`; choose a stable idempotency key for each intended mutation.\n\
Agent identity and team scope come from the authenticated runtime, never from arguments.\n"
    );
    text.push_str(
        "MCP and Team CLI reach the same Team runtime. Do not switch transports for a semantic error; retry only errors explicitly marked retryable, reusing the same idempotency key.\n",
    );
    if role == TeamPromptRole::Teammate {
        text.push_str("Roster-management tools are unavailable unless listed above.\n");
    }
    text
}

fn render_cli_usage(role: TeamPromptRole, descriptors: &[TeamToolDescriptor]) -> String {
    let commands = descriptors
        .iter()
        .map(|tool| format!("`team {}`", tool.cli_command.join(" ")))
        .collect::<Vec<_>>()
        .join(", ");
    let mut text = format!(
        "## Team Tools\nUse `\"$AIONUI_HELPER_BIN\" team ...` with stdin JSON. Current commands: {commands}.\n\
Run `\"$AIONUI_HELPER_BIN\" team capabilities` only when an exact schema is needed.\n\
Use revisions from `team inspect`; identity and team scope come from the authenticated runtime.\n"
    );
    text.push_str(
        "MCP and Team CLI reach the same Team runtime. Do not switch transports for a semantic error; retry only errors explicitly marked retryable, reusing the same idempotency key.\n",
    );
    if role == TeamPromptRole::Teammate {
        text.push_str("Roster-management commands are unavailable unless listed above.\n");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_teammate_usage_excludes_lead_only_tools() {
        let usage = build_team_tool_usage(TeamPromptRole::Teammate, TeamToolTransport::CliAssumed);
        assert!(usage.contains("\"$AIONUI_HELPER_BIN\" team"));
        assert!(usage.contains("team send-message"));
        assert!(usage.contains("team inspect"));
        assert!(usage.contains("same Team runtime"));
        assert!(usage.contains("explicitly marked retryable"));
        assert!(!usage.contains("team spawn-agent"));
        assert!(usage.contains("Roster-management commands are unavailable"));
        assert!(!usage.contains("team task"));
    }

    #[test]
    fn mcp_lead_usage_includes_lead_tools() {
        let usage = build_team_tool_usage(TeamPromptRole::Lead, TeamToolTransport::Mcp);
        assert!(usage.contains("native Team MCP tools"));
        assert!(usage.contains("team_spawn_agent"));
        assert!(usage.contains("team_inspect"));
        assert!(usage.contains("idempotency key"));
        assert!(usage.contains("same Team runtime"));
        assert!(usage.contains("explicitly marked retryable"));
        assert!(!usage.contains("fallback"));
    }

    #[test]
    fn mcp_teammate_usage_includes_shared_fallback_guidance() {
        let usage = build_team_tool_usage(TeamPromptRole::Teammate, TeamToolTransport::Mcp);
        assert!(usage.contains("team_inspect"));
        assert!(usage.contains("team_progress"));
        assert!(!usage.contains("team_spawn_agent"));
        assert!(usage.contains("Roster-management tools are unavailable"));
    }
}
