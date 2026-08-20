use crate::governance::with_team_governance;
use crate::team_tool_usage::build_team_tool_usage;
use aionui_api_types::TeamToolTransport;
use serde::Serialize;
use std::collections::HashMap;

pub const LEAD_PROMPT_TEMPLATE: &str = r#"# Team Role

Identity: {{AGENT_NAME}} (`{{AGENT_SLOT_ID}}`)
Team: {{TEAM_NAME}}
Role: lead

You own decomposition, delegation, review, and the final answer. Use the
canonical WorkItem state as the collaboration source of truth; mailbox
messages carry context, questions, and result summaries.${workspaceSection}

## Allowed Actions
- Inspect current work and revisions with `team_inspect`.
- Delegate only to a direct subordinate with `team_delegate`.
- Review submitted work with `team_review`. Include concise `feedback` when
  requesting changes; it is committed with the assignee notification. After
  `team_cancel`, the runtime notifies the assignee to stop; cancelled submissions
  cannot enter review.
- After accepting a Git delivery, inspect its exact revisions and call
  `team_integrate`. Never change the integration target with raw `git merge`,
  `git cherry-pick`, `git rebase`, `git reset`, `git update-ref`, or forced
  branch movement; only `team_integrate` may do so.
- Use `team_send_message` only for optional follow-up context or attachments.
- Use roster tools only when the user asks to change the team. Get explicit
  approval before spawning a teammate unless the user already gave that instruction.

## Operating Loop
1. Inspect before acting on work that may have changed.
2. Delegate bounded WorkItems with a clear delivery requirement.
3. The runtime durably queues the assignee's WorkItem notification. For Git work,
   that notification includes the exact `prepared_workspace` from `team_delegate`.
4. Send only optional attachments or extra context with `team_send_message`, then
   wait by ending the turn.
5. Review evidence, resolve follow-up work, and synthesize the result.

{{TEAM_TOOL_USAGE}}"#;

const PLACEHOLDER_WORKSPACE_SECTION: &str = "${workspaceSection}";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamPromptRole {
    Lead,
    Teammate,
}

impl std::fmt::Display for TeamPromptRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TeamPromptRole::Lead => f.write_str("lead"),
            TeamPromptRole::Teammate => f.write_str("teammate"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TeamPromptAgent {
    pub slot_id: String,
    pub name: String,
    pub role: TeamPromptRole,
    pub backend: String,
    pub model: String,
    pub status: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AvailableAgentType {
    pub agent_type: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct AvailableAssistant {
    pub assistant_id: String,
    pub name: String,
    pub backend: String,
    pub description: String,
    pub skills: Vec<String>,
}

pub struct LeadPromptParams<'a> {
    pub agent: &'a TeamPromptAgent,
    pub team_name: &'a str,
    pub teammates: &'a [TeamPromptAgent],
    pub available_agent_types: &'a [AvailableAgentType],
    pub available_assistants: &'a [AvailableAssistant],
    pub renamed_agents: &'a HashMap<String, String>,
    pub team_workspace: Option<&'a str>,
    pub tool_transport: TeamToolTransport,
}

pub struct TeammatePromptParams<'a> {
    pub agent: &'a TeamPromptAgent,
    pub team_name: &'a str,
    pub leader: &'a TeamPromptAgent,
    pub teammates: &'a [TeamPromptAgent],
    pub renamed_agents: &'a HashMap<String, String>,
    pub team_workspace: Option<&'a str>,
    pub tool_transport: TeamToolTransport,
}

pub fn build_lead_prompt(params: &LeadPromptParams<'_>) -> String {
    let role_prompt = build_lead_role_prompt(params);
    with_team_governance(&role_prompt)
}

pub fn build_teammate_prompt(params: &TeammatePromptParams<'_>) -> String {
    let role_prompt = build_teammate_role_prompt(params);
    with_team_governance(&role_prompt)
}

fn build_lead_role_prompt(params: &LeadPromptParams<'_>) -> String {
    let _ = (
        params.teammates,
        params.available_agent_types,
        params.available_assistants,
        params.renamed_agents,
    );
    let workspace_section = render_workspace_section(params.team_workspace);

    LEAD_PROMPT_TEMPLATE
        .replace("{{AGENT_NAME}}", &params.agent.name)
        .replace("{{AGENT_SLOT_ID}}", &params.agent.slot_id)
        .replace("{{TEAM_NAME}}", params.team_name)
        .replace(
            "{{TEAM_TOOL_USAGE}}",
            &build_team_tool_usage(TeamPromptRole::Lead, params.tool_transport),
        )
        .replace(PLACEHOLDER_WORKSPACE_SECTION, &workspace_section)
}

fn render_workspace_section(team_workspace: Option<&str>) -> String {
    match team_workspace {
        Some(workspace) => format!(
            "\n\n## Team Workspace\n`{workspace}` is the shared integration target and the workspace for non-Git work.\n\
             Git Workers modify only their prepared worktrees; never ask them to edit this target directly."
        ),
        None => String::new(),
    }
}

const TEAMMATE_PROMPT_TEMPLATE: &str = r#"# Team Role

Identity: {{AGENT_NAME}} (`{{AGENT_SLOT_ID}}`)
Team: {{TEAM_NAME}}
Role: teammate
Direct superior: {{LEADER_NAME}} (`{{LEADER_SLOT_ID}}`){{WORKSPACE}}

Your responsibility is the canonical WorkItems assigned to you. Do not infer
work state from chat history. Mailbox messages provide context; WorkItem state
and revisions decide what can happen next.

## Allowed Actions
- Inspect your current responsibilities with `team_inspect`.
- Use `team_progress` to start, block, or resume assigned work.
- Commit a blocker with `team_progress` and include concise `context`; the state,
  context, and controller notification commit together.
- When a result is ready, call `team_submit` with concise `evidence`; the state,
  evidence, and reviewer notification commit together. For Git delivery, submit
  the content revision and immutable head commit; assignment coordinates are
  server-owned.
- For Git work, operate only in the `prepared_workspace` sent by your superior.
  Verify its branch and base before editing; never edit the integration target.
- Never run `git merge`, `git cherry-pick`, `git rebase`, `git reset`,
  `git update-ref`, or force-move the integration target. Only call
  `team_integrate` when `team_inspect` lists `integrate` as an allowed action.
- Do not review, cancel, or reassign work unless `team_inspect` explicitly lists
  that action for you.

When no action is available, end the turn. The mailbox will wake you when work
changes. For a shutdown request, answer the leader once with
`shutdown_approved` or `shutdown_rejected: <reason>`.

{{TEAM_TOOL_USAGE}}"#;

fn build_teammate_role_prompt(params: &TeammatePromptParams<'_>) -> String {
    let _ = (params.teammates, params.renamed_agents);

    let workspace_section = match params.team_workspace {
        Some(workspace) => format!(
            "\n\n## Workspaces\n\
- **Integration target**: `{workspace}` — shared context and non-Git work only.\n\
- **Git worktree**: use the exact `prepared_workspace` supplied with your WorkItem.\n\n\
Never modify the integration target for a Git WorkItem."
        ),
        None => String::new(),
    };

    TEAMMATE_PROMPT_TEMPLATE
        .replace("{{AGENT_NAME}}", &params.agent.name)
        .replace("{{AGENT_SLOT_ID}}", &params.agent.slot_id)
        .replace("{{TEAM_NAME}}", params.team_name)
        .replace("{{LEADER_NAME}}", &params.leader.name)
        .replace("{{LEADER_SLOT_ID}}", &params.leader.slot_id)
        .replace(
            "{{TEAM_TOOL_USAGE}}",
            &build_team_tool_usage(TeamPromptRole::Teammate, params.tool_transport),
        )
        .replace("{{WORKSPACE}}", &workspace_section)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prompt_agent(slot_id: &str, name: &str, role: TeamPromptRole) -> TeamPromptAgent {
        TeamPromptAgent {
            slot_id: slot_id.to_owned(),
            name: name.to_owned(),
            role,
            backend: "claude".to_owned(),
            model: "sonnet".to_owned(),
            status: None,
        }
    }

    #[test]
    fn lead_prompt_prepends_governance_and_fills_sections() {
        let renamed = HashMap::new();
        let leader = prompt_agent("lead-1", "Lead", TeamPromptRole::Lead);
        let teammate = prompt_agent("worker-1", "Worker", TeamPromptRole::Teammate);
        let assistants = vec![AvailableAssistant {
            assistant_id: "word-creator".to_owned(),
            name: "Word Creator".to_owned(),
            backend: "claude".to_owned(),
            description: "Drafts documents".to_owned(),
            skills: vec!["docx".to_owned()],
        }];
        let prompt = build_lead_prompt(&LeadPromptParams {
            agent: &leader,
            team_name: "Alpha",
            teammates: &[teammate],
            available_agent_types: &[],
            available_assistants: &assistants,
            renamed_agents: &renamed,
            team_workspace: None,
            tool_transport: TeamToolTransport::Mcp,
        });

        assert!(prompt.starts_with("## Team Governance"));
        assert!(prompt.contains("Identity: Lead (`lead-1`)"));
        assert!(prompt.contains("Role: lead"));
        assert!(!prompt.contains("## Your Teammates"));
        assert!(!prompt.contains("## Available Assistants for Spawning"));
        assert!(!prompt.contains("- Worker (claude, status: unknown)"));
        assert!(prompt.contains("team_inspect"));
        assert!(prompt.contains("team_delegate"));
        assert!(prompt.contains("team_integrate"));
        assert!(prompt.contains("git merge"));
        assert!(prompt.contains("canonical WorkItem state"));
        assert!(!prompt.contains("team_task_"));
        assert!(prompt.len() < 6_000);
        assert!(!prompt.contains("${"));
    }

    #[test]
    fn teammate_prompt_contains_canonical_coordination_rules() {
        let leader = prompt_agent("lead-1", "Lead", TeamPromptRole::Lead);
        let worker = prompt_agent("worker-1", "Worker", TeamPromptRole::Teammate);
        let prompt = build_teammate_prompt(&TeammatePromptParams {
            agent: &worker,
            team_name: "Alpha",
            leader: &leader,
            teammates: &[],
            renamed_agents: &HashMap::new(),
            team_workspace: None,
            tool_transport: TeamToolTransport::Mcp,
        });

        assert!(prompt.contains("## Team Governance"));
        assert!(prompt.contains("Identity: Worker (`worker-1`)"));
        assert!(prompt.contains("Role: teammate"));
        assert!(!prompt.contains("Role: general-purpose AI assistant"));
        assert!(prompt.contains("Direct superior: Lead (`lead-1`)"));
        assert!(prompt.contains("team_progress"));
        assert!(prompt.contains("team_submit"));
        assert!(prompt.contains("team_integrate"));
        assert!(prompt.contains("git cherry-pick"));
        assert!(prompt.contains("end the turn"));
        assert!(!prompt.contains("team_task_"));
        assert!(prompt.len() < 5_000);
        assert!(!prompt.contains("Teammates: Worker"));
    }
}
