use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const TEAM_TOOLS_SCHEMA_VERSION: u32 = 1;

pub const TEAM_SPAWN_AGENT_DESCRIPTION: &str = r#"Create a new teammate agent to join the team.

Use this only when one of the following is true:
- The user explicitly approved the proposed teammate lineup in a previous message
- The user explicitly instructed you to create a specific teammate immediately

Before calling this tool in the normal planning flow:
- Start with one short sentence explaining why additional teammates would help
- Tell the user which teammate(s) you recommend
- Present the proposal as a table with: name, responsibility, and recommended assistant
- Include each teammate's responsibility and recommended assistant
- Ask whether to create them as proposed or change any names, responsibilities, or assistant choices
- In that approval question, remind the user that they can later ask you to replace or adjust any teammate if the lineup is not working well
- Do NOT call this tool in that same turn; wait for explicit approval in a later user message

When calling this tool, always provide assistant_id from the available assistants catalog.
Do not provide a model. The new teammate uses the selected assistant's configured/default model; users can adjust models from the UI model selector.

The new agent will be created and added to the team. You can then assign tasks and send messages to it."#;

pub const TEAM_DESCRIBE_ASSISTANT_DESCRIPTION: &str = "Get detailed information about an assistant before spawning it as a teammate.\n\n\
Returns the assistant's full description, enabled skills, and example tasks so you can\n\
judge whether it fits the user's request. Use this when two or more assistants look\n\
relevant from the one-line catalog in your system prompt.\n\n\
Use team_list_assistants to find candidate assistant_id values.\n\
After confirming a match, call team_spawn_agent with the same assistant_id.";

pub const TEAM_LIST_ASSISTANTS_DESCRIPTION: &str = "List the assistants available for team spawning. Returns the real assistant catalog with \
real assistant_id values, names, backends, descriptions, and skills.\n\nUse this before \
team_spawn_agent when you need the exact assistant_id for a teammate. Do NOT guess from backend \
names like claude/codex/gemini — only use assistant_id values returned here.";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamToolPermission {
    AnyTeamAgent,
    LeadOnly,
}

impl TeamToolPermission {
    pub fn is_lead_only(self) -> bool {
        self == Self::LeadOnly
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamToolRole {
    Lead,
    Teammate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamToolTransport {
    Mcp,
    CliAssumed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamToolName {
    TeamMembers,
    TeamSendMessage,
    TeamInspect,
    TeamDelegate,
    TeamProgress,
    TeamSubmit,
    TeamReview,
    TeamIntegrate,
    TeamCancel,
    TeamListAssistants,
    TeamDescribeAssistant,
    TeamSpawnAgent,
    TeamRenameAgent,
    TeamShutdownAgent,
}

impl TeamToolName {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TeamMembers => "team_members",
            Self::TeamSendMessage => "team_send_message",
            Self::TeamInspect => "team_inspect",
            Self::TeamDelegate => "team_delegate",
            Self::TeamProgress => "team_progress",
            Self::TeamSubmit => "team_submit",
            Self::TeamReview => "team_review",
            Self::TeamIntegrate => "team_integrate",
            Self::TeamCancel => "team_cancel",
            Self::TeamListAssistants => "team_list_assistants",
            Self::TeamDescribeAssistant => "team_describe_assistant",
            Self::TeamSpawnAgent => "team_spawn_agent",
            Self::TeamRenameAgent => "team_rename_agent",
            Self::TeamShutdownAgent => "team_shutdown_agent",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "team_members" => Self::TeamMembers,
            "team_send_message" => Self::TeamSendMessage,
            "team_inspect" => Self::TeamInspect,
            "team_delegate" => Self::TeamDelegate,
            "team_progress" => Self::TeamProgress,
            "team_submit" => Self::TeamSubmit,
            "team_review" => Self::TeamReview,
            "team_integrate" => Self::TeamIntegrate,
            "team_cancel" => Self::TeamCancel,
            "team_list_assistants" => Self::TeamListAssistants,
            "team_describe_assistant" => Self::TeamDescribeAssistant,
            "team_spawn_agent" => Self::TeamSpawnAgent,
            "team_rename_agent" => Self::TeamRenameAgent,
            "team_shutdown_agent" => Self::TeamShutdownAgent,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolDescriptor {
    pub name: String,
    pub permission: TeamToolPermission,
    pub description: String,
    pub input_schema: Value,
    pub cli_command: Vec<String>,
    pub when: String,
    pub input_summary: String,
}

impl TeamToolDescriptor {
    pub fn lead_only(&self) -> bool {
        self.permission.is_lead_only()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolCall {
    pub tool: TeamToolName,
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamToolErrorCode {
    UnknownTool,
    SchemaValidationFailed,
    PermissionDenied,
    TeamNotFound,
    WorkItemNotFound,
    ConversationNotFound,
    AgentNotFound,
    NotInTeam,
    TransportUnavailable,
    RuntimeContextMissing,
    RuntimeAuthFailed,
    RevisionConflict,
    BusinessRuleViolation,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TeamToolErrorPayload {
    pub code: TeamToolErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

impl TeamToolErrorPayload {
    pub fn new(code: TeamToolErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolCliMeta {
    pub schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolCliEnvelope<T> {
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<T>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<TeamToolErrorPayload>,
    pub meta: TeamToolCliMeta,
}

impl<T> TeamToolCliEnvelope<T> {
    pub fn success(data: T, command: Option<String>) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            meta: TeamToolCliMeta {
                schema_version: TEAM_TOOLS_SCHEMA_VERSION,
                command,
            },
        }
    }

    pub fn failure(error: TeamToolErrorPayload, command: Option<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(error),
            meta: TeamToolCliMeta {
                schema_version: TEAM_TOOLS_SCHEMA_VERSION,
                command,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolContextResponse {
    pub in_team: bool,
    pub conversation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub team_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<TeamToolRole>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<TeamToolTransport>,
    #[serde(default)]
    pub allowed_tools: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolRuntimeCallRequest {
    pub tool: TeamToolName,
    #[serde(default)]
    pub arguments: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamToolRuntimeCallResponse {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<TeamToolErrorPayload>,
}

pub fn team_tool_descriptors() -> Vec<TeamToolDescriptor> {
    tool_specs()
        .into_iter()
        .map(|spec| TeamToolDescriptor {
            name: spec.name.as_str().to_owned(),
            permission: spec.permission,
            description: spec.description.to_owned(),
            input_schema: spec.input_schema,
            cli_command: spec.cli_command.iter().map(|part| (*part).to_owned()).collect(),
            when: spec.when.to_owned(),
            input_summary: spec.input_summary.to_owned(),
        })
        .collect()
}

pub fn team_tool_descriptors_for_role(role: TeamToolRole) -> Vec<TeamToolDescriptor> {
    let is_lead = role == TeamToolRole::Lead;
    team_tool_descriptors()
        .into_iter()
        .filter(|descriptor| is_lead || !descriptor.permission.is_lead_only())
        .collect()
}

pub fn team_tool_descriptor(name: &str) -> Option<TeamToolDescriptor> {
    tool_specs()
        .into_iter()
        .find(|spec| spec.name.as_str() == name)
        .map(|spec| TeamToolDescriptor {
            name: spec.name.as_str().to_owned(),
            permission: spec.permission,
            description: spec.description.to_owned(),
            input_schema: spec.input_schema,
            cli_command: spec.cli_command.iter().map(|part| (*part).to_owned()).collect(),
            when: spec.when.to_owned(),
            input_summary: spec.input_summary.to_owned(),
        })
}

pub fn cli_command_for_tool(name: &str) -> Option<&'static [&'static str]> {
    tool_specs()
        .into_iter()
        .find(|spec| spec.name.as_str() == name)
        .map(|spec| spec.cli_command)
}

pub fn tool_name_for_cli_path(path: &[String]) -> Option<TeamToolName> {
    tool_specs()
        .into_iter()
        .find(|spec| spec.cli_command == path.iter().map(String::as_str).collect::<Vec<_>>())
        .map(|spec| spec.name)
}

#[derive(Debug, Clone)]
struct TeamToolSpec {
    name: TeamToolName,
    permission: TeamToolPermission,
    description: &'static str,
    input_schema: Value,
    cli_command: &'static [&'static str],
    when: &'static str,
    input_summary: &'static str,
}

fn tool_specs() -> Vec<TeamToolSpec> {
    vec![
        TeamToolSpec {
            name: TeamToolName::TeamMembers,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "List all team members with their roles and current status.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
            cli_command: &["members"],
            when: "Check roster/status",
            input_summary: "{}",
        },
        TeamToolSpec {
            name: TeamToolName::TeamSendMessage,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Send a message to a teammate, to the team leader (to=\"leader\"), or broadcast to all (to=\"*\"). When delegating work that depends on user attachments, forward their absolute paths in files.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "to": { "type": "string", "description": "Target agent slot_id, \"leader\" for the current team leader, or \"*\" for broadcast" },
                    "message": { "type": "string", "description": "Message content" },
                    "files": {
                        "type": "array",
                        "items": { "type": "string" },
                        "description": "Absolute attachment paths to forward to the target agent"
                    },
                    "idempotency_key": {
                        "type": "string",
                        "description": "Optional stable key for safely retrying the same message"
                    }
                },
                "required": ["to", "message"]
            }),
            cli_command: &["send-message"],
            when: "Send teammate message",
            input_summary: "to, message, optional idempotency_key",
        },
        TeamToolSpec {
            name: TeamToolName::TeamInspect,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Inspect canonical Team work visible to the authenticated member. Returns current revisions and server-computed allowed_actions. Omit work_item_id for the scoped list.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "work_item_id": { "type": "string", "description": "Optional exact WorkItem ID" }
                }
            }),
            cli_command: &["inspect"],
            when: "Inspect canonical work",
            input_summary: "optional work_item_id",
        },
        TeamToolSpec {
            name: TeamToolName::TeamDelegate,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Create and queue a canonical WorkItem for a direct subordinate. The runtime durably notifies and wakes the assignee after the command commits. Actor identity comes from the authenticated member credential, never from this payload.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "idempotency_key": { "type": "string" },
                    "parent_work_item_id": { "type": "string" },
                    "subject": { "type": "string" },
                    "description": { "type": "string" },
                    "assignee_member_id": { "type": "string" },
                    "delivery_requirement": { "type": "string", "enum": ["none", "git"] }
                },
                "required": ["idempotency_key", "subject", "assignee_member_id", "delivery_requirement"]
            }),
            cli_command: &["delegate"],
            when: "Delegate work",
            input_summary: "idempotency_key, subject, assignee_member_id, delivery_requirement",
        },
        TeamToolSpec {
            name: TeamToolName::TeamProgress,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Advance assigned canonical work with action start, block, or resume. Block requires concise context, which is committed atomically with the state change and controller notification. Start and resume must not include context. The authenticated member must be the WorkItem assignee.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "idempotency_key": { "type": "string" },
                    "work_item_id": { "type": "string" },
                    "expected_work_revision": { "type": "integer", "minimum": 0 },
                    "action": { "type": "string", "enum": ["start", "block", "resume"] },
                    "context": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 4000,
                        "description": "Required only for block; explains the blocker to the controller"
                    }
                },
                "required": ["idempotency_key", "work_item_id", "expected_work_revision", "action"],
                "allOf": [
                    {
                        "if": { "properties": { "action": { "const": "block" } }, "required": ["action"] },
                        "then": { "required": ["context"] }
                    },
                    {
                        "if": { "properties": { "action": { "enum": ["start", "resume"] } }, "required": ["action"] },
                        "then": { "not": { "required": ["context"] } }
                    }
                ]
            }),
            cli_command: &["progress"],
            when: "Start, block, or resume work",
            input_summary: "idempotency_key, work_item_id, expected_work_revision, action, block context",
        },
        TeamToolSpec {
            name: TeamToolName::TeamSubmit,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Commit a submission and concise evidence atomically with the reviewer notification. Use kind=inline without a result body, or kind=git with the next unused content revision and immutable head commit. Inspect existing deliveries after changes are requested; Git assignment is resolved from the WorkItem.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "idempotency_key": { "type": "string" },
                    "work_item_id": { "type": "string" },
                    "expected_work_revision": { "type": "integer", "minimum": 0 },
                    "kind": { "type": "string", "enum": ["inline", "git"] },
                    "evidence": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 8000,
                        "description": "Concise result, validation, and handoff evidence for the reviewer"
                    },
                    "git": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {
                            "content_revision": { "type": "integer", "minimum": 1 },
                            "head_commit": { "type": "string", "minLength": 1 }
                        },
                        "required": ["content_revision", "head_commit"]
                    }
                },
                "required": ["idempotency_key", "work_item_id", "expected_work_revision", "kind", "evidence"],
                "allOf": [
                    {
                        "if": { "properties": { "kind": { "const": "git" } }, "required": ["kind"] },
                        "then": { "required": ["git"] }
                    },
                    {
                        "if": { "properties": { "kind": { "const": "inline" } }, "required": ["kind"] },
                        "then": { "not": { "required": ["git"] } }
                    }
                ]
            }),
            cli_command: &["submit"],
            when: "Submit inline or Git work",
            input_summary: "idempotency_key, work_item_id, expected_work_revision, kind, evidence, optional git",
        },
        TeamToolSpec {
            name: TeamToolName::TeamReview,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Review a submitted canonical WorkItem and accept it, request changes, or reject it. Request changes requires concise feedback, which is committed atomically with the assignee notification. Git acceptance atomically queues the integrator notification. The authenticated member must be its reviewer.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "idempotency_key": { "type": "string" },
                    "work_item_id": { "type": "string" },
                    "expected_work_revision": { "type": "integer", "minimum": 0 },
                    "expected_delivery_revision": { "type": "integer", "minimum": 0 },
                    "decision": { "type": "string", "enum": ["accept", "request_changes", "reject"] },
                    "feedback": {
                        "type": "string",
                        "minLength": 1,
                        "maxLength": 4000,
                        "description": "Required only for request_changes"
                    }
                },
                "required": ["idempotency_key", "work_item_id", "expected_work_revision", "decision"],
                "allOf": [
                    {
                        "if": { "properties": { "decision": { "const": "request_changes" } }, "required": ["decision"] },
                        "then": { "required": ["feedback"] }
                    },
                    {
                        "if": { "properties": { "decision": { "enum": ["accept", "reject"] } }, "required": ["decision"] },
                        "then": { "not": { "required": ["feedback"] } }
                    }
                ]
            }),
            cli_command: &["review"],
            when: "Review submitted work",
            input_summary: "idempotency_key, work_item_id, revisions, decision, request_changes feedback",
        },
        TeamToolSpec {
            name: TeamToolName::TeamIntegrate,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Integrate the exact accepted Git delivery for a canonical WorkItem. The authenticated member must be its bound integrator. This is the only supported way to change the integration target: never run raw git merge, cherry-pick, rebase, reset, update-ref, or force-move the target branch. Repository coordinates and merged evidence are derived and verified by the server.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "idempotency_key": { "type": "string" },
                    "work_item_id": { "type": "string" },
                    "expected_work_revision": { "type": "integer", "minimum": 0 },
                    "expected_delivery_revision": { "type": "integer", "minimum": 0 }
                },
                "required": [
                    "idempotency_key",
                    "work_item_id",
                    "expected_work_revision",
                    "expected_delivery_revision"
                ]
            }),
            cli_command: &["integrate"],
            when: "Integrate accepted Git delivery",
            input_summary: "idempotency_key, work_item_id, expected_work_revision, expected_delivery_revision",
        },
        TeamToolSpec {
            name: TeamToolName::TeamCancel,
            permission: TeamToolPermission::AnyTeamAgent,
            description: "Cancel a canonical WorkItem controlled by the authenticated member.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "idempotency_key": { "type": "string" },
                    "work_item_id": { "type": "string" },
                    "expected_work_revision": { "type": "integer", "minimum": 0 },
                    "expected_delivery_revision": { "type": "integer", "minimum": 0 }
                },
                "required": ["idempotency_key", "work_item_id", "expected_work_revision"]
            }),
            cli_command: &["cancel"],
            when: "Cancel controlled work",
            input_summary: "idempotency_key, work_item_id, revisions",
        },
        TeamToolSpec {
            name: TeamToolName::TeamListAssistants,
            permission: TeamToolPermission::AnyTeamAgent,
            description: TEAM_LIST_ASSISTANTS_DESCRIPTION,
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {}
            }),
            cli_command: &["list-assistants"],
            when: "List spawn assistants",
            input_summary: "{}",
        },
        TeamToolSpec {
            name: TeamToolName::TeamDescribeAssistant,
            permission: TeamToolPermission::AnyTeamAgent,
            description: TEAM_DESCRIBE_ASSISTANT_DESCRIPTION,
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "assistant_id": { "type": "string", "description": "The assistant ID from the available assistants catalog (e.g., \"word-creator\")." },
                    "locale": { "type": "string", "description": "Locale like \"zh-CN\" or \"en-US\". Defaults to the user's current UI language when omitted." }
                },
                "required": ["assistant_id"]
            }),
            cli_command: &["describe-assistant"],
            when: "Inspect assistant",
            input_summary: "assistant_id, optional locale",
        },
        TeamToolSpec {
            name: TeamToolName::TeamSpawnAgent,
            permission: TeamToolPermission::LeadOnly,
            description: TEAM_SPAWN_AGENT_DESCRIPTION,
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "name": { "type": "string", "description": "Agent display name" },
                    "assistant_id": { "type": "string", "description": "Assistant ID to spawn. Call team_list_assistants when you need candidates; the runtime backend is derived from this assistant." }
                },
                "required": ["name", "assistant_id"]
            }),
            cli_command: &["spawn-agent"],
            when: "Spawn teammate",
            input_summary: "name, assistant_id",
        },
        TeamToolSpec {
            name: TeamToolName::TeamRenameAgent,
            permission: TeamToolPermission::LeadOnly,
            description: "Rename a team member. Lead only.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "slot_id": { "type": "string", "description": "Agent slot_id to rename" },
                    "new_name": { "type": "string", "description": "New display name" }
                },
                "required": ["slot_id", "new_name"]
            }),
            cli_command: &["rename-agent"],
            when: "Rename teammate",
            input_summary: "slot_id, new_name",
        },
        TeamToolSpec {
            name: TeamToolName::TeamShutdownAgent,
            permission: TeamToolPermission::LeadOnly,
            description: "Initiate shutdown of a teammate. Lead only. Sends a shutdown_request to the target agent.",
            input_schema: json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "slot_id": { "type": "string", "description": "Agent slot_id to shut down" },
                    "reason": { "type": "string", "description": "Reason for shutdown" }
                },
                "required": ["slot_id"]
            }),
            cli_command: &["shutdown-agent"],
            when: "Shut down teammate",
            input_summary: "slot_id, optional reason",
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn descriptor_count_and_names_are_unique() {
        let descriptors = team_tool_descriptors();
        assert_eq!(descriptors.len(), 14);
        let names = descriptors
            .iter()
            .map(|descriptor| descriptor.name.as_str())
            .collect::<HashSet<_>>();
        assert_eq!(names.len(), descriptors.len());
    }

    #[test]
    fn descriptors_have_required_prompt_and_schema_fields() {
        for descriptor in team_tool_descriptors() {
            assert!(!descriptor.name.is_empty());
            assert!(!descriptor.description.is_empty());
            assert!(!descriptor.when.is_empty());
            assert!(!descriptor.input_summary.is_empty());
            assert!(!descriptor.cli_command.is_empty());
            assert_eq!(descriptor.input_schema["type"], "object");
        }
    }

    #[test]
    fn cli_command_mapping_matches_spec() {
        let cases = [
            ("team_members", vec!["members"]),
            ("team_send_message", vec!["send-message"]),
            ("team_inspect", vec!["inspect"]),
            ("team_delegate", vec!["delegate"]),
            ("team_progress", vec!["progress"]),
            ("team_submit", vec!["submit"]),
            ("team_review", vec!["review"]),
            ("team_integrate", vec!["integrate"]),
            ("team_cancel", vec!["cancel"]),
            ("team_list_assistants", vec!["list-assistants"]),
            ("team_describe_assistant", vec!["describe-assistant"]),
            ("team_spawn_agent", vec!["spawn-agent"]),
            ("team_rename_agent", vec!["rename-agent"]),
            ("team_shutdown_agent", vec!["shutdown-agent"]),
        ];
        for (tool, path) in cases {
            assert_eq!(cli_command_for_tool(tool), Some(path.as_slice()));
            let owned = path.into_iter().map(str::to_owned).collect::<Vec<_>>();
            assert_eq!(tool_name_for_cli_path(&owned).map(TeamToolName::as_str), Some(tool));
        }
    }

    #[test]
    fn teammate_role_hides_lead_only_tools() {
        let names = team_tool_descriptors_for_role(TeamToolRole::Teammate)
            .into_iter()
            .map(|descriptor| descriptor.name)
            .collect::<Vec<_>>();
        assert!(!names.contains(&"team_spawn_agent".to_owned()));
        assert!(!names.contains(&"team_rename_agent".to_owned()));
        assert!(!names.contains(&"team_shutdown_agent".to_owned()));
        assert!(names.contains(&"team_send_message".to_owned()));
        assert!(names.contains(&"team_inspect".to_owned()));
        assert!(names.contains(&"team_progress".to_owned()));
    }

    #[test]
    fn spawn_schema_is_assistant_first_and_excludes_legacy_fields() {
        let descriptor = team_tool_descriptor("team_spawn_agent").expect("spawn descriptor");
        assert_eq!(descriptor.permission, TeamToolPermission::LeadOnly);
        let props = descriptor.input_schema["properties"].as_object().unwrap();
        assert!(props.contains_key("name"));
        assert!(props.contains_key("assistant_id"));
        assert!(!props.contains_key("model"));
        assert!(!props.contains_key("backend"));
        assert!(!props.contains_key("agent_type"));
        assert!(!props.contains_key("role"));
        let required = descriptor.input_schema["required"].as_array().unwrap();
        assert!(required.contains(&json!("name")));
        assert!(required.contains(&json!("assistant_id")));
    }

    #[test]
    fn send_message_schema_documents_stable_leader_alias() {
        let descriptor = team_tool_descriptor("team_send_message").expect("send descriptor");
        assert!(descriptor.description.contains("to=\"leader\""));
        assert!(
            descriptor.input_schema["properties"]["to"]["description"]
                .as_str()
                .expect("to description")
                .contains("\"leader\"")
        );
        assert_eq!(
            descriptor.input_schema["properties"]["idempotency_key"]["type"],
            "string"
        );
        let required = descriptor.input_schema["required"].as_array().unwrap();
        assert!(!required.contains(&json!("idempotency_key")));
    }

    #[test]
    fn canonical_work_schemas_never_accept_an_actor_identity() {
        for name in [
            "team_delegate",
            "team_progress",
            "team_submit",
            "team_review",
            "team_integrate",
            "team_cancel",
        ] {
            let descriptor = team_tool_descriptor(name).expect("canonical descriptor");
            let properties = descriptor.input_schema["properties"].as_object().unwrap();
            assert!(!properties.contains_key("actor_member_id"));
            assert!(!properties.contains_key("caller_slot_id"));
            assert!(!properties.contains_key("team_id"));
            assert_eq!(descriptor.input_schema["additionalProperties"], false);
        }
    }

    #[test]
    fn git_submit_schema_accepts_only_delivery_evidence() {
        let descriptor = team_tool_descriptor("team_submit").expect("submit descriptor");
        let required = descriptor.input_schema["required"].as_array().unwrap();
        assert!(required.contains(&json!("evidence")));
        assert_eq!(descriptor.input_schema["properties"]["evidence"]["maxLength"], 8000);
        let git = &descriptor.input_schema["properties"]["git"];
        let properties = git["properties"].as_object().expect("git properties");
        assert_eq!(properties.len(), 2);
        assert!(properties.contains_key("content_revision"));
        assert!(properties.contains_key("head_commit"));
        assert!(!properties.contains_key("repository_id"));
        assert!(!properties.contains_key("base_commit"));
        assert!(!properties.contains_key("branch_ref"));
        assert_eq!(properties["head_commit"]["minLength"], 1);
        assert_eq!(git["additionalProperties"], false);
    }

    #[test]
    fn git_submit_schema_requires_git_payload_for_git_kind() {
        let descriptor = team_tool_descriptor("team_submit").expect("submit descriptor");
        let conditions = descriptor.input_schema["allOf"].as_array().expect("submit conditions");

        assert!(conditions.iter().any(|condition| {
            condition["if"]["properties"]["kind"]["const"] == "git"
                && condition["if"]["required"] == json!(["kind"])
                && condition["then"]["required"] == json!(["git"])
        }));
    }

    #[test]
    fn inline_submit_schema_forbids_git_payload() {
        let descriptor = team_tool_descriptor("team_submit").expect("submit descriptor");
        let conditions = descriptor.input_schema["allOf"].as_array().expect("submit conditions");

        assert!(conditions.iter().any(|condition| {
            condition["if"]["properties"]["kind"]["const"] == "inline"
                && condition["if"]["required"] == json!(["kind"])
                && condition["then"]["not"]["required"] == json!(["git"])
        }));
    }

    #[test]
    fn progress_and_review_schemas_bind_context_to_the_command() {
        let progress = team_tool_descriptor("team_progress").expect("progress descriptor");
        assert_eq!(progress.input_schema["properties"]["context"]["maxLength"], 4000);
        assert!(progress.description.contains("committed atomically"));

        let review = team_tool_descriptor("team_review").expect("review descriptor");
        assert_eq!(review.input_schema["properties"]["feedback"]["maxLength"], 4000);
        assert!(review.description.contains("committed atomically"));
    }

    #[test]
    fn integrate_schema_accepts_only_canonical_identity_and_revisions() {
        let descriptor = team_tool_descriptor("team_integrate").expect("integrate descriptor");
        assert_eq!(descriptor.permission, TeamToolPermission::AnyTeamAgent);
        let properties = descriptor.input_schema["properties"].as_object().unwrap();
        assert_eq!(properties.len(), 4);
        for field in [
            "idempotency_key",
            "work_item_id",
            "expected_work_revision",
            "expected_delivery_revision",
        ] {
            assert!(properties.contains_key(field));
        }
        assert!(descriptor.description.contains("only supported way"));
        assert_eq!(descriptor.input_schema["additionalProperties"], false);
    }
}
