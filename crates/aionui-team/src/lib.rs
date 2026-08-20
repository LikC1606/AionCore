#![warn(clippy::disallowed_types)]

//! Multi-agent team sessions with role-based prompts, canonical work, mailbox, and scheduling.
pub mod capability;
pub mod crash_detection;
pub mod error;
pub mod event_loop;
pub mod events;
pub mod kernel;
pub mod mailbox;
pub mod mcp;
mod member_runtime;
pub mod message_projection;
pub mod ports;
pub mod prompt_dump;
pub mod prompts;
pub mod provisioning;
pub mod routes;
pub mod runtime_tools;
pub mod scheduler;
pub mod service;
pub mod session;
pub mod team_run;
#[cfg(test)]
pub(crate) mod test_utils;
pub mod tool_executor;
pub mod types;
pub mod visibility;
mod work_coordinator;
mod work_source;
mod workspace;

pub use crash_detection::{CrashReason, detect_crash, is_rate_limited};
pub use error::TeamError;
pub use events::TeamEventEmitter;
pub use kernel::IntegrationRecoveryReason;
pub use mailbox::Mailbox;
pub use mcp::{TEAM_MCP_SERVER_NAME, TeamMcpServer, TeamMcpStdioConfig, TeamMcpStdioServerSpec};
pub use message_projection::{
    ProjectedTeamMessage, TeamMessageProjection, TeamProjectionMessageStore, TeamProjectionRequest,
    TeamProjectionSource,
};
pub use ports::{
    AgentTurnCancellationPort, AgentTurnExecutionError, AgentTurnExecutionPort, AgentTurnOutcome, AgentTurnRequest,
    AgentTurnSource, AgentTurnStarted, AgentTurnStartedCallback, AgentTurnStatus, GitDeliveryPortError,
    GitDeliveryPortOutcome, GitDeliveryReconciliation, GitIntegrationIntent, GitIntegrationTarget,
    GitWorkAssignmentPlan, GitWorkspacePortError, PreparedGitWorkAssignment, TeamAssistantCatalogEntry,
    TeamAssistantCatalogPort, TeamConversationBindingLookup, TeamConversationLookupPort, TeamGitDeliveryPort,
    TeamGitWorkspacePort,
};

pub use prompt_dump::TeamPromptDumpConfig;
pub use prompts::{build_lead_prompt, build_teammate_prompt, build_wake_payload};
pub use provisioning::{
    TeamAgentProvisioner, TeamConversationCleanupCandidate, TeamConversationCreateRequest,
    TeamConversationCreateResult, TeamConversationProvisioningPort,
};
pub use routes::{TeamRouterState, team_routes};
pub use runtime_tools::ResolvedTeamToolContext;
pub use scheduler::{
    SchedulerAction, TeammateManager, WAKE_TIMEOUT_MS, WakePayload, format_crash_testament, normalize_name,
};
pub use service::{
    BeginGitIntegrationRequest, GitIntegrationRecoveryReport, MailboxRecoveryReport,
    OrphanTeamConversationRecoveryReport, TeamCommandService, TeamDeliveryError, TeamDeliveryService,
    TeamIdleCleanupCoordinator, TeamIntegrationReceipt, TeamIntegrationResolution, TeamQueryError, TeamQueryService,
    TeamSessionService, TeamWorkCoordinator,
};
pub use session::{TeamSession, WakeInput};
pub use team_run::{TeamRunManager, target_role_for};
pub use tool_executor::{TeamToolContext, TeamToolExecutor, team_tool_call_from_name};
pub use types::{MailboxMessage, MailboxMessageType, Team, TeamAgent, TeammateRole, TeammateStatus};
pub use visibility::TeamVisibilityPolicy;
