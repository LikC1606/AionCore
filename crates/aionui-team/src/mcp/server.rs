use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock, Weak};

use aionui_api_types::TeamSendMessageQueuedResponse;
use aionui_realtime::EventBroadcaster;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tracing::{debug, error, info, warn};

use crate::error::{TeamError, classify_public_error};
use crate::prompt_dump::{TeamPromptDumpConfig, TeamToolsListDump, dump_team_tools_list};
use crate::scheduler::TeammateManager;
use crate::service::TeamSessionService;
use crate::session::{AgentMessageQueueResult, SpawnAgentRequest};
use crate::tool_executor::{TeamToolContext, TeamToolExecutor, team_tool_call_from_name};
use crate::types::{TeamAgent, TeammateRole, TeammateStatus};
use crate::work_source::WorkSource;

use super::protocol::{
    INVALID_PARAMS, INVALID_REQUEST, JsonRpcResponse, METHOD_NOT_FOUND, PROTOCOL_VERSION, SERVER_NAME, SERVER_VERSION,
    read_request, write_response,
};
use super::tools::{RenameAgentInput, SendMessageInput, ShutdownAgentInput, SpawnAgentInput};

// ---------------------------------------------------------------------------
// TeamMcpServer
// ---------------------------------------------------------------------------

#[derive(Default)]
struct TeamMcpCredentialState {
    token_by_slot: HashMap<String, String>,
    slot_by_token: HashMap<String, String>,
}

/// Session-local credentials. A token identifies exactly one member slot;
/// caller-provided slot metadata is never an authorization input.
struct TeamMcpCredentialRegistry {
    state: RwLock<TeamMcpCredentialState>,
}

impl TeamMcpCredentialRegistry {
    fn new(bootstrap_token: String, agents: &[TeamAgent]) -> Result<Self, TeamError> {
        if bootstrap_token.trim().is_empty() {
            return Err(TeamError::InvalidRequest(
                "Team MCP bootstrap credential must not be empty".to_owned(),
            ));
        }
        let lead = agents
            .iter()
            .find(|agent| agent.role == TeammateRole::Lead)
            .ok_or_else(|| TeamError::InvalidRequest("Team MCP requires a lead member".to_owned()))?;
        let mut state = TeamMcpCredentialState::default();
        state
            .token_by_slot
            .insert(lead.slot_id.clone(), bootstrap_token.clone());
        state.slot_by_token.insert(bootstrap_token, lead.slot_id.clone());
        for agent in agents {
            Self::ensure_slot_locked(&mut state, &agent.slot_id);
        }
        Ok(Self {
            state: RwLock::new(state),
        })
    }

    fn credential_for_slot(&self, slot_id: &str) -> Option<String> {
        self.read_state().token_by_slot.get(slot_id).cloned()
    }

    fn ensure_slot(&self, slot_id: &str) -> String {
        let mut state = self.write_state();
        Self::ensure_slot_locked(&mut state, slot_id)
    }

    fn resolve_slot(&self, token: &str) -> Option<String> {
        self.read_state().slot_by_token.get(token).cloned()
    }

    fn revoke_slot(&self, slot_id: &str) {
        let mut state = self.write_state();
        if let Some(token) = state.token_by_slot.remove(slot_id) {
            state.slot_by_token.remove(&token);
        }
    }

    fn ensure_slot_locked(state: &mut TeamMcpCredentialState, slot_id: &str) -> String {
        if let Some(token) = state.token_by_slot.get(slot_id) {
            return token.clone();
        }
        let token = loop {
            let candidate = aionui_common::generate_id();
            if !state.slot_by_token.contains_key(&candidate) {
                break candidate;
            }
        };
        state.token_by_slot.insert(slot_id.to_owned(), token.clone());
        state.slot_by_token.insert(token.clone(), slot_id.to_owned());
        token
    }

    fn read_state(&self) -> std::sync::RwLockReadGuard<'_, TeamMcpCredentialState> {
        self.state.read().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write_state(&self) -> std::sync::RwLockWriteGuard<'_, TeamMcpCredentialState> {
        self.state.write().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Clone)]
struct TeamMcpTransportContext {
    credentials: Arc<TeamMcpCredentialRegistry>,
}

pub struct TeamMcpServer {
    addr: SocketAddr,
    http_addr: SocketAddr,
    bootstrap_auth_token: String,
    credentials: Arc<TeamMcpCredentialRegistry>,
    shutdown_tx: watch::Sender<bool>,
}

impl TeamMcpServer {
    pub async fn start(
        auth_token: String,
        scheduler: Arc<TeammateManager>,
        team_id: String,
        broadcaster: Arc<dyn EventBroadcaster>,
        service: Weak<TeamSessionService>,
    ) -> Result<Self, TeamError> {
        Self::start_with_prompt_dump(
            auth_token,
            scheduler,
            team_id,
            broadcaster,
            service,
            Some(TeamPromptDumpConfig::disabled()),
        )
        .await
    }

    pub async fn start_with_prompt_dump(
        auth_token: String,
        scheduler: Arc<TeammateManager>,
        team_id: String,
        _broadcaster: Arc<dyn EventBroadcaster>,
        service: Weak<TeamSessionService>,
        prompt_dump: Option<TeamPromptDumpConfig>,
    ) -> Result<Self, TeamError> {
        let initial_agents = scheduler.list_agents().await;
        let credentials = Arc::new(TeamMcpCredentialRegistry::new(auth_token.clone(), &initial_agents)?);
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(l) => l,
            Err(e) => {
                return Err(TeamError::InvalidRequest(format!("Failed to bind TCP: {e}")));
            }
        };
        let addr = listener
            .local_addr()
            .map_err(|e| TeamError::InvalidRequest(format!("Failed to get local addr: {e}")))?;

        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let sched_for_tcp = scheduler.clone();
        let service_for_tcp = service.clone();
        let team_id_for_tcp = team_id.clone();
        let prompt_dump = prompt_dump.unwrap_or_else(TeamPromptDumpConfig::disabled);
        let prompt_dump_for_tcp = prompt_dump.clone();
        let transport_context = TeamMcpTransportContext {
            credentials: Arc::clone(&credentials),
        };
        tokio::spawn(accept_loop(
            listener,
            transport_context.clone(),
            sched_for_tcp,
            service_for_tcp,
            team_id_for_tcp,
            prompt_dump_for_tcp,
            shutdown_rx.clone(),
        ));

        // HTTP MCP endpoint for agents that prefer http transport.
        let http_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| TeamError::InvalidRequest(format!("Failed to bind HTTP: {e}")))?;
        let http_addr = http_listener
            .local_addr()
            .map_err(|e| TeamError::InvalidRequest(format!("Failed to get HTTP addr: {e}")))?;

        let http_sched = scheduler.clone();
        let http_service = service.clone();
        let http_team_id = team_id.clone();
        let http_prompt_dump = prompt_dump.clone();
        tokio::spawn(http_mcp_loop(
            http_listener,
            transport_context,
            http_sched,
            http_service,
            http_team_id,
            http_prompt_dump,
            shutdown_rx,
        ));

        debug!(
            tcp_port = addr.port(),
            http_port = http_addr.port(),
            "Team MCP Server started"
        );

        Ok(Self {
            addr,
            http_addr,
            bootstrap_auth_token: auth_token,
            credentials,
            shutdown_tx,
        })
    }

    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub fn http_port(&self) -> u16 {
        self.http_addr.port()
    }

    /// Compatibility accessor for callers that bootstrap a standalone server.
    /// This credential is bound to the lead slot and is not valid for workers.
    pub fn auth_token(&self) -> &str {
        &self.bootstrap_auth_token
    }

    pub fn credential_for_slot(&self, slot_id: &str) -> Option<String> {
        self.credentials.credential_for_slot(slot_id)
    }

    pub(crate) fn ensure_slot_credential(&self, slot_id: &str) -> String {
        self.credentials.ensure_slot(slot_id)
    }

    pub(crate) fn revoke_slot_credential(&self, slot_id: &str) {
        self.credentials.revoke_slot(slot_id);
    }

    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
        debug!(port = self.addr.port(), "Team MCP Server stop requested");
    }
}

impl Drop for TeamMcpServer {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
    }
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

async fn accept_loop(
    listener: TcpListener,
    transport_context: TeamMcpTransportContext,
    scheduler: Arc<TeammateManager>,
    service: Weak<TeamSessionService>,
    team_id: String,
    prompt_dump: TeamPromptDumpConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, peer)) => {
                        debug!(?peer, "New MCP connection");
                        let connection_context = transport_context.clone();
                        let sched = Arc::clone(&scheduler);
                        let svc = service.clone();
                        let tid = team_id.clone();
                        let dump = prompt_dump.clone();
                        let connection_shutdown = shutdown_rx.clone();
                        tokio::spawn(handle_connection(
                            stream,
                            connection_context,
                            sched,
                            svc,
                            tid,
                            dump,
                            connection_shutdown,
                        ));
                    }
                    Err(e) => {
                        error!("Accept error: {e}");
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    debug!("MCP server shutting down");
                    break;
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection(
    stream: TcpStream,
    transport_context: TeamMcpTransportContext,
    scheduler: Arc<TeammateManager>,
    service: Weak<TeamSessionService>,
    team_id: String,
    prompt_dump: TeamPromptDumpConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let (mut reader, mut writer) = tokio::io::split(stream);

    let mut authenticated = false;
    let mut caller_slot_id: Option<String> = None;

    loop {
        let request = tokio::select! {
            request = read_request(&mut reader) => match request {
                Ok(req) => req,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    warn!("Read error: {e}");
                    break;
                }
            },
            _ = shutdown_rx.changed() => break,
        };
        if *shutdown_rx.borrow() {
            break;
        }

        if request.id.is_none() {
            continue;
        }

        let response = if !authenticated {
            match handle_initialize(&request, &transport_context.credentials) {
                InitResult::Authenticated(slot_id, resp) => {
                    if scheduler.get_agent(&slot_id).await.is_err() {
                        JsonRpcResponse::error(
                            request.id,
                            INVALID_REQUEST,
                            "Authentication failed: credential is no longer active",
                        )
                    } else {
                        info!(team_id = %team_id, slot_id = %slot_id, "MCP agent authenticated");
                        authenticated = true;
                        caller_slot_id = Some(slot_id);
                        resp
                    }
                }
                InitResult::Response(resp) => {
                    warn!(team_id = %team_id, method = %request.method, "MCP auth rejected");
                    resp
                }
            }
        } else {
            handle_method(
                &request,
                &scheduler,
                &service,
                &team_id,
                caller_slot_id.as_deref().unwrap_or("unknown"),
                &prompt_dump,
            )
            .await
        };

        if write_response(&mut writer, &response).await.is_err() {
            warn!(team_id = %team_id, "MCP connection write failed, closing");
            break;
        }
    }
}

// ---------------------------------------------------------------------------
// Initialize / handshake
// ---------------------------------------------------------------------------

enum InitResult {
    Authenticated(String, JsonRpcResponse),
    Response(JsonRpcResponse),
}

fn handle_initialize(request: &super::protocol::JsonRpcRequest, credentials: &TeamMcpCredentialRegistry) -> InitResult {
    if request.method != "initialize" {
        return InitResult::Response(JsonRpcResponse::error(
            request.id,
            INVALID_REQUEST,
            "Expected 'initialize' as first request",
        ));
    }

    let params = request.params.as_ref();

    let token = params
        .and_then(|p| p.get("auth_token"))
        .or_else(|| params.and_then(|p| p.get("authToken")))
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let Some(slot_id) = credentials.resolve_slot(token) else {
        return InitResult::Response(JsonRpcResponse::error(
            request.id,
            INVALID_REQUEST,
            "Authentication failed: invalid auth_token",
        ));
    };

    let resp = JsonRpcResponse::success(
        request.id,
        json!({
            "protocolVersion": PROTOCOL_VERSION,
            "serverInfo": {
                "name": SERVER_NAME,
                "version": SERVER_VERSION
            },
            "capabilities": {
                "tools": {}
            }
        }),
    );

    InitResult::Authenticated(slot_id, resp)
}

// ---------------------------------------------------------------------------
// Method router
// ---------------------------------------------------------------------------

async fn handle_method(
    request: &super::protocol::JsonRpcRequest,
    scheduler: &TeammateManager,
    service: &Weak<TeamSessionService>,
    team_id: &str,
    caller_slot_id: &str,
    prompt_dump: &TeamPromptDumpConfig,
) -> JsonRpcResponse {
    if scheduler.get_agent(caller_slot_id).await.is_err() {
        return JsonRpcResponse::error(
            request.id,
            INVALID_REQUEST,
            "Authentication failed: credential is no longer active",
        );
    }
    match request.method.as_str() {
        "notifications/initialized" => JsonRpcResponse::success(request.id, json!({})),
        "tools/list" => handle_tools_list(request.id, scheduler, team_id, caller_slot_id, prompt_dump).await,
        "tools/call" => handle_tools_call(request, scheduler, service, team_id, caller_slot_id).await,
        _ => JsonRpcResponse::error(
            request.id,
            METHOD_NOT_FOUND,
            format!("Unknown method: {}", request.method),
        ),
    }
}

async fn caller_role_for_tools_list(scheduler: &TeammateManager, caller_slot_id: &str) -> TeammateRole {
    scheduler
        .get_agent(caller_slot_id)
        .await
        .map(|agent| agent.role)
        .unwrap_or(TeammateRole::Teammate)
}

async fn handle_tools_list(
    id: Option<u64>,
    scheduler: &TeammateManager,
    team_id: &str,
    caller_slot_id: &str,
    prompt_dump: &TeamPromptDumpConfig,
) -> JsonRpcResponse {
    let caller_role = caller_role_for_tools_list(scheduler, caller_slot_id).await;
    let empty_service = Weak::new();
    let executor = TeamToolExecutor::new(scheduler, &empty_service);
    let context = TeamToolContext {
        team_id: team_id.to_owned(),
        caller_slot_id: caller_slot_id.to_owned(),
        caller_role,
        user_id: None,
        conversation_id: None,
        transport: aionui_api_types::TeamToolTransport::Mcp,
    };
    let tools = executor.list_tools(&context);
    let tools_for_dump = tool_descriptors_to_mcp_json(&tools);
    if let Err(error) = dump_team_tools_list(
        prompt_dump,
        TeamToolsListDump {
            team_id,
            caller_slot_id,
            caller_role,
            tools: &tools_for_dump,
        },
    ) {
        warn!(
            team_id,
            caller_slot_id,
            error = %error,
            "team tools/list prompt dump failed"
        );
    }
    JsonRpcResponse::success(id, json!({ "tools": tools }))
}

fn tool_descriptors_to_mcp_json(tools: &[super::tools::ToolDescriptor]) -> Vec<Value> {
    tools
        .iter()
        .map(|d| {
            json!({
                "name": d.name,
                "description": d.description,
                "inputSchema": d.input_schema,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// tools/call dispatcher
// ---------------------------------------------------------------------------

async fn handle_tools_call(
    request: &super::protocol::JsonRpcRequest,
    scheduler: &TeammateManager,
    service: &Weak<TeamSessionService>,
    team_id: &str,
    caller_slot_id: &str,
) -> JsonRpcResponse {
    let params = match request.params.as_ref() {
        Some(p) => p,
        None => {
            return JsonRpcResponse::error(request.id, INVALID_PARAMS, "Missing params for tools/call");
        }
    };

    let tool_name = match params.get("name").and_then(|v| v.as_str()) {
        Some(n) => n,
        None => {
            return JsonRpcResponse::error(request.id, INVALID_PARAMS, "Missing 'name' in tools/call params");
        }
    };

    let arguments = params.get("arguments").cloned().unwrap_or(json!({}));

    let (caller_role, caller_conversation_id) = match scheduler.get_agent(caller_slot_id).await {
        Ok(agent) => (agent.role, Some(agent.conversation_id)),
        Err(_) => (TeammateRole::Teammate, None),
    };

    info!(
        team_id = %team_id,
        caller = %caller_slot_id,
        tool = %tool_name,
        "MCP tools/call invoked"
    );

    let context = TeamToolContext {
        team_id: team_id.to_owned(),
        caller_slot_id: caller_slot_id.to_owned(),
        caller_role,
        user_id: None,
        conversation_id: caller_conversation_id,
        transport: aionui_api_types::TeamToolTransport::Mcp,
    };
    let result = match team_tool_call_from_name(tool_name, arguments) {
        Ok(call) => execute_mcp_tool(scheduler, service, &context, call).await,
        Err(error) => Err(error),
    };

    match &result {
        Ok(_) => info!(team_id = %team_id, tool = %tool_name, caller = %caller_slot_id, "MCP tool call succeeded"),
        Err(e) => {
            warn!(
                team_id = %team_id,
                tool = %tool_name,
                caller = %caller_slot_id,
                error = %e.message,
                "MCP tool call failed"
            )
        }
    }

    match result {
        Ok(content) => JsonRpcResponse::success(
            request.id,
            json!({
                "content": [{ "type": "text", "text": serde_json::to_string(&content).unwrap_or_else(|_| content.to_string()) }]
            }),
        ),
        Err(err) => {
            let mut result = json!({
                "content": [{ "type": "text", "text": err.message }],
                "isError": true
            });
            result["structuredContent"] = serde_json::to_value(&err).unwrap_or_else(|_| json!({}));
            JsonRpcResponse::success(request.id, result)
        }
    }
}

async fn execute_mcp_tool(
    scheduler: &TeammateManager,
    service: &Weak<TeamSessionService>,
    context: &TeamToolContext,
    call: aionui_api_types::TeamToolCall,
) -> Result<Value, aionui_api_types::TeamToolErrorPayload> {
    if let Some(service) = service.upgrade() {
        return service.execute_team_tool(context, call).await;
    }

    // Standalone MCP servers have no Team lifecycle owner and therefore no
    // concurrent remove_team operation to coordinate with.
    TeamToolExecutor::new(scheduler, service).execute(context, call).await
}

// ---------------------------------------------------------------------------
// Tool dispatch
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ToolCallError {
    pub message: String,
    pub domain_code: Option<&'static str>,
    pub details: Option<Value>,
}

impl ToolCallError {
    fn from_message(message: impl Into<String>) -> Self {
        let message = message.into();
        let classified = classify_public_error(&message);
        Self {
            message,
            domain_code: classified.as_ref().map(|value| value.code),
            details: classified.and_then(|value| value.details),
        }
    }
}

impl std::fmt::Display for ToolCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

pub(crate) async fn dispatch_tool(
    tool_name: &str,
    arguments: &Value,
    scheduler: &TeammateManager,
    service: &Weak<TeamSessionService>,
    team_id: &str,
    caller_slot_id: &str,
    caller_role: TeammateRole,
) -> Result<String, ToolCallError> {
    super::tools::authorize_tool(caller_role, tool_name).map_err(ToolCallError::from_message)?;

    match tool_name {
        "team_send_message" => exec_send_message(arguments, scheduler, service, team_id, caller_slot_id).await,
        "team_spawn_agent" => exec_spawn_agent(arguments, service, team_id, caller_slot_id, caller_role).await,
        "team_members" => exec_members(scheduler).await,
        "team_rename_agent" => exec_rename_agent(arguments, scheduler, service, team_id).await,
        "team_shutdown_agent" => {
            exec_shutdown_agent(arguments, scheduler, service, team_id, caller_slot_id, caller_role).await
        }
        "team_list_assistants" => exec_list_assistants(arguments, service).await,
        "team_describe_assistant" => exec_describe_assistant(arguments, service).await,
        _ => Err(ToolCallError::from_message(format!("Unknown tool: {tool_name}"))),
    }
}

async fn exec_list_assistants(args: &Value, service: &Weak<TeamSessionService>) -> Result<String, ToolCallError> {
    let props = args.as_object().cloned().unwrap_or_default();
    if !props.is_empty() {
        return Err(ToolCallError::from_message(
            "team_list_assistants does not accept arguments",
        ));
    }
    let service = service
        .upgrade()
        .ok_or_else(|| ToolCallError::from_message("Team service not available"))?;
    let assistants = service.list_team_selectable_assistants().await;
    let value = json!({ "assistants": assistants });
    serde_json::to_string_pretty(&value).map_err(|e| ToolCallError::from_message(format!("Serialization error: {e}")))
}

async fn exec_describe_assistant(args: &Value, service: &Weak<TeamSessionService>) -> Result<String, ToolCallError> {
    if args.get("custom_agent_id").is_some() {
        return Err(ToolCallError::from_message(
            "custom_agent_id is no longer accepted; use assistant_id",
        ));
    }
    let assistant_id = args
        .get("assistant_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ToolCallError::from_message("Missing required field: assistant_id"))?;
    let locale = args.get("locale").and_then(Value::as_str);
    let service = service
        .upgrade()
        .ok_or_else(|| ToolCallError::from_message("Team service not available"))?;

    service
        .describe_assistant(assistant_id, locale)
        .await
        .map_err(|error| ToolCallError::from_message(error.to_string()))
}

// ---------------------------------------------------------------------------
// Individual tool handlers
// ---------------------------------------------------------------------------

async fn resolve_agent_target(
    scheduler: &TeammateManager,
    target: &str,
    allow_broadcast: bool,
) -> Result<String, String> {
    let agents = scheduler.list_agents().await;
    resolve_agent_target_from_agents(&agents, target, allow_broadcast)
}

fn resolve_agent_target_from_agents(
    agents: &[TeamAgent],
    target: &str,
    allow_broadcast: bool,
) -> Result<String, String> {
    if agents.iter().any(|a| a.slot_id == target) {
        return Ok(target.to_owned());
    }

    if target.eq_ignore_ascii_case("leader") || target.eq_ignore_ascii_case("lead") {
        let leaders = agents
            .iter()
            .filter(|agent| agent.role == TeammateRole::Lead)
            .collect::<Vec<_>>();
        return match leaders.as_slice() {
            [leader] => Ok(leader.slot_id.clone()),
            [] => Err("Cannot resolve recipient 'leader': this team has no leader slot. Call team_members and use an exact slot_id.".to_owned()),
            _ => Err("Cannot resolve recipient 'leader': this team has multiple leader slots. Call team_members and use an exact slot_id.".to_owned()),
        };
    }

    if allow_broadcast {
        Err(format!(
            "Invalid agent target '{target}': expected slot_id, \"leader\", or \"*\". Do not use team_id as a recipient; call team_members to get slot_id."
        ))
    } else {
        Err(format!(
            "Invalid agent target '{target}': expected slot_id. Call team_members to get slot_id."
        ))
    }
}

async fn exec_send_message(
    args: &Value,
    scheduler: &TeammateManager,
    service: &Weak<TeamSessionService>,
    team_id: &str,
    caller_slot_id: &str,
) -> Result<String, ToolCallError> {
    let input: SendMessageInput = serde_json::from_value(args.clone())
        .map_err(|e| ToolCallError::from_message(format!("Invalid params: {e}")))?;

    let trimmed = input.message.trim();
    if trimmed == "shutdown_approved" {
        debug!(from = caller_slot_id, "shutdown_approved intercepted");
        scheduler.notify_shutdown_acknowledged(caller_slot_id);

        // Deferred cleanup: kill process, delete conversation, remove from team DB.
        // Spawned so the MCP response can be sent back before the process is killed.
        let slot = caller_slot_id.to_owned();
        let tid = team_id.to_owned();
        let svc_weak = service.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            if let Some(svc) = svc_weak.upgrade() {
                let user_id = svc.get_session_user_id(&tid).await.unwrap_or_default();
                if let Err(e) = svc.remove_agent(&user_id, &tid, &slot).await {
                    warn!(slot_id = %slot, error = %e, "shutdown cleanup failed");
                } else {
                    info!(slot_id = %slot, "agent fully removed after shutdown_approved");
                }
            }
        });

        return Ok(json!({"status": "shutdown_approved_received"}).to_string());
    }
    if let Some(rest) = trimmed.strip_prefix("shutdown_rejected:") {
        let reason = rest.trim();
        scheduler
            .notify_shutdown_rejected(caller_slot_id, reason)
            .await
            .map_err(|e| ToolCallError::from_message(e.to_string()))?;
        if let Some(svc) = service.upgrade()
            && let Err(e) = svc
                .wake_leader_after_recovery_message(team_id, caller_slot_id, WorkSource::ShutdownRejected)
                .await
        {
            warn!(
                team_id,
                slot_id = %caller_slot_id,
                wake_source = %WorkSource::ShutdownRejected,
                error = %e,
                "failed to apply shutdown_rejected wake policy"
            );
        }
        debug!(from = caller_slot_id, reason, "shutdown_rejected handled");
        return Ok(json!({
            "status": "ok",
            "action": "shutdown_rejected",
            "reason": reason,
        })
        .to_string());
    }

    let resolved_to = if input.to == "*" {
        "*".to_owned()
    } else {
        resolve_agent_target(scheduler, &input.to, true)
            .await
            .map_err(ToolCallError::from_message)?
    };

    let service = service
        .upgrade()
        .ok_or_else(|| ToolCallError::from_message("Team service not available; cannot wake target"))?;

    let targets = if resolved_to == "*" {
        scheduler
            .list_agents()
            .await
            .iter()
            .filter(|a| a.slot_id != caller_slot_id)
            .map(|a| a.slot_id.clone())
            .collect::<Vec<_>>()
    } else {
        vec![resolved_to.clone()]
    };
    let mut target_results = Vec::with_capacity(targets.len());
    for target in &targets {
        let result = service
            .send_agent_message_from_agent_with_idempotency(
                team_id,
                caller_slot_id,
                target,
                &input.message,
                Some(input.files.clone()),
                input.idempotency_key.clone(),
            )
            .await
            .map_err(|e| ToolCallError::from_message(e.to_string()))?;
        target_results.push(result);
    }

    let response = build_send_message_queued_response(target_results).map_err(ToolCallError::from_message)?;

    serde_json::to_string(&response).map_err(|e| ToolCallError::from_message(format!("Serialization error: {e}")))
}

fn build_send_message_queued_response(
    target_results: Vec<AgentMessageQueueResult>,
) -> Result<TeamSendMessageQueuedResponse, String> {
    let first = target_results
        .first()
        .ok_or_else(|| "No message targets resolved".to_string())?;
    Ok(TeamSendMessageQueuedResponse {
        team_run_id: first.team_run_id.clone(),
        target: first.target.clone(),
    })
}

fn agent_json(agent: &TeamAgent) -> Value {
    let status = agent.status.unwrap_or(TeammateStatus::Idle);
    json!({
        "slot_id": agent.slot_id,
        "name": agent.name,
        "role": agent.role,
        "status": status,
        "assistant_id": agent.assistant_id,
        "model": agent.model,
    })
}

fn json_text(value: &Value) -> Result<String, ToolCallError> {
    serde_json::to_string_pretty(value).map_err(|e| ToolCallError::from_message(format!("Serialization error: {e}")))
}

async fn exec_spawn_agent(
    args: &Value,
    service: &Weak<TeamSessionService>,
    team_id: &str,
    caller_slot_id: &str,
    caller_role: TeammateRole,
) -> Result<String, ToolCallError> {
    // Lead-only at the MCP dispatch layer. `TeamSession::spawn_agent` also
    // re-checks via `TeamError::LeaderOnly`, but the dispatch-level string
    // keeps the user-visible "Only Lead ..." phrasing that the MCP client
    // (and existing protocol tests) expect.
    if caller_role != TeammateRole::Lead {
        return Err(ToolCallError::from_message("Only Lead can spawn agents"));
    }
    if args.get("backend").is_some() {
        return Err(ToolCallError::from_message(
            "backend is no longer accepted; use assistant_id",
        ));
    }
    if args.get("agent_type").is_some() {
        return Err(ToolCallError::from_message(
            "agent_type is no longer accepted; use assistant_id",
        ));
    }
    if args.get("model").is_some() {
        return Err(ToolCallError::from_message(
            "model is no longer accepted; use the assistant configuration or UI model selector",
        ));
    }

    let input: SpawnAgentInput = serde_json::from_value(args.clone())
        .map_err(|e| ToolCallError::from_message(format!("Invalid params: {e}")))?;
    let assistant_id = input
        .assistant_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ToolCallError::from_message("Missing required field: assistant_id"))?;

    // Requested name — normalization / emptiness / uniqueness live in
    // `TeamSession::spawn_agent` so we do not double-validate here.
    let requested_name = input.name.clone();

    let req = SpawnAgentRequest {
        name: requested_name.clone(),
        assistant_id: Some(assistant_id),
    };

    let service = service
        .upgrade()
        .ok_or_else(|| ToolCallError::from_message("Team service not available; cannot spawn agent"))?;

    service
        .spawn_agent_in_session(team_id, caller_slot_id, req)
        .await
        .and_then(|agent| {
            serde_json::to_string_pretty(&json!({
                "status": "ok",
                "action": "agent_spawned",
                "agent": agent_json(&agent),
            }))
            .map_err(TeamError::Json)
        })
        .map_err(|e| ToolCallError::from_message(e.to_string()))
}

async fn exec_members(scheduler: &TeammateManager) -> Result<String, ToolCallError> {
    let mut agents = scheduler.list_agents().await;
    agents.sort_by_key(|agent| match agent.role {
        TeammateRole::Lead => 0,
        TeammateRole::Teammate => 1,
    });
    let output: Vec<Value> = agents.iter().map(agent_json).collect();
    serde_json::to_string_pretty(&output).map_err(|e| ToolCallError::from_message(format!("Serialization error: {e}")))
}

async fn exec_rename_agent(
    args: &Value,
    scheduler: &TeammateManager,
    service: &Weak<TeamSessionService>,
    team_id: &str,
) -> Result<String, ToolCallError> {
    let input: RenameAgentInput = serde_json::from_value(args.clone())
        .map_err(|e| ToolCallError::from_message(format!("Invalid params: {e}")))?;

    let resolved_slot = resolve_agent_target(scheduler, &input.slot_id, false)
        .await
        .map_err(ToolCallError::from_message)?;

    if let Some(svc) = service.upgrade() {
        let user_id = svc
            .get_session_user_id(team_id)
            .await
            .ok_or_else(|| ToolCallError::from_message(format!("No active session for team {team_id}")))?;
        svc.rename_agent(&user_id, team_id, &resolved_slot, &input.new_name)
            .await
            .map_err(|e| ToolCallError::from_message(e.to_string()))?;
    } else {
        scheduler
            .rename_agent(&resolved_slot, &input.new_name)
            .await
            .map_err(|e| ToolCallError::from_message(e.to_string()))?;
    }

    let agent = scheduler
        .get_agent(&resolved_slot)
        .await
        .map_err(|e| ToolCallError::from_message(e.to_string()))?;
    json_text(&json!({
        "status": "ok",
        "action": "agent_renamed",
        "agent": agent_json(&agent),
    }))
}

async fn exec_shutdown_agent(
    args: &Value,
    scheduler: &TeammateManager,
    service: &Weak<TeamSessionService>,
    team_id: &str,
    caller_slot_id: &str,
    caller_role: TeammateRole,
) -> Result<String, ToolCallError> {
    if caller_role != TeammateRole::Lead {
        return Err(ToolCallError::from_message("Only Lead can shut down agents"));
    }
    let input: ShutdownAgentInput = serde_json::from_value(args.clone())
        .map_err(|e| ToolCallError::from_message(format!("Invalid params: {e}")))?;

    let target_slot_id = resolve_agent_target(scheduler, &input.slot_id, false)
        .await
        .map_err(ToolCallError::from_message)?;
    let service = service
        .upgrade()
        .ok_or_else(|| ToolCallError::from_message("Team service not available; cannot wake shutdown target"))?;
    let reason = input.reason.clone();
    service
        .shutdown_agent_in_session(team_id, caller_slot_id, &target_slot_id, input.reason)
        .await
        .map_err(|e| ToolCallError::from_message(e.to_string()))?;

    json_text(&json!({
        "status": "ok",
        "action": "shutdown_requested",
        "target_slot_id": target_slot_id,
        "reason": reason,
    }))
}

// ---------------------------------------------------------------------------
// HTTP MCP endpoint (Streamable HTTP transport for MCP)
// ---------------------------------------------------------------------------

async fn http_mcp_loop(
    listener: TcpListener,
    transport_context: TeamMcpTransportContext,
    scheduler: Arc<TeammateManager>,
    service: Weak<TeamSessionService>,
    team_id: String,
    prompt_dump: TeamPromptDumpConfig,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    loop {
        tokio::select! {
            accept = listener.accept() => {
                let Ok((mut stream, peer)) = accept else { continue };
                info!(team_id = %team_id, ?peer, "HTTP MCP: new connection accepted");
                let request_context = transport_context.clone();
                let sched = scheduler.clone();
                let svc = service.clone();
                let tid = team_id.clone();
                let dump = prompt_dump.clone();
                let mut request_shutdown = shutdown_rx.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    let n = tokio::select! {
                        result = stream.read(&mut buf) => match result {
                            Ok(n) if n > 0 => n,
                            _ => return,
                        },
                        _ = request_shutdown.changed() => return,
                    };
                    if *request_shutdown.borrow() {
                        return;
                    }
                    let request = String::from_utf8_lossy(&buf[..n]);

                    // Extract JSON body (after \r\n\r\n)
                    let body = request.split("\r\n\r\n").nth(1).unwrap_or("");
                    let Ok(value): Result<Value, _> = serde_json::from_str(body) else {
                        let resp = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
                        let _ = stream.write_all(resp.as_bytes()).await;
                        return;
                    };

                    // Handle JSON-RPC request
                    let method = value.get("method").and_then(Value::as_str).unwrap_or("");
                    let id = value.get("id").cloned();
                    let caller_slot_id = http_bearer_token(&request)
                        .and_then(|provided| request_context.credentials.resolve_slot(provided));
                    let caller = match caller_slot_id
                        .as_deref()
                        .map(|slot_id| sched.get_agent(slot_id))
                    {
                        Some(agent) => agent.await.ok(),
                        None => None,
                    };
                    let Some(caller) = caller else {
                        let response_body = json!({
                            "jsonrpc": "2.0",
                            "id": id,
                            "error": {
                                "code": INVALID_REQUEST,
                                "message": "Authentication failed: invalid auth_token"
                            }
                        });
                        let body_bytes = serde_json::to_vec(&response_body).unwrap_or_default();
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                            body_bytes.len()
                        );
                        let _ = stream.write_all(header.as_bytes()).await;
                        let _ = stream.write_all(&body_bytes).await;
                        return;
                    };
                    let caller_slot_id = caller.slot_id;
                    let caller_role = caller.role;
                    let caller_conversation_id = caller.conversation_id;

                    let result = match method {
                        "initialize" => {
                            json!({
                                "capabilities": { "tools": {} },
                                "protocolVersion": PROTOCOL_VERSION,
                                "serverInfo": { "name": SERVER_NAME, "version": SERVER_VERSION }
                            })
                        }
                        "notifications/initialized" => {
                            let resp = "HTTP/1.1 204 No Content\r\n\r\n";
                            let _ = stream.write_all(resp.as_bytes()).await;
                            return;
                        }
                        "tools/list" => {
                            let context = TeamToolContext {
                                team_id: tid.clone(),
                                caller_slot_id: caller_slot_id.clone(),
                                caller_role,
                                user_id: None,
                                conversation_id: Some(caller_conversation_id.clone()),
                                transport: aionui_api_types::TeamToolTransport::Mcp,
                            };
                            let descriptors = TeamToolExecutor::new(&sched, &svc).list_tools(&context);
                            let tools: Vec<Value> = tool_descriptors_to_mcp_json(&descriptors);
                            if let Err(error) = dump_team_tools_list(
                                &dump,
                                TeamToolsListDump {
                                    team_id: &tid,
                                    caller_slot_id: &caller_slot_id,
                                    caller_role,
                                    tools: &tools,
                                },
                            ) {
                                warn!(
                                    team_id = %tid,
                                    caller_slot_id,
                                    error = %error,
                                    "team HTTP tools/list prompt dump failed"
                                );
                            }
                            json!({ "tools": tools })
                        }
                        "tools/call" => {
                            let params = value.get("params").cloned().unwrap_or(json!({}));
                            let tool_name = params.get("name").and_then(Value::as_str).unwrap_or("");
                            let arguments = params.get("arguments").cloned().unwrap_or(json!({}));
                            let context = TeamToolContext {
                                team_id: tid.clone(),
                                caller_slot_id: caller_slot_id.clone(),
                                caller_role,
                                user_id: None,
                                conversation_id: Some(caller_conversation_id.clone()),
                                transport: aionui_api_types::TeamToolTransport::Mcp,
                            };
                            let call_result = match team_tool_call_from_name(tool_name, arguments) {
                                Ok(call) => execute_mcp_tool(&sched, &svc, &context, call).await,
                                Err(error) => Err(error),
                            };
                            match call_result {
                                Ok(value) => json!({
                                    "content": [{
                                        "type": "text",
                                        "text": serde_json::to_string(&value).unwrap_or_else(|_| value.to_string())
                                    }]
                                }),
                                Err(err) => {
                                    let mut result = json!({
                                        "content": [{"type": "text", "text": err.message}],
                                        "isError": true,
                                    });
                                    result["structuredContent"] = serde_json::to_value(&err).unwrap_or_else(|_| json!({}));
                                    result
                                }
                            }
                        }
                        _ => {
                            json!({"error": {"code": -32601, "message": "Method not found"}})
                        }
                    };

                    let response_body = if result.get("error").is_some() {
                        json!({"jsonrpc": "2.0", "id": id, "error": result["error"]})
                    } else {
                        json!({"jsonrpc": "2.0", "id": id, "result": result})
                    };
                    let body_bytes = serde_json::to_vec(&response_body).unwrap_or_default();
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                        body_bytes.len()
                    );
                    let _ = stream.write_all(header.as_bytes()).await;
                    let _ = stream.write_all(&body_bytes).await;
                });
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() { break; }
            }
        }
    }
}

fn http_bearer_token(request: &str) -> Option<&str> {
    request
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
        .and_then(|line| line.split_once(':').map(|(_, value)| value.trim()))
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

// ---------------------------------------------------------------------------
// Tests — exec_spawn_agent dispatch-layer unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use aionui_api_types::{TeamRunTargetRole, TeamSlotWorkPayload, TeamSlotWorkState};

    #[test]
    fn build_send_message_queued_response_serializes_json_contract() {
        let response = build_send_message_queued_response(vec![AgentMessageQueueResult {
            team_run_id: Some("run-1".into()),
            target: TeamSlotWorkPayload {
                slot_id: "worker-1".into(),
                role: TeamRunTargetRole::Teammate,
                state: TeamSlotWorkState::Queued,
                queued_foreground_count: 0,
                queued_background_count: 1,
                active_turn_id: None,
                active_turn_started_at_ms: None,
                active_turn_elapsed_ms: None,
                active_turn_slow: None,
                active_turn_slow_threshold_ms: None,
                blocked_reason: None,
                team_run_id: Some("run-1".into()),
            },
        }])
        .unwrap();

        let text = serde_json::to_string(&response).unwrap();
        let payload: serde_json::Value = serde_json::from_str(&text).expect("team_send_message result must be JSON");
        assert_eq!(payload["team_run_id"], "run-1");
        assert_eq!(payload["target"]["slot_id"], "worker-1");
        assert_eq!(payload["target"]["state"], "queued");
        assert_eq!(payload["target"]["queued_background_count"], 1);
    }

    fn target_agent(slot_id: &str, role: TeammateRole) -> TeamAgent {
        TeamAgent {
            slot_id: slot_id.to_owned(),
            name: slot_id.to_owned(),
            role,
            conversation_id: format!("conversation-{slot_id}"),
            backend: "codex".to_owned(),
            model: "test".to_owned(),
            assistant_id: None,
            status: Some(TeammateStatus::Idle),
            conversation_type: None,
            cli_path: None,
        }
    }

    #[test]
    fn leader_alias_resolves_to_the_actual_leader_slot() {
        let agents = vec![
            target_agent("lead-slot", TeammateRole::Lead),
            target_agent("worker-slot", TeammateRole::Teammate),
        ];

        assert_eq!(
            resolve_agent_target_from_agents(&agents, "leader", true),
            Ok("lead-slot".to_owned())
        );
        assert_eq!(
            resolve_agent_target_from_agents(&agents, "LEAD", true),
            Ok("lead-slot".to_owned())
        );
    }

    #[test]
    fn invalid_recipient_explains_that_team_id_is_not_a_target() {
        let agents = vec![target_agent("lead-slot", TeammateRole::Lead)];
        let error = resolve_agent_target_from_agents(&agents, "team-123", true)
            .expect_err("team id must not be accepted as an agent recipient");

        assert!(error.contains("Do not use team_id"));
        assert!(error.contains("team_members"));
    }

    /// Non-Lead callers are rejected at the dispatch layer with the
    /// "Only Lead ..." phrasing. Service weak is never upgraded because
    /// the early role gate short-circuits.
    #[tokio::test]
    async fn exec_spawn_agent_rejects_non_lead() {
        let service: Weak<TeamSessionService> = Weak::new();
        let args = json!({ "name": "Helper", "agent_type": "claude" });
        let result = exec_spawn_agent(&args, &service, "team-1", "worker-1", TeammateRole::Teammate).await;
        let err = result.expect_err("non-Lead caller must be rejected");
        assert!(
            err.message.contains("Only Lead"),
            "error must keep legacy 'Only Lead' phrasing, got {err:?}"
        );
    }

    /// Malformed JSON body is rejected before the service is consulted.
    #[tokio::test]
    async fn exec_spawn_agent_rejects_malformed_args() {
        let service: Weak<TeamSessionService> = Weak::new();
        // Wrong `name` type so serde fails before any service lookup.
        let args = json!({ "assistant_id": "word-creator", "name": 42 });
        let result = exec_spawn_agent(&args, &service, "team-1", "lead-1", TeammateRole::Lead).await;
        let err = result.expect_err("malformed args must be rejected");
        assert!(
            err.message.contains("Invalid params"),
            "must surface Invalid params for JSON deserialize failure, got {err:?}"
        );
    }

    /// Lead caller with a well-formed request but no live service (Weak
    /// cannot upgrade) surfaces the service-unavailable error rather than
    /// silently returning a fake success. This is the path exercised in
    /// tests where the MCP server is spun up without a real
    /// `TeamSessionService` — in production the Weak always upgrades.
    #[tokio::test]
    async fn exec_spawn_agent_reports_service_unavailable_when_weak_dead() {
        let service: Weak<TeamSessionService> = Weak::new();
        let args = json!({
            "name": "Helper",
            "assistant_id": "word-creator"
        });
        let result = exec_spawn_agent(&args, &service, "team-1", "lead-1", TeammateRole::Lead).await;
        let err = result.expect_err("dead Weak<TeamSessionService> must not succeed");
        assert!(
            err.message.contains("Team service not available"),
            "dead service weak must surface the unavailable message, got {err:?}"
        );
    }

    #[tokio::test]
    async fn exec_spawn_agent_rejects_model_override() {
        let service: Weak<TeamSessionService> = Weak::new();
        let args = json!({
            "name": "Helper",
            "assistant_id": "word-creator",
            "model": "claude-sonnet-4"
        });
        let result = exec_spawn_agent(&args, &service, "team-1", "lead-1", TeammateRole::Lead).await;
        let err = result.expect_err("model override must be rejected before service lookup");
        assert!(
            err.message.contains("model is no longer accepted"),
            "expected explicit model rejection, got {err:?}"
        );
    }

    #[tokio::test]
    async fn exec_spawn_agent_rejects_legacy_backend_alias() {
        let service: Weak<TeamSessionService> = Weak::new();
        let args = json!({ "name": "Helper", "backend": "claude" });
        let result = exec_spawn_agent(&args, &service, "team-1", "lead-1", TeammateRole::Lead).await;
        let err = result.expect_err("legacy backend alias must be rejected");
        assert!(
            err.message.contains("backend is no longer accepted"),
            "expected explicit backend alias rejection, got {err:?}"
        );
        assert_eq!(err.domain_code, Some("TEAM_ASSISTANT_FIELD_UNSUPPORTED"));
    }

    #[tokio::test]
    async fn exec_spawn_agent_rejects_legacy_agent_type_alias() {
        let service: Weak<TeamSessionService> = Weak::new();
        let args = json!({ "name": "Helper", "agent_type": "claude" });
        let result = exec_spawn_agent(&args, &service, "team-1", "lead-1", TeammateRole::Lead).await;
        let err = result.expect_err("legacy agent_type alias must be rejected");
        assert!(
            err.message.contains("agent_type is no longer accepted"),
            "expected explicit agent_type rejection, got {err:?}"
        );
        assert_eq!(err.domain_code, Some("TEAM_ASSISTANT_FIELD_UNSUPPORTED"));
    }

    #[tokio::test]
    async fn exec_spawn_agent_requires_assistant_identity() {
        let service: Weak<TeamSessionService> = Weak::new();
        let args = json!({ "name": "Helper" });
        let result = exec_spawn_agent(&args, &service, "team-1", "lead-1", TeammateRole::Lead).await;
        let err = result.expect_err("assistant_id must now be required");
        assert!(
            err.message.contains("Missing required field: assistant_id"),
            "expected assistant_id requirement, got {err:?}"
        );
        assert_eq!(err.domain_code, Some("TEAM_ASSISTANT_ID_REQUIRED"));
    }

    #[tokio::test]
    async fn exec_list_assistants_reports_service_unavailable_when_weak_dead() {
        let service: Weak<TeamSessionService> = Weak::new();
        let result = exec_list_assistants(&json!({}), &service).await;
        let err = result.expect_err("dead service should be surfaced");
        assert!(
            err.message.contains("Team service not available"),
            "expected service unavailable error, got {err:?}"
        );
        assert_eq!(err.domain_code, Some("TEAM_SERVICE_UNAVAILABLE"));
    }
}
