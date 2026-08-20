//! `aioncore mcp-team-stdio` subcommand: MCP stdio server for team tools.
//!
//! Uses the `rmcp` crate (Rust MCP SDK) for protocol handling. Tool calls are
//! forwarded to the TeamMcpServer TCP listener via 4-byte big-endian
//! length-prefixed JSON frames — the same wire protocol used by `mcp-bridge`,
//! but with proper tool registration via rmcp instead of transparent proxying.
//!
//! Each tool call opens a fresh TCP connection, sends an `initialize` frame
//! (injecting auth_token + slot_id), then sends the `tools/call` frame, reads
//! the response, and closes the connection (one-shot mode).

use std::process::ExitCode;

use crate::commands::error::{CliBoundaryCode, CliBoundaryError, missing_env, parse_required_port};
use aionui_api_types::TeamMcpStdioConfig;
use aionui_team::mcp::protocol::{read_frame, write_frame};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, Content, ListToolsResult, Tool};
use rmcp::{schemars, service::ServiceExt, tool, tool_router, transport};
use serde::Deserialize;
use tokio::net::TcpStream;

const SUBCOMMAND: &str = "mcp-team-stdio";
const CONNECT_HOST: &str = "127.0.0.1";
const ERR_JSON_SERIALIZE: &str = "failed to serialize MCP JSON frame";
const ERR_TCP_CONNECT: &str = "failed to connect to local MCP TCP listener";
const ERR_TCP_WRITE: &str = "failed to write MCP frame to TCP listener";
const ERR_TCP_READ: &str = "failed to read MCP frame from TCP listener";
const ERR_TOOL_REMOTE: &str = "local team tool returned an error";
const ERR_TOOL_RESPONSE_UNEXPECTED: &str = "unexpected local team tool response";

pub async fn run_team_stdio() -> ExitCode {
    let env = match TeamStdioEnv::from_env() {
        Ok(env) => env,
        Err(err) => {
            eprintln!("{}", err.stderr_line());
            return err.exit_code();
        }
    };

    let server = TeamStdioServer {
        port: env.port,
        token: env.token,
        slot_id: env.slot_id,
    };

    let transport = transport::io::stdio();
    match server.serve(transport).await {
        Ok(peer) => {
            if let Err(_e) = peer.waiting().await {
                let err = CliBoundaryError::new(
                    CliBoundaryCode::McpSessionEndedWithError,
                    SUBCOMMAND,
                    "MCP stdio session ended with an error",
                );
                eprintln!("{}", err.stderr_line());
                err.exit_code()
            } else {
                ExitCode::SUCCESS
            }
        }
        Err(_e) => {
            let err = CliBoundaryError::new(
                CliBoundaryCode::McpStdioServeFailed,
                SUBCOMMAND,
                "failed to start MCP stdio server",
            );
            eprintln!("{}", err.stderr_line());
            err.exit_code()
        }
    }
}

#[derive(Clone, Debug)]
struct TeamStdioEnv {
    port: u16,
    token: String,
    slot_id: String,
}

impl TeamStdioEnv {
    fn from_env() -> Result<Self, CliBoundaryError> {
        let port_raw = std::env::var(TeamMcpStdioConfig::ENV_PORT)
            .map_err(|_| missing_env(SUBCOMMAND, TeamMcpStdioConfig::ENV_PORT))?;
        let token = std::env::var(TeamMcpStdioConfig::ENV_TOKEN)
            .map_err(|_| missing_env(SUBCOMMAND, TeamMcpStdioConfig::ENV_TOKEN))?;
        let slot_id = std::env::var(TeamMcpStdioConfig::ENV_SLOT_ID)
            .map_err(|_| missing_env(SUBCOMMAND, TeamMcpStdioConfig::ENV_SLOT_ID))?;
        Self::from_values(&port_raw, token, slot_id)
    }

    fn from_values(
        port_raw: &str,
        token: impl Into<String>,
        slot_id: impl Into<String>,
    ) -> Result<Self, CliBoundaryError> {
        Ok(Self {
            port: parse_required_port(SUBCOMMAND, TeamMcpStdioConfig::ENV_PORT, port_raw)?,
            token: token.into(),
            slot_id: slot_id.into(),
        })
    }
}

// ---------------------------------------------------------------------------
// Server struct
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct TeamStdioServer {
    port: u16,
    token: String,
    slot_id: String,
}

// ---------------------------------------------------------------------------
// Parameter types
// ---------------------------------------------------------------------------

#[derive(Deserialize, schemars::JsonSchema)]
struct SendMessageParams {
    /// Target agent slot_id or "*" for broadcast.
    to: String,
    /// Message content.
    message: String,
    /// Absolute attachment paths to forward to the target agent.
    #[serde(default)]
    files: Vec<String>,
    /// Stable key for safely retrying the same message.
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct InspectParams {
    #[serde(default)]
    work_item_id: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct DelegateParams {
    idempotency_key: String,
    #[serde(default)]
    parent_work_item_id: Option<String>,
    subject: String,
    #[serde(default)]
    description: Option<String>,
    assignee_member_id: String,
    delivery_requirement: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ProgressParams {
    idempotency_key: String,
    work_item_id: String,
    expected_work_revision: u64,
    action: String,
    #[serde(default)]
    context: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct GitSubmissionParams {
    content_revision: u64,
    head_commit: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct SubmitParams {
    idempotency_key: String,
    work_item_id: String,
    expected_work_revision: u64,
    kind: String,
    evidence: String,
    #[serde(default)]
    git: Option<GitSubmissionParams>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct ReviewParams {
    idempotency_key: String,
    work_item_id: String,
    expected_work_revision: u64,
    #[serde(default)]
    expected_delivery_revision: Option<u64>,
    decision: String,
    #[serde(default)]
    feedback: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct IntegrateParams {
    idempotency_key: String,
    work_item_id: String,
    expected_work_revision: u64,
    expected_delivery_revision: u64,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct CancelParams {
    idempotency_key: String,
    work_item_id: String,
    expected_work_revision: u64,
    #[serde(default)]
    expected_delivery_revision: Option<u64>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct SpawnAgentParams {
    /// Agent display name.
    name: String,
    /// Assistant identifier from the available assistants catalog.
    #[serde(default)]
    assistant_id: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct RenameAgentParams {
    /// Agent slot_id to rename.
    slot_id: String,
    /// New display name.
    new_name: String,
}

#[derive(Deserialize, schemars::JsonSchema)]
struct ShutdownAgentParams {
    /// Agent slot_id to shut down.
    slot_id: String,
    /// Reason for shutdown.
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct DescribeAssistantParams {
    /// The assistant ID from the "Available Assistants" catalog.
    assistant_id: String,
    /// Locale for the description (e.g. "en", "zh"). Default when omitted.
    #[serde(default)]
    locale: Option<String>,
}

// ---------------------------------------------------------------------------
// Tool router
// ---------------------------------------------------------------------------

#[tool_router]
impl TeamStdioServer {
    #[tool(
        name = "team_send_message",
        description = "Send a message to a teammate, to the team leader (to=\"leader\"), or broadcast to all (to=\"*\"). When delegating work that depends on user attachments, forward their absolute paths in files."
    )]
    async fn send_message(&self, Parameters(params): Parameters<SendMessageParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_send_message",
            &serde_json::json!({
                "to": params.to,
                "message": params.message,
                "files": params.files,
                "idempotency_key": params.idempotency_key,
            }),
        )
        .await
    }

    #[tool(
        name = "team_inspect",
        description = "Inspect canonical Team work visible to the authenticated member. Returns current revisions and server-computed allowed_actions. Omit work_item_id for the scoped list."
    )]
    async fn inspect(&self, Parameters(params): Parameters<InspectParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_inspect",
            &serde_json::json!({ "work_item_id": params.work_item_id }),
        )
        .await
    }

    #[tool(
        name = "team_delegate",
        description = "Create and queue a canonical WorkItem for a direct subordinate. The runtime durably notifies and wakes the assignee after the command commits. Actor identity comes from the authenticated member credential, never from this payload."
    )]
    async fn delegate(&self, Parameters(params): Parameters<DelegateParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_delegate",
            &serde_json::json!({
                "idempotency_key": params.idempotency_key,
                "parent_work_item_id": params.parent_work_item_id,
                "subject": params.subject,
                "description": params.description,
                "assignee_member_id": params.assignee_member_id,
                "delivery_requirement": params.delivery_requirement,
            }),
        )
        .await
    }

    #[tool(
        name = "team_progress",
        description = "Advance assigned canonical work with action start, block, or resume. Block requires concise context, which is committed atomically with the state change and controller notification. Start and resume must not include context. The authenticated member must be the WorkItem assignee."
    )]
    async fn progress(&self, Parameters(params): Parameters<ProgressParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_progress",
            &serde_json::json!({
                "idempotency_key": params.idempotency_key,
                "work_item_id": params.work_item_id,
                "expected_work_revision": params.expected_work_revision,
                "action": params.action,
                "context": params.context,
            }),
        )
        .await
    }

    #[tool(
        name = "team_submit",
        description = "Commit a submission and concise evidence atomically with the reviewer notification. Use kind=inline without a result body, or kind=git with the next unused content revision and immutable head commit. Inspect existing deliveries after changes are requested; Git assignment is resolved from the WorkItem."
    )]
    async fn submit(&self, Parameters(params): Parameters<SubmitParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_submit",
            &serde_json::json!({
                "idempotency_key": params.idempotency_key,
                "work_item_id": params.work_item_id,
                "expected_work_revision": params.expected_work_revision,
                "kind": params.kind,
                "evidence": params.evidence,
                "git": params.git.map(|git| serde_json::json!({
                    "content_revision": git.content_revision,
                    "head_commit": git.head_commit,
                })),
            }),
        )
        .await
    }

    #[tool(
        name = "team_review",
        description = "Review a submitted canonical WorkItem and accept it, request changes, or reject it. Request changes requires concise feedback, which is committed atomically with the assignee notification. Git acceptance atomically queues the integrator notification. The authenticated member must be its reviewer."
    )]
    async fn review(&self, Parameters(params): Parameters<ReviewParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_review",
            &serde_json::json!({
                "idempotency_key": params.idempotency_key,
                "work_item_id": params.work_item_id,
                "expected_work_revision": params.expected_work_revision,
                "expected_delivery_revision": params.expected_delivery_revision,
                "decision": params.decision,
                "feedback": params.feedback,
            }),
        )
        .await
    }

    #[tool(
        name = "team_integrate",
        description = "Integrate the exact accepted Git delivery for a canonical WorkItem. The authenticated member must be its bound integrator. This is the only supported way to change the integration target: never run raw git merge, cherry-pick, rebase, reset, update-ref, or force-move the target branch. Repository coordinates and merged evidence are derived and verified by the server."
    )]
    async fn integrate(&self, Parameters(params): Parameters<IntegrateParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_integrate",
            &serde_json::json!({
                "idempotency_key": params.idempotency_key,
                "work_item_id": params.work_item_id,
                "expected_work_revision": params.expected_work_revision,
                "expected_delivery_revision": params.expected_delivery_revision,
            }),
        )
        .await
    }

    #[tool(
        name = "team_cancel",
        description = "Cancel a canonical WorkItem controlled by the authenticated member."
    )]
    async fn cancel(&self, Parameters(params): Parameters<CancelParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_cancel",
            &serde_json::json!({
                "idempotency_key": params.idempotency_key,
                "work_item_id": params.work_item_id,
                "expected_work_revision": params.expected_work_revision,
                "expected_delivery_revision": params.expected_delivery_revision,
            }),
        )
        .await
    }

    #[tool(
        name = "team_spawn_agent",
        description = "Create a new teammate agent to join the team.\n\nUse this only when one of the following is true:\n- The user explicitly approved the proposed teammate lineup in a previous message\n- The user explicitly instructed you to create a specific teammate immediately\n\nBefore calling this tool in the normal planning flow:\n- Start with one short sentence explaining why additional teammates would help\n- Tell the user which teammate(s) you recommend\n- Present the proposal as a table with: name, responsibility, and recommended assistant\n- Include each teammate's responsibility and recommended assistant\n- Ask whether to create them as proposed or change any names, responsibilities, or assistant choices\n- In that approval question, remind the user that they can later ask you to replace or adjust any teammate if the lineup is not working well\n- Do NOT call this tool in that same turn; wait for explicit approval in a later user message\n\nWhen calling this tool, always provide assistant_id from the available assistants catalog.\nDo not provide a model. The new teammate uses the selected assistant's configured/default model; users can adjust models from the UI model selector.\n\nThe new agent will be created and added to the team. You can then assign tasks and send messages to it."
    )]
    async fn spawn_agent(&self, Parameters(params): Parameters<SpawnAgentParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_spawn_agent",
            &serde_json::json!({
                "name": params.name,
                "assistant_id": params.assistant_id,
            }),
        )
        .await
    }

    #[tool(
        name = "team_members",
        description = "List all team members with their roles and current status."
    )]
    async fn members(&self) -> CallToolResult {
        self.forward_to_tcp("team_members", &serde_json::json!({})).await
    }

    #[tool(name = "team_rename_agent", description = "Rename a team member. Lead only.")]
    async fn rename_agent(&self, Parameters(params): Parameters<RenameAgentParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_rename_agent",
            &serde_json::json!({ "slot_id": params.slot_id, "new_name": params.new_name }),
        )
        .await
    }

    #[tool(
        name = "team_shutdown_agent",
        description = "Initiate shutdown of a teammate. Lead only. Sends a shutdown_request to the target agent."
    )]
    async fn shutdown_agent(&self, Parameters(params): Parameters<ShutdownAgentParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_shutdown_agent",
            &serde_json::json!({ "slot_id": params.slot_id, "reason": params.reason }),
        )
        .await
    }

    #[tool(
        name = "team_list_assistants",
        description = "List the assistants available for team spawning. Returns the real assistant catalog with real assistant_id values, names, backends, descriptions, and skills.\n\nUse this before team_spawn_agent when you need the exact assistant_id for a teammate. Do NOT guess from backend names like claude/codex/gemini — only use assistant_id values returned here."
    )]
    async fn list_assistants(&self) -> CallToolResult {
        self.forward_to_tcp("team_list_assistants", &serde_json::json!({}))
            .await
    }

    #[tool(
        name = "team_describe_assistant",
        description = "Get detailed information about an assistant before spawning it as a teammate.\n\nReturns the assistant's full description, enabled skills, and example tasks so you can\njudge whether it fits the user's request. Use this when two or more assistants look\nrelevant from the one-line catalog in your system prompt.\n\nUse team_list_assistants to find candidate assistant_id values.\nAfter confirming a match, call team_spawn_agent with the same assistant_id."
    )]
    async fn describe_assistant(&self, Parameters(params): Parameters<DescribeAssistantParams>) -> CallToolResult {
        self.forward_to_tcp(
            "team_describe_assistant",
            &serde_json::json!({ "assistant_id": params.assistant_id, "locale": params.locale }),
        )
        .await
    }
}

#[rmcp::tool_handler(router = Self::tool_router())]
impl rmcp::ServerHandler for TeamStdioServer {
    async fn list_tools(
        &self,
        _request: Option<rmcp::model::PaginatedRequestParams>,
        _context: rmcp::service::RequestContext<rmcp::RoleServer>,
    ) -> Result<ListToolsResult, rmcp::ErrorData> {
        let tools = self
            .list_tools_from_tcp()
            .await
            .map_err(|_| rmcp::ErrorData::internal_error("failed to list local team tools", None))?;
        Ok(ListToolsResult::with_all_items(tools))
    }
}

// ---------------------------------------------------------------------------
// TCP forwarding
// ---------------------------------------------------------------------------

impl TeamStdioServer {
    /// One-shot TCP forward: connect → initialize (with auth) → tools/call → read response → close.
    async fn forward_to_tcp(&self, tool_name: &str, args: &serde_json::Value) -> CallToolResult {
        match self.do_forward(tool_name, args).await {
            Ok(result) => tool_success(result),
            Err(ToolForwardError::Boundary(err)) => {
                eprintln!("{}", err.stderr_line());
                tool_error(err.code(), tool_error_message(err.code()), None, None)
            }
            Err(ToolForwardError::Tool {
                code,
                message,
                upstream_code,
                domain_code,
            }) => tool_error(code, message, upstream_code, domain_code),
        }
    }

    async fn do_forward(&self, tool_name: &str, args: &serde_json::Value) -> Result<String, ToolForwardError> {
        let mut stream = TcpStream::connect((CONNECT_HOST, self.port))
            .await
            .map_err(|_| tcp_connect_error(self.port))?;
        stream.set_nodelay(true).ok();

        // initialize with auth
        let init_frame = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "auth_token": self.token,
                "slot_id": self.slot_id,
            }
        });
        let init_bytes = serde_json::to_vec(&init_frame).map_err(|_| json_serialize_error())?;
        write_frame(&mut stream, &init_bytes)
            .await
            .map_err(|_| tcp_write_error())?;
        let init_resp = read_frame(&mut stream).await.map_err(|_| tcp_read_error())?;
        drop(init_resp);

        // tools/call
        let call_frame = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": tool_name,
                "arguments": args,
            }
        });
        let call_bytes = serde_json::to_vec(&call_frame).map_err(|_| json_serialize_error())?;
        write_frame(&mut stream, &call_bytes)
            .await
            .map_err(|_| tcp_write_error())?;
        let resp_bytes = read_frame(&mut stream).await.map_err(|_| tcp_read_error())?;

        let text = String::from_utf8_lossy(&resp_bytes).into_owned();

        parse_tool_response(&text)
    }

    async fn list_tools_from_tcp(&self) -> Result<Vec<Tool>, ToolForwardError> {
        let mut stream = TcpStream::connect((CONNECT_HOST, self.port))
            .await
            .map_err(|_| tcp_connect_error(self.port))?;
        stream.set_nodelay(true).ok();

        let init_frame = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "auth_token": self.token,
                "slot_id": self.slot_id,
            }
        });
        let init_bytes = serde_json::to_vec(&init_frame).map_err(|_| json_serialize_error())?;
        write_frame(&mut stream, &init_bytes)
            .await
            .map_err(|_| tcp_write_error())?;
        let init_resp = read_frame(&mut stream).await.map_err(|_| tcp_read_error())?;
        let init_text = String::from_utf8_lossy(&init_resp).into_owned();
        parse_json_rpc_success(&init_text)?;

        let list_frame = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
        });
        let list_bytes = serde_json::to_vec(&list_frame).map_err(|_| json_serialize_error())?;
        write_frame(&mut stream, &list_bytes)
            .await
            .map_err(|_| tcp_write_error())?;
        let resp_bytes = read_frame(&mut stream).await.map_err(|_| tcp_read_error())?;
        let text = String::from_utf8_lossy(&resp_bytes).into_owned();
        parse_tools_list_response(&text)
    }
}

#[derive(Debug)]
enum ToolForwardError {
    Boundary(CliBoundaryError),
    Tool {
        code: CliBoundaryCode,
        message: &'static str,
        upstream_code: Option<serde_json::Value>,
        domain_code: Option<serde_json::Value>,
    },
}

impl From<CliBoundaryError> for ToolForwardError {
    fn from(error: CliBoundaryError) -> Self {
        Self::Boundary(error)
    }
}

fn parse_tool_response(text: &str) -> Result<String, ToolForwardError> {
    let value = serde_json::from_str::<serde_json::Value>(text).map_err(|_| tool_response_unexpected())?;
    if value.get("error").is_some() {
        return Err(remote_tool_error(
            extract_nested_code(&value, &["error", "code"]),
            extract_nested_code(&value, &["error", "data", "domainCode"])
                .or_else(|| extract_nested_code(&value, &["error", "data", "code"]))
                .or_else(|| extract_nested_code(&value, &["error", "data", "errorCode"])),
        ));
    }
    let result = value.get("result").ok_or_else(tool_response_unexpected)?;
    if let Some(result) = result.as_str() {
        return Ok(result.to_owned());
    }
    if result
        .get("isError")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return Err(remote_tool_error(
            extract_nested_code(result, &["structuredContent", "upstreamCode"])
                .or_else(|| extract_nested_code(result, &["upstreamCode"])),
            extract_nested_code(result, &["structuredContent", "domainCode"])
                .or_else(|| extract_nested_code(result, &["structuredContent", "code"]))
                .or_else(|| extract_nested_code(result, &["structuredContent", "errorCode"]))
                .or_else(|| extract_nested_code(result, &["domainCode"]))
                .or_else(|| extract_nested_code(result, &["code"]))
                .or_else(|| extract_nested_code(result, &["errorCode"])),
        ));
    }
    if let Some(content) = result.get("content").and_then(serde_json::Value::as_array) {
        let text_parts: Vec<&str> = content
            .iter()
            .filter_map(|item| item.get("text").and_then(serde_json::Value::as_str))
            .collect();
        if !text_parts.is_empty() {
            return Ok(text_parts.join("\n"));
        }
    }
    Err(tool_response_unexpected().into())
}

fn parse_json_rpc_success(text: &str) -> Result<serde_json::Value, ToolForwardError> {
    let value = serde_json::from_str::<serde_json::Value>(text).map_err(|_| tool_response_unexpected())?;
    if value.get("error").is_some() {
        return Err(remote_tool_error(
            extract_nested_code(&value, &["error", "code"]),
            extract_nested_code(&value, &["error", "data", "domainCode"])
                .or_else(|| extract_nested_code(&value, &["error", "data", "code"]))
                .or_else(|| extract_nested_code(&value, &["error", "data", "errorCode"])),
        ));
    }
    value
        .get("result")
        .cloned()
        .ok_or_else(tool_response_unexpected)
        .map_err(Into::into)
}

#[derive(Deserialize)]
struct RemoteToolDescriptor {
    name: String,
    #[serde(default)]
    description: String,
    #[serde(default, alias = "inputSchema")]
    input_schema: serde_json::Value,
}

fn parse_tools_list_response(text: &str) -> Result<Vec<Tool>, ToolForwardError> {
    let result = parse_json_rpc_success(text)?;
    let descriptors = serde_json::from_value::<Vec<RemoteToolDescriptor>>(
        result.get("tools").cloned().ok_or_else(tool_response_unexpected)?,
    )
    .map_err(|_| tool_response_unexpected())?;

    descriptors
        .into_iter()
        .map(|descriptor| {
            let schema = descriptor
                .input_schema
                .as_object()
                .cloned()
                .ok_or_else(tool_response_unexpected)?;
            Ok(Tool::new(descriptor.name, descriptor.description, schema))
        })
        .collect()
}

fn json_serialize_error() -> CliBoundaryError {
    CliBoundaryError::new(CliBoundaryCode::McpJsonSerializeFailed, SUBCOMMAND, ERR_JSON_SERIALIZE)
}

fn tcp_connect_error(port: u16) -> CliBoundaryError {
    CliBoundaryError::new(CliBoundaryCode::McpTcpConnectFailed, SUBCOMMAND, ERR_TCP_CONNECT)
        .with_field("host", CONNECT_HOST)
        .with_field("port", port.to_string())
}

fn tcp_write_error() -> CliBoundaryError {
    CliBoundaryError::new(CliBoundaryCode::McpTcpWriteFailed, SUBCOMMAND, ERR_TCP_WRITE)
}

fn tcp_read_error() -> CliBoundaryError {
    CliBoundaryError::new(CliBoundaryCode::McpTcpReadFailed, SUBCOMMAND, ERR_TCP_READ)
}

fn remote_tool_error(
    upstream_code: Option<serde_json::Value>,
    domain_code: Option<serde_json::Value>,
) -> ToolForwardError {
    ToolForwardError::Tool {
        code: CliBoundaryCode::McpToolRemoteError,
        message: ERR_TOOL_REMOTE,
        upstream_code,
        domain_code,
    }
}

fn tool_response_unexpected() -> CliBoundaryError {
    CliBoundaryError::new(
        CliBoundaryCode::McpToolResponseUnexpected,
        SUBCOMMAND,
        ERR_TOOL_RESPONSE_UNEXPECTED,
    )
}

fn tool_success(text: String) -> CallToolResult {
    CallToolResult::success(vec![Content::text(text)])
}

fn tool_error(
    code: CliBoundaryCode,
    message: &'static str,
    upstream_code: Option<serde_json::Value>,
    domain_code: Option<serde_json::Value>,
) -> CallToolResult {
    let mut structured = serde_json::json!({
        "code": code.as_str(),
        "message": message,
    });
    if let Some(upstream_code) = upstream_code {
        structured["upstreamCode"] = upstream_code;
    }
    if let Some(domain_code) = domain_code {
        structured["domainCode"] = domain_code;
    }

    let mut result = CallToolResult::error(vec![Content::text(message)]);
    result.structured_content = Some(structured);
    result
}

fn extract_nested_code(value: &serde_json::Value, path: &[&str]) -> Option<serde_json::Value> {
    let mut current = value;
    for key in path {
        current = current.get(*key)?;
    }
    match current {
        serde_json::Value::String(_) | serde_json::Value::Number(_) => Some(current.clone()),
        _ => None,
    }
}

fn tool_error_message(code: CliBoundaryCode) -> &'static str {
    match code {
        CliBoundaryCode::McpJsonSerializeFailed => ERR_JSON_SERIALIZE,
        CliBoundaryCode::McpTcpConnectFailed => ERR_TCP_CONNECT,
        CliBoundaryCode::McpTcpWriteFailed => ERR_TCP_WRITE,
        CliBoundaryCode::McpTcpReadFailed => ERR_TCP_READ,
        CliBoundaryCode::McpToolRemoteError => ERR_TOOL_REMOTE,
        CliBoundaryCode::McpToolResponseUnexpected => ERR_TOOL_RESPONSE_UNEXPECTED,
        _ => "team stdio tool forwarding failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::error::CliBoundaryCode;
    use serde_json::json;
    use tokio::net::TcpListener;

    fn first_text(result: &CallToolResult) -> &str {
        result.content[0].as_text().expect("text content").text.as_str()
    }

    #[test]
    fn team_stdio_env_rejects_invalid_port_with_stable_code() {
        let err = TeamStdioEnv::from_values("bad", "tok", "slot-a").unwrap_err();
        assert_eq!(err.code(), CliBoundaryCode::McpEnvInvalidPort);
        assert_eq!(err.exit_code(), std::process::ExitCode::from(2));
    }

    #[test]
    fn team_stdio_env_accepts_valid_values() {
        let env = TeamStdioEnv::from_values("12345", "tok", "slot-a").unwrap();
        assert_eq!(env.port, 12345);
        assert_eq!(env.token, "tok");
        assert_eq!(env.slot_id, "slot-a");
    }

    #[test]
    fn spawn_agent_params_reject_legacy_custom_agent_id_alias() {
        let parsed = serde_json::from_value::<SpawnAgentParams>(json!({
            "name": "helper",
            "custom_agent_id": "assistant-123",
        }));
        assert!(parsed.is_err(), "legacy custom_agent_id alias should be rejected");
        let err = parsed.err().unwrap();

        assert!(err.to_string().contains("unknown field"));
        assert!(err.to_string().contains("custom_agent_id"));
    }

    #[test]
    fn describe_assistant_params_reject_legacy_custom_agent_id_alias() {
        let parsed = serde_json::from_value::<DescribeAssistantParams>(json!({
            "custom_agent_id": "assistant-123",
        }));
        assert!(parsed.is_err(), "legacy custom_agent_id alias should be rejected");
        let err = parsed.err().unwrap();

        assert!(err.to_string().contains("unknown field"));
        assert!(err.to_string().contains("custom_agent_id"));
    }

    #[test]
    fn send_message_params_accept_optional_idempotency_key() {
        let keyed = serde_json::from_value::<SendMessageParams>(json!({
            "to": "worker-1",
            "message": "retryable",
            "idempotency_key": "stdio-call-1"
        }))
        .unwrap();
        assert_eq!(keyed.idempotency_key.as_deref(), Some("stdio-call-1"));

        let unkeyed = serde_json::from_value::<SendMessageParams>(json!({
            "to": "worker-1",
            "message": "legacy"
        }))
        .unwrap();
        assert_eq!(unkeyed.idempotency_key, None);
    }

    #[test]
    fn integrate_params_reject_server_owned_git_evidence() {
        let parsed = serde_json::from_value::<IntegrateParams>(json!({
            "idempotency_key": "integrate-1",
            "work_item_id": "work-1",
            "expected_work_revision": 5,
            "expected_delivery_revision": 1,
            "merged_commit": "self-reported"
        }));
        assert!(parsed.is_err());
        let error = match parsed {
            Ok(_) => panic!("server-owned Git integration evidence must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn git_submit_params_reject_server_owned_assignment_coordinates() {
        let parsed = serde_json::from_value::<SubmitParams>(json!({
            "idempotency_key": "submit-1",
            "work_item_id": "work-1",
            "expected_work_revision": 2,
            "kind": "git",
            "evidence": "Ready for review",
            "git": {
                "content_revision": 1,
                "head_commit": "0123456789abcdef0123456789abcdef01234567",
                "branch_ref": "refs/heads/forged"
            }
        }));
        assert!(parsed.is_err());
        let error = match parsed {
            Ok(_) => panic!("server-owned Git assignment coordinates must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn team_stdio_send_message_schema_exposes_optional_idempotency_key() {
        let router = TeamStdioServer::tool_router();
        let tool = router
            .list_all()
            .into_iter()
            .find(|tool| tool.name == "team_send_message")
            .expect("team_send_message tool missing");
        let properties = tool.input_schema["properties"].as_object().unwrap();
        assert!(properties.contains_key("idempotency_key"));
        let required = tool.input_schema["required"].as_array().unwrap();
        assert!(!required.contains(&json!("idempotency_key")));
    }

    #[test]
    fn team_stdio_router_exposes_team_list_assistants() {
        let router = TeamStdioServer::tool_router();
        let tools = router.list_all();
        let team_list_assistants = tools
            .iter()
            .find(|tool| tool.name == "team_list_assistants")
            .expect("team_list_assistants tool missing");
        let properties = team_list_assistants.input_schema["properties"].as_object().unwrap();
        assert!(
            properties.is_empty(),
            "team_list_assistants should not accept arguments"
        );
    }

    #[test]
    fn team_stdio_descriptions_match_prompt_registry() {
        let router = TeamStdioServer::tool_router();
        let tools = router.list_all();
        let mut actual_names: Vec<_> = tools.iter().map(|tool| tool.name.as_ref()).collect();
        actual_names.sort_unstable();
        let mut expected_names: Vec<_> = aionui_team_prompts::tools::team_tool_specs()
            .iter()
            .map(|spec| spec.name)
            .collect();
        expected_names.sort_unstable();
        assert_eq!(actual_names, expected_names, "stdio tool registry drift");

        for spec in aionui_team_prompts::tools::team_tool_specs() {
            let tool = tools
                .iter()
                .find(|tool| tool.name == spec.name)
                .unwrap_or_else(|| panic!("missing tool {}", spec.name));
            let description = tool
                .description
                .as_ref()
                .unwrap_or_else(|| panic!("missing description for {}", spec.name));
            assert_eq!(
                description.as_ref(),
                spec.description,
                "description drift for {}",
                spec.name
            );
        }
    }

    #[tokio::test]
    async fn list_tools_uses_team_server_filtered_descriptors() {
        let listener = TcpListener::bind((CONNECT_HOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let init = read_frame(&mut socket).await.unwrap();
            let init_value: serde_json::Value = serde_json::from_slice(&init).unwrap();
            assert_eq!(init_value["method"], "initialize");
            assert_eq!(init_value["params"]["slot_id"], "worker-1");

            let init_response = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {}
            }))
            .unwrap();
            write_frame(&mut socket, &init_response).await.unwrap();

            let list = read_frame(&mut socket).await.unwrap();
            let list_value: serde_json::Value = serde_json::from_slice(&list).unwrap();
            assert_eq!(list_value["method"], "tools/list");

            let list_response = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": {
                    "tools": [
                        {
                            "name": "team_send_message",
                            "description": "Send a message",
                            "input_schema": {
                                "type": "object",
                                "properties": {
                                    "to": { "type": "string" },
                                    "message": { "type": "string" }
                                },
                                "required": ["to", "message"]
                            }
                        }
                    ]
                }
            }))
            .unwrap();
            write_frame(&mut socket, &list_response).await.unwrap();
        });
        let server = TeamStdioServer {
            port,
            token: "dummy-token".into(),
            slot_id: "worker-1".into(),
        };

        let tools = server.list_tools_from_tcp().await.expect("tools/list");

        accept_task.await.unwrap();
        let names: Vec<_> = tools.iter().map(|tool| tool.name.as_ref()).collect();
        assert_eq!(names, vec!["team_send_message"]);
        assert!(!names.contains(&"team_spawn_agent"));
        assert!(!names.contains(&"team_rename_agent"));
        assert!(!names.contains(&"team_shutdown_agent"));
        assert_eq!(
            tools[0]
                .input_schema
                .get("properties")
                .and_then(|value| value.as_object())
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn forward_to_tcp_reports_read_failure_after_accept_close() {
        let listener = TcpListener::bind((CONNECT_HOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_task = tokio::spawn(async move {
            let _ = listener.accept().await;
        });
        let server = TeamStdioServer {
            port,
            token: "dummy-token".into(),
            slot_id: "dummy-slot".into(),
        };

        let result = server.forward_to_tcp("team_inspect", &json!({})).await;

        accept_task.await.unwrap();
        assert_eq!(result.is_error, Some(true));
        assert_eq!(first_text(&result), "failed to read MCP frame from TCP listener");
        assert_eq!(
            result.structured_content.as_ref().unwrap()["code"],
            "MCP_TCP_READ_FAILED"
        );
    }

    #[tokio::test]
    async fn forward_to_tcp_sanitizes_tool_level_error_result() {
        let listener = TcpListener::bind((CONNECT_HOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _init = read_frame(&mut socket).await.unwrap();
            let init_response = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {}
            }))
            .unwrap();
            write_frame(&mut socket, &init_response).await.unwrap();

            let _call = read_frame(&mut socket).await.unwrap();
            let tool_response = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": "upstream failure for conv-secret-123"
                        }
                    ],
                    "isError": true
                }
            }))
            .unwrap();
            write_frame(&mut socket, &tool_response).await.unwrap();
        });
        let server = TeamStdioServer {
            port,
            token: "dummy-token".into(),
            slot_id: "dummy-slot".into(),
        };

        let result = server.forward_to_tcp("team_inspect", &json!({})).await;

        accept_task.await.unwrap();
        assert_eq!(result.is_error, Some(true));
        assert_eq!(first_text(&result), "local team tool returned an error");
        assert_eq!(
            result.structured_content.as_ref().unwrap()["code"],
            "MCP_TOOL_REMOTE_ERROR"
        );
        let serialized = serde_json::to_string(&result).unwrap();
        assert!(!serialized.contains("conv-secret-123"));
    }

    #[tokio::test]
    async fn spawn_agent_forwards_only_assistant_first_arguments() {
        let listener = TcpListener::bind((CONNECT_HOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _init = read_frame(&mut socket).await.unwrap();
            let init_response = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {}
            }))
            .unwrap();
            write_frame(&mut socket, &init_response).await.unwrap();

            let call = read_frame(&mut socket).await.unwrap();
            let call_value: serde_json::Value = serde_json::from_slice(&call).unwrap();
            assert_eq!(call_value["params"]["name"], json!("team_spawn_agent"));
            let arguments = &call_value["params"]["arguments"];
            assert_eq!(arguments["name"], json!("CodexCLI"));
            assert_eq!(arguments["assistant_id"], json!("bare:8e1acf31"));
            assert!(arguments.get("model").is_none());
            assert!(arguments.get("role").is_none());
            assert!(arguments.get("agent_type").is_none());
            assert!(arguments.get("backend").is_none());

            let tool_response = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": {
                    "content": [{ "type": "text", "text": "ok" }],
                    "isError": false
                }
            }))
            .unwrap();
            write_frame(&mut socket, &tool_response).await.unwrap();
        });
        let server = TeamStdioServer {
            port,
            token: "dummy-token".into(),
            slot_id: "dummy-slot".into(),
        };

        let result = server
            .spawn_agent(Parameters(SpawnAgentParams {
                name: "CodexCLI".into(),
                assistant_id: Some("bare:8e1acf31".into()),
            }))
            .await;

        accept_task.await.unwrap();
        assert_eq!(result.is_error, Some(false));
        assert_eq!(first_text(&result), "ok");
    }

    #[tokio::test]
    async fn send_message_forwards_optional_idempotency_key() {
        let listener = TcpListener::bind((CONNECT_HOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _init = read_frame(&mut socket).await.unwrap();
            let init_response = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {}
            }))
            .unwrap();
            write_frame(&mut socket, &init_response).await.unwrap();

            let call = read_frame(&mut socket).await.unwrap();
            let call_value: serde_json::Value = serde_json::from_slice(&call).unwrap();
            assert_eq!(call_value["params"]["name"], json!("team_send_message"));
            let arguments = &call_value["params"]["arguments"];
            assert_eq!(arguments["to"], json!("worker-1"));
            assert_eq!(arguments["message"], json!("retryable"));
            assert_eq!(arguments["idempotency_key"], json!("stdio-call-1"));

            let tool_response = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": {
                    "content": [{ "type": "text", "text": "ok" }],
                    "isError": false
                }
            }))
            .unwrap();
            write_frame(&mut socket, &tool_response).await.unwrap();
        });
        let server = TeamStdioServer {
            port,
            token: "dummy-token".into(),
            slot_id: "lead-1".into(),
        };

        let result = server
            .send_message(Parameters(SendMessageParams {
                to: "worker-1".into(),
                message: "retryable".into(),
                files: Vec::new(),
                idempotency_key: Some("stdio-call-1".into()),
            }))
            .await;

        accept_task.await.unwrap();
        assert_eq!(result.is_error, Some(false));
        assert_eq!(first_text(&result), "ok");
    }

    #[tokio::test]
    async fn integrate_forwards_only_canonical_identity_and_revisions() {
        let listener = TcpListener::bind((CONNECT_HOST, 0)).await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _init = read_frame(&mut socket).await.unwrap();
            let init_response = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "result": {}
            }))
            .unwrap();
            write_frame(&mut socket, &init_response).await.unwrap();

            let call = read_frame(&mut socket).await.unwrap();
            let call_value: serde_json::Value = serde_json::from_slice(&call).unwrap();
            assert_eq!(call_value["params"]["name"], json!("team_integrate"));
            assert_eq!(
                call_value["params"]["arguments"],
                json!({
                    "idempotency_key": "integrate-stdio-1",
                    "work_item_id": "work-1",
                    "expected_work_revision": 5,
                    "expected_delivery_revision": 1
                })
            );

            let tool_response = serde_json::to_vec(&json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": {
                    "content": [{ "type": "text", "text": "merged" }],
                    "isError": false
                }
            }))
            .unwrap();
            write_frame(&mut socket, &tool_response).await.unwrap();
        });
        let server = TeamStdioServer {
            port,
            token: "dummy-token".into(),
            slot_id: "lead-1".into(),
        };

        let result = server
            .integrate(Parameters(IntegrateParams {
                idempotency_key: "integrate-stdio-1".into(),
                work_item_id: "work-1".into(),
                expected_work_revision: 5,
                expected_delivery_revision: 1,
            }))
            .await;

        accept_task.await.unwrap();
        assert_eq!(result.is_error, Some(false));
        assert_eq!(first_text(&result), "merged");
    }

    #[test]
    fn parse_tool_response_extracts_content_text() {
        let result = parse_tool_response(
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "result": {
                    "content": [
                        { "type": "text", "text": "first line" },
                        { "type": "text", "text": "second line" }
                    ],
                    "isError": false
                }
            })
            .to_string(),
        )
        .unwrap();

        assert_eq!(result, "first line\nsecond line");
    }

    #[test]
    fn parse_tool_response_sanitizes_top_level_error() {
        let err = parse_tool_response(
            &json!({
                "jsonrpc": "2.0",
                "id": 2,
                "error": {
                    "code": -32000,
                    "message": "remote failure for conv-secret-123"
                }
            })
            .to_string(),
        )
        .unwrap_err();

        let ToolForwardError::Tool {
            code, upstream_code, ..
        } = err
        else {
            panic!("expected tool error");
        };
        assert_eq!(code, CliBoundaryCode::McpToolRemoteError);
        assert_eq!(upstream_code, Some(json!(-32000)));
    }
}
