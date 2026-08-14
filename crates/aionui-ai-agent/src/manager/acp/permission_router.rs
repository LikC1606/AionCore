use crate::agent_runtime::AgentRuntime;
use crate::error::AgentError;
use crate::protocol::acp::{PermissionDecision, PermissionRequest};
use crate::protocol::events::{AgentStreamEvent, permission_request_to_event_data};
use agent_client_protocol::schema::{PermissionOptionKind as SdkPermissionOptionKind, ToolKind as SdkToolKind};
use aionui_api_types::TEAM_MCP_SERVER_NAME;
use aionui_common::Confirmation;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::{debug, info};

const BENCHMARK_CONTAINER_MCP_SERVER_NAME: &str = "deepscientist-benchmark-container";
const BENCHMARK_CONTAINER_MCP_TOOLS: &[&str] = &[
    "benchmark_exec",
    "benchmark_read",
    "benchmark_write",
    "benchmark_list",
    "benchmark_stat",
];
const AUTO_APPROVE_MCP_SERVERS: &[&str] = &[TEAM_MCP_SERVER_NAME];

struct PendingPermission {
    responder: oneshot::Sender<PermissionDecision>,
    confirmation: Confirmation,
}

/// Routes ACP permission requests from the protocol layer to the user
/// (via `event_tx`) and back (via `confirm`). Owns the receiver channel
/// for incoming permission requests, the pending responder map, and the
/// `closing` flag that prevents new requests from being routed after a
/// graceful shutdown has started.
pub struct PermissionRouter {
    /// Receiver for permission requests from the protocol layer.
    permission_rx: Mutex<mpsc::Receiver<PermissionRequest>>,
    /// Pending ACP permission responders and recovery data keyed by tool call ID.
    pending_permissions: StdMutex<HashMap<String, PendingPermission>>,
    /// Whether a graceful shutdown is in progress.
    closing: AtomicBool,
    benchmark_container_isolation: bool,
}

impl PermissionRouter {
    /// Create a new permission router.
    pub fn new(permission_rx: mpsc::Receiver<PermissionRequest>) -> Self {
        Self::with_benchmark_container_isolation(permission_rx, false)
    }

    /// Construct a router with the operator-owned benchmark MCP auto-approval
    /// gate. The flag is supplied from the immutable ACP build extra, never
    /// from model output or prompt content.
    pub fn with_benchmark_container_isolation(
        permission_rx: mpsc::Receiver<PermissionRequest>,
        benchmark_container_isolation: bool,
    ) -> Self {
        Self {
            permission_rx: Mutex::new(permission_rx),
            pending_permissions: StdMutex::new(HashMap::new()),
            closing: AtomicBool::new(false),
            benchmark_container_isolation,
        }
    }

    /// Start the permission handler loop.
    ///
    /// This background task receives permission requests from the protocol
    /// layer, converts them to `Permission` events, and waits for user
    /// responses routed through the `confirm()` method.
    ///
    /// `runtime` is shared with the parent manager so permission
    /// arrivals count as activity (preventing idle timeouts) via
    /// `runtime.bump_activity()`.
    pub fn start(self: &Arc<Self>, runtime: AgentRuntime) {
        let this = Arc::clone(self);

        tokio::spawn(async move {
            let mut rx = this.permission_rx.lock().await;

            while let Some(perm_req) = rx.recv().await {
                runtime.bump_activity();

                let call_id = perm_req.request.tool_call.tool_call_id.to_string();

                // Auto-approve team MCP tools without user interaction.
                if let Some(option_id) =
                    auto_approve_option_id_with_benchmark(&perm_req.request, this.benchmark_container_isolation)
                {
                    info!(
                        conversation_id = %runtime.conversation_id(),
                        call_id,
                        option_id = %option_id,
                        server_name = ?extract_mcp_server_name(&perm_req.request),
                        "ACP team MCP permission auto-approved"
                    );
                    let _ = perm_req.response_tx.send(PermissionDecision::Selected { option_id });
                    continue;
                }

                let permission_event = permission_request_to_event_data(&perm_req.request);
                let confirmation = permission_event
                    .as_confirmation()
                    .expect("ACP permission events must be recoverable as confirmations");

                let mut pending = this.pending_permissions.lock().unwrap();
                if let Some(previous) = pending.insert(
                    call_id.clone(),
                    PendingPermission {
                        responder: perm_req.response_tx,
                        confirmation,
                    },
                ) {
                    let _ = previous.responder.send(PermissionDecision::Cancelled);
                }
                drop(pending);
                debug!(
                    conversation_id = %runtime.conversation_id(),
                    call_id,
                    "ACP permission pending confirmation registered"
                );

                if runtime
                    .event_sender()
                    .send(AgentStreamEvent::AcpPermission(permission_event))
                    .is_err()
                    && let Some(pending) = this.pending_permissions.lock().unwrap().remove(&call_id)
                {
                    let _ = pending.responder.send(PermissionDecision::Cancelled);
                }
            }
        });
    }

    /// Pending permission items recoverable by conversation confirmation APIs.
    pub fn get_confirmations(&self) -> Vec<Confirmation> {
        self.pending_permissions
            .lock()
            .unwrap()
            .values()
            .map(|pending| pending.confirmation.clone())
            .collect()
    }

    /// Resolve a pending permission request with the user's selected option.
    pub fn confirm(&self, call_id: &str, option_id: String, conversation_id: &str) -> Result<(), AgentError> {
        let pending = self
            .pending_permissions
            .lock()
            .unwrap()
            .remove(call_id)
            .ok_or_else(|| AgentError::bad_request(format!("Pending ACP permission not found: {call_id}")))?;

        pending
            .responder
            .send(PermissionDecision::Selected { option_id })
            .map_err(|_| AgentError::bad_request(format!("Pending ACP permission expired: {call_id}")))?;

        debug!(conversation_id = %conversation_id, call_id, "ACP permission response forwarded");
        Ok(())
    }

    /// Cancel all pending permission requests. Called during `stop()` and `kill()`.
    pub fn cancel_all(&self) {
        for (_, pending) in self.pending_permissions.lock().unwrap().drain() {
            let _ = pending.responder.send(PermissionDecision::Cancelled);
        }
    }

    /// Whether a graceful shutdown is in progress.
    pub fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// Mark the router as closing (graceful shutdown in progress).
    pub fn set_closing(&self) {
        self.closing.store(true, Ordering::Release);
    }

    #[cfg(test)]
    fn insert_pending_for_test(
        &self,
        call_id: String,
        responder: oneshot::Sender<PermissionDecision>,
        confirmation: Confirmation,
    ) {
        self.pending_permissions.lock().unwrap().insert(
            call_id,
            PendingPermission {
                responder,
                confirmation,
            },
        );
    }
}

#[cfg(test)]
fn is_auto_approve_tool(request: &agent_client_protocol::schema::RequestPermissionRequest) -> bool {
    auto_approve_option_id(request).is_some()
}

#[cfg(test)]
fn auto_approve_option_id(request: &agent_client_protocol::schema::RequestPermissionRequest) -> Option<String> {
    auto_approve_option_id_with_benchmark(request, false)
}

fn auto_approve_option_id_with_benchmark(
    request: &agent_client_protocol::schema::RequestPermissionRequest,
    benchmark_container_isolation: bool,
) -> Option<String> {
    let team_server = extract_mcp_server_name(request)
        .is_some_and(|server_name| AUTO_APPROVE_MCP_SERVERS.contains(&server_name.as_str()));
    let benchmark_server = benchmark_container_isolation
        && (is_exact_benchmark_mcp_request(request) || is_correlated_benchmark_mcp_approval(request));
    if !team_server && !benchmark_server {
        return None;
    }
    select_allow_option_id(request)
}

fn is_correlated_benchmark_mcp_approval(request: &agent_client_protocol::schema::RequestPermissionRequest) -> bool {
    let fields = &request.tool_call.fields;

    // codex-acp 1.1.2 omits title/rawInput from a permission request when it
    // correlated the approval with an earlier MCP tool-call update. This path
    // is enabled only by the operator-owned benchmark isolation flag, whose
    // runtime contract exposes exactly the five benchmark MCP tools.
    fields.title.is_none()
        && fields.raw_input.is_none()
        && matches!(fields.kind, Some(SdkToolKind::Execute))
        && !request.tool_call.tool_call_id.to_string().starts_with("elicitation-")
        && request
            .meta
            .as_ref()
            .and_then(|meta| meta.get("is_mcp_tool_approval"))
            .and_then(serde_json::Value::as_bool)
            == Some(true)
}

fn is_exact_benchmark_mcp_request(request: &agent_client_protocol::schema::RequestPermissionRequest) -> bool {
    let fields = &request.tool_call.fields;
    let Some(title) = fields.title.as_deref() else {
        return false;
    };
    let Some(tool) = benchmark_tool_from_dotted_title(title) else {
        return false;
    };

    // Codex ACP permission requests may omit raw_input even though the
    // preceding tool-call update included it. When raw_input is present,
    // require every available identity field to agree with the exact title.
    let Some(raw_input) = fields.raw_input.as_ref() else {
        return true;
    };
    let server_matches = raw_input
        .get("server")
        .or_else(|| raw_input.get("server_name"))
        .and_then(serde_json::Value::as_str)
        .is_none_or(|server| server == BENCHMARK_CONTAINER_MCP_SERVER_NAME);
    let tool_matches = raw_input
        .get("tool")
        .and_then(serde_json::Value::as_str)
        .is_none_or(|raw_tool| raw_tool == tool);
    server_matches && tool_matches
}

fn benchmark_tool_from_dotted_title(title: &str) -> Option<&str> {
    let prefix = format!("mcp.{BENCHMARK_CONTAINER_MCP_SERVER_NAME}.");
    let tool = title.strip_prefix(&prefix)?;
    BENCHMARK_CONTAINER_MCP_TOOLS.contains(&tool).then_some(tool)
}

fn select_allow_option_id(request: &agent_client_protocol::schema::RequestPermissionRequest) -> Option<String> {
    request
        .options
        .iter()
        .find(|option| matches!(option.kind, SdkPermissionOptionKind::AllowAlways))
        .or_else(|| {
            request
                .options
                .iter()
                .find(|option| matches!(option.kind, SdkPermissionOptionKind::AllowOnce))
        })
        .map(|option| option.option_id.to_string())
}

fn extract_mcp_server_name(request: &agent_client_protocol::schema::RequestPermissionRequest) -> Option<String> {
    extract_mcp_server_from_raw_input(request).or_else(|| {
        request
            .tool_call
            .fields
            .title
            .as_deref()
            .and_then(extract_mcp_server_from_prefixed_title)
            .map(str::to_owned)
    })
}

fn extract_mcp_server_from_raw_input(
    request: &agent_client_protocol::schema::RequestPermissionRequest,
) -> Option<String> {
    let fields = &request.tool_call.fields;
    let raw_input = fields.raw_input.as_ref()?;
    if let Some(server_name) = raw_input
        .get("server_name")
        .and_then(serde_json::Value::as_str)
        .filter(|server_name| !server_name.is_empty())
    {
        return Some(server_name.to_owned());
    }

    // Codex ACP 0.146 emits the MCP identity as `server` + `tool` and
    // renders the corresponding title as `mcp.<server>.<tool>`. Require all
    // three values to agree so a native tool cannot gain approval merely by
    // supplying an unrelated `server` argument.
    let server = raw_input
        .get("server")
        .and_then(serde_json::Value::as_str)
        .filter(|server| !server.is_empty())?;
    let tool = raw_input
        .get("tool")
        .and_then(serde_json::Value::as_str)
        .filter(|tool| !tool.is_empty())?;
    let title = fields.title.as_deref()?;
    (title == format!("mcp.{server}.{tool}")).then(|| server.to_owned())
}

fn extract_mcp_server_from_prefixed_title(title: &str) -> Option<&str> {
    let rest = title.strip_prefix("mcp__")?;
    let (server_name, tool_name) = rest.split_once("__")?;
    if server_name.is_empty() || tool_name.is_empty() {
        return None;
    }
    Some(server_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::events::AgentStreamEvent;
    use agent_client_protocol::schema::{
        PermissionOption, PermissionOptionKind as SdkPermissionOptionKind, RequestPermissionRequest,
        ToolCallUpdate as SdkToolCallUpdate, ToolCallUpdateFields, ToolKind as SdkToolKind,
    };
    use aionui_common::Confirmation;
    use serde_json::json;
    use std::time::Duration;

    fn permission_request_with_title_and_raw_input(
        title: &str,
        raw_input: Option<serde_json::Value>,
        options: Vec<PermissionOption>,
    ) -> RequestPermissionRequest {
        RequestPermissionRequest::new(
            "session-1",
            SdkToolCallUpdate::new(
                "tool-1",
                ToolCallUpdateFields::new()
                    .kind(SdkToolKind::Other)
                    .title(title.to_owned())
                    .raw_input(raw_input),
            ),
            options,
        )
    }

    fn correlated_mcp_permission_request(
        call_id: &str,
        mcp_approval: bool,
        options: Vec<PermissionOption>,
    ) -> RequestPermissionRequest {
        let mut request = RequestPermissionRequest::new(
            "session-1",
            SdkToolCallUpdate::new(
                call_id.to_owned(),
                ToolCallUpdateFields::new().kind(SdkToolKind::Execute),
            ),
            options,
        );
        request.meta = Some(serde_json::from_value(json!({ "is_mcp_tool_approval": mcp_approval })).unwrap());
        request
    }

    fn allow_always_option(option_id: &'static str) -> PermissionOption {
        PermissionOption::new(
            option_id,
            "Allow for this session",
            SdkPermissionOptionKind::AllowAlways,
        )
    }

    fn allow_once_option(option_id: &'static str) -> PermissionOption {
        PermissionOption::new(option_id, "Allow", SdkPermissionOptionKind::AllowOnce)
    }

    fn reject_option(option_id: &'static str) -> PermissionOption {
        PermissionOption::new(option_id, "Reject", SdkPermissionOptionKind::RejectOnce)
    }

    fn sample_confirmation(call_id: &str) -> Confirmation {
        Confirmation {
            id: call_id.to_owned(),
            call_id: call_id.to_owned(),
            title: Some("Write file".to_owned()),
            action: None,
            description: "Write /tmp/current_time.txt".to_owned(),
            command_type: Some("edit".to_owned()),
            options: vec![aionui_common::ConfirmationOption {
                label: "Allow".to_owned(),
                value: json!("allow_once"),
                params: None,
            }],
        }
    }

    #[test]
    fn get_confirmations_returns_pending_acp_permission() {
        let (_tx, rx) = mpsc::channel(1);
        let router = PermissionRouter::new(rx);
        let (response_tx, _response_rx) = oneshot::channel();

        router.insert_pending_for_test("tool-1".to_owned(), response_tx, sample_confirmation("tool-1"));

        let confirmations = router.get_confirmations();
        assert_eq!(confirmations.len(), 1);
        assert_eq!(confirmations[0].id, "tool-1");
        assert_eq!(confirmations[0].call_id, "tool-1");
        assert_eq!(confirmations[0].description, "Write /tmp/current_time.txt");
    }

    #[test]
    fn confirm_removes_pending_confirmation_and_forwards_option() {
        let (_tx, rx) = mpsc::channel(1);
        let router = PermissionRouter::new(rx);
        let (response_tx, mut response_rx) = oneshot::channel();
        router.insert_pending_for_test("tool-1".to_owned(), response_tx, sample_confirmation("tool-1"));

        router
            .confirm("tool-1", "allow_once".to_owned(), "conv-1")
            .expect("confirm should succeed");

        assert!(router.get_confirmations().is_empty());
        assert!(matches!(
            response_rx.try_recv(),
            Ok(PermissionDecision::Selected { option_id }) if option_id == "allow_once"
        ));
    }

    #[test]
    fn auto_approve_matches_claude_team_mcp_title_prefix() {
        let request = permission_request_with_title_and_raw_input(
            "mcp__aionui-team__team_members",
            None,
            vec![allow_always_option("allow_always"), reject_option("reject")],
        );

        assert!(is_auto_approve_tool(&request));
    }

    #[test]
    fn auto_approve_matches_codex_raw_input_server_name() {
        let request = permission_request_with_title_and_raw_input(
            "Approve MCP tool call",
            Some(json!({
                "server_name": "aionui-team",
                "request": {
                    "_meta": {
                        "codex_approval_kind": "mcp_tool_call"
                    }
                }
            })),
            vec![
                allow_once_option("approved"),
                allow_always_option("approved-for-session"),
                allow_always_option("approved-always"),
                reject_option("cancel"),
            ],
        );

        assert!(is_auto_approve_tool(&request));
    }

    #[test]
    fn auto_approve_rejects_non_team_mcp_server() {
        let request = permission_request_with_title_and_raw_input(
            "Approve MCP tool call",
            Some(json!({ "server_name": "aionui-image-generation" })),
            vec![allow_always_option("approved-for-session"), reject_option("cancel")],
        );

        assert!(!is_auto_approve_tool(&request));
    }

    #[test]
    fn benchmark_isolation_auto_approves_only_the_reserved_server() {
        let request = permission_request_with_title_and_raw_input(
            "mcp.deepscientist-benchmark-container.benchmark_list",
            Some(json!({ "server_name": "deepscientist-benchmark-container" })),
            vec![allow_always_option("benchmark-allow"), reject_option("cancel")],
        );

        assert_eq!(
            auto_approve_option_id_with_benchmark(&request, true).as_deref(),
            Some("benchmark-allow")
        );
        assert_eq!(auto_approve_option_id_with_benchmark(&request, false), None);
    }

    #[test]
    fn benchmark_isolation_accepts_the_codex_0_146_mcp_wire_shape() {
        let request = permission_request_with_title_and_raw_input(
            "mcp.deepscientist-benchmark-container.benchmark_list",
            Some(json!({
                "arguments": { "path": "" },
                "server": "deepscientist-benchmark-container",
                "tool": "benchmark_list"
            })),
            vec![allow_always_option("benchmark-allow"), reject_option("cancel")],
        );

        assert_eq!(
            auto_approve_option_id_with_benchmark(&request, true).as_deref(),
            Some("benchmark-allow")
        );
    }

    #[test]
    fn benchmark_isolation_accepts_permission_request_without_raw_input() {
        let request = permission_request_with_title_and_raw_input(
            "mcp.deepscientist-benchmark-container.benchmark_list",
            None,
            vec![allow_always_option("benchmark-allow"), reject_option("cancel")],
        );

        assert_eq!(
            auto_approve_option_id_with_benchmark(&request, true).as_deref(),
            Some("benchmark-allow")
        );
        assert_eq!(auto_approve_option_id_with_benchmark(&request, false), None);
    }

    #[test]
    fn benchmark_isolation_accepts_correlated_codex_acp_mcp_approval() {
        let request = correlated_mcp_permission_request(
            "call_00_exact",
            true,
            vec![allow_once_option("allow"), reject_option("cancel")],
        );

        assert_eq!(
            auto_approve_option_id_with_benchmark(&request, true).as_deref(),
            Some("allow")
        );
        assert_eq!(auto_approve_option_id_with_benchmark(&request, false), None);
    }

    #[test]
    fn benchmark_isolation_rejects_unmarked_correlated_permission() {
        let request = correlated_mcp_permission_request(
            "call_00_exact",
            false,
            vec![allow_once_option("allow"), reject_option("cancel")],
        );

        assert_eq!(auto_approve_option_id_with_benchmark(&request, true), None);
    }

    #[test]
    fn benchmark_isolation_rejects_uncorrelated_elicitation_permission() {
        let request = correlated_mcp_permission_request(
            "elicitation-deepscientist-benchmark-container",
            true,
            vec![allow_once_option("allow"), reject_option("cancel")],
        );

        assert_eq!(auto_approve_option_id_with_benchmark(&request, true), None);
    }

    #[test]
    fn benchmark_isolation_rejects_unknown_dotted_benchmark_tool() {
        let request = permission_request_with_title_and_raw_input(
            "mcp.deepscientist-benchmark-container.shell",
            None,
            vec![allow_always_option("allow"), reject_option("cancel")],
        );

        assert_eq!(auto_approve_option_id_with_benchmark(&request, true), None);
    }

    #[test]
    fn benchmark_isolation_rejects_raw_identity_that_disagrees_with_title() {
        let request = permission_request_with_title_and_raw_input(
            "mcp.deepscientist-benchmark-container.benchmark_list",
            Some(json!({
                "server": "deepscientist-benchmark-container",
                "tool": "benchmark_exec"
            })),
            vec![allow_always_option("allow"), reject_option("cancel")],
        );

        assert_eq!(auto_approve_option_id_with_benchmark(&request, true), None);
    }

    #[test]
    fn benchmark_isolation_rejects_a_mismatched_codex_mcp_title() {
        let request = permission_request_with_title_and_raw_input(
            "shell",
            Some(json!({
                "server": "deepscientist-benchmark-container",
                "tool": "benchmark_list"
            })),
            vec![allow_always_option("allow"), reject_option("cancel")],
        );

        assert_eq!(auto_approve_option_id_with_benchmark(&request, true), None);
    }

    #[test]
    fn benchmark_isolation_does_not_auto_approve_a_lookalike_server() {
        let request = permission_request_with_title_and_raw_input(
            "Approve MCP tool call",
            Some(json!({ "server_name": "deepscientist-benchmark-container-evil" })),
            vec![allow_always_option("allow"), reject_option("cancel")],
        );

        assert_eq!(auto_approve_option_id_with_benchmark(&request, true), None);
    }

    #[test]
    fn auto_approve_selects_first_codex_allow_always_option() {
        let request = permission_request_with_title_and_raw_input(
            "Approve MCP tool call",
            Some(json!({ "server_name": "aionui-team" })),
            vec![
                allow_once_option("approved"),
                allow_always_option("approved-for-session"),
                allow_always_option("approved-always"),
                reject_option("cancel"),
            ],
        );

        // `approved-for-session` is selected because it is the first AllowAlways option,
        // not because the option id has special meaning in AionCore.
        assert_eq!(
            auto_approve_option_id(&request).as_deref(),
            Some("approved-for-session")
        );
    }

    #[test]
    fn auto_approve_selects_claude_allow_always_by_kind() {
        let request = permission_request_with_title_and_raw_input(
            "mcp__aionui-team__team_write_plan",
            None,
            vec![
                allow_always_option("allow_always"),
                allow_once_option("allow"),
                reject_option("reject"),
            ],
        );

        // `allow_always` is selected because it is the only AllowAlways option,
        // not because the option id has special meaning in AionCore.
        assert_eq!(auto_approve_option_id(&request).as_deref(), Some("allow_always"));
    }

    #[test]
    fn auto_approve_ignores_removed_upgrade_server() {
        let request = permission_request_with_title_and_raw_input(
            concat!("mcp__aionui-team", "-guide__guide_write_plan"),
            None,
            vec![allow_always_option("allow_always"), reject_option("reject")],
        );

        assert_eq!(auto_approve_option_id(&request), None);
    }

    #[test]
    fn auto_approve_selects_first_available_allow_always_option() {
        let request = permission_request_with_title_and_raw_input(
            "Approve MCP tool call",
            Some(json!({ "server_name": "aionui-team" })),
            vec![
                allow_always_option("custom-allow-always"),
                allow_once_option("custom-allow-once"),
            ],
        );

        assert_eq!(auto_approve_option_id(&request).as_deref(), Some("custom-allow-always"));
    }

    #[test]
    fn auto_approve_returns_none_when_team_mcp_has_no_allow_option() {
        let request = permission_request_with_title_and_raw_input(
            "Approve MCP tool call",
            Some(json!({ "server_name": "aionui-team" })),
            vec![reject_option("cancel")],
        );

        assert_eq!(auto_approve_option_id(&request), None);
    }

    #[test]
    fn confirm_missing_permission_returns_specific_error() {
        let (_tx, rx) = mpsc::channel(1);
        let router = PermissionRouter::new(rx);

        let error = router
            .confirm("missing-tool", "allow_once".to_owned(), "conv-1")
            .expect_err("missing permission should fail");

        assert!(
            error
                .to_string()
                .contains("Pending ACP permission not found: missing-tool")
        );
    }

    #[test]
    fn cancel_all_removes_pending_confirmations() {
        let (_tx, rx) = mpsc::channel(1);
        let router = PermissionRouter::new(rx);
        let (response_tx, _response_rx) = oneshot::channel();
        router.insert_pending_for_test("tool-1".to_owned(), response_tx, sample_confirmation("tool-1"));

        router.cancel_all();

        assert!(router.get_confirmations().is_empty());
    }

    #[tokio::test]
    async fn start_routes_permission_request_and_exposes_recoverable_confirmation() {
        let (permission_tx, permission_rx) = mpsc::channel(1);
        let router = Arc::new(PermissionRouter::new(permission_rx));
        let runtime = AgentRuntime::new("conv-1", "/tmp/workspace", 8);
        let mut event_rx = runtime.subscribe();
        router.start(runtime);

        let request = RequestPermissionRequest::new(
            "session-1",
            SdkToolCallUpdate::new(
                "tool-1",
                ToolCallUpdateFields::new()
                    .title("Write file")
                    .kind(SdkToolKind::Edit)
                    .raw_input(json!({ "description": "Write /tmp/current_time.txt" })),
            ),
            vec![PermissionOption::new(
                "allow_once",
                "Allow",
                SdkPermissionOptionKind::AllowOnce,
            )],
        );
        let (response_tx, mut response_rx) = oneshot::channel();

        permission_tx
            .send(PermissionRequest { request, response_tx })
            .await
            .expect("permission request should be accepted");

        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("permission event should be emitted")
            .expect("permission event channel should stay open");
        assert!(matches!(event, AgentStreamEvent::AcpPermission(_)));

        let confirmations = router.get_confirmations();
        assert_eq!(confirmations.len(), 1);
        assert_eq!(confirmations[0].id, "tool-1");
        assert_eq!(confirmations[0].call_id, "tool-1");
        assert_eq!(confirmations[0].command_type.as_deref(), Some("edit"));

        router
            .confirm("tool-1", "allow_once".to_owned(), "conv-1")
            .expect("confirm should resolve routed request");

        assert!(router.get_confirmations().is_empty());
        assert!(matches!(
            response_rx.try_recv(),
            Ok(PermissionDecision::Selected { option_id }) if option_id == "allow_once"
        ));
    }

    #[tokio::test]
    async fn start_auto_approves_team_mcp_with_existing_option_id() {
        let (permission_tx, permission_rx) = mpsc::channel(1);
        let router = Arc::new(PermissionRouter::new(permission_rx));
        let runtime = AgentRuntime::new("conv-1", "/tmp/workspace", 8);
        router.start(runtime);

        let request = permission_request_with_title_and_raw_input(
            "Approve MCP tool call",
            Some(json!({ "server_name": "aionui-team" })),
            vec![
                allow_once_option("approved"),
                allow_always_option("approved-for-session"),
                reject_option("cancel"),
            ],
        );
        let (response_tx, response_rx) = oneshot::channel();

        permission_tx
            .send(PermissionRequest { request, response_tx })
            .await
            .expect("permission request should be accepted");

        let decision = tokio::time::timeout(Duration::from_secs(1), response_rx)
            .await
            .expect("auto approval should respond")
            .expect("auto approval responder should stay open");

        assert!(matches!(
            decision,
            PermissionDecision::Selected { option_id } if option_id == "approved-for-session"
        ));
        assert!(router.get_confirmations().is_empty());
    }

    #[tokio::test]
    async fn start_auto_approves_benchmark_mcp_only_when_isolation_is_enabled() {
        let (permission_tx, permission_rx) = mpsc::channel(1);
        let router = Arc::new(PermissionRouter::with_benchmark_container_isolation(
            permission_rx,
            true,
        ));
        let runtime = AgentRuntime::new("conv-1", "/tmp/workspace", 8);
        router.start(runtime);

        let request = permission_request_with_title_and_raw_input(
            "mcp.deepscientist-benchmark-container.benchmark_list",
            Some(json!({ "server_name": "deepscientist-benchmark-container" })),
            vec![allow_always_option("benchmark-allow"), reject_option("cancel")],
        );
        let (response_tx, response_rx) = oneshot::channel();
        permission_tx
            .send(PermissionRequest { request, response_tx })
            .await
            .expect("permission request should be accepted");

        let decision = tokio::time::timeout(Duration::from_secs(1), response_rx)
            .await
            .expect("benchmark auto approval should respond")
            .expect("benchmark auto approval responder should stay open");
        assert!(matches!(
            decision,
            PermissionDecision::Selected { option_id } if option_id == "benchmark-allow"
        ));
        assert!(router.get_confirmations().is_empty());
    }
}
