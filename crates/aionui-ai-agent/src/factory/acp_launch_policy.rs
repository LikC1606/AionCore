use crate::cc_switch;
use crate::manager::acp::mode_normalize::normalize_requested_mode;
use crate::shared_kernel::PersistedSessionState;
use aionui_api_types::{AcpBuildExtra, AgentMetadata};
use aionui_common::CommandSpec;
use serde_json::json;

const CODEX_CONFIG_FLAG: &str = "-c";
const CODEX_CONFIG_ENV: &str = "CODEX_CONFIG";
const CODEX_WINDOWS_UNELEVATED_SANDBOX: &str = "windows.sandbox=\"unelevated\"";
#[cfg(test)]
const MATH_BUDGET_ENV: [&str; 3] = [
    "DEEPSCIENTIST_MATH_BUDGET_SOCKET",
    "DEEPSCIENTIST_MATH_BUDGET_SECRET",
    "DEEPSCIENTIST_MATH_BUDGET_MAX_OUTPUT_TOKENS",
];

pub(super) struct AcpLaunchPolicyInput<'a> {
    pub metadata: &'a AgentMetadata,
    pub config: &'a AcpBuildExtra,
    pub session_snapshot: Option<&'a PersistedSessionState>,
    pub runtime_env: &'a [(String, String)],
    pub belongs_to_team: bool,
}

pub(super) fn apply_acp_launch_policy(
    command_spec: &mut CommandSpec,
    input: AcpLaunchPolicyInput<'_>,
) -> Result<(), String> {
    super::acp_isolation::validate_isolation_env(
        command_spec
            .env
            .iter()
            .map(|entry| entry.name.as_str())
            .chain(input.runtime_env.iter().map(|(name, _)| name.as_str())),
    )?;
    if command_spec.env.iter().any(|entry| is_math_budget_env(&entry.name)) {
        return Err("math_budget_launch_env_invalid: budget binding must come from session runtime".to_owned());
    }
    validate_math_budget_backend(input.metadata.backend.as_deref(), input.runtime_env)?;
    if command_spec.env.iter().any(|entry| {
        entry
            .name
            .to_ascii_uppercase()
            .starts_with("DEEPSCIENTIST_MATH_REQUEST_")
    }) {
        return Err("Math request routing must come from session runtime".into());
    }
    if std::env::var("DEEPSCIENTIST_MATH_REQUEST_GATE_URL").is_ok()
        && input
            .runtime_env
            .iter()
            .any(|(name, _)| name == "DEEPSCIENTIST_MATH_REQUEST_GROUP")
        && input.metadata.backend.as_deref() != Some("codex")
    {
        return Err("Math request gateway is restricted to Codex ACP".into());
    }
    let initial_mode = initial_mode_from_build_context(input.metadata, input.config, input.session_snapshot);
    apply_codex_runtime_config_args(command_spec, input.metadata, initial_mode.as_deref());
    append_runtime_env(command_spec, input.runtime_env);
    append_codex_config_env(
        command_spec,
        input.metadata,
        initial_mode.as_deref(),
        input.runtime_env,
        input.belongs_to_team,
    )?;
    append_claude_provider_env(command_spec, input.metadata);
    Ok(())
}

fn append_runtime_env(command_spec: &mut CommandSpec, runtime_env: &[(String, String)]) {
    for (name, value) in runtime_env {
        command_spec.env.push(aionui_common::EnvVar {
            name: name.clone(),
            value: value.clone(),
        });
    }
}

pub(super) fn validate_math_budget_backend(
    backend: Option<&str>,
    runtime_env: &[(String, String)],
) -> Result<(), String> {
    if runtime_env.iter().any(|(name, _)| is_math_budget_env(name)) && backend != Some("codex") {
        return Err("math_budget_backend_invalid: math budget environment is restricted to Codex ACP".to_owned());
    }
    Ok(())
}

fn is_math_budget_env(name: &str) -> bool {
    name.to_ascii_uppercase().starts_with("DEEPSCIENTIST_MATH_BUDGET_")
}

fn append_claude_provider_env(command_spec: &mut CommandSpec, metadata: &AgentMetadata) {
    if metadata.backend.as_deref() != Some("claude") {
        return;
    }

    // A run-scoped provider environment is authoritative. Mixing it with the
    // user's global cc-switch profile makes headless experiments depend on
    // ambient desktop state and can silently replace credentials or routing.
    // The final process environment still inherits these run-scoped values via
    // agent_process_env; this guard only prevents a later cc-switch override.
    if ["ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"]
        .iter()
        .any(|name| std::env::var_os(name).is_some())
    {
        tracing::info!("cc-switch: skipped because run-scoped Claude provider env is present");
        return;
    }

    let cc_switch_env = cc_switch::read_claude_provider_env();
    if cc_switch_env.is_empty() {
        return;
    }

    let keys: Vec<&str> = cc_switch_env.keys().map(|key| key.as_str()).collect();
    for (name, value) in &cc_switch_env {
        command_spec.env.push(aionui_common::EnvVar {
            name: name.clone(),
            value: value.clone(),
        });
    }
    tracing::info!(?keys, "cc-switch: env vars injected");
}

fn initial_mode_from_build_context(
    metadata: &AgentMetadata,
    config: &AcpBuildExtra,
    session_snapshot: Option<&PersistedSessionState>,
) -> Option<String> {
    session_snapshot
        .and_then(|snapshot| snapshot.current_mode_id.as_ref())
        .map(|mode| normalize_requested_mode(metadata, mode.as_str()))
        .or_else(|| {
            config
                .session_mode
                .as_ref()
                .map(|mode| normalize_requested_mode(metadata, mode))
        })
        .filter(|mode| !mode.is_empty())
}

fn apply_codex_runtime_config_args(
    command_spec: &mut CommandSpec,
    metadata: &AgentMetadata,
    initial_mode: Option<&str>,
) {
    if metadata.backend.as_deref() != Some("codex") {
        return;
    }

    let sandbox_mode = codex_sandbox_mode_for_requested_mode(initial_mode);
    push_codex_config_arg(command_spec, &format!("sandbox_mode=\"{sandbox_mode}\""));
    if sandbox_mode == "danger-full-access" {
        push_codex_config_arg(command_spec, CODEX_WINDOWS_UNELEVATED_SANDBOX);
    }
}

/// `codex-acp` 1.1.2 consumes configuration through its documented
/// `CODEX_CONFIG` JSON environment variable. Its ACP entrypoint does not
/// forward Codex's `-c` flags, so shell isolation must be injected here and
/// bound to one deterministic object. Refuse any caller-provided value to
/// avoid an ambiguous merge that could re-enable snapshots or login shells.
fn append_codex_config_env(
    command_spec: &mut CommandSpec,
    metadata: &AgentMetadata,
    initial_mode: Option<&str>,
    runtime_env: &[(String, String)],
    belongs_to_team: bool,
) -> Result<(), String> {
    if metadata.backend.as_deref() != Some("codex") {
        return Ok(());
    }
    if command_spec.env.iter().any(|entry| entry.name == CODEX_CONFIG_ENV)
        || runtime_env.iter().any(|(name, _)| name == CODEX_CONFIG_ENV)
    {
        return Err("Codex launch rejects a caller-provided CODEX_CONFIG".to_owned());
    }
    let sandbox_mode = codex_sandbox_mode_for_requested_mode(initial_mode);
    let mut features = serde_json::Map::from_iter([("shell_snapshot".to_owned(), json!(false))]);
    if belongs_to_team {
        // DeepScientist Team members are ACP conversations managed by Core,
        // not Codex-native child threads. Exposing Codex's second orchestration
        // layer lets a native empty `wait` block the ACP turn while Team
        // deliveries queue behind it in Core's event loop.
        features.insert("multi_agent".to_owned(), json!(false));
    }
    let mut config = json!({
        "allow_login_shell": false,
        "shell_environment_policy": {
            "inherit": "all",
            "experimental_use_profile": false,
            "include_only": [],
            "exclude": ["DEEPSEEK_API_KEY", "DEEPSCIENTIST_MATH_BUDGET_*", "DEEPSCIENTIST_MATH_REQUEST_GATE_SECRET"],
            "set": {},
        },
        "features": features,
        "sandbox_mode": sandbox_mode,
    });
    append_math_request_gateway_config(
        &mut config,
        runtime_env,
        std::env::var("DEEPSCIENTIST_MATH_REQUEST_GATE_URL").ok().as_deref(),
        std::env::var("DEEPSCIENTIST_MATH_REQUEST_GATE_SECRET").ok().as_deref(),
    )?;
    if config.get("model_providers.WestlakeHPC.base_url").is_some() {
        command_spec.env.push(aionui_common::EnvVar {
            name: "DEEPSCIENTIST_MATH_REQUEST_GATE_SECRET".into(),
            value: std::env::var("DEEPSCIENTIST_MATH_REQUEST_GATE_SECRET")
                .map_err(|_| "Math request gateway secret unavailable")?,
        });
    }
    command_spec.env.push(aionui_common::EnvVar {
        name: CODEX_CONFIG_ENV.to_owned(),
        value: config.to_string(),
    });
    Ok(())
}

fn append_math_request_gateway_config(
    config: &mut serde_json::Value,
    runtime_env: &[(String, String)],
    gateway_url: Option<&str>,
    secret: Option<&str>,
) -> Result<(), String> {
    if !runtime_env
        .iter()
        .any(|(name, _)| name == "DEEPSCIENTIST_MATH_REQUEST_GROUP")
    {
        return Ok(());
    }
    if let Some(url) = gateway_url {
        let valid_local = url
            .strip_prefix("http://127.0.0.1:")
            .and_then(|port| port.parse::<u16>().ok())
            .is_some_and(|port| port > 0);
        if !valid_local || secret.is_none_or(str::is_empty) {
            return Err("Math request gateway requires a local URL and secret".into());
        }
        config["model_providers.WestlakeHPC.base_url"] = json!(url);
        config["model_providers.WestlakeHPC.env_http_headers"] = json!({
            "x-deepscientist-gate-secret": "DEEPSCIENTIST_MATH_REQUEST_GATE_SECRET",
            "x-deepscientist-run-root": "DEEPSCIENTIST_MATH_REQUEST_GROUP",
        });
    }
    Ok(())
}

fn push_codex_config_arg(command_spec: &mut CommandSpec, value: &str) {
    command_spec.args.push(CODEX_CONFIG_FLAG.to_owned());
    command_spec.args.push(value.to_owned());
}

fn codex_sandbox_mode_for_requested_mode(mode: Option<&str>) -> &'static str {
    match mode.map(str::trim) {
        Some("agent-full-access" | "full-access" | "yoloNoSandbox") => "danger-full-access",
        Some("read-only") => "read-only",
        _ => "workspace-write",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn math_gateway_overrides_only_bound_sessions_and_keeps_secret_out_of_config() {
        let binding = [("DEEPSCIENTIST_MATH_REQUEST_GROUP".into(), "/run/a".into())];
        let mut config = json!({});
        append_math_request_gateway_config(&mut config, &[], Some("http://127.0.0.1:1234"), Some("private-value"))
            .unwrap();
        assert_eq!(config, json!({}));
        append_math_request_gateway_config(
            &mut config,
            &binding,
            Some("http://127.0.0.1:1234"),
            Some("private-value"),
        )
        .unwrap();
        assert_eq!(config["model_providers.WestlakeHPC.base_url"], "http://127.0.0.1:1234");
        assert_eq!(
            config["model_providers.WestlakeHPC.env_http_headers"]["x-deepscientist-run-root"],
            "DEEPSCIENTIST_MATH_REQUEST_GROUP"
        );
        assert!(!config.to_string().contains("private-value"));
        for url in [
            "http://127.0.0.1:1234@evil.test",
            "https://example.com",
            "http://127.0.0.1:0",
        ] {
            assert!(
                append_math_request_gateway_config(&mut config, &binding, Some(url), Some("secret"))
                    .unwrap_err()
                    .contains("local URL")
            );
        }
        assert!(
            append_math_request_gateway_config(&mut config, &binding, Some("http://127.0.0.1:1234"), None).is_err()
        );
    }

    fn agent_metadata_with_backend(backend: Option<&str>) -> AgentMetadata {
        AgentMetadata {
            id: "agent-1".into(),
            icon: None,
            name: "Test ACP".into(),
            name_i18n: None,
            description: None,
            description_i18n: None,
            backend: backend.map(str::to_owned),
            agent_type: aionui_common::AgentType::Acp,
            agent_source: aionui_api_types::AgentSource::Builtin,
            agent_source_info: aionui_api_types::AgentSourceInfo::default(),
            enabled: true,
            available: true,
            command: None,
            resolved_command: None,
            args: vec![],
            env: vec![],
            native_skills_dirs: None,
            behavior_policy: aionui_api_types::BehaviorPolicy::default(),
            yolo_id: Some("agent-full-access".into()),
            sort_order: 0,
            team_capable: false,
            last_check_status: None,
            last_check_kind: None,
            last_check_error_code: None,
            last_check_error_message: None,
            last_check_error_details: None,
            last_check_guidance: None,
            last_check_latency_ms: None,
            last_check_at: None,
            last_success_at: None,
            last_failure_at: None,
            handshake: aionui_api_types::AgentHandshake::default(),
            has_command_override: false,
            env_override_key_count: 0,
        }
    }

    #[test]
    fn apply_acp_launch_policy_adds_runtime_env_and_codex_full_access_config() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["codex-acp.js".into()],
            env: vec![],
            cwd: None,
        };
        let metadata = agent_metadata_with_backend(Some("codex"));
        let config = AcpBuildExtra {
            session_mode: Some("full-access".into()),
            ..Default::default()
        };

        apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &metadata,
                config: &config,
                session_snapshot: None,
                runtime_env: &[("AIONUI_CONVERSATION_ID".into(), "conv-1".into())],
                belongs_to_team: false,
            },
        )
        .expect("Codex launch policy should materialize CODEX_CONFIG");

        assert_eq!(
            command_spec.args,
            vec![
                "codex-acp.js",
                "-c",
                "sandbox_mode=\"danger-full-access\"",
                "-c",
                "windows.sandbox=\"unelevated\"",
            ]
        );
        assert!(
            command_spec
                .env
                .iter()
                .any(|entry| entry.name == "AIONUI_CONVERSATION_ID" && entry.value == "conv-1")
        );
    }

    #[test]
    fn apply_acp_launch_policy_adds_codex_full_access_config_for_agent_full_access() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["codex-acp.js".into()],
            env: vec![],
            cwd: None,
        };
        let metadata = agent_metadata_with_backend(Some("codex"));
        let config = AcpBuildExtra {
            session_mode: Some("agent-full-access".into()),
            ..Default::default()
        };

        apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &metadata,
                config: &config,
                session_snapshot: None,
                runtime_env: &[],
                belongs_to_team: false,
            },
        )
        .expect("Codex launch policy should materialize CODEX_CONFIG");

        assert!(
            command_spec
                .args
                .iter()
                .any(|arg| arg == "sandbox_mode=\"danger-full-access\"")
        );
        assert!(
            command_spec
                .args
                .iter()
                .any(|arg| arg == CODEX_WINDOWS_UNELEVATED_SANDBOX)
        );
    }

    #[test]
    fn apply_acp_launch_policy_keeps_codex_read_only_at_process_boundary() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["codex-acp.js".into()],
            env: vec![],
            cwd: None,
        };
        let metadata = agent_metadata_with_backend(Some("codex"));
        let config = AcpBuildExtra {
            session_mode: Some("read-only".into()),
            ..Default::default()
        };

        apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &metadata,
                config: &config,
                session_snapshot: None,
                runtime_env: &[],
                belongs_to_team: true,
            },
        )
        .expect("Codex read-only launch policy should materialize process isolation");

        assert_eq!(
            command_spec.args,
            vec!["codex-acp.js", "-c", "sandbox_mode=\"read-only\""]
        );
        let config = command_spec
            .env
            .iter()
            .find(|entry| entry.name == CODEX_CONFIG_ENV)
            .expect("Codex launch must inject CODEX_CONFIG");
        let value: serde_json::Value = serde_json::from_str(&config.value).expect("valid CODEX_CONFIG JSON");
        assert_eq!(value["sandbox_mode"], "read-only");
        assert_eq!(value["features"]["multi_agent"], false);
    }

    #[test]
    fn apply_acp_launch_policy_materializes_codex_config_shell_lock() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["codex-acp.js".into()],
            env: vec![],
            cwd: None,
        };
        let metadata = agent_metadata_with_backend(Some("codex"));

        apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &metadata,
                config: &AcpBuildExtra::default(),
                session_snapshot: None,
                runtime_env: &[],
                belongs_to_team: false,
            },
        )
        .expect("Codex launch policy should materialize CODEX_CONFIG");

        let config = command_spec
            .env
            .iter()
            .find(|entry| entry.name == CODEX_CONFIG_ENV)
            .expect("Codex launch must inject CODEX_CONFIG");
        let value: serde_json::Value = serde_json::from_str(&config.value).expect("valid CODEX_CONFIG JSON");
        assert_eq!(value["allow_login_shell"], false);
        assert_eq!(value["features"]["shell_snapshot"], false);
        assert!(value["features"].get("multi_agent").is_none());
        assert_eq!(value["shell_environment_policy"]["exclude"][0], "DEEPSEEK_API_KEY");
        assert_eq!(
            value["shell_environment_policy"]["exclude"][1],
            "DEEPSCIENTIST_MATH_BUDGET_*"
        );
        assert_eq!(value["shell_environment_policy"]["set"], json!({}));
        assert_eq!(value["shell_environment_policy"]["include_only"], serde_json::json!([]));
    }

    #[test]
    fn apply_acp_launch_policy_disables_native_multi_agent_for_team_codex() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["codex-acp.js".into()],
            env: vec![],
            cwd: None,
        };
        let metadata = agent_metadata_with_backend(Some("codex"));

        apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &metadata,
                config: &AcpBuildExtra::default(),
                session_snapshot: None,
                runtime_env: &[],
                belongs_to_team: true,
            },
        )
        .expect("Team Codex launch policy should materialize CODEX_CONFIG");

        let config = command_spec
            .env
            .iter()
            .find(|entry| entry.name == CODEX_CONFIG_ENV)
            .expect("Codex launch must inject CODEX_CONFIG");
        let value: serde_json::Value = serde_json::from_str(&config.value).expect("valid CODEX_CONFIG JSON");
        assert_eq!(value["features"]["multi_agent"], false);
    }

    #[test]
    fn apply_acp_launch_policy_rejects_conflicting_codex_config() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["codex-acp.js".into()],
            env: vec![aionui_common::EnvVar {
                name: CODEX_CONFIG_ENV.into(),
                value: "{}".into(),
            }],
            cwd: None,
        };
        let metadata = agent_metadata_with_backend(Some("codex"));
        let error = apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &metadata,
                config: &AcpBuildExtra::default(),
                session_snapshot: None,
                runtime_env: &[],
                belongs_to_team: false,
            },
        )
        .expect_err("conflicting CODEX_CONFIG must fail closed");
        assert!(error.contains("CODEX_CONFIG"));
    }

    #[test]
    fn apply_acp_launch_policy_rejects_runtime_env_codex_config_override() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["codex-acp.js".into()],
            env: vec![],
            cwd: None,
        };
        let metadata = agent_metadata_with_backend(Some("codex"));
        let error = apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &metadata,
                config: &AcpBuildExtra::default(),
                session_snapshot: None,
                runtime_env: &[(CODEX_CONFIG_ENV.into(), "{\"features\":{}}".into())],
                belongs_to_team: false,
            },
        )
        .expect_err("runtime CODEX_CONFIG override must fail closed");
        assert!(error.contains("CODEX_CONFIG"));
    }

    #[test]
    fn apply_acp_launch_policy_keeps_legacy_full_access_dangerous_for_persisted_snapshots() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["codex-acp.js".into()],
            env: vec![],
            cwd: None,
        };
        let metadata = agent_metadata_with_backend(Some("codex"));
        let snapshot = PersistedSessionState {
            current_mode_id: Some(crate::shared_kernel::ModeId::new("full-access")),
            ..Default::default()
        };

        apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &metadata,
                config: &AcpBuildExtra::default(),
                session_snapshot: Some(&snapshot),
                runtime_env: &[],
                belongs_to_team: false,
            },
        )
        .expect("Codex launch policy should materialize CODEX_CONFIG");

        assert!(
            command_spec
                .args
                .iter()
                .any(|arg| arg == "sandbox_mode=\"danger-full-access\"")
        );
        assert!(
            command_spec
                .args
                .iter()
                .any(|arg| arg == CODEX_WINDOWS_UNELEVATED_SANDBOX)
        );
    }

    #[test]
    fn apply_acp_launch_policy_skips_codex_config_for_non_codex_agents() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["claude-agent-acp.js".into()],
            env: vec![],
            cwd: None,
        };
        let metadata = agent_metadata_with_backend(Some("claude"));
        let config = AcpBuildExtra::default();

        apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &metadata,
                config: &config,
                session_snapshot: None,
                runtime_env: &[],
                belongs_to_team: false,
            },
        )
        .expect("non-Codex launch policy should succeed");

        assert_eq!(command_spec.args, vec!["claude-agent-acp.js"]);
    }

    #[test]
    fn apply_acp_launch_policy_rejects_budget_environment_for_non_codex_agents() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["claude-agent-acp.js".into()],
            env: vec![],
            cwd: None,
        };
        let error = apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &agent_metadata_with_backend(Some("claude")),
                config: &AcpBuildExtra::default(),
                session_snapshot: None,
                runtime_env: &[(MATH_BUDGET_ENV[1].into(), "redacted".into())],
                belongs_to_team: false,
            },
        )
        .expect_err("budget environment must not reach non-Codex ACP");
        assert!(error.contains("restricted to Codex"));
        assert!(command_spec.env.is_empty());
    }

    #[test]
    fn apply_acp_launch_policy_passes_budget_environment_only_to_codex() {
        let mut command_spec = CommandSpec {
            command: "node".into(),
            args: vec!["codex-acp.js".into()],
            env: vec![],
            cwd: None,
        };
        apply_acp_launch_policy(
            &mut command_spec,
            AcpLaunchPolicyInput {
                metadata: &agent_metadata_with_backend(Some("codex")),
                config: &AcpBuildExtra::default(),
                session_snapshot: None,
                runtime_env: &[(MATH_BUDGET_ENV[0].into(), "/private/broker.sock".into())],
                belongs_to_team: false,
            },
        )
        .expect("Codex may receive its bound budget environment");
        assert!(command_spec.env.iter().any(|entry| entry.name == MATH_BUDGET_ENV[0]));
    }

    #[test]
    fn isolation_configuration_cannot_be_overridden_by_catalog_or_runtime_environment() {
        for catalog_override in [false, true] {
            let mut command_spec = CommandSpec {
                command: "node".into(),
                args: vec!["agent.js".into()],
                cwd: None,
                env: Vec::new(),
            };
            let override_entry = (
                "DEEPSCIENTIST_MATH_ACP_ISOLATION_POLICY".to_owned(),
                "fixture-only".to_owned(),
            );
            let mut runtime_env = Vec::new();
            if catalog_override {
                command_spec.env.push(aionui_common::EnvVar {
                    name: override_entry.0,
                    value: override_entry.1,
                });
            } else {
                runtime_env.push(override_entry);
            }
            let error = apply_acp_launch_policy(
                &mut command_spec,
                AcpLaunchPolicyInput {
                    metadata: &agent_metadata_with_backend(Some("codex")),
                    config: &AcpBuildExtra::default(),
                    session_snapshot: None,
                    runtime_env: &runtime_env,
                    belongs_to_team: false,
                },
            )
            .unwrap_err();
            assert_eq!(error, "math_acp_isolation_env_override_rejected");
            assert_eq!(command_spec.args, vec!["agent.js"]);
            assert!(!error.contains("fixture-only"));
        }
    }

    #[test]
    fn catalog_budget_environment_is_rejected_before_launch_mutation() {
        for backend in [Some("codex"), Some("claude"), None] {
            let mut command_spec = CommandSpec {
                command: "node".into(),
                args: vec!["agent.js".into()],
                cwd: None,
                env: vec![aionui_common::EnvVar {
                    name: MATH_BUDGET_ENV[1].to_ascii_lowercase(),
                    value: "fixture-only".into(),
                }],
            };
            let error = apply_acp_launch_policy(
                &mut command_spec,
                AcpLaunchPolicyInput {
                    metadata: &agent_metadata_with_backend(backend),
                    config: &AcpBuildExtra::default(),
                    session_snapshot: None,
                    runtime_env: &[],
                    belongs_to_team: false,
                },
            )
            .unwrap_err();
            assert_eq!(
                error,
                "math_budget_launch_env_invalid: budget binding must come from session runtime"
            );
            assert_eq!(command_spec.args, vec!["agent.js"]);
            assert_eq!(command_spec.env.len(), 1);
        }
    }

    #[test]
    fn unknown_and_case_variant_budget_keys_cannot_reach_other_backends() {
        for key in ["deepscientist_math_budget_secret", "DEEPSCIENTIST_MATH_BUDGET_FUTURE"] {
            let error = validate_math_budget_backend(Some("custom"), &[(key.into(), "fixture".into())]).unwrap_err();
            assert_eq!(
                error,
                "math_budget_backend_invalid: math budget environment is restricted to Codex ACP"
            );
        }
    }

    #[test]
    fn initial_mode_from_build_context_prefers_persisted_snapshot() {
        let snapshot = PersistedSessionState {
            current_mode_id: Some(crate::shared_kernel::ModeId::new("full-access")),
            ..Default::default()
        };
        let config = AcpBuildExtra {
            session_mode: Some("auto".into()),
            ..Default::default()
        };

        let mode =
            initial_mode_from_build_context(&agent_metadata_with_backend(Some("codex")), &config, Some(&snapshot));

        assert_eq!(mode.as_deref(), Some("agent-full-access"));
    }
}
