//! Operator-owned ACP launcher binding. The launcher owns the actual sandbox
//! and its evidence; this module only validates and applies its launch contract.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use aionui_api_types::MathAcpIsolationCapability;
use aionui_common::CommandSpec;
use tracing::{info, warn};

const ENV_PREFIX: &str = "DEEPSCIENTIST_MATH_ACP_ISOLATION_";
const ENV_KEYS: [&str; 3] = [
    "DEEPSCIENTIST_MATH_ACP_ISOLATION_LAUNCHER",
    "DEEPSCIENTIST_MATH_ACP_ISOLATION_POLICY",
    "DEEPSCIENTIST_MATH_ACP_ISOLATION_NODE",
];

struct IsolationLauncher {
    launcher: PathBuf,
    policy: PathBuf,
    node: PathBuf,
}

impl IsolationLauncher {
    fn from_process_env() -> Result<Option<Self>, &'static str> {
        Self::from_values(ENV_KEYS.map(std::env::var_os))
    }

    fn from_values(values: [Option<OsString>; 3]) -> Result<Option<Self>, &'static str> {
        if values.iter().all(Option::is_none) {
            return Ok(None);
        }
        let [Some(launcher), Some(policy), Some(node)] = values else {
            return Err("math_acp_isolation_config_incomplete");
        };
        let launcher = PathBuf::from(launcher);
        let policy = PathBuf::from(policy);
        let node = PathBuf::from(node);
        validate_file(&launcher, Some("js"))?;
        validate_file(&policy, Some("json"))?;
        validate_file(&node, None)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(&node).map_err(|_| "math_acp_isolation_config_unavailable")?;
            if metadata.permissions().mode() & 0o111 == 0 {
                return Err("math_acp_isolation_node_not_executable");
            }
        }
        Ok(Some(Self { launcher, policy, node }))
    }

    fn wrap(self, command: &mut CommandSpec, conversation_id: &str) -> Result<(), &'static str> {
        if conversation_id.is_empty() || conversation_id.chars().any(char::is_control) {
            return Err("math_acp_isolation_conversation_invalid");
        }
        let original_program = command
            .command
            .to_str()
            .ok_or("math_acp_isolation_command_invalid")?
            .to_owned();
        let mut args = vec![
            self.launcher.to_string_lossy().into_owned(),
            "--policy".to_owned(),
            self.policy.to_string_lossy().into_owned(),
            "--conversation-id".to_owned(),
            conversation_id.to_owned(),
            "--".to_owned(),
            original_program,
        ];
        args.append(&mut command.args);
        command.command = self.node;
        command.args = args;
        Ok(())
    }
}

fn validate_file(path: &Path, extension: Option<&str>) -> Result<(), &'static str> {
    if !path.is_absolute()
        || path.to_str().is_none()
        || extension.is_some_and(|expected| path.extension().and_then(|value| value.to_str()) != Some(expected))
    {
        return Err("math_acp_isolation_config_invalid");
    }
    if !std::fs::metadata(path)
        .map_err(|_| "math_acp_isolation_config_unavailable")?
        .is_file()
    {
        return Err("math_acp_isolation_config_invalid");
    }
    Ok(())
}

pub(super) fn validate_isolation_env<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<(), &'static str> {
    if names
        .into_iter()
        .any(|name| name.to_ascii_uppercase().starts_with(ENV_PREFIX))
    {
        return Err("math_acp_isolation_env_override_rejected");
    }
    Ok(())
}

fn validate_backend(backend: Option<&str>, configured: bool) -> Result<(), &'static str> {
    if configured && backend != Some("codex") {
        return Err("math_acp_isolation_backend_unsupported");
    }
    Ok(())
}

pub(super) fn validate_non_acp_isolation(runtime_env: &[(String, String)]) -> Result<(), &'static str> {
    validate_isolation_env(runtime_env.iter().map(|(name, _)| name.as_str()))?;
    validate_backend(Some("aionrs"), IsolationLauncher::from_process_env()?.is_some())
}

/// Called after all ACP session parameters, including Team MCP, are assembled.
/// Rebuilt and resumed sessions pass through the same factory boundary.
pub(super) fn apply_isolation_launcher(
    command: &mut CommandSpec,
    backend: Option<&str>,
    conversation_id: &str,
) -> Result<(), &'static str> {
    let result = (|| {
        validate_isolation_env(command.env.iter().map(|entry| entry.name.as_str()))?;
        let config = IsolationLauncher::from_process_env()?;
        validate_backend(backend, config.is_some())?;
        if let Some(config) = config {
            config.wrap(command, conversation_id)?;
            info!(
                conversation_id,
                protocol_version = 1,
                "Math ACP isolation launcher enabled"
            );
        }
        Ok(())
    })();
    if let Err(reason) = result {
        warn!(conversation_id, reason, "Math ACP isolation launcher rejected");
    }
    result
}

/// Configuration availability only. This is deliberately not an attestation
/// that a sandbox has started or that blind-isolation canaries have passed.
pub fn math_acp_isolation_capability() -> MathAcpIsolationCapability {
    capability_for_config(IsolationLauncher::from_process_env())
}

fn capability_for_config(config: Result<Option<IsolationLauncher>, &'static str>) -> MathAcpIsolationCapability {
    MathAcpIsolationCapability {
        enabled: matches!(config, Ok(Some(_))),
        protocol_version: 1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aionui_common::EnvVar;

    fn fixture() -> (tempfile::TempDir, [Option<OsString>; 3]) {
        let dir = tempfile::tempdir().unwrap();
        let paths = [
            dir.path().join("launcher.js"),
            dir.path().join("policy.json"),
            dir.path().join("node"),
        ];
        for path in &paths {
            std::fs::write(path, "fixture").unwrap();
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&paths[2], std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        (dir, paths.map(|path| Some(path.into_os_string())))
    }

    #[test]
    fn missing_configuration_keeps_capability_disabled() {
        let config = IsolationLauncher::from_values([None, None, None]);
        assert!(config.as_ref().is_ok_and(Option::is_none));
        assert_eq!(
            serde_json::to_value(capability_for_config(config)).unwrap(),
            serde_json::json!({"enabled": false, "protocol_version": 1})
        );
        assert!(validate_backend(Some("claude"), false).is_ok());
    }

    #[test]
    fn partial_configuration_is_rejected_and_not_advertised() {
        let (_dir, values) = fixture();
        for missing in 0..3 {
            let mut partial = values.clone();
            partial[missing] = None;
            let config = IsolationLauncher::from_values(partial);
            assert_eq!(
                config.as_ref().err().copied(),
                Some("math_acp_isolation_config_incomplete")
            );
            assert!(!capability_for_config(config).enabled);
        }
    }

    #[test]
    fn paths_must_be_absolute_existing_files_with_expected_extensions() {
        let (dir, values) = fixture();
        for (index, invalid) in [
            (0, PathBuf::from("relative.js")),
            (0, dir.path().join("launcher.py")),
            (1, dir.path().join("missing.json")),
            (2, dir.path().to_owned()),
        ] {
            let mut candidate = values.clone();
            candidate[index] = Some(invalid.into_os_string());
            let error = IsolationLauncher::from_values(candidate).err().unwrap();
            assert!(matches!(
                error,
                "math_acp_isolation_config_invalid" | "math_acp_isolation_config_unavailable"
            ));
        }
    }

    #[cfg(unix)]
    #[test]
    fn node_must_be_executable() {
        use std::os::unix::fs::PermissionsExt;
        let (_dir, values) = fixture();
        std::fs::set_permissions(
            Path::new(values[2].as_ref().unwrap()),
            std::fs::Permissions::from_mode(0o600),
        )
        .unwrap();
        assert_eq!(
            IsolationLauncher::from_values(values).err(),
            Some("math_acp_isolation_node_not_executable")
        );
    }

    #[test]
    fn enabled_configuration_rejects_every_non_codex_backend() {
        for backend in [None, Some("claude"), Some("custom"), Some("aionrs")] {
            assert_eq!(
                validate_backend(backend, true),
                Err("math_acp_isolation_backend_unsupported")
            );
        }
        assert!(validate_backend(Some("codex"), true).is_ok());
    }

    #[test]
    fn session_environment_cannot_override_operator_configuration() {
        for name in ENV_KEYS.into_iter().chain([
            "deepscientist_math_acp_isolation_launcher",
            "DEEPSCIENTIST_MATH_ACP_ISOLATION_FUTURE",
        ]) {
            assert_eq!(
                validate_isolation_env(["PATH", name]),
                Err("math_acp_isolation_env_override_rejected")
            );
        }
        assert!(validate_isolation_env(["PATH", "AIONUI_RUNTIME_TOKEN"]).is_ok());
    }

    #[test]
    fn launcher_receives_original_arguments_identity_cwd_and_environment_without_secret_arguments() {
        let (_dir, values) = fixture();
        let mut command = CommandSpec {
            command: PathBuf::from("/managed/node"),
            args: vec!["/managed/codex-acp.js".into(), "argument with spaces".into()],
            env: vec![EnvVar {
                name: "AIONUI_RUNTIME_TOKEN".into(),
                value: "fixture-secret".into(),
            }],
            cwd: Some("/workspace/worker".into()),
        };
        let config = IsolationLauncher::from_values(values.clone()).unwrap().unwrap();
        config.wrap(&mut command, "conv-worker").unwrap();
        assert_eq!(command.command.as_os_str(), values[2].as_ref().unwrap());
        assert_eq!(
            command.args,
            vec![
                values[0].as_ref().unwrap().to_str().unwrap(),
                "--policy",
                values[1].as_ref().unwrap().to_str().unwrap(),
                "--conversation-id",
                "conv-worker",
                "--",
                "/managed/node",
                "/managed/codex-acp.js",
                "argument with spaces"
            ]
        );
        assert_eq!(command.cwd.as_deref(), Some("/workspace/worker"));
        assert_eq!(command.env[0].value, "fixture-secret");
        assert!(!command.args.iter().any(|arg| arg.contains("fixture-secret")));
        assert!(capability_for_config(IsolationLauncher::from_values(values)).enabled);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn wrapped_process_preserves_stdio_workspace_and_environment() {
        use crate::capability::cli_process::CliAgentProcess;
        use tokio::io::AsyncReadExt;
        use tokio::time::{Duration, timeout};

        let (dir, mut values) = fixture();
        let launcher = Path::new(values[0].as_ref().unwrap());
        std::fs::write(launcher, "test \"$1\" = --policy && test \"$3\" = --conversation-id && test \"$4\" = conv-real && test \"$5\" = -- || exit 91\nshift 5\nexec \"$@\"\n").unwrap();
        values[2] = Some(OsString::from("/bin/sh"));
        let mut command = CommandSpec {
            command: PathBuf::from("/bin/sh"),
            args: vec!["-c".into(), "printf '%s|%s' \"$PWD\" \"$PASSTHROUGH_TEST\"".into()],
            env: vec![EnvVar {
                name: "PASSTHROUGH_TEST".into(),
                value: "preserved".into(),
            }],
            cwd: Some(dir.path().to_str().unwrap().to_owned()),
        };
        IsolationLauncher::from_values(values)
            .unwrap()
            .unwrap()
            .wrap(&mut command, "conv-real")
            .unwrap();
        let process = CliAgentProcess::spawn_for_sdk(command).await.unwrap();
        let (stdin, mut stdout) = process.take_stdio().await.unwrap();
        drop(stdin);
        let mut output = String::new();
        timeout(Duration::from_secs(5), stdout.read_to_string(&mut output))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output, format!("{}|preserved", dir.path().display()));
        assert!(
            timeout(Duration::from_secs(5), process.wait_for_exit())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}
