use std::path::Path;

pub(crate) const BENCHMARK_CONTAINER_MCP_NAME: &str = "deepscientist-benchmark-container";

const RETIRED_PROJECT_AGENT_NAMES: &[&str] = &[
    "builtin-lark-project-agent",
    "ds-team-project",
    "lark-project-agent",
    "lark_project_agent",
    "deepscientist-lark-project-agent",
];
const RETIRED_PROJECT_AGENT_PREFIXES: &[&str] = &["ds-team-project-runtime-", "deepscientist-project-runtime-"];
const RETIRED_PROJECT_AGENT_SCRIPT: &str = "builtin-mcp-lark-project-agent.js";
const DEEPSCIENTIST_BUILTIN_MCP_SCRIPT_PREFIX: &str = "builtin-mcp-";

/// Reject stale DeepScientist-owned stdio references before the runtime tries
/// to spawn them.  This is intentionally narrower than a `ds-team-*` match:
/// current Team MCP servers are implemented by the Core binary and must remain
/// available.
pub(crate) fn validate_deepscientist_stdio_reference(
    server_name: &str,
    command: &str,
    args: &[String],
) -> Result<(), String> {
    let normalized_name = server_name.trim().to_ascii_lowercase();
    if RETIRED_PROJECT_AGENT_NAMES.contains(&normalized_name.as_str())
        || RETIRED_PROJECT_AGENT_PREFIXES
            .iter()
            .any(|prefix| normalized_name.starts_with(prefix))
    {
        return Err(format!(
            "retired DeepScientist project-agent MCP reference: {server_name}"
        ));
    }

    for candidate in std::iter::once(command).chain(args.iter().map(String::as_str)) {
        let Some(basename) = portable_basename(candidate) else {
            continue;
        };
        let normalized_basename = basename.to_ascii_lowercase();
        if normalized_basename == RETIRED_PROJECT_AGENT_SCRIPT {
            return Err(format!("retired DeepScientist project-agent MCP script: {candidate}"));
        }
        if is_deepscientist_builtin_mcp_script(&normalized_basename) && !Path::new(candidate).is_file() {
            return Err(format!("DeepScientist built-in MCP script is unavailable: {candidate}"));
        }
    }

    Ok(())
}

fn portable_basename(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    value.rsplit(['/', '\\']).next().filter(|part| !part.is_empty())
}

fn is_deepscientist_builtin_mcp_script(basename: &str) -> bool {
    basename.starts_with(DEEPSCIENTIST_BUILTIN_MCP_SCRIPT_PREFIX) && basename.ends_with(".js")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_all_retired_project_agent_names_and_prefixes() {
        for name in RETIRED_PROJECT_AGENT_NAMES {
            let error = validate_deepscientist_stdio_reference(name, "/usr/bin/node", &[]).unwrap_err();
            assert!(error.contains("retired DeepScientist project-agent MCP reference"));
        }
        for name in ["ds-team-project-runtime-42", "deepscientist-project-runtime-conv-1"] {
            let error = validate_deepscientist_stdio_reference(name, "/usr/bin/node", &[]).unwrap_err();
            assert!(error.contains("retired DeepScientist project-agent MCP reference"));
        }
    }

    #[test]
    fn rejects_retired_project_agent_script_even_under_an_unrelated_name() {
        let args = vec![r"C:\\stale\\builtin-mcp-lark-project-agent.js".to_owned()];
        let error = validate_deepscientist_stdio_reference("unrelated", "node", &args).unwrap_err();
        assert!(error.contains("retired DeepScientist project-agent MCP script"));
    }

    #[test]
    fn rejects_missing_deepscientist_builtin_script_but_accepts_an_existing_one() {
        let temp = tempfile::tempdir().expect("tempdir");
        let script = temp.path().join("builtin-mcp-research-evidence.js");
        let args = vec![script.to_string_lossy().into_owned()];

        let error = validate_deepscientist_stdio_reference("research-evidence", "node", &args).unwrap_err();
        assert!(error.contains("DeepScientist built-in MCP script is unavailable"));

        std::fs::write(&script, "// test MCP\n").expect("write script");
        validate_deepscientist_stdio_reference("research-evidence", "node", &args)
            .expect("existing built-in MCP script should remain available");
    }

    #[test]
    fn preserves_core_native_team_and_unrelated_ds_team_names() {
        for (name, arg) in [
            ("aionui-team-mcp", "mcp-team-stdio"),
            ("ds-team-runtime-0", "mcp-bridge"),
            ("ds-team-company-runtime", "custom-team-command"),
        ] {
            validate_deepscientist_stdio_reference(name, "/opt/deepscientist-core", &[arg.to_owned()])
                .expect("current Core-native Team reference must not be retired");
        }
    }
}
