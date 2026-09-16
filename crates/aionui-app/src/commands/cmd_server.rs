//! `aioncore` (no subcommand): the main HTTP server.

use std::io::{self, Write};
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{future::Future, pin::Pin};

use tokio::net::TcpListener;
use tracing::{info, warn};

use aionui_api_types::{RuntimeStatusScope, RuntimeStatusScopeKind};
use aionui_app::{AppConfig, AppServices, RouterBuildError, create_router_with_runtime};
use aionui_system::RuntimePrepareService;
use aionui_team::TeamIdleCleanupCoordinator;

use crate::bootstrap::{BootstrapError, BootstrapErrorCode, ParentExitSignal, ServerEnvironment};

const LISTENING_EVENT_PREFIX: &str = "AIONCORE_LISTENING";
const DYNAMIC_BACKEND_BIND_MAX_ATTEMPTS: usize = 50;
/// Port selected for a dynamic listener is part of the durable local-server
/// identity.  Headless Team runners persist the Core URL alongside the Team
/// id, so choosing a new ephemeral port after a Core restart would make an
/// otherwise recoverable Team look like it belongs to another instance.
const PERSISTED_DYNAMIC_PORT_FILE: &str = ".aioncore-listen-port";
const WORKER_TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const TEAM_MAILBOX_RECOVERY_INTERVAL: Duration = Duration::from_secs(60);
const TEAM_GIT_INTEGRATION_RECOVERY_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ShutdownReason {
    Sigint,
    #[cfg(unix)]
    Sigterm,
    ParentExit,
}

#[derive(Debug)]
pub(crate) struct BoundHttpListener {
    listener: TcpListener,
    addr: SocketAddr,
}

/// Bind the main HTTP listener before constructing services that may start
/// their own local listeners. When `config.port == 0`, reuse the port selected
/// by the previous process for this data directory; the first successful
/// dynamic bind is persisted before downstream services are built. This keeps
/// a run's loopback URL stable across a Core restart without changing the
/// explicit `--port` contract.
pub(crate) async fn bind_http_listener(config: &mut AppConfig) -> Result<BoundHttpListener, BootstrapError> {
    if config.port != 0 && is_fetch_forbidden_backend_port(config.port) {
        return Err(BootstrapError::new(
            BootstrapErrorCode::ConfigInvalid,
            "config.port",
            "invalid startup configuration",
        )
        .with_field("port", config.port.to_string()));
    }

    let dynamic_port = config.port == 0;
    let persisted_port = dynamic_port.then(|| read_persisted_dynamic_port(config)).flatten();
    if let Some(port) = persisted_port {
        info!(port, path = %persisted_dynamic_port_path(config).display(), "startup: reusing persisted dynamic backend port");
        config.port = port;
    }
    let max_attempts = if dynamic_port && persisted_port.is_none() {
        DYNAMIC_BACKEND_BIND_MAX_ATTEMPTS
    } else {
        1
    };

    for attempt in 1..=max_attempts {
        let addr = config.socket_addr();
        info!(address = %addr, attempt, "startup: socket bind started");
        let listener = TcpListener::bind(&addr).await.map_err(|error| {
            BootstrapError::new(
                BootstrapErrorCode::BindFailed,
                "bind.listener",
                "failed to bind HTTP listener",
            )
            .with_source(error)
            .with_field("address", addr.to_string())
        })?;
        let local_addr = listener.local_addr().map_err(|error| {
            BootstrapError::new(
                BootstrapErrorCode::BindFailed,
                "bind.listener",
                "failed to bind HTTP listener",
            )
            .with_source(error)
        })?;

        if dynamic_port && is_fetch_forbidden_backend_port(local_addr.port()) {
            warn!(
                port = local_addr.port(),
                attempt, "startup: skipped Fetch-forbidden dynamic backend port"
            );
            continue;
        }

        config.port = local_addr.port();
        if dynamic_port {
            persist_dynamic_port(config, local_addr.port())?;
        }
        info!(address = %local_addr, "startup: socket bind completed");
        emit_listening_event(local_addr);

        return Ok(BoundHttpListener {
            listener,
            addr: local_addr,
        });
    }

    Err(BootstrapError::new(
        BootstrapErrorCode::BindFailed,
        "bind.dynamic_port",
        "failed to bind HTTP listener",
    ))
}

fn persisted_dynamic_port_path(config: &AppConfig) -> std::path::PathBuf {
    config.data_dir.join(PERSISTED_DYNAMIC_PORT_FILE)
}

fn read_persisted_dynamic_port(config: &AppConfig) -> Option<u16> {
    let path = persisted_dynamic_port_path(config);
    let raw = match std::fs::read_to_string(&path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return None,
        Err(error) => {
            warn!(path = %path.display(), error = %error, "startup: could not read persisted dynamic backend port; selecting a new port");
            return None;
        }
    };
    let port = match raw.trim().parse::<u16>() {
        Ok(port) if port != 0 && !is_fetch_forbidden_backend_port(port) => port,
        _ => {
            warn!(path = %path.display(), "startup: ignoring invalid persisted dynamic backend port");
            return None;
        }
    };
    Some(port)
}

fn persist_dynamic_port(config: &AppConfig, port: u16) -> Result<(), BootstrapError> {
    let path = persisted_dynamic_port_path(config);
    let temporary = path.with_file_name(format!(".{}.tmp-{}", PERSISTED_DYNAMIC_PORT_FILE, std::process::id()));
    std::fs::create_dir_all(&config.data_dir).map_err(|error| {
        BootstrapError::new(
            BootstrapErrorCode::BindFailed,
            "bind.dynamic_port_persist",
            "failed to persist dynamic HTTP listener port",
        )
        .with_source(error)
    })?;
    std::fs::write(&temporary, format!("{port}\n")).map_err(|error| {
        BootstrapError::new(
            BootstrapErrorCode::BindFailed,
            "bind.dynamic_port_persist",
            "failed to persist dynamic HTTP listener port",
        )
        .with_source(error)
    })?;
    if let Err(error) = std::fs::rename(&temporary, &path) {
        // Windows does not replace an existing destination with rename. The
        // target is only a small derived metadata file, so remove it and
        // complete the atomic temp-file handoff on that platform.
        if cfg!(windows) {
            std::fs::remove_file(&path).map_err(|remove_error| {
                BootstrapError::new(
                    BootstrapErrorCode::BindFailed,
                    "bind.dynamic_port_persist",
                    "failed to replace persisted dynamic HTTP listener port",
                )
                .with_source(remove_error)
            })?;
            std::fs::rename(&temporary, &path).map_err(|rename_error| {
                BootstrapError::new(
                    BootstrapErrorCode::BindFailed,
                    "bind.dynamic_port_persist",
                    "failed to persist dynamic HTTP listener port",
                )
                .with_source(rename_error)
            })?;
        } else {
            let _ = std::fs::remove_file(&temporary);
            return Err(BootstrapError::new(
                BootstrapErrorCode::BindFailed,
                "bind.dynamic_port_persist",
                "failed to persist dynamic HTTP listener port",
            )
            .with_source(error));
        }
    }
    Ok(())
}

fn is_fetch_forbidden_backend_port(port: u16) -> bool {
    matches!(
        port,
        1 | 7
            | 9
            | 11
            | 13
            | 15
            | 17
            | 19
            | 20
            | 21
            | 22
            | 23
            | 25
            | 37
            | 42
            | 43
            | 53
            | 69
            | 77
            | 79
            | 87
            | 95
            | 101
            | 102
            | 103
            | 104
            | 109
            | 110
            | 111
            | 113
            | 115
            | 117
            | 119
            | 123
            | 135
            | 137
            | 139
            | 143
            | 161
            | 179
            | 389
            | 427
            | 465
            | 512
            | 513
            | 514
            | 515
            | 526
            | 530
            | 531
            | 532
            | 540
            | 548
            | 554
            | 556
            | 563
            | 587
            | 601
            | 636
            | 989
            | 990
            | 993
            | 995
            | 1719
            | 1720
            | 1723
            | 2049
            | 3659
            | 4045
            | 5060
            | 5061
            | 6000
            | 6566
            | 6665
            | 6666
            | 6667
            | 6668
            | 6669
            | 6697
            | 10080
    )
}

fn format_listening_event(addr: SocketAddr) -> String {
    let payload = serde_json::json!({
        "host": addr.ip().to_string(),
        "port": addr.port(),
    });
    format!("{LISTENING_EVENT_PREFIX} {payload}")
}

fn emit_listening_event(addr: SocketAddr) {
    println!("{}", format_listening_event(addr));
    let _ = io::stdout().flush();
}

/// Start the HTTP server with fully constructed services.
pub(crate) async fn run_server(
    env: ServerEnvironment,
    services: AppServices,
    bound: BoundHttpListener,
    parent_exit: Option<ParentExitSignal>,
) -> Result<ExitCode, BootstrapError> {
    let boot = Instant::now();

    let has_users = services.user_repo.has_users().await.map_err(|error| {
        BootstrapError::new(
            BootstrapErrorCode::ServerFailed,
            "server.preflight",
            "server startup preflight failed",
        )
        .with_source(error)
    })?;
    if !has_users {
        info!("No configured users detected — initial setup required via /api/auth/status");
    }

    let (router, router_runtime) = create_router_with_runtime(&services)
        .await
        .map_err(router_build_error_to_bootstrap)?;
    info!(
        elapsed_ms = boot.elapsed().as_millis(),
        "startup: router ready for bound socket"
    );
    let listener = bound.listener;
    let addr = bound.addr;
    info!(elapsed_ms = boot.elapsed().as_millis(), "Server listening on {addr}");

    let runtime_prepare_service = RuntimePrepareService::new(services.event_bus.clone());
    let runtime_agent_registry = services.agent_registry.clone();
    tokio::spawn(async move {
        let scope = RuntimeStatusScope {
            kind: RuntimeStatusScopeKind::CustomAgent,
            id: "startup".into(),
        };
        let prepare_started = Instant::now();
        info!("startup: managed runtime background preparation started");
        let result = async {
            runtime_prepare_service.ensure_node_runtime(scope.clone()).await?;
            runtime_prepare_service
                .ensure_managed_acp_tool(scope.clone(), "codex-acp")
                .await?;
            runtime_prepare_service
                .ensure_managed_acp_tool(scope, "claude-agent-acp")
                .await?;
            Ok::<(), aionui_system::SystemError>(())
        }
        .await;

        match result {
            Ok(()) => {
                // The initial registry hydration can run before bundled ACP
                // tools have been materialized into the managed runtime. Make
                // those newly available commands visible without requiring a
                // process restart.
                runtime_agent_registry.refresh_availability().await;
                info!(
                    prepare_elapsed_ms = prepare_started.elapsed().as_millis(),
                    "startup: managed runtime background preparation completed"
                );
            }
            Err(error) => warn!(
                code = "BOOTSTRAP_DEGRADED_MANAGED_RUNTIME_PREPARE",
                stage = "runtime.prepare",
                prepare_elapsed_ms = prepare_started.elapsed().as_millis(),
                error = %error,
                "startup: managed runtime background preparation failed"
            ),
        }
    });

    // Kick off the idle-ACP-agent reaper. `start_idle_scanner` returns
    // immediately with a `JoinHandle`; the scanner task polls on the scan
    // interval and kills ACP agents idle beyond their timeout — solo
    // (single-chat) agents at the solo threshold, team sessions cleaned as a
    // whole at the team threshold. Thresholds and scan interval default to
    // 10 min / 30 min / 60 s and are overridable via AIONUI_IDLE_TIMEOUT_SECS,
    // AIONUI_TEAM_IDLE_TIMEOUT_SECS, and AIONUI_IDLE_SCAN_INTERVAL_SECS. The
    // watch channel propagates graceful-shutdown so the scanner exits on
    // SIGINT/SIGTERM.
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (shutdown_error_tx, shutdown_error_rx) = tokio::sync::oneshot::channel::<BootstrapError>();
    let mailbox_recovery_handle = router_runtime
        .team_service
        .start_unread_mailbox_reconciler(shutdown_rx.clone(), TEAM_MAILBOX_RECOVERY_INTERVAL);
    let git_integration_recovery_handle = router_runtime
        .team_delivery_service
        .start_pending_integration_reconciler(shutdown_rx.clone(), TEAM_GIT_INTEGRATION_RECOVERY_INTERVAL);
    let idle_cleanup_coordinator: Arc<dyn aionui_ai_agent::IdleCleanupCoordinator> =
        Arc::new(TeamIdleCleanupCoordinator::new(
            router_runtime.team_service.clone(),
            services.active_lease_registry.clone(),
        ));
    let (solo_timeout_secs, team_timeout_secs, scan_interval_secs) = aionui_ai_agent::resolve_idle_config_from_env();
    let idle_scanner_handle = aionui_ai_agent::start_idle_scanner_with_coordinator(
        services.worker_task_manager.clone(),
        shutdown_rx,
        Some(solo_timeout_secs),
        Some(team_timeout_secs),
        Some(scan_interval_secs),
        Some(idle_cleanup_coordinator),
    );
    let conversation_runtime_state = services.conversation_runtime_state.clone();
    let worker_task_manager = services.worker_task_manager.clone();

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let shutdown_result = shutdown_signal(parent_exit).await;
            let _ = shutdown_tx.send(true);
            match shutdown_result {
                Err(error) => {
                    error.log_source();
                    tracing::error!(error = %error.stderr_line(), "shutdown signal handler failed");
                    let _ = shutdown_error_tx.send(error);
                }
                Ok(reason) => {
                    match reason {
                        ShutdownReason::Sigint => info!("Received SIGINT, shutting down..."),
                        #[cfg(unix)]
                        ShutdownReason::Sigterm => info!("Received SIGTERM, shutting down..."),
                        ShutdownReason::ParentExit => info!("Detected desktop parent exit, shutting down..."),
                    }
                    let active_turn_count = conversation_runtime_state.mark_shutting_down();
                    info!(
                        reason = "graceful_shutdown",
                        active_turn_count, "conversation runtime shutdown prepared"
                    );
                    let active_task_count = worker_task_manager.active_count();
                    match tokio::time::timeout(WORKER_TASK_SHUTDOWN_TIMEOUT, worker_task_manager.clear()).await {
                        Ok(()) => info!(active_task_count, "worker task manager shutdown completed"),
                        Err(_) => warn!(active_task_count, "worker task manager shutdown timed out"),
                    }
                }
            }
        })
        .await
        .map_err(|error| {
            BootstrapError::new(
                BootstrapErrorCode::ServerFailed,
                "server.serve",
                "server runtime failed",
            )
            .with_source(error)
        })?;

    let shutdown_error = shutdown_error_rx.await.ok();

    // Wait for the scanner to observe the shutdown watch value and
    // return; at worst this blocks for the current 60 s tick.
    if let Err(e) = idle_scanner_handle.await {
        warn!(
            code = "BOOTSTRAP_DEGRADED_IDLE_SCANNER",
            stage = "idle_scanner.join",
            error = %e,
            "idle scanner join failed"
        );
    }
    if let Err(error) = mailbox_recovery_handle.await {
        warn!(
            code = "BOOTSTRAP_DEGRADED_TEAM_MAILBOX_RECOVERY",
            stage = "team_mailbox_recovery.join",
            error = %error,
            "Team mailbox recovery reconciler join failed"
        );
    }
    if let Err(error) = git_integration_recovery_handle.await {
        warn!(
            code = "BOOTSTRAP_DEGRADED_TEAM_GIT_INTEGRATION_RECOVERY",
            stage = "team_git_integration_recovery.join",
            error = %error,
            "Team Git integration recovery reconciler join failed"
        );
    }

    services.database.close().await;
    info!("Server shut down gracefully");

    // Prevent the log guard from being dropped before final log flush.
    drop(env);

    finish_server_shutdown(shutdown_error)
}

fn router_build_error_to_bootstrap(error: RouterBuildError) -> BootstrapError {
    let stage = error.stage();
    let message = error.message();
    BootstrapError::new(BootstrapErrorCode::ServerFailed, stage, message).with_source(error)
}

fn finish_server_shutdown(shutdown_error: Option<BootstrapError>) -> Result<ExitCode, BootstrapError> {
    if let Some(error) = shutdown_error {
        return Err(error);
    }

    Ok(ExitCode::SUCCESS)
}

type ShutdownFuture = Pin<Box<dyn Future<Output = Result<ShutdownReason, BootstrapError>> + Send>>;

async fn shutdown_signal(parent_exit: Option<ParentExitSignal>) -> Result<ShutdownReason, BootstrapError> {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.map_err(|error| {
            BootstrapError::new(
                BootstrapErrorCode::ShutdownFailed,
                "shutdown.signal_handler",
                "failed to install shutdown signal handler",
            )
            .with_source(error)
        })?;
        Ok::<ShutdownReason, BootstrapError>(ShutdownReason::Sigint)
    };

    #[cfg(unix)]
    let terminate = async {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).map_err(|error| {
                BootstrapError::new(
                    BootstrapErrorCode::ShutdownFailed,
                    "shutdown.signal_handler",
                    "failed to install shutdown signal handler",
                )
                .with_source(error)
            })?;
        terminate.recv().await;
        Ok::<ShutdownReason, BootstrapError>(ShutdownReason::Sigterm)
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<Result<ShutdownReason, BootstrapError>>();

    let parent_exit: ShutdownFuture = match parent_exit {
        Some(signal) => Box::pin(async move {
            signal.await;
            Ok(ShutdownReason::ParentExit)
        }),
        None => Box::pin(std::future::pending()),
    };

    tokio::select! {
        result = ctrl_c => result,
        result = terminate => result,
        result = parent_exit => result,
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use aionui_app::AppConfig;

    use super::*;

    #[test]
    fn listening_event_line_is_machine_readable() {
        let addr: SocketAddr = "127.0.0.1:49153".parse().unwrap();

        let line = format_listening_event(addr);

        let payload = line
            .strip_prefix("AIONCORE_LISTENING ")
            .expect("line should start with the listening event prefix");
        let parsed: serde_json::Value = serde_json::from_str(payload).expect("payload should be valid JSON");
        assert_eq!(parsed["host"], "127.0.0.1");
        assert_eq!(parsed["port"], 49153);
    }

    #[test]
    fn fetch_forbidden_backend_ports_are_rejected() {
        assert!(is_fetch_forbidden_backend_port(1720));
        assert!(is_fetch_forbidden_backend_port(10080));
        assert!(!is_fetch_forbidden_backend_port(49153));
    }

    #[tokio::test]
    async fn bind_http_listener_updates_dynamic_port_config() {
        let data_dir = tempfile::tempdir().expect("temporary data dir");
        let mut config = AppConfig {
            port: 0,
            data_dir: data_dir.path().to_path_buf(),
            ..AppConfig::default()
        };

        let bound = bind_http_listener(&mut config).await.expect("bind should succeed");

        assert!(config.port > 0);
        assert_eq!(config.port, bound.addr.port());
        assert_eq!(
            std::fs::read_to_string(persisted_dynamic_port_path(&config)).unwrap(),
            format!("{}\n", config.port)
        );
    }

    #[tokio::test]
    async fn dynamic_listener_reuses_port_after_core_restart() {
        let data_dir = tempfile::tempdir().expect("temporary data dir");
        let mut first = AppConfig {
            port: 0,
            data_dir: data_dir.path().to_path_buf(),
            ..AppConfig::default()
        };
        let first_bound = bind_http_listener(&mut first).await.expect("first bind should succeed");
        let first_port = first_bound.addr.port();
        drop(first_bound);

        let mut restarted = AppConfig {
            port: 0,
            data_dir: data_dir.path().to_path_buf(),
            ..AppConfig::default()
        };
        let restarted_bound = bind_http_listener(&mut restarted)
            .await
            .expect("restart should reuse the persisted port");

        assert_eq!(restarted.port, first_port);
        assert_eq!(restarted_bound.addr.port(), first_port);
    }

    #[tokio::test]
    async fn invalid_persisted_dynamic_port_is_replaced() {
        let data_dir = tempfile::tempdir().expect("temporary data dir");
        std::fs::write(data_dir.path().join(PERSISTED_DYNAMIC_PORT_FILE), "not-a-port\n")
            .expect("write invalid metadata");
        let mut config = AppConfig {
            port: 0,
            data_dir: data_dir.path().to_path_buf(),
            ..AppConfig::default()
        };

        let bound = bind_http_listener(&mut config).await.expect("bind should succeed");

        assert!(config.port > 0);
        assert_eq!(
            std::fs::read_to_string(persisted_dynamic_port_path(&config)).unwrap(),
            format!("{}\n", bound.addr.port())
        );
    }

    #[tokio::test]
    async fn parent_exit_signal_triggers_shutdown() {
        let reason = shutdown_signal(Some(Box::pin(std::future::ready(()))))
            .await
            .expect("parent exit should shut down cleanly");

        assert_eq!(reason, ShutdownReason::ParentExit);
    }

    #[tokio::test]
    async fn forbidden_backend_port_maps_to_bootstrap_config_invalid() {
        let mut config = AppConfig {
            port: 1720,
            ..AppConfig::default()
        };

        let err = bind_http_listener(&mut config).await.unwrap_err();
        assert_eq!(err.code(), crate::bootstrap::BootstrapErrorCode::ConfigInvalid);
        assert_eq!(err.stage(), "config.port");
        assert_eq!(err.exit_code(), std::process::ExitCode::from(2));
        assert!(
            err.stderr_line()
                .starts_with("BOOTSTRAP_CONFIG_INVALID stage=config.port")
        );
    }

    #[test]
    fn graceful_shutdown_returns_signal_error_when_serve_succeeds() {
        let error = BootstrapError::new(
            BootstrapErrorCode::ShutdownFailed,
            "shutdown.signal_handler",
            "failed to install shutdown signal handler",
        )
        .with_source(anyhow::anyhow!("raw shutdown source"));

        let err = finish_server_shutdown(Some(error)).unwrap_err();

        assert_eq!(err.code(), BootstrapErrorCode::ShutdownFailed);
        assert_eq!(err.stage(), "shutdown.signal_handler");
        assert!(
            err.stderr_line()
                .starts_with("BOOTSTRAP_SHUTDOWN_FAILED stage=shutdown.signal_handler")
        );
        assert!(!err.stderr_line().contains("raw shutdown source"));
    }

    #[test]
    fn router_build_error_maps_to_bootstrap_server_failed() {
        let err = router_build_error_to_bootstrap(
            RouterBuildError::new("router.file_watch", "failed to initialize file watch service")
                .with_source(anyhow::anyhow!("raw watch backend unavailable")),
        );

        assert_eq!(err.code(), BootstrapErrorCode::ServerFailed);
        assert_eq!(err.stage(), "router.file_watch");
        assert!(
            err.stderr_line()
                .starts_with("BOOTSTRAP_SERVER_FAILED stage=router.file_watch")
        );
        assert!(!err.stderr_line().contains("raw watch backend unavailable"));
    }
}
