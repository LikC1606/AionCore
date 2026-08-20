use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use aionui_db::models::TeamRow;
use aionui_db::{DbError, ITeamModeRepository, ITeamRepository, TeamGitIntegrationAttemptRow};
use thiserror::Error;
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use super::team_command::{
    ConflictedIntegrationEvidence, MergedIntegrationEvidence, RetryableIntegrationEvidence, TeamCommand,
    TeamCommandError, TeamCommandPrincipal, TeamCommandReceipt, TeamCommandService,
};
use crate::kernel::IntegrationRecoveryReason;
use crate::ports::{
    GitDeliveryPortError, GitDeliveryPortOutcome, GitDeliveryReconciliation, GitIntegrationIntent,
    GitWorkspacePortError, TeamGitDeliveryPort, TeamGitWorkspacePort,
};

const PUBLIC_IDEMPOTENCY_KEY_MAX_LEN: usize = 220;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginGitIntegrationRequest {
    pub work_item_id: String,
    pub expected_work_revision: u64,
    pub expected_delivery_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TeamIntegrationResolution {
    Merged,
    Conflicted,
    Retryable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TeamIntegrationReceipt {
    pub attempt_id: String,
    pub work_item_id: String,
    pub delivery_id: String,
    pub resolution: TeamIntegrationResolution,
    pub merged_commit: Option<String>,
    pub observed_target_head: Option<String>,
    pub recovery_reason: Option<IntegrationRecoveryReason>,
}

#[derive(Debug, Error)]
pub enum TeamDeliveryError {
    #[error("Team not found: {0}")]
    TeamNotFound(String),
    #[error("authenticated user does not own this Team")]
    ForbiddenTeam,
    #[error("authenticated Team member cannot integrate this WorkItem: {0}")]
    ForbiddenActor(String),
    #[error("Git integration attempt not found: {0}")]
    AttemptNotFound(String),
    #[error("invalid Git integration request: {0}")]
    InvalidRequest(String),
    #[error("Git integration state changed concurrently: {0}")]
    Conflict(String),
    #[error("durable Git integration state is corrupt")]
    CorruptState,
    #[error(transparent)]
    Port(#[from] GitDeliveryPortError),
    #[error(transparent)]
    Workspace(#[from] GitWorkspacePortError),
    #[error(transparent)]
    Database(#[from] DbError),
}

pub struct TeamDeliveryService {
    team_repo: Arc<dyn ITeamRepository>,
    mode_repo: Arc<dyn ITeamModeRepository>,
    command_service: Arc<TeamCommandService>,
    port: Arc<dyn TeamGitDeliveryPort>,
    workspace_port: Arc<dyn TeamGitWorkspacePort>,
    attempt_locks: StdMutex<HashMap<String, Weak<AsyncMutex<()>>>>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct GitIntegrationRecoveryReport {
    pub scanned_team_count: usize,
    pub pending_attempt_count: usize,
    pub merged_attempt_count: usize,
    pub conflicted_attempt_count: usize,
    pub retryable_attempt_count: usize,
    pub failed_team_count: usize,
    pub failed_attempt_count: usize,
}

impl GitIntegrationRecoveryReport {
    pub fn resolved_attempt_count(&self) -> usize {
        self.merged_attempt_count + self.conflicted_attempt_count + self.retryable_attempt_count
    }
}

#[derive(Clone, Copy)]
enum DeliveryPrincipal<'a> {
    Owner {
        authenticated_user_id: &'a str,
        team_id: &'a str,
    },
    Member(&'a TeamCommandPrincipal),
}

impl<'a> DeliveryPrincipal<'a> {
    fn authenticated_user_id(self) -> &'a str {
        match self {
            Self::Owner {
                authenticated_user_id, ..
            } => authenticated_user_id,
            Self::Member(principal) => principal.authenticated_user_id(),
        }
    }

    fn team_id(self) -> &'a str {
        match self {
            Self::Owner { team_id, .. } => team_id,
            Self::Member(principal) => principal.team_id(),
        }
    }
}

impl TeamDeliveryService {
    pub fn new(
        team_repo: Arc<dyn ITeamRepository>,
        mode_repo: Arc<dyn ITeamModeRepository>,
        command_service: Arc<TeamCommandService>,
        port: Arc<dyn TeamGitDeliveryPort>,
        workspace_port: Arc<dyn TeamGitWorkspacePort>,
    ) -> Self {
        Self {
            team_repo,
            mode_repo,
            command_service,
            port,
            workspace_port,
            attempt_locks: StdMutex::new(HashMap::new()),
        }
    }

    /// Reconcile every durable pending Git integration attempt once.
    ///
    /// Each Team and attempt is isolated so one unavailable repository cannot
    /// prevent recovery of unrelated deliveries. The attempt row remains the
    /// sole durable truth; a failed reconciliation is retried by the next scan.
    pub async fn reconcile_pending_once(&self) -> Result<GitIntegrationRecoveryReport, TeamDeliveryError> {
        let teams = self.team_repo.list_teams().await?;
        let mut report = GitIntegrationRecoveryReport::default();

        for team in teams {
            report.scanned_team_count += 1;
            let attempt_ids = match self.pending_attempt_ids_as_owner(&team.user_id, &team.id).await {
                Ok(attempt_ids) => attempt_ids,
                Err(error) => {
                    report.failed_team_count += 1;
                    warn!(
                        team_id = %team.id,
                        error = %error,
                        "pending Team Git integration lookup failed; continuing recovery scan"
                    );
                    continue;
                }
            };

            report.pending_attempt_count += attempt_ids.len();
            for attempt_id in attempt_ids {
                let idempotency_key = recovery_idempotency_key(&attempt_id);
                match self
                    .reconcile_as_owner(&team.user_id, &team.id, &attempt_id, &idempotency_key)
                    .await
                {
                    Ok(receipt) => match receipt.resolution {
                        TeamIntegrationResolution::Merged => report.merged_attempt_count += 1,
                        TeamIntegrationResolution::Conflicted => report.conflicted_attempt_count += 1,
                        TeamIntegrationResolution::Retryable => report.retryable_attempt_count += 1,
                    },
                    Err(error) => {
                        report.failed_attempt_count += 1;
                        warn!(
                            team_id = %team.id,
                            attempt_id,
                            error = %error,
                            "pending Team Git integration reconciliation failed; continuing recovery scan"
                        );
                    }
                }
            }
        }

        if report.pending_attempt_count > 0 || report.failed_team_count > 0 {
            info!(
                scanned_team_count = report.scanned_team_count,
                pending_attempt_count = report.pending_attempt_count,
                resolved_attempt_count = report.resolved_attempt_count(),
                merged_attempt_count = report.merged_attempt_count,
                conflicted_attempt_count = report.conflicted_attempt_count,
                retryable_attempt_count = report.retryable_attempt_count,
                failed_team_count = report.failed_team_count,
                failed_attempt_count = report.failed_attempt_count,
                "pending Team Git integration recovery scan completed"
            );
        }
        Ok(report)
    }

    /// Start one immediate-then-periodic recovery loop. Scans are awaited in a
    /// single task and missed ticks are skipped, so reconciliation never overlaps.
    pub fn start_pending_integration_reconciler(
        self: &Arc<Self>,
        mut shutdown_rx: watch::Receiver<bool>,
        scan_interval: Duration,
    ) -> JoinHandle<()> {
        let service = Arc::downgrade(self);
        tokio::spawn(async move {
            if scan_interval.is_zero() {
                warn!("pending Team Git integration reconciler disabled because scan interval is zero");
                return;
            }
            if *shutdown_rx.borrow() {
                return;
            }

            let mut interval = tokio::time::interval(scan_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            info!(
                scan_interval_secs = scan_interval.as_secs(),
                "pending Team Git integration reconciler started"
            );

            loop {
                tokio::select! {
                    biased;
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                        let Some(service) = service.upgrade() else {
                            break;
                        };
                        if let Err(error) = service.reconcile_pending_once().await {
                            warn!(
                                error = %error,
                                "pending Team Git integration recovery scan failed; retrying on the next scan"
                            );
                        }
                    }
                }
            }
            info!("pending Team Git integration reconciler stopped");
        })
    }

    /// Persists exact intent before invoking the Git adapter.
    ///
    /// A replay that finds a pending attempt reconciles before retrying the
    /// side effect. A replay that finds a resolved attempt returns its durable
    /// outcome without calling the adapter.
    pub async fn integrate_as_owner(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
        idempotency_key: &str,
        request: BeginGitIntegrationRequest,
    ) -> Result<TeamIntegrationReceipt, TeamDeliveryError> {
        self.integrate(
            DeliveryPrincipal::Owner {
                authenticated_user_id,
                team_id,
            },
            idempotency_key,
            request,
        )
        .await
    }

    pub(crate) async fn integrate_as_member(
        &self,
        principal: &TeamCommandPrincipal,
        idempotency_key: &str,
        request: BeginGitIntegrationRequest,
    ) -> Result<TeamIntegrationReceipt, TeamDeliveryError> {
        self.integrate(DeliveryPrincipal::Member(principal), idempotency_key, request)
            .await
    }

    async fn integrate(
        &self,
        principal: DeliveryPrincipal<'_>,
        idempotency_key: &str,
        request: BeginGitIntegrationRequest,
    ) -> Result<TeamIntegrationReceipt, TeamDeliveryError> {
        validate_idempotency_key(idempotency_key)?;
        let team_id = principal.team_id();
        let begin_key = derived_key(idempotency_key, "begin");
        let prior_begin = self
            .find_receipt(principal, &begin_key, "begin_integration")
            .await
            .map_err(map_command_error)?;
        if prior_begin.is_none() {
            let mut pending = self
                .mode_repo
                .list_pending_git_integration_attempts(team_id)
                .await?
                .into_iter()
                .filter(|attempt| attempt.work_item_id == request.work_item_id);
            if let Some(attempt) = pending.next() {
                if pending.next().is_some() {
                    return Err(TeamDeliveryError::CorruptState);
                }
                return self.reconcile(principal, &attempt.attempt_id, idempotency_key).await;
            }
            self.authorize_integration(principal, &request.work_item_id)
                .await
                .map_err(map_command_error)?;
        }
        let (target_ref, target_head) = if let Some(prior) = prior_begin {
            let prior_delivery = prior.result.delivery.ok_or(TeamDeliveryError::CorruptState)?;
            let attempt_id = prior_delivery
                .integration_attempt_id
                .ok_or(TeamDeliveryError::CorruptState)?;
            let attempt = self.load_attempt(team_id, &attempt_id).await?;
            (attempt.target_ref, attempt.target_head)
        } else {
            let team = self.require_owner(principal.authenticated_user_id(), team_id).await?;
            let delivery = self
                .mode_repo
                .list_git_deliveries(team_id, Some(&request.work_item_id))
                .await?
                .into_iter()
                .find(|delivery| {
                    delivery.state == "accepted"
                        && u64::try_from(delivery.revision).ok() == Some(request.expected_delivery_revision)
                })
                .ok_or_else(|| TeamDeliveryError::Conflict("accepted delivery revision changed".into()))?;
            let target = self
                .workspace_port
                .resolve_integration_target(&team.workspace, &delivery.repository_id)
                .await?;
            (target.branch_ref().to_owned(), target.head_commit().to_owned())
        };
        let begin = self
            .execute_command(
                principal,
                &begin_key,
                TeamCommand::BeginIntegration {
                    work_item_id: request.work_item_id,
                    expected_work_revision: request.expected_work_revision,
                    expected_delivery_revision: request.expected_delivery_revision,
                    target_ref,
                    target_head,
                },
            )
            .await
            .map_err(map_command_error)?;
        let delivery = begin.result.delivery.as_ref().ok_or(TeamDeliveryError::CorruptState)?;
        let attempt_id = delivery
            .integration_attempt_id
            .as_deref()
            .ok_or(TeamDeliveryError::CorruptState)?;
        let attempt_lock = self.attempt_lock(attempt_id);
        let _attempt_guard = attempt_lock.lock().await;
        let attempt = self.load_attempt(team_id, attempt_id).await?;
        validate_attempt_receipt(&attempt, &begin.result.work_item_id, &delivery.delivery_id)?;
        if attempt.state != "pending" {
            return receipt_from_resolved_attempt(&attempt);
        }

        info!(
            team_id,
            work_item_id = %attempt.work_item_id,
            delivery_id = %attempt.delivery_id,
            attempt_id = %attempt.attempt_id,
            replayed = begin.replayed,
            "driving durable Team Git integration attempt"
        );
        let intent = intent_from_row(&attempt)?;
        let outcome = if begin.replayed {
            match self.port.reconcile(&intent).await {
                Ok(GitDeliveryReconciliation::Resolved(outcome)) => outcome,
                Ok(GitDeliveryReconciliation::ReadyToIntegrate) => self.port.integrate(&intent).await?,
                Err(error) => {
                    warn!(team_id, attempt_id = %attempt.attempt_id, "Git integration reconcile left attempt pending");
                    return Err(error.into());
                }
            }
        } else {
            match self.port.integrate(&intent).await {
                Ok(outcome) => outcome,
                Err(error) => {
                    warn!(team_id, attempt_id = %attempt.attempt_id, "Git integration execution left attempt pending");
                    return Err(error.into());
                }
            }
        };
        self.resolve(principal, &derived_key(idempotency_key, "resolve"), &attempt, outcome)
            .await
    }

    /// Reconciles one pending attempt loaded from durable state.
    pub async fn reconcile_as_owner(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
        attempt_id: &str,
        idempotency_key: &str,
    ) -> Result<TeamIntegrationReceipt, TeamDeliveryError> {
        self.reconcile(
            DeliveryPrincipal::Owner {
                authenticated_user_id,
                team_id,
            },
            attempt_id,
            idempotency_key,
        )
        .await
    }

    async fn reconcile(
        &self,
        principal: DeliveryPrincipal<'_>,
        attempt_id: &str,
        idempotency_key: &str,
    ) -> Result<TeamIntegrationReceipt, TeamDeliveryError> {
        validate_idempotency_key(idempotency_key)?;
        let attempt_lock = self.attempt_lock(attempt_id);
        let _attempt_guard = attempt_lock.lock().await;
        let team_id = principal.team_id();
        let attempt = self.load_attempt(team_id, attempt_id).await?;
        if attempt.state != "pending" {
            return receipt_from_resolved_attempt(&attempt);
        }
        self.authorize_integration(principal, &attempt.work_item_id)
            .await
            .map_err(map_command_error)?;
        let intent = intent_from_row(&attempt)?;
        let outcome = match self.port.reconcile(&intent).await? {
            GitDeliveryReconciliation::Resolved(outcome) => outcome,
            GitDeliveryReconciliation::ReadyToIntegrate => self.port.integrate(&intent).await?,
        };
        self.resolve(principal, &derived_key(idempotency_key, "resolve"), &attempt, outcome)
            .await
    }

    fn attempt_lock(&self, attempt_id: &str) -> Arc<AsyncMutex<()>> {
        let mut locks = self
            .attempt_locks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(lock) = locks.get(attempt_id).and_then(Weak::upgrade) {
            return lock;
        }

        locks.retain(|_, lock| lock.strong_count() > 0);
        let lock = Arc::new(AsyncMutex::new(()));
        locks.insert(attempt_id.to_owned(), Arc::downgrade(&lock));
        lock
    }

    /// Returns only opaque IDs so startup code can reconcile without exposing
    /// repository coordinates outside the service/port boundary.
    pub async fn pending_attempt_ids_as_owner(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
    ) -> Result<Vec<String>, TeamDeliveryError> {
        self.require_owner(authenticated_user_id, team_id).await?;
        Ok(self
            .mode_repo
            .list_pending_git_integration_attempts(team_id)
            .await?
            .into_iter()
            .map(|attempt| attempt.attempt_id)
            .collect())
    }

    async fn resolve(
        &self,
        principal: DeliveryPrincipal<'_>,
        idempotency_key: &str,
        attempt: &TeamGitIntegrationAttemptRow,
        outcome: GitDeliveryPortOutcome,
    ) -> Result<TeamIntegrationReceipt, TeamDeliveryError> {
        let team_id = principal.team_id();
        let work = self
            .mode_repo
            .get_work_item(team_id, &attempt.work_item_id)
            .await?
            .ok_or(TeamDeliveryError::CorruptState)?;
        let delivery = self
            .mode_repo
            .get_git_delivery(team_id, &attempt.delivery_id)
            .await?
            .ok_or(TeamDeliveryError::CorruptState)?;
        if delivery.state != "integrating" {
            let current = self.load_attempt(team_id, &attempt.attempt_id).await?;
            return receipt_from_resolved_attempt(&current);
        }
        let expected_work_revision = u64::try_from(work.revision).map_err(|_| TeamDeliveryError::CorruptState)?;
        let expected_delivery_revision =
            u64::try_from(delivery.revision).map_err(|_| TeamDeliveryError::CorruptState)?;
        let command = match outcome {
            GitDeliveryPortOutcome::Merged {
                merged_commit,
                observed_target_head,
            } => {
                require_non_empty("merged_commit", &merged_commit)?;
                require_non_empty("observed_target_head", &observed_target_head)?;
                TeamCommand::ResolveIntegrationMerged {
                    work_item_id: attempt.work_item_id.clone(),
                    expected_work_revision,
                    expected_delivery_revision,
                    evidence: MergedIntegrationEvidence {
                        attempt_id: attempt.attempt_id.clone(),
                        merged_commit,
                        observed_target_head,
                    },
                }
            }
            GitDeliveryPortOutcome::Conflicted { observed_target_head } => {
                require_non_empty("observed_target_head", &observed_target_head)?;
                TeamCommand::ResolveIntegrationConflict {
                    work_item_id: attempt.work_item_id.clone(),
                    expected_work_revision,
                    expected_delivery_revision,
                    evidence: ConflictedIntegrationEvidence {
                        attempt_id: attempt.attempt_id.clone(),
                        observed_target_head,
                    },
                }
            }
            GitDeliveryPortOutcome::Retryable {
                reason,
                observed_target_head,
            } => {
                if reason == IntegrationRecoveryReason::PreconditionChanged && observed_target_head.is_none() {
                    return Err(TeamDeliveryError::InvalidRequest(
                        "precondition-changed evidence requires observed_target_head".into(),
                    ));
                }
                if let Some(head) = observed_target_head.as_deref() {
                    require_non_empty("observed_target_head", head)?;
                }
                TeamCommand::ResolveIntegrationRetryable {
                    work_item_id: attempt.work_item_id.clone(),
                    expected_work_revision,
                    expected_delivery_revision,
                    evidence: RetryableIntegrationEvidence {
                        attempt_id: attempt.attempt_id.clone(),
                        reason,
                        observed_target_head,
                    },
                }
            }
        };

        match self.execute_command(principal, idempotency_key, command).await {
            Ok(_) => {}
            Err(TeamCommandError::RevisionConflict { .. }) => {
                let current = self.load_attempt(team_id, &attempt.attempt_id).await?;
                if current.state != "pending" {
                    return receipt_from_resolved_attempt(&current);
                }
                return Err(TeamDeliveryError::Conflict("delivery revision changed".into()));
            }
            Err(error) => return Err(map_command_error(error)),
        }
        let resolved = self.load_attempt(team_id, &attempt.attempt_id).await?;
        let receipt = receipt_from_resolved_attempt(&resolved)?;
        info!(
            team_id,
            work_item_id = %resolved.work_item_id,
            delivery_id = %resolved.delivery_id,
            attempt_id = %resolved.attempt_id,
            resolution = ?receipt.resolution,
            "resolved durable Team Git integration attempt"
        );
        Ok(receipt)
    }

    async fn find_receipt(
        &self,
        principal: DeliveryPrincipal<'_>,
        idempotency_key: &str,
        expected_command_name: &str,
    ) -> Result<Option<TeamCommandReceipt>, TeamCommandError> {
        match principal {
            DeliveryPrincipal::Owner {
                authenticated_user_id,
                team_id,
            } => {
                self.command_service
                    .find_receipt_as_owner(authenticated_user_id, team_id, idempotency_key, expected_command_name)
                    .await
            }
            DeliveryPrincipal::Member(principal) => {
                self.command_service
                    .find_receipt(principal, idempotency_key, expected_command_name)
                    .await
            }
        }
    }

    async fn execute_command(
        &self,
        principal: DeliveryPrincipal<'_>,
        idempotency_key: &str,
        command: TeamCommand,
    ) -> Result<TeamCommandReceipt, TeamCommandError> {
        match principal {
            DeliveryPrincipal::Owner {
                authenticated_user_id,
                team_id,
            } => {
                self.command_service
                    .execute_as_owner(authenticated_user_id, team_id, idempotency_key, command)
                    .await
            }
            DeliveryPrincipal::Member(principal) => {
                self.command_service.execute(principal, idempotency_key, command).await
            }
        }
    }

    async fn authorize_integration(
        &self,
        principal: DeliveryPrincipal<'_>,
        work_item_id: &str,
    ) -> Result<(), TeamCommandError> {
        match principal {
            DeliveryPrincipal::Owner {
                authenticated_user_id,
                team_id,
            } => {
                self.command_service
                    .authorize_integration_as_owner(authenticated_user_id, team_id, work_item_id)
                    .await
            }
            DeliveryPrincipal::Member(principal) => {
                self.command_service
                    .authorize_integration(principal, work_item_id)
                    .await
            }
        }
    }

    async fn require_owner(&self, authenticated_user_id: &str, team_id: &str) -> Result<TeamRow, TeamDeliveryError> {
        let team = self
            .team_repo
            .get_team(team_id)
            .await?
            .ok_or_else(|| TeamDeliveryError::TeamNotFound(team_id.to_owned()))?;
        if team.user_id != authenticated_user_id {
            return Err(TeamDeliveryError::ForbiddenTeam);
        }
        Ok(team)
    }

    async fn load_attempt(
        &self,
        team_id: &str,
        attempt_id: &str,
    ) -> Result<TeamGitIntegrationAttemptRow, TeamDeliveryError> {
        self.mode_repo
            .get_git_integration_attempt(team_id, attempt_id)
            .await?
            .ok_or_else(|| TeamDeliveryError::AttemptNotFound(attempt_id.to_owned()))
    }
}

fn validate_idempotency_key(key: &str) -> Result<(), TeamDeliveryError> {
    if key.trim().is_empty() || key.len() > PUBLIC_IDEMPOTENCY_KEY_MAX_LEN || key.chars().any(char::is_control) {
        return Err(TeamDeliveryError::InvalidRequest(format!(
            "idempotency key must contain 1..={PUBLIC_IDEMPOTENCY_KEY_MAX_LEN} bytes and no control characters"
        )));
    }
    Ok(())
}

fn derived_key(key: &str, step: &str) -> String {
    format!("{key}:{step}")
}

fn recovery_idempotency_key(attempt_id: &str) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(attempt_id.as_bytes());
    format!("system:git-integration-recovery:{digest:x}")
}

fn validate_attempt_receipt(
    attempt: &TeamGitIntegrationAttemptRow,
    work_item_id: &str,
    delivery_id: &str,
) -> Result<(), TeamDeliveryError> {
    if attempt.work_item_id != work_item_id || attempt.delivery_id != delivery_id {
        return Err(TeamDeliveryError::CorruptState);
    }
    Ok(())
}

fn intent_from_row(row: &TeamGitIntegrationAttemptRow) -> Result<GitIntegrationIntent, TeamDeliveryError> {
    for value in [
        row.attempt_id.as_str(),
        row.team_id.as_str(),
        row.work_item_id.as_str(),
        row.delivery_id.as_str(),
        row.repository_id.as_str(),
        row.base_commit.as_str(),
        row.source_ref.as_str(),
        row.source_head.as_str(),
        row.target_ref.as_str(),
        row.target_head.as_str(),
    ] {
        if value.trim().is_empty() {
            return Err(TeamDeliveryError::CorruptState);
        }
    }
    Ok(GitIntegrationIntent::new(
        &row.attempt_id,
        &row.team_id,
        &row.work_item_id,
        &row.delivery_id,
        &row.repository_id,
        &row.base_commit,
        &row.source_ref,
        &row.source_head,
        &row.target_ref,
        &row.target_head,
    ))
}

fn receipt_from_resolved_attempt(
    attempt: &TeamGitIntegrationAttemptRow,
) -> Result<TeamIntegrationReceipt, TeamDeliveryError> {
    let (resolution, recovery_reason) = match attempt.state.as_str() {
        "merged" if attempt.merged_commit.as_deref().is_some_and(|value| !value.is_empty()) => {
            (TeamIntegrationResolution::Merged, None)
        }
        "conflicted" => (TeamIntegrationResolution::Conflicted, None),
        "retryable" => (
            TeamIntegrationResolution::Retryable,
            Some(parse_recovery_reason(
                attempt
                    .recovery_reason
                    .as_deref()
                    .ok_or(TeamDeliveryError::CorruptState)?,
            )?),
        ),
        _ => return Err(TeamDeliveryError::CorruptState),
    };
    Ok(TeamIntegrationReceipt {
        attempt_id: attempt.attempt_id.clone(),
        work_item_id: attempt.work_item_id.clone(),
        delivery_id: attempt.delivery_id.clone(),
        resolution,
        merged_commit: attempt.merged_commit.clone(),
        observed_target_head: attempt.observed_target_head.clone(),
        recovery_reason,
    })
}

fn parse_recovery_reason(value: &str) -> Result<IntegrationRecoveryReason, TeamDeliveryError> {
    match value {
        "interrupted" => Ok(IntegrationRecoveryReason::Interrupted),
        "retryable_infrastructure" => Ok(IntegrationRecoveryReason::RetryableInfrastructure),
        "precondition_changed" => Ok(IntegrationRecoveryReason::PreconditionChanged),
        _ => Err(TeamDeliveryError::CorruptState),
    }
}

fn require_non_empty(field: &str, value: &str) -> Result<(), TeamDeliveryError> {
    if value.trim().is_empty() {
        Err(TeamDeliveryError::InvalidRequest(format!(
            "port evidence {field} must not be empty"
        )))
    } else {
        Ok(())
    }
}

fn map_command_error(error: TeamCommandError) -> TeamDeliveryError {
    match error {
        TeamCommandError::TeamNotFound(id) => TeamDeliveryError::TeamNotFound(id),
        TeamCommandError::ForbiddenTeam => TeamDeliveryError::ForbiddenTeam,
        TeamCommandError::CallerNotMember
        | TeamCommandError::MemberNotFound(_)
        | TeamCommandError::RelationPolicy(_)
        | TeamCommandError::WorkItemPolicy(_) => TeamDeliveryError::ForbiddenActor(error.to_string()),
        TeamCommandError::InvalidCommand(message) => TeamDeliveryError::InvalidRequest(message),
        TeamCommandError::WorkItemNotFound(id) => TeamDeliveryError::Conflict(format!("WorkItem not found: {id}")),
        TeamCommandError::IdempotencyConflict
        | TeamCommandError::GitContentRevisionAlreadyUsed(_)
        | TeamCommandError::RosterChanged
        | TeamCommandError::ConcurrentSnapshotChange
        | TeamCommandError::RevisionConflict { .. }
        | TeamCommandError::WorkItemTransition(_)
        | TeamCommandError::GitDeliveryTransition(_) => TeamDeliveryError::Conflict(error.to_string()),
        TeamCommandError::InvalidParentWorkItem(_)
        | TeamCommandError::ExpectedDeliveryRevisionRequired
        | TeamCommandError::UnexpectedDeliveryRevision
        | TeamCommandError::GitDeliveryRequired
        | TeamCommandError::CorruptAggregate(_)
        | TeamCommandError::CorruptReceipt(_)
        | TeamCommandError::Roster(_)
        | TeamCommandError::Snapshot(_)
        | TeamCommandError::Database(_)
        | TeamCommandError::Json(_) => TeamDeliveryError::CorruptState,
    }
}
