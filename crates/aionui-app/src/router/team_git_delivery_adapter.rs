use std::fs;
use std::path::PathBuf;

use aionui_file::git_delivery::{
    ExactRepository, GitDeliveryAdapter, GitDeliveryError, IntegrateGitDelivery, IntegrationOutcome,
    MergeCommitIdentity, ReconciliationOutcome,
};
use aionui_team::kernel::{GitWorkAssignment, IntegrationRecoveryReason};
use aionui_team::{
    GitDeliveryPortError, GitDeliveryPortOutcome, GitDeliveryReconciliation, GitIntegrationIntent,
    GitIntegrationTarget, GitWorkAssignmentPlan, GitWorkspacePortError, PreparedGitWorkAssignment, TeamGitDeliveryPort,
    TeamGitWorkspacePort,
};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug)]
pub(super) struct LocalTeamGitDeliveryPort {
    worktree_root: PathBuf,
    adapter: GitDeliveryAdapter,
}

impl LocalTeamGitDeliveryPort {
    pub(super) fn new(worktree_root: PathBuf, managed_runtime_skill_source_roots: Vec<PathBuf>) -> Self {
        Self {
            worktree_root,
            adapter: GitDeliveryAdapter::with_managed_runtime_skill_sources(managed_runtime_skill_source_roots),
        }
    }

    fn exact_delivery(
        adapter: &GitDeliveryAdapter,
        intent: &GitIntegrationIntent,
    ) -> Result<aionui_file::git_delivery::ExactGitDelivery, GitDeliveryPortError> {
        let repository = ExactRepository {
            repository_id: intent.repository_id().to_owned(),
            root: PathBuf::from(intent.repository_id()),
        };
        adapter
            .resolve_exact_delivery(
                repository,
                intent.base_commit(),
                intent.source_ref(),
                intent.source_head(),
                intent.target_ref(),
                intent.target_head(),
            )
            .map_err(map_adapter_error)
    }
}

#[async_trait::async_trait]
impl TeamGitWorkspacePort for LocalTeamGitDeliveryPort {
    async fn plan_assignment(&self, plan: &GitWorkAssignmentPlan) -> Result<GitWorkAssignment, GitWorkspacePortError> {
        let plan = plan.clone();
        let adapter = self.adapter.clone();
        tokio::task::spawn_blocking(move || {
            let (identity, target) = adapter
                .resolve_current_worktree(plan.workspace())
                .map_err(map_workspace_adapter_error)?;
            Ok(GitWorkAssignment::new(
                identity.repository_id(),
                target.head.to_string(),
                assignment_branch(&plan),
            ))
        })
        .await
        .map_err(|error| GitWorkspacePortError::Unavailable(format!("Git assignment planner stopped: {error}")))?
    }

    async fn prepare_assignment(
        &self,
        plan: &GitWorkAssignmentPlan,
        assignment: &GitWorkAssignment,
    ) -> Result<PreparedGitWorkAssignment, GitWorkspacePortError> {
        let plan = plan.clone();
        let assignment = assignment.clone();
        let worktree_root = self.worktree_root.clone();
        let adapter = self.adapter.clone();
        tokio::task::spawn_blocking(move || {
            let expected_branch = assignment_branch(&plan);
            if assignment.branch_ref() != expected_branch {
                return Err(GitWorkspacePortError::InvalidRequest(
                    "persisted branch does not match the deterministic assignment key".to_owned(),
                ));
            }
            let repository_token = short_hash(assignment.repository_id(), 16);
            let assignment_token = short_hash(
                &format!(
                    "{}\0{}\0{}",
                    plan.team_id(),
                    plan.assignee_member_id(),
                    plan.assignment_key()
                ),
                32,
            );
            let parent = worktree_root.join(repository_token);
            fs::create_dir_all(&parent).map_err(|error| {
                GitWorkspacePortError::Unavailable(format!(
                    "unable to create Team worktree directory {}: {error}",
                    parent.display()
                ))
            })?;
            let worktree_path = parent.join(&assignment_token);
            let prepared = adapter
                .prepare_member_worktree_at_commit(
                    ExactRepository {
                        repository_id: assignment.repository_id().to_owned(),
                        root: PathBuf::from(assignment.repository_id()),
                    },
                    format!("ds-{assignment_token}"),
                    worktree_path,
                    assignment.branch_ref().to_owned(),
                    assignment.base_commit(),
                )
                .map_err(map_workspace_adapter_error)?;
            let workspace_path = prepared.path.to_str().ok_or_else(|| {
                GitWorkspacePortError::InvalidRequest("prepared worktree path is not valid UTF-8".to_owned())
            })?;
            Ok(PreparedGitWorkAssignment::new(assignment, workspace_path))
        })
        .await
        .map_err(|error| GitWorkspacePortError::Unavailable(format!("Git worktree preparation stopped: {error}")))?
    }

    async fn resolve_integration_target(
        &self,
        workspace: &str,
        expected_repository_id: &str,
    ) -> Result<GitIntegrationTarget, GitWorkspacePortError> {
        let workspace = workspace.to_owned();
        let expected_repository_id = expected_repository_id.to_owned();
        let adapter = self.adapter.clone();
        tokio::task::spawn_blocking(move || {
            let (identity, target) = adapter
                .resolve_current_worktree(&workspace)
                .map_err(map_workspace_adapter_error)?;
            if identity.repository_id() != expected_repository_id {
                return Err(GitWorkspacePortError::InvalidRequest(
                    "Team workspace does not match the accepted delivery repository".to_owned(),
                ));
            }
            Ok(GitIntegrationTarget::new(target.branch_ref, target.head.to_string()))
        })
        .await
        .map_err(|error| GitWorkspacePortError::Unavailable(format!("Git target resolver stopped: {error}")))?
    }
}

#[async_trait::async_trait]
impl TeamGitDeliveryPort for LocalTeamGitDeliveryPort {
    async fn integrate(&self, intent: &GitIntegrationIntent) -> Result<GitDeliveryPortOutcome, GitDeliveryPortError> {
        let intent = intent.clone();
        let adapter = self.adapter.clone();
        tokio::task::spawn_blocking(move || {
            let delivery = Self::exact_delivery(&adapter, &intent)?;
            let request = IntegrateGitDelivery {
                delivery,
                commit_identity: MergeCommitIdentity {
                    name: "DeepScientist Team".to_owned(),
                    email: "team@deepscientist.local".to_owned(),
                },
                message: format!("Integrate Team WorkItem {}", intent.work_item_id()),
            };
            match adapter.integrate(&request) {
                Ok(IntegrationOutcome::Integrated { merge_head }) => Ok(GitDeliveryPortOutcome::Merged {
                    merged_commit: merge_head.to_string(),
                    observed_target_head: merge_head.to_string(),
                }),
                Ok(IntegrationOutcome::AlreadyIntegrated { target_head }) => Ok(GitDeliveryPortOutcome::Merged {
                    merged_commit: target_head.to_string(),
                    observed_target_head: target_head.to_string(),
                }),
                Err(GitDeliveryError::MergeConflict { target_head }) => Ok(GitDeliveryPortOutcome::Conflicted {
                    observed_target_head: target_head.to_string(),
                }),
                Err(GitDeliveryError::TargetMoved { actual, .. }) => Ok(GitDeliveryPortOutcome::Retryable {
                    reason: IntegrationRecoveryReason::PreconditionChanged,
                    observed_target_head: Some(actual.to_string()),
                }),
                Err(error) => Err(map_adapter_error(error)),
            }
        })
        .await
        .map_err(|error| GitDeliveryPortError::Unavailable(format!("Git delivery worker stopped: {error}")))?
    }

    async fn reconcile(
        &self,
        intent: &GitIntegrationIntent,
    ) -> Result<GitDeliveryReconciliation, GitDeliveryPortError> {
        let intent = intent.clone();
        let adapter = self.adapter.clone();
        tokio::task::spawn_blocking(move || {
            let delivery = Self::exact_delivery(&adapter, &intent)?;
            match adapter.reconcile(&delivery) {
                Ok(ReconciliationOutcome::Integrated { target_head }) => {
                    Ok(GitDeliveryReconciliation::Resolved(GitDeliveryPortOutcome::Merged {
                        merged_commit: target_head.to_string(),
                        observed_target_head: target_head.to_string(),
                    }))
                }
                Ok(ReconciliationOutcome::ReadyToRetry { .. }) => Ok(GitDeliveryReconciliation::ReadyToIntegrate),
                Ok(ReconciliationOutcome::TargetMoved { actual_target_head, .. }) => {
                    Ok(GitDeliveryReconciliation::Resolved(GitDeliveryPortOutcome::Retryable {
                        reason: IntegrationRecoveryReason::PreconditionChanged,
                        observed_target_head: Some(actual_target_head.to_string()),
                    }))
                }
                Err(error) => Err(map_adapter_error(error)),
            }
        })
        .await
        .map_err(|error| GitDeliveryPortError::Unavailable(format!("Git reconciliation worker stopped: {error}")))?
    }
}

fn map_adapter_error(error: GitDeliveryError) -> GitDeliveryPortError {
    match error {
        GitDeliveryError::Git { .. }
        | GitDeliveryError::Io { .. }
        | GitDeliveryError::IntegratedButCheckoutFailed { .. } => GitDeliveryPortError::Unavailable(error.to_string()),
        _ => GitDeliveryPortError::InvalidIntent(error.to_string()),
    }
}

fn map_workspace_adapter_error(error: GitDeliveryError) -> GitWorkspacePortError {
    match error {
        GitDeliveryError::Git { .. } | GitDeliveryError::Io { .. } => {
            GitWorkspacePortError::Unavailable(error.to_string())
        }
        _ => GitWorkspacePortError::InvalidRequest(error.to_string()),
    }
}

fn assignment_branch(plan: &GitWorkAssignmentPlan) -> String {
    let token = short_hash(
        &format!(
            "{}\0{}\0{}",
            plan.team_id(),
            plan.assignee_member_id(),
            plan.assignment_key()
        ),
        32,
    );
    format!("refs/heads/ds/team/{token}")
}

fn short_hash(value: &str, length: usize) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let encoded = format!("{digest:x}");
    encoded[..length].to_owned()
}
