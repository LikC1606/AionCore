use std::sync::Arc;

use aionui_db::{DbError, ITeamModeRepository, ITeamRepository, TeamWorkItemRow};
use thiserror::Error;

use super::TeamCommandService;
use super::team_command::{TeamCommand, TeamCommandError, TeamCommandPrincipal, TeamCommandReceipt};
use crate::kernel::{DeliveryRequirement, GitWorkAssignment};
use crate::ports::{GitWorkAssignmentPlan, GitWorkspacePortError, TeamGitWorkspacePort};

const PUBLIC_IDEMPOTENCY_KEY_MAX_LEN: usize = 220;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DelegateWorkItem {
    pub parent_work_item_id: Option<String>,
    pub subject: String,
    pub description: Option<String>,
    pub assignee_member_id: String,
    pub delivery_requirement: DeliveryRequirement,
}

#[derive(Debug)]
pub(crate) struct TeamWorkDelegationReceipt {
    pub(crate) command: TeamCommandReceipt,
    pub(crate) prepared_workspace: Option<String>,
}

#[derive(Debug, Error)]
pub(crate) enum TeamWorkCoordinatorError {
    #[error("Team not found: {0}")]
    TeamNotFound(String),
    #[error("authenticated user does not own this Team")]
    ForbiddenTeam,
    #[error("Team workspace is not a Git repository")]
    GitWorkspaceRequired,
    #[error("stored Team WorkItem does not match its delegation receipt")]
    CorruptState,
    #[error(transparent)]
    Command(#[from] TeamCommandError),
    #[error(transparent)]
    Git(#[from] GitWorkspacePortError),
    #[error(transparent)]
    Database(#[from] DbError),
}

/// Coordinates durable WorkItem commands with the external Git worktree
/// side effect. The command receipt is always checked before touching Git.
pub struct TeamWorkCoordinator {
    team_repo: Arc<dyn ITeamRepository>,
    mode_repo: Arc<dyn ITeamModeRepository>,
    command_service: Arc<TeamCommandService>,
    git_workspace: Arc<dyn TeamGitWorkspacePort>,
}

impl TeamWorkCoordinator {
    pub fn new(
        team_repo: Arc<dyn ITeamRepository>,
        mode_repo: Arc<dyn ITeamModeRepository>,
        command_service: Arc<TeamCommandService>,
        git_workspace: Arc<dyn TeamGitWorkspacePort>,
    ) -> Self {
        Self {
            team_repo,
            mode_repo,
            command_service,
            git_workspace,
        }
    }

    pub(crate) async fn delegate_as_owner(
        &self,
        authenticated_user_id: &str,
        team_id: &str,
        idempotency_key: &str,
        request: DelegateWorkItem,
    ) -> Result<TeamWorkDelegationReceipt, TeamWorkCoordinatorError> {
        let actor = DelegateActor::Owner {
            authenticated_user_id,
            team_id,
        };
        self.delegate(actor, idempotency_key, request).await
    }

    pub(crate) async fn delegate_as_member(
        &self,
        principal: &TeamCommandPrincipal,
        idempotency_key: &str,
        request: DelegateWorkItem,
    ) -> Result<TeamWorkDelegationReceipt, TeamWorkCoordinatorError> {
        self.delegate(DelegateActor::Member(principal), idempotency_key, request)
            .await
    }

    async fn delegate(
        &self,
        actor: DelegateActor<'_>,
        idempotency_key: &str,
        request: DelegateWorkItem,
    ) -> Result<TeamWorkDelegationReceipt, TeamWorkCoordinatorError> {
        validate_public_key(idempotency_key)?;
        actor
            .authorize_delegate(&self.command_service, &request.assignee_member_id)
            .await?;

        let create_key = derived_key(idempotency_key, "create");
        let queue_key = derived_key(idempotency_key, "queue");
        let team_id = actor.team_id();
        let user_id = actor.authenticated_user_id();
        let team = self
            .team_repo
            .get_team(team_id)
            .await?
            .ok_or_else(|| TeamWorkCoordinatorError::TeamNotFound(team_id.to_owned()))?;
        if team.user_id != user_id {
            return Err(TeamWorkCoordinatorError::ForbiddenTeam);
        }

        let existing_create = actor
            .find_receipt(&self.command_service, &create_key, "create_work_item")
            .await?;
        let (created, work) = if let Some(receipt) = existing_create {
            let work = self.load_and_validate_work(team_id, &receipt, &request).await?;
            (receipt, work)
        } else {
            let git_assignment = match request.delivery_requirement {
                DeliveryRequirement::None => None,
                DeliveryRequirement::Git => {
                    if team.workspace.trim().is_empty() {
                        return Err(TeamWorkCoordinatorError::GitWorkspaceRequired);
                    }
                    let plan =
                        GitWorkAssignmentPlan::new(team_id, &request.assignee_member_id, &team.workspace, &create_key);
                    Some(self.git_workspace.plan_assignment(&plan).await?)
                }
            };
            let receipt = actor
                .execute(
                    &self.command_service,
                    &create_key,
                    TeamCommand::CreateWorkItem {
                        parent_work_item_id: request.parent_work_item_id.clone(),
                        subject: request.subject.clone(),
                        description: request.description.clone(),
                        assignee_member_id: request.assignee_member_id.clone(),
                        delivery_requirement: request.delivery_requirement,
                        git_assignment,
                    },
                )
                .await?;
            let work = self.load_and_validate_work(team_id, &receipt, &request).await?;
            (receipt, work)
        };

        if let Some(queued) = actor.find_receipt(&self.command_service, &queue_key, "queue").await? {
            self.validate_receipt_work_id(&created, &queued)?;
            let prepared_workspace = match request.delivery_requirement {
                DeliveryRequirement::None => None,
                DeliveryRequirement::Git => Some(
                    self.prepare_git_workspace(
                        team_id,
                        &request.assignee_member_id,
                        &team.workspace,
                        &create_key,
                        &work,
                    )
                    .await?,
                ),
            };
            return Ok(TeamWorkDelegationReceipt {
                command: queued,
                prepared_workspace,
            });
        }

        let prepared_workspace = if request.delivery_requirement == DeliveryRequirement::Git {
            Some(
                self.prepare_git_workspace(
                    team_id,
                    &request.assignee_member_id,
                    &team.workspace,
                    &create_key,
                    &work,
                )
                .await?,
            )
        } else {
            None
        };

        let expected_work_revision =
            u64::try_from(work.revision).map_err(|_| TeamWorkCoordinatorError::CorruptState)?;
        let queued = actor
            .execute(
                &self.command_service,
                &queue_key,
                TeamCommand::Queue {
                    work_item_id: created.result.work_item_id.clone(),
                    expected_work_revision,
                    prepared_workspace: prepared_workspace.clone(),
                },
            )
            .await?;
        Ok(TeamWorkDelegationReceipt {
            command: queued,
            prepared_workspace,
        })
    }

    async fn prepare_git_workspace(
        &self,
        team_id: &str,
        assignee_member_id: &str,
        team_workspace: &str,
        create_key: &str,
        work: &TeamWorkItemRow,
    ) -> Result<String, TeamWorkCoordinatorError> {
        let assignment = assignment_from_row(work)?;
        let plan = GitWorkAssignmentPlan::new(team_id, assignee_member_id, team_workspace, create_key);
        Ok(self
            .git_workspace
            .prepare_assignment(&plan, &assignment)
            .await?
            .workspace_path()
            .to_owned())
    }

    async fn load_and_validate_work(
        &self,
        team_id: &str,
        receipt: &TeamCommandReceipt,
        request: &DelegateWorkItem,
    ) -> Result<TeamWorkItemRow, TeamWorkCoordinatorError> {
        let row = self
            .mode_repo
            .get_work_item(team_id, &receipt.result.work_item_id)
            .await?
            .ok_or(TeamWorkCoordinatorError::CorruptState)?;
        let expected_requirement = match request.delivery_requirement {
            DeliveryRequirement::None => "none",
            DeliveryRequirement::Git => "git",
        };
        if row.parent_work_item_id != request.parent_work_item_id
            || row.subject != request.subject.trim()
            || row.description != request.description
            || row.assignee_member_id != request.assignee_member_id
            || row.delivery_requirement != expected_requirement
        {
            return Err(TeamCommandError::IdempotencyConflict.into());
        }
        match request.delivery_requirement {
            DeliveryRequirement::None
                if row.git_repository_id.is_none() && row.git_base_commit.is_none() && row.git_branch_ref.is_none() => {
            }
            DeliveryRequirement::None => return Err(TeamWorkCoordinatorError::CorruptState),
            DeliveryRequirement::Git => {
                assignment_from_row(&row)?;
            }
        }
        Ok(row)
    }

    fn validate_receipt_work_id(
        &self,
        created: &TeamCommandReceipt,
        queued: &TeamCommandReceipt,
    ) -> Result<(), TeamWorkCoordinatorError> {
        if created.result.work_item_id != queued.result.work_item_id {
            return Err(TeamWorkCoordinatorError::CorruptState);
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum DelegateActor<'a> {
    Owner {
        authenticated_user_id: &'a str,
        team_id: &'a str,
    },
    Member(&'a TeamCommandPrincipal),
}

impl DelegateActor<'_> {
    fn authenticated_user_id(&self) -> &str {
        match *self {
            Self::Owner {
                authenticated_user_id, ..
            } => authenticated_user_id,
            Self::Member(principal) => principal.authenticated_user_id(),
        }
    }

    fn team_id(&self) -> &str {
        match *self {
            Self::Owner { team_id, .. } => team_id,
            Self::Member(principal) => principal.team_id(),
        }
    }

    async fn authorize_delegate(
        self,
        service: &TeamCommandService,
        assignee_member_id: &str,
    ) -> Result<(), TeamCommandError> {
        match self {
            Self::Owner {
                authenticated_user_id,
                team_id,
            } => {
                service
                    .authorize_delegate_as_owner(authenticated_user_id, team_id, assignee_member_id)
                    .await
            }
            Self::Member(principal) => service.authorize_delegate(principal, assignee_member_id).await,
        }
    }

    async fn find_receipt(
        self,
        service: &TeamCommandService,
        idempotency_key: &str,
        expected_command_name: &str,
    ) -> Result<Option<TeamCommandReceipt>, TeamCommandError> {
        match self {
            Self::Owner {
                authenticated_user_id,
                team_id,
            } => {
                service
                    .find_receipt_as_owner(authenticated_user_id, team_id, idempotency_key, expected_command_name)
                    .await
            }
            Self::Member(principal) => {
                service
                    .find_receipt(principal, idempotency_key, expected_command_name)
                    .await
            }
        }
    }

    async fn execute(
        self,
        service: &TeamCommandService,
        idempotency_key: &str,
        command: TeamCommand,
    ) -> Result<TeamCommandReceipt, TeamCommandError> {
        match self {
            Self::Owner {
                authenticated_user_id,
                team_id,
            } => {
                service
                    .execute_as_owner(authenticated_user_id, team_id, idempotency_key, command)
                    .await
            }
            Self::Member(principal) => service.execute(principal, idempotency_key, command).await,
        }
    }
}

fn assignment_from_row(row: &TeamWorkItemRow) -> Result<GitWorkAssignment, TeamWorkCoordinatorError> {
    match (
        row.git_repository_id.as_deref(),
        row.git_base_commit.as_deref(),
        row.git_branch_ref.as_deref(),
    ) {
        (Some(repository_id), Some(base_commit), Some(branch_ref))
            if !repository_id.trim().is_empty() && !base_commit.trim().is_empty() && !branch_ref.trim().is_empty() =>
        {
            Ok(GitWorkAssignment::new(repository_id, base_commit, branch_ref))
        }
        _ => Err(TeamWorkCoordinatorError::CorruptState),
    }
}

fn validate_public_key(key: &str) -> Result<(), TeamWorkCoordinatorError> {
    if key.trim().is_empty() || key.len() > PUBLIC_IDEMPOTENCY_KEY_MAX_LEN {
        return Err(TeamCommandError::InvalidCommand(format!(
            "idempotency_key must contain 1..={PUBLIC_IDEMPOTENCY_KEY_MAX_LEN} bytes"
        ))
        .into());
    }
    Ok(())
}

fn derived_key(base: &str, suffix: &str) -> String {
    format!("{base}:{suffix}")
}
