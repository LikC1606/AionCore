use std::path::PathBuf;

use git2::Oid;
use thiserror::Error;

/// A locally resolved Git repository.
///
/// `repository_id` is deliberately the canonical repository root on this
/// machine. It is not a portable or globally unique identifier. A transport
/// that moves deliveries between machines must map its own repository identity
/// to this local root before calling the adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RepositoryIdentity {
    repository_id: String,
    root: PathBuf,
}

impl RepositoryIdentity {
    pub(crate) fn new(repository_id: String, root: PathBuf) -> Self {
        Self { repository_id, root }
    }

    pub fn repository_id(&self) -> &str {
        &self.repository_id
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }
}

/// An exact local repository locator supplied to a mutating operation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactRepository {
    pub repository_id: String,
    pub root: PathBuf,
}

impl From<&RepositoryIdentity> for ExactRepository {
    fn from(identity: &RepositoryIdentity) -> Self {
        Self {
            repository_id: identity.repository_id.clone(),
            root: identity.root.clone(),
        }
    }
}

/// The deterministic worktree coordinates chosen by the Team runtime.
///
/// The adapter never invents ownership from the filesystem. The caller owns
/// the stable `worktree_name`, path, and branch mapping and must pass the same
/// values on retries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrepareMemberWorktree {
    pub repository: ExactRepository,
    pub worktree_name: String,
    pub worktree_path: PathBuf,
    pub branch_ref: String,
    pub base_head: Oid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorktreePreparation {
    Created,
    Reused,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedMemberWorktree {
    pub path: PathBuf,
    pub branch_ref: String,
    pub head: Oid,
    pub preparation: WorktreePreparation,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactWorktreeHead {
    pub path: PathBuf,
    pub branch_ref: String,
    pub head: Oid,
}

/// An immutable Git delivery claim.
///
/// The source and target refs, their exact heads, and the source base are all
/// checked again immediately before integration. No remote operation is ever
/// performed by this adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExactGitDelivery {
    pub repository: ExactRepository,
    pub base_head: Oid,
    pub source: ExactWorktreeHead,
    pub target: ExactWorktreeHead,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeliveryPreflight {
    pub repository: RepositoryIdentity,
    pub base_head: Oid,
    pub source_head: Oid,
    pub target_head: Oid,
}

/// Explicit author data for the synthetic no-fast-forward merge commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MergeCommitIdentity {
    pub name: String,
    pub email: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntegrateGitDelivery {
    pub delivery: ExactGitDelivery,
    pub commit_identity: MergeCommitIdentity,
    pub message: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntegrationOutcome {
    Integrated { merge_head: Oid },
    AlreadyIntegrated { target_head: Oid },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconciliationOutcome {
    Integrated {
        target_head: Oid,
    },
    ReadyToRetry {
        target_head: Oid,
    },
    TargetMoved {
        expected_target_head: Oid,
        actual_target_head: Oid,
    },
}

#[derive(Debug, Error)]
pub enum GitDeliveryError {
    #[error("unable to resolve a non-bare Git repository from {path}: {message}")]
    RepositoryResolution { path: PathBuf, message: String },

    #[error("repository root must match the resolved canonical root (expected {expected}, got {actual})")]
    RepositoryRootMismatch { expected: PathBuf, actual: PathBuf },

    #[error("canonical repository root is not valid UTF-8: {0}")]
    NonUtf8RepositoryRoot(PathBuf),

    #[error("repository identity mismatch (expected {expected}, got {actual})")]
    RepositoryIdMismatch { expected: String, actual: String },

    #[error("invalid local branch reference: {0}")]
    InvalidBranchReference(String),

    #[error("invalid worktree name: {0}")]
    InvalidWorktreeName(String),

    #[error("worktree path must be an absolute path with an existing directory parent: {0}")]
    InvalidWorktreePath(PathBuf),

    #[error("Git object {oid} is not a commit")]
    CommitNotFound { oid: Oid },

    #[error("branch {branch_ref} does not exist")]
    BranchNotFound { branch_ref: String },

    #[error("branch {branch_ref} must be a direct local reference")]
    SymbolicBranch { branch_ref: String },

    #[error("branch {branch_ref} is not checked out in a registered worktree")]
    BranchWorktreeNotFound { branch_ref: String },

    #[error("invalid Git commit id: {0}")]
    InvalidCommitId(String),

    #[error("branch {branch_ref} head mismatch (expected {expected}, got {actual})")]
    BranchHeadMismatch {
        branch_ref: String,
        expected: Oid,
        actual: Oid,
    },

    #[error("worktree at {path} is not registered as {worktree_name}")]
    WorktreeRegistrationMismatch { path: PathBuf, worktree_name: String },

    #[error("worktree at {path} belongs to another repository")]
    ForeignWorktree { path: PathBuf },

    #[error("worktree at {path} is on {actual:?}, expected {expected}")]
    WorktreeBranchMismatch {
        path: PathBuf,
        expected: String,
        actual: Option<String>,
    },

    #[error("worktree at {path} head mismatch (expected {expected}, got {actual})")]
    WorktreeHeadMismatch { path: PathBuf, expected: Oid, actual: Oid },

    #[error("worktree at {path} is not clean")]
    DirtyWorktree { path: PathBuf },

    #[error("worktree at {path} has an in-progress Git operation")]
    WorktreeOperationInProgress { path: PathBuf },

    #[error("delivery base {base_head} is not an ancestor of {descendant_head}")]
    BaseNotAncestor { base_head: Oid, descendant_head: Oid },

    #[error("source and target branches must be different")]
    SameSourceAndTarget,

    #[error("delivery conflicts with target head {target_head}")]
    MergeConflict { target_head: Oid },

    #[error("target branch moved during integration (expected {expected}, got {actual})")]
    TargetMoved { expected: Oid, actual: Oid },

    #[error("delivery was integrated as {merge_head}, but the target worktree could not be updated: {message}")]
    IntegratedButCheckoutFailed { merge_head: Oid, message: String },

    #[error("invalid merge commit identity: {0}")]
    InvalidCommitIdentity(String),

    #[error("Git operation {operation} failed: {message}")]
    Git { operation: &'static str, message: String },

    #[error("filesystem operation failed for {path}: {message}")]
    Io { path: PathBuf, message: String },
}

impl GitDeliveryError {
    pub(crate) fn git(operation: &'static str, error: git2::Error) -> Self {
        Self::Git {
            operation,
            message: error.message().to_owned(),
        }
    }
}
