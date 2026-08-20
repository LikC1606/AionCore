use std::fs;
use std::path::{Path, PathBuf};

use git2::{ErrorCode, Oid, Reference, Repository, RepositoryState, Status, StatusOptions};

use super::{ExactRepository, ExactWorktreeHead, GitDeliveryError, RepositoryIdentity};

pub(super) fn resolve_repository(path: &Path) -> Result<RepositoryIdentity, GitDeliveryError> {
    let discovered = Repository::discover(path).map_err(|error| GitDeliveryError::RepositoryResolution {
        path: path.to_path_buf(),
        message: error.message().to_owned(),
    })?;
    if discovered.is_bare() {
        return Err(GitDeliveryError::RepositoryResolution {
            path: path.to_path_buf(),
            message: "bare repositories do not have a delivery worktree".to_owned(),
        });
    }

    let repository = main_repository(&discovered)?;
    let root = repository
        .workdir()
        .ok_or_else(|| GitDeliveryError::RepositoryResolution {
            path: path.to_path_buf(),
            message: "repository has no main worktree".to_owned(),
        })
        .and_then(canonicalize)?;
    let repository_id = root
        .to_str()
        .ok_or_else(|| GitDeliveryError::NonUtf8RepositoryRoot(root.clone()))?
        .to_owned();
    Ok(RepositoryIdentity::new(repository_id, root))
}

pub(super) fn open_exact_repository(
    expected: &ExactRepository,
) -> Result<(RepositoryIdentity, Repository), GitDeliveryError> {
    let supplied_root = canonicalize(&expected.root)?;
    let identity = resolve_repository(&supplied_root)?;
    if identity.root() != supplied_root {
        return Err(GitDeliveryError::RepositoryRootMismatch {
            expected: identity.root().to_path_buf(),
            actual: supplied_root,
        });
    }
    if identity.repository_id() != expected.repository_id {
        return Err(GitDeliveryError::RepositoryIdMismatch {
            expected: identity.repository_id().to_owned(),
            actual: expected.repository_id.clone(),
        });
    }
    let repository =
        Repository::open(identity.root()).map_err(|error| GitDeliveryError::git("open repository", error))?;
    Ok((identity, repository))
}

pub(super) fn validate_branch_ref(branch_ref: &str) -> Result<(), GitDeliveryError> {
    if !branch_ref.starts_with("refs/heads/") || !Reference::is_valid_name(branch_ref) {
        return Err(GitDeliveryError::InvalidBranchReference(branch_ref.to_owned()));
    }
    Ok(())
}

pub(super) fn validate_worktree_name(name: &str) -> Result<(), GitDeliveryError> {
    let valid = !name.is_empty()
        && name != "."
        && name != ".."
        && !name.ends_with(".lock")
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !valid {
        return Err(GitDeliveryError::InvalidWorktreeName(name.to_owned()));
    }
    Ok(())
}

pub(super) fn canonical_new_worktree_path(path: &Path) -> Result<PathBuf, GitDeliveryError> {
    if !path.is_absolute() {
        return Err(GitDeliveryError::InvalidWorktreePath(path.to_path_buf()));
    }
    let file_name = path
        .file_name()
        .filter(|name| !name.is_empty())
        .ok_or_else(|| GitDeliveryError::InvalidWorktreePath(path.to_path_buf()))?;
    let parent = path
        .parent()
        .ok_or_else(|| GitDeliveryError::InvalidWorktreePath(path.to_path_buf()))?;
    let parent = canonicalize(parent).map_err(|_| GitDeliveryError::InvalidWorktreePath(path.to_path_buf()))?;
    Ok(parent.join(file_name))
}

pub(super) fn ensure_commit(repository: &Repository, oid: Oid) -> Result<(), GitDeliveryError> {
    repository
        .find_commit(oid)
        .map(|_| ())
        .map_err(|_| GitDeliveryError::CommitNotFound { oid })
}

pub(super) fn branch_head(repository: &Repository, branch_ref: &str) -> Result<Oid, GitDeliveryError> {
    validate_branch_ref(branch_ref)?;
    match repository.find_reference(branch_ref) {
        Ok(reference) => reference.target().ok_or_else(|| GitDeliveryError::SymbolicBranch {
            branch_ref: branch_ref.to_owned(),
        }),
        Err(error) if error.code() == ErrorCode::NotFound => Err(GitDeliveryError::BranchNotFound {
            branch_ref: branch_ref.to_owned(),
        }),
        Err(error) => Err(GitDeliveryError::git("resolve branch head", error)),
    }
}

pub(super) fn open_and_validate_worktree(
    root_repository: &Repository,
    expected: &ExactWorktreeHead,
    managed_runtime_skill_source_roots: &[PathBuf],
) -> Result<Repository, GitDeliveryError> {
    let path = canonicalize(&expected.path)?;
    let worktree = Repository::open(&path).map_err(|error| GitDeliveryError::RepositoryResolution {
        path: path.clone(),
        message: error.message().to_owned(),
    })?;
    ensure_same_common_repository(root_repository, &worktree, &path)?;
    validate_exact_worktree(
        &worktree,
        &path,
        &expected.branch_ref,
        expected.head,
        managed_runtime_skill_source_roots,
    )?;
    Ok(worktree)
}

pub(super) fn validate_exact_worktree(
    repository: &Repository,
    path: &Path,
    branch_ref: &str,
    expected_head: Oid,
    managed_runtime_skill_source_roots: &[PathBuf],
) -> Result<(), GitDeliveryError> {
    validate_branch_ref(branch_ref)?;
    if repository.state() != RepositoryState::Clean {
        return Err(GitDeliveryError::WorktreeOperationInProgress {
            path: path.to_path_buf(),
        });
    }
    let head = repository
        .head()
        .map_err(|error| GitDeliveryError::git("read worktree HEAD", error))?;
    let actual_branch = head.name().map(str::to_owned);
    if actual_branch.as_deref() != Some(branch_ref) {
        return Err(GitDeliveryError::WorktreeBranchMismatch {
            path: path.to_path_buf(),
            expected: branch_ref.to_owned(),
            actual: actual_branch,
        });
    }
    let actual_head = head
        .target()
        .ok_or(GitDeliveryError::CommitNotFound { oid: expected_head })?;
    if actual_head != expected_head {
        return Err(GitDeliveryError::WorktreeHeadMismatch {
            path: path.to_path_buf(),
            expected: expected_head,
            actual: actual_head,
        });
    }
    ensure_clean(repository, path, managed_runtime_skill_source_roots)
}

pub(super) fn ensure_same_common_repository(
    root_repository: &Repository,
    worktree_repository: &Repository,
    worktree_path: &Path,
) -> Result<(), GitDeliveryError> {
    let expected = canonicalize(root_repository.commondir())?;
    let actual = canonicalize(worktree_repository.commondir())?;
    if actual != expected {
        return Err(GitDeliveryError::ForeignWorktree {
            path: worktree_path.to_path_buf(),
        });
    }
    Ok(())
}

pub(super) fn ensure_clean(
    repository: &Repository,
    path: &Path,
    managed_runtime_skill_source_roots: &[PathBuf],
) -> Result<(), GitDeliveryError> {
    let mut options = StatusOptions::new();
    options
        .include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false);
    let statuses = repository
        .statuses(Some(&mut options))
        .map_err(|error| GitDeliveryError::git("read worktree status", error))?;
    if statuses.iter().all(|entry| {
        entry.status() == Status::WT_NEW
            && entry.path().is_some_and(|relative_path| {
                is_managed_runtime_skill_link(path, Path::new(relative_path), managed_runtime_skill_source_roots)
            })
    }) {
        Ok(())
    } else {
        Err(GitDeliveryError::DirtyWorktree {
            path: path.to_path_buf(),
        })
    }
}

fn is_managed_runtime_skill_link(worktree: &Path, relative_path: &Path, source_roots: &[PathBuf]) -> bool {
    let mut components = relative_path.components();
    let is_codex_skill = matches!(components.next(), Some(std::path::Component::Normal(value)) if value == ".codex")
        && matches!(components.next(), Some(std::path::Component::Normal(value)) if value == "skills");
    let Some(std::path::Component::Normal(skill_name)) = components.next() else {
        return false;
    };
    if !is_codex_skill || components.next().is_some() {
        return false;
    }

    let expected_link_path = worktree.join(relative_path);
    let Ok(resolved_target) = canonicalize(&expected_link_path) else {
        return false;
    };
    if resolved_target == expected_link_path || resolved_target.file_name() != Some(skill_name) {
        return false;
    }
    if !resolved_target.join("SKILL.md").is_file() {
        return false;
    }

    source_roots.iter().any(|source_root| {
        let Ok(relative_target) = resolved_target.strip_prefix(source_root) else {
            return false;
        };
        let mut target_components = relative_target.components();
        match (
            target_components.next(),
            target_components.next(),
            target_components.next(),
        ) {
            (Some(std::path::Component::Normal(name)), None, None) => name == skill_name,
            (Some(std::path::Component::Normal(parent)), Some(std::path::Component::Normal(name)), None) => {
                parent == "auto-inject" && name == skill_name
            }
            _ => false,
        }
    })
}

pub(super) fn canonicalize(path: &Path) -> Result<PathBuf, GitDeliveryError> {
    fs::canonicalize(path).map_err(|error| GitDeliveryError::Io {
        path: path.to_path_buf(),
        message: error.to_string(),
    })
}

fn main_repository(repository: &Repository) -> Result<Repository, GitDeliveryError> {
    if !repository.is_worktree() {
        return Repository::open(repository.path())
            .map_err(|error| GitDeliveryError::git("open main repository", error));
    }
    Repository::open(repository.commondir()).map_err(|error| GitDeliveryError::git("open common repository", error))
}
