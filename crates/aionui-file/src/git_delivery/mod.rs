mod model;
mod repository;

use std::fs;
use std::path::{Path, PathBuf};

use git2::{ErrorCode, Oid, Repository, Signature, WorktreeAddOptions, build::CheckoutBuilder};

pub use model::{
    DeliveryPreflight, ExactGitDelivery, ExactRepository, ExactWorktreeHead, GitDeliveryError, IntegrateGitDelivery,
    IntegrationOutcome, MergeCommitIdentity, PrepareMemberWorktree, PreparedMemberWorktree, ReconciliationOutcome,
    RepositoryIdentity, WorktreePreparation,
};
use repository::{
    branch_head, canonical_new_worktree_path, canonicalize, ensure_commit, ensure_same_common_repository,
    open_and_validate_worktree, open_exact_repository, validate_branch_ref, validate_exact_worktree,
    validate_worktree_name,
};

/// Synchronous, local-only Git isolation and delivery adapter.
///
/// This adapter intentionally has no Team database, HTTP, remote, fetch, push,
/// or filesystem ownership-record dependency. All authorization and durable
/// lifecycle decisions remain with the caller; this type only proves and
/// performs exact local Git transitions.
#[derive(Clone, Debug, Default)]
pub struct GitDeliveryAdapter {
    managed_runtime_skill_source_roots: Vec<PathBuf>,
}

impl GitDeliveryAdapter {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allows Team-owned, untracked Codex workspace Skill links to coexist
    /// with strict delivery cleanliness checks.
    ///
    /// This policy is intentionally opt-in. The default adapter rejects every
    /// untracked path, and even the configured adapter only exempts links at
    /// `.codex/skills/<name>` whose resolved target is below one of these
    /// source roots. Tracked or staged paths are never exempted.
    pub fn with_managed_runtime_skill_sources(source_roots: impl IntoIterator<Item = PathBuf>) -> Self {
        let mut source_roots = source_roots
            .into_iter()
            .filter_map(|root| canonicalize(&root).ok())
            .collect::<Vec<_>>();
        source_roots.sort();
        source_roots.dedup();
        Self {
            managed_runtime_skill_source_roots: source_roots,
        }
    }

    pub fn resolve_repository(&self, path: impl AsRef<Path>) -> Result<RepositoryIdentity, GitDeliveryError> {
        repository::resolve_repository(path.as_ref())
    }

    /// Resolves the checked-out branch and exact head for one clean local
    /// worktree. This is used before an integration attempt is persisted; all
    /// later retries use the immutable coordinates stored by Team Mode.
    pub fn resolve_current_worktree(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<(RepositoryIdentity, ExactWorktreeHead), GitDeliveryError> {
        let discovered =
            Repository::discover(path.as_ref()).map_err(|error| GitDeliveryError::RepositoryResolution {
                path: path.as_ref().to_path_buf(),
                message: error.message().to_owned(),
            })?;
        let worktree_path = discovered
            .workdir()
            .ok_or_else(|| GitDeliveryError::RepositoryResolution {
                path: path.as_ref().to_path_buf(),
                message: "bare repositories do not have a delivery worktree".to_owned(),
            })
            .and_then(repository::canonicalize)?;
        let identity = repository::resolve_repository(&worktree_path)?;
        let head = discovered
            .head()
            .map_err(|error| GitDeliveryError::git("read worktree HEAD", error))?;
        let branch_ref = head
            .name()
            .map(str::to_owned)
            .filter(|name| name.starts_with("refs/heads/"))
            .ok_or_else(|| GitDeliveryError::SymbolicBranch {
                branch_ref: head.name().unwrap_or("HEAD").to_owned(),
            })?;
        let head = head.target().ok_or_else(|| GitDeliveryError::SymbolicBranch {
            branch_ref: branch_ref.clone(),
        })?;
        validate_exact_worktree(
            &discovered,
            &worktree_path,
            &branch_ref,
            head,
            &self.managed_runtime_skill_source_roots,
        )?;
        Ok((
            identity,
            ExactWorktreeHead {
                path: worktree_path,
                branch_ref,
                head,
            },
        ))
    }

    /// Resolves durable string coordinates into exact, clean local worktrees.
    ///
    /// The repository id remains caller-owned. This method only maps that id
    /// to a local root and proves that both refs are checked out at the exact
    /// heads recorded by the Team integration attempt.
    #[allow(clippy::too_many_arguments)]
    pub fn resolve_exact_delivery(
        &self,
        repository: ExactRepository,
        base_head: &str,
        source_ref: &str,
        source_head: &str,
        target_ref: &str,
        target_head: &str,
    ) -> Result<ExactGitDelivery, GitDeliveryError> {
        let base_head = parse_oid(base_head)?;
        let source_head = parse_oid(source_head)?;
        let target_head = parse_oid(target_head)?;
        let (identity, root_repository) = open_exact_repository(&repository)?;
        ensure_commit(&root_repository, base_head)?;
        ensure_commit(&root_repository, source_head)?;
        ensure_commit(&root_repository, target_head)?;
        let source = resolve_exact_worktree(
            &root_repository,
            identity.root(),
            source_ref,
            source_head,
            &self.managed_runtime_skill_source_roots,
        )?;
        let target = resolve_exact_worktree(
            &root_repository,
            identity.root(),
            target_ref,
            target_head,
            &self.managed_runtime_skill_source_roots,
        )?;
        Ok(ExactGitDelivery {
            repository,
            base_head,
            source,
            target,
        })
    }

    /// Creates or verifies an exact member worktree at the supplied base.
    ///
    /// Retrying the same request is idempotent. An existing branch, worktree
    /// registration, path, or checkout that differs from the request fails
    /// closed instead of being repurposed.
    pub fn prepare_member_worktree(
        &self,
        request: &PrepareMemberWorktree,
    ) -> Result<PreparedMemberWorktree, GitDeliveryError> {
        validate_worktree_name(&request.worktree_name)?;
        validate_branch_ref(&request.branch_ref)?;
        let expected_path = canonical_new_worktree_path(&request.worktree_path)?;
        let (_, repository) = open_exact_repository(&request.repository)?;
        ensure_commit(&repository, request.base_head)?;

        match repository.find_worktree(&request.worktree_name) {
            Ok(worktree) => {
                worktree
                    .validate()
                    .map_err(|error| GitDeliveryError::git("validate registered worktree", error))?;
                let actual_path = canonicalize(worktree.path())?;
                if actual_path != expected_path {
                    return Err(GitDeliveryError::WorktreeRegistrationMismatch {
                        path: expected_path,
                        worktree_name: request.worktree_name.clone(),
                    });
                }
                let worktree_repository = Repository::open_from_worktree(&worktree)
                    .map_err(|error| GitDeliveryError::git("open registered worktree", error))?;
                ensure_same_common_repository(&repository, &worktree_repository, &actual_path)?;
                validate_exact_worktree(
                    &worktree_repository,
                    &actual_path,
                    &request.branch_ref,
                    request.base_head,
                    &self.managed_runtime_skill_source_roots,
                )?;
                return Ok(PreparedMemberWorktree {
                    path: actual_path,
                    branch_ref: request.branch_ref.clone(),
                    head: request.base_head,
                    preparation: WorktreePreparation::Reused,
                });
            }
            Err(error) if error.code() == ErrorCode::NotFound => {}
            Err(error) => return Err(GitDeliveryError::git("find registered worktree", error)),
        }

        if fs::symlink_metadata(&expected_path).is_ok() {
            return Err(GitDeliveryError::WorktreeRegistrationMismatch {
                path: expected_path,
                worktree_name: request.worktree_name.clone(),
            });
        }

        let branch_created = match branch_head(&repository, &request.branch_ref) {
            Ok(actual) if actual == request.base_head => false,
            Ok(actual) => {
                return Err(GitDeliveryError::BranchHeadMismatch {
                    branch_ref: request.branch_ref.clone(),
                    expected: request.base_head,
                    actual,
                });
            }
            Err(GitDeliveryError::BranchNotFound { .. }) => {
                repository
                    .reference(
                        &request.branch_ref,
                        request.base_head,
                        false,
                        "prepare deterministic member worktree",
                    )
                    .map_err(|error| GitDeliveryError::git("create member branch", error))?;
                true
            }
            Err(error) => return Err(error),
        };

        let branch = repository
            .find_reference(&request.branch_ref)
            .map_err(|error| GitDeliveryError::git("open member branch", error))?;
        let mut options = WorktreeAddOptions::new();
        options.reference(Some(&branch));
        let created = repository.worktree(&request.worktree_name, &expected_path, Some(&options));
        let worktree = match created {
            Ok(worktree) => worktree,
            Err(error) => {
                if branch_created {
                    remove_branch_if_unchanged(&repository, &request.branch_ref, request.base_head);
                }
                return Err(GitDeliveryError::git("create member worktree", error));
            }
        };
        let actual_path = canonicalize(worktree.path())?;
        let worktree_repository = Repository::open_from_worktree(&worktree)
            .map_err(|error| GitDeliveryError::git("open created worktree", error))?;
        validate_exact_worktree(
            &worktree_repository,
            &actual_path,
            &request.branch_ref,
            request.base_head,
            &self.managed_runtime_skill_source_roots,
        )?;
        Ok(PreparedMemberWorktree {
            path: actual_path,
            branch_ref: request.branch_ref.clone(),
            head: request.base_head,
            preparation: WorktreePreparation::Created,
        })
    }

    /// String-coordinate convenience used by transport adapters. Commit
    /// parsing remains inside this Git boundary.
    pub fn prepare_member_worktree_at_commit(
        &self,
        repository: ExactRepository,
        worktree_name: String,
        worktree_path: std::path::PathBuf,
        branch_ref: String,
        base_commit: &str,
    ) -> Result<PreparedMemberWorktree, GitDeliveryError> {
        self.prepare_member_worktree(&PrepareMemberWorktree {
            repository,
            worktree_name,
            worktree_path,
            branch_ref,
            base_head: parse_oid(base_commit)?,
        })
    }

    pub fn preflight(&self, delivery: &ExactGitDelivery) -> Result<DeliveryPreflight, GitDeliveryError> {
        if delivery.source.branch_ref == delivery.target.branch_ref {
            return Err(GitDeliveryError::SameSourceAndTarget);
        }
        validate_branch_ref(&delivery.source.branch_ref)?;
        validate_branch_ref(&delivery.target.branch_ref)?;
        let (identity, repository) = open_exact_repository(&delivery.repository)?;
        ensure_commit(&repository, delivery.base_head)?;
        ensure_commit(&repository, delivery.source.head)?;
        ensure_commit(&repository, delivery.target.head)?;
        ensure_branch_head(&repository, &delivery.source.branch_ref, delivery.source.head)?;
        ensure_branch_head(&repository, &delivery.target.branch_ref, delivery.target.head)?;
        ensure_ancestor(&repository, delivery.base_head, delivery.source.head)?;
        ensure_ancestor(&repository, delivery.base_head, delivery.target.head)?;
        open_and_validate_worktree(&repository, &delivery.source, &self.managed_runtime_skill_source_roots)?;
        open_and_validate_worktree(&repository, &delivery.target, &self.managed_runtime_skill_source_roots)?;
        Ok(DeliveryPreflight {
            repository: identity,
            base_head: delivery.base_head,
            source_head: delivery.source.head,
            target_head: delivery.target.head,
        })
    }

    /// Produces a two-parent merge commit and advances the exact target ref.
    ///
    /// The two-parent commit is the libgit2 equivalent of `git merge --no-ff`.
    /// Conflicts are detected in an in-memory index, so a rejected delivery
    /// leaves both the target ref and worktree untouched and clean.
    pub fn integrate(&self, request: &IntegrateGitDelivery) -> Result<IntegrationOutcome, GitDeliveryError> {
        if let ReconciliationOutcome::Integrated { target_head } = self.reconcile(&request.delivery)? {
            return Ok(IntegrationOutcome::AlreadyIntegrated { target_head });
        }
        self.preflight(&request.delivery)?;

        let (_, repository) = open_exact_repository(&request.delivery.repository)?;
        let target_commit =
            repository
                .find_commit(request.delivery.target.head)
                .map_err(|_| GitDeliveryError::CommitNotFound {
                    oid: request.delivery.target.head,
                })?;
        let source_commit =
            repository
                .find_commit(request.delivery.source.head)
                .map_err(|_| GitDeliveryError::CommitNotFound {
                    oid: request.delivery.source.head,
                })?;
        let mut merge_index = repository
            .merge_commits(&target_commit, &source_commit, None)
            .map_err(|error| GitDeliveryError::git("compute delivery merge", error))?;
        if merge_index.has_conflicts() {
            return Err(GitDeliveryError::MergeConflict {
                target_head: request.delivery.target.head,
            });
        }
        let tree_oid = merge_index
            .write_tree_to(&repository)
            .map_err(|error| GitDeliveryError::git("write delivery merge tree", error))?;
        let tree = repository
            .find_tree(tree_oid)
            .map_err(|error| GitDeliveryError::git("open delivery merge tree", error))?;
        let signature = merge_signature(&request.commit_identity)?;
        let merge_head = repository
            .commit(
                None,
                &signature,
                &signature,
                &request.message,
                &tree,
                &[&target_commit, &source_commit],
            )
            .map_err(|error| GitDeliveryError::git("create no-fast-forward merge commit", error))?;

        match repository.reference_matching(
            &request.delivery.target.branch_ref,
            merge_head,
            true,
            request.delivery.target.head,
            "integrate exact Git delivery",
        ) {
            Ok(_) => {}
            Err(error) if error.code() == ErrorCode::Modified => {
                let actual = branch_head(&repository, &request.delivery.target.branch_ref)?;
                return Err(GitDeliveryError::TargetMoved {
                    expected: request.delivery.target.head,
                    actual,
                });
            }
            Err(error) => return Err(GitDeliveryError::git("advance target branch", error)),
        }

        if let Err(error) = checkout_integrated_target(
            &repository,
            &request.delivery.target,
            merge_head,
            &self.managed_runtime_skill_source_roots,
        ) {
            return Err(GitDeliveryError::IntegratedButCheckoutFailed {
                merge_head,
                message: error.to_string(),
            });
        }
        Ok(IntegrationOutcome::Integrated { merge_head })
    }

    /// Reconciles a possibly interrupted integration using commit ancestry.
    ///
    /// If the exact source head is already an ancestor of the current target
    /// ref, the delivery is complete even when the original process stopped
    /// before recording success. A moved target that does not contain the
    /// source fails closed and must be reviewed as a new delivery attempt.
    pub fn reconcile(&self, delivery: &ExactGitDelivery) -> Result<ReconciliationOutcome, GitDeliveryError> {
        if delivery.source.branch_ref == delivery.target.branch_ref {
            return Err(GitDeliveryError::SameSourceAndTarget);
        }
        validate_branch_ref(&delivery.source.branch_ref)?;
        validate_branch_ref(&delivery.target.branch_ref)?;
        let (_, repository) = open_exact_repository(&delivery.repository)?;
        ensure_commit(&repository, delivery.base_head)?;
        ensure_commit(&repository, delivery.source.head)?;
        ensure_commit(&repository, delivery.target.head)?;
        ensure_ancestor(&repository, delivery.base_head, delivery.source.head)?;
        ensure_ancestor(&repository, delivery.base_head, delivery.target.head)?;
        let current_target = branch_head(&repository, &delivery.target.branch_ref)?;
        if is_ancestor(&repository, delivery.source.head, current_target)? {
            return Ok(ReconciliationOutcome::Integrated {
                target_head: current_target,
            });
        }
        if current_target == delivery.target.head {
            Ok(ReconciliationOutcome::ReadyToRetry {
                target_head: current_target,
            })
        } else {
            Ok(ReconciliationOutcome::TargetMoved {
                expected_target_head: delivery.target.head,
                actual_target_head: current_target,
            })
        }
    }
}

fn parse_oid(value: &str) -> Result<Oid, GitDeliveryError> {
    value
        .parse::<Oid>()
        .map_err(|_| GitDeliveryError::InvalidCommitId(value.to_owned()))
}

fn resolve_exact_worktree(
    repository: &Repository,
    main_root: &Path,
    branch_ref: &str,
    expected_head: Oid,
    managed_runtime_skill_source_roots: &[PathBuf],
) -> Result<ExactWorktreeHead, GitDeliveryError> {
    validate_branch_ref(branch_ref)?;
    ensure_branch_head(repository, branch_ref, expected_head)?;

    if repository
        .head()
        .ok()
        .and_then(|head| head.name().map(str::to_owned))
        .as_deref()
        == Some(branch_ref)
    {
        validate_exact_worktree(
            repository,
            main_root,
            branch_ref,
            expected_head,
            managed_runtime_skill_source_roots,
        )?;
        return Ok(ExactWorktreeHead {
            path: main_root.to_path_buf(),
            branch_ref: branch_ref.to_owned(),
            head: expected_head,
        });
    }

    let worktree_names = repository
        .worktrees()
        .map_err(|error| GitDeliveryError::git("list registered worktrees", error))?;
    for name in worktree_names.iter().flatten() {
        let worktree = repository
            .find_worktree(name)
            .map_err(|error| GitDeliveryError::git("open registered worktree", error))?;
        let path = repository::canonicalize(worktree.path())?;
        let worktree_repository = Repository::open_from_worktree(&worktree)
            .map_err(|error| GitDeliveryError::git("open registered worktree repository", error))?;
        let checked_out_ref = worktree_repository
            .head()
            .ok()
            .and_then(|head| head.name().map(str::to_owned));
        if checked_out_ref.as_deref() != Some(branch_ref) {
            continue;
        }
        validate_exact_worktree(
            &worktree_repository,
            &path,
            branch_ref,
            expected_head,
            managed_runtime_skill_source_roots,
        )?;
        return Ok(ExactWorktreeHead {
            path,
            branch_ref: branch_ref.to_owned(),
            head: expected_head,
        });
    }
    Err(GitDeliveryError::BranchWorktreeNotFound {
        branch_ref: branch_ref.to_owned(),
    })
}

fn ensure_branch_head(repository: &Repository, branch_ref: &str, expected: Oid) -> Result<(), GitDeliveryError> {
    let actual = branch_head(repository, branch_ref)?;
    if actual == expected {
        Ok(())
    } else {
        Err(GitDeliveryError::BranchHeadMismatch {
            branch_ref: branch_ref.to_owned(),
            expected,
            actual,
        })
    }
}

fn ensure_ancestor(repository: &Repository, ancestor: Oid, descendant: Oid) -> Result<(), GitDeliveryError> {
    if is_ancestor(repository, ancestor, descendant)? {
        Ok(())
    } else {
        Err(GitDeliveryError::BaseNotAncestor {
            base_head: ancestor,
            descendant_head: descendant,
        })
    }
}

fn is_ancestor(repository: &Repository, ancestor: Oid, descendant: Oid) -> Result<bool, GitDeliveryError> {
    if ancestor == descendant {
        return Ok(true);
    }
    repository
        .graph_descendant_of(descendant, ancestor)
        .map_err(|error| GitDeliveryError::git("check commit ancestry", error))
}

fn merge_signature(identity: &MergeCommitIdentity) -> Result<Signature<'_>, GitDeliveryError> {
    let name = identity.name.trim();
    let email = identity.email.trim();
    if name.is_empty() || email.is_empty() {
        return Err(GitDeliveryError::InvalidCommitIdentity(
            "name and email are required".to_owned(),
        ));
    }
    Signature::now(name, email).map_err(|error| GitDeliveryError::InvalidCommitIdentity(error.message().to_owned()))
}

fn checkout_integrated_target(
    root_repository: &Repository,
    target: &ExactWorktreeHead,
    merge_head: Oid,
    managed_runtime_skill_source_roots: &[PathBuf],
) -> Result<(), GitDeliveryError> {
    let target_path = canonicalize(&target.path)?;
    let target_repository = Repository::open(&target_path).map_err(|error| GitDeliveryError::RepositoryResolution {
        path: target_path.clone(),
        message: error.message().to_owned(),
    })?;
    ensure_same_common_repository(root_repository, &target_repository, &target_path)?;
    let mut checkout = CheckoutBuilder::new();
    checkout.safe().recreate_missing(true).update_index(true).refresh(true);
    target_repository
        .checkout_head(Some(&mut checkout))
        .map_err(|error| GitDeliveryError::git("checkout integrated target", error))?;
    validate_exact_worktree(
        &target_repository,
        &target_path,
        &target.branch_ref,
        merge_head,
        managed_runtime_skill_source_roots,
    )
}

fn remove_branch_if_unchanged(repository: &Repository, branch_ref: &str, expected_head: Oid) {
    if repository.refname_to_id(branch_ref) != Ok(expected_head) {
        return;
    }
    if let Ok(mut reference) = repository.find_reference(branch_ref) {
        let _ = reference.delete();
    }
}
