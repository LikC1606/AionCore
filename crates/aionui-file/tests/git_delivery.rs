use std::fs;
use std::path::{Path, PathBuf};

use aionui_file::git_delivery::{
    ExactGitDelivery, ExactRepository, ExactWorktreeHead, GitDeliveryAdapter, GitDeliveryError, IntegrateGitDelivery,
    IntegrationOutcome, MergeCommitIdentity, PrepareMemberWorktree, ReconciliationOutcome, WorktreePreparation,
};
use git2::{IndexAddOption, Oid, Repository, RepositoryState, Signature};
use tempfile::TempDir;

const MAIN_REF: &str = "refs/heads/main";
const MEMBER_REF: &str = "refs/heads/team/member-one";

struct DeliveryFixture {
    _temp: TempDir,
    root: PathBuf,
    member_worktree: PathBuf,
    repository: ExactRepository,
    base_head: Oid,
}

impl DeliveryFixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("repository");
        let worktrees = temp.path().join("worktrees");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&worktrees).unwrap();

        let repository = Repository::init(&root).unwrap();
        repository.set_head(MAIN_REF).unwrap();
        fs::write(root.join("shared.txt"), "base\n").unwrap();
        let base_head = commit_all(&repository, "base");
        drop(repository);

        let adapter = GitDeliveryAdapter::new();
        let identity = adapter.resolve_repository(&root).unwrap();
        let exact_repository = ExactRepository::from(&identity);
        let member_worktree = worktrees.join("member-one");
        let prepared = adapter
            .prepare_member_worktree(&PrepareMemberWorktree {
                repository: exact_repository.clone(),
                worktree_name: "team.member-one".to_owned(),
                worktree_path: member_worktree.clone(),
                branch_ref: MEMBER_REF.to_owned(),
                base_head,
            })
            .unwrap();
        assert_eq!(prepared.preparation, WorktreePreparation::Created);

        Self {
            _temp: temp,
            root,
            member_worktree,
            repository: exact_repository,
            base_head,
        }
    }

    fn source_commit(&self, file: &str, contents: &str) -> Oid {
        commit_file(&self.member_worktree, file, contents, "member change")
    }

    fn target_commit(&self, file: &str, contents: &str) -> Oid {
        commit_file(&self.root, file, contents, "target change")
    }

    fn delivery(&self, source_head: Oid, target_head: Oid) -> ExactGitDelivery {
        ExactGitDelivery {
            repository: self.repository.clone(),
            base_head: self.base_head,
            source: ExactWorktreeHead {
                path: self.member_worktree.clone(),
                branch_ref: MEMBER_REF.to_owned(),
                head: source_head,
            },
            target: ExactWorktreeHead {
                path: self.root.clone(),
                branch_ref: MAIN_REF.to_owned(),
                head: target_head,
            },
        }
    }

    fn integration(&self, source_head: Oid, target_head: Oid) -> IntegrateGitDelivery {
        IntegrateGitDelivery {
            delivery: self.delivery(source_head, target_head),
            commit_identity: MergeCommitIdentity {
                name: "Team Integrator".to_owned(),
                email: "team-integrator@example.test".to_owned(),
            },
            message: "Integrate member delivery".to_owned(),
        }
    }
}

#[test]
fn resolves_repository_identity_and_reuses_exact_prepared_worktree() {
    let fixture = DeliveryFixture::new();
    let adapter = GitDeliveryAdapter::new();
    let identity = adapter.resolve_repository(&fixture.member_worktree).unwrap();
    assert_eq!(identity.repository_id(), fixture.repository.repository_id);
    assert_eq!(identity.root(), fixture.root.canonicalize().unwrap());

    let prepared = adapter
        .prepare_member_worktree(&PrepareMemberWorktree {
            repository: fixture.repository.clone(),
            worktree_name: "team.member-one".to_owned(),
            worktree_path: fixture.member_worktree.clone(),
            branch_ref: MEMBER_REF.to_owned(),
            base_head: fixture.base_head,
        })
        .unwrap();
    assert_eq!(prepared.preparation, WorktreePreparation::Reused);
    assert_eq!(prepared.head, fixture.base_head);
}

#[test]
fn resolves_the_clean_current_target_worktree() {
    let fixture = DeliveryFixture::new();
    let (identity, target) = GitDeliveryAdapter::new()
        .resolve_current_worktree(&fixture.root)
        .unwrap();

    assert_eq!(identity.repository_id(), fixture.repository.repository_id);
    assert_eq!(target.path, fixture.root.canonicalize().unwrap());
    assert_eq!(target.branch_ref, MAIN_REF);
    assert_eq!(target.head, fixture.base_head);
}

#[cfg(unix)]
#[test]
fn configured_adapter_allows_only_managed_untracked_codex_skill_links() {
    use std::os::unix::fs::symlink;

    let fixture = DeliveryFixture::new();
    let source_root = fixture.root.parent().unwrap().join("managed-skills");
    let source = source_root.join("auto-inject/runtime-skill");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("SKILL.md"), "# Managed runtime Skill\n").unwrap();
    let link = fixture.root.join(".codex/skills/runtime-skill");
    fs::create_dir_all(link.parent().unwrap()).unwrap();
    symlink(&source, &link).unwrap();

    assert!(matches!(
        GitDeliveryAdapter::new()
            .resolve_current_worktree(&fixture.root)
            .unwrap_err(),
        GitDeliveryError::DirtyWorktree { .. }
    ));
    GitDeliveryAdapter::with_managed_runtime_skill_sources([source_root])
        .resolve_current_worktree(&fixture.root)
        .unwrap();
}

#[cfg(unix)]
#[test]
fn managed_skill_policy_does_not_hide_other_codex_changes_or_foreign_links() {
    use std::os::unix::fs::symlink;

    let fixture = DeliveryFixture::new();
    let source_root = fixture.root.parent().unwrap().join("managed-skills");
    let source = source_root.join("runtime-skill");
    fs::create_dir_all(&source).unwrap();
    fs::write(source.join("SKILL.md"), "# Managed runtime Skill\n").unwrap();
    let link = fixture.root.join(".codex/skills/runtime-skill");
    fs::create_dir_all(link.parent().unwrap()).unwrap();
    symlink(&source, &link).unwrap();
    fs::write(fixture.root.join(".codex/config.toml"), "model = 'user-choice'\n").unwrap();

    let adapter = GitDeliveryAdapter::with_managed_runtime_skill_sources([source_root]);
    assert!(matches!(
        adapter.resolve_current_worktree(&fixture.root).unwrap_err(),
        GitDeliveryError::DirtyWorktree { .. }
    ));

    fs::remove_file(fixture.root.join(".codex/config.toml")).unwrap();
    fs::remove_file(&link).unwrap();
    let foreign_source = fixture.root.parent().unwrap().join("foreign-skills/runtime-skill");
    fs::create_dir_all(&foreign_source).unwrap();
    fs::write(foreign_source.join("SKILL.md"), "# User-owned external Skill\n").unwrap();
    symlink(foreign_source, link).unwrap();
    assert!(matches!(
        adapter.resolve_current_worktree(&fixture.root).unwrap_err(),
        GitDeliveryError::DirtyWorktree { .. }
    ));
}

#[test]
fn managed_skill_policy_does_not_hide_regular_skill_directories() {
    let fixture = DeliveryFixture::new();
    let source_root = fixture.root.parent().unwrap().join("managed-skills");
    fs::create_dir_all(source_root.join("runtime-skill")).unwrap();
    fs::write(source_root.join("runtime-skill/SKILL.md"), "# Managed source Skill\n").unwrap();
    let workspace_skill = fixture.root.join(".codex/skills/runtime-skill");
    fs::create_dir_all(&workspace_skill).unwrap();
    fs::write(workspace_skill.join("SKILL.md"), "# User workspace Skill\n").unwrap();

    let adapter = GitDeliveryAdapter::with_managed_runtime_skill_sources([source_root]);
    assert!(matches!(
        adapter.resolve_current_worktree(&fixture.root).unwrap_err(),
        GitDeliveryError::DirtyWorktree { .. }
    ));
}

#[cfg(unix)]
#[test]
fn managed_skill_policy_does_not_hide_tracked_skill_link_changes() {
    use std::os::unix::fs::symlink;

    let fixture = DeliveryFixture::new();
    let first_source_root = fixture.root.parent().unwrap().join("managed-skills-first");
    let second_source_root = fixture.root.parent().unwrap().join("managed-skills-second");
    let first_source = first_source_root.join("runtime-skill");
    let second_source = second_source_root.join("runtime-skill");
    for source in [&first_source, &second_source] {
        fs::create_dir_all(source).unwrap();
        fs::write(source.join("SKILL.md"), "# Managed runtime Skill\n").unwrap();
    }
    let link = fixture.root.join(".codex/skills/runtime-skill");
    fs::create_dir_all(link.parent().unwrap()).unwrap();
    symlink(&first_source, &link).unwrap();
    let repository = Repository::open(&fixture.root).unwrap();
    let mut index = repository.index().unwrap();
    index.add_path(Path::new(".codex/skills/runtime-skill")).unwrap();
    index.write().unwrap();
    commit_all(&repository, "track workspace Skill link");
    fs::remove_file(&link).unwrap();
    symlink(&second_source, &link).unwrap();

    let adapter = GitDeliveryAdapter::with_managed_runtime_skill_sources([first_source_root, second_source_root]);
    assert!(matches!(
        adapter.resolve_current_worktree(&fixture.root).unwrap_err(),
        GitDeliveryError::DirtyWorktree { .. }
    ));
}

#[test]
fn resolves_durable_string_coordinates_to_exact_registered_worktrees() {
    let fixture = DeliveryFixture::new();
    let source_head = fixture.source_commit("source.txt", "source\n");
    let resolved = GitDeliveryAdapter::new()
        .resolve_exact_delivery(
            fixture.repository.clone(),
            &fixture.base_head.to_string(),
            MEMBER_REF,
            &source_head.to_string(),
            MAIN_REF,
            &fixture.base_head.to_string(),
        )
        .unwrap();

    assert_eq!(resolved.base_head, fixture.base_head);
    assert_eq!(resolved.source.path, fixture.member_worktree.canonicalize().unwrap());
    assert_eq!(resolved.source.head, source_head);
    assert_eq!(resolved.target.path, fixture.root.canonicalize().unwrap());
    assert_eq!(resolved.target.head, fixture.base_head);
}

#[test]
fn durable_coordinate_resolution_rejects_an_unchecked_out_branch() {
    let fixture = DeliveryFixture::new();
    let repository = Repository::open(&fixture.root).unwrap();
    repository
        .reference("refs/heads/not-checked-out", fixture.base_head, false, "test branch")
        .unwrap();

    let error = GitDeliveryAdapter::new()
        .resolve_exact_delivery(
            fixture.repository.clone(),
            &fixture.base_head.to_string(),
            "refs/heads/not-checked-out",
            &fixture.base_head.to_string(),
            MAIN_REF,
            &fixture.base_head.to_string(),
        )
        .unwrap_err();
    assert!(matches!(
        error,
        GitDeliveryError::BranchWorktreeNotFound { branch_ref }
            if branch_ref == "refs/heads/not-checked-out"
    ));
}

#[test]
fn preflight_rejects_a_spoofed_source_head() {
    let fixture = DeliveryFixture::new();
    let actual_source = fixture.source_commit("source.txt", "source\n");
    let delivery = fixture.delivery(fixture.base_head, fixture.base_head);

    let error = GitDeliveryAdapter::new().preflight(&delivery).unwrap_err();
    assert!(matches!(
        error,
        GitDeliveryError::BranchHeadMismatch {
            branch_ref,
            expected,
            actual,
        } if branch_ref == MEMBER_REF && expected == fixture.base_head && actual == actual_source
    ));
}

#[test]
fn preflight_rejects_a_dirty_member_worktree() {
    let fixture = DeliveryFixture::new();
    let source_head = fixture.source_commit("source.txt", "source\n");
    fs::write(fixture.member_worktree.join("untracked.txt"), "not delivered\n").unwrap();

    let error = GitDeliveryAdapter::new()
        .preflight(&fixture.delivery(source_head, fixture.base_head))
        .unwrap_err();
    assert!(matches!(
        error,
        GitDeliveryError::DirtyWorktree { path } if path == fixture.member_worktree.canonicalize().unwrap()
    ));
}

#[test]
fn preflight_rejects_a_spoofed_target_head() {
    let fixture = DeliveryFixture::new();
    let source_head = fixture.source_commit("source.txt", "source\n");
    let actual_target = fixture.target_commit("target.txt", "target\n");

    let error = GitDeliveryAdapter::new()
        .preflight(&fixture.delivery(source_head, fixture.base_head))
        .unwrap_err();
    assert!(matches!(
        error,
        GitDeliveryError::BranchHeadMismatch {
            branch_ref,
            expected,
            actual,
        } if branch_ref == MAIN_REF && expected == fixture.base_head && actual == actual_target
    ));
}

#[test]
fn preflight_rejects_a_dirty_target_worktree() {
    let fixture = DeliveryFixture::new();
    let source_head = fixture.source_commit("source.txt", "source\n");
    fs::write(fixture.root.join("untracked.txt"), "local target work\n").unwrap();

    let error = GitDeliveryAdapter::new()
        .preflight(&fixture.delivery(source_head, fixture.base_head))
        .unwrap_err();
    assert!(matches!(
        error,
        GitDeliveryError::DirtyWorktree { path } if path == fixture.root.canonicalize().unwrap()
    ));
}

#[test]
fn conflict_aborts_without_moving_or_dirtying_the_target() {
    let fixture = DeliveryFixture::new();
    let source_head = fixture.source_commit("shared.txt", "source version\n");
    let target_head = fixture.target_commit("shared.txt", "target version\n");

    let error = GitDeliveryAdapter::new()
        .integrate(&fixture.integration(source_head, target_head))
        .unwrap_err();
    assert!(matches!(
        error,
        GitDeliveryError::MergeConflict { target_head: actual } if actual == target_head
    ));

    let repository = Repository::open(&fixture.root).unwrap();
    assert_eq!(repository.refname_to_id(MAIN_REF).unwrap(), target_head);
    assert_eq!(repository.state(), RepositoryState::Clean);
    assert!(repository.statuses(None).unwrap().is_empty());
    assert_eq!(
        fs::read_to_string(fixture.root.join("shared.txt")).unwrap(),
        "target version\n"
    );
}

#[test]
fn integration_is_no_ff_and_same_request_retries_as_already_integrated() {
    let fixture = DeliveryFixture::new();
    let source_head = fixture.source_commit("source.txt", "source\n");
    let request = fixture.integration(source_head, fixture.base_head);

    let first = GitDeliveryAdapter::new().integrate(&request).unwrap();
    let IntegrationOutcome::Integrated { merge_head } = first else {
        panic!("first integration must create a merge commit");
    };
    let repository = Repository::open(&fixture.root).unwrap();
    let merge = repository.find_commit(merge_head).unwrap();
    assert_eq!(merge.parent_count(), 2);
    assert_eq!(merge.parent_id(0).unwrap(), fixture.base_head);
    assert_eq!(merge.parent_id(1).unwrap(), source_head);
    assert_eq!(repository.refname_to_id(MAIN_REF).unwrap(), merge_head);
    assert!(repository.statuses(None).unwrap().is_empty());
    assert_eq!(fs::read_to_string(fixture.root.join("source.txt")).unwrap(), "source\n");

    assert_eq!(
        GitDeliveryAdapter::new().integrate(&request).unwrap(),
        IntegrationOutcome::AlreadyIntegrated {
            target_head: merge_head
        }
    );
    assert_eq!(
        GitDeliveryAdapter::new().reconcile(&request.delivery).unwrap(),
        ReconciliationOutcome::Integrated {
            target_head: merge_head
        }
    );
}

#[test]
fn independently_integrated_source_is_detected_by_exact_ancestry() {
    let fixture = DeliveryFixture::new();
    let source_head = fixture.source_commit("source.txt", "source\n");
    let delivery = fixture.delivery(source_head, fixture.base_head);

    let root_repository = Repository::open(&fixture.root).unwrap();
    root_repository
        .reference_matching(
            MAIN_REF,
            source_head,
            true,
            fixture.base_head,
            "external exact integration",
        )
        .unwrap();
    root_repository.checkout_head(None).unwrap();

    assert_eq!(
        GitDeliveryAdapter::new().reconcile(&delivery).unwrap(),
        ReconciliationOutcome::Integrated {
            target_head: source_head
        }
    );
    assert_eq!(
        GitDeliveryAdapter::new()
            .integrate(&IntegrateGitDelivery {
                delivery,
                commit_identity: MergeCommitIdentity {
                    name: "Team Integrator".to_owned(),
                    email: "team-integrator@example.test".to_owned(),
                },
                message: "must not create a duplicate merge".to_owned(),
            })
            .unwrap(),
        IntegrationOutcome::AlreadyIntegrated {
            target_head: source_head
        }
    );
}

#[test]
fn reconcile_requires_review_when_target_moved_without_the_source() {
    let fixture = DeliveryFixture::new();
    let source_head = fixture.source_commit("source.txt", "source\n");
    let delivery = fixture.delivery(source_head, fixture.base_head);
    let actual_target = fixture.target_commit("target.txt", "target\n");

    assert_eq!(
        GitDeliveryAdapter::new().reconcile(&delivery).unwrap(),
        ReconciliationOutcome::TargetMoved {
            expected_target_head: fixture.base_head,
            actual_target_head: actual_target,
        }
    );
}

#[test]
fn repository_id_must_match_the_canonical_root_exactly() {
    let fixture = DeliveryFixture::new();
    let mut delivery = fixture.delivery(fixture.base_head, fixture.base_head);
    delivery.repository.repository_id.push_str("-spoofed");

    let error = GitDeliveryAdapter::new().preflight(&delivery).unwrap_err();
    assert!(matches!(error, GitDeliveryError::RepositoryIdMismatch { .. }));
}

fn commit_file(repository_path: &Path, relative_path: &str, contents: &str, message: &str) -> Oid {
    fs::write(repository_path.join(relative_path), contents).unwrap();
    let repository = Repository::open(repository_path).unwrap();
    commit_all(&repository, message)
}

fn commit_all(repository: &Repository, message: &str) -> Oid {
    let mut index = repository.index().unwrap();
    index.add_all(["*"].iter(), IndexAddOption::DEFAULT, None).unwrap();
    index.write().unwrap();
    let tree_id = index.write_tree().unwrap();
    let tree = repository.find_tree(tree_id).unwrap();
    let signature = Signature::now("Test Author", "test-author@example.test").unwrap();
    let parents = repository
        .head()
        .ok()
        .and_then(|head| head.target())
        .map(|head| repository.find_commit(head).unwrap());
    match parents.as_ref() {
        Some(parent) => repository
            .commit(Some("HEAD"), &signature, &signature, message, &tree, &[parent])
            .unwrap(),
        None => repository
            .commit(Some(MAIN_REF), &signature, &signature, message, &tree, &[])
            .unwrap(),
    }
}
