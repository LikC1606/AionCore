use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use aionui_api_types::{
    ProjectGitBindConversationRequest, ProjectGitBindConversationResponse, ProjectGitBlobRequest,
    ProjectGitBlobResponse, ProjectGitChangedFileResponse, ProjectGitCommitDetailResponse, ProjectGitCommitRequest,
    ProjectGitCommitResponse, ProjectGitCommitSummaryResponse, ProjectGitDiffRequest, ProjectGitDiffResponse,
    ProjectGitDiscoverResponse, ProjectGitFrontierResponse, ProjectGitGraphRequest, ProjectGitGraphResponse,
    ProjectGitPreflightResponse, ProjectGitPrepareWorkspaceRequest, ProjectGitPrepareWorkspaceResponse,
    ProjectGitRepositoryResponse, ProjectGitResearchBranchResponse, ProjectGitTreeEntryResponse, ProjectGitTreeRequest,
    ProjectGitTreeResponse, ProjectGitWorkingDiffRequest, ProjectGitWorkingTreeResponse,
    ProjectGitWorkspaceBindingResponse,
};
use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};
use git2::{
    BranchType, Delta, Diff, DiffFlags, DiffFormat, DiffOptions, ObjectType, Oid, Repository, RepositoryInitOptions,
    Signature, Sort, Status, StatusOptions,
};
use serde_json::json;

use crate::error::FileError;

const MAX_DIRECT_CHILDREN: usize = 256;
const MAX_GRAPH_COMMITS: usize = 200;
const MAX_TEXT_BYTES: usize = 5 * 1024 * 1024;
const MAX_BINARY_BYTES: usize = 10 * 1024 * 1024;
const PROJECT_MANIFEST_PATH: &str = ".deepscientist/project.json";
const DEEPSCIENTIST_GITIGNORE_PATH: &str = ".deepscientist/.gitignore";

const MANAGED_ROOT_GITIGNORE: &str = ".DS_Store\n\
.deepscientist/artifact-repo/\n\
.deepscientist/science-artifacts/\n\
.deepscientist/factor-search/\n\
.agents/skills/\n\
.codex/skills/\n\
node_modules/\n\
out/\n\
dist/\n\
build/\n\
coverage/\n\
.cache/\n\
__pycache__/\n\
.pytest_cache/\n\
*.log\n\
.env\n\
.env.*\n\
*.pem\n\
*.key\n";

const MANAGED_LOCAL_EXCLUDES: &str = ".agents/\n\
.codex/\n\
.deepscientist/artifact-repo/\n\
.deepscientist/science-artifacts/\n\
.deepscientist/exports/\n\
.deepscientist/factor-search/\n";

struct RepositoryContext {
    repository: Repository,
    response: ProjectGitRepositoryResponse,
}

fn managed_conversation_id(value: &str) -> Result<&str, FileError> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 120
        || !value
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err(FileError::BadRequest(
            "Invalid managed project conversation id.".to_owned(),
        ));
    }
    Ok(value)
}

fn checkpoint_conversation_id(value: &str) -> Result<&str, FileError> {
    let value = value.trim();
    if value.is_empty() || value.len() > 120 || value.contains(['\0', '\n', '\r']) {
        return Err(FileError::BadRequest("Invalid project conversation id.".to_owned()));
    }
    Ok(value)
}

fn managed_project_response(
    workspace: &Path,
    project_id: &str,
    created: bool,
) -> Result<ProjectGitPrepareWorkspaceResponse, FileError> {
    let repository = Repository::open(workspace)
        .map_err(|error| FileError::Internal(format!("Unable to open managed Git repository: {error}")))?;
    let selected = fs::canonicalize(workspace)
        .map_err(|error| FileError::Internal(format!("Unable to resolve managed project workspace: {error}")))?;
    let repository_response = repository_response(&repository, &selected, false)?;
    let head = repository_response
        .head
        .clone()
        .ok_or_else(|| FileError::Internal("The managed project has no baseline commit.".to_owned()))?;
    let root = response_path(&selected);
    Ok(ProjectGitPrepareWorkspaceResponse {
        ok: true,
        workspace: Some(root.clone()),
        project_id: Some(project_id.to_owned()),
        created: Some(created),
        binding: Some(ProjectGitWorkspaceBindingResponse {
            schema: "deepscientist.project_git.v1".to_owned(),
            strategy: "managed".to_owned(),
            project_id: Some(project_id.to_owned()),
            source_root: root.clone(),
            workspace_root: root,
            branch: repository_response.branch,
            base_commit: Some(head),
            source_dirty: Some(false),
        }),
        error: None,
    })
}

fn validate_existing_managed_project(workspace: &Path, project_id: &str) -> Result<(), FileError> {
    let manifest_path = workspace.join(PROJECT_MANIFEST_PATH);
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&manifest_path)
            .map_err(|_| FileError::BadRequest("The managed project manifest is missing.".to_owned()))?,
    )
    .map_err(|_| FileError::BadRequest("The managed project manifest is invalid.".to_owned()))?;
    if manifest.get("managed").and_then(serde_json::Value::as_bool) != Some(true)
        || manifest.get("projectId").and_then(serde_json::Value::as_str) != Some(project_id)
    {
        return Err(FileError::Forbidden(
            "The existing workspace is not the expected managed project.".to_owned(),
        ));
    }
    let repository = Repository::open(workspace)
        .map_err(|_| FileError::BadRequest("The managed project Git repository is missing.".to_owned()))?;
    let head = repository
        .head()
        .and_then(|reference| reference.peel_to_commit())
        .map_err(|_| FileError::BadRequest("The managed project baseline is missing.".to_owned()))?;
    let message = head.message().unwrap_or_default();
    if !message.lines().any(|line| line == "DeepScientist-Checkpoint: true")
        || !message.lines().any(|line| line == "DeepScientist-Stage: project")
    {
        return Err(FileError::Forbidden(
            "The existing workspace is not a DeepScientist managed project.".to_owned(),
        ));
    }
    Ok(())
}

/// Create an idempotent managed repository below the server-owned work
/// directory. `baseDir` is accepted for desktop contract parity, but it may not
/// redirect the WebUI backend to another filesystem location.
pub fn prepare_managed_workspace(
    request: ProjectGitPrepareWorkspaceRequest,
    trusted_work_dir: PathBuf,
) -> Result<ProjectGitPrepareWorkspaceResponse, FileError> {
    if request.strategy != "managed" {
        return Err(FileError::BadRequest(
            "WebUI project preparation currently supports only the managed strategy.".to_owned(),
        ));
    }
    if request.source_workspace.is_some() || request.branch_kind.is_some() {
        return Err(FileError::BadRequest(
            "Managed project preparation does not accept a source workspace or branch kind.".to_owned(),
        ));
    }
    let goal = request.goal.trim();
    if goal.is_empty() || goal.chars().count() > 16_000 {
        return Err(FileError::BadRequest("A bounded project goal is required.".to_owned()));
    }
    let conversation_id = managed_conversation_id(&request.conversation_id)?;

    fs::create_dir_all(&trusted_work_dir)
        .map_err(|error| FileError::Internal(format!("Unable to create the server work directory: {error}")))?;
    let trusted_work_dir = fs::canonicalize(&trusted_work_dir)
        .map_err(|error| FileError::Internal(format!("Unable to resolve the server work directory: {error}")))?;
    let requested_base = fs::canonicalize(request.base_dir.trim())
        .map_err(|_| FileError::Forbidden("Managed projects must use the server work directory.".to_owned()))?;
    if requested_base != trusted_work_dir {
        return Err(FileError::Forbidden(
            "Managed projects must use the server work directory.".to_owned(),
        ));
    }

    let managed_root = trusted_work_dir.join("managed-projects");
    fs::create_dir_all(&managed_root)
        .map_err(|error| FileError::Internal(format!("Unable to create the managed projects directory: {error}")))?;
    let managed_root = fs::canonicalize(&managed_root)
        .map_err(|error| FileError::Internal(format!("Unable to resolve the managed projects directory: {error}")))?;
    let project_id = if conversation_id.starts_with("project_") {
        conversation_id.to_owned()
    } else {
        format!("project_{conversation_id}")
    };
    let workspace = managed_root.join(&project_id);
    if workspace.exists() {
        validate_existing_managed_project(&workspace, &project_id)?;
        return managed_project_response(&workspace, &project_id, false);
    }

    let temporary = managed_root.join(format!(
        ".{project_id}.{}.{}.tmp",
        std::process::id(),
        Utc::now().timestamp_millis()
    ));
    fs::create_dir(&temporary)
        .map_err(|error| FileError::Internal(format!("Unable to reserve the managed project workspace: {error}")))?;
    let initialized = (|| -> Result<(), FileError> {
        fs::create_dir_all(temporary.join(".deepscientist"))
            .map_err(|error| FileError::Internal(format!("Unable to create managed project metadata: {error}")))?;
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        let name: String = goal.chars().take(120).collect();
        let manifest = json!({
            "schema": "deepscientist.project.v1",
            "id": project_id,
            "projectId": project_id,
            "name": name,
            "goal": goal,
            "managed": true,
            "createdAt": now,
            "updatedAt": now,
            "artifactRepoRelativePath": ".deepscientist/artifact-repo",
            "scienceArtifactsRelativePath": ".deepscientist/science-artifacts",
            "exportsRelativePath": ".deepscientist/exports"
        });
        fs::write(
            temporary.join(PROJECT_MANIFEST_PATH),
            format!(
                "{}\n",
                serde_json::to_string_pretty(&manifest).expect("manifest serialization")
            ),
        )
        .map_err(|error| FileError::Internal(format!("Unable to write the managed project manifest: {error}")))?;
        fs::write(
            temporary.join(DEEPSCIENTIST_GITIGNORE_PATH),
            "/artifact-repo/\n/science-artifacts/\n/exports/\n",
        )
        .map_err(|error| FileError::Internal(format!("Unable to write managed project exclusions: {error}")))?;
        fs::write(temporary.join(".gitignore"), MANAGED_ROOT_GITIGNORE)
            .map_err(|error| FileError::Internal(format!("Unable to write the managed project .gitignore: {error}")))?;
        fs::write(
            temporary.join("README.md"),
            format!("# DeepScientist Project\n\n## Goal\n\n{goal}\n"),
        )
        .map_err(|error| FileError::Internal(format!("Unable to write the managed project README: {error}")))?;

        let mut options = RepositoryInitOptions::new();
        options.initial_head("main");
        let repository = Repository::init_opts(&temporary, &options).map_err(|error| {
            FileError::Internal(format!("Unable to initialize the managed Git repository: {error}"))
        })?;
        let exclude_path = repository.path().join("info").join("exclude");
        fs::write(&exclude_path, MANAGED_LOCAL_EXCLUDES)
            .map_err(|error| FileError::Internal(format!("Unable to write managed Git exclusions: {error}")))?;
        let mut index = repository
            .index()
            .map_err(|error| FileError::Internal(format!("Unable to open the managed Git index: {error}")))?;
        for path in [
            ".gitignore",
            "README.md",
            DEEPSCIENTIST_GITIGNORE_PATH,
            PROJECT_MANIFEST_PATH,
        ] {
            index
                .add_path(Path::new(path))
                .map_err(|error| FileError::Internal(format!("Unable to stage {path}: {error}")))?;
        }
        index
            .write()
            .map_err(|error| FileError::Internal(format!("Unable to write the managed Git index: {error}")))?;
        let tree_id = index
            .write_tree()
            .map_err(|error| FileError::Internal(format!("Unable to write the managed project tree: {error}")))?;
        let tree = repository
            .find_tree(tree_id)
            .map_err(|error| FileError::Internal(format!("Unable to read the managed project tree: {error}")))?;
        let signature = Signature::now("DeepScientist", "checkpoint@deepscientist.local")
            .map_err(|error| FileError::Internal(format!("Unable to create the managed project signature: {error}")))?;
        let message = format!(
            "[project] initialize managed project\n\nDeepScientist-Checkpoint: true\nDeepScientist-Stage: project\nDeepScientist-Conversation: {conversation_id}"
        );
        repository
            .commit(Some("HEAD"), &signature, &signature, &message, &tree, &[])
            .map_err(|error| FileError::Internal(format!("Unable to create the managed project baseline: {error}")))?;
        Ok(())
    })();
    if let Err(error) = initialized {
        let _ = fs::remove_dir_all(&temporary);
        return Err(error);
    }
    if let Err(error) = fs::rename(&temporary, &workspace) {
        let _ = fs::remove_dir_all(&temporary);
        if workspace.exists() {
            validate_existing_managed_project(&workspace, &project_id)?;
            return managed_project_response(&workspace, &project_id, false);
        }
        return Err(FileError::Internal(format!(
            "Unable to publish the managed project workspace: {error}"
        )));
    }
    managed_project_response(&workspace, &project_id, true)
}

/// Bind a DeepScientist project checkpoint to the real conversation id.
///
/// The caller is responsible for constraining `workspace` to a trusted root.
/// Both server-managed projects and explicitly selected current worktrees are
/// accepted, but an ordinary Git commit is never amended: the current HEAD
/// must already carry the DeepScientist project-checkpoint trailers.
pub fn bind_project_conversation(
    request: ProjectGitBindConversationRequest,
    workspace: PathBuf,
) -> Result<ProjectGitBindConversationResponse, FileError> {
    let conversation_id = checkpoint_conversation_id(&request.conversation_id)?;
    let repository = Repository::discover(&workspace)
        .map_err(|error| FileError::BadRequest(format!("Unable to open the project Git repository: {error}")))?;
    let head = repository
        .head()
        .and_then(|reference| reference.peel_to_commit())
        .map_err(|_| FileError::BadRequest("The project baseline is missing.".to_owned()))?;
    let message = head.message().unwrap_or_default();
    if !message.lines().any(|line| line == "DeepScientist-Checkpoint: true")
        || !message.lines().any(|line| line == "DeepScientist-Stage: project")
    {
        return Err(FileError::Forbidden(
            "The project baseline is not a DeepScientist project checkpoint.".to_owned(),
        ));
    }
    let current = message
        .lines()
        .find_map(|line| line.strip_prefix("DeepScientist-Conversation: "));
    let commit_id = if current == Some(conversation_id) {
        head.id()
    } else {
        let mut lines: Vec<&str> = message
            .lines()
            .filter(|line| !line.starts_with("DeepScientist-Conversation: "))
            .collect();
        lines.push("DeepScientist-Conversation: __BOUND_CONVERSATION__");
        let updated = lines.join("\n").replace("__BOUND_CONVERSATION__", conversation_id);
        head.amend(Some("HEAD"), None, None, None, Some(&updated), None)
            .map_err(|error| FileError::Internal(format!("Unable to bind the project baseline: {error}")))?
    };
    drop(head);
    let selected = fs::canonicalize(&workspace)
        .map_err(|error| FileError::Internal(format!("Unable to resolve the managed project workspace: {error}")))?;
    let response = repository_response(&repository, &selected, false)?;
    let commit = commit_id.to_string();
    Ok(ProjectGitBindConversationResponse {
        ok: true,
        repository: Some(response),
        short_commit: Some(short_oid(commit_id)),
        commit: Some(commit),
        error: None,
    })
}

pub fn preflight_workspace(workspace: PathBuf) -> Result<ProjectGitPreflightResponse, FileError> {
    preflight_workspace_with_status(workspace, true)
}

pub fn preflight_workspace_with_status(
    workspace: PathBuf,
    include_status: bool,
) -> Result<ProjectGitPreflightResponse, FileError> {
    if !workspace.is_dir() {
        return Err(FileError::BadRequest(
            "The project workspace must be a directory.".to_owned(),
        ));
    }

    if let Ok(repository) = Repository::discover(&workspace) {
        return response_for_repository(repository, workspace, include_status);
    }

    let candidates = direct_child_repositories(&workspace)?;
    match candidates.as_slice() {
        [] => Ok(ProjectGitPreflightResponse {
            ok: true,
            kind: Some("non-git".to_owned()),
            repository: None,
            can_isolate: Some(false),
            error: None,
            repository_candidates: None,
        }),
        [candidate] => {
            let repository = Repository::open(candidate)
                .map_err(|error| FileError::Internal(format!("Unable to open Git repository: {error}")))?;
            response_for_repository(repository, candidate.clone(), include_status)
        }
        _ => Ok(ProjectGitPreflightResponse {
            ok: false,
            kind: None,
            repository: None,
            can_isolate: None,
            error: Some(
                "The selected directory contains multiple Git projects. Choose the specific project folder.".to_owned(),
            ),
            repository_candidates: Some(candidates.iter().map(|path| response_path(path)).collect()),
        }),
    }
}

fn direct_child_repositories(workspace: &Path) -> Result<Vec<PathBuf>, FileError> {
    let entries = fs::read_dir(workspace)
        .map_err(|error| FileError::BadRequest(format!("Unable to inspect project directory: {error}")))?;
    let mut roots = HashSet::new();
    for entry in entries.take(MAX_DIRECT_CHILDREN) {
        let entry = match entry {
            Ok(value) => value,
            Err(_) => continue,
        };
        let path = entry.path();
        if !entry.file_type().map(|kind| kind.is_dir()).unwrap_or(false) {
            continue;
        }
        let repository = match Repository::open(&path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        if let Some(root) = repository_worktree_root(&repository) {
            roots.insert(root);
        }
    }
    let mut candidates: Vec<_> = roots.into_iter().collect();
    candidates.sort();
    Ok(candidates)
}

fn repository_worktree_root(repository: &Repository) -> Option<PathBuf> {
    repository
        .workdir()
        .or_else(|| repository.is_bare().then(|| repository.path()))
        .and_then(|path| fs::canonicalize(path).ok())
}

fn response_for_repository(
    repository: Repository,
    selected_workspace: PathBuf,
    include_status: bool,
) -> Result<ProjectGitPreflightResponse, FileError> {
    let repo_root = repository_worktree_root(&repository)
        .ok_or_else(|| FileError::Internal("Unable to resolve the Git repository root.".to_owned()))?;
    let workspace_root = fs::canonicalize(&selected_workspace)
        .map_err(|error| FileError::BadRequest(format!("Unable to resolve project workspace: {error}")))?;
    let prefix = workspace_root
        .strip_prefix(&repo_root)
        .ok()
        .and_then(|path| (!path.as_os_str().is_empty()).then(|| path.to_string_lossy().replace('\\', "/")));
    let head_reference = repository.head().ok();
    let branch = head_reference
        .as_ref()
        .and_then(|reference| reference.shorthand())
        .map(str::to_owned);
    let head = head_reference
        .as_ref()
        .and_then(|reference| reference.target())
        .map(|oid| oid.to_string());
    let detached = repository.head_detached().unwrap_or(false);
    let bare = repository.is_bare();
    let (staged_count, modified_count, untracked_count, dirty_file_count) = if include_status {
        status_counts(&repository)?
    } else {
        (0, 0, 0, 0)
    };
    let can_isolate = head.is_some() && !bare && prefix.is_none();

    Ok(ProjectGitPreflightResponse {
        ok: true,
        kind: Some("git".to_owned()),
        repository: Some(ProjectGitRepositoryResponse {
            workspace_root: response_path(&workspace_root),
            repo_root: response_path(&repo_root),
            workspace_prefix: prefix,
            branch,
            head,
            detached,
            bare,
            dirty: staged_count + modified_count + untracked_count > 0,
            staged_count,
            modified_count,
            untracked_count,
            dirty_file_count,
        }),
        can_isolate: Some(can_isolate),
        error: None,
        repository_candidates: None,
    })
}

pub fn discover_workspace(workspace: PathBuf) -> Result<ProjectGitDiscoverResponse, FileError> {
    // Discovery is the one metadata endpoint that promises an up-to-date
    // working-tree summary. History/tree/blob endpoints use lightweight
    // repository metadata so opening a large project does not rescan every
    // untracked file for each parallel request.
    let context = open_repository_with_status(workspace, true)?;
    Ok(ProjectGitDiscoverResponse {
        ok: true,
        repository: Some(context.response),
        error: None,
    })
}

pub fn graph(request: ProjectGitGraphRequest, workspace: PathBuf) -> Result<ProjectGitGraphResponse, FileError> {
    let context = open_repository(workspace)?;
    let repository = &context.repository;
    let limit = request.limit.unwrap_or(100).clamp(1, MAX_GRAPH_COMMITS);
    let skip = request.skip.unwrap_or(0);
    let mut walk = repository
        .revwalk()
        .map_err(|error| FileError::Internal(format!("Unable to read Git history: {error}")))?;
    walk.set_sorting(Sort::TOPOLOGICAL | Sort::TIME)
        .map_err(|error| FileError::Internal(format!("Unable to order Git history: {error}")))?;
    if let Some(value) = request.r#ref.as_deref() {
        walk.push(resolve_revision(repository, value)?)
            .map_err(|error| FileError::BadRequest(format!("Unable to read the selected Git revision: {error}")))?;
    } else if request.all_refs.unwrap_or(false) {
        for reference in repository
            .references()
            .map_err(|error| FileError::Internal(format!("Unable to list Git references: {error}")))?
        {
            let Ok(reference) = reference else { continue };
            let Ok(object) = reference.peel(ObjectType::Commit) else {
                continue;
            };
            let _ = walk.push(object.id());
        }
    } else if let Ok(head) = repository.head().and_then(|reference| reference.peel_to_commit()) {
        walk.push(head.id())
            .map_err(|error| FileError::Internal(format!("Unable to read Git HEAD: {error}")))?;
    } else {
        return Ok(ProjectGitGraphResponse {
            ok: true,
            repository: Some(context.response),
            commits: Vec::new(),
            has_more: false,
            next_skip: None,
            error: None,
        });
    }

    let mut commits = Vec::new();
    for oid in walk.skip(skip).take(limit + 1) {
        let oid = oid.map_err(|error| FileError::Internal(format!("Unable to walk Git history: {error}")))?;
        commits.push(commit_summary(repository, oid)?);
    }
    let has_more = commits.len() > limit;
    commits.truncate(limit);
    Ok(ProjectGitGraphResponse {
        ok: true,
        repository: Some(context.response),
        commits,
        has_more,
        next_skip: has_more.then_some(skip + limit),
        error: None,
    })
}

pub fn frontier(workspace: PathBuf) -> Result<ProjectGitFrontierResponse, FileError> {
    let context = open_repository(workspace)?;
    let active_branch = context.response.branch.clone();
    let mut branches = Vec::new();
    for branch in context
        .repository
        .branches(Some(BranchType::Local))
        .map_err(|error| FileError::Internal(format!("Unable to list Git branches: {error}")))?
    {
        let Ok((branch, _)) = branch else { continue };
        let name = branch
            .name()
            .ok()
            .flatten()
            .map(str::to_owned)
            .unwrap_or_else(|| "(unnamed)".to_owned());
        let Ok(commit) = branch.get().peel_to_commit() else {
            continue;
        };
        let head = commit.id().to_string();
        let active = active_branch.as_deref() == Some(name.as_str());
        branches.push(ProjectGitResearchBranchResponse {
            name: name.clone(),
            r#ref: format!("refs/heads/{name}"),
            short_head: short_oid(commit.id()),
            head,
            active,
            status: if active { "active" } else { "candidate" }.to_owned(),
            updated_at: timestamp(commit.time().seconds()),
        });
    }
    branches.sort_by(|left, right| {
        right
            .active
            .cmp(&left.active)
            .then_with(|| right.updated_at.cmp(&left.updated_at))
            .then_with(|| left.name.cmp(&right.name))
    });
    Ok(ProjectGitFrontierResponse {
        ok: true,
        repository: Some(context.response),
        active_branch,
        branches,
        error: None,
    })
}

pub fn commit_detail(
    request: ProjectGitCommitRequest,
    workspace: PathBuf,
) -> Result<ProjectGitCommitResponse, FileError> {
    let context = open_repository(workspace)?;
    let oid = resolve_revision(&context.repository, &request.sha)?;
    let commit = context
        .repository
        .find_commit(oid)
        .map_err(|error| FileError::BadRequest(format!("Git commit not found: {error}")))?;
    let diff = commit_diff(&context.repository, &commit, None, None, 3)?;
    let files = changed_files(&diff, context.response.workspace_prefix.as_deref())?;
    let mut summary = commit_summary(&context.repository, oid)?;
    apply_diff_stats(&mut summary, &diff)?;
    Ok(ProjectGitCommitResponse {
        ok: true,
        repository: Some(context.response),
        commit: Some(ProjectGitCommitDetailResponse {
            summary,
            body: commit.message().unwrap_or_default().to_owned(),
            files,
        }),
        error: None,
    })
}

pub fn committed_diff(request: ProjectGitDiffRequest, workspace: PathBuf) -> Result<ProjectGitDiffResponse, FileError> {
    let context = open_repository(workspace)?;
    let oid = resolve_revision(&context.repository, &request.sha)?;
    let commit = context
        .repository
        .find_commit(oid)
        .map_err(|error| FileError::BadRequest(format!("Git commit not found: {error}")))?;
    let parent_oid = request
        .parent
        .as_deref()
        .map(|value| resolve_revision(&context.repository, value))
        .transpose()?;
    if let Some(parent_oid) = parent_oid
        && !commit.parent_ids().any(|candidate| candidate == parent_oid)
    {
        return Err(FileError::BadRequest(
            "The selected comparison base is not a parent of this commit.".to_owned(),
        ));
    }
    let path = normalize_relative_path(request.path.as_deref())?;
    let diff = commit_diff(
        &context.repository,
        &commit,
        parent_oid,
        scoped_path(context.response.workspace_prefix.as_deref(), path.as_deref()).as_deref(),
        request.context_lines.unwrap_or(3).min(20),
    )?;
    let (content, truncated, binary) = render_diff(&diff)?;
    Ok(ProjectGitDiffResponse {
        ok: true,
        repository: Some(context.response),
        sha: Some(oid.to_string()),
        base: parent_oid
            .or_else(|| commit.parent_id(0).ok())
            .map(|value| value.to_string()),
        path,
        content,
        truncated,
        binary,
        error: None,
    })
}

pub fn tree(request: ProjectGitTreeRequest, workspace: PathBuf) -> Result<ProjectGitTreeResponse, FileError> {
    let context = open_repository(workspace)?;
    let oid = resolve_revision(&context.repository, &request.sha)?;
    let commit = context
        .repository
        .find_commit(oid)
        .map_err(|error| FileError::BadRequest(format!("Git commit not found: {error}")))?;
    let path = normalize_relative_path(request.path.as_deref())?;
    let scoped = scoped_path(context.response.workspace_prefix.as_deref(), path.as_deref());
    let root = commit
        .tree()
        .map_err(|error| FileError::Internal(format!("Unable to read Git tree: {error}")))?;
    let selected = if let Some(scoped) = scoped.as_deref() {
        let entry = root
            .get_path(Path::new(scoped))
            .map_err(|_| FileError::NotFound("Git directory not found.".to_owned()))?;
        context
            .repository
            .find_tree(entry.id())
            .map_err(|error| FileError::Internal(format!("Unable to read Git directory: {error}")))?
    } else {
        root
    };
    let entries = selected
        .iter()
        .filter_map(|entry| {
            let name = entry.name()?.to_owned();
            let kind = match entry.kind() {
                Some(ObjectType::Tree) => "directory",
                Some(ObjectType::Commit) => "submodule",
                Some(ObjectType::Blob) => "file",
                _ => return None,
            };
            Some(ProjectGitTreeEntryResponse {
                path: [path.as_deref(), Some(name.as_str())]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
                    .join("/"),
                name,
                kind: kind.to_owned(),
                mode: format!("{:06o}", entry.filemode()),
                object_id: entry.id().to_string(),
                changed_in_revision: None,
            })
        })
        .collect();
    Ok(ProjectGitTreeResponse {
        ok: true,
        repository: Some(context.response),
        sha: Some(oid.to_string()),
        path,
        entries,
        error: None,
    })
}

pub fn blob(request: ProjectGitBlobRequest, workspace: PathBuf) -> Result<ProjectGitBlobResponse, FileError> {
    let context = open_repository(workspace)?;
    let oid = resolve_revision(&context.repository, &request.sha)?;
    let path = normalize_relative_path(Some(&request.path))?
        .ok_or_else(|| FileError::BadRequest("A project-relative file path is required.".to_owned()))?;
    let scoped = scoped_path(context.response.workspace_prefix.as_deref(), Some(&path)).unwrap_or_else(|| path.clone());
    let commit = context
        .repository
        .find_commit(oid)
        .map_err(|error| FileError::BadRequest(format!("Git commit not found: {error}")))?;
    let tree = commit
        .tree()
        .map_err(|error| FileError::Internal(format!("Unable to read Git tree: {error}")))?;
    let entry = tree
        .get_path(Path::new(&scoped))
        .map_err(|_| FileError::NotFound("Git file not found.".to_owned()))?;
    let blob = context
        .repository
        .find_blob(entry.id())
        .map_err(|_| FileError::NotFound("Git file not found.".to_owned()))?;
    let bytes = blob.content();
    let size = bytes.len();
    let binary = bytes.contains(&0);
    let (content, content_base64, truncated) = if size > MAX_BINARY_BYTES {
        (None, None, true)
    } else if binary {
        (
            None,
            Some(base64::engine::general_purpose::STANDARD.encode(bytes)),
            false,
        )
    } else {
        let boundary = size.min(MAX_TEXT_BYTES);
        (
            Some(String::from_utf8_lossy(&bytes[..boundary]).into_owned()),
            None,
            size > MAX_TEXT_BYTES,
        )
    };
    Ok(ProjectGitBlobResponse {
        ok: true,
        repository: Some(context.response),
        sha: Some(oid.to_string()),
        path: Some(path),
        size: Some(size),
        binary,
        truncated,
        content,
        content_base64,
        error: None,
    })
}

pub fn working_tree(workspace: PathBuf) -> Result<ProjectGitWorkingTreeResponse, FileError> {
    let mut context = open_repository(workspace)?;
    let mut options = StatusOptions::new();
    options
        .include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false);
    let statuses = context
        .repository
        .statuses(Some(&mut options))
        .map_err(|error| FileError::Internal(format!("Unable to read working tree status: {error}")))?;
    let (staged_count, modified_count, untracked_count, dirty_file_count) = summarize_statuses(&statuses);
    context.response.dirty = staged_count + modified_count + untracked_count > 0;
    context.response.staged_count = staged_count;
    context.response.modified_count = modified_count;
    context.response.untracked_count = untracked_count;
    context.response.dirty_file_count = dirty_file_count;
    let prefix = context.response.workspace_prefix.as_deref();
    let files = statuses
        .iter()
        .filter_map(|entry| {
            let path = workspace_path(prefix, entry.path()?)?;
            Some(ProjectGitChangedFileResponse {
                path,
                old_path: None,
                status: status_name(entry.status()).to_owned(),
                additions: None,
                deletions: None,
                binary: false,
            })
        })
        .collect();
    Ok(ProjectGitWorkingTreeResponse {
        ok: true,
        repository: Some(context.response),
        files,
        error: None,
    })
}

pub fn working_diff(
    request: ProjectGitWorkingDiffRequest,
    workspace: PathBuf,
) -> Result<ProjectGitDiffResponse, FileError> {
    let context = open_repository(workspace)?;
    let path = normalize_relative_path(Some(&request.path))?
        .ok_or_else(|| FileError::BadRequest("A project-relative file path is required.".to_owned()))?;
    let scoped = scoped_path(context.response.workspace_prefix.as_deref(), Some(&path)).unwrap_or_else(|| path.clone());
    let mut options = DiffOptions::new();
    options
        .context_lines(request.context_lines.unwrap_or(3).min(20))
        .include_untracked(true)
        .recurse_untracked_dirs(true)
        .show_untracked_content(true)
        .pathspec(&scoped);
    let diff = context
        .repository
        .diff_index_to_workdir(None, Some(&mut options))
        .map_err(|error| FileError::Internal(format!("Unable to read working file diff: {error}")))?;
    let (content, truncated, binary) = render_diff(&diff)?;
    Ok(ProjectGitDiffResponse {
        ok: true,
        repository: Some(context.response),
        sha: None,
        base: None,
        path: Some(path),
        content,
        truncated,
        binary,
        error: None,
    })
}

fn open_repository(workspace: PathBuf) -> Result<RepositoryContext, FileError> {
    open_repository_with_status(workspace, false)
}

fn open_repository_with_status(workspace: PathBuf, include_status: bool) -> Result<RepositoryContext, FileError> {
    if !workspace.is_dir() {
        return Err(FileError::BadRequest(
            "The project workspace must be a directory.".to_owned(),
        ));
    }
    let selected = fs::canonicalize(&workspace)
        .map_err(|error| FileError::BadRequest(format!("Unable to resolve project workspace: {error}")))?;
    let repository = Repository::discover(&selected)
        .map_err(|_| FileError::BadRequest("The selected workspace is not a Git repository.".to_owned()))?;
    let response = repository_response(&repository, &selected, include_status)?;
    Ok(RepositoryContext { repository, response })
}

fn repository_response(
    repository: &Repository,
    selected_workspace: &Path,
    include_status: bool,
) -> Result<ProjectGitRepositoryResponse, FileError> {
    let repo_root = repository_worktree_root(repository)
        .ok_or_else(|| FileError::Internal("Unable to resolve the Git repository root.".to_owned()))?;
    let workspace_root = fs::canonicalize(selected_workspace)
        .map_err(|error| FileError::BadRequest(format!("Unable to resolve project workspace: {error}")))?;
    let workspace_prefix = workspace_root
        .strip_prefix(&repo_root)
        .ok()
        .and_then(|path| (!path.as_os_str().is_empty()).then(|| path.to_string_lossy().replace('\\', "/")));
    let head_reference = repository.head().ok();
    let branch = head_reference
        .as_ref()
        .and_then(|reference| reference.shorthand())
        .map(str::to_owned);
    let head = head_reference
        .as_ref()
        .and_then(|reference| reference.target())
        .map(|oid| oid.to_string());
    let (staged_count, modified_count, untracked_count, dirty_file_count) = if include_status {
        status_counts(repository)?
    } else {
        (0, 0, 0, 0)
    };
    Ok(ProjectGitRepositoryResponse {
        workspace_root: response_path(&workspace_root),
        repo_root: response_path(&repo_root),
        workspace_prefix,
        branch,
        head,
        detached: repository.head_detached().unwrap_or(false),
        bare: repository.is_bare(),
        dirty: staged_count + modified_count + untracked_count > 0,
        staged_count,
        modified_count,
        untracked_count,
        dirty_file_count,
    })
}

fn resolve_revision(repository: &Repository, value: &str) -> Result<Oid, FileError> {
    let revision = value.trim();
    if revision.is_empty() || revision.starts_with('-') || revision.contains(['\0', '\n', '\r']) {
        return Err(FileError::BadRequest("Invalid Git revision.".to_owned()));
    }
    repository
        .revparse_single(revision)
        .and_then(|object| object.peel_to_commit())
        .map(|commit| commit.id())
        .map_err(|_| FileError::NotFound("Git revision not found.".to_owned()))
}

fn normalize_relative_path(value: Option<&str>) -> Result<Option<String>, FileError> {
    let Some(value) = value else { return Ok(None) };
    let normalized = value.trim().replace('\\', "/").trim_start_matches("./").to_owned();
    if normalized.is_empty() {
        return Ok(None);
    }
    if normalized.starts_with('/') || normalized.contains('\0') || normalized.split('/').any(|segment| segment == "..")
    {
        return Err(FileError::BadRequest(
            "Git path escapes the project workspace.".to_owned(),
        ));
    }
    Ok(Some(normalized))
}

fn scoped_path(prefix: Option<&str>, path: Option<&str>) -> Option<String> {
    let parts: Vec<_> = [prefix, path]
        .into_iter()
        .flatten()
        .filter(|value| !value.is_empty())
        .collect();
    (!parts.is_empty()).then(|| parts.join("/"))
}

fn workspace_path(prefix: Option<&str>, repo_path: &str) -> Option<String> {
    match prefix {
        None => Some(repo_path.to_owned()),
        Some(prefix) if repo_path == prefix => Some(String::new()),
        Some(prefix) => repo_path.strip_prefix(&format!("{prefix}/")).map(str::to_owned),
    }
}

fn timestamp(seconds: i64) -> Option<String> {
    DateTime::<Utc>::from_timestamp(seconds, 0).map(|value| value.to_rfc3339())
}

fn short_oid(oid: Oid) -> String {
    oid.to_string().chars().take(7).collect()
}

fn commit_summary(repository: &Repository, oid: Oid) -> Result<ProjectGitCommitSummaryResponse, FileError> {
    let commit = repository
        .find_commit(oid)
        .map_err(|error| FileError::Internal(format!("Unable to read Git commit: {error}")))?;
    let message = commit.message().unwrap_or_default();
    let subject = commit.summary().unwrap_or("(untitled commit)").to_owned();
    let body_preview = message
        .strip_prefix(&subject)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.chars().take(240).collect());
    let author = commit.author();
    let refs = references_for_oid(repository, oid);
    let diff = commit_diff(repository, &commit, None, None, 0)?;
    let stats = diff
        .stats()
        .map_err(|error| FileError::Internal(format!("Unable to read Git statistics: {error}")))?;
    Ok(ProjectGitCommitSummaryResponse {
        sha: oid.to_string(),
        short_sha: short_oid(oid),
        parents: commit.parent_ids().map(|parent| parent.to_string()).collect(),
        subject,
        body_preview,
        author_name: author.name().map(str::to_owned),
        author_email: author.email().map(str::to_owned),
        authored_at: timestamp(commit.time().seconds()),
        refs,
        changed_file_count: stats.files_changed(),
        additions: stats.insertions(),
        deletions: stats.deletions(),
    })
}

fn references_for_oid(repository: &Repository, oid: Oid) -> Vec<String> {
    let mut refs = repository
        .references()
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|reference| {
            let matches = reference.target().map(|target| target == oid).unwrap_or_else(|| {
                reference
                    .peel_to_commit()
                    .map(|commit| commit.id() == oid)
                    .unwrap_or(false)
            });
            matches.then(|| reference.shorthand().map(str::to_owned)).flatten()
        })
        .collect::<Vec<_>>();
    refs.sort();
    refs.dedup();
    refs
}

fn commit_diff<'repo>(
    repository: &'repo Repository,
    commit: &git2::Commit<'repo>,
    requested_parent: Option<Oid>,
    path: Option<&str>,
    context_lines: u32,
) -> Result<Diff<'repo>, FileError> {
    let current = commit
        .tree()
        .map_err(|error| FileError::Internal(format!("Unable to read Git tree: {error}")))?;
    let parent_tree = requested_parent
        .map(|oid| repository.find_commit(oid).and_then(|parent| parent.tree()))
        .transpose()
        .map_err(|error| FileError::BadRequest(format!("Unable to read parent commit: {error}")))?
        .or_else(|| commit.parent(0).ok().and_then(|parent| parent.tree().ok()));
    let mut options = DiffOptions::new();
    options.context_lines(context_lines);
    if let Some(path) = path {
        options.pathspec(path);
    }
    repository
        .diff_tree_to_tree(parent_tree.as_ref(), Some(&current), Some(&mut options))
        .map_err(|error| FileError::Internal(format!("Unable to read Git diff: {error}")))
}

fn render_diff(diff: &Diff<'_>) -> Result<(String, bool, bool), FileError> {
    let mut bytes = Vec::new();
    let mut truncated = false;
    diff.print(DiffFormat::Patch, |_delta, _hunk, line| {
        if bytes.len() >= MAX_TEXT_BYTES {
            truncated = true;
            return true;
        }
        let origin = line.origin();
        if origin == '+' || origin == '-' || origin == ' ' {
            bytes.push(origin as u8);
        }
        let remaining = MAX_TEXT_BYTES.saturating_sub(bytes.len());
        let content = line.content();
        bytes.extend_from_slice(&content[..content.len().min(remaining)]);
        if content.len() > remaining {
            truncated = true;
        }
        true
    })
    .map_err(|error| FileError::Internal(format!("Unable to render Git diff: {error}")))?;
    let binary = diff.deltas().any(|delta| delta.flags().contains(DiffFlags::BINARY));
    Ok((String::from_utf8_lossy(&bytes).into_owned(), truncated, binary))
}

fn changed_files(diff: &Diff<'_>, prefix: Option<&str>) -> Result<Vec<ProjectGitChangedFileResponse>, FileError> {
    let mut line_stats: HashMap<String, (usize, usize)> = HashMap::new();
    diff.print(DiffFormat::Patch, |delta, _hunk, line| {
        let Some(path) = delta.new_file().path().or_else(|| delta.old_file().path()) else {
            return true;
        };
        let Some(path) = workspace_path(prefix, &path.to_string_lossy()) else {
            return true;
        };
        let entry = line_stats.entry(path).or_default();
        match line.origin() {
            '+' => entry.0 += 1,
            '-' => entry.1 += 1,
            _ => {}
        }
        true
    })
    .map_err(|error| FileError::Internal(format!("Unable to inspect Git changes: {error}")))?;
    Ok(diff
        .deltas()
        .filter_map(|delta| {
            let repo_path = delta
                .new_file()
                .path()
                .or_else(|| delta.old_file().path())?
                .to_string_lossy();
            let path = workspace_path(prefix, &repo_path)?;
            let old_path = delta
                .old_file()
                .path()
                .map(|value| value.to_string_lossy())
                .and_then(|value| workspace_path(prefix, &value))
                .filter(|value| value != &path);
            let (additions, deletions) = line_stats.get(&path).copied().unwrap_or_default();
            Some(ProjectGitChangedFileResponse {
                path,
                old_path,
                status: delta_name(delta.status()).to_owned(),
                additions: Some(additions),
                deletions: Some(deletions),
                binary: delta.flags().contains(DiffFlags::BINARY),
            })
        })
        .collect())
}

fn apply_diff_stats(summary: &mut ProjectGitCommitSummaryResponse, diff: &Diff<'_>) -> Result<(), FileError> {
    let stats = diff
        .stats()
        .map_err(|error| FileError::Internal(format!("Unable to read Git statistics: {error}")))?;
    summary.changed_file_count = stats.files_changed();
    summary.additions = stats.insertions();
    summary.deletions = stats.deletions();
    Ok(())
}

fn delta_name(delta: Delta) -> &'static str {
    match delta {
        Delta::Added => "added",
        Delta::Deleted => "deleted",
        Delta::Renamed => "renamed",
        Delta::Copied => "copied",
        Delta::Typechange => "type-changed",
        Delta::Untracked => "untracked",
        _ => "modified",
    }
}

fn status_name(status: Status) -> &'static str {
    if status.contains(Status::WT_NEW) {
        "untracked"
    } else if status.intersects(Status::INDEX_NEW) {
        "added"
    } else if status.intersects(Status::WT_DELETED | Status::INDEX_DELETED) {
        "deleted"
    } else if status.intersects(Status::WT_RENAMED | Status::INDEX_RENAMED) {
        "renamed"
    } else if status.intersects(Status::WT_TYPECHANGE | Status::INDEX_TYPECHANGE) {
        "type-changed"
    } else {
        "modified"
    }
}

fn response_path(path: &Path) -> String {
    strip_verbatim_prefix(&path.to_string_lossy())
}

fn strip_verbatim_prefix(path: &str) -> String {
    if let Some(rest) = path.strip_prefix(r"\\?\UNC\") {
        format!(r"\\{rest}")
    } else if let Some(rest) = path.strip_prefix(r"\\?\") {
        rest.to_owned()
    } else {
        path.to_owned()
    }
}

fn status_counts(repository: &Repository) -> Result<(usize, usize, usize, usize), FileError> {
    let mut options = StatusOptions::new();
    options
        .include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false);
    let statuses = repository
        .statuses(Some(&mut options))
        .map_err(|error| FileError::Internal(format!("Unable to read Git status: {error}")))?;
    Ok(summarize_statuses(&statuses))
}

fn summarize_statuses(statuses: &git2::Statuses<'_>) -> (usize, usize, usize, usize) {
    let mut staged_count = 0;
    let mut modified_count = 0;
    let mut untracked_count = 0;
    let mut dirty_files = HashSet::new();
    for entry in statuses.iter() {
        let status = entry.status();
        if status.intersects(
            Status::INDEX_NEW
                | Status::INDEX_MODIFIED
                | Status::INDEX_DELETED
                | Status::INDEX_RENAMED
                | Status::INDEX_TYPECHANGE,
        ) {
            staged_count += 1;
        }
        if status.intersects(Status::WT_MODIFIED | Status::WT_DELETED | Status::WT_RENAMED | Status::WT_TYPECHANGE) {
            modified_count += 1;
        }
        if status.intersects(Status::WT_NEW) {
            untracked_count += 1;
        }
        if let Some(path) = entry.path() {
            dirty_files.insert(path.to_owned());
        }
    }
    (staged_count, modified_count, untracked_count, dirty_files.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepare_request(base_dir: &Path, conversation_id: &str) -> ProjectGitPrepareWorkspaceRequest {
        ProjectGitPrepareWorkspaceRequest {
            base_dir: base_dir.to_string_lossy().into_owned(),
            goal: "Audit a reproducible scientific claim".to_owned(),
            conversation_id: conversation_id.to_owned(),
            strategy: "managed".to_owned(),
            source_workspace: None,
            branch_kind: None,
        }
    }

    fn initialize_project_checkpoint(workspace: &Path, conversation_id: &str) -> Oid {
        fs::create_dir_all(workspace).unwrap();
        fs::write(workspace.join("README.md"), "# Existing project\n").unwrap();
        let mut options = RepositoryInitOptions::new();
        options.initial_head("main");
        let repository = Repository::init_opts(workspace, &options).unwrap();
        let mut index = repository.index().unwrap();
        index.add_path(Path::new("README.md")).unwrap();
        index.write().unwrap();
        let tree_id = index.write_tree().unwrap();
        let tree = repository.find_tree(tree_id).unwrap();
        let signature = Signature::now("DeepScientist", "checkpoint@deepscientist.local").unwrap();
        repository
            .commit(
                Some("HEAD"),
                &signature,
                &signature,
                &format!(
                    "[project] initialize selected project\n\nDeepScientist-Checkpoint: true\nDeepScientist-Stage: project\nDeepScientist-Conversation: {conversation_id}"
                ),
                &tree,
                &[],
            )
            .unwrap()
    }

    #[test]
    fn creates_and_idempotently_reuses_a_managed_workspace() {
        let root = tempfile::tempdir().unwrap();
        let first =
            prepare_managed_workspace(prepare_request(root.path(), "project_test"), root.path().to_path_buf()).unwrap();
        let workspace = PathBuf::from(first.workspace.as_deref().unwrap());

        assert_eq!(first.created, Some(true));
        assert_eq!(first.project_id.as_deref(), Some("project_test"));
        assert!(workspace.starts_with(fs::canonicalize(root.path()).unwrap().join("managed-projects")));
        assert!(workspace.join(PROJECT_MANIFEST_PATH).is_file());
        assert!(workspace.join("README.md").is_file());
        let repository = Repository::open(&workspace).unwrap();
        assert_eq!(repository.head().unwrap().shorthand(), Some("main"));
        assert!(
            repository
                .head()
                .unwrap()
                .peel_to_commit()
                .unwrap()
                .message()
                .unwrap()
                .contains("DeepScientist-Conversation: project_test")
        );

        let reused =
            prepare_managed_workspace(prepare_request(root.path(), "project_test"), root.path().to_path_buf()).unwrap();
        assert_eq!(reused.created, Some(false));
        assert_eq!(reused.workspace, first.workspace);
    }

    #[test]
    fn rejects_a_managed_workspace_base_outside_the_server_work_dir() {
        let trusted = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let error = prepare_managed_workspace(
            prepare_request(outside.path(), "project_outside"),
            trusted.path().to_path_buf(),
        )
        .unwrap_err();
        assert!(matches!(error, FileError::Forbidden(_)));
    }

    #[test]
    fn binds_an_existing_current_worktree_checkpoint_without_a_managed_manifest() {
        let root = tempfile::tempdir().unwrap();
        let workspace = root.path().join("selected-project");
        let planned = initialize_project_checkpoint(&workspace, "project_planned");

        let bound = bind_project_conversation(
            ProjectGitBindConversationRequest {
                workspace: workspace.to_string_lossy().into_owned(),
                conversation_id: "conversation_real".to_owned(),
            },
            workspace.clone(),
        )
        .unwrap();
        let commit = Oid::from_str(bound.commit.as_deref().unwrap()).unwrap();
        assert_ne!(commit, planned);
        let repository = Repository::open(&workspace).unwrap();
        let message = repository.find_commit(commit).unwrap().message().unwrap().to_owned();
        assert!(message.contains("DeepScientist-Conversation: conversation_real"));
        assert!(!message.contains("DeepScientist-Conversation: project_planned"));

        let rebound = bind_project_conversation(
            ProjectGitBindConversationRequest {
                workspace: workspace.to_string_lossy().into_owned(),
                conversation_id: "conversation_real".to_owned(),
            },
            workspace,
        )
        .unwrap();
        assert_eq!(rebound.commit, bound.commit);
    }

    #[test]
    fn resolves_the_only_direct_child_repository() {
        let parent = tempfile::tempdir().unwrap();
        let child = parent.path().join("actual-project");
        fs::create_dir(&child).unwrap();
        Repository::init(&child).unwrap();

        let response = preflight_workspace(parent.path().to_path_buf()).unwrap();
        assert!(response.ok);
        assert_eq!(response.kind.as_deref(), Some("git"));
        assert_eq!(
            response.repository.unwrap().repo_root,
            fs::canonicalize(child).unwrap().to_string_lossy()
        );
    }

    #[test]
    fn fails_closed_when_multiple_child_repositories_exist() {
        let parent = tempfile::tempdir().unwrap();
        for name in ["project-a", "project-b"] {
            let child = parent.path().join(name);
            fs::create_dir(&child).unwrap();
            Repository::init(child).unwrap();
        }

        let response = preflight_workspace(parent.path().to_path_buf()).unwrap();
        assert!(!response.ok);
        assert_eq!(response.repository_candidates.unwrap().len(), 2);
    }

    #[test]
    fn preserves_an_explicit_repository_workspace() {
        let workspace = tempfile::tempdir().unwrap();
        Repository::init(workspace.path()).unwrap();

        let response = preflight_workspace(workspace.path().to_path_buf()).unwrap();
        assert!(response.ok);
        assert_eq!(response.kind.as_deref(), Some("git"));
        assert_eq!(
            response.repository.unwrap().workspace_root,
            fs::canonicalize(workspace.path()).unwrap().to_string_lossy()
        );
    }

    #[test]
    fn lightweight_preflight_skips_worktree_status() {
        let workspace = tempfile::tempdir().unwrap();
        Repository::init(workspace.path()).unwrap();
        fs::write(workspace.path().join("untracked.txt"), "draft\n").unwrap();

        let response = preflight_workspace_with_status(workspace.path().to_path_buf(), false).unwrap();
        let repository = response.repository.unwrap();
        assert!(!repository.dirty);
        assert_eq!(repository.untracked_count, 0);
        assert_eq!(repository.dirty_file_count, 0);
    }

    #[test]
    fn reports_a_plain_directory_as_non_git() {
        let workspace = tempfile::tempdir().unwrap();
        let response = preflight_workspace(workspace.path().to_path_buf()).unwrap();
        assert!(response.ok);
        assert_eq!(response.kind.as_deref(), Some("non-git"));
    }

    #[test]
    fn strips_windows_verbatim_prefixes_from_response_paths() {
        assert_eq!(
            strip_verbatim_prefix(r"\\?\C:\research\project"),
            r"C:\research\project"
        );
        assert_eq!(
            strip_verbatim_prefix(r"\\?\UNC\server\share\project"),
            r"\\server\share\project"
        );
    }
}
