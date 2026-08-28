#![allow(clippy::disallowed_types)]

use axum::Router;
use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Json, Multipart, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use axum::routing::{get, post};
use base64::Engine;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use tower_http::limit::RequestBodyLimitLayer;

use aionui_api_types::{
    ApiResponse, BrowseDirectoryQuery, BrowseDirectoryResponse, CancelZipRequest, CopyFilesRequest, CopyFilesResponse,
    CreateTempFileRequest, DirOrFileResponse, FetchRemoteImageRequest, FileChangeInfoResponse, FileMetadataResponse,
    FileWatchRequest, GetFileMetadataRequest, GetFilesByDirRequest, GetImageBase64Request, ListWorkspaceFilesRequest,
    PreviewFileQuery, ProjectGitBindConversationRequest, ProjectGitBindConversationResponse, ProjectGitBlobRequest,
    ProjectGitBlobResponse, ProjectGitCommitRequest, ProjectGitCommitResponse, ProjectGitDiffRequest,
    ProjectGitDiffResponse, ProjectGitDiscoverRequest, ProjectGitDiscoverResponse, ProjectGitFrontierResponse,
    ProjectGitGraphRequest, ProjectGitGraphResponse, ProjectGitPreflightRequest, ProjectGitPreflightResponse,
    ProjectGitPrepareWorkspaceRequest, ProjectGitPrepareWorkspaceResponse, ProjectGitTreeRequest,
    ProjectGitTreeResponse, ProjectGitWorkingDiffRequest, ProjectGitWorkingDiffResponse, ProjectGitWorkingTreeResponse,
    ReadFileBufferRequest, ReadFileRequest, RemoveEntryRequest, RenameRequest, RenameResponse, SnapshotBaselineRequest,
    SnapshotCompareResponse, SnapshotDiscardRequest, SnapshotInfoResponse, SnapshotStageRequest,
    SnapshotWorkspaceRequest, WorkspaceFlatFileResponse, WorkspaceOfficeWatchRequest, WriteFileRequest, ZipRequest,
};
use aionui_common::ApiError;
use aionui_common::constants::UPLOAD_MAX_SIZE;

use crate::browse;
use crate::error::FileError;
use crate::traits::{FileServiceRef, FileWatchServiceRef, SnapshotServiceRef};
use crate::types::{
    CompareResult, CopyResult, DirOrFile, FileChangeInfo, FileMetadata, SnapshotInfo, SnapshotMode, WorkspaceFlatFile,
    ZipEntry,
};

impl From<FileError> for ApiError {
    fn from(error: FileError) -> Self {
        match error {
            FileError::BadRequest(message) => ApiError::BadRequest(message),
            FileError::Forbidden(message) => ApiError::Forbidden(message),
            FileError::PathOutsideSandbox {
                message,
                field,
                operation,
            } => ApiError::PathOutsideSandbox {
                message,
                field,
                operation,
            },
            FileError::NotFound(message) => ApiError::NotFound(message),
            FileError::Internal(message) => ApiError::Internal(message),
        }
    }
}

// ---------------------------------------------------------------------------
// Router state
// ---------------------------------------------------------------------------

type BrowseRootsResolver = dyn Fn() -> Vec<PathBuf> + Send + Sync;

/// Lazily resolves roots for the shallow `/api/fs/browse` endpoint.
#[derive(Clone)]
pub struct BrowseRoots {
    roots: Arc<OnceLock<Vec<PathBuf>>>,
    resolver: Arc<BrowseRootsResolver>,
}

impl BrowseRoots {
    pub fn new() -> Self {
        Self {
            roots: Arc::new(OnceLock::new()),
            resolver: Arc::new(browse::default_browse_roots),
        }
    }

    #[cfg(test)]
    fn with_resolver(resolver: impl Fn() -> Vec<PathBuf> + Send + Sync + 'static) -> Self {
        Self {
            roots: Arc::new(OnceLock::new()),
            resolver: Arc::new(resolver),
        }
    }

    fn get(&self) -> Vec<PathBuf> {
        self.roots.get_or_init(|| (self.resolver)()).clone()
    }
}

impl Default for BrowseRoots {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared state for all file-related route handlers.
#[derive(Clone)]
pub struct FileRouterState {
    pub file_service: FileServiceRef,
    pub watch_service: FileWatchServiceRef,
    pub snapshot_service: SnapshotServiceRef,
    pub allowed_roots: Vec<std::path::PathBuf>,
    /// Backend-owned root for managed project creation. Unlike `browse_roots`,
    /// this path is never supplied by the browser and must not be widened to a
    /// home directory or filesystem root.
    pub work_dir: std::path::PathBuf,
    /// Roots permitted by the shallow `/api/fs/browse` endpoint. This is
    /// typically wider than `allowed_roots` (it includes `cwd`, Windows
    /// drive letters, and `/` on Unix) because the WebUI host-file picker
    /// legitimately needs to reach outside any single workspace.
    pub browse_roots: BrowseRoots,
}

// ---------------------------------------------------------------------------
// Router builder
// ---------------------------------------------------------------------------

/// Build the file router with all `/api/fs/*` routes.
///
/// All routes require authentication (applied by the caller).
pub fn file_routes(state: FileRouterState) -> Router {
    // Upload route carries its own body-size limit (UPLOAD_MAX_SIZE, 30 MB).
    // We first disable the global `DefaultBodyLimit` that `aionui-app`
    // installs (otherwise the `Multipart` extractor would cap the body at
    // `BODY_LIMIT`), then apply `RequestBodyLimitLayer` as the sole hard
    // cap. The layers are added in outer->inner order via `.layer()`.
    let upload_router = Router::new()
        .route("/api/fs/upload", post(upload_file))
        .layer(DefaultBodyLimit::disable())
        .layer(RequestBodyLimitLayer::new(UPLOAD_MAX_SIZE))
        .with_state(state.clone());

    Router::new()
        // A. Core file operations
        .route("/api/fs/browse", get(browse_directory))
        .route("/api/fs/project-git/preflight", post(project_git_preflight))
        .route(
            "/api/fs/project-git/prepare-workspace",
            post(project_git_prepare_workspace),
        )
        .route(
            "/api/fs/project-git/bind-conversation",
            post(project_git_bind_conversation),
        )
        .route("/api/fs/project-git/discover", post(project_git_discover))
        .route("/api/fs/project-git/graph", post(project_git_graph))
        .route("/api/fs/project-git/frontier", post(project_git_frontier))
        .route("/api/fs/project-git/commit", post(project_git_commit))
        .route("/api/fs/project-git/diff", post(project_git_diff))
        .route("/api/fs/project-git/tree", post(project_git_tree))
        .route("/api/fs/project-git/blob", post(project_git_blob))
        .route("/api/fs/project-git/working-tree", post(project_git_working_tree))
        .route("/api/fs/project-git/working-diff", post(project_git_working_diff))
        .route("/api/fs/dir", post(get_files_by_dir))
        .route("/api/fs/list", post(list_workspace_files))
        .route("/api/fs/metadata", post(get_file_metadata))
        .route("/api/fs/read", post(read_file))
        .route("/api/fs/read-buffer", post(read_file_buffer))
        .route("/api/fs/preview", get(preview_file))
        .route("/api/fs/write", post(write_file))
        .route("/api/fs/copy", post(copy_files))
        .route("/api/fs/remove", post(remove_entry))
        .route("/api/fs/rename", post(rename_entry))
        .route("/api/fs/temp", post(create_temp_file))
        .route("/api/fs/image-base64", post(get_image_base64))
        .route("/api/fs/fetch-remote-image", post(fetch_remote_image))
        .route("/api/fs/zip", post(create_zip))
        .route("/api/fs/zip/cancel", post(cancel_zip))
        // D. File watch
        .route("/api/fs/watch/start", post(start_watch))
        .route("/api/fs/watch/stop", post(stop_watch))
        .route("/api/fs/watch/stop-all", post(stop_all_watches))
        .route("/api/fs/office-watch/start", post(start_office_watch))
        .route("/api/fs/office-watch/stop", post(stop_office_watch))
        // E. Workspace snapshot
        .route("/api/fs/snapshot/init", post(snapshot_init))
        .route("/api/fs/snapshot/info", post(snapshot_info))
        .route("/api/fs/snapshot/compare", post(snapshot_compare))
        .route("/api/fs/snapshot/baseline", post(snapshot_baseline))
        .route("/api/fs/snapshot/stage", post(snapshot_stage_file))
        .route("/api/fs/snapshot/stage-all", post(snapshot_stage_all))
        .route("/api/fs/snapshot/unstage", post(snapshot_unstage_file))
        .route("/api/fs/snapshot/unstage-all", post(snapshot_unstage_all))
        .route("/api/fs/snapshot/discard", post(snapshot_discard))
        .route("/api/fs/snapshot/reset", post(snapshot_reset))
        .route("/api/fs/snapshot/branches", post(snapshot_branches))
        .route("/api/fs/snapshot/dispose", post(snapshot_dispose))
        .with_state(state)
        .merge(upload_router)
}

// ---------------------------------------------------------------------------
// A. Core file operations — handlers
// ---------------------------------------------------------------------------

/// `GET /api/fs/browse` — shallow directory listing for the WebUI host-file
/// picker. Runs on the Tokio blocking pool because it does synchronous
/// filesystem I/O.
async fn browse_directory(
    State(state): State<FileRouterState>,
    Query(query): Query<BrowseDirectoryQuery>,
) -> Result<Json<ApiResponse<BrowseDirectoryResponse>>, ApiError> {
    let show_files = matches!(query.show_files.as_deref(), Some("true") | Some("1"));
    let raw_path = query.path.clone();
    let browse_roots = state.browse_roots.clone();

    let response = tokio::task::spawn_blocking(move || {
        let roots = browse_roots.get();
        browse::browse(raw_path.as_deref(), show_files, &roots)
    })
    .await
    .map_err(|e| ApiError::Internal(format!("browse task failed: {}", e)))??;

    Ok(Json(ApiResponse::ok(response)))
}

async fn project_git_preflight(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitPreflightRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitPreflightResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = req.workspace.trim();
    if workspace.is_empty() {
        return Err(ApiError::BadRequest("workspace is required".to_owned()));
    }
    let workspace = browse::resolve_browse_path(workspace, &state.browse_roots.get())?;
    let include_status = req.include_status.unwrap_or(true);
    let response = tokio::task::spawn_blocking(move || {
        crate::project_git::preflight_workspace_with_status(workspace, include_status)
    })
    .await
    .map_err(|error| ApiError::Internal(format!("Git preflight task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

async fn project_git_prepare_workspace(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitPrepareWorkspaceRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitPrepareWorkspaceResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    if req.strategy == "current-worktree" {
        let source_workspace = req
            .source_workspace
            .as_deref()
            .ok_or_else(|| ApiError::BadRequest("A source workspace is required.".to_owned()))?;
        let workspace = browse::resolve_browse_path(source_workspace, &state.browse_roots.get())?;
        let response =
            tokio::task::spawn_blocking(move || crate::project_git::prepare_current_workspace(req, workspace))
                .await
                .map_err(|error| ApiError::Internal(format!("Selected Git preparation task failed: {error}")))??;
        return Ok(Json(ApiResponse::ok(response)));
    }
    let work_dir = state.work_dir.clone();
    let response = tokio::task::spawn_blocking(move || crate::project_git::prepare_managed_workspace(req, work_dir))
        .await
        .map_err(|error| ApiError::Internal(format!("Managed Git preparation task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

fn resolve_project_bind_path(workspace: &str, allowed_roots: &[PathBuf]) -> Result<PathBuf, FileError> {
    let workspace = workspace.trim();
    if workspace.is_empty() {
        return Err(FileError::BadRequest("workspace is required".to_owned()));
    }
    let allowed_root_refs: Vec<&Path> = allowed_roots.iter().map(PathBuf::as_path).collect();
    let workspace = crate::path_safety::validate_path(workspace, &allowed_root_refs)?;
    let repository = git2::Repository::discover(&workspace)
        .map_err(|_| FileError::BadRequest("The selected workspace is not a Git repository.".to_owned()))?;
    let repository_root = repository
        .workdir()
        .ok_or_else(|| FileError::BadRequest("Bare Git repositories cannot be bound to conversations.".to_owned()))?;
    // A selected subdirectory may live below a repository whose root is above
    // it. Validate the actual mutation target as well as the browser-supplied
    // workspace before allowing HEAD to be amended.
    crate::path_safety::validate_path(&repository_root.to_string_lossy(), &allowed_root_refs)?;
    Ok(workspace)
}

async fn project_git_bind_conversation(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitBindConversationRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitBindConversationResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = resolve_project_bind_path(&req.workspace, &state.allowed_roots)?;
    let response = tokio::task::spawn_blocking(move || crate::project_git::bind_project_conversation(req, workspace))
        .await
        .map_err(|error| ApiError::Internal(format!("Project Git binding task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

fn resolve_project_git_workspace(state: &FileRouterState, workspace: &str) -> Result<PathBuf, ApiError> {
    let workspace = workspace.trim();
    if workspace.is_empty() {
        return Err(ApiError::BadRequest("workspace is required".to_owned()));
    }
    browse::resolve_browse_path(workspace, &state.browse_roots.get()).map_err(ApiError::from)
}

async fn project_git_discover(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitDiscoverRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitDiscoverResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = resolve_project_git_workspace(&state, &req.workspace)?;
    let response = tokio::task::spawn_blocking(move || crate::project_git::discover_workspace(workspace))
        .await
        .map_err(|error| ApiError::Internal(format!("Git discover task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

async fn project_git_graph(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitGraphRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitGraphResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = resolve_project_git_workspace(&state, &req.workspace)?;
    let response = tokio::task::spawn_blocking(move || crate::project_git::graph(req, workspace))
        .await
        .map_err(|error| ApiError::Internal(format!("Git graph task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

async fn project_git_frontier(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitDiscoverRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitFrontierResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = resolve_project_git_workspace(&state, &req.workspace)?;
    let response = tokio::task::spawn_blocking(move || crate::project_git::frontier(workspace))
        .await
        .map_err(|error| ApiError::Internal(format!("Git frontier task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

async fn project_git_commit(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitCommitRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitCommitResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = resolve_project_git_workspace(&state, &req.workspace)?;
    let response = tokio::task::spawn_blocking(move || crate::project_git::commit_detail(req, workspace))
        .await
        .map_err(|error| ApiError::Internal(format!("Git commit task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

async fn project_git_diff(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitDiffRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitDiffResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = resolve_project_git_workspace(&state, &req.workspace)?;
    let response = tokio::task::spawn_blocking(move || crate::project_git::committed_diff(req, workspace))
        .await
        .map_err(|error| ApiError::Internal(format!("Git diff task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

async fn project_git_tree(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitTreeRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitTreeResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = resolve_project_git_workspace(&state, &req.workspace)?;
    let response = tokio::task::spawn_blocking(move || crate::project_git::tree(req, workspace))
        .await
        .map_err(|error| ApiError::Internal(format!("Git tree task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

async fn project_git_blob(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitBlobRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitBlobResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = resolve_project_git_workspace(&state, &req.workspace)?;
    let response = tokio::task::spawn_blocking(move || crate::project_git::blob(req, workspace))
        .await
        .map_err(|error| ApiError::Internal(format!("Git blob task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

async fn project_git_working_tree(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitDiscoverRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitWorkingTreeResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = resolve_project_git_workspace(&state, &req.workspace)?;
    let response = tokio::task::spawn_blocking(move || crate::project_git::working_tree(workspace))
        .await
        .map_err(|error| ApiError::Internal(format!("Git working-tree task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

async fn project_git_working_diff(
    State(state): State<FileRouterState>,
    body: Result<Json<ProjectGitWorkingDiffRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<ProjectGitWorkingDiffResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = resolve_project_git_workspace(&state, &req.workspace)?;
    let response = tokio::task::spawn_blocking(move || crate::project_git::working_diff(req, workspace))
        .await
        .map_err(|error| ApiError::Internal(format!("Git working-diff task failed: {error}")))??;
    Ok(Json(ApiResponse::ok(response)))
}

async fn get_files_by_dir(
    State(state): State<FileRouterState>,
    body: Result<Json<GetFilesByDirRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<Vec<DirOrFileResponse>>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let items = state.file_service.get_files_by_dir(&req.dir, &req.root).await?;
    let response: Vec<DirOrFileResponse> = items.into_iter().map(to_dir_or_file_response).collect();
    Ok(Json(ApiResponse::ok(response)))
}

async fn list_workspace_files(
    State(state): State<FileRouterState>,
    body: Result<Json<ListWorkspaceFilesRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<Vec<WorkspaceFlatFileResponse>>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let root = req.root.trim();
    if root.is_empty() {
        return Err(ApiError::BadRequest("root is required".to_owned()));
    }
    let items = state
        .file_service
        .list_workspace_files_with_extra_root(root, Some(Path::new(root)))
        .await?;

    let response: Vec<WorkspaceFlatFileResponse> = items.into_iter().map(to_flat_file_response).collect();
    Ok(Json(ApiResponse::ok(response)))
}

async fn get_file_metadata(
    State(state): State<FileRouterState>,
    body: Result<Json<GetFileMetadataRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<FileMetadataResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let meta = state
        .file_service
        .get_file_metadata(&req.path, req.workspace.as_deref().map(Path::new))
        .await?;
    Ok(Json(ApiResponse::ok(to_metadata_response(meta))))
}

async fn read_file(
    State(state): State<FileRouterState>,
    body: Result<Json<ReadFileRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<Option<String>>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let content = state
        .file_service
        .read_file(&req.path, req.workspace.as_deref().map(Path::new))
        .await?;
    Ok(Json(ApiResponse::ok(content)))
}

async fn read_file_buffer(
    State(state): State<FileRouterState>,
    body: Result<Json<ReadFileBufferRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<Option<String>>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let data = state
        .file_service
        .read_file_buffer(&req.path, req.workspace.as_deref().map(Path::new))
        .await?;
    // Binary data is base64-encoded for JSON transport.
    let encoded = data.map(|bytes| base64::engine::general_purpose::STANDARD.encode(bytes));
    Ok(Json(ApiResponse::ok(encoded)))
}

fn decode_preview_path(encoded: &str, field: &str) -> Result<String, ApiError> {
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ApiError::BadRequest(format!("invalid {field} encoding")))?;
    String::from_utf8(bytes).map_err(|_| ApiError::BadRequest(format!("invalid {field} encoding")))
}

/// Parse one RFC 7233 byte range. Multiple ranges are deliberately rejected:
/// the preview client only needs a single contiguous span.
fn parse_preview_range(value: Option<&str>, size: u64) -> Result<Option<(u64, u64)>, ()> {
    let Some(value) = value else {
        return Ok(None);
    };
    if size == 0 {
        return Err(());
    }

    let range = value.strip_prefix("bytes=").ok_or(())?;
    if range.contains(',') {
        return Err(());
    }
    let (start, end) = range.split_once('-').ok_or(())?;

    if start.is_empty() {
        let suffix = end.parse::<u64>().map_err(|_| ())?;
        if suffix == 0 {
            return Err(());
        }
        let length = suffix.min(size);
        return Ok(Some((size - length, size - 1)));
    }

    let start = start.parse::<u64>().map_err(|_| ())?;
    if start >= size {
        return Err(());
    }
    let end = if end.is_empty() {
        size - 1
    } else {
        end.parse::<u64>().map_err(|_| ())?.min(size - 1)
    };
    if end < start {
        return Err(());
    }
    Ok(Some((start, end)))
}

fn range_not_satisfiable(size: u64) -> Result<Response, ApiError> {
    Response::builder()
        .status(StatusCode::RANGE_NOT_SATISFIABLE)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CONTENT_RANGE, format!("bytes */{size}"))
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::empty())
        .map_err(|error| ApiError::Internal(format!("cannot build range response: {error}")))
}

/// Serve a sandboxed local file to the WebUI. Browser PDF viewers request
/// bounded ranges, avoiding whole-file base64 transport and wasted reads when
/// navigation cancels an in-flight preview.
async fn preview_file(
    State(state): State<FileRouterState>,
    Query(query): Query<PreviewFileQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let path = decode_preview_path(&query.path, "path")?;
    let workspace = query
        .workspace
        .as_deref()
        .map(|value| decode_preview_path(value, "workspace"))
        .transpose()?;
    let extra_root = workspace.as_deref().map(Path::new);
    let metadata = state.file_service.get_file_metadata(&path, extra_root).await?;
    if metadata.is_directory {
        return Err(ApiError::BadRequest("preview path must be a file".to_owned()));
    }

    let requested_range = headers.get(header::RANGE).and_then(|value| value.to_str().ok());
    let range = match parse_preview_range(requested_range, metadata.size) {
        Ok(range) => range,
        Err(()) => return range_not_satisfiable(metadata.size),
    };
    let (start, end, status) = match range {
        Some((start, end)) => (start, end, StatusCode::PARTIAL_CONTENT),
        None if metadata.size > 0 => (0, metadata.size - 1, StatusCode::OK),
        None => (0, 0, StatusCode::OK),
    };
    let length = if metadata.size == 0 { 0 } else { end - start + 1 };
    let bytes = state
        .file_service
        .read_file_range(&path, extra_root, start, length)
        .await?;

    let mut response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, metadata.mime_type)
        .header(header::CONTENT_LENGTH, bytes.len().to_string())
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::CACHE_CONTROL, "no-store")
        .header("x-content-type-options", "nosniff");
    if status == StatusCode::PARTIAL_CONTENT {
        response = response.header(header::CONTENT_RANGE, format!("bytes {start}-{end}/{}", metadata.size));
    }
    response
        .body(Body::from(bytes))
        .map_err(|error| ApiError::Internal(format!("cannot build preview response: {error}")))
}

async fn write_file(
    State(state): State<FileRouterState>,
    body: Result<Json<WriteFileRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<bool>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = req.workspace.unwrap_or_else(|| {
        std::path::Path::new(&req.path)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default()
    });
    let ok = state
        .file_service
        .write_file(&req.path, req.data.as_bytes(), &workspace)
        .await?;
    Ok(Json(ApiResponse::ok(ok)))
}

async fn copy_files(
    State(state): State<FileRouterState>,
    body: Result<Json<CopyFilesRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<CopyFilesResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let result = state
        .file_service
        .copy_files_to_workspace(&req.file_paths, &req.workspace, req.source_root.as_deref())
        .await?;
    Ok(Json(ApiResponse::ok(to_copy_response(result))))
}

async fn remove_entry(
    State(state): State<FileRouterState>,
    body: Result<Json<RemoveEntryRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = req.workspace.unwrap_or_else(|| {
        std::path::Path::new(&req.path)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default()
    });
    state.file_service.remove_entry(&req.path, &workspace).await?;
    Ok(Json(ApiResponse::success()))
}

async fn rename_entry(
    State(state): State<FileRouterState>,
    body: Result<Json<RenameRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<RenameResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let workspace = req.workspace.unwrap_or_else(|| {
        std::path::Path::new(&req.path)
            .parent()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default()
    });
    let new_path = state
        .file_service
        .rename_entry_with_extra_root(&req.path, &req.new_name, Some(Path::new(&workspace)))
        .await?;
    Ok(Json(ApiResponse::ok(RenameResponse { new_path })))
}

async fn create_temp_file(
    State(state): State<FileRouterState>,
    body: Result<Json<CreateTempFileRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<String>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let path = state.file_service.create_temp_file(&req.file_name).await?;
    Ok(Json(ApiResponse::ok(path)))
}

/// Fields extracted from a `/api/fs/upload` multipart request.
struct UploadMultipartFields {
    file_data: Vec<u8>,
    file_name: Option<String>,
    dispo_file_name: Option<String>,
    conversation_id: Option<String>,
}

/// Strip any directory component from a file name and reject empty results.
/// The returned name is guaranteed not to contain path separators; deeper
/// traversal validation happens in [`IFileService::create_upload_file`].
fn sanitize_upload_filename(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let last = trimmed.rsplit(['/', '\\']).next().unwrap_or("");
    let last = last.trim();
    if last.is_empty() { None } else { Some(last.to_owned()) }
}

async fn extract_upload_multipart(mut multipart: Multipart) -> Result<UploadMultipartFields, ApiError> {
    let mut file_data: Option<Vec<u8>> = None;
    let mut file_name: Option<String> = None;
    let mut dispo_file_name: Option<String> = None;
    let mut conversation_id: Option<String> = None;

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ApiError::BadRequest(format!("multipart error: {e}")))?
    {
        let name = field.name().unwrap_or("").to_owned();
        match name.as_str() {
            "file" => {
                // Capture the Content-Disposition filename (if any) before
                // consuming the field body — `field.file_name()` is only
                // available on the field metadata, not on the Bytes below.
                dispo_file_name = field.file_name().and_then(sanitize_upload_filename);
                file_data = Some(
                    field
                        .bytes()
                        .await
                        .map_err(|e| ApiError::BadRequest(format!("failed to read file: {e}")))?
                        .to_vec(),
                );
            }
            "file_name" => {
                let text = field
                    .text()
                    .await
                    .map_err(|e| ApiError::BadRequest(format!("failed to read file_name: {e}")))?;
                if let Some(name) = sanitize_upload_filename(&text) {
                    file_name = Some(name);
                }
            }
            "conversation_id" => {
                let text = field
                    .text()
                    .await
                    .map_err(|e| ApiError::BadRequest(format!("failed to read conversation_id: {e}")))?;
                let trimmed = text.trim();
                if !trimmed.is_empty() {
                    conversation_id = Some(trimmed.to_owned());
                }
            }
            _ => {}
        }
    }

    let file_data = file_data.ok_or_else(|| ApiError::BadRequest("missing 'file' field".to_owned()))?;

    Ok(UploadMultipartFields {
        file_data,
        file_name,
        dispo_file_name,
        conversation_id,
    })
}

async fn upload_file(
    State(state): State<FileRouterState>,
    multipart: Multipart,
) -> Result<Json<ApiResponse<String>>, ApiError> {
    let fields = extract_upload_multipart(multipart).await?;

    let file_name = fields.file_name.or(fields.dispo_file_name).ok_or_else(|| {
        ApiError::BadRequest("missing file name: provide 'file_name' or a multipart filename".to_owned())
    })?;

    let path = state
        .file_service
        .create_upload_file(&file_name, &fields.file_data, fields.conversation_id.as_deref())
        .await?;
    Ok(Json(ApiResponse::ok(path)))
}

async fn get_image_base64(
    State(state): State<FileRouterState>,
    body: Result<Json<GetImageBase64Request>, JsonRejection>,
) -> Result<Json<ApiResponse<String>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let data_url = state
        .file_service
        .get_image_base64(&req.path, req.workspace.as_deref().map(Path::new))
        .await?;
    Ok(Json(ApiResponse::ok(data_url)))
}

async fn fetch_remote_image(
    State(state): State<FileRouterState>,
    body: Result<Json<FetchRemoteImageRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<String>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let data_url = state.file_service.fetch_remote_image(&req.url).await;
    Ok(Json(ApiResponse::ok(data_url)))
}

async fn create_zip(
    State(state): State<FileRouterState>,
    body: Result<Json<ZipRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<bool>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let entries: Vec<ZipEntry> = req.files.into_iter().map(to_zip_entry).collect();
    let ok = state
        .file_service
        .create_zip_with_extra_roots(
            &req.path,
            entries,
            req.request_id,
            req.workspace.as_deref().map(Path::new),
            req.source_root.as_deref().map(Path::new),
        )
        .await?;
    Ok(Json(ApiResponse::ok(ok)))
}

async fn cancel_zip(
    State(state): State<FileRouterState>,
    body: Result<Json<CancelZipRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<bool>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let ok = state.file_service.cancel_zip(&req.request_id).await;
    Ok(Json(ApiResponse::ok(ok)))
}

// ---------------------------------------------------------------------------
// D. File watch — handlers
// ---------------------------------------------------------------------------

async fn start_watch(
    State(state): State<FileRouterState>,
    body: Result<Json<FileWatchRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state.watch_service.start_watch(&req.file_path).await?;
    Ok(Json(ApiResponse::success()))
}

async fn stop_watch(
    State(state): State<FileRouterState>,
    body: Result<Json<FileWatchRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state.watch_service.stop_watch(&req.file_path).await?;
    Ok(Json(ApiResponse::success()))
}

async fn stop_all_watches(State(state): State<FileRouterState>) -> Result<Json<ApiResponse<()>>, ApiError> {
    state.watch_service.stop_all_watches().await?;
    Ok(Json(ApiResponse::success()))
}

async fn start_office_watch(
    State(state): State<FileRouterState>,
    body: Result<Json<WorkspaceOfficeWatchRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let allowed_roots: Vec<&Path> = state.allowed_roots.iter().map(std::path::PathBuf::as_path).collect();
    crate::path_safety::validate_path_with_extra_root(&req.workspace, &allowed_roots, Some(Path::new(&req.workspace)))?;
    state.watch_service.start_office_watch(&req.workspace).await?;
    Ok(Json(ApiResponse::success()))
}

async fn stop_office_watch(
    State(state): State<FileRouterState>,
    body: Result<Json<WorkspaceOfficeWatchRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state.watch_service.stop_office_watch(&req.workspace).await?;
    Ok(Json(ApiResponse::success()))
}

// ---------------------------------------------------------------------------
// E. Workspace snapshot — handlers
// ---------------------------------------------------------------------------

async fn snapshot_init(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotWorkspaceRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<SnapshotInfoResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let info = state.snapshot_service.init(&req.workspace).await?;
    Ok(Json(ApiResponse::ok(to_snapshot_info_response(info))))
}

async fn snapshot_info(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotWorkspaceRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<SnapshotInfoResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let info = state.snapshot_service.get_info(&req.workspace).await?;
    Ok(Json(ApiResponse::ok(to_snapshot_info_response(info))))
}

async fn snapshot_compare(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotWorkspaceRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<SnapshotCompareResponse>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let result = state.snapshot_service.compare(&req.workspace).await?;
    Ok(Json(ApiResponse::ok(to_compare_response(result))))
}

async fn snapshot_baseline(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotBaselineRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<Option<String>>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let content = state
        .snapshot_service
        .get_baseline_content(&req.workspace, &req.file_path)
        .await?;
    Ok(Json(ApiResponse::ok(content)))
}

async fn snapshot_stage_file(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotStageRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state
        .snapshot_service
        .stage_file(&req.workspace, &req.file_path)
        .await?;
    Ok(Json(ApiResponse::success()))
}

async fn snapshot_stage_all(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotWorkspaceRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state.snapshot_service.stage_all(&req.workspace).await?;
    Ok(Json(ApiResponse::success()))
}

async fn snapshot_unstage_file(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotStageRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state
        .snapshot_service
        .unstage_file(&req.workspace, &req.file_path)
        .await?;
    Ok(Json(ApiResponse::success()))
}

async fn snapshot_unstage_all(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotWorkspaceRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state.snapshot_service.unstage_all(&req.workspace).await?;
    Ok(Json(ApiResponse::success()))
}

async fn snapshot_discard(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotDiscardRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state
        .snapshot_service
        .discard_file(&req.workspace, &req.file_path, req.operation)
        .await?;
    Ok(Json(ApiResponse::success()))
}

async fn snapshot_reset(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotDiscardRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state
        .snapshot_service
        .reset_file(&req.workspace, &req.file_path, req.operation)
        .await?;
    Ok(Json(ApiResponse::success()))
}

async fn snapshot_branches(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotWorkspaceRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<Vec<String>>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    let branches = state.snapshot_service.get_branches(&req.workspace).await?;
    Ok(Json(ApiResponse::ok(branches)))
}

async fn snapshot_dispose(
    State(state): State<FileRouterState>,
    body: Result<Json<SnapshotWorkspaceRequest>, JsonRejection>,
) -> Result<Json<ApiResponse<()>>, ApiError> {
    let Json(req) = body.map_err(ApiError::from)?;
    state.snapshot_service.dispose(&req.workspace).await?;
    Ok(Json(ApiResponse::success()))
}

// ---------------------------------------------------------------------------
// Domain → DTO conversions
// ---------------------------------------------------------------------------

fn to_dir_or_file_response(d: DirOrFile) -> DirOrFileResponse {
    let children = if d.is_dir {
        Some(d.children.into_iter().map(to_dir_or_file_response).collect())
    } else {
        None
    };
    DirOrFileResponse {
        name: d.name,
        full_path: d.full_path,
        relative_path: d.relative_path,
        is_dir: d.is_dir,
        is_file: !d.is_dir,
        children,
    }
}

fn to_flat_file_response(f: WorkspaceFlatFile) -> WorkspaceFlatFileResponse {
    WorkspaceFlatFileResponse {
        name: f.name,
        full_path: f.full_path,
        relative_path: f.relative_path,
    }
}

fn to_metadata_response(m: FileMetadata) -> FileMetadataResponse {
    FileMetadataResponse {
        name: m.name,
        path: m.path,
        size: m.size,
        mime_type: m.mime_type,
        last_modified: m.last_modified,
        is_directory: if m.is_directory { Some(true) } else { None },
    }
}

fn to_copy_response(r: CopyResult) -> CopyFilesResponse {
    CopyFilesResponse {
        copied_files: r.copied_files,
        failed_files: r.failed_files,
    }
}

fn to_zip_entry(e: aionui_api_types::ZipFileEntry) -> ZipEntry {
    if let Some(content) = e.content {
        ZipEntry::Text { name: e.name, content }
    } else if let Some(file_path) = e.file_path {
        ZipEntry::Disk {
            name: e.name,
            file_path,
        }
    } else {
        // Fallback: treat as empty text entry
        ZipEntry::Text {
            name: e.name,
            content: String::new(),
        }
    }
}

fn to_snapshot_info_response(info: SnapshotInfo) -> SnapshotInfoResponse {
    let mode = match info.mode {
        SnapshotMode::GitRepo => aionui_api_types::SnapshotMode::GitRepo,
        SnapshotMode::Snapshot => aionui_api_types::SnapshotMode::Snapshot,
    };
    SnapshotInfoResponse {
        mode,
        branch: info.branch,
    }
}

fn to_file_change_response(c: FileChangeInfo) -> FileChangeInfoResponse {
    FileChangeInfoResponse {
        file_path: c.file_path,
        relative_path: c.relative_path,
        operation: c.operation,
    }
}

fn to_compare_response(r: CompareResult) -> SnapshotCompareResponse {
    SnapshotCompareResponse {
        staged: r.staged.into_iter().map(to_file_change_response).collect(),
        unstaged: r.unstaged.into_iter().map(to_file_change_response).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn file_path_outside_sandbox_maps_to_explicit_api_code() {
        let api_err = ApiError::from(FileError::PathOutsideSandbox {
            message: "path '/tmp/x' is outside the allowed sandbox".into(),
            field: Some("path"),
            operation: Some("access"),
        });
        assert_eq!(api_err.error_code(), "PATH_OUTSIDE_SANDBOX");
        assert_eq!(api_err.error_details().unwrap()["field"], "path");
        assert_eq!(api_err.error_details().unwrap()["operation"], "access");
    }

    #[test]
    fn browse_roots_are_resolved_lazily() {
        let calls = Arc::new(AtomicUsize::new(0));
        let roots = BrowseRoots::with_resolver({
            let calls = calls.clone();
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                vec![std::env::current_dir().unwrap()]
            }
        });

        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let first = roots.get();
        let second = roots.get();

        assert!(!first.is_empty());
        assert_eq!(first, second);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn project_bind_path_accepts_a_git_workspace_inside_an_allowed_root() {
        let allowed = tempfile::tempdir().unwrap();
        let workspace = allowed.path().join("project");
        std::fs::create_dir(&workspace).unwrap();
        git2::Repository::init(&workspace).unwrap();

        let resolved =
            resolve_project_bind_path(&workspace.to_string_lossy(), &[allowed.path().to_path_buf()]).unwrap();
        assert_eq!(resolved, std::fs::canonicalize(workspace).unwrap());
    }

    #[test]
    fn project_bind_path_rejects_non_git_and_outside_workspaces() {
        let allowed = tempfile::tempdir().unwrap();
        let plain = allowed.path().join("plain");
        std::fs::create_dir(&plain).unwrap();
        assert!(matches!(
            resolve_project_bind_path(&plain.to_string_lossy(), &[allowed.path().to_path_buf()]),
            Err(FileError::BadRequest(_))
        ));

        let outside = tempfile::tempdir().unwrap();
        git2::Repository::init(outside.path()).unwrap();
        assert!(matches!(
            resolve_project_bind_path(&outside.path().to_string_lossy(), &[allowed.path().to_path_buf()]),
            Err(FileError::PathOutsideSandbox { .. })
        ));
    }

    #[test]
    fn dir_or_file_response_conversion_file() {
        let d = DirOrFile {
            name: "test.txt".into(),
            full_path: "/ws/test.txt".into(),
            relative_path: "test.txt".into(),
            is_dir: false,
            children: vec![],
        };
        let r = to_dir_or_file_response(d);
        assert_eq!(r.name, "test.txt");
        assert!(!r.is_dir);
        assert!(r.is_file);
        assert!(r.children.is_none());
    }

    #[test]
    fn dir_or_file_response_conversion_dir_with_children() {
        let d = DirOrFile {
            name: "src".into(),
            full_path: "/ws/src".into(),
            relative_path: "src".into(),
            is_dir: true,
            children: vec![DirOrFile {
                name: "main.rs".into(),
                full_path: "/ws/src/main.rs".into(),
                relative_path: "src/main.rs".into(),
                is_dir: false,
                children: vec![],
            }],
        };
        let r = to_dir_or_file_response(d);
        assert!(r.is_dir);
        assert!(!r.is_file);
        let children = r.children.unwrap();
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].name, "main.rs");
    }

    #[test]
    fn flat_file_response_conversion() {
        let f = WorkspaceFlatFile {
            name: "lib.rs".into(),
            full_path: "/ws/src/lib.rs".into(),
            relative_path: "src/lib.rs".into(),
        };
        let r = to_flat_file_response(f);
        assert_eq!(r.name, "lib.rs");
        assert_eq!(r.full_path, "/ws/src/lib.rs");
        assert_eq!(r.relative_path, "src/lib.rs");
    }

    #[test]
    fn metadata_response_conversion_file() {
        let m = FileMetadata {
            name: "readme.md".into(),
            path: "/ws/readme.md".into(),
            size: 1024,
            mime_type: "text/markdown".into(),
            last_modified: 1700000000000,
            is_directory: false,
        };
        let r = to_metadata_response(m);
        assert_eq!(r.name, "readme.md");
        assert_eq!(r.size, 1024);
        assert!(r.is_directory.is_none());
    }

    #[test]
    fn metadata_response_conversion_directory() {
        let m = FileMetadata {
            name: "src".into(),
            path: "/ws/src".into(),
            size: 0,
            mime_type: "".into(),
            last_modified: 1700000000000,
            is_directory: true,
        };
        let r = to_metadata_response(m);
        assert_eq!(r.is_directory, Some(true));
    }

    #[test]
    fn zip_entry_conversion_text() {
        let e = aionui_api_types::ZipFileEntry {
            name: "a.txt".into(),
            content: Some("hello".into()),
            file_path: None,
        };
        let z = to_zip_entry(e);
        match z {
            ZipEntry::Text { name, content } => {
                assert_eq!(name, "a.txt");
                assert_eq!(content, "hello");
            }
            _ => panic!("expected Text variant"),
        }
    }

    #[test]
    fn zip_entry_conversion_disk() {
        let e = aionui_api_types::ZipFileEntry {
            name: "b.bin".into(),
            content: None,
            file_path: Some("/src/b.bin".into()),
        };
        let z = to_zip_entry(e);
        match z {
            ZipEntry::Disk { name, file_path } => {
                assert_eq!(name, "b.bin");
                assert_eq!(file_path, "/src/b.bin");
            }
            _ => panic!("expected Disk variant"),
        }
    }

    #[test]
    fn zip_entry_conversion_empty_fallback() {
        let e = aionui_api_types::ZipFileEntry {
            name: "empty.txt".into(),
            content: None,
            file_path: None,
        };
        let z = to_zip_entry(e);
        match z {
            ZipEntry::Text { name, content } => {
                assert_eq!(name, "empty.txt");
                assert!(content.is_empty());
            }
            _ => panic!("expected Text variant"),
        }
    }

    #[test]
    fn snapshot_info_response_git_repo() {
        let info = SnapshotInfo {
            mode: SnapshotMode::GitRepo,
            branch: Some("main".into()),
        };
        let r = to_snapshot_info_response(info);
        assert_eq!(r.mode, aionui_api_types::SnapshotMode::GitRepo);
        assert_eq!(r.branch, Some("main".into()));
    }

    #[test]
    fn snapshot_info_response_snapshot_mode() {
        let info = SnapshotInfo {
            mode: SnapshotMode::Snapshot,
            branch: None,
        };
        let r = to_snapshot_info_response(info);
        assert_eq!(r.mode, aionui_api_types::SnapshotMode::Snapshot);
        assert!(r.branch.is_none());
    }

    #[test]
    fn compare_response_conversion() {
        use aionui_common::FileChangeOperation;
        let result = CompareResult {
            staged: vec![FileChangeInfo {
                file_path: "/ws/a.txt".into(),
                relative_path: "a.txt".into(),
                operation: FileChangeOperation::Create,
            }],
            unstaged: vec![FileChangeInfo {
                file_path: "/ws/b.txt".into(),
                relative_path: "b.txt".into(),
                operation: FileChangeOperation::Modify,
            }],
        };
        let r = to_compare_response(result);
        assert_eq!(r.staged.len(), 1);
        assert_eq!(r.staged[0].file_path, "/ws/a.txt");
        assert_eq!(r.staged[0].operation, FileChangeOperation::Create);
        assert_eq!(r.unstaged.len(), 1);
        assert_eq!(r.unstaged[0].operation, FileChangeOperation::Modify);
    }

    // ---- sanitize_upload_filename -----------------------------------------

    #[test]
    fn sanitize_upload_filename_strips_directory_components() {
        assert_eq!(sanitize_upload_filename("a/b/c.png").as_deref(), Some("c.png"));
        assert_eq!(sanitize_upload_filename("C:\\tmp\\d.jpg").as_deref(), Some("d.jpg"));
        assert_eq!(
            sanitize_upload_filename("  spaced.txt  ").as_deref(),
            Some("spaced.txt")
        );
    }

    #[test]
    fn sanitize_upload_filename_rejects_empty() {
        assert_eq!(sanitize_upload_filename(""), None);
        assert_eq!(sanitize_upload_filename("   "), None);
        assert_eq!(sanitize_upload_filename("/"), None);
        assert_eq!(sanitize_upload_filename("a/b/"), None);
    }

    #[test]
    fn sanitize_upload_filename_plain_passthrough() {
        assert_eq!(sanitize_upload_filename("image.png").as_deref(), Some("image.png"));
    }
}
