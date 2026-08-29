use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{OnceLock, RwLock};

use walkdir::WalkDir;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedResourcesMode {
    Bundled,
    Download,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManagedResourceSourceKind {
    Bundled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedResourceSource {
    pub kind: ManagedResourceSourceKind,
    pub root: PathBuf,
}

const BUNDLED_RESOURCES_ENV: &str = "AIONUI_BUNDLED_MANAGED_RESOURCES";

pub fn set_managed_resources_mode(mode: ManagedResourcesMode) {
    *mode_lock().write().expect("managed resources mode lock poisoned") = mode;
}

pub fn managed_resources_mode() -> ManagedResourcesMode {
    *mode_lock().read().expect("managed resources mode lock poisoned")
}

pub fn bundled_root_path() -> Option<PathBuf> {
    bundled_root().filter(|root| root.is_dir())
}

pub fn bundled_root_candidate() -> Option<PathBuf> {
    bundled_root()
}

pub fn requires_bundled_resources() -> bool {
    matches!(managed_resources_mode(), ManagedResourcesMode::Bundled)
}

pub fn node_sources(directory_name: &str) -> Vec<ManagedResourceSource> {
    resource_roots()
        .into_iter()
        .map(|source| ManagedResourceSource {
            root: source.root.join("node").join(directory_name),
            ..source
        })
        .filter(|source| source.root.is_dir())
        .collect()
}

pub fn acp_tool_sources(tool_slug: &str, version: &str, platform_key: &str) -> Vec<ManagedResourceSource> {
    resource_roots()
        .into_iter()
        .map(|source| ManagedResourceSource {
            root: source.root.join("acp").join(tool_slug).join(version).join(platform_key),
            ..source
        })
        .filter(|source| source.root.is_dir())
        .collect()
}

pub fn export_node_runtime_to_root(root: &Path, source_root: &Path, directory_name: &str) -> std::io::Result<PathBuf> {
    let target = root.join("node").join(directory_name);
    materialize_directory(source_root, &target)?;
    Ok(target)
}

pub fn export_acp_tool_to_root(
    root: &Path,
    source_root: &Path,
    tool_slug: &str,
    version: &str,
    platform_key: &str,
) -> std::io::Result<PathBuf> {
    let target = root.join("acp").join(tool_slug).join(version).join(platform_key);
    materialize_directory(source_root, &target)?;
    Ok(target)
}

pub fn materialize_directory(source_root: &Path, target_root: &Path) -> std::io::Result<()> {
    if !source_root.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("managed resource source missing: {}", source_root.display()),
        ));
    }

    if source_root == target_root {
        return Ok(());
    }
    if let (Ok(source), Ok(target)) = (fs::canonicalize(source_root), fs::canonicalize(target_root))
        && source == target
    {
        return Ok(());
    }

    if target_root.exists() {
        fs::remove_dir_all(target_root)?;
    }
    fs::create_dir_all(target_root)?;
    let canonical_source_root = fs::canonicalize(source_root)?;
    let mut directory_permissions = Vec::new();

    for entry in WalkDir::new(source_root) {
        let entry = entry?;
        let relative = entry
            .path()
            .strip_prefix(source_root)
            .expect("walkdir path should stay under source root");

        if relative.as_os_str().is_empty() {
            continue;
        }

        let target_path = target_root.join(relative);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target_path)?;
            // Keep target directories writable while descendants are copied.
            // Applying a bundled 0555 mode here would make the remaining walk
            // fail with EACCES even though the source tree is valid.
            directory_permissions.push((entry.path().to_path_buf(), target_path));
            continue;
        }

        if entry.file_type().is_symlink() {
            if let Some(parent) = target_path.parent() {
                fs::create_dir_all(parent)?;
            }
            copy_symlink(entry.path(), &target_path, &canonical_source_root, target_root)?;
            continue;
        }

        if let Some(parent) = target_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(entry.path(), &target_path)?;
        copy_materialized_permissions(entry.path(), &target_path, false)?;
    }

    directory_permissions.sort_by_key(|(_, target)| std::cmp::Reverse(target.components().count()));
    for (source, target) in directory_permissions {
        copy_materialized_permissions(&source, &target, true)?;
    }

    Ok(())
}

fn resource_roots() -> Vec<ManagedResourceSource> {
    let mut roots = Vec::new();

    match managed_resources_mode() {
        ManagedResourcesMode::Bundled => {
            if let Some(root) = bundled_root()
                && root.is_dir()
            {
                roots.push(ManagedResourceSource {
                    kind: ManagedResourceSourceKind::Bundled,
                    root,
                });
            }
        }
        ManagedResourcesMode::Download => {}
    }

    roots
}

fn mode_lock() -> &'static RwLock<ManagedResourcesMode> {
    static MODE: OnceLock<RwLock<ManagedResourcesMode>> = OnceLock::new();
    MODE.get_or_init(|| RwLock::new(default_managed_resources_mode()))
}

fn default_managed_resources_mode() -> ManagedResourcesMode {
    ManagedResourcesMode::Download
}

fn bundled_root() -> Option<PathBuf> {
    configured_root(BUNDLED_RESOURCES_ENV).or_else(default_bundled_root)
}

fn configured_root(env_key: &str) -> Option<PathBuf> {
    std::env::var_os(env_key)
        .map(PathBuf::from)
        .filter(|path| !path.as_os_str().is_empty())
}

fn default_bundled_root() -> Option<PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let exe_dir = fs::canonicalize(exe).ok()?.parent()?.to_path_buf();
    Some(exe_dir.join("managed-resources"))
}

fn copy_materialized_permissions(source: &Path, target: &Path, is_directory: bool) -> std::io::Result<()> {
    let metadata = fs::metadata(source)?;
    let mut permissions = metadata.permissions();

    // Bundled resources are immutable inputs, while their per-runtime copy is
    // operational state. Keep the source's execute bits, but make the copy
    // writable by its owner so Node/Codex can initialize caches and the
    // runtime can clean up the assignment directory without thawing the
    // shared source tree.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let owner_bits = if is_directory { 0o700 } else { 0o600 };
        permissions.set_mode(permissions.mode() | owner_bits);
    }
    #[cfg(not(unix))]
    permissions.set_readonly(false);

    fs::set_permissions(target, permissions)
}

fn copy_symlink(source: &Path, target: &Path, source_root: &Path, target_root: &Path) -> std::io::Result<()> {
    let link_target = fs::read_link(source)?;
    let relative_to_source = if link_target.is_absolute() {
        resolve_absolute_link_inside_source(source, &link_target, source_root)?
    } else {
        resolve_relative_link_inside_source(source, &link_target, source_root)?
    };
    let mapped_target = target_root.join(relative_to_source);
    let target_parent = target.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("managed resource symlink has no parent: {}", target.display()),
        )
    })?;
    let materialized_link_target = relative_path(target_parent, &mapped_target)?;

    if fs::symlink_metadata(target).is_ok() {
        fs::remove_file(target)?;
    }
    create_symlink(&materialized_link_target, target, source)
}

fn resolve_relative_link_inside_source(
    source: &Path,
    link_target: &Path,
    source_root: &Path,
) -> std::io::Result<PathBuf> {
    let source_parent = source.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("managed resource symlink has no parent: {}", source.display()),
        )
    })?;
    let resolved = fs::canonicalize(source_parent.join(link_target)).map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!(
                "managed resource relative symlink target is unavailable: {} -> {}: {error}",
                source.display(),
                link_target.display()
            ),
        )
    })?;
    resolved.strip_prefix(source_root).map(Path::to_path_buf).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "managed resource relative symlink escapes its source tree: {} -> {}",
                source.display(),
                link_target.display()
            ),
        )
    })
}

fn resolve_absolute_link_inside_source(
    source: &Path,
    link_target: &Path,
    source_root: &Path,
) -> std::io::Result<PathBuf> {
    if let Ok(resolved) = fs::canonicalize(link_target) {
        return resolved.strip_prefix(source_root).map(Path::to_path_buf).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "managed resource absolute symlink escapes its source tree: {} -> {}",
                    source.display(),
                    link_target.display()
                ),
            )
        });
    }

    if let Some(root_name) = source_root.file_name()
        && let Some(recovered_relative) = path_suffix_after_component(link_target, root_name)
        && let Ok(recovered) = fs::canonicalize(source_root.join(&recovered_relative))
        && let Ok(relative) = recovered.strip_prefix(source_root)
    {
        return Ok(relative.to_path_buf());
    }

    let source_parent = source.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("managed resource symlink has no parent: {}", source.display()),
        )
    })?;
    let canonical_source_parent = fs::canonicalize(source_parent)?;
    let source_location = canonical_source_parent.join(source.file_name().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("managed resource symlink has no filename: {}", source.display()),
        )
    })?);
    let source_relative = source_location.strip_prefix(source_root).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "managed resource symlink is outside its source tree: {}",
                source.display()
            ),
        )
    })?;
    let mut source_components = source_relative.components();
    let is_npm_bin_link = source_components
        .next()
        .is_some_and(|part| part.as_os_str() == "node_modules")
        && source_components.next().is_some_and(|part| part.as_os_str() == ".bin");
    if !is_npm_bin_link {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "managed resource absolute symlink target is unavailable: {} -> {}",
                source.display(),
                link_target.display()
            ),
        ));
    }

    let recovered_relative =
        path_suffix_from_component(link_target, std::ffi::OsStr::new("node_modules")).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "managed npm bin link has no recoverable node_modules target: {} -> {}",
                    source.display(),
                    link_target.display()
                ),
            )
        })?;

    let recovered = fs::canonicalize(source_root.join(&recovered_relative)).map_err(|error| {
        std::io::Error::new(
            error.kind(),
            format!(
                "managed npm bin link target could not be recovered inside its source tree: {} -> {}: {error}",
                source.display(),
                link_target.display()
            ),
        )
    })?;
    recovered.strip_prefix(source_root).map(Path::to_path_buf).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "managed npm bin link recovery escaped its source tree: {} -> {}",
                source.display(),
                link_target.display()
            ),
        )
    })
}

fn path_suffix_after_component(path: &Path, marker: &std::ffi::OsStr) -> Option<PathBuf> {
    let components = path.components().collect::<Vec<_>>();
    let marker_index = components.iter().rposition(|part| part.as_os_str() == marker)?;
    let suffix = components.get(marker_index + 1..)?;
    if suffix.is_empty() {
        return None;
    }
    normal_components_to_path(suffix)
}

fn path_suffix_from_component(path: &Path, marker: &std::ffi::OsStr) -> Option<PathBuf> {
    let components = path.components().collect::<Vec<_>>();
    let marker_index = components.iter().rposition(|part| part.as_os_str() == marker)?;
    normal_components_to_path(components.get(marker_index..)?)
}

fn normal_components_to_path(components: &[std::path::Component<'_>]) -> Option<PathBuf> {
    let mut result = PathBuf::new();
    for component in components {
        match component {
            std::path::Component::Normal(part) => result.push(part),
            _ => return None,
        }
    }
    (!result.as_os_str().is_empty()).then_some(result)
}

fn relative_path(from_directory: &Path, to_path: &Path) -> std::io::Result<PathBuf> {
    let from_components = from_directory.components().collect::<Vec<_>>();
    let to_components = to_path.components().collect::<Vec<_>>();
    let shared_components = from_components
        .iter()
        .zip(&to_components)
        .take_while(|(from, to)| from == to)
        .count();

    if shared_components == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "managed resource symlink paths do not share a filesystem root: {} and {}",
                from_directory.display(),
                to_path.display()
            ),
        ));
    }

    let mut relative = PathBuf::new();
    for _ in shared_components..from_components.len() {
        relative.push("..");
    }
    for component in &to_components[shared_components..] {
        relative.push(component.as_os_str());
    }
    if relative.as_os_str().is_empty() {
        relative.push(".");
    }
    Ok(relative)
}

#[cfg(unix)]
fn create_symlink(link_target: &Path, target: &Path, _source: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(link_target, target)
}

#[cfg(windows)]
fn create_symlink(link_target: &Path, target: &Path, source: &Path) -> std::io::Result<()> {
    let file_type = fs::metadata(source)?;
    if file_type.is_dir() {
        std::os::windows::fs::symlink_dir(link_target, target)
    } else {
        std::os::windows::fs::symlink_file(link_target, target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_mode_is_download() {
        if !crate::test_support::run_in_env_child("managed_resources::tests::default_mode_is_download", |command| {
            command.env_remove(BUNDLED_RESOURCES_ENV);
        }) {
            return;
        }
        assert_eq!(default_managed_resources_mode(), ManagedResourcesMode::Download);
    }

    #[test]
    fn bundled_mode_uses_configured_bundled_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("managed");
        if !crate::test_support::run_in_env_child(
            "managed_resources::tests::bundled_mode_uses_configured_bundled_root",
            |command| {
                command.env(BUNDLED_RESOURCES_ENV, &root);
            },
        ) {
            return;
        }
        let root = PathBuf::from(std::env::var_os(BUNDLED_RESOURCES_ENV).expect("bundled root env"));
        fs::create_dir_all(root.join("node").join("node-v24.11.0-darwin-arm64")).expect("create node dir");

        set_managed_resources_mode(ManagedResourcesMode::Bundled);

        let sources = node_sources("node-v24.11.0-darwin-arm64");
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0].kind, ManagedResourceSourceKind::Bundled);
        assert_eq!(sources[0].root, root.join("node").join("node-v24.11.0-darwin-arm64"));

        set_managed_resources_mode(ManagedResourcesMode::Download);
    }

    #[test]
    fn download_mode_ignores_configured_bundled_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let root = temp.path().join("managed");
        if !crate::test_support::run_in_env_child(
            "managed_resources::tests::download_mode_ignores_configured_bundled_root",
            |command| {
                command.env(BUNDLED_RESOURCES_ENV, &root);
            },
        ) {
            return;
        }
        let root = PathBuf::from(std::env::var_os(BUNDLED_RESOURCES_ENV).expect("bundled root env"));
        fs::create_dir_all(root.join("node").join("node-v24.11.0-darwin-arm64")).expect("create node dir");

        set_managed_resources_mode(ManagedResourcesMode::Download);

        let sources = node_sources("node-v24.11.0-darwin-arm64");
        assert!(sources.is_empty());
        assert!(!requires_bundled_resources());

        set_managed_resources_mode(ManagedResourcesMode::Download);
    }

    #[cfg(unix)]
    #[test]
    fn materialize_directory_preserves_symlink_entries() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("source-node");
        fs::create_dir_all(source.join("bin")).expect("create source");
        fs::create_dir_all(source.join("lib").join("node_modules").join("npm").join("bin")).expect("create npm bin");
        fs::write(
            source
                .join("lib")
                .join("node_modules")
                .join("npm")
                .join("bin")
                .join("npm-cli.js"),
            b"#!/usr/bin/env node\n",
        )
        .expect("write npm cli");
        std::os::unix::fs::symlink(
            Path::new("../lib/node_modules/npm/bin/npm-cli.js"),
            source.join("bin").join("npm"),
        )
        .expect("create symlink");

        let target = temp.path().join("target-node");
        materialize_directory(&source, &target).expect("materialize");

        let copied_link = target.join("bin").join("npm");
        let metadata = fs::symlink_metadata(&copied_link).expect("metadata");
        assert!(metadata.file_type().is_symlink());
        assert_eq!(
            fs::read_link(&copied_link).expect("read link"),
            PathBuf::from("../lib/node_modules/npm/bin/npm-cli.js")
        );
    }

    #[cfg(unix)]
    #[test]
    fn materialize_directory_rewrites_internal_absolute_symlinks_as_relocatable_links() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("source-tool");
        let source_entrypoint = source.join("node_modules/package/dist/index.js");
        let source_bin = source.join("node_modules/.bin/tool");
        fs::create_dir_all(source_entrypoint.parent().expect("entrypoint parent")).expect("create package");
        fs::create_dir_all(source_bin.parent().expect("bin parent")).expect("create bin");
        fs::write(&source_entrypoint, b"console.log('tool');\n").expect("write entrypoint");
        std::os::unix::fs::symlink(&source_entrypoint, &source_bin).expect("create absolute symlink");

        let target = temp.path().join("target-tool");
        materialize_directory(&source, &target).expect("materialize");

        let copied_link = target.join("node_modules/.bin/tool");
        assert_eq!(
            fs::read_link(&copied_link).expect("read copied link"),
            PathBuf::from("../package/dist/index.js")
        );
        fs::remove_dir_all(&source).expect("remove source staging tree");
        let relocated = temp.path().join("relocated-tool");
        fs::rename(&target, &relocated).expect("relocate materialized tree");
        assert_eq!(
            fs::read(relocated.join("node_modules/.bin/tool")).expect("read relocated link target"),
            b"console.log('tool');\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn materialize_directory_recovers_stale_absolute_npm_bin_links_from_a_previous_staging_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("source-tool");
        let source_entrypoint = source.join("node_modules/package/dist/index.js");
        let source_bin = source.join("node_modules/.bin/tool");
        fs::create_dir_all(source_entrypoint.parent().expect("entrypoint parent")).expect("create package");
        fs::create_dir_all(source_bin.parent().expect("bin parent")).expect("create bin");
        fs::write(&source_entrypoint, b"console.log('tool');\n").expect("write entrypoint");
        std::os::unix::fs::symlink(
            temp.path()
                .join("deleted-staging/project/node_modules/package/dist/index.js"),
            &source_bin,
        )
        .expect("create stale absolute symlink");

        let target = temp.path().join("target-tool");
        materialize_directory(&source, &target).expect("recover stale npm bin link");

        let copied_link = target.join("node_modules/.bin/tool");
        assert_eq!(
            fs::read_link(&copied_link).expect("read recovered link"),
            PathBuf::from("../package/dist/index.js")
        );
        assert_eq!(
            fs::read(&copied_link).expect("read recovered link target"),
            b"console.log('tool');\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn materialize_directory_recovers_stale_absolute_links_from_the_same_versioned_resource_root() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("node-v24.11.0-linux-x64");
        let source_cli = source.join("lib/node_modules/npm/bin/npm-cli.js");
        let source_bin = source.join("bin/npm");
        fs::create_dir_all(source_cli.parent().expect("cli parent")).expect("create npm package");
        fs::create_dir_all(source_bin.parent().expect("bin parent")).expect("create bin");
        fs::write(&source_cli, b"console.log('npm');\n").expect("write npm cli");
        std::os::unix::fs::symlink(
            temp.path().join(
                "deleted-staging/managed-resources/node/node-v24.11.0-linux-x64/lib/node_modules/npm/bin/npm-cli.js",
            ),
            &source_bin,
        )
        .expect("create stale Node symlink");

        let target = temp.path().join("target-node");
        materialize_directory(&source, &target).expect("recover stale versioned resource link");

        let copied_link = target.join("bin/npm");
        assert_eq!(
            fs::read_link(&copied_link).expect("read recovered link"),
            PathBuf::from("../lib/node_modules/npm/bin/npm-cli.js")
        );
        assert_eq!(
            fs::read(&copied_link).expect("read recovered link target"),
            b"console.log('npm');\n"
        );
    }

    #[cfg(unix)]
    #[test]
    fn materialize_directory_rejects_absolute_symlinks_outside_the_source_tree() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("source-tool");
        let external = temp.path().join("external.js");
        fs::create_dir_all(source.join("bin")).expect("create source");
        fs::write(&external, b"external\n").expect("write external target");
        std::os::unix::fs::symlink(&external, source.join("bin/tool")).expect("create external symlink");

        let error = materialize_directory(&source, &temp.path().join("target-tool"))
            .expect_err("external absolute symlink should be rejected");

        assert!(error.to_string().contains("escapes its source tree"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn materialize_directory_rejects_relative_symlinks_outside_the_source_tree() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("source-tool");
        let external = temp.path().join("external.js");
        fs::create_dir_all(source.join("bin")).expect("create source");
        fs::write(&external, b"external\n").expect("write external target");
        std::os::unix::fs::symlink(Path::new("../../external.js"), source.join("bin/tool"))
            .expect("create external symlink");

        let error = materialize_directory(&source, &temp.path().join("target-tool"))
            .expect_err("external relative symlink should be rejected");

        assert!(error.to_string().contains("escapes its source tree"), "{error}");
    }

    #[cfg(unix)]
    #[test]
    fn materialize_directory_keeps_source_read_only_and_target_writable() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("source");
        let source_bin = source.join("bin");
        fs::create_dir_all(&source_bin).expect("create source");
        fs::write(source_bin.join("node"), b"managed runtime\n").expect("write source file");
        fs::set_permissions(source_bin.join("node"), fs::Permissions::from_mode(0o444)).expect("freeze source file");
        fs::set_permissions(&source_bin, fs::Permissions::from_mode(0o555)).expect("freeze source dir");

        let target = temp.path().join("target");
        materialize_directory(&source, &target).expect("materialize read-only source");

        assert_eq!(
            fs::read(target.join("bin/node")).expect("read target"),
            b"managed runtime\n"
        );
        assert_eq!(
            fs::metadata(target.join("bin"))
                .expect("target metadata")
                .permissions()
                .mode()
                & 0o777,
            0o755
        );
        assert_eq!(
            fs::metadata(target.join("bin/node"))
                .expect("target file metadata")
                .permissions()
                .mode()
                & 0o777,
            0o644
        );
        assert_eq!(
            fs::metadata(&source_bin).expect("source metadata").permissions().mode() & 0o777,
            0o555
        );
        assert_eq!(
            fs::metadata(source_bin.join("node"))
                .expect("source file metadata")
                .permissions()
                .mode()
                & 0o777,
            0o444
        );

        fs::set_permissions(&source_bin, fs::Permissions::from_mode(0o755)).expect("thaw source cleanup");
        fs::set_permissions(source_bin.join("node"), fs::Permissions::from_mode(0o644))
            .expect("thaw source file cleanup");
    }
}
