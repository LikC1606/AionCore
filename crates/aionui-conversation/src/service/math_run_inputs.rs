#[cfg(unix)]
use std::path::Path;
use std::path::PathBuf;

use serde::Deserialize;
use serde_json::Value;

use crate::error::ConversationError;

pub(crate) fn capability() -> aionui_api_types::MathRunInputsCapability {
    aionui_api_types::MathRunInputsCapability {
        supported_versions: if cfg!(unix) { vec![1, 2] } else { vec![] },
    }
}

pub(crate) const INTEGRITY_CODE: &str = "math_run_inputs_integrity_failed";
pub(crate) const INTEGRITY_MESSAGE: &str = "math_run_inputs_integrity_failed: Run inputs changed or are unavailable. Stop this run; model edits cannot repair its identity.";

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{INTEGRITY_MESSAGE}")]
pub(crate) struct IntegrityError;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileIdentity {
    path: PathBuf,
    digest: String,
    bytes: u64,
    device: String,
    inode: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MathRunInputs {
    pub(crate) root: PathBuf,
    task: FileIdentity,
    environment: FileIdentity,
    #[serde(default)]
    binding: Option<FileIdentity>,
    #[serde(default, rename = "retrievalCorpus")]
    retrieval_corpus: Option<FileIdentity>,
}

fn raw_inputs(extra: &Value) -> Option<&Value> {
    if extra.get("research_team_profile_id").and_then(Value::as_str) != Some("mathematics-research-team") {
        return None;
    }
    extra.get("mathematics_runtime")?.get("run_inputs")
}

pub(crate) fn initializes_inputs(existing: &Value, incoming: &Value) -> bool {
    raw_inputs(existing).is_none() && raw_inputs(incoming).is_some()
}

pub(crate) fn validate_update(
    existing: &Value,
    merged: &Value,
    allow_initialization: bool,
) -> Result<(), ConversationError> {
    if existing.pointer("/mathematics_runtime/budget_required") == Some(&serde_json::Value::Bool(true))
        && merged.pointer("/mathematics_runtime/budget_required") != Some(&serde_json::Value::Bool(true))
    {
        return Err(ConversationError::bad_request("math_budget_required_immutable"));
    }
    if let Some(previous) = raw_inputs(existing) {
        if raw_inputs(merged) != Some(previous)
            || existing.pointer("/mathematics_runtime/envFile") != merged.pointer("/mathematics_runtime/envFile")
        {
            return Err(ConversationError::bad_request(
                "Mathematics run inputs are immutable after initialization",
            ));
        }
    } else if raw_inputs(merged).is_some() {
        if !allow_initialization {
            return Err(ConversationError::bad_request(
                "Mathematics run inputs require a new conversation or initial Team bootstrap",
            ));
        }
        validate_initialization(merged)?;
    }
    Ok(())
}

pub(crate) fn validate_initialization(extra: &Value) -> Result<(), ConversationError> {
    parse(extra)
        .and_then(|inputs| verify_optional(inputs.as_ref()))
        .map_err(|_| ConversationError::bad_request(INTEGRITY_MESSAGE))
}

pub(crate) fn from_persisted_extra(extra: &str) -> Result<Option<MathRunInputs>, IntegrityError> {
    // Unrelated legacy conversations keep their existing malformed-extra handling.
    parse(&serde_json::from_str(extra).unwrap_or(Value::Null))
}

fn parse(extra: &Value) -> Result<Option<MathRunInputs>, IntegrityError> {
    let Some(value) = raw_inputs(extra) else {
        return Ok(None);
    };
    if ["binding", "retrievalCorpus"]
        .iter()
        .any(|key| value.get(key).is_some_and(Value::is_null))
    {
        return Err(IntegrityError);
    }
    let inputs: MathRunInputs = serde_json::from_value(value.clone()).map_err(|_| IntegrityError)?;
    if extra.pointer("/mathematics_runtime/envFile").and_then(Value::as_str) != inputs.environment.path.to_str() {
        return Err(IntegrityError);
    }
    Ok(Some(inputs))
}

pub(crate) fn verify_optional(inputs: Option<&MathRunInputs>) -> Result<(), IntegrityError> {
    inputs.map_or(Ok(()), MathRunInputs::verify)
}

impl MathRunInputs {
    #[cfg(unix)]
    fn verify(&self) -> Result<(), IntegrityError> {
        use std::fs;
        use std::os::unix::fs::MetadataExt;

        let directory = fs::symlink_metadata(&self.root).map_err(|_| IntegrityError)?;
        if !self.root.is_absolute()
            || fs::canonicalize(&self.root).map_err(|_| IntegrityError)? != self.root
            || !directory.is_dir()
            || directory.mode() & 0o222 != 0
            || self.task.path == self.environment.path
        {
            return Err(IntegrityError);
        }
        self.task.verify(&self.root)?;
        if self.environment.bytes > 512 * 1024 {
            return Err(IntegrityError);
        }
        let environment: Value =
            serde_json::from_slice(&self.environment.verify_contents(&self.root, true)?).map_err(|_| IntegrityError)?;
        let environment = environment.as_object().ok_or(IntegrityError)?;
        let corpus_path = environment.get("DEEPSCIENTIST_MATH_RETRIEVAL_CORPUS");
        let corpus_digest = environment.get("DEEPSCIENTIST_MATH_RETRIEVAL_CORPUS_SHA256");
        if let Some(corpus) = &self.retrieval_corpus {
            if corpus.path == self.task.path
                || corpus.path == self.environment.path
                || self.binding.as_ref().is_some_and(|binding| binding.path == corpus.path)
                || corpus_path.and_then(Value::as_str) != corpus.path.to_str()
                || corpus_digest.and_then(Value::as_str) != Some(corpus.digest.as_str())
            {
                return Err(IntegrityError);
            }
            corpus.verify(&self.root)?;
        } else if corpus_path.is_some() || corpus_digest.is_some() {
            return Err(IntegrityError);
        }
        if let Some(binding) = &self.binding {
            if binding.path == self.task.path || binding.path == self.environment.path {
                return Err(IntegrityError);
            }
            binding.verify(&self.root)?;
        }
        Ok(())
    }

    #[cfg(not(unix))]
    fn verify(&self) -> Result<(), IntegrityError> {
        // The runner's current device/inode/read-only contract is Unix-specific.
        Err(IntegrityError)
    }
}

impl FileIdentity {
    #[cfg(unix)]
    fn verify(&self, root: &Path) -> Result<(), IntegrityError> {
        self.verify_contents(root, false).map(|_| ())
    }

    #[cfg(unix)]
    fn verify_contents(&self, root: &Path, capture: bool) -> Result<Vec<u8>, IntegrityError> {
        use sha2::{Digest, Sha256};
        use std::fs::{self, OpenOptions};
        use std::io::Read;
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

        if self.path.parent() != Some(root)
            || fs::canonicalize(&self.path).map_err(|_| IntegrityError)? != self.path
            || self.bytes > 9_007_199_254_740_991
            || self.digest.len() != 64
            || !self
                .digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(IntegrityError);
        }
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&self.path)
            .map_err(|_| IntegrityError)?;
        let before = file.metadata().map_err(|_| IntegrityError)?;
        if !before.is_file()
            || before.nlink() != 1
            || before.mode() & 0o222 != 0
            || before.len() != self.bytes
            || before.dev().to_string() != self.device
            || before.ino().to_string() != self.inode
        {
            return Err(IntegrityError);
        }
        // Stream only the pinned length plus one byte: file growth cannot cause
        // unbounded allocation or keep the verifier reading forever.
        let mut reader = (&mut file).take(self.bytes + 1);
        let mut digest = Sha256::new();
        let mut bytes = 0;
        let mut contents = Vec::new();
        let mut buffer = [0u8; 8192];
        loop {
            let size = reader.read(&mut buffer).map_err(|_| IntegrityError)?;
            if size == 0 {
                break;
            }
            bytes += size as u64;
            digest.update(&buffer[..size]);
            if capture {
                contents.extend_from_slice(&buffer[..size]);
            }
        }
        let after = file.metadata().map_err(|_| IntegrityError)?;
        let current = fs::symlink_metadata(&self.path).map_err(|_| IntegrityError)?;
        if bytes != self.bytes
            || format!("{:x}", digest.finalize()) != self.digest
            || after.len() != before.len()
            || after.mtime() != before.mtime()
            || after.mtime_nsec() != before.mtime_nsec()
            || after.ctime() != before.ctime()
            || after.ctime_nsec() != before.ctime_nsec()
            || current.dev() != before.dev()
            || current.ino() != before.ino()
            || current.mode() != before.mode()
            || fs::canonicalize(&self.path).map_err(|_| IntegrityError)? != self.path
        {
            return Err(IntegrityError);
        }
        Ok(contents)
    }
}

#[cfg(all(test, unix))]
pub(crate) mod tests {
    use super::*;
    use serde_json::json;
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::os::unix::fs::{MetadataExt, PermissionsExt, symlink};

    pub(crate) struct Fixture {
        pub directory: tempfile::TempDir,
        pub root: PathBuf,
        pub extra: Value,
    }

    impl Fixture {
        pub(crate) fn new() -> Self {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().canonicalize().unwrap().join("inputs");
            fs::create_dir(&root).unwrap();
            let identity = |name: &str, content: &str| {
                let file = root.join(name);
                fs::write(&file, content).unwrap();
                fs::set_permissions(&file, fs::Permissions::from_mode(0o400)).unwrap();
                let metadata = fs::metadata(&file).unwrap();
                json!({"path": file, "bytes": content.len(), "digest": format!("{:x}", Sha256::digest(content)),
                    "device": metadata.dev().to_string(), "inode": metadata.ino().to_string()})
            };
            let task = identity("task.md", "Prove the public theorem.");
            let environment = identity(
                "environment.json",
                &json!({"PATH": "/toolchain", "DEEPSCIENTIST_MATH_ENV_FILE": root.join("environment.json")})
                    .to_string(),
            );
            fs::set_permissions(&root, fs::Permissions::from_mode(0o500)).unwrap();
            let extra = json!({"research_team_profile_id": "mathematics-research-team",
                "mathematics_runtime": {"envFile": root.join("environment.json"),
                    "run_inputs": {"root": root, "task": task, "environment": environment}}});
            Self { directory, root, extra }
        }

        pub(crate) fn change_task(&self) {
            let task = self.root.join("task.md");
            fs::set_permissions(&task, fs::Permissions::from_mode(0o600)).unwrap();
            fs::write(&task, "Changed task contents.").unwrap();
            fs::set_permissions(&task, fs::Permissions::from_mode(0o400)).unwrap();
        }

        pub(crate) fn bind_retrieval_corpus(&mut self) -> PathBuf {
            fs::set_permissions(&self.root, fs::Permissions::from_mode(0o700)).unwrap();
            let identity = |name: &str, content: &str| {
                let file = self.root.join(name);
                if file.exists() {
                    fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
                }
                fs::write(&file, content).unwrap();
                fs::set_permissions(&file, fs::Permissions::from_mode(0o400)).unwrap();
                let metadata = fs::metadata(&file).unwrap();
                json!({"path": file, "bytes": content.len(), "digest": format!("{:x}", Sha256::digest(content)),
                    "device": metadata.dev().to_string(), "inode": metadata.ino().to_string()})
            };
            let corpus = identity("retrieval-corpus.json", "{\"test\":\"frozen\"}");
            let environment = identity(
                "environment.json",
                &json!({
                    "DEEPSCIENTIST_MATH_ENV_FILE": self.root.join("environment.json"),
                    "DEEPSCIENTIST_MATH_RETRIEVAL_CORPUS": corpus["path"],
                    "DEEPSCIENTIST_MATH_RETRIEVAL_CORPUS_SHA256": corpus["digest"]
                })
                .to_string(),
            );
            self.extra["mathematics_runtime"]["run_inputs"]["retrievalCorpus"] = corpus;
            self.extra["mathematics_runtime"]["run_inputs"]["environment"] = environment;
            fs::set_permissions(&self.root, fs::Permissions::from_mode(0o500)).unwrap();
            self.root.join("retrieval-corpus.json")
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::set_permissions(&self.root, fs::Permissions::from_mode(0o700));
        }
    }

    #[test]
    fn roundtripped_persisted_identity_verifies_and_detects_later_task_change() {
        let fixture = Fixture::new();
        let inputs = from_persisted_extra(&fixture.extra.to_string()).unwrap();
        assert_eq!(verify_optional(inputs.as_ref()), Ok(()));
        fixture.change_task();
        assert_eq!(verify_optional(inputs.as_ref()), Err(IntegrityError));
    }

    #[test]
    fn retrieval_corpus_is_verified_and_cannot_be_omitted_aliased_or_null() {
        let mut fixture = Fixture::new();
        let file = fixture.bind_retrieval_corpus();
        let inputs = from_persisted_extra(&fixture.extra.to_string()).unwrap();
        assert_eq!(verify_optional(inputs.as_ref()), Ok(()));
        for replacement in [
            Value::Null,
            fixture.extra["mathematics_runtime"]["run_inputs"]["task"].clone(),
        ] {
            let mut extra = fixture.extra.clone();
            extra["mathematics_runtime"]["run_inputs"]["retrievalCorpus"] = replacement;
            assert_eq!(
                validate_initialization(&extra).unwrap_err().to_string(),
                format!("Bad request: {INTEGRITY_MESSAGE}")
            );
        }
        let mut omitted = fixture.extra.clone();
        omitted["mathematics_runtime"]["run_inputs"]
            .as_object_mut()
            .unwrap()
            .remove("retrievalCorpus");
        assert!(validate_initialization(&omitted).is_err());
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&file, "changed corpus").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o400)).unwrap();
        assert_eq!(verify_optional(inputs.as_ref()), Err(IntegrityError));
    }

    #[test]
    fn frozen_binding_is_checked_and_cannot_alias_task_or_be_null() {
        let mut fixture = Fixture::new();
        fixture.extra["mathematics_runtime"]["run_inputs"]["binding"] = Value::Null;
        assert!(validate_initialization(&fixture.extra).is_err());
        fixture.extra["mathematics_runtime"]["run_inputs"]["binding"] =
            fixture.extra["mathematics_runtime"]["run_inputs"]["task"].clone();
        assert!(validate_initialization(&fixture.extra).is_err());
        fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o700)).unwrap();
        let file = fixture.root.join("binding.json");
        let content = "{\"schema\":\"deepscientist.math.run-binding.v2\"}";
        fs::write(&file, content).unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o400)).unwrap();
        let stat = fs::metadata(&file).unwrap();
        fixture.extra["mathematics_runtime"]["run_inputs"]["binding"] = json!({
            "path": file, "digest": format!("{:x}", sha2::Sha256::digest(content.as_bytes())),
            "bytes": stat.len(), "device": stat.dev().to_string(), "inode": stat.ino().to_string()
        });
        fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o500)).unwrap();
        let inputs = parse(&fixture.extra).unwrap();
        assert_eq!(verify_optional(inputs.as_ref()), Ok(()));
        fs::set_permissions(&file, fs::Permissions::from_mode(0o600)).unwrap();
        fs::write(&file, "changed binding").unwrap();
        fs::set_permissions(&file, fs::Permissions::from_mode(0o400)).unwrap();
        assert_eq!(verify_optional(inputs.as_ref()), Err(IntegrityError));
    }

    #[test]
    fn rejects_malformed_identity_and_environment_path_without_echoing_values() {
        let fixture = Fixture::new();
        for pointer in [
            "/mathematics_runtime/run_inputs",
            "/mathematics_runtime/run_inputs/task/bytes",
            "/mathematics_runtime/run_inputs/task/digest",
            "/mathematics_runtime/envFile",
        ] {
            let mut extra = fixture.extra.clone();
            *extra.pointer_mut(pointer).unwrap() = json!("private-invalid-value");
            let error = validate_initialization(&extra).unwrap_err();
            assert_eq!(error.to_string(), format!("Bad request: {INTEGRITY_MESSAGE}"));
        }
        let mut extra = fixture.extra.clone();
        extra["mathematics_runtime"]["run_inputs"]["unexpected"] = json!(true);
        assert!(validate_initialization(&extra).is_err());
    }

    #[test]
    fn rejects_replacement_symlink_hardlink_permissions_and_missing_files() {
        for mutation in [
            "replace",
            "symlink",
            "hardlink",
            "file_mode",
            "directory_mode",
            "missing",
            "environment",
        ] {
            let fixture = Fixture::new();
            let inputs = parse(&fixture.extra).unwrap();
            let task = fixture.root.join("task.md");
            fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o700)).unwrap();
            match mutation {
                "replace" => {
                    let replacement = fixture.root.join("replacement");
                    fs::copy(&task, &replacement).unwrap();
                    fs::rename(replacement, &task).unwrap();
                }
                "symlink" => {
                    let moved = fixture.directory.path().join("moved");
                    fs::rename(&task, &moved).unwrap();
                    symlink(&moved, &task).unwrap();
                }
                "hardlink" => fs::hard_link(&task, fixture.directory.path().join("alias")).unwrap(),
                "file_mode" => fs::set_permissions(&task, fs::Permissions::from_mode(0o600)).unwrap(),
                "missing" => fs::remove_file(&task).unwrap(),
                "environment" => fs::remove_file(fixture.root.join("environment.json")).unwrap(),
                "directory_mode" => {}
                _ => unreachable!(),
            }
            if mutation != "directory_mode" {
                fs::set_permissions(&fixture.root, fs::Permissions::from_mode(0o500)).unwrap();
            }
            assert_eq!(verify_optional(inputs.as_ref()), Err(IntegrityError), "{mutation}");
        }
    }

    #[test]
    fn unrelated_profiles_and_legacy_math_without_metadata_keep_existing_behavior() {
        for extra in [
            json!({}),
            json!({"research_team_profile_id": "mathematics-research-team"}),
            json!({"research_team_profile_id": "financial-factor-research", "mathematics_runtime": {"run_inputs": null}}),
        ] {
            assert!(parse(&extra).unwrap().is_none());
        }
        let extra = json!({"research_team_profile_id": "mathematics-research-team", "mathematics_runtime": {"run_inputs": null}});
        assert_eq!(parse(&extra).err(), Some(IntegrityError));
    }

    #[test]
    fn inputs_cannot_be_rebound_removed_or_disabled_after_initialization() {
        let fixture = Fixture::new();
        for pointer in [
            "/mathematics_runtime",
            "/mathematics_runtime/run_inputs",
            "/mathematics_runtime/envFile",
            "/research_team_profile_id",
        ] {
            let mut merged = fixture.extra.clone();
            *merged.pointer_mut(pointer).unwrap() = Value::Null;
            assert_eq!(
                validate_update(&fixture.extra, &merged, true).unwrap_err().to_string(),
                "Bad request: Mathematics run inputs are immutable after initialization"
            );
        }
        assert!(validate_update(&fixture.extra, &fixture.extra, false).is_ok());
        assert!(validate_update(&json!({}), &fixture.extra, true).is_ok());
        assert_eq!(
            validate_update(&json!({}), &fixture.extra, false)
                .unwrap_err()
                .to_string(),
            "Bad request: Mathematics run inputs require a new conversation or initial Team bootstrap"
        );
    }
}
