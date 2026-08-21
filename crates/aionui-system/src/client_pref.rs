use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use aionui_api_types::{ClientPreferencesResponse, UpdateClientPreferencesRequest};
use aionui_db::IClientPreferenceRepository;
use tracing::{debug, info, warn};

use crate::error::SystemError;
use crate::keep_awake::{DynKeepAwakeController, KEEP_AWAKE_KEY, NoopKeepAwakeController};

/// Maximum allowed key length for client preferences.
const MAX_KEY_LENGTH: usize = 255;
const GLOBAL_MEMORY_KEY: &str = "memory.global.entries";
const GLOBAL_MEMORY_SCHEMA: &str = "deepscientist.global-memory.v1";
const GLOBAL_MEMORY_MAX_VALUE_BYTES: usize = 64 * 1024;
const GLOBAL_MEMORY_MAX_ENTRIES: usize = 20;
const GLOBAL_MEMORY_MAX_ID_CHARS: usize = 128;
const GLOBAL_MEMORY_MAX_TITLE_CHARS: usize = 80;
const GLOBAL_MEMORY_MAX_CONTENT_CHARS: usize = 2_000;
const GLOBAL_MEMORY_MAX_ENABLED_CONTENT_CHARS: usize = 8_000;
const GLOBAL_MEMORY_MAX_UPDATED_AT_MS: u64 = 8_640_000_000_000_000;

/// Business logic for client preferences (generic key-value store).
#[derive(Clone)]
pub struct ClientPrefService {
    repo: Arc<dyn IClientPreferenceRepository>,
    keep_awake_controller: DynKeepAwakeController,
}

impl ClientPrefService {
    pub fn new(repo: Arc<dyn IClientPreferenceRepository>) -> Self {
        Self {
            repo,
            keep_awake_controller: Arc::new(NoopKeepAwakeController),
        }
    }

    pub fn with_keep_awake_controller(
        repo: Arc<dyn IClientPreferenceRepository>,
        keep_awake_controller: DynKeepAwakeController,
    ) -> Self {
        let service = Self {
            repo,
            keep_awake_controller,
        };
        service.restore_keep_awake_from_preferences();
        service
    }

    /// Get all client preferences, or only the specified keys.
    pub async fn get_preferences(&self, keys: Option<&[&str]>) -> Result<ClientPreferencesResponse, SystemError> {
        let rows = match keys {
            Some(k) if !k.is_empty() => self.repo.get_by_keys(k).await,
            _ => self.repo.get_all().await,
        }
        .map_err(|e| SystemError::Internal(format!("Failed to get preferences: {e}")))?;

        let mut found_keys = BTreeSet::new();
        let mut map = ClientPreferencesResponse::new();
        for row in rows {
            let value: serde_json::Value =
                serde_json::from_str(&row.value).unwrap_or(serde_json::Value::String(row.value));
            found_keys.insert(row.key.clone());
            debug!(
                target: "aionui_feedback_diagnostics",
                diagnostic_event = "feedback.runtime.client_preference_read",
                key = %row.key,
                found = true,
                "feedback.runtime.client_preference_read"
            );
            map.insert(row.key, value);
        }
        if let Some(keys) = keys {
            for key in keys.iter().filter(|key| !found_keys.contains(**key)) {
                debug!(
                    target: "aionui_feedback_diagnostics",
                    diagnostic_event = "feedback.runtime.client_preference_read",
                    key = %key,
                    found = false,
                    "feedback.runtime.client_preference_read"
                );
            }
        }
        Ok(map)
    }

    /// Batch update client preferences. Null values delete the key.
    pub async fn update_preferences(&self, req: UpdateClientPreferencesRequest) -> Result<(), SystemError> {
        let mut upserts: Vec<(String, String)> = Vec::new();
        let mut deletes: Vec<String> = Vec::new();
        let keep_awake_update = resolve_keep_awake_update(&req)?;

        for (key, value) in req {
            validate_key(&key)?;
            validate_preference_value(&key, &value)?;

            if value.is_null() {
                info!(
                    target: "aionui_feedback_diagnostics",
                    diagnostic_event = "feedback.runtime.client_preference_write",
                    key = %key,
                    value_type = %"null",
                    value_bytes = 0,
                    "feedback.runtime.client_preference_write"
                );
                deletes.push(key);
            } else {
                let serialized = serde_json::to_string(&value)
                    .map_err(|e| SystemError::Internal(format!("Failed to serialize value: {e}")))?;
                info!(
                    target: "aionui_feedback_diagnostics",
                    diagnostic_event = "feedback.runtime.client_preference_write",
                    key = %key,
                    value_type = %json_value_type(&value),
                    value_bytes = serialized.len(),
                    "feedback.runtime.client_preference_write"
                );
                upserts.push((key, serialized));
            }
        }

        let previous_keep_awake = if keep_awake_update.is_some() {
            Some(self.get_stored_keep_awake().await?)
        } else {
            None
        };

        if let Some(enabled) = keep_awake_update {
            self.apply_keep_awake(enabled).await?;
        }

        if !upserts.is_empty() {
            let entries: Vec<(&str, &str)> = upserts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
            if let Err(error) = self
                .repo
                .upsert_batch(&entries)
                .await
                .map_err(|e| SystemError::Internal(format!("Failed to upsert preferences: {e}")))
            {
                if let Some(previous) = previous_keep_awake {
                    let _ = self.apply_keep_awake(previous).await;
                }
                return Err(error);
            }
        }

        if !deletes.is_empty() {
            let keys: Vec<&str> = deletes.iter().map(|k| k.as_str()).collect();
            if let Err(error) = self
                .repo
                .delete_keys(&keys)
                .await
                .map_err(|e| SystemError::Internal(format!("Failed to delete preferences: {e}")))
            {
                if let Some(previous) = previous_keep_awake {
                    let _ = self.apply_keep_awake(previous).await;
                }
                return Err(error);
            }
        }

        Ok(())
    }

    async fn get_stored_keep_awake(&self) -> Result<bool, SystemError> {
        let rows = self
            .repo
            .get_by_keys(&[KEEP_AWAKE_KEY])
            .await
            .map_err(|e| SystemError::Internal(format!("Failed to get keep-awake preference: {e}")))?;

        if let Some(row) = rows.iter().find(|row| row.key == KEEP_AWAKE_KEY) {
            let value: serde_json::Value =
                serde_json::from_str(&row.value).unwrap_or(serde_json::Value::String(row.value.clone()));
            match parse_keep_awake_value(&value) {
                Ok(enabled) => return Ok(enabled),
                Err(error) => {
                    warn!(key = KEEP_AWAKE_KEY, error = %error, "Ignoring invalid stored keep-awake preference")
                }
            }
        }
        Ok(false)
    }

    async fn apply_keep_awake(&self, enabled: bool) -> Result<(), SystemError> {
        self.keep_awake_controller.set_enabled(enabled).await.map_err(|error| {
            warn!(enabled, error = %error, "Failed to update system keep-awake assertion");
            error
        })?;
        info!(enabled, "System keep-awake preference applied");
        Ok(())
    }

    fn restore_keep_awake_from_preferences(&self) {
        let service = self.clone();
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            warn!("Cannot restore system keep-awake preference without a Tokio runtime");
            return;
        };
        handle.spawn(async move {
            match service.get_stored_keep_awake().await {
                Ok(true) => {
                    if let Err(error) = service.apply_keep_awake(true).await {
                        warn!(error = %error, "Failed to restore system keep-awake assertion");
                    }
                }
                Ok(false) => {}
                Err(error) => warn!(error = %error, "Failed to read system keep-awake preference for restore"),
            }
        });
    }
}

fn json_value_type(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

fn validate_key(key: &str) -> Result<(), SystemError> {
    if key.is_empty() {
        return Err(SystemError::BadRequest("Preference key must not be empty".into()));
    }
    if key.len() > MAX_KEY_LENGTH {
        return Err(SystemError::BadRequest(format!(
            "Preference key exceeds maximum length of {MAX_KEY_LENGTH} characters"
        )));
    }
    Ok(())
}

fn validate_preference_value(key: &str, value: &serde_json::Value) -> Result<(), SystemError> {
    if key != GLOBAL_MEMORY_KEY || value.is_null() {
        return Ok(());
    }
    let serialized_bytes = serde_json::to_vec(value)
        .map_err(|error| SystemError::BadRequest(format!("Invalid global Memory value: {error}")))?
        .len();
    if serialized_bytes > GLOBAL_MEMORY_MAX_VALUE_BYTES {
        return Err(global_memory_error(format!(
            "value exceeds {GLOBAL_MEMORY_MAX_VALUE_BYTES} bytes"
        )));
    }
    let object = value
        .as_object()
        .ok_or_else(|| global_memory_error("value must be an object"))?;
    if object.len() != 2 || object.keys().any(|key| key != "schema" && key != "entries") {
        return Err(global_memory_error("value contains unsupported fields"));
    }
    if object.get("schema").and_then(serde_json::Value::as_str) != Some(GLOBAL_MEMORY_SCHEMA) {
        return Err(global_memory_error(format!("schema must be {GLOBAL_MEMORY_SCHEMA}")));
    }
    let entries = object
        .get("entries")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| global_memory_error("entries must be an array"))?;
    if entries.len() > GLOBAL_MEMORY_MAX_ENTRIES {
        return Err(global_memory_error(format!(
            "entries exceeds the limit of {GLOBAL_MEMORY_MAX_ENTRIES}"
        )));
    }

    let mut ids = HashSet::new();
    let mut enabled_content_chars = 0usize;
    for (index, entry) in entries.iter().enumerate() {
        let entry = entry
            .as_object()
            .ok_or_else(|| global_memory_error(format!("entries[{index}] must be an object")))?;
        if entry.len() != 5
            || entry
                .keys()
                .any(|key| !["id", "enabled", "title", "content", "updatedAt"].contains(&key.as_str()))
        {
            return Err(global_memory_error(format!(
                "entries[{index}] contains unsupported fields"
            )));
        }
        let id = required_global_memory_text(entry, index, "id", GLOBAL_MEMORY_MAX_ID_CHARS)?;
        if !ids.insert(id) {
            return Err(global_memory_error(format!("entries[{index}].id is duplicated")));
        }
        required_global_memory_text(entry, index, "title", GLOBAL_MEMORY_MAX_TITLE_CHARS)?;
        let content = required_global_memory_text(entry, index, "content", GLOBAL_MEMORY_MAX_CONTENT_CHARS)?;
        let enabled = entry
            .get("enabled")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| global_memory_error(format!("entries[{index}].enabled must be a boolean")))?;
        let updated_at = entry
            .get("updatedAt")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| global_memory_error(format!("entries[{index}].updatedAt must be a non-negative integer")))?;
        if updated_at > GLOBAL_MEMORY_MAX_UPDATED_AT_MS {
            return Err(global_memory_error(format!(
                "entries[{index}].updatedAt exceeds the JavaScript Date range"
            )));
        }
        if enabled {
            enabled_content_chars = enabled_content_chars.saturating_add(content.chars().count());
        }
    }
    if enabled_content_chars > GLOBAL_MEMORY_MAX_ENABLED_CONTENT_CHARS {
        return Err(global_memory_error(format!(
            "enabled content exceeds {GLOBAL_MEMORY_MAX_ENABLED_CONTENT_CHARS} characters"
        )));
    }
    Ok(())
}

fn required_global_memory_text<'a>(
    entry: &'a serde_json::Map<String, serde_json::Value>,
    index: usize,
    field: &str,
    max_chars: usize,
) -> Result<&'a str, SystemError> {
    let value = entry
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| global_memory_error(format!("entries[{index}].{field} must be a non-empty string")))?;
    if value.chars().count() > max_chars {
        return Err(global_memory_error(format!(
            "entries[{index}].{field} exceeds {max_chars} characters"
        )));
    }
    Ok(value)
}

fn global_memory_error(reason: impl std::fmt::Display) -> SystemError {
    SystemError::BadRequest(format!("Invalid {GLOBAL_MEMORY_KEY}: {reason}"))
}

fn resolve_keep_awake_update(req: &UpdateClientPreferencesRequest) -> Result<Option<bool>, SystemError> {
    req.get(KEEP_AWAKE_KEY).map(parse_keep_awake_value).transpose()
}

fn parse_keep_awake_value(value: &serde_json::Value) -> Result<bool, SystemError> {
    match value {
        serde_json::Value::Bool(enabled) => Ok(*enabled),
        serde_json::Value::Null => Ok(false),
        _ => Err(SystemError::BadRequest(format!(
            "{KEEP_AWAKE_KEY} must be a boolean or null"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aionui_db::{SqliteClientPreferenceRepository, init_database_memory};
    use async_trait::async_trait;
    use serde_json::json;
    use std::io::Write;
    use std::sync::{Mutex, Once, OnceLock};
    use tracing::Level;
    use tracing_subscriber::fmt;

    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    static LOG_CAPTURE_INIT: Once = Once::new();
    static LOG_CAPTURE_LOCK: Mutex<()> = Mutex::new(());
    static LOG_CAPTURE_BUFFER: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();

    fn capture_logs(max_level: Level, f: impl FnOnce()) -> String {
        let _capture_guard = LOG_CAPTURE_LOCK.lock().unwrap();
        let buffer = Arc::clone(LOG_CAPTURE_BUFFER.get_or_init(|| Arc::new(Mutex::new(Vec::<u8>::new()))));
        buffer.lock().unwrap().clear();
        let make_writer = {
            let buffer = Arc::clone(&buffer);
            move || SharedBuf(Arc::clone(&buffer))
        };
        LOG_CAPTURE_INIT.call_once(|| {
            let subscriber = fmt::Subscriber::builder()
                .with_max_level(max_level)
                .with_writer(make_writer)
                .with_ansi(false)
                .finish();
            tracing::subscriber::set_global_default(subscriber).expect("set global test subscriber");
        });

        f();
        String::from_utf8(buffer.lock().unwrap().clone()).unwrap()
    }

    async fn setup() -> ClientPrefService {
        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteClientPreferenceRepository::new(db.pool().clone()));
        std::mem::forget(db);
        ClientPrefService::new(repo)
    }

    async fn setup_with_keep_awake_controller(controller: DynKeepAwakeController) -> ClientPrefService {
        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteClientPreferenceRepository::new(db.pool().clone()));
        std::mem::forget(db);
        ClientPrefService {
            repo,
            keep_awake_controller: controller,
        }
    }

    #[derive(Default)]
    struct RecordingKeepAwakeController {
        calls: Mutex<Vec<bool>>,
        fail: bool,
    }

    #[async_trait]
    impl crate::keep_awake::KeepAwakeController for RecordingKeepAwakeController {
        async fn set_enabled(&self, enabled: bool) -> Result<(), SystemError> {
            self.calls.lock().unwrap().push(enabled);
            if self.fail {
                Err(SystemError::Internal("keep-awake failed".into()))
            } else {
                Ok(())
            }
        }
    }

    #[test]
    fn validate_key_accepts_valid() {
        assert!(validate_key("theme").is_ok());
        assert!(validate_key("system.closeToTray").is_ok());
        assert!(validate_key("a").is_ok());
    }

    #[test]
    fn validate_key_rejects_empty() {
        assert!(validate_key("").is_err());
    }

    #[test]
    fn validate_key_rejects_too_long() {
        let long_key = "x".repeat(MAX_KEY_LENGTH + 1);
        assert!(validate_key(&long_key).is_err());
    }

    #[tokio::test]
    async fn get_empty_returns_empty_map() {
        let svc = setup().await;
        let prefs = svc.get_preferences(None).await.unwrap();
        assert!(prefs.is_empty());
    }

    #[tokio::test]
    async fn update_and_get_boolean() {
        let svc = setup().await;
        let mut req = UpdateClientPreferencesRequest::new();
        req.insert("system.closeToTray".into(), json!(true));
        svc.update_preferences(req).await.unwrap();

        let prefs = svc.get_preferences(None).await.unwrap();
        assert_eq!(prefs["system.closeToTray"], json!(true));
    }

    #[tokio::test]
    async fn update_and_get_number() {
        let svc = setup().await;
        let mut req = UpdateClientPreferencesRequest::new();
        req.insert("pet.size".into(), json!(360));
        svc.update_preferences(req).await.unwrap();

        let prefs = svc.get_preferences(None).await.unwrap();
        assert_eq!(prefs["pet.size"], json!(360));
    }

    #[tokio::test]
    async fn update_and_get_string() {
        let svc = setup().await;
        let mut req = UpdateClientPreferencesRequest::new();
        req.insert("theme".into(), json!("dark"));
        svc.update_preferences(req).await.unwrap();

        let prefs = svc.get_preferences(None).await.unwrap();
        assert_eq!(prefs["theme"], json!("dark"));
    }

    #[tokio::test]
    async fn null_deletes_key() {
        let svc = setup().await;

        let mut req = UpdateClientPreferencesRequest::new();
        req.insert("theme".into(), json!("dark"));
        svc.update_preferences(req).await.unwrap();

        let mut req2 = UpdateClientPreferencesRequest::new();
        req2.insert("theme".into(), json!(null));
        svc.update_preferences(req2).await.unwrap();

        let prefs = svc.get_preferences(None).await.unwrap();
        assert!(!prefs.contains_key("theme"));
    }

    #[tokio::test]
    async fn get_by_keys_filters() {
        let svc = setup().await;

        let mut req = UpdateClientPreferencesRequest::new();
        req.insert("a".into(), json!(1));
        req.insert("b".into(), json!(2));
        req.insert("c".into(), json!(3));
        svc.update_preferences(req).await.unwrap();

        let prefs = svc.get_preferences(Some(&["a", "c"])).await.unwrap();
        assert_eq!(prefs.len(), 2);
        assert_eq!(prefs["a"], json!(1));
        assert_eq!(prefs["c"], json!(3));
    }

    #[test]
    fn client_preference_diagnostic_logs_do_not_include_values() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let captured = capture_logs(Level::DEBUG, || {
            runtime.block_on(async {
                let svc = setup().await;
                let mut req = UpdateClientPreferencesRequest::new();
                req.insert("appearance.secretTheme".into(), json!("super-secret-value"));
                req.insert("appearance.deleted".into(), json!(null));
                svc.update_preferences(req).await.unwrap();

                let _ = svc
                    .get_preferences(Some(&["appearance.secretTheme", "appearance.missing"]))
                    .await
                    .unwrap();
            });
        });

        assert!(captured.contains("aionui_feedback_diagnostics"), "{captured}");
        assert!(
            captured.contains("feedback.runtime.client_preference_write"),
            "{captured}"
        );
        assert!(
            captured.contains("feedback.runtime.client_preference_read"),
            "{captured}"
        );
        assert!(captured.contains("key=appearance.secretTheme"), "{captured}");
        assert!(captured.contains("key=appearance.missing"), "{captured}");
        assert!(captured.contains("value_type=string"), "{captured}");
        assert!(captured.contains("value_type=null"), "{captured}");
        assert!(captured.contains("found=true"), "{captured}");
        assert!(captured.contains("found=false"), "{captured}");
        assert!(!captured.contains("super-secret-value"), "{captured}");
    }

    #[tokio::test]
    async fn overwrite_existing_value() {
        let svc = setup().await;

        let mut req1 = UpdateClientPreferencesRequest::new();
        req1.insert("k".into(), json!("v1"));
        svc.update_preferences(req1).await.unwrap();

        let mut req2 = UpdateClientPreferencesRequest::new();
        req2.insert("k".into(), json!("v2"));
        svc.update_preferences(req2).await.unwrap();

        let prefs = svc.get_preferences(None).await.unwrap();
        assert_eq!(prefs["k"], json!("v2"));
    }

    #[tokio::test]
    async fn empty_key_rejected() {
        let svc = setup().await;
        let mut req = UpdateClientPreferencesRequest::new();
        req.insert("".into(), json!(true));
        let err = svc.update_preferences(req).await.unwrap_err();
        assert!(matches!(err, SystemError::BadRequest(_)));
    }

    #[tokio::test]
    async fn long_key_rejected() {
        let svc = setup().await;
        let mut req = UpdateClientPreferencesRequest::new();
        req.insert("x".repeat(256), json!(true));
        let err = svc.update_preferences(req).await.unwrap_err();
        assert!(matches!(err, SystemError::BadRequest(_)));
    }

    #[tokio::test]
    async fn global_memory_accepts_bounded_schema() {
        let svc = setup().await;
        let mut req = UpdateClientPreferencesRequest::new();
        req.insert(
            GLOBAL_MEMORY_KEY.into(),
            json!({
                "schema": GLOBAL_MEMORY_SCHEMA,
                "entries": [{
                    "id": "memory-1",
                    "enabled": true,
                    "title": "Units",
                    "content": "Use SI units.",
                    "updatedAt": 1,
                }],
            }),
        );

        svc.update_preferences(req).await.unwrap();

        let stored = svc.get_preferences(Some(&[GLOBAL_MEMORY_KEY])).await.unwrap();
        assert_eq!(stored[GLOBAL_MEMORY_KEY]["entries"][0]["title"], "Units");
    }

    #[tokio::test]
    async fn global_memory_rejects_invalid_schema_and_oversized_content() {
        let svc = setup().await;
        for value in [
            json!({ "schema": "wrong", "entries": [] }),
            json!({
                "schema": GLOBAL_MEMORY_SCHEMA,
                "entries": [],
                "unexpected": "must not be persisted",
            }),
            json!({
                "schema": GLOBAL_MEMORY_SCHEMA,
                "entries": [{
                    "id": "memory-1",
                    "enabled": true,
                    "title": "Future",
                    "content": "Content",
                    "updatedAt": GLOBAL_MEMORY_MAX_UPDATED_AT_MS + 1,
                }],
            }),
            json!({
                "schema": GLOBAL_MEMORY_SCHEMA,
                "entries": [{
                    "id": "memory-1",
                    "enabled": true,
                    "title": "Too long",
                    "content": "x".repeat(GLOBAL_MEMORY_MAX_CONTENT_CHARS + 1),
                    "updatedAt": 1,
                }],
            }),
        ] {
            let mut req = UpdateClientPreferencesRequest::new();
            req.insert(GLOBAL_MEMORY_KEY.into(), value);
            assert!(matches!(
                svc.update_preferences(req).await.unwrap_err(),
                SystemError::BadRequest(_)
            ));
        }
    }

    #[tokio::test]
    async fn global_memory_rejects_duplicate_ids_and_enabled_total_overflow() {
        let svc = setup().await;
        let base = |id: &str, content: String| {
            json!({
                "id": id,
                "enabled": true,
                "title": "Memory",
                "content": content,
                "updatedAt": 1,
            })
        };
        for entries in [
            vec![base("same", "a".into()), base("same", "b".into())],
            (0..5)
                .map(|index| base(&format!("memory-{index}"), "x".repeat(GLOBAL_MEMORY_MAX_CONTENT_CHARS)))
                .collect(),
        ] {
            let mut req = UpdateClientPreferencesRequest::new();
            req.insert(
                GLOBAL_MEMORY_KEY.into(),
                json!({ "schema": GLOBAL_MEMORY_SCHEMA, "entries": entries }),
            );
            assert!(matches!(
                svc.update_preferences(req).await.unwrap_err(),
                SystemError::BadRequest(_)
            ));
        }
    }

    #[tokio::test]
    async fn batch_mixed_upsert_and_delete() {
        let svc = setup().await;

        let mut setup_req = UpdateClientPreferencesRequest::new();
        setup_req.insert("keep".into(), json!(1));
        setup_req.insert("remove".into(), json!(2));
        svc.update_preferences(setup_req).await.unwrap();

        let mut req = UpdateClientPreferencesRequest::new();
        req.insert("remove".into(), json!(null));
        req.insert("new".into(), json!(3));
        svc.update_preferences(req).await.unwrap();

        let prefs = svc.get_preferences(None).await.unwrap();
        assert_eq!(prefs.len(), 2);
        assert_eq!(prefs["keep"], json!(1));
        assert_eq!(prefs["new"], json!(3));
    }

    #[tokio::test]
    async fn keep_awake_true_enables_controller_and_persists_value() {
        let controller = Arc::new(RecordingKeepAwakeController::default());
        let svc = setup_with_keep_awake_controller(controller.clone()).await;
        let mut req = UpdateClientPreferencesRequest::new();
        req.insert(KEEP_AWAKE_KEY.into(), json!(true));

        svc.update_preferences(req).await.unwrap();

        assert_eq!(*controller.calls.lock().unwrap(), vec![true]);
        let prefs = svc.get_preferences(Some(&[KEEP_AWAKE_KEY])).await.unwrap();
        assert_eq!(prefs[KEEP_AWAKE_KEY], json!(true));
    }

    #[tokio::test]
    async fn keep_awake_null_disables_controller_and_deletes_value() {
        let controller = Arc::new(RecordingKeepAwakeController::default());
        let svc = setup_with_keep_awake_controller(controller.clone()).await;
        let mut setup_req = UpdateClientPreferencesRequest::new();
        setup_req.insert(KEEP_AWAKE_KEY.into(), json!(true));
        svc.update_preferences(setup_req).await.unwrap();

        let mut req = UpdateClientPreferencesRequest::new();
        req.insert(KEEP_AWAKE_KEY.into(), json!(null));
        svc.update_preferences(req).await.unwrap();

        assert_eq!(*controller.calls.lock().unwrap(), vec![true, false]);
        let prefs = svc.get_preferences(Some(&[KEEP_AWAKE_KEY])).await.unwrap();
        assert!(!prefs.contains_key(KEEP_AWAKE_KEY));
    }

    #[tokio::test]
    async fn keep_awake_rejects_non_boolean_values() {
        let controller = Arc::new(RecordingKeepAwakeController::default());
        let svc = setup_with_keep_awake_controller(controller.clone()).await;
        let mut req = UpdateClientPreferencesRequest::new();
        req.insert(KEEP_AWAKE_KEY.into(), json!("yes"));

        let err = svc.update_preferences(req).await.unwrap_err();

        assert!(matches!(err, SystemError::BadRequest(_)));
        assert!(controller.calls.lock().unwrap().is_empty());
        let prefs = svc.get_preferences(Some(&[KEEP_AWAKE_KEY])).await.unwrap();
        assert!(!prefs.contains_key(KEEP_AWAKE_KEY));
    }

    #[tokio::test]
    async fn keep_awake_controller_failure_does_not_persist_value() {
        let controller = Arc::new(RecordingKeepAwakeController {
            fail: true,
            ..Default::default()
        });
        let svc = setup_with_keep_awake_controller(controller.clone()).await;
        let mut req = UpdateClientPreferencesRequest::new();
        req.insert(KEEP_AWAKE_KEY.into(), json!(true));

        let err = svc.update_preferences(req).await.unwrap_err();

        assert!(matches!(err, SystemError::Internal(_)));
        assert_eq!(*controller.calls.lock().unwrap(), vec![true]);
        let prefs = svc.get_preferences(Some(&[KEEP_AWAKE_KEY])).await.unwrap();
        assert!(!prefs.contains_key(KEEP_AWAKE_KEY));
    }

    #[tokio::test]
    async fn keep_awake_restore_applies_persisted_value() {
        let initial = setup().await;
        let mut req = UpdateClientPreferencesRequest::new();
        req.insert(KEEP_AWAKE_KEY.into(), json!(true));
        initial.update_preferences(req).await.unwrap();

        let controller = Arc::new(RecordingKeepAwakeController::default());
        let service = ClientPrefService::with_keep_awake_controller(initial.repo.clone(), controller.clone());

        for _ in 0..50 {
            if !controller.calls.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        assert_eq!(*controller.calls.lock().unwrap(), vec![true]);
        drop(service);
    }
}
