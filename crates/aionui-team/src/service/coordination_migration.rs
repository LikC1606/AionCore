use std::collections::HashMap;

use aionui_api_types::{
    TeamCoordinationMigrationRequest, TeamCoordinationMigrationResponse, TeamCoordinationMigrationSnapshot,
    TeamCoordinationProtocol, TeamManagedTool,
};
use aionui_common::now_ms;
use aionui_db::{DbError, TeamCoordinationMigrationRow, models::TeamRow};
use ed25519_dalek::{Signature, VerifyingKey};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tracing::info;

use super::TeamSessionService;
use crate::{Team, TeamError};

const SCHEMA: &str = "aionui.team.coordination-migration.v1";
const PROFILE: &str = "mathematics-research-team";
const LEGACY: &str = r#"{"kind":"legacy_managed_migration_required"}"#;
const SIGNING_DOMAIN: &[u8] = b"aionui.team.coordination-migration-proof.v1\0";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustedKeyInput {
    user_id: String,
    key_id: String,
    public_key: String,
}

pub(super) struct MigrationTrust(HashMap<(String, String), VerifyingKey>);

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:".contains(&byte))
}

fn decode_hex<const N: usize>(value: &str) -> Option<[u8; N]> {
    if value.len() != N * 2
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut bytes = [0; N];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

fn invalid() -> TeamError {
    TeamError::InvalidRequest("TEAM_COORDINATION_MIGRATION_INVALID".into())
}

fn signing_payload(request: &TeamCoordinationMigrationRequest) -> Result<Vec<u8>, TeamError> {
    let mut payload = SIGNING_DOMAIN.to_vec();
    // An ordered array of ASCII identities avoids cross-language object-key
    // ordering differences. The trusted key is scoped to the signed user.
    payload.extend(serde_json::to_vec(&[
        request.schema.as_str(),
        request.migration_id.as_str(),
        request.team_id.as_str(),
        request.user_id.as_str(),
        request.profile_id.as_str(),
        request.snapshot_digest.as_str(),
        request.source_binding_digest.as_str(),
        request.key_id.as_str(),
    ])?);
    Ok(payload)
}

fn snapshot_digest(row: &TeamRow) -> Result<String, TeamError> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(row)?)))
}

fn managed() -> TeamCoordinationProtocol {
    TeamCoordinationProtocol::ManagedMcp {
        logical_tool: TeamManagedTool::ResearchTeam,
    }
}

impl TeamSessionService {
    /// App composition installs public trust roots once. No request can enroll
    /// keys, and an unconfigured service rejects every migration.
    pub fn configure_coordination_migration_keys(&self, json: &str) -> Result<(), TeamError> {
        if json.len() > 32_768 {
            return Err(invalid());
        }
        let entries: Vec<TrustedKeyInput> = serde_json::from_str(json).map_err(|_| invalid())?;
        if entries.is_empty() || entries.len() > 32 {
            return Err(invalid());
        }
        let mut keys = HashMap::new();
        for entry in entries {
            if !valid_id(&entry.user_id) || !valid_id(&entry.key_id) {
                return Err(invalid());
            }
            let key = VerifyingKey::from_bytes(&decode_hex::<32>(&entry.public_key).ok_or_else(invalid)?)
                .map_err(|_| invalid())?;
            if key.is_weak() || keys.insert((entry.user_id, entry.key_id), key).is_some() {
                return Err(invalid());
            }
        }
        self.coordination_migration_keys
            .set(MigrationTrust(keys))
            .map_err(|_| TeamError::InvalidRequest("TEAM_COORDINATION_MIGRATION_KEYS_ALREADY_CONFIGURED".into()))
    }

    async fn owned_migration_row(&self, user_id: &str, team_id: &str) -> Result<TeamRow, TeamError> {
        let row = self
            .repo
            .get_team(team_id)
            .await?
            .ok_or_else(|| TeamError::TeamNotFound(team_id.into()))?;
        if row.user_id != user_id {
            return Err(TeamError::Forbidden("TEAM_COORDINATION_MIGRATION_FORBIDDEN".into()));
        }
        Ok(row)
    }

    pub async fn coordination_migration_snapshot(
        &self,
        user_id: &str,
        team_id: &str,
    ) -> Result<TeamCoordinationMigrationSnapshot, TeamError> {
        let row = self.owned_migration_row(user_id, team_id).await?;
        if row.coordination_protocol.as_deref() != Some(LEGACY) {
            return Err(TeamError::InvalidRequest(
                "TEAM_COORDINATION_MIGRATION_NOT_LEGACY_MANAGED".into(),
            ));
        }
        Ok(TeamCoordinationMigrationSnapshot {
            schema: "aionui.team.coordination-migration-snapshot.v1",
            team_id: row.id.clone(),
            user_id: row.user_id.clone(),
            profile_id: PROFILE,
            snapshot_digest: snapshot_digest(&row)?,
        })
    }

    pub async fn migrate_coordination_protocol(
        &self,
        user_id: &str,
        team_id: &str,
        request: TeamCoordinationMigrationRequest,
    ) -> Result<TeamCoordinationMigrationResponse, TeamError> {
        self.owned_migration_row(user_id, team_id).await?;
        if request.schema != SCHEMA
            || request.profile_id != PROFILE
            || [
                &request.migration_id,
                &request.team_id,
                &request.user_id,
                &request.key_id,
            ]
            .iter()
            .any(|value| !valid_id(value))
            || decode_hex::<32>(&request.snapshot_digest).is_none()
            || decode_hex::<32>(&request.source_binding_digest).is_none()
        {
            return Err(invalid());
        }
        if request.user_id != user_id || request.team_id != team_id {
            return Err(TeamError::Forbidden("TEAM_COORDINATION_MIGRATION_FORBIDDEN".into()));
        }
        let key = self
            .coordination_migration_keys
            .get()
            .and_then(|trust| trust.0.get(&(request.user_id.clone(), request.key_id.clone())))
            .ok_or_else(|| TeamError::Forbidden("TEAM_COORDINATION_MIGRATION_UNTRUSTED_KEY".into()))?;
        let signature = Signature::from_bytes(&decode_hex::<64>(&request.signature).ok_or_else(invalid)?);
        key.verify_strict(&signing_payload(&request)?, &signature)
            .map_err(|_| TeamError::Forbidden("TEAM_COORDINATION_MIGRATION_INVALID_SIGNATURE".into()))?;
        let proof_json = serde_json::to_string(&request)?;
        let proof_digest = format!("{:x}", Sha256::digest(proof_json.as_bytes()));

        // Use the same lifecycle gate as startup and removal. A migration may
        // change durable authority only before a session exists, never mid-turn.
        let lifecycle_lock = self.lifecycle_lock(team_id);
        let _guard = lifecycle_lock.write().await;
        let row = self.owned_migration_row(user_id, team_id).await?;
        let previous = self.repo.get_coordination_migration(team_id).await?;
        if previous.is_none() {
            if self.sessions.contains_key(team_id) {
                return Err(TeamError::InvalidRequest(
                    "TEAM_COORDINATION_MIGRATION_SESSION_ACTIVE".into(),
                ));
            }
            if row.coordination_protocol.as_deref() != Some(LEGACY) || snapshot_digest(&row)? != request.snapshot_digest
            {
                return Err(DbError::Conflict("TEAM_COORDINATION_MIGRATION_STALE_SNAPSHOT".into()).into());
            }
            let mut prospective = row.clone();
            prospective.coordination_protocol = Some(serde_json::to_string(&managed())?);
            Team::from_row(&prospective)?;
        }
        let response = TeamCoordinationMigrationResponse {
            schema: SCHEMA.into(),
            migration_id: request.migration_id.clone(),
            team_id: team_id.into(),
            snapshot_digest: request.snapshot_digest.clone(),
            coordination_protocol: managed(),
            applied_at: now_ms(),
        };
        let audit = self
            .repo
            .migrate_coordination_protocol(
                &row,
                &TeamCoordinationMigrationRow {
                    team_id: team_id.into(),
                    user_id: user_id.into(),
                    migration_id: request.migration_id.clone(),
                    proof_digest,
                    proof_json,
                    receipt_json: serde_json::to_string(&response)?,
                    applied_at: response.applied_at,
                },
            )
            .await?;
        let receipt: TeamCoordinationMigrationResponse = serde_json::from_str(&audit.receipt_json)?;
        if receipt.schema != SCHEMA
            || receipt.team_id != team_id
            || receipt.migration_id != request.migration_id
            || receipt.snapshot_digest != request.snapshot_digest
            || receipt.coordination_protocol != managed()
            || receipt.applied_at != audit.applied_at
        {
            return Err(TeamError::InvalidRequest(
                "TEAM_COORDINATION_MIGRATION_CORRUPT_AUDIT".into(),
            ));
        }
        info!(
            team_id,
            migration_id = request.migration_id,
            replay = previous.is_some(),
            "Team coordination migration committed"
        );
        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::workspace_harness::setup_with_factory_metadata_team_repo_and_conversation_repo;
    use aionui_db::{ITeamRepository, SqliteTeamRepository, UpdateTeamParams, init_database_memory};
    use ed25519_dalek::{Signer, SigningKey};
    use serde_json::json;
    use std::sync::Arc;

    fn service(repo: Arc<dyn ITeamRepository>) -> Arc<TeamSessionService> {
        let (template, _, _, _) = setup_with_factory_metadata_team_repo_and_conversation_repo();
        TeamSessionService::new(
            repo,
            template.agent_metadata_repo.clone(),
            template.assistant_catalog.clone(),
            template.assistant_definition_repo.clone(),
            template.assistant_overlay_repo.clone(),
            template.provider_repo.clone(),
            template.conversation_port.clone(),
            template.projection_store.clone(),
            template.broadcaster.clone(),
            template.task_manager.clone(),
            template.turn_port.clone(),
            template.cancellation_port.clone(),
            template.backend_binary_path.clone(),
        )
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn keys(key: &SigningKey) -> String {
        json!([{"user_id":"system_default_user", "key_id":"operator-1", "public_key":hex(key.verifying_key().as_bytes())}]).to_string()
    }

    async fn setup() -> (
        aionui_db::Database,
        Arc<SqliteTeamRepository>,
        Arc<TeamSessionService>,
        SigningKey,
    ) {
        let db = init_database_memory().await.unwrap();
        let repo = Arc::new(SqliteTeamRepository::new(db.pool().clone()));
        repo.create_team(&TeamRow {
            coordination_protocol: Some(LEGACY.into()),
            id: "team-migrate".into(),
            user_id: "system_default_user".into(),
            name: "Retained team".into(),
            workspace: "/retained/workspace".into(),
            workspace_mode: "shared".into(),
            agents: "[]".into(),
            lead_agent_id: None,
            session_mode: None,
            agents_version: "1.0.1".into(),
            created_at: 1,
            updated_at: 1,
        })
        .await
        .unwrap();
        let service = service(repo.clone());
        let key = SigningKey::from_bytes(&[7; 32]);
        service.configure_coordination_migration_keys(&keys(&key)).unwrap();
        (db, repo, service, key)
    }

    async fn proof(service: &TeamSessionService, key: &SigningKey) -> TeamCoordinationMigrationRequest {
        let snapshot = service
            .coordination_migration_snapshot("system_default_user", "team-migrate")
            .await
            .unwrap();
        let mut request = TeamCoordinationMigrationRequest {
            schema: SCHEMA.into(),
            migration_id: "migration-1".into(),
            team_id: snapshot.team_id,
            user_id: snapshot.user_id,
            profile_id: PROFILE.into(),
            snapshot_digest: snapshot.snapshot_digest,
            source_binding_digest: "a".repeat(64),
            key_id: "operator-1".into(),
            signature: String::new(),
        };
        request.signature = hex(&key.sign(&signing_payload(&request).unwrap()).to_bytes());
        request
    }

    #[tokio::test]
    async fn signed_migration_and_service_restart_replay_preserve_one_durable_receipt() {
        let (_db, repo, first, key) = setup().await;
        let request = proof(&first, &key).await;
        let receipt = first
            .migrate_coordination_protocol("system_default_user", "team-migrate", request.clone())
            .await
            .unwrap();
        drop(first);
        let second = service(repo.clone());
        second.configure_coordination_migration_keys(&keys(&key)).unwrap();
        let replay = second
            .migrate_coordination_protocol("system_default_user", "team-migrate", request)
            .await
            .unwrap();
        assert_eq!(receipt, replay);
        let row = repo.get_team("team-migrate").await.unwrap().unwrap();
        assert_eq!(Team::from_row(&row).unwrap().coordination_protocol, managed());
        assert_eq!(row.workspace, "/retained/workspace");
    }

    #[tokio::test]
    async fn modified_signed_fields_and_unknown_keys_cannot_migrate() {
        let (_db, repo, service, key) = setup().await;
        let original = proof(&service, &key).await;
        let mut altered = original.clone();
        altered.source_binding_digest = "b".repeat(64);
        let error = service
            .migrate_coordination_protocol("system_default_user", "team-migrate", altered)
            .await
            .unwrap_err();
        assert!(
            matches!(error, TeamError::Forbidden(message) if message == "TEAM_COORDINATION_MIGRATION_INVALID_SIGNATURE")
        );
        let mut unknown = original;
        unknown.key_id = "untrusted".into();
        let error = service
            .migrate_coordination_protocol("system_default_user", "team-migrate", unknown)
            .await
            .unwrap_err();
        assert!(
            matches!(error, TeamError::Forbidden(message) if message == "TEAM_COORDINATION_MIGRATION_UNTRUSTED_KEY")
        );
        assert!(repo.get_coordination_migration("team-migrate").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn missing_trust_and_cross_user_access_fail_closed() {
        let (_db, repo, configured, key) = setup().await;
        let request = proof(&configured, &key).await;
        let unconfigured = service(repo);
        let error = unconfigured
            .migrate_coordination_protocol("system_default_user", "team-migrate", request.clone())
            .await
            .unwrap_err();
        assert!(
            matches!(error, TeamError::Forbidden(message) if message == "TEAM_COORDINATION_MIGRATION_UNTRUSTED_KEY")
        );
        let error = configured
            .migrate_coordination_protocol("other-user", "team-migrate", request)
            .await
            .unwrap_err();
        assert!(matches!(error, TeamError::Forbidden(message) if message == "TEAM_COORDINATION_MIGRATION_FORBIDDEN"));
        assert!(matches!(
            configured
                .coordination_migration_snapshot("other-user", "team-migrate")
                .await,
            Err(TeamError::Forbidden(_))
        ));
    }

    #[tokio::test]
    async fn changed_snapshot_cannot_be_authorized_by_an_old_signature() {
        let (_db, repo, service, key) = setup().await;
        let request = proof(&service, &key).await;
        repo.update_team(
            "team-migrate",
            &UpdateTeamParams {
                workspace: Some("/other/workspace".into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let error = service
            .migrate_coordination_protocol("system_default_user", "team-migrate", request)
            .await
            .unwrap_err();
        assert!(
            matches!(error, TeamError::Database(DbError::Conflict(message)) if message == "TEAM_COORDINATION_MIGRATION_STALE_SNAPSHOT")
        );
        assert!(repo.get_coordination_migration("team-migrate").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn different_signed_request_cannot_replace_a_committed_migration() {
        let (_db, repo, service, key) = setup().await;
        let request = proof(&service, &key).await;
        service
            .migrate_coordination_protocol("system_default_user", "team-migrate", request.clone())
            .await
            .unwrap();
        let mut changed = request;
        changed.migration_id = "migration-2".into();
        changed.signature = hex(&key.sign(&signing_payload(&changed).unwrap()).to_bytes());
        let error = service
            .migrate_coordination_protocol("system_default_user", "team-migrate", changed)
            .await
            .unwrap_err();
        assert!(
            matches!(error, TeamError::Database(DbError::Conflict(message)) if message == "Team coordination migration replay conflict")
        );
        assert_eq!(
            repo.get_coordination_migration("team-migrate")
                .await
                .unwrap()
                .unwrap()
                .migration_id,
            "migration-1"
        );
    }

    #[tokio::test]
    async fn snapshot_is_read_only_and_unknown_schema_is_rejected() {
        let (_db, repo, service, key) = setup().await;
        let before = serde_json::to_value(repo.get_team("team-migrate").await.unwrap()).unwrap();
        let mut request = proof(&service, &key).await;
        request.schema = "future-schema".into();
        let error = service
            .migrate_coordination_protocol("system_default_user", "team-migrate", request)
            .await
            .unwrap_err();
        assert!(
            matches!(error, TeamError::InvalidRequest(message) if message == "TEAM_COORDINATION_MIGRATION_INVALID")
        );
        assert_eq!(
            before,
            serde_json::to_value(repo.get_team("team-migrate").await.unwrap()).unwrap()
        );
        assert!(repo.get_coordination_migration("team-migrate").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn trust_configuration_is_immutable_and_rejects_duplicate_or_unknown_fields() {
        let (_db, repo, configured, key) = setup().await;
        assert!(configured.configure_coordination_migration_keys(&keys(&key)).is_err());
        let fresh = service(repo);
        let entry =
            json!({"user_id":"system_default_user", "key_id":"k", "public_key":hex(key.verifying_key().as_bytes())});
        assert!(
            fresh
                .configure_coordination_migration_keys(&json!([entry, entry]).to_string())
                .is_err()
        );
        let error = fresh
            .configure_coordination_migration_keys(
                r#"[{"user_id":"u","key_id":"k","public_key":"00","private_key":"never accepted"}]"#,
            )
            .unwrap_err();
        assert!(!error.to_string().contains("never accepted"));
    }
}
