use aionui_common::TimestampMs;
use serde::{Deserialize, Serialize};

use crate::TeamCoordinationProtocol;

/// Signed approval of one exact legacy Team snapshot. Never a model tool input.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TeamCoordinationMigrationRequest {
    pub schema: String,
    pub migration_id: String,
    pub team_id: String,
    pub user_id: String,
    pub profile_id: String,
    pub snapshot_digest: String,
    pub source_binding_digest: String,
    pub key_id: String,
    pub signature: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TeamCoordinationMigrationResponse {
    pub schema: String,
    pub migration_id: String,
    pub team_id: String,
    pub snapshot_digest: String,
    pub coordination_protocol: TeamCoordinationProtocol,
    pub applied_at: TimestampMs,
}

#[derive(Debug, Serialize)]
pub struct TeamCoordinationMigrationSnapshot {
    pub schema: &'static str,
    pub team_id: String,
    pub user_id: String,
    pub profile_id: &'static str,
    pub snapshot_digest: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn migration_request_rejects_unknown_fields_missing_signature_and_wrong_types() {
        let valid = json!({
            "schema":"aionui.team.coordination-migration.v1", "migration_id":"m1", "team_id":"t1", "user_id":"u1",
            "profile_id":"mathematics-research-team", "snapshot_digest":"a".repeat(64),
            "source_binding_digest":"b".repeat(64), "key_id":"k1", "signature":"c".repeat(128),
        });
        assert!(serde_json::from_value::<TeamCoordinationMigrationRequest>(valid.clone()).is_ok());
        for (field, value) in [
            ("public_key", json!("untrusted")),
            ("signature", json!(null)),
            ("team_id", json!(1)),
        ] {
            let mut malformed = valid.clone();
            malformed[field] = value;
            assert!(serde_json::from_value::<TeamCoordinationMigrationRequest>(malformed).is_err());
        }
        let mut missing = valid;
        missing.as_object_mut().unwrap().remove("signature");
        assert!(serde_json::from_value::<TeamCoordinationMigrationRequest>(missing).is_err());
    }
}
