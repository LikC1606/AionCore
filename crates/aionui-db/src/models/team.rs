use aionui_common::TimestampMs;
use serde::{Deserialize, Serialize};

/// Row mapping for the `teams` table.
///
/// The `agents` column stores a JSON array of `TeamAgent` objects.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TeamRow {
    /// Immutable JSON protocol. NULL denotes a legacy ordinary Team.
    #[serde(default)]
    pub coordination_protocol: Option<String>,
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub workspace: String,
    pub workspace_mode: String,
    /// JSON array: serialized `TeamAgent[]`.
    pub agents: String,
    pub lead_agent_id: Option<String>,
    pub session_mode: Option<String>,
    pub agents_version: String,
    pub created_at: TimestampMs,
    pub updated_at: TimestampMs,
}

/// Row mapping for the `mailbox` table.
///
/// Represents an inter-agent message within a team.
/// The `read` column tracks whether the message has been consumed.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct MailboxMessageRow {
    pub id: String,
    pub team_id: String,
    pub to_agent_id: String,
    pub from_agent_id: String,
    /// Message type: 'message', 'idle_notification', or 'shutdown_request'.
    #[sqlx(rename = "type")]
    pub msg_type: String,
    pub content: String,
    pub summary: Option<String>,
    /// JSON-serialized file paths attached to the message.
    pub files: Option<String>,
    pub read: bool,
    pub created_at: TimestampMs,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn team_row_default_agents_is_empty_json_array() {
        let row = TeamRow {
            coordination_protocol: None,
            id: "t1".into(),
            user_id: "system_default_user".into(),
            name: "Team".into(),
            workspace: "/tmp/ws".into(),
            workspace_mode: "shared".into(),
            agents: "[]".into(),
            lead_agent_id: None,
            session_mode: None,
            agents_version: "1.0.1".into(),
            created_at: 0,
            updated_at: 0,
        };
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&row.agents).expect("agents should be valid JSON");
        assert!(parsed.is_empty());
    }

    #[test]
    fn mailbox_row_msg_type_field_maps_correctly() {
        let row = MailboxMessageRow {
            id: "m1".into(),
            team_id: "t1".into(),
            to_agent_id: "a1".into(),
            from_agent_id: "a2".into(),
            msg_type: "message".into(),
            content: "hello".into(),
            summary: None,
            files: None,
            read: false,
            created_at: 0,
        };
        assert_eq!(row.msg_type, "message");
    }
}
