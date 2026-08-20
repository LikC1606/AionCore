use std::sync::Arc;

use aionui_common::{generate_id, now_ms};
use aionui_db::models::MailboxMessageRow;
use aionui_db::{ITeamRepository, MailboxIdempotencyParams, MailboxWriteResult};
use sha2::{Digest, Sha256};
use tracing::debug;

use crate::error::TeamError;
use crate::types::{MailboxMessage, MailboxMessageType};

pub struct Mailbox {
    repo: Arc<dyn ITeamRepository>,
}

pub(crate) struct MailboxWrite {
    pub(crate) message: MailboxMessage,
    pub(crate) replayed: bool,
}

pub(crate) fn user_message_fingerprint(content: &str, files: Option<&[String]>) -> String {
    let encoded = serde_json::to_vec(&(1_u8, content, files)).expect("Team message fingerprint input is serializable");
    format!("{:x}", Sha256::digest(encoded))
}

impl Mailbox {
    pub fn new(repo: Arc<dyn ITeamRepository>) -> Self {
        Self { repo }
    }

    pub async fn write(
        &self,
        team_id: &str,
        to_agent_id: &str,
        from_agent_id: &str,
        msg_type: MailboxMessageType,
        content: &str,
        summary: Option<&str>,
    ) -> Result<MailboxMessage, TeamError> {
        self.write_with_files(team_id, to_agent_id, from_agent_id, msg_type, content, summary, None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write_with_files(
        &self,
        team_id: &str,
        to_agent_id: &str,
        from_agent_id: &str,
        msg_type: MailboxMessageType,
        content: &str,
        summary: Option<&str>,
        files: Option<&[String]>,
    ) -> Result<MailboxMessage, TeamError> {
        Ok(self
            .write_with_files_inner(
                team_id,
                to_agent_id,
                from_agent_id,
                msg_type,
                content,
                summary,
                files,
                None,
            )
            .await?
            .message)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn write_with_files_idempotent(
        &self,
        team_id: &str,
        to_agent_id: &str,
        from_agent_id: &str,
        msg_type: MailboxMessageType,
        content: &str,
        summary: Option<&str>,
        files: Option<&[String]>,
        idempotency_scope: &str,
        idempotency_key: &str,
    ) -> Result<MailboxWrite, TeamError> {
        let request_fingerprint = user_message_fingerprint(content, files);
        self.write_with_files_inner(
            team_id,
            to_agent_id,
            from_agent_id,
            msg_type,
            content,
            summary,
            files,
            Some(MailboxIdempotencyParams {
                scope: idempotency_scope,
                key: idempotency_key,
                request_fingerprint: &request_fingerprint,
            }),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn write_with_files_inner(
        &self,
        team_id: &str,
        to_agent_id: &str,
        from_agent_id: &str,
        msg_type: MailboxMessageType,
        content: &str,
        summary: Option<&str>,
        files: Option<&[String]>,
        idempotency: Option<MailboxIdempotencyParams<'_>>,
    ) -> Result<MailboxWrite, TeamError> {
        let files_json = files
            .filter(|f| !f.is_empty())
            .map(|f| serde_json::to_string(f).unwrap_or_default());
        let row = MailboxMessageRow {
            id: generate_id(),
            team_id: team_id.to_owned(),
            to_agent_id: to_agent_id.to_owned(),
            from_agent_id: from_agent_id.to_owned(),
            msg_type: msg_type.to_string(),
            content: content.to_owned(),
            summary: summary.map(str::to_owned),
            files: files_json,
            read: false,
            created_at: now_ms(),
        };

        let (row, replayed) = match idempotency.as_ref() {
            Some(idempotency) => match self.repo.write_message_idempotent(&row, idempotency).await? {
                MailboxWriteResult::Inserted => (row, false),
                MailboxWriteResult::Existing(existing) => (existing, true),
                MailboxWriteResult::IdempotencyConflict { .. } => {
                    return Err(TeamError::InvalidRequest(
                        "idempotency_key was already used for a different Team message".into(),
                    ));
                }
            },
            None => {
                self.repo.write_message(&row).await?;
                (row, false)
            }
        };

        debug!(
            team_id,
            to = to_agent_id,
            from = from_agent_id,
            msg_type = %msg_type,
            replayed,
            "mailbox message written"
        );

        let message = MailboxMessage::from_row(&row)
            .ok_or_else(|| TeamError::InvalidRequest(format!("invalid message type: {msg_type}")))?;
        Ok(MailboxWrite { message, replayed })
    }

    pub async fn read_unread(&self, team_id: &str, agent_id: &str) -> Result<Vec<MailboxMessage>, TeamError> {
        let rows = self.repo.read_unread_and_mark(team_id, agent_id).await?;

        debug!(team_id, agent_id, count = rows.len(), "mailbox unread messages read");

        let messages = rows.iter().filter_map(MailboxMessage::from_row).collect();
        Ok(messages)
    }

    /// Reads all unread messages without marking them as read.
    /// Used by the drain_mailbox pattern: peek → prompt → mark_read on success.
    pub async fn peek_unread(&self, team_id: &str, agent_id: &str) -> Result<Vec<MailboxMessage>, TeamError> {
        let rows = self.repo.peek_unread(team_id, agent_id).await?;
        debug!(team_id, agent_id, count = rows.len(), "mailbox peek_unread");
        let messages = rows.iter().filter_map(MailboxMessage::from_row).collect();
        Ok(messages)
    }

    /// Marks the given message IDs as read. Called after successful prompt delivery.
    pub async fn mark_read_batch(&self, ids: &[String]) -> Result<(), TeamError> {
        self.repo.mark_read_batch(ids).await?;
        Ok(())
    }

    /// Runtime-only cancel helper: mark currently unread mailbox rows as read
    /// for the provided agents so a cancelled run does not consume them later.
    pub async fn mark_all_unread_for_agents_read(
        &self,
        team_id: &str,
        agent_ids: &[String],
    ) -> Result<usize, TeamError> {
        let mut ids = Vec::new();
        for agent_id in agent_ids {
            let unread = self.peek_unread(team_id, agent_id).await?;
            ids.extend(unread.into_iter().map(|message| message.id));
        }
        let count = ids.len();
        if !ids.is_empty() {
            self.mark_read_batch(&ids).await?;
        }
        Ok(count)
    }

    pub async fn get_history(
        &self,
        team_id: &str,
        agent_id: &str,
        limit: Option<i64>,
    ) -> Result<Vec<MailboxMessage>, TeamError> {
        let rows = self.repo.get_history(team_id, agent_id, limit).await?;
        let messages = rows.iter().filter_map(MailboxMessage::from_row).collect();
        Ok(messages)
    }

    pub async fn has_unread(&self, team_id: &str, agent_id: &str) -> Result<bool, TeamError> {
        let rows = self.repo.get_history(team_id, agent_id, None).await?;
        Ok(rows.iter().any(|r| !r.read))
    }

    pub async fn delete_by_team(&self, team_id: &str) -> Result<(), TeamError> {
        self.repo.delete_mailbox_by_team(team_id).await?;
        debug!(team_id, "mailbox messages deleted for team");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::MockTeamRepo;

    // -- Tests ----------------------------------------------------------------

    #[tokio::test]
    async fn write_and_read_unread() {
        let repo = Arc::new(MockTeamRepo::new());
        let mailbox = Mailbox::new(repo);

        mailbox
            .write("t1", "a1", "user", MailboxMessageType::Message, "hi", None)
            .await
            .unwrap();
        mailbox
            .write("t1", "a1", "a2", MailboxMessageType::Message, "hello", None)
            .await
            .unwrap();

        let unread = mailbox.read_unread("t1", "a1").await.unwrap();
        assert_eq!(unread.len(), 2);
        assert_eq!(unread[0].content, "hi");
        assert_eq!(unread[1].content, "hello");

        let unread_again = mailbox.read_unread("t1", "a1").await.unwrap();
        assert!(unread_again.is_empty());
    }

    #[tokio::test]
    async fn write_idle_notification_with_summary() {
        let repo = Arc::new(MockTeamRepo::new());
        let mailbox = Mailbox::new(repo);

        let msg = mailbox
            .write(
                "t1",
                "lead",
                "a1",
                MailboxMessageType::IdleNotification,
                "done",
                Some("Task complete"),
            )
            .await
            .unwrap();

        assert_eq!(msg.msg_type, MailboxMessageType::IdleNotification);
        assert_eq!(msg.summary.as_deref(), Some("Task complete"));
    }

    #[tokio::test]
    async fn idempotent_write_reuses_durable_message_and_conflicts_on_changed_payload() {
        let repo = Arc::new(MockTeamRepo::new());
        let first_mailbox = Mailbox::new(repo.clone());
        let first = first_mailbox
            .write_with_files_idempotent(
                "t1",
                "a1",
                "user",
                MailboxMessageType::Message,
                "retry me",
                None,
                None,
                "lead",
                "turn-1",
            )
            .await
            .unwrap();
        assert!(!first.replayed);

        let rebuilt_mailbox = Mailbox::new(repo.clone());
        let replay = rebuilt_mailbox
            .write_with_files_idempotent(
                "t1",
                "a1",
                "user",
                MailboxMessageType::Message,
                "retry me",
                None,
                None,
                "lead",
                "turn-1",
            )
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.message.id, first.message.id);
        assert_eq!(repo.state.lock().unwrap().messages.len(), 1);

        let conflict = rebuilt_mailbox
            .write_with_files_idempotent(
                "t1",
                "a1",
                "user",
                MailboxMessageType::Message,
                "changed payload",
                None,
                None,
                "lead",
                "turn-1",
            )
            .await;
        assert!(matches!(conflict, Err(TeamError::InvalidRequest(_))));
        assert_eq!(repo.state.lock().unwrap().messages.len(), 1);
    }

    #[tokio::test]
    async fn get_history_includes_read_messages() {
        let repo = Arc::new(MockTeamRepo::new());
        let mailbox = Mailbox::new(repo);

        mailbox
            .write("t1", "a1", "user", MailboxMessageType::Message, "m1", None)
            .await
            .unwrap();
        mailbox
            .write("t1", "a1", "user", MailboxMessageType::Message, "m2", None)
            .await
            .unwrap();

        mailbox.read_unread("t1", "a1").await.unwrap();

        let history = mailbox.get_history("t1", "a1", None).await.unwrap();
        assert_eq!(history.len(), 2);
    }

    #[tokio::test]
    async fn get_history_with_limit() {
        let repo = Arc::new(MockTeamRepo::new());
        let mailbox = Mailbox::new(repo);

        for i in 0..5 {
            mailbox
                .write(
                    "t1",
                    "a1",
                    "user",
                    MailboxMessageType::Message,
                    &format!("msg-{i}"),
                    None,
                )
                .await
                .unwrap();
        }

        let history = mailbox.get_history("t1", "a1", Some(3)).await.unwrap();
        assert_eq!(history.len(), 3);
    }

    #[tokio::test]
    async fn delete_by_team_removes_all() {
        let repo = Arc::new(MockTeamRepo::new());
        let mailbox = Mailbox::new(repo);

        mailbox
            .write("t1", "a1", "user", MailboxMessageType::Message, "x", None)
            .await
            .unwrap();
        mailbox
            .write("t2", "a1", "user", MailboxMessageType::Message, "y", None)
            .await
            .unwrap();

        mailbox.delete_by_team("t1").await.unwrap();

        let h1 = mailbox.get_history("t1", "a1", None).await.unwrap();
        assert!(h1.is_empty());

        let h2 = mailbox.get_history("t2", "a1", None).await.unwrap();
        assert_eq!(h2.len(), 1);
    }

    #[tokio::test]
    async fn read_unread_empty_when_no_messages() {
        let repo = Arc::new(MockTeamRepo::new());
        let mailbox = Mailbox::new(repo);

        let unread = mailbox.read_unread("t1", "a1").await.unwrap();
        assert!(unread.is_empty());
    }

    #[tokio::test]
    async fn read_unread_scoped_to_agent() {
        let repo = Arc::new(MockTeamRepo::new());
        let mailbox = Mailbox::new(repo);

        mailbox
            .write("t1", "a1", "user", MailboxMessageType::Message, "for-a1", None)
            .await
            .unwrap();
        mailbox
            .write("t1", "a2", "user", MailboxMessageType::Message, "for-a2", None)
            .await
            .unwrap();

        let unread_a1 = mailbox.read_unread("t1", "a1").await.unwrap();
        assert_eq!(unread_a1.len(), 1);
        assert_eq!(unread_a1[0].content, "for-a1");

        let unread_a2 = mailbox.read_unread("t1", "a2").await.unwrap();
        assert_eq!(unread_a2.len(), 1);
        assert_eq!(unread_a2[0].content, "for-a2");
    }
}
