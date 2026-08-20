use aionui_db::models::{MailboxMessageRow, TeamRow};
use aionui_db::{DbError, ITeamRepository, MailboxIdempotencyParams, MailboxWriteResult, UpdateTeamParams};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct MockState {
    pub messages: Vec<MailboxMessageRow>,
    pub idempotency_receipts: HashMap<(String, String, String), (String, String)>,
}

pub struct MockTeamRepo {
    pub state: Mutex<MockState>,
}

impl MockTeamRepo {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(MockState::default()),
        }
    }
}

#[async_trait::async_trait]
impl ITeamRepository for MockTeamRepo {
    async fn create_team(&self, _row: &TeamRow) -> Result<(), DbError> {
        Ok(())
    }
    async fn list_teams(&self) -> Result<Vec<TeamRow>, DbError> {
        Ok(vec![])
    }
    async fn list_teams_by_user(&self, _user_id: &str) -> Result<Vec<TeamRow>, DbError> {
        Ok(vec![])
    }
    async fn get_team(&self, _id: &str) -> Result<Option<TeamRow>, DbError> {
        Ok(None)
    }
    async fn update_team(&self, _id: &str, _p: &UpdateTeamParams) -> Result<(), DbError> {
        Ok(())
    }
    async fn delete_team(&self, _id: &str) -> Result<(), DbError> {
        Ok(())
    }

    async fn write_message(&self, row: &MailboxMessageRow) -> Result<(), DbError> {
        self.state.lock().unwrap().messages.push(row.clone());
        Ok(())
    }

    async fn write_message_idempotent(
        &self,
        row: &MailboxMessageRow,
        idempotency: &MailboxIdempotencyParams<'_>,
    ) -> Result<MailboxWriteResult, DbError> {
        let mut state = self.state.lock().unwrap();
        let key = (
            row.team_id.clone(),
            idempotency.scope.to_owned(),
            idempotency.key.to_owned(),
        );
        if let Some((existing_fingerprint, existing_id)) = state.idempotency_receipts.get(&key).cloned() {
            if existing_fingerprint != idempotency.request_fingerprint {
                return Ok(MailboxWriteResult::IdempotencyConflict {
                    existing_request_fingerprint: existing_fingerprint,
                });
            }
            let existing = state
                .messages
                .iter()
                .find(|message| message.id == existing_id)
                .cloned()
                .ok_or_else(|| DbError::Init("mock mailbox receipt points to a missing message".into()))?;
            return Ok(MailboxWriteResult::Existing(existing));
        }
        state.messages.push(row.clone());
        state
            .idempotency_receipts
            .insert(key, (idempotency.request_fingerprint.to_owned(), row.id.clone()));
        Ok(MailboxWriteResult::Inserted)
    }

    async fn read_unread_and_mark(&self, team_id: &str, to_agent_id: &str) -> Result<Vec<MailboxMessageRow>, DbError> {
        let mut state = self.state.lock().unwrap();
        let mut result = vec![];
        for msg in &mut state.messages {
            if msg.team_id == team_id && msg.to_agent_id == to_agent_id && !msg.read {
                msg.read = true;
                result.push(msg.clone());
            }
        }
        Ok(result)
    }

    async fn peek_unread(&self, team_id: &str, to_agent_id: &str) -> Result<Vec<MailboxMessageRow>, DbError> {
        let state = self.state.lock().unwrap();
        let result = state
            .messages
            .iter()
            .filter(|m| m.team_id == team_id && m.to_agent_id == to_agent_id && !m.read)
            .cloned()
            .collect();
        Ok(result)
    }

    async fn list_team_ids_with_recoverable_unread_mailbox(
        &self,
        after_team_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<String>, DbError> {
        let state = self.state.lock().unwrap();
        let mut team_ids = state
            .messages
            .iter()
            .filter(|message| !message.read && message.from_agent_id != message.to_agent_id)
            .map(|message| message.team_id.clone())
            .filter(|team_id| after_team_id.is_none_or(|after| team_id.as_str() > after))
            .collect::<Vec<_>>();
        team_ids.sort();
        team_ids.dedup();
        team_ids.truncate(limit as usize);
        Ok(team_ids)
    }

    async fn mark_read_batch(&self, ids: &[String]) -> Result<(), DbError> {
        let mut state = self.state.lock().unwrap();
        for msg in &mut state.messages {
            if ids.contains(&msg.id) {
                msg.read = true;
            }
        }
        Ok(())
    }

    async fn get_history(
        &self,
        team_id: &str,
        to_agent_id: &str,
        limit: Option<i64>,
    ) -> Result<Vec<MailboxMessageRow>, DbError> {
        let state = self.state.lock().unwrap();
        let iter = state
            .messages
            .iter()
            .filter(|m| m.team_id == team_id && m.to_agent_id == to_agent_id);
        let msgs: Vec<_> = match limit {
            Some(n) => iter.take(n as usize).cloned().collect(),
            None => iter.cloned().collect(),
        };
        Ok(msgs)
    }

    async fn delete_mailbox_by_team(&self, team_id: &str) -> Result<(), DbError> {
        self.state.lock().unwrap().messages.retain(|m| m.team_id != team_id);
        Ok(())
    }
}
