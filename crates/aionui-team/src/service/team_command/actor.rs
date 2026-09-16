use std::collections::{HashMap, HashSet};

use aionui_db::models::TeamRow;
use thiserror::Error;

use crate::kernel::MemberRelation;
use crate::types::{TeamAgent, TeammateRole};

#[derive(Debug, Clone)]
pub(super) struct RosterMember {
    pub member_id: String,
    pub role: TeammateRole,
}

#[derive(Debug)]
pub(super) struct LegacyTeamRoster {
    members: HashMap<String, RosterMember>,
    member_by_conversation: HashMap<String, String>,
}

impl LegacyTeamRoster {
    pub fn from_row(row: &TeamRow) -> Result<Self, RosterIntegrityError> {
        let agents: Vec<TeamAgent> = serde_json::from_str(&row.agents)
            .map_err(|error| RosterIntegrityError::InvalidAgentsJson(error.to_string()))?;
        if agents.is_empty() {
            return Err(RosterIntegrityError::EmptyRoster);
        }

        let mut members = HashMap::with_capacity(agents.len());
        let mut member_by_conversation = HashMap::with_capacity(agents.len());
        let mut lead_ids = HashSet::new();
        for agent in agents {
            if agent.slot_id.trim().is_empty() {
                return Err(RosterIntegrityError::EmptyMemberId);
            }
            if agent.conversation_id.trim().is_empty() {
                return Err(RosterIntegrityError::EmptyConversationId {
                    member_id: agent.slot_id,
                });
            }
            let member = RosterMember {
                member_id: agent.slot_id.clone(),
                role: agent.role,
            };
            if members.insert(agent.slot_id.clone(), member).is_some() {
                return Err(RosterIntegrityError::DuplicateMemberId(agent.slot_id));
            }
            if member_by_conversation
                .insert(agent.conversation_id.clone(), agent.slot_id.clone())
                .is_some()
            {
                return Err(RosterIntegrityError::DuplicateConversationId(agent.conversation_id));
            }
            if agent.role == TeammateRole::Lead {
                lead_ids.insert(agent.slot_id);
            }
        }

        if lead_ids.len() != 1 {
            return Err(RosterIntegrityError::ExpectedSingleLead { actual: lead_ids.len() });
        }
        if let Some(configured_lead) = row.lead_agent_id.as_deref()
            && !lead_ids.contains(configured_lead)
        {
            return Err(RosterIntegrityError::LeadBindingMismatch {
                configured: configured_lead.to_owned(),
            });
        }

        Ok(Self {
            members,
            member_by_conversation,
        })
    }

    pub fn resolve_conversation(&self, conversation_id: &str) -> Option<&RosterMember> {
        self.member_by_conversation
            .get(conversation_id)
            .and_then(|member_id| self.members.get(member_id))
    }

    pub fn member(&self, member_id: &str) -> Option<&RosterMember> {
        self.members.get(member_id)
    }

    pub fn lead(&self) -> &RosterMember {
        self.members
            .values()
            .find(|member| member.role == TeammateRole::Lead)
            .expect("validated Team roster always contains exactly one Lead")
    }

    pub fn relation(&self, actor_member_id: &str, target_member_id: &str) -> MemberRelation {
        if actor_member_id == target_member_id {
            return if self.members.contains_key(actor_member_id) {
                MemberRelation::SelfMember
            } else {
                MemberRelation::Unrelated
            };
        }
        let Some(actor) = self.member(actor_member_id) else {
            return MemberRelation::Unrelated;
        };
        let Some(target) = self.member(target_member_id) else {
            return MemberRelation::Unrelated;
        };
        match (actor.role, target.role) {
            (TeammateRole::Lead, TeammateRole::Teammate) => MemberRelation::Subordinate,
            (TeammateRole::Teammate, TeammateRole::Lead) => MemberRelation::Superior,
            (TeammateRole::Teammate, TeammateRole::Teammate) => MemberRelation::Peer,
            _ => MemberRelation::Unrelated,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub(crate) enum RosterIntegrityError {
    #[error("team roster agents JSON is invalid: {0}")]
    InvalidAgentsJson(String),
    #[error("team roster is empty")]
    EmptyRoster,
    #[error("team roster contains an empty member id")]
    EmptyMemberId,
    #[error("team member {member_id} has an empty conversation id")]
    EmptyConversationId { member_id: String },
    #[error("team roster contains duplicate member id {0}")]
    DuplicateMemberId(String),
    #[error("team roster contains duplicate conversation id {0}")]
    DuplicateConversationId(String),
    #[error("team roster must contain exactly one Lead, found {actual}")]
    ExpectedSingleLead { actual: usize },
    #[error("team lead binding {configured} does not identify the Lead member")]
    LeadBindingMismatch { configured: String },
}

#[cfg(test)]
mod tests {
    use aionui_common::now_ms;

    use super::*;

    fn row(agents: Vec<TeamAgent>, lead_agent_id: Option<&str>) -> TeamRow {
        TeamRow {
            coordination_protocol: None,
            id: "team-1".into(),
            user_id: "user-1".into(),
            name: "Team".into(),
            workspace: "/tmp/team".into(),
            workspace_mode: "shared".into(),
            agents: serde_json::to_string(&agents).unwrap(),
            lead_agent_id: lead_agent_id.map(str::to_owned),
            session_mode: None,
            agents_version: "1".into(),
            created_at: now_ms(),
            updated_at: now_ms(),
        }
    }

    fn member(slot: &str, conversation: &str, role: TeammateRole) -> TeamAgent {
        TeamAgent {
            slot_id: slot.into(),
            name: slot.into(),
            role,
            conversation_id: conversation.into(),
            backend: "mock".into(),
            model: String::new(),
            assistant_id: None,
            status: None,
            conversation_type: None,
            cli_path: None,
        }
    }

    #[test]
    fn projects_flat_team_into_directional_relations() {
        let roster = LegacyTeamRoster::from_row(&row(
            vec![
                member("lead", "conv-lead", TeammateRole::Lead),
                member("worker-a", "conv-a", TeammateRole::Teammate),
                member("worker-b", "conv-b", TeammateRole::Teammate),
            ],
            Some("lead"),
        ))
        .unwrap();

        assert_eq!(roster.relation("lead", "worker-a"), MemberRelation::Subordinate);
        assert_eq!(roster.relation("worker-a", "lead"), MemberRelation::Superior);
        assert_eq!(roster.relation("worker-a", "worker-b"), MemberRelation::Peer);
        assert_eq!(roster.relation("worker-a", "worker-a"), MemberRelation::SelfMember);
        assert_eq!(roster.resolve_conversation("conv-a").unwrap().member_id, "worker-a");
    }

    #[test]
    fn rejects_ambiguous_or_inconsistent_identity_bindings() {
        let duplicate_conversation = row(
            vec![
                member("lead", "same", TeammateRole::Lead),
                member("worker", "same", TeammateRole::Teammate),
            ],
            Some("lead"),
        );
        assert!(matches!(
            LegacyTeamRoster::from_row(&duplicate_conversation),
            Err(RosterIntegrityError::DuplicateConversationId(_))
        ));

        let wrong_lead = row(
            vec![
                member("lead", "conv-lead", TeammateRole::Lead),
                member("worker", "conv-worker", TeammateRole::Teammate),
            ],
            Some("worker"),
        );
        assert!(matches!(
            LegacyTeamRoster::from_row(&wrong_lead),
            Err(RosterIntegrityError::LeadBindingMismatch { .. })
        ));
    }
}
