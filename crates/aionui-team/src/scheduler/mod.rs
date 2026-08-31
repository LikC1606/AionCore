use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use aionui_realtime::EventBroadcaster;
use dashmap::{DashMap, DashSet};
use tokio::sync::Mutex;

use crate::error::TeamError;
use crate::events::TeamEventEmitter;
use crate::mailbox::Mailbox;
use crate::types::{MailboxMessage, TeamAgent, TeammateRole, TeammateStatus};

mod actions;
mod agent_lifecycle;
mod crash_recovery;
mod dedup;
mod state;
mod wake;

#[cfg(test)]
mod tests;

pub use actions::SchedulerAction;
pub use crash_recovery::format_crash_testament;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const WAKE_TIMEOUT_MS: u64 = 60_000;

pub(crate) const FINALIZE_DEDUP_WINDOW: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// normalize_name — canonical form for agent-name conflict checks
// ---------------------------------------------------------------------------

/// Normalize an agent name to its canonical form for conflict detection.
///
/// Rules (see interface-contracts 15.1):
/// 1. Trim leading/trailing whitespace.
/// 2. Drop control characters (`char::is_control`).
/// 3. Lowercase (Unicode-aware via `to_lowercase`).
pub fn normalize_name(name: &str) -> String {
    name.trim()
        .chars()
        .filter(|c| !c.is_control())
        .collect::<String>()
        .to_lowercase()
}

// ---------------------------------------------------------------------------
// is_settled — helper for "all teammates settled" transitions
// ---------------------------------------------------------------------------

/// Status set that counts as "settled" for the purpose of
/// "all teammates settled -> wake leader" transitions.
///
/// Expanded beyond `Idle` to match the AionUi reference implementation
/// (TeammateManager.ts:440-452): `Completed` and `Error` teammates are
/// terminal and should not block the leader from being woken up.
/// `Pending` is not in the set because the backend currently serde-aliases
/// `"pending"` to `Idle`; it will be reintroduced when the variant is split.
pub(crate) fn is_settled(status: TeammateStatus) -> bool {
    matches!(
        status,
        TeammateStatus::Idle | TeammateStatus::Completed | TeammateStatus::Error
    )
}

// ---------------------------------------------------------------------------
// WakeTimeoutHandler type alias
// ---------------------------------------------------------------------------

/// Callback invoked when the wake-timeout watchdog elapses without seeing
/// any stream activity for a slot.
///
/// Reason: `arm_wake_timeout` is written against `origin/main`, where
/// `handle_inactivity_timeout` (W4-D22, PR #99) does not yet exist. Taking
/// the recovery action as an injected closure keeps this module decoupled --
/// once D22 lands, callers just pass `mgr.handle_inactivity_timeout(...)`
/// through this slot without touching `arm_wake_timeout` itself.
pub type WakeTimeoutHandler = Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

// ---------------------------------------------------------------------------
// WakePayload — context assembled for an agent when it is woken up
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct WakePayload {
    pub agent: TeamAgent,
    pub unread_messages: Vec<MailboxMessage>,
}

// ---------------------------------------------------------------------------
// AgentSlot — per-agent runtime state tracked by the scheduler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) struct AgentSlot {
    pub(crate) agent: TeamAgent,
    pub(crate) status: TeammateStatus,
    /// True until the first wake completes — used to inject role prompt on cold start.
    pub(crate) needs_role_prompt: bool,
}

// ---------------------------------------------------------------------------
// TeammateManager
// ---------------------------------------------------------------------------

pub struct TeammateManager {
    pub(crate) team_id: String,
    pub(crate) slots: Mutex<HashMap<String, AgentSlot>>,
    pub(crate) mailbox: Arc<Mailbox>,
    pub(crate) events: TeamEventEmitter,
    pub(crate) active_wakes: DashSet<String>,
    /// Per-slot state for the currently executing event-loop turn. The flag
    /// is set only after that turn successfully commits a durable message to
    /// the Lead, so mailbox read timing cannot cause duplicate wakes.
    pub(crate) active_turn_lead_deliveries: DashMap<String, bool>,
    // Reason: Finish / Error events may fire back-to-back for the same
    // conversation; without this dedup window, finalize_turn would run twice
    // and double-write the IdleNotification (aionui-audit 4.3, 8 #3).
    pub(crate) finalized_turns: Arc<DashMap<String, Instant>>,
    pub(crate) wake_timeouts: Arc<DashMap<String, tokio::task::JoinHandle<()>>>,
}

impl TeammateManager {
    pub fn new(
        team_id: String,
        agents: &[TeamAgent],
        mailbox: Arc<Mailbox>,
        broadcaster: Arc<dyn EventBroadcaster>,
    ) -> Self {
        let mut slots = HashMap::new();
        for agent in agents {
            let mut a = agent.clone();
            a.status = Some(TeammateStatus::Idle);
            slots.insert(
                a.slot_id.clone(),
                AgentSlot {
                    agent: a,
                    status: TeammateStatus::Idle,
                    needs_role_prompt: true,
                },
            );
        }
        let events = TeamEventEmitter::new(team_id.clone(), broadcaster);
        Self {
            team_id,
            slots: Mutex::new(slots),
            mailbox,
            events,
            active_wakes: DashSet::new(),
            active_turn_lead_deliveries: DashMap::new(),
            finalized_turns: Arc::new(DashMap::new()),
            wake_timeouts: Arc::new(DashMap::new()),
        }
    }

    pub async fn get_agent(&self, slot_id: &str) -> Result<TeamAgent, TeamError> {
        let slots = self.slots.lock().await;
        let slot = slots
            .get(slot_id)
            .ok_or_else(|| TeamError::AgentNotFound(slot_id.to_owned()))?;
        Ok(slot.agent.clone())
    }

    pub async fn list_agents(&self) -> Vec<TeamAgent> {
        let slots = self.slots.lock().await;
        slots.values().map(|s| s.agent.clone()).collect()
    }

    pub async fn find_lead_slot_id(&self) -> Option<String> {
        let slots = self.slots.lock().await;
        slots
            .values()
            .find(|s| s.agent.role == TeammateRole::Lead)
            .map(|s| s.agent.slot_id.clone())
    }

    /// Start tracking delivery side effects for one event-loop turn.
    pub(crate) fn begin_turn(&self, slot_id: &str) {
        self.active_turn_lead_deliveries.insert(slot_id.to_owned(), false);
    }

    /// Record a durable message to the Lead for the active turn. Calls made
    /// outside the event loop are intentionally ignored.
    pub(crate) fn record_lead_delivery(&self, slot_id: &str) {
        if let Some(mut entry) = self.active_turn_lead_deliveries.get_mut(slot_id) {
            *entry = true;
        }
    }

    /// Consume the turn-bound delivery flag during finalization. A missing
    /// entry preserves legacy behavior for callers outside the event loop.
    pub(crate) fn take_lead_delivery(&self, slot_id: &str) -> Option<bool> {
        self.active_turn_lead_deliveries
            .remove(slot_id)
            .map(|(_, delivered)| delivered)
    }

    /// Discard turn tracking after a turn fails before normal finalization.
    pub(crate) fn clear_turn(&self, slot_id: &str) {
        self.active_turn_lead_deliveries.remove(slot_id);
    }
}
