//! A bounded first-turn barrier for ACP sessions that inject MCP servers.
//!
//! `session/new` is acknowledged by codex-acp before its MCP connection
//! manager has finished starting every configured server. ACP currently has
//! no standard readiness notification, so the manager uses a short, bounded
//! settle window before the first prompt. The gate is armed once per ACP
//! session and is not involved in later turns.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Notify};

/// codex-acp normally starts the bundled MCP processes well below this
/// window. Keeping it bounded prevents a slow or unavailable MCP server from
/// blocking the conversation indefinitely; the existing startup error path
/// still reports a genuine failure after the window.
pub(super) const MCP_STARTUP_SETTLE_DELAY: Duration = Duration::from_secs(3);

pub(super) struct McpStartupGate {
    deadline: Mutex<Option<Instant>>,
    cancelled: Notify,
    cancelled_flag: AtomicBool,
    settle_delay: Duration,
}

impl Default for McpStartupGate {
    fn default() -> Self {
        Self::new(MCP_STARTUP_SETTLE_DELAY)
    }
}

impl McpStartupGate {
    pub(super) fn new(settle_delay: Duration) -> Self {
        Self {
            deadline: Mutex::new(None),
            cancelled: Notify::new(),
            cancelled_flag: AtomicBool::new(false),
            settle_delay,
        }
    }

    /// Arm the gate only when an ACP session carries MCP servers. Repeated
    /// calls are intentionally idempotent so a resume/reconcile path cannot
    /// extend the first-turn delay.
    pub(super) async fn arm_if_needed(&self, server_count: usize) -> bool {
        if server_count == 0 {
            return false;
        }

        let mut deadline = self.deadline.lock().await;
        if deadline.is_none() {
            self.cancelled_flag.store(false, Ordering::Release);
            *deadline = Some(Instant::now() + self.settle_delay);
            return true;
        }
        false
    }

    /// Wait until the first-turn settle window has elapsed. Returns `false`
    /// when a user cancellation or session teardown interrupted the wait.
    pub(super) async fn wait(&self) -> bool {
        loop {
            let current_deadline = *self.deadline.lock().await;
            let Some(deadline) = current_deadline else {
                return true;
            };
            if self.cancelled_flag.load(Ordering::Acquire) {
                let mut guard = self.deadline.lock().await;
                if *guard == Some(deadline) {
                    guard.take();
                }
                return false;
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let mut guard = self.deadline.lock().await;
                if *guard == Some(deadline) {
                    guard.take();
                }
                return true;
            }

            tokio::select! {
                _ = tokio::time::sleep(remaining) => {},
                _ = self.cancelled.notified() => {
                    let mut guard = self.deadline.lock().await;
                    if *guard == Some(deadline) {
                        guard.take();
                    }
                    return false;
                },
            }
        }
    }

    /// Interrupt a pending first-turn wait. This is deliberately separate
    /// from the ACP `session/cancel` notification so a prompt is never sent
    /// after the user has already stopped the turn.
    pub(super) fn cancel_wait(&self) {
        self.cancelled_flag.store(true, Ordering::Release);
        self.cancelled.notify_waiters();
    }

    #[cfg(test)]
    async fn is_armed(&self) -> bool {
        self.deadline.lock().await.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::McpStartupGate;
    use std::time::{Duration, Instant};

    #[tokio::test]
    async fn does_not_arm_without_mcp_servers() {
        let gate = McpStartupGate::new(Duration::from_millis(5));
        assert!(!gate.arm_if_needed(0).await);
        assert!(!gate.is_armed().await);
        assert!(gate.wait().await);
    }

    #[tokio::test]
    async fn first_wait_is_bounded_and_consumed() {
        let gate = McpStartupGate::new(Duration::from_millis(15));
        assert!(gate.arm_if_needed(1).await);
        let started = Instant::now();
        assert!(gate.wait().await);
        assert!(started.elapsed() >= Duration::from_millis(10));
        assert!(!gate.is_armed().await);
        assert!(gate.wait().await);
    }

    #[tokio::test]
    async fn cancellation_interrupts_pending_wait() {
        let gate = std::sync::Arc::new(McpStartupGate::new(Duration::from_secs(30)));
        gate.arm_if_needed(1).await;
        let waiting = {
            let gate = std::sync::Arc::clone(&gate);
            tokio::spawn(async move { gate.wait().await })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        gate.cancel_wait();
        assert!(!waiting.await.expect("gate task should finish"));
    }

    #[tokio::test]
    async fn next_session_can_rearm_after_cancellation() {
        let gate = McpStartupGate::new(Duration::from_millis(5));
        assert!(gate.arm_if_needed(1).await);
        gate.cancel_wait();
        assert!(!gate.wait().await);

        assert!(gate.arm_if_needed(1).await);
        assert!(gate.wait().await);
        assert!(!gate.is_armed().await);
    }
}
