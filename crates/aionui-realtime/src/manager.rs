use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use aionui_api_types::{RuntimeEventGap, WebSocketMessage};
use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::broadcaster::EventBroadcaster;
use crate::types::{
    ClientInfo, ConnectionId, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT, RealtimeError, WebSocketCloseCode, WsOutbound,
};

/// Validates whether a JWT token is still valid.
/// Returns `true` if the token is valid, `false` if expired or revoked.
pub type TokenValidator = Arc<dyn Fn(&str) -> bool + Send + Sync>;

/// Manages active WebSocket connections, heartbeat detection,
/// and provides broadcast/unicast messaging.
pub struct WebSocketManager {
    connections: Arc<DashMap<ConnectionId, ClientInfo>>,
    next_id: AtomicU64,
    forced_close: DashMap<ConnectionId, oneshot::Sender<()>>,
}

impl WebSocketManager {
    pub fn new() -> Self {
        Self {
            connections: Arc::new(DashMap::new()),
            next_id: AtomicU64::new(1),
            forced_close: DashMap::new(),
        }
    }

    /// Register a new client connection and return its assigned ID.
    pub fn add_client(&self, token: String, tx: mpsc::Sender<WsOutbound>) -> ConnectionId {
        self.register_client(token, tx, None)
    }

    /// Out-of-band shutdown is not blocked by a saturated outbound queue.
    pub fn add_client_with_close(
        &self,
        token: String,
        tx: mpsc::Sender<WsOutbound>,
    ) -> (ConnectionId, oneshot::Receiver<()>) {
        let (close_tx, close_rx) = oneshot::channel();
        (self.register_client(token, tx, Some(close_tx)), close_rx)
    }

    fn register_client(
        &self,
        token: String,
        tx: mpsc::Sender<WsOutbound>,
        close: Option<oneshot::Sender<()>>,
    ) -> ConnectionId {
        let id = ConnectionId(self.next_id.fetch_add(1, Ordering::Relaxed));
        let info = ClientInfo {
            token,
            last_ping: Instant::now(),
            tx,
        };
        if let Some(close) = close {
            self.forced_close.insert(id, close);
        }
        self.connections.insert(id, info);
        debug!(%id, "client added");
        id
    }

    /// Remove a client connection by ID.
    pub fn remove_client(&self, conn_id: ConnectionId) {
        if self.connections.remove(&conn_id).is_some() {
            debug!(%conn_id, "client removed");
        }
        if let Some((_, close)) = self.forced_close.remove(&conn_id) {
            let _ = close.send(());
        }
    }

    pub fn disconnect_all(&self) {
        let ids: Vec<_> = self.connections.iter().map(|entry| *entry.key()).collect();
        for id in ids {
            self.remove_client(id);
        }
    }

    /// A bus gap invalidates client projections but does not end the bridge.
    pub async fn receive_event(
        &self,
        receiver: &mut broadcast::Receiver<WebSocketMessage<serde_json::Value>>,
    ) -> Option<WebSocketMessage<serde_json::Value>> {
        loop {
            match receiver.recv().await {
                Ok(event) => return Some(event),
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(skipped, "event bridge lagged; client snapshots require reconciliation");
                    self.report_event_gap(skipped);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    warn!("event bridge closed; disconnecting clients");
                    self.disconnect_all();
                    return None;
                }
            }
        }
    }

    /// Use only payload-free metadata when an event cannot be delivered safely.
    pub fn report_event_gap(&self, skipped: u64) {
        let gap = RuntimeEventGap {
            skipped,
            observed_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .min(u128::from(u64::MAX)) as u64,
        };
        self.broadcast_all(WebSocketMessage::new("runtime.eventGap", serde_json::json!(gap)));
    }

    /// Update the last heartbeat timestamp for a connection.
    pub fn update_last_ping(&self, conn_id: ConnectionId) {
        if let Some(mut client) = self.connections.get_mut(&conn_id) {
            client.last_ping = Instant::now();
        }
    }

    /// Returns the number of active connections.
    pub fn client_count(&self) -> usize {
        self.connections.len()
    }

    /// Send a message to all connected clients.
    ///
    /// Uses `try_send` for backpressure. A saturated channel cannot reliably
    /// receive an additional `REALTIME_BACKPRESSURE` event on the same path, so
    /// critical-event backpressure closes the connection to force reconciliation.
    /// Ordinary telemetry may be dropped. Closed channels trigger client removal.
    pub fn broadcast_all(&self, msg: WebSocketMessage<serde_json::Value>) {
        let critical = requires_reconciliation(&msg.name);
        let text = match serde_json::to_string(&msg) {
            Ok(t) => t,
            Err(e) => {
                warn!(error = %e, "failed to serialize broadcast message");
                return;
            }
        };

        let mut disconnected = Vec::new();
        for entry in self.connections.iter() {
            let conn_id = *entry.key();
            match entry.value().tx.try_send(WsOutbound::Text(text.clone())) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    if critical {
                        disconnected.push(conn_id);
                    }
                    warn!(
                        %conn_id,
                        critical,
                        code = RealtimeError::Backpressure.code(),
                        "outbound channel full, broadcast message dropped"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    disconnected.push(conn_id);
                }
            }
        }

        for conn_id in disconnected {
            self.remove_client(conn_id);
        }
    }

    /// Send an event only to connections whose authenticated token resolves
    /// to `user_id`.
    ///
    /// Token resolution is supplied by the application boundary so this crate
    /// stays independent from the concrete authentication implementation.
    /// Invalid, expired, or revoked tokens must resolve to `None` and are
    /// skipped.
    pub fn broadcast_to_user(
        &self,
        user_id: &str,
        msg: WebSocketMessage<serde_json::Value>,
        resolve_user: &(dyn Fn(&str) -> Option<String> + Send + Sync),
    ) {
        let critical = requires_reconciliation(&msg.name);
        let text = match serde_json::to_string(&msg) {
            Ok(text) => text,
            Err(error) => {
                warn!(error = %error, "failed to serialize user-scoped message");
                return;
            }
        };

        let mut disconnected = Vec::new();
        for entry in self.connections.iter() {
            let conn_id = *entry.key();
            let client = entry.value();
            if resolve_user(&client.token).as_deref() != Some(user_id) {
                continue;
            }
            match client.tx.try_send(WsOutbound::Text(text.clone())) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    if critical {
                        disconnected.push(conn_id);
                    }
                    warn!(
                        %conn_id,
                        critical,
                        code = RealtimeError::Backpressure.code(),
                        "outbound channel full, user-scoped message dropped"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => disconnected.push(conn_id),
            }
        }

        for conn_id in disconnected {
            self.remove_client(conn_id);
        }
    }

    /// Send a message to a specific connection.
    pub fn send_to(&self, conn_id: ConnectionId, msg: WebSocketMessage<serde_json::Value>) {
        let critical = requires_reconciliation(&msg.name);
        let text = match serde_json::to_string(&msg) {
            Ok(t) => t,
            Err(e) => {
                warn!(
                    %conn_id, error = %e,
                    "failed to serialize unicast message"
                );
                return;
            }
        };

        self.send_raw(conn_id, WsOutbound::Text(text), critical);
    }

    /// Send a raw outbound message to a specific connection.
    ///
    /// Used for non-`WebSocketMessage` payloads (e.g. error responses). A full
    /// channel cannot receive a send-failure event through the same queue, so
    /// backpressure is logged as the downgrade path.
    pub fn send_raw_to(&self, conn_id: ConnectionId, outbound: WsOutbound) {
        self.send_raw(conn_id, outbound, false);
    }

    fn send_raw(&self, conn_id: ConnectionId, outbound: WsOutbound, critical: bool) {
        if let Some(client) = self.connections.get(&conn_id) {
            match client.tx.try_send(outbound) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(
                        %conn_id,
                        code = RealtimeError::SendFailed.code(),
                        "outbound channel full, raw message dropped"
                    );
                    if critical {
                        drop(client);
                        self.remove_client(conn_id);
                    }
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    drop(client);
                    self.remove_client(conn_id);
                }
            }
        }
    }

    /// Start the heartbeat check loop.
    ///
    /// Every `HEARTBEAT_INTERVAL` (30s), iterates all connections:
    /// 1. Timeout check — closes connections with no pong for `HEARTBEAT_TIMEOUT`
    /// 2. Token expiry — validates token and sends `realtime.error` if invalid
    /// 3. Sends a `ping` message with current timestamp
    ///
    /// Returns a `JoinHandle` — abort it to stop the heartbeat loop.
    pub fn start_heartbeat(&self, token_validator: TokenValidator) -> JoinHandle<()> {
        let connections = Arc::clone(&self.connections);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
            loop {
                interval.tick().await;
                heartbeat_tick(&connections, &token_validator);
            }
        })
    }
}

fn requires_reconciliation(name: &str) -> bool {
    matches!(
        name,
        "runtime.eventGap"
            | "runtime.subscriptionReady"
            | "team.runCompleted"
            | "team.runFailed"
            | "team.runCancelled"
            | "team.childTurnCompleted"
            | "team.childTurnCancelled"
            | "team.childTurnFailed"
    )
}

impl Default for WebSocketManager {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBroadcaster for WebSocketManager {
    fn broadcast(&self, event: WebSocketMessage<serde_json::Value>) {
        self.broadcast_all(event);
    }
}

/// Single heartbeat tick: check timeouts, token validity, send pings.
fn heartbeat_tick(connections: &DashMap<ConnectionId, ClientInfo>, token_validator: &TokenValidator) {
    let now = Instant::now();
    let mut to_remove = Vec::new();

    for entry in connections.iter() {
        let conn_id = *entry.key();
        let client = entry.value();

        // 1. Heartbeat timeout
        if now.duration_since(client.last_ping) > HEARTBEAT_TIMEOUT {
            info!(%conn_id, "heartbeat timeout, closing connection");
            let _ = client.tx.try_send(terminal_realtime_error(
                conn_id,
                RealtimeError::HeartbeatTimeout,
                "heartbeat timeout",
            ));
            to_remove.push(conn_id);
            continue;
        }

        // 2. Token expiry
        if !token_validator(&client.token) {
            info!(%conn_id, "token expired, closing connection");
            let outbound = terminal_realtime_error(conn_id, RealtimeError::AuthExpired, "token expired");

            match client.tx.try_send(outbound) {
                Ok(()) | Err(mpsc::error::TrySendError::Closed(_)) => {
                    to_remove.push(conn_id);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(
                        %conn_id,
                        code = RealtimeError::Backpressure.code(),
                        "outbound channel full, terminal auth close dropped"
                    );
                    to_remove.push(conn_id);
                }
            }
            continue;
        }

        // 3. Send ping
        let duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let timestamp = duration.as_secs() * 1000 + u64::from(duration.subsec_millis());

        let ping = WebSocketMessage::new("ping", json!({"timestamp": timestamp}));
        if let Ok(text) = serde_json::to_string(&ping) {
            match client.tx.try_send(WsOutbound::Text(text)) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    warn!(%conn_id, "outbound channel full, ping dropped");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    to_remove.push(conn_id);
                }
            }
        }
    }

    for conn_id in to_remove {
        connections.remove(&conn_id);
        debug!(%conn_id, "connection removed by heartbeat");
    }
}

fn terminal_realtime_error(conn_id: ConnectionId, error: RealtimeError, reason: &str) -> WsOutbound {
    match serde_json::to_string(&error.into_event()) {
        Ok(text) => WsOutbound::TextThenClose(text, WebSocketCloseCode::PolicyViolation, reason.into()),
        Err(e) => {
            warn!(%conn_id, error = %e, code = error.code(), "failed to serialize terminal realtime error");
            WsOutbound::Close(WebSocketCloseCode::PolicyViolation, reason.into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PER_CONNECTION_BUFFER;

    #[tokio::test]
    async fn lag_emits_payload_free_gap_and_continues_to_receive_events() {
        let manager = WebSocketManager::new();
        let (outbound, mut client) = mpsc::channel(8);
        manager.add_client("owner".into(), outbound);
        let (bus, mut receiver) = broadcast::channel(1);
        bus.send(WebSocketMessage::new("private", json!({"secret": "not-in-gap"})))
            .unwrap();
        bus.send(WebSocketMessage::new("latest", json!({}))).unwrap();
        assert_eq!(manager.receive_event(&mut receiver).await.unwrap().name, "latest");
        let WsOutbound::Text(gap) = client.recv().await.unwrap() else {
            panic!("expected gap");
        };
        let value: serde_json::Value = serde_json::from_str(&gap).unwrap();
        assert_eq!(value["name"], "runtime.eventGap");
        assert_eq!(value["data"]["skipped"], 1);
        assert_eq!(value["data"].as_object().unwrap().len(), 2);
        assert!(value["data"]["observedAt"].as_u64().unwrap() > 0);
        bus.send(WebSocketMessage::new("after-gap", json!({}))).unwrap();
        assert_eq!(manager.receive_event(&mut receiver).await.unwrap().name, "after-gap");
    }

    #[tokio::test]
    async fn closed_event_bus_forces_live_sockets_to_disconnect() {
        let manager = WebSocketManager::new();
        let (outbound, _client) = mpsc::channel(1);
        let (_, closed) = manager.add_client_with_close("owner".into(), outbound);
        let (bus, mut receiver) = broadcast::channel(1);
        drop(bus);
        assert!(manager.receive_event(&mut receiver).await.is_none());
        assert_eq!(manager.client_count(), 0);
        closed.await.unwrap();
    }

    #[tokio::test]
    async fn critical_backpressure_disconnects_even_with_an_extra_sender_alive() {
        for name in [
            "runtime.eventGap",
            "team.runCompleted",
            "team.runFailed",
            "team.runCancelled",
            "team.childTurnCompleted",
            "team.childTurnCancelled",
        ] {
            let manager = WebSocketManager::new();
            let (outbound, _client) = mpsc::channel(1);
            outbound.try_send(WsOutbound::Text("full".into())).unwrap();
            let (_, closed) = manager.add_client_with_close("owner".into(), outbound.clone());
            manager.broadcast_all(WebSocketMessage::new(name, json!({})));
            assert_eq!(manager.client_count(), 0, "{name}");
            closed.await.unwrap();
            drop(outbound);
        }
    }

    #[tokio::test]
    async fn user_scoped_terminal_pressure_does_not_disconnect_other_users() {
        let manager = WebSocketManager::new();
        let (owner, _owner_rx) = mpsc::channel(1);
        owner.try_send(WsOutbound::Text("full".into())).unwrap();
        let (_, closed) = manager.add_client_with_close("owner".into(), owner);
        let (other, mut other_rx) = mpsc::channel(1);
        manager.add_client("other".into(), other);
        manager.broadcast_to_user(
            "owner",
            WebSocketMessage::new("team.runCompleted", json!({"team_id": "private"})),
            &|token| Some(token.to_owned()),
        );
        closed.await.unwrap();
        assert_eq!(manager.client_count(), 1);
        assert!(other_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn ordinary_telemetry_pressure_preserves_connection() {
        let manager = WebSocketManager::new();
        let (outbound, _client) = mpsc::channel(1);
        outbound.try_send(WsOutbound::Text("full".into())).unwrap();
        let (_, mut closed) = manager.add_client_with_close("owner".into(), outbound);
        manager.broadcast_all(WebSocketMessage::new("team.runUpdated", json!({})));
        assert_eq!(manager.client_count(), 1);
        assert!(closed.try_recv().is_err());
    }

    fn always_valid() -> TokenValidator {
        Arc::new(|_| true)
    }

    fn always_expired() -> TokenValidator {
        Arc::new(|_| false)
    }

    fn new_client_tx() -> (mpsc::Sender<WsOutbound>, mpsc::Receiver<WsOutbound>) {
        mpsc::channel(PER_CONNECTION_BUFFER)
    }

    fn assert_realtime_auth_expired(text: &str) {
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["name"], "realtime.error");
        assert_eq!(parsed["data"]["code"], "REALTIME_AUTH_EXPIRED");
        assert!(parsed["data"]["message"].is_string());
        assert_eq!(parsed["data"]["recoverable"], false);
        assert!(parsed["data"]["details"].is_object());
    }

    fn assert_realtime_heartbeat_timeout(text: &str) {
        let parsed: serde_json::Value = serde_json::from_str(text).unwrap();
        assert_eq!(parsed["name"], "realtime.error");
        assert_eq!(parsed["data"]["code"], "REALTIME_HEARTBEAT_TIMEOUT");
        assert!(parsed["data"]["message"].is_string());
        assert_eq!(parsed["data"]["recoverable"], false);
        assert!(parsed["data"]["details"].is_object());
    }

    #[test]
    fn add_client_assigns_sequential_ids() {
        let mgr = WebSocketManager::new();
        let (tx1, _rx1) = new_client_tx();
        let (tx2, _rx2) = new_client_tx();

        let id1 = mgr.add_client("token-a".into(), tx1);
        let id2 = mgr.add_client("token-b".into(), tx2);

        assert_eq!(id1, ConnectionId(1));
        assert_eq!(id2, ConnectionId(2));
        assert_eq!(mgr.client_count(), 2);
    }

    #[test]
    fn remove_client_decrements_count() {
        let mgr = WebSocketManager::new();
        let (tx, _rx) = new_client_tx();
        let id = mgr.add_client("token".into(), tx);

        assert_eq!(mgr.client_count(), 1);
        mgr.remove_client(id);
        assert_eq!(mgr.client_count(), 0);
    }

    #[test]
    fn remove_nonexistent_client_is_noop() {
        let mgr = WebSocketManager::new();
        mgr.remove_client(ConnectionId(999));
        assert_eq!(mgr.client_count(), 0);
    }

    #[test]
    fn update_last_ping_refreshes_timestamp() {
        let mgr = WebSocketManager::new();
        let (tx, _rx) = new_client_tx();
        let id = mgr.add_client("token".into(), tx);

        let before = mgr.connections.get(&id).map(|c| c.last_ping).unwrap();

        // Small busy-wait to ensure time advances
        std::thread::sleep(std::time::Duration::from_millis(5));

        mgr.update_last_ping(id);

        let after = mgr.connections.get(&id).map(|c| c.last_ping).unwrap();

        assert!(after > before);
    }

    #[test]
    fn update_last_ping_nonexistent_is_noop() {
        let mgr = WebSocketManager::new();
        mgr.update_last_ping(ConnectionId(999));
    }

    #[test]
    fn broadcast_all_delivers_to_all() {
        let mgr = WebSocketManager::new();
        let (tx1, mut rx1) = new_client_tx();
        let (tx2, mut rx2) = new_client_tx();

        mgr.add_client("t1".into(), tx1);
        mgr.add_client("t2".into(), tx2);

        let event = WebSocketMessage::new("test-event", json!({"key": "val"}));
        mgr.broadcast_all(event);

        let msg1 = rx1.try_recv().unwrap();
        let msg2 = rx2.try_recv().unwrap();

        match (&msg1, &msg2) {
            (WsOutbound::Text(t1), WsOutbound::Text(t2)) => {
                assert_eq!(t1, t2);
                assert!(t1.contains("test-event"));
            }
            _ => panic!("expected Text messages"),
        }
    }

    #[test]
    fn broadcast_to_user_never_reaches_another_user() {
        let mgr = WebSocketManager::new();
        let (alice_tx, mut alice_rx) = new_client_tx();
        let (bob_tx, mut bob_rx) = new_client_tx();
        let (expired_tx, mut expired_rx) = new_client_tx();
        mgr.add_client("token-alice".into(), alice_tx);
        mgr.add_client("token-bob".into(), bob_tx);
        mgr.add_client("token-expired".into(), expired_tx);

        let resolver = |token: &str| match token {
            "token-alice" => Some("alice".to_owned()),
            "token-bob" => Some("bob".to_owned()),
            _ => None,
        };
        mgr.broadcast_to_user(
            "alice",
            WebSocketMessage::new("team.changed", json!({ "team_id": "team-a" })),
            &resolver,
        );

        assert!(alice_rx.try_recv().is_ok());
        assert!(bob_rx.try_recv().is_err());
        assert!(expired_rx.try_recv().is_err());
    }

    #[test]
    fn broadcast_all_removes_closed_channels() {
        let mgr = WebSocketManager::new();
        let (tx1, rx1) = new_client_tx();
        let (tx2, _rx2) = new_client_tx();

        mgr.add_client("t1".into(), tx1);
        mgr.add_client("t2".into(), tx2);

        // Drop rx1 to close the channel
        drop(rx1);

        let event = WebSocketMessage::new("test", json!(null));
        mgr.broadcast_all(event);

        // Client 1 should be removed
        assert_eq!(mgr.client_count(), 1);
    }

    #[test]
    fn broadcast_all_handles_full_channel() {
        let mgr = WebSocketManager::new();
        // Use a channel with capacity 1
        let (tx, _rx) = mpsc::channel(1);
        mgr.add_client("tok".into(), tx);

        // Fill the channel
        mgr.broadcast_all(WebSocketMessage::new("e1", json!(null)));
        // This should warn but not remove the client
        mgr.broadcast_all(WebSocketMessage::new("e2", json!(null)));

        assert_eq!(mgr.client_count(), 1);
    }

    #[test]
    fn send_to_delivers_to_target_only() {
        let mgr = WebSocketManager::new();
        let (tx1, mut rx1) = new_client_tx();
        let (tx2, mut rx2) = new_client_tx();

        let id1 = mgr.add_client("t1".into(), tx1);
        mgr.add_client("t2".into(), tx2);

        let msg = WebSocketMessage::new("unicast", json!({"for": "id1"}));
        mgr.send_to(id1, msg);

        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_err());
    }

    #[test]
    fn send_to_nonexistent_is_noop() {
        let mgr = WebSocketManager::new();
        let msg = WebSocketMessage::new("ghost", json!(null));
        mgr.send_to(ConnectionId(999), msg);
    }

    #[test]
    fn send_to_removes_closed_channel() {
        let mgr = WebSocketManager::new();
        let (tx, rx) = new_client_tx();
        let id = mgr.add_client("tok".into(), tx);
        drop(rx);

        mgr.send_to(id, WebSocketMessage::new("test", json!(null)));
        assert_eq!(mgr.client_count(), 0);
    }

    #[test]
    fn heartbeat_tick_sends_ping_to_healthy_connection() {
        let connections = Arc::new(DashMap::new());
        let (tx, mut rx) = new_client_tx();

        connections.insert(
            ConnectionId(1),
            ClientInfo {
                token: "valid".into(),
                last_ping: Instant::now(),
                tx,
            },
        );

        heartbeat_tick(&connections, &always_valid());

        // Should still be connected
        assert_eq!(connections.len(), 1);

        // Should have received a ping
        let msg = rx.try_recv().unwrap();
        match msg {
            WsOutbound::Text(text) => {
                let parsed: serde_json::Value = serde_json::from_str(&text).unwrap();
                assert_eq!(parsed["name"], "ping");
                assert!(parsed["data"]["timestamp"].is_u64());
            }
            _ => panic!("expected Text ping"),
        }
    }

    #[test]
    fn heartbeat_tick_removes_timed_out_connection() {
        let connections = Arc::new(DashMap::new());
        let (tx, mut rx) = new_client_tx();

        // Set last_ping to well past the timeout
        let old_ping = Instant::now() - (HEARTBEAT_TIMEOUT * 2);

        connections.insert(
            ConnectionId(1),
            ClientInfo {
                token: "valid".into(),
                last_ping: old_ping,
                tx,
            },
        );

        heartbeat_tick(&connections, &always_valid());

        // Connection should be removed
        assert_eq!(connections.len(), 0);

        // Should have received realtime heartbeat-timeout event and close as one terminal outbound.
        let msg = rx.try_recv().unwrap();
        match msg {
            WsOutbound::TextThenClose(text, code, reason) => {
                assert_realtime_heartbeat_timeout(&text);
                assert_eq!(code, WebSocketCloseCode::PolicyViolation);
                assert_eq!(reason, "heartbeat timeout");
            }
            other => panic!("expected realtime heartbeat-timeout terminal message, got {other:?}"),
        }
    }

    #[test]
    fn heartbeat_tick_removes_expired_token_connection() {
        let connections = Arc::new(DashMap::new());
        let (tx, mut rx) = new_client_tx();

        connections.insert(
            ConnectionId(1),
            ClientInfo {
                token: "expired-token".into(),
                last_ping: Instant::now(),
                tx,
            },
        );

        heartbeat_tick(&connections, &always_expired());

        // Connection should be removed
        assert_eq!(connections.len(), 0);

        // Should have received realtime auth-expired event and close as one terminal outbound.
        let msg1 = rx.try_recv().unwrap();
        match msg1 {
            WsOutbound::TextThenClose(text, code, reason) => {
                assert_realtime_auth_expired(&text);
                assert_eq!(code, WebSocketCloseCode::PolicyViolation);
                assert_eq!(reason, "token expired");
            }
            _ => panic!("expected realtime auth-expired terminal message"),
        }
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn heartbeat_tick_removes_expired_token_connection_when_terminal_queue_is_full() {
        let connections = Arc::new(DashMap::new());
        let (tx, mut rx) = mpsc::channel(1);

        tx.try_send(WsOutbound::Text("queued".into())).unwrap();
        connections.insert(
            ConnectionId(1),
            ClientInfo {
                token: "expired-token".into(),
                last_ping: Instant::now(),
                tx,
            },
        );

        heartbeat_tick(&connections, &always_expired());

        assert_eq!(connections.len(), 0);
        assert_eq!(rx.try_recv().unwrap(), WsOutbound::Text("queued".into()));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn heartbeat_tick_timeout_takes_priority_over_token_check() {
        let connections = Arc::new(DashMap::new());
        let (tx, mut rx) = new_client_tx();

        // Both timed out AND expired token
        let old_ping = Instant::now() - (HEARTBEAT_TIMEOUT * 2);
        connections.insert(
            ConnectionId(1),
            ClientInfo {
                token: "expired".into(),
                last_ping: old_ping,
                tx,
            },
        );

        heartbeat_tick(&connections, &always_expired());

        assert_eq!(connections.len(), 0);

        // Only heartbeat timeout terminal message (no auth-expired event)
        let msg = rx.try_recv().unwrap();
        match msg {
            WsOutbound::TextThenClose(text, code, reason) => {
                assert_realtime_heartbeat_timeout(&text);
                assert_eq!(code, WebSocketCloseCode::PolicyViolation);
                assert_eq!(reason, "heartbeat timeout");
            }
            other => panic!("expected realtime heartbeat-timeout terminal message, got {other:?}"),
        }
        // No more messages
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn heartbeat_tick_mixed_connections() {
        let connections = Arc::new(DashMap::new());

        // Healthy connection
        let (tx1, _rx1) = new_client_tx();
        connections.insert(
            ConnectionId(1),
            ClientInfo {
                token: "good".into(),
                last_ping: Instant::now(),
                tx: tx1,
            },
        );

        // Timed-out connection
        let (tx2, _rx2) = new_client_tx();
        connections.insert(
            ConnectionId(2),
            ClientInfo {
                token: "good".into(),
                last_ping: Instant::now() - (HEARTBEAT_TIMEOUT * 2),
                tx: tx2,
            },
        );

        let selective_validator: TokenValidator = Arc::new(|_| true);
        heartbeat_tick(&connections, &selective_validator);

        // Only healthy connection remains
        assert_eq!(connections.len(), 1);
        assert!(connections.contains_key(&ConnectionId(1)));
    }

    #[test]
    fn event_broadcaster_impl_delegates_to_broadcast_all() {
        let mgr = WebSocketManager::new();
        let (tx, mut rx) = new_client_tx();
        mgr.add_client("tok".into(), tx);

        let broadcaster: &dyn EventBroadcaster = &mgr;
        broadcaster.broadcast(WebSocketMessage::new("via-trait", json!({})));

        let msg = rx.try_recv().unwrap();
        match msg {
            WsOutbound::Text(text) => {
                assert!(text.contains("via-trait"));
            }
            _ => panic!("expected Text"),
        }
    }

    #[test]
    fn default_creates_empty_manager() {
        let mgr = WebSocketManager::default();
        assert_eq!(mgr.client_count(), 0);
    }
}
