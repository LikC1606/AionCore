//! Process-wide, cancellation-safe scheduling at managed mathematics prompt boundaries.
use std::{
    collections::{HashSet, VecDeque},
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::oneshot;

type Waiting = (u64, oneshot::Sender<()>);
#[derive(Default)]
struct State {
    next_id: u64,
    active: HashSet<u64>,
    groups: VecDeque<(String, VecDeque<Waiting>)>,
    last_group: Option<String>,
}

pub(crate) struct Gate {
    concurrency: usize,
    state: Mutex<State>,
}
pub(crate) struct Permit {
    gate: Arc<Gate>,
    id: u64,
}

impl State {
    fn dispatch(&mut self, concurrency: usize) {
        while self.active.len() < concurrency && !self.groups.is_empty() {
            if self.groups.len() > 1 && self.groups.front().map(|g| &g.0) == self.last_group.as_ref() {
                self.groups.rotate_left(1);
            }
            let (name, mut queue) = self.groups.pop_front().expect("nonempty queue");
            if let Some((id, sender)) = queue.pop_front()
                && sender.send(()).is_ok()
            {
                self.active.insert(id);
                self.last_group = Some(name.clone());
            }
            if !queue.is_empty() {
                self.groups.push_back((name, queue));
            }
        }
    }
}

impl Gate {
    fn new(concurrency: usize) -> Arc<Self> {
        Arc::new(Self {
            concurrency,
            state: Mutex::new(State::default()),
        })
    }

    async fn acquire(self: &Arc<Self>, group: String) -> Permit {
        let (sender, receiver) = oneshot::channel();
        let ticket = {
            let mut state = self.state.lock().expect("math turn gate poisoned");
            let id = state.next_id;
            state.next_id += 1;
            if let Some((_, queue)) = state.groups.iter_mut().find(|g| g.0 == group) {
                queue.push_back((id, sender));
            } else {
                state.groups.push_back((group, VecDeque::from([(id, sender)])));
            }
            state.dispatch(self.concurrency);
            Permit { gate: self.clone(), id }
        };
        // The ticket exists across the await, so cancellation removes both queued and granted work.
        receiver.await.expect("math gate retains queued senders");
        ticket
    }
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().expect("math turn gate poisoned");
        state.active.remove(&self.id);
        for (_, queue) in &mut state.groups {
            queue.retain(|(id, _)| *id != self.id);
        }
        state.groups.retain(|(_, queue)| !queue.is_empty());
        state.dispatch(self.gate.concurrency);
    }
}

fn global() -> &'static Option<Arc<Gate>> {
    static INSTANCE: OnceLock<Option<Arc<Gate>>> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        if std::env::var("DEEPSCIENTIST_MATH_REQUEST_GATE_URL").is_ok() {
            return None;
        }
        let raw = std::env::var("DEEPSCIENTIST_MATH_FAIR_TURN_CONCURRENCY").ok()?;
        match raw.parse::<usize>() {
            Ok(concurrency) if concurrency > 0 => {
                tracing::info!(concurrency, "Managed mathematics fair prompt scheduling enabled");
                Some(Gate::new(concurrency))
            }
            _ => {
                tracing::warn!("Invalid mathematics fair prompt concurrency; capability disabled");
                None
            }
        }
    })
}

pub(crate) fn capability() -> aionui_api_types::MathFairTurnGateCapability {
    aionui_api_types::MathFairTurnGateCapability {
        enabled: global().is_some(),
        concurrency: global().as_ref().map_or(0, |gate| gate.concurrency),
        scope: "math_run_root".to_owned(),
    }
}

pub(crate) async fn acquire(root: &Path) -> Option<Permit> {
    match global() {
        Some(gate) => Some(gate.acquire(root.to_string_lossy().into_owned()).await),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::task::Poll;

    #[tokio::test]
    async fn rotates_groups_and_releases_cancelled_waiters() {
        let gate = Gate::new(1);
        let first = gate.acquire("a".into()).await;
        let mut a = Box::pin(gate.acquire("a".into()));
        let mut b = Box::pin(gate.acquire("b".into()));
        assert!(matches!(
            std::future::poll_fn(|cx| Poll::Ready(a.as_mut().poll(cx))).await,
            Poll::Pending
        ));
        assert!(matches!(
            std::future::poll_fn(|cx| Poll::Ready(b.as_mut().poll(cx))).await,
            Poll::Pending
        ));
        drop(first);
        let second = b.await;
        assert_eq!(gate.state.lock().unwrap().active.len(), 1);
        drop(a);
        drop(second);
        assert!(gate.state.lock().unwrap().active.is_empty());
        let _next = gate.acquire("c".into()).await;
    }

    #[tokio::test]
    async fn cancellation_after_grant_does_not_leak_capacity() {
        let gate = Gate::new(2);
        let first = gate.acquire("a".into()).await;
        let second = gate.acquire("b".into()).await;
        let mut waiting = Box::pin(gate.acquire("c".into()));
        assert!(matches!(
            std::future::poll_fn(|cx| Poll::Ready(waiting.as_mut().poll(cx))).await,
            Poll::Pending
        ));
        drop(first);
        drop(waiting);
        assert_eq!(gate.state.lock().unwrap().active.len(), 1);
        drop(second);
        assert!(gate.state.lock().unwrap().groups.is_empty());
    }
}
