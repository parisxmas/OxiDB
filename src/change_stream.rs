use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, RwLock};

use serde::Serialize;
use serde_json::Value;

use crate::document::DocumentId;

/// Unique identifier for a change stream subscriber.
pub type SubscriberId = u64;

/// Type of mutation that triggered the change event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OperationType {
    Insert,
    Update,
    Delete,
}

/// A change event emitted when a document is mutated.
#[derive(Debug, Clone, Serialize)]
pub struct ChangeEvent {
    /// Monotonic sequence number for resume support.
    pub token: u64,
    pub operation: OperationType,
    pub collection: String,
    pub doc_id: DocumentId,
    /// The document image: inserts (upserts included) carry the inserted
    /// document, updates the post-image, deletes the pre-image — inside
    /// and outside transactions (since 0.42.9; before that only inserts
    /// carried one, and a transactional update was misreported as an
    /// Insert event).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document: Option<Value>,
    /// Transaction ID if the mutation was part of a transaction.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tx_id: Option<u64>,
}

/// Filter controlling which events a subscriber receives.
#[derive(Debug, Clone)]
pub enum WatchFilter {
    /// Receive events from all collections.
    All,
    /// Receive events only from the named collection.
    Collection(String),
}

/// Error returned when a resume token is no longer available in the replay buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResumeError {
    TokenTooOld,
}

impl std::fmt::Display for ResumeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResumeError::TokenTooOld => write!(f, "resume token too old"),
        }
    }
}

impl std::error::Error for ResumeError {}

/// Handle returned from `subscribe()`. Provides access to the event receiver
/// and backpressure metrics.
pub struct WatchHandle {
    pub id: SubscriberId,
    pub rx: Receiver<ChangeEvent>,
    dropped: Arc<AtomicU64>,
    pending: Arc<Mutex<VecDeque<ChangeEvent>>>,
}

impl WatchHandle {
    /// Returns and resets the count of events LOST to backpressure — an
    /// event superseded by a newer one for the same document, or evicted
    /// past the pending cap. A merely *deferred* event (sitting in the
    /// pending queue) is not lost and is not counted.
    pub fn take_dropped(&self) -> u64 {
        self.dropped.swap(0, Ordering::Relaxed)
    }

    /// Drain events that were coalesced under backpressure. The broker
    /// flushes this queue into the channel on every later emit, but a
    /// consumer that has fully drained `rx` should also drain here —
    /// otherwise the last known state of a document could sit deferred
    /// until the next unrelated write. Per-document order is preserved
    /// (an event for a document is never queued here while a newer one
    /// for the same document is in the channel).
    pub fn drain_pending(&self) -> Vec<ChangeEvent> {
        let mut pending = self.pending.lock().unwrap();
        pending.drain(..).collect()
    }
}

struct Subscriber {
    id: SubscriberId,
    filter: WatchFilter,
    sender: SyncSender<ChangeEvent>,
    dropped: Arc<AtomicU64>,
    /// Coalescing buffer for a slow subscriber: when the channel is full,
    /// the newest event per (collection, doc_id) waits here instead of
    /// being dropped — for state-shaped streams (a vehicle's position) the
    /// last event supersedes every earlier one, so a subscriber that falls
    /// behind still converges on current state.
    pending: Arc<Mutex<VecDeque<ChangeEvent>>>,
}

/// Default number of events retained in the replay buffer
/// (`OXIDB_CHANGE_REPLAY_EVENTS` overrides).
const REPLAY_BUFFER_CAPACITY: usize = 4096;

/// Hard cap on a subscriber's coalescing buffer: past this many DISTINCT
/// backlogged documents the oldest deferred event is evicted (and counted
/// dropped) — a slow subscriber may cost bounded memory, never unbounded.
const COALESCE_PENDING_CAP: usize = 4096;

/// Broker that manages change stream subscribers and distributes events.
///
/// Zero-cost when no subscribers: the `subscriber_count` atomic is checked
/// before acquiring any locks.
pub struct ChangeStreamBroker {
    subscribers: RwLock<Vec<Subscriber>>,
    next_id: AtomicU64,
    subscriber_count: AtomicU64,
    next_token: AtomicU64,
    event_log: RwLock<VecDeque<ChangeEvent>>,
    /// Replay buffer capacity — `OXIDB_CHANGE_REPLAY_EVENTS` (default 4096).
    replay_capacity: usize,
}

impl Default for ChangeStreamBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeStreamBroker {
    pub fn new() -> Self {
        let replay_capacity = std::env::var("OXIDB_CHANGE_REPLAY_EVENTS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n: &usize| n > 0)
            .unwrap_or(REPLAY_BUFFER_CAPACITY);
        Self::with_replay_capacity(replay_capacity)
    }

    /// `new()` with an explicit replay-buffer capacity (the env-independent
    /// constructor tests use).
    pub fn with_replay_capacity(replay_capacity: usize) -> Self {
        Self {
            subscribers: RwLock::new(Vec::new()),
            next_id: AtomicU64::new(1),
            subscriber_count: AtomicU64::new(0),
            next_token: AtomicU64::new(1),
            event_log: RwLock::new(VecDeque::new()),
            replay_capacity,
        }
    }

    /// Returns `true` if there are any active subscribers.
    /// This is a cheap atomic load — use it to guard event emission on the hot path.
    #[inline]
    pub fn has_subscribers(&self) -> bool {
        self.subscriber_count.load(Ordering::Relaxed) > 0
    }

    /// Create a new subscription. Returns a `WatchHandle` with the subscriber ID,
    /// event receiver, and backpressure tracking.
    ///
    /// If `resume_after` is `Some(token)`, events with `token > resume_after` that
    /// match the filter are replayed from the buffer into the channel before live
    /// events start flowing. Returns `Err(ResumeError::TokenTooOld)` if the
    /// requested token has been evicted from the buffer.
    pub fn subscribe(
        &self,
        filter: WatchFilter,
        buffer: usize,
        resume_after: Option<u64>,
    ) -> std::result::Result<WatchHandle, ResumeError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::sync_channel(buffer);
        let dropped = Arc::new(AtomicU64::new(0));

        // Replay from event_log if requested
        if let Some(token) = resume_after {
            let log = self.event_log.read().unwrap();
            // Check if the requested token is still in the buffer
            let oldest_token = log.front().map(|e| e.token);
            if let Some(oldest) = oldest_token
                && token < oldest
            {
                return Err(ResumeError::TokenTooOld);
            }
            // Even if log is empty and token > 0, that means no events have been
            // emitted since the token — valid resume with nothing to replay.
            // Replay matching events
            for event in log.iter() {
                if event.token > token && Self::matches_filter(&filter, &event.collection) {
                    // Use try_send; if the channel fills up, the rest will be dropped
                    // (caller chose a small buffer).
                    let _ = tx.try_send(event.clone());
                }
            }
        }

        let pending = Arc::new(Mutex::new(VecDeque::new()));
        let sub = Subscriber {
            id,
            filter,
            sender: tx,
            dropped: Arc::clone(&dropped),
            pending: Arc::clone(&pending),
        };
        self.subscribers.write().unwrap().push(sub);
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);

        Ok(WatchHandle {
            id,
            rx,
            dropped,
            pending,
        })
    }

    /// Remove a subscriber by ID.
    pub fn unsubscribe(&self, id: SubscriberId) {
        let mut subs = self.subscribers.write().unwrap();
        let before = subs.len();
        subs.retain(|s| s.id != id);
        let removed = before - subs.len();
        if removed > 0 {
            self.subscriber_count
                .fetch_sub(removed as u64, Ordering::Relaxed);
        }
    }

    /// Emit an event to all matching subscribers.
    /// Assigns a monotonic token, stores in replay buffer, then fans out.
    /// Uses `try_send` so a slow subscriber never blocks the mutation path.
    /// A full channel COALESCES instead of dropping: the newest event per
    /// (collection, doc_id) waits in the subscriber's pending queue and is
    /// flushed on a later emit (or drained by the consumer) — the event an
    /// older one is replaced by supersedes it, so a lagging subscriber
    /// still converges on every document's last state.
    /// Dead subscribers (disconnected receivers) are lazily cleaned up.
    pub fn emit(&self, mut event: ChangeEvent) {
        // Assign monotonic token
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        event.token = token;

        // Store in replay buffer
        {
            let mut log = self.event_log.write().unwrap();
            if log.len() >= self.replay_capacity {
                log.pop_front();
            }
            log.push_back(event.clone());
        }

        let subs = self.subscribers.read().unwrap();
        let mut dead_ids: Vec<SubscriberId> = Vec::new();

        for sub in subs.iter() {
            if !Self::matches_filter(&sub.filter, &event.collection) {
                continue;
            }
            // Deferred events go first — per-document order must hold, and
            // an event may only enter the pending queue when the channel
            // refuses it, so pending is always older than the channel tail.
            let mut pending = sub.pending.lock().unwrap();
            while let Some(head) = pending.front() {
                match sub.sender.try_send(head.clone()) {
                    Ok(()) => {
                        pending.pop_front();
                    }
                    Err(_) => break,
                }
            }
            match sub.sender.try_send(event.clone()) {
                Ok(()) => {}
                Err(TrySendError::Disconnected(_)) => {
                    dead_ids.push(sub.id);
                }
                Err(TrySendError::Full(ev)) => {
                    if let Some(slot) = pending
                        .iter_mut()
                        .find(|p| p.doc_id == ev.doc_id && p.collection == ev.collection)
                    {
                        // Same document already backlogged: the new event
                        // supersedes it. The replaced one is the loss.
                        *slot = ev;
                        sub.dropped.fetch_add(1, Ordering::Relaxed);
                    } else if pending.len() >= COALESCE_PENDING_CAP {
                        pending.pop_front();
                        pending.push_back(ev);
                        sub.dropped.fetch_add(1, Ordering::Relaxed);
                    } else {
                        // Deferred, not lost.
                        pending.push_back(ev);
                    }
                }
            }
        }

        drop(subs);

        // Lazy cleanup of dead subscribers
        if !dead_ids.is_empty() {
            let mut subs = self.subscribers.write().unwrap();
            let before = subs.len();
            subs.retain(|s| !dead_ids.contains(&s.id));
            let removed = before - subs.len();
            if removed > 0 {
                self.subscriber_count
                    .fetch_sub(removed as u64, Ordering::Relaxed);
            }
        }
    }

    fn matches_filter(filter: &WatchFilter, collection: &str) -> bool {
        match filter {
            WatchFilter::All => true,
            WatchFilter::Collection(name) => name == collection,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::time::Duration;

    fn make_event(op: OperationType, collection: &str, doc_id: DocumentId) -> ChangeEvent {
        ChangeEvent {
            token: 0, // will be assigned by emit()
            operation: op,
            collection: collection.to_string(),
            doc_id,
            document: None,
            tx_id: None,
        }
    }

    #[test]
    fn subscribe_and_receive_event() {
        let broker = ChangeStreamBroker::new();
        let handle = broker.subscribe(WatchFilter::All, 16, None).unwrap();

        broker.emit(ChangeEvent {
            token: 0,
            operation: OperationType::Insert,
            collection: "users".to_string(),
            doc_id: 1,
            document: Some(json!({"_id": 1, "name": "Alice"})),
            tx_id: None,
        });

        let event = handle.rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(event.operation, OperationType::Insert);
        assert_eq!(event.collection, "users");
        assert_eq!(event.doc_id, 1);
        assert!(event.document.is_some());
        assert!(event.token > 0);
    }

    #[test]
    fn collection_filter() {
        let broker = ChangeStreamBroker::new();
        let handle = broker
            .subscribe(WatchFilter::Collection("orders".to_string()), 16, None)
            .unwrap();

        // Emit to "users" — should NOT be received
        broker.emit(ChangeEvent {
            token: 0,
            operation: OperationType::Insert,
            collection: "users".to_string(),
            doc_id: 1,
            document: Some(json!({"_id": 1})),
            tx_id: None,
        });

        // Emit to "orders" — should be received
        broker.emit(ChangeEvent {
            token: 0,
            operation: OperationType::Insert,
            collection: "orders".to_string(),
            doc_id: 2,
            document: Some(json!({"_id": 2})),
            tx_id: None,
        });

        let event = handle.rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(event.collection, "orders");
        assert_eq!(event.doc_id, 2);

        // No more events
        assert!(handle.rx.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn unsubscribe_stops_events() {
        let broker = ChangeStreamBroker::new();
        let handle = broker.subscribe(WatchFilter::All, 16, None).unwrap();

        broker.unsubscribe(handle.id);
        assert!(!broker.has_subscribers());

        broker.emit(ChangeEvent {
            token: 0,
            operation: OperationType::Delete,
            collection: "users".to_string(),
            doc_id: 1,
            document: None,
            tx_id: None,
        });

        assert!(handle.rx.recv_timeout(Duration::from_millis(50)).is_err());
    }

    #[test]
    fn dead_subscriber_cleanup() {
        let broker = ChangeStreamBroker::new();
        let handle = broker.subscribe(WatchFilter::All, 16, None).unwrap();
        assert!(broker.has_subscribers());

        // Drop the receiver to simulate disconnect
        drop(handle);

        // Emit triggers lazy cleanup
        broker.emit(ChangeEvent {
            token: 0,
            operation: OperationType::Update,
            collection: "users".to_string(),
            doc_id: 1,
            document: None,
            tx_id: None,
        });

        assert!(!broker.has_subscribers());
    }

    #[test]
    fn resume_after_replays_missed_events() {
        let broker = ChangeStreamBroker::new();

        // Emit 5 events (no subscribers yet — they go into the replay buffer)
        for i in 1..=5 {
            broker.emit(make_event(OperationType::Insert, "users", i));
        }

        // Subscribe with resume_after = token of event 2
        // Events have tokens 1..=5, so we want events with token > 2 → tokens 3,4,5
        let handle = broker.subscribe(WatchFilter::All, 16, Some(2)).unwrap();

        let mut received = Vec::new();
        while let Ok(event) = handle.rx.recv_timeout(Duration::from_millis(100)) {
            received.push(event);
        }
        assert_eq!(received.len(), 3);
        assert_eq!(received[0].token, 3);
        assert_eq!(received[1].token, 4);
        assert_eq!(received[2].token, 5);
    }

    #[test]
    fn resume_too_old_returns_error() {
        let broker = ChangeStreamBroker::new();

        // Fill buffer beyond capacity so oldest tokens get evicted
        for i in 0..REPLAY_BUFFER_CAPACITY + 100 {
            broker.emit(make_event(OperationType::Insert, "users", i as u64));
        }

        // Try to resume from token 1, which has been evicted
        let result = broker.subscribe(WatchFilter::All, 16, Some(1));
        assert_eq!(result.err(), Some(ResumeError::TokenTooOld));
    }

    #[test]
    fn backpressure_defers_distinct_docs_instead_of_dropping() {
        let broker = ChangeStreamBroker::new();
        let handle = broker.subscribe(WatchFilter::All, 1, None).unwrap();

        // Buffer of 1: the first event fills the channel. Before 0.42.10
        // the second and third were DROPPED; now they wait in the pending
        // queue — nothing is lost, so the dropped counter stays 0.
        broker.emit(make_event(OperationType::Insert, "users", 1));
        broker.emit(make_event(OperationType::Insert, "users", 2));
        broker.emit(make_event(OperationType::Insert, "users", 3));

        assert_eq!(handle.take_dropped(), 0);
        let first = handle.rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(first.doc_id, 1);
        let deferred = handle.drain_pending();
        assert_eq!(
            deferred.iter().map(|e| e.doc_id).collect::<Vec<_>>(),
            vec![2, 3]
        );
    }

    #[test]
    fn backpressure_coalesces_same_doc_to_its_latest_state() {
        // The location-tracking shape: many updates to ONE document while
        // the subscriber lags. The subscriber must converge on the LAST
        // state; the superseded intermediates are the (counted) loss.
        let broker = ChangeStreamBroker::new();
        let handle = broker.subscribe(WatchFilter::All, 1, None).unwrap();

        for speed in 1..=5 {
            broker.emit(ChangeEvent {
                token: 0,
                operation: OperationType::Update,
                collection: "pings".to_string(),
                doc_id: 7,
                document: Some(json!({"_id": 7, "speed": speed})),
                tx_id: None,
            });
        }

        // Channel got speed=1; speeds 2..4 were each superseded in place.
        let first = handle.rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(first.document.unwrap()["speed"], 1);
        let deferred = handle.drain_pending();
        assert_eq!(deferred.len(), 1, "one pending entry per document");
        assert_eq!(deferred[0].document.as_ref().unwrap()["speed"], 5);
        assert_eq!(handle.take_dropped(), 3);
    }

    #[test]
    fn a_later_emit_flushes_the_pending_queue_in_order() {
        let broker = ChangeStreamBroker::new();
        let handle = broker.subscribe(WatchFilter::All, 1, None).unwrap();

        broker.emit(make_event(OperationType::Update, "pings", 1)); // fills channel
        broker.emit(make_event(OperationType::Update, "pings", 2)); // deferred

        // Drain the channel, then emit again: the flush must deliver the
        // deferred doc-2 event BEFORE the new doc-3 event.
        let first = handle.rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(first.doc_id, 1);
        broker.emit(make_event(OperationType::Update, "pings", 3));

        let second = handle.rx.recv_timeout(Duration::from_millis(100)).unwrap();
        assert_eq!(second.doc_id, 2, "pending flushes first");
        // Channel capacity is 1, so doc 3 is now the deferred one.
        assert_eq!(
            handle
                .drain_pending()
                .iter()
                .map(|e| e.doc_id)
                .collect::<Vec<_>>(),
            vec![3]
        );
        assert_eq!(handle.take_dropped(), 0);
    }

    #[test]
    fn pending_cap_evicts_oldest_and_counts_it_dropped() {
        let broker = ChangeStreamBroker::new();
        let handle = broker.subscribe(WatchFilter::All, 1, None).unwrap();

        // Fill the channel, then back up COALESCE_PENDING_CAP + 10 distinct
        // documents. The 10 oldest deferred events must be evicted, counted.
        broker.emit(make_event(OperationType::Insert, "users", 0));
        for i in 0..(COALESCE_PENDING_CAP as u64 + 10) {
            broker.emit(make_event(OperationType::Insert, "users", i + 1));
        }
        assert_eq!(handle.take_dropped(), 10);
        let deferred = handle.drain_pending();
        assert_eq!(deferred.len(), COALESCE_PENDING_CAP);
        assert_eq!(deferred.first().unwrap().doc_id, 11);
    }

    #[test]
    fn replay_capacity_is_configurable() {
        let broker = ChangeStreamBroker::with_replay_capacity(8);
        for i in 0..20 {
            broker.emit(make_event(OperationType::Insert, "users", i));
        }
        // Tokens 1..=20; only the last 8 (13..=20) remain, so resuming from
        // token 5 must refuse.
        assert_eq!(
            broker.subscribe(WatchFilter::All, 16, Some(5)).err(),
            Some(ResumeError::TokenTooOld)
        );
        let handle = broker.subscribe(WatchFilter::All, 16, Some(13)).unwrap();
        let mut got = Vec::new();
        while let Ok(e) = handle.rx.recv_timeout(Duration::from_millis(50)) {
            got.push(e.token);
        }
        assert_eq!(got, (14..=20).collect::<Vec<u64>>());
    }
}
