use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, RwLock};

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
    /// Present for insert operations; `None` for update/delete.
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
}

impl WatchHandle {
    /// Returns and resets the count of events dropped due to backpressure.
    pub fn take_dropped(&self) -> u64 {
        self.dropped.swap(0, Ordering::Relaxed)
    }
}

struct Subscriber {
    id: SubscriberId,
    filter: WatchFilter,
    sender: SyncSender<ChangeEvent>,
    dropped: Arc<AtomicU64>,
}

/// Maximum number of events retained in the replay buffer.
const REPLAY_BUFFER_CAPACITY: usize = 4096;

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
}

impl ChangeStreamBroker {
    pub fn new() -> Self {
        Self {
            subscribers: RwLock::new(Vec::new()),
            next_id: AtomicU64::new(1),
            subscriber_count: AtomicU64::new(0),
            next_token: AtomicU64::new(1),
            event_log: RwLock::new(VecDeque::new()),
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
            if let Some(oldest) = oldest_token {
                if token < oldest {
                    return Err(ResumeError::TokenTooOld);
                }
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

        let sub = Subscriber {
            id,
            filter,
            sender: tx,
            dropped: Arc::clone(&dropped),
        };
        self.subscribers.write().unwrap().push(sub);
        self.subscriber_count.fetch_add(1, Ordering::Relaxed);

        Ok(WatchHandle { id, rx, dropped })
    }

    /// Remove a subscriber by ID.
    pub fn unsubscribe(&self, id: SubscriberId) {
        let mut subs = self.subscribers.write().unwrap();
        let before = subs.len();
        subs.retain(|s| s.id != id);
        let removed = before - subs.len();
        if removed > 0 {
            self.subscriber_count.fetch_sub(removed as u64, Ordering::Relaxed);
        }
    }

    /// Emit an event to all matching subscribers.
    /// Assigns a monotonic token, stores in replay buffer, then fans out.
    /// Uses `try_send` so a slow subscriber never blocks the mutation path.
    /// Dead subscribers (disconnected receivers) are lazily cleaned up.
    pub fn emit(&self, mut event: ChangeEvent) {
        // Assign monotonic token
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        event.token = token;

        // Store in replay buffer
        {
            let mut log = self.event_log.write().unwrap();
            if log.len() >= REPLAY_BUFFER_CAPACITY {
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
            match sub.sender.try_send(event.clone()) {
                Ok(()) => {}
                Err(TrySendError::Disconnected(_)) => {
                    dead_ids.push(sub.id);
                }
                Err(TrySendError::Full(_)) => {
                    sub.dropped.fetch_add(1, Ordering::Relaxed);
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
                self.subscriber_count.fetch_sub(removed as u64, Ordering::Relaxed);
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
        let handle = broker.subscribe(WatchFilter::Collection("orders".to_string()), 16, None).unwrap();

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
    fn backpressure_tracks_dropped() {
        let broker = ChangeStreamBroker::new();
        let handle = broker.subscribe(WatchFilter::All, 1, None).unwrap();

        // Emit 3 events — buffer of 1 means first fills the channel,
        // second and third are dropped
        broker.emit(make_event(OperationType::Insert, "users", 1));
        broker.emit(make_event(OperationType::Insert, "users", 2));
        broker.emit(make_event(OperationType::Insert, "users", 3));

        assert_eq!(handle.take_dropped(), 2);
        // take_dropped resets the counter
        assert_eq!(handle.take_dropped(), 0);
    }
}
