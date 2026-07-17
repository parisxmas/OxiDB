//! Persistent MQTT sessions — the state that outlives a connection.
//!
//! The bare broker (`mqtt.rs`) made the delivery machinery the connection: a
//! SUBSCRIBE handed the socket an `mpsc::Receiver`, and when the socket closed
//! the receiver dropped, so a `PUBLISH` to a momentarily-offline subscriber
//! evaporated. That is at-most-once, and the broker was advertising QoS 1.
//! ADR-0015.
//!
//! This module holds, per client id, the state MQTT 3.1.1 requires a persistent
//! session (`clean_session=false`) to keep across reconnects:
//!
//!   * the subscription set,
//!   * the outbound QoS-1 messages awaiting a PUBACK (so they can be resent),
//!   * a queue of messages that arrived while the client was offline.
//!
//! The pivot that makes offline delivery work: the store subscription's
//! `Receiver` is owned by the **session**, not the connection. `mpsc` buffers
//! while a receiver is alive with no reader, so a publish to an offline session
//! keeps landing in the channel. A connection drains it while attached; a
//! broker-wide reaper drains it into a bounded queue while detached — the bound
//! is what stops a flooding publisher from OOMing an idle session.
//!
//! In-memory only in this phase: a restart is a clean slate, and the docs say
//! so. ADR-0015 Phase 2 mirrors this through the doc-engine WAL for durability.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::mpsc::Receiver;
use std::sync::{Mutex, OnceLock};

/// Per-session cap on buffered offline messages. A slow or vanished consumer
/// must not grow without bound — the same discipline as a bounded WAL. When the
/// queue is full the OLDEST message is dropped: for telemetry the freshest
/// reading is the one worth keeping, and QoS 1 promises "at least once", not
/// "never lost under unbounded backlog against a dead consumer".
pub const MAX_QUEUED: usize = 4096;

/// One live subscription: the filter as written, the granted QoS, the compiled
/// matcher, and the channel the store publishes into. The receiver lives here
/// so it survives a disconnect and keeps buffering.
pub struct Subscription {
    pub filter: String,
    pub qos: u8,
    pub regex: regex::Regex,
    pub rx: Receiver<(String, String)>,
}

/// An outbound QoS-1 message that has been sent but not yet PUBACK'd. Held so it
/// can be retransmitted (with DUP set) on reconnect.
#[derive(Clone)]
pub struct Inflight {
    pub topic: String,
    pub message: String,
}

/// Everything about a client that must outlive its socket.
pub struct Session {
    /// True while a connection is attached and draining live. The reaper skips
    /// connected sessions — their receivers are being drained by the connection.
    pub connected: bool,
    /// Bumped each time a connection attaches. An older connection that lost a
    /// takeover race sees the generation move and exits instead of double-
    /// delivering (MQTT-3.1.4-2: a second CONNECT with the same id wins).
    pub generation: u64,
    pub subs: Vec<Subscription>,
    /// packet id → message awaiting PUBACK. BTreeMap so redelivery is ordered.
    pub inflight: BTreeMap<u16, Inflight>,
    /// Messages that arrived while offline, oldest first — (topic, payload,
    /// qos). Bounded by MAX_QUEUED. The qos rides along so a message resumed
    /// from the queue is delivered at the guarantee its subscription was
    /// granted, not downgraded to 0.
    pub queue: VecDeque<(String, String, u8)>,
    /// Next packet id to hand out for a QoS-1 delivery. Per-session, so ids do
    /// not collide across a resume.
    pub next_pkt_id: u16,
    /// How many messages the bound has discarded — surfaced for observability
    /// so a silently-lossy session is not invisible.
    pub dropped: u64,
}

impl Session {
    fn new() -> Self {
        Session {
            connected: false,
            generation: 0,
            subs: Vec::new(),
            inflight: BTreeMap::new(),
            queue: VecDeque::new(),
            next_pkt_id: 0,
            dropped: 0,
        }
    }

    /// Drain everything currently buffered in the subscription receivers into
    /// the bounded queue. Called by the reaper while the session is offline, and
    /// once more on reconnect before the queue is replayed.
    pub fn drain_into_queue(&mut self) {
        for sub in &self.subs {
            let qos = sub.qos;
            while let Ok((topic, message)) = sub.rx.try_recv() {
                self.queue.push_back((topic, message, qos));
                while self.queue.len() > MAX_QUEUED {
                    self.queue.pop_front();
                    self.dropped += 1;
                }
            }
        }
    }

    fn next_id(&mut self) -> u16 {
        self.next_pkt_id = self.next_pkt_id.wrapping_add(1).max(1);
        self.next_pkt_id
    }

    /// Build the next batch of PUBLISHes to write to the socket, and record the
    /// QoS-1 ones as inflight in the SAME critical section so a crash between
    /// "sent" and "acked" leaves a resendable record — never a lost message and
    /// never a silently-dropped id.
    ///
    /// `resend_inflight` is set on the first call after a (re)connect: every
    /// message not yet PUBACK'd goes out again with DUP set, reusing its
    /// original packet id (MQTT-4.3.2-1).
    pub fn take_deliveries(&mut self, resend_inflight: bool) -> Vec<Delivery> {
        let mut out = Vec::new();
        if resend_inflight {
            for (pid, inf) in &self.inflight {
                out.push(Delivery {
                    topic: inf.topic.clone(),
                    message: inf.message.clone(),
                    qos: 1,
                    pkt_id: *pid,
                    dup: true,
                });
            }
        }
        self.drain_into_queue(); // receivers -> bounded queue
        while let Some((topic, message, qos)) = self.queue.pop_front() {
            if qos >= 1 {
                let pid = self.next_id();
                self.inflight.insert(pid, Inflight { topic: topic.clone(), message: message.clone() });
                out.push(Delivery { topic, message, qos: 1, pkt_id: pid, dup: false });
            } else {
                out.push(Delivery { topic, message, qos: 0, pkt_id: 0, dup: false });
            }
        }
        out
    }
}

/// One PUBLISH to write to the wire.
pub struct Delivery {
    pub topic: String,
    pub message: String,
    pub qos: u8,
    pub pkt_id: u16,
    pub dup: bool,
}

/// The process-wide session registry. MQTT is node-local (ADR-0015), one broker
/// per process, so a single registry behind a `Mutex` is the whole story — no
/// sharding, no per-connection plumbing.
pub struct MqttSessions {
    inner: Mutex<HashMap<String, Session>>,
}

impl MqttSessions {
    fn new() -> Self {
        MqttSessions {
            inner: Mutex::new(HashMap::new()),
        }
    }

    /// The shared registry.
    pub fn global() -> &'static MqttSessions {
        static REG: OnceLock<MqttSessions> = OnceLock::new();
        REG.get_or_init(MqttSessions::new)
    }

    /// Attach a connection to a session, resuming it if one exists.
    ///
    /// Returns `(session_present, generation)`. `clean_start` discards any prior
    /// session first, so `session_present` is then always false. A resumed
    /// session keeps its subscriptions, its inflight messages and its offline
    /// queue — that is the whole point.
    pub fn attach(&self, client_id: &str, clean_start: bool) -> (bool, u64) {
        let mut map = self.inner.lock().unwrap();
        if clean_start {
            map.remove(client_id);
        }
        let existed = map.contains_key(client_id);
        let sess = map.entry(client_id.to_string()).or_insert_with(Session::new);
        sess.connected = true;
        sess.generation = sess.generation.wrapping_add(1);
        (existed && !clean_start, sess.generation)
    }

    /// Run `f` against a session while holding the registry lock. Every
    /// connection-side mutation (add a sub, record an inflight, clear a PUBACK,
    /// drain deliveries) goes through here so the reaper never races it.
    pub fn with<R>(&self, client_id: &str, f: impl FnOnce(&mut Session) -> R) -> Option<R> {
        let mut map = self.inner.lock().unwrap();
        map.get_mut(client_id).map(f)
    }

    /// Detach a connection. A `clean_session` client is forgotten entirely; a
    /// persistent one is marked offline and left for the reaper to keep draining
    /// (its receivers stay alive, so publishes keep arriving into the buffer).
    ///
    /// `generation` guards against a superseded connection detaching the session
    /// that a newer connection now owns.
    pub fn detach(&self, client_id: &str, generation: u64, persistent: bool) {
        let mut map = self.inner.lock().unwrap();
        if let Some(sess) = map.get_mut(client_id) {
            if sess.generation != generation {
                return; // a newer connection owns this session now
            }
            if persistent {
                sess.connected = false;
                sess.drain_into_queue();
            } else {
                map.remove(client_id);
            }
        }
    }

    /// Drain every offline session's receivers into its bounded queue. Called on
    /// a timer by the broker reaper thread; this is what enforces MAX_QUEUED for
    /// a session whose publisher keeps sending while nobody is connected.
    pub fn reap(&self) {
        let mut map = self.inner.lock().unwrap();
        for sess in map.values_mut() {
            if !sess.connected {
                sess.drain_into_queue();
            }
        }
    }
}

/// Spawn the reaper. One thread for the whole broker, like the TTL evictor.
pub fn spawn_reaper() {
    std::thread::Builder::new()
        .name("mqtt-session-reaper".into())
        .spawn(|| {
            let reg = MqttSessions::global();
            loop {
                std::thread::sleep(std::time::Duration::from_millis(500));
                reg.reap();
            }
        })
        .expect("spawn mqtt reaper");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    fn sub(filter: &str) -> Subscription {
        let (_tx, rx) = mpsc::channel();
        Subscription {
            filter: filter.to_string(),
            qos: 1,
            regex: regex::Regex::new("^.*$").unwrap(),
            rx,
        }
    }

    #[test]
    fn a_clean_session_is_forgotten_on_detach() {
        let reg = MqttSessions::new();
        let (_present, g) = reg.attach("c1", true);
        reg.with("c1", |s| s.subs.push(sub("x"))).unwrap();
        reg.detach("c1", g, false); // clean_session = not persistent
        // Re-attaching finds nothing to resume.
        let (present, _) = reg.attach("c1", true);
        assert!(!present);
        assert_eq!(reg.with("c1", |s| s.subs.len()), Some(0));
    }

    #[test]
    fn a_persistent_session_resumes_with_its_state() {
        let reg = MqttSessions::new();
        let (present, g) = reg.attach("c2", false);
        assert!(!present, "first connect has nothing to resume");
        reg.with("c2", |s| {
            s.subs.push(sub("topic"));
            s.inflight.insert(7, Inflight { topic: "t".into(), message: "m".into() });
        });
        reg.detach("c2", g, true); // persistent

        let (present, _) = reg.attach("c2", false);
        assert!(present, "the session must resume");
        assert_eq!(reg.with("c2", |s| s.subs.len()), Some(1));
        assert_eq!(reg.with("c2", |s| s.inflight.len()), Some(1), "inflight survives to be resent");
    }

    #[test]
    fn clean_start_wipes_a_prior_persistent_session() {
        let reg = MqttSessions::new();
        let (_p, g) = reg.attach("c3", false);
        reg.with("c3", |s| s.subs.push(sub("a")));
        reg.detach("c3", g, true);
        // Reconnecting with clean_session=true must discard it.
        let (present, _) = reg.attach("c3", true);
        assert!(!present);
        assert_eq!(reg.with("c3", |s| s.subs.len()), Some(0));
    }

    #[test]
    fn the_offline_queue_is_bounded_and_counts_drops() {
        let reg = MqttSessions::new();
        let (tx, rx) = mpsc::channel();
        let (_p, g) = reg.attach("c4", false);
        reg.with("c4", |s| {
            s.subs.push(Subscription {
                filter: "f".into(),
                qos: 1,
                regex: regex::Regex::new("^.*$").unwrap(),
                rx,
            });
        });
        reg.detach("c4", g, true); // offline

        // Flood far past the bound.
        for i in 0..(MAX_QUEUED + 100) {
            tx.send(("f".into(), format!("{i}"))).unwrap();
        }
        reg.reap();

        let (len, dropped) = reg.with("c4", |s| (s.queue.len(), s.dropped)).unwrap();
        assert_eq!(len, MAX_QUEUED, "the queue is capped");
        assert_eq!(dropped, 100, "the overflow is counted, not silent");
        // Drop-oldest: the newest message survived, the oldest did not.
        let newest = reg.with("c4", |s| s.queue.back().unwrap().1.clone()).unwrap(); // .1 = payload
        assert_eq!(newest, format!("{}", MAX_QUEUED + 99));
    }

    #[test]
    fn a_stale_generation_cannot_detach_the_live_session() {
        let reg = MqttSessions::new();
        let (_p, old_g) = reg.attach("c5", false);
        // A second connect (takeover) bumps the generation.
        let (_p2, _new_g) = reg.attach("c5", false);
        // The old connection tries to detach with its stale generation.
        reg.detach("c5", old_g, false);
        // The session the new connection owns must still be there.
        assert!(reg.with("c5", |s| s.connected).is_some());
    }
}
