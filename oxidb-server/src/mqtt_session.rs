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

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use oxidb::OxiDb;
use serde_json::json;

/// The document collection every persistent-MQTT record lives in. One
/// collection, discriminated by `_kind`: "sub" (a subscription), "msg" (an
/// unacked message), "retain" (a retained payload).
pub const MQTT_COLLECTION: &str = "_mqtt";

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
    /// The durable record's sequence number, when one exists. The PUBACK path
    /// deletes by (client, seq) — exact — because deleting by content collapses
    /// duplicates: publish the same payload twice, ack once, and a content
    /// match would delete both records, quietly breaking at-least-once for the
    /// second copy.
    pub seq: Option<u64>,
    /// Delivery QoS (1 or 2). Decides which acknowledgement retires it: PUBACK
    /// for QoS 1, PUBREC (into `pubrel_pending`) for QoS 2.
    pub qos: u8,
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
    /// qos, durable seq). Bounded by MAX_QUEUED. The qos rides along so a
    /// message resumed from the queue is delivered at the guarantee its
    /// subscription was granted; the seq (None for non-durable bus messages)
    /// ties the entry to its on-disk record.
    pub queue: VecDeque<(String, String, u8, Option<u64>)>,
    /// Next packet id to hand out for a QoS-1 delivery. Per-session, so ids do
    /// not collide across a resume.
    pub next_pkt_id: u16,
    /// How many messages the bound has discarded — surfaced for observability
    /// so a silently-lossy session is not invisible.
    pub dropped: u64,
    /// True when this session's state is mirrored to the doc engine (persist on
    /// and clean_session=false). A durable session's deliveries come from the
    /// durable queue, so it does not also subscribe to the in-memory bus —
    /// otherwise a publish would arrive twice.
    pub durable: bool,
    /// Outbound QoS-2, phase two: packet ids we have received a PUBREC for and
    /// owe a PUBCOMP on. The message itself is gone (PUBREC is the moment the
    /// receiver owns it — that is the exactly-once point); what must survive is
    /// the obligation to (re)send PUBREL until PUBCOMP arrives, or the
    /// subscriber's own state machine wedges on a forever-pending id.
    pub pubrel_pending: BTreeSet<u16>,
    /// Inbound QoS-2 dedup: packet ids of publisher PUBLISHes we have already
    /// fanned out but not yet seen the PUBREL for. A retransmitted PUBLISH
    /// (DUP) with an id in here is re-acked with PUBREC and NOT fanned out
    /// again — this set is the entire difference between "exactly once" and
    /// "at least once with a fancier handshake".
    pub rx2: BTreeSet<u16>,
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
            durable: false,
            pubrel_pending: BTreeSet::new(),
            rx2: BTreeSet::new(),
        }
    }

    /// Drain everything currently buffered in the subscription receivers into
    /// the bounded queue. Called by the reaper while the session is offline, and
    /// once more on reconnect before the queue is replayed.
    pub fn drain_into_queue(&mut self) {
        for sub in &self.subs {
            let qos = sub.qos;
            while let Ok((topic, message)) = sub.rx.try_recv() {
                self.queue.push_back((topic, message, qos, None));
                while self.queue.len() > MAX_QUEUED {
                    self.queue.pop_front();
                    self.dropped += 1;
                }
            }
        }
    }

    fn next_id(&mut self) -> u16 {
        // Skip ids still owned by an unacked delivery or a pending PUBREL —
        // reusing one would splice two unrelated messages into one handshake.
        for _ in 0..=u16::MAX {
            self.next_pkt_id = self.next_pkt_id.wrapping_add(1).max(1);
            let id = self.next_pkt_id;
            if !self.inflight.contains_key(&id) && !self.pubrel_pending.contains(&id) {
                return id;
            }
        }
        self.next_pkt_id // 65535 simultaneously-busy ids: unreachable in practice
    }

    /// Build the next batch of PUBLISHes to write to the socket, and record the
    /// QoS-1 ones as inflight in the SAME critical section so a crash between
    /// "sent" and "acked" leaves a resendable record — never a lost message and
    /// never a silently-dropped id.
    ///
    /// `resend_inflight` is set on the first call after a (re)connect: every
    /// message not yet PUBACK'd goes out again with DUP set, reusing its
    /// original packet id (MQTT-4.3.2-1).
    /// Returns `(publishes, pubrels)`: the PUBLISH frames to write, and — on
    /// the first poll after a reconnect — the packet ids to resend PUBREL for
    /// (QoS-2 deliveries whose PUBREC we saw but whose PUBCOMP we did not).
    pub fn take_deliveries(&mut self, resend_inflight: bool) -> (Vec<Delivery>, Vec<u16>) {
        let mut out = Vec::new();
        let mut rels = Vec::new();
        if resend_inflight {
            for (pid, inf) in &self.inflight {
                out.push(Delivery {
                    topic: inf.topic.clone(),
                    message: inf.message.clone(),
                    qos: inf.qos.max(1),
                    pkt_id: *pid,
                    dup: true,
                });
            }
            rels.extend(self.pubrel_pending.iter().copied());
        }
        self.drain_into_queue(); // receivers -> bounded queue
        while let Some((topic, message, qos, seq)) = self.queue.pop_front() {
            if qos >= 1 {
                let qos = qos.min(2);
                let pid = self.next_id();
                self.inflight.insert(pid, Inflight { topic: topic.clone(), message: message.clone(), seq, qos });
                out.push(Delivery { topic, message, qos, pkt_id: pid, dup: false });
            } else {
                out.push(Delivery { topic, message, qos: 0, pkt_id: 0, dup: false });
            }
        }
        (out, rels)
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
    /// The doc engine, when persistence is enabled (`OXIDB_MQTT_PERSIST`).
    /// Durable sessions mirror their subscriptions and unacked messages here so
    /// they survive a crash — the same WAL the document engine already fsyncs.
    persist: RwLock<Option<Arc<OxiDb>>>,
    /// A single atomic read on the publish hot path: skip all persistence work
    /// unless persistence is on AND at least one durable session exists.
    has_durable: AtomicBool,
    /// Monotonic message sequence, so a durable message has a total order that
    /// survives a restart (resumed past the max seen at recovery).
    seq: AtomicU64,
}

impl MqttSessions {
    fn new() -> Self {
        MqttSessions {
            inner: Mutex::new(HashMap::new()),
            persist: RwLock::new(None),
            has_durable: AtomicBool::new(false),
            seq: AtomicU64::new(0),
        }
    }

    /// Whether a durable session might want this publish. One atomic load —
    /// callable from the publish hot path without touching the registry lock.
    pub fn wants_persist(&self) -> bool {
        self.has_durable.load(Ordering::Relaxed)
    }

    /// Whether persistence is enabled at all (regardless of whether any durable
    /// session exists yet). Checked at CONNECT to decide if a persistent session
    /// should be durable.
    pub fn wants_persist_globally(&self) -> bool {
        self.persist.read().unwrap().is_some()
    }

    fn db(&self) -> Option<Arc<OxiDb>> {
        self.persist.read().unwrap().clone()
    }

    /// Enable persistence and recover any sessions the doc engine remembers.
    /// Called once at startup when `OXIDB_MQTT_PERSIST` is set. Returns how many
    /// sessions and messages were restored.
    pub fn enable_persistence(&self, db: Arc<OxiDb>) -> (usize, usize) {
        *self.persist.write().unwrap() = Some(Arc::clone(&db));
        self.recover(&db)
    }

    /// Rebuild sessions from the doc engine: each "sub" record recreates an
    /// offline durable session with its filter, and each "msg" record becomes a
    /// queued message to redeliver. A durable session has no in-memory bus
    /// subscription — its receiver is a dead channel — so recovery does not need
    /// the store at all.
    fn recover(&self, db: &Arc<OxiDb>) -> (usize, usize) {
        let subs = db.find(MQTT_COLLECTION, &json!({ "_kind": "sub" })).unwrap_or_default();
        let msgs = db.find(MQTT_COLLECTION, &json!({ "_kind": "msg" })).unwrap_or_default();

        let mut map = self.inner.lock().unwrap();
        let mut max_seq = 0u64;
        for doc in &subs {
            let (Some(client), Some(filter), Some(qos)) = (
                doc.get("client").and_then(|v| v.as_str()),
                doc.get("filter").and_then(|v| v.as_str()),
                doc.get("qos").and_then(|v| v.as_u64()),
            ) else {
                continue;
            };
            let re = if crate::mqtt::has_wildcard(filter) {
                match crate::mqtt::filter_to_regex(filter) {
                    Some(re) => re,
                    None => continue,
                }
            } else {
                match regex::Regex::new(&format!("^{}$", regex::escape(filter))) {
                    Ok(re) => re,
                    Err(_) => continue,
                }
            };
            let (_dead_tx, dead_rx) = std::sync::mpsc::channel();
            let sess = map.entry(client.to_string()).or_insert_with(Session::new);
            sess.durable = true;
            sess.connected = false;
            sess.subs.push(Subscription {
                filter: filter.to_string(),
                qos: qos as u8,
                regex: re,
                rx: dead_rx,
            });
        }
        let mut restored_msgs = 0;
        let mut per_client: HashMap<String, Vec<(u64, String, String, u8)>> = HashMap::new();
        for doc in &msgs {
            let (Some(client), Some(seq), Some(topic), Some(payload)) = (
                doc.get("client").and_then(|v| v.as_str()),
                doc.get("seq").and_then(|v| v.as_u64()),
                doc.get("topic").and_then(|v| v.as_str()),
                doc.get("payload").and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            let qos = doc.get("qos").and_then(|v| v.as_u64()).unwrap_or(1) as u8;
            max_seq = max_seq.max(seq);
            per_client.entry(client.to_string()).or_default().push((
                seq,
                topic.to_string(),
                payload.to_string(),
                qos,
            ));
        }
        for (client, mut list) in per_client {
            list.sort_by_key(|(seq, ..)| *seq); // redeliver in receipt order
            // Enforce the bound at recovery too: a crash window can leave more
            // records than MAX_QUEUED, and restoring past the bound would undo
            // it. Keep the newest, delete the rest from disk (drop-oldest, the
            // same policy as the live path).
            let excess = list.len().saturating_sub(MAX_QUEUED);
            if let Some(sess) = map.get_mut(&client) {
                for (seq, _t, _p, _q) in list.drain(..excess) {
                    let _ = db.delete(
                        MQTT_COLLECTION,
                        &json!({ "_kind": "msg", "client": client, "seq": seq }),
                    );
                    sess.dropped += 1;
                }
                for (seq, topic, payload, qos) in list {
                    sess.queue.push_back((topic, payload, qos, Some(seq)));
                    restored_msgs += 1;
                }
            }
        }
        // The QoS-2 halves: PUBREL debts (outbound) and dedup ids (inbound).
        // Both are per-client sets; a client can have these with no live subs
        // (it unsubscribed mid-handshake), so entries create sessions too.
        for kind in ["rel", "rx2"] {
            for doc in db.find(MQTT_COLLECTION, &json!({ "_kind": kind })).unwrap_or_default() {
                let (Some(client), Some(pkt_id)) = (
                    doc.get("client").and_then(|v| v.as_str()),
                    doc.get("pkt_id").and_then(|v| v.as_u64()),
                ) else {
                    continue;
                };
                let sess = map.entry(client.to_string()).or_insert_with(Session::new);
                sess.durable = true;
                sess.connected = false;
                if kind == "rel" {
                    sess.pubrel_pending.insert(pkt_id as u16);
                } else {
                    sess.rx2.insert(pkt_id as u16);
                }
            }
        }
        self.seq.store(max_seq + 1, Ordering::SeqCst);
        self.has_durable.store(!map.is_empty(), Ordering::Relaxed);
        (map.len(), restored_msgs)
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
        self.attach_durable(client_id, clean_start, false)
    }

    /// `attach`, marking the session durable (persistence on + persistent). A
    /// clean_start on a durable client also wipes its persisted records, so a
    /// deliberate fresh start does not silently resume from disk.
    pub fn attach_durable(&self, client_id: &str, clean_start: bool, durable: bool) -> (bool, u64) {
        if clean_start && durable {
            if let Some(db) = self.db() {
                let _ = db.delete(MQTT_COLLECTION, &json!({ "client": client_id }));
            }
        }
        let mut map = self.inner.lock().unwrap();
        if clean_start {
            map.remove(client_id);
        }
        let existed = map.contains_key(client_id);
        let sess = map.entry(client_id.to_string()).or_insert_with(Session::new);
        sess.connected = true;
        sess.durable = durable;
        sess.generation = sess.generation.wrapping_add(1);
        if durable {
            self.has_durable.store(true, Ordering::Relaxed);
        }
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

    /// Persist a durable session's subscription so it is restored on restart.
    /// A no-op when persistence is off.
    pub fn persist_subscription(&self, client_id: &str, filter: &str, qos: u8) {
        if let Some(db) = self.db() {
            // Replace any prior record for this (client, filter) so a QoS change
            // does not leave two.
            let _ = db.delete(
                MQTT_COLLECTION,
                &json!({ "_kind": "sub", "client": client_id, "filter": filter }),
            );
            let _ = db.insert(
                MQTT_COLLECTION,
                json!({ "_kind": "sub", "client": client_id, "filter": filter, "qos": qos }),
            );
            self.has_durable.store(true, Ordering::Relaxed);
        }
    }

    /// Remove a durable subscription (on UNSUBSCRIBE).
    pub fn forget_subscription(&self, client_id: &str, filter: &str) {
        if let Some(db) = self.db() {
            let _ = db.delete(
                MQTT_COLLECTION,
                &json!({ "_kind": "sub", "client": client_id, "filter": filter }),
            );
        }
    }

    /// Route a publish to every durable session whose subscription matches, and
    /// **make it durable before returning**. The caller (the PUBLISH handler)
    /// only PUBACKs the publisher after this returns, so an acknowledged QoS-1
    /// message is on disk before the publisher is told it was accepted — the
    /// write-before-ack ordering QoS 1 requires. ADR-0015.
    ///
    /// Cheap when idle: one atomic load rejects the common case where no durable
    /// session exists, before any lock or disk touch.
    pub fn persist_incoming(&self, topic: &str, message: &str, pub_qos: u8) {
        if !self.wants_persist() {
            return;
        }
        let Some(db) = self.db() else { return };

        // Collect the durable sessions this matches, and the seq to stamp each,
        // under the registry lock; do the fsync insert outside it so a slow disk
        // does not stall every other session.
        let mut writes: Vec<(String, u64, u8)> = Vec::new();
        let mut overflow: Vec<(String, u64)> = Vec::new();
        {
            let mut map = self.inner.lock().unwrap();
            for (client, sess) in map.iter_mut() {
                if !sess.durable {
                    continue;
                }
                // Highest granted qos among matching filters (deliver once).
                let mut qos: Option<u8> = None;
                for sub in &sess.subs {
                    if sub.regex.is_match(topic) {
                        qos = Some(qos.map_or(sub.qos, |q| q.max(sub.qos)));
                    }
                }
                let Some(qos) = qos else { continue };
                // MQTT delivers at min(publish qos, granted qos): a QoS-0
                // publish does not acquire a handshake by matching a QoS-2
                // subscription. It is still QUEUED for the offline session —
                // the broker may store QoS-0 for offline delivery, and doing
                // so is the point of a persistent session at an ingestion
                // edge — it just delivers without one.
                let qos = qos.min(pub_qos);
                let seq = self.seq.fetch_add(1, Ordering::SeqCst);
                writes.push((client.clone(), seq, qos));
                // Enqueue in memory now so a connected session delivers promptly;
                // the durable record below is what survives a crash.
                sess.queue.push_back((topic.to_string(), message.to_string(), qos, Some(seq)));
                while sess.queue.len() > MAX_QUEUED {
                    // The bound applies to disk too: a dropped message's durable
                    // record must go with it, or a dead consumer grows the
                    // collection forever and a restart resurrects messages the
                    // bound already discarded.
                    if let Some((_, _, _, Some(dropped_seq))) = sess.queue.pop_front() {
                        overflow.push((client.clone(), dropped_seq));
                    }
                    sess.dropped += 1;
                }
            }
        }
        for (client, seq, qos) in writes {
            // fsync'd insert — durable on return (strict ACID-D).
            let _ = db.insert(
                MQTT_COLLECTION,
                json!({
                    "_kind": "msg", "client": client, "seq": seq,
                    "topic": topic, "payload": message, "qos": qos,
                }),
            );
        }
        for (client, seq) in overflow {
            let _ = db.delete(
                MQTT_COLLECTION,
                &json!({ "_kind": "msg", "client": client, "seq": seq }),
            );
        }
    }

    /// A durable message was acknowledged by its subscriber (PUBACK) — delete
    /// exactly its record, by (client, seq), so it is not redelivered after a
    /// restart. Inflight carries the seq precisely so this can be exact; a
    /// content match would collapse duplicate payloads (see `Inflight::seq`).
    /// No seq (a retained-delivery inflight, or a non-durable session) — no
    /// record to delete.
    pub fn ack_durable(&self, client_id: &str, seq: Option<u64>) {
        let Some(seq) = seq else { return };
        if let Some(db) = self.db() {
            let _ = db.delete(
                MQTT_COLLECTION,
                &json!({ "_kind": "msg", "client": client_id, "seq": seq }),
            );
        }
    }

    /// Persist a pending-PUBREL obligation (outbound QoS 2, after PUBREC). The
    /// message doc is deleted at PUBREC — the receiver owns the message from
    /// that moment — but the PUBREL debt must survive a crash or the
    /// subscriber's state machine wedges on the id.
    pub fn persist_rel(&self, client_id: &str, pkt_id: u16) {
        if let Some(db) = self.db() {
            let _ = db.insert(
                MQTT_COLLECTION,
                json!({ "_kind": "rel", "client": client_id, "pkt_id": pkt_id }),
            );
        }
    }

    /// PUBCOMP arrived — the QoS-2 exchange is complete.
    pub fn forget_rel(&self, client_id: &str, pkt_id: u16) {
        if let Some(db) = self.db() {
            let _ = db.delete(
                MQTT_COLLECTION,
                &json!({ "_kind": "rel", "client": client_id, "pkt_id": pkt_id }),
            );
        }
    }

    /// Persist an inbound QoS-2 dedup id (between the publisher's PUBLISH and
    /// its PUBREL). Without this a durable publisher that reconnects after a
    /// broker crash and retransmits gets its message fanned out twice —
    /// exactly-once has to survive the crash too.
    pub fn persist_rx2(&self, client_id: &str, pkt_id: u16) {
        if let Some(db) = self.db() {
            let _ = db.insert(
                MQTT_COLLECTION,
                json!({ "_kind": "rx2", "client": client_id, "pkt_id": pkt_id }),
            );
        }
    }

    /// The publisher's PUBREL arrived — the dedup window for this id is over.
    pub fn forget_rx2(&self, client_id: &str, pkt_id: u16) {
        if let Some(db) = self.db() {
            let _ = db.delete(
                MQTT_COLLECTION,
                &json!({ "_kind": "rx2", "client": client_id, "pkt_id": pkt_id }),
            );
        }
    }

    /// Persist / clear a retained message so the last value on a topic survives a
    /// restart. Empty payload clears (MQTT-3.3.1-11).
    pub fn persist_retained(&self, topic: &str, message: &str) {
        if let Some(db) = self.db() {
            let _ = db.delete(MQTT_COLLECTION, &json!({ "_kind": "retain", "topic": topic }));
            if !message.is_empty() {
                let _ = db.insert(
                    MQTT_COLLECTION,
                    json!({ "_kind": "retain", "topic": topic, "payload": message }),
                );
            }
        }
    }

    /// Restore retained messages into the store on startup.
    pub fn recover_retained(&self, store: &crate::oximem::OxiMemStore) {
        if let Some(db) = self.db() {
            for doc in db.find(MQTT_COLLECTION, &json!({ "_kind": "retain" })).unwrap_or_default() {
                if let (Some(t), Some(p)) = (
                    doc.get("topic").and_then(|v| v.as_str()),
                    doc.get("payload").and_then(|v| v.as_str()),
                ) {
                    store.retain_set(t, p);
                }
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
            s.inflight.insert(7, Inflight { topic: "t".into(), message: "m".into(), seq: None, qos: 1 });
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
