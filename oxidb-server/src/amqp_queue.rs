//! The AMQP broker state: exchanges, queues, competing consumers. ADR-0016.
//!
//! This is the semantic MQTT cannot express and this protocol exists for: an
//! MQTT subscription copies every message to every matching subscriber, while
//! an AMQP queue hands each message to exactly ONE of its consumers — work
//! distribution. The queue is therefore a shared object with a consumer list
//! and a round-robin pointer, not a per-client session.
//!
//! Durability follows the protocol, not an env var (the deliberate divergence
//! from MQTT, argued in the ADR): a queue declared `durable` whose messages
//! arrive with `delivery_mode=2` mirrors them to the `_amqp` collection, and
//! the publisher's confirm is only sent after that fsync'd insert — the same
//! write-before-ack rule as the MQTT broker's PUBACK. Everything else lives
//! and dies in memory, which is what the client asked for by not saying
//! `durable`.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, RwLock};

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use oxidb::OxiDb;
use serde_json::{json, Value};

use crate::oximem::OxiMemStore;

/// The document collection durable AMQP state lives in. Separate from MQTT's
/// `_mqtt` so neither broker can corrupt the other's recovery.
pub const AMQP_COLLECTION: &str = "_amqp";

/// The MQTT ↔ AMQP bridge exchange (ADR-0016 Phase 3): every MQTT publish
/// routes into this topic exchange with `/` → `.`, and every AMQP publish TO
/// it comes out on the MQTT side with `.` → `/` — the same mapping RabbitMQ's
/// own MQTT plugin uses, and the same pre-declared name. Pre-declared by
/// `enable()`, so binding a queue to it needs no declare.
pub const BRIDGE_EXCHANGE: &str = "amq.topic";

/// Ready-queue bound, drop-oldest, drops counted — the same discipline as the
/// MQTT session queue: a producer outrunning a dead consumer must cost bounded
/// memory, and the loss must be countable rather than silent.
pub const MAX_READY: usize = 100_000;

/// A channel-level protocol error: (reply-code, reply-text). The connection
/// layer turns this into Channel.Close — the honest answer the ADR requires
/// for anything outside the Phase 1 subset.
pub type ChannelError = (u16, String);

pub const NOT_FOUND: u16 = 404;
pub const PRECONDITION_FAILED: u16 = 406;
pub const NOT_IMPLEMENTED: u16 = 540;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ExchangeKind {
    Direct,
    Fanout,
    Topic,
}

impl ExchangeKind {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "direct" => Some(ExchangeKind::Direct),
            "fanout" => Some(ExchangeKind::Fanout),
            "topic" => Some(ExchangeKind::Topic),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            ExchangeKind::Direct => "direct",
            ExchangeKind::Fanout => "fanout",
            ExchangeKind::Topic => "topic",
        }
    }
}

/// AMQP topic matching: the pattern and key are `.`-separated words, `*`
/// matches exactly one word, `#` matches zero or more.
pub fn topic_matches(pattern: &str, key: &str) -> bool {
    fn rec(p: &[&str], k: &[&str]) -> bool {
        match (p.first(), k.first()) {
            (None, None) => true,
            (Some(&"#"), _) => rec(&p[1..], k) || (!k.is_empty() && rec(p, &k[1..])),
            (Some(&"*"), Some(_)) => rec(&p[1..], &k[1..]),
            (Some(&w), Some(&kw)) if w == kw => rec(&p[1..], &k[1..]),
            _ => false,
        }
    }
    let p: Vec<&str> = pattern.split('.').collect();
    let k: Vec<&str> = key.split('.').collect();
    rec(&p, &k)
}

#[derive(Clone)]
pub struct QMsg {
    /// Durable doc sequence, when one exists. Ack deletes by (queue, seq) —
    /// exact, for the same duplicate-collapse reason as the MQTT broker.
    pub seq: Option<u64>,
    pub exchange: String,
    pub routing_key: String,
    /// The content header's property section, verbatim — replayed on delivery
    /// so content-type, correlation ids and application headers survive
    /// without the broker modelling any of them.
    pub props_raw: Vec<u8>,
    pub body: Vec<u8>,
    pub redelivered: bool,
}

struct Consumer {
    conn_id: u64,
    channel: u16,
    ctag: String,
    no_ack: bool,
}

struct Queue {
    durable: bool,
    /// Owning connection for an `exclusive` queue — deleted when it closes.
    exclusive_conn: Option<u64>,
    auto_delete: bool,
    /// True once the queue has had a consumer; auto-delete fires when the
    /// count returns to zero, not before the first consumer ever arrives.
    had_consumer: bool,
    ready: VecDeque<QMsg>,
    consumers: Vec<Consumer>,
    /// Round-robin pointer for competing consumers.
    rr: usize,
    dropped: u64,
}

impl Queue {
    fn new(durable: bool, exclusive_conn: Option<u64>, auto_delete: bool) -> Self {
        Queue {
            durable,
            exclusive_conn,
            auto_delete,
            had_consumer: false,
            ready: VecDeque::new(),
            consumers: Vec::new(),
            rr: 0,
            dropped: 0,
        }
    }
}

struct Exchange {
    durable: bool,
    kind: ExchangeKind,
    /// (routing key, queue name). Routing depends on the kind: direct = exact
    /// key match, fanout = every binding, topic = wildcard match. The default
    /// exchange "" is not stored here — it routes by queue name and cannot be
    /// declared, bound or deleted.
    bindings: Vec<(String, String)>,
}

/// One wire publish, routed but not yet committed: which queues it goes to
/// and the durable seq reserved per durable target. Produced by
/// `prepare_publish`, consumed by `commit_publishes` — the split that lets
/// the connection layer batch a pipeline burst into one fsync.
pub struct PreparedPublish {
    exchange: String,
    routing_key: String,
    props_raw: Vec<u8>,
    body: Vec<u8>,
    targets: Vec<(String, Option<u64>)>,
    /// Forward to MQTT on commit (wire publishes to amq.topic only — an
    /// MQTT-origin publish must not loop back out).
    bridge: bool,
    delivery_mode: Option<u8>,
}

/// One message the connection layer should write to a consumer.
pub struct OutMsg {
    pub channel: u16,
    pub ctag: String,
    pub no_ack: bool,
    pub queue: String,
    pub msg: QMsg,
}

/// Cross-CONNECTION fsync grouping for durable publishes. Each connection
/// already batches its own pipeline burst into one `insert_many`; this layer
/// merges bursts arriving from different connections at the same time.
/// Whoever finds no commit in flight becomes the leader and keeps taking
/// rounds — everything in `pending` — until it is empty; everyone else piles
/// on and waits for the round carrying their docs. Concurrency IS the batch
/// window (the fsync's own duration), so a lone publisher is its own leader
/// immediately and pays nothing for this.
#[derive(Default)]
struct GroupCommit {
    state: Mutex<GcState>,
    done: Condvar,
}

#[derive(Default)]
struct GcState {
    pending: Vec<Value>,
    leader_running: bool,
    /// Generation the current `pending` set will commit under.
    next_gen: u64,
    committed_gen: u64,
}

impl GroupCommit {
    /// Block until `docs` are committed — possibly by another connection's
    /// leader round. `commit` runs WITHOUT the lock held; the leader may run
    /// it several times (one per round).
    fn submit<F: Fn(Vec<Value>)>(&self, docs: Vec<Value>, commit: F) {
        let mut st = self.state.lock().unwrap();
        st.pending.extend(docs);
        let my_gen = st.next_gen;
        if st.leader_running {
            // A leader is on the disk right now; it (or its successor round)
            // will carry our docs. Wait for our generation.
            while st.committed_gen < my_gen {
                st = self.done.wait(st).unwrap();
            }
            return;
        }
        st.leader_running = true;
        loop {
            let round = std::mem::take(&mut st.pending);
            let round_gen = st.next_gen;
            st.next_gen += 1;
            drop(st);
            commit(round);
            st = self.state.lock().unwrap();
            st.committed_gen = round_gen;
            self.done.notify_all();
            if st.pending.is_empty() {
                st.leader_running = false;
                return;
            }
            // More piled up while we were on disk: take another round — we
            // are still the leader, and they are already waiting on us.
        }
    }
}

/// A consuming connection's wake handle: one end of a nonblocking pipe whose
/// other end sits in that connection's `poll(2)` set. A publish that lands in
/// a queue pokes every consumer's pipe, so delivery happens NOW instead of at
/// the next poll tick — the cross-thread wakeup that takes end-to-end latency
/// from tick-bounded to push-like. Unix only; elsewhere it is a no-op and the
/// adaptive tick in `amqp.rs` bounds latency instead.
pub struct Waker {
    #[cfg(unix)]
    tx: std::os::unix::net::UnixStream,
}

impl Waker {
    #[cfg(unix)]
    pub fn new(tx: std::os::unix::net::UnixStream) -> Self {
        Waker { tx }
    }
    #[cfg(not(unix))]
    pub fn new() -> Self {
        Waker {}
    }

    fn wake(&self) {
        #[cfg(unix)]
        {
            use std::io::Write;
            // Nonblocking; a full pipe means a wake is already pending,
            // which is exactly as good as another byte would be.
            let _ = (&self.tx).write(&[1]);
        }
    }
}

#[derive(Default)]
struct BrokerState {
    exchanges: HashMap<String, Exchange>,
    queues: HashMap<String, Queue>,
    /// Basic.Qos prefetch limits, per (connection, channel). 0 or absent =
    /// unlimited, per spec.
    prefetch: HashMap<(u64, u16), u32>,
    /// Deliveries outstanding (sent, not yet settled) against each channel's
    /// prefetch limit. no-ack deliveries are never counted — the spec exempts
    /// them, there being nothing outstanding to wait for.
    inflight: HashMap<(u64, u16), u32>,
}

pub struct AmqpBroker {
    inner: Mutex<BrokerState>,
    persist: RwLock<Option<std::sync::Arc<OxiDb>>>,
    /// True once the AMQP listener is up (`enable` ran). The MQTT publish
    /// hot path checks this one atomic before touching the broker at all —
    /// the bridge must cost nothing when AMQP is off.
    active: AtomicBool,
    /// The OxiMem/MQTT bus, when the bridge is attached — the AMQP → MQTT
    /// direction publishes into it.
    mqtt: RwLock<Option<Arc<OxiMemStore>>>,
    /// Per-connection wake handles (see `Waker`). NEVER locked while `inner`
    /// is held the other way around: callers collect conn ids under `inner`,
    /// release it, then wake.
    wakers: Mutex<HashMap<u64, Waker>>,
    /// Cross-connection fsync grouping for durable publishes (see
    /// `GroupCommit`). Its lock is independent of `inner` and never held
    /// together with it.
    group: GroupCommit,
    seq: AtomicU64,
    gen_name: AtomicU64,
}

impl AmqpBroker {
    fn new() -> Self {
        AmqpBroker {
            inner: Mutex::new(BrokerState::default()),
            persist: RwLock::new(None),
            active: AtomicBool::new(false),
            mqtt: RwLock::new(None),
            wakers: Mutex::new(HashMap::new()),
            group: GroupCommit::default(),
            seq: AtomicU64::new(1),
            gen_name: AtomicU64::new(1),
        }
    }

    /// Register a connection's wake handle. Removed by `connection_closed`.
    pub fn register_waker(&self, conn_id: u64, w: Waker) {
        self.wakers.lock().unwrap().insert(conn_id, w);
    }

    /// Poke each listed connection's wake pipe. Callers collect the ids under
    /// the `inner` lock, RELEASE it, then call this.
    fn wake_conns(&self, ids: &std::collections::HashSet<u64>) {
        if ids.is_empty() {
            return;
        }
        let wakers = self.wakers.lock().unwrap();
        for id in ids {
            if let Some(w) = wakers.get(id) {
                w.wake();
            }
        }
    }

    pub fn global() -> &'static AmqpBroker {
        static B: OnceLock<AmqpBroker> = OnceLock::new();
        B.get_or_init(AmqpBroker::new)
    }

    fn db(&self) -> Option<std::sync::Arc<OxiDb>> {
        self.persist.read().unwrap().clone()
    }

    /// Attach the MQTT/OxiMem bus for the AMQP → MQTT bridge direction.
    pub fn attach_mqtt(&self, store: Arc<OxiMemStore>) {
        *self.mqtt.write().unwrap() = Some(store);
    }

    #[cfg(test)]
    fn activate_for_tests(&self) {
        self.active.store(true, Ordering::SeqCst);
    }

    /// Attach the doc engine and recover durable state. Returns
    /// (queues, messages) restored.
    pub fn enable(&self, db: std::sync::Arc<OxiDb>) -> (usize, usize) {
        *self.persist.write().unwrap() = Some(std::sync::Arc::clone(&db));
        self.active.store(true, Ordering::SeqCst);
        let mut st = self.inner.lock().unwrap();
        // The bridge exchange exists from the first moment, like RabbitMQ's
        // pre-declared amq.* set — binding to it must need no declare. Not
        // persisted: this line IS its recovery.
        st.exchanges
            .entry(BRIDGE_EXCHANGE.to_string())
            .or_insert(Exchange {
                durable: true,
                kind: ExchangeKind::Topic,
                bindings: Vec::new(),
            });
        let mut nq = 0;
        let mut nm = 0;

        for doc in db
            .find(AMQP_COLLECTION, &json!({ "_kind": "exchange" }))
            .unwrap_or_default()
        {
            if let Some(name) = doc.get("name").and_then(|v| v.as_str()) {
                let kind = doc
                    .get("kind")
                    .and_then(|v| v.as_str())
                    .and_then(ExchangeKind::parse)
                    .unwrap_or(ExchangeKind::Direct);
                st.exchanges.entry(name.to_string()).or_insert(Exchange {
                    durable: true,
                    kind,
                    bindings: Vec::new(),
                });
            }
        }
        for doc in db
            .find(AMQP_COLLECTION, &json!({ "_kind": "queue" }))
            .unwrap_or_default()
        {
            if let Some(name) = doc.get("name").and_then(|v| v.as_str()) {
                st.queues
                    .entry(name.to_string())
                    .or_insert_with(|| Queue::new(true, None, false));
                nq += 1;
            }
        }
        for doc in db
            .find(AMQP_COLLECTION, &json!({ "_kind": "bind" }))
            .unwrap_or_default()
        {
            let (Some(ex), Some(q), Some(k)) = (
                doc.get("exchange").and_then(|v| v.as_str()),
                doc.get("queue").and_then(|v| v.as_str()),
                doc.get("key").and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            if let Some(e) = st.exchanges.get_mut(ex) {
                e.bindings.push((k.to_string(), q.to_string()));
            }
        }

        let mut per_queue: HashMap<String, Vec<(u64, QMsg)>> = HashMap::new();
        let mut max_seq = 0u64;
        for doc in db
            .find(AMQP_COLLECTION, &json!({ "_kind": "qmsg" }))
            .unwrap_or_default()
        {
            let (Some(q), Some(seq)) = (
                doc.get("queue").and_then(|v| v.as_str()),
                doc.get("seq").and_then(|v| v.as_u64()),
            ) else {
                continue;
            };
            let dec = |k: &str| {
                doc.get(k)
                    .and_then(|v| v.as_str())
                    .and_then(|s| B64.decode(s).ok())
                    .unwrap_or_default()
            };
            max_seq = max_seq.max(seq);
            per_queue.entry(q.to_string()).or_default().push((
                seq,
                QMsg {
                    seq: Some(seq),
                    exchange: doc
                        .get("exchange")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .into(),
                    routing_key: doc
                        .get("rkey")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .into(),
                    props_raw: dec("props"),
                    body: dec("body"),
                    // Conservatively true: "may have been delivered before the
                    // crash" is the promise the flag exists to keep, and exact
                    // delivery tracking would cost a disk write per delivery.
                    redelivered: true,
                },
            ));
        }
        for (qname, mut list) in per_queue {
            list.sort_by_key(|(s, _)| *s);
            let q = st
                .queues
                .entry(qname)
                .or_insert_with(|| Queue::new(true, None, false));
            for (_, m) in list {
                q.ready.push_back(m);
                nm += 1;
            }
        }
        self.seq.store(max_seq + 1, Ordering::SeqCst);
        (nq, nm)
    }

    // ── Topology ────────────────────────────────────────────────────────

    pub fn declare_exchange(
        &self,
        name: &str,
        kind: &str,
        durable: bool,
    ) -> Result<(), ChannelError> {
        if name.is_empty() {
            // The default exchange exists implicitly and cannot be declared.
            return Ok(());
        }
        let Some(parsed) = ExchangeKind::parse(kind) else {
            // The honest refusal ADR-0016 requires: naming what is missing
            // beats accepting a topology we will not route. `headers` is
            // explicitly out of the ADR's scope, not merely unimplemented.
            return Err((
                NOT_IMPLEMENTED,
                format!(
                    "exchange type '{kind}' is outside ADR-0016's scope; supported: direct, fanout, topic"
                ),
            ));
        };
        let mut st = self.inner.lock().unwrap();
        if let Some(ex) = st.exchanges.get(name) {
            if ex.kind != parsed {
                // Accepting a redeclare with a different type silently would
                // leave one of the two declarers routing wrong — RabbitMQ
                // answers 406, so do we.
                return Err((
                    PRECONDITION_FAILED,
                    format!(
                        "exchange '{name}' exists as type '{}', redeclared as '{kind}'",
                        ex.kind.as_str()
                    ),
                ));
            }
        }
        st.exchanges.entry(name.to_string()).or_insert(Exchange {
            durable,
            kind: parsed,
            bindings: Vec::new(),
        });
        drop(st);
        if durable {
            if let Some(db) = self.db() {
                let _ = db.delete(
                    AMQP_COLLECTION,
                    &json!({ "_kind": "exchange", "name": name }),
                );
                let _ = db.insert(
                    AMQP_COLLECTION,
                    json!({ "_kind": "exchange", "name": name, "kind": kind }),
                );
            }
        }
        Ok(())
    }

    /// Returns (queue name, message count, consumer count).
    pub fn declare_queue(
        &self,
        name: &str,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
        passive: bool,
        conn_id: u64,
    ) -> Result<(String, u32, u32), ChannelError> {
        let name = if name.is_empty() {
            format!("amq.gen-{:x}", self.gen_name.fetch_add(1, Ordering::SeqCst))
        } else {
            name.to_string()
        };
        let mut st = self.inner.lock().unwrap();
        if passive && !st.queues.contains_key(&name) {
            return Err((NOT_FOUND, format!("no queue '{name}'")));
        }
        let q = st
            .queues
            .entry(name.clone())
            .or_insert_with(|| Queue::new(durable, exclusive.then_some(conn_id), auto_delete));
        let counts = (q.ready.len() as u32, q.consumers.len() as u32);
        let persist = q.durable && !passive;
        drop(st);
        if persist {
            if let Some(db) = self.db() {
                let _ = db.delete(AMQP_COLLECTION, &json!({ "_kind": "queue", "name": name }));
                let _ = db.insert(AMQP_COLLECTION, json!({ "_kind": "queue", "name": name }));
            }
        }
        Ok((name, counts.0, counts.1))
    }

    pub fn bind(&self, queue: &str, exchange: &str, key: &str) -> Result<(), ChannelError> {
        let mut st = self.inner.lock().unwrap();
        if !st.queues.contains_key(queue) {
            return Err((NOT_FOUND, format!("no queue '{queue}'")));
        }
        let durable_q = st.queues[queue].durable;
        let Some(ex) = st.exchanges.get_mut(exchange) else {
            return Err((NOT_FOUND, format!("no exchange '{exchange}'")));
        };
        let pair = (key.to_string(), queue.to_string());
        let durable_bind = durable_q && ex.durable;
        if !ex.bindings.contains(&pair) {
            ex.bindings.push(pair);
        }
        drop(st);
        if durable_bind {
            if let Some(db) = self.db() {
                let _ = db.delete(
                    AMQP_COLLECTION,
                    &json!({ "_kind": "bind", "exchange": exchange, "queue": queue, "key": key }),
                );
                let _ = db.insert(
                    AMQP_COLLECTION,
                    json!({ "_kind": "bind", "exchange": exchange, "queue": queue, "key": key }),
                );
            }
        }
        Ok(())
    }

    // ── Publish path ────────────────────────────────────────────────────

    /// A wire publish (from an AMQP client): prepare + commit as a batch of
    /// one. Routes into queues, and — the AMQP → MQTT half of the ADR-0016
    /// Phase 3 bridge — a message addressed to `amq.topic` also comes out on
    /// the MQTT/OxiMem bus with `.` → `/`, all BEFORE this returns, so the
    /// publisher's confirm covers the bridged copies too. Returns how many
    /// destinations accepted the message (the mandatory-flag decision).
    pub fn publish(
        &self,
        exchange: &str,
        routing_key: &str,
        props_raw: Vec<u8>,
        body: Vec<u8>,
        delivery_mode: Option<u8>,
    ) -> Result<usize, ChannelError> {
        let p =
            self.prepare_publish(exchange, routing_key, props_raw, body, delivery_mode, true)?;
        Ok(self.commit_publishes(vec![p])[0])
    }

    /// The MQTT → AMQP half of the bridge: an MQTT publish routes into the
    /// `amq.topic` exchange with `/` → `.`, so a worker pool consumes over
    /// AMQP what a sensor publishes over MQTT. `bridge: false` — never
    /// forwarded back out to MQTT, which is the loop this flag exists to
    /// prevent. A QoS ≥ 1 publish arrives with a durability promise and
    /// keeps it: delivery_mode=2 into durable queues (and the props carry it,
    /// so an AMQP consumer sees a persistent message).
    pub fn publish_from_mqtt(&self, topic: &str, payload: &[u8], qos: u8) {
        if !self.active.load(Ordering::Relaxed) {
            return;
        }
        let rkey = topic.replace('/', ".");
        let (props, dm) = if qos >= 1 {
            // Property flags 0x1000 = delivery-mode present; value 2.
            (vec![0x10, 0x00, 2], Some(2))
        } else {
            (Vec::new(), None)
        };
        if let Ok(p) =
            self.prepare_publish(BRIDGE_EXCHANGE, &rkey, props, payload.to_vec(), dm, false)
        {
            self.commit_publishes(vec![p]);
        }
    }

    /// Route one wire publish under the broker lock WITHOUT touching disk or
    /// queues: which queues it goes to, and a reserved durable seq per
    /// durable target. The connection layer batches prepared publishes and
    /// commits them together — that batch is what turns one fsync per
    /// message into one fsync per pipeline burst.
    pub fn prepare_publish(
        &self,
        exchange: &str,
        routing_key: &str,
        props_raw: Vec<u8>,
        body: Vec<u8>,
        delivery_mode: Option<u8>,
        bridge: bool,
    ) -> Result<PreparedPublish, ChannelError> {
        let st = self.inner.lock().unwrap();
        let target_names: Vec<String> = if exchange.is_empty() {
            // Default exchange: the routing key IS the queue name.
            if st.queues.contains_key(routing_key) {
                vec![routing_key.to_string()]
            } else {
                Vec::new()
            }
        } else {
            let Some(ex) = st.exchanges.get(exchange) else {
                return Err((NOT_FOUND, format!("no exchange '{exchange}'")));
            };
            let mut ts: Vec<String> = ex
                .bindings
                .iter()
                .filter(|(k, _)| match ex.kind {
                    ExchangeKind::Direct => k == routing_key,
                    ExchangeKind::Fanout => true,
                    ExchangeKind::Topic => topic_matches(k, routing_key),
                })
                .map(|(_, q)| q.clone())
                .collect();
            // One copy per queue however many of its bindings matched —
            // RabbitMQ semantics, and the reason this dedups.
            ts.sort();
            ts.dedup();
            ts
        };
        let mut targets = Vec::with_capacity(target_names.len());
        for qname in target_names {
            let Some(q) = st.queues.get(&qname) else {
                continue;
            };
            let seq = (q.durable && delivery_mode == Some(2))
                .then(|| self.seq.fetch_add(1, Ordering::SeqCst));
            targets.push((qname, seq));
        }
        drop(st);
        Ok(PreparedPublish {
            exchange: exchange.to_string(),
            routing_key: routing_key.to_string(),
            props_raw,
            body,
            targets,
            bridge: bridge && exchange == BRIDGE_EXCHANGE,
            delivery_mode,
        })
    }

    /// Commit a batch of prepared publishes: ONE `insert_many` (one fsync)
    /// for every durable record in the batch, then enqueue, then the MQTT
    /// bridge forwards. Returns each publish's routed-destination count, in
    /// input order. The disk write comes FIRST — an ack racing the enqueue
    /// must never delete a record that has not been written yet, or a crash
    /// resurrects a consumed message (the order the per-message path used to
    /// have was wrong in exactly that window).
    pub fn commit_publishes(&self, batch: Vec<PreparedPublish>) -> Vec<usize> {
        let mut docs = Vec::new();
        for p in &batch {
            for (qname, seq) in &p.targets {
                if let Some(s) = seq {
                    docs.push(json!({
                        "_kind": "qmsg", "queue": qname, "seq": s,
                        "exchange": p.exchange, "rkey": p.routing_key,
                        "props": B64.encode(&p.props_raw), "body": B64.encode(&p.body),
                    }));
                }
            }
        }
        if !docs.is_empty() {
            if let Some(db) = self.db() {
                // Through the group committer: bursts arriving from OTHER
                // connections while a commit is on the disk share the next
                // round's insert_many — cross-connection fsync grouping. A
                // lone publisher leads its own round immediately.
                self.group.submit(docs, |round| {
                    let _ = db.insert_many(AMQP_COLLECTION, round);
                });
            }
        }

        let mqtt = self.mqtt.read().unwrap().clone();
        let mut results = Vec::with_capacity(batch.len());
        let mut bridge_out: Vec<(usize, String, String, u8)> = Vec::new();
        let mut dead_docs: Vec<u64> = Vec::new();
        let mut wake_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        let mut st = self.inner.lock().unwrap();
        for (idx, p) in batch.into_iter().enumerate() {
            let mut routed = 0usize;
            for (qname, seq) in &p.targets {
                // A queue can vanish between prepare and commit (auto-delete,
                // exclusive teardown): drop the copy — and its already-written
                // durable record, or a restart resurrects it into nowhere.
                let Some(q) = st.queues.get_mut(qname) else {
                    if let Some(s) = seq {
                        dead_docs.push(*s);
                    }
                    continue;
                };
                routed += 1;
                for c in &q.consumers {
                    wake_ids.insert(c.conn_id);
                }
                q.ready.push_back(QMsg {
                    seq: *seq,
                    exchange: p.exchange.clone(),
                    routing_key: p.routing_key.clone(),
                    props_raw: p.props_raw.clone(),
                    body: p.body.clone(),
                    redelivered: false,
                });
                while q.ready.len() > MAX_READY {
                    // Drop-oldest, counted, and the durable record goes with
                    // it — the bound must bind the disk too or a restart
                    // resurrects what the bound discarded (the MQTT lesson).
                    if let Some(old) = q.ready.pop_front() {
                        if let Some(s) = old.seq {
                            dead_docs.push(s);
                        }
                    }
                    q.dropped += 1;
                }
            }
            if p.bridge && mqtt.is_some() {
                bridge_out.push((
                    idx,
                    p.routing_key.replace('.', "/"),
                    String::from_utf8_lossy(&p.body).to_string(),
                    if p.delivery_mode == Some(2) { 1 } else { 0 },
                ));
            }
            results.push(routed);
        }
        drop(st);
        // Wake consumers NOW — before the slower doc deletes and bridge
        // forwards below — so their delivery pass overlaps with our tail work.
        self.wake_conns(&wake_ids);

        if !dead_docs.is_empty() {
            if let Some(db) = self.db() {
                for seq in dead_docs {
                    let _ = db.delete(AMQP_COLLECTION, &json!({ "_kind": "qmsg", "seq": seq }));
                }
            }
        }
        if let Some(store) = mqtt {
            for (idx, topic, text, qos) in bridge_out {
                // Live MQTT/RESP subscribers via the bus; durable MQTT
                // sessions via their fsync'd queue — still before the
                // caller's confirm, so write-before-confirm crosses the
                // bridge intact.
                results[idx] += store.publish(&topic, &text).max(0) as usize;
                crate::mqtt_session::MqttSessions::global().persist_incoming(&topic, &text, qos);
            }
        }
        results
    }

    // ── Consume path ────────────────────────────────────────────────────

    pub fn register_consumer(
        &self,
        queue: &str,
        conn_id: u64,
        channel: u16,
        ctag: String,
        no_ack: bool,
    ) -> Result<(), ChannelError> {
        let mut st = self.inner.lock().unwrap();
        let Some(q) = st.queues.get_mut(queue) else {
            return Err((NOT_FOUND, format!("no queue '{queue}'")));
        };
        q.had_consumer = true;
        q.consumers.push(Consumer {
            conn_id,
            channel,
            ctag,
            no_ack,
        });
        Ok(())
    }

    pub fn cancel_consumer(&self, conn_id: u64, ctag: &str) {
        let mut st = self.inner.lock().unwrap();
        let mut auto_deleted = Vec::new();
        let mut wake_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (name, q) in st.queues.iter_mut() {
            q.consumers
                .retain(|c| !(c.conn_id == conn_id && c.ctag == ctag));
            if q.auto_delete && q.had_consumer && q.consumers.is_empty() {
                auto_deleted.push(name.clone());
            } else if !q.ready.is_empty() {
                // The cancelled consumer may have been the round-robin turn:
                // wake the survivors so the backlog does not sit a tick.
                for c in &q.consumers {
                    wake_ids.insert(c.conn_id);
                }
            }
        }
        for name in &auto_deleted {
            st.queues.remove(name);
        }
        Self::strip_bindings(&mut st, &auto_deleted);
        drop(st);
        self.wake_conns(&wake_ids);
        self.forget_queues(&auto_deleted);
    }

    /// Deliveries owed to `conn_id`'s consumers. Round-robin: each ready
    /// message goes to the NEXT consumer in turn; if that turn belongs to a
    /// different connection, the message waits for that connection's poll
    /// (≤50ms away) — that is what makes the consumers actually compete
    /// instead of the fastest poller taking everything. A consumer at its
    /// Basic.Qos prefetch limit is skipped: its turn passes to one with
    /// capacity — the point of Qos, a busy worker must not stall the queue
    /// behind it.
    pub fn poll(&self, conn_id: u64) -> Vec<OutMsg> {
        let mut guard = self.inner.lock().unwrap();
        let BrokerState {
            queues,
            prefetch,
            inflight,
            ..
        } = &mut *guard;
        let mut out = Vec::new();
        let mut acked_docs: Vec<u64> = Vec::new();
        for (name, q) in queues.iter_mut() {
            if q.consumers.is_empty() {
                continue;
            }
            while !q.ready.is_empty() {
                let n = q.consumers.len();
                let chosen = (0..n).map(|i| (q.rr + i) % n).find(|&idx| {
                    let c = &q.consumers[idx];
                    c.no_ack || {
                        let key = (c.conn_id, c.channel);
                        match prefetch.get(&key).copied().unwrap_or(0) {
                            0 => true,
                            limit => inflight.get(&key).copied().unwrap_or(0) < limit,
                        }
                    }
                });
                let Some(turn) = chosen else {
                    break; // every consumer is at its prefetch limit
                };
                let c = &q.consumers[turn];
                if c.conn_id != conn_id {
                    break; // another connection's turn; it polls within 50ms
                }
                let msg = q.ready.pop_front().expect("checked non-empty");
                if c.no_ack {
                    // Fire-and-forget consumption: the message is gone at
                    // delivery, so its durable record goes now.
                    if let Some(s) = msg.seq {
                        acked_docs.push(s);
                    }
                } else {
                    *inflight.entry((c.conn_id, c.channel)).or_insert(0) += 1;
                }
                out.push(OutMsg {
                    channel: c.channel,
                    ctag: c.ctag.clone(),
                    no_ack: c.no_ack,
                    queue: name.clone(),
                    msg,
                });
                q.rr = turn + 1;
            }
        }
        drop(guard);
        if !acked_docs.is_empty() {
            if let Some(db) = self.db() {
                for s in acked_docs {
                    let _ = db.delete(AMQP_COLLECTION, &json!({ "_kind": "qmsg", "seq": s }));
                }
            }
        }
        out
    }

    /// Basic.Get: pop one message. Returns (message, remaining) or None.
    pub fn get_one(&self, queue: &str, no_ack: bool) -> Result<Option<(QMsg, u32)>, ChannelError> {
        let mut st = self.inner.lock().unwrap();
        let Some(q) = st.queues.get_mut(queue) else {
            return Err((NOT_FOUND, format!("no queue '{queue}'")));
        };
        let Some(msg) = q.ready.pop_front() else {
            return Ok(None);
        };
        let remaining = q.ready.len() as u32;
        let doc = if no_ack { msg.seq } else { None };
        drop(st);
        if let Some(s) = doc {
            if let Some(db) = self.db() {
                let _ = db.delete(AMQP_COLLECTION, &json!({ "_kind": "qmsg", "seq": s }));
            }
        }
        Ok(Some((msg, remaining)))
    }

    /// Basic.Qos: cap the unacked deliveries outstanding on a channel. The
    /// `global` bit is not distinguished — the cap applies per channel either
    /// way, the conservative reading. 0 = unlimited, per spec.
    pub fn set_qos(&self, conn_id: u64, channel: u16, count: u16) {
        let mut st = self.inner.lock().unwrap();
        st.prefetch.insert((conn_id, channel), count as u32);
    }

    /// A consumed delivery was settled (acked, rejected or requeued) — its
    /// prefetch slot frees.
    pub fn settle(&self, conn_id: u64, channel: u16) {
        let mut st = self.inner.lock().unwrap();
        if let Some(v) = st.inflight.get_mut(&(conn_id, channel)) {
            *v = v.saturating_sub(1);
        }
    }

    /// A channel died: its prefetch setting and slots die with it. (The
    /// connection layer requeues its unacked deliveries first.)
    pub fn channel_closed(&self, conn_id: u64, channel: u16) {
        let mut st = self.inner.lock().unwrap();
        st.prefetch.remove(&(conn_id, channel));
        st.inflight.remove(&(conn_id, channel));
    }

    /// The consumer acknowledged a delivery — its durable record is done.
    pub fn ack(&self, seq: Option<u64>) {
        let Some(s) = seq else { return };
        if let Some(db) = self.db() {
            let _ = db.delete(AMQP_COLLECTION, &json!({ "_kind": "qmsg", "seq": s }));
        }
    }

    /// An unacked delivery whose channel or connection died goes back to the
    /// FRONT of its queue, flagged redelivered. Its durable record was never
    /// deleted, so a crash in this window changes nothing.
    pub fn requeue(&self, queue: &str, mut msg: QMsg) {
        msg.redelivered = true;
        let mut st = self.inner.lock().unwrap();
        let mut wake_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        if let Some(q) = st.queues.get_mut(queue) {
            q.ready.push_front(msg);
            for c in &q.consumers {
                wake_ids.insert(c.conn_id);
            }
        }
        // Queue gone (deleted while the delivery was in flight): the message
        // goes with it, matching RabbitMQ.
        drop(st);
        self.wake_conns(&wake_ids);
    }

    /// Connection teardown: drop its consumers, fire auto-delete, delete its
    /// exclusive queues. The connection requeues its own unacked first.
    pub fn connection_closed(&self, conn_id: u64) {
        let mut st = self.inner.lock().unwrap();
        let mut gone = Vec::new();
        let mut wake_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for (name, q) in st.queues.iter_mut() {
            q.consumers.retain(|c| c.conn_id != conn_id);
            let exclusive_dies = q.exclusive_conn == Some(conn_id);
            let auto_dies = q.auto_delete && q.had_consumer && q.consumers.is_empty();
            if exclusive_dies || auto_dies {
                gone.push(name.clone());
            } else if !q.ready.is_empty() {
                // The dead connection may have owned the round-robin turn:
                // wake the survivors so the backlog does not sit a tick.
                for c in &q.consumers {
                    wake_ids.insert(c.conn_id);
                }
            }
        }
        for name in &gone {
            st.queues.remove(name);
        }
        Self::strip_bindings(&mut st, &gone);
        st.prefetch.retain(|(c, _), _| *c != conn_id);
        st.inflight.retain(|(c, _), _| *c != conn_id);
        drop(st);
        self.wakers.lock().unwrap().remove(&conn_id);
        self.wake_conns(&wake_ids);
        self.forget_queues(&gone);
    }

    /// A removed queue's bindings go with it — a stale binding would route
    /// copies into nowhere (and the publish path must never find one pointing
    /// at a queue that is gone).
    fn strip_bindings(st: &mut BrokerState, gone: &[String]) {
        if gone.is_empty() {
            return;
        }
        for ex in st.exchanges.values_mut() {
            ex.bindings.retain(|(_, q)| !gone.contains(q));
        }
    }

    fn forget_queues(&self, names: &[String]) {
        if names.is_empty() {
            return;
        }
        if let Some(db) = self.db() {
            for name in names {
                let _ = db.delete(AMQP_COLLECTION, &json!({ "_kind": "queue", "name": name }));
                let _ = db.delete(AMQP_COLLECTION, &json!({ "_kind": "qmsg", "queue": name }));
                let _ = db.delete(AMQP_COLLECTION, &json!({ "_kind": "bind", "queue": name }));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn broker() -> AmqpBroker {
        AmqpBroker::new()
    }

    fn publish(b: &AmqpBroker, queue: &str, body: &[u8]) {
        b.publish("", queue, Vec::new(), body.to_vec(), None)
            .unwrap();
    }

    #[test]
    fn default_exchange_routes_by_queue_name_and_drops_unroutable() {
        let b = broker();
        b.declare_queue("jobs", false, false, false, false, 1)
            .unwrap();
        assert_eq!(
            b.publish("", "jobs", vec![], b"x".to_vec(), None).unwrap(),
            1
        );
        // No queue named "nope": dropped, not an error — RabbitMQ semantics
        // for a non-mandatory unroutable publish.
        assert_eq!(
            b.publish("", "nope", vec![], b"x".to_vec(), None).unwrap(),
            0
        );
    }

    #[test]
    fn competing_consumers_split_the_work_round_robin() {
        let b = broker();
        b.declare_queue("w", false, false, false, false, 1).unwrap();
        b.register_consumer("w", 1, 1, "c1".into(), true).unwrap();
        b.register_consumer("w", 2, 1, "c2".into(), true).unwrap();
        for i in 0..6 {
            publish(&b, "w", format!("{i}").as_bytes());
        }
        // A single poll stops at the other connection's turn — that is the
        // point of the round-robin: the fastest poller must not take
        // everything. Alternate polls, as the real 50ms loops do.
        let (mut a, mut c) = (Vec::new(), Vec::new());
        loop {
            let x = b.poll(1);
            let y = b.poll(2);
            if x.is_empty() && y.is_empty() {
                break;
            }
            a.extend(x);
            c.extend(y);
        }
        assert_eq!(a.len() + c.len(), 6);
        assert_eq!(a.len(), 3, "round-robin must split the work evenly");
        let mut all: Vec<String> = a
            .iter()
            .chain(c.iter())
            .map(|o| String::from_utf8_lossy(&o.msg.body).to_string())
            .collect();
        all.sort();
        assert_eq!(
            all,
            vec!["0", "1", "2", "3", "4", "5"],
            "no duplicates, no losses"
        );
    }

    #[test]
    fn direct_exchange_routes_on_exact_key() {
        let b = broker();
        b.declare_exchange("logs", "direct", false).unwrap();
        b.declare_queue("errors", false, false, false, false, 1)
            .unwrap();
        b.declare_queue("infos", false, false, false, false, 1)
            .unwrap();
        b.bind("errors", "logs", "error").unwrap();
        b.bind("infos", "logs", "info").unwrap();
        assert_eq!(
            b.publish("logs", "error", vec![], b"boom".to_vec(), None)
                .unwrap(),
            1
        );
        b.register_consumer("errors", 1, 1, "c".into(), true)
            .unwrap();
        b.register_consumer("infos", 1, 1, "d".into(), true)
            .unwrap();
        let out = b.poll(1);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].queue, "errors",
            "the key must route to the bound queue only"
        );
    }

    #[test]
    fn a_headers_exchange_is_refused_by_name_not_accepted_silently() {
        let b = broker();
        let err = b.declare_exchange("h", "headers", false).unwrap_err();
        assert_eq!(err.0, NOT_IMPLEMENTED);
        assert!(
            err.1.contains("ADR-0016"),
            "the refusal must say where the boundary is: {}",
            err.1
        );
    }

    #[test]
    fn redeclaring_an_exchange_with_a_different_type_is_a_precondition_failure() {
        let b = broker();
        b.declare_exchange("e", "direct", false).unwrap();
        let err = b.declare_exchange("e", "fanout", false).unwrap_err();
        assert_eq!(
            err.0, PRECONDITION_FAILED,
            "silent acceptance would leave one declarer routing wrong"
        );
        b.declare_exchange("e", "direct", false).unwrap();
    }

    #[test]
    fn topic_wildcards_star_is_one_word_hash_is_any_number() {
        assert!(topic_matches("kern.*", "kern.crit"));
        assert!(
            !topic_matches("kern.*", "kern.crit.disk"),
            "'*' is exactly one word"
        );
        assert!(!topic_matches("kern.*", "kern"), "'*' is not zero words");
        assert!(topic_matches("kern.#", "kern.crit.disk"));
        assert!(topic_matches("kern.#", "kern"), "'#' matches zero words");
        assert!(topic_matches("#", "anything.at.all"));
        assert!(topic_matches("*.critical.#", "disk.critical.raid.0"));
        assert!(!topic_matches("*.critical.#", "critical.raid"));
    }

    #[test]
    fn topic_exchange_routes_on_wildcards() {
        let b = broker();
        b.declare_exchange("logs", "topic", false).unwrap();
        b.declare_queue("kern", false, false, false, false, 1)
            .unwrap();
        b.declare_queue("all", false, false, false, false, 1)
            .unwrap();
        b.bind("kern", "logs", "kern.*").unwrap();
        b.bind("all", "logs", "#").unwrap();
        assert_eq!(
            b.publish("logs", "kern.crit", vec![], b"x".to_vec(), None)
                .unwrap(),
            2
        );
        assert_eq!(
            b.publish("logs", "app.info", vec![], b"y".to_vec(), None)
                .unwrap(),
            1
        );
        assert_eq!(b.get_one("kern", true).unwrap().unwrap().0.body, b"x");
        assert!(
            b.get_one("kern", true).unwrap().is_none(),
            "app.info must not match kern.*"
        );
    }

    #[test]
    fn fanout_copies_to_every_queue_once_even_when_bound_twice() {
        let b = broker();
        b.declare_exchange("bcast", "fanout", false).unwrap();
        b.declare_queue("q1", false, false, false, false, 1)
            .unwrap();
        b.declare_queue("q2", false, false, false, false, 1)
            .unwrap();
        b.bind("q1", "bcast", "").unwrap();
        b.bind("q1", "bcast", "other-key").unwrap();
        b.bind("q2", "bcast", "").unwrap();
        assert_eq!(
            b.publish("bcast", "ignored", vec![], b"copy".to_vec(), None)
                .unwrap(),
            2,
            "one copy per queue, however many bindings it has"
        );
        assert_eq!(b.get_one("q1", true).unwrap().unwrap().0.body, b"copy");
        assert!(
            b.get_one("q1", true).unwrap().is_none(),
            "the double binding must not duplicate"
        );
        assert_eq!(b.get_one("q2", true).unwrap().unwrap().0.body, b"copy");
    }

    #[test]
    fn prefetch_passes_a_full_consumers_turn_to_an_idle_one() {
        let b = broker();
        b.declare_queue("w", false, false, false, false, 1).unwrap();
        b.set_qos(1, 1, 1); // conn 1, channel 1: prefetch 1
        b.register_consumer("w", 1, 1, "busy".into(), false)
            .unwrap();
        b.register_consumer("w", 2, 1, "idle".into(), true).unwrap();
        for i in 0..4 {
            publish(&b, "w", format!("{i}").as_bytes());
        }
        let (mut busy, mut idle) = (0, 0);
        loop {
            let x = b.poll(1);
            let y = b.poll(2);
            if x.is_empty() && y.is_empty() {
                break;
            }
            busy += x.len();
            idle += y.len();
        }
        assert_eq!(
            busy, 1,
            "the prefetch cap must hold at one unacked delivery"
        );
        assert_eq!(
            idle, 3,
            "the capped consumer's turns must pass to the one with capacity"
        );
        // The ack frees the slot and delivery resumes.
        b.settle(1, 1);
        publish(&b, "w", b"more");
        let mut total = 0;
        loop {
            let n = b.poll(1).len() + b.poll(2).len();
            if n == 0 {
                break;
            }
            total += n;
        }
        assert_eq!(total, 1, "a freed slot must let delivery continue");
    }

    #[test]
    fn mqtt_publishes_route_into_the_bridge_exchange_with_slash_to_dot() {
        let b = broker();
        b.activate_for_tests();
        b.declare_exchange(BRIDGE_EXCHANGE, "topic", false).unwrap(); // enable() does this in prod
        b.declare_queue("workers", false, false, false, false, 1)
            .unwrap();
        b.bind("workers", BRIDGE_EXCHANGE, "sensors.#").unwrap();
        b.publish_from_mqtt("sensors/floor1/temp", b"21.5", 1);
        let (m, _) = b.get_one("workers", true).unwrap().unwrap();
        assert_eq!(m.body, b"21.5");
        assert_eq!(m.routing_key, "sensors.floor1.temp", "'/' must become '.'");
        assert_eq!(
            m.props_raw,
            vec![0x10, 0x00, 2],
            "QoS 1 must arrive as delivery_mode=2"
        );
        b.publish_from_mqtt("app/log", b"x", 0);
        assert!(
            b.get_one("workers", true).unwrap().is_none(),
            "a non-matching topic must not route"
        );
    }

    #[test]
    fn an_inactive_broker_ignores_the_mqtt_side_of_the_bridge() {
        let b = broker();
        b.declare_exchange(BRIDGE_EXCHANGE, "topic", false).unwrap();
        b.declare_queue("workers", false, false, false, false, 1)
            .unwrap();
        b.bind("workers", BRIDGE_EXCHANGE, "#").unwrap();
        b.publish_from_mqtt("sensors/temp", b"x", 0);
        assert!(
            b.get_one("workers", true).unwrap().is_none(),
            "with AMQP off the bridge must cost (and do) nothing"
        );
    }

    #[test]
    fn amqp_publishes_to_the_bridge_exchange_reach_mqtt_subscribers() {
        let b = broker();
        b.declare_exchange(BRIDGE_EXCHANGE, "topic", false).unwrap();
        let store = Arc::new(OxiMemStore::new());
        b.attach_mqtt(Arc::clone(&store));
        let rx = store.subscribe("sensors/hum");
        let n = b
            .publish(BRIDGE_EXCHANGE, "sensors.hum", vec![], b"55".to_vec(), None)
            .unwrap();
        assert_eq!(
            n, 1,
            "the MQTT subscriber must count as a routed destination"
        );
        let (topic, msg) = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(
            (topic.as_str(), msg.as_str()),
            ("sensors/hum", "55"),
            "'.' must become '/'"
        );
    }

    #[test]
    fn a_publish_to_an_ordinary_exchange_does_not_cross_the_bridge() {
        let b = broker();
        b.declare_exchange("plain", "topic", false).unwrap();
        let store = Arc::new(OxiMemStore::new());
        b.attach_mqtt(Arc::clone(&store));
        let rx = store.subscribe("a/b");
        b.declare_queue("q", false, false, false, false, 1).unwrap();
        b.bind("q", "plain", "a.b").unwrap();
        b.publish("plain", "a.b", vec![], b"x".to_vec(), None)
            .unwrap();
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(100))
                .is_err(),
            "only amq.topic bridges to MQTT"
        );
    }

    #[test]
    fn group_commit_merges_concurrent_submitters_into_fewer_rounds() {
        use std::sync::atomic::AtomicUsize;
        let gc = Arc::new(GroupCommit::default());
        let rounds = Arc::new(AtomicUsize::new(0));
        let committed = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for i in 0..8 {
            let gc = Arc::clone(&gc);
            let rounds = Arc::clone(&rounds);
            let committed = Arc::clone(&committed);
            handles.push(std::thread::spawn(move || {
                gc.submit(vec![json!({ "i": i })], |round| {
                    rounds.fetch_add(1, Ordering::SeqCst);
                    committed.fetch_add(round.len(), Ordering::SeqCst);
                    // A slow "fsync": the window concurrency piles into.
                    std::thread::sleep(std::time::Duration::from_millis(20));
                });
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(
            committed.load(Ordering::SeqCst),
            8,
            "every submitted doc must be committed exactly once"
        );
        let r = rounds.load(Ordering::SeqCst);
        assert!(
            r < 8,
            "concurrent submitters must share commit rounds; 8 submissions took {r} rounds"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_publish_pokes_the_consuming_connections_wake_pipe() {
        use std::io::Read;
        let b = broker();
        let (rx, tx) = std::os::unix::net::UnixStream::pair().unwrap();
        rx.set_nonblocking(true).unwrap();
        tx.set_nonblocking(true).unwrap();
        b.register_waker(7, Waker::new(tx));
        b.declare_queue("q", false, false, false, false, 7).unwrap();
        b.register_consumer("q", 7, 1, "c".into(), true).unwrap();

        let mut buf = [0u8; 8];
        assert!(
            (&rx).read(&mut buf).is_err(),
            "no wake before any publish (nonblocking read must return WouldBlock)"
        );
        publish(&b, "q", b"x");
        assert!(
            (&rx).read(&mut buf).map(|n| n > 0).unwrap_or(false),
            "the publish must poke the consumer's wake pipe"
        );
    }

    #[test]
    fn a_binding_to_a_deleted_queue_neither_panics_nor_routes() {
        let b = broker();
        b.declare_exchange("e", "fanout", false).unwrap();
        b.declare_queue("tmp", false, false, true, false, 1)
            .unwrap();
        b.bind("tmp", "e", "").unwrap();
        b.register_consumer("tmp", 1, 1, "c".into(), true).unwrap();
        b.cancel_consumer(1, "c"); // auto-delete fires: the queue is gone
        assert_eq!(
            b.publish("e", "", vec![], b"x".to_vec(), None).unwrap(),
            0,
            "a binding must not outlive its queue"
        );
    }

    #[test]
    fn requeue_goes_to_the_front_flagged_redelivered() {
        let b = broker();
        b.declare_queue("q", false, false, false, false, 1).unwrap();
        publish(&b, "q", b"first");
        publish(&b, "q", b"second");
        let (m, _) = b.get_one("q", false).unwrap().unwrap();
        assert!(!m.redelivered);
        b.requeue("q", m);
        let (m2, _) = b.get_one("q", false).unwrap().unwrap();
        assert_eq!(
            m2.body, b"first",
            "a requeued message must not lose its place in line"
        );
        assert!(m2.redelivered);
    }

    #[test]
    fn an_exclusive_queue_dies_with_its_connection() {
        let b = broker();
        b.declare_queue("mine", false, true, false, false, 7)
            .unwrap();
        b.connection_closed(7);
        assert!(matches!(b.get_one("mine", false), Err((NOT_FOUND, _))));
    }

    #[test]
    fn the_ready_bound_drops_oldest_and_counts() {
        let b = broker();
        b.declare_queue("full", false, false, false, false, 1)
            .unwrap();
        for i in 0..(MAX_READY + 5) {
            publish(&b, "full", format!("{i}").as_bytes());
        }
        let st = b.inner.lock().unwrap();
        let q = &st.queues["full"];
        assert_eq!(q.ready.len(), MAX_READY);
        assert_eq!(q.dropped, 5, "overflow must be counted, not silent");
        assert_eq!(q.ready.front().unwrap().body, b"5", "drop-oldest");
    }
}
