//! OxiMem — native in-memory key-value store with RESP wire protocol.
//!
//! By default, commands operate on raw `HashMap`/`VecDeque`/`HashSet`
//! structures for maximum throughput (no JSON, no WAL, no indexes).
//!
//! When constructed with `OxiMemStore::new_with_sql(db)`, data is also mirrored
//! to OxiDB collections (`_kv`, `_hash`, `_list`, `_set`) so it can be queried
//! via SQL: `SELECT * FROM _kv WHERE _key LIKE 'session:%'`

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use crate::resp::{self, RespValue};

// Optional SQL bridge
use oxidb::OxiDb;
use serde_json::json;

/// Entry with optional expiration.
struct KvEntry {
    value: String,
    expires_at: Option<Instant>,
}

impl KvEntry {
    fn new(value: String) -> Self {
        Self {
            value,
            expires_at: None,
        }
    }
    fn with_ttl(value: String, secs: u64) -> Self {
        Self {
            value,
            expires_at: Some(Instant::now() + std::time::Duration::from_secs(secs)),
        }
    }
    fn is_expired(&self) -> bool {
        self.expires_at
            .map(|e| Instant::now() >= e)
            .unwrap_or(false)
    }
    fn ttl_secs(&self) -> i64 {
        match self.expires_at {
            Some(e) => {
                let now = Instant::now();
                if now >= e {
                    -2
                } else {
                    (e - now).as_secs() as i64
                }
            }
            None => -1,
        }
    }
}

/// f64 wrapper with total ordering for BTreeSet usage.
#[derive(Clone, PartialEq)]
struct Score(f64);
impl Eq for Score {}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Sorted set with O(log n) insert/remove and O(1) score lookup.
struct SortedSet {
    scores: HashMap<String, f64>,
    tree: BTreeSet<(Score, String)>,
}

impl SortedSet {
    fn new() -> Self {
        Self {
            scores: HashMap::new(),
            tree: BTreeSet::new(),
        }
    }
    fn insert(&mut self, member: String, score: f64) -> bool {
        if let Some(&old) = self.scores.get(&member) {
            self.tree.remove(&(Score(old), member.clone()));
            self.tree.insert((Score(score), member.clone()));
            self.scores.insert(member, score);
            false // updated, not added
        } else {
            self.tree.insert((Score(score), member.clone()));
            self.scores.insert(member, score);
            true // newly added
        }
    }
    fn remove(&mut self, member: &str) -> bool {
        if let Some(score) = self.scores.remove(member) {
            self.tree.remove(&(Score(score), member.to_string()));
            true
        } else {
            false
        }
    }
    fn score(&self, member: &str) -> Option<f64> {
        self.scores.get(member).copied()
    }
    fn len(&self) -> usize {
        self.scores.len()
    }
    fn rank(&self, member: &str) -> Option<usize> {
        let score = self.scores.get(member)?;
        let target = (Score(*score), member.to_string());
        Some(self.tree.iter().take_while(|e| *e != &target).count())
    }
    fn range_by_rank(&self, start: isize, stop: isize) -> Vec<(&str, f64)> {
        let len = self.tree.len() as isize;
        let s = if start < 0 {
            (len + start).max(0) as usize
        } else {
            start as usize
        };
        let e = if stop < 0 {
            (len + stop).max(0) as usize
        } else {
            stop as usize
        };
        if s > e || s >= len as usize {
            return vec![];
        }
        self.tree
            .iter()
            .skip(s)
            .take(e - s + 1)
            .map(|(sc, m)| (m.as_str(), sc.0))
            .collect()
    }
    fn range_by_score(&self, min: f64, max: f64) -> Vec<(&str, f64)> {
        self.tree
            .range((Score(min), String::new())..)
            .take_while(|(sc, _)| sc.0 <= max)
            .map(|(sc, m)| (m.as_str(), sc.0))
            .collect()
    }
    fn count_by_score(&self, min: f64, max: f64) -> usize {
        self.tree
            .range((Score(min), String::new())..)
            .take_while(|(sc, _)| sc.0 <= max)
            .count()
    }
}

/// Pub/Sub subscriber channel.
type PubSubSender = mpsc::Sender<(String, String)>;

/// Native in-memory store for Redis-compatible operations.
pub struct OxiMemStore {
    strings: RwLock<HashMap<String, KvEntry>>,
    hashes: RwLock<HashMap<String, HashMap<String, String>>>,
    lists: RwLock<HashMap<String, VecDeque<String>>>,
    sets: RwLock<HashMap<String, HashSet<String>>>,
    sorted_sets: RwLock<HashMap<String, SortedSet>>,
    pubsub: Mutex<HashMap<String, Vec<PubSubSender>>>,
    /// PSUBSCRIBE pattern subscribers: (glob pattern, compiled regex, senders).
    psubs: Mutex<Vec<(String, regex::Regex, Vec<PubSubSender>)>>,
    /// MQTT retained messages: topic → last retained payload.
    retained: RwLock<HashMap<String, String>>,
    /// Serializes MULTI/EXEC transaction blocks so two EXECs never interleave
    /// their queued commands — the isolation Redis gets for free from being
    /// single-threaded. Held only for the duration of an EXEC.
    tx_lock: Mutex<()>,
    /// EVAL concurrency gate: scripts take a READ lock (many can run at
    /// once); EXEC takes the WRITE lock (excludes all scripts + other EXECs).
    eval_gate: RwLock<()>,
    /// Striped key locks for EVAL: a script locks the stripes of its
    /// DECLARED KEYS (sorted — deadlock-free), so scripts touching disjoint
    /// keys run in parallel. Undeclared-key access falls back to per-command
    /// atomicity — the same contract Redis documents for scripts.
    eval_stripes: Vec<Mutex<()>>,
    /// Per-key mutation counters for WATCH: every write command bumps its
    /// key's counter, so a WATCH snapshot is an O(1) integer instead of a
    /// serialized copy of the value (which would be O(n) for a large book).
    versions: RwLock<HashMap<String, u64>>,
    /// Bumped by FLUSHALL/FLUSHDB — invalidates every outstanding WATCH.
    epoch: AtomicU64,
    /// Sessions currently holding WATCHes — lets `bump_version` garbage-
    /// collect the versions map when nobody is watching.
    active_watches: AtomicUsize,
    /// Keyspace notifications (`__keyspace@0__:<key>` / `__keyevent@0__:<cmd>`)
    /// — off by default, toggled via `CONFIG SET notify-keyspace-events`.
    notify: AtomicBool,
    /// SCRIPT LOAD cache: sha1-hex → Lua source (for EVALSHA).
    scripts: RwLock<HashMap<String, String>>,
    /// SCRIPT KILL flag — the Lua hook aborts the running script when set.
    script_kill: Arc<AtomicBool>,
    /// Wakes blocked BLPOP/BRPOP/BZPOPMIN/BLMOVE waiters on list/zset writes.
    write_cv: Arc<(Mutex<u64>, std::sync::Condvar)>,
    /// Optional OxiDB reference for SQL-queryable mirroring.
    db: Option<Arc<OxiDb>>,
}

impl OxiMemStore {
    /// Fast mode — no SQL support, pure in-memory.
    pub fn new() -> Self {
        Self {
            strings: RwLock::new(HashMap::new()),
            hashes: RwLock::new(HashMap::new()),
            lists: RwLock::new(HashMap::new()),
            sets: RwLock::new(HashMap::new()),
            sorted_sets: RwLock::new(HashMap::new()),
            pubsub: Mutex::new(HashMap::new()),
            psubs: Mutex::new(Vec::new()),
            retained: RwLock::new(HashMap::new()),
            tx_lock: Mutex::new(()),
            eval_gate: RwLock::new(()),
            eval_stripes: (0..128).map(|_| Mutex::new(())).collect(),
            versions: RwLock::new(HashMap::new()),
            epoch: AtomicU64::new(0),
            active_watches: AtomicUsize::new(0),
            notify: AtomicBool::new(false),
            scripts: RwLock::new(HashMap::new()),
            script_kill: Arc::new(AtomicBool::new(false)),
            write_cv: Arc::new((Mutex::new(0), std::sync::Condvar::new())),
            db: None,
        }
    }

    /// SQL mode — data mirrored to OxiDB collections for SQL queries, and
    /// loaded BACK from them on construction, so a restart repopulates the
    /// in-memory state from the durable mirror (rebuild-on-boot).
    pub fn new_with_sql(db: Arc<OxiDb>) -> Self {
        let store = Self::new_with_sql_inner(db);
        store.load_from_mirror();
        store
    }

    fn new_with_sql_inner(db: Arc<OxiDb>) -> Self {
        Self {
            strings: RwLock::new(HashMap::new()),
            hashes: RwLock::new(HashMap::new()),
            lists: RwLock::new(HashMap::new()),
            sets: RwLock::new(HashMap::new()),
            sorted_sets: RwLock::new(HashMap::new()),
            pubsub: Mutex::new(HashMap::new()),
            psubs: Mutex::new(Vec::new()),
            retained: RwLock::new(HashMap::new()),
            tx_lock: Mutex::new(()),
            eval_gate: RwLock::new(()),
            eval_stripes: (0..128).map(|_| Mutex::new(())).collect(),
            versions: RwLock::new(HashMap::new()),
            epoch: AtomicU64::new(0),
            active_watches: AtomicUsize::new(0),
            notify: AtomicBool::new(false),
            scripts: RwLock::new(HashMap::new()),
            script_kill: Arc::new(AtomicBool::new(false)),
            write_cv: Arc::new((Mutex::new(0), std::sync::Condvar::new())),
            db: Some(db),
        }
    }

    /// Subscribe to a channel, returns a receiver for messages.
    pub fn subscribe(&self, channel: &str) -> mpsc::Receiver<(String, String)> {
        let (tx, rx) = mpsc::channel();
        let mut map = self.pubsub.lock().unwrap();
        map.entry(channel.to_string()).or_default().push(tx);
        rx
    }

    /// Unsubscribe from a channel (removes dead senders on next publish).
    pub fn unsubscribe(&self, channel: &str) {
        let mut map = self.pubsub.lock().unwrap();
        // We can't easily remove a specific sender, but publish() cleans dead ones.
        // For explicit unsubscribe, just drop the receiver — sender will fail on next publish.
        if let Some(senders) = map.get_mut(channel) {
            if senders.is_empty() {
                map.remove(channel);
            }
        }
    }

    /// Subscribe to a glob pattern (PSUBSCRIBE), returns a receiver.
    pub fn psubscribe(&self, pattern: &str) -> Option<mpsc::Receiver<(String, String)>> {
        let re = regex::Regex::new(&glob_to_regex(pattern)).ok()?;
        let (tx, rx) = mpsc::channel();
        let mut subs = self.psubs.lock().unwrap();
        if let Some(entry) = subs.iter_mut().find(|(p, _, _)| p == pattern) {
            entry.2.push(tx);
        } else {
            subs.push((pattern.to_string(), re, vec![tx]));
        }
        Some(rx)
    }

    /// Pattern-subscribe with a pre-compiled regex (MQTT wildcard filters).
    pub fn psubscribe_regex(
        &self,
        display: &str,
        re: regex::Regex,
    ) -> mpsc::Receiver<(String, String)> {
        let (tx, rx) = mpsc::channel();
        let mut subs = self.psubs.lock().unwrap();
        if let Some(e) = subs.iter_mut().find(|(p, _, _)| p == display) {
            e.2.push(tx);
        } else {
            subs.push((display.to_string(), re, vec![tx]));
        }
        rx
    }

    /// Store / clear / query MQTT retained messages.
    pub fn retain_set(&self, topic: &str, msg: &str) {
        self.retained
            .write()
            .unwrap()
            .insert(topic.to_string(), msg.to_string());
    }
    pub fn retain_clear(&self, topic: &str) {
        self.retained.write().unwrap().remove(topic);
    }
    pub fn retained_matching(&self, re: &regex::Regex) -> Vec<(String, String)> {
        self.retained
            .read()
            .unwrap()
            .iter()
            .filter(|(t, _)| re.is_match(t))
            .map(|(t, m)| (t.clone(), m.clone()))
            .collect()
    }

    /// Remove a pattern subscription entry entirely (PUNSUBSCRIBE).
    pub fn punsubscribe(&self, pattern: &str) {
        self.psubs.lock().unwrap().retain(|(p, _, _)| p != pattern);
    }

    /// Publish a message to a channel, returns number of receivers
    /// (exact-channel subscribers + matching pattern subscribers).
    pub fn publish(&self, channel: &str, message: &str) -> i64 {
        let mut count = 0i64;
        {
            let mut map = self.pubsub.lock().unwrap();
            if let Some(senders) = map.get_mut(channel) {
                senders.retain(|tx| tx.send((channel.to_string(), message.to_string())).is_ok());
                count += senders.len() as i64;
                if senders.is_empty() {
                    map.remove(channel);
                }
            }
        }
        {
            let mut subs = self.psubs.lock().unwrap();
            for (_, re, senders) in subs.iter_mut() {
                if re.is_match(channel) {
                    senders
                        .retain(|tx| tx.send((channel.to_string(), message.to_string())).is_ok());
                    count += senders.len() as i64;
                }
            }
            subs.retain(|(_, _, s)| !s.is_empty());
        }
        count
    }

    pub fn sql_enabled(&self) -> bool {
        self.db.is_some()
    }

    /// Record a mutation on `key` (for WATCH change detection). When the map
    /// grows large and NO session holds a WATCH, it is safe to clear it (a
    /// future WATCH re-snapshots from scratch) — bounds memory on long runs.
    fn bump_version(&self, key: &str) {
        let mut v = self.versions.write().unwrap();
        static GC_AT: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
            std::env::var("OXIMEM_VERSIONS_GC")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(100_000)
        });
        if v.len() > *GC_AT && self.active_watches.load(Ordering::SeqCst) == 0 {
            v.clear();
            self.epoch.fetch_add(1, Ordering::SeqCst); // belt & suspenders
        }
        *v.entry(key.to_string()).or_insert(0) += 1;
    }

    /// Repopulate in-memory state from the SQL-mirror collections.
    fn load_from_mirror(&self) {
        let db = match &self.db {
            Some(db) => db,
            None => return,
        };
        let all = json!({});
        if let Ok(rows) = db.find("_kv", &all) {
            let mut s = self.strings.write().unwrap();
            let now = now_secs();
            for r in rows {
                if let (Some(k), Some(v)) = (r["_key"].as_str(), r["_value"].as_str()) {
                    // Restore the REMAINING ttl from the absolute deadline;
                    // already-expired keys are simply not loaded.
                    match r["_exp"].as_u64() {
                        Some(exp) if exp <= now => continue,
                        Some(exp) => {
                            s.insert(k.to_string(), KvEntry::with_ttl(v.to_string(), exp - now));
                        }
                        None => {
                            s.insert(k.to_string(), KvEntry::new(v.to_string()));
                        }
                    }
                }
            }
        }
        if let Ok(rows) = db.find("_hash", &all) {
            let mut h = self.hashes.write().unwrap();
            for r in rows {
                if let (Some(k), Some(obj)) = (r["_key"].as_str(), r.as_object()) {
                    // Fields are mirrored flat on the doc; skip internal keys.
                    let m: HashMap<String, String> = obj
                        .iter()
                        .filter(|(f, _)| !f.starts_with('_'))
                        .filter_map(|(f, v)| v.as_str().map(|s| (f.clone(), s.to_string())))
                        .collect();
                    h.insert(k.to_string(), m);
                }
            }
        }
        if let Ok(rows) = db.find("_list", &all) {
            let mut l = self.lists.write().unwrap();
            for r in rows {
                if let (Some(k), Some(items)) = (r["_key"].as_str(), r["_items"].as_array()) {
                    let v: VecDeque<String> = items
                        .iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect();
                    l.insert(k.to_string(), v);
                }
            }
        }
        if let Ok(rows) = db.find("_set", &all) {
            let mut st = self.sets.write().unwrap();
            for r in rows {
                if let (Some(k), Some(items)) = (r["_key"].as_str(), r["_members"].as_array()) {
                    let m: HashSet<String> = items
                        .iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect();
                    st.insert(k.to_string(), m);
                }
            }
        }
        if let Ok(rows) = db.find("_zset", &all) {
            let mut z = self.sorted_sets.write().unwrap();
            for r in rows {
                if let (Some(k), Some(items)) = (r["_key"].as_str(), r["_members"].as_array()) {
                    let ss = z.entry(k.to_string()).or_insert_with(SortedSet::new);
                    for it in items {
                        if let (Some(m), Some(sc)) = (it["member"].as_str(), it["score"].as_f64())
                        {
                            ss.insert(m.to_string(), sc);
                        }
                    }
                }
            }
        }
    }

    /// Remove expired string keys eagerly, returning them so the caller can
    /// emit `expired` keyspace events. Run from a periodic sweeper thread.
    pub fn sweep_expired(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut s = self.strings.write().unwrap();
        s.retain(|k, e| {
            if e.is_expired() {
                out.push(k.clone());
                false
            } else {
                true
            }
        });
        drop(s);
        for k in &out {
            self.bump_version(k);
            if self.notify.load(Ordering::Relaxed) {
                self.publish(&format!("__keyspace@0__:{k}"), "expired");
                self.publish("__keyevent@0__:expired", k);
            }
        }
        out
    }

    /// Key counts per type, for metrics gauges.
    pub fn key_counts(&self) -> [u64; 5] {
        [
            self.strings.read().unwrap().len() as u64,
            self.hashes.read().unwrap().len() as u64,
            self.lists.read().unwrap().len() as u64,
            self.sets.read().unwrap().len() as u64,
            self.sorted_sets.read().unwrap().len() as u64,
        ]
    }

    /// Serialize the whole store to JSON (fast-mode snapshot persistence).
    pub fn snapshot_json(&self) -> String {
        let now = now_secs();
        let strings: Vec<serde_json::Value> = self
            .strings.read().unwrap().iter()
            .filter(|(_, e)| !e.is_expired())
            .map(|(k, e)| {
                let t = e.ttl_secs();
                json!({"k": k, "v": e.value, "exp": if t >= 0 { Some(now + t as u64) } else { None }})
            })
            .collect();
        let hashes: Vec<serde_json::Value> = self.hashes.read().unwrap().iter()
            .map(|(k, m)| json!({"k": k, "m": m})).collect();
        let lists: Vec<serde_json::Value> = self.lists.read().unwrap().iter()
            .map(|(k, v)| json!({"k": k, "v": v.iter().collect::<Vec<_>>()})).collect();
        let sets: Vec<serde_json::Value> = self.sets.read().unwrap().iter()
            .map(|(k, m)| json!({"k": k, "m": m.iter().collect::<Vec<_>>()})).collect();
        let zsets: Vec<serde_json::Value> = self.sorted_sets.read().unwrap().iter()
            .map(|(k, z)| {
                let pairs: Vec<serde_json::Value> =
                    z.scores.iter().map(|(m, s)| json!([m, s])).collect();
                json!({"k": k, "z": pairs})
            })
            .collect();
        json!({"s": strings, "h": hashes, "l": lists, "t": sets, "z": zsets}).to_string()
    }

    /// Restore a snapshot produced by `snapshot_json` (expired keys skipped).
    pub fn load_snapshot_json(&self, data: &str) {
        let v: serde_json::Value = match serde_json::from_str(data) {
            Ok(v) => v,
            Err(_) => return,
        };
        let now = now_secs();
        if let Some(arr) = v["s"].as_array() {
            let mut s = self.strings.write().unwrap();
            for it in arr {
                if let (Some(k), Some(val)) = (it["k"].as_str(), it["v"].as_str()) {
                    match it["exp"].as_u64() {
                        Some(exp) if exp <= now => continue,
                        Some(exp) => {
                            s.insert(k.into(), KvEntry::with_ttl(val.into(), exp - now));
                        }
                        None => {
                            s.insert(k.into(), KvEntry::new(val.into()));
                        }
                    }
                }
            }
        }
        if let Some(arr) = v["h"].as_array() {
            let mut h = self.hashes.write().unwrap();
            for it in arr {
                if let (Some(k), Some(m)) = (it["k"].as_str(), it["m"].as_object()) {
                    h.insert(k.into(), m.iter()
                        .filter_map(|(f, x)| x.as_str().map(|s| (f.clone(), s.to_string())))
                        .collect());
                }
            }
        }
        if let Some(arr) = v["l"].as_array() {
            let mut l = self.lists.write().unwrap();
            for it in arr {
                if let (Some(k), Some(items)) = (it["k"].as_str(), it["v"].as_array()) {
                    l.insert(k.into(), items.iter()
                        .filter_map(|x| x.as_str().map(String::from)).collect());
                }
            }
        }
        if let Some(arr) = v["t"].as_array() {
            let mut t = self.sets.write().unwrap();
            for it in arr {
                if let (Some(k), Some(items)) = (it["k"].as_str(), it["m"].as_array()) {
                    t.insert(k.into(), items.iter()
                        .filter_map(|x| x.as_str().map(String::from)).collect());
                }
            }
        }
        if let Some(arr) = v["z"].as_array() {
            let mut zm = self.sorted_sets.write().unwrap();
            for it in arr {
                if let (Some(k), Some(pairs)) = (it["k"].as_str(), it["z"].as_array()) {
                    let z = zm.entry(k.into()).or_insert_with(SortedSet::new);
                    for p in pairs {
                        if let (Some(m), Some(s)) = (p[0].as_str(), p[1].as_f64()) {
                            z.insert(m.to_string(), s);
                        }
                    }
                }
            }
        }
    }

    /// All live keys across every type map (strings filtered by expiry).
    fn all_live_keys(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        {
            let s = self.strings.read().unwrap();
            out.extend(
                s.iter()
                    .filter(|(_, e)| !e.is_expired())
                    .map(|(k, _)| k.clone()),
            );
        }
        out.extend(self.hashes.read().unwrap().keys().cloned());
        out.extend(self.lists.read().unwrap().keys().cloned());
        out.extend(self.sets.read().unwrap().keys().cloned());
        out.extend(self.sorted_sets.read().unwrap().keys().cloned());
        out.sort();
        out.dedup();
        out
    }

    /// Record a whole-store mutation (FLUSHALL/FLUSHDB).
    fn bump_epoch(&self) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
    }

    fn key_version(&self, key: &str) -> u64 {
        self.versions.read().unwrap().get(key).copied().unwrap_or(0)
    }

    /// Whether the key currently holds a live value of any type.
    fn key_exists(&self, key: &str) -> bool {
        {
            let s = self.strings.read().unwrap();
            if let Some(e) = s.get(key) {
                if !e.is_expired() {
                    return true;
                }
            }
        }
        self.hashes.read().unwrap().contains_key(key)
            || self.lists.read().unwrap().contains_key(key)
            || self.sets.read().unwrap().contains_key(key)
            || self.sorted_sets.read().unwrap().contains_key(key)
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Execute a pipeline of commands with lock coalescing.
/// When all commands are the same type, the relevant lock is acquired once.
pub fn execute_pipeline(store: &OxiMemStore, commands: &[RespValue]) -> Vec<RespValue> {
    if commands.is_empty() {
        return vec![];
    }

    // Parse commands: extract (command_name, full_args_slice)
    let parsed: Vec<Option<&[RespValue]>> = commands
        .iter()
        .map(|cmd| {
            if let RespValue::Array(items) = cmd {
                if !items.is_empty() {
                    return Some(items.as_slice());
                }
            }
            None
        })
        .collect();

    for p in parsed.iter().flatten() {
        if let Some(c) = p.first().and_then(|a| a.as_str()) {
            crate::metrics::METRICS.record_oximem(&c.to_uppercase());
        }
    }

    // Check if all commands are the same type
    let first_cmd = parsed
        .first()
        .and_then(|p| p.as_ref())
        .and_then(|items| items[0].as_str())
        .unwrap_or("")
        .to_ascii_uppercase();

    let all_same = !first_cmd.is_empty()
        && parsed.iter().all(|p| {
            p.and_then(|items| items[0].as_str())
                .map(|s| s.eq_ignore_ascii_case(&first_cmd))
                .unwrap_or(false)
        });

    if all_same {
        // Extract args without command name
        let args_list: Vec<&[RespValue]> = parsed
            .iter()
            .filter_map(|p| p.map(|items| &items[1..]))
            .collect();

        // Coalesced write paths bypass execute(), so bump WATCH versions here.
        let bump_all_keys = |store: &OxiMemStore| {
            for args in &args_list {
                if let Some(k) = args.first().and_then(|a| a.as_str()) {
                    store.bump_version(k);
                }
            }
        };
        match first_cmd.as_str() {
            "GET" => return pipeline_get(store, &args_list),
            "SET" => {
                bump_all_keys(store);
                return pipeline_set(store, &args_list);
            }
            "INCR" => {
                bump_all_keys(store);
                return pipeline_incr(store, &args_list);
            }
            "LPUSH" => {
                bump_all_keys(store);
                return pipeline_lpush(store, &args_list);
            }
            "RPUSH" => {
                bump_all_keys(store);
                return pipeline_rpush(store, &args_list);
            }
            "HSET" => {
                bump_all_keys(store);
                return pipeline_hset(store, &args_list);
            }
            "PING" => {
                return args_list
                    .iter()
                    .map(|_| RespValue::SimpleString("PONG".to_string()))
                    .collect();
            }
            _ => {}
        }
    }

    // Fallback: execute one by one
    parsed
        .iter()
        .map(|p| match p {
            Some(items) => execute(store, items),
            None => resp::err("expected array"),
        })
        .collect()
}

fn pipeline_get(store: &OxiMemStore, args_list: &[&[RespValue]]) -> Vec<RespValue> {
    let map = store.strings.read().unwrap();
    args_list
        .iter()
        .map(|args| {
            let key = args.first().and_then(|a| a.as_str()).unwrap_or("");
            match map.get(key) {
                Some(e) if !e.is_expired() => resp::bulk_string(&e.value),
                _ => resp::null(),
            }
        })
        .collect()
}

fn pipeline_set(store: &OxiMemStore, args_list: &[&[RespValue]]) -> Vec<RespValue> {
    let mut map = store.strings.write().unwrap();
    args_list
        .iter()
        .map(|args| {
            if args.len() < 2 {
                return resp::err("wrong number of arguments for 'set' command");
            }
            let key = args[0].as_str().unwrap_or("").to_string();
            let value = args[1].as_str().unwrap_or("").to_string();
            if args.len() >= 4 {
                if let Some(opt) = args[2].as_str() {
                    if opt.eq_ignore_ascii_case("EX") {
                        if let Some(secs) = args[3].as_str().and_then(|s| s.parse::<u64>().ok()) {
                            map.insert(key, KvEntry::with_ttl(value, secs));
                            return resp::ok();
                        }
                    } else if opt.eq_ignore_ascii_case("PX") {
                        if let Some(ms) = args[3].as_str().and_then(|s| s.parse::<u64>().ok()) {
                            map.insert(key, KvEntry::with_ttl(value, ms / 1000));
                            return resp::ok();
                        }
                    }
                }
            }
            map.insert(key, KvEntry::new(value));
            resp::ok()
        })
        .collect()
}

fn pipeline_incr(store: &OxiMemStore, args_list: &[&[RespValue]]) -> Vec<RespValue> {
    let mut map = store.strings.write().unwrap();
    args_list
        .iter()
        .map(|args| {
            let key = args.first().and_then(|a| a.as_str()).unwrap_or("");
            let entry = map
                .entry(key.to_string())
                .or_insert_with(|| KvEntry::new("0".to_string()));
            if entry.is_expired() {
                entry.value = "0".to_string();
                entry.expires_at = None;
            }
            match entry.value.parse::<i64>() {
                Ok(n) => {
                    entry.value = (n + 1).to_string();
                    resp::integer(n + 1)
                }
                Err(_) => resp::err("value is not an integer or out of range"),
            }
        })
        .collect()
}

fn pipeline_lpush(store: &OxiMemStore, args_list: &[&[RespValue]]) -> Vec<RespValue> {
    let mut map = store.lists.write().unwrap();
    args_list
        .iter()
        .map(|args| {
            if args.is_empty() {
                return resp::err("wrong number of arguments for 'lpush' command");
            }
            let key = args[0].as_str().unwrap_or("");
            let list = map.entry(key.to_string()).or_default();
            for arg in args[1..].iter().rev() {
                list.push_front(arg.as_str().unwrap_or("").to_string());
            }
            resp::integer(list.len() as i64)
        })
        .collect()
}

fn pipeline_rpush(store: &OxiMemStore, args_list: &[&[RespValue]]) -> Vec<RespValue> {
    let mut map = store.lists.write().unwrap();
    args_list
        .iter()
        .map(|args| {
            if args.is_empty() {
                return resp::err("wrong number of arguments for 'rpush' command");
            }
            let key = args[0].as_str().unwrap_or("");
            let list = map.entry(key.to_string()).or_default();
            for arg in &args[1..] {
                list.push_back(arg.as_str().unwrap_or("").to_string());
            }
            resp::integer(list.len() as i64)
        })
        .collect()
}

fn pipeline_hset(store: &OxiMemStore, args_list: &[&[RespValue]]) -> Vec<RespValue> {
    let mut map = store.hashes.write().unwrap();
    args_list
        .iter()
        .map(|args| {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return resp::err("wrong number of arguments for 'hset' command");
            }
            let key = args[0].as_str().unwrap_or("");
            let hash = map.entry(key.to_string()).or_default();
            let mut added = 0i64;
            for pair in args[1..].chunks(2) {
                let field = pair[0].as_str().unwrap_or("");
                let value = pair[1].as_str().unwrap_or("").to_string();
                if !hash.contains_key(field) {
                    added += 1;
                }
                hash.insert(field.to_string(), value);
            }
            resp::integer(added)
        })
        .collect()
}

/// Execute a Redis command against the native store.
/// Per-connection transaction state for MULTI / EXEC / WATCH.
#[derive(Default)]
pub struct Session {
    in_multi: bool,
    aborted: bool,
    queued: Vec<Vec<RespValue>>,
    watched: Vec<(String, KeySnap)>,
}

impl Session {
    /// True while mid-MULTI or holding WATCHes — the caller must then route
    /// every command through `execute_session` (not the batched fast path).
    pub fn is_active(&self) -> bool {
        self.in_multi || !self.watched.is_empty()
    }
    fn clear_watches(&mut self, store: &OxiMemStore) {
        if !self.watched.is_empty() {
            store.active_watches.fetch_sub(1, Ordering::SeqCst);
            self.watched.clear();
        }
    }
    fn reset(&mut self, store: &OxiMemStore) {
        self.in_multi = false;
        self.aborted = false;
        self.queued.clear();
        self.clear_watches(store);
    }
}

/// Whether a command participates in transaction control (so a pipeline
/// containing one must be run statefully rather than lock-coalesced).
pub fn is_tx_command(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH"
    )
}

/// Every command the dispatcher understands — used to validate commands at
/// MULTI queue time, so a typo aborts the transaction (Redis EXECABORT
/// semantics) instead of failing half-way through EXEC.
fn is_known_command(name: &str) -> bool {
    matches!(
        name,
        "PING" | "ECHO" | "QUIT" | "SELECT" | "COMMAND" | "CLIENT" | "AUTH" | "CONFIG"
            | "INFO" | "DBSIZE" | "FLUSHALL" | "FLUSHDB" | "KEYS" | "SCAN" | "RANDOMKEY"
            | "RENAME" | "TYPE" | "EXISTS" | "DEL" | "EXPIRE" | "PEXPIRE" | "EXPIREAT"
            | "PERSIST" | "TTL" | "PTTL" | "SET" | "GET" | "GETSET" | "SETNX" | "SETEX"
            | "PSETEX" | "MSET" | "MGET" | "INCR" | "DECR" | "INCRBY" | "DECRBY"
            | "INCRBYFLOAT" | "DECRBYFLOATGE" | "APPEND" | "STRLEN" | "GETRANGE" | "HSET" | "HMSET"
            | "HSETNX" | "HGET" | "HMGET" | "HGETALL" | "HDEL" | "HEXISTS" | "HKEYS"
            | "HVALS" | "HLEN" | "HINCRBY" | "LPUSH" | "RPUSH" | "LPOP" | "RPOP"
            | "LLEN" | "LRANGE" | "LINDEX" | "SADD" | "SREM" | "SMEMBERS" | "SISMEMBER"
            | "SCARD" | "ZADD" | "ZREM" | "ZSCORE" | "ZCARD" | "ZCOUNT" | "ZINCRBY"
            | "ZRANK" | "ZREVRANK" | "ZRANGE" | "ZREVRANGE" | "ZRANGEBYSCORE"
            | "ZREVRANGEBYSCORE" | "ZPOPMIN" | "ZPOPMAX" | "PUBLISH" | "HELLO"
            | "GETDEL" | "SETRANGE" | "PEXPIREAT" | "COPY" | "SINTER" | "SUNION"
            | "SDIFF" | "SINTERSTORE" | "SUNIONSTORE" | "SDIFFSTORE" | "HRANDFIELD"
            | "ZRANGEBYLEX" | "BLPOP" | "BRPOP" | "BZPOPMIN" | "EVAL" | "EVALSHA" | "SCRIPT"
            | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "ZUNIONSTORE" | "ZINTERSTORE"
            | "LREM" | "LSET" | "LTRIM" | "RPOPLPUSH" | "LMOVE" | "SPOP" | "SRANDMEMBER"
            | "HSCAN" | "SSCAN" | "ZSCAN" | "GETEX" | "SETBIT" | "GETBIT" | "BITCOUNT"
            | "SMISMEMBER" | "LMPOP" | "ZMPOP" | "BLMOVE" | "BRPOPLPUSH" | "PUNSUBSCRIBE"
    )
}

/// O(1) fingerprint of a key for WATCH change detection: the store epoch
/// (bumped by FLUSHALL), the key's mutation counter (bumped by every write
/// command touching it), and whether the key currently exists (so a lazy
/// TTL expiry between WATCH and EXEC still registers as a change).
type KeySnap = (u64, u64, bool);

fn snapshot_key(store: &OxiMemStore, key: &str) -> KeySnap {
    (
        store.epoch.load(Ordering::SeqCst),
        store.key_version(key),
        store.key_exists(key),
    )
}

/// Session-aware entry point: handles MULTI/EXEC/DISCARD/WATCH/UNWATCH, queues
/// commands while in MULTI, and delegates everything else to `execute`.
///
/// Isolation model: EXEC takes `store.tx_lock`, so two EXEC blocks never
/// interleave their queued commands, and re-checks WATCHed keys under that lock
/// — if any changed since WATCH, EXEC aborts (returns nil) and the client
/// retries. This is the standard Redis optimistic-transaction primitive, and is
/// exactly what a multi-account settlement needs: WATCH the two cash accounts,
/// verify sufficiency, then move funds atomically or retry.
pub fn execute_session(store: &OxiMemStore, sess: &mut Session, args: &[RespValue]) -> RespValue {
    let t0 = Instant::now();
    let r = execute_session_inner(store, sess, args);
    crate::metrics::METRICS.record_oximem_latency(t0.elapsed().as_micros() as u64);
    r
}

fn execute_session_inner(store: &OxiMemStore, sess: &mut Session, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("empty command");
    }
    let cmd = match args[0].as_str() {
        Some(s) => s.to_uppercase(),
        None => return resp::err("invalid command"),
    };
    crate::metrics::METRICS.record_oximem(&cmd);
    match cmd.as_str() {
        "MULTI" => {
            if sess.in_multi {
                return resp::err("MULTI calls can not be nested");
            }
            sess.in_multi = true;
            sess.aborted = false;
            sess.queued.clear();
            resp::ok()
        }
        "DISCARD" => {
            if !sess.in_multi {
                return resp::err("DISCARD without MULTI");
            }
            sess.reset(store);
            resp::ok()
        }
        "UNWATCH" => {
            sess.clear_watches(store);
            resp::ok()
        }
        "WATCH" => {
            if sess.in_multi {
                return resp::err("WATCH inside MULTI is not allowed");
            }
            if args.len() < 2 {
                return resp::err("wrong number of arguments for 'watch' command");
            }
            if sess.watched.is_empty() {
                store.active_watches.fetch_add(1, Ordering::SeqCst);
            }
            for a in &args[1..] {
                if let Some(k) = a.as_str() {
                    let snap = snapshot_key(store, k);
                    sess.watched.push((k.to_string(), snap));
                }
            }
            resp::ok()
        }
        "EXEC" => {
            if !sess.in_multi {
                return resp::err("EXEC without MULTI");
            }
            if sess.aborted {
                sess.reset(store);
                return resp::err(
                    "EXECABORT Transaction discarded because of previous errors.",
                );
            }
            let _gate = store.eval_gate.write().unwrap(); // exclude running scripts
            let _guard = store.tx_lock.lock().unwrap();
            for (key, snap) in &sess.watched {
                if &snapshot_key(store, key) != snap {
                    sess.reset(store);
                    return RespValue::NullArray; // a WATCHed key changed — abort
                }
            }
            let queued = std::mem::take(&mut sess.queued);
            let mut out = Vec::with_capacity(queued.len());
            for qcmd in &queued {
                // Blocking commands never block inside a transaction (Redis
                // semantics) — they'd stall every EXEC via tx_lock. Rewrite
                // their timeout to "poll once".
                if let Some(name) = qcmd.first().and_then(|a| a.as_str()) {
                    if matches!(
                        name.to_uppercase().as_str(),
                        "BLPOP" | "BRPOP" | "BZPOPMIN" | "BLMOVE" | "BRPOPLPUSH"
                    ) {
                        let mut nb = qcmd.clone();
                        let last = nb.len() - 1;
                        nb[last] = resp::bulk_string("-1"); // sentinel: single poll
                        out.push(execute(store, &nb));
                        continue;
                    }
                }
                out.push(execute(store, qcmd));
            }
            sess.reset(store);
            RespValue::Array(out)
        }
        _ => {
            if sess.in_multi {
                // Queue-time validation: an unknown command poisons the whole
                // transaction (Redis EXECABORT semantics) rather than failing
                // part-way through EXEC.
                if !is_known_command(&cmd) {
                    sess.aborted = true;
                    return resp::err(&format!("unknown command '{cmd}'"));
                }
                if args.len() < min_args(&cmd) {
                    sess.aborted = true;
                    return resp::err(&format!(
                        "wrong number of arguments for '{}' command",
                        cmd.to_lowercase()
                    ));
                }
                sess.queued.push(args.to_vec());
                RespValue::SimpleString("QUEUED".to_string())
            } else {
                execute(store, args)
            }
        }
    }
}

/// Which argument positions hold keys a command mutates (for WATCH version
/// bumps). Empty for read-only / unknown commands; `None` marks FLUSH-class
/// commands that invalidate everything.
fn write_key_indices(cmd: &str, argc: usize) -> Option<Vec<usize>> {
    match cmd {
        "SET" | "SETNX" | "SETEX" | "PSETEX" | "GETSET" | "APPEND" | "INCR" | "DECR"
        | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "DECRBYFLOATGE" | "EXPIRE" | "PEXPIRE" | "EXPIREAT"
        | "PERSIST" | "HSET" | "HMSET" | "HSETNX" | "HDEL" | "HINCRBY" | "LPUSH" | "RPUSH"
        | "LPOP" | "RPOP" | "SADD" | "SREM" | "ZADD" | "ZINCRBY" | "ZREM" | "ZPOPMIN"
        | "ZPOPMAX" | "GETDEL" | "SETRANGE" | "PEXPIREAT" | "SINTERSTORE" | "SUNIONSTORE"
        | "SDIFFSTORE" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "ZUNIONSTORE"
        | "ZINTERSTORE" | "LREM" | "LSET" | "LTRIM" | "SPOP" | "GETEX" | "SETBIT" => {
            Some(vec![1])
        }
        "RPOPLPUSH" | "LMOVE" | "BLMOVE" | "BRPOPLPUSH" => Some(vec![1, 2]),
        "LMPOP" | "ZMPOP" => Some((2..argc).collect()),
        "DEL" => Some((1..argc).collect()),
        "RENAME" => Some(vec![1, 2]),
        "COPY" => Some(vec![2]),
        "MSET" => Some((1..argc).step_by(2).collect()),
        // Blocking pops may consume from any of their keys (last arg = timeout).
        "BLPOP" | "BRPOP" | "BZPOPMIN" => Some((1..argc.saturating_sub(1)).collect()),
        "FLUSHALL" | "FLUSHDB" => None, // whole-store: bump the epoch
        _ => Some(vec![]),
    }
}

/// Minimum argc (command name included) for queue-time arity validation.
fn min_args(cmd: &str) -> usize {
    match cmd {
        "GET" | "DEL" | "EXISTS" | "TTL" | "PTTL" | "TYPE" | "STRLEN" | "INCR" | "DECR"
        | "PERSIST" | "GETDEL" | "HGETALL" | "HKEYS" | "HVALS" | "HLEN" | "LLEN" | "LPOP"
        | "RPOP" | "SMEMBERS" | "SCARD" | "ZCARD" | "SINTER" | "SUNION" | "SDIFF"
        | "HRANDFIELD" | "SPOP" | "SRANDMEMBER" | "BITCOUNT" | "GETEX" => 2,
        "SET" | "GETSET" | "SETNX" | "APPEND" | "INCRBY" | "DECRBY" | "INCRBYFLOAT"
        | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "RENAME" | "HGET" | "HDEL"
        | "HEXISTS" | "LPUSH" | "RPUSH" | "LINDEX" | "SADD" | "SREM" | "SISMEMBER"
        | "ZREM" | "ZSCORE" | "ZRANK" | "ZREVRANK" | "PUBLISH" | "MSET" | "MGET"
        | "COPY" | "SINTERSTORE" | "SUNIONSTORE" | "SDIFFSTORE" | "BLPOP" | "BRPOP"
        | "BZPOPMIN" | "GETBIT" | "HSCAN" | "SSCAN" | "ZSCAN" | "RPOPLPUSH" => 3,
        "SETEX" | "PSETEX" | "SETRANGE" | "HSET" | "HMSET" | "HSETNX" | "HINCRBY"
        | "LRANGE" | "ZADD" | "ZINCRBY" | "ZCOUNT" | "ZRANGE" | "ZREVRANGE"
        | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZRANGEBYLEX" | "HMGET" | "LREM" | "LSET"
        | "LTRIM" | "SETBIT" | "ZREMRANGEBYRANK" | "ZREMRANGEBYSCORE" | "ZUNIONSTORE"
        | "ZINTERSTORE" => 4,
        "LMOVE" => 5,
        _ => 1,
    }
}

/// Bump WATCH version counters for every key `args` mutates.
fn bump_write_versions(store: &OxiMemStore, args: &[RespValue]) {
    let cmd = match args.first().and_then(|a| a.as_str()) {
        Some(s) => s.to_uppercase(),
        None => return,
    };
    match write_key_indices(&cmd, args.len()) {
        None => store.bump_epoch(),
        Some(idxs) => {
            for i in idxs {
                if let Some(k) = args.get(i).and_then(|a| a.as_str()) {
                    store.bump_version(k);
                }
            }
        }
    }
}

pub fn execute(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let result = execute_cmd(store, args);
    // Errors don't modify state; anything else on a write command might have,
    // so bump conservatively (a spurious WATCH abort just retries — a missed
    // change would break the optimistic lock).
    if !matches!(result, RespValue::Error(_)) {
        bump_write_versions(store, args);
        if store.notify.load(Ordering::Relaxed) {
            emit_keyspace_events(store, args);
        }
        // Wake blocked pop waiters on any list/zset write.
        if let Some(cmd) = args.first().and_then(|a| a.as_str()) {
            if matches!(
                cmd.to_uppercase().as_str(),
                "LPUSH" | "RPUSH" | "ZADD" | "ZINCRBY" | "LMOVE" | "RPOPLPUSH" | "LSET"
            ) {
                let (lock, cv) = &*store.write_cv;
                *lock.lock().unwrap() += 1;
                cv.notify_all();
            }
        }
        // Sorted sets are mirrored centrally (their write commands predate the
        // mirror layer): re-snapshot the touched zset after any zset write.
        if store.sql_enabled() {
            if let Some(cmd) = args.first().and_then(|a| a.as_str()) {
                if matches!(
                    cmd.to_uppercase().as_str(),
                    "ZADD" | "ZREM" | "ZINCRBY" | "ZPOPMIN" | "ZPOPMAX" | "BZPOPMIN"
                ) {
                    if let Some(key) = args.get(1).and_then(|a| a.as_str()) {
                        let pairs: Vec<(String, f64)> = store
                            .sorted_sets
                            .read()
                            .unwrap()
                            .get(key)
                            .map(|ss| ss.scores.iter().map(|(m, &s)| (m.clone(), s)).collect())
                            .unwrap_or_default();
                        if pairs.is_empty() {
                            mirror_zset_del(store, key);
                        } else {
                            mirror_zset_save(store, key, &pairs);
                        }
                    }
                }
            }
        }
    }
    result
}

/// Keyspace notifications over the existing pub/sub: for every key a write
/// command touched, publish `__keyspace@0__:<key>` (payload = event) and
/// `__keyevent@0__:<event>` (payload = key). Event name = lowercased command.
fn emit_keyspace_events(store: &OxiMemStore, args: &[RespValue]) {
    let cmd = match args.first().and_then(|a| a.as_str()) {
        Some(s) => s.to_uppercase(),
        None => return,
    };
    let idxs = match write_key_indices(&cmd, args.len()) {
        Some(v) if !v.is_empty() => v,
        _ => return,
    };
    let event = cmd.to_lowercase();
    for i in idxs {
        if let Some(key) = args.get(i).and_then(|a| a.as_str()) {
            store.publish(&format!("__keyspace@0__:{key}"), &event);
            store.publish(&format!("__keyevent@0__:{event}"), key);
        }
    }
}

fn execute_cmd(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("empty command");
    }

    let cmd = match args[0].as_str() {
        Some(s) => s.to_uppercase(),
        None => return resp::err("invalid command"),
    };

    match cmd.as_str() {
        // -- Connection --
        "PING" => {
            if args.len() > 1 {
                if let Some(msg) = args[1].as_bytes() {
                    return resp::bulk(msg);
                }
            }
            RespValue::SimpleString("PONG".to_string())
        }
        "ECHO" => {
            if args.len() < 2 {
                return resp::err("wrong number of arguments for 'echo' command");
            }
            match args[1].as_bytes() {
                Some(b) => resp::bulk(b),
                None => resp::null(),
            }
        }
        "QUIT" => RespValue::SimpleString("OK".to_string()),
        "SELECT" => resp::ok(),
        "COMMAND" => resp::ok(),
        "CLIENT" => resp::ok(),
        "AUTH" => resp::ok(),
        "HELLO" => cmd_hello(&args[1..]),
        "GETDEL" => cmd_getdel(store, &args[1..]),
        "DECRBYFLOATGE" => cmd_decrbyfloatge(store, &args[1..]),
        "SETRANGE" => cmd_setrange(store, &args[1..]),
        "PEXPIREAT" => cmd_pexpireat(store, &args[1..]),
        "COPY" => cmd_copy(store, &args[1..]),
        "SINTER" => cmd_setop(store, &args[1..], SetOp::Inter, false),
        "SUNION" => cmd_setop(store, &args[1..], SetOp::Union, false),
        "SDIFF" => cmd_setop(store, &args[1..], SetOp::Diff, false),
        "SINTERSTORE" => cmd_setop(store, &args[1..], SetOp::Inter, true),
        "SUNIONSTORE" => cmd_setop(store, &args[1..], SetOp::Union, true),
        "SDIFFSTORE" => cmd_setop(store, &args[1..], SetOp::Diff, true),
        "HRANDFIELD" => cmd_hrandfield(store, &args[1..]),
        "ZRANGEBYLEX" => cmd_zrangebylex(store, &args[1..]),
        "BLPOP" => cmd_bpop(store, &args[1..], PopSide::Left),
        "BRPOP" => cmd_bpop(store, &args[1..], PopSide::Right),
        "BZPOPMIN" => cmd_bpop(store, &args[1..], PopSide::ZMin),
        "ZREMRANGEBYRANK" => cmd_zremrangebyrank(store, &args[1..]),
        "ZREMRANGEBYSCORE" => cmd_zremrangebyscore(store, &args[1..]),
        "ZUNIONSTORE" => cmd_zsetstore(store, &args[1..], false),
        "ZINTERSTORE" => cmd_zsetstore(store, &args[1..], true),
        "LREM" => cmd_lrem(store, &args[1..]),
        "LSET" => cmd_lset(store, &args[1..]),
        "LTRIM" => cmd_ltrim(store, &args[1..]),
        "RPOPLPUSH" => cmd_lmove(store, &args[1..], true),
        "LMOVE" => cmd_lmove(store, &args[1..], false),
        "SPOP" => cmd_spop(store, &args[1..]),
        "SRANDMEMBER" => cmd_srandmember(store, &args[1..]),
        "HSCAN" => cmd_subscan(store, &args[1..], b'h'),
        "SSCAN" => cmd_subscan(store, &args[1..], b's'),
        "ZSCAN" => cmd_subscan(store, &args[1..], b'z'),
        "GETEX" => cmd_getex(store, &args[1..]),
        "SETBIT" => cmd_setbit(store, &args[1..]),
        "GETBIT" => cmd_getbit(store, &args[1..]),
        "BITCOUNT" => cmd_bitcount(store, &args[1..]),
        "SMISMEMBER" => cmd_smismember(store, &args[1..]),
        "LMPOP" => cmd_mpop(store, &args[1..], false),
        "ZMPOP" => cmd_mpop(store, &args[1..], true),
        "BLMOVE" => cmd_blmove(store, &args[1..], false),
        "BRPOPLPUSH" => cmd_blmove(store, &args[1..], true),
        "PUNSUBSCRIBE" => resp::ok(),
        "EVAL" => cmd_eval(store, &args[1..], false),
        "EVALSHA" => cmd_eval(store, &args[1..], true),
        "SCRIPT" => cmd_script(store, &args[1..]),

        // -- String commands --
        "SET" => cmd_set(store, &args[1..]),
        "GET" => cmd_get(store, &args[1..]),
        "GETSET" => cmd_getset(store, &args[1..]),
        "SETNX" => cmd_setnx(store, &args[1..]),
        "SETEX" => cmd_setex(store, &args[1..]),
        "PSETEX" => cmd_psetex(store, &args[1..]),
        "MSET" => cmd_mset(store, &args[1..]),
        "MGET" => cmd_mget(store, &args[1..]),
        "INCR" => cmd_incr(store, &args[1..], 1),
        "DECR" => cmd_incr(store, &args[1..], -1),
        "INCRBY" => cmd_incrby(store, &args[1..]),
        "DECRBY" => cmd_decrby(store, &args[1..]),
        "INCRBYFLOAT" => cmd_incrbyfloat(store, &args[1..]),
        "APPEND" => cmd_append(store, &args[1..]),
        "STRLEN" => cmd_strlen(store, &args[1..]),
        "GETRANGE" => cmd_getrange(store, &args[1..]),

        // -- Key commands --
        "DEL" => cmd_del(store, &args[1..]),
        "EXISTS" => cmd_exists(store, &args[1..]),
        "EXPIRE" => cmd_expire(store, &args[1..]),
        "PEXPIRE" => cmd_pexpire(store, &args[1..]),
        "EXPIREAT" => cmd_expireat(store, &args[1..]),
        "PERSIST" => cmd_persist(store, &args[1..]),
        "TTL" => cmd_ttl(store, &args[1..]),
        "PTTL" => cmd_pttl(store, &args[1..]),
        "TYPE" => cmd_type(store, &args[1..]),
        "KEYS" => cmd_keys(store, &args[1..]),
        "RENAME" => cmd_rename(store, &args[1..]),
        "RANDOMKEY" => cmd_randomkey(store),
        "DBSIZE" => cmd_dbsize(store),
        "FLUSHDB" | "FLUSHALL" => cmd_flushdb(store),
        "SCAN" => cmd_scan(store, &args[1..]),

        // -- Hash commands --
        "HSET" | "HMSET" => cmd_hset(store, &args[1..]),
        "HGET" => cmd_hget(store, &args[1..]),
        "HMGET" => cmd_hmget(store, &args[1..]),
        "HDEL" => cmd_hdel(store, &args[1..]),
        "HEXISTS" => cmd_hexists(store, &args[1..]),
        "HGETALL" => cmd_hgetall(store, &args[1..]),
        "HKEYS" => cmd_hkeys(store, &args[1..]),
        "HVALS" => cmd_hvals(store, &args[1..]),
        "HLEN" => cmd_hlen(store, &args[1..]),
        "HINCRBY" => cmd_hincrby(store, &args[1..]),
        "HSETNX" => cmd_hsetnx(store, &args[1..]),

        // -- List commands --
        "LPUSH" => cmd_lpush(store, &args[1..]),
        "RPUSH" => cmd_rpush(store, &args[1..]),
        "LPOP" => cmd_lpop(store, &args[1..]),
        "RPOP" => cmd_rpop(store, &args[1..]),
        "LLEN" => cmd_llen(store, &args[1..]),
        "LRANGE" => cmd_lrange(store, &args[1..]),
        "LINDEX" => cmd_lindex(store, &args[1..]),

        // -- Set commands --
        "SADD" => cmd_sadd(store, &args[1..]),
        "SREM" => cmd_srem(store, &args[1..]),
        "SMEMBERS" => cmd_smembers(store, &args[1..]),
        "SISMEMBER" => cmd_sismember(store, &args[1..]),
        "SCARD" => cmd_scard(store, &args[1..]),

        // -- Sorted set commands --
        "ZADD" => cmd_zadd(store, &args[1..]),
        "ZREM" => cmd_zrem(store, &args[1..]),
        "ZSCORE" => cmd_zscore(store, &args[1..]),
        "ZRANK" => cmd_zrank(store, &args[1..]),
        "ZREVRANK" => cmd_zrevrank(store, &args[1..]),
        "ZRANGE" => cmd_zrange(store, &args[1..], false),
        "ZREVRANGE" => cmd_zrange(store, &args[1..], true),
        "ZRANGEBYSCORE" => cmd_zrangebyscore(store, &args[1..], false),
        "ZREVRANGEBYSCORE" => cmd_zrangebyscore(store, &args[1..], true),
        "ZCARD" => cmd_zcard(store, &args[1..]),
        "ZCOUNT" => cmd_zcount(store, &args[1..]),
        "ZINCRBY" => cmd_zincrby(store, &args[1..]),
        "ZPOPMIN" => cmd_zpopmin(store, &args[1..]),
        "ZPOPMAX" => cmd_zpopmax(store, &args[1..]),

        // -- Pub/Sub --
        "PUBLISH" => cmd_publish(store, &args[1..]),
        // SUBSCRIBE/UNSUBSCRIBE handled in connection handler

        // -- Server commands --
        "INFO" => cmd_info(store),
        "CONFIG" => cmd_config(store, &args[1..]),

        _ => resp::err(&format!("unknown command '{cmd}'")),
    }
}

// ===========================================================================
// SQL mirror helpers
// ===========================================================================

fn mirror_kv_set(store: &OxiMemStore, key: &str, value: &str, ttl: Option<u64>) {
    if let Some(db) = &store.db {
        let _ = db.delete("_kv", &json!({"_key": key}));
        let mut doc = json!({"_key": key, "_value": value});
        if let Some(t) = ttl {
            doc["_ttl"] = json!(t);
            // Absolute deadline so a restart can restore the REMAINING ttl.
            doc["_exp"] = json!(now_secs() + t);
        }
        let _ = db.insert("_kv", doc);
    }
}

fn mirror_kv_del(store: &OxiMemStore, key: &str) {
    if let Some(db) = &store.db {
        let _ = db.delete("_kv", &json!({"_key": key}));
    }
}

fn mirror_hash_save(store: &OxiMemStore, key: &str, fields: &HashMap<String, String>) {
    if let Some(db) = &store.db {
        let _ = db.delete("_hash", &json!({"_key": key}));
        let mut doc = json!({"_key": key});
        for (f, v) in fields {
            doc[f] = json!(v);
        }
        let _ = db.insert("_hash", doc);
    }
}

fn mirror_hash_del(store: &OxiMemStore, key: &str) {
    if let Some(db) = &store.db {
        let _ = db.delete("_hash", &json!({"_key": key}));
    }
}

fn mirror_list_save(store: &OxiMemStore, key: &str, items: &VecDeque<String>) {
    if let Some(db) = &store.db {
        let _ = db.delete("_list", &json!({"_key": key}));
        if !items.is_empty() {
            let arr: Vec<&str> = items.iter().map(|s| s.as_str()).collect();
            let _ = db.insert("_list", json!({"_key": key, "_items": arr}));
        }
    }
}

fn mirror_list_del(store: &OxiMemStore, key: &str) {
    if let Some(db) = &store.db {
        let _ = db.delete("_list", &json!({"_key": key}));
    }
}

fn mirror_set_save(store: &OxiMemStore, key: &str, members: &HashSet<String>) {
    if let Some(db) = &store.db {
        let _ = db.delete("_set", &json!({"_key": key}));
        if !members.is_empty() {
            let arr: Vec<&str> = members.iter().map(|s| s.as_str()).collect();
            let _ = db.insert("_set", json!({"_key": key, "_members": arr}));
        }
    }
}

fn mirror_set_del(store: &OxiMemStore, key: &str) {
    if let Some(db) = &store.db {
        let _ = db.delete("_set", &json!({"_key": key}));
    }
}

fn mirror_zset_save(store: &OxiMemStore, key: &str, pairs: &[(String, f64)]) {
    if let Some(db) = &store.db {
        let _ = db.delete("_zset", &json!({"_key": key}));
        if !pairs.is_empty() {
            let arr: Vec<serde_json::Value> = pairs
                .iter()
                .map(|(m, s)| json!({"member": m, "score": s}))
                .collect();
            let _ = db.insert("_zset", json!({"_key": key, "_members": arr}));
        }
    }
}

fn mirror_zset_del(store: &OxiMemStore, key: &str) {
    if let Some(db) = &store.db {
        let _ = db.delete("_zset", &json!({"_key": key}));
    }
}

fn mirror_flush(store: &OxiMemStore) {
    if let Some(db) = &store.db {
        let _ = db.drop_collection("_kv");
        let _ = db.drop_collection("_hash");
        let _ = db.drop_collection("_list");
        let _ = db.drop_collection("_set");
        let _ = db.drop_collection("_zset");
    }
}

// ===========================================================================
// String commands
// ===========================================================================

fn cmd_set(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'set' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };
    let value = match args[1].as_str() {
        Some(v) => v.to_string(),
        None => match args[1].as_bytes() {
            Some(b) => String::from_utf8_lossy(b).to_string(),
            None => return resp::err("invalid value"),
        },
    };

    let mut ttl_secs: Option<u64> = None;
    let mut nx = false;
    let mut xx = false;
    let mut i = 2;
    while i < args.len() {
        let flag = args[i].as_str().map(|s| s.to_uppercase());
        match flag.as_deref() {
            Some("EX") => {
                i += 1;
                ttl_secs = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok());
            }
            Some("PX") => {
                i += 1;
                let ms: Option<u64> = args
                    .get(i)
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse().ok());
                ttl_secs = ms.map(|ms| (ms + 999) / 1000);
            }
            Some("NX") => nx = true,
            Some("XX") => xx = true,
            _ => {}
        }
        i += 1;
    }

    let mut map = store.strings.write().unwrap();

    // Clean expired
    if let Some(entry) = map.get(key) {
        if entry.is_expired() {
            map.remove(key);
        }
    }

    if nx && map.contains_key(key) {
        return resp::null();
    }
    if xx && !map.contains_key(key) {
        return resp::null();
    }

    let entry = match ttl_secs {
        Some(t) => KvEntry::with_ttl(value.clone(), t),
        None => KvEntry::new(value.clone()),
    };
    map.insert(key.to_string(), entry);
    drop(map);

    mirror_kv_set(store, key, &value, ttl_secs);
    resp::ok()
}

fn cmd_get(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'get' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::null(),
    };

    let mut map = store.strings.write().unwrap();
    if let Some(entry) = map.get(key) {
        if entry.is_expired() {
            let k = key.to_string();
            map.remove(&k);
            drop(map);
            mirror_kv_del(store, &k);
            return resp::null();
        }
        let v = entry.value.clone();
        return resp::bulk_string(&v);
    }
    resp::null()
}

fn cmd_getset(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'getset' command");
    }
    let old = cmd_get(store, &args[..1]);
    cmd_set(store, args);
    old
}

fn cmd_setnx(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'setnx' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };

    let mut map = store.strings.write().unwrap();
    // Clean expired
    if let Some(entry) = map.get(key) {
        if entry.is_expired() {
            map.remove(key);
        }
    }
    if map.contains_key(key) {
        return resp::integer(0);
    }

    let value = args[1].as_str().unwrap_or("").to_string();
    map.insert(key.to_string(), KvEntry::new(value.clone()));
    drop(map);

    mirror_kv_set(store, key, &value, None);
    resp::integer(1)
}

fn cmd_setex(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'setex' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };
    let seconds: u64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => return resp::err("value is not an integer or out of range"),
    };
    let value = args[2].as_str().unwrap_or("").to_string();

    store
        .strings
        .write()
        .unwrap()
        .insert(key.to_string(), KvEntry::with_ttl(value.clone(), seconds));
    mirror_kv_set(store, key, &value, Some(seconds));
    resp::ok()
}

fn cmd_psetex(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'psetex' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };
    let ms: u64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => return resp::err("value is not an integer or out of range"),
    };
    let value = args[2].as_str().unwrap_or("").to_string();
    let secs = (ms + 999) / 1000;

    store
        .strings
        .write()
        .unwrap()
        .insert(key.to_string(), KvEntry::with_ttl(value.clone(), secs));
    mirror_kv_set(store, key, &value, Some(secs));
    resp::ok()
}

fn cmd_mset(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 || args.len() % 2 != 0 {
        return resp::err("wrong number of arguments for 'mset' command");
    }
    let mut map = store.strings.write().unwrap();
    for pair in args.chunks(2) {
        let key = match pair[0].as_str() {
            Some(k) => k,
            None => continue,
        };
        let value = match pair[1].as_str() {
            Some(v) => v.to_string(),
            None => match pair[1].as_bytes() {
                Some(b) => String::from_utf8_lossy(b).to_string(),
                None => continue,
            },
        };
        map.insert(key.to_string(), KvEntry::new(value));
    }
    drop(map);

    if store.sql_enabled() {
        for pair in args.chunks(2) {
            if let (Some(k), Some(v)) = (pair[0].as_str(), pair[1].as_str()) {
                mirror_kv_set(store, k, v, None);
            }
        }
    }
    resp::ok()
}

fn cmd_mget(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let map = store.strings.read().unwrap();
    let results: Vec<RespValue> = args
        .iter()
        .map(|arg| {
            let key = arg.as_str().unwrap_or("");
            match map.get(key) {
                Some(entry) if !entry.is_expired() => resp::bulk_string(&entry.value),
                _ => resp::null(),
            }
        })
        .collect();
    resp::array(results)
}

fn cmd_incr(store: &OxiMemStore, args: &[RespValue], delta: i64) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'incr' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };

    let mut map = store.strings.write().unwrap();
    // Clean expired
    if let Some(entry) = map.get(key) {
        if entry.is_expired() {
            map.remove(key);
        }
    }

    let current: i64 = match map.get(key) {
        Some(entry) => match entry.value.parse() {
            Ok(n) => n,
            Err(_) => return resp::err("value is not an integer or out of range"),
        },
        None => 0,
    };

    let new_val = current + delta;
    let val_str = new_val.to_string();
    // Preserve TTL if existing
    let entry = match map.get(key).and_then(|e| e.expires_at) {
        Some(exp) => KvEntry {
            value: val_str.clone(),
            expires_at: Some(exp),
        },
        None => KvEntry::new(val_str.clone()),
    };
    map.insert(key.to_string(), entry);
    drop(map);

    mirror_kv_set(store, key, &val_str, None);
    resp::integer(new_val)
}

fn cmd_incrby(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'incrby' command");
    }
    let delta: i64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => return resp::err("value is not an integer or out of range"),
    };
    cmd_incr(store, &args[..1], delta)
}

fn cmd_decrby(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'decrby' command");
    }
    let delta: i64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => return resp::err("value is not an integer or out of range"),
    };
    cmd_incr(store, &args[..1], -delta)
}

fn cmd_incrbyfloat(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'incrbyfloat' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };
    let delta: f64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => return resp::err("value is not a valid float"),
    };

    let mut map = store.strings.write().unwrap();
    let current: f64 = match map.get(key) {
        Some(entry) if !entry.is_expired() => entry.value.parse().unwrap_or(0.0),
        _ => 0.0,
    };

    let new_val = current + delta;
    let val_str = format!("{new_val}");
    map.insert(key.to_string(), KvEntry::new(val_str.clone()));
    drop(map);

    mirror_kv_set(store, key, &val_str, None);
    resp::bulk_string(&val_str)
}

fn cmd_append(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'append' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };
    let suffix = args[1].as_str().unwrap_or("");

    let mut map = store.strings.write().unwrap();
    let new_val = match map.get(key) {
        Some(entry) if !entry.is_expired() => format!("{}{}", entry.value, suffix),
        _ => suffix.to_string(),
    };
    let len = new_val.len() as i64;
    map.insert(key.to_string(), KvEntry::new(new_val.clone()));
    drop(map);

    mirror_kv_set(store, key, &new_val, None);
    resp::integer(len)
}

fn cmd_strlen(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'strlen' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.strings.read().unwrap();
    match map.get(key) {
        Some(entry) if !entry.is_expired() => resp::integer(entry.value.len() as i64),
        _ => resp::integer(0),
    }
}

fn cmd_getrange(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'getrange' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let start: i64 = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let end: i64 = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);

    let map = store.strings.read().unwrap();
    let val = match map.get(key) {
        Some(entry) if !entry.is_expired() => &entry.value,
        _ => return resp::bulk_string(""),
    };

    let len = val.len() as i64;
    let s = if start < 0 {
        (len + start).max(0) as usize
    } else {
        start as usize
    };
    let e = if end < 0 {
        (len + end).max(0) as usize
    } else {
        end.min(len - 1) as usize
    };
    if s > e || s >= val.len() {
        return resp::bulk_string("");
    }
    resp::bulk_string(&val[s..=e.min(val.len() - 1)])
}

// ===========================================================================
// Key commands
// ===========================================================================

fn cmd_del(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let mut count = 0i64;
    {
        let mut strings = store.strings.write().unwrap();
        let mut hashes = store.hashes.write().unwrap();
        let mut lists = store.lists.write().unwrap();
        let mut sets = store.sets.write().unwrap();
        let mut zsets = store.sorted_sets.write().unwrap();
        for arg in args {
            if let Some(key) = arg.as_str() {
                if strings.remove(key).is_some() {
                    count += 1;
                } else if hashes.remove(key).is_some() {
                    count += 1;
                } else if lists.remove(key).is_some() {
                    count += 1;
                } else if sets.remove(key).is_some() {
                    count += 1;
                } else if zsets.remove(key).is_some() {
                    count += 1;
                }
            }
        }
    }
    if store.sql_enabled() {
        for arg in args {
            if let Some(key) = arg.as_str() {
                mirror_kv_del(store, key);
                mirror_hash_del(store, key);
                mirror_list_del(store, key);
                mirror_set_del(store, key);
            }
        }
    }
    resp::integer(count)
}

fn cmd_exists(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let strings = store.strings.read().unwrap();
    let hashes = store.hashes.read().unwrap();
    let lists = store.lists.read().unwrap();
    let sets = store.sets.read().unwrap();
    let zsets = store.sorted_sets.read().unwrap();
    let mut count = 0i64;
    for arg in args {
        if let Some(key) = arg.as_str() {
            if strings.get(key).map(|e| !e.is_expired()).unwrap_or(false)
                || hashes.contains_key(key)
                || lists.contains_key(key)
                || sets.contains_key(key)
                || zsets.contains_key(key)
            {
                count += 1;
            }
        }
    }
    resp::integer(count)
}

fn cmd_expire(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'expire' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };
    let seconds: u64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => return resp::err("value is not an integer or out of range"),
    };

    let mut map = store.strings.write().unwrap();
    if let Some(entry) = map.get_mut(key) {
        if !entry.is_expired() {
            entry.expires_at = Some(Instant::now() + std::time::Duration::from_secs(seconds));
            return resp::integer(1);
        }
    }
    resp::integer(0)
}

fn cmd_pexpire(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'pexpire' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };
    let ms: u64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(s) => s,
        None => return resp::err("value is not an integer or out of range"),
    };

    let mut map = store.strings.write().unwrap();
    if let Some(entry) = map.get_mut(key) {
        if !entry.is_expired() {
            entry.expires_at = Some(Instant::now() + std::time::Duration::from_millis(ms));
            return resp::integer(1);
        }
    }
    resp::integer(0)
}

fn cmd_expireat(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'expireat' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };
    let timestamp: u64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(t) => t,
        None => return resp::err("value is not an integer or out of range"),
    };

    let now = now_secs();
    let mut map = store.strings.write().unwrap();
    if timestamp <= now {
        if map.remove(key).is_some() {
            drop(map);
            mirror_kv_del(store, key);
        }
        return resp::integer(1);
    }

    let ttl = timestamp - now;
    if let Some(entry) = map.get_mut(key) {
        if !entry.is_expired() {
            entry.expires_at = Some(Instant::now() + std::time::Duration::from_secs(ttl));
            return resp::integer(1);
        }
    }
    resp::integer(0)
}

fn cmd_persist(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'persist' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };

    let mut map = store.strings.write().unwrap();
    if let Some(entry) = map.get_mut(key) {
        if !entry.is_expired() && entry.expires_at.is_some() {
            entry.expires_at = None;
            return resp::integer(1);
        }
    }
    resp::integer(0)
}

fn cmd_ttl(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'ttl' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.strings.read().unwrap();
    match map.get(key) {
        Some(entry) if !entry.is_expired() => resp::integer(entry.ttl_secs()),
        _ => resp::integer(-2),
    }
}

fn cmd_pttl(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'pttl' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.strings.read().unwrap();
    match map.get(key) {
        Some(entry) if !entry.is_expired() => match entry.expires_at {
            Some(exp) => {
                let now = Instant::now();
                if now >= exp {
                    resp::integer(-2)
                } else {
                    resp::integer((exp - now).as_millis() as i64)
                }
            }
            None => resp::integer(-1),
        },
        _ => resp::integer(-2),
    }
}

fn cmd_type(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'type' command");
    }
    let key = args[0].as_str().unwrap_or("");

    if store
        .strings
        .read()
        .unwrap()
        .get(key)
        .map(|e| !e.is_expired())
        .unwrap_or(false)
    {
        return RespValue::SimpleString("string".to_string());
    }
    if store.hashes.read().unwrap().contains_key(key) {
        return RespValue::SimpleString("hash".to_string());
    }
    if store.lists.read().unwrap().contains_key(key) {
        return RespValue::SimpleString("list".to_string());
    }
    if store.sets.read().unwrap().contains_key(key) {
        return RespValue::SimpleString("set".to_string());
    }
    if store.sorted_sets.read().unwrap().contains_key(key) {
        return RespValue::SimpleString("zset".to_string());
    }
    RespValue::SimpleString("none".to_string())
}

fn cmd_keys(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let pattern = args.first().and_then(|a| a.as_str()).unwrap_or("*");
    let regex_pattern = glob_to_regex(pattern);
    let re = match regex::Regex::new(&regex_pattern) {
        Ok(r) => r,
        Err(_) => return resp::array(vec![]),
    };

    let map = store.strings.read().unwrap();
    let keys: Vec<RespValue> = map
        .iter()
        .filter(|(_, e)| !e.is_expired())
        .map(|(k, _)| k.as_str())
        .filter(|k| re.is_match(k))
        .map(resp::bulk_string)
        .collect();
    resp::array(keys)
}

fn cmd_rename(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'rename' command");
    }
    let old_key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };
    let new_key = match args[1].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };

    let mut map = store.strings.write().unwrap();
    match map.remove(old_key) {
        Some(entry) if !entry.is_expired() => {
            map.remove(new_key);
            map.insert(new_key.to_string(), entry);
            drop(map);
            if store.sql_enabled() {
                mirror_kv_del(store, old_key);
                // Re-read to mirror
                let val = {
                    let map = store.strings.read().unwrap();
                    map.get(new_key).map(|e| e.value.clone())
                };
                if let Some(v) = val {
                    mirror_kv_set(store, new_key, &v, None);
                }
            }
            resp::ok()
        }
        _ => resp::err("no such key"),
    }
}

fn cmd_randomkey(store: &OxiMemStore) -> RespValue {
    let map = store.strings.read().unwrap();
    for (k, e) in map.iter() {
        if !e.is_expired() {
            return resp::bulk_string(k);
        }
    }
    resp::null()
}

fn cmd_dbsize(store: &OxiMemStore) -> RespValue {
    let count = store
        .strings
        .read()
        .unwrap()
        .iter()
        .filter(|(_, e)| !e.is_expired())
        .count();
    resp::integer(count as i64)
}

fn cmd_flushdb(store: &OxiMemStore) -> RespValue {
    store.strings.write().unwrap().clear();
    store.hashes.write().unwrap().clear();
    store.lists.write().unwrap().clear();
    store.sets.write().unwrap().clear();
    store.sorted_sets.write().unwrap().clear();
    mirror_flush(store);
    resp::ok()
}

/// Real cursor-based SCAN over ALL key types: the cursor is an offset into
/// the sorted live-key list, COUNT bounds the batch (default 10), MATCH is
/// applied after the batch is taken (Redis semantics — a page may be empty).
fn cmd_scan(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let cursor: usize = args
        .first()
        .and_then(|a| a.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut pattern = "*";
    let mut count = 10usize;
    let mut i = 1;
    while i + 1 < args.len() {
        if let Some(flag) = args[i].as_str() {
            if flag.eq_ignore_ascii_case("MATCH") {
                if let Some(p) = args.get(i + 1).and_then(|a| a.as_str()) {
                    pattern = p;
                }
            } else if flag.eq_ignore_ascii_case("COUNT") {
                if let Some(c) = args.get(i + 1).and_then(|a| a.as_str()).and_then(|s| s.parse().ok())
                {
                    count = c;
                }
            }
        }
        i += 2;
    }
    let re = match regex::Regex::new(&glob_to_regex(pattern)) {
        Ok(r) => r,
        Err(_) => return resp::array(vec![resp::bulk_string("0"), resp::array(vec![])]),
    };
    let all = store.all_live_keys();
    let end = (cursor + count.max(1)).min(all.len());
    let batch: Vec<RespValue> = all[cursor.min(all.len())..end]
        .iter()
        .filter(|k| re.is_match(k))
        .map(|k| resp::bulk_string(k))
        .collect();
    let next = if end >= all.len() { 0 } else { end };
    resp::array(vec![
        resp::bulk_string(&next.to_string()),
        resp::array(batch),
    ])
}

// ===========================================================================
// Hash commands
// ===========================================================================

fn cmd_hset(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return resp::err("wrong number of arguments for 'hset' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };

    let mut map = store.hashes.write().unwrap();
    let hash = map.entry(key.to_string()).or_default();
    let mut added = 0i64;

    for pair in args[1..].chunks(2) {
        let field = match pair[0].as_str() {
            Some(f) => f,
            None => continue,
        };
        let value = pair[1].as_str().unwrap_or("").to_string();
        if !hash.contains_key(field) {
            added += 1;
        }
        hash.insert(field.to_string(), value);
    }

    if store.sql_enabled() {
        let hash_clone = hash.clone();
        drop(map);
        mirror_hash_save(store, key, &hash_clone);
    }
    resp::integer(added)
}

fn cmd_hget(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'hget' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let field = args[1].as_str().unwrap_or("");

    let map = store.hashes.read().unwrap();
    match map.get(key).and_then(|h| h.get(field)) {
        Some(v) => resp::bulk_string(v),
        None => resp::null(),
    }
}

fn cmd_hmget(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'hmget' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.hashes.read().unwrap();
    let hash = map.get(key);

    let results: Vec<RespValue> = args[1..]
        .iter()
        .map(|arg| {
            let field = arg.as_str().unwrap_or("");
            match hash.and_then(|h| h.get(field)) {
                Some(v) => resp::bulk_string(v),
                None => resp::null(),
            }
        })
        .collect();
    resp::array(results)
}

fn cmd_hdel(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'hdel' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };

    let mut map = store.hashes.write().unwrap();
    let hash = match map.get_mut(key) {
        Some(h) => h,
        None => return resp::integer(0),
    };

    let mut removed = 0i64;
    for arg in &args[1..] {
        if let Some(field) = arg.as_str() {
            if hash.remove(field).is_some() {
                removed += 1;
            }
        }
    }

    if store.sql_enabled() {
        if hash.is_empty() {
            map.remove(key);
            drop(map);
            mirror_hash_del(store, key);
        } else {
            let hash_clone = hash.clone();
            drop(map);
            mirror_hash_save(store, key, &hash_clone);
        }
    } else if hash.is_empty() {
        map.remove(key);
    }
    resp::integer(removed)
}

fn cmd_hexists(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'hexists' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let field = args[1].as_str().unwrap_or("");
    let map = store.hashes.read().unwrap();
    let exists = map.get(key).map(|h| h.contains_key(field)).unwrap_or(false);
    resp::integer(if exists { 1 } else { 0 })
}

fn cmd_hgetall(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'hgetall' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.hashes.read().unwrap();
    match map.get(key) {
        Some(hash) => {
            let mut items = Vec::with_capacity(hash.len() * 2);
            for (k, v) in hash {
                items.push(resp::bulk_string(k));
                items.push(resp::bulk_string(v));
            }
            resp::array(items)
        }
        None => resp::array(vec![]),
    }
}

fn cmd_hkeys(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'hkeys' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.hashes.read().unwrap();
    match map.get(key) {
        Some(hash) => resp::array(hash.keys().map(|k| resp::bulk_string(k)).collect()),
        None => resp::array(vec![]),
    }
}

fn cmd_hvals(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'hvals' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.hashes.read().unwrap();
    match map.get(key) {
        Some(hash) => resp::array(hash.values().map(|v| resp::bulk_string(v)).collect()),
        None => resp::array(vec![]),
    }
}

fn cmd_hlen(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'hlen' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.hashes.read().unwrap();
    let count = map.get(key).map(|h| h.len()).unwrap_or(0);
    resp::integer(count as i64)
}

fn cmd_hincrby(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'hincrby' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };
    let field = match args[1].as_str() {
        Some(f) => f,
        None => return resp::err("invalid field"),
    };
    let delta: i64 = match args[2].as_str().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => return resp::err("value is not an integer or out of range"),
    };

    let mut map = store.hashes.write().unwrap();
    let hash = map.entry(key.to_string()).or_default();
    let current: i64 = hash.get(field).and_then(|v| v.parse().ok()).unwrap_or(0);
    let new_val = current + delta;
    hash.insert(field.to_string(), new_val.to_string());

    if store.sql_enabled() {
        let hash_clone = hash.clone();
        drop(map);
        mirror_hash_save(store, key, &hash_clone);
    }
    resp::integer(new_val)
}

fn cmd_hsetnx(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'hsetnx' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let field = args[1].as_str().unwrap_or("");

    let map = store.hashes.read().unwrap();
    if map.get(key).map(|h| h.contains_key(field)).unwrap_or(false) {
        return resp::integer(0);
    }
    drop(map);

    cmd_hset(store, args);
    resp::integer(1)
}

// ===========================================================================
// List commands
// ===========================================================================

fn cmd_lpush(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'lpush' command");
    }
    let key = args[0].as_str().unwrap_or("");

    let mut map = store.lists.write().unwrap();
    let list = map.entry(key.to_string()).or_default();

    for arg in args[1..].iter().rev() {
        list.push_front(arg.as_str().unwrap_or("").to_string());
    }

    let len = list.len() as i64;
    if store.sql_enabled() {
        let list_clone = list.clone();
        drop(map);
        mirror_list_save(store, key, &list_clone);
    }
    resp::integer(len)
}

fn cmd_rpush(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'rpush' command");
    }
    let key = args[0].as_str().unwrap_or("");

    let mut map = store.lists.write().unwrap();
    let list = map.entry(key.to_string()).or_default();

    for arg in &args[1..] {
        list.push_back(arg.as_str().unwrap_or("").to_string());
    }

    let len = list.len() as i64;
    if store.sql_enabled() {
        let list_clone = list.clone();
        drop(map);
        mirror_list_save(store, key, &list_clone);
    }
    resp::integer(len)
}

fn cmd_lpop(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'lpop' command");
    }
    let key = args[0].as_str().unwrap_or("");

    let mut map = store.lists.write().unwrap();
    let list = match map.get_mut(key) {
        Some(l) => l,
        None => return resp::null(),
    };

    match list.pop_front() {
        Some(val) => {
            if store.sql_enabled() {
                if list.is_empty() {
                    map.remove(key);
                    drop(map);
                    mirror_list_del(store, key);
                } else {
                    let list_clone = list.clone();
                    drop(map);
                    mirror_list_save(store, key, &list_clone);
                }
            } else if list.is_empty() {
                map.remove(key);
            }
            resp::bulk_string(&val)
        }
        None => resp::null(),
    }
}

fn cmd_rpop(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'rpop' command");
    }
    let key = args[0].as_str().unwrap_or("");

    let mut map = store.lists.write().unwrap();
    let list = match map.get_mut(key) {
        Some(l) => l,
        None => return resp::null(),
    };

    match list.pop_back() {
        Some(val) => {
            if store.sql_enabled() {
                if list.is_empty() {
                    map.remove(key);
                    drop(map);
                    mirror_list_del(store, key);
                } else {
                    let list_clone = list.clone();
                    drop(map);
                    mirror_list_save(store, key, &list_clone);
                }
            } else if list.is_empty() {
                map.remove(key);
            }
            resp::bulk_string(&val)
        }
        None => resp::null(),
    }
}

fn cmd_llen(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'llen' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.lists.read().unwrap();
    let len = map.get(key).map(|l| l.len()).unwrap_or(0);
    resp::integer(len as i64)
}

fn cmd_lrange(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'lrange' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let start: i64 = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let stop: i64 = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);

    let map = store.lists.read().unwrap();
    let list = match map.get(key) {
        Some(l) => l,
        None => return resp::array(vec![]),
    };

    let len = list.len() as i64;
    let s = if start < 0 {
        (len + start).max(0) as usize
    } else {
        start as usize
    };
    let e = if stop < 0 {
        (len + stop).max(0) as usize
    } else {
        stop as usize
    };

    if s >= list.len() || s > e {
        return resp::array(vec![]);
    }
    let end = (e + 1).min(list.len());

    let result: Vec<RespValue> = list
        .iter()
        .skip(s)
        .take(end - s)
        .map(|v| resp::bulk_string(v))
        .collect();
    resp::array(result)
}

fn cmd_lindex(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'lindex' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let index: i64 = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);

    let map = store.lists.read().unwrap();
    let list = match map.get(key) {
        Some(l) => l,
        None => return resp::null(),
    };
    let len = list.len() as i64;
    let idx = if index < 0 {
        (len + index) as usize
    } else {
        index as usize
    };

    match list.get(idx) {
        Some(v) => resp::bulk_string(v),
        None => resp::null(),
    }
}

// ===========================================================================
// Set commands
// ===========================================================================

fn cmd_sadd(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'sadd' command");
    }
    let key = args[0].as_str().unwrap_or("");

    let mut map = store.sets.write().unwrap();
    let set = map.entry(key.to_string()).or_default();
    let mut added = 0i64;

    for arg in &args[1..] {
        let val = arg.as_str().unwrap_or("").to_string();
        if set.insert(val) {
            added += 1;
        }
    }

    if store.sql_enabled() {
        let set_clone = set.clone();
        drop(map);
        mirror_set_save(store, key, &set_clone);
    }
    resp::integer(added)
}

fn cmd_srem(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'srem' command");
    }
    let key = args[0].as_str().unwrap_or("");

    let mut map = store.sets.write().unwrap();
    let set = match map.get_mut(key) {
        Some(s) => s,
        None => return resp::integer(0),
    };

    let mut removed = 0i64;
    for arg in &args[1..] {
        if let Some(val) = arg.as_str() {
            if set.remove(val) {
                removed += 1;
            }
        }
    }

    if store.sql_enabled() {
        if set.is_empty() {
            map.remove(key);
            drop(map);
            mirror_set_del(store, key);
        } else {
            let set_clone = set.clone();
            drop(map);
            mirror_set_save(store, key, &set_clone);
        }
    } else if set.is_empty() {
        map.remove(key);
    }
    resp::integer(removed)
}

fn cmd_smembers(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'smembers' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.sets.read().unwrap();
    match map.get(key) {
        Some(set) => resp::array(set.iter().map(|m| resp::bulk_string(m)).collect()),
        None => resp::array(vec![]),
    }
}

fn cmd_sismember(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'sismember' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let member = args[1].as_str().unwrap_or("");
    let map = store.sets.read().unwrap();
    let exists = map.get(key).map(|s| s.contains(member)).unwrap_or(false);
    resp::integer(if exists { 1 } else { 0 })
}

fn cmd_scard(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'scard' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.sets.read().unwrap();
    let count = map.get(key).map(|s| s.len()).unwrap_or(0);
    resp::integer(count as i64)
}

// ===========================================================================
// Server commands
// ===========================================================================

fn cmd_info(store: &OxiMemStore) -> RespValue {
    let mode = if store.sql_enabled() { "sql" } else { "raw" };
    let kv_count = store
        .strings
        .read()
        .unwrap()
        .iter()
        .filter(|(_, e)| !e.is_expired())
        .count();

    let info = format!(
        "# Server\r\n\
         oxidb_version:0.19.3\r\n\
         oximem_mode:{mode}\r\n\
         resp_compat:true\r\n\
         \r\n\
         # Keyspace\r\n\
         db0:keys={kv_count},expires=0\r\n"
    );
    resp::bulk_string(&info)
}

// ===========================================================================
// Sorted set commands
// ===========================================================================

fn cmd_zadd(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'zadd' command");
    }
    let key = args[0].as_str().unwrap_or("");

    // Parse optional flags: NX, XX, GT, LT, CH
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str().unwrap_or("").to_uppercase().as_str() {
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            "CH" => {
                ch = true;
                i += 1;
            }
            "GT" | "LT" => {
                i += 1;
            } // accepted but not enforced for simplicity
            _ => break,
        }
    }

    if (args.len() - i) < 2 || (args.len() - i) % 2 != 0 {
        return resp::err("wrong number of arguments for 'zadd' command");
    }

    let mut map = store.sorted_sets.write().unwrap();
    let zset = map.entry(key.to_string()).or_insert_with(SortedSet::new);
    let mut added = 0i64;
    let mut changed = 0i64;

    while i + 1 < args.len() {
        let score: f64 = match args[i].as_str().and_then(|s| s.parse().ok()) {
            Some(s) => s,
            None => return resp::err("value is not a valid float"),
        };
        let member = args[i + 1].as_str().unwrap_or("").to_string();
        i += 2;

        let exists = zset.scores.contains_key(&member);
        if nx && exists {
            continue;
        }
        if xx && !exists {
            continue;
        }

        let was_new = zset.insert(member, score);
        if was_new {
            added += 1;
            changed += 1;
        } else {
            changed += 1;
        }
    }

    resp::integer(if ch { changed } else { added })
}

fn cmd_zrem(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'zrem' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let mut map = store.sorted_sets.write().unwrap();
    let zset = match map.get_mut(key) {
        Some(z) => z,
        None => return resp::integer(0),
    };
    let mut removed = 0i64;
    for arg in &args[1..] {
        if let Some(member) = arg.as_str() {
            if zset.remove(member) {
                removed += 1;
            }
        }
    }
    if zset.len() == 0 {
        map.remove(key);
    }
    resp::integer(removed)
}

fn cmd_zscore(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'zscore' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let member = args[1].as_str().unwrap_or("");
    let map = store.sorted_sets.read().unwrap();
    match map.get(key).and_then(|z| z.score(member)) {
        Some(s) => resp::bulk_string(&format_score(s)),
        None => resp::null(),
    }
}

fn cmd_zrank(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'zrank' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let member = args[1].as_str().unwrap_or("");
    let map = store.sorted_sets.read().unwrap();
    match map.get(key).and_then(|z| z.rank(member)) {
        Some(r) => resp::integer(r as i64),
        None => resp::null(),
    }
}

fn cmd_zrevrank(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'zrevrank' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let member = args[1].as_str().unwrap_or("");
    let map = store.sorted_sets.read().unwrap();
    match map.get(key) {
        Some(z) => match z.rank(member) {
            Some(r) => resp::integer((z.len() - 1 - r) as i64),
            None => resp::null(),
        },
        None => resp::null(),
    }
}

fn cmd_zrange(store: &OxiMemStore, args: &[RespValue], rev: bool) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'zrange' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let start: isize = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let stop: isize = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);
    let withscores = args
        .get(3)
        .and_then(|a| a.as_str())
        .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
        .unwrap_or(false);

    let map = store.sorted_sets.read().unwrap();
    let zset = match map.get(key) {
        Some(z) => z,
        None => return resp::array(vec![]),
    };

    // For ZREVRANGE the ranks are positions in the DESCENDING view, so map
    // them onto ascending ranks BEFORE slicing, then reverse the slice.
    // (The old code sliced ascending first and reversed after, which made
    // `ZREVRANGE key 0 0` return the LOWEST member instead of the highest.)
    let mut items = if rev {
        let n = zset.len() as isize;
        let norm = |i: isize| -> isize {
            if i < 0 { (n + i).max(0) } else { i.min(n - 1) }
        };
        if n == 0 {
            Vec::new()
        } else {
            let (s, e) = (norm(start), norm(stop));
            if s > e {
                Vec::new()
            } else {
                let mut v = zset.range_by_rank(n - 1 - e, n - 1 - s);
                v.reverse();
                v
            }
        }
    } else {
        zset.range_by_rank(start, stop)
    };
    let _ = &mut items;

    if withscores {
        let result: Vec<RespValue> = items
            .iter()
            .flat_map(|(m, s)| vec![resp::bulk_string(m), resp::bulk_string(&format_score(*s))])
            .collect();
        resp::array(result)
    } else {
        let result: Vec<RespValue> = items.iter().map(|(m, _)| resp::bulk_string(m)).collect();
        resp::array(result)
    }
}

fn cmd_zrangebyscore(store: &OxiMemStore, args: &[RespValue], rev: bool) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'zrangebyscore' command");
    }
    let key = args[0].as_str().unwrap_or("");

    let (min_s, max_s) = if rev {
        (
            args[2].as_str().unwrap_or(""),
            args[1].as_str().unwrap_or(""),
        )
    } else {
        (
            args[1].as_str().unwrap_or(""),
            args[2].as_str().unwrap_or(""),
        )
    };

    let min = parse_score_bound(min_s, f64::NEG_INFINITY);
    let max = parse_score_bound(max_s, f64::INFINITY);

    let withscores = args[3..].iter().any(|a| {
        a.as_str()
            .map(|s| s.eq_ignore_ascii_case("WITHSCORES"))
            .unwrap_or(false)
    });

    let map = store.sorted_sets.read().unwrap();
    let zset = match map.get(key) {
        Some(z) => z,
        None => return resp::array(vec![]),
    };

    let mut items = zset.range_by_score(min, max);
    if rev {
        items.reverse();
    }

    if withscores {
        let result: Vec<RespValue> = items
            .iter()
            .flat_map(|(m, s)| vec![resp::bulk_string(m), resp::bulk_string(&format_score(*s))])
            .collect();
        resp::array(result)
    } else {
        let result: Vec<RespValue> = items.iter().map(|(m, _)| resp::bulk_string(m)).collect();
        resp::array(result)
    }
}

fn cmd_zcard(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'zcard' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.sorted_sets.read().unwrap();
    let count = map.get(key).map(|z| z.len()).unwrap_or(0);
    resp::integer(count as i64)
}

fn cmd_zcount(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'zcount' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let min = parse_score_bound(args[1].as_str().unwrap_or("-inf"), f64::NEG_INFINITY);
    let max = parse_score_bound(args[2].as_str().unwrap_or("+inf"), f64::INFINITY);

    let map = store.sorted_sets.read().unwrap();
    let count = map
        .get(key)
        .map(|z| z.count_by_score(min, max))
        .unwrap_or(0);
    resp::integer(count as i64)
}

fn cmd_zincrby(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'zincrby' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let increment: f64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(i) => i,
        None => return resp::err("value is not a valid float"),
    };
    let member = args[2].as_str().unwrap_or("").to_string();

    let mut map = store.sorted_sets.write().unwrap();
    let zset = map.entry(key.to_string()).or_insert_with(SortedSet::new);
    let new_score = zset.score(&member).unwrap_or(0.0) + increment;
    zset.insert(member, new_score);
    resp::bulk_string(&format_score(new_score))
}

fn cmd_zpopmin(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'zpopmin' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let count: usize = args
        .get(1)
        .and_then(|a| a.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let mut map = store.sorted_sets.write().unwrap();
    let zset = match map.get_mut(key) {
        Some(z) => z,
        None => return resp::array(vec![]),
    };

    let mut result = Vec::with_capacity(count * 2);
    for _ in 0..count {
        if let Some(entry) = zset.tree.iter().next().cloned() {
            zset.tree.remove(&entry);
            zset.scores.remove(&entry.1);
            result.push(resp::bulk_string(&entry.1));
            result.push(resp::bulk_string(&format_score(entry.0.0)));
        } else {
            break;
        }
    }
    if zset.len() == 0 {
        map.remove(key);
    }
    resp::array(result)
}

fn cmd_zpopmax(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'zpopmax' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let count: usize = args
        .get(1)
        .and_then(|a| a.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    let mut map = store.sorted_sets.write().unwrap();
    let zset = match map.get_mut(key) {
        Some(z) => z,
        None => return resp::array(vec![]),
    };

    let mut result = Vec::with_capacity(count * 2);
    for _ in 0..count {
        if let Some(entry) = zset.tree.iter().next_back().cloned() {
            zset.tree.remove(&entry);
            zset.scores.remove(&entry.1);
            result.push(resp::bulk_string(&entry.1));
            result.push(resp::bulk_string(&format_score(entry.0.0)));
        } else {
            break;
        }
    }
    if zset.len() == 0 {
        map.remove(key);
    }
    resp::array(result)
}

// ===========================================================================
// Pub/Sub commands
// ===========================================================================

fn cmd_publish(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'publish' command");
    }
    let channel = args[0].as_str().unwrap_or("");
    let message = args[1].as_str().unwrap_or("");
    let count = store.publish(channel, message);
    resp::integer(count)
}

// ===========================================================================
// Extended commands: HELLO/CONFIG, GETDEL/SETRANGE/PEXPIREAT/COPY, set ops,
// HRANDFIELD, ZRANGEBYLEX, real SCAN cursor, blocking pops
// ===========================================================================

fn cmd_hello(args: &[RespValue]) -> RespValue {
    if let Some(ver) = args.first().and_then(|a| a.as_str()) {
        if ver != "2" {
            // We speak RESP2 only; clients asking for RESP3 fall back.
            return resp::err("NOPROTO unsupported protocol version");
        }
    }
    resp::array(vec![
        resp::bulk_string("server"),
        resp::bulk_string("oximem"),
        resp::bulk_string("version"),
        resp::bulk_string(env!("CARGO_PKG_VERSION")),
        resp::bulk_string("proto"),
        resp::integer(2),
    ])
}

fn cmd_config(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .unwrap_or("")
        .to_uppercase();
    let name = args.get(1).and_then(|a| a.as_str()).unwrap_or("");
    match sub.as_str() {
        "SET" if name.eq_ignore_ascii_case("notify-keyspace-events") => {
            let val = args.get(2).and_then(|a| a.as_str()).unwrap_or("");
            store.notify.store(!val.is_empty(), Ordering::Relaxed);
            resp::ok()
        }
        "GET" if name.eq_ignore_ascii_case("notify-keyspace-events") => {
            let v = if store.notify.load(Ordering::Relaxed) {
                "KEA"
            } else {
                ""
            };
            resp::array(vec![resp::bulk_string(name), resp::bulk_string(v)])
        }
        "GET" => resp::array(vec![resp::bulk_string(name), resp::bulk_string("")]),
        _ => resp::ok(),
    }
}

/// DECRBYFLOATGE key amount — atomic check-and-debit: if the key's numeric
/// value is >= amount, subtract and return the new value; otherwise return
/// Null and change nothing. This is the single-command primitive that makes
/// balance debits safe WITHOUT a surrounding transaction/script lock.
fn cmd_decrbyfloatge(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'decrbyfloatge' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let amt: f64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(a) => a,
        None => return resp::err("value is not a valid float"),
    };
    let mut map = store.strings.write().unwrap();
    let e = match map.get_mut(key) {
        Some(e) if !e.is_expired() => e,
        _ => return resp::null(),
    };
    let cur: f64 = match e.value.parse() {
        Ok(c) => c,
        Err(_) => return resp::err("value is not a valid float"),
    };
    if cur < amt {
        return resp::null();
    }
    let newv = cur - amt;
    e.value = format_score(newv);
    let out = e.value.clone();
    drop(map);
    if store.sql_enabled() {
        mirror_kv_set(store, key, &out, None);
    }
    resp::bulk_string(&out)
}

fn cmd_getdel(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let key = match args.first().and_then(|a| a.as_str()) {
        Some(k) => k,
        None => return resp::err("wrong number of arguments for 'getdel' command"),
    };
    let mut map = store.strings.write().unwrap();
    match map.remove(key) {
        Some(e) if !e.is_expired() => {
            drop(map);
            mirror_kv_del(store, key);
            resp::bulk_string(&e.value)
        }
        _ => resp::null(),
    }
}

fn cmd_setrange(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'setrange' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let offset: usize = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(o) => o,
        None => return resp::err("value is not an integer or out of range"),
    };
    let patch = args[2].as_str().unwrap_or("");
    let mut map = store.strings.write().unwrap();
    let entry = map
        .entry(key.to_string())
        .or_insert_with(|| KvEntry::new(String::new()));
    let mut bytes = entry.value.clone().into_bytes();
    if bytes.len() < offset + patch.len() {
        bytes.resize(offset + patch.len(), 0);
    }
    bytes[offset..offset + patch.len()].copy_from_slice(patch.as_bytes());
    entry.value = String::from_utf8_lossy(&bytes).to_string();
    let len = entry.value.len() as i64;
    let val = entry.value.clone();
    drop(map);
    mirror_kv_set(store, key, &val, None);
    resp::integer(len)
}

fn cmd_pexpireat(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'pexpireat' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };
    let ts_ms: u64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(t) => t,
        None => return resp::err("value is not an integer or out of range"),
    };
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut map = store.strings.write().unwrap();
    if ts_ms <= now_ms {
        if map.remove(key).is_some() {
            drop(map);
            mirror_kv_del(store, key);
            return resp::integer(1);
        }
        return resp::integer(0);
    }
    if let Some(entry) = map.get_mut(key) {
        if !entry.is_expired() {
            entry.expires_at =
                Some(Instant::now() + std::time::Duration::from_millis(ts_ms - now_ms));
            return resp::integer(1);
        }
    }
    resp::integer(0)
}

/// COPY src dst [REPLACE] — copies a value of any type.
fn cmd_copy(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'copy' command");
    }
    let src = args[0].as_str().unwrap_or("");
    let dst = args[1].as_str().unwrap_or("");
    let replace = args
        .get(2)
        .and_then(|a| a.as_str())
        .map(|s| s.eq_ignore_ascii_case("REPLACE"))
        .unwrap_or(false);
    if !replace && store.key_exists(dst) {
        return resp::integer(0);
    }
    {
        let s = store.strings.read().unwrap();
        if let Some(e) = s.get(src) {
            if !e.is_expired() {
                let val = e.value.clone();
                drop(s);
                store
                    .strings
                    .write()
                    .unwrap()
                    .insert(dst.to_string(), KvEntry::new(val.clone()));
                mirror_kv_set(store, dst, &val, None);
                return resp::integer(1);
            }
        }
    }
    {
        let h = store.hashes.read().unwrap();
        if let Some(m) = h.get(src) {
            let m = m.clone();
            drop(h);
            store
                .hashes
                .write()
                .unwrap()
                .insert(dst.to_string(), m.clone());
            mirror_hash_save(store, dst, &m);
            return resp::integer(1);
        }
    }
    {
        let l = store.lists.read().unwrap();
        if let Some(v) = l.get(src) {
            let v = v.clone();
            drop(l);
            store
                .lists
                .write()
                .unwrap()
                .insert(dst.to_string(), v.clone());
            mirror_list_save(store, dst, &v);
            return resp::integer(1);
        }
    }
    {
        let st = store.sets.read().unwrap();
        if let Some(m) = st.get(src) {
            let m = m.clone();
            drop(st);
            store
                .sets
                .write()
                .unwrap()
                .insert(dst.to_string(), m.clone());
            mirror_set_save(store, dst, &m);
            return resp::integer(1);
        }
    }
    {
        let z = store.sorted_sets.read().unwrap();
        if let Some(ss) = z.get(src) {
            let pairs: Vec<(String, f64)> =
                ss.scores.iter().map(|(m, &sc)| (m.clone(), sc)).collect();
            drop(z);
            let mut zw = store.sorted_sets.write().unwrap();
            let dst_set = zw.entry(dst.to_string()).or_insert_with(SortedSet::new);
            dst_set.scores.clear();
            dst_set.tree.clear();
            for (m, sc) in &pairs {
                dst_set.insert(m.clone(), *sc);
            }
            drop(zw);
            mirror_zset_save(store, dst, &pairs);
            return resp::integer(1);
        }
    }
    resp::integer(0)
}

enum SetOp {
    Inter,
    Union,
    Diff,
}

/// SINTER/SUNION/SDIFF (+STORE variants — dest is the first arg then).
fn cmd_setop(store: &OxiMemStore, args: &[RespValue], op: SetOp, store_dest: bool) -> RespValue {
    let min = if store_dest { 2 } else { 1 };
    if args.len() < min {
        return resp::err("wrong number of arguments");
    }
    let (dest, key_args) = if store_dest {
        (Some(args[0].as_str().unwrap_or("")), &args[1..])
    } else {
        (None, args)
    };
    let map = store.sets.read().unwrap();
    let empty = HashSet::new();
    let mut iter = key_args
        .iter()
        .map(|a| map.get(a.as_str().unwrap_or("")).unwrap_or(&empty));
    let first = match iter.next() {
        Some(s) => s.clone(),
        None => HashSet::new(),
    };
    let result: HashSet<String> = iter.fold(first, |acc, s| match op {
        SetOp::Inter => acc.intersection(s).cloned().collect(),
        SetOp::Union => acc.union(s).cloned().collect(),
        SetOp::Diff => acc.difference(s).cloned().collect(),
    });
    drop(map);
    match dest {
        Some(d) => {
            let n = result.len() as i64;
            store
                .sets
                .write()
                .unwrap()
                .insert(d.to_string(), result.clone());
            mirror_set_save(store, d, &result);
            resp::integer(n)
        }
        None => {
            let mut v: Vec<&String> = result.iter().collect();
            v.sort(); // deterministic output
            resp::array(v.into_iter().map(|s| resp::bulk_string(s)).collect())
        }
    }
}

fn cmd_hrandfield(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let key = match args.first().and_then(|a| a.as_str()) {
        Some(k) => k,
        None => return resp::err("wrong number of arguments for 'hrandfield' command"),
    };
    let count: Option<i64> = args.get(1).and_then(|a| a.as_str()).and_then(|s| s.parse().ok());
    let map = store.hashes.read().unwrap();
    let hash = match map.get(key) {
        Some(h) if !h.is_empty() => h,
        _ => {
            return match count {
                Some(_) => resp::array(vec![]),
                None => resp::null(),
            };
        }
    };
    let mut fields: Vec<&String> = hash.keys().collect();
    fields.sort();
    // Pseudo-random pick from the clock — good enough without a rand dep.
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as usize;
    match count {
        None => resp::bulk_string(fields[seed % fields.len()]),
        Some(n) => {
            let n = n.unsigned_abs() as usize;
            let out: Vec<RespValue> = (0..n.min(fields.len()))
                .map(|i| resp::bulk_string(fields[(seed + i) % fields.len()]))
                .collect();
            resp::array(out)
        }
    }
}

/// ZRANGEBYLEX key min max — `[m` inclusive, `(m` exclusive, `-`/`+` open.
fn cmd_zrangebylex(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'zrangebylex' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let parse = |s: &str| -> Option<(String, bool)> {
        // (member, inclusive)
        match s.chars().next()? {
            '[' => Some((s[1..].to_string(), true)),
            '(' => Some((s[1..].to_string(), false)),
            '-' | '+' => Some((s.to_string(), true)),
            _ => None,
        }
    };
    let min = match args[1].as_str().and_then(parse) {
        Some(m) => m,
        None => return resp::err("min or max not valid string range item"),
    };
    let max = match args[2].as_str().and_then(parse) {
        Some(m) => m,
        None => return resp::err("min or max not valid string range item"),
    };
    let map = store.sorted_sets.read().unwrap();
    let zset = match map.get(key) {
        Some(z) => z,
        None => return resp::array(vec![]),
    };
    let in_range = |m: &str| -> bool {
        let lo_ok = match min.0.as_str() {
            "-" => true,
            "+" => false,
            v => {
                if min.1 {
                    m >= v
                } else {
                    m > v
                }
            }
        };
        let hi_ok = match max.0.as_str() {
            "+" => true,
            "-" => false,
            v => {
                if max.1 {
                    m <= v
                } else {
                    m < v
                }
            }
        };
        lo_ok && hi_ok
    };
    let out: Vec<RespValue> = zset
        .tree
        .iter()
        .filter(|(_, m)| in_range(m))
        .map(|(_, m)| resp::bulk_string(m))
        .collect();
    resp::array(out)
}

enum PopSide {
    Left,
    Right,
    ZMin,
}

/// BLPOP/BRPOP/BZPOPMIN keys… timeout — poll-based blocking (20ms tick).
/// A timeout of "-1" is the internal single-poll sentinel used inside EXEC.
fn cmd_bpop(store: &OxiMemStore, args: &[RespValue], side: PopSide) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments");
    }
    let timeout: f64 = args[args.len() - 1]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let keys = &args[..args.len() - 1];
    let deadline = if timeout > 0.0 {
        Some(Instant::now() + std::time::Duration::from_secs_f64(timeout))
    } else {
        None
    };
    loop {
        for karg in keys {
            let key = karg.as_str().unwrap_or("");
            match side {
                PopSide::Left | PopSide::Right => {
                    let mut map = store.lists.write().unwrap();
                    if let Some(list) = map.get_mut(key) {
                        let popped = match side {
                            PopSide::Left => list.pop_front(),
                            _ => list.pop_back(),
                        };
                        if let Some(v) = popped {
                            if list.is_empty() {
                                map.remove(key);
                            }
                            drop(map);
                            store.bump_version(key);
                            return resp::array(vec![
                                resp::bulk_string(key),
                                resp::bulk_string(&v),
                            ]);
                        }
                    }
                }
                PopSide::ZMin => {
                    let mut map = store.sorted_sets.write().unwrap();
                    if let Some(zset) = map.get_mut(key) {
                        if let Some(entry) = zset.tree.iter().next().cloned() {
                            zset.tree.remove(&entry);
                            zset.scores.remove(&entry.1);
                            if zset.len() == 0 {
                                map.remove(key);
                            }
                            drop(map);
                            store.bump_version(key);
                            return resp::array(vec![
                                resp::bulk_string(key),
                                resp::bulk_string(&entry.1),
                                resp::bulk_string(&format_score(entry.0.0)),
                            ]);
                        }
                    }
                }
            }
        }
        if timeout < 0.0 {
            return RespValue::NullArray; // single-poll sentinel (inside EXEC)
        }
        if let Some(d) = deadline {
            if Instant::now() >= d {
                return RespValue::NullArray;
            }
        }
        // Wait for a producer instead of spinning: any list/zset write
        // notifies write_cv; cap the wait so timeouts stay accurate.
        let (lock, cv) = &*store.write_cv;
        let guard = lock.lock().unwrap();
        let _ = cv
            .wait_timeout(guard, std::time::Duration::from_millis(20))
            .unwrap();
    }
}

// ===========================================================================
// Round-2 commands: zset range-removal & store-combinators, list surgery,
// set sampling, sub-scans, GETEX, bit operations
// ===========================================================================

fn zpairs(store: &OxiMemStore, key: &str) -> Vec<(String, f64)> {
    store
        .sorted_sets
        .read()
        .unwrap()
        .get(key)
        .map(|z| z.scores.iter().map(|(m, &s)| (m.clone(), s)).collect())
        .unwrap_or_default()
}

fn cmd_zremrangebyrank(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments");
    }
    let key = args[0].as_str().unwrap_or("");
    let start: isize = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let stop: isize = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);
    let mut map = store.sorted_sets.write().unwrap();
    let z = match map.get_mut(key) {
        Some(z) => z,
        None => return resp::integer(0),
    };
    let victims: Vec<String> = z
        .range_by_rank(start, stop)
        .into_iter()
        .map(|(m, _)| m.to_string())
        .collect();
    for m in &victims {
        z.remove(m);
    }
    if z.len() == 0 {
        map.remove(key);
    }
    resp::integer(victims.len() as i64)
}

fn cmd_zremrangebyscore(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments");
    }
    let key = args[0].as_str().unwrap_or("");
    let parse = |s: &str| -> (f64, bool) {
        // (bound, inclusive)
        if let Some(rest) = s.strip_prefix('(') {
            (rest.parse().unwrap_or(f64::NAN), false)
        } else if s == "-inf" {
            (f64::NEG_INFINITY, true)
        } else if s == "+inf" {
            (f64::INFINITY, true)
        } else {
            (s.parse().unwrap_or(f64::NAN), true)
        }
    };
    let (lo, lo_inc) = parse(args[1].as_str().unwrap_or(""));
    let (hi, hi_inc) = parse(args[2].as_str().unwrap_or(""));
    let mut map = store.sorted_sets.write().unwrap();
    let z = match map.get_mut(key) {
        Some(z) => z,
        None => return resp::integer(0),
    };
    let victims: Vec<String> = z
        .scores
        .iter()
        .filter(|&(_, &s)| {
            (if lo_inc { s >= lo } else { s > lo }) && (if hi_inc { s <= hi } else { s < hi })
        })
        .map(|(m, _)| m.clone())
        .collect();
    for m in &victims {
        z.remove(m);
    }
    if z.len() == 0 {
        map.remove(key);
    }
    resp::integer(victims.len() as i64)
}

/// ZUNIONSTORE/ZINTERSTORE dest numkeys key… (SUM aggregate).
fn cmd_zsetstore(store: &OxiMemStore, args: &[RespValue], inter: bool) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments");
    }
    let dest = args[0].as_str().unwrap_or("");
    let numkeys: usize = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    if numkeys == 0 || args.len() < 2 + numkeys {
        return resp::err("at least 1 input key is needed");
    }
    let mut acc: HashMap<String, (f64, usize)> = HashMap::new();
    for a in &args[2..2 + numkeys] {
        for (m, s) in zpairs(store, a.as_str().unwrap_or("")) {
            let e = acc.entry(m).or_insert((0.0, 0));
            e.0 += s;
            e.1 += 1;
        }
    }
    let mut map = store.sorted_sets.write().unwrap();
    let z = map.entry(dest.to_string()).or_insert_with(SortedSet::new);
    z.scores.clear();
    z.tree.clear();
    let mut n = 0i64;
    for (m, (s, cnt)) in acc {
        if !inter || cnt == numkeys {
            z.insert(m, s);
            n += 1;
        }
    }
    if z.len() == 0 {
        map.remove(dest);
    }
    resp::integer(n)
}

fn cmd_lrem(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments");
    }
    let key = args[0].as_str().unwrap_or("");
    let count: i64 = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let val = args[2].as_str().unwrap_or("");
    let mut map = store.lists.write().unwrap();
    let list = match map.get_mut(key) {
        Some(l) => l,
        None => return resp::integer(0),
    };
    let before = list.len();
    if count == 0 {
        list.retain(|x| x != val);
    } else if count > 0 {
        let mut left = count;
        list.retain(|x| {
            if left > 0 && x == val {
                left -= 1;
                false
            } else {
                true
            }
        });
    } else {
        let mut left = -count;
        let mut keep: VecDeque<String> = VecDeque::with_capacity(list.len());
        while let Some(x) = list.pop_back() {
            if left > 0 && x == val {
                left -= 1;
            } else {
                keep.push_front(x);
            }
        }
        *list = keep;
    }
    let removed = (before - list.len()) as i64;
    if list.is_empty() {
        map.remove(key);
    }
    resp::integer(removed)
}

fn cmd_lset(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments");
    }
    let key = args[0].as_str().unwrap_or("");
    let idx: isize = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let val = args[2].as_str().unwrap_or("").to_string();
    let mut map = store.lists.write().unwrap();
    let list = match map.get_mut(key) {
        Some(l) => l,
        None => return resp::err("no such key"),
    };
    let n = list.len() as isize;
    let i = if idx < 0 { n + idx } else { idx };
    if i < 0 || i >= n {
        return resp::err("index out of range");
    }
    list[i as usize] = val;
    resp::ok()
}

fn cmd_ltrim(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments");
    }
    let key = args[0].as_str().unwrap_or("");
    let start: isize = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let stop: isize = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);
    let mut map = store.lists.write().unwrap();
    if let Some(list) = map.get_mut(key) {
        let n = list.len() as isize;
        let norm = |i: isize| if i < 0 { (n + i).max(0) } else { i.min(n) };
        let (s, e) = (norm(start), norm(stop));
        let kept: VecDeque<String> = if s > e {
            VecDeque::new()
        } else {
            list.iter()
                .skip(s as usize)
                .take((e - s + 1) as usize)
                .cloned()
                .collect()
        };
        if kept.is_empty() {
            map.remove(key);
        } else {
            *list = kept;
        }
    }
    resp::ok()
}

/// RPOPLPUSH src dst / LMOVE src dst LEFT|RIGHT LEFT|RIGHT.
fn cmd_lmove(store: &OxiMemStore, args: &[RespValue], legacy: bool) -> RespValue {
    let (src, dst, from_left, to_left) = if legacy {
        if args.len() < 2 {
            return resp::err("wrong number of arguments");
        }
        (
            args[0].as_str().unwrap_or(""),
            args[1].as_str().unwrap_or(""),
            false,
            true,
        )
    } else {
        if args.len() < 4 {
            return resp::err("wrong number of arguments");
        }
        (
            args[0].as_str().unwrap_or(""),
            args[1].as_str().unwrap_or(""),
            args[2]
                .as_str()
                .map(|s| s.eq_ignore_ascii_case("LEFT"))
                .unwrap_or(false),
            args[3]
                .as_str()
                .map(|s| s.eq_ignore_ascii_case("LEFT"))
                .unwrap_or(false),
        )
    };
    let mut map = store.lists.write().unwrap();
    let val = match map.get_mut(src) {
        Some(l) => {
            let v = if from_left { l.pop_front() } else { l.pop_back() };
            if l.is_empty() {
                map.remove(src);
            }
            v
        }
        None => None,
    };
    match val {
        Some(v) => {
            let dl = map.entry(dst.to_string()).or_default();
            if to_left {
                dl.push_front(v.clone());
            } else {
                dl.push_back(v.clone());
            }
            resp::bulk_string(&v)
        }
        None => resp::null(),
    }
}

fn cmd_spop(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let key = match args.first().and_then(|a| a.as_str()) {
        Some(k) => k,
        None => return resp::err("wrong number of arguments"),
    };
    let mut map = store.sets.write().unwrap();
    let set = match map.get_mut(key) {
        Some(s) if !s.is_empty() => s,
        _ => return resp::null(),
    };
    let count: Option<usize> = args.get(1).and_then(|a| a.as_str()).and_then(|s| s.parse().ok());
    match count {
        None => {
            let victim = set.iter().next().cloned().unwrap();
            set.remove(&victim);
            if set.is_empty() {
                map.remove(key);
            }
            resp::bulk_string(&victim)
        }
        Some(n) => {
            let victims: Vec<String> = set.iter().take(n).cloned().collect();
            for v in &victims {
                set.remove(v);
            }
            if set.is_empty() {
                map.remove(key);
            }
            resp::array(victims.iter().map(|v| resp::bulk_string(v)).collect())
        }
    }
}

fn cmd_srandmember(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let key = match args.first().and_then(|a| a.as_str()) {
        Some(k) => k,
        None => return resp::err("wrong number of arguments"),
    };
    let map = store.sets.read().unwrap();
    let set = match map.get(key) {
        Some(s) if !s.is_empty() => s,
        _ => return resp::null(),
    };
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as usize;
    let v: Vec<&String> = set.iter().collect();
    match args.get(1).and_then(|a| a.as_str()).and_then(|s| s.parse::<i64>().ok()) {
        None => resp::bulk_string(v[seed % v.len()]),
        Some(n) => {
            let n = n.unsigned_abs() as usize;
            resp::array(
                (0..n.min(v.len()))
                    .map(|i| resp::bulk_string(v[(seed + i) % v.len()]))
                    .collect(),
            )
        }
    }
}

/// HSCAN/SSCAN/ZSCAN key cursor [MATCH p] [COUNT n] — real offset cursor
/// over the sorted element list, MATCH applied after the page is taken.
fn cmd_subscan(store: &OxiMemStore, args: &[RespValue], kind: u8) -> RespValue {
    let key = args.first().and_then(|a| a.as_str()).unwrap_or("");
    let cursor: usize = args
        .get(1)
        .and_then(|a| a.as_str())
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let mut count = 10usize;
    let mut pattern = "*".to_string();
    let mut i = 2;
    while i + 1 < args.len() {
        if let Some(f) = args[i].as_str() {
            if f.eq_ignore_ascii_case("MATCH") {
                pattern = args[i + 1].as_str().unwrap_or("*").to_string();
            } else if f.eq_ignore_ascii_case("COUNT") {
                if let Some(c) = args[i + 1].as_str().and_then(|s| s.parse().ok()) {
                    count = c;
                }
            }
        }
        i += 2;
    }
    let re = regex::Regex::new(&glob_to_regex(&pattern)).ok();
    let ok = |s: &str| re.as_ref().map(|r| r.is_match(s)).unwrap_or(true);
    // (element name, optional paired value) in a stable sorted order.
    let mut all: Vec<(String, Option<String>)> = match kind {
        b'h' => store
            .hashes.read().unwrap().get(key)
            .map(|h| h.iter().map(|(f, v)| (f.clone(), Some(v.clone()))).collect())
            .unwrap_or_default(),
        b's' => store
            .sets.read().unwrap().get(key)
            .map(|s| s.iter().map(|m| (m.clone(), None)).collect())
            .unwrap_or_default(),
        _ => store
            .sorted_sets.read().unwrap().get(key)
            .map(|z| {
                z.tree.iter()
                    .map(|(Score(s), m)| (m.clone(), Some(format_score(*s))))
                    .collect()
            })
            .unwrap_or_default(),
    };
    if kind != b'z' {
        all.sort(); // zset already ordered by (score, member)
    }
    let end = (cursor + count.max(1)).min(all.len());
    let mut items = Vec::new();
    for (name, val) in &all[cursor.min(all.len())..end] {
        if ok(name) {
            items.push(resp::bulk_string(name));
            if let Some(v) = val {
                items.push(resp::bulk_string(v));
            }
        }
    }
    let next = if end >= all.len() { 0 } else { end };
    resp::array(vec![
        resp::bulk_string(&next.to_string()),
        resp::array(items),
    ])
}

/// GETEX key [EX seconds | PERSIST] — GET that can adjust the ttl.
fn cmd_getex(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let key = match args.first().and_then(|a| a.as_str()) {
        Some(k) => k,
        None => return resp::err("wrong number of arguments"),
    };
    let mut map = store.strings.write().unwrap();
    let e = match map.get_mut(key) {
        Some(e) if !e.is_expired() => e,
        _ => return resp::null(),
    };
    let num = |i: usize| -> Option<u64> {
        args.get(i).and_then(|a| a.as_str()).and_then(|s| s.parse().ok())
    };
    match args.get(1).and_then(|a| a.as_str()).map(|s| s.to_uppercase()) {
        Some(ref s) if s == "EX" => {
            if let Some(secs) = num(2) {
                e.expires_at = Some(Instant::now() + std::time::Duration::from_secs(secs));
            }
        }
        Some(ref s) if s == "PX" => {
            if let Some(ms) = num(2) {
                e.expires_at = Some(Instant::now() + std::time::Duration::from_millis(ms));
            }
        }
        Some(ref s) if s == "EXAT" => {
            if let Some(at) = num(2) {
                let now = now_secs();
                e.expires_at = Some(Instant::now() + std::time::Duration::from_secs(at.saturating_sub(now)));
            }
        }
        Some(ref s) if s == "PXAT" => {
            if let Some(at_ms) = num(2) {
                let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
                e.expires_at = Some(Instant::now() + std::time::Duration::from_millis(at_ms.saturating_sub(now_ms)));
            }
        }
        Some(ref s) if s == "PERSIST" => e.expires_at = None,
        _ => {}
    }
    resp::bulk_string(&e.value)
}

fn cmd_setbit(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments");
    }
    let key = args[0].as_str().unwrap_or("");
    let off: usize = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let bit: u8 = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let mut map = store.strings.write().unwrap();
    let e = map
        .entry(key.to_string())
        .or_insert_with(|| KvEntry::new(String::new()));
    let mut bytes = e.value.clone().into_bytes();
    let byte_i = off / 8;
    if bytes.len() <= byte_i {
        bytes.resize(byte_i + 1, 0);
    }
    let mask = 1u8 << (7 - (off % 8));
    let old = (bytes[byte_i] & mask != 0) as i64;
    if bit != 0 {
        bytes[byte_i] |= mask;
    } else {
        bytes[byte_i] &= !mask;
    }
    e.value = unsafe { String::from_utf8_unchecked(bytes) };
    resp::integer(old)
}

fn cmd_getbit(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments");
    }
    let key = args[0].as_str().unwrap_or("");
    let off: usize = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let map = store.strings.read().unwrap();
    let bit = map
        .get(key)
        .filter(|e| !e.is_expired())
        .map(|e| {
            let bytes = e.value.as_bytes();
            let bi = off / 8;
            if bi < bytes.len() {
                ((bytes[bi] >> (7 - (off % 8))) & 1) as i64
            } else {
                0
            }
        })
        .unwrap_or(0);
    resp::integer(bit)
}

fn cmd_bitcount(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let key = args.first().and_then(|a| a.as_str()).unwrap_or("");
    let map = store.strings.read().unwrap();
    let n = map
        .get(key)
        .filter(|e| !e.is_expired())
        .map(|e| {
            let bytes = e.value.as_bytes();
            let len = bytes.len() as isize;
            let norm = |i: isize| if i < 0 { (len + i).max(0) } else { i.min(len - 1) };
            let (s, e2) = match (
                args.get(1).and_then(|a| a.as_str()).and_then(|s| s.parse().ok()),
                args.get(2).and_then(|a| a.as_str()).and_then(|s| s.parse().ok()),
            ) {
                (Some(a), Some(b)) => (norm(a), norm(b)),
                _ => (0, len - 1),
            };
            if len == 0 || s > e2 {
                0
            } else {
                bytes[s as usize..=(e2 as usize)]
                    .iter()
                    .map(|b| b.count_ones() as i64)
                    .sum()
            }
        })
        .unwrap_or(0);
    resp::integer(n)
}

fn cmd_smismember(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments");
    }
    let key = args[0].as_str().unwrap_or("");
    let map = store.sets.read().unwrap();
    let empty = HashSet::new();
    let set = map.get(key).unwrap_or(&empty);
    resp::array(
        args[1..]
            .iter()
            .map(|a| resp::integer(set.contains(a.as_str().unwrap_or("")) as i64))
            .collect(),
    )
}

/// LMPOP numkeys key… LEFT|RIGHT / ZMPOP numkeys key… MIN — first non-empty.
fn cmd_mpop(store: &OxiMemStore, args: &[RespValue], zset: bool) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments");
    }
    let numkeys: usize = args[0].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    if numkeys == 0 || args.len() < 1 + numkeys + 1 {
        return resp::err("numkeys should be greater than 0");
    }
    let dir = args[1 + numkeys].as_str().unwrap_or("").to_uppercase();
    for karg in &args[1..1 + numkeys] {
        let key = karg.as_str().unwrap_or("");
        if zset {
            let r = cmd_zpopmin(store, &[resp::bulk_string(key)]);
            if let RespValue::Array(v) = &r {
                if !v.is_empty() {
                    store.bump_version(key);
                    return resp::array(vec![resp::bulk_string(key), r]);
                }
            }
        } else {
            let mut map = store.lists.write().unwrap();
            if let Some(l) = map.get_mut(key) {
                let popped = if dir == "LEFT" { l.pop_front() } else { l.pop_back() };
                if let Some(v) = popped {
                    if l.is_empty() {
                        map.remove(key);
                    }
                    drop(map);
                    store.bump_version(key);
                    return resp::array(vec![
                        resp::bulk_string(key),
                        resp::array(vec![resp::bulk_string(&v)]),
                    ]);
                }
            }
        }
    }
    RespValue::NullArray
}

/// BLMOVE src dst LEFT|RIGHT LEFT|RIGHT timeout / BRPOPLPUSH src dst timeout —
/// blocking variants of LMOVE (condvar-woken, "-1" = single-poll in EXEC).
fn cmd_blmove(store: &OxiMemStore, args: &[RespValue], legacy: bool) -> RespValue {
    let need = if legacy { 3 } else { 5 };
    if args.len() < need {
        return resp::err("wrong number of arguments");
    }
    let timeout: f64 = args[args.len() - 1]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0);
    let inner = &args[..args.len() - 1];
    let deadline = if timeout > 0.0 {
        Some(Instant::now() + std::time::Duration::from_secs_f64(timeout))
    } else {
        None
    };
    loop {
        let r = cmd_lmove(store, inner, legacy);
        if !matches!(r, RespValue::Null) {
            if let Some(k) = inner.first().and_then(|a| a.as_str()) {
                store.bump_version(k);
            }
            if let Some(k) = inner.get(1).and_then(|a| a.as_str()) {
                store.bump_version(k);
            }
            return r;
        }
        if timeout < 0.0 {
            return resp::null();
        }
        if let Some(d) = deadline {
            if Instant::now() >= d {
                return resp::null();
            }
        }
        let (lock, cv) = &*store.write_cv;
        let guard = lock.lock().unwrap();
        let _ = cv
            .wait_timeout(guard, std::time::Duration::from_millis(20))
            .unwrap();
    }
}

// ===========================================================================
// EVAL — server-side Lua scripting (mlua, Lua 5.4 vendored)
// ===========================================================================

fn sha1_hex(src: &str) -> String {
    use sha1::{Digest, Sha1};
    let mut h = Sha1::new();
    h.update(src.as_bytes());
    h.finalize().iter().map(|b| format!("{b:02x}")).collect()
}

fn cmd_script(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let sub = args
        .first()
        .and_then(|a| a.as_str())
        .unwrap_or("")
        .to_uppercase();
    match sub.as_str() {
        "LOAD" => {
            let src = match args.get(1).and_then(|a| a.as_str()) {
                Some(s) => s.to_string(),
                None => return resp::err("wrong number of arguments for 'script load'"),
            };
            let sha = sha1_hex(&src);
            store.scripts.write().unwrap().insert(sha.clone(), src);
            resp::bulk_string(&sha)
        }
        "EXISTS" => {
            let cache = store.scripts.read().unwrap();
            let out: Vec<RespValue> = args[1..]
                .iter()
                .map(|a| {
                    let sha = a.as_str().unwrap_or("").to_lowercase();
                    resp::integer(if cache.contains_key(&sha) { 1 } else { 0 })
                })
                .collect();
            resp::array(out)
        }
        "FLUSH" => {
            store.scripts.write().unwrap().clear();
            resp::ok()
        }
        "KILL" => {
            store.script_kill.store(true, Ordering::SeqCst);
            resp::ok()
        }
        _ => resp::err("unknown SCRIPT subcommand"),
    }
}

fn lua_to_resp(v: mlua::Value) -> RespValue {
    match v {
        mlua::Value::Nil => resp::null(),
        mlua::Value::Boolean(b) => {
            if b {
                resp::integer(1)
            } else {
                resp::null()
            }
        }
        mlua::Value::Integer(n) => resp::integer(n),
        mlua::Value::Number(n) => resp::integer(n as i64),
        mlua::Value::String(s) => resp::bulk(&s.as_bytes()),
        mlua::Value::Table(t) => {
            // {err=...} / {ok=...} Redis conventions, else a sequential array.
            if let Ok(e) = t.get::<String>("err") {
                return resp::err(&e);
            }
            if let Ok(o) = t.get::<String>("ok") {
                return RespValue::SimpleString(o);
            }
            let mut items = Vec::new();
            for i in 1.. {
                match t.get::<mlua::Value>(i) {
                    Ok(mlua::Value::Nil) => break,
                    Ok(x) => items.push(lua_to_resp(x)),
                    Err(_) => break,
                }
            }
            resp::array(items)
        }
        _ => resp::null(),
    }
}

fn resp_to_lua(lua: &mlua::Lua, v: &RespValue) -> mlua::Result<mlua::Value> {
    Ok(match v {
        RespValue::Integer(n) => mlua::Value::Integer(*n),
        RespValue::SimpleString(s) => {
            // redis.call returns status replies as {ok=...}
            let t = lua.create_table()?;
            t.set("ok", s.as_str())?;
            mlua::Value::Table(t)
        }
        RespValue::BulkString(b) => mlua::Value::String(lua.create_string(b)?),
        RespValue::Null | RespValue::NullArray => mlua::Value::Boolean(false),
        RespValue::Array(items) => {
            let t = lua.create_table()?;
            for (i, it) in items.iter().enumerate() {
                t.set(i + 1, resp_to_lua(lua, it)?)?;
            }
            mlua::Value::Table(t)
        }
        RespValue::Error(e) => {
            let t = lua.create_table()?;
            t.set("err", e.as_str())?;
            mlua::Value::Table(t)
        }
    })
}

/// EVAL script numkeys key… arg… / EVALSHA sha1 numkeys key… arg…
/// The whole script runs under `tx_lock`, so it is atomic and isolated w.r.t.
/// MULTI/EXEC blocks and other scripts — exactly Redis's guarantee.
fn cmd_eval(store: &OxiMemStore, args: &[RespValue], by_sha: bool) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'eval' command");
    }
    let src = if by_sha {
        let sha = args[0].as_str().unwrap_or("").to_lowercase();
        match store.scripts.read().unwrap().get(&sha) {
            Some(s) => s.clone(),
            None => return resp::err("NOSCRIPT No matching script."),
        }
    } else {
        let s = args[0].as_str().unwrap_or("").to_string();
        // EVAL also populates the cache (Redis behaviour).
        store
            .scripts
            .write()
            .unwrap()
            .insert(sha1_hex(&s), s.clone());
        s
    };
    let numkeys: usize = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(n) => n,
        None => return resp::err("value is not an integer or out of range"),
    };
    if args.len() < 2 + numkeys {
        return resp::err("Number of keys can't be greater than number of args");
    }
    let keys: Vec<String> = args[2..2 + numkeys]
        .iter()
        .map(|a| a.as_str().unwrap_or("").to_string())
        .collect();
    let argv: Vec<String> = args[2 + numkeys..]
        .iter()
        .map(|a| a.as_str().unwrap_or("").to_string())
        .collect();

    // Sharded locking: gate read-lock (so EXEC can exclude us) + the sorted
    // stripe locks of the DECLARED keys. Scripts with disjoint declared keys
    // run fully in parallel.
    let _gate = store.eval_gate.read().unwrap();
    let mut stripe_ids: Vec<usize> = keys
        .iter()
        .map(|k| {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            k.hash(&mut h);
            (h.finish() as usize) % store.eval_stripes.len()
        })
        .collect();
    stripe_ids.sort_unstable();
    stripe_ids.dedup();
    let _stripes: Vec<_> = stripe_ids
        .iter()
        .map(|&i| store.eval_stripes[i].lock().unwrap())
        .collect();

    // The Lua VM is REUSED per server thread (thread_local) and compiled
    // scripts are cached in its registry by sha1 — creating a fresh VM and
    // re-parsing the script on every EVAL dominated settlement latency
    // inside tx_lock. Globals (KEYS/ARGV/redis/cjson) are re-bound per call
    // through a scope, so `store` borrows stay sound.
    thread_local! {
        static LUA_VM: mlua::Lua = mlua::Lua::new();
    }

    let out: mlua::Result<RespValue> = LUA_VM.with(|lua| {
        store.script_kill.store(false, Ordering::SeqCst);
        let kill = Arc::clone(&store.script_kill);
        let started = Instant::now();
        lua.set_hook(
            mlua::HookTriggers::new().every_nth_instruction(20_000),
            move |_lua, _dbg| {
                if kill.load(Ordering::SeqCst) {
                    Err(mlua::Error::runtime("script killed by SCRIPT KILL"))
                } else if started.elapsed() > std::time::Duration::from_millis(200) {
                    Err(mlua::Error::runtime("script exceeded time limit"))
                } else {
                    Ok(mlua::VmState::Continue)
                }
            },
        );
        let sha_key = sha1_hex(&src);
        let result = lua.scope(|scope| {
            let globals = lua.globals();
            let keys_t = lua.create_table()?;
            for (i, k) in keys.iter().enumerate() {
                keys_t.set(i + 1, k.as_str())?;
            }
            let argv_t = lua.create_table()?;
            for (i, a) in argv.iter().enumerate() {
                argv_t.set(i + 1, a.as_str())?;
            }
            globals.set("KEYS", keys_t)?;
            globals.set("ARGV", argv_t)?;

            let redis_t = lua.create_table()?;
            let call = scope.create_function(|lua, cargs: mlua::MultiValue| {
                let mut parts: Vec<RespValue> = Vec::with_capacity(cargs.len());
                for v in cargs {
                    match v {
                        mlua::Value::String(s) => parts.push(resp::bulk(&s.as_bytes())),
                        mlua::Value::Integer(n) => parts.push(resp::bulk_string(&n.to_string())),
                        mlua::Value::Number(n) => parts.push(resp::bulk_string(&format_score(n))),
                        _ => return Err(mlua::Error::runtime("invalid redis.call argument")),
                    }
                }
                let r = execute(store, &parts);
                if let RespValue::Error(e) = &r {
                    return Err(mlua::Error::runtime(e.clone()));
                }
                resp_to_lua(lua, &r)
            })?;
            let pcall = scope.create_function(|lua, cargs: mlua::MultiValue| {
                let mut parts: Vec<RespValue> = Vec::with_capacity(cargs.len());
                for v in cargs {
                    match v {
                        mlua::Value::String(s) => parts.push(resp::bulk(&s.as_bytes())),
                        mlua::Value::Integer(n) => parts.push(resp::bulk_string(&n.to_string())),
                        mlua::Value::Number(n) => parts.push(resp::bulk_string(&format_score(n))),
                        _ => return Err(mlua::Error::runtime("invalid redis.pcall argument")),
                    }
                }
                resp_to_lua(lua, &execute(store, &parts))
            })?;
            let error_reply = scope.create_function(|lua, msg: String| {
                let t = lua.create_table()?;
                t.set("err", msg)?;
                Ok(t)
            })?;
            let status_reply = scope.create_function(|lua, msg: String| {
                let t = lua.create_table()?;
                t.set("ok", msg)?;
                Ok(t)
            })?;
            let sha1hex_f = scope.create_function(|_, s: mlua::String| {
                Ok(sha1_hex(&s.to_string_lossy()))
            })?;
            redis_t.set("sha1hex", sha1hex_f)?;
            redis_t.set("call", call)?;
            redis_t.set("pcall", pcall)?;
            redis_t.set("error_reply", error_reply)?;
            redis_t.set("status_reply", status_reply)?;
            globals.set("redis", redis_t)?;

            // Minimal cjson: encode/decode bridged through serde_json.
            let cjson_t = lua.create_table()?;
            let enc = scope.create_function(|lua, v: mlua::Value| {
                fn to_json(lua: &mlua::Lua, v: &mlua::Value) -> serde_json::Value {
                    match v {
                        mlua::Value::Nil => serde_json::Value::Null,
                        mlua::Value::Boolean(b) => json!(b),
                        mlua::Value::Integer(n) => json!(n),
                        mlua::Value::Number(n) => json!(n),
                        mlua::Value::String(s) => json!(s.to_string_lossy()),
                        mlua::Value::Table(t) => {
                            let len = t.raw_len();
                            if len > 0 {
                                let arr: Vec<serde_json::Value> = (1..=len)
                                    .filter_map(|i| t.get::<mlua::Value>(i).ok())
                                    .map(|x| to_json(lua, &x))
                                    .collect();
                                serde_json::Value::Array(arr)
                            } else {
                                let mut m = serde_json::Map::new();
                                for pair in t.clone().pairs::<String, mlua::Value>().flatten() {
                                    m.insert(pair.0, to_json(lua, &pair.1));
                                }
                                serde_json::Value::Object(m)
                            }
                        }
                        _ => serde_json::Value::Null,
                    }
                }
                Ok(to_json(lua, &v).to_string())
            })?;
            let dec = scope.create_function(|lua, s: mlua::String| {
                fn to_lua(lua: &mlua::Lua, v: &serde_json::Value) -> mlua::Result<mlua::Value> {
                    Ok(match v {
                        serde_json::Value::Null => mlua::Value::Nil,
                        serde_json::Value::Bool(b) => mlua::Value::Boolean(*b),
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                mlua::Value::Integer(i)
                            } else {
                                mlua::Value::Number(n.as_f64().unwrap_or(0.0))
                            }
                        }
                        serde_json::Value::String(s) => mlua::Value::String(lua.create_string(s)?),
                        serde_json::Value::Array(a) => {
                            let t = lua.create_table()?;
                            for (i, x) in a.iter().enumerate() {
                                t.set(i + 1, to_lua(lua, x)?)?;
                            }
                            mlua::Value::Table(t)
                        }
                        serde_json::Value::Object(o) => {
                            let t = lua.create_table()?;
                            for (k, x) in o {
                                t.set(k.as_str(), to_lua(lua, x)?)?;
                            }
                            mlua::Value::Table(t)
                        }
                    })
                }
                let parsed: serde_json::Value =
                    serde_json::from_str(&s.to_string_lossy()).map_err(mlua::Error::runtime)?;
                to_lua(lua, &parsed)
            })?;
            cjson_t.set("encode", enc)?;
            cjson_t.set("decode", dec)?;
            globals.set("cjson", cjson_t)?;

            // Compile-once: cached Function in the VM registry, keyed by sha1.
            let cache: mlua::Table = match lua.named_registry_value("oxi_scripts") {
                Ok(t) => t,
                Err(_) => {
                    let t = lua.create_table()?;
                    lua.set_named_registry_value("oxi_scripts", t.clone())?;
                    t
                }
            };
            let func: mlua::Function = match cache.get::<mlua::Value>(sha_key.as_str())? {
                mlua::Value::Function(f) => f,
                _ => {
                    let f = lua.load(&src).into_function()?;
                    cache.set(sha_key.as_str(), f.clone())?;
                    f
                }
            };
            let v: mlua::Value = func.call(())?;
            Ok(lua_to_resp(v))
        });
        lua.remove_hook();
        result
    });
    match out {
        Ok(r) => r,
        Err(e) => resp::err(&format!("ERR Error running script: {e}")),
    }
}

fn format_score(s: f64) -> String {
    if s == s.floor() && s.is_finite() {
        format!("{}", s as i64)
    } else {
        format!("{s}")
    }
}

fn parse_score_bound(s: &str, default: f64) -> f64 {
    match s {
        "-inf" | "-INF" => f64::NEG_INFINITY,
        "+inf" | "+INF" | "inf" | "INF" => f64::INFINITY,
        _ => {
            let s = s.strip_prefix('(').unwrap_or(s);
            s.parse().unwrap_or(default)
        }
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

fn glob_to_regex(pattern: &str) -> String {
    let mut regex = String::from("^");
    for ch in pattern.chars() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '.' | '+' | '^' | '$' | '(' | ')' | '{' | '}' | '[' | ']' | '|' | '\\' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex.push('$');
    regex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_to_regex() {
        assert_eq!(glob_to_regex("*"), "^.*$");
        assert_eq!(glob_to_regex("user:*"), "^user:.*$");
        assert_eq!(glob_to_regex("h?llo"), "^h.llo$");
        assert_eq!(glob_to_regex("hello.world"), "^hello\\.world$");
    }

    // ---- MULTI / EXEC / WATCH transaction tests ----

    fn c(parts: &[&str]) -> Vec<RespValue> {
        parts
            .iter()
            .map(|p| RespValue::BulkString(p.as_bytes().to_vec()))
            .collect()
    }
    fn is_ok(r: &RespValue) -> bool {
        matches!(r, RespValue::SimpleString(s) if s == "OK")
    }
    fn getf(store: &OxiMemStore, key: &str) -> f64 {
        execute(store, &c(&["GET", key]))
            .as_str()
            .unwrap()
            .parse()
            .unwrap()
    }

    #[test]
    fn multi_exec_runs_queued_atomically() {
        let store = OxiMemStore::new();
        let mut s = Session::default();
        assert!(is_ok(&execute_session(&store, &mut s, &c(&["MULTI"]))));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["SET", "a", "1"])),
            RespValue::SimpleString(ref x) if x == "QUEUED"
        ));
        execute_session(&store, &mut s, &c(&["INCR", "a"]));
        match execute_session(&store, &mut s, &c(&["EXEC"])) {
            RespValue::Array(v) => assert_eq!(v.len(), 2),
            other => panic!("expected array, got {other:?}"),
        }
        assert_eq!(execute(&store, &c(&["GET", "a"])).as_str(), Some("2"));
    }

    #[test]
    fn discard_cancels_queue() {
        let store = OxiMemStore::new();
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["MULTI"]));
        execute_session(&store, &mut s, &c(&["SET", "x", "1"]));
        assert!(is_ok(&execute_session(&store, &mut s, &c(&["DISCARD"]))));
        assert!(matches!(execute(&store, &c(&["GET", "x"])), RespValue::Null));
        // EXEC now has no transaction to run.
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["EXEC"])),
            RespValue::Error(_)
        ));
    }

    #[test]
    fn exec_and_multi_state_errors() {
        let store = OxiMemStore::new();
        let mut s = Session::default();
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["EXEC"])),
            RespValue::Error(_)
        ));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["DISCARD"])),
            RespValue::Error(_)
        ));
        execute_session(&store, &mut s, &c(&["MULTI"]));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["MULTI"])),
            RespValue::Error(_)
        ));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["WATCH", "k"])),
            RespValue::Error(_)
        ));
    }

    #[test]
    fn watch_allows_exec_when_unchanged() {
        let store = OxiMemStore::new();
        execute(&store, &c(&["SET", "k", "1"]));
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["WATCH", "k"]));
        execute_session(&store, &mut s, &c(&["MULTI"]));
        execute_session(&store, &mut s, &c(&["SET", "k", "2"]));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["EXEC"])),
            RespValue::Array(_)
        ));
        assert_eq!(execute(&store, &c(&["GET", "k"])).as_str(), Some("2"));
    }

    #[test]
    fn watch_aborts_exec_on_change() {
        let store = OxiMemStore::new();
        execute(&store, &c(&["SET", "k", "1"]));
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["WATCH", "k"]));
        // Another connection modifies the watched key.
        execute(&store, &c(&["SET", "k", "999"]));
        execute_session(&store, &mut s, &c(&["MULTI"]));
        execute_session(&store, &mut s, &c(&["SET", "k", "2"]));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["EXEC"])),
            RespValue::NullArray // aborted
        ));
        // The transaction did not run.
        assert_eq!(execute(&store, &c(&["GET", "k"])).as_str(), Some("999"));
    }

    #[test]
    fn atomic_settlement_transfer() {
        // The exchange pattern: move 100 cash from A to B atomically.
        let store = OxiMemStore::new();
        execute(&store, &c(&["SET", "usd:A", "1000"]));
        execute(&store, &c(&["SET", "usd:B", "500"]));
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["WATCH", "usd:A", "usd:B"]));
        // (client checks A has enough) then moves funds in one atomic block
        execute_session(&store, &mut s, &c(&["MULTI"]));
        execute_session(&store, &mut s, &c(&["INCRBYFLOAT", "usd:A", "-100"]));
        execute_session(&store, &mut s, &c(&["INCRBYFLOAT", "usd:B", "100"]));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["EXEC"])),
            RespValue::Array(_)
        ));
        assert!((getf(&store, "usd:A") - 900.0).abs() < 1e-9);
        assert!((getf(&store, "usd:B") - 600.0).abs() < 1e-9);
    }

    #[test]
    fn settlement_aborts_on_concurrent_change_no_double_spend() {
        // WATCH is what makes hot-account settlement safe: if the balance moved
        // between the sufficiency check and EXEC, the transaction aborts instead
        // of overdrawing.
        let store = OxiMemStore::new();
        execute(&store, &c(&["SET", "usd:A", "1000"]));
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["WATCH", "usd:A"]));
        // A concurrent matcher drains the account after our WATCH.
        execute(&store, &c(&["INCRBYFLOAT", "usd:A", "-1000"]));
        execute_session(&store, &mut s, &c(&["MULTI"]));
        execute_session(&store, &mut s, &c(&["INCRBYFLOAT", "usd:A", "-100"]));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["EXEC"])),
            RespValue::NullArray // aborted — no overdraft
        ));
        assert!((getf(&store, "usd:A") - 0.0).abs() < 1e-9);
    }

    #[test]
    fn unknown_command_in_multi_aborts_exec() {
        let store = OxiMemStore::new();
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["MULTI"]));
        execute_session(&store, &mut s, &c(&["SET", "a", "1"]));
        // Typo'd command poisons the transaction at queue time…
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["SETT", "b", "2"])),
            RespValue::Error(_)
        ));
        // …so EXEC refuses to run anything (EXECABORT).
        match execute_session(&store, &mut s, &c(&["EXEC"])) {
            RespValue::Error(e) => assert!(e.contains("EXECABORT"), "got {e}"),
            other => panic!("expected EXECABORT, got {other:?}"),
        }
        assert!(matches!(execute(&store, &c(&["GET", "a"])), RespValue::Null));
    }

    #[test]
    fn watch_detects_expiry_and_flushall() {
        let store = OxiMemStore::new();
        // Lazy expiry between WATCH and EXEC counts as a change.
        execute(&store, &c(&["SET", "e", "1"]));
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["WATCH", "e"]));
        execute(&store, &c(&["DEL", "e"])); // stand-in for expiry: key vanishes
        execute_session(&store, &mut s, &c(&["MULTI"]));
        execute_session(&store, &mut s, &c(&["SET", "e", "2"]));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["EXEC"])),
            RespValue::NullArray
        ));
        // FLUSHALL bumps the epoch — aborts any WATCH, even on other keys.
        execute(&store, &c(&["SET", "f", "1"]));
        let mut s2 = Session::default();
        execute_session(&store, &mut s2, &c(&["WATCH", "f"]));
        execute(&store, &c(&["FLUSHALL"]));
        execute_session(&store, &mut s2, &c(&["MULTI"]));
        execute_session(&store, &mut s2, &c(&["SET", "f", "2"]));
        assert!(matches!(
            execute_session(&store, &mut s2, &c(&["EXEC"])),
            RespValue::NullArray
        ));
    }

    #[test]
    fn watch_sees_coalesced_pipeline_writes() {
        // The lock-coalesced pipeline path bypasses execute(); its writes must
        // still bump WATCH versions.
        let store = OxiMemStore::new();
        execute(&store, &c(&["SET", "p", "1"]));
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["WATCH", "p"]));
        let batch = vec![
            RespValue::Array(c(&["SET", "p", "9"])),
            RespValue::Array(c(&["SET", "p2", "9"])),
        ];
        execute_pipeline(&store, &batch);
        execute_session(&store, &mut s, &c(&["MULTI"]));
        execute_session(&store, &mut s, &c(&["SET", "p", "2"]));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["EXEC"])),
            RespValue::NullArray
        ));
        assert_eq!(execute(&store, &c(&["GET", "p"])).as_str(), Some("9"));
    }

    // ---- extended-command tests ----

    #[test]
    fn getdel_setrange_copy_roundtrip() {
        let store = OxiMemStore::new();
        execute(&store, &c(&["SET", "g", "hello"]));
        assert_eq!(execute(&store, &c(&["GETDEL", "g"])).as_str(), Some("hello"));
        assert!(matches!(execute(&store, &c(&["GET", "g"])), RespValue::Null));
        execute(&store, &c(&["SET", "r", "Hello World"]));
        assert_eq!(
            execute(&store, &c(&["SETRANGE", "r", "6", "Redis"])),
            RespValue::Integer(11)
        );
        assert_eq!(execute(&store, &c(&["GET", "r"])).as_str(), Some("Hello Redis"));
        // COPY without REPLACE fails onto an existing key.
        execute(&store, &c(&["SET", "dst", "x"]));
        assert_eq!(execute(&store, &c(&["COPY", "r", "dst"])), RespValue::Integer(0));
        assert_eq!(
            execute(&store, &c(&["COPY", "r", "dst", "REPLACE"])),
            RespValue::Integer(1)
        );
        assert_eq!(execute(&store, &c(&["GET", "dst"])).as_str(), Some("Hello Redis"));
    }

    #[test]
    fn set_operations_and_store_variants() {
        let store = OxiMemStore::new();
        execute(&store, &c(&["SADD", "a", "1", "2", "3"]));
        execute(&store, &c(&["SADD", "b", "2", "3", "4"]));
        match execute(&store, &c(&["SINTER", "a", "b"])) {
            RespValue::Array(v) => assert_eq!(v.len(), 2), // 2,3
            other => panic!("{other:?}"),
        }
        match execute(&store, &c(&["SUNION", "a", "b"])) {
            RespValue::Array(v) => assert_eq!(v.len(), 4),
            other => panic!("{other:?}"),
        }
        match execute(&store, &c(&["SDIFF", "a", "b"])) {
            RespValue::Array(v) => assert_eq!(v.len(), 1), // 1
            other => panic!("{other:?}"),
        }
        assert_eq!(
            execute(&store, &c(&["SINTERSTORE", "dest", "a", "b"])),
            RespValue::Integer(2)
        );
        assert_eq!(execute(&store, &c(&["SCARD", "dest"])), RespValue::Integer(2));
    }

    #[test]
    fn scan_cursor_pages_through_all_types() {
        let store = OxiMemStore::new();
        for i in 0..25 {
            execute(&store, &c(&["SET", &format!("k{i:02}"), "v"]));
        }
        execute(&store, &c(&["HSET", "h1", "f", "v"]));
        execute(&store, &c(&["ZADD", "z1", "1", "m"]));
        let mut cursor = "0".to_string();
        let mut seen = 0;
        loop {
            let r = execute(&store, &c(&["SCAN", &cursor, "COUNT", "7"]));
            let arr = match r {
                RespValue::Array(v) => v,
                other => panic!("{other:?}"),
            };
            cursor = arr[0].as_str().unwrap().to_string();
            if let RespValue::Array(keys) = &arr[1] {
                seen += keys.len();
            }
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen, 27); // 25 strings + hash + zset
    }

    #[test]
    fn zrevrange_ranks_are_relative_to_descending_view() {
        // Found by the hybrid-exchange prototype: `ZREVRANGE book 0 0` (best
        // bid) returned the LOWEST price. Ranks must index the DESC ordering.
        let store = OxiMemStore::new();
        execute(&store, &c(&["ZADD", "z", "1", "low"]));
        execute(&store, &c(&["ZADD", "z", "3", "high"]));
        execute(&store, &c(&["ZADD", "z", "2", "mid"]));
        match execute(&store, &c(&["ZREVRANGE", "z", "0", "0"])) {
            RespValue::Array(v) => assert_eq!(v[0].as_str(), Some("high")),
            other => panic!("{other:?}"),
        }
        match execute(&store, &c(&["ZREVRANGE", "z", "1", "2"])) {
            RespValue::Array(v) => {
                let names: Vec<_> = v.iter().filter_map(|x| x.as_str()).collect();
                assert_eq!(names, ["mid", "low"]);
            }
            other => panic!("{other:?}"),
        }
        match execute(&store, &c(&["ZRANGE", "z", "0", "0"])) {
            RespValue::Array(v) => assert_eq!(v[0].as_str(), Some("low")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn zrangebylex_bounds() {
        let store = OxiMemStore::new();
        for m in ["a", "b", "c", "d"] {
            execute(&store, &c(&["ZADD", "z", "0", m]));
        }
        match execute(&store, &c(&["ZRANGEBYLEX", "z", "[b", "(d"])) {
            RespValue::Array(v) => {
                let names: Vec<_> = v.iter().filter_map(|x| x.as_str()).collect();
                assert_eq!(names, ["b", "c"]);
            }
            other => panic!("{other:?}"),
        }
        match execute(&store, &c(&["ZRANGEBYLEX", "z", "-", "+"])) {
            RespValue::Array(v) => assert_eq!(v.len(), 4),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn blocking_pop_returns_immediately_when_data_exists() {
        let store = OxiMemStore::new();
        execute(&store, &c(&["RPUSH", "q", "job1"]));
        match execute(&store, &c(&["BLPOP", "q", "5"])) {
            RespValue::Array(v) => {
                assert_eq!(v[0].as_str(), Some("q"));
                assert_eq!(v[1].as_str(), Some("job1"));
            }
            other => panic!("{other:?}"),
        }
        // Empty + short timeout → NullArray after ~timeout.
        let t0 = std::time::Instant::now();
        assert!(matches!(
            execute(&store, &c(&["BLPOP", "q", "0.1"])),
            RespValue::NullArray
        ));
        assert!(t0.elapsed() >= std::time::Duration::from_millis(90));
        // BZPOPMIN pops lowest score.
        execute(&store, &c(&["ZADD", "zq", "2", "b"]));
        execute(&store, &c(&["ZADD", "zq", "1", "a"]));
        match execute(&store, &c(&["BZPOPMIN", "zq", "1"])) {
            RespValue::Array(v) => assert_eq!(v[1].as_str(), Some("a")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn keyspace_notifications_publish_events() {
        let store = OxiMemStore::new();
        execute(&store, &c(&["CONFIG", "SET", "notify-keyspace-events", "KEA"]));
        let rx = store.subscribe("__keyspace@0__:nk");
        execute(&store, &c(&["SET", "nk", "1"]));
        let (chan, msg) = rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap();
        assert_eq!(chan, "__keyspace@0__:nk");
        assert_eq!(msg, "set");
    }

    #[test]
    fn hello_negotiates_resp2_and_rejects_resp3() {
        let store = OxiMemStore::new();
        assert!(matches!(execute(&store, &c(&["HELLO"])), RespValue::Array(_)));
        assert!(matches!(execute(&store, &c(&["HELLO", "2"])), RespValue::Array(_)));
        match execute(&store, &c(&["HELLO", "3"])) {
            RespValue::Error(e) => assert!(e.contains("NOPROTO")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn arity_error_in_multi_aborts() {
        let store = OxiMemStore::new();
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["MULTI"]));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["SET", "onlykey"])), // missing value
            RespValue::Error(_)
        ));
        match execute_session(&store, &mut s, &c(&["EXEC"])) {
            RespValue::Error(e) => assert!(e.contains("EXECABORT")),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn eval_runs_scripts_atomically() {
        let store = OxiMemStore::new();
        // Plain return value conversions.
        assert_eq!(
            execute(&store, &c(&["EVAL", "return 1 + 1", "0"])),
            RespValue::Integer(2)
        );
        assert_eq!(
            execute(&store, &c(&["EVAL", "return ARGV[1]", "0", "hi"])).as_str(),
            Some("hi")
        );
        // redis.call bridge + KEYS.
        execute(&store, &c(&["SET", "bal", "100"]));
        let script = "local b = tonumber(redis.call('GET', KEYS[1])) \
                      if b >= tonumber(ARGV[1]) then \
                        redis.call('INCRBYFLOAT', KEYS[1], '-' .. ARGV[1]) \
                        return 1 \
                      end \
                      return 0";
        assert_eq!(
            execute(&store, &c(&["EVAL", script, "1", "bal", "40"])),
            RespValue::Integer(1)
        );
        assert_eq!(execute(&store, &c(&["GET", "bal"])).as_str(), Some("60"));
        // Insufficient funds path.
        assert_eq!(
            execute(&store, &c(&["EVAL", script, "1", "bal", "999"])),
            RespValue::Integer(0)
        );
        assert_eq!(execute(&store, &c(&["GET", "bal"])).as_str(), Some("60"));
    }

    #[test]
    fn evalsha_and_script_cache() {
        let store = OxiMemStore::new();
        let sha = match execute(&store, &c(&["SCRIPT", "LOAD", "return 42"])) {
            RespValue::BulkString(b) => String::from_utf8(b).unwrap(),
            other => panic!("{other:?}"),
        };
        assert_eq!(
            execute(&store, &c(&["EVALSHA", &sha, "0"])),
            RespValue::Integer(42)
        );
        match execute(&store, &c(&["SCRIPT", "EXISTS", &sha, "deadbeef"])) {
            RespValue::Array(v) => {
                assert_eq!(v[0], RespValue::Integer(1));
                assert_eq!(v[1], RespValue::Integer(0));
            }
            other => panic!("{other:?}"),
        }
        assert!(matches!(
            execute(&store, &c(&["EVALSHA", "deadbeef", "0"])),
            RespValue::Error(_)
        ));
    }

    #[test]
    fn eval_writes_are_visible_to_watch() {
        let store = OxiMemStore::new();
        execute(&store, &c(&["SET", "w", "1"]));
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["WATCH", "w"]));
        execute(&store, &c(&["EVAL", "redis.call('SET', KEYS[1], '2')", "1", "w"]));
        execute_session(&store, &mut s, &c(&["MULTI"]));
        execute_session(&store, &mut s, &c(&["SET", "w", "3"]));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["EXEC"])),
            RespValue::NullArray
        ));
    }

    #[test]
    fn rebuild_on_boot_restores_all_types() {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(OxiDb::open(dir.path()).unwrap());
        {
            let store = OxiMemStore::new_with_sql(Arc::clone(&db));
            execute(&store, &c(&["SET", "k", "v"]));
            execute(&store, &c(&["HSET", "h", "f1", "x", "f2", "y"]));
            execute(&store, &c(&["RPUSH", "l", "a", "b"]));
            execute(&store, &c(&["SADD", "s", "m1", "m2"]));
            execute(&store, &c(&["ZADD", "z", "1.5", "mem"]));
        }
        // A fresh store over the same OxiDb must repopulate everything.
        let store2 = OxiMemStore::new_with_sql(Arc::clone(&db));
        assert_eq!(execute(&store2, &c(&["GET", "k"])).as_str(), Some("v"));
        assert_eq!(execute(&store2, &c(&["HGET", "h", "f2"])).as_str(), Some("y"));
        assert_eq!(execute(&store2, &c(&["LLEN", "l"])), RespValue::Integer(2));
        assert_eq!(execute(&store2, &c(&["SCARD", "s"])), RespValue::Integer(2));
        assert_eq!(
            execute(&store2, &c(&["ZSCORE", "z", "mem"])).as_str(),
            Some("1.5")
        );
    }

    #[test]
    fn round3_command_variants() {
        let store = OxiMemStore::new();
        // SPOP/SRANDMEMBER count
        execute(&store, &c(&["SADD", "s3", "a", "b", "c", "d"]));
        match execute(&store, &c(&["SPOP", "s3", "2"])) {
            RespValue::Array(v) => assert_eq!(v.len(), 2),
            o => panic!("{o:?}"),
        }
        assert_eq!(execute(&store, &c(&["SCARD", "s3"])), RespValue::Integer(2));
        match execute(&store, &c(&["SRANDMEMBER", "s3", "2"])) {
            RespValue::Array(v) => assert_eq!(v.len(), 2),
            o => panic!("{o:?}"),
        }
        // SMISMEMBER
        match execute(&store, &c(&["SMISMEMBER", "s3", "zzz"])) {
            RespValue::Array(v) => assert_eq!(v[0], RespValue::Integer(0)),
            o => panic!("{o:?}"),
        }
        // BITCOUNT range
        execute(&store, &c(&["SET", "bits", "foobar"]));
        assert_eq!(execute(&store, &c(&["BITCOUNT", "bits"])), RespValue::Integer(26));
        assert_eq!(
            execute(&store, &c(&["BITCOUNT", "bits", "1", "1"])),
            RespValue::Integer(6)
        );
        // GETEX PX then PERSIST
        execute(&store, &c(&["SET", "gx", "v"]));
        execute(&store, &c(&["GETEX", "gx", "PX", "60000"]));
        assert!(matches!(execute(&store, &c(&["TTL", "gx"])), RespValue::Integer(n) if n > 0));
        execute(&store, &c(&["GETEX", "gx", "PERSIST"]));
        assert_eq!(execute(&store, &c(&["TTL", "gx"])), RespValue::Integer(-1));
        // LMPOP / ZMPOP
        execute(&store, &c(&["RPUSH", "l3", "x", "y"]));
        match execute(&store, &c(&["LMPOP", "2", "nope", "l3", "LEFT"])) {
            RespValue::Array(v) => assert_eq!(v[0].as_str(), Some("l3")),
            o => panic!("{o:?}"),
        }
        execute(&store, &c(&["ZADD", "z3", "1", "m"]));
        match execute(&store, &c(&["ZMPOP", "1", "z3", "MIN"])) {
            RespValue::Array(v) => assert_eq!(v[0].as_str(), Some("z3")),
            o => panic!("{o:?}"),
        }
        // BRPOPLPUSH immediate + BLMOVE
        execute(&store, &c(&["RPUSH", "q1", "job"]));
        assert_eq!(
            execute(&store, &c(&["BRPOPLPUSH", "q1", "q2", "1"])).as_str(),
            Some("job")
        );
        assert_eq!(execute(&store, &c(&["LLEN", "q2"])), RespValue::Integer(1));
    }

    #[test]
    fn round3_subscan_cursors_and_punsubscribe() {
        let store = OxiMemStore::new();
        for i in 0..25 {
            execute(&store, &c(&["HSET", "bigh", &format!("f{i:02}"), "v"]));
        }
        let mut cursor = "0".to_string();
        let mut seen = 0;
        loop {
            let r = execute(&store, &c(&["HSCAN", "bigh", &cursor, "COUNT", "7"]));
            let arr = match r {
                RespValue::Array(v) => v,
                o => panic!("{o:?}"),
            };
            cursor = arr[0].as_str().unwrap().to_string();
            if let RespValue::Array(items) = &arr[1] {
                seen += items.len() / 2;
            }
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen, 25);
        // PSUBSCRIBE receives, PUNSUBSCRIBE stops delivery.
        let rx = store.psubscribe("ch.*").unwrap();
        execute(&store, &c(&["PUBLISH", "ch.one", "hello"]));
        assert_eq!(rx.recv_timeout(std::time::Duration::from_secs(1)).unwrap().1, "hello");
        store.punsubscribe("ch.*");
        assert_eq!(execute(&store, &c(&["PUBLISH", "ch.one", "again"])), RespValue::Integer(0));
    }

    #[test]
    fn round3_eval_helpers_and_script_kill_flag() {
        let store = OxiMemStore::new();
        // cjson round-trip inside Lua.
        let r = execute(&store, &c(&[
            "EVAL",
            "local t = cjson.decode(ARGV[1]) return cjson.encode({t.a, t.b})",
            "0",
            "{\"a\":1,\"b\":\"x\"}",
        ]));
        assert_eq!(r.as_str(), Some("[1,\"x\"]"));
        // sha1hex
        let r = execute(&store, &c(&["EVAL", "return redis.sha1hex('')", "0"]));
        assert_eq!(r.as_str(), Some("da39a3ee5e6b4b0d3255bfef95601890afd80709"));
        // Busy-script guard still kills infinite loops.
        let r = execute(&store, &c(&["EVAL", "while true do end", "0"]));
        assert!(matches!(r, RespValue::Error(e) if e.contains("time limit")));
    }

    #[test]
    fn decrbyfloatge_conditional_debit() {
        let store = OxiMemStore::new();
        execute(&store, &c(&["SET", "bal", "100"]));
        assert_eq!(
            execute(&store, &c(&["DECRBYFLOATGE", "bal", "40"])).as_str(),
            Some("60")
        );
        // Insufficient → Null, unchanged.
        assert!(matches!(
            execute(&store, &c(&["DECRBYFLOATGE", "bal", "100"])),
            RespValue::Null
        ));
        assert_eq!(execute(&store, &c(&["GET", "bal"])).as_str(), Some("60"));
        // Missing key → Null.
        assert!(matches!(
            execute(&store, &c(&["DECRBYFLOATGE", "nope", "1"])),
            RespValue::Null
        ));
    }

    #[test]
    fn concurrent_disjoint_evals_are_correct() {
        // 4 threads × disjoint counters via EVAL: sharded stripes must let
        // them run concurrently AND keep every increment.
        let store = std::sync::Arc::new(OxiMemStore::new());
        let mut handles = Vec::new();
        for t in 0..4 {
            let st = std::sync::Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                let key = format!("cnt{t}");
                for _ in 0..500 {
                    execute(
                        &st,
                        &c(&["EVAL", "redis.call('INCR', KEYS[1]) return 1", "1", &key]),
                    );
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        for t in 0..4 {
            assert_eq!(
                execute(&store, &c(&["GET", &format!("cnt{t}")])).as_str(),
                Some("500")
            );
        }
    }

    #[test]
    fn concurrent_conditional_debits_never_overdraw() {
        // 8 threads race DECRBYFLOATGE on ONE balance via EVAL (undeclared-
        // key pattern): total debited must equal the starting balance, never
        // more — the single-command atomicity contract.
        let store = std::sync::Arc::new(OxiMemStore::new());
        execute(&store, &c(&["SET", "hot", "1000"]));
        let ok = std::sync::Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let st = std::sync::Arc::clone(&store);
            let okc = std::sync::Arc::clone(&ok);
            handles.push(std::thread::spawn(move || {
                for _ in 0..500 {
                    let r = execute(
                        &st,
                        &c(&[
                            "EVAL",
                            "return redis.call('DECRBYFLOATGE', 'hot', '1') ~= false and 1 or 0",
                            "0",
                        ]),
                    );
                    if r == RespValue::Integer(1) {
                        okc.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(ok.load(Ordering::SeqCst), 1000); // exactly the balance
        assert_eq!(execute(&store, &c(&["GET", "hot"])).as_str(), Some("0"));
    }

    #[test]
    fn watch_hash_and_zset_keys() {
        // WATCH must detect changes on hash (balances) and sorted-set (book) keys.
        let store = OxiMemStore::new();
        execute(&store, &c(&["HSET", "bal", "usd", "10"]));
        execute(&store, &c(&["ZADD", "book", "1", "o1"]));
        let mut s = Session::default();
        execute_session(&store, &mut s, &c(&["WATCH", "bal", "book"]));
        execute(&store, &c(&["ZADD", "book", "2", "o2"])); // book changed
        execute_session(&store, &mut s, &c(&["MULTI"]));
        execute_session(&store, &mut s, &c(&["HINCRBY", "bal", "usd", "5"]));
        assert!(matches!(
            execute_session(&store, &mut s, &c(&["EXEC"])),
            RespValue::NullArray
        ));
        assert_eq!(execute(&store, &c(&["HGET", "bal", "usd"])).as_str(), Some("10"));
    }
}
