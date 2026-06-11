//! OxiMem — native in-memory key-value store with RESP wire protocol.
//!
//! By default, commands operate on raw `HashMap`/`VecDeque`/`HashSet`
//! structures for maximum throughput (no JSON, no WAL, no indexes).
//!
//! When constructed with `OxiMemStore::new_with_sql(db)`, data is also mirrored
//! to OxiDB collections (`_kv`, `_hash`, `_list`, `_set`) so it can be queried
//! via SQL: `SELECT * FROM _kv WHERE _key LIKE 'session:%'`

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
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
            db: None,
        }
    }

    /// SQL mode — data mirrored to OxiDB collections for SQL queries.
    pub fn new_with_sql(db: Arc<OxiDb>) -> Self {
        Self {
            strings: RwLock::new(HashMap::new()),
            hashes: RwLock::new(HashMap::new()),
            lists: RwLock::new(HashMap::new()),
            sets: RwLock::new(HashMap::new()),
            sorted_sets: RwLock::new(HashMap::new()),
            pubsub: Mutex::new(HashMap::new()),
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

    /// Publish a message to a channel, returns number of receivers.
    pub fn publish(&self, channel: &str, message: &str) -> i64 {
        let mut map = self.pubsub.lock().unwrap();
        let senders = match map.get_mut(channel) {
            Some(s) => s,
            None => return 0,
        };
        // Send to all, remove dead senders
        senders.retain(|tx| tx.send((channel.to_string(), message.to_string())).is_ok());
        let count = senders.len() as i64;
        if senders.is_empty() {
            map.remove(channel);
        }
        count
    }

    pub fn sql_enabled(&self) -> bool {
        self.db.is_some()
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

        match first_cmd.as_str() {
            "GET" => return pipeline_get(store, &args_list),
            "SET" => return pipeline_set(store, &args_list),
            "INCR" => return pipeline_incr(store, &args_list),
            "LPUSH" => return pipeline_lpush(store, &args_list),
            "RPUSH" => return pipeline_rpush(store, &args_list),
            "HSET" => return pipeline_hset(store, &args_list),
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
pub fn execute(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
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
        "CONFIG" => resp::ok(),

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

fn mirror_flush(store: &OxiMemStore) {
    if let Some(db) = &store.db {
        let _ = db.drop_collection("_kv");
        let _ = db.drop_collection("_hash");
        let _ = db.drop_collection("_list");
        let _ = db.drop_collection("_set");
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

fn cmd_scan(store: &OxiMemStore, args: &[RespValue]) -> RespValue {
    let mut pattern = "*";
    let mut i = 1;
    while i + 1 < args.len() {
        if let Some(flag) = args[i].as_str() {
            if flag.eq_ignore_ascii_case("MATCH") {
                if let Some(p) = args.get(i + 1).and_then(|a| a.as_str()) {
                    pattern = p;
                }
            }
        }
        i += 2;
    }
    let keys_resp = cmd_keys(store, &[resp::bulk_string(pattern)]);
    resp::array(vec![resp::bulk_string("0"), keys_resp])
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

    let mut items = zset.range_by_rank(start, stop);
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
}
