//! Redis command handler — maps Redis commands to OxiDB operations.
//!
//! Data model mapping:
//! - String commands (GET/SET/DEL/etc.) → `_kv` collection, `{_key, _value, _ttl?}`
//! - Hash commands (HSET/HGET/etc.) → `_hash:{key}` → `{_key, field1, field2, ...}`
//! - List commands (LPUSH/RPUSH/etc.) → `_list` collection, `{_key, _items: [...]}`
//! - Set commands (SADD/SMEMBERS/etc.) → `_set` collection, `{_key, _members: [...]}`
//! - Sorted set commands → `_zset` collection, `{_key, _members: [{_m, _s}, ...]}`
//!
//! This gives OxiDB full Redis CLI / client compatibility while also allowing
//! SQL queries on the same data:
//!   `SELECT * FROM _kv WHERE _key LIKE 'session:%'`

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value};

use oxidb::OxiDb;

use crate::resp::{self, RespValue};

const KV_COLLECTION: &str = "_kv";

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Execute a Redis command against the OxiDB engine.
pub fn execute(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
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
        "SELECT" => resp::ok(), // single-db for now
        "COMMAND" => resp::ok(),
        "CLIENT" => resp::ok(),
        "AUTH" => resp::ok(), // no auth in this layer

        // -- String commands --
        "SET" => cmd_set(db, &args[1..]),
        "GET" => cmd_get(db, &args[1..]),
        "GETSET" => cmd_getset(db, &args[1..]),
        "SETNX" => cmd_setnx(db, &args[1..]),
        "SETEX" => cmd_setex(db, &args[1..]),
        "PSETEX" => cmd_psetex(db, &args[1..]),
        "MSET" => cmd_mset(db, &args[1..]),
        "MGET" => cmd_mget(db, &args[1..]),
        "INCR" => cmd_incr(db, &args[1..], 1),
        "DECR" => cmd_incr(db, &args[1..], -1),
        "INCRBY" => cmd_incrby(db, &args[1..]),
        "DECRBY" => cmd_decrby(db, &args[1..]),
        "INCRBYFLOAT" => cmd_incrbyfloat(db, &args[1..]),
        "APPEND" => cmd_append(db, &args[1..]),
        "STRLEN" => cmd_strlen(db, &args[1..]),
        "GETRANGE" => cmd_getrange(db, &args[1..]),

        // -- Key commands --
        "DEL" => cmd_del(db, &args[1..]),
        "EXISTS" => cmd_exists(db, &args[1..]),
        "EXPIRE" => cmd_expire(db, &args[1..]),
        "PEXPIRE" => cmd_pexpire(db, &args[1..]),
        "EXPIREAT" => cmd_expireat(db, &args[1..]),
        "PERSIST" => cmd_persist(db, &args[1..]),
        "TTL" => cmd_ttl(db, &args[1..]),
        "PTTL" => cmd_pttl(db, &args[1..]),
        "TYPE" => cmd_type(db, &args[1..]),
        "KEYS" => cmd_keys(db, &args[1..]),
        "RENAME" => cmd_rename(db, &args[1..]),
        "RANDOMKEY" => cmd_randomkey(db),
        "DBSIZE" => cmd_dbsize(db),
        "FLUSHDB" | "FLUSHALL" => cmd_flushdb(db),
        "SCAN" => cmd_scan(db, &args[1..]),

        // -- Hash commands --
        "HSET" => cmd_hset(db, &args[1..]),
        "HGET" => cmd_hget(db, &args[1..]),
        "HMSET" => cmd_hset(db, &args[1..]), // same as HSET
        "HMGET" => cmd_hmget(db, &args[1..]),
        "HDEL" => cmd_hdel(db, &args[1..]),
        "HEXISTS" => cmd_hexists(db, &args[1..]),
        "HGETALL" => cmd_hgetall(db, &args[1..]),
        "HKEYS" => cmd_hkeys(db, &args[1..]),
        "HVALS" => cmd_hvals(db, &args[1..]),
        "HLEN" => cmd_hlen(db, &args[1..]),
        "HINCRBY" => cmd_hincrby(db, &args[1..]),
        "HSETNX" => cmd_hsetnx(db, &args[1..]),

        // -- List commands --
        "LPUSH" => cmd_lpush(db, &args[1..]),
        "RPUSH" => cmd_rpush(db, &args[1..]),
        "LPOP" => cmd_lpop(db, &args[1..]),
        "RPOP" => cmd_rpop(db, &args[1..]),
        "LLEN" => cmd_llen(db, &args[1..]),
        "LRANGE" => cmd_lrange(db, &args[1..]),
        "LINDEX" => cmd_lindex(db, &args[1..]),

        // -- Set commands --
        "SADD" => cmd_sadd(db, &args[1..]),
        "SREM" => cmd_srem(db, &args[1..]),
        "SMEMBERS" => cmd_smembers(db, &args[1..]),
        "SISMEMBER" => cmd_sismember(db, &args[1..]),
        "SCARD" => cmd_scard(db, &args[1..]),

        // -- Server commands --
        "INFO" => cmd_info(db),
        "CONFIG" => resp::ok(),

        _ => resp::err(&format!("unknown command '{cmd}'")),
    }
}

// ---------------------------------------------------------------------------
// String commands
// ---------------------------------------------------------------------------

fn cmd_set(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
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

    // Parse optional flags: EX seconds | PX milliseconds | NX | XX
    let mut ttl_secs: Option<u64> = None;
    let mut nx = false;
    let mut xx = false;
    let mut i = 2;
    while i < args.len() {
        let flag = args[i].as_str().map(|s| s.to_uppercase());
        match flag.as_deref() {
            Some("EX") => {
                i += 1;
                ttl_secs = args.get(i).and_then(|v| v.as_str()).and_then(|s| s.parse().ok());
            }
            Some("PX") => {
                i += 1;
                let ms: Option<u64> = args.get(i).and_then(|v| v.as_str()).and_then(|s| s.parse().ok());
                ttl_secs = ms.map(|ms| (ms + 999) / 1000); // round up
            }
            Some("NX") => nx = true,
            Some("XX") => xx = true,
            _ => {}
        }
        i += 1;
    }

    // NX: only set if not exists
    if nx {
        let existing = db.find(KV_COLLECTION, &json!({"_key": key}));
        if let Ok(docs) = &existing {
            if !docs.is_empty() {
                return resp::null();
            }
        }
    }

    // XX: only set if exists
    if xx {
        let existing = db.find(KV_COLLECTION, &json!({"_key": key}));
        if let Ok(docs) = &existing {
            if docs.is_empty() {
                return resp::null();
            }
        }
    }

    // Delete existing key first
    let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));

    let mut doc = json!({"_key": key, "_value": value});
    if let Some(ttl) = ttl_secs {
        doc["_ttl"] = json!(ttl);
    }

    match db.insert(KV_COLLECTION, doc) {
        Ok(_) => resp::ok(),
        Err(e) => resp::err(&e.to_string()),
    }
}

fn cmd_get(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'get' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::null(),
    };

    match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => match doc.get("_value") {
            Some(Value::String(s)) => resp::bulk_string(s),
            Some(Value::Number(n)) => resp::bulk_string(&n.to_string()),
            Some(Value::Bool(b)) => resp::bulk_string(if *b { "1" } else { "0" }),
            _ => resp::null(),
        },
        _ => resp::null(),
    }
}

fn cmd_getset(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'getset' command");
    }
    let old = cmd_get(db, &args[..1]);
    cmd_set(db, args);
    old
}

fn cmd_setnx(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'setnx' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };

    if let Ok(Some(_)) = db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        return resp::integer(0);
    }

    let value = args[1].as_str().unwrap_or("");
    match db.insert(KV_COLLECTION, json!({"_key": key, "_value": value})) {
        Ok(_) => resp::integer(1),
        Err(_) => resp::integer(0),
    }
}

fn cmd_setex(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    // SETEX key seconds value
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
    let value = args[2].as_str().unwrap_or("");

    let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));
    match db.insert(KV_COLLECTION, json!({"_key": key, "_value": value, "_ttl": seconds})) {
        Ok(_) => resp::ok(),
        Err(e) => resp::err(&e.to_string()),
    }
}

fn cmd_psetex(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    // PSETEX key milliseconds value
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
    let value = args[2].as_str().unwrap_or("");

    let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));
    let secs = (ms + 999) / 1000;
    match db.insert(KV_COLLECTION, json!({"_key": key, "_value": value, "_ttl": secs})) {
        Ok(_) => resp::ok(),
        Err(e) => resp::err(&e.to_string()),
    }
}

fn cmd_mset(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 || args.len() % 2 != 0 {
        return resp::err("wrong number of arguments for 'mset' command");
    }
    for pair in args.chunks(2) {
        cmd_set(db, pair);
    }
    resp::ok()
}

fn cmd_mget(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    let results: Vec<RespValue> = args.iter().map(|arg| cmd_get(db, std::slice::from_ref(arg))).collect();
    resp::array(results)
}

fn cmd_incr(db: &Arc<OxiDb>, args: &[RespValue], delta: i64) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'incr' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };

    let current = match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => {
            let val_str = doc.get("_value").and_then(|v| v.as_str()).unwrap_or("0");
            match val_str.parse::<i64>() {
                Ok(n) => n,
                Err(_) => return resp::err("value is not an integer or out of range"),
            }
        }
        _ => 0,
    };

    let new_val = current + delta;
    let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));
    let _ = db.insert(KV_COLLECTION, json!({"_key": key, "_value": new_val.to_string()}));
    resp::integer(new_val)
}

fn cmd_incrby(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'incrby' command");
    }
    let delta: i64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => return resp::err("value is not an integer or out of range"),
    };
    cmd_incr(db, &args[..1], delta)
}

fn cmd_decrby(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'decrby' command");
    }
    let delta: i64 = match args[1].as_str().and_then(|s| s.parse().ok()) {
        Some(d) => d,
        None => return resp::err("value is not an integer or out of range"),
    };
    cmd_incr(db, &args[..1], -delta)
}

fn cmd_incrbyfloat(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
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

    let current: f64 = match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => {
            doc.get("_value").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0.0)
        }
        _ => 0.0,
    };

    let new_val = current + delta;
    let val_str = format!("{new_val}");
    let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));
    let _ = db.insert(KV_COLLECTION, json!({"_key": key, "_value": val_str}));
    resp::bulk_string(&val_str)
}

fn cmd_append(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'append' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };
    let suffix = args[1].as_str().unwrap_or("");

    let current = match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => doc.get("_value").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        _ => String::new(),
    };

    let new_val = format!("{current}{suffix}");
    let len = new_val.len() as i64;
    let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));
    let _ = db.insert(KV_COLLECTION, json!({"_key": key, "_value": new_val}));
    resp::integer(len)
}

fn cmd_strlen(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'strlen' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };

    match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => {
            let len = doc.get("_value").and_then(|v| v.as_str()).map(|s| s.len()).unwrap_or(0);
            resp::integer(len as i64)
        }
        _ => resp::integer(0),
    }
}

fn cmd_getrange(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'getrange' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::bulk_string(""),
    };
    let start: i64 = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let end: i64 = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);

    let val = match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => doc.get("_value").and_then(|v| v.as_str()).unwrap_or("").to_string(),
        _ => return resp::bulk_string(""),
    };

    let len = val.len() as i64;
    let s = if start < 0 { (len + start).max(0) as usize } else { start as usize };
    let e = if end < 0 { (len + end).max(0) as usize } else { end.min(len - 1) as usize };
    if s > e || s >= val.len() {
        return resp::bulk_string("");
    }
    resp::bulk_string(&val[s..=e.min(val.len() - 1)])
}

// ---------------------------------------------------------------------------
// Key commands
// ---------------------------------------------------------------------------

fn cmd_del(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    let mut count = 0i64;
    for arg in args {
        if let Some(key) = arg.as_str() {
            // Delete from _kv
            if let Ok(n) = db.delete(KV_COLLECTION, &json!({"_key": key})) {
                count += n as i64;
            }
            // Also delete from hash/list/set collections
            let _ = db.delete("_hash", &json!({"_key": key}));
            let _ = db.delete("_list", &json!({"_key": key}));
            let _ = db.delete("_set", &json!({"_key": key}));
        }
    }
    resp::integer(count)
}

fn cmd_exists(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    let mut count = 0i64;
    for arg in args {
        if let Some(key) = arg.as_str() {
            if let Ok(Some(_)) = db.find_one(KV_COLLECTION, &json!({"_key": key})) {
                count += 1;
            }
        }
    }
    resp::integer(count)
}

fn cmd_expire(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
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

    // Get current value, delete, re-insert with TTL
    match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => {
            let value = doc.get("_value").cloned().unwrap_or(Value::Null);
            let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));
            let _ = db.insert(KV_COLLECTION, json!({"_key": key, "_value": value, "_ttl": seconds}));
            resp::integer(1)
        }
        _ => resp::integer(0),
    }
}

fn cmd_pexpire(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
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

    match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => {
            let value = doc.get("_value").cloned().unwrap_or(Value::Null);
            let secs = (ms + 999) / 1000;
            let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));
            let _ = db.insert(KV_COLLECTION, json!({"_key": key, "_value": value, "_ttl": secs}));
            resp::integer(1)
        }
        _ => resp::integer(0),
    }
}

fn cmd_expireat(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
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

    let now = now_ms() / 1000;
    if timestamp <= now {
        let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));
        return resp::integer(1);
    }
    let ttl = timestamp - now;

    match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => {
            let value = doc.get("_value").cloned().unwrap_or(Value::Null);
            let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));
            let _ = db.insert(KV_COLLECTION, json!({"_key": key, "_value": value, "_ttl": ttl}));
            resp::integer(1)
        }
        _ => resp::integer(0),
    }
}

fn cmd_persist(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'persist' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };

    match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => {
            let value = doc.get("_value").cloned().unwrap_or(Value::Null);
            let _ = db.delete(KV_COLLECTION, &json!({"_key": key}));
            let _ = db.insert(KV_COLLECTION, json!({"_key": key, "_value": value}));
            resp::integer(1)
        }
        _ => resp::integer(0),
    }
}

fn cmd_ttl(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'ttl' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(-2),
    };

    match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => {
            match doc.get("_ttl") {
                Some(Value::Number(n)) => {
                    // _ttl is set at insert time in seconds. We'd need _expires_at for accuracy.
                    // For now, return the stored TTL value (approximate).
                    resp::integer(n.as_i64().unwrap_or(-1))
                }
                _ => resp::integer(-1), // key exists but no TTL
            }
        }
        _ => resp::integer(-2), // key does not exist
    }
}

fn cmd_pttl(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'pttl' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(-2),
    };

    match db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        Ok(Some(doc)) => {
            match doc.get("_ttl") {
                Some(Value::Number(n)) => {
                    resp::integer(n.as_i64().unwrap_or(-1) * 1000)
                }
                _ => resp::integer(-1),
            }
        }
        _ => resp::integer(-2),
    }
}

fn cmd_type(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'type' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return RespValue::SimpleString("none".to_string()),
    };

    if let Ok(Some(_)) = db.find_one(KV_COLLECTION, &json!({"_key": key})) {
        return RespValue::SimpleString("string".to_string());
    }
    if let Ok(Some(_)) = db.find_one("_hash", &json!({"_key": key})) {
        return RespValue::SimpleString("hash".to_string());
    }
    if let Ok(Some(_)) = db.find_one("_list", &json!({"_key": key})) {
        return RespValue::SimpleString("list".to_string());
    }
    if let Ok(Some(_)) = db.find_one("_set", &json!({"_key": key})) {
        return RespValue::SimpleString("set".to_string());
    }
    RespValue::SimpleString("none".to_string())
}

fn cmd_keys(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    let pattern = args.first().and_then(|a| a.as_str()).unwrap_or("*");

    let docs = match db.find(KV_COLLECTION, &json!({})) {
        Ok(d) => d,
        Err(_) => return resp::array(vec![]),
    };

    let regex_pattern = glob_to_regex(pattern);
    let re = match regex::Regex::new(&regex_pattern) {
        Ok(r) => r,
        Err(_) => return resp::array(vec![]),
    };

    let keys: Vec<RespValue> = docs
        .iter()
        .filter_map(|doc| doc.get("_key").and_then(|v| v.as_str()))
        .filter(|k| re.is_match(k))
        .map(|k| resp::bulk_string(k))
        .collect();

    resp::array(keys)
}

fn cmd_rename(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
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

    match db.find_one(KV_COLLECTION, &json!({"_key": old_key})) {
        Ok(Some(doc)) => {
            let value = doc.get("_value").cloned().unwrap_or(Value::Null);
            let _ = db.delete(KV_COLLECTION, &json!({"_key": old_key}));
            let _ = db.delete(KV_COLLECTION, &json!({"_key": new_key}));
            let _ = db.insert(KV_COLLECTION, json!({"_key": new_key, "_value": value}));
            resp::ok()
        }
        _ => resp::err("no such key"),
    }
}

fn cmd_randomkey(db: &Arc<OxiDb>) -> RespValue {
    match db.find(KV_COLLECTION, &json!({})) {
        Ok(docs) if !docs.is_empty() => {
            let idx = now_ms() as usize % docs.len();
            match docs[idx].get("_key").and_then(|v| v.as_str()) {
                Some(k) => resp::bulk_string(k),
                None => resp::null(),
            }
        }
        _ => resp::null(),
    }
}

fn cmd_dbsize(db: &Arc<OxiDb>) -> RespValue {
    let count = db.find(KV_COLLECTION, &json!({})).map(|d| d.len()).unwrap_or(0);
    resp::integer(count as i64)
}

fn cmd_flushdb(db: &Arc<OxiDb>) -> RespValue {
    let _ = db.drop_collection(KV_COLLECTION);
    let _ = db.drop_collection("_hash");
    let _ = db.drop_collection("_list");
    let _ = db.drop_collection("_set");
    resp::ok()
}

fn cmd_scan(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    // Simplified SCAN: always returns cursor "0" (complete) with all matching keys
    let _cursor = args.first().and_then(|a| a.as_str()).unwrap_or("0");

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

    let keys_resp = cmd_keys(db, &[resp::bulk_string(pattern)]);
    resp::array(vec![resp::bulk_string("0"), keys_resp])
}

// ---------------------------------------------------------------------------
// Hash commands
// ---------------------------------------------------------------------------

const HASH_COLLECTION: &str = "_hash";

fn get_hash_doc(db: &Arc<OxiDb>, key: &str) -> Option<Value> {
    db.find_one(HASH_COLLECTION, &json!({"_key": key})).ok().flatten()
}

fn cmd_hset(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return resp::err("wrong number of arguments for 'hset' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::err("invalid key"),
    };

    let mut doc = get_hash_doc(db, key).unwrap_or_else(|| json!({"_key": key}));
    let obj = doc.as_object_mut().unwrap();
    let mut added = 0i64;

    for pair in args[1..].chunks(2) {
        let field = match pair[0].as_str() {
            Some(f) => f,
            None => continue,
        };
        let value = pair[1].as_str().unwrap_or("").to_string();
        if !obj.contains_key(field) {
            added += 1;
        }
        obj.insert(field.to_string(), Value::String(value));
    }

    // Remove internal OxiDB fields before re-insert
    let key_str = key.to_string();
    obj.remove("_id");
    obj.remove("_version");
    let _ = db.delete(HASH_COLLECTION, &json!({"_key": key_str}));
    let _ = db.insert(HASH_COLLECTION, doc);

    resp::integer(added)
}

fn cmd_hget(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'hget' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::null(),
    };
    let field = match args[1].as_str() {
        Some(f) => f,
        None => return resp::null(),
    };

    match get_hash_doc(db, key) {
        Some(doc) => match doc.get(field) {
            Some(Value::String(s)) => resp::bulk_string(s),
            Some(Value::Number(n)) => resp::bulk_string(&n.to_string()),
            _ => resp::null(),
        },
        None => resp::null(),
    }
}

fn cmd_hmget(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'hmget' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::array(args[1..].iter().map(|_| resp::null()).collect()),
    };

    let doc = get_hash_doc(db, key);
    let results: Vec<RespValue> = args[1..]
        .iter()
        .map(|arg| {
            let field = arg.as_str().unwrap_or("");
            match &doc {
                Some(d) => match d.get(field) {
                    Some(Value::String(s)) => resp::bulk_string(s),
                    Some(Value::Number(n)) => resp::bulk_string(&n.to_string()),
                    _ => resp::null(),
                },
                None => resp::null(),
            }
        })
        .collect();
    resp::array(results)
}

fn cmd_hdel(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'hdel' command");
    }
    let key = match args[0].as_str() {
        Some(k) => k,
        None => return resp::integer(0),
    };

    let mut doc = match get_hash_doc(db, key) {
        Some(d) => d,
        None => return resp::integer(0),
    };

    let obj = doc.as_object_mut().unwrap();
    let mut removed = 0i64;
    for arg in &args[1..] {
        if let Some(field) = arg.as_str() {
            if obj.remove(field).is_some() {
                removed += 1;
            }
        }
    }

    obj.remove("_id");
    obj.remove("_version");
    let _ = db.delete(HASH_COLLECTION, &json!({"_key": key}));
    let _ = db.insert(HASH_COLLECTION, doc);
    resp::integer(removed)
}

fn cmd_hexists(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'hexists' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let field = args[1].as_str().unwrap_or("");

    match get_hash_doc(db, key) {
        Some(doc) => {
            if doc.get(field).is_some() { resp::integer(1) } else { resp::integer(0) }
        }
        None => resp::integer(0),
    }
}

fn cmd_hgetall(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'hgetall' command");
    }
    let key = args[0].as_str().unwrap_or("");

    match get_hash_doc(db, key) {
        Some(doc) => {
            let obj = doc.as_object().unwrap();
            let mut items = Vec::new();
            for (k, v) in obj {
                if k.starts_with('_') { continue; } // skip internal fields
                items.push(resp::bulk_string(k));
                match v {
                    Value::String(s) => items.push(resp::bulk_string(s)),
                    Value::Number(n) => items.push(resp::bulk_string(&n.to_string())),
                    _ => items.push(resp::bulk_string(&v.to_string())),
                }
            }
            resp::array(items)
        }
        None => resp::array(vec![]),
    }
}

fn cmd_hkeys(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'hkeys' command");
    }
    let key = args[0].as_str().unwrap_or("");

    match get_hash_doc(db, key) {
        Some(doc) => {
            let keys: Vec<RespValue> = doc
                .as_object()
                .unwrap()
                .keys()
                .filter(|k| !k.starts_with('_'))
                .map(|k| resp::bulk_string(k))
                .collect();
            resp::array(keys)
        }
        None => resp::array(vec![]),
    }
}

fn cmd_hvals(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'hvals' command");
    }
    let key = args[0].as_str().unwrap_or("");

    match get_hash_doc(db, key) {
        Some(doc) => {
            let vals: Vec<RespValue> = doc
                .as_object()
                .unwrap()
                .iter()
                .filter(|(k, _)| !k.starts_with('_'))
                .map(|(_, v)| match v {
                    Value::String(s) => resp::bulk_string(s),
                    Value::Number(n) => resp::bulk_string(&n.to_string()),
                    _ => resp::bulk_string(&v.to_string()),
                })
                .collect();
            resp::array(vals)
        }
        None => resp::array(vec![]),
    }
}

fn cmd_hlen(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'hlen' command");
    }
    let key = args[0].as_str().unwrap_or("");

    match get_hash_doc(db, key) {
        Some(doc) => {
            let count = doc.as_object().unwrap().keys().filter(|k| !k.starts_with('_')).count();
            resp::integer(count as i64)
        }
        None => resp::integer(0),
    }
}

fn cmd_hincrby(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
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

    let mut doc = get_hash_doc(db, key).unwrap_or_else(|| json!({"_key": key}));
    let obj = doc.as_object_mut().unwrap();
    let current: i64 = obj.get(field).and_then(|v| {
        v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    }).unwrap_or(0);
    let new_val = current + delta;
    obj.insert(field.to_string(), json!(new_val.to_string()));
    obj.remove("_id");
    obj.remove("_version");

    let _ = db.delete(HASH_COLLECTION, &json!({"_key": key}));
    let _ = db.insert(HASH_COLLECTION, doc);
    resp::integer(new_val)
}

fn cmd_hsetnx(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'hsetnx' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let field = args[1].as_str().unwrap_or("");

    if let Some(doc) = get_hash_doc(db, key) {
        if doc.get(field).is_some() {
            return resp::integer(0);
        }
    }
    cmd_hset(db, args);
    resp::integer(1)
}

// ---------------------------------------------------------------------------
// List commands
// ---------------------------------------------------------------------------

const LIST_COLLECTION: &str = "_list";

fn get_list_doc(db: &Arc<OxiDb>, key: &str) -> Option<Value> {
    db.find_one(LIST_COLLECTION, &json!({"_key": key})).ok().flatten()
}

fn get_list_items(doc: &Value) -> Vec<Value> {
    doc.get("_items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
}

fn save_list(db: &Arc<OxiDb>, key: &str, items: Vec<Value>) {
    let _ = db.delete(LIST_COLLECTION, &json!({"_key": key}));
    if !items.is_empty() {
        let _ = db.insert(LIST_COLLECTION, json!({"_key": key, "_items": items}));
    }
}

fn cmd_lpush(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'lpush' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let mut items = get_list_doc(db, key).map(|d| get_list_items(&d)).unwrap_or_default();

    for arg in args[1..].iter().rev() {
        let val = arg.as_str().unwrap_or("").to_string();
        items.insert(0, Value::String(val));
    }

    let len = items.len() as i64;
    save_list(db, key, items);
    resp::integer(len)
}

fn cmd_rpush(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'rpush' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let mut items = get_list_doc(db, key).map(|d| get_list_items(&d)).unwrap_or_default();

    for arg in &args[1..] {
        let val = arg.as_str().unwrap_or("").to_string();
        items.push(Value::String(val));
    }

    let len = items.len() as i64;
    save_list(db, key, items);
    resp::integer(len)
}

fn cmd_lpop(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'lpop' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let mut items = match get_list_doc(db, key) {
        Some(d) => get_list_items(&d),
        None => return resp::null(),
    };

    if items.is_empty() {
        return resp::null();
    }
    let val = items.remove(0);
    save_list(db, key, items);
    match val {
        Value::String(s) => resp::bulk_string(&s),
        _ => resp::bulk_string(&val.to_string()),
    }
}

fn cmd_rpop(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'rpop' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let mut items = match get_list_doc(db, key) {
        Some(d) => get_list_items(&d),
        None => return resp::null(),
    };

    if items.is_empty() {
        return resp::null();
    }
    let val = items.pop().unwrap();
    save_list(db, key, items);
    match val {
        Value::String(s) => resp::bulk_string(&s),
        _ => resp::bulk_string(&val.to_string()),
    }
}

fn cmd_llen(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'llen' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let len = get_list_doc(db, key).map(|d| get_list_items(&d).len()).unwrap_or(0);
    resp::integer(len as i64)
}

fn cmd_lrange(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 3 {
        return resp::err("wrong number of arguments for 'lrange' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let start: i64 = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);
    let stop: i64 = args[2].as_str().and_then(|s| s.parse().ok()).unwrap_or(-1);

    let items = get_list_doc(db, key).map(|d| get_list_items(&d)).unwrap_or_default();
    let len = items.len() as i64;

    let s = if start < 0 { (len + start).max(0) as usize } else { start as usize };
    let e = if stop < 0 { (len + stop).max(0) as usize } else { stop as usize };

    if s >= items.len() || s > e {
        return resp::array(vec![]);
    }
    let end = (e + 1).min(items.len());

    let result: Vec<RespValue> = items[s..end]
        .iter()
        .map(|v| match v {
            Value::String(s) => resp::bulk_string(s),
            _ => resp::bulk_string(&v.to_string()),
        })
        .collect();
    resp::array(result)
}

fn cmd_lindex(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'lindex' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let index: i64 = args[1].as_str().and_then(|s| s.parse().ok()).unwrap_or(0);

    let items = get_list_doc(db, key).map(|d| get_list_items(&d)).unwrap_or_default();
    let len = items.len() as i64;
    let idx = if index < 0 { (len + index) as usize } else { index as usize };

    match items.get(idx) {
        Some(Value::String(s)) => resp::bulk_string(s),
        Some(v) => resp::bulk_string(&v.to_string()),
        None => resp::null(),
    }
}

// ---------------------------------------------------------------------------
// Set commands
// ---------------------------------------------------------------------------

const SET_COLLECTION: &str = "_set";

fn get_set_doc(db: &Arc<OxiDb>, key: &str) -> Option<Value> {
    db.find_one(SET_COLLECTION, &json!({"_key": key})).ok().flatten()
}

fn get_set_members(doc: &Value) -> Vec<String> {
    doc.get("_members")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

fn save_set(db: &Arc<OxiDb>, key: &str, members: Vec<String>) {
    let _ = db.delete(SET_COLLECTION, &json!({"_key": key}));
    if !members.is_empty() {
        let _ = db.insert(SET_COLLECTION, json!({"_key": key, "_members": members}));
    }
}

fn cmd_sadd(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'sadd' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let mut members = get_set_doc(db, key).map(|d| get_set_members(&d)).unwrap_or_default();
    let mut added = 0i64;

    for arg in &args[1..] {
        let val = arg.as_str().unwrap_or("").to_string();
        if !members.contains(&val) {
            members.push(val);
            added += 1;
        }
    }

    save_set(db, key, members);
    resp::integer(added)
}

fn cmd_srem(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'srem' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let mut members = get_set_doc(db, key).map(|d| get_set_members(&d)).unwrap_or_default();
    let mut removed = 0i64;

    for arg in &args[1..] {
        let val = arg.as_str().unwrap_or("");
        let before = members.len();
        members.retain(|m| m != val);
        if members.len() < before {
            removed += 1;
        }
    }

    save_set(db, key, members);
    resp::integer(removed)
}

fn cmd_smembers(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'smembers' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let members = get_set_doc(db, key).map(|d| get_set_members(&d)).unwrap_or_default();
    resp::array(members.into_iter().map(|m| resp::bulk_string(&m)).collect())
}

fn cmd_sismember(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.len() < 2 {
        return resp::err("wrong number of arguments for 'sismember' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let member = args[1].as_str().unwrap_or("");
    let members = get_set_doc(db, key).map(|d| get_set_members(&d)).unwrap_or_default();
    resp::integer(if members.contains(&member.to_string()) { 1 } else { 0 })
}

fn cmd_scard(db: &Arc<OxiDb>, args: &[RespValue]) -> RespValue {
    if args.is_empty() {
        return resp::err("wrong number of arguments for 'scard' command");
    }
    let key = args[0].as_str().unwrap_or("");
    let count = get_set_doc(db, key).map(|d| get_set_members(&d).len()).unwrap_or(0);
    resp::integer(count as i64)
}

// ---------------------------------------------------------------------------
// Server commands
// ---------------------------------------------------------------------------

fn cmd_info(db: &Arc<OxiDb>) -> RespValue {
    let mode = if db.is_in_memory() { "memory" } else { "file" };
    let cols = db.list_collections();
    let kv_count = db.find(KV_COLLECTION, &json!({})).map(|d| d.len()).unwrap_or(0);

    let info = format!(
        "# Server\r\n\
         oxidb_version:0.18.0\r\n\
         oxidb_mode:{mode}\r\n\
         redis_compat:true\r\n\
         \r\n\
         # Keyspace\r\n\
         db0:keys={kv_count},expires=0\r\n\
         collections:{}\r\n",
        cols.join(",")
    );
    resp::bulk_string(&info)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a Redis glob pattern to a regex.
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
