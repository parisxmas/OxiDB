//! OxiMem (Redis-compatible) browser backend. OxiMem listens on its own
//! port (RESP protocol), separate from the document/SQL TCP port, so it gets
//! its own managed connection here rather than reusing DbBackend.

use std::io::{BufRead, BufReader, Read as _, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::Duration;

use serde_json::{json, Value};
use tauri::State;

pub struct OxiMemConn {
    writer: TcpStream,
    reader: BufReader<TcpStream>,
    pub host: String,
    pub port: u16,
}

#[derive(Default)]
pub struct OxiMemState(pub Mutex<Option<OxiMemConn>>);

impl OxiMemConn {
    fn connect(host: &str, port: u16) -> Result<Self, String> {
        let writer = TcpStream::connect((host, port))
            .map_err(|e| format!("connect {host}:{port}: {e}"))?;
        writer.set_read_timeout(Some(Duration::from_secs(15))).ok();
        let reader = BufReader::new(
            writer.try_clone().map_err(|e| format!("clone stream: {e}"))?,
        );
        Ok(Self { writer, reader, host: host.into(), port })
    }

    /// Send one command (array of bulk strings) and parse the reply.
    fn command(&mut self, args: &[&str]) -> Result<Value, String> {
        let mut buf = format!("*{}\r\n", args.len());
        for a in args {
            buf.push_str(&format!("${}\r\n{}\r\n", a.len(), a));
        }
        self.writer
            .write_all(buf.as_bytes())
            .map_err(|e| format!("write: {e}"))?;
        self.writer.flush().map_err(|e| format!("flush: {e}"))?;
        parse_reply(&mut self.reader)
    }
}

/// Parse one RESP value into JSON. Errors (`-`) become Err.
fn parse_reply<R: BufRead>(r: &mut R) -> Result<Value, String> {
    let mut line = String::new();
    r.read_line(&mut line).map_err(|e| format!("read: {e}"))?;
    if line.is_empty() {
        return Err("connection closed".into());
    }
    let (tag, rest) = line.split_at(1);
    let rest = rest.trim_end_matches(['\r', '\n']);
    match tag {
        "+" => Ok(json!(rest)),
        "-" => Err(rest.to_string()),
        ":" => Ok(json!(rest.parse::<i64>().unwrap_or(0))),
        "$" => {
            let len: i64 = rest.parse().map_err(|_| "bad bulk length")?;
            if len < 0 {
                return Ok(Value::Null);
            }
            let mut data = vec![0u8; len as usize + 2]; // + CRLF
            r.read_exact(&mut data).map_err(|e| format!("read bulk: {e}"))?;
            data.truncate(len as usize);
            Ok(json!(String::from_utf8_lossy(&data).to_string()))
        }
        "*" => {
            let n: i64 = rest.parse().map_err(|_| "bad array length")?;
            if n < 0 {
                return Ok(Value::Null);
            }
            let mut arr = Vec::with_capacity(n as usize);
            for _ in 0..n {
                arr.push(parse_reply(r)?);
            }
            Ok(Value::Array(arr))
        }
        other => Err(format!("unknown RESP tag: {other:?}")),
    }
}

fn as_strings(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| a.iter().map(|x| x.as_str().unwrap_or("").to_string()).collect())
        .unwrap_or_default()
}

#[tauri::command]
pub fn oximem_connect(
    host: String,
    port: u16,
    state: State<'_, OxiMemState>,
) -> Result<Value, String> {
    let mut conn = OxiMemConn::connect(&host, port)?;
    conn.command(&["PING"])?;
    let db = conn.command(&["DBSIZE"]).ok();
    *state.0.lock().unwrap() = Some(conn);
    Ok(json!({ "connected": true, "detail": format!("{host}:{port}"), "dbsize": db }))
}

#[tauri::command]
pub fn oximem_disconnect(state: State<'_, OxiMemState>) {
    *state.0.lock().unwrap() = None;
}

#[tauri::command]
pub fn oximem_status(state: State<'_, OxiMemState>) -> Value {
    match &*state.0.lock().unwrap() {
        Some(c) => json!({ "connected": true, "detail": format!("{}:{}", c.host, c.port) }),
        None => json!({ "connected": false }),
    }
}

/// One SCAN step: returns `{cursor, keys: [name, ...]}`. Cursor "0" ends it.
#[tauri::command]
pub fn oximem_scan(
    cursor: String,
    pattern: String,
    state: State<'_, OxiMemState>,
) -> Result<Value, String> {
    let mut guard = state.0.lock().unwrap();
    let conn = guard.as_mut().ok_or("not connected")?;
    let pat = if pattern.is_empty() { "*" } else { &pattern };
    let reply = conn.command(&["SCAN", &cursor, "MATCH", pat, "COUNT", "300"])?;
    let arr = reply.as_array().ok_or("unexpected SCAN reply")?;
    let next = arr.first().and_then(|v| v.as_str()).unwrap_or("0").to_string();
    let keys = arr.get(1).map(as_strings).unwrap_or_default();
    Ok(json!({ "cursor": next, "keys": keys }))
}

/// Full detail for one key: type, ttl (seconds; -1 none, -2 missing), value.
#[tauri::command]
pub fn oximem_get(key: String, state: State<'_, OxiMemState>) -> Result<Value, String> {
    let mut guard = state.0.lock().unwrap();
    let conn = guard.as_mut().ok_or("not connected")?;
    let ty = conn.command(&["TYPE", &key])?;
    let ty = ty.as_str().unwrap_or("none").to_string();
    let ttl = conn.command(&["TTL", &key]).ok().and_then(|v| v.as_i64());
    let value = match ty.as_str() {
        "string" => json!({ "kind": "string", "value": conn.command(&["GET", &key])? }),
        "hash" => {
            let flat = as_strings(&conn.command(&["HGETALL", &key])?);
            let pairs: Vec<Value> = flat.chunks(2).map(|c| json!([c[0], c.get(1)])).collect();
            json!({ "kind": "hash", "value": pairs })
        }
        "list" => json!({ "kind": "list", "value": as_strings(&conn.command(&["LRANGE", &key, "0", "-1"])?) }),
        "set" => json!({ "kind": "set", "value": as_strings(&conn.command(&["SMEMBERS", &key])?) }),
        "zset" => {
            let flat = as_strings(&conn.command(&["ZRANGE", &key, "0", "-1", "WITHSCORES"])?);
            let pairs: Vec<Value> = flat.chunks(2).map(|c| json!([c[0], c.get(1)])).collect();
            json!({ "kind": "zset", "value": pairs })
        }
        _ => json!({ "kind": "none", "value": null }),
    };
    Ok(json!({ "type": ty, "ttl": ttl, "value": value }))
}

#[tauri::command]
pub fn oximem_set_string(
    key: String,
    value: String,
    state: State<'_, OxiMemState>,
) -> Result<Value, String> {
    let mut guard = state.0.lock().unwrap();
    let conn = guard.as_mut().ok_or("not connected")?;
    conn.command(&["SET", &key, &value])
}

#[tauri::command]
pub fn oximem_del(key: String, state: State<'_, OxiMemState>) -> Result<Value, String> {
    let mut guard = state.0.lock().unwrap();
    let conn = guard.as_mut().ok_or("not connected")?;
    conn.command(&["DEL", &key])
}

#[cfg(test)]
mod live {
    //! Live RESP round-trip against a running OxiMem port.
    //!   OXIDB_TEST_OXIMEM=127.0.0.1:6490 cargo test --lib oximem::live -- --ignored --nocapture
    use super::*;

    #[test]
    #[ignore]
    fn resp_roundtrip_all_types() {
        let addr = std::env::var("OXIDB_TEST_OXIMEM").unwrap();
        let (host, port) = addr.split_once(':').unwrap();
        let mut c = OxiMemConn::connect(host, port.parse().unwrap()).unwrap();

        // Seed one key of each type under a unique prefix.
        c.command(&["SET", "brt:str", "hello world"]).unwrap();
        c.command(&["HSET", "brt:hash", "a", "1", "b", "2"]).unwrap();
        c.command(&["RPUSH", "brt:list", "x", "y", "z"]).unwrap();
        c.command(&["SADD", "brt:set", "m1", "m2"]).unwrap();
        c.command(&["ZADD", "brt:zset", "1.5", "alpha", "2.5", "beta"]).unwrap();

        // SCAN finds them.
        let reply = c.command(&["SCAN", "0", "MATCH", "brt:*", "COUNT", "100"]).unwrap();
        let found = as_strings(&reply.as_array().unwrap()[1]);
        for k in ["brt:str", "brt:hash", "brt:list", "brt:set", "brt:zset"] {
            assert!(found.contains(&k.to_string()), "SCAN missing {k}: {found:?}");
        }

        // TYPE + value shape per key.
        assert_eq!(c.command(&["TYPE", "brt:str"]).unwrap().as_str(), Some("string"));
        assert_eq!(c.command(&["GET", "brt:str"]).unwrap().as_str(), Some("hello world"));
        assert_eq!(as_strings(&c.command(&["HGETALL", "brt:hash"]).unwrap()).len(), 4);
        assert_eq!(as_strings(&c.command(&["LRANGE", "brt:list", "0", "-1"]).unwrap()), vec!["x", "y", "z"]);
        assert_eq!(as_strings(&c.command(&["SMEMBERS", "brt:set"]).unwrap()).len(), 2);
        let z = as_strings(&c.command(&["ZRANGE", "brt:zset", "0", "-1", "WITHSCORES"]).unwrap());
        assert_eq!(z, vec!["alpha", "1.5", "beta", "2.5"]);

        // DEL cleans up.
        for k in ["brt:str", "brt:hash", "brt:list", "brt:set", "brt:zset"] {
            c.command(&["DEL", k]).unwrap();
        }
        println!("OK — all RESP types round-tripped");
    }
}
