//! A native OxiWire client for OxiDB: length-prefixed JSON over TCP with
//! SCRAM-SHA-256 authentication — the protocol the first-party clients speak.
//! Blocking and dependency-light, so Rust services (e.g. `oxibase`) can talk to
//! `oxidb-server` over its native protocol instead of REST.
//!
//! ```no_run
//! let mut c = oxidb_client::Client::connect("127.0.0.1:4444").map_err(|e| e.to_string())?;
//! c.authenticate("admin", "secret")?;               // SCRAM (skip if auth is off)
//! c.create_database("oxibase")?;
//! c.insert("oxibase", "projects", &serde_json::json!({ "ref": "abc" }))?;
//! let rows = c.find("oxibase", "projects", &serde_json::json!({ "ref": "abc" }))?;
//! # Ok::<(), String>(())
//! ```

pub mod scram;

use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use serde_json::{Value, json};

use scram::{ScramClient, verify_server_final};

const MAX_MSG: usize = 16 * 1024 * 1024;

/// A single connection to an OxiDB wire server.
pub struct Client {
    stream: TcpStream,
}

impl Client {
    /// Connect (no auth yet). `addr` is `host:port`.
    pub fn connect(addr: &str) -> io::Result<Self> {
        let stream = TcpStream::connect(addr)?;
        stream.set_nodelay(true).ok();
        stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
        Ok(Self { stream })
    }

    /// Send one request and return its `data` payload, mapping the wire's
    /// `{"ok":false,"error":…}` to `Err`.
    pub fn call(&mut self, request: &Value) -> Result<Value, String> {
        let bytes = serde_json::to_vec(request).map_err(|e| e.to_string())?;
        write_message(&mut self.stream, &bytes).map_err(|e| e.to_string())?;
        let resp = read_message(&mut self.stream).map_err(|e| e.to_string())?;
        let v: Value = serde_json::from_slice(&resp).map_err(|e| e.to_string())?;
        if v.get("ok").and_then(|b| b.as_bool()).unwrap_or(false) {
            Ok(v.get("data").cloned().unwrap_or(Value::Null))
        } else {
            Err(v
                .get("error")
                .and_then(|e| e.as_str())
                .unwrap_or("unknown wire error")
                .to_string())
        }
    }

    /// Authenticate with SCRAM-SHA-256. Skip this when the server has auth
    /// disabled (the connection is then anonymous-admin).
    pub fn authenticate(&mut self, username: &str, password: &str) -> Result<(), String> {
        let mut sc = ScramClient::new(username, password);
        let client_first = sc.client_first();
        let r1 = self.call(&json!({ "cmd": "authenticate", "payload": client_first }))?;
        let server_first = r1
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or("no server-first payload")?
            .to_string();
        let (client_final, expected_sig) = sc.client_final(&server_first)?;
        let r2 = self.call(&json!({ "cmd": "authenticate_continue", "payload": client_final }))?;
        let server_final = r2
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or("no server-final payload")?;
        verify_server_final(server_final, &expected_sig)
    }

    // ── Convenience wrappers for the commands oxibase needs ──────────────

    pub fn create_database(&mut self, name: &str) -> Result<(), String> {
        self.call(&json!({ "cmd": "create_database", "name": name }))
            .map(|_| ())
    }

    pub fn drop_database(&mut self, name: &str) -> Result<(), String> {
        self.call(&json!({ "cmd": "drop_database", "name": name }))
            .map(|_| ())
    }

    /// Insert one document into `db.collection`; returns the wire response
    /// (the assigned id).
    pub fn insert(&mut self, db: &str, collection: &str, doc: &Value) -> Result<Value, String> {
        self.call(&json!({ "cmd": "insert", "db": db, "collection": collection, "doc": doc }))
    }

    /// Find documents in `db.collection` matching `query`.
    pub fn find(
        &mut self,
        db: &str,
        collection: &str,
        query: &Value,
    ) -> Result<Vec<Value>, String> {
        let data = self
            .call(&json!({ "cmd": "find", "db": db, "collection": collection, "query": query }))?;
        Ok(data.as_array().cloned().unwrap_or_default())
    }

    pub fn update(
        &mut self,
        db: &str,
        collection: &str,
        query: &Value,
        update: &Value,
    ) -> Result<(), String> {
        self.call(&json!({
            "cmd": "update", "db": db, "collection": collection,
            "query": query, "update": update,
        }))
        .map(|_| ())
    }

    pub fn delete(&mut self, db: &str, collection: &str, query: &Value) -> Result<(), String> {
        self.call(&json!({ "cmd": "delete", "db": db, "collection": collection, "query": query }))
            .map(|_| ())
    }
}

// ── Framing: [u32 LE length][payload], matching oxidb-server's protocol ─────

fn read_message(reader: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_MSG {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "message too large",
        ));
    }
    let mut buf = vec![0u8; len];
    reader.read_exact(&mut buf)?;
    Ok(buf)
}

fn write_message(writer: &mut impl Write, data: &[u8]) -> io::Result<()> {
    writer.write_all(&(data.len() as u32).to_le_bytes())?;
    writer.write_all(data)?;
    writer.flush()
}
