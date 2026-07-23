//! A native OxiWire client for OxiDB: length-prefixed JSON over TCP with
//! SCRAM-SHA-256 authentication — the protocol the first-party clients speak.
//! Blocking and dependency-light, so Rust services (e.g. `oxibase`) can talk to
//! `oxidb-server` over its native protocol instead of REST.
//!
//! A [`Pool`] keeps a set of authenticated connections for reuse; it discards a
//! connection whose response fails (it may be corrupt) and transparently
//! re-dials a connection that was closed while idle (the request was never
//! sent, so the retry is safe).
//!
//! ```no_run
//! let pool = oxidb_client::Pool::new("127.0.0.1:4444".into(), Some("admin".into()), Some("secret".into()), 8);
//! pool.with(|c| c.create_database("oxibase"))?;
//! pool.with(|c| c.insert("oxibase", "projects", &serde_json::json!({ "ref": "abc" })))?;
//! # Ok::<(), oxidb_client::Error>(())
//! ```

pub mod scram;

use std::fmt;
use std::io::{self, Read, Write};
use std::net::TcpStream;
use std::sync::Mutex;
use std::time::Duration;

use serde_json::{Value, json};

use scram::{ScramClient, verify_server_final};

const MAX_MSG: usize = 16 * 1024 * 1024;

/// A client error, classified by whether a retry is safe and whether the
/// connection is still usable.
#[derive(Debug)]
pub enum Error {
    /// The request could not be sent (the socket was dead on write). Nothing
    /// executed, so a retry on a fresh connection is safe.
    Connect(String),
    /// The request was sent but the response failed. It may have executed —
    /// **do not** retry. The connection is discarded.
    Io(String),
    /// The server returned an error response. The connection is healthy.
    Wire(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connect(m) | Error::Io(m) | Error::Wire(m) => write!(f, "{m}"),
        }
    }
}
impl std::error::Error for Error {}

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
    /// `{"ok":false,"error":…}` to [`Error::Wire`].
    pub fn call(&mut self, request: &Value) -> Result<Value, Error> {
        let bytes = serde_json::to_vec(request).map_err(|e| Error::Io(e.to_string()))?;
        // A write failure means the request never reached the server.
        write_message(&mut self.stream, &bytes).map_err(|e| Error::Connect(e.to_string()))?;
        // Past this point the request may have executed; read failures are Io.
        let resp = read_message(&mut self.stream).map_err(|e| Error::Io(e.to_string()))?;
        let v: Value = serde_json::from_slice(&resp).map_err(|e| Error::Io(e.to_string()))?;
        if v.get("ok").and_then(|b| b.as_bool()).unwrap_or(false) {
            Ok(v.get("data").cloned().unwrap_or(Value::Null))
        } else {
            Err(Error::Wire(
                v.get("error")
                    .and_then(|e| e.as_str())
                    .unwrap_or("unknown wire error")
                    .to_string(),
            ))
        }
    }

    /// Authenticate with SCRAM-SHA-256. Skip this when the server has auth
    /// disabled (the connection is then anonymous-admin).
    pub fn authenticate(&mut self, username: &str, password: &str) -> Result<(), Error> {
        let mut sc = ScramClient::new(username, password);
        let client_first = sc.client_first();
        let r1 = self.call(&json!({ "cmd": "authenticate", "payload": client_first }))?;
        let server_first = r1
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Wire("no server-first payload".into()))?
            .to_string();
        let (client_final, expected_sig) = sc.client_final(&server_first).map_err(Error::Wire)?;
        let r2 = self.call(&json!({ "cmd": "authenticate_continue", "payload": client_final }))?;
        let server_final = r2
            .get("payload")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::Wire("no server-final payload".into()))?;
        verify_server_final(server_final, &expected_sig).map_err(Error::Wire)
    }

    // ── Convenience wrappers for the commands oxibase needs ──────────────

    pub fn create_database(&mut self, name: &str) -> Result<(), Error> {
        self.call(&json!({ "cmd": "create_database", "name": name }))
            .map(|_| ())
    }

    pub fn drop_database(&mut self, name: &str) -> Result<(), Error> {
        self.call(&json!({ "cmd": "drop_database", "name": name }))
            .map(|_| ())
    }

    pub fn insert(&mut self, db: &str, collection: &str, doc: &Value) -> Result<Value, Error> {
        self.call(&json!({ "cmd": "insert", "db": db, "collection": collection, "doc": doc }))
    }

    pub fn find(&mut self, db: &str, collection: &str, query: &Value) -> Result<Vec<Value>, Error> {
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
    ) -> Result<(), Error> {
        self.call(&json!({
            "cmd": "update", "db": db, "collection": collection, "query": query, "update": update,
        }))
        .map(|_| ())
    }

    pub fn delete(&mut self, db: &str, collection: &str, query: &Value) -> Result<(), Error> {
        self.call(&json!({ "cmd": "delete", "db": db, "collection": collection, "query": query }))
            .map(|_| ())
    }
}

// ---------------------------------------------------------------------------
// Connection pool
// ---------------------------------------------------------------------------

/// A pool of authenticated connections to one wire server. Cheap to clone the
/// config; connections are created lazily and reused up to `max_idle`.
pub struct Pool {
    addr: String,
    user: Option<String>,
    password: Option<String>,
    max_idle: usize,
    idle: Mutex<Vec<Client>>,
}

impl Pool {
    pub fn new(
        addr: String,
        user: Option<String>,
        password: Option<String>,
        max_idle: usize,
    ) -> Self {
        Self {
            addr,
            user,
            password,
            max_idle: max_idle.max(1),
            idle: Mutex::new(Vec::new()),
        }
    }

    /// Dial + authenticate a fresh connection.
    fn dial(&self) -> Result<Client, Error> {
        let mut c = Client::connect(&self.addr).map_err(|e| Error::Connect(e.to_string()))?;
        if let (Some(u), Some(p)) = (&self.user, &self.password) {
            c.authenticate(u, p)?;
        }
        Ok(c)
    }

    /// Check out an idle connection or dial a new one.
    fn checkout(&self) -> Result<Client, Error> {
        if let Some(c) = self.idle.lock().unwrap().pop() {
            return Ok(c);
        }
        self.dial()
    }

    /// Return a healthy connection to the pool (dropped if the pool is full).
    fn checkin(&self, c: Client) {
        let mut idle = self.idle.lock().unwrap();
        if idle.len() < self.max_idle {
            idle.push(c);
        }
    }

    /// Run `f` on a pooled connection. On success the connection returns to the
    /// pool. A [`Error::Wire`] keeps the connection (it is healthy); an
    /// [`Error::Io`] discards it (no retry — the op may have run); an
    /// [`Error::Connect`] discards it and retries once on a fresh connection
    /// (the request was never sent, so this is safe — e.g. an idle-closed
    /// pooled connection).
    pub fn with<F, R>(&self, mut f: F) -> Result<R, Error>
    where
        F: FnMut(&mut Client) -> Result<R, Error>,
    {
        let mut last: Option<Error> = None;
        for _ in 0..2 {
            let mut c = match self.checkout() {
                Ok(c) => c,
                Err(e) => {
                    last = Some(e);
                    continue;
                }
            };
            match f(&mut c) {
                Ok(r) => {
                    self.checkin(c);
                    return Ok(r);
                }
                Err(Error::Wire(m)) => {
                    self.checkin(c);
                    return Err(Error::Wire(m));
                }
                Err(Error::Io(m)) => return Err(Error::Io(m)), // discard c, no retry
                Err(Error::Connect(m)) => last = Some(Error::Connect(m)), // discard c, retry
            }
        }
        Err(last.unwrap_or_else(|| Error::Connect("pool exhausted".into())))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_is_the_message() {
        assert_eq!(Error::Wire("boom".into()).to_string(), "boom");
        assert_eq!(Error::Connect("down".into()).to_string(), "down");
    }

    #[test]
    fn pool_clamps_max_idle_to_at_least_one() {
        let p = Pool::new("127.0.0.1:1".into(), None, None, 0);
        assert_eq!(p.max_idle, 1);
    }
}
