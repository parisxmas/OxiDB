//! Minimal OxiDB-to-OxiDB client used by the linked-collection
//! (FDW) proxy. v2b: connection pool + SCRAM-SHA-256 authentication
//! passthrough. The pool is keyed on `(host, port, Option<username>)`
//! so an authed conn for user X is never reused by a request that
//! wants to be user Y (or no auth at all).
//!
//! Wire format matches what `oxidb-server::protocol` reads: a 4-byte
//! little-endian length prefix followed by a JSON payload.
//!
//! Auth flow when a link URL carries userinfo:
//!     1. Dial fresh TCP conn.
//!     2. Send `authenticate` with client-first SCRAM message.
//!     3. Receive server-first; send `authenticate_continue` with the
//!        client proof.
//!     4. Receive `v=<server_signature>`; verify it matches what we
//!        derived from the plaintext. On mismatch we drop the conn —
//!        a server that can't prove knowledge of the verifier is not
//!        a server we want to forward queries to.
//!     5. Hand the now-authed stream to `round_trip` for the user's
//!        actual command, then return it to the user-keyed pool.

use serde_json::{json, Value};
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use oxidb::links::ParsedRemote;

use crate::protocol::{read_message, write_message};
use crate::scram_client::{verify_server_final, ScramClient};

/// Default connect + read timeout for proxy calls. Linked collections
/// are usually on a fast LAN, but we don't want a wedged remote to
/// hang a request forever — the local query just fails fast and
/// surfaces the error to the user.
const PROXY_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum number of idle connections kept warm per remote endpoint.
/// Above this, returned connections are dropped (closed). Picked to
/// match what a single OxiDB-on-OxiDB linked-collection workload
/// realistically benefits from before the per-conn TCP cost
/// dominates fan-out budget.
const MAX_IDLE_PER_REMOTE: usize = 8;

/// How long an idle connection stays warm before being discarded.
/// 60s catches typical LB / NAT idle-timeout values without keeping
/// dead conns indefinitely.
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Execute one command against a remote OxiDB and return the raw
/// response payload (`{"ok": true, "data": ...}` or
/// `{"ok": false, "error": "..."}`). The caller passes the already-
/// rewritten command — i.e. with `collection` set to the REMOTE
/// collection name, not the local link alias.
///
/// Pooling: the first call dials, subsequent calls to the same
/// (host, port, user) reuse an idle conn from the pool. A pooled
/// conn that fails write or read is dropped and the request retried
/// ONCE with a fresh dial — covers the standard "server kicked the
/// conn while we held it idle" failure mode without surfacing it to
/// the caller.
///
/// Authentication: if `remote.user` is Some, the fresh-dial path
/// performs a full SCRAM exchange before the payload round-trip.
/// Pooled conns are user-keyed so a take() never returns an
/// unauthenticated conn for a user-bound request (or vice versa).
pub fn proxy_command(remote: &ParsedRemote, command: &Value) -> Result<Value, String> {
    let payload = serde_json::to_vec(command)
        .map_err(|e| format!("encode command: {}", e))?;

    let user_key = remote.user.as_deref();

    // First attempt — pooled conn if any.
    if let Some(stream) = pool().take(&remote.host, remote.port, user_key) {
        match round_trip(&stream, &payload) {
            Ok(resp) => {
                pool().give_back(&remote.host, remote.port, user_key, stream);
                return Ok(resp);
            }
            // Pooled conn went bad. Don't surface the error — fall
            // through to a fresh dial below.
            Err(_) => {}
        }
    }

    // Fresh dial — and if THIS fails, we surface it.
    let stream = dial(remote)?;

    // Authenticate if the URL carried userinfo. Failure must NOT
    // pool the conn — half-authed sockets cannot be reused safely.
    if let (Some(user), Some(password)) = (remote.user.as_deref(), remote.password.as_deref()) {
        authenticate(&stream, user, password)?;
    } else if remote.user.is_some() {
        return Err("link URL has user but no password — SCRAM needs both".into());
    }

    match round_trip(&stream, &payload) {
        Ok(resp) => {
            pool().give_back(&remote.host, remote.port, user_key, stream);
            Ok(resp)
        }
        Err(e) => Err(e),
    }
}

/// authenticate runs one SCRAM-SHA-256 exchange on the given stream.
/// On success the stream is left in an authenticated session state on
/// the remote (the server's Session.is_authenticated() returns true)
/// and the caller can issue any RBAC-permitted command. On failure
/// the caller MUST drop the stream — the SCRAM state machine on the
/// remote may have leftover partial state that would confuse a
/// subsequent attempt on the same socket.
pub fn authenticate(stream: &TcpStream, username: &str, password: &str) -> Result<(), String> {
    let mut client = ScramClient::new(username, password);

    // Step 1: client-first → server-first.
    let client_first_msg = client.client_first();
    let req1 = json!({
        "command": "authenticate",
        "payload": client_first_msg,
    });
    let req1_bytes = serde_json::to_vec(&req1)
        .map_err(|e| format!("encode authenticate: {}", e))?;
    let resp1 = round_trip(stream, &req1_bytes)?;
    let server_first = extract_payload(&resp1, "authenticate")?;

    // Step 2: server-first → client-final → server-final.
    let (client_final_msg, expected_sig) = client
        .client_final(&server_first)
        .map_err(|e| format!("SCRAM client_final: {}", e))?;
    let req2 = json!({
        "command": "authenticate_continue",
        "payload": client_final_msg,
    });
    let req2_bytes = serde_json::to_vec(&req2)
        .map_err(|e| format!("encode authenticate_continue: {}", e))?;
    let resp2 = round_trip(stream, &req2_bytes)?;
    let server_final = extract_payload(&resp2, "authenticate_continue")?;

    verify_server_final(&server_final, &expected_sig)
        .map_err(|e| format!("SCRAM verify_server_final: {}", e))?;
    Ok(())
}

/// extract_payload pulls `data.payload` from the standard
/// `{"ok": true, "data": {"payload": "...", "done": bool}}` envelope,
/// or returns the embedded error string when `ok == false`. Used for
/// the SCRAM round-trips — every other command goes through
/// round_trip directly and the caller inspects the envelope itself.
fn extract_payload(resp: &Value, step: &str) -> Result<String, String> {
    if resp.get("ok").and_then(|v| v.as_bool()) != Some(true) {
        let err = resp
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown error");
        return Err(format!("{} rejected by remote: {}", step, err));
    }
    let payload = resp
        .pointer("/data/payload")
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("{} response missing data.payload: {}", step, resp))?;
    Ok(payload.to_string())
}

/// dial opens a new TCP connection to the remote, applies the proxy
/// timeouts, and returns the stream. The hot-path constructor for
/// pool misses; the pool gets handed the stream right after a
/// successful round-trip.
fn dial(remote: &ParsedRemote) -> Result<TcpStream, String> {
    let addr_str = format!("{}:{}", remote.host, remote.port);
    let stream = TcpStream::connect_timeout(&resolve(&addr_str)?, PROXY_TIMEOUT)
        .map_err(|e| format!("connect {}: {}", addr_str, e))?;
    stream
        .set_read_timeout(Some(PROXY_TIMEOUT))
        .map_err(|e| format!("set read timeout: {}", e))?;
    stream
        .set_write_timeout(Some(PROXY_TIMEOUT))
        .map_err(|e| format!("set write timeout: {}", e))?;
    Ok(stream)
}

/// round_trip writes one already-serialized JSON command and reads
/// the response. Returns the parsed Value on success; any IO or
/// encoding failure becomes an error string the caller can either
/// surface or use as a "drop this pooled conn" signal.
fn round_trip(stream: &TcpStream, payload: &[u8]) -> Result<Value, String> {
    // OxiDB's wire reader auto-detects format from the first byte
    // (JSON / MsgPack / OxiWire). JSON payloads start with '{' which
    // the server treats as JSON — exactly what we want.
    let mut tx = stream;
    write_message(&mut tx, payload).map_err(|e| format!("write: {}", e))?;
    let mut rx = stream;
    let resp_bytes = read_message(&mut rx).map_err(|e| format!("read: {}", e))?;
    serde_json::from_slice::<Value>(&resp_bytes).map_err(|e| format!("parse response: {}", e))
}

/// resolve turns a "host:port" string into a SocketAddr. Picks the
/// first matching address from std's resolver — keeps the v2 client
/// simple without dragging in a DNS resolver crate.
fn resolve(addr: &str) -> Result<SocketAddr, String> {
    let mut iter = addr
        .to_socket_addrs()
        .map_err(|e| format!("resolve {}: {}", addr, e))?;
    iter.next()
        .ok_or_else(|| format!("resolve {}: no addresses returned", addr))
}

// -----------------------------------------------------------------------
// Connection pool (v2a, extended in v2b with per-user keying)
// -----------------------------------------------------------------------

/// PooledConn pairs a stream with its last-used timestamp so the pool
/// can age out stale connections without a background sweeper.
struct PooledConn {
    stream: TcpStream,
    last_used: Instant,
}

/// PoolKey distinguishes anonymous from per-user buckets. A None user
/// is an unauthenticated link; a Some(user) bucket only holds conns
/// that successfully completed SCRAM as that user. Mixing the two
/// would either send unauthed traffic to an auth-required server, or
/// leak one tenant's session into another's request.
type PoolKey = (String, u16, Option<String>);

/// Pool is a per-process map from PoolKey to a bounded list of idle
/// connections. Locked on every take / give_back; contention is fine
/// because every operation is short (no IO under the lock).
pub struct Pool {
    inner: Mutex<HashMap<PoolKey, Vec<PooledConn>>>,
}

impl Pool {
    /// take returns an idle conn for (host, port, user), or None if
    /// the bucket is empty / all entries are stale (those are evicted
    /// as a side effect — saves the caller from a stale-conn round
    /// trip).
    pub fn take(&self, host: &str, port: u16, user: Option<&str>) -> Option<TcpStream> {
        let mut map = self.inner.lock().ok()?;
        let key = (host.to_string(), port, user.map(|s| s.to_string()));
        let bucket = map.get_mut(&key)?;
        let now = Instant::now();
        // Evict stale entries from the back first (most-recently used
        // is at the back per the give_back contract). The walk is
        // small (MAX_IDLE_PER_REMOTE) so a linear pass is fine.
        bucket.retain(|c| now.duration_since(c.last_used) < IDLE_TIMEOUT);
        bucket.pop().map(|c| c.stream)
    }

    /// give_back returns a connection to the pool, dropping it if the
    /// bucket is full. The caller is responsible for NOT returning a
    /// conn that failed IO — the round_trip call site already does
    /// this via the Result match — and for NOT returning a conn whose
    /// authentication failed (proxy_command surfaces the error before
    /// give_back can run).
    pub fn give_back(&self, host: &str, port: u16, user: Option<&str>, stream: TcpStream) {
        let mut map = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let key = (host.to_string(), port, user.map(|s| s.to_string()));
        let bucket = map.entry(key).or_default();
        if bucket.len() < MAX_IDLE_PER_REMOTE {
            bucket.push(PooledConn {
                stream,
                last_used: Instant::now(),
            });
        }
        // else: bucket full → just let `stream` drop; socket closes.
    }

    /// idle_count returns the number of pooled connections for a
    /// remote-user triple. Public so external integration tests +
    /// operator introspection paths can observe pool state without
    /// going through a debug-only cfg gate.
    pub fn idle_count(&self, host: &str, port: u16, user: Option<&str>) -> usize {
        let map = self.inner.lock().unwrap();
        let key = (host.to_string(), port, user.map(|s| s.to_string()));
        map.get(&key).map(|b| b.len()).unwrap_or(0)
    }
}

/// pool returns the per-process FDW connection pool. Constructed lazily
/// on first use; no shutdown — process exit reaps everything.
pub fn pool() -> &'static Pool {
    static POOL: OnceLock<Pool> = OnceLock::new();
    POOL.get_or_init(|| Pool {
        inner: Mutex::new(HashMap::new()),
    })
}

// Re-export so handler.rs can construct one without a fresh import
// chain. The proxy code lives entirely in this module.
pub use oxidb::links::{parse_remote, LinkConfig, ParsedRemote as Remote};

// Convenience: io::Error -> String for the call sites that prefer
// String-typed errors (matching the handler's `{"ok": false,
// "error": "..."}` shape).
#[allow(dead_code)]
fn io_err(e: io::Error) -> String {
    e.to_string()
}
