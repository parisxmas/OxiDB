//! Minimal OxiDB-to-OxiDB client used by the linked-collection
//! (FDW) proxy. v2a: idle connection pool keyed on (host, port). One
//! conn per concurrent request; reused across calls when idle. Bounded
//! per-remote so a misbehaving link can't starve file descriptors.
//!
//! Wire format matches what `oxidb-server::protocol` reads: a 4-byte
//! little-endian length prefix followed by a JSON payload. Auth is
//! still not exercised (the URL grammar accepts userinfo but ignores
//! it — that's v2b).

use serde_json::Value;
use std::collections::HashMap;
use std::io;
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use oxidb::links::ParsedRemote;

use crate::protocol::{read_message, write_message};

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
/// (host, port) reuse an idle conn from the pool. A pooled conn that
/// fails write or read is dropped and the request retried ONCE with
/// a fresh dial — covers the standard "server kicked the conn while
/// we held it idle" failure mode without surfacing it to the caller.
pub fn proxy_command(remote: &ParsedRemote, command: &Value) -> Result<Value, String> {
    let payload = serde_json::to_vec(command)
        .map_err(|e| format!("encode command: {}", e))?;

    // First attempt — pooled conn if any.
    if let Some(stream) = pool().take(&remote.host, remote.port) {
        match round_trip(&stream, &payload) {
            Ok(resp) => {
                pool().give_back(&remote.host, remote.port, stream);
                return Ok(resp);
            }
            // Pooled conn went bad. Don't surface the error — fall
            // through to a fresh dial below.
            Err(_) => {}
        }
    }

    // Fresh dial — and if THIS fails, we surface it.
    let stream = dial(remote)?;
    match round_trip(&stream, &payload) {
        Ok(resp) => {
            pool().give_back(&remote.host, remote.port, stream);
            Ok(resp)
        }
        Err(e) => Err(e),
    }
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
// Connection pool (v2a)
// -----------------------------------------------------------------------

/// PooledConn pairs a stream with its last-used timestamp so the pool
/// can age out stale connections without a background sweeper.
struct PooledConn {
    stream: TcpStream,
    last_used: Instant,
}

/// Pool is a per-process map from (host, port) to a bounded list of
/// idle connections. Locked on every take / give_back; contention is
/// fine because every operation is short (no IO under the lock).
pub struct Pool {
    inner: Mutex<HashMap<(String, u16), Vec<PooledConn>>>,
}

impl Pool {
    /// take returns an idle conn for (host, port), or None if the
    /// bucket is empty / all entries are stale (those are evicted as
    /// a side effect — saves the caller from a stale-conn round trip).
    pub fn take(&self, host: &str, port: u16) -> Option<TcpStream> {
        let mut map = self.inner.lock().ok()?;
        let bucket = map.get_mut(&(host.to_string(), port))?;
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
    /// this via the Result match.
    pub fn give_back(&self, host: &str, port: u16, stream: TcpStream) {
        let mut map = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let bucket = map.entry((host.to_string(), port)).or_default();
        if bucket.len() < MAX_IDLE_PER_REMOTE {
            bucket.push(PooledConn {
                stream,
                last_used: Instant::now(),
            });
        }
        // else: bucket full → just let `stream` drop; socket closes.
    }

    /// idle_count returns the number of pooled connections for a
    /// remote. Public so external integration tests + operator
    /// introspection paths can observe pool state without going
    /// through a debug-only cfg gate.
    pub fn idle_count(&self, host: &str, port: u16) -> usize {
        let map = self.inner.lock().unwrap();
        map.get(&(host.to_string(), port))
            .map(|b| b.len())
            .unwrap_or(0)
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
