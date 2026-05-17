//! Minimal OxiDB-to-OxiDB client used by the linked-collection
//! (FDW) proxy. One connect-per-request — no pooling in v1; that's
//! a v2 concern once we measure linked-collection traffic.
//!
//! Wire format matches what `oxidb-server::protocol` reads: a 4-byte
//! little-endian length prefix followed by a JSON payload. Auth is
//! not exercised in v1 (the link URL grammar reserves user/password
//! for future use; today the proxy assumes the remote is unauthed
//! or shares the local server's trust boundary).

use serde_json::Value;
use std::io;
use std::net::TcpStream;
use std::time::Duration;

use oxidb::links::ParsedRemote;

use crate::protocol::{read_message, write_message};

/// Default connect + read timeout for proxy calls. Linked collections
/// are usually on a fast LAN, but we don't want a wedged remote to
/// hang a request forever — the local query just fails fast and
/// surfaces the error to the user.
const PROXY_TIMEOUT: Duration = Duration::from_secs(10);

/// Execute one command against a remote OxiDB and return the raw
/// response payload (`{"ok": true, "data": ...}` or
/// `{"ok": false, "error": "..."}`). The caller passes the already-
/// rewritten command — i.e. with `collection` set to the REMOTE
/// collection name, not the local link alias.
pub fn proxy_command(remote: &ParsedRemote, command: &Value) -> Result<Value, String> {
    let addr = format!("{}:{}", remote.host, remote.port);
    let stream = TcpStream::connect_timeout(&resolve(&addr)?, PROXY_TIMEOUT)
        .map_err(|e| format!("connect {}: {}", addr, e))?;
    stream
        .set_read_timeout(Some(PROXY_TIMEOUT))
        .map_err(|e| format!("set read timeout: {}", e))?;
    stream
        .set_write_timeout(Some(PROXY_TIMEOUT))
        .map_err(|e| format!("set write timeout: {}", e))?;

    let payload = serde_json::to_vec(command)
        .map_err(|e| format!("encode command: {}", e))?;

    // OxiDB's wire reader auto-detects format from the first byte
    // (JSON / MsgPack / OxiWire). JSON payloads start with '{' which
    // the server treats as JSON — exactly what we want.
    let mut tx = &stream;
    write_message(&mut tx, &payload).map_err(|e| format!("write: {}", e))?;
    let mut rx = &stream;
    let resp_bytes = read_message(&mut rx).map_err(|e| format!("read: {}", e))?;
    serde_json::from_slice::<Value>(&resp_bytes).map_err(|e| format!("parse response: {}", e))
}

/// resolve turns a "host:port" string into the SocketAddr the
/// TcpStream::connect_timeout call needs. Picks the first matching
/// IPv4 address, falling back to IPv6 — keeps the v1 client simple
/// without dragging in a DNS resolver crate.
fn resolve(addr: &str) -> Result<std::net::SocketAddr, String> {
    use std::net::ToSocketAddrs;
    let mut iter = addr
        .to_socket_addrs()
        .map_err(|e| format!("resolve {}: {}", addr, e))?;
    iter.next()
        .ok_or_else(|| format!("resolve {}: no addresses returned", addr))
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
