use std::net::UdpSocket;
use std::time::{SystemTime, UNIX_EPOCH};

/// GELF severity levels (syslog-compatible).
#[repr(u8)]
#[derive(Clone, Copy)]
pub enum GelfLevel {
    Emergency = 0,
    Alert = 1,
    Critical = 2,
    Error = 3,
    Warning = 4,
    Notice = 5,
    Informational = 6,
    Debug = 7,
}

/// Fire-and-forget GELF UDP logger.
///
/// Wraps a non-blocking `UdpSocket` pre-connected to the GELF target.
/// Send failures are silently ignored — logging must never block or crash
/// the server.
pub struct GelfLogger {
    socket: UdpSocket,
    hostname: String,
}

impl GelfLogger {
    /// Create a new GELF logger targeting `addr` (e.g. `"192.0.2.100:12201"`).
    pub fn new(addr: &str) -> Result<Self, String> {
        let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("GELF bind: {e}"))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| format!("GELF nonblocking: {e}"))?;
        socket
            .connect(addr)
            .map_err(|e| format!("GELF connect to {addr}: {e}"))?;

        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "oxidb-server".to_string());

        Ok(Self { socket, hostname })
    }

    /// Send a GELF message. Extra fields are added as `_key` entries.
    /// This is fire-and-forget: errors are silently ignored.
    pub fn send(&self, level: GelfLevel, short_message: &str, extra: &[(&str, &str)]) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        let mut msg = serde_json::json!({
            "version": "1.1",
            "host": self.hostname,
            "short_message": short_message,
            "timestamp": ts,
            "level": level as u8,
        });

        if let Some(obj) = msg.as_object_mut() {
            for &(k, v) in extra {
                obj.insert(format!("_{k}"), serde_json::Value::String(v.to_string()));
            }
        }

        let payload = msg.to_string();
        let _ = self.socket.send(payload.as_bytes());
    }
}

// ---------------------------------------------------------------------------
// Process-global logger — lets any subsystem (REST, wire) emit without
// threading a handle through every state struct. Off (zero cost) unless
// `OXIDB_GELF_ADDR` is set.
// ---------------------------------------------------------------------------

/// Fire-and-forget **MessagePack** UDP log sender — a cheaper sibling of GELF
/// (compact binary; the ingest does no per-field indexing). Records are flat
/// MessagePack maps that [`crate::msgpack_ingest`] decodes and appends verbatim.
pub struct MsgpackLogger {
    socket: UdpSocket,
    hostname: String,
}

impl MsgpackLogger {
    pub fn new(addr: &str) -> Result<Self, String> {
        let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("msgpack bind: {e}"))?;
        socket
            .set_nonblocking(true)
            .map_err(|e| format!("msgpack nonblocking: {e}"))?;
        socket
            .connect(addr)
            .map_err(|e| format!("msgpack connect to {addr}: {e}"))?;
        let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| "oxidb-server".to_string());
        Ok(Self { socket, hostname })
    }

    pub fn send(&self, level: GelfLevel, short_message: &str, extra: &[(&str, &str)]) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let mut m = serde_json::Map::with_capacity(extra.len() + 4);
        m.insert("host".into(), serde_json::Value::String(self.hostname.clone()));
        m.insert(
            "short_message".into(),
            serde_json::Value::String(short_message.to_string()),
        );
        m.insert("level".into(), serde_json::json!(level as u8));
        m.insert("ts".into(), serde_json::json!(ts));
        for &(k, v) in extra {
            m.insert(k.to_string(), serde_json::Value::String(v.to_string()));
        }
        if let Ok(bytes) = rmp_serde::to_vec_named(&serde_json::Value::Object(m)) {
            let _ = self.socket.send(&bytes);
        }
    }
}

use std::sync::OnceLock;

static GLOBAL: OnceLock<Option<GelfLogger>> = OnceLock::new();
static MSGPACK: OnceLock<Option<MsgpackLogger>> = OnceLock::new();

/// Initialize the global GELF (`OXIDB_GELF_ADDR`) and MessagePack
/// (`OXIDB_MSGPACK_ADDR`) log senders (idempotent). Either, both, or neither.
pub fn init_global() {
    GLOBAL.get_or_init(|| {
        std::env::var("OXIDB_GELF_ADDR")
            .ok()
            .filter(|s| !s.is_empty())
            .and_then(|addr| GelfLogger::new(&addr).ok())
    });
    MSGPACK.get_or_init(|| {
        std::env::var("OXIDB_MSGPACK_ADDR")
            .ok()
            .filter(|s| !s.is_empty())
            .and_then(|addr| MsgpackLogger::new(&addr).ok())
    });
}

/// Emit via whichever global sinks are configured (GELF and/or MessagePack).
/// No-op when both are disabled.
pub fn log(level: GelfLevel, short_message: &str, extra: &[(&str, &str)]) {
    if let Some(Some(g)) = GLOBAL.get() {
        g.send(level, short_message, extra);
    }
    if let Some(Some(m)) = MSGPACK.get() {
        m.send(level, short_message, extra);
    }
}

/// `true` when at least one global log sink is active.
pub fn enabled() -> bool {
    matches!(GLOBAL.get(), Some(Some(_))) || matches!(MSGPACK.get(), Some(Some(_)))
}
