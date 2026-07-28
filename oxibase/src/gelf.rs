//! Minimal fire-and-forget GELF (Graylog Extended Log Format) UDP logger, so
//! the control plane can ship a message per request to OxiDB's GELF port
//! (`OXIDB_GELF_ADDR`, same knob the data plane uses). Off = zero cost.

use std::net::UdpSocket;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

/// Syslog-compatible GELF level.
#[derive(Clone, Copy)]
pub enum Level {
    Error = 3,
    Warning = 4,
    Info = 6,
}

/// Wire format for a log sink.
enum Format {
    /// GELF v1.1 JSON.
    Gelf,
    /// Compact MessagePack map (OxiDB's cheaper log ingest).
    Msgpack,
}

struct Logger {
    socket: UdpSocket,
    host: String,
    format: Format,
}

impl Logger {
    fn new(addr: &str, format: Format) -> Option<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
        socket.set_nonblocking(true).ok()?;
        socket.connect(addr).ok()?;
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "oxibase".to_string());
        Some(Self {
            socket,
            host,
            format,
        })
    }

    fn send(&self, level: Level, short_message: &str, extra: &[(&str, &str)]) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        match self.format {
            Format::Gelf => {
                let mut msg = json!({
                    "version": "1.1",
                    "host": self.host,
                    "short_message": short_message,
                    "timestamp": ts,
                    "level": level as u8,
                });
                if let Some(o) = msg.as_object_mut() {
                    for &(k, v) in extra {
                        o.insert(format!("_{k}"), Value::String(v.to_string()));
                    }
                }
                let _ = self.socket.send(msg.to_string().as_bytes());
            }
            Format::Msgpack => {
                let mut m = serde_json::Map::with_capacity(extra.len() + 4);
                m.insert("host".into(), Value::String(self.host.clone()));
                m.insert(
                    "short_message".into(),
                    Value::String(short_message.to_string()),
                );
                m.insert("level".into(), json!(level as u8));
                m.insert("ts".into(), json!(ts));
                for &(k, v) in extra {
                    m.insert(k.to_string(), Value::String(v.to_string()));
                }
                if let Ok(bytes) = rmp_serde::to_vec_named(&Value::Object(m)) {
                    let _ = self.socket.send(&bytes);
                }
            }
        }
    }
}

static GELF: OnceLock<Option<Logger>> = OnceLock::new();
static MSGPACK: OnceLock<Option<Logger>> = OnceLock::new();

/// Arm the global GELF (`OXIDB_GELF_ADDR`) and MessagePack (`OXIDB_MSGPACK_ADDR`)
/// log sinks (idempotent). Either, both, or neither.
pub fn init() {
    GELF.get_or_init(|| {
        std::env::var("OXIDB_GELF_ADDR")
            .ok()
            .filter(|s| !s.is_empty())
            .and_then(|a| Logger::new(&a, Format::Gelf))
    });
    MSGPACK.get_or_init(|| {
        std::env::var("OXIDB_MSGPACK_ADDR")
            .ok()
            .filter(|s| !s.is_empty())
            .and_then(|a| Logger::new(&a, Format::Msgpack))
    });
}

/// `true` when at least one log sink is armed.
pub fn enabled() -> bool {
    matches!(GELF.get(), Some(Some(_))) || matches!(MSGPACK.get(), Some(Some(_)))
}

/// Emit to whichever sinks are armed. No-op when both are disabled.
pub fn log(level: Level, short_message: &str, extra: &[(&str, &str)]) {
    if let Some(Some(g)) = GELF.get() {
        g.send(level, short_message, extra);
    }
    if let Some(Some(m)) = MSGPACK.get() {
        m.send(level, short_message, extra);
    }
}
