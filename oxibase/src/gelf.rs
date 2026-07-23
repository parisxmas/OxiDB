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

struct Logger {
    socket: UdpSocket,
    host: String,
}

impl Logger {
    fn new(addr: &str) -> Option<Self> {
        let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
        socket.set_nonblocking(true).ok()?;
        socket.connect(addr).ok()?;
        let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "oxibase".to_string());
        Some(Self { socket, host })
    }

    fn send(&self, level: Level, short_message: &str, extra: &[(&str, &str)]) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
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
}

static GLOBAL: OnceLock<Option<Logger>> = OnceLock::new();

/// Arm the global logger from `OXIDB_GELF_ADDR` (idempotent).
pub fn init() {
    GLOBAL.get_or_init(|| {
        std::env::var("OXIDB_GELF_ADDR")
            .ok()
            .filter(|s| !s.is_empty())
            .and_then(|a| Logger::new(&a))
    });
}

/// `true` when GELF is armed.
pub fn enabled() -> bool {
    matches!(GLOBAL.get(), Some(Some(_)))
}

/// Emit a message. No-op when GELF is disabled.
pub fn log(level: Level, short_message: &str, extra: &[(&str, &str)]) {
    if let Some(Some(g)) = GLOBAL.get() {
        g.send(level, short_message, extra);
    }
}
