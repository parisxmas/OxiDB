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
    /// Create a new GELF logger targeting `addr` (e.g. `"172.17.0.1:12201"`).
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
    pub fn send(
        &self,
        level: GelfLevel,
        short_message: &str,
        extra: &[(&str, &str)],
    ) {
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
