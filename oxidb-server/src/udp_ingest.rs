//! UDP log ingestion listener for OxiDB.
//!
//! Receives JSON or plain-text log messages over UDP and inserts them into
//! a configurable collection (default: `_udp_logs`). Designed for high-throughput
//! fire-and-forget logging where TCP overhead is unacceptable.
//!
//! Supports two formats:
//! - **JSON object**: inserted as-is with `_ts` timestamp added
//! - **Plain text**: wrapped in `{"message": "<text>", "_ts": "<iso8601>"}`
//!
//! Enable with `OXIDB_UDP_PORT=5140` (syslog-style port, or any port).
//! Collection name: `OXIDB_UDP_COLLECTION` (default: `_udp_logs`).

use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{json, Value};

use oxidb::OxiDb;

/// Start the UDP ingestion listener on a background thread.
/// Returns the thread handle.
pub fn start_udp_listener(
    addr: &str,
    db: Arc<OxiDb>,
    collection: String,
) -> std::thread::JoinHandle<()> {
    let socket = UdpSocket::bind(addr).expect(&format!("failed to bind UDP on {addr}"));
    // Large receive buffer for burst traffic
    let _ = socket.set_read_timeout(None); // blocking reads

    let received = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let recv_clone = Arc::clone(&received);
    let err_clone = Arc::clone(&errors);

    eprintln!("UDP log ingestion: listening on {addr} → collection '{collection}'");

    std::thread::Builder::new()
        .name("oxidb-udp-ingest".into())
        .spawn(move || {
            let mut buf = [0u8; 65535]; // max UDP packet size

            loop {
                match socket.recv_from(&mut buf) {
                    Ok((len, _src)) => {
                        if len == 0 {
                            continue;
                        }

                        let data = &buf[..len];
                        let doc = parse_log_message(data);

                        match db.insert(&collection, doc) {
                            Ok(_) => {
                                recv_clone.fetch_add(1, Ordering::Relaxed);
                            }
                            Err(_) => {
                                err_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("UDP recv error: {e}");
                    }
                }
            }
        })
        .expect("failed to spawn UDP ingest thread")
}

/// Parse a UDP log message into a JSON Value suitable for insertion.
fn parse_log_message(data: &[u8]) -> Value {
    let now = chrono::Utc::now().to_rfc3339();

    // Try JSON parse first
    if let Ok(mut val) = serde_json::from_slice::<Value>(data) {
        if let Some(obj) = val.as_object_mut() {
            // Add timestamp if not present
            if !obj.contains_key("_ts") {
                obj.insert("_ts".to_string(), Value::String(now));
            }
        }
        return val;
    }

    // Plain text fallback
    let text = String::from_utf8_lossy(data);
    json!({
        "message": text.trim_end(),
        "_ts": now,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_json_message() {
        let data = br#"{"level":"error","msg":"disk full"}"#;
        let doc = parse_log_message(data);
        assert_eq!(doc.get("level").unwrap(), "error");
        assert_eq!(doc.get("msg").unwrap(), "disk full");
        assert!(doc.get("_ts").is_some());
    }

    #[test]
    fn parse_plain_text() {
        let data = b"something went wrong";
        let doc = parse_log_message(data);
        assert_eq!(doc.get("message").unwrap(), "something went wrong");
        assert!(doc.get("_ts").is_some());
    }

    #[test]
    fn parse_json_preserves_existing_ts() {
        let data = br#"{"_ts":"2024-01-01T00:00:00Z","msg":"hello"}"#;
        let doc = parse_log_message(data);
        assert_eq!(doc.get("_ts").unwrap(), "2024-01-01T00:00:00Z");
    }
}
