//! UDP log ingestion listener for OxiDB.
//!
//! Receives JSON or plain-text log messages over UDP and inserts them into
//! a configurable collection (default: `_udp_logs`).
//!
//! Architecture:
//! - N receiver threads (SO_REUSEPORT) push parsed docs into a channel
//! - 1 writer thread drains the channel in batches and calls `insert_many`
//! - Batching amortizes collection lock overhead across hundreds of docs
//!
//! Enable with `OXIDB_UDP_PORT=5140`.

use std::net::UdpSocket;
use std::sync::Arc;

use serde_json::{Value, json};

use oxidb::OxiDb;

/// Start multi-threaded UDP ingestion.
/// Each thread receives UDP packets and inserts directly into the DB.
/// With scc::HashMap + interior mutability, concurrent inserts are safe.
pub fn start_udp_listener(
    addr: &str,
    db: Arc<OxiDb>,
    collection: String,
) -> Vec<std::thread::JoinHandle<()>> {
    let num_threads = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4);

    eprintln!(
        "UDP log ingestion: listening on {addr} → collection '{collection}' ({num_threads} threads)"
    );

    let mut handles = Vec::with_capacity(num_threads);

    for i in 0..num_threads {
        let db = Arc::clone(&db);
        let collection = collection.clone();
        let addr = addr.to_string();

        let handle = std::thread::Builder::new()
            .name(format!("oxidb-udp-{i}"))
            .spawn(move || {
                let socket = bind_reuseport(&addr);
                let mut buf = [0u8; 65535];

                loop {
                    if let Ok((len, _)) = socket.recv_from(&mut buf) {
                        if len == 0 {
                            continue;
                        }
                        let doc = parse_log_message(&buf[..len]);
                        let _ = db.insert(&collection, doc);
                    }
                }
            })
            .expect("failed to spawn UDP ingest thread");
        handles.push(handle);
    }

    handles
}

/// Bind a UDP socket with SO_REUSEPORT for kernel-level load balancing.
fn bind_reuseport(addr: &str) -> UdpSocket {
    use std::net::ToSocketAddrs;

    let sock_addr = addr
        .to_socket_addrs()
        .expect("invalid UDP address")
        .next()
        .expect("no socket address resolved");

    let socket = socket2::Socket::new(
        if sock_addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        },
        socket2::Type::DGRAM,
        Some(socket2::Protocol::UDP),
    )
    .expect("failed to create UDP socket");

    socket.set_reuse_address(true).expect("SO_REUSEADDR failed");

    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            let val: i32 = 1;
            libc::setsockopt(
                socket.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_REUSEPORT,
                &val as *const _ as *const std::ffi::c_void,
                std::mem::size_of::<i32>() as u32,
            );
        }
    }

    let _ = socket.set_recv_buffer_size(8 * 1024 * 1024); // 8MB recv buffer

    socket
        .bind(&sock_addr.into())
        .unwrap_or_else(|_| panic!("failed to bind UDP on {addr}"));

    UdpSocket::from(socket)
}

/// Parse a UDP log message into a JSON Value.
fn parse_log_message(data: &[u8]) -> Value {
    let now = chrono::Utc::now().to_rfc3339();

    if let Ok(mut val) = serde_json::from_slice::<Value>(data) {
        if let Some(obj) = val.as_object_mut()
            && !obj.contains_key("_ts")
        {
            obj.insert("_ts".to_string(), Value::String(now));
        }
        return val;
    }

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
        assert!(doc.get("_ts").is_some());
    }

    #[test]
    fn parse_plain_text() {
        let data = b"something went wrong";
        let doc = parse_log_message(data);
        assert_eq!(doc.get("message").unwrap(), "something went wrong");
    }

    #[test]
    fn parse_json_preserves_existing_ts() {
        let data = br#"{"_ts":"2024-01-01T00:00:00Z","msg":"hello"}"#;
        let doc = parse_log_message(data);
        assert_eq!(doc.get("_ts").unwrap(), "2024-01-01T00:00:00Z");
    }
}
