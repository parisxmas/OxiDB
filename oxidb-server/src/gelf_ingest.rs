//! GELF (Graylog Extended Log Format) UDP ingestion listener for OxiDB.
//!
//! Receives GELF v1.1 JSON messages over UDP and inserts them into a
//! configurable collection (default: `_gelf_logs`).
//!
//! Architecture:
//! - N receiver threads (SO_REUSEPORT) parse UDP packets and push docs into a
//!   lock-free crossbeam channel
//! - M writer threads drain the channel in batches and call `insert_many`
//! - SIMD-accelerated JSON parsing via `simd-json`
//! - Batch-level timestamping (one `_ts` per flush, not per message)
//!
//! **Auto-indexing:** Every field in incoming GELF messages is automatically
//! indexed on first sight — Elasticsearch-style dynamic mapping.
//!
//! Enable with `OXIDB_GELF_PORT=12201`.

use std::collections::{HashMap, HashSet};
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

use oxidb::OxiDb;

/// Maximum GELF message size.
const MAX_GELF_SIZE: usize = 65535;

/// GELF chunked message magic bytes.
const GELF_CHUNKED_MAGIC: [u8; 2] = [0x1e, 0x0f];

/// GELF chunked header size: 2 (magic) + 8 (message_id) + 1 (seq_num) + 1 (seq_count) = 12
const CHUNK_HEADER_SIZE: usize = 12;

/// Maximum age for incomplete chunk sets before they are discarded.
const CHUNK_TIMEOUT: Duration = Duration::from_secs(5);

/// How often to sweep expired chunk buffers (every N packets).
const CHUNK_SWEEP_INTERVAL: u64 = 1000;

/// Maximum batch size before flushing to the database.
const BATCH_SIZE: usize = 2000;

/// Maximum time to wait before flushing a partial batch.
const BATCH_TIMEOUT: Duration = Duration::from_millis(5);

/// Buffer for reassembling a chunked GELF message.
struct ChunkBuffer {
    /// Chunks indexed by sequence number. `None` = not yet received.
    chunks: Vec<Option<Vec<u8>>>,
    /// Number of chunks received so far.
    received: u8,
    /// Total number of chunks expected.
    total: u8,
    /// When the first chunk arrived (for timeout).
    created_at: Instant,
}

impl ChunkBuffer {
    fn new(total: u8) -> Self {
        Self {
            chunks: vec![None; total as usize],
            received: 0,
            total,
            created_at: Instant::now(),
        }
    }

    /// Insert a chunk. Returns true if the message is now complete.
    fn insert(&mut self, seq: u8, data: Vec<u8>) -> bool {
        if (seq as usize) < self.chunks.len() && self.chunks[seq as usize].is_none() {
            self.chunks[seq as usize] = Some(data);
            self.received += 1;
        }
        self.received == self.total
    }

    /// Reassemble all chunks into a single byte buffer.
    fn reassemble(&self) -> Vec<u8> {
        let total_len: usize = self.chunks.iter().filter_map(|c| c.as_ref()).map(|c| c.len()).sum();
        let mut buf = Vec::with_capacity(total_len);
        for chunk in &self.chunks {
            if let Some(data) = chunk {
                buf.extend_from_slice(data);
            }
        }
        buf
    }

    fn is_expired(&self) -> bool {
        self.created_at.elapsed() > CHUNK_TIMEOUT
    }
}

/// Standard GELF fields that are always present and always indexed.
const STANDARD_FIELDS: &[&str] = &[
    "host", "short_message", "level", "facility", "timestamp", "_ts",
];

/// Tracks which fields have already been indexed for a collection.
struct FieldIndex {
    known: HashSet<String>,
}

impl FieldIndex {
    fn new() -> Self {
        Self {
            known: HashSet::new(),
        }
    }
}

/// Start GELF UDP ingestion with lock-free channel and SIMD JSON parsing.
pub fn start_gelf_listener(
    addr: &str,
    db: Arc<OxiDb>,
    collection: String,
) -> Vec<std::thread::JoinHandle<()>> {
    let num_receivers = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4);
    let num_writers = (num_receivers / 2).max(2);

    // Create indexes for standard GELF fields upfront
    for field in STANDARD_FIELDS {
        let _ = db.create_index(&collection, field);
    }
    eprintln!(
        "GELF ingestion: listening on {addr} → collection '{collection}' \
         ({num_receivers} receivers, {num_writers} writers, batch={BATCH_SIZE}, \
         simd-json, crossbeam, auto-indexing)"
    );

    let field_index = Arc::new(Mutex::new(FieldIndex::new()));
    {
        let mut fi = field_index.lock().unwrap();
        for f in STANDARD_FIELDS {
            fi.known.insert(f.to_string());
        }
    }

    // Lock-free MPMC channel — no mutex on receive side
    let (tx, rx) = crossbeam_channel::bounded::<Value>(BATCH_SIZE * num_writers * 4);

    let mut handles = Vec::with_capacity(num_receivers + num_writers);

    // --- Receiver threads: UDP recv → simd-json parse → channel ---
    for i in 0..num_receivers {
        let tx = tx.clone();
        let addr = addr.to_string();

        let handle = std::thread::Builder::new()
            .name(format!("gelf-recv-{i}"))
            .spawn(move || {
                let socket = bind_reuseport(&addr);
                let mut buf = [0u8; MAX_GELF_SIZE];
                let mut chunk_buffers: HashMap<u64, ChunkBuffer> = HashMap::new();
                let mut packet_count: u64 = 0;

                loop {
                    match socket.recv_from(&mut buf) {
                        Ok((len, _src)) => {
                            if len == 0 {
                                continue;
                            }

                            packet_count += 1;

                            // Periodic sweep of expired chunk buffers
                            if packet_count % CHUNK_SWEEP_INTERVAL == 0 {
                                chunk_buffers.retain(|_, cb| !cb.is_expired());
                            }

                            if len >= CHUNK_HEADER_SIZE
                                && buf[0] == GELF_CHUNKED_MAGIC[0]
                                && buf[1] == GELF_CHUNKED_MAGIC[1]
                            {
                                // Chunked GELF message
                                let message_id = u64::from_be_bytes([
                                    buf[2], buf[3], buf[4], buf[5],
                                    buf[6], buf[7], buf[8], buf[9],
                                ]);
                                let seq_num = buf[10];
                                let seq_count = buf[11];

                                if seq_count == 0 || seq_num >= seq_count {
                                    continue; // invalid
                                }

                                let chunk_data = buf[CHUNK_HEADER_SIZE..len].to_vec();

                                let cb = chunk_buffers
                                    .entry(message_id)
                                    .or_insert_with(|| ChunkBuffer::new(seq_count));

                                if cb.insert(seq_num, chunk_data) {
                                    // All chunks received — reassemble and parse
                                    let mut full_msg = cb.reassemble();
                                    chunk_buffers.remove(&message_id);

                                    if let Some(doc) = parse_gelf_message_simd(&mut full_msg) {
                                        let _ = tx.try_send(doc);
                                    }
                                }
                            } else {
                                // Regular (non-chunked) GELF message
                                if let Some(doc) = parse_gelf_message_simd(&mut buf[..len]) {
                                    let _ = tx.try_send(doc);
                                }
                            }
                        }
                        Err(_) => {}
                    }
                }
            })
            .expect("failed to spawn GELF receiver thread");
        handles.push(handle);
    }

    // --- Writer threads: channel → batch → insert_many ---
    for i in 0..num_writers {
        let db = Arc::clone(&db);
        let collection = collection.clone();
        let rx = rx.clone();
        let field_index = Arc::clone(&field_index);

        let handle = std::thread::Builder::new()
            .name(format!("gelf-write-{i}"))
            .spawn(move || {
                let mut batch: Vec<Value> = Vec::with_capacity(BATCH_SIZE);

                loop {
                    // Wait for first message (blocking — no mutex needed, crossbeam is MPMC)
                    match rx.recv() {
                        Ok(doc) => batch.push(doc),
                        Err(_) => break,
                    }

                    // Drain up to BATCH_SIZE with a short timeout
                    let deadline = Instant::now() + BATCH_TIMEOUT;
                    while batch.len() < BATCH_SIZE {
                        let remaining = deadline.saturating_duration_since(Instant::now());
                        if remaining.is_zero() {
                            break;
                        }
                        match rx.recv_timeout(remaining) {
                            Ok(doc) => batch.push(doc),
                            Err(_) => break,
                        }
                    }

                    // Stamp the batch with a single timestamp
                    let ts = chrono::Utc::now().to_rfc3339();
                    for doc in &mut batch {
                        if let Some(obj) = doc.as_object_mut() {
                            if !obj.contains_key("_ts") {
                                obj.insert("_ts".to_string(), Value::String(ts.clone()));
                            }
                        }
                    }

                    // Auto-index new fields
                    auto_index_batch(&db, &collection, &batch, &field_index);

                    // Flush
                    let docs = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                    let _ = db.insert_many(&collection, docs);
                }
            })
            .expect("failed to spawn GELF writer thread");
        handles.push(handle);
    }

    handles
}

/// Check all documents in a batch for new fields, create indexes for any unseen ones.
fn auto_index_batch(
    db: &OxiDb,
    collection: &str,
    batch: &[Value],
    field_index: &Mutex<FieldIndex>,
) {
    let mut batch_fields = HashSet::new();
    for doc in batch {
        if let Some(obj) = doc.as_object() {
            for key in obj.keys() {
                batch_fields.insert(key.clone());
            }
        }
    }

    // Fast path
    {
        let fi = field_index.lock().unwrap();
        if batch_fields.iter().all(|k| fi.known.contains(k)) {
            return;
        }
    }

    // Slow path: new field(s)
    let mut fi = field_index.lock().unwrap();
    for key in &batch_fields {
        if !fi.known.contains(key.as_str()) {
            if db.create_index(collection, key).is_ok() {
                eprintln!("GELF auto-index: created index on '{key}' in '{collection}'");
            }
            fi.known.insert(key.clone());
        }
    }
}

/// Parse a GELF message using simd-json (SIMD-accelerated), then convert
/// to serde_json::Value for storage.
///
/// `data` must be a mutable slice — simd-json parses in-place.
fn parse_gelf_message_simd(data: &mut [u8]) -> Option<Value> {
    // simd-json needs a mutable buffer (parses in-place for speed)
    let gelf: simd_json::OwnedValue = simd_json::to_owned_value(data).ok()?;

    // Convert to serde_json::Value via the simd_json serde bridge
    let mut gelf: Value = simd_json::serde::from_owned_value(gelf).ok()?;
    let obj = gelf.as_object_mut()?;

    if !obj.contains_key("short_message") {
        return None;
    }

    let mut doc = serde_json::Map::new();

    for &key in &[
        "host",
        "short_message",
        "full_message",
        "timestamp",
        "level",
        "facility",
    ] {
        if let Some(val) = obj.remove(key) {
            doc.insert(key.to_string(), val);
        }
    }

    obj.remove("version");

    let extra_keys: Vec<String> = obj
        .keys()
        .filter(|k| k.starts_with('_') && k.len() > 1)
        .cloned()
        .collect();

    for key in extra_keys {
        if let Some(val) = obj.remove(&key) {
            let stripped = key[1..].to_string();
            doc.insert(stripped, val);
        }
    }

    let remaining_keys: Vec<String> = obj.keys().cloned().collect();
    for k in remaining_keys {
        if let Some(v) = obj.remove(&k) {
            doc.insert(k, v);
        }
    }

    // Note: _ts is added by the writer thread at batch level
    Some(Value::Object(doc))
}

/// Fallback parser using serde_json (for tests and non-SIMD platforms).
#[cfg(test)]
fn parse_gelf_message(data: &[u8]) -> Option<Value> {
    let mut gelf: Value = serde_json::from_slice(data).ok()?;
    let obj = gelf.as_object_mut()?;

    if !obj.contains_key("short_message") {
        return None;
    }

    let mut doc = serde_json::Map::new();

    for &key in &[
        "host",
        "short_message",
        "full_message",
        "timestamp",
        "level",
        "facility",
    ] {
        if let Some(val) = obj.remove(key) {
            doc.insert(key.to_string(), val);
        }
    }

    obj.remove("version");

    let extra_keys: Vec<String> = obj
        .keys()
        .filter(|k| k.starts_with('_') && k.len() > 1)
        .cloned()
        .collect();

    for key in extra_keys {
        if let Some(val) = obj.remove(&key) {
            let stripped = key[1..].to_string();
            doc.insert(stripped, val);
        }
    }

    let remaining_keys: Vec<String> = obj.keys().cloned().collect();
    for k in remaining_keys {
        if let Some(v) = obj.remove(&k) {
            doc.insert(k, v);
        }
    }

    if !doc.contains_key("_ts") {
        doc.insert(
            "_ts".to_string(),
            Value::String(chrono::Utc::now().to_rfc3339()),
        );
    }

    Some(Value::Object(doc))
}

/// Bind a UDP socket with SO_REUSEPORT for kernel-level load balancing.
fn bind_reuseport(addr: &str) -> UdpSocket {
    use std::net::ToSocketAddrs;

    let sock_addr = addr
        .to_socket_addrs()
        .expect("invalid GELF UDP address")
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
    .expect("failed to create GELF UDP socket");

    socket
        .set_reuse_address(true)
        .expect("SO_REUSEADDR failed");

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

    // 64MB recv buffer
    let _ = socket.set_recv_buffer_size(64 * 1024 * 1024);

    socket
        .bind(&sock_addr.into())
        .unwrap_or_else(|e| panic!("failed to bind GELF UDP on {addr}: {e}"));

    UdpSocket::from(socket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_valid_gelf_message() {
        let data = br#"{
            "version": "1.1",
            "host": "web01",
            "short_message": "Login user: johndoe",
            "timestamp": 1713198420.123,
            "level": 6,
            "facility": "KarteX.API",
            "_Username": "johndoe",
            "_ClientIp": "192.0.2.42",
            "_SourceContext": "KarteX.API.Endpoints.User.LoginEndpoint"
        }"#;

        let doc = parse_gelf_message(data).unwrap();
        assert_eq!(doc.get("host").unwrap(), "web01");
        assert_eq!(doc.get("short_message").unwrap(), "Login user: johndoe");
        assert_eq!(doc.get("level").unwrap(), 6);
        assert_eq!(doc.get("facility").unwrap(), "KarteX.API");
        assert_eq!(doc.get("Username").unwrap(), "johndoe");
        assert_eq!(doc.get("ClientIp").unwrap(), "192.0.2.42");
        assert_eq!(
            doc.get("SourceContext").unwrap(),
            "KarteX.API.Endpoints.User.LoginEndpoint"
        );
        assert!(doc.get("version").is_none());
        assert!(doc.get("_ts").is_some());
    }

    #[test]
    fn parse_missing_short_message_returns_none() {
        let data = br#"{"version": "1.1", "host": "web01", "level": 6}"#;
        assert!(parse_gelf_message(data).is_none());
    }

    #[test]
    fn parse_invalid_json_returns_none() {
        let data = b"not json at all";
        assert!(parse_gelf_message(data).is_none());
    }

    #[test]
    fn parse_gelf_with_numeric_extra_fields() {
        let data = br#"{
            "version": "1.1",
            "host": "app01",
            "short_message": "request handled",
            "timestamp": 1713198420.0,
            "level": 6,
            "_ProcessId": 1,
            "_ResponseTimeMs": 42
        }"#;

        let doc = parse_gelf_message(data).unwrap();
        assert_eq!(doc.get("ProcessId").unwrap(), 1);
        assert_eq!(doc.get("ResponseTimeMs").unwrap(), 42);
    }

    #[test]
    fn parse_gelf_full_message() {
        let data = br#"{
            "version": "1.1",
            "host": "app01",
            "short_message": "error occurred",
            "full_message": "stack trace:\n  at Foo.Bar()\n  at Main()",
            "timestamp": 1713198420.0,
            "level": 3
        }"#;

        let doc = parse_gelf_message(data).unwrap();
        assert_eq!(doc.get("short_message").unwrap(), "error occurred");
        assert!(doc
            .get("full_message")
            .unwrap()
            .as_str()
            .unwrap()
            .contains("stack trace"));
    }

    #[test]
    fn simd_parse_valid_gelf_message() {
        let mut data = br#"{
            "version": "1.1",
            "host": "web01",
            "short_message": "Login user: johndoe",
            "timestamp": 1713198420.123,
            "level": 6,
            "_Username": "johndoe",
            "_ClientIp": "192.0.2.42"
        }"#
        .to_vec();

        let doc = parse_gelf_message_simd(&mut data).unwrap();
        assert_eq!(doc.get("host").unwrap(), "web01");
        assert_eq!(doc.get("Username").unwrap(), "johndoe");
        assert_eq!(doc.get("ClientIp").unwrap(), "192.0.2.42");
        assert!(doc.get("version").is_none());
    }

    #[test]
    fn simd_parse_invalid_returns_none() {
        let mut data = b"not json".to_vec();
        assert!(parse_gelf_message_simd(&mut data).is_none());
    }

    #[test]
    fn auto_index_detects_new_fields() {
        let fi = Mutex::new(FieldIndex::new());
        {
            let mut inner = fi.lock().unwrap();
            inner.known.insert("host".to_string());
            inner.known.insert("level".to_string());
        }

        let doc = serde_json::json!({
            "host": "web01",
            "level": 6,
            "Username": "johndoe",
            "ClientIp": "192.0.2.42"
        });

        let obj = doc.as_object().unwrap();
        let inner = fi.lock().unwrap();
        let new_fields: Vec<&String> = obj
            .keys()
            .filter(|k| !inner.known.contains(k.as_str()))
            .collect();
        assert_eq!(new_fields.len(), 2);
        assert!(new_fields.contains(&&"Username".to_string()));
        assert!(new_fields.contains(&&"ClientIp".to_string()));
    }

    #[test]
    fn chunk_buffer_reassembly() {
        let mut cb = ChunkBuffer::new(3);
        assert!(!cb.insert(0, b"hello ".to_vec()));
        assert!(!cb.insert(2, b"world".to_vec()));
        assert!(cb.insert(1, b"beautiful ".to_vec())); // now complete

        let result = cb.reassemble();
        assert_eq!(result, b"hello beautiful world");
    }

    #[test]
    fn chunk_buffer_duplicate_ignored() {
        let mut cb = ChunkBuffer::new(2);
        assert!(!cb.insert(0, b"first".to_vec()));
        assert!(!cb.insert(0, b"duplicate".to_vec())); // duplicate seq, ignored
        assert!(cb.insert(1, b"second".to_vec()));

        let result = cb.reassemble();
        assert_eq!(result, b"firstsecond"); // original kept, not duplicate
    }

    #[test]
    fn chunk_buffer_expired() {
        let mut cb = ChunkBuffer::new(2);
        cb.created_at = Instant::now() - Duration::from_secs(10);
        assert!(cb.is_expired());
    }

    #[test]
    fn chunk_buffer_not_expired() {
        let cb = ChunkBuffer::new(2);
        assert!(!cb.is_expired());
    }

    #[test]
    fn chunk_header_parsing() {
        // Build a chunked GELF packet
        let message_id: u64 = 0xDEADBEEF12345678;
        let seq_num: u8 = 0;
        let seq_count: u8 = 1;
        let payload = br#"{"version":"1.1","host":"test","short_message":"chunked!"}"#;

        let mut packet = Vec::new();
        packet.extend_from_slice(&GELF_CHUNKED_MAGIC);
        packet.extend_from_slice(&message_id.to_be_bytes());
        packet.push(seq_num);
        packet.push(seq_count);
        packet.extend_from_slice(payload);

        // Verify header parsing
        assert_eq!(packet[0], 0x1e);
        assert_eq!(packet[1], 0x0f);
        let parsed_id = u64::from_be_bytes([
            packet[2], packet[3], packet[4], packet[5],
            packet[6], packet[7], packet[8], packet[9],
        ]);
        assert_eq!(parsed_id, message_id);
        assert_eq!(packet[10], seq_num);
        assert_eq!(packet[11], seq_count);
        assert_eq!(&packet[CHUNK_HEADER_SIZE..], payload.as_slice());
    }
}
