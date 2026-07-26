//! MessagePack log ingestion listener for OxiDB — a cheaper sibling of the GELF
//! ingest (`gelf_ingest`).
//!
//! Receives **MessagePack**-encoded log records over UDP and appends them to a
//! collection (default `_msgpack_logs`). Two things make it much lighter than
//! GELF:
//!   1. MessagePack is a compact binary format — smaller packets, faster parse.
//!   2. It does **not** auto-index every field. GELF's Elasticsearch-style
//!      dynamic mapping builds a BTree index per field (high-cardinality ones
//!      like request latency dominate memory); a log stream is append-only and
//!      rarely queried by arbitrary field, so we skip indexing entirely.
//!
//! Enable with `OXIDB_MSGPACK_PORT=12202`.

use std::collections::{HashMap, HashSet};
use std::net::UdpSocket;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

use oxidb::database_manager::DatabaseManager;
use oxidb::OxiDb;

/// Maximum datagram size (log records are small; one record per packet).
const MAX_MSG_SIZE: usize = 65535;
/// Flush a batch once it reaches this many records…
const BATCH_SIZE: usize = 2000;
/// …or after this long, whichever comes first.
const BATCH_TIMEOUT: Duration = Duration::from_millis(5);

/// Start MessagePack UDP ingestion. Mirrors the GELF ingest's receiver/writer
/// split but parses MessagePack and does no per-field indexing.
pub fn start_msgpack_listener(
    addr: &str,
    db: Arc<OxiDb>,
    collection: String,
) -> Vec<std::thread::JoinHandle<()>> {
    start_msgpack_listener_routed(addr, db, None, collection)
}

/// As [`start_msgpack_listener`], but a record carrying a `db` field is written
/// to **that database's** copy of the collection.
///
/// One shared sink made every read of one project's logs walk past every other
/// project's rows, and made one project's traffic burst everyone else's problem
/// — retention, size and query cost were all shared. A tenant's requests are the
/// tenant's data, so they live in the tenant's database: reads touch only that
/// project, retention is per project, and dropping a project takes its logs with
/// it.
///
/// Records with no `db` (the control plane's own requests) stay in the default
/// database, which is where they belong: they are not any one project's.
pub fn start_msgpack_listener_routed(
    addr: &str,
    db: Arc<OxiDb>,
    manager: Option<Arc<DatabaseManager>>,
    collection: String,
) -> Vec<std::thread::JoinHandle<()>> {
    let num_receivers = std::thread::available_parallelism()
        .map(|n| n.get().min(8))
        .unwrap_or(4);
    let num_writers = (num_receivers / 2).max(2);

    eprintln!(
        "MessagePack ingestion: listening on {addr} → collection '{collection}' \
         ({num_receivers} receivers, {num_writers} writers, batch={BATCH_SIZE}, \
         no auto-indexing)"
    );

    let (tx, rx) = crossbeam_channel::bounded::<Value>(BATCH_SIZE * num_writers * 4);
    let mut handles = Vec::with_capacity(num_receivers + num_writers);

    // Receivers: UDP recv → MessagePack parse → channel.
    for i in 0..num_receivers {
        let tx = tx.clone();
        let addr = addr.to_string();
        let handle = std::thread::Builder::new()
            .name(format!("mpack-recv-{i}"))
            .spawn(move || {
                let socket = bind_reuseport(&addr);
                let mut buf = [0u8; MAX_MSG_SIZE];
                loop {
                    match socket.recv_from(&mut buf) {
                        Ok((len, _)) if len > 0 => {
                            if let Ok(doc) = rmp_serde::from_slice::<Value>(&buf[..len]) {
                                if doc.is_object() {
                                    let _ = tx.try_send(doc);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            })
            .expect("failed to spawn msgpack receiver thread");
        handles.push(handle);
    }

    // Writers: channel → batch → insert_many (no indexing).
    for i in 0..num_writers {
        let db = Arc::clone(&db);
        let manager = manager.clone();
        let collection = collection.clone();
        let rx = rx.clone();
        let handle = std::thread::Builder::new()
            .name(format!("mpack-write-{i}"))
            .spawn(move || {
                let mut batch: Vec<Value> = Vec::with_capacity(BATCH_SIZE);
                loop {
                    match rx.recv() {
                        Ok(doc) => batch.push(doc),
                        Err(_) => break,
                    }
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
                    let docs = std::mem::replace(&mut batch, Vec::with_capacity(BATCH_SIZE));
                    match &manager {
                        // Split the batch by target database and write each part
                        // once — a batch is a few milliseconds of traffic, so it
                        // is usually one or two projects, not many.
                        Some(mgr) => {
                            let mut by_db: HashMap<String, Vec<Value>> = HashMap::new();
                            for doc in docs {
                                let target = doc
                                    .get("db")
                                    .and_then(|v| v.as_str())
                                    .filter(|s| !s.is_empty())
                                    .unwrap_or("")
                                    .to_string();
                                by_db.entry(target).or_default().push(doc);
                            }
                            for (target, docs) in by_db {
                                if target.is_empty() {
                                    let _ = db.insert_many(&collection, docs);
                                    continue;
                                }
                                match mgr.get_database(&target) {
                                    Ok(tenant) => {
                                        ensure_log_indexes(&tenant, &target, &collection);
                                        let _ = tenant.insert_many(&collection, docs);
                                    }
                                    // A `db` naming no database is a stale or
                                    // hand-written value; the record is still a
                                    // request that happened, so keep it rather
                                    // than drop it.
                                    Err(_) => {
                                        let _ = db.insert_many(&collection, docs);
                                    }
                                }
                            }
                        }
                        None => {
                            let _ = db.insert_many(&collection, docs);
                        }
                    }
                }
            })
            .expect("failed to spawn msgpack writer thread");
        handles.push(handle);
    }

    handles
}

/// Bind a UDP socket with SO_REUSEPORT for kernel-level load balancing.
fn bind_reuseport(addr: &str) -> UdpSocket {
    use std::net::ToSocketAddrs;

    let sock_addr = addr
        .to_socket_addrs()
        .expect("invalid MessagePack UDP address")
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
    .expect("failed to create MessagePack UDP socket");

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

    let _ = socket.set_recv_buffer_size(64 * 1024 * 1024);

    socket
        .bind(&sock_addr.into())
        .unwrap_or_else(|e| panic!("failed to bind MessagePack UDP on {addr}: {e}"));

    UdpSocket::from(socket)
}


/// Retention and the index the dashboard's newest-first paging walks, created
/// once per database per process.
///
/// A tenant database gets its log collection the first time it is written to,
/// and a collection created that way has no indexes — which is how an unindexed,
/// unbounded log collection appears without anybody deciding to make one.
fn ensure_log_indexes(db: &Arc<OxiDb>, db_name: &str, collection: &str) {
    use std::sync::Mutex;
    use std::sync::OnceLock;
    static DONE: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let done = DONE.get_or_init(|| Mutex::new(HashSet::new()));
    // Keyed by name, not by the Arc's address: a database that is closed and
    // reopened lands wherever the allocator puts it, and a freed address reused
    // by a different database would make this skip the indexes for it.
    let key = format!("{db_name}/{collection}");
    {
        let mut set = done.lock().unwrap();
        if !set.insert(key) {
            return;
        }
    }
    let ttl: u64 = std::env::var("OXIDB_MSGPACK_TTL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(604_800);
    let _ = if ttl > 0 {
        db.create_ttl_index(collection, "_ts", ttl)
    } else {
        db.create_index(collection, "_ts")
    };
}
