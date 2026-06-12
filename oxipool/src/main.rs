use std::env;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::timeout;

mod scatter;
mod shard;

// ─── Config ──────────────────────────────────────────────────────────

struct Config {
    listen: String,
    master: String,
    replicas: Vec<String>,
    master_pool_size: usize,
    replica_pool_size: usize,
    shard_pool_size: usize,
    max_clients: usize,
    connect_timeout: Duration,
    stats_interval: Duration,
    /// Deadline for a full backend exchange (write request + read response).
    /// 0 disables. Without it, a shard that accepts a request but never
    /// answers hangs the client forever AND permanently eats the borrowed
    /// pooled connection — repeated against a hung shard, the whole pool.
    request_timeout: Duration,
    /// Client inactivity deadline. 0 (default) disables. An idle client
    /// holding a pinned transaction otherwise keeps a pooled connection out
    /// of circulation indefinitely.
    idle_timeout: Duration,
}

/// Request timeout shared with `scatter.rs` (set once at startup).
static REQUEST_TIMEOUT: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();

pub(crate) fn request_timeout() -> Option<Duration> {
    let d = *REQUEST_TIMEOUT.get_or_init(|| Duration::from_secs(30));
    if d.is_zero() { None } else { Some(d) }
}

impl Config {
    fn from_env() -> Self {
        let replicas_str = env::var("OXIPOOL_REPLICAS").unwrap_or_default();
        let replicas: Vec<String> = if replicas_str.is_empty() {
            vec![]
        } else {
            replicas_str
                .split(',')
                .map(|s| s.trim().to_string())
                .collect()
        };

        Self {
            listen: env::var("OXIPOOL_LISTEN").unwrap_or_else(|_| "127.0.0.1:4445".into()),
            master: env::var("OXIPOOL_MASTER").unwrap_or_else(|_| {
                env::var("OXIPOOL_BACKEND").unwrap_or_else(|_| "127.0.0.1:4444".into())
            }),
            replicas,
            master_pool_size: env::var("OXIPOOL_MASTER_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or_else(|| {
                    env::var("OXIPOOL_SIZE")
                        .ok()
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(10)
                }),
            replica_pool_size: env::var("OXIPOOL_REPLICA_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
            shard_pool_size: env::var("OXIPOOL_SHARD_POOL_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
            max_clients: env::var("OXIPOOL_MAX_CLIENTS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1000),
            connect_timeout: Duration::from_secs(
                env::var("OXIPOOL_CONNECT_TIMEOUT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(5),
            ),
            stats_interval: Duration::from_secs(
                env::var("OXIPOOL_STATS_INTERVAL")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(60),
            ),
            request_timeout: Duration::from_secs(
                env::var("OXIPOOL_REQUEST_TIMEOUT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(30),
            ),
            idle_timeout: Duration::from_secs(
                env::var("OXIPOOL_IDLE_TIMEOUT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0),
            ),
        }
    }
}

// ─── Stats ───────────────────────────────────────────────────────────

struct Stats {
    total_requests: AtomicU64,
    master_requests: AtomicU64,
    replica_requests: AtomicU64,
    shard_requests: AtomicU64,
    scatter_requests: AtomicU64,
    active_clients: AtomicI64,
    active_transactions: AtomicI64,
    pool_hits: AtomicU64,
    pool_waits: AtomicU64,
    backend_errors: AtomicU64,
}

impl Stats {
    fn new() -> Self {
        Self {
            total_requests: AtomicU64::new(0),
            master_requests: AtomicU64::new(0),
            replica_requests: AtomicU64::new(0),
            shard_requests: AtomicU64::new(0),
            scatter_requests: AtomicU64::new(0),
            active_clients: AtomicI64::new(0),
            active_transactions: AtomicI64::new(0),
            pool_hits: AtomicU64::new(0),
            pool_waits: AtomicU64::new(0),
            backend_errors: AtomicU64::new(0),
        }
    }
}

// ─── Connection Pool ─────────────────────────────────────────────────

pub(crate) struct Pool {
    conns: Mutex<Vec<TcpStream>>,
    sem: Semaphore,
    addr: String,
    connect_timeout: Duration,
    size: usize,
    label: String,
}

impl Pool {
    async fn new(
        addr: &str,
        size: usize,
        connect_timeout: Duration,
        label: &str,
    ) -> Result<Arc<Self>, String> {
        let pool = Arc::new(Self {
            conns: Mutex::new(Vec::with_capacity(size)),
            sem: Semaphore::new(0),
            addr: addr.to_string(),
            connect_timeout,
            size,
            label: label.to_string(),
        });
        for i in 0..size {
            let conn = pool
                .connect_backend()
                .await
                .map_err(|e| format!("{} init {}/{}: {}", label, i + 1, size, e))?;
            pool.conns.lock().await.push(conn);
            pool.sem.add_permits(1);
        }
        Ok(pool)
    }

    pub(crate) async fn get(&self) -> Result<TcpStream, String> {
        let permit = self
            .sem
            .acquire()
            .await
            .map_err(|_| "pool closed".to_string())?;
        permit.forget();
        let mut conns = self.conns.lock().await;
        match conns.pop() {
            Some(conn) => Ok(conn),
            None => {
                drop(conns);
                self.connect_backend()
                    .await
                    .map_err(|e| format!("{} fallback connect: {}", self.label, e))
            }
        }
    }

    pub(crate) async fn put(&self, conn: TcpStream) {
        self.conns.lock().await.push(conn);
        self.sem.add_permits(1);
    }

    pub(crate) fn spawn_replace(pool: Arc<Pool>) {
        let label = pool.label.clone();
        tokio::spawn(async move {
            let mut delay = Duration::from_millis(500);
            let max_delay = Duration::from_secs(10);
            loop {
                match pool.connect_backend().await {
                    Ok(conn) => {
                        pool.conns.lock().await.push(conn);
                        pool.sem.add_permits(1);
                        eprintln!("[oxipool] {} connection replaced", label);
                        return;
                    }
                    Err(e) => {
                        eprintln!(
                            "[oxipool] {} reconnect failed (retry in {:?}): {}",
                            label, delay, e
                        );
                        tokio::time::sleep(delay).await;
                        delay = (delay * 2).min(max_delay);
                    }
                }
            }
        });
    }

    async fn connect_backend(&self) -> Result<TcpStream, std::io::Error> {
        let conn = timeout(self.connect_timeout, TcpStream::connect(&self.addr))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timeout"))??;
        conn.set_nodelay(true)?;
        Ok(conn)
    }

    fn available(&self) -> usize {
        self.sem.available_permits()
    }
}

// ─── Replica Router (round-robin) ────────────────────────────────────

struct ReplicaRouter {
    pools: Vec<Arc<Pool>>,
    next: AtomicU64,
}

impl ReplicaRouter {
    fn get_pool(&self) -> &Arc<Pool> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) as usize % self.pools.len();
        &self.pools[idx]
    }

    fn total_available(&self) -> usize {
        self.pools.iter().map(|p| p.available()).sum()
    }

    fn total_size(&self) -> usize {
        self.pools.iter().map(|p| p.size).sum()
    }
}

// ─── Wire Protocol ───────────────────────────────────────────────────

const MAX_FRAME: usize = 16 * 1024 * 1024;

async fn read_frame(stream: &mut TcpStream) -> Result<Vec<u8>, std::io::Error> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("frame too large: {} bytes", len),
        ));
    }
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<(), std::io::Error> {
    let len_buf = (payload.len() as u32).to_le_bytes();
    stream.write_all(&len_buf).await?;
    stream.write_all(payload).await?;
    stream.flush().await
}

/// Read a frame from a CLIENT, bounded by the idle timeout (0 = unbounded).
/// On expiry the caller drops the client; any pinned transaction is rolled
/// back by the disconnect cleanup, returning its connection to the pool.
async fn read_client_frame(
    stream: &mut TcpStream,
    idle: Duration,
) -> Result<Vec<u8>, std::io::Error> {
    if idle.is_zero() {
        return read_frame(stream).await;
    }
    match timeout(idle, read_frame(stream)).await {
        Ok(r) => r,
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "client idle timeout",
        )),
    }
}

// ─── Command Classification (non-sharded fallback) ──────────────────

#[derive(PartialEq)]
enum CmdRoute {
    Write,      // → master
    Read,       // → replica (or master if no replicas)
    TxBegin,    // → master (pin)
    TxCommit,   // → pinned master
    TxRollback, // → pinned master
    /// Authentication — rejected: sessions are per-connection on the server
    /// while oxipool multiplexes pooled connections across clients.
    Auth,
}

/// Classify by PARSING the request's actual `cmd` field. The old substring
/// scan ran over the whole payload INCLUDING user data: a find whose
/// document contained the string "begin_tx" was classified as a transaction
/// begin — pinning (and leaking) a master pool connection per occurrence —
/// and any value containing "update"/"insert" silently rerouted reads to
/// the master.
fn classify_command(payload: &[u8]) -> CmdRoute {
    let json: serde_json::Value = match serde_json::from_slice(payload) {
        Ok(v) => v,
        Err(_) => return CmdRoute::Write, // binary/unknown → master (safe default)
    };
    let cmd = json.get("cmd").and_then(|v| v.as_str()).unwrap_or("");

    match cmd {
        "begin_tx" => CmdRoute::TxBegin,
        "commit_tx" => CmdRoute::TxCommit,
        "rollback_tx" => CmdRoute::TxRollback,
        "auth_simple" | "authenticate" | "auth" => CmdRoute::Auth,

        "insert"
        | "insert_many"
        | "update"
        | "update_one"
        | "find_and_modify"
        | "delete"
        | "delete_one"
        | "create_collection"
        | "create_collection_with_options"
        | "drop_collection"
        | "create_index"
        | "create_unique_index"
        | "create_composite_index"
        | "create_text_index"
        | "create_vector_index"
        | "create_ttl_index"
        | "drop_index"
        | "compact"
        | "create_bucket"
        | "delete_bucket"
        | "put_object"
        | "delete_object"
        | "create_database"
        | "drop_database"
        | "create_user"
        | "drop_user"
        | "update_user"
        | "grant_db_role"
        | "revoke_db_role"
        | "create_schedule"
        | "delete_schedule"
        | "enable_schedule"
        | "disable_schedule" => CmdRoute::Write,

        // Aggregations write only when the pipeline ends in $out / $merge.
        "aggregate" => {
            let writes = json
                .get("pipeline")
                .and_then(|p| p.as_array())
                .is_some_and(|stages| {
                    stages.iter().any(|s| {
                        s.as_object()
                            .is_some_and(|o| o.contains_key("$out") || o.contains_key("$merge"))
                    })
                });
            if writes {
                CmdRoute::Write
            } else {
                CmdRoute::Read
            }
        }

        "sql" => classify_sql(&json),

        _ => CmdRoute::Read,
    }
}

fn classify_sql(json: &serde_json::Value) -> CmdRoute {
    let query = json.get("query").and_then(|v| v.as_str()).unwrap_or("");
    let trimmed = query.trim_start().to_uppercase();
    if trimmed.starts_with("SELECT")
        || trimmed.starts_with("SHOW")
        || trimmed.starts_with("DESCRIBE")
        || trimmed.starts_with("EXPLAIN")
    {
        CmdRoute::Read
    } else {
        CmdRoute::Write
    }
}

// ─── Request Forwarding ─────────────────────────────────────────────

/// Which side of a proxied exchange failed. The distinction matters for
/// connection hygiene: a BACKEND failure means the pooled connection's
/// framing state is unknown (replace it), while a CLIENT-write failure
/// happens after a complete backend exchange — the backend connection is
/// perfectly healthy and must go back to the pool, not be discarded.
enum ForwardError {
    Backend(std::io::Error),
    Client(std::io::Error),
}

async fn forward(
    backend: &mut TcpStream,
    client: &mut TcpStream,
    request: &[u8],
) -> Result<(), ForwardError> {
    let exchange = async {
        write_frame(backend, request).await?;
        read_frame(backend).await
    };
    let response = match request_timeout() {
        Some(d) => match timeout(d, exchange).await {
            Ok(r) => r,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("backend request timed out after {:?}", d),
            )),
        },
        None => exchange.await,
    }
    .map_err(ForwardError::Backend)?;
    write_frame(client, &response)
        .await
        .map_err(ForwardError::Client)
}

// ─── Client Handler (non-sharded — original behavior) ───────────────

async fn handle_client(
    mut client: TcpStream,
    master: Arc<Pool>,
    replicas: Option<Arc<ReplicaRouter>>,
    stats: Arc<Stats>,
    idle_timeout: Duration,
) {
    let addr = client.peer_addr().ok();
    stats.active_clients.fetch_add(1, Ordering::Relaxed);

    let mut pinned: Option<TcpStream> = None;

    loop {
        let payload = match read_client_frame(&mut client, idle_timeout).await {
            Ok(p) => p,
            Err(_) => break,
        };

        stats.total_requests.fetch_add(1, Ordering::Relaxed);
        let route = classify_command(&payload);

        let result: Result<(), std::io::Error> = match route {
            CmdRoute::Auth => {
                let resp = b"{\"ok\":false,\"error\":\"authentication through oxipool is not supported: sessions are per-connection and oxipool multiplexes pooled connections; run backends with auth disabled\"}";
                write_frame(&mut client, resp).await
            }

            CmdRoute::TxBegin => {
                stats.master_requests.fetch_add(1, Ordering::Relaxed);
                match master.get().await {
                    Ok(mut backend) => match forward(&mut backend, &mut client, &payload).await {
                        Ok(()) => {
                            stats.active_transactions.fetch_add(1, Ordering::Relaxed);
                            pinned = Some(backend);
                            Ok(())
                        }
                        Err(ForwardError::Backend(e)) => {
                            Pool::spawn_replace(Arc::clone(&master));
                            Err(e)
                        }
                        Err(ForwardError::Client(e)) => {
                            // Backend exchange completed — the connection is
                            // healthy and the tx is live on it; pin it so the
                            // disconnect cleanup below rolls it back.
                            stats.active_transactions.fetch_add(1, Ordering::Relaxed);
                            pinned = Some(backend);
                            Err(e)
                        }
                    },
                    Err(e) => Err(std::io::Error::other(e)),
                }
            }

            CmdRoute::TxCommit | CmdRoute::TxRollback => {
                stats.master_requests.fetch_add(1, Ordering::Relaxed);
                if let Some(mut backend) = pinned.take() {
                    stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
                    match forward(&mut backend, &mut client, &payload).await {
                        Ok(()) => {
                            master.put(backend).await;
                            Ok(())
                        }
                        Err(ForwardError::Backend(e)) => {
                            Pool::spawn_replace(Arc::clone(&master));
                            Err(e)
                        }
                        Err(ForwardError::Client(e)) => {
                            // Commit/rollback reached the backend — done.
                            master.put(backend).await;
                            Err(e)
                        }
                    }
                } else {
                    forward_to_pool(&master, &mut client, &payload, &stats).await
                }
            }

            CmdRoute::Write => {
                stats.master_requests.fetch_add(1, Ordering::Relaxed);
                if let Some(ref mut backend) = pinned {
                    match forward(backend, &mut client, &payload).await {
                        Ok(()) => Ok(()),
                        Err(ForwardError::Backend(e)) => {
                            pinned = None;
                            stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
                            Pool::spawn_replace(Arc::clone(&master));
                            Err(e)
                        }
                        Err(ForwardError::Client(e)) => Err(e),
                    }
                } else {
                    forward_to_pool(&master, &mut client, &payload, &stats).await
                }
            }

            CmdRoute::Read => {
                if let Some(ref mut backend) = pinned {
                    stats.master_requests.fetch_add(1, Ordering::Relaxed);
                    match forward(backend, &mut client, &payload).await {
                        Ok(()) => Ok(()),
                        Err(ForwardError::Backend(e)) => {
                            pinned = None;
                            stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
                            Pool::spawn_replace(Arc::clone(&master));
                            Err(e)
                        }
                        Err(ForwardError::Client(e)) => Err(e),
                    }
                } else if let Some(ref router) = replicas {
                    stats.replica_requests.fetch_add(1, Ordering::Relaxed);
                    let pool = router.get_pool();
                    forward_to_pool(pool, &mut client, &payload, &stats).await
                } else {
                    stats.master_requests.fetch_add(1, Ordering::Relaxed);
                    forward_to_pool(&master, &mut client, &payload, &stats).await
                }
            }
        };

        if let Err(e) = result {
            stats.backend_errors.fetch_add(1, Ordering::Relaxed);
            let msg = format!("oxipool: {}", e);
            let resp = format!(
                "{{\"ok\":false,\"error\":\"{}\"}}",
                msg.replace('\\', "\\\\").replace('"', "\\\"")
            );
            if write_frame(&mut client, resp.as_bytes()).await.is_err() {
                break;
            }
        }
    }

    // Client disconnected — cleanup orphaned transaction
    if let Some(mut backend) = pinned {
        stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
        let rollback = b"{\"cmd\":\"rollback_tx\"}";
        if write_frame(&mut backend, rollback).await.is_ok() {
            let _ = read_frame(&mut backend).await;
            master.put(backend).await;
        } else {
            Pool::spawn_replace(Arc::clone(&master));
        }
    }

    stats.active_clients.fetch_sub(1, Ordering::Relaxed);
    if let Some(addr) = addr {
        eprintln!("[oxipool] client disconnected: {}", addr);
    }
}

// ─── Sharded Client Handler ─────────────────────────────────────────

/// Open a transaction on `shard_id`: borrow a connection and run `begin_tx`
/// on it. Returns the pinned connection with the tx live.
async fn pin_transaction(
    shard_pools: &[Arc<Pool>],
    shard_id: u32,
) -> Result<TcpStream, std::io::Error> {
    let pool = &shard_pools[shard_id as usize];
    let mut backend = pool.get().await.map_err(std::io::Error::other)?;
    let begin = b"{\"cmd\":\"begin_tx\"}";
    let exchange = async {
        write_frame(&mut backend, begin).await?;
        read_frame(&mut backend).await
    };
    let resp = match request_timeout() {
        Some(d) => match timeout(d, exchange).await {
            Ok(r) => r,
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "begin_tx timed out",
            )),
        },
        None => exchange.await,
    };
    match resp {
        Ok(bytes) => {
            let ok = serde_json::from_slice::<serde_json::Value>(&bytes)
                .ok()
                .and_then(|v| v.get("ok").and_then(|b| b.as_bool()))
                .unwrap_or(false);
            if ok {
                Ok(backend)
            } else {
                pool.put(backend).await;
                Err(std::io::Error::other("shard refused begin_tx"))
            }
        }
        Err(e) => {
            Pool::spawn_replace(Arc::clone(pool));
            Err(e)
        }
    }
}

async fn handle_client_sharded(
    mut client: TcpStream,
    shard_pools: Arc<Vec<Arc<Pool>>>,
    router: Arc<shard::ShardRouter>,
    stats: Arc<Stats>,
    idle_timeout: Duration,
) {
    let addr = client.peer_addr().ok();
    stats.active_clients.fetch_add(1, Ordering::Relaxed);

    // Transaction pinning: (shard_id, connection). `tx_pending` means the
    // client sent begin_tx but no shard has been chosen yet — pinning is
    // DEFERRED to the first statement that routes to a shard. The old code
    // pinned shard 0 blindly at begin_tx, which rejected every transaction
    // whose data lives on any other shard as "cross-shard".
    let mut pinned: Option<(u32, TcpStream)> = None;
    let mut tx_pending = false;

    loop {
        let payload = match read_client_frame(&mut client, idle_timeout).await {
            Ok(p) => p,
            Err(_) => break,
        };

        stats.total_requests.fetch_add(1, Ordering::Relaxed);

        // Parse command and determine routing
        let parsed = match shard::parse_and_route(&router, &payload).await {
            Ok(p) => p,
            Err(e) => {
                let resp = format!(
                    "{{\"ok\":false,\"error\":\"{}\"}}",
                    e.replace('\\', "\\\\").replace('"', "\\\"")
                );
                if write_frame(&mut client, resp.as_bytes()).await.is_err() {
                    break;
                }
                continue;
            }
        };

        // Resolve where a statement inside a transaction must run, pinning
        // lazily on first use. Returns None when the statement conflicts
        // with the pinned shard.
        let result: Result<(), std::io::Error> = match parsed.routing {
            shard::CommandRouting::Transaction => {
                stats.master_requests.fetch_add(1, Ordering::Relaxed);
                if parsed.cmd == "begin_tx" {
                    if pinned.is_some() || tx_pending {
                        let resp = b"{\"ok\":false,\"error\":\"transaction already active\"}";
                        write_frame(&mut client, resp).await
                    } else {
                        // Defer the shard choice; reply with a synthetic ok.
                        // (The real tx_id is allocated when the first keyed
                        // statement pins a shard; the server tracks the tx
                        // per connection, so clients never echo the id back.)
                        tx_pending = true;
                        let resp = b"{\"ok\":true,\"data\":{\"tx_id\":0}}";
                        write_frame(&mut client, resp).await
                    }
                } else {
                    // commit_tx / rollback_tx
                    if let Some((shard_id, mut backend)) = pinned.take() {
                        stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
                        match forward(&mut backend, &mut client, &payload).await {
                            Ok(()) => {
                                shard_pools[shard_id as usize].put(backend).await;
                                Ok(())
                            }
                            Err(ForwardError::Backend(e)) => {
                                Pool::spawn_replace(Arc::clone(&shard_pools[shard_id as usize]));
                                Err(e)
                            }
                            Err(ForwardError::Client(e)) => {
                                shard_pools[shard_id as usize].put(backend).await;
                                Err(e)
                            }
                        }
                    } else if tx_pending {
                        // Transaction never touched a shard — nothing to do.
                        tx_pending = false;
                        let resp: &[u8] = if parsed.cmd == "commit_tx" {
                            b"{\"ok\":true,\"data\":\"committed\"}"
                        } else {
                            b"{\"ok\":true,\"data\":\"rolled back\"}"
                        };
                        write_frame(&mut client, resp).await
                    } else {
                        // Not in a transaction — let shard 0 produce the
                        // server's own "no active transaction" error.
                        forward_to_pool(&shard_pools[0], &mut client, &payload, &stats).await
                    }
                }
            }

            shard::CommandRouting::Targeted(shard_id) => {
                if tx_pending && pinned.is_none() {
                    // First keyed statement of the transaction → pin here.
                    match pin_transaction(&shard_pools, shard_id).await {
                        Ok(backend) => {
                            stats.active_transactions.fetch_add(1, Ordering::Relaxed);
                            pinned = Some((shard_id, backend));
                            tx_pending = false;
                        }
                        Err(e) => {
                            tx_pending = false;
                            let msg = format!("oxipool: failed to start transaction: {}", e);
                            let resp = format!(
                                "{{\"ok\":false,\"error\":\"{}\"}}",
                                msg.replace('\\', "\\\\").replace('"', "\\\"")
                            );
                            if write_frame(&mut client, resp.as_bytes()).await.is_err() {
                                break;
                            }
                            continue;
                        }
                    }
                }
                if let Some((pinned_shard, ref mut backend)) = pinned {
                    // Inside transaction — must use pinned connection
                    if pinned_shard != shard_id {
                        // Cross-shard transaction — reject
                        let resp = b"{\"ok\":false,\"error\":\"cross-shard transactions not supported; use the same shard key within a transaction\"}";
                        write_frame(&mut client, resp).await
                    } else {
                        stats.shard_requests.fetch_add(1, Ordering::Relaxed);
                        match forward(backend, &mut client, &payload).await {
                            Ok(()) => Ok(()),
                            Err(ForwardError::Backend(e)) => {
                                let s = pinned.take().unwrap().0;
                                stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
                                Pool::spawn_replace(Arc::clone(&shard_pools[s as usize]));
                                Err(e)
                            }
                            Err(ForwardError::Client(e)) => Err(e),
                        }
                    }
                } else {
                    stats.shard_requests.fetch_add(1, Ordering::Relaxed);
                    let pool = &shard_pools[shard_id as usize];
                    forward_to_pool(pool, &mut client, &payload, &stats).await
                }
            }

            shard::CommandRouting::ScatterGather => {
                if tx_pending && pinned.is_none() {
                    // Un-keyed statement first — preserve the legacy
                    // single-shard behavior by pinning shard 0.
                    match pin_transaction(&shard_pools, 0).await {
                        Ok(backend) => {
                            stats.active_transactions.fetch_add(1, Ordering::Relaxed);
                            pinned = Some((0, backend));
                            tx_pending = false;
                        }
                        Err(e) => {
                            tx_pending = false;
                            let msg = format!("oxipool: failed to start transaction: {}", e);
                            let resp = format!(
                                "{{\"ok\":false,\"error\":\"{}\"}}",
                                msg.replace('\\', "\\\\").replace('"', "\\\"")
                            );
                            if write_frame(&mut client, resp.as_bytes()).await.is_err() {
                                break;
                            }
                            continue;
                        }
                    }
                }
                if let Some((_, ref mut backend)) = pinned {
                    // Inside transaction — scatter-gather not allowed, send to pinned shard
                    stats.master_requests.fetch_add(1, Ordering::Relaxed);
                    match forward(backend, &mut client, &payload).await {
                        Ok(()) => Ok(()),
                        Err(ForwardError::Backend(e)) => {
                            let s = pinned.take().unwrap().0;
                            stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
                            Pool::spawn_replace(Arc::clone(&shard_pools[s as usize]));
                            Err(e)
                        }
                        Err(ForwardError::Client(e)) => Err(e),
                    }
                } else {
                    stats.scatter_requests.fetch_add(1, Ordering::Relaxed);

                    // Command-aware fan-out: insert_many splits docs by
                    // shard; aggregate gets a pipeline-aware split+merge;
                    // find honors sort/skip/limit globally; ranked searches
                    // merge by score; the `_one` writes probe serially so at
                    // most ONE document changes cluster-wide.
                    let response = if parsed.cmd == "insert_many" {
                        scatter::scatter_insert_many(&shard_pools, &payload, &router).await
                    } else if parsed.cmd == "aggregate" {
                        scatter::scatter_aggregate(&shard_pools, &payload).await
                    } else if parsed.cmd == "find" {
                        scatter::scatter_find(&shard_pools, &payload).await
                    } else if parsed.cmd == "text_search" || parsed.cmd == "vector_search" {
                        scatter::scatter_search(&shard_pools, &payload, &parsed.cmd).await
                    } else if parsed.cmd == "update_one" || parsed.cmd == "delete_one" {
                        scatter::scatter_one_write(&shard_pools, &payload).await
                    } else {
                        let strategy = scatter::MergeStrategy::for_command(&parsed.cmd);
                        scatter::scatter_gather(&shard_pools, &payload, strategy).await
                    };

                    write_frame(&mut client, &response).await
                }
            }

            shard::CommandRouting::Broadcast => {
                if pinned.is_some() || tx_pending {
                    // Inside transaction — DDL not allowed
                    let resp = b"{\"ok\":false,\"error\":\"DDL commands not allowed inside transactions\"}";
                    write_frame(&mut client, resp).await
                } else {
                    stats.scatter_requests.fetch_add(1, Ordering::Relaxed);
                    let response = scatter::broadcast(&shard_pools, &payload).await;
                    write_frame(&mut client, &response).await
                }
            }

            shard::CommandRouting::Primary => {
                if tx_pending && pinned.is_none() {
                    match pin_transaction(&shard_pools, 0).await {
                        Ok(backend) => {
                            stats.active_transactions.fetch_add(1, Ordering::Relaxed);
                            pinned = Some((0, backend));
                            tx_pending = false;
                        }
                        Err(e) => {
                            tx_pending = false;
                            let msg = format!("oxipool: failed to start transaction: {}", e);
                            let resp = format!(
                                "{{\"ok\":false,\"error\":\"{}\"}}",
                                msg.replace('\\', "\\\\").replace('"', "\\\"")
                            );
                            if write_frame(&mut client, resp.as_bytes()).await.is_err() {
                                break;
                            }
                            continue;
                        }
                    }
                }
                if let Some((_, ref mut backend)) = pinned {
                    stats.master_requests.fetch_add(1, Ordering::Relaxed);
                    match forward(backend, &mut client, &payload).await {
                        Ok(()) => Ok(()),
                        Err(ForwardError::Backend(e)) => {
                            let s = pinned.take().unwrap().0;
                            stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
                            Pool::spawn_replace(Arc::clone(&shard_pools[s as usize]));
                            Err(e)
                        }
                        Err(ForwardError::Client(e)) => Err(e),
                    }
                } else {
                    stats.master_requests.fetch_add(1, Ordering::Relaxed);
                    forward_to_pool(&shard_pools[0], &mut client, &payload, &stats).await
                }
            }
        };

        if let Err(e) = result {
            stats.backend_errors.fetch_add(1, Ordering::Relaxed);
            let msg = format!("oxipool: {}", e);
            let resp = format!(
                "{{\"ok\":false,\"error\":\"{}\"}}",
                msg.replace('\\', "\\\\").replace('"', "\\\"")
            );
            if write_frame(&mut client, resp.as_bytes()).await.is_err() {
                break;
            }
        }
    }

    // Client disconnected — cleanup orphaned transaction
    if let Some((shard_id, mut backend)) = pinned {
        stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
        let rollback = b"{\"cmd\":\"rollback_tx\"}";
        if write_frame(&mut backend, rollback).await.is_ok() {
            let _ = read_frame(&mut backend).await;
            shard_pools[shard_id as usize].put(backend).await;
        } else {
            Pool::spawn_replace(Arc::clone(&shard_pools[shard_id as usize]));
        }
    }

    stats.active_clients.fetch_sub(1, Ordering::Relaxed);
    if let Some(addr) = addr {
        eprintln!("[oxipool] client disconnected: {}", addr);
    }
}

// ─── Pool Forwarding ────────────────────────────────────────────────

async fn forward_to_pool(
    pool: &Arc<Pool>,
    client: &mut TcpStream,
    payload: &[u8],
    stats: &Stats,
) -> Result<(), std::io::Error> {
    if pool.available() > 0 {
        stats.pool_hits.fetch_add(1, Ordering::Relaxed);
    } else {
        stats.pool_waits.fetch_add(1, Ordering::Relaxed);
    }

    let mut backend = pool.get().await.map_err(std::io::Error::other)?;

    match forward(&mut backend, client, payload).await {
        Ok(()) => {
            pool.put(backend).await;
            Ok(())
        }
        Err(ForwardError::Backend(e)) => {
            // Backend framing state unknown — replace the connection.
            Pool::spawn_replace(Arc::clone(pool));
            Err(e)
        }
        Err(ForwardError::Client(e)) => {
            // The backend exchange completed cleanly; only the client write
            // failed. The pooled connection is healthy — return it instead
            // of discarding it and paying a reconnect.
            pool.put(backend).await;
            Err(e)
        }
    }
}

/// Startup probe: backends must run with authentication DISABLED — oxipool
/// multiplexes pooled connections across clients, which is incompatible
/// with the server's per-connection (SCRAM) sessions. A data command on a
/// fresh connection answers "authentication required" on an auth-enabled
/// server (`ping` is exempt from auth, so it can't detect this).
async fn verify_backend_auth_disabled(pool: &Arc<Pool>, label: &str) {
    let probe = b"{\"cmd\":\"list_collections\"}";
    match scatter::forward_to_shard(pool, probe).await {
        Ok(resp) => {
            if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&resp) {
                let err = v.get("error").and_then(|e| e.as_str()).unwrap_or("");
                if v.get("ok").and_then(|b| b.as_bool()) != Some(true)
                    && err.to_ascii_lowercase().contains("auth")
                {
                    eprintln!(
                        "FATAL: backend {label} requires authentication ({err}); oxipool \
                         cannot proxy per-connection auth sessions over pooled connections. \
                         Run the backend with auth disabled and secure the network path."
                    );
                    std::process::exit(1);
                }
            }
        }
        Err(e) => {
            eprintln!("[oxipool] warning: startup probe of {label} failed: {e}");
        }
    }
}

// ─── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let config = Config::from_env();
    let _ = REQUEST_TIMEOUT.set(config.request_timeout);
    let shard_config = shard::ShardConfig::from_env();
    let has_replicas = !config.replicas.is_empty();
    let is_sharded = shard_config.is_some();

    eprintln!(
        "OxiPool v{} — connection pooler for OxiDB",
        env!("CARGO_PKG_VERSION")
    );
    eprintln!("  listen:       {}", config.listen);
    if config.request_timeout.is_zero() {
        eprintln!("  req_timeout:  disabled");
    } else {
        eprintln!("  req_timeout:  {:?}", config.request_timeout);
    }
    if !config.idle_timeout.is_zero() {
        eprintln!("  idle_timeout: {:?}", config.idle_timeout);
    }

    if is_sharded {
        // ─── Sharded mode ───────────────────────────────────────────
        let shard_cfg = shard_config.unwrap();
        let num_shards = shard_cfg.num_shards();
        eprintln!(
            "  mode:         SHARDED ({} shards, {} chunks)",
            num_shards, shard_cfg.num_chunks
        );
        for (i, addr) in shard_cfg.shards.iter().enumerate() {
            eprintln!("  shard[{}]:     {}", i, addr);
        }
        if !shard_cfg.collection_keys.is_empty() {
            for (coll, key) in &shard_cfg.collection_keys {
                eprintln!("  shard_key:    {} → {}", coll, key.field);
            }
        }
        eprintln!(
            "  shard_pool:   {} per shard ({} total)",
            config.shard_pool_size,
            config.shard_pool_size * num_shards
        );
        eprintln!("  max_clients:  {}", config.max_clients);

        // Create per-shard pools
        let mut shard_pools = Vec::with_capacity(num_shards);
        for (i, addr) in shard_cfg.shards.iter().enumerate() {
            let label = format!("shard[{}]", i);
            match Pool::new(addr, config.shard_pool_size, config.connect_timeout, &label).await {
                Ok(p) => {
                    eprintln!(
                        "  shard[{}]:     {} connections to {}",
                        i, config.shard_pool_size, addr
                    );
                    shard_pools.push(p);
                }
                Err(e) => {
                    eprintln!("FATAL: {}", e);
                    std::process::exit(1);
                }
            }
        }
        let shard_pools = Arc::new(shard_pools);
        let router = shard::ShardRouter::new(shard_cfg);

        // Fail fast on auth-enabled shards (incompatible with pooling).
        for (i, pool) in shard_pools.iter().enumerate() {
            verify_backend_auth_disabled(pool, &format!("shard[{i}]")).await;
        }

        // Cross-shard _id uniqueness depends on each shard running with a
        // DISTINCT OXIDB_SHARD_ID (disjoint 2^48 id ranges). oxipool cannot
        // verify it remotely — make the requirement impossible to miss.
        eprintln!(
            "  NOTE: each shard MUST run with a distinct OXIDB_SHARD_ID, or _id values \
             collide across shards and _id lookups through the pool return arbitrary documents"
        );

        let stats = Arc::new(Stats::new());

        // Periodic stats
        if config.stats_interval > Duration::ZERO {
            let s = Arc::clone(&stats);
            let sp = Arc::clone(&shard_pools);
            let interval = config.stats_interval;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                let mut last_total = 0u64;
                loop {
                    ticker.tick().await;
                    let total = s.total_requests.load(Ordering::Relaxed);
                    let rps = (total - last_total) as f64 / interval.as_secs_f64();
                    last_total = total;
                    let shard_avail: usize = sp.iter().map(|p| p.available()).sum();
                    let shard_total: usize = sp.iter().map(|p| p.size).sum();
                    eprintln!(
                        "[stats] reqs={} rps={:.0} targeted={} scatter={} primary={} clients={} tx={} shard_pool={}/{} errs={}",
                        total,
                        rps,
                        s.shard_requests.load(Ordering::Relaxed),
                        s.scatter_requests.load(Ordering::Relaxed),
                        s.master_requests.load(Ordering::Relaxed),
                        s.active_clients.load(Ordering::Relaxed),
                        s.active_transactions.load(Ordering::Relaxed),
                        shard_avail,
                        shard_total,
                        s.backend_errors.load(Ordering::Relaxed),
                    );
                }
            });
        }

        let client_limit = Arc::new(Semaphore::new(config.max_clients));

        let listener = TcpListener::bind(&config.listen).await.unwrap_or_else(|e| {
            eprintln!("FATAL: bind {}: {}", config.listen, e);
            std::process::exit(1);
        });
        eprintln!("OxiPool listening on {} (sharded)", config.listen);

        loop {
            let (client, addr) = tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("[oxipool] accept error: {}", e);
                        continue;
                    }
                },
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("[oxipool] shutdown signal — no longer accepting clients");
                    return;
                }
            };
            let _ = client.set_nodelay(true);

            let permit = match client_limit.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    eprintln!("[oxipool] max clients reached, rejecting {}", addr);
                    drop(client);
                    continue;
                }
            };

            let shard_pools = Arc::clone(&shard_pools);
            let router = Arc::clone(&router);
            let stats = Arc::clone(&stats);
            let idle = config.idle_timeout;

            tokio::spawn(async move {
                handle_client_sharded(client, shard_pools, router, stats, idle).await;
                drop(permit);
            });
        }
    } else {
        // ─── Non-sharded mode (original behavior) ──────────────────
        eprintln!("  mode:         SINGLE (non-sharded)");
        eprintln!("  master:       {}", config.master);
        if has_replicas {
            for (i, r) in config.replicas.iter().enumerate() {
                eprintln!("  replica[{}]:   {}", i, r);
            }
            eprintln!("  routing:      writes → master, reads → replicas (round-robin)");
        } else {
            eprintln!("  replicas:     none (all traffic → master)");
        }
        eprintln!("  master_pool:  {}", config.master_pool_size);
        if has_replicas {
            eprintln!(
                "  replica_pool: {} per replica ({} total)",
                config.replica_pool_size,
                config.replica_pool_size * config.replicas.len()
            );
        }
        eprintln!("  max_clients:  {}", config.max_clients);

        // Create master pool
        let master = match Pool::new(
            &config.master,
            config.master_pool_size,
            config.connect_timeout,
            "master",
        )
        .await
        {
            Ok(p) => p,
            Err(e) => {
                eprintln!("FATAL: {}", e);
                std::process::exit(1);
            }
        };
        eprintln!(
            "  master pool:  {} connections to {}",
            config.master_pool_size, config.master
        );
        verify_backend_auth_disabled(&master, "master").await;

        // Create replica pools
        let replicas = if has_replicas {
            let mut pools = Vec::new();
            for (i, addr) in config.replicas.iter().enumerate() {
                let label = format!("replica[{}]", i);
                match Pool::new(
                    addr,
                    config.replica_pool_size,
                    config.connect_timeout,
                    &label,
                )
                .await
                {
                    Ok(p) => {
                        eprintln!(
                            "  replica[{}]:   {} connections to {}",
                            i, config.replica_pool_size, addr
                        );
                        pools.push(p);
                    }
                    Err(e) => {
                        eprintln!("FATAL: {}", e);
                        std::process::exit(1);
                    }
                }
            }
            Some(Arc::new(ReplicaRouter {
                pools,
                next: AtomicU64::new(0),
            }))
        } else {
            None
        };

        let stats = Arc::new(Stats::new());

        // Periodic stats
        if config.stats_interval > Duration::ZERO {
            let s = Arc::clone(&stats);
            let m = Arc::clone(&master);
            let r = replicas.clone();
            let interval = config.stats_interval;
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                let mut last_total = 0u64;
                loop {
                    ticker.tick().await;
                    let total = s.total_requests.load(Ordering::Relaxed);
                    let rps = (total - last_total) as f64 / interval.as_secs_f64();
                    last_total = total;
                    let (r_avail, r_size) = match &r {
                        Some(router) => (router.total_available(), router.total_size()),
                        None => (0, 0),
                    };
                    eprintln!(
                        "[stats] reqs={} rps={:.0} master={} replica={} clients={} tx={} master_pool={}/{} replica_pool={}/{} errs={}",
                        total,
                        rps,
                        s.master_requests.load(Ordering::Relaxed),
                        s.replica_requests.load(Ordering::Relaxed),
                        s.active_clients.load(Ordering::Relaxed),
                        s.active_transactions.load(Ordering::Relaxed),
                        m.available(),
                        m.size,
                        r_avail,
                        r_size,
                        s.backend_errors.load(Ordering::Relaxed),
                    );
                }
            });
        }

        let client_limit = Arc::new(Semaphore::new(config.max_clients));

        let listener = TcpListener::bind(&config.listen).await.unwrap_or_else(|e| {
            eprintln!("FATAL: bind {}: {}", config.listen, e);
            std::process::exit(1);
        });
        eprintln!("OxiPool listening on {}", config.listen);

        loop {
            let (client, addr) = tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("[oxipool] accept error: {}", e);
                        continue;
                    }
                },
                _ = tokio::signal::ctrl_c() => {
                    eprintln!("[oxipool] shutdown signal — no longer accepting clients");
                    return;
                }
            };
            let _ = client.set_nodelay(true);

            let permit = match client_limit.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    eprintln!("[oxipool] max clients reached, rejecting {}", addr);
                    drop(client);
                    continue;
                }
            };

            let master = Arc::clone(&master);
            let replicas = replicas.clone();
            let stats = Arc::clone(&stats);
            let idle = config.idle_timeout;

            tokio::spawn(async move {
                handle_client(client, master, replicas, stats, idle).await;
                drop(permit);
            });
        }
    }
}
