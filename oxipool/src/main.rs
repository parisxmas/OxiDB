use std::env;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::timeout;

// ─── Config ──────────────────────────────────────────────────────────

struct Config {
    listen: String,
    master: String,
    replicas: Vec<String>,
    master_pool_size: usize,
    replica_pool_size: usize,
    max_clients: usize,
    connect_timeout: Duration,
    stats_interval: Duration,
}

impl Config {
    fn from_env() -> Self {
        let replicas_str = env::var("OXIPOOL_REPLICAS").unwrap_or_default();
        let replicas: Vec<String> = if replicas_str.is_empty() {
            vec![]
        } else {
            replicas_str.split(',').map(|s| s.trim().to_string()).collect()
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
        }
    }
}

// ─── Stats ───────────────────────────────────────────────────────────

struct Stats {
    total_requests: AtomicU64,
    master_requests: AtomicU64,
    replica_requests: AtomicU64,
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
            active_clients: AtomicI64::new(0),
            active_transactions: AtomicI64::new(0),
            pool_hits: AtomicU64::new(0),
            pool_waits: AtomicU64::new(0),
            backend_errors: AtomicU64::new(0),
        }
    }
}

// ─── Connection Pool ─────────────────────────────────────────────────

struct Pool {
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

    async fn get(&self) -> Result<TcpStream, String> {
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

    async fn put(&self, conn: TcpStream) {
        self.conns.lock().await.push(conn);
        self.sem.add_permits(1);
    }

    fn spawn_replace(pool: Arc<Pool>) {
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
                        eprintln!("[oxipool] {} reconnect failed (retry in {:?}): {}", label, delay, e);
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

// ─── Command Classification ─────────────────────────────────────────

#[derive(PartialEq)]
enum CmdRoute {
    Write,       // → master
    Read,        // → replica (or master if no replicas)
    TxBegin,     // → master (pin)
    TxCommit,    // → pinned master
    TxRollback,  // → pinned master
}

fn classify_command(payload: &[u8]) -> CmdRoute {
    let s = match std::str::from_utf8(payload) {
        Ok(s) => s,
        Err(_) => return CmdRoute::Write, // binary/unknown → master (safe default)
    };

    // Transaction commands
    if s.contains("\"begin_tx\"") {
        return CmdRoute::TxBegin;
    }
    if s.contains("\"commit_tx\"") {
        return CmdRoute::TxCommit;
    }
    if s.contains("\"rollback_tx\"") {
        return CmdRoute::TxRollback;
    }

    // Write commands → master
    if s.contains("\"insert\"")
        || s.contains("\"insert_many\"")
        || s.contains("\"update\"")
        || s.contains("\"update_one\"")
        || s.contains("\"delete\"")
        || s.contains("\"delete_one\"")
        || s.contains("\"create_collection\"")
        || s.contains("\"drop_collection\"")
        || s.contains("\"create_index\"")
        || s.contains("\"create_unique_index\"")
        || s.contains("\"create_composite_index\"")
        || s.contains("\"create_text_index\"")
        || s.contains("\"create_vector_index\"")
        || s.contains("\"drop_index\"")
        || s.contains("\"compact\"")
        || s.contains("\"create_bucket\"")
        || s.contains("\"delete_bucket\"")
        || s.contains("\"put_object\"")
        || s.contains("\"delete_object\"")
        || s.contains("\"create_database\"")
        || s.contains("\"drop_database\"")
        || s.contains("\"create_user\"")
        || s.contains("\"drop_user\"")
        || s.contains("\"update_user\"")
        || s.contains("\"grant_db_role\"")
        || s.contains("\"revoke_db_role\"")
        || s.contains("\"create_schedule\"")
        || s.contains("\"delete_schedule\"")
        || s.contains("\"enable_schedule\"")
        || s.contains("\"disable_schedule\"")
        || s.contains("\"pipeline\"")
    {
        return CmdRoute::Write;
    }

    // SQL: detect if it's a write or read
    if s.contains("\"sql\"") {
        return classify_sql(s);
    }

    // Everything else is a read → replica
    // find, find_one, count, aggregate, list_collections, list_indexes,
    // text_search, vector_search, search, ping, list_buckets, list_objects,
    // get_object, head_object, list_databases, list_users, list_schedules,
    // get_schedule, auth_simple
    CmdRoute::Read
}

fn classify_sql(s: &str) -> CmdRoute {
    // Extract the SQL query string and check if it's a read or write
    // Look for "query" field value
    if let Some(pos) = s.find("\"query\"") {
        let after = &s[pos..];
        // Find the query value after the colon and opening quote
        if let Some(colon) = after.find(':') {
            let value_part = after[colon + 1..].trim_start();
            let upper = value_part.to_uppercase();
            if upper.starts_with('"') {
                let inner = &upper[1..]; // skip opening quote
                let trimmed = inner.trim_start();
                if trimmed.starts_with("SELECT")
                    || trimmed.starts_with("SHOW")
                    || trimmed.starts_with("DESCRIBE")
                    || trimmed.starts_with("EXPLAIN")
                {
                    return CmdRoute::Read;
                }
            }
        }
    }
    CmdRoute::Write // INSERT, UPDATE, DELETE, CREATE, DROP → master
}

// ─── Request Forwarding ─────────────────────────────────────────────

async fn forward(
    backend: &mut TcpStream,
    client: &mut TcpStream,
    request: &[u8],
) -> Result<(), std::io::Error> {
    write_frame(backend, request).await?;
    let response = read_frame(backend).await?;
    write_frame(client, &response).await
}

// ─── Client Handler ─────────────────────────────────────────────────

async fn handle_client(
    mut client: TcpStream,
    master: Arc<Pool>,
    replicas: Option<Arc<ReplicaRouter>>,
    stats: Arc<Stats>,
) {
    let addr = client.peer_addr().ok();
    stats.active_clients.fetch_add(1, Ordering::Relaxed);

    let mut pinned: Option<TcpStream> = None;

    loop {
        let payload = match read_frame(&mut client).await {
            Ok(p) => p,
            Err(_) => break,
        };

        stats.total_requests.fetch_add(1, Ordering::Relaxed);
        let route = classify_command(&payload);

        let result = match route {
            CmdRoute::TxBegin => {
                // Transactions always go to master, pin the connection
                stats.master_requests.fetch_add(1, Ordering::Relaxed);
                match master.get().await {
                    Ok(mut backend) => match forward(&mut backend, &mut client, &payload).await {
                        Ok(()) => {
                            stats.active_transactions.fetch_add(1, Ordering::Relaxed);
                            pinned = Some(backend);
                            Ok(())
                        }
                        Err(e) => {
                            Pool::spawn_replace(Arc::clone(&master));
                            Err(e)
                        }
                    },
                    Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
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
                        Err(e) => {
                            Pool::spawn_replace(Arc::clone(&master));
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
                    // Inside transaction → use pinned master connection
                    match forward(backend, &mut client, &payload).await {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            pinned = None;
                            stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
                            Pool::spawn_replace(Arc::clone(&master));
                            Err(e)
                        }
                    }
                } else {
                    forward_to_pool(&master, &mut client, &payload, &stats).await
                }
            }

            CmdRoute::Read => {
                if let Some(ref mut backend) = pinned {
                    // Inside transaction → reads also go to pinned master
                    stats.master_requests.fetch_add(1, Ordering::Relaxed);
                    match forward(backend, &mut client, &payload).await {
                        Ok(()) => Ok(()),
                        Err(e) => {
                            pinned = None;
                            stats.active_transactions.fetch_sub(1, Ordering::Relaxed);
                            Pool::spawn_replace(Arc::clone(&master));
                            Err(e)
                        }
                    }
                } else if let Some(ref router) = replicas {
                    // Route to a replica
                    stats.replica_requests.fetch_add(1, Ordering::Relaxed);
                    let pool = router.get_pool();
                    forward_to_pool(pool, &mut client, &payload, &stats).await
                } else {
                    // No replicas configured — fall back to master
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

    let mut backend = pool
        .get()
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    match forward(&mut backend, client, payload).await {
        Ok(()) => {
            pool.put(backend).await;
            Ok(())
        }
        Err(e) => {
            Pool::spawn_replace(Arc::clone(pool));
            Err(e)
        }
    }
}

// ─── Main ────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    let config = Config::from_env();
    let has_replicas = !config.replicas.is_empty();

    eprintln!("OxiPool v0.2.0 — connection pooler for OxiDB");
    eprintln!("  listen:       {}", config.listen);
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

    // Create replica pools
    let replicas = if has_replicas {
        let mut pools = Vec::new();
        for (i, addr) in config.replicas.iter().enumerate() {
            let label = format!("replica[{}]", i);
            match Pool::new(addr, config.replica_pool_size, config.connect_timeout, &label).await {
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

    let listener = TcpListener::bind(&config.listen)
        .await
        .unwrap_or_else(|e| {
            eprintln!("FATAL: bind {}: {}", config.listen, e);
            std::process::exit(1);
        });
    eprintln!("OxiPool listening on {}", config.listen);

    loop {
        let (client, addr) = match listener.accept().await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("[oxipool] accept error: {}", e);
                continue;
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

        tokio::spawn(async move {
            handle_client(client, master, replicas, stats).await;
            drop(permit);
        });
    }
}
