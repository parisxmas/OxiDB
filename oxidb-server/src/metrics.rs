//! Prometheus metrics — zero-dependency global counter registry plus
//! text-exposition rendering (format `text/plain; version=0.0.4`).
//!
//! Scraped via `GET /metrics` on the REST listener (`OXIDB_HTTP_PORT`
//! must be set). Two kinds of series:
//!
//! - **Counters** — lock-free `AtomicU64`s bumped on the hot paths:
//!   every wire command (classed by kind) in
//!   `handler::handle_request_session`, every error response built by
//!   `handler::err_bytes`, every HTTP request in the REST router, and
//!   transaction commits/conflicts at the `commit_tx` branch.
//! - **Gauges** — computed at scrape time: process stats (RSS, CPU%,
//!   threads, uptime from [`crate::proc_stats::PROC_STATS`]), collection
//!   count, and per-collection document counts (index-backed, cheap).
//!
//! The endpoint is deliberately unauthenticated (standard Prometheus
//! practice — scrapers don't do JWT): bind `OXIDB_HTTP_PORT` on a
//! private interface if the REST API itself is private.

use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use oxidb::OxiDb;
use serde_json::json;

/// Process-wide metrics registry.
pub static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::default);

#[derive(Default)]
pub struct Metrics {
    /// HTTP requests seen by the REST router (any route).
    pub http_requests: AtomicU64,
    /// Error responses built by `handler::err_bytes` (any cause).
    pub errors: AtomicU64,

    // Wire commands by class (see `record_command`).
    pub cmd_insert: AtomicU64,
    pub cmd_find: AtomicU64,
    pub cmd_update: AtomicU64,
    pub cmd_delete: AtomicU64,
    pub cmd_count: AtomicU64,
    pub cmd_aggregate: AtomicU64,
    pub cmd_sql: AtomicU64,
    pub cmd_tx: AtomicU64,
    pub cmd_blob: AtomicU64,
    pub cmd_other: AtomicU64,

    /// Successful / conflict-aborted document-engine transaction commits.
    pub tx_commits: AtomicU64,
    pub tx_conflicts: AtomicU64,

    /// Wire operations that exceeded OXIDB_SLOW_QUERY_MS (see profiler.rs).
    pub slow_queries: AtomicU64,

    // OxiMem key-count gauges (updated by the sweeper thread).
    pub oximem_keys_strings: AtomicU64,
    pub oximem_keys_hashes: AtomicU64,
    pub oximem_keys_lists: AtomicU64,
    pub oximem_keys_sets: AtomicU64,
    pub oximem_keys_zsets: AtomicU64,

    // OxiMem (RESP) commands by class.
    pub oximem_reads: AtomicU64,
    pub oximem_writes: AtomicU64,
    pub oximem_tx: AtomicU64,
    pub oximem_other: AtomicU64,
}

impl Metrics {
    /// Classify and count one wire command. Called once per request at
    /// the session-handler chokepoint, so every surface that routes
    /// through it (sync TCP, async/cluster TCP) is covered.
    pub fn record_command(&self, cmd: &str) {
        let counter = match cmd {
            "insert" | "insert_many" => &self.cmd_insert,
            "find" | "find_one" | "get" => &self.cmd_find,
            "find_for_update" => &self.cmd_find,
            "update" | "update_one" | "find_and_modify" => &self.cmd_update,
            "delete" | "delete_one" => &self.cmd_delete,
            "count" => &self.cmd_count,
            "aggregate" => &self.cmd_aggregate,
            "sql" => &self.cmd_sql,
            "begin_tx" | "commit_tx" | "rollback_tx" => &self.cmd_tx,
            c if c.starts_with("blob_") || c.starts_with("bucket_") => &self.cmd_blob,
            _ => &self.cmd_other,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Classify and count one OxiMem RESP command.
    pub fn record_oximem(&self, cmd: &str) {
        let counter = match cmd {
            "GET" | "MGET" | "GETRANGE" | "STRLEN" | "EXISTS" | "TTL" | "PTTL" | "TYPE"
            | "KEYS" | "SCAN" | "HGET" | "HMGET" | "HGETALL" | "HKEYS" | "HVALS" | "HLEN"
            | "HEXISTS" | "HRANDFIELD" | "LRANGE" | "LLEN" | "LINDEX" | "SMEMBERS"
            | "SISMEMBER" | "SCARD" | "SINTER" | "SUNION" | "SDIFF" | "ZRANGE" | "ZREVRANGE"
            | "ZRANGEBYSCORE" | "ZREVRANGEBYSCORE" | "ZRANGEBYLEX" | "ZSCORE" | "ZCARD"
            | "ZCOUNT" | "ZRANK" | "ZREVRANK" | "RANDOMKEY" | "DBSIZE" => &self.oximem_reads,
            "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" => &self.oximem_tx,
            "SET" | "SETNX" | "SETEX" | "PSETEX" | "MSET" | "GETSET" | "GETDEL" | "SETRANGE"
            | "APPEND" | "INCR" | "DECR" | "INCRBY" | "DECRBY" | "INCRBYFLOAT" | "DEL"
            | "EXPIRE" | "PEXPIRE" | "EXPIREAT" | "PEXPIREAT" | "PERSIST" | "RENAME"
            | "COPY" | "HSET" | "HMSET" | "HSETNX" | "HDEL" | "HINCRBY" | "LPUSH" | "RPUSH"
            | "LPOP" | "RPOP" | "BLPOP" | "BRPOP" | "SADD" | "SREM" | "SINTERSTORE"
            | "SUNIONSTORE" | "SDIFFSTORE" | "ZADD" | "ZREM" | "ZINCRBY" | "ZPOPMIN"
            | "ZPOPMAX" | "BZPOPMIN" | "FLUSHALL" | "FLUSHDB" => &self.oximem_writes,
            _ => &self.oximem_other,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Escape a Prometheus label value: backslash, double quote, newline.
fn escape_label(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

fn counter(out: &mut String, name: &str, help: &str, v: u64) {
    out.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} counter\n{name} {v}\n"
    ));
}

fn gauge(out: &mut String, name: &str, help: &str, v: f64) {
    out.push_str(&format!(
        "# HELP {name} {help}\n# TYPE {name} gauge\n{name} {v}\n"
    ));
}

/// Render the full exposition for one scrape.
pub fn render_prometheus(db: &Arc<OxiDb>) -> String {
    let m = &*METRICS;
    let mut out = String::with_capacity(4096);

    gauge(&mut out, "oxidb_up", "1 while the server is serving.", 1.0);

    // ── Process (from /proc/self; zeros on non-Linux where unavailable) ──
    let snap = crate::proc_stats::PROC_STATS.snapshot();
    gauge(
        &mut out,
        "oxidb_uptime_seconds",
        "Seconds since server start.",
        snap["uptime_s"].as_f64().unwrap_or(0.0),
    );
    gauge(
        &mut out,
        "oxidb_process_resident_memory_bytes",
        "Resident set size in bytes.",
        snap["mem_rss_mb"].as_f64().unwrap_or(0.0) * 1024.0 * 1024.0,
    );
    gauge(
        &mut out,
        "oxidb_process_cpu_percent",
        "CPU usage percent since the previous scrape (0 on first scrape).",
        snap["cpu_percent"].as_f64().unwrap_or(0.0),
    );
    gauge(
        &mut out,
        "oxidb_process_threads",
        "OS threads in the server process.",
        snap["threads"].as_f64().unwrap_or(0.0),
    );

    // ── Request counters ──
    counter(
        &mut out,
        "oxidb_http_requests_total",
        "HTTP requests handled by the REST listener.",
        m.http_requests.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "oxidb_errors_total",
        "Error responses returned on the wire protocol.",
        m.errors.load(Ordering::Relaxed),
    );

    out.push_str(
        "# HELP oxidb_commands_total Wire commands processed, by class.\n\
         # TYPE oxidb_commands_total counter\n",
    );
    for (class, v) in [
        ("insert", &m.cmd_insert),
        ("find", &m.cmd_find),
        ("update", &m.cmd_update),
        ("delete", &m.cmd_delete),
        ("count", &m.cmd_count),
        ("aggregate", &m.cmd_aggregate),
        ("sql", &m.cmd_sql),
        ("tx", &m.cmd_tx),
        ("blob", &m.cmd_blob),
        ("other", &m.cmd_other),
    ] {
        out.push_str(&format!(
            "oxidb_commands_total{{class=\"{class}\"}} {}\n",
            v.load(Ordering::Relaxed)
        ));
    }

    counter(
        &mut out,
        "oxidb_tx_commits_total",
        "Document-engine transactions committed.",
        m.tx_commits.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "oxidb_tx_conflicts_total",
        "Document-engine transaction commits aborted by OCC conflict.",
        m.tx_conflicts.load(Ordering::Relaxed),
    );
    counter(
        &mut out,
        "oxidb_slow_queries_total",
        "Wire operations slower than OXIDB_SLOW_QUERY_MS (0 when profiler off).",
        m.slow_queries.load(Ordering::Relaxed),
    );

    // ── OxiMem (RESP) commands by class ──
    out.push_str(
        "# HELP oximem_commands_total OxiMem RESP commands processed, by class.\n\
         # TYPE oximem_commands_total counter\n",
    );
    for (class, v) in [
        ("read", m.oximem_reads.load(Ordering::Relaxed)),
        ("write", m.oximem_writes.load(Ordering::Relaxed)),
        ("tx", m.oximem_tx.load(Ordering::Relaxed)),
        ("other", m.oximem_other.load(Ordering::Relaxed)),
    ] {
        out.push_str(&format!("oximem_commands_total{{class=\"{class}\"}} {v}\n"));
    }
    out.push_str(
        "# HELP oximem_keys OxiMem live keys by type.\n# TYPE oximem_keys gauge\n",
    );
    for (t, v) in [
        ("string", m.oximem_keys_strings.load(Ordering::Relaxed)),
        ("hash", m.oximem_keys_hashes.load(Ordering::Relaxed)),
        ("list", m.oximem_keys_lists.load(Ordering::Relaxed)),
        ("set", m.oximem_keys_sets.load(Ordering::Relaxed)),
        ("zset", m.oximem_keys_zsets.load(Ordering::Relaxed)),
    ] {
        out.push_str(&format!("oximem_keys{{type=\"{t}\"}} {v}\n"));
    }

    // ── Engine gauges (scrape-time) ──
    let collections = db.list_collections();
    gauge(
        &mut out,
        "oxidb_collections",
        "Collections in the default database.",
        collections.len() as f64,
    );

    out.push_str(
        "# HELP oxidb_documents Documents per collection (index-backed count).\n\
         # TYPE oxidb_documents gauge\n",
    );
    let mut total: u64 = 0;
    for col in &collections {
        let n = db.count(col, &json!({})).unwrap_or(0) as u64;
        total += n;
        out.push_str(&format!(
            "oxidb_documents{{collection=\"{}\"}} {n}\n",
            escape_label(col)
        ));
    }
    gauge(
        &mut out,
        "oxidb_documents_total",
        "Documents across all collections in the default database.",
        total as f64,
    );

    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_expected_series() {
        let db = Arc::new(OxiDb::open_in_memory().unwrap());
        db.insert("m_test", json!({"a": 1})).unwrap();
        db.insert("m_test", json!({"a": 2})).unwrap();

        METRICS.record_command("insert");
        METRICS.record_command("find_one");
        METRICS.record_command("blob_put");
        METRICS.record_command("whatever");
        METRICS.record_error();

        let text = render_prometheus(&db);
        assert!(text.contains("oxidb_up 1"));
        assert!(text.contains("# TYPE oxidb_commands_total counter"));
        assert!(text.contains("oxidb_commands_total{class=\"insert\"}"));
        assert!(text.contains("oxidb_commands_total{class=\"blob\"}"));
        assert!(text.contains("oxidb_documents{collection=\"m_test\"} 2"));
        assert!(text.contains("oxidb_errors_total"));
        assert!(text.contains("oxidb_uptime_seconds"));
        // Counters are cumulative and process-global; just assert the
        // recorded classes are non-zero.
        for class in ["insert", "find", "blob", "other"] {
            let needle = format!("oxidb_commands_total{{class=\"{class}\"}} 0\n");
            assert!(
                !text.contains(&needle),
                "class {class} should be non-zero after record_command"
            );
        }
    }

    #[test]
    fn label_escaping() {
        assert_eq!(escape_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }
}
