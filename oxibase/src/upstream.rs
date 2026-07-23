//! The control plane's client to the data plane (`oxidb-server`), over OxiDB's
//! **native OxiWire protocol** (ADR-0021 follow-up). OxiBase is an admin client
//! of OxiDB: it provisions tenant databases with `create_database` and stores
//! its own accounts/projects in an `oxibase` database. Connections are drawn
//! from an [`oxidb_client::Pool`] (concurrent, SCRAM-authenticated, self-healing).

use oxidb_client::Pool;
use serde_json::Value;

/// The metadata database OxiBase keeps its accounts + projects in.
pub const META_DB: &str = "oxibase";
/// Idle connections kept warm — sized to the listener's worker pool.
const POOL_MAX_IDLE: usize = 8;

pub struct Upstream {
    pool: Pool,
}

impl Upstream {
    /// `addr` is the wire server `host:port`. When `user`/`password` are set,
    /// every connection authenticates with SCRAM; otherwise it relies on the
    /// server having auth disabled (anonymous-admin).
    pub fn new(addr: String, user: Option<String>, password: Option<String>) -> Self {
        Self {
            pool: Pool::new(addr, user, password, POOL_MAX_IDLE),
        }
    }

    /// Ensure the metadata database exists (idempotent).
    pub fn ensure_meta_db(&self) -> Result<(), String> {
        self.create_database(META_DB).or_else(|e| {
            if e.contains("already exists") {
                Ok(())
            } else {
                Err(e)
            }
        })
    }

    pub fn create_database(&self, name: &str) -> Result<(), String> {
        self.pool
            .with(|c| c.create_database(name))
            .map_err(|e| e.to_string())
    }

    pub fn drop_database(&self, name: &str) -> Result<(), String> {
        self.pool
            .with(|c| c.drop_database(name))
            .map_err(|e| e.to_string())
    }

    pub fn insert(&self, col: &str, doc: &Value) -> Result<Value, String> {
        self.pool
            .with(|c| c.insert(META_DB, col, doc))
            .map_err(|e| e.to_string())
    }

    pub fn find(&self, col: &str, query: &Value) -> Result<Vec<Value>, String> {
        self.pool
            .with(|c| c.find(META_DB, col, query))
            .map_err(|e| e.to_string())
    }

    pub fn count(&self, col: &str, query: &Value) -> Result<usize, String> {
        Ok(self.find(col, query)?.len())
    }

    pub fn update(&self, col: &str, query: &Value, update: &Value) -> Result<(), String> {
        self.pool
            .with(|c| c.update(META_DB, col, query, update))
            .map_err(|e| e.to_string())
    }

    pub fn delete(&self, col: &str, query: &Value) -> Result<(), String> {
        self.pool
            .with(|c| c.delete(META_DB, col, query))
            .map_err(|e| e.to_string())
    }
}
