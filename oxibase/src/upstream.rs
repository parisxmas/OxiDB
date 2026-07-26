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

    /// One raw wire request (any command), returning its `data` payload.
    pub fn raw_call(&self, request: &Value) -> Result<Value, String> {
        self.pool.with(|c| c.call(request)).map_err(|e| e.to_string())
    }

    /// Collection names of an arbitrary database.
    pub fn list_collections_in(&self, db: &str) -> Result<Vec<String>, String> {
        self.raw_call(&serde_json::json!({ "cmd": "list_collections", "db": db }))
            .map(|d| {
                d.as_array()
                    .map(|a| {
                        a.iter()
                            .filter_map(|v| v.as_str().map(String::from))
                            .collect()
                    })
                    .unwrap_or_default()
            })
    }

    /// Sorted, limited find in the control plane's own metadata database.
    ///
    /// The plain [`find`](Self::find) returns everything it matches, which is
    /// fine for a project row and wrong for a user table.
    pub fn find_page(
        &self,
        col: &str,
        query: &Value,
        sort: &Value,
        limit: u64,
        skip: u64,
    ) -> Result<Vec<Value>, String> {
        self.find_sorted_in(META_DB, col, query, sort, limit, skip)
    }

    /// Sorted, limited find in an arbitrary database (raw wire `find` with
    /// options) — used to read the shared request-log sink in the default db.
    pub fn find_sorted_in(
        &self,
        db: &str,
        col: &str,
        query: &Value,
        sort: &Value,
        limit: u64,
        skip: u64,
    ) -> Result<Vec<Value>, String> {
        self.pool
            .with(|c| {
                c.call(&serde_json::json!({
                    "cmd": "find",
                    "db": db,
                    "collection": col,
                    "query": query,
                    "sort": sort,
                    "limit": limit,
                    "skip": skip,
                }))
                .map(|d| d.as_array().cloned().unwrap_or_default())
            })
            .map_err(|e| e.to_string())
    }
}
