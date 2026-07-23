//! The control plane's client to the data plane (`oxidb-server`) — now over
//! OxiDB's **native OxiWire protocol** (ADR-0021 follow-up), not REST. OxiBase
//! is an admin client of OxiDB: it provisions tenant databases with the
//! `create_database` command and stores its own accounts/projects in an
//! `oxibase` database. A single connection is reused behind a mutex and
//! re-dialed (with SCRAM re-auth) on error.

use std::sync::Mutex;

use oxidb_client::Client;
use serde_json::Value;

/// The metadata database OxiBase keeps its accounts + projects in.
pub const META_DB: &str = "oxibase";

pub struct Upstream {
    addr: String,
    user: Option<String>,
    password: Option<String>,
    conn: Mutex<Option<Client>>,
}

impl Upstream {
    /// `addr` is the wire server `host:port`. When `user`/`password` are set,
    /// each connection authenticates with SCRAM; otherwise it relies on the
    /// server having auth disabled (anonymous-admin).
    pub fn new(addr: String, user: Option<String>, password: Option<String>) -> Self {
        Self {
            addr,
            user,
            password,
            conn: Mutex::new(None),
        }
    }

    fn dial(&self) -> Result<Client, String> {
        let mut c = Client::connect(&self.addr).map_err(|e| e.to_string())?;
        if let (Some(u), Some(p)) = (&self.user, &self.password) {
            c.authenticate(u, p)?;
        }
        Ok(c)
    }

    /// Run `f` on a live connection, re-dialing once on failure.
    fn with_client<F, R>(&self, mut f: F) -> Result<R, String>
    where
        F: FnMut(&mut Client) -> Result<R, String>,
    {
        let mut guard = self.conn.lock().unwrap();
        let mut last = String::from("no attempt");
        for attempt in 0..2 {
            if guard.is_none() {
                match self.dial() {
                    Ok(c) => *guard = Some(c),
                    Err(e) => {
                        last = e;
                        continue;
                    }
                }
            }
            match f(guard.as_mut().unwrap()) {
                Ok(r) => return Ok(r),
                Err(e) => {
                    *guard = None; // force a reconnect on the next attempt
                    last = e;
                    if attempt == 1 {
                        break;
                    }
                }
            }
        }
        Err(last)
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
        let name = name.to_string();
        self.with_client(move |c| c.create_database(&name))
    }

    pub fn drop_database(&self, name: &str) -> Result<(), String> {
        let name = name.to_string();
        self.with_client(move |c| c.drop_database(&name))
    }

    pub fn insert(&self, col: &str, doc: &Value) -> Result<Value, String> {
        self.with_client(|c| c.insert(META_DB, col, doc))
    }

    pub fn find(&self, col: &str, query: &Value) -> Result<Vec<Value>, String> {
        self.with_client(|c| c.find(META_DB, col, query))
    }

    pub fn count(&self, col: &str, query: &Value) -> Result<usize, String> {
        Ok(self.find(col, query)?.len())
    }

    pub fn update(&self, col: &str, query: &Value, update: &Value) -> Result<(), String> {
        self.with_client(|c| c.update(META_DB, col, query, update))
    }

    pub fn delete(&self, col: &str, query: &Value) -> Result<(), String> {
        self.with_client(|c| c.delete(META_DB, col, query))
    }
}
