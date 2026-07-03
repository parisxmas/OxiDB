use crate::auth::Role;
use crate::scram::ScramState;

/// Per-connection session tracking authentication state and current database.
pub struct Session {
    pub authenticated: bool,
    pub username: Option<String>,
    pub role: Option<Role>,
    pub scram_state: Option<ScramState>,
    pub current_database: String,
    /// The database the active transaction was begun against, if any. A
    /// transaction's buffered writes belong to one engine; requests that
    /// target a different database while it is open are rejected.
    pub tx_db: Option<String>,
    /// Wire-protocol version negotiated via HELLO. Defaults to v1 for clients
    /// that never call `hello` (backward-compat with pre-1.0 clients).
    pub wire_version: u32,
}

impl Session {
    pub fn new() -> Self {
        Self {
            authenticated: false,
            username: None,
            role: None,
            scram_state: None,
            current_database: "oxidb".to_string(),
            tx_db: None,
            wire_version: 1,
        }
    }

    /// Set the current database for this session.
    pub fn set_database(&mut self, name: String) {
        self.current_database = name;
    }

    /// Mark session as authenticated with given username and role.
    pub fn set_authenticated(&mut self, username: String, role: Role) {
        self.authenticated = true;
        self.username = Some(username);
        self.role = Some(role);
        self.scram_state = None;
    }

    /// Return true if the session is authenticated (or auth is not required).
    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    pub fn role(&self) -> Option<Role> {
        self.role
    }

    pub fn username_str(&self) -> &str {
        self.username.as_deref().unwrap_or("anonymous")
    }
}
