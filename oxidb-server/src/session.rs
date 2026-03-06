use crate::auth::Role;
use crate::scram::ScramState;

/// Per-connection session tracking authentication state and current database.
pub struct Session {
    pub authenticated: bool,
    pub username: Option<String>,
    pub role: Option<Role>,
    pub scram_state: Option<ScramState>,
    pub current_database: String,
    pub sql_dialect: oxidb::SqlDialect,
}

impl Session {
    pub fn new() -> Self {
        Self {
            authenticated: false,
            username: None,
            role: None,
            scram_state: None,
            current_database: "oxidb".to_string(),
            sql_dialect: oxidb::SqlDialect::default(),
        }
    }

    /// Set the current database for this session.
    pub fn set_database(&mut self, name: String) {
        self.current_database = name;
    }

    /// Set the SQL dialect for this session.
    pub fn set_sql_dialect(&mut self, dialect: oxidb::SqlDialect) {
        self.sql_dialect = dialect;
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
