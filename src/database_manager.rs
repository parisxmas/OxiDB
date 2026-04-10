use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::crypto::EncryptionKey;
use crate::engine::{LogCallback, OxiDb};
use crate::error::{Error, Result};

/// Default database name — always exists and cannot be dropped.
pub const DEFAULT_DATABASE: &str = "oxidb";

/// Directories at the top level that are global (not per-database).
const GLOBAL_DIRS: &[&str] = &["_auth", "_audit"];

/// File extensions that belong to OxiDb collections.
const COLLECTION_EXTENSIONS: &[&str] = &["dat", "wal", "idx", "fidx", "cidx", "vidx"];

/// Manages multiple isolated OxiDb databases, each in its own subdirectory.
///
/// ```text
/// data_dir/
/// ├── _auth/              # global
/// ├── _audit/             # global
/// ├── oxidb/              # default database
/// │   ├── users.dat/.wal/.idx
/// │   └── ...
/// ├── myapp/              # user-created database
/// │   └── ...
/// └── postgres/           # alias for PG compat
/// ```
pub struct DatabaseManager {
    data_dir: PathBuf,
    databases: RwLock<HashMap<String, Arc<OxiDb>>>,
    encryption: Option<Arc<EncryptionKey>>,
    verbose: bool,
    log_callback: Option<LogCallback>,
    in_memory: bool,
}

impl DatabaseManager {
    /// Open the database manager, scanning for existing databases and auto-migrating
    /// old flat layouts.
    pub fn open(
        data_dir: &Path,
        encryption: Option<Arc<EncryptionKey>>,
        verbose: bool,
        log_callback: Option<LogCallback>,
    ) -> Result<Self> {
        std::fs::create_dir_all(data_dir)?;

        let mgr = Self {
            data_dir: data_dir.to_path_buf(),
            databases: RwLock::new(HashMap::new()),
            encryption,
            verbose,
            log_callback,
            in_memory: false,
        };

        // Auto-migrate old flat layout if needed.
        mgr.maybe_migrate()?;

        // Ensure default database directory exists.
        let default_dir = data_dir.join(DEFAULT_DATABASE);
        std::fs::create_dir_all(&default_dir)?;

        // Ensure "postgres" alias directory exists (for PG wire compat).
        let pg_dir = data_dir.join("postgres");
        std::fs::create_dir_all(&pg_dir)?;

        Ok(mgr)
    }

    /// Create a pure in-memory database manager.
    /// All databases and collections live only in RAM.
    pub fn open_in_memory() -> Result<Self> {
        let db = OxiDb::open_in_memory()?;
        let arc = Arc::new(db);
        let mut databases = HashMap::new();
        databases.insert(DEFAULT_DATABASE.to_string(), arc);

        Ok(Self {
            data_dir: PathBuf::new(),
            databases: RwLock::new(databases),
            encryption: None,
            verbose: false,
            log_callback: None,
            in_memory: true,
        })
    }

    /// Get or lazily open a database by name. Uses double-check locking.
    pub fn get_database(&self, name: &str) -> Result<Arc<OxiDb>> {
        // Normalize: "postgres" is an alias for "oxidb"
        let name = if name == "postgres" { DEFAULT_DATABASE } else { name };

        // Fast path: read lock
        {
            let dbs = self.databases.read().unwrap();
            if let Some(db) = dbs.get(name) {
                return Ok(Arc::clone(db));
            }
        }

        if self.in_memory {
            return Err(Error::DatabaseNotFound(name.to_string()));
        }

        #[cfg(not(target_arch = "wasm32"))]
        {
            // Check that the database directory exists
            let db_dir = self.data_dir.join(name);
            if !db_dir.exists() {
                return Err(Error::DatabaseNotFound(name.to_string()));
            }

            // Slow path: open the database
            let db = self.open_db_instance(&db_dir)?;
            let arc = Arc::new(db);

            // Insert with write lock (double-check)
            let mut dbs = self.databases.write().unwrap();
            if let Some(existing) = dbs.get(name) {
                return Ok(Arc::clone(existing));
            }
            dbs.insert(name.to_string(), Arc::clone(&arc));
            Ok(arc)
        }

        #[cfg(target_arch = "wasm32")]
        Err(Error::DatabaseNotFound(name.to_string()))
    }

    /// Get the default database.
    pub fn get_default_database(&self) -> Result<Arc<OxiDb>> {
        self.get_database(DEFAULT_DATABASE)
    }

    /// Create a new database. Returns error if it already exists.
    pub fn create_database(&self, name: &str) -> Result<()> {
        validate_database_name(name)?;

        {
            let dbs = self.databases.read().unwrap();
            if dbs.contains_key(name) {
                return Err(Error::DatabaseAlreadyExists(name.to_string()));
            }
        }

        #[cfg(not(target_arch = "wasm32"))]
        let db = if self.in_memory {
            OxiDb::open_in_memory()?
        } else {
            let db_dir = self.data_dir.join(name);
            if db_dir.exists() {
                return Err(Error::DatabaseAlreadyExists(name.to_string()));
            }
            std::fs::create_dir_all(&db_dir)?;
            self.open_db_instance(&db_dir)?
        };
        #[cfg(target_arch = "wasm32")]
        let db = OxiDb::open_in_memory()?;

        let mut dbs = self.databases.write().unwrap();
        dbs.insert(name.to_string(), Arc::new(db));

        Ok(())
    }

    /// Drop a database. Refuses to drop the default database.
    pub fn drop_database(&self, name: &str) -> Result<()> {
        if name == DEFAULT_DATABASE || name == "postgres" {
            return Err(Error::CannotDropDefaultDatabase);
        }

        let db_dir = self.data_dir.join(name);
        if !db_dir.exists() {
            return Err(Error::DatabaseNotFound(name.to_string()));
        }

        // Remove from in-memory map first.
        {
            let mut dbs = self.databases.write().unwrap();
            dbs.remove(name);
        }

        // Remove the directory.
        std::fs::remove_dir_all(&db_dir)?;

        Ok(())
    }

    /// List all database names by scanning subdirectories.
    pub fn list_databases(&self) -> Vec<String> {
        let mut names = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.data_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if !path.is_dir() {
                    continue;
                }
                let dir_name = match path.file_name().and_then(|n| n.to_str()) {
                    Some(n) => n.to_string(),
                    None => continue,
                };
                // Skip global directories.
                if GLOBAL_DIRS.contains(&dir_name.as_str()) {
                    continue;
                }
                // Skip hidden directories.
                if dir_name.starts_with('.') {
                    continue;
                }
                names.push(dir_name);
            }
        }
        names.sort();
        names
    }

    /// Check if a database exists.
    pub fn database_exists(&self, name: &str) -> bool {
        let name = if name == "postgres" { DEFAULT_DATABASE } else { name };
        self.data_dir.join(name).is_dir()
    }

    /// Start the scheduler on the default database.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn start_scheduler(&self) -> Result<()> {
        let db = self.get_default_database()?;
        db.start_scheduler();
        Ok(())
    }

    /// Return the path to the data directory (for auth/audit which are global).
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    // -----------------------------------------------------------------------
    // Internal
    // -----------------------------------------------------------------------

    #[cfg(not(target_arch = "wasm32"))]
    fn open_db_instance(&self, db_dir: &Path) -> Result<OxiDb> {
        if let Some(ref cb) = self.log_callback {
            OxiDb::open_with_log(
                db_dir,
                self.encryption.clone(),
                self.verbose,
                Arc::clone(cb),
            )
        } else {
            OxiDb::open_verbose(db_dir, self.encryption.clone(), self.verbose)
        }
    }

    /// Auto-migration: if the data_dir has `.dat` files at the top level
    /// (old flat layout), move them into the default database subdirectory.
    fn maybe_migrate(&self) -> Result<()> {
        let has_dat_files = std::fs::read_dir(&self.data_dir)?
            .filter_map(|e| e.ok())
            .any(|e| {
                e.path()
                    .extension()
                    .and_then(|ext| ext.to_str())
                    == Some("dat")
            });

        if !has_dat_files {
            return Ok(());
        }

        if self.verbose {
            eprintln!(
                "[database_manager] migrating flat layout to multi-database layout"
            );
        }

        let target = self.data_dir.join(DEFAULT_DATABASE);
        std::fs::create_dir_all(&target)?;

        // Move collection files (.dat, .wal, .idx, .fidx, .cidx, .vidx)
        for entry in std::fs::read_dir(&self.data_dir)?.filter_map(|e| e.ok()) {
            let path = entry.path();

            if path.is_file() {
                let ext = path
                    .extension()
                    .and_then(|e| e.to_str())
                    .unwrap_or("");
                if COLLECTION_EXTENSIONS.contains(&ext) {
                    let file_name = path.file_name().unwrap();
                    let dest = target.join(file_name);
                    std::fs::rename(&path, &dest)?;
                }
            }
        }

        // Move _blobs, _fts, _tx_commit_log into the default db directory
        for name in &["_blobs", "_fts", "_tx_commit_log"] {
            let src = self.data_dir.join(name);
            if src.exists() {
                let dest = target.join(name);
                std::fs::rename(&src, &dest)?;
            }
        }

        if self.verbose {
            eprintln!("[database_manager] migration complete");
        }

        Ok(())
    }
}

/// Validate that a database name is safe and well-formed.
fn validate_database_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::InvalidDatabaseName(
            "database name cannot be empty".into(),
        ));
    }
    if name.len() > 64 {
        return Err(Error::InvalidDatabaseName(
            "database name too long (max 64 characters)".into(),
        ));
    }
    if name.starts_with('_') {
        return Err(Error::InvalidDatabaseName(
            "database name cannot start with underscore".into(),
        ));
    }
    if name.starts_with('.') {
        return Err(Error::InvalidDatabaseName(
            "database name cannot start with dot".into(),
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(Error::InvalidDatabaseName(
            "database name can only contain alphanumeric characters, underscores, and hyphens"
                .into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn temp_mgr() -> (tempfile::TempDir, DatabaseManager) {
        let dir = tempdir().unwrap();
        let mgr = DatabaseManager::open(dir.path(), None, false, None).unwrap();
        (dir, mgr)
    }

    #[test]
    fn default_database_exists() {
        let (_dir, mgr) = temp_mgr();
        let db = mgr.get_default_database().unwrap();
        db.insert("test", json!({"x": 1})).unwrap();
        let docs = db.find("test", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn list_databases_includes_default() {
        let (_dir, mgr) = temp_mgr();
        let dbs = mgr.list_databases();
        assert!(dbs.contains(&"oxidb".to_string()));
        assert!(dbs.contains(&"postgres".to_string()));
    }

    #[test]
    fn create_and_use_database() {
        let (_dir, mgr) = temp_mgr();
        mgr.create_database("myapp").unwrap();

        let db = mgr.get_database("myapp").unwrap();
        db.insert("users", json!({"name": "Alice"})).unwrap();

        let docs = db.find("users", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);

        // Default db should not have this collection
        let default_db = mgr.get_default_database().unwrap();
        let default_docs = default_db.find("users", &json!({})).unwrap();
        assert_eq!(default_docs.len(), 0);
    }

    #[test]
    fn create_database_already_exists() {
        let (_dir, mgr) = temp_mgr();
        mgr.create_database("myapp").unwrap();
        let err = mgr.create_database("myapp").unwrap_err();
        assert!(matches!(err, Error::DatabaseAlreadyExists(_)));
    }

    #[test]
    fn drop_database() {
        let (_dir, mgr) = temp_mgr();
        mgr.create_database("myapp").unwrap();
        mgr.drop_database("myapp").unwrap();

        assert!(matches!(
            mgr.get_database("myapp"),
            Err(Error::DatabaseNotFound(_))
        ));
    }

    #[test]
    fn drop_default_database_fails() {
        let (_dir, mgr) = temp_mgr();
        assert!(matches!(
            mgr.drop_database("oxidb"),
            Err(Error::CannotDropDefaultDatabase)
        ));
    }

    #[test]
    fn drop_postgres_alias_fails() {
        let (_dir, mgr) = temp_mgr();
        assert!(matches!(
            mgr.drop_database("postgres"),
            Err(Error::CannotDropDefaultDatabase)
        ));
    }

    #[test]
    fn get_nonexistent_database() {
        let (_dir, mgr) = temp_mgr();
        assert!(matches!(
            mgr.get_database("nonexistent"),
            Err(Error::DatabaseNotFound(_))
        ));
    }

    #[test]
    fn postgres_alias_maps_to_default() {
        let (_dir, mgr) = temp_mgr();
        let default = mgr.get_default_database().unwrap();
        default.insert("test", json!({"x": 1})).unwrap();

        let pg = mgr.get_database("postgres").unwrap();
        let docs = pg.find("test", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn invalid_database_names() {
        assert!(validate_database_name("").is_err());
        assert!(validate_database_name("_secret").is_err());
        assert!(validate_database_name(".hidden").is_err());
        assert!(validate_database_name("has spaces").is_err());
        assert!(validate_database_name("has/slash").is_err());
        assert!(validate_database_name(&"a".repeat(65)).is_err());

        assert!(validate_database_name("myapp").is_ok());
        assert!(validate_database_name("my-app").is_ok());
        assert!(validate_database_name("my_app_2").is_ok());
    }

    #[test]
    fn auto_migration_from_flat_layout() {
        let dir = tempdir().unwrap();
        let data_dir = dir.path();

        // Simulate old flat layout: create .dat files at top level
        std::fs::write(data_dir.join("users.dat"), b"fake_data").unwrap();
        std::fs::write(data_dir.join("users.wal"), b"fake_wal").unwrap();
        std::fs::write(data_dir.join("orders.dat"), b"fake_data").unwrap();

        // Create _auth (should stay at top level)
        std::fs::create_dir_all(data_dir.join("_auth")).unwrap();
        std::fs::write(data_dir.join("_auth/users.json"), b"{}").unwrap();

        // Create _blobs (should be moved)
        std::fs::create_dir_all(data_dir.join("_blobs")).unwrap();
        std::fs::write(data_dir.join("_blobs/test.data"), b"blob").unwrap();

        // Open the manager — should trigger migration
        let _mgr = DatabaseManager::open(data_dir, None, false, None).unwrap();

        // Verify: .dat files moved to oxidb/
        assert!(!data_dir.join("users.dat").exists());
        assert!(!data_dir.join("users.wal").exists());
        assert!(!data_dir.join("orders.dat").exists());
        assert!(data_dir.join("oxidb/users.dat").exists());
        assert!(data_dir.join("oxidb/users.wal").exists());
        assert!(data_dir.join("oxidb/orders.dat").exists());

        // _auth should stay at top level
        assert!(data_dir.join("_auth/users.json").exists());

        // _blobs should be moved
        assert!(!data_dir.join("_blobs").exists());
        assert!(data_dir.join("oxidb/_blobs/test.data").exists());
    }

    #[test]
    fn databases_are_isolated() {
        let (_dir, mgr) = temp_mgr();
        mgr.create_database("db1").unwrap();
        mgr.create_database("db2").unwrap();

        let db1 = mgr.get_database("db1").unwrap();
        let db2 = mgr.get_database("db2").unwrap();

        db1.insert("shared_name", json!({"from": "db1"})).unwrap();
        db2.insert("shared_name", json!({"from": "db2"})).unwrap();

        let docs1 = db1.find("shared_name", &json!({})).unwrap();
        let docs2 = db2.find("shared_name", &json!({})).unwrap();

        assert_eq!(docs1.len(), 1);
        assert_eq!(docs1[0]["from"], "db1");
        assert_eq!(docs2.len(), 1);
        assert_eq!(docs2[0]["from"], "db2");
    }
}
