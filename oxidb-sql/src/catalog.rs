//! Persistent schema catalog.
//!
//! The catalog is the SQL engine's map of table name -> table definition. It is
//! persisted as `sql/catalog.json`, written atomically (temp file + rename) so a
//! crash never leaves a half-written catalog. Table *data* lives elsewhere (the
//! per-table `.rdat` snapshots and the WAL); the catalog only holds structure.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{Result, SqlError};
use crate::types::{SqlType, Value};

/// One column of a table.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: SqlType,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub primary_key: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: SqlType) -> Self {
        Column {
            name: name.into(),
            ty,
            nullable: true,
            primary_key: false,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }
}

/// A table definition: an ordered list of typed columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
}

impl Table {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Table {
            name: name.into(),
            columns,
        }
    }

    /// Number of columns; a valid row has exactly this many cells.
    pub fn arity(&self) -> usize {
        self.columns.len()
    }

    /// Position of the PRIMARY KEY column, if the table has one.
    pub fn pk_pos(&self) -> Option<usize> {
        self.columns.iter().position(|c| c.primary_key)
    }

    /// Apply implicit numeric coercions a SQL user expects: an integer value
    /// destined for a `DOUBLE` column widens to a float, and an integer
    /// destined for a `TIMESTAMP` column is taken as epoch milliseconds.
    pub fn coerce_row(&self, cells: &mut [Value]) {
        for (col, cell) in self.columns.iter().zip(cells.iter_mut()) {
            match (col.ty, &*cell) {
                (SqlType::Double, Value::Int(i)) => *cell = Value::Double(*i as f64),
                (SqlType::Timestamp, Value::Int(i)) => *cell = Value::Timestamp(*i),
                _ => {}
            }
        }
    }

    /// Validate a candidate row against this schema (arity, per-column type,
    /// and nullability). Returns `Err(SchemaMismatch)` describing the first
    /// problem found.
    pub fn validate_row(&self, cells: &[Value]) -> Result<()> {
        if cells.len() != self.columns.len() {
            return Err(SqlError::SchemaMismatch(format!(
                "table {:?} expects {} columns, got {}",
                self.name,
                self.columns.len(),
                cells.len()
            )));
        }
        for (col, cell) in self.columns.iter().zip(cells) {
            if matches!(cell, Value::Null) {
                if !col.nullable {
                    return Err(SqlError::SchemaMismatch(format!(
                        "column {:?} is NOT NULL but got NULL",
                        col.name
                    )));
                }
            } else if !cell.matches_type(col.ty) {
                return Err(SqlError::SchemaMismatch(format!(
                    "column {:?} expects {:?}, got incompatible value",
                    col.name, col.ty
                )));
            }
        }
        Ok(())
    }
}

/// A secondary index definition over one or more columns.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IndexDef {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
}

// Older catalogs / WAL records store a single `"column": "x"` field; accept
// both shapes on read (new writes always use `columns`).
impl<'de> Deserialize<'de> for IndexDef {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Wire {
            name: String,
            table: String,
            #[serde(default)]
            column: Option<String>,
            #[serde(default)]
            columns: Vec<String>,
        }
        let w = Wire::deserialize(deserializer)?;
        let columns = if w.columns.is_empty() {
            match w.column {
                Some(c) => vec![c],
                None => {
                    return Err(serde::de::Error::custom("index definition has no columns"));
                }
            }
        } else {
            w.columns
        };
        Ok(IndexDef {
            name: w.name,
            table: w.table,
            columns,
        })
    }
}

/// The in-memory catalog plus where it persists.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Catalog {
    pub tables: BTreeMap<String, Table>,
    /// Secondary indexes, keyed by index name.
    #[serde(default)]
    pub indexes: BTreeMap<String, IndexDef>,
}

impl Catalog {
    /// Load `catalog.json` from `dir`, or return an empty catalog if absent.
    pub fn load(dir: &Path) -> Result<Catalog> {
        let path = Self::path(dir);
        match fs::read(&path) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Catalog::default()),
            Err(e) => Err(e.into()),
        }
    }

    /// Atomically persist the catalog to `dir/catalog.json`.
    pub fn save(&self, dir: &Path) -> Result<()> {
        let path = Self::path(dir);
        let tmp = path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(self)?;
        fs::write(&tmp, &bytes)?;
        // fsync the temp file before rename so the rename can't expose an
        // unflushed file after a crash.
        fs::File::open(&tmp)?.sync_all()?;
        fs::rename(&tmp, &path)?;
        Ok(())
    }

    fn path(dir: &Path) -> PathBuf {
        dir.join("catalog.json")
    }

    pub fn get(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn contains(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users_table() -> Table {
        Table::new(
            "users",
            vec![
                Column::new("id", SqlType::Int).primary_key(),
                Column::new("name", SqlType::Text).not_null(),
                Column::new("age", SqlType::Int),
            ],
        )
    }

    #[test]
    fn catalog_persist_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut cat = Catalog::default();
        cat.tables.insert("users".into(), users_table());
        cat.save(dir.path()).unwrap();

        let loaded = Catalog::load(dir.path()).unwrap();
        assert_eq!(loaded.get("users"), Some(&users_table()));
    }

    #[test]
    fn load_missing_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let cat = Catalog::load(dir.path()).unwrap();
        assert!(cat.tables.is_empty());
    }

    #[test]
    fn row_validation() {
        let t = users_table();
        // good row
        assert!(
            t.validate_row(&[Value::Int(1), Value::Text("ada".into()), Value::Int(30)])
                .is_ok()
        );
        // wrong arity
        assert!(t.validate_row(&[Value::Int(1)]).is_err());
        // NOT NULL violation on name
        assert!(
            t.validate_row(&[Value::Int(1), Value::Null, Value::Int(30)])
                .is_err()
        );
        // nullable age accepts NULL
        assert!(
            t.validate_row(&[Value::Int(1), Value::Text("ada".into()), Value::Null])
                .is_ok()
        );
        // type mismatch on id
        assert!(
            t.validate_row(&[
                Value::Text("x".into()),
                Value::Text("ada".into()),
                Value::Int(30)
            ])
            .is_err()
        );
    }
}
