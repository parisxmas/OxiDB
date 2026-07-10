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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: SqlType,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub primary_key: bool,
    /// `AUTO_INCREMENT` / `AUTOINCREMENT` / `GENERATED ... AS IDENTITY`:
    /// an INT PRIMARY KEY whose omitted (or NULL) insert values are assigned
    /// from a per-table counter. Old catalogs deserialize as `false`.
    #[serde(default)]
    pub auto_increment: bool,
    /// `DEFAULT <literal>`: the value an INSERT that omits this column gets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<Value>,
    /// Column-level `UNIQUE` — enforced on writes (NULLs are exempt,
    /// per SQL).
    #[serde(default)]
    pub unique: bool,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: SqlType) -> Self {
        Column {
            name: name.into(),
            ty,
            nullable: true,
            primary_key: false,
            auto_increment: false,
            default_value: None,
            unique: false,
        }
    }

    pub fn not_null(mut self) -> Self {
        self.nullable = false;
        self
    }

    pub fn auto_increment(mut self) -> Self {
        self.auto_increment = true;
        self
    }

    pub fn primary_key(mut self) -> Self {
        self.primary_key = true;
        self.nullable = false;
        self
    }
}

/// A table definition: an ordered list of typed columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
                // Binary columns accept base64 text (the JSON wire has no
                // byte type); invalid base64 fails type validation below.
                (SqlType::Blob, Value::Text(s)) => {
                    if let Ok(b) = base64_decode(s) {
                        *cell = Value::Bytes(b);
                    }
                }
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

/// The language a stored procedure is written in (ADR-0014).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ProcLanguage {
    /// A SQL-text body (`AS BEGIN dml; ... END`) — the default, and what
    /// every catalog/WAL record written before ADR-0014 deserializes as.
    #[default]
    Sql,
    /// Compiled Cobra bytecode (`LANGUAGE COBRA AS '<base64 .cobrac>'`).
    Cobra,
}

impl ProcLanguage {
    /// The lowercase name shown by `SHOW PROCEDURES`.
    pub fn as_str(self) -> &'static str {
        match self {
            ProcLanguage::Sql => "sql",
            ProcLanguage::Cobra => "cobra",
        }
    }
}

/// A stored procedure: declared parameters and a body of SQL statements.
/// Parameter references in the body were rewritten to `$1..$N` at creation,
/// so calling is exactly a parameterized batch execution.
///
/// A COBRA procedure (ADR-0014) instead stores validated `.cobrac` bytecode
/// in `bytecode`; `body` then holds a display placeholder. Both new fields
/// default, so catalogs and WAL records from older versions still load.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureDef {
    /// `(name, type)` per declared parameter, in call order.
    pub params: Vec<(String, SqlType)>,
    /// SQL: the body text (`stmt; stmt; ...`), params rewritten to `$N`.
    /// COBRA: `<cobra bytecode, N bytes>` (display only) — except between
    /// parse and CREATE-time validation, where it carries the raw base64
    /// payload the executor decodes.
    pub body: String,
    #[serde(default)]
    pub language: ProcLanguage,
    /// COBRA only: the decoded `.cobrac` bytes (base64 in JSON).
    #[serde(default, skip_serializing_if = "Vec::is_empty", with = "b64_bytes")]
    pub bytecode: Vec<u8>,
    /// COBRA only: the original `.cobra` source text, kept so tooling can show
    /// and edit the procedure. Optional — empty when the source wasn't
    /// supplied (a plain `AS '<bytecode>'` upload). Never executed.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub source: String,
}

/// serde adapter: `Vec<u8>` as a base64 string (a JSON number array would
/// quadruple the catalog/WAL footprint of stored bytecode).
mod b64_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&super::base64_encode(v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(d)?;
        super::base64_decode(&s)
            .map_err(|()| serde::de::Error::custom("invalid base64 in stored bytecode"))
    }
}

/// The in-memory catalog plus where it persists.
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Catalog {
    pub tables: BTreeMap<String, Table>,
    /// Secondary indexes, keyed by index name.
    #[serde(default)]
    pub indexes: BTreeMap<String, IndexDef>,
    /// Views: name -> the view body as SQL text (re-parsed on use).
    #[serde(default)]
    pub views: BTreeMap<String, String>,
    /// Stored procedures, keyed by name (their own namespace).
    #[serde(default)]
    pub procedures: BTreeMap<String, ProcedureDef>,
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

    /// A ProcedureDef serialized before ADR-0014 (no `language`/`bytecode`
    /// fields) must still deserialize — as a SQL procedure.
    #[test]
    fn legacy_procedure_def_deserializes() {
        let legacy = r#"{
            "params": [["kime", "text"], ["tutar", "double"]],
            "body": "UPDATE hesap SET bakiye = bakiye + $2 WHERE ad = $1"
        }"#;
        let def: ProcedureDef = serde_json::from_str(legacy).unwrap();
        assert_eq!(def.language, ProcLanguage::Sql);
        assert!(def.bytecode.is_empty());
        assert_eq!(def.params.len(), 2);

        // And a cobra def round-trips (bytecode as base64 text in JSON).
        let cobra = ProcedureDef {
            params: vec![("a".into(), SqlType::Int)],
            body: "<cobra bytecode, 3 bytes>".into(),
            language: ProcLanguage::Cobra,
            bytecode: vec![1, 2, 3],
            source: "def run(db, a) return a end".into(),
        };
        let json = serde_json::to_string(&cobra).unwrap();
        assert!(json.contains("\"AQID\""), "bytecode must be base64: {json}");
        let back: ProcedureDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back, cobra);

        // A sql def serializes without the bytecode field at all.
        let sql = ProcedureDef {
            params: vec![],
            body: "SELECT 1".into(),
            language: ProcLanguage::Sql,
            bytecode: vec![],
            source: String::new(),
        };
        let sj = serde_json::to_string(&sql).unwrap();
        assert!(!sj.contains("bytecode") && !sj.contains("source"), "{sj}");
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

/// Minimal RFC 4648 base64 (standard alphabet, `=` padding) — kept local so
/// the engine gains no dependency for one wire shim.
pub(crate) fn base64_decode(s: &str) -> std::result::Result<Vec<u8>, ()> {
    fn val(c: u8) -> std::result::Result<u32, ()> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(()),
        }
    }
    let s = s.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    for chunk in s.chunks(4) {
        let mut acc = 0u32;
        for &c in chunk {
            acc = (acc << 6) | val(c)?;
        }
        let bits = chunk.len() * 6;
        acc <<= 24 - bits;
        let bytes = [(acc >> 16) as u8, (acc >> 8) as u8, acc as u8];
        out.extend_from_slice(&bytes[..bits / 8]);
    }
    Ok(out)
}

pub(crate) fn base64_encode(b: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(b.len().div_ceil(3) * 4);
    for chunk in b.chunks(3) {
        let mut acc = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            acc |= (c as u32) << (16 - i * 8);
        }
        for i in 0..4 {
            if i <= chunk.len() {
                out.push(A[((acc >> (18 - i * 6)) & 63) as usize] as char);
            } else {
                out.push('=');
            }
        }
    }
    out
}
