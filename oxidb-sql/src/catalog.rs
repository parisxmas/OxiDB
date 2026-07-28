//! Persistent schema catalog.
//!
//! The catalog is the SQL engine's map of table name -> table definition. It is
//! persisted as `sql/catalog.json`, written atomically (temp file + rename) so a
//! crash never leaves a half-written catalog. Table *data* lives elsewhere (the
//! per-table `.rdat` snapshots and the WAL); the catalog only holds structure.

use std::borrow::Cow;
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
    /// Declared max character length from `VARCHAR(n)` / `CHAR(n)` /
    /// `NVARCHAR(n)`. Enforced on write — a longer string is rejected with
    /// [`SqlError::ValueTooLong`]. `None` = unbounded `TEXT`. Old catalogs (and
    /// non-string columns) deserialize as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_len: Option<u32>,
    /// Declared width of an integer column, in bytes: `2` for `SMALLINT`, `4`
    /// for `INT`/`INTEGER`, `None` for `BIGINT` and for a bare integer.
    ///
    /// Every integer is *stored* as an i64 whatever it was declared; this
    /// records what the user asked for so a value outside that range is
    /// rejected on write ([`SqlError::IntegerOutOfRange`]) rather than
    /// silently widened. Old catalogs deserialize as `None`, which keeps their
    /// columns exactly as permissive as they were.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub int_width: Option<u8>,
    /// Set by a metadata-only `ALTER TABLE DROP COLUMN`: the column keeps its
    /// physical slot in every stored row (so the drop rewrites nothing) but is
    /// invisible to queries — filtered out of the schema the executor sees.
    /// Old catalogs deserialize as `false`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub dropped: bool,
}

fn is_false(b: &bool) -> bool {
    !*b
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
            max_len: None,
            int_width: None,
            dropped: false,
        }
    }

    /// The inclusive range an integer of this column's declared width may hold.
    /// `None` for a column that is not a width-restricted integer.
    pub fn int_range(&self) -> Option<(i64, i64)> {
        if self.ty != SqlType::Int {
            return None;
        }
        match self.int_width? {
            1 => Some((i64::from(i8::MIN), i64::from(i8::MAX))),
            2 => Some((i64::from(i16::MIN), i64::from(i16::MAX))),
            4 => Some((i64::from(i32::MIN), i64::from(i32::MAX))),
            _ => None,
        }
    }

    /// How this column's type is spelled in an error message.
    pub fn type_name(&self) -> &'static str {
        match (self.ty, self.int_width) {
            (SqlType::Int, Some(1)) => "TINYINT",
            (SqlType::Int, Some(2)) => "SMALLINT",
            (SqlType::Int, Some(4)) => "INT",
            (SqlType::Int, _) => "BIGINT",
            (SqlType::Double, _) => "DOUBLE",
            (SqlType::Text, _) => "TEXT",
            (SqlType::Bool, _) => "BOOL",
            (SqlType::Timestamp, _) => "TIMESTAMP",
            (SqlType::Blob, _) => "BLOB",
            (SqlType::Decimal, _) => "DECIMAL",
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

/// Referential action for a FOREIGN KEY's `ON DELETE` / `ON UPDATE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum FkAction {
    /// Reject the parent change while a child still references it. SQL
    /// `NO ACTION` and `RESTRICT` are treated identically here.
    #[default]
    NoAction,
    /// `ON DELETE CASCADE`: delete the referencing child rows too.
    Cascade,
    /// `ON DELETE SET NULL`: null out the child's referencing column.
    SetNull,
}

/// A single-column FOREIGN KEY: `column` in this table references
/// `parent_table(parent_column)`. Enforced on the child (INSERT/UPDATE must
/// find the parent) and on the parent (DELETE honours `on_delete`; changing a
/// referenced key is restricted). Multi-column foreign keys are accepted at
/// parse time but not represented here — they are not enforced (documented).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignKey {
    pub column: String,
    pub parent_table: String,
    pub parent_column: String,
    #[serde(default)]
    pub on_delete: FkAction,
    #[serde(default)]
    pub on_update: FkAction,
}

/// A table definition: an ordered list of typed columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    /// Single-column FOREIGN KEY constraints. Empty for old catalogs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_keys: Vec<ForeignKey>,
}

impl Table {
    pub fn new(name: impl Into<String>, columns: Vec<Column>) -> Self {
        Table {
            name: name.into(),
            columns,
            foreign_keys: Vec::new(),
        }
    }

    /// Attach foreign-key constraints (builder form used by the parser).
    pub fn with_foreign_keys(mut self, fks: Vec<ForeignKey>) -> Self {
        self.foreign_keys = fks;
        self
    }

    /// Number of *physical* columns; a stored row has exactly this many cells.
    /// Includes columns tombstoned by a lazy `DROP COLUMN` (they keep their
    /// slot on disk), so this is the arity `.rdat` snapshots decode at.
    pub fn arity(&self) -> usize {
        self.columns.len()
    }

    /// Positions of the PRIMARY KEY columns, in column order. Empty when the
    /// table has no primary key, one entry for a single-column key, several for
    /// a composite one — every member column carries the `primary_key` flag
    /// (as MySQL's `DESCRIBE` shows `PRI` on each key part).
    ///
    /// Key *order* is column order, not the order written in
    /// `PRIMARY KEY (b, a)`: uniqueness is set-semantics and lookups are
    /// full-key equality, so the two differ only in how the key is reported.
    pub fn pk_cols(&self) -> Vec<usize> {
        (0..self.columns.len())
            .filter(|&i| self.columns[i].primary_key)
            .collect()
    }

    /// Physical slot indices of the live (non-dropped) columns, in order. Maps
    /// a logical column position to its physical cell position. The identity
    /// `0..arity` until a column is dropped.
    pub fn live_slots(&self) -> Vec<usize> {
        (0..self.columns.len())
            .filter(|&i| !self.columns[i].dropped)
            .collect()
    }

    /// Whether any column has been tombstoned by a lazy `DROP COLUMN`.
    pub fn has_dropped(&self) -> bool {
        self.columns.iter().any(|c| c.dropped)
    }

    /// The query-visible schema: the physical columns with tombstoned ones
    /// removed. This is what the executor sees via `table_def`, so it never
    /// has to know about dropped columns. Borrowed (free) when nothing is
    /// dropped — the overwhelmingly common case.
    pub fn logical(&self) -> Cow<'_, Table> {
        if self.has_dropped() {
            Cow::Owned(Table {
                name: self.name.clone(),
                columns: self
                    .columns
                    .iter()
                    .filter(|c| !c.dropped)
                    .cloned()
                    .collect(),
                foreign_keys: self.foreign_keys.clone(),
            })
        } else {
            Cow::Borrowed(self)
        }
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
                        *cell = Value::Bytes((b).into());
                    }
                }
                // DECIMAL columns accept exact conversions: Int and Text
                // ('12.34') widen to Decimal; a Double converts best-effort
                // via its shortest decimal string. A bad Text/Double string is
                // left as-is so validation reports the type mismatch.
                (SqlType::Decimal, Value::Int(i)) => {
                    *cell = Value::Decimal(Box::new(crate::decimal::Decimal::from_i64(*i)));
                }
                (SqlType::Decimal, Value::Text(s)) => {
                    if let Some(d) = crate::decimal::Decimal::parse(s) {
                        *cell = Value::Decimal(Box::new(d));
                    }
                }
                (SqlType::Decimal, Value::Double(f)) => {
                    if let Some(d) = crate::decimal::Decimal::parse(&format!("{f}")) {
                        *cell = Value::Decimal(Box::new(d));
                    }
                }
                // A Decimal destined for a DOUBLE column drops to float.
                (SqlType::Double, Value::Decimal(d)) => *cell = Value::Double(d.to_f64()),
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
            } else if let (Some(max), Value::Text(s)) = (col.max_len, cell) {
                // VARCHAR(n)/CHAR(n) length is measured in characters, not bytes.
                let got = s.chars().count();
                if got as u64 > u64::from(max) {
                    return Err(SqlError::ValueTooLong {
                        column: col.name.clone(),
                        max,
                        got,
                    });
                }
            } else if let (Some((min, max)), Value::Int(v)) = (col.int_range(), cell)
                && (*v < min || *v > max)
            {
                return Err(SqlError::IntegerOutOfRange {
                    column: col.name.clone(),
                    type_name: col.type_name(),
                    value: *v,
                    min,
                    max,
                });
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
    /// Sequences, keyed by name. Backs EF Core's HiLo value generation
    /// (`CREATE SEQUENCE` + `NEXT VALUE FOR`).
    #[serde(default)]
    pub sequences: BTreeMap<String, SequenceDef>,
}

/// A named integer sequence. `next` is the value the next `NEXT VALUE FOR`
/// hands out; each call returns `next` then advances it by `increment`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SequenceDef {
    pub next: i64,
    pub increment: i64,
}

fn sequences_path(root: &Path) -> PathBuf {
    root.join("sequences.json")
}

/// Persist the sequence counters to `root/sequences.json`, atomically. They
/// live outside the generationed catalog because they change on every
/// `NEXT VALUE FOR` — far more often than a checkpoint — and are independent of
/// table arity, so a direct in-place save can never desync a generation's
/// catalog from its snapshots.
pub fn save_sequences(root: &Path, sequences: &BTreeMap<String, SequenceDef>) -> Result<()> {
    let path = sequences_path(root);
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(sequences)?)?;
    fs::File::open(&tmp)?.sync_all()?;
    fs::rename(&tmp, &path)?;
    Ok(())
}

/// Load the sequence counters, or `None` when the file is absent (no sequence
/// has ever been created, or a legacy database that still keeps them inside
/// `catalog.json`).
pub fn load_sequences(root: &Path) -> Result<Option<BTreeMap<String, SequenceDef>>> {
    match fs::read(sequences_path(root)) {
        Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
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
