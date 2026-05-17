//! CSV foreign-collection adapter. Treats a single CSV file as a
//! flat document collection: the first non-blank line is the header,
//! each subsequent line is one row. Rows are presented to the rest
//! of OxiDB as JSON objects whose keys are the header columns.
//!
//! Scope (v3a):
//!   - find / find_one / count with simple equality predicates
//!     (`{"field": "value"}`); the empty query `{}` matches every row.
//!   - insert / insert_many — append, header inferred from first row
//!     if the file is empty.
//!   - update_one — `$set` operator only; matched row gets its
//!     listed fields overwritten, others preserved.
//!   - delete_one — drop the first matching row.
//!
//! Out of scope (would need a real CSV parser + query engine):
//!   - quoted fields with embedded commas / newlines
//!   - numeric / boolean type coercion in queries
//!   - `$gt` / `$in` / `$or` etc — only equality on top-level fields
//!
//! Concurrency: a per-path mutex serialises all access so concurrent
//! requests against the same file can't interleave mid-rewrite and
//! corrupt the file. Different files share nothing.

use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::fdw::Adapter;

/// Global per-path lock map. Acquired for the duration of every
/// `execute` so reads and writes can't race on a half-written file.
fn file_lock(path: &Path) -> &'static Mutex<()> {
    static LOCKS: OnceLock<Mutex<HashMap<PathBuf, &'static Mutex<()>>>> = OnceLock::new();
    let map = LOCKS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map.lock().unwrap();
    if let Some(m) = guard.get(path) {
        return m;
    }
    // Per-path mutex outlives the process — leaking is fine because
    // the set of distinct linked CSV files is bounded by operator
    // intent, not by request volume.
    let m: &'static Mutex<()> = Box::leak(Box::new(Mutex::new(())));
    guard.insert(path.to_path_buf(), m);
    m
}

/// CsvAdapter is a thin handle around a file path. All state lives on
/// disk; the adapter holds nothing but the path so a link URL change
/// (via unlink + link) is picked up on the very next request.
pub struct CsvAdapter {
    path: PathBuf,
}

impl CsvAdapter {
    /// Construct from a filesystem path. The file is NOT required to
    /// exist at construction time — it'll be created on first insert.
    /// This matches the "create a fresh linked CSV and start writing"
    /// workflow without a separate provisioning step.
    ///
    /// We DO canonicalise the path when the file exists, so two link
    /// URLs that resolve to the same file share the same per-path
    /// lock — concurrent updates can't sneak past serialisation by
    /// using a different spelling of the same path.
    pub fn from_url(raw_path: &str) -> Result<Self, String> {
        if raw_path.is_empty() {
            return Err("csv link URL has empty path".into());
        }
        let p = PathBuf::from(raw_path);
        let path = if p.exists() {
            p.canonicalize()
                .map_err(|e| format!("canonicalise {}: {}", raw_path, e))?
        } else {
            p
        };
        Ok(Self { path })
    }
}

impl Adapter for CsvAdapter {
    fn execute(&self, cmd: &str, request: &Value) -> Result<Value, String> {
        let _guard = file_lock(&self.path).lock().unwrap();

        let query = request.get("query").cloned().unwrap_or_else(|| json!({}));

        match cmd {
            "find" => {
                let rows = load_rows(&self.path)?;
                let matching = filter_rows(&rows, &query);
                Ok(envelope_data(Value::Array(matching)))
            }
            "find_one" => {
                let rows = load_rows(&self.path)?;
                let matching = filter_rows(&rows, &query);
                let first = matching.into_iter().next().unwrap_or(Value::Null);
                Ok(envelope_data(first))
            }
            "count" => {
                let rows = load_rows(&self.path)?;
                let n = filter_rows(&rows, &query).len();
                Ok(envelope_data(json!({ "count": n })))
            }
            "insert" => {
                let doc = request
                    .get("doc")
                    .ok_or("missing 'doc'")?
                    .as_object()
                    .ok_or("'doc' must be an object")?
                    .clone();
                let new_id = append_row(&self.path, &doc)?;
                Ok(envelope_data(json!({ "id": new_id })))
            }
            "insert_many" => {
                let docs = request
                    .get("docs")
                    .and_then(|v| v.as_array())
                    .ok_or("missing or invalid 'docs' array")?;
                let mut ids = Vec::with_capacity(docs.len());
                for d in docs {
                    let obj = d.as_object().ok_or("each 'docs' entry must be an object")?;
                    ids.push(append_row(&self.path, obj)?);
                }
                Ok(envelope_data(json!({ "ids": ids })))
            }
            "update_one" => {
                let update = request
                    .get("update")
                    .ok_or("missing 'update'")?
                    .clone();
                let modified = update_one_row(&self.path, &query, &update)?;
                Ok(envelope_data(json!({ "modified": modified })))
            }
            "delete_one" => {
                let deleted = delete_one_row(&self.path, &query)?;
                Ok(envelope_data(json!({ "deleted": deleted })))
            }
            other => Err(format!(
                "CSV FDW adapter does not implement command {:?}",
                other
            )),
        }
    }
}

/// envelope_data wraps a value in the same `{"ok": true, "data": ...}`
/// envelope the local handler emits. Adapters return this directly so
/// the handler can forward verbatim.
fn envelope_data(data: Value) -> Value {
    json!({ "ok": true, "data": data })
}

/// load_rows reads the whole file into (header, rows) pairs and
/// returns each row as a JSON object. Missing file → empty list (NOT
/// an error) so a find against a never-yet-inserted-into link works
/// out of the box.
fn load_rows(path: &Path) -> Result<Vec<Value>, String> {
    let bytes = match fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(format!("read {}: {}", path.display(), e)),
    };
    let mut lines = bytes.lines().filter(|l| !l.is_empty());
    let header = match lines.next() {
        Some(h) => parse_simple_csv_line(h),
        None => return Ok(Vec::new()),
    };
    let rows = lines.map(|l| row_to_json(&header, &parse_simple_csv_line(l))).collect();
    Ok(rows)
}

/// row_to_json turns a parsed CSV row (Vec<String>) into a JSON
/// object using the header as keys. Rows shorter than the header get
/// null-filled; rows longer get truncated — both happen rarely in
/// practice but it's better than panicking on a malformed file.
fn row_to_json(header: &[String], fields: &[String]) -> Value {
    let mut obj = Map::new();
    for (i, name) in header.iter().enumerate() {
        let v = fields.get(i).map(|s| Value::String(s.clone())).unwrap_or(Value::Null);
        obj.insert(name.clone(), v);
    }
    Value::Object(obj)
}

/// parse_simple_csv_line splits a line on commas. v3a doesn't try to
/// handle quoted fields with embedded commas / newlines — that's a
/// real CSV parser's job, and pulling one in is outside the scope of
/// this PR. Operators with quoted CSVs should use a different
/// adapter (or wait for a follow-up).
fn parse_simple_csv_line(line: &str) -> Vec<String> {
    line.split(',').map(|s| s.to_string()).collect()
}

/// filter_rows applies a simple equality predicate. Empty query
/// matches everything; otherwise every (k, v) in `query` must equal
/// the row's value at `k` (compared as strings — CSV has no types).
fn filter_rows(rows: &[Value], query: &Value) -> Vec<Value> {
    let pred = match query.as_object() {
        Some(o) if !o.is_empty() => o,
        _ => return rows.to_vec(),
    };
    rows.iter()
        .filter(|row| {
            let obj = match row.as_object() {
                Some(o) => o,
                None => return false,
            };
            pred.iter().all(|(k, expected)| {
                let actual = obj.get(k).unwrap_or(&Value::Null);
                values_equal_loosely(actual, expected)
            })
        })
        .cloned()
        .collect()
}

/// values_equal_loosely compares two JSON values as strings — CSV
/// stores everything as text, so a query like `{"qty": 20}` against
/// a row where qty is the string "20" must match. Strict JSON
/// equality would miss that.
fn values_equal_loosely(a: &Value, b: &Value) -> bool {
    fn as_text(v: &Value) -> String {
        match v {
            Value::String(s) => s.clone(),
            Value::Null => String::new(),
            other => other.to_string(),
        }
    }
    as_text(a) == as_text(b)
}

/// append_row writes one row to the end of the file, creating the
/// file (and the header from the doc's keys) if it doesn't yet
/// exist. Returns the row's 1-based position as the "id" — matches
/// what a fresh OxiDB collection's insert returns conceptually
/// (monotonic per-collection counter).
fn append_row(path: &Path, doc: &Map<String, Value>) -> Result<u64, String> {
    let existing = fs::read_to_string(path).ok();

    let (header, row_count) = match existing.as_deref() {
        Some(s) if !s.is_empty() => {
            let mut lines = s.lines().filter(|l| !l.is_empty());
            let h = lines.next().map(parse_simple_csv_line).unwrap_or_default();
            let count = lines.count() as u64;
            (h, count)
        }
        _ => {
            // Fresh file → take the doc's keys (in their JSON order
            // — serde preserves insertion order via the BTreeMap /
            // IndexMap backing) as the header.
            let h: Vec<String> = doc.keys().cloned().collect();
            (h, 0)
        }
    };

    let row: Vec<String> = header
        .iter()
        .map(|name| match doc.get(name) {
            Some(Value::String(s)) => s.clone(),
            Some(Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        })
        .collect();

    let mut out = String::new();
    if existing.as_deref().is_none_or(|s| s.is_empty()) {
        out.push_str(&header.join(","));
        out.push('\n');
    }
    out.push_str(&row.join(","));
    out.push('\n');

    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("open {} for append: {}", path.display(), e))?;
    file.write_all(out.as_bytes())
        .map_err(|e| format!("write {}: {}", path.display(), e))?;
    Ok(row_count + 1)
}

/// update_one_row rewrites the file with the first row matching
/// `query` updated according to `update.$set`. Returns 1 if a row
/// was updated, 0 otherwise. Non-`$set` update operators are NOT
/// supported in v3a — they'd need real query-engine semantics on a
/// stringly-typed backing store.
fn update_one_row(path: &Path, query: &Value, update: &Value) -> Result<usize, String> {
    let set = update
        .get("$set")
        .and_then(|v| v.as_object())
        .ok_or("CSV FDW supports only the $set update operator")?;

    let rows = load_rows(path)?;
    if rows.is_empty() {
        return Ok(0);
    }
    // Header comes from the file directly so we preserve column
    // order on rewrite — the user shouldn't see their CSV columns
    // shuffled because we updated a row.
    let header = read_header(path)?;

    let mut modified = 0usize;
    let mut out: Vec<Value> = Vec::with_capacity(rows.len());
    let pred_empty = query.as_object().map(|o| o.is_empty()).unwrap_or(true);
    for row in rows {
        let mut row_obj = row.as_object().cloned().unwrap_or_default();
        let matches = pred_empty
            || query.as_object().unwrap().iter().all(|(k, expected)| {
                let actual = row_obj.get(k).unwrap_or(&Value::Null);
                values_equal_loosely(actual, expected)
            });
        if matches && modified == 0 {
            for (k, v) in set.iter() {
                row_obj.insert(k.clone(), v.clone());
            }
            modified = 1;
        }
        out.push(Value::Object(row_obj));
    }

    write_table(path, &header, &out)?;
    Ok(modified)
}

/// delete_one_row rewrites the file without the first matching row.
/// Returns 1 if a row was removed, 0 otherwise.
fn delete_one_row(path: &Path, query: &Value) -> Result<usize, String> {
    let rows = load_rows(path)?;
    if rows.is_empty() {
        return Ok(0);
    }
    let header = read_header(path)?;

    let pred_empty = query.as_object().map(|o| o.is_empty()).unwrap_or(true);
    let mut deleted = 0usize;
    let mut out: Vec<Value> = Vec::with_capacity(rows.len());
    for row in rows {
        let obj = row.as_object().cloned().unwrap_or_default();
        let matches = pred_empty
            || query.as_object().unwrap().iter().all(|(k, expected)| {
                let actual = obj.get(k).unwrap_or(&Value::Null);
                values_equal_loosely(actual, expected)
            });
        if matches && deleted == 0 {
            deleted = 1;
            continue;
        }
        out.push(Value::Object(obj));
    }
    write_table(path, &header, &out)?;
    Ok(deleted)
}

/// read_header pulls the first non-blank line and returns it as a
/// list of column names. Used by the mutating operations so a rewrite
/// keeps the original column order even when no rows match.
fn read_header(path: &Path) -> Result<Vec<String>, String> {
    let s = fs::read_to_string(path).map_err(|e| format!("read {}: {}", path.display(), e))?;
    Ok(s.lines()
        .find(|l| !l.is_empty())
        .map(parse_simple_csv_line)
        .unwrap_or_default())
}

/// write_table atomically replaces the file with header + rows. The
/// write goes to a sibling `.tmp` first then renames into place,
/// matching the standard durable-write pattern — a crash mid-write
/// leaves the original file untouched.
fn write_table(path: &Path, header: &[String], rows: &[Value]) -> Result<(), String> {
    let mut out = String::new();
    out.push_str(&header.join(","));
    out.push('\n');
    for row in rows {
        let obj = match row.as_object() {
            Some(o) => o,
            None => continue,
        };
        let fields: Vec<String> = header
            .iter()
            .map(|name| match obj.get(name) {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Null) | None => String::new(),
                Some(other) => other.to_string(),
            })
            .collect();
        out.push_str(&fields.join(","));
        out.push('\n');
    }
    let tmp = path.with_extension(format!(
        "{}.tmp",
        path.extension().and_then(|e| e.to_str()).unwrap_or("csv")
    ));
    fs::write(&tmp, out).map_err(|e| format!("write {}: {}", tmp.display(), e))?;
    fs::rename(&tmp, path).map_err(|e| format!("rename {} → {}: {}", tmp.display(), path.display(), e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn write_csv(path: &Path, content: &str) {
        fs::write(path, content).unwrap();
    }

    #[test]
    fn find_returns_all_rows_for_empty_query() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        write_csv(&path, "name,age\nalice,30\nbob,25\n");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();

        let resp = a.execute("find", &json!({"query": {}})).unwrap();
        assert_eq!(resp["ok"], true);
        let rows = resp["data"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "alice");
        assert_eq!(rows[1]["name"], "bob");
    }

    #[test]
    fn find_applies_equality_predicate() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        write_csv(&path, "name,age\nalice,30\nbob,25\ncarol,30\n");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();

        let resp = a.execute("find", &json!({"query": {"age": "30"}})).unwrap();
        let rows = resp["data"].as_array().unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn find_query_compares_strings_loosely_against_json_numbers() {
        // A query of `{age: 30}` (JSON number) should match a CSV cell
        // "30" (string) — without this, queries would have to manually
        // quote every numeric value.
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        write_csv(&path, "name,age\nalice,30\n");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();
        let resp = a.execute("find", &json!({"query": {"age": 30}})).unwrap();
        assert_eq!(resp["data"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn count_matches_find_length() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        write_csv(&path, "x\na\nb\nc\n");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();
        let resp = a.execute("count", &json!({"query": {}})).unwrap();
        assert_eq!(resp["data"]["count"], 3);
    }

    #[test]
    fn insert_into_empty_file_creates_header_from_doc() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();

        let resp = a.execute(
            "insert",
            &json!({"doc": {"name": "alice", "age": "30"}}),
        ).unwrap();
        assert_eq!(resp["ok"], true);
        assert_eq!(resp["data"]["id"], 1, "first row gets id 1");

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.starts_with("name,age\n") || content.starts_with("age,name\n"),
            "header inferred from doc keys: {:?}", content);
    }

    #[test]
    fn insert_into_existing_file_appends_and_increments_id() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        write_csv(&path, "name,age\nalice,30\n");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();

        let resp = a.execute(
            "insert",
            &json!({"doc": {"name": "bob", "age": "25"}}),
        ).unwrap();
        assert_eq!(resp["data"]["id"], 2);

        let rows = a.execute("find", &json!({"query": {}})).unwrap()
            ["data"].as_array().unwrap().clone();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn insert_many_returns_ids_in_order() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();
        let resp = a.execute(
            "insert_many",
            &json!({"docs": [{"x": "1"}, {"x": "2"}, {"x": "3"}]}),
        ).unwrap();
        let ids = resp["data"]["ids"].as_array().unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(ids[0], 1);
        assert_eq!(ids[2], 3);
    }

    #[test]
    fn update_one_rewrites_first_matching_row_only() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        write_csv(&path, "name,age\nalice,30\nbob,30\ncarol,25\n");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();

        let resp = a.execute("update_one", &json!({
            "query": {"age": "30"},
            "update": {"$set": {"age": "31"}},
        })).unwrap();
        assert_eq!(resp["data"]["modified"], 1);

        let content = fs::read_to_string(&path).unwrap();
        // Only the FIRST age=30 row (alice) gets bumped. Bob stays at 30.
        assert!(content.contains("alice,31"));
        assert!(content.contains("bob,30"));
        assert!(content.contains("carol,25"));
    }

    #[test]
    fn update_one_rejects_non_set_operators_with_clear_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        write_csv(&path, "x\na\n");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();

        let err = a.execute("update_one", &json!({
            "query": {},
            "update": {"$inc": {"x": 1}},
        })).unwrap_err();
        assert!(err.contains("$set"), "{err}");
    }

    #[test]
    fn delete_one_removes_first_match_only() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        write_csv(&path, "name\na\nb\na\n");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();

        let resp = a.execute("delete_one", &json!({"query": {"name": "a"}})).unwrap();
        assert_eq!(resp["data"]["deleted"], 1);

        let rows = a.execute("find", &json!({"query": {}})).unwrap()
            ["data"].as_array().unwrap().clone();
        // First 'a' gone, 'b' and second 'a' remain.
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["name"], "b");
        assert_eq!(rows[1]["name"], "a");
    }

    #[test]
    fn find_on_missing_file_is_empty_not_error() {
        // A linked CSV that hasn't been inserted-into yet returns an
        // empty list, not a "file not found" — lets callers issue
        // their first insert and then read without a special-case.
        let dir = tempdir().unwrap();
        let path = dir.path().join("nonexistent.csv");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();
        let resp = a.execute("find", &json!({"query": {}})).unwrap();
        assert_eq!(resp["data"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn unsupported_command_returns_descriptive_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("data.csv");
        write_csv(&path, "x\na\n");
        let a = CsvAdapter::from_url(path.to_str().unwrap()).unwrap();
        let err = a.execute("aggregate", &json!({})).unwrap_err();
        assert!(err.contains("does not implement"));
        assert!(err.contains("aggregate"));
    }
}
