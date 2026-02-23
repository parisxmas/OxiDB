use serde_json::Value;
use std::collections::BTreeMap;

use super::codec::ColumnDef;

// ── PostgreSQL Type OIDs ─────────────────────────────────────────────

pub const OID_BOOL: i32 = 16;
pub const OID_INT8: i32 = 20; // BIGINT
pub const OID_FLOAT8: i32 = 701; // DOUBLE PRECISION
pub const OID_TEXT: i32 = 25;
pub const OID_JSON: i32 = 114;

/// Map a JSON value to a PostgreSQL type OID.
fn json_type_to_oid(val: &Value) -> i32 {
    match val {
        Value::Bool(_) => OID_BOOL,
        Value::Number(n) => {
            if n.is_f64() && n.as_i64().is_none() {
                OID_FLOAT8
            } else {
                OID_INT8
            }
        }
        Value::String(_) => OID_TEXT,
        Value::Array(_) | Value::Object(_) => OID_JSON,
        Value::Null => OID_TEXT, // fallback
    }
}

/// Type size for a given OID (used in RowDescription).
fn type_len(oid: i32) -> i16 {
    match oid {
        OID_BOOL => 1,
        OID_INT8 => 8,
        OID_FLOAT8 => 8,
        _ => -1, // variable length
    }
}

/// Infer column definitions from a set of JSON document rows.
///
/// Since OxiDB is schemaless, columns are gathered from all rows:
/// 1. Collect all unique keys across all rows.
/// 2. Put `_id` first, then remaining keys in alphabetical order.
/// 3. For each key, find the first non-null value to determine the PG type.
pub fn infer_columns(rows: &[Value]) -> Vec<ColumnDef> {
    if rows.is_empty() {
        return Vec::new();
    }

    // Collect all keys and first non-null value per key.
    let mut key_types: BTreeMap<String, i32> = BTreeMap::new();

    for row in rows {
        if let Value::Object(map) = row {
            for (k, v) in map {
                key_types.entry(k.clone()).or_insert_with(|| {
                    if v.is_null() {
                        OID_TEXT // will be overwritten by a non-null value later
                    } else {
                        json_type_to_oid(v)
                    }
                });
                // If existing entry was null-derived, try to upgrade.
                if let Some(oid) = key_types.get_mut(k)
                    && *oid == OID_TEXT && !v.is_null() && !v.is_string()
                {
                    *oid = json_type_to_oid(v);
                }
            }
        }
    }

    // Build ordered list: _id first, then alphabetical.
    let mut columns = Vec::with_capacity(key_types.len());

    if let Some(oid) = key_types.remove("_id") {
        columns.push(ColumnDef {
            name: "_id".to_string(),
            type_oid: oid,
            type_len: type_len(oid),
            type_mod: -1,
        });
    }

    for (name, oid) in &key_types {
        columns.push(ColumnDef {
            name: name.clone(),
            type_oid: *oid,
            type_len: type_len(*oid),
            type_mod: -1,
        });
    }

    columns
}

/// Convert a JSON value to its text-format representation for a PG DataRow.
/// Returns `None` for JSON null (sent as SQL NULL).
pub fn value_to_text(val: &Value) -> Option<String> {
    match val {
        Value::Null => None,
        Value::Bool(b) => Some(if *b { "t".to_string() } else { "f".to_string() }),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        Value::Array(_) | Value::Object(_) => Some(val.to_string()),
    }
}

/// Extract a row of text values from a JSON document, aligned with the given columns.
pub fn row_values(doc: &Value, columns: &[ColumnDef]) -> Vec<Option<String>> {
    columns
        .iter()
        .map(|col| {
            doc.get(&col.name)
                .map(value_to_text)
                .unwrap_or(None) // missing key = NULL
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_infer_columns_basic() {
        let rows = vec![
            json!({"_id": 1, "name": "Alice", "age": 30}),
            json!({"_id": 2, "name": "Bob", "age": 25, "email": "bob@x.com"}),
        ];
        let cols = infer_columns(&rows);
        assert_eq!(cols[0].name, "_id");
        assert_eq!(cols[0].type_oid, OID_INT8);

        let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["_id", "age", "email", "name"]);
    }

    #[test]
    fn test_infer_columns_empty() {
        let cols = infer_columns(&[]);
        assert!(cols.is_empty());
    }

    #[test]
    fn test_infer_columns_mixed_types() {
        let rows = vec![
            json!({"_id": 1, "score": 3.14, "active": true, "tags": ["a", "b"]}),
        ];
        let cols = infer_columns(&rows);
        let map: BTreeMap<&str, i32> = cols.iter().map(|c| (c.name.as_str(), c.type_oid)).collect();
        assert_eq!(map["score"], OID_FLOAT8);
        assert_eq!(map["active"], OID_BOOL);
        assert_eq!(map["tags"], OID_JSON);
    }

    #[test]
    fn test_value_to_text() {
        assert_eq!(value_to_text(&json!(null)), None);
        assert_eq!(value_to_text(&json!(true)), Some("t".into()));
        assert_eq!(value_to_text(&json!(false)), Some("f".into()));
        assert_eq!(value_to_text(&json!(42)), Some("42".into()));
        assert_eq!(value_to_text(&json!(3.14)), Some("3.14".into()));
        assert_eq!(value_to_text(&json!("hello")), Some("hello".into()));
        assert!(value_to_text(&json!({"a": 1})).unwrap().contains("\"a\""));
    }

    #[test]
    fn test_row_values() {
        let cols = vec![
            ColumnDef { name: "_id".into(), type_oid: OID_INT8, type_len: 8, type_mod: -1 },
            ColumnDef { name: "name".into(), type_oid: OID_TEXT, type_len: -1, type_mod: -1 },
            ColumnDef { name: "missing".into(), type_oid: OID_TEXT, type_len: -1, type_mod: -1 },
        ];
        let doc = json!({"_id": 1, "name": "Alice"});
        let vals = row_values(&doc, &cols);
        assert_eq!(vals[0], Some("1".into()));
        assert_eq!(vals[1], Some("Alice".into()));
        assert_eq!(vals[2], None); // missing field
    }

    #[test]
    fn test_null_column_upgrade() {
        let rows = vec![
            json!({"_id": 1, "val": null}),
            json!({"_id": 2, "val": 42}),
        ];
        let cols = infer_columns(&rows);
        let val_col = cols.iter().find(|c| c.name == "val").unwrap();
        assert_eq!(val_col.type_oid, OID_INT8);
    }
}
