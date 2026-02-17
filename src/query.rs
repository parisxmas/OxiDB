use std::collections::BTreeSet;
use std::ops::Bound;

use serde_json::Value as JsonValue;

use crate::document::{Document, DocumentId};
use crate::error::{Error, Result};
use crate::index::{CompositeIndex, FieldIndex};
use crate::value::IndexValue;

// ---------------------------------------------------------------------------
// Find options: sort / skip / limit
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub enum SortOrder {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Default)]
pub struct FindOptions {
    pub sort: Option<Vec<(String, SortOrder)>>,
    pub skip: Option<u64>,
    pub limit: Option<u64>,
}

/// Parse find options from the JSON request object.
/// Expects optional fields: `sort` (object: field→1/-1), `skip` (u64), `limit` (u64).
pub fn parse_find_options(request: &JsonValue) -> Result<FindOptions> {
    let mut opts = FindOptions::default();

    if let Some(sort_val) = request.get("sort") {
        if let Some(obj) = sort_val.as_object() {
            let mut sort_fields = Vec::new();
            for (field, dir) in obj {
                let order = match dir.as_i64() {
                    Some(1) => SortOrder::Asc,
                    Some(-1) => SortOrder::Desc,
                    _ => {
                        return Err(Error::InvalidQuery(
                            "sort direction must be 1 (asc) or -1 (desc)".into(),
                        ))
                    }
                };
                sort_fields.push((field.clone(), order));
            }
            if !sort_fields.is_empty() {
                opts.sort = Some(sort_fields);
            }
        }
    }

    if let Some(skip_val) = request.get("skip") {
        if let Some(n) = skip_val.as_u64() {
            opts.skip = Some(n);
        }
    }

    if let Some(limit_val) = request.get("limit") {
        if let Some(n) = limit_val.as_u64() {
            opts.limit = Some(n);
        }
    }

    Ok(opts)
}

// ---------------------------------------------------------------------------
// Query AST
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub enum QueryOp {
    Eq(IndexValue),
    Ne(IndexValue),
    Gt(IndexValue),
    Gte(IndexValue),
    Lt(IndexValue),
    Lte(IndexValue),
    In(Vec<IndexValue>),
    Exists(bool),
}

#[derive(Debug, Clone)]
pub enum Query {
    Field { field: String, op: QueryOp },
    And(Vec<Query>),
    Or(Vec<Query>),
    All, // match everything
}

// ---------------------------------------------------------------------------
// Parsing: JSON → Query AST
// ---------------------------------------------------------------------------

pub fn parse_query(query: &JsonValue) -> Result<Query> {
    let obj = query
        .as_object()
        .ok_or_else(|| Error::InvalidQuery("query must be a JSON object".into()))?;

    if obj.is_empty() {
        return Ok(Query::All);
    }

    let mut conditions: Vec<Query> = Vec::new();

    for (key, value) in obj {
        match key.as_str() {
            "$and" => {
                let arr = value
                    .as_array()
                    .ok_or_else(|| Error::InvalidQuery("$and must be an array".into()))?;
                let subs: Result<Vec<Query>> = arr.iter().map(parse_query).collect();
                conditions.push(Query::And(subs?));
            }
            "$or" => {
                let arr = value
                    .as_array()
                    .ok_or_else(|| Error::InvalidQuery("$or must be an array".into()))?;
                let subs: Result<Vec<Query>> = arr.iter().map(parse_query).collect();
                conditions.push(Query::Or(subs?));
            }
            field => {
                if value.is_object() {
                    let ops = value.as_object().unwrap();
                    // Check if this is actually an operator object (keys start with $)
                    let has_ops = ops.keys().any(|k| k.starts_with('$'));
                    if has_ops {
                        for (op_key, op_val) in ops {
                            let op = parse_op(op_key, op_val)?;
                            conditions.push(Query::Field {
                                field: field.to_string(),
                                op,
                            });
                        }
                    } else {
                        // Plain object equality
                        conditions.push(Query::Field {
                            field: field.to_string(),
                            op: QueryOp::Eq(IndexValue::from_json(value)),
                        });
                    }
                } else {
                    // Shorthand for $eq
                    conditions.push(Query::Field {
                        field: field.to_string(),
                        op: QueryOp::Eq(IndexValue::from_json(value)),
                    });
                }
            }
        }
    }

    match conditions.len() {
        0 => Ok(Query::All),
        1 => Ok(conditions.pop().unwrap()),
        _ => Ok(Query::And(conditions)),
    }
}

fn parse_op(op_key: &str, op_val: &JsonValue) -> Result<QueryOp> {
    match op_key {
        "$eq" => Ok(QueryOp::Eq(IndexValue::from_json(op_val))),
        "$ne" => Ok(QueryOp::Ne(IndexValue::from_json(op_val))),
        "$gt" => Ok(QueryOp::Gt(IndexValue::from_json(op_val))),
        "$gte" => Ok(QueryOp::Gte(IndexValue::from_json(op_val))),
        "$lt" => Ok(QueryOp::Lt(IndexValue::from_json(op_val))),
        "$lte" => Ok(QueryOp::Lte(IndexValue::from_json(op_val))),
        "$in" => {
            let arr = op_val
                .as_array()
                .ok_or_else(|| Error::InvalidQuery("$in must be an array".into()))?;
            Ok(QueryOp::In(arr.iter().map(IndexValue::from_json).collect()))
        }
        "$exists" => {
            let b = op_val
                .as_bool()
                .ok_or_else(|| Error::InvalidQuery("$exists must be a boolean".into()))?;
            Ok(QueryOp::Exists(b))
        }
        _ => Err(Error::InvalidQuery(format!("unknown operator: {}", op_key))),
    }
}

// ---------------------------------------------------------------------------
// Execution: evaluate a query against indexes + documents
// ---------------------------------------------------------------------------

/// Execute a query using available indexes. Returns matching document IDs.
pub fn execute_indexed(
    query: &Query,
    field_indexes: &std::collections::HashMap<String, FieldIndex>,
    composite_indexes: &[CompositeIndex],
) -> Option<BTreeSet<DocumentId>> {
    match query {
        Query::All => None, // None = full scan needed
        Query::Field { field, op } => execute_field_op(field, op, field_indexes, composite_indexes),
        Query::And(subs) => {
            let mut result: Option<BTreeSet<DocumentId>> = None;
            for sub in subs {
                if let Some(ids) = execute_indexed(sub, field_indexes, composite_indexes) {
                    result = Some(match result {
                        Some(existing) => &existing & &ids,
                        None => ids,
                    });
                }
            }
            result
        }
        Query::Or(subs) => {
            let mut all_resolved = true;
            let mut result = BTreeSet::new();
            for sub in subs {
                if let Some(ids) = execute_indexed(sub, field_indexes, composite_indexes) {
                    result = &result | &ids;
                } else {
                    all_resolved = false;
                    break;
                }
            }
            if all_resolved {
                Some(result)
            } else {
                None
            }
        }
    }
}

fn execute_field_op(
    field: &str,
    op: &QueryOp,
    field_indexes: &std::collections::HashMap<String, FieldIndex>,
    _composite_indexes: &[CompositeIndex],
) -> Option<BTreeSet<DocumentId>> {
    let idx = field_indexes.get(field)?;

    Some(match op {
        QueryOp::Eq(v) => idx.find_eq(v),
        QueryOp::Ne(v) => idx.find_ne(v),
        QueryOp::Gt(v) => idx.find_range(Bound::Excluded(v), Bound::Unbounded),
        QueryOp::Gte(v) => idx.find_range(Bound::Included(v), Bound::Unbounded),
        QueryOp::Lt(v) => idx.find_range(Bound::Unbounded, Bound::Excluded(v)),
        QueryOp::Lte(v) => idx.find_range(Bound::Unbounded, Bound::Included(v)),
        QueryOp::In(vals) => idx.find_in(vals),
        QueryOp::Exists(_) => return None, // can't use index for $exists
    })
}

// ---------------------------------------------------------------------------
// Filter: evaluate a query against a single document (post-filter)
// ---------------------------------------------------------------------------

pub fn matches_doc(query: &Query, doc: &Document) -> bool {
    match query {
        Query::All => true,
        Query::Field { field, op } => {
            let field_val = doc.get_field(field);
            match op {
                QueryOp::Exists(expected) => field_val.is_some() == *expected,
                _ => {
                    let Some(val) = field_val else {
                        return false;
                    };
                    let iv = IndexValue::from_json(val);
                    match op {
                        QueryOp::Eq(v) => iv == *v,
                        QueryOp::Ne(v) => iv != *v,
                        QueryOp::Gt(v) => iv > *v,
                        QueryOp::Gte(v) => iv >= *v,
                        QueryOp::Lt(v) => iv < *v,
                        QueryOp::Lte(v) => iv <= *v,
                        QueryOp::In(vals) => vals.contains(&iv),
                        QueryOp::Exists(_) => unreachable!(),
                    }
                }
            }
        }
        Query::And(subs) => subs.iter().all(|s| matches_doc(s, doc)),
        Query::Or(subs) => subs.iter().any(|s| matches_doc(s, doc)),
    }
}

/// Resolve a field path (with dot notation) directly on a &Value.
fn resolve_field_ref<'a>(data: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    let mut current = data;
    for part in path.split('.') {
        current = current.as_object()?.get(part)?;
    }
    Some(current)
}

/// Returns true if every condition in the query is backed by a field index,
/// meaning `execute_indexed` returns the exact set of matching IDs and no
/// post-filtering with `matches_value` is needed.
pub fn is_fully_indexed(
    query: &Query,
    field_indexes: &std::collections::HashMap<String, FieldIndex>,
) -> bool {
    match query {
        Query::All => true,
        Query::Field { field, op } => {
            // $exists can't be resolved by index
            if matches!(op, QueryOp::Exists(_)) {
                return false;
            }
            field_indexes.contains_key(field.as_str())
        }
        Query::And(subs) => subs.iter().all(|s| is_fully_indexed(s, field_indexes)),
        Query::Or(subs) => subs.iter().all(|s| is_fully_indexed(s, field_indexes)),
    }
}

/// Count matching documents using only indexes (no BTreeSet allocation).
/// Returns None if the query can't be counted by index alone.
pub fn count_indexed(
    query: &Query,
    field_indexes: &std::collections::HashMap<String, FieldIndex>,
) -> Option<usize> {
    match query {
        Query::All => None, // caller should use primary_index.len()
        Query::Field { field, op } => {
            let idx = field_indexes.get(field.as_str())?;
            Some(match op {
                QueryOp::Eq(v) => idx.count_eq(v),
                QueryOp::Ne(_v) => return None, // expensive, fall through
                QueryOp::Gt(v) => idx.count_range(Bound::Excluded(v), Bound::Unbounded),
                QueryOp::Gte(v) => idx.count_range(Bound::Included(v), Bound::Unbounded),
                QueryOp::Lt(v) => idx.count_range(Bound::Unbounded, Bound::Excluded(v)),
                QueryOp::Lte(v) => idx.count_range(Bound::Unbounded, Bound::Included(v)),
                QueryOp::In(vals) => idx.count_in(vals),
                QueryOp::Exists(_) => return None,
            })
        }
        Query::And(subs) => {
            // Common case: AND of range conditions on the SAME indexed field
            // e.g. {created_at: {$gte: "2023-01-01", $lt: "2024-01-01"}}
            count_single_field_and(subs, field_indexes)
        }
        Query::Or(_) => None,
    }
}

/// Handle AND of range conditions on the same field — count without BTreeSet.
fn count_single_field_and(
    subs: &[Query],
    field_indexes: &std::collections::HashMap<String, FieldIndex>,
) -> Option<usize> {
    let mut field_name: Option<&str> = None;
    let mut gte_bound: Option<&IndexValue> = None;
    let mut gt_bound: Option<&IndexValue> = None;
    let mut lt_bound: Option<&IndexValue> = None;
    let mut lte_bound: Option<&IndexValue> = None;
    let mut eq_value: Option<&IndexValue> = None;

    for sub in subs {
        match sub {
            Query::Field { field, op } => {
                if let Some(name) = field_name {
                    if name != field {
                        return None; // Different fields — can't merge into single range
                    }
                } else {
                    field_name = Some(field);
                }
                match op {
                    QueryOp::Gte(v) => gte_bound = Some(v),
                    QueryOp::Gt(v) => gt_bound = Some(v),
                    QueryOp::Lt(v) => lt_bound = Some(v),
                    QueryOp::Lte(v) => lte_bound = Some(v),
                    QueryOp::Eq(v) => eq_value = Some(v),
                    _ => return None,
                }
            }
            _ => return None,
        }
    }

    let field = field_name?;
    let idx = field_indexes.get(field)?;

    // If there's an eq value, the range constraints must also be satisfied
    if let Some(eq_val) = eq_value {
        return Some(idx.count_eq(eq_val));
    }

    let start = if let Some(v) = gte_bound {
        Bound::Included(v)
    } else if let Some(v) = gt_bound {
        Bound::Excluded(v)
    } else {
        Bound::Unbounded
    };

    let end = if let Some(v) = lt_bound {
        Bound::Excluded(v)
    } else if let Some(v) = lte_bound {
        Bound::Included(v)
    } else {
        Bound::Unbounded
    };

    Some(idx.count_range(start, end))
}

/// Like `matches_doc` but operates directly on `&Value`, avoiding Document construction.
pub fn matches_value(query: &Query, data: &JsonValue) -> bool {
    match query {
        Query::All => true,
        Query::Field { field, op } => {
            let field_val = resolve_field_ref(data, field);
            match op {
                QueryOp::Exists(expected) => field_val.is_some() == *expected,
                _ => {
                    let Some(val) = field_val else {
                        return false;
                    };
                    let iv = IndexValue::from_json(val);
                    match op {
                        QueryOp::Eq(v) => iv == *v,
                        QueryOp::Ne(v) => iv != *v,
                        QueryOp::Gt(v) => iv > *v,
                        QueryOp::Gte(v) => iv >= *v,
                        QueryOp::Lt(v) => iv < *v,
                        QueryOp::Lte(v) => iv <= *v,
                        QueryOp::In(vals) => vals.contains(&iv),
                        QueryOp::Exists(_) => unreachable!(),
                    }
                }
            }
        }
        Query::And(subs) => subs.iter().all(|s| matches_value(s, data)),
        Query::Or(subs) => subs.iter().any(|s| matches_value(s, data)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_simple_eq() {
        let q = parse_query(&json!({"name": "Alice"})).unwrap();
        let doc = Document::new(1, json!({"name": "Alice"})).unwrap();
        assert!(matches_doc(&q, &doc));
    }

    #[test]
    fn parse_range() {
        let q = parse_query(&json!({"age": {"$gte": 18, "$lt": 65}})).unwrap();
        let doc_ok = Document::new(1, json!({"age": 30})).unwrap();
        let doc_young = Document::new(2, json!({"age": 10})).unwrap();
        let doc_old = Document::new(3, json!({"age": 70})).unwrap();
        assert!(matches_doc(&q, &doc_ok));
        assert!(!matches_doc(&q, &doc_young));
        assert!(!matches_doc(&q, &doc_old));
    }

    #[test]
    fn parse_date_range() {
        let q = parse_query(&json!({
            "created_at": {"$gte": "2024-01-01", "$lt": "2025-01-01"}
        }))
        .unwrap();

        let doc_in = Document::new(1, json!({"created_at": "2024-06-15"})).unwrap();
        let doc_out = Document::new(2, json!({"created_at": "2023-06-15"})).unwrap();
        assert!(matches_doc(&q, &doc_in));
        assert!(!matches_doc(&q, &doc_out));
    }

    #[test]
    fn parse_or() {
        let q = parse_query(&json!({
            "$or": [{"status": "active"}, {"priority": {"$gte": 5}}]
        }))
        .unwrap();

        let doc1 = Document::new(1, json!({"status": "active", "priority": 1})).unwrap();
        let doc2 = Document::new(2, json!({"status": "closed", "priority": 10})).unwrap();
        let doc3 = Document::new(3, json!({"status": "closed", "priority": 1})).unwrap();
        assert!(matches_doc(&q, &doc1));
        assert!(matches_doc(&q, &doc2));
        assert!(!matches_doc(&q, &doc3));
    }
}
