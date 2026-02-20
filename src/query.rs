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
    Regex(regex::Regex),
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
                            // $options is consumed by $regex, skip it
                            if op_key == "$options" {
                                continue;
                            }
                            let op = parse_op(op_key, op_val, ops)?;
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

fn parse_op(
    op_key: &str,
    op_val: &JsonValue,
    sibling_ops: &serde_json::Map<String, JsonValue>,
) -> Result<QueryOp> {
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
        "$regex" => {
            let pattern = op_val
                .as_str()
                .ok_or_else(|| Error::InvalidQuery("$regex must be a string".into()))?;
            let options = sibling_ops
                .get("$options")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let mut re_pattern = String::new();
            if options.contains('i') {
                re_pattern.push_str("(?i)");
            }
            re_pattern.push_str(pattern);
            let re = regex::Regex::new(&re_pattern).map_err(|e| {
                Error::InvalidQuery(format!("invalid regex: {}", e))
            })?;
            Ok(QueryOp::Regex(re))
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
            // Try merged range on same field (e.g. {age: {$gte: 25, $lte: 35}})
            if let Some((idx, start, end)) = try_merge_range_and(subs, field_indexes) {
                return Some(idx.find_range(start, end));
            }
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
        QueryOp::Exists(_) | QueryOp::Regex(_) => return None,
    })
}

// ---------------------------------------------------------------------------
// Lazy index execution — callback-based with early termination
// ---------------------------------------------------------------------------

/// Try to merge an AND of conditions on the same indexed field into a single
/// range. Returns the merged (start, end) bounds and the field index, or None.
fn try_merge_range_and<'a>(
    subs: &'a [Query],
    field_indexes: &'a std::collections::HashMap<String, FieldIndex>,
) -> Option<(
    &'a FieldIndex,
    Bound<&'a IndexValue>,
    Bound<&'a IndexValue>,
)> {
    let mut field_name: Option<&str> = None;
    let mut gte_bound: Option<&IndexValue> = None;
    let mut gt_bound: Option<&IndexValue> = None;
    let mut lt_bound: Option<&IndexValue> = None;
    let mut lte_bound: Option<&IndexValue> = None;

    for sub in subs {
        match sub {
            Query::Field { field, op } => {
                if let Some(name) = field_name {
                    if name != field {
                        return None;
                    }
                } else {
                    field_name = Some(field);
                }
                match op {
                    QueryOp::Gte(v) => gte_bound = Some(v),
                    QueryOp::Gt(v) => gt_bound = Some(v),
                    QueryOp::Lt(v) => lt_bound = Some(v),
                    QueryOp::Lte(v) => lte_bound = Some(v),
                    _ => return None,
                }
            }
            _ => return None,
        }
    }

    let field = field_name?;
    let idx = field_indexes.get(field)?;

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

    Some((idx, start, end))
}

/// Execute a query lazily against indexes, calling `callback` for each matching
/// DocumentId. The callback returns `true` to continue or `false` to stop.
/// Returns `Some(true)` if the query was fully handled by indexes,
/// `Some(false)` if stopped early by callback, `None` if indexes couldn't handle it.
pub fn execute_indexed_lazy(
    query: &Query,
    field_indexes: &std::collections::HashMap<String, FieldIndex>,
    callback: &mut dyn FnMut(DocumentId) -> bool,
) -> Option<bool> {
    match query {
        Query::All => None,
        Query::Field { field, op } => {
            let idx = field_indexes.get(field.as_str())?;
            Some(match op {
                QueryOp::Eq(v) => {
                    let mut cont = true;
                    idx.for_each_eq(v, |id| {
                        cont = callback(id);
                        cont
                    });
                    cont
                }
                QueryOp::Ne(v) => {
                    let mut cont = true;
                    idx.for_each_ne(v, |id| {
                        cont = callback(id);
                        cont
                    });
                    cont
                }
                QueryOp::Gt(v) => {
                    let mut cont = true;
                    idx.for_each_in_range(Bound::Excluded(v), Bound::Unbounded, |id| {
                        cont = callback(id);
                        cont
                    });
                    cont
                }
                QueryOp::Gte(v) => {
                    let mut cont = true;
                    idx.for_each_in_range(Bound::Included(v), Bound::Unbounded, |id| {
                        cont = callback(id);
                        cont
                    });
                    cont
                }
                QueryOp::Lt(v) => {
                    let mut cont = true;
                    idx.for_each_in_range(Bound::Unbounded, Bound::Excluded(v), |id| {
                        cont = callback(id);
                        cont
                    });
                    cont
                }
                QueryOp::Lte(v) => {
                    let mut cont = true;
                    idx.for_each_in_range(Bound::Unbounded, Bound::Included(v), |id| {
                        cont = callback(id);
                        cont
                    });
                    cont
                }
                QueryOp::In(vals) => {
                    let mut cont = true;
                    idx.for_each_in(vals, |id| {
                        cont = callback(id);
                        cont
                    });
                    cont
                }
                QueryOp::Exists(_) | QueryOp::Regex(_) => return None,
            })
        }
        Query::And(subs) => {
            // Try merged range on same field (e.g. {age: {$gte: 25, $lte: 35}})
            if let Some((idx, start, end)) = try_merge_range_and(subs, field_indexes) {
                let mut cont = true;
                idx.for_each_in_range(start, end, |id| {
                    cont = callback(id);
                    cont
                });
                return Some(cont);
            }
            // For AND with multiple indexed conditions, find the most selective
            // sub-query (smallest estimated result set), iterate it lazily,
            // and post-filter the rest.
            let mut indexable_idx: Option<usize> = None;
            let mut all_indexable = true;
            for (i, sub) in subs.iter().enumerate() {
                if execute_indexed(sub, field_indexes, &[]).is_some() {
                    if indexable_idx.is_none() {
                        indexable_idx = Some(i);
                    }
                } else {
                    all_indexable = false;
                }
            }
            if let Some(idx) = indexable_idx {
                let sub = &subs[idx];

                // If there are other indexed subs, materialize them for intersection
                let other_indexed: Vec<BTreeSet<DocumentId>> = if all_indexable {
                    subs.iter().enumerate()
                        .filter(|(i, _)| *i != idx)
                        .filter_map(|(_, s)| execute_indexed(s, field_indexes, &[]))
                        .collect()
                } else {
                    vec![]
                };

                let mut cont = true;
                execute_indexed_lazy(sub, field_indexes, &mut |id| {
                    // Check against other indexed sets
                    for set in &other_indexed {
                        if !set.contains(&id) {
                            return true; // skip this id, continue iterating
                        }
                    }
                    cont = callback(id);
                    cont
                });
                return Some(cont);
            }
            None
        }
        Query::Or(_) => None, // OR requires full materialization
    }
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
                QueryOp::Regex(re) => {
                    field_val
                        .and_then(|v| v.as_str())
                        .is_some_and(|s| re.is_match(s))
                }
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
                        QueryOp::Exists(_) | QueryOp::Regex(_) => unreachable!(),
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
            // $exists and $regex can't be resolved by index
            if matches!(op, QueryOp::Exists(_) | QueryOp::Regex(_)) {
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
                QueryOp::Exists(_) | QueryOp::Regex(_) => return None,
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

/// Extract equality conditions from a query as a field→value map.
/// Returns None if the query contains no equality conditions.
/// Only extracts top-level $eq conditions (not nested in $or).
pub fn extract_eq_conditions(query: &Query) -> Option<std::collections::HashMap<String, IndexValue>> {
    let mut map = std::collections::HashMap::new();
    match query {
        Query::Field {
            field,
            op: QueryOp::Eq(v),
        } => {
            map.insert(field.clone(), v.clone());
        }
        Query::And(subs) => {
            for sub in subs {
                if let Query::Field {
                    field,
                    op: QueryOp::Eq(v),
                } = sub
                {
                    map.insert(field.clone(), v.clone());
                }
            }
        }
        _ => {}
    }
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

/// Check if every condition in a query is a $eq on one of the given fields.
/// Used to determine if a composite index prefix scan fully covers the query.
pub fn is_eq_only_on(query: &Query, fields: &[String]) -> bool {
    match query {
        Query::All => false,
        Query::Field {
            field,
            op: QueryOp::Eq(_),
        } => fields.iter().any(|f| f == field),
        Query::And(subs) => subs.iter().all(|s| is_eq_only_on(s, fields)),
        _ => false,
    }
}

/// Like `matches_doc` but operates directly on `&Value`, avoiding Document construction.
pub fn matches_value(query: &Query, data: &JsonValue) -> bool {
    match query {
        Query::All => true,
        Query::Field { field, op } => {
            let field_val = resolve_field_ref(data, field);
            match op {
                QueryOp::Exists(expected) => field_val.is_some() == *expected,
                QueryOp::Regex(re) => {
                    field_val
                        .and_then(|v| v.as_str())
                        .is_some_and(|s| re.is_match(s))
                }
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
                        QueryOp::Exists(_) | QueryOp::Regex(_) => unreachable!(),
                    }
                }
            }
        }
        Query::And(subs) => subs.iter().all(|s| matches_value(s, data)),
        Query::Or(subs) => subs.iter().any(|s| matches_value(s, data)),
    }
}

// ---------------------------------------------------------------------------
// Raw JSONB matching — extract only queried fields, skip full decode
// ---------------------------------------------------------------------------

/// Check if raw JSONB bytes match the query WITHOUT full deserialization.
/// Extracts only the fields referenced by the query using JSONB path lookup.
/// Returns `None` for legacy JSON text (bytes starting with '{' or '[').
pub fn matches_raw_jsonb(query: &Query, bytes: &[u8]) -> Option<bool> {
    // Only works with JSONB binary, not legacy JSON text
    if bytes.is_empty() || bytes[0] == b'{' || bytes[0] == b'[' {
        return None;
    }

    let raw = jsonb::RawJsonb::new(bytes);
    matches_raw_inner(query, &raw)
}

fn matches_raw_inner(query: &Query, raw: &jsonb::RawJsonb) -> Option<bool> {
    match query {
        Query::All => Some(true),
        Query::Field { field, op } => {
            // For $regex, we need the raw string — fall back to full decode
            if matches!(op, QueryOp::Regex(_)) {
                return extract_raw_string_value(raw, field).map(|opt_s| {
                    let QueryOp::Regex(re) = op else {
                        unreachable!()
                    };
                    opt_s.is_some_and(|s| re.is_match(&s))
                });
            }
            let field_val = extract_raw_field_value(raw, field);
            match op {
                QueryOp::Exists(expected) => Some(field_val.is_some() == *expected),
                _ => {
                    let Some(iv) = field_val else {
                        return Some(false);
                    };
                    Some(match op {
                        QueryOp::Eq(v) => iv == *v,
                        QueryOp::Ne(v) => iv != *v,
                        QueryOp::Gt(v) => iv > *v,
                        QueryOp::Gte(v) => iv >= *v,
                        QueryOp::Lt(v) => iv < *v,
                        QueryOp::Lte(v) => iv <= *v,
                        QueryOp::In(vals) => vals.contains(&iv),
                        QueryOp::Exists(_) | QueryOp::Regex(_) => unreachable!(),
                    })
                }
            }
        }
        Query::And(subs) => {
            for sub in subs {
                match matches_raw_inner(sub, raw) {
                    Some(true) => continue,
                    Some(false) => return Some(false),
                    None => return None,
                }
            }
            Some(true)
        }
        Query::Or(subs) => {
            for sub in subs {
                match matches_raw_inner(sub, raw) {
                    Some(true) => return Some(true),
                    Some(false) => continue,
                    None => return None,
                }
            }
            Some(false)
        }
    }
}

/// Extract a field value from raw JSONB using path lookup.
/// Handles dot-notation (e.g., "data.experience") by traversing nested objects.
fn extract_raw_field_value(raw: &jsonb::RawJsonb, field: &str) -> Option<IndexValue> {
    use jsonb::keypath::KeyPath;
    use std::borrow::Cow;

    let parts: Vec<&str> = field.split('.').collect();
    let keypath: Vec<KeyPath> = parts
        .iter()
        .map(|p| KeyPath::Name(Cow::Borrowed(p)))
        .collect();

    let owned = raw.get_by_keypath(keypath.iter()).ok()??;
    let field_raw = owned.as_raw();

    // Try scalar types in order of likelihood
    if let Ok(Some(s)) = field_raw.as_str() {
        return Some(IndexValue::parse_string(&s));
    }
    if let Ok(Some(n)) = field_raw.as_i64() {
        return Some(IndexValue::Integer(n));
    }
    if let Ok(Some(f)) = field_raw.as_f64() {
        return Some(IndexValue::Float(f));
    }
    if let Ok(Some(b)) = field_raw.as_bool() {
        return Some(IndexValue::Boolean(b));
    }
    if let Ok(Some(())) = field_raw.as_null() {
        return Some(IndexValue::Null);
    }
    // Complex types (array/object) — fall back to full decode
    None
}

/// Extract a raw string value from JSONB for regex matching.
/// Returns `Some(Some(string))` if the field is a string, `Some(None)` if the
/// field exists but is not a string or is missing, `None` if JSONB extraction fails.
fn extract_raw_string_value(raw: &jsonb::RawJsonb, field: &str) -> Option<Option<String>> {
    use jsonb::keypath::KeyPath;
    use std::borrow::Cow;

    let parts: Vec<&str> = field.split('.').collect();
    let keypath: Vec<KeyPath> = parts
        .iter()
        .map(|p| KeyPath::Name(Cow::Borrowed(p)))
        .collect();

    let owned = match raw.get_by_keypath(keypath.iter()) {
        Ok(Some(v)) => v,
        Ok(None) => return Some(None),
        Err(_) => return None,
    };
    let field_raw = owned.as_raw();
    match field_raw.as_str() {
        Ok(Some(s)) => Some(Some(s.to_string())),
        Ok(None) => Some(None),
        Err(_) => None,
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

    #[test]
    fn empty_query_matches_all() {
        let q = parse_query(&json!({})).unwrap();
        let doc = Document::new(1, json!({"anything": true})).unwrap();
        assert!(matches_doc(&q, &doc));
    }

    #[test]
    fn ne_operator() {
        let q = parse_query(&json!({"status": {"$ne": "deleted"}})).unwrap();
        let doc_ok = Document::new(1, json!({"status": "active"})).unwrap();
        let doc_bad = Document::new(2, json!({"status": "deleted"})).unwrap();
        assert!(matches_doc(&q, &doc_ok));
        assert!(!matches_doc(&q, &doc_bad));
    }

    #[test]
    fn in_operator() {
        let q = parse_query(&json!({"color": {"$in": ["red", "blue"]}})).unwrap();
        let doc_red = Document::new(1, json!({"color": "red"})).unwrap();
        let doc_green = Document::new(2, json!({"color": "green"})).unwrap();
        let doc_blue = Document::new(3, json!({"color": "blue"})).unwrap();
        assert!(matches_doc(&q, &doc_red));
        assert!(!matches_doc(&q, &doc_green));
        assert!(matches_doc(&q, &doc_blue));
    }

    #[test]
    fn exists_operator() {
        let q = parse_query(&json!({"email": {"$exists": true}})).unwrap();
        let doc_has = Document::new(1, json!({"email": "a@b.c"})).unwrap();
        let doc_no = Document::new(2, json!({"name": "Bob"})).unwrap();
        assert!(matches_doc(&q, &doc_has));
        assert!(!matches_doc(&q, &doc_no));
    }

    #[test]
    fn exists_false() {
        let q = parse_query(&json!({"deleted_at": {"$exists": false}})).unwrap();
        let doc_no = Document::new(1, json!({"name": "active"})).unwrap();
        let doc_has = Document::new(2, json!({"name": "old", "deleted_at": "2024-01-01"})).unwrap();
        assert!(matches_doc(&q, &doc_no));
        assert!(!matches_doc(&q, &doc_has));
    }

    #[test]
    fn nested_and_or() {
        let q = parse_query(&json!({
            "$and": [
                {"$or": [{"a": 1}, {"a": 2}]},
                {"b": {"$gt": 10}}
            ]
        }))
        .unwrap();

        let doc1 = Document::new(1, json!({"a": 1, "b": 20})).unwrap();
        let doc2 = Document::new(2, json!({"a": 3, "b": 20})).unwrap();
        let doc3 = Document::new(3, json!({"a": 1, "b": 5})).unwrap();
        assert!(matches_doc(&q, &doc1));
        assert!(!matches_doc(&q, &doc2));
        assert!(!matches_doc(&q, &doc3));
    }

    #[test]
    fn dot_notation_in_query() {
        let q = parse_query(&json!({"address.city": "NYC"})).unwrap();
        let doc = Document::new(1, json!({"address": {"city": "NYC"}})).unwrap();
        let doc2 = Document::new(2, json!({"address": {"city": "LA"}})).unwrap();
        assert!(matches_doc(&q, &doc));
        assert!(!matches_doc(&q, &doc2));
    }

    #[test]
    fn gt_and_lt_operators() {
        let q = parse_query(&json!({"score": {"$gt": 50, "$lt": 100}})).unwrap();
        let doc_in = Document::new(1, json!({"score": 75})).unwrap();
        let doc_low = Document::new(2, json!({"score": 30})).unwrap();
        let doc_high = Document::new(3, json!({"score": 100})).unwrap();
        let doc_edge = Document::new(4, json!({"score": 50})).unwrap();
        assert!(matches_doc(&q, &doc_in));
        assert!(!matches_doc(&q, &doc_low));
        assert!(!matches_doc(&q, &doc_high));
        assert!(!matches_doc(&q, &doc_edge));
    }

    #[test]
    fn lte_operator() {
        let q = parse_query(&json!({"age": {"$lte": 18}})).unwrap();
        let doc_under = Document::new(1, json!({"age": 15})).unwrap();
        let doc_exact = Document::new(2, json!({"age": 18})).unwrap();
        let doc_over = Document::new(3, json!({"age": 19})).unwrap();
        assert!(matches_doc(&q, &doc_under));
        assert!(matches_doc(&q, &doc_exact));
        assert!(!matches_doc(&q, &doc_over));
    }

    #[test]
    fn unknown_operator_errors() {
        let result = parse_query(&json!({"x": {"$bogus": "abc"}}));
        assert!(result.is_err());
    }

    #[test]
    fn regex_basic() {
        let q = parse_query(&json!({"name": {"$regex": "^Al"}})).unwrap();
        let doc_match = Document::new(1, json!({"name": "Alice"})).unwrap();
        let doc_no = Document::new(2, json!({"name": "Bob"})).unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_no));
    }

    #[test]
    fn regex_case_insensitive() {
        let q = parse_query(&json!({"name": {"$regex": "alice", "$options": "i"}})).unwrap();
        let doc = Document::new(1, json!({"name": "Alice"})).unwrap();
        assert!(matches_doc(&q, &doc));
    }

    #[test]
    fn regex_anchored() {
        let q = parse_query(&json!({"email": {"$regex": "^admin@.*\\.com$"}})).unwrap();
        let doc_match = Document::new(1, json!({"email": "admin@example.com"})).unwrap();
        let doc_no = Document::new(2, json!({"email": "user@example.com"})).unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_no));
    }

    #[test]
    fn regex_non_string_no_match() {
        let q = parse_query(&json!({"age": {"$regex": "30"}})).unwrap();
        let doc = Document::new(1, json!({"age": 30})).unwrap();
        assert!(!matches_doc(&q, &doc));
    }

    #[test]
    fn regex_matches_value_function() {
        let q = parse_query(&json!({"name": {"$regex": "ob$"}})).unwrap();
        let data = json!({"name": "Bob"});
        assert!(matches_value(&q, &data));
    }

    #[test]
    fn invalid_query_not_object() {
        let result = parse_query(&json!("string"));
        assert!(result.is_err());
    }

    #[test]
    fn matches_value_function() {
        let q = parse_query(&json!({"name": "Alice"})).unwrap();
        let data = json!({"name": "Alice", "age": 30});
        assert!(matches_value(&q, &data));
    }

    #[test]
    fn execute_indexed_eq() {
        let mut idx = FieldIndex::new("status".into());
        idx.insert_value(1, &json!({"status": "active"}));
        idx.insert_value(2, &json!({"status": "inactive"}));
        idx.insert_value(3, &json!({"status": "active"}));

        let mut field_indexes = std::collections::HashMap::new();
        field_indexes.insert("status".into(), idx);

        let q = parse_query(&json!({"status": "active"})).unwrap();
        let result = execute_indexed(&q, &field_indexes, &[]);
        assert_eq!(result, Some(BTreeSet::from([1, 3])));
    }

    #[test]
    fn execute_indexed_returns_none_for_unindexed() {
        let field_indexes = std::collections::HashMap::new();
        let q = parse_query(&json!({"unindexed": "val"})).unwrap();
        let result = execute_indexed(&q, &field_indexes, &[]);
        assert!(result.is_none());
    }

    #[test]
    fn count_indexed_eq() {
        let mut idx = FieldIndex::new("x".into());
        idx.insert_value(1, &json!({"x": 10}));
        idx.insert_value(2, &json!({"x": 10}));
        idx.insert_value(3, &json!({"x": 20}));

        let mut field_indexes = std::collections::HashMap::new();
        field_indexes.insert("x".into(), idx);

        let q = parse_query(&json!({"x": 10})).unwrap();
        assert_eq!(count_indexed(&q, &field_indexes), Some(2));
    }

    #[test]
    fn is_fully_indexed_check() {
        let mut idx = FieldIndex::new("a".into());
        idx.insert_value(1, &json!({"a": 1}));

        let mut field_indexes = std::collections::HashMap::new();
        field_indexes.insert("a".into(), idx);

        let q1 = parse_query(&json!({"a": 1})).unwrap();
        assert!(is_fully_indexed(&q1, &field_indexes));

        let q2 = parse_query(&json!({"b": 1})).unwrap();
        assert!(!is_fully_indexed(&q2, &field_indexes));

        let q3 = parse_query(&json!({"a": {"$exists": true}})).unwrap();
        assert!(!is_fully_indexed(&q3, &field_indexes));
    }

    #[test]
    fn parse_find_options_sort() {
        let req = json!({"sort": {"name": 1, "age": -1}, "skip": 5, "limit": 10});
        let opts = parse_find_options(&req).unwrap();
        assert!(opts.sort.is_some());
        assert_eq!(opts.skip, Some(5));
        assert_eq!(opts.limit, Some(10));
    }

    #[test]
    fn parse_find_options_invalid_sort_direction() {
        let req = json!({"sort": {"name": 2}});
        let result = parse_find_options(&req);
        assert!(result.is_err());
    }

    #[test]
    fn missing_field_no_match() {
        let q = parse_query(&json!({"missing": "value"})).unwrap();
        let doc = Document::new(1, json!({"other": "field"})).unwrap();
        assert!(!matches_doc(&q, &doc));
    }
}
