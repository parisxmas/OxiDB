use std::collections::BTreeSet;
use std::ops::Bound;

use serde_json::Value as JsonValue;

use std::collections::HashMap;

use crate::document::{Document, DocumentId};
use crate::error::{Error, Result};
use crate::index::CompositeIndex;
use crate::paged_field_index::PagedFieldIndex;
use crate::value::IndexValue;

// ---------------------------------------------------------------------------
// Query cost estimation (selectivity-based index selection)
// ---------------------------------------------------------------------------

/// Estimate how many documents a query condition would match using index metadata.
/// Returns None if the condition can't be estimated (no index, regex, exists, etc.).
pub fn estimate_rows(
    query: &Query,
    field_indexes: &HashMap<String, PagedFieldIndex>,
) -> Option<usize> {
    match query {
        Query::All => None,
        Query::Field { field, op } => {
            let idx = field_indexes.get(field.as_str())?;
            match op {
                QueryOp::Eq(v) => Some(idx.count_eq(v)),
                QueryOp::Gt(v) => Some(idx.count_range(Bound::Excluded(v), Bound::Unbounded)),
                QueryOp::Gte(v) => Some(idx.count_range(Bound::Included(v), Bound::Unbounded)),
                QueryOp::Lt(v) => Some(idx.count_range(Bound::Unbounded, Bound::Excluded(v))),
                QueryOp::Lte(v) => Some(idx.count_range(Bound::Unbounded, Bound::Included(v))),
                QueryOp::In(vals) => Some(idx.count_in(vals)),
                // `$ne` is no longer index-served (it must include missing-field
                // docs the index doesn't hold), so it has no index estimate.
                QueryOp::Ne(_)
                | QueryOp::Nin(_)
                | QueryOp::Exists(_)
                | QueryOp::Regex(_)
                | QueryOp::ElemMatch(_)
                | QueryOp::Not(_)
                | QueryOp::ArrayAll(_)
                | QueryOp::Size(_)
                | QueryOp::Type(_)
                | QueryOp::Mod(..) => None,
            }
        }
        Query::And(subs) => {
            let estimates: Vec<usize> = subs
                .iter()
                .filter_map(|s| estimate_rows(s, field_indexes))
                .collect();
            if estimates.is_empty() {
                None
            } else {
                Some(*estimates.iter().min().unwrap())
            }
        }
        Query::Or(subs) => {
            let estimates: Vec<usize> = subs
                .iter()
                .filter_map(|s| estimate_rows(s, field_indexes))
                .collect();
            if estimates.is_empty() {
                None
            } else {
                Some(estimates.iter().sum())
            }
        }
        Query::Nor(_) | Query::Expr(_) => None,
    }
}

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

    if let Some(sort_val) = request.get("sort")
        && let Some(obj) = sort_val.as_object()
    {
        let mut sort_fields = Vec::new();
        for (field, dir) in obj {
            let order = match dir.as_i64() {
                Some(1) => SortOrder::Asc,
                Some(-1) => SortOrder::Desc,
                _ => {
                    return Err(Error::InvalidQuery(
                        "sort direction must be 1 (asc) or -1 (desc)".into(),
                    ));
                }
            };
            sort_fields.push((field.clone(), order));
        }
        if !sort_fields.is_empty() {
            opts.sort = Some(sort_fields);
        }
    }

    if let Some(skip_val) = request.get("skip")
        && let Some(n) = skip_val.as_u64()
    {
        opts.skip = Some(n);
    }

    if let Some(limit_val) = request.get("limit")
        && let Some(n) = limit_val.as_u64()
    {
        opts.limit = Some(n);
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
    Nin(Vec<IndexValue>),
    Exists(bool),
    Regex(regex::Regex),
    ElemMatch(Box<Query>),
    Not(Vec<QueryOp>),         // negate: {"field": {"$not": {"$gt": 30}}}
    ArrayAll(Vec<IndexValue>), // array contains all: {"tags": {"$all": ["a","b"]}}
    Size(usize),               // array length: {"tags": {"$size": 3}}
    Type(String),              // JSON type check: {"field": {"$type": "string"}}
    Mod(i64, i64),             // modulo: {"qty": {"$mod": [4, 0]}}
}

#[derive(Debug, Clone)]
pub enum Query {
    Field { field: String, op: QueryOp },
    And(Vec<Query>),
    Or(Vec<Query>),
    Nor(Vec<Query>),     // none match: {"$nor": [{...}, {...}]}
    Expr(ExprCondition), // cross-field comparison: {"$expr": {"$gt": ["$a", "$b"]}}
    All,                 // match everything
}

// ---------------------------------------------------------------------------
// $expr types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ExprCondition {
    pub op: ExprCmpOp,
    pub lhs: ExprArg,
    pub rhs: ExprArg,
}

#[derive(Debug, Clone)]
pub enum ExprCmpOp {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone)]
pub enum ExprArg {
    FieldRef(String),    // "$fieldName" → resolves at query time
    Literal(IndexValue), // constant value
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
            "$nor" => {
                let arr = value
                    .as_array()
                    .ok_or_else(|| Error::InvalidQuery("$nor must be an array".into()))?;
                let subs: Result<Vec<Query>> = arr.iter().map(parse_query).collect();
                conditions.push(Query::Nor(subs?));
            }
            "$expr" => {
                conditions.push(Query::Expr(parse_expr(value)?));
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
            // Sorted + deduped at parse time: the post-filter can then
            // binary-search instead of scanning the whole operand list per
            // document (large $in lists were O(n·k)), and `count_in` no
            // longer double-counts duplicated operand values.
            let mut vals: Vec<IndexValue> = arr.iter().map(IndexValue::from_json).collect();
            vals.sort();
            vals.dedup();
            Ok(QueryOp::In(vals))
        }
        "$nin" => {
            let arr = op_val
                .as_array()
                .ok_or_else(|| Error::InvalidQuery("$nin must be an array".into()))?;
            let mut vals: Vec<IndexValue> = arr.iter().map(IndexValue::from_json).collect();
            vals.sort();
            vals.dedup();
            Ok(QueryOp::Nin(vals))
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
            let re = regex::Regex::new(&re_pattern)
                .map_err(|e| Error::InvalidQuery(format!("invalid regex: {}", e)))?;
            Ok(QueryOp::Regex(re))
        }
        "$elemMatch" => {
            let inner = parse_elem_match_inner(op_val)?;
            Ok(QueryOp::ElemMatch(Box::new(inner)))
        }
        "$not" => {
            let inner_obj = op_val
                .as_object()
                .ok_or_else(|| Error::InvalidQuery("$not must be an object".into()))?;
            let mut inner_ops = Vec::new();
            for (k, v) in inner_obj {
                if k == "$options" {
                    continue;
                }
                inner_ops.push(parse_op(k, v, inner_obj)?);
            }
            if inner_ops.is_empty() {
                return Err(Error::InvalidQuery(
                    "$not must contain at least one operator".into(),
                ));
            }
            Ok(QueryOp::Not(inner_ops))
        }
        "$all" => {
            let arr = op_val
                .as_array()
                .ok_or_else(|| Error::InvalidQuery("$all must be an array".into()))?;
            Ok(QueryOp::ArrayAll(
                arr.iter().map(IndexValue::from_json).collect(),
            ))
        }
        "$size" => {
            let n = op_val.as_u64().ok_or_else(|| {
                Error::InvalidQuery("$size must be a non-negative integer".into())
            })?;
            Ok(QueryOp::Size(n as usize))
        }
        "$type" => {
            let t = op_val
                .as_str()
                .ok_or_else(|| Error::InvalidQuery("$type must be a string".into()))?;
            Ok(QueryOp::Type(t.to_string()))
        }
        "$mod" => {
            let arr = op_val.as_array().ok_or_else(|| {
                Error::InvalidQuery("$mod must be an array [divisor, remainder]".into())
            })?;
            if arr.len() != 2 {
                return Err(Error::InvalidQuery(
                    "$mod must have exactly 2 elements [divisor, remainder]".into(),
                ));
            }
            let divisor = arr[0]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("$mod divisor must be an integer".into()))?;
            if divisor == 0 {
                return Err(Error::InvalidQuery("$mod divisor must not be zero".into()));
            }
            let remainder = arr[1]
                .as_i64()
                .ok_or_else(|| Error::InvalidQuery("$mod remainder must be an integer".into()))?;
            Ok(QueryOp::Mod(divisor, remainder))
        }
        _ => Err(Error::InvalidQuery(format!("unknown operator: {}", op_key))),
    }
}

/// Parse the inner query of a `$elemMatch`.
///
/// Three forms:
/// 1. Scalar value: `{"$elemMatch": 5}` → implicit $eq on element value
/// 2. Operator object: `{"$elemMatch": {"$gt": 80, "$lt": 85}}` → operators on element value
/// 3. Field-level sub-query: `{"$elemMatch": {"price": {"$gt": 100}}}` → query on element fields
///
/// Also used by `$pull`, whose operand is the same per-element condition
/// shape (MongoDB semantics).
pub(crate) fn parse_elem_match_inner(val: &JsonValue) -> Result<Query> {
    if !val.is_object() {
        // Scalar: implicit $eq on element value
        return Ok(Query::Field {
            field: String::new(),
            op: QueryOp::Eq(IndexValue::from_json(val)),
        });
    }
    let obj = val.as_object().unwrap();
    if obj.is_empty() {
        return Ok(Query::All);
    }
    let has_ops = obj.keys().any(|k| k.starts_with('$'));
    if has_ops {
        // Operator object: apply operators to element value itself (empty field = self)
        let mut conditions: Vec<Query> = Vec::new();
        for (op_key, op_val) in obj {
            if op_key == "$options" {
                continue;
            }
            match op_key.as_str() {
                "$and" | "$or" => {
                    // Allow $and / $or inside $elemMatch
                    let arr = op_val
                        .as_array()
                        .ok_or_else(|| Error::InvalidQuery(format!("{op_key} must be an array")))?;
                    let subs: Result<Vec<Query>> = arr.iter().map(parse_elem_match_inner).collect();
                    if op_key == "$and" {
                        conditions.push(Query::And(subs?));
                    } else {
                        conditions.push(Query::Or(subs?));
                    }
                }
                _ => {
                    let op = parse_op(op_key, op_val, obj)?;
                    conditions.push(Query::Field {
                        field: String::new(),
                        op,
                    });
                }
            }
        }
        match conditions.len() {
            0 => Ok(Query::All),
            1 => Ok(conditions.pop().unwrap()),
            _ => Ok(Query::And(conditions)),
        }
    } else {
        // Sub-query: field names resolve on each array element
        parse_query(val)
    }
}

/// Parse `$expr` value: `{"$gt": ["$field1", "$field2"]}` or `{"$gt": ["$field", 100]}`.
fn parse_expr(val: &JsonValue) -> Result<ExprCondition> {
    let obj = val
        .as_object()
        .ok_or_else(|| Error::InvalidQuery("$expr must be an object".into()))?;
    if obj.len() != 1 {
        return Err(Error::InvalidQuery(
            "$expr must contain exactly one comparison operator".into(),
        ));
    }
    let (op_str, args_val) = obj.iter().next().unwrap();
    let op = match op_str.as_str() {
        "$eq" => ExprCmpOp::Eq,
        "$ne" => ExprCmpOp::Ne,
        "$gt" => ExprCmpOp::Gt,
        "$gte" => ExprCmpOp::Gte,
        "$lt" => ExprCmpOp::Lt,
        "$lte" => ExprCmpOp::Lte,
        other => {
            return Err(Error::InvalidQuery(format!(
                "$expr: unsupported operator: {other}"
            )));
        }
    };
    let args = args_val.as_array().ok_or_else(|| {
        Error::InvalidQuery("$expr operator value must be a 2-element array".into())
    })?;
    if args.len() != 2 {
        return Err(Error::InvalidQuery(
            "$expr operator value must have exactly 2 elements".into(),
        ));
    }
    let lhs = parse_expr_arg(&args[0]);
    let rhs = parse_expr_arg(&args[1]);
    Ok(ExprCondition { op, lhs, rhs })
}

fn parse_expr_arg(val: &JsonValue) -> ExprArg {
    if let Some(s) = val.as_str()
        && let Some(field) = s.strip_prefix('$')
    {
        return ExprArg::FieldRef(field.to_string());
    }
    ExprArg::Literal(IndexValue::from_json(val))
}

// ---------------------------------------------------------------------------
// Execution: evaluate a query against indexes + documents
// ---------------------------------------------------------------------------

/// Execute a query using available indexes. Returns matching document IDs.
pub fn execute_indexed(
    query: &Query,
    field_indexes: &std::collections::HashMap<String, PagedFieldIndex>,
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
            if all_resolved { Some(result) } else { None }
        }
        Query::Nor(_) | Query::Expr(_) => None,
    }
}

fn execute_field_op(
    field: &str,
    op: &QueryOp,
    field_indexes: &std::collections::HashMap<String, PagedFieldIndex>,
    _composite_indexes: &[CompositeIndex],
) -> Option<BTreeSet<DocumentId>> {
    let idx = field_indexes.get(field)?;

    Some(match op {
        QueryOp::Eq(v) => idx.find_eq(v),
        QueryOp::Gt(v) => idx.find_range(Bound::Excluded(v), Bound::Unbounded),
        QueryOp::Gte(v) => idx.find_range(Bound::Included(v), Bound::Unbounded),
        QueryOp::Lt(v) => idx.find_range(Bound::Unbounded, Bound::Excluded(v)),
        QueryOp::Lte(v) => idx.find_range(Bound::Unbounded, Bound::Included(v)),
        QueryOp::In(vals) => idx.find_in(vals),
        // `$ne` can't be served by the field index alone: the index omits
        // documents that lack the field, but those MUST match `$ne` (MongoDB
        // semantics). Fall through to a full scan + post-filter.
        QueryOp::Ne(_)
        | QueryOp::Nin(_)
        | QueryOp::Exists(_)
        | QueryOp::Regex(_)
        | QueryOp::ElemMatch(_)
        | QueryOp::Not(_)
        | QueryOp::ArrayAll(_)
        | QueryOp::Size(_)
        | QueryOp::Type(_)
        | QueryOp::Mod(..) => return None,
    })
}

/// Check if any element in a JSON array matches the inner query.
/// Used by both `matches_doc` and `matches_value`.
fn array_any_matches(arr: &[JsonValue], inner: &Query) -> bool {
    arr.iter().any(|elem| matches_value(inner, elem))
}

// ---------------------------------------------------------------------------
// Lazy index execution — callback-based with early termination
// ---------------------------------------------------------------------------

/// Tightest-bound accumulator for an AND of range conditions on one field.
/// Each `$gt/$gte/$lt/$lte` narrows the range: the highest lower bound and
/// lowest upper bound win, with the exclusive form winning ties (it is
/// strictly tighter). Last-wins slot assignment — the previous behavior —
/// silently *widened* ranges (`{$gt:10, $gte:5}` started at 5), and the
/// callers skip the post-filter for these queries, so the widened range
/// went straight into results.
struct MergedRange<'a> {
    /// (value, exclusive) — exclusive means `$gt` / `$lt`.
    lower: Option<(&'a IndexValue, bool)>,
    upper: Option<(&'a IndexValue, bool)>,
}

impl<'a> MergedRange<'a> {
    fn new() -> Self {
        Self {
            lower: None,
            upper: None,
        }
    }

    fn add_lower(&mut self, v: &'a IndexValue, exclusive: bool) {
        let tighter = match self.lower {
            None => true,
            Some((cur, cur_ex)) => match v.cmp(cur) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => exclusive && !cur_ex,
                std::cmp::Ordering::Less => false,
            },
        };
        if tighter {
            self.lower = Some((v, exclusive));
        }
    }

    fn add_upper(&mut self, v: &'a IndexValue, exclusive: bool) {
        let tighter = match self.upper {
            None => true,
            Some((cur, cur_ex)) => match v.cmp(cur) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Equal => exclusive && !cur_ex,
                std::cmp::Ordering::Greater => false,
            },
        };
        if tighter {
            self.upper = Some((v, exclusive));
        }
    }

    /// True when no value can satisfy the merged bounds (lower > upper, or
    /// equal with either side exclusive). Callers must not feed such a
    /// range to a BTree range iterator — std panics on start > end.
    fn is_empty(&self) -> bool {
        if let (Some((lo, lo_ex)), Some((hi, hi_ex))) = (self.lower, self.upper) {
            match lo.cmp(hi) {
                std::cmp::Ordering::Greater => true,
                std::cmp::Ordering::Equal => lo_ex || hi_ex,
                std::cmp::Ordering::Less => false,
            }
        } else {
            false
        }
    }

    /// Whether a single value satisfies the merged bounds.
    fn contains(&self, v: &IndexValue) -> bool {
        if let Some((lo, lo_ex)) = self.lower {
            match v.cmp(lo) {
                std::cmp::Ordering::Less => return false,
                std::cmp::Ordering::Equal if lo_ex => return false,
                _ => {}
            }
        }
        if let Some((hi, hi_ex)) = self.upper {
            match v.cmp(hi) {
                std::cmp::Ordering::Greater => return false,
                std::cmp::Ordering::Equal if hi_ex => return false,
                _ => {}
            }
        }
        true
    }

    fn start(&self) -> Bound<&'a IndexValue> {
        match self.lower {
            Some((v, true)) => Bound::Excluded(v),
            Some((v, false)) => Bound::Included(v),
            None => Bound::Unbounded,
        }
    }

    fn end(&self) -> Bound<&'a IndexValue> {
        match self.upper {
            Some((v, true)) => Bound::Excluded(v),
            Some((v, false)) => Bound::Included(v),
            None => Bound::Unbounded,
        }
    }
}

/// Try to merge an AND of conditions on the same indexed field into a single
/// range. Returns the merged (start, end) bounds and the field index, or None.
/// A contradictory range (e.g. `{$gt: 10, $lt: 5}`) returns None so the
/// caller falls back to the post-filter scan, which yields the correct empty
/// result without risking a reversed-bounds range iteration.
fn try_merge_range_and<'a>(
    subs: &'a [Query],
    field_indexes: &'a std::collections::HashMap<String, PagedFieldIndex>,
) -> Option<(
    &'a PagedFieldIndex,
    Bound<&'a IndexValue>,
    Bound<&'a IndexValue>,
)> {
    let mut field_name: Option<&str> = None;
    let mut range = MergedRange::new();

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
                    QueryOp::Gte(v) => range.add_lower(v, false),
                    QueryOp::Gt(v) => range.add_lower(v, true),
                    QueryOp::Lt(v) => range.add_upper(v, true),
                    QueryOp::Lte(v) => range.add_upper(v, false),
                    _ => return None,
                }
            }
            _ => return None,
        }
    }

    let field = field_name?;
    let idx = field_indexes.get(field)?;

    if range.is_empty() {
        return None;
    }

    Some((idx, range.start(), range.end()))
}

/// Execute a query lazily against indexes, calling `callback` for each matching
/// DocumentId. The callback returns `true` to continue or `false` to stop.
/// Returns `Some(true)` if the query was fully handled by indexes,
/// `Some(false)` if stopped early by callback, `None` if indexes couldn't handle it.
pub fn execute_indexed_lazy(
    query: &Query,
    field_indexes: &std::collections::HashMap<String, PagedFieldIndex>,
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
                // `$ne` omits missing-field docs in the index — fall through to
                // a full scan + post-filter (see execute_field_op).
                QueryOp::Ne(_)
                | QueryOp::Nin(_)
                | QueryOp::Exists(_)
                | QueryOp::Regex(_)
                | QueryOp::ElemMatch(_)
                | QueryOp::Not(_)
                | QueryOp::ArrayAll(_)
                | QueryOp::Size(_)
                | QueryOp::Type(_)
                | QueryOp::Mod(..) => return None,
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
            // For AND with multiple indexed conditions, pick the most selective
            // sub-query (smallest estimated result set) and iterate it lazily.
            let mut best_idx: Option<(usize, usize)> = None; // (index, estimated_rows)
            let mut all_indexable = true;
            for (i, sub) in subs.iter().enumerate() {
                if execute_indexed(sub, field_indexes, &[]).is_some() {
                    let est = estimate_rows(sub, field_indexes).unwrap_or(usize::MAX);
                    if best_idx.is_none() || est < best_idx.unwrap().1 {
                        best_idx = Some((i, est));
                    }
                } else {
                    all_indexable = false;
                }
            }
            if let Some((idx, _)) = best_idx {
                let sub = &subs[idx];

                // If there are other indexed subs, materialize them for intersection
                let other_indexed: Vec<BTreeSet<DocumentId>> = if all_indexable {
                    subs.iter()
                        .enumerate()
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
        Query::Or(_) | Query::Nor(_) | Query::Expr(_) => None,
    }
}

// ---------------------------------------------------------------------------
// Filter: evaluate a query against a single document (post-filter)
// ---------------------------------------------------------------------------

/// Evaluate a single field-level operator against a (possibly missing) field value.
/// Shared by `matches_doc` and `matches_value`.
fn eval_field_op(op: &QueryOp, field_val: Option<&JsonValue>) -> bool {
    match op {
        QueryOp::Exists(expected) => field_val.is_some() == *expected,
        QueryOp::Regex(re) => field_val
            .and_then(|v| v.as_str())
            .is_some_and(|s| re.is_match(s)),
        QueryOp::ElemMatch(inner) => match field_val {
            Some(JsonValue::Array(arr)) => array_any_matches(arr, inner),
            _ => false,
        },
        QueryOp::Not(inner_ops) => {
            // $not inverts the result. Missing field → inner ops return false → !false = true.
            !inner_ops
                .iter()
                .all(|inner| eval_field_op(inner, field_val))
        }
        QueryOp::ArrayAll(required) => match field_val {
            // `$all: []` matches nothing (MongoDB semantics), not every array.
            Some(JsonValue::Array(arr)) if !required.is_empty() => {
                let arr_vals: Vec<IndexValue> = arr.iter().map(IndexValue::from_json).collect();
                required.iter().all(|v| arr_vals.contains(v))
            }
            _ => false,
        },
        QueryOp::Size(n) => match field_val {
            Some(JsonValue::Array(arr)) => arr.len() == *n,
            _ => false,
        },
        QueryOp::Type(type_name) => match field_val {
            Some(v) => json_type_matches(v, type_name),
            None => false,
        },
        QueryOp::Mod(divisor, remainder) => {
            // A zero divisor would panic on `%`; treat it as a non-match
            // rather than aborting the whole query (MongoDB rejects it).
            if *divisor == 0 {
                return false;
            }
            field_val
                .and_then(|v| v.as_f64())
                .filter(|n| n.is_finite())
                .is_some_and(|n| {
                    // Truncate toward zero (MongoDB semantics), guarding the
                    // i64 cast so an out-of-range float doesn't saturate to
                    // i64::MAX/MIN and produce an arbitrary match.
                    let t = n.trunc();
                    if t < i64::MIN as f64 || t > i64::MAX as f64 {
                        return false;
                    }
                    (t as i64) % divisor == *remainder
                })
        }
        _ => {
            let Some(val) = field_val else {
                // A missing field is "not equal to" any concrete value, so
                // `$ne`/`$nin` MATCH a missing field (MongoDB semantics) —
                // except when the operand is null, since MongoDB treats an
                // absent field as null for these comparisons.
                return match op {
                    QueryOp::Ne(v) => *v != IndexValue::Null,
                    QueryOp::Nin(vals) => vals.binary_search(&IndexValue::Null).is_err(),
                    _ => false,
                };
            };
            let iv = IndexValue::from_json(val);
            match op {
                QueryOp::Eq(v) => iv == *v,
                QueryOp::Ne(v) => iv != *v,
                QueryOp::Gt(v) => iv > *v,
                QueryOp::Gte(v) => iv >= *v,
                QueryOp::Lt(v) => iv < *v,
                QueryOp::Lte(v) => iv <= *v,
                // The operand list is sorted+deduped at parse time.
                QueryOp::In(vals) => vals.binary_search(&iv).is_ok(),
                QueryOp::Nin(vals) => vals.binary_search(&iv).is_err(),
                _ => unreachable!(),
            }
        }
    }
}

/// Check if a JSON value matches a MongoDB-style type name.
fn json_type_matches(val: &JsonValue, type_name: &str) -> bool {
    match type_name {
        // A "double" is a non-integral JSON number only — an integer like `42`
        // must NOT match `$type:"double"`. "number" is the union of all
        // numeric types.
        "double" => val.is_f64(),
        "number" => val.is_f64() || val.is_i64() || val.is_u64(),
        "string" => val.is_string(),
        "object" => val.is_object(),
        "array" => val.is_array(),
        "bool" | "boolean" => val.is_boolean(),
        "null" => val.is_null(),
        "int" | "integer" | "long" => val.is_i64() || val.is_u64(),
        _ => false,
    }
}

/// Evaluate `$expr` by resolving field references from a document.
fn eval_expr_with<F>(expr: &ExprCondition, resolve: F) -> bool
where
    F: Fn(&str) -> Option<IndexValue>,
{
    let lhs = match &expr.lhs {
        ExprArg::FieldRef(f) => resolve(f),
        ExprArg::Literal(v) => Some(v.clone()),
    };
    let rhs = match &expr.rhs {
        ExprArg::FieldRef(f) => resolve(f),
        ExprArg::Literal(v) => Some(v.clone()),
    };
    match (lhs, rhs) {
        (Some(l), Some(r)) => match expr.op {
            ExprCmpOp::Eq => l == r,
            ExprCmpOp::Ne => l != r,
            ExprCmpOp::Gt => l > r,
            ExprCmpOp::Gte => l >= r,
            ExprCmpOp::Lt => l < r,
            ExprCmpOp::Lte => l <= r,
        },
        _ => false,
    }
}

pub fn matches_doc(query: &Query, doc: &Document) -> bool {
    match query {
        Query::All => true,
        Query::Field { field, op } => eval_field_op(op, doc.get_field(field)),
        Query::And(subs) => subs.iter().all(|s| matches_doc(s, doc)),
        Query::Or(subs) => subs.iter().any(|s| matches_doc(s, doc)),
        Query::Nor(subs) => !subs.iter().any(|s| matches_doc(s, doc)),
        Query::Expr(expr) => eval_expr_with(expr, |f| doc.get_field(f).map(IndexValue::from_json)),
    }
}

/// Resolve a field path (with dot notation) directly on a &Value.
/// An empty path returns the data itself (used by `$elemMatch` for scalar elements).
fn resolve_field_ref<'a>(data: &'a JsonValue, path: &str) -> Option<&'a JsonValue> {
    if path.is_empty() {
        return Some(data);
    }
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
    field_indexes: &std::collections::HashMap<String, PagedFieldIndex>,
) -> bool {
    match query {
        Query::All => true,
        Query::Field { field, op } => {
            if matches!(
                op,
                QueryOp::Exists(_)
                    | QueryOp::Regex(_)
                    | QueryOp::Nin(_)
                    | QueryOp::ElemMatch(_)
                    | QueryOp::Not(_)
                    | QueryOp::ArrayAll(_)
                    | QueryOp::Size(_)
                    | QueryOp::Type(_)
                    | QueryOp::Mod(..)
            ) {
                return false;
            }
            field_indexes.contains_key(field.as_str())
        }
        Query::And(subs) => subs.iter().all(|s| is_fully_indexed(s, field_indexes)),
        Query::Or(subs) => subs.iter().all(|s| is_fully_indexed(s, field_indexes)),
        Query::Nor(_) | Query::Expr(_) => false,
    }
}

/// Count matching documents using only indexes (no BTreeSet allocation).
/// Returns None if the query can't be counted by index alone.
pub fn count_indexed(
    query: &Query,
    field_indexes: &std::collections::HashMap<String, PagedFieldIndex>,
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
                QueryOp::Nin(_)
                | QueryOp::Exists(_)
                | QueryOp::Regex(_)
                | QueryOp::ElemMatch(_)
                | QueryOp::Not(_)
                | QueryOp::ArrayAll(_)
                | QueryOp::Size(_)
                | QueryOp::Type(_)
                | QueryOp::Mod(..) => return None,
            })
        }
        Query::And(subs) => {
            // Common case: AND of range conditions on the SAME indexed field
            // e.g. {created_at: {$gte: "2023-01-01", $lt: "2024-01-01"}}
            count_single_field_and(subs, field_indexes)
        }
        Query::Or(_) | Query::Nor(_) | Query::Expr(_) => None,
    }
}

/// Handle AND of range conditions on the same field — count without BTreeSet.
fn count_single_field_and(
    subs: &[Query],
    field_indexes: &std::collections::HashMap<String, PagedFieldIndex>,
) -> Option<usize> {
    let mut field_name: Option<&str> = None;
    let mut range = MergedRange::new();
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
                    QueryOp::Gte(v) => range.add_lower(v, false),
                    QueryOp::Gt(v) => range.add_lower(v, true),
                    QueryOp::Lt(v) => range.add_upper(v, true),
                    QueryOp::Lte(v) => range.add_upper(v, false),
                    QueryOp::Eq(v) => match eq_value {
                        // Two different $eq values on the same field can never
                        // both hold — the count is exactly 0.
                        Some(prev) if prev != v => return Some(0),
                        _ => eq_value = Some(v),
                    },
                    _ => return None,
                }
            }
            _ => return None,
        }
    }

    let field = field_name?;
    let idx = field_indexes.get(field)?;

    // An $eq must also satisfy every range constraint in the same AND —
    // `{x: {$eq: 5, $gt: 10}}` matches nothing.
    if let Some(eq_val) = eq_value {
        if !range.contains(eq_val) {
            return Some(0);
        }
        return Some(idx.count_eq(eq_val));
    }

    if range.is_empty() {
        return Some(0);
    }

    Some(idx.count_range(range.start(), range.end()))
}

/// Extract equality conditions from a query as a field→value map.
/// Returns None if the query contains no equality conditions.
/// Only extracts top-level $eq conditions (not nested in $or).
pub fn extract_eq_conditions(
    query: &Query,
) -> Option<std::collections::HashMap<String, IndexValue>> {
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
    if map.is_empty() { None } else { Some(map) }
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
        Query::Field { field, op } => eval_field_op(op, resolve_field_ref(data, field)),
        Query::And(subs) => subs.iter().all(|s| matches_value(s, data)),
        Query::Or(subs) => subs.iter().any(|s| matches_value(s, data)),
        Query::Nor(subs) => !subs.iter().any(|s| matches_value(s, data)),
        Query::Expr(expr) => eval_expr_with(expr, |f| {
            resolve_field_ref(data, f).map(IndexValue::from_json)
        }),
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
            // Complex ops require full decode — fall back
            if matches!(
                op,
                QueryOp::ElemMatch(_)
                    | QueryOp::Not(_)
                    | QueryOp::ArrayAll(_)
                    | QueryOp::Size(_)
                    | QueryOp::Type(_)
                    | QueryOp::Mod(..)
            ) {
                return None;
            }
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
                        // Missing field matches `$ne`/`$nin` (MongoDB semantics),
                        // unless the operand is null (absent ≡ null here).
                        return Some(match op {
                            QueryOp::Ne(v) => *v != IndexValue::Null,
                            QueryOp::Nin(vals) => vals.binary_search(&IndexValue::Null).is_err(),
                            _ => false,
                        });
                    };
                    Some(match op {
                        QueryOp::Eq(v) => iv == *v,
                        QueryOp::Ne(v) => iv != *v,
                        QueryOp::Gt(v) => iv > *v,
                        QueryOp::Gte(v) => iv >= *v,
                        QueryOp::Lt(v) => iv < *v,
                        QueryOp::Lte(v) => iv <= *v,
                        // Sorted+deduped at parse time — binary search.
                        QueryOp::In(vals) => vals.binary_search(&iv).is_ok(),
                        QueryOp::Nin(vals) => vals.binary_search(&iv).is_err(),
                        _ => unreachable!(),
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
        Query::Nor(_) | Query::Expr(_) => None,
    }
}

/// Extract a field value from raw JSONB using path lookup.
/// Handles dot-notation (e.g., "data.experience") by traversing nested objects.
/// Uses direct scalar accessors to avoid full serde_json::Value allocation.
fn extract_raw_field_value(raw: &jsonb::RawJsonb, field: &str) -> Option<IndexValue> {
    use jsonb::keypath::KeyPath;
    use std::borrow::Cow;

    let parts: Vec<&str> = field.split('.').collect();
    let keypath: Vec<KeyPath> = parts
        .iter()
        .map(|p| KeyPath::Name(Cow::Borrowed(p)))
        .collect();

    let owned = raw.get_by_keypath(keypath.iter()).ok()??;
    let sub = owned.as_raw();

    // Try scalar accessors directly — avoids allocating serde_json::Value
    if let Ok(Some(())) = sub.as_null() {
        return Some(IndexValue::Null);
    }
    if let Ok(Some(b)) = sub.as_bool() {
        return Some(IndexValue::Boolean(b));
    }
    if let Ok(Some(i)) = sub.as_i64() {
        return Some(IndexValue::Integer(i));
    }
    if let Ok(Some(f)) = sub.as_f64() {
        return Some(IndexValue::Float(f));
    }
    if let Ok(Some(s)) = sub.as_str() {
        return Some(IndexValue::parse_string(&s));
    }
    // Fallback for arrays/objects — full decode
    let val: serde_json::Value = jsonb::from_raw_jsonb(&sub).ok()?;
    Some(IndexValue::from_json(&val))
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
    fn nin_operator() {
        let q = parse_query(&json!({"color": {"$nin": ["red", "blue"]}})).unwrap();
        let doc_red = Document::new(1, json!({"color": "red"})).unwrap();
        let doc_green = Document::new(2, json!({"color": "green"})).unwrap();
        let doc_blue = Document::new(3, json!({"color": "blue"})).unwrap();
        let doc_yellow = Document::new(4, json!({"color": "yellow"})).unwrap();
        assert!(!matches_doc(&q, &doc_red));
        assert!(matches_doc(&q, &doc_green));
        assert!(!matches_doc(&q, &doc_blue));
        assert!(matches_doc(&q, &doc_yellow));
    }

    #[test]
    fn nin_missing_field() {
        // MongoDB treats an absent field as null; null is not in the operand
        // list, so a missing field MATCHES `$nin` (and would match `$ne` too).
        let q = parse_query(&json!({"color": {"$nin": ["red", "blue"]}})).unwrap();
        let doc_no_field = Document::new(1, json!({"name": "test"})).unwrap();
        assert!(matches_doc(&q, &doc_no_field));

        // …but if null is one of the operands, the absent field is excluded.
        let q_null = parse_query(&json!({"color": {"$nin": ["red", null]}})).unwrap();
        assert!(!matches_doc(&q_null, &doc_no_field));
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
        let mut idx = PagedFieldIndex::new("status".into());
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
        let mut idx = PagedFieldIndex::new("x".into());
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
        let mut idx = PagedFieldIndex::new("a".into());
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

    // ---------------------------------------------------------------
    // $elemMatch tests
    // ---------------------------------------------------------------

    #[test]
    fn elem_match_scalar_eq() {
        let q = parse_query(&json!({"tags": {"$elemMatch": {"$eq": "red"}}})).unwrap();
        let doc_match = Document::new(1, json!({"tags": ["red", "blue"]})).unwrap();
        let doc_no = Document::new(2, json!({"tags": ["green", "blue"]})).unwrap();
        let doc_empty = Document::new(3, json!({"tags": []})).unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_no));
        assert!(!matches_doc(&q, &doc_empty));
    }

    #[test]
    fn elem_match_operator_range() {
        // Match array element where element >= 80 AND < 85
        let q = parse_query(&json!({"scores": {"$elemMatch": {"$gte": 80, "$lt": 85}}})).unwrap();
        let doc_match = Document::new(1, json!({"scores": [70, 82, 90]})).unwrap();
        let doc_no = Document::new(2, json!({"scores": [70, 75, 90]})).unwrap();
        let doc_edge = Document::new(3, json!({"scores": [80]})).unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_no));
        assert!(matches_doc(&q, &doc_edge));
    }

    #[test]
    fn elem_match_object_fields() {
        // Match array of objects where element has price > 100
        let q = parse_query(&json!({"items": {"$elemMatch": {"price": {"$gt": 100}}}})).unwrap();
        let doc_match = Document::new(
            1,
            json!({"items": [
                {"name": "A", "price": 50},
                {"name": "B", "price": 150}
            ]}),
        )
        .unwrap();
        let doc_no = Document::new(
            2,
            json!({"items": [
                {"name": "A", "price": 50},
                {"name": "B", "price": 80}
            ]}),
        )
        .unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_no));
    }

    #[test]
    fn elem_match_multiple_field_conditions() {
        // Match element where price > 100 AND inStock == true (both on SAME element)
        let q = parse_query(&json!({"items": {"$elemMatch": {
            "price": {"$gt": 100},
            "inStock": true
        }}}))
        .unwrap();
        // Element B has price > 100 AND inStock true → match
        let doc_match = Document::new(
            1,
            json!({"items": [
                {"price": 50, "inStock": true},
                {"price": 150, "inStock": true}
            ]}),
        )
        .unwrap();
        // No single element satisfies both: price>100 is B (inStock false), inStock true is A (price 50)
        let doc_no = Document::new(
            2,
            json!({"items": [
                {"price": 50, "inStock": true},
                {"price": 150, "inStock": false}
            ]}),
        )
        .unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_no));
    }

    #[test]
    fn elem_match_not_array_fails() {
        let q = parse_query(&json!({"tags": {"$elemMatch": {"$eq": "red"}}})).unwrap();
        // Field is a string, not an array
        let doc_str = Document::new(1, json!({"tags": "red"})).unwrap();
        // Field is missing
        let doc_missing = Document::new(2, json!({"other": 1})).unwrap();
        // Field is number
        let doc_num = Document::new(3, json!({"tags": 42})).unwrap();
        assert!(!matches_doc(&q, &doc_str));
        assert!(!matches_doc(&q, &doc_missing));
        assert!(!matches_doc(&q, &doc_num));
    }

    #[test]
    fn elem_match_in_operator() {
        let q = parse_query(&json!({"tags": {"$elemMatch": {"$in": ["red", "yellow"]}}})).unwrap();
        let doc_match = Document::new(1, json!({"tags": ["blue", "red"]})).unwrap();
        let doc_no = Document::new(2, json!({"tags": ["blue", "green"]})).unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_no));
    }

    #[test]
    fn elem_match_nested_dot_notation() {
        let q =
            parse_query(&json!({"items": {"$elemMatch": {"details.cost": {"$gte": 50}}}})).unwrap();
        let doc_match = Document::new(
            1,
            json!({"items": [
                {"details": {"cost": 30}},
                {"details": {"cost": 75}}
            ]}),
        )
        .unwrap();
        let doc_no = Document::new(
            2,
            json!({"items": [
                {"details": {"cost": 10}},
                {"details": {"cost": 20}}
            ]}),
        )
        .unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_no));
    }

    #[test]
    fn elem_match_with_regex() {
        let q =
            parse_query(&json!({"emails": {"$elemMatch": {"$regex": "@example\\.com$"}}})).unwrap();
        let doc_match =
            Document::new(1, json!({"emails": ["a@test.com", "b@example.com"]})).unwrap();
        let doc_no = Document::new(2, json!({"emails": ["a@test.com", "b@other.org"]})).unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_no));
    }

    #[test]
    fn elem_match_matches_value() {
        // Test through matches_value (used by pipeline $match)
        let q = parse_query(&json!({"nums": {"$elemMatch": {"$gt": 5}}})).unwrap();
        let data = json!({"nums": [1, 3, 7, 2]});
        assert!(matches_value(&q, &data));
        let data2 = json!({"nums": [1, 3, 4, 2]});
        assert!(!matches_value(&q, &data2));
    }

    #[test]
    fn elem_match_combined_with_other_conditions() {
        // $elemMatch combined with a regular field condition via AND
        let q = parse_query(&json!({
            "status": "active",
            "scores": {"$elemMatch": {"$gte": 90}}
        }))
        .unwrap();
        let doc_match = Document::new(1, json!({"status": "active", "scores": [80, 95]})).unwrap();
        let doc_wrong_status =
            Document::new(2, json!({"status": "inactive", "scores": [80, 95]})).unwrap();
        let doc_low_scores =
            Document::new(3, json!({"status": "active", "scores": [80, 85]})).unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_wrong_status));
        assert!(!matches_doc(&q, &doc_low_scores));
    }

    #[test]
    fn elem_match_exists_inside() {
        let q = parse_query(&json!({"items": {"$elemMatch": {"discount": {"$exists": true}}}}))
            .unwrap();
        let doc_match = Document::new(
            1,
            json!({"items": [
                {"price": 10},
                {"price": 20, "discount": 5}
            ]}),
        )
        .unwrap();
        let doc_no = Document::new(
            2,
            json!({"items": [
                {"price": 10},
                {"price": 20}
            ]}),
        )
        .unwrap();
        assert!(matches_doc(&q, &doc_match));
        assert!(!matches_doc(&q, &doc_no));
    }

    #[test]
    fn raw_jsonb_gt_integer() {
        let doc = json!({"name": "Alice", "age": 30, "city": "NYC"});
        let bytes = crate::codec::encode_doc(&doc).unwrap();
        let q = parse_query(&json!({"age": {"$gt": 28}})).unwrap();
        assert_eq!(matches_raw_jsonb(&q, &bytes), Some(true));

        let q2 = parse_query(&json!({"age": {"$gt": 35}})).unwrap();
        assert_eq!(matches_raw_jsonb(&q2, &bytes), Some(false));
    }

    // ---------------------------------------------------------------
    // $not tests
    // ---------------------------------------------------------------

    #[test]
    fn not_basic_gt() {
        // age is NOT > 30
        let q = parse_query(&json!({"age": {"$not": {"$gt": 30}}})).unwrap();
        let young = Document::new(1, json!({"age": 25})).unwrap();
        let exact = Document::new(2, json!({"age": 30})).unwrap();
        let old = Document::new(3, json!({"age": 35})).unwrap();
        assert!(matches_doc(&q, &young));
        assert!(matches_doc(&q, &exact)); // 30 is NOT > 30
        assert!(!matches_doc(&q, &old));
    }

    #[test]
    fn not_missing_field_matches() {
        // MongoDB behavior: $not on missing field returns true
        let q = parse_query(&json!({"age": {"$not": {"$gt": 30}}})).unwrap();
        let no_age = Document::new(1, json!({"name": "Bob"})).unwrap();
        assert!(matches_doc(&q, &no_age));
    }

    #[test]
    fn not_with_regex() {
        let q = parse_query(&json!({"name": {"$not": {"$regex": "^admin"}}})).unwrap();
        let admin = Document::new(1, json!({"name": "admin_user"})).unwrap();
        let normal = Document::new(2, json!({"name": "alice"})).unwrap();
        assert!(!matches_doc(&q, &admin));
        assert!(matches_doc(&q, &normal));
    }

    #[test]
    fn not_with_in() {
        let q = parse_query(&json!({"status": {"$not": {"$in": ["deleted", "banned"]}}})).unwrap();
        let active = Document::new(1, json!({"status": "active"})).unwrap();
        let deleted = Document::new(2, json!({"status": "deleted"})).unwrap();
        assert!(matches_doc(&q, &active));
        assert!(!matches_doc(&q, &deleted));
    }

    // Real-world: content moderation — find posts that are NOT flagged as spam
    #[test]
    fn not_scenario_content_moderation() {
        let q = parse_query(&json!({
            "spam_score": {"$not": {"$gte": 0.8}},
            "published": true
        }))
        .unwrap();
        let clean = Document::new(
            1,
            json!({"title": "Good post", "spam_score": 0.1, "published": true}),
        )
        .unwrap();
        let spam = Document::new(
            2,
            json!({"title": "Buy now!!!", "spam_score": 0.95, "published": true}),
        )
        .unwrap();
        let draft = Document::new(
            3,
            json!({"title": "Draft", "spam_score": 0.1, "published": false}),
        )
        .unwrap();
        assert!(matches_doc(&q, &clean));
        assert!(!matches_doc(&q, &spam));
        assert!(!matches_doc(&q, &draft));
    }

    // ---------------------------------------------------------------
    // $nor tests
    // ---------------------------------------------------------------

    #[test]
    fn nor_basic() {
        let q = parse_query(&json!({
            "$nor": [{"status": "deleted"}, {"status": "banned"}]
        }))
        .unwrap();
        let active = Document::new(1, json!({"status": "active"})).unwrap();
        let deleted = Document::new(2, json!({"status": "deleted"})).unwrap();
        let banned = Document::new(3, json!({"status": "banned"})).unwrap();
        assert!(matches_doc(&q, &active));
        assert!(!matches_doc(&q, &deleted));
        assert!(!matches_doc(&q, &banned));
    }

    #[test]
    fn nor_multiple_fields() {
        let q = parse_query(&json!({
            "$nor": [{"age": {"$lt": 18}}, {"banned": true}]
        }))
        .unwrap();
        let adult = Document::new(1, json!({"age": 25, "banned": false})).unwrap();
        let minor = Document::new(2, json!({"age": 15, "banned": false})).unwrap();
        let banned_adult = Document::new(3, json!({"age": 30, "banned": true})).unwrap();
        assert!(matches_doc(&q, &adult));
        assert!(!matches_doc(&q, &minor));
        assert!(!matches_doc(&q, &banned_adult));
    }

    // Real-world: e-commerce — exclude products that are out of stock OR discontinued
    #[test]
    fn nor_scenario_ecommerce_available_products() {
        let q = parse_query(&json!({
            "$nor": [
                {"stock": 0},
                {"discontinued": true},
                {"price": {"$lte": 0}}
            ]
        }))
        .unwrap();
        let available = Document::new(
            1,
            json!({"name": "Widget", "stock": 50, "discontinued": false, "price": 29.99}),
        )
        .unwrap();
        let out_of_stock = Document::new(
            2,
            json!({"name": "Gadget", "stock": 0, "discontinued": false, "price": 19.99}),
        )
        .unwrap();
        let discontinued = Document::new(
            3,
            json!({"name": "Old Phone", "stock": 5, "discontinued": true, "price": 99.99}),
        )
        .unwrap();
        let free = Document::new(
            4,
            json!({"name": "Sample", "stock": 100, "discontinued": false, "price": 0}),
        )
        .unwrap();
        assert!(matches_doc(&q, &available));
        assert!(!matches_doc(&q, &out_of_stock));
        assert!(!matches_doc(&q, &discontinued));
        assert!(!matches_doc(&q, &free));
    }

    // ---------------------------------------------------------------
    // $all tests
    // ---------------------------------------------------------------

    #[test]
    fn all_basic() {
        let q = parse_query(&json!({"tags": {"$all": ["red", "blue"]}})).unwrap();
        let both = Document::new(1, json!({"tags": ["red", "blue", "green"]})).unwrap();
        let one_only = Document::new(2, json!({"tags": ["red", "green"]})).unwrap();
        let neither = Document::new(3, json!({"tags": ["green", "yellow"]})).unwrap();
        assert!(matches_doc(&q, &both));
        assert!(!matches_doc(&q, &one_only));
        assert!(!matches_doc(&q, &neither));
    }

    #[test]
    fn all_empty_matches_nothing() {
        // `$all: []` must match no documents (MongoDB semantics), not every
        // array-valued document.
        let q = parse_query(&json!({"tags": {"$all": []}})).unwrap();
        let arr = Document::new(1, json!({"tags": ["red", "blue"]})).unwrap();
        let empty = Document::new(2, json!({"tags": []})).unwrap();
        assert!(!matches_doc(&q, &arr));
        assert!(!matches_doc(&q, &empty));
    }

    #[test]
    fn all_single_element() {
        let q = parse_query(&json!({"tags": {"$all": ["urgent"]}})).unwrap();
        let has = Document::new(1, json!({"tags": ["urgent", "bug"]})).unwrap();
        let not = Document::new(2, json!({"tags": ["feature"]})).unwrap();
        assert!(matches_doc(&q, &has));
        assert!(!matches_doc(&q, &not));
    }

    #[test]
    fn all_not_array_fails() {
        let q = parse_query(&json!({"tags": {"$all": ["a"]}})).unwrap();
        let string_val = Document::new(1, json!({"tags": "a"})).unwrap();
        let missing = Document::new(2, json!({"other": 1})).unwrap();
        assert!(!matches_doc(&q, &string_val));
        assert!(!matches_doc(&q, &missing));
    }

    #[test]
    fn all_numeric() {
        let q = parse_query(&json!({"scores": {"$all": [90, 100]}})).unwrap();
        let has_both = Document::new(1, json!({"scores": [80, 90, 100]})).unwrap();
        let has_one = Document::new(2, json!({"scores": [80, 90]})).unwrap();
        assert!(matches_doc(&q, &has_both));
        assert!(!matches_doc(&q, &has_one));
    }

    // Real-world: job board — candidates must have ALL required skills
    #[test]
    fn all_scenario_job_skills_matching() {
        let q = parse_query(&json!({
            "skills": {"$all": ["rust", "postgresql", "docker"]},
            "years_exp": {"$gte": 3}
        }))
        .unwrap();
        let perfect = Document::new(1, json!({"name": "Alice", "skills": ["rust", "postgresql", "docker", "k8s"], "years_exp": 5})).unwrap();
        let missing_skill = Document::new(
            2,
            json!({"name": "Bob", "skills": ["rust", "postgresql"], "years_exp": 4}),
        )
        .unwrap();
        let junior = Document::new(
            3,
            json!({"name": "Charlie", "skills": ["rust", "postgresql", "docker"], "years_exp": 1}),
        )
        .unwrap();
        assert!(matches_doc(&q, &perfect));
        assert!(!matches_doc(&q, &missing_skill));
        assert!(!matches_doc(&q, &junior));
    }

    // Real-world: recipe finder — recipe must contain ALL specified ingredients
    #[test]
    fn all_scenario_recipe_ingredients() {
        let q = parse_query(&json!({
            "ingredients": {"$all": ["flour", "eggs", "butter"]}
        }))
        .unwrap();
        let cake = Document::new(
            1,
            json!({"name": "Cake", "ingredients": ["flour", "eggs", "butter", "sugar", "vanilla"]}),
        )
        .unwrap();
        let omelette = Document::new(
            2,
            json!({"name": "Omelette", "ingredients": ["eggs", "butter", "cheese"]}),
        )
        .unwrap();
        assert!(matches_doc(&q, &cake));
        assert!(!matches_doc(&q, &omelette));
    }

    // ---------------------------------------------------------------
    // $size tests
    // ---------------------------------------------------------------

    #[test]
    fn size_basic() {
        let q = parse_query(&json!({"tags": {"$size": 3}})).unwrap();
        let three = Document::new(1, json!({"tags": ["a", "b", "c"]})).unwrap();
        let two = Document::new(2, json!({"tags": ["a", "b"]})).unwrap();
        let four = Document::new(3, json!({"tags": ["a", "b", "c", "d"]})).unwrap();
        assert!(matches_doc(&q, &three));
        assert!(!matches_doc(&q, &two));
        assert!(!matches_doc(&q, &four));
    }

    #[test]
    fn size_zero() {
        let q = parse_query(&json!({"tags": {"$size": 0}})).unwrap();
        let empty = Document::new(1, json!({"tags": []})).unwrap();
        let one = Document::new(2, json!({"tags": ["a"]})).unwrap();
        assert!(matches_doc(&q, &empty));
        assert!(!matches_doc(&q, &one));
    }

    #[test]
    fn size_not_array_fails() {
        let q = parse_query(&json!({"name": {"$size": 5}})).unwrap();
        let str_val = Document::new(1, json!({"name": "hello"})).unwrap();
        let num_val = Document::new(2, json!({"name": 12345})).unwrap();
        assert!(!matches_doc(&q, &str_val));
        assert!(!matches_doc(&q, &num_val));
    }

    // Real-world: social media — find users with exactly 0 followers (new accounts)
    #[test]
    fn size_scenario_new_accounts() {
        let q = parse_query(&json!({
            "followers": {"$size": 0},
            "verified": false
        }))
        .unwrap();
        let new_user = Document::new(
            1,
            json!({"name": "newbie", "followers": [], "verified": false}),
        )
        .unwrap();
        let has_followers = Document::new(
            2,
            json!({"name": "popular", "followers": ["user1"], "verified": false}),
        )
        .unwrap();
        assert!(matches_doc(&q, &new_user));
        assert!(!matches_doc(&q, &has_followers));
    }

    // Real-world: quiz — find questions with exactly 4 answer options
    #[test]
    fn size_scenario_quiz_validation() {
        let q = parse_query(&json!({"options": {"$size": 4}})).unwrap();
        let valid = Document::new(
            1,
            json!({"question": "What is 2+2?", "options": ["3", "4", "5", "6"]}),
        )
        .unwrap();
        let invalid = Document::new(
            2,
            json!({"question": "True or false?", "options": ["true", "false"]}),
        )
        .unwrap();
        assert!(matches_doc(&q, &valid));
        assert!(!matches_doc(&q, &invalid));
    }

    // ---------------------------------------------------------------
    // $type tests
    // ---------------------------------------------------------------

    #[test]
    fn type_string() {
        let q = parse_query(&json!({"name": {"$type": "string"}})).unwrap();
        let str_val = Document::new(1, json!({"name": "Alice"})).unwrap();
        let num_val = Document::new(2, json!({"name": 42})).unwrap();
        let null_val = Document::new(3, json!({"name": null})).unwrap();
        assert!(matches_doc(&q, &str_val));
        assert!(!matches_doc(&q, &num_val));
        assert!(!matches_doc(&q, &null_val));
    }

    #[test]
    fn type_number() {
        let q = parse_query(&json!({"age": {"$type": "number"}})).unwrap();
        let int_val = Document::new(1, json!({"age": 30})).unwrap();
        let float_val = Document::new(2, json!({"age": 30.5})).unwrap();
        let str_val = Document::new(3, json!({"age": "thirty"})).unwrap();
        assert!(matches_doc(&q, &int_val));
        assert!(matches_doc(&q, &float_val));
        assert!(!matches_doc(&q, &str_val));
    }

    #[test]
    fn type_bool() {
        let q = parse_query(&json!({"active": {"$type": "bool"}})).unwrap();
        let bool_val = Document::new(1, json!({"active": true})).unwrap();
        let str_val = Document::new(2, json!({"active": "yes"})).unwrap();
        let int_val = Document::new(3, json!({"active": 1})).unwrap();
        assert!(matches_doc(&q, &bool_val));
        assert!(!matches_doc(&q, &str_val));
        assert!(!matches_doc(&q, &int_val));
    }

    #[test]
    fn type_array() {
        let q = parse_query(&json!({"data": {"$type": "array"}})).unwrap();
        let arr = Document::new(1, json!({"data": [1, 2, 3]})).unwrap();
        let obj = Document::new(2, json!({"data": {"a": 1}})).unwrap();
        assert!(matches_doc(&q, &arr));
        assert!(!matches_doc(&q, &obj));
    }

    #[test]
    fn type_object() {
        let q = parse_query(&json!({"meta": {"$type": "object"}})).unwrap();
        let obj = Document::new(1, json!({"meta": {"key": "val"}})).unwrap();
        let arr = Document::new(2, json!({"meta": [1, 2]})).unwrap();
        let str_val = Document::new(3, json!({"meta": "string"})).unwrap();
        assert!(matches_doc(&q, &obj));
        assert!(!matches_doc(&q, &arr));
        assert!(!matches_doc(&q, &str_val));
    }

    #[test]
    fn type_null() {
        let q = parse_query(&json!({"deleted_at": {"$type": "null"}})).unwrap();
        let null_val = Document::new(1, json!({"deleted_at": null})).unwrap();
        let str_val = Document::new(2, json!({"deleted_at": "2024-01-01"})).unwrap();
        let missing = Document::new(3, json!({"other": 1})).unwrap();
        assert!(matches_doc(&q, &null_val));
        assert!(!matches_doc(&q, &str_val));
        assert!(!matches_doc(&q, &missing)); // missing ≠ null
    }

    #[test]
    fn type_int() {
        let q = parse_query(&json!({"count": {"$type": "int"}})).unwrap();
        let int_val = Document::new(1, json!({"count": 42})).unwrap();
        let float_val = Document::new(2, json!({"count": 42.5})).unwrap();
        assert!(matches_doc(&q, &int_val));
        assert!(!matches_doc(&q, &float_val));
    }

    #[test]
    fn type_double_excludes_integers() {
        // "double" is a floating-point type only — an integer must NOT match.
        let q = parse_query(&json!({"v": {"$type": "double"}})).unwrap();
        let int_val = Document::new(1, json!({"v": 42})).unwrap();
        let float_val = Document::new(2, json!({"v": 42.5})).unwrap();
        assert!(!matches_doc(&q, &int_val));
        assert!(matches_doc(&q, &float_val));
    }

    // Real-world: data quality — find documents where "email" is not a string
    #[test]
    fn type_scenario_data_validation() {
        let q = parse_query(&json!({
            "email": {"$not": {"$type": "string"}}
        }))
        .unwrap();
        let valid = Document::new(1, json!({"email": "alice@example.com"})).unwrap();
        let corrupt_num = Document::new(2, json!({"email": 12345})).unwrap();
        let corrupt_null = Document::new(3, json!({"email": null})).unwrap();
        let missing = Document::new(4, json!({"name": "no email"})).unwrap();
        assert!(!matches_doc(&q, &valid));
        assert!(matches_doc(&q, &corrupt_num));
        assert!(matches_doc(&q, &corrupt_null));
        assert!(matches_doc(&q, &missing)); // $not on missing → true
    }

    // ---------------------------------------------------------------
    // $mod tests
    // ---------------------------------------------------------------

    #[test]
    fn mod_basic() {
        let q = parse_query(&json!({"qty": {"$mod": [4, 0]}})).unwrap();
        let div4 = Document::new(1, json!({"qty": 12})).unwrap();
        let not_div4 = Document::new(2, json!({"qty": 13})).unwrap();
        let zero = Document::new(3, json!({"qty": 0})).unwrap();
        assert!(matches_doc(&q, &div4));
        assert!(!matches_doc(&q, &not_div4));
        assert!(matches_doc(&q, &zero)); // 0 % 4 == 0
    }

    #[test]
    fn mod_with_remainder() {
        let q = parse_query(&json!({"num": {"$mod": [3, 1]}})).unwrap();
        let r1 = Document::new(1, json!({"num": 7})).unwrap(); // 7 % 3 == 1
        let r0 = Document::new(2, json!({"num": 9})).unwrap(); // 9 % 3 == 0
        let r2 = Document::new(3, json!({"num": 8})).unwrap(); // 8 % 3 == 2
        assert!(matches_doc(&q, &r1));
        assert!(!matches_doc(&q, &r0));
        assert!(!matches_doc(&q, &r2));
    }

    #[test]
    fn mod_non_numeric_fails() {
        let q = parse_query(&json!({"qty": {"$mod": [2, 0]}})).unwrap();
        let str_val = Document::new(1, json!({"qty": "twelve"})).unwrap();
        let missing = Document::new(2, json!({"other": 1})).unwrap();
        assert!(!matches_doc(&q, &str_val));
        assert!(!matches_doc(&q, &missing));
    }

    #[test]
    fn mod_zero_divisor_errors() {
        let result = parse_query(&json!({"qty": {"$mod": [0, 0]}}));
        assert!(result.is_err());
    }

    // Real-world: sharding — select documents that belong to shard 2 of 5
    #[test]
    fn mod_scenario_sharding() {
        let q = parse_query(&json!({"user_id": {"$mod": [5, 2]}})).unwrap();
        let shard2a = Document::new(1, json!({"user_id": 7})).unwrap(); // 7 % 5 == 2
        let shard2b = Document::new(2, json!({"user_id": 12})).unwrap(); // 12 % 5 == 2
        let shard0 = Document::new(3, json!({"user_id": 10})).unwrap(); // 10 % 5 == 0
        let shard1 = Document::new(4, json!({"user_id": 11})).unwrap(); // 11 % 5 == 1
        assert!(matches_doc(&q, &shard2a));
        assert!(matches_doc(&q, &shard2b));
        assert!(!matches_doc(&q, &shard0));
        assert!(!matches_doc(&q, &shard1));
    }

    // Real-world: batch processing — find even-numbered order IDs
    #[test]
    fn mod_scenario_even_orders() {
        let q = parse_query(&json!({"order_id": {"$mod": [2, 0]}})).unwrap();
        let even = Document::new(1, json!({"order_id": 1024})).unwrap();
        let odd = Document::new(2, json!({"order_id": 1025})).unwrap();
        assert!(matches_doc(&q, &even));
        assert!(!matches_doc(&q, &odd));
    }

    // ---------------------------------------------------------------
    // $expr tests
    // ---------------------------------------------------------------

    #[test]
    fn expr_compare_two_fields_gt() {
        let q = parse_query(&json!({"$expr": {"$gt": ["$sold", "$stock"]}})).unwrap();
        let oversold = Document::new(1, json!({"sold": 100, "stock": 50})).unwrap();
        let normal = Document::new(2, json!({"sold": 30, "stock": 50})).unwrap();
        let equal = Document::new(3, json!({"sold": 50, "stock": 50})).unwrap();
        assert!(matches_doc(&q, &oversold));
        assert!(!matches_doc(&q, &normal));
        assert!(!matches_doc(&q, &equal));
    }

    #[test]
    fn expr_compare_field_to_literal() {
        let q = parse_query(&json!({"$expr": {"$gte": ["$score", 90]}})).unwrap();
        let high = Document::new(1, json!({"score": 95})).unwrap();
        let low = Document::new(2, json!({"score": 80})).unwrap();
        assert!(matches_doc(&q, &high));
        assert!(!matches_doc(&q, &low));
    }

    #[test]
    fn expr_eq_two_fields() {
        let q =
            parse_query(&json!({"$expr": {"$eq": ["$password", "$confirm_password"]}})).unwrap();
        let match_doc = Document::new(
            1,
            json!({"password": "abc123", "confirm_password": "abc123"}),
        )
        .unwrap();
        let mismatch = Document::new(
            2,
            json!({"password": "abc123", "confirm_password": "xyz789"}),
        )
        .unwrap();
        assert!(matches_doc(&q, &match_doc));
        assert!(!matches_doc(&q, &mismatch));
    }

    #[test]
    fn expr_ne() {
        let q = parse_query(&json!({"$expr": {"$ne": ["$actual", "$expected"]}})).unwrap();
        let different = Document::new(1, json!({"actual": 42, "expected": 50})).unwrap();
        let same = Document::new(2, json!({"actual": 42, "expected": 42})).unwrap();
        assert!(matches_doc(&q, &different));
        assert!(!matches_doc(&q, &same));
    }

    #[test]
    fn expr_missing_field_no_match() {
        let q = parse_query(&json!({"$expr": {"$gt": ["$a", "$b"]}})).unwrap();
        let missing_a = Document::new(1, json!({"b": 10})).unwrap();
        let missing_b = Document::new(2, json!({"a": 10})).unwrap();
        assert!(!matches_doc(&q, &missing_a));
        assert!(!matches_doc(&q, &missing_b));
    }

    #[test]
    fn expr_string_comparison() {
        let q = parse_query(&json!({"$expr": {"$lt": ["$first_name", "$last_name"]}})).unwrap();
        let ab = Document::new(1, json!({"first_name": "Alice", "last_name": "Zeta"})).unwrap();
        let za = Document::new(2, json!({"first_name": "Zara", "last_name": "Adams"})).unwrap();
        assert!(matches_doc(&q, &ab));
        assert!(!matches_doc(&q, &za));
    }

    // Real-world: inventory — find products where sold quantity exceeds reorder threshold
    #[test]
    fn expr_scenario_inventory_reorder() {
        let q = parse_query(&json!({
            "$expr": {"$lte": ["$stock", "$reorder_level"]},
        }))
        .unwrap();
        let needs_reorder = Document::new(
            1,
            json!({"product": "Widget", "stock": 5, "reorder_level": 10}),
        )
        .unwrap();
        let ok = Document::new(
            2,
            json!({"product": "Gadget", "stock": 50, "reorder_level": 10}),
        )
        .unwrap();
        let exact = Document::new(
            3,
            json!({"product": "Bolt", "stock": 10, "reorder_level": 10}),
        )
        .unwrap();
        assert!(matches_doc(&q, &needs_reorder));
        assert!(!matches_doc(&q, &ok));
        assert!(matches_doc(&q, &exact)); // stock == reorder_level → needs reorder
    }

    // Real-world: pricing — find items sold below cost
    #[test]
    fn expr_scenario_below_cost() {
        let q = parse_query(&json!({
            "$expr": {"$lt": ["$sale_price", "$cost"]}
        }))
        .unwrap();
        let loss = Document::new(
            1,
            json!({"item": "Clearance TV", "sale_price": 199, "cost": 250}),
        )
        .unwrap();
        let profit = Document::new(
            2,
            json!({"item": "Phone Case", "sale_price": 25, "cost": 5}),
        )
        .unwrap();
        assert!(matches_doc(&q, &loss));
        assert!(!matches_doc(&q, &profit));
    }

    // Real-world: SLA monitoring — response time exceeds allowed limit
    #[test]
    fn expr_scenario_sla_breach() {
        let q = parse_query(&json!({
            "$expr": {"$gt": ["$response_ms", "$sla_limit_ms"]}
        }))
        .unwrap();
        let breach = Document::new(
            1,
            json!({"endpoint": "/api/data", "response_ms": 550, "sla_limit_ms": 500}),
        )
        .unwrap();
        let ok = Document::new(
            2,
            json!({"endpoint": "/api/health", "response_ms": 50, "sla_limit_ms": 500}),
        )
        .unwrap();
        assert!(matches_doc(&q, &breach));
        assert!(!matches_doc(&q, &ok));
    }

    // ---------------------------------------------------------------
    // $expr with matches_value (pipeline $match)
    // ---------------------------------------------------------------

    #[test]
    fn expr_matches_value() {
        let q = parse_query(&json!({"$expr": {"$gt": ["$revenue", "$expenses"]}})).unwrap();
        let profit = json!({"revenue": 1000, "expenses": 800});
        let loss = json!({"revenue": 500, "expenses": 800});
        assert!(matches_value(&q, &profit));
        assert!(!matches_value(&q, &loss));
    }

    // ---------------------------------------------------------------
    // Combined operator real-world scenarios
    // ---------------------------------------------------------------

    // Healthcare: find patients needing follow-up
    #[test]
    fn combined_scenario_healthcare_followup() {
        let q = parse_query(&json!({
            "conditions": {"$all": ["diabetes", "hypertension"]},
            "last_visit_days_ago": {"$gt": 90},
            "$nor": [{"status": "deceased"}, {"status": "transferred"}]
        }))
        .unwrap();
        let needs_followup = Document::new(
            1,
            json!({
                "name": "Patient A", "conditions": ["diabetes", "hypertension", "obesity"],
                "last_visit_days_ago": 120, "status": "active"
            }),
        )
        .unwrap();
        let recent_visit = Document::new(
            2,
            json!({
                "name": "Patient B", "conditions": ["diabetes", "hypertension"],
                "last_visit_days_ago": 30, "status": "active"
            }),
        )
        .unwrap();
        let transferred = Document::new(
            3,
            json!({
                "name": "Patient C", "conditions": ["diabetes", "hypertension"],
                "last_visit_days_ago": 200, "status": "transferred"
            }),
        )
        .unwrap();
        let missing_condition = Document::new(
            4,
            json!({
                "name": "Patient D", "conditions": ["diabetes"],
                "last_visit_days_ago": 150, "status": "active"
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &needs_followup));
        assert!(!matches_doc(&q, &recent_visit));
        assert!(!matches_doc(&q, &transferred));
        assert!(!matches_doc(&q, &missing_condition));
    }

    // IoT: find sensors reporting anomalous readings
    #[test]
    fn combined_scenario_iot_sensor_anomaly() {
        let q = parse_query(&json!({
            "$and": [
                {"readings": {"$elemMatch": {"$gt": 100}}},
                {"readings": {"$size": 5}}
            ],
            "sensor_type": {"$type": "string"},
            "sensor_id": {"$mod": [10, 0]}
        }))
        .unwrap();
        let anomaly = Document::new(
            1,
            json!({
                "sensor_id": 100, "sensor_type": "temperature",
                "readings": [22.0, 23.0, 105.0, 21.0, 22.5]
            }),
        )
        .unwrap();
        let normal = Document::new(
            2,
            json!({
                "sensor_id": 100, "sensor_type": "temperature",
                "readings": [22.0, 23.0, 24.0, 21.0, 22.5]
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &anomaly));
        assert!(!matches_doc(&q, &normal));
    }

    // E-commerce: find orders eligible for bulk discount
    #[test]
    fn combined_scenario_bulk_discount() {
        let q = parse_query(&json!({
            "items": {"$elemMatch": {"qty": {"$gte": 100}}},
            "$nor": [{"status": "cancelled"}, {"status": "returned"}],
            "$expr": {"$gt": ["$total", "$discount_threshold"]}
        }))
        .unwrap();
        let eligible = Document::new(
            1,
            json!({
                "order_id": 1001, "status": "confirmed",
                "items": [{"name": "Bolts", "qty": 200}, {"name": "Nuts", "qty": 50}],
                "total": 5000, "discount_threshold": 1000
            }),
        )
        .unwrap();
        let too_small = Document::new(
            2,
            json!({
                "order_id": 1002, "status": "confirmed",
                "items": [{"name": "Bolts", "qty": 10}],
                "total": 500, "discount_threshold": 1000
            }),
        )
        .unwrap();
        let cancelled = Document::new(
            3,
            json!({
                "order_id": 1003, "status": "cancelled",
                "items": [{"name": "Bolts", "qty": 200}],
                "total": 5000, "discount_threshold": 1000
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &eligible));
        assert!(!matches_doc(&q, &too_small));
        assert!(!matches_doc(&q, &cancelled));
    }

    // ---------------------------------------------------------------
    // Parse error tests for new operators
    // ---------------------------------------------------------------

    #[test]
    fn not_must_be_object() {
        assert!(parse_query(&json!({"x": {"$not": "string"}})).is_err());
    }

    #[test]
    fn all_must_be_array() {
        assert!(parse_query(&json!({"x": {"$all": "not_array"}})).is_err());
    }

    #[test]
    fn size_must_be_integer() {
        assert!(parse_query(&json!({"x": {"$size": "three"}})).is_err());
    }

    #[test]
    fn type_must_be_string() {
        assert!(parse_query(&json!({"x": {"$type": 42}})).is_err());
    }

    #[test]
    fn mod_must_be_two_element_array() {
        assert!(parse_query(&json!({"x": {"$mod": [4]}})).is_err());
        assert!(parse_query(&json!({"x": {"$mod": [4, 0, 1]}})).is_err());
        assert!(parse_query(&json!({"x": {"$mod": "bad"}})).is_err());
    }

    #[test]
    fn expr_must_have_one_operator() {
        assert!(parse_query(&json!({"$expr": "bad"})).is_err());
        assert!(parse_query(&json!({"$expr": {}})).is_err());
        assert!(parse_query(&json!({"$expr": {"$bogus": ["$a", "$b"]}})).is_err());
    }

    #[test]
    fn nor_must_be_array() {
        assert!(parse_query(&json!({"$nor": "bad"})).is_err());
    }

    // ===============================================================
    // Real-world scenario tests
    // ===============================================================

    // --- Fintech / Banking ---

    // Suspicious transaction detection: large amount, foreign currency, new account
    #[test]
    fn scenario_fraud_detection() {
        let q = parse_query(&json!({
            "amount": {"$gt": 10000},
            "currency": {"$nin": ["USD", "EUR", "GBP"]},
            "account_age_days": {"$lt": 30},
            "$nor": [{"verified": true}, {"whitelisted": true}]
        }))
        .unwrap();
        let suspicious = Document::new(
            1,
            json!({
                "tx_id": "TX-9001", "amount": 25000, "currency": "BTC",
                "account_age_days": 3, "verified": false, "whitelisted": false
            }),
        )
        .unwrap();
        let normal_currency = Document::new(
            2,
            json!({
                "tx_id": "TX-9002", "amount": 25000, "currency": "USD",
                "account_age_days": 3, "verified": false, "whitelisted": false
            }),
        )
        .unwrap();
        let verified = Document::new(
            3,
            json!({
                "tx_id": "TX-9003", "amount": 25000, "currency": "BTC",
                "account_age_days": 3, "verified": true, "whitelisted": false
            }),
        )
        .unwrap();
        let old_account = Document::new(
            4,
            json!({
                "tx_id": "TX-9004", "amount": 25000, "currency": "BTC",
                "account_age_days": 365, "verified": false, "whitelisted": false
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &suspicious));
        assert!(!matches_doc(&q, &normal_currency));
        assert!(!matches_doc(&q, &verified));
        assert!(!matches_doc(&q, &old_account));
    }

    // Loan eligibility: income exceeds expenses by at least 30%
    #[test]
    fn scenario_loan_eligibility() {
        let q = parse_query(&json!({
            "$expr": {"$gte": ["$monthly_income", "$monthly_expenses"]},
            "credit_score": {"$gte": 650},
            "existing_loans": {"$not": {"$gt": 3}},
            "employment_status": {"$in": ["employed", "self_employed"]}
        }))
        .unwrap();
        let eligible = Document::new(
            1,
            json!({
                "name": "Alice", "monthly_income": 8000, "monthly_expenses": 5000,
                "credit_score": 720, "existing_loans": 1, "employment_status": "employed"
            }),
        )
        .unwrap();
        let low_credit = Document::new(
            2,
            json!({
                "name": "Bob", "monthly_income": 8000, "monthly_expenses": 5000,
                "credit_score": 580, "existing_loans": 1, "employment_status": "employed"
            }),
        )
        .unwrap();
        let too_many_loans = Document::new(
            3,
            json!({
                "name": "Carol", "monthly_income": 8000, "monthly_expenses": 5000,
                "credit_score": 720, "existing_loans": 5, "employment_status": "employed"
            }),
        )
        .unwrap();
        let unemployed = Document::new(
            4,
            json!({
                "name": "Dave", "monthly_income": 8000, "monthly_expenses": 5000,
                "credit_score": 720, "existing_loans": 1, "employment_status": "unemployed"
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &eligible));
        assert!(!matches_doc(&q, &low_credit));
        assert!(!matches_doc(&q, &too_many_loans));
        assert!(!matches_doc(&q, &unemployed));
    }

    // --- DevOps / Monitoring ---

    // Alert rule: error rate spike in the last window
    #[test]
    fn scenario_error_rate_alert() {
        let q = parse_query(&json!({
            "level": {"$in": ["error", "critical", "fatal"]},
            "service": {"$regex": "^payment-"},
            "tags": {"$all": ["production", "us-east"]},
            "retry_count": {"$mod": [3, 0]}
        }))
        .unwrap();
        let alert = Document::new(
            1,
            json!({
                "level": "error", "service": "payment-gateway",
                "tags": ["production", "us-east", "high-priority"],
                "retry_count": 6, "message": "timeout"
            }),
        )
        .unwrap();
        let wrong_service = Document::new(
            2,
            json!({
                "level": "error", "service": "user-auth",
                "tags": ["production", "us-east"],
                "retry_count": 6, "message": "timeout"
            }),
        )
        .unwrap();
        let wrong_region = Document::new(
            3,
            json!({
                "level": "error", "service": "payment-api",
                "tags": ["production", "eu-west"],
                "retry_count": 6, "message": "timeout"
            }),
        )
        .unwrap();
        let not_retry_cycle = Document::new(
            4,
            json!({
                "level": "error", "service": "payment-api",
                "tags": ["production", "us-east"],
                "retry_count": 5, "message": "timeout"
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &alert));
        assert!(!matches_doc(&q, &wrong_service));
        assert!(!matches_doc(&q, &wrong_region));
        assert!(!matches_doc(&q, &not_retry_cycle));
    }

    // CI/CD: find flaky tests — passed sometimes, failed on retries
    #[test]
    fn scenario_flaky_test_detection() {
        let q = parse_query(&json!({
            "results": {"$elemMatch": {"$eq": "pass"}},
            "$and": [
                {"results": {"$elemMatch": {"$eq": "fail"}}},
                {"results": {"$size": 3}}
            ],
            "test_suite": {"$type": "string"}
        }))
        .unwrap();
        let flaky = Document::new(
            1,
            json!({
                "test_name": "test_payment_flow", "test_suite": "integration",
                "results": ["pass", "fail", "pass"]
            }),
        )
        .unwrap();
        let stable_pass = Document::new(
            2,
            json!({
                "test_name": "test_health", "test_suite": "unit",
                "results": ["pass", "pass", "pass"]
            }),
        )
        .unwrap();
        let stable_fail = Document::new(
            3,
            json!({
                "test_name": "test_broken", "test_suite": "integration",
                "results": ["fail", "fail", "fail"]
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &flaky));
        assert!(!matches_doc(&q, &stable_pass));
        assert!(!matches_doc(&q, &stable_fail));
    }

    // --- HR / People Analytics ---

    // Find employees eligible for promotion
    #[test]
    fn scenario_promotion_eligibility() {
        let q = parse_query(&json!({
            "performance_scores": {"$all": [4, 5]},
            "years_in_role": {"$gte": 2},
            "$nor": [
                {"on_pip": true},
                {"department": "contractor"}
            ],
            "certifications": {"$elemMatch": {"status": "active"}},
            "$expr": {"$gte": ["$completed_projects", "$target_projects"]}
        }))
        .unwrap();
        let eligible = Document::new(
            1,
            json!({
                "name": "Alice", "performance_scores": [4, 5, 4, 5],
                "years_in_role": 3, "on_pip": false, "department": "engineering",
                "certifications": [{"name": "AWS", "status": "active"}],
                "completed_projects": 12, "target_projects": 10
            }),
        )
        .unwrap();
        let on_pip = Document::new(
            2,
            json!({
                "name": "Bob", "performance_scores": [4, 5, 4, 5],
                "years_in_role": 3, "on_pip": true, "department": "engineering",
                "certifications": [{"name": "AWS", "status": "active"}],
                "completed_projects": 12, "target_projects": 10
            }),
        )
        .unwrap();
        let low_perf = Document::new(
            3,
            json!({
                "name": "Carol", "performance_scores": [3, 3, 4, 3],
                "years_in_role": 5, "on_pip": false, "department": "engineering",
                "certifications": [{"name": "AWS", "status": "active"}],
                "completed_projects": 15, "target_projects": 10
            }),
        )
        .unwrap();
        let under_target = Document::new(
            4,
            json!({
                "name": "Dave", "performance_scores": [4, 5, 5, 4],
                "years_in_role": 3, "on_pip": false, "department": "engineering",
                "certifications": [{"name": "AWS", "status": "active"}],
                "completed_projects": 7, "target_projects": 10
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &eligible));
        assert!(!matches_doc(&q, &on_pip));
        assert!(!matches_doc(&q, &low_perf));
        assert!(!matches_doc(&q, &under_target));
    }

    // --- Gaming ---

    // Matchmaking: find players for a balanced lobby
    #[test]
    fn scenario_game_matchmaking() {
        let q = parse_query(&json!({
            "elo": {"$gte": 1400, "$lte": 1600},
            "preferred_modes": {"$elemMatch": {"$eq": "ranked"}},
            "recent_results": {"$not": {"$size": 0}},
            "$nor": [
                {"banned": true},
                {"afk_count": {"$gte": 3}}
            ],
            "ping_ms": {"$lt": 100}
        }))
        .unwrap();
        let good_player = Document::new(
            1,
            json!({
                "username": "pro_gamer", "elo": 1520,
                "preferred_modes": ["ranked", "casual"],
                "recent_results": ["win", "win", "loss"],
                "banned": false, "afk_count": 0, "ping_ms": 45
            }),
        )
        .unwrap();
        let too_high_elo = Document::new(
            2,
            json!({
                "username": "grandmaster", "elo": 2100,
                "preferred_modes": ["ranked"],
                "recent_results": ["win", "win"],
                "banned": false, "afk_count": 0, "ping_ms": 30
            }),
        )
        .unwrap();
        let afk_abuser = Document::new(
            3,
            json!({
                "username": "afk_andy", "elo": 1500,
                "preferred_modes": ["ranked"],
                "recent_results": ["loss"],
                "banned": false, "afk_count": 5, "ping_ms": 50
            }),
        )
        .unwrap();
        let high_ping = Document::new(
            4,
            json!({
                "username": "laggy", "elo": 1500,
                "preferred_modes": ["ranked"],
                "recent_results": ["win"],
                "banned": false, "afk_count": 0, "ping_ms": 250
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &good_player));
        assert!(!matches_doc(&q, &too_high_elo));
        assert!(!matches_doc(&q, &afk_abuser));
        assert!(!matches_doc(&q, &high_ping));
    }

    // --- Real Estate ---

    // Property search with complex criteria
    #[test]
    fn scenario_property_search() {
        let q = parse_query(&json!({
            "price": {"$gte": 200000, "$lte": 500000},
            "bedrooms": {"$gte": 3},
            "amenities": {"$all": ["parking", "garden"]},
            "features": {"$elemMatch": {"type": "renovation", "year": {"$gte": 2020}}},
            "$nor": [
                {"status": "sold"},
                {"status": "under_offer"}
            ],
            "photos": {"$not": {"$size": 0}}
        }))
        .unwrap();
        let dream_home = Document::new(
            1,
            json!({
                "address": "123 Oak Lane", "price": 350000, "bedrooms": 4,
                "amenities": ["parking", "garden", "pool"],
                "features": [
                    {"type": "renovation", "year": 2022, "detail": "kitchen"},
                    {"type": "original", "year": 1990}
                ],
                "status": "available", "photos": ["front.jpg", "kitchen.jpg"]
            }),
        )
        .unwrap();
        let too_expensive = Document::new(
            2,
            json!({
                "address": "456 Elm St", "price": 750000, "bedrooms": 5,
                "amenities": ["parking", "garden"],
                "features": [{"type": "renovation", "year": 2023}],
                "status": "available", "photos": ["front.jpg"]
            }),
        )
        .unwrap();
        let no_garden = Document::new(
            3,
            json!({
                "address": "789 Pine Ave", "price": 300000, "bedrooms": 3,
                "amenities": ["parking"],
                "features": [{"type": "renovation", "year": 2021}],
                "status": "available", "photos": ["front.jpg"]
            }),
        )
        .unwrap();
        let sold = Document::new(
            4,
            json!({
                "address": "321 Maple Dr", "price": 400000, "bedrooms": 4,
                "amenities": ["parking", "garden"],
                "features": [{"type": "renovation", "year": 2022}],
                "status": "sold", "photos": ["front.jpg"]
            }),
        )
        .unwrap();
        let old_renovation = Document::new(
            5,
            json!({
                "address": "654 Birch Rd", "price": 280000, "bedrooms": 3,
                "amenities": ["parking", "garden"],
                "features": [{"type": "renovation", "year": 2015}],
                "status": "available", "photos": ["front.jpg"]
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &dream_home));
        assert!(!matches_doc(&q, &too_expensive));
        assert!(!matches_doc(&q, &no_garden));
        assert!(!matches_doc(&q, &sold));
        assert!(!matches_doc(&q, &old_renovation));
    }

    // --- Education ---

    // Student at-risk detection
    #[test]
    fn scenario_student_at_risk() {
        let q = parse_query(&json!({
            "gpa": {"$lt": 2.0},
            "absences": {"$gt": 10},
            "failed_courses": {"$not": {"$size": 0}},
            "$nor": [
                {"status": "graduated"},
                {"status": "withdrawn"},
                {"on_leave": true}
            ],
            "advisor_notes": {"$type": "array"}
        }))
        .unwrap();
        let at_risk = Document::new(
            1,
            json!({
                "name": "Student A", "gpa": 1.5, "absences": 15,
                "failed_courses": ["MATH101", "PHYS201"],
                "status": "enrolled", "on_leave": false,
                "advisor_notes": ["Struggling with coursework"]
            }),
        )
        .unwrap();
        let doing_fine = Document::new(
            2,
            json!({
                "name": "Student B", "gpa": 3.5, "absences": 2,
                "failed_courses": [],
                "status": "enrolled", "on_leave": false,
                "advisor_notes": []
            }),
        )
        .unwrap();
        let withdrawn = Document::new(
            3,
            json!({
                "name": "Student C", "gpa": 1.2, "absences": 20,
                "failed_courses": ["ENG101"],
                "status": "withdrawn", "on_leave": false,
                "advisor_notes": ["Left program"]
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &at_risk));
        assert!(!matches_doc(&q, &doing_fine));
        assert!(!matches_doc(&q, &withdrawn));
    }

    // --- Supply Chain ---

    // Find shipments that are late and over budget
    #[test]
    fn scenario_late_shipments() {
        let q = parse_query(&json!({
            "$expr": {"$gt": ["$actual_days", "$estimated_days"]},
            "$and": [
                {"$expr": {"$gt": ["$actual_cost", "$budgeted_cost"]}}
            ],
            "priority": {"$in": ["high", "critical"]},
            "checkpoints": {"$elemMatch": {
                "status": "delayed",
                "reason": {"$regex": "customs|weather"}
            }}
        }))
        .unwrap();
        let problem = Document::new(
            1,
            json!({
                "shipment_id": "SH-001", "actual_days": 14, "estimated_days": 7,
                "actual_cost": 5200, "budgeted_cost": 3000, "priority": "high",
                "checkpoints": [
                    {"location": "port_a", "status": "cleared", "reason": ""},
                    {"location": "customs_b", "status": "delayed", "reason": "customs hold"}
                ]
            }),
        )
        .unwrap();
        let on_time = Document::new(
            2,
            json!({
                "shipment_id": "SH-002", "actual_days": 5, "estimated_days": 7,
                "actual_cost": 2800, "budgeted_cost": 3000, "priority": "high",
                "checkpoints": [
                    {"location": "port_a", "status": "cleared", "reason": ""}
                ]
            }),
        )
        .unwrap();
        let low_priority = Document::new(
            3,
            json!({
                "shipment_id": "SH-003", "actual_days": 14, "estimated_days": 7,
                "actual_cost": 5200, "budgeted_cost": 3000, "priority": "low",
                "checkpoints": [
                    {"location": "customs_b", "status": "delayed", "reason": "customs hold"}
                ]
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &problem));
        assert!(!matches_doc(&q, &on_time));
        assert!(!matches_doc(&q, &low_priority));
    }

    // --- Content Management ---

    // Find blog posts ready for review
    #[test]
    fn scenario_content_review_queue() {
        let q = parse_query(&json!({
            "status": "draft",
            "word_count": {"$gte": 500},
            "tags": {"$all": ["reviewed-by-ai", "seo-optimized"]},
            "categories": {"$not": {"$size": 0}},
            "meta.author_role": {"$in": ["editor", "senior_writer"]},
            "$nor": [
                {"flagged": true},
                {"duplicate_of": {"$exists": true}}
            ]
        }))
        .unwrap();
        let ready = Document::new(
            1,
            json!({
                "title": "Great Article", "status": "draft", "word_count": 1200,
                "tags": ["reviewed-by-ai", "seo-optimized", "featured"],
                "categories": ["tech", "tutorial"],
                "meta": {"author_role": "editor", "author": "alice"},
                "flagged": false
            }),
        )
        .unwrap();
        let too_short = Document::new(
            2,
            json!({
                "title": "Quick Note", "status": "draft", "word_count": 150,
                "tags": ["reviewed-by-ai", "seo-optimized"],
                "categories": ["blog"],
                "meta": {"author_role": "editor"},
                "flagged": false
            }),
        )
        .unwrap();
        let missing_tag = Document::new(
            3,
            json!({
                "title": "Needs SEO", "status": "draft", "word_count": 800,
                "tags": ["reviewed-by-ai"],
                "categories": ["tech"],
                "meta": {"author_role": "editor"},
                "flagged": false
            }),
        )
        .unwrap();
        let flagged = Document::new(
            4,
            json!({
                "title": "Plagiarized", "status": "draft", "word_count": 900,
                "tags": ["reviewed-by-ai", "seo-optimized"],
                "categories": ["tech"],
                "meta": {"author_role": "editor"},
                "flagged": true
            }),
        )
        .unwrap();
        let duplicate = Document::new(
            5,
            json!({
                "title": "Copy of Great Article", "status": "draft", "word_count": 1200,
                "tags": ["reviewed-by-ai", "seo-optimized"],
                "categories": ["tech"],
                "meta": {"author_role": "editor"},
                "flagged": false, "duplicate_of": "article-001"
            }),
        )
        .unwrap();
        let junior = Document::new(
            6,
            json!({
                "title": "My First Post", "status": "draft", "word_count": 600,
                "tags": ["reviewed-by-ai", "seo-optimized"],
                "categories": ["blog"],
                "meta": {"author_role": "intern"},
                "flagged": false
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &ready));
        assert!(!matches_doc(&q, &too_short));
        assert!(!matches_doc(&q, &missing_tag));
        assert!(!matches_doc(&q, &flagged));
        assert!(!matches_doc(&q, &duplicate));
        assert!(!matches_doc(&q, &junior));
    }

    // --- Telecom ---

    // Find mobile plans that match customer usage
    #[test]
    fn scenario_telecom_plan_match() {
        let q = parse_query(&json!({
            "data_gb": {"$gte": 50},
            "price": {"$lte": 60},
            "coverage_regions": {"$all": ["urban", "suburban"]},
            "features": {"$elemMatch": {"name": "5g", "available": true}},
            "contract_months": {"$mod": [12, 0]},
            "plan_type": {"$not": {"$in": ["prepaid", "business"]}}
        }))
        .unwrap();
        let good_plan = Document::new(1, json!({
            "name": "Ultra Plan", "data_gb": 100, "price": 55,
            "coverage_regions": ["urban", "suburban", "rural"],
            "features": [{"name": "5g", "available": true}, {"name": "hotspot", "available": true}],
            "contract_months": 24, "plan_type": "postpaid"
        })).unwrap();
        let too_expensive = Document::new(
            2,
            json!({
                "name": "Premium Plan", "data_gb": 200, "price": 120,
                "coverage_regions": ["urban", "suburban", "rural"],
                "features": [{"name": "5g", "available": true}],
                "contract_months": 12, "plan_type": "postpaid"
            }),
        )
        .unwrap();
        let no_5g = Document::new(
            3,
            json!({
                "name": "Basic Plan", "data_gb": 50, "price": 30,
                "coverage_regions": ["urban", "suburban"],
                "features": [{"name": "5g", "available": false}],
                "contract_months": 12, "plan_type": "postpaid"
            }),
        )
        .unwrap();
        let odd_contract = Document::new(
            4,
            json!({
                "name": "Flex Plan", "data_gb": 80, "price": 45,
                "coverage_regions": ["urban", "suburban"],
                "features": [{"name": "5g", "available": true}],
                "contract_months": 18, "plan_type": "postpaid"
            }),
        )
        .unwrap();
        let prepaid = Document::new(
            5,
            json!({
                "name": "Prepaid Special", "data_gb": 60, "price": 40,
                "coverage_regions": ["urban", "suburban"],
                "features": [{"name": "5g", "available": true}],
                "contract_months": 12, "plan_type": "prepaid"
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &good_plan));
        assert!(!matches_doc(&q, &too_expensive));
        assert!(!matches_doc(&q, &no_5g));
        assert!(!matches_doc(&q, &odd_contract));
        assert!(!matches_doc(&q, &prepaid));
    }

    // --- Food Delivery ---

    // Find restaurants matching dietary requirements
    #[test]
    fn scenario_restaurant_search() {
        let q = parse_query(&json!({
            "dietary_options": {"$all": ["vegan", "gluten_free"]},
            "rating": {"$gte": 4.0},
            "delivery_fee": {"$lte": 5.0},
            "menu_items": {"$not": {"$size": 0}},
            "$nor": [
                {"temporarily_closed": true},
                {"accepting_orders": false}
            ],
            "delivery_zones": {"$elemMatch": {"$eq": "downtown"}}
        }))
        .unwrap();
        let match_restaurant = Document::new(
            1,
            json!({
                "name": "Green Bowl", "dietary_options": ["vegan", "gluten_free", "organic"],
                "rating": 4.7, "delivery_fee": 3.50,
                "menu_items": ["salad", "smoothie", "wrap"],
                "temporarily_closed": false, "accepting_orders": true,
                "delivery_zones": ["downtown", "midtown", "uptown"]
            }),
        )
        .unwrap();
        let low_rating = Document::new(
            2,
            json!({
                "name": "Cheap Eats", "dietary_options": ["vegan", "gluten_free"],
                "rating": 3.2, "delivery_fee": 2.0,
                "menu_items": ["burger"],
                "temporarily_closed": false, "accepting_orders": true,
                "delivery_zones": ["downtown"]
            }),
        )
        .unwrap();
        let closed = Document::new(
            3,
            json!({
                "name": "Zen Kitchen", "dietary_options": ["vegan", "gluten_free"],
                "rating": 4.8, "delivery_fee": 4.0,
                "menu_items": ["ramen"],
                "temporarily_closed": true, "accepting_orders": true,
                "delivery_zones": ["downtown"]
            }),
        )
        .unwrap();
        let no_zone = Document::new(
            4,
            json!({
                "name": "Far Away", "dietary_options": ["vegan", "gluten_free"],
                "rating": 4.5, "delivery_fee": 3.0,
                "menu_items": ["pasta"],
                "temporarily_closed": false, "accepting_orders": true,
                "delivery_zones": ["suburbs", "airport"]
            }),
        )
        .unwrap();
        assert!(matches_doc(&q, &match_restaurant));
        assert!(!matches_doc(&q, &low_rating));
        assert!(!matches_doc(&q, &closed));
        assert!(!matches_doc(&q, &no_zone));
    }
}
