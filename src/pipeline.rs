use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use serde_json::{json, Map, Value};

use crate::document::DocumentId;
use crate::error::{Error, Result};
use crate::paged_field_index::PagedFieldIndex;
use crate::query::{self, SortOrder};
use crate::value::IndexValue;

// ---------------------------------------------------------------------------
// DocRef trait — allows exec_group to work with both Value and Arc<Value>
// ---------------------------------------------------------------------------

trait DocRef {
    fn as_value(&self) -> &Value;
}

impl DocRef for Value {
    fn as_value(&self) -> &Value {
        self
    }
}

impl DocRef for Arc<Value> {
    fn as_value(&self) -> &Value {
        self
    }
}

// ---------------------------------------------------------------------------
// Expression
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) enum Expression {
    Literal(Value),
    FieldRef(String),
    Add(Vec<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
    Multiply(Vec<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    // Conditional
    Cond(Box<Expression>, Box<Expression>, Box<Expression>), // condition, then, else
    IfNull(Box<Expression>, Box<Expression>),                // expr, replacement
    // String
    Concat(Vec<Expression>),
    ToLower(Box<Expression>),
    ToUpper(Box<Expression>),
    Substr(Box<Expression>, Box<Expression>, Box<Expression>), // string, start, length
    Trim(Box<Expression>),
    Split(Box<Expression>, Box<Expression>), // string, delimiter
    // Date
    Year(Box<Expression>),
    Month(Box<Expression>),
    DayOfMonth(Box<Expression>),
    Hour(Box<Expression>),
    Minute(Box<Expression>),
    Second(Box<Expression>),
    DayOfWeek(Box<Expression>),
    // Modulo
    Mod(Box<Expression>, Box<Expression>),
    // Array
    Size(Box<Expression>),
}

// ---------------------------------------------------------------------------
// Group key
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) enum GroupKey {
    Null,
    Single(Expression),
    Compound(Vec<(String, Expression)>),
}

// ---------------------------------------------------------------------------
// Accumulators
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub(crate) enum Accumulator {
    Sum(Expression),
    Avg(Expression),
    Min(Expression),
    Max(Expression),
    Count,
    First(Expression),
    Last(Expression),
    Push(Expression),
    AddToSet(Expression),
}

enum AccumulatorState {
    Sum(f64),
    Avg { sum: f64, count: u64 },
    Min(Option<(Value, IndexValue)>),
    Max(Option<(Value, IndexValue)>),
    Count(u64),
    First(Option<Value>),
    Last(Option<Value>),
    Push(Vec<Value>),
    AddToSet(Vec<Value>),
}

// ---------------------------------------------------------------------------
// Projection
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum ProjectionField {
    Include,
    Exclude,
    Compute(Expression),
}

// ---------------------------------------------------------------------------
// Pipeline stages
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Stage {
    Match(Value),
    Group {
        key: GroupKey,
        accumulators: Vec<(String, Accumulator)>,
    },
    Sort(Vec<(String, SortOrder)>),
    Skip(u64),
    Limit(u64),
    Project(Vec<(String, ProjectionField)>),
    Count(String),
    Unwind {
        path: String,
        preserve_null: bool,
    },
    AddFields(Vec<(String, Expression)>),
    Lookup {
        from: String,
        local_field: String,
        foreign_field: String,
        as_field: String,
        /// Additional field pairs for composite join conditions.
        extra_pairs: Vec<(String, String)>,
    },
    Out(String),
}

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

pub struct Pipeline {
    stages: Vec<Stage>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve a field path by reference — zero allocations.
/// Supports numeric segments as array indexes: `"items.0.name"` resolves
/// `items[0].name` when `items` is an array.
fn resolve_field_ref<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = doc;
    for part in path.split('.') {
        match current {
            Value::Object(map) => current = map.get(part)?,
            Value::Array(arr) => {
                let idx: usize = part.parse().ok()?;
                current = arr.get(idx)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

pub(crate) fn resolve_field(doc: &Value, path: &str) -> Value {
    resolve_field_ref(doc, path).cloned().unwrap_or(Value::Null)
}

/// Set a value at a dot-notation path, creating intermediate objects as needed.
/// Supports numeric segments as array indexes: `"items.0.stock"` sets
/// `items[0].stock` when `items` is an array.
pub(crate) fn set_field(doc: &mut Value, path: &str, value: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = doc;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            // Last segment: write the value
            match current {
                Value::Object(map) => {
                    map.insert(part.to_string(), value);
                }
                Value::Array(arr) => {
                    if let Ok(idx) = part.parse::<usize>() {
                        if idx < arr.len() {
                            arr[idx] = value;
                        }
                    }
                }
                _ => {}
            }
            return;
        }
        // Intermediate segment: navigate deeper
        match current {
            Value::Object(map) => {
                if let Some(idx) = part.parse::<usize>().ok().filter(|_| {
                    map.get(*part).map_or(false, |v| v.is_array() || v.is_object())
                        == false
                        && !map.contains_key(*part)
                }) {
                    // Numeric key but no existing entry — can't create array out of thin air
                    let _ = idx;
                    map.insert(part.to_string(), json!({}));
                } else if !map.contains_key(*part) {
                    map.insert(part.to_string(), json!({}));
                } else if let Some(v) = map.get(*part) {
                    if !v.is_object() && !v.is_array() {
                        map.insert(part.to_string(), json!({}));
                    }
                }
                current = map.get_mut(*part).unwrap();
            }
            Value::Array(arr) => {
                if let Ok(idx) = part.parse::<usize>() {
                    if idx < arr.len() {
                        current = &mut arr[idx];
                    } else {
                        return;
                    }
                } else {
                    return;
                }
            }
            _ => return,
        }
    }
}

fn to_f64(v: &Value) -> Option<f64> {
    v.as_f64()
}

/// Convert a Value to a string representation for string operators.
fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => None,
        _ => Some(v.to_string()),
    }
}

/// Check if a value is "truthy" for $cond evaluation (MongoDB semantics).
fn is_truthy(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map_or(false, |f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}

/// Parse an ISO 8601 / RFC 3339 date string into (year, month, day, hour, min, sec, weekday).
/// weekday: 1=Sunday .. 7=Saturday (MongoDB convention).
fn parse_date_parts(v: &Value) -> Option<(i32, u32, u32, u32, u32, u32, u32)> {
    let s = v.as_str()?;
    let b = s.as_bytes();
    if b.len() < 10 {
        return None;
    }
    // YYYY-MM-DD
    let year: i32 = s[0..4].parse().ok()?;
    let month: u32 = s[5..7].parse().ok()?;
    let day: u32 = s[8..10].parse().ok()?;

    let (hour, minute, second) = if b.len() >= 19 && b[10] == b'T' {
        (
            s[11..13].parse::<u32>().unwrap_or(0),
            s[14..16].parse::<u32>().unwrap_or(0),
            s[17..19].parse::<u32>().unwrap_or(0),
        )
    } else {
        (0, 0, 0)
    };

    // Zeller's formula for day of week
    let (y, m) = if month <= 2 {
        (year - 1, month + 12)
    } else {
        (year, month)
    };
    let q = day as i32;
    let k = y % 100;
    let j = y / 100;
    let h = (q + (13 * (m as i32 + 1)) / 5 + k + k / 4 + j / 4 - 2 * j).rem_euclid(7);
    // h: 0=Saturday, 1=Sunday, 2=Monday, ..., 6=Friday
    // MongoDB: 1=Sunday, 2=Monday, ..., 7=Saturday
    let dow = match h {
        0 => 7, // Saturday
        1 => 1, // Sunday
        n => n as u32,
    };

    Some((year, month, day, hour, minute, second, dow))
}

fn number_to_value(n: f64) -> Value {
    if n.fract() == 0.0 && n >= i64::MIN as f64 && n <= i64::MAX as f64 {
        Value::Number((n as i64).into())
    } else {
        serde_json::Number::from_f64(n)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}

// ---------------------------------------------------------------------------
// Expression parsing & evaluation
// ---------------------------------------------------------------------------

fn parse_expression(val: &Value) -> Result<Expression> {
    match val {
        Value::String(s) if s.starts_with('$') => Ok(Expression::FieldRef(s[1..].to_string())),
        Value::Object(map) if map.len() == 1 => {
            let (key, arg) = map.iter().next().unwrap();
            match key.as_str() {
                "$add" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidPipeline("$add requires an array".into())
                    })?;
                    let exprs: Result<Vec<_>> = arr.iter().map(parse_expression).collect();
                    Ok(Expression::Add(exprs?))
                }
                "$subtract" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidPipeline("$subtract requires an array".into())
                    })?;
                    if arr.len() != 2 {
                        return Err(Error::InvalidPipeline(
                            "$subtract requires exactly 2 arguments".into(),
                        ));
                    }
                    Ok(Expression::Subtract(
                        Box::new(parse_expression(&arr[0])?),
                        Box::new(parse_expression(&arr[1])?),
                    ))
                }
                "$multiply" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidPipeline("$multiply requires an array".into())
                    })?;
                    let exprs: Result<Vec<_>> = arr.iter().map(parse_expression).collect();
                    Ok(Expression::Multiply(exprs?))
                }
                "$divide" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidPipeline("$divide requires an array".into())
                    })?;
                    if arr.len() != 2 {
                        return Err(Error::InvalidPipeline(
                            "$divide requires exactly 2 arguments".into(),
                        ));
                    }
                    Ok(Expression::Divide(
                        Box::new(parse_expression(&arr[0])?),
                        Box::new(parse_expression(&arr[1])?),
                    ))
                }
                // Conditional
                "$cond" => {
                    match arg {
                        Value::Array(arr) if arr.len() == 3 => Ok(Expression::Cond(
                            Box::new(parse_expression(&arr[0])?),
                            Box::new(parse_expression(&arr[1])?),
                            Box::new(parse_expression(&arr[2])?),
                        )),
                        Value::Object(obj) => {
                            let if_expr = obj.get("if").ok_or_else(|| {
                                Error::InvalidPipeline("$cond requires 'if' field".into())
                            })?;
                            let then_expr = obj.get("then").ok_or_else(|| {
                                Error::InvalidPipeline("$cond requires 'then' field".into())
                            })?;
                            let else_expr = obj.get("else").ok_or_else(|| {
                                Error::InvalidPipeline("$cond requires 'else' field".into())
                            })?;
                            Ok(Expression::Cond(
                                Box::new(parse_expression(if_expr)?),
                                Box::new(parse_expression(then_expr)?),
                                Box::new(parse_expression(else_expr)?),
                            ))
                        }
                        _ => Err(Error::InvalidPipeline(
                            "$cond requires array [if,then,else] or object {if,then,else}".into(),
                        )),
                    }
                }
                "$ifNull" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidPipeline("$ifNull requires an array".into())
                    })?;
                    if arr.len() != 2 {
                        return Err(Error::InvalidPipeline(
                            "$ifNull requires exactly 2 arguments".into(),
                        ));
                    }
                    Ok(Expression::IfNull(
                        Box::new(parse_expression(&arr[0])?),
                        Box::new(parse_expression(&arr[1])?),
                    ))
                }
                // String
                "$concat" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidPipeline("$concat requires an array".into())
                    })?;
                    let exprs: Result<Vec<_>> = arr.iter().map(parse_expression).collect();
                    Ok(Expression::Concat(exprs?))
                }
                "$toLower" => Ok(Expression::ToLower(Box::new(parse_expression(arg)?))),
                "$toUpper" => Ok(Expression::ToUpper(Box::new(parse_expression(arg)?))),
                "$substr" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidPipeline("$substr requires an array".into())
                    })?;
                    if arr.len() != 3 {
                        return Err(Error::InvalidPipeline(
                            "$substr requires exactly 3 arguments".into(),
                        ));
                    }
                    Ok(Expression::Substr(
                        Box::new(parse_expression(&arr[0])?),
                        Box::new(parse_expression(&arr[1])?),
                        Box::new(parse_expression(&arr[2])?),
                    ))
                }
                "$trim" => {
                    match arg {
                        Value::Object(obj) => {
                            let input = obj.get("input").ok_or_else(|| {
                                Error::InvalidPipeline("$trim requires 'input' field".into())
                            })?;
                            Ok(Expression::Trim(Box::new(parse_expression(input)?)))
                        }
                        _ => Ok(Expression::Trim(Box::new(parse_expression(arg)?))),
                    }
                }
                "$split" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidPipeline("$split requires an array".into())
                    })?;
                    if arr.len() != 2 {
                        return Err(Error::InvalidPipeline(
                            "$split requires exactly 2 arguments".into(),
                        ));
                    }
                    Ok(Expression::Split(
                        Box::new(parse_expression(&arr[0])?),
                        Box::new(parse_expression(&arr[1])?),
                    ))
                }
                // Date
                "$year" => Ok(Expression::Year(Box::new(parse_expression(arg)?))),
                "$month" => Ok(Expression::Month(Box::new(parse_expression(arg)?))),
                "$dayOfMonth" => Ok(Expression::DayOfMonth(Box::new(parse_expression(arg)?))),
                "$hour" => Ok(Expression::Hour(Box::new(parse_expression(arg)?))),
                "$minute" => Ok(Expression::Minute(Box::new(parse_expression(arg)?))),
                "$second" => Ok(Expression::Second(Box::new(parse_expression(arg)?))),
                "$dayOfWeek" => Ok(Expression::DayOfWeek(Box::new(parse_expression(arg)?))),
                // Math
                "$mod" => {
                    let arr = arg.as_array().ok_or_else(|| {
                        Error::InvalidPipeline("$mod requires an array".into())
                    })?;
                    if arr.len() != 2 {
                        return Err(Error::InvalidPipeline(
                            "$mod requires exactly 2 arguments".into(),
                        ));
                    }
                    Ok(Expression::Mod(
                        Box::new(parse_expression(&arr[0])?),
                        Box::new(parse_expression(&arr[1])?),
                    ))
                }
                // Array
                "$size" => Ok(Expression::Size(Box::new(parse_expression(arg)?))),
                _ => Ok(Expression::Literal(Value::Object(map.clone()))),
            }
        }
        _ => Ok(Expression::Literal(val.clone())),
    }
}

/// Cow-like result for eval_ref: either a borrow from the doc or an owned value.
enum ValRef<'a> {
    Borrowed(&'a Value),
    Owned(Value),
}

impl<'a> ValRef<'a> {
    fn as_value(&self) -> &Value {
        match self {
            ValRef::Borrowed(v) => v,
            ValRef::Owned(v) => v,
        }
    }

    fn into_owned(self) -> Value {
        match self {
            ValRef::Borrowed(v) => v.clone(),
            ValRef::Owned(v) => v,
        }
    }
}

static NULL_VALUE: Value = Value::Null;

impl Expression {
    /// Fast numeric evaluation — avoids Value clone for FieldRef and Literal.
    fn eval_num(&self, doc: &Value) -> Option<f64> {
        match self {
            Expression::Literal(v) => v.as_f64(),
            Expression::FieldRef(path) => resolve_field_ref(doc, path)?.as_f64(),
            _ => to_f64(&self.eval(doc)),
        }
    }

    /// Evaluate returning a reference when possible (FieldRef, Literal).
    /// Avoids Value clones on the hot path.
    fn eval_ref<'a>(&'a self, doc: &'a Value) -> ValRef<'a> {
        match self {
            Expression::Literal(v) => ValRef::Borrowed(v),
            Expression::FieldRef(path) => {
                match resolve_field_ref(doc, path) {
                    Some(v) => ValRef::Borrowed(v),
                    None => ValRef::Borrowed(&NULL_VALUE),
                }
            }
            _ => ValRef::Owned(self.eval(doc)),
        }
    }

    fn eval(&self, doc: &Value) -> Value {
        match self {
            Expression::Literal(v) => v.clone(),
            Expression::FieldRef(path) => resolve_field(doc, path),
            Expression::Add(exprs) => {
                let mut sum = 0.0_f64;
                for e in exprs {
                    match to_f64(&e.eval(doc)) {
                        Some(n) => sum += n,
                        None => return Value::Null,
                    }
                }
                number_to_value(sum)
            }
            Expression::Subtract(a, b) => {
                match (to_f64(&a.eval(doc)), to_f64(&b.eval(doc))) {
                    (Some(a), Some(b)) => number_to_value(a - b),
                    _ => Value::Null,
                }
            }
            Expression::Multiply(exprs) => {
                let mut product = 1.0_f64;
                for e in exprs {
                    match to_f64(&e.eval(doc)) {
                        Some(n) => product *= n,
                        None => return Value::Null,
                    }
                }
                number_to_value(product)
            }
            Expression::Divide(a, b) => {
                match (to_f64(&a.eval(doc)), to_f64(&b.eval(doc))) {
                    (Some(a), Some(b)) if b != 0.0 => number_to_value(a / b),
                    _ => Value::Null,
                }
            }
            Expression::Mod(a, b) => {
                match (to_f64(&a.eval(doc)), to_f64(&b.eval(doc))) {
                    (Some(a), Some(b)) if b != 0.0 => number_to_value(a % b),
                    _ => Value::Null,
                }
            }
            // Conditional
            Expression::Cond(cond, then_expr, else_expr) => {
                if is_truthy(&cond.eval(doc)) {
                    then_expr.eval(doc)
                } else {
                    else_expr.eval(doc)
                }
            }
            Expression::IfNull(expr, replacement) => {
                let val = expr.eval(doc);
                if val.is_null() {
                    replacement.eval(doc)
                } else {
                    val
                }
            }
            // String
            Expression::Concat(exprs) => {
                let mut result = String::new();
                for e in exprs {
                    match value_to_string(&e.eval(doc)) {
                        Some(s) => result.push_str(&s),
                        None => return Value::Null,
                    }
                }
                Value::String(result)
            }
            Expression::ToLower(expr) => {
                match value_to_string(&expr.eval(doc)) {
                    Some(s) => Value::String(s.to_lowercase()),
                    None => Value::Null,
                }
            }
            Expression::ToUpper(expr) => {
                match value_to_string(&expr.eval(doc)) {
                    Some(s) => Value::String(s.to_uppercase()),
                    None => Value::Null,
                }
            }
            Expression::Substr(string_expr, start_expr, len_expr) => {
                let s = match value_to_string(&string_expr.eval(doc)) {
                    Some(s) => s,
                    None => return Value::Null,
                };
                let start = to_f64(&start_expr.eval(doc)).unwrap_or(0.0) as usize;
                let len = to_f64(&len_expr.eval(doc)).unwrap_or(0.0) as usize;
                if start >= s.len() {
                    Value::String(String::new())
                } else {
                    let end = (start + len).min(s.len());
                    Value::String(s[start..end].to_string())
                }
            }
            Expression::Trim(expr) => {
                match value_to_string(&expr.eval(doc)) {
                    Some(s) => Value::String(s.trim().to_string()),
                    None => Value::Null,
                }
            }
            Expression::Split(string_expr, delim_expr) => {
                let s = match value_to_string(&string_expr.eval(doc)) {
                    Some(s) => s,
                    None => return Value::Null,
                };
                let delim = match value_to_string(&delim_expr.eval(doc)) {
                    Some(d) => d,
                    None => return Value::Null,
                };
                let parts: Vec<Value> = s.split(&delim).map(|p| Value::String(p.to_string())).collect();
                Value::Array(parts)
            }
            // Date
            Expression::Year(expr) => {
                match parse_date_parts(&expr.eval(doc)) {
                    Some((year, _, _, _, _, _, _)) => json!(year),
                    None => Value::Null,
                }
            }
            Expression::Month(expr) => {
                match parse_date_parts(&expr.eval(doc)) {
                    Some((_, month, _, _, _, _, _)) => json!(month),
                    None => Value::Null,
                }
            }
            Expression::DayOfMonth(expr) => {
                match parse_date_parts(&expr.eval(doc)) {
                    Some((_, _, day, _, _, _, _)) => json!(day),
                    None => Value::Null,
                }
            }
            Expression::Hour(expr) => {
                match parse_date_parts(&expr.eval(doc)) {
                    Some((_, _, _, hour, _, _, _)) => json!(hour),
                    None => Value::Null,
                }
            }
            Expression::Minute(expr) => {
                match parse_date_parts(&expr.eval(doc)) {
                    Some((_, _, _, _, minute, _, _)) => json!(minute),
                    None => Value::Null,
                }
            }
            Expression::Second(expr) => {
                match parse_date_parts(&expr.eval(doc)) {
                    Some((_, _, _, _, _, second, _)) => json!(second),
                    None => Value::Null,
                }
            }
            Expression::DayOfWeek(expr) => {
                match parse_date_parts(&expr.eval(doc)) {
                    Some((_, _, _, _, _, _, dow)) => json!(dow),
                    None => Value::Null,
                }
            }
            // Array
            Expression::Size(expr) => {
                match expr.eval(doc) {
                    Value::Array(arr) => json!(arr.len()),
                    _ => Value::Null,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Stage parsing helpers
// ---------------------------------------------------------------------------

fn parse_accumulator(val: &Value) -> Result<Accumulator> {
    let obj = val
        .as_object()
        .ok_or_else(|| Error::InvalidPipeline("accumulator must be an object".into()))?;
    if obj.len() != 1 {
        return Err(Error::InvalidPipeline(
            "accumulator must have exactly one operator".into(),
        ));
    }
    let (op, arg) = obj.iter().next().unwrap();
    match op.as_str() {
        "$sum" => Ok(Accumulator::Sum(parse_expression(arg)?)),
        "$avg" => Ok(Accumulator::Avg(parse_expression(arg)?)),
        "$min" => Ok(Accumulator::Min(parse_expression(arg)?)),
        "$max" => Ok(Accumulator::Max(parse_expression(arg)?)),
        "$count" => Ok(Accumulator::Count),
        "$first" => Ok(Accumulator::First(parse_expression(arg)?)),
        "$last" => Ok(Accumulator::Last(parse_expression(arg)?)),
        "$push" => Ok(Accumulator::Push(parse_expression(arg)?)),
        "$addToSet" => Ok(Accumulator::AddToSet(parse_expression(arg)?)),
        _ => Err(Error::InvalidPipeline(format!(
            "unknown accumulator: {}",
            op
        ))),
    }
}

fn parse_group_stage(val: &Value) -> Result<Stage> {
    let obj = val
        .as_object()
        .ok_or_else(|| Error::InvalidPipeline("$group must be an object".into()))?;

    let id_val = obj
        .get("_id")
        .ok_or_else(|| Error::InvalidPipeline("$group requires '_id' field".into()))?;

    let key = match id_val {
        Value::Null => GroupKey::Null,
        Value::String(s) if s.starts_with('$') => {
            GroupKey::Single(Expression::FieldRef(s[1..].to_string()))
        }
        Value::Object(map) => {
            let has_operators = map.keys().any(|k| k.starts_with('$'));
            if has_operators {
                GroupKey::Single(parse_expression(id_val)?)
            } else {
                let fields: Result<Vec<_>> = map
                    .iter()
                    .map(|(k, v)| Ok((k.clone(), parse_expression(v)?)))
                    .collect();
                GroupKey::Compound(fields?)
            }
        }
        _ => GroupKey::Single(Expression::Literal(id_val.clone())),
    };

    let mut accumulators = Vec::new();
    for (name, spec) in obj {
        if name == "_id" {
            continue;
        }
        accumulators.push((name.clone(), parse_accumulator(spec)?));
    }

    Ok(Stage::Group { key, accumulators })
}

fn parse_sort(val: &Value) -> Result<Vec<(String, SortOrder)>> {
    let obj = val
        .as_object()
        .ok_or_else(|| Error::InvalidPipeline("$sort must be an object".into()))?;
    let mut fields = Vec::new();
    for (field, dir) in obj {
        let order = match dir.as_i64() {
            Some(1) => SortOrder::Asc,
            Some(-1) => SortOrder::Desc,
            _ => {
                return Err(Error::InvalidPipeline(
                    "sort direction must be 1 or -1".into(),
                ))
            }
        };
        fields.push((field.clone(), order));
    }
    Ok(fields)
}

fn parse_project(val: &Value) -> Result<Vec<(String, ProjectionField)>> {
    let obj = val
        .as_object()
        .ok_or_else(|| Error::InvalidPipeline("$project must be an object".into()))?;
    let mut fields = Vec::new();
    for (field, spec) in obj {
        let pf = match spec {
            Value::Number(n) if n.as_i64() == Some(1) => ProjectionField::Include,
            Value::Number(n) if n.as_i64() == Some(0) => ProjectionField::Exclude,
            Value::Bool(true) => ProjectionField::Include,
            Value::Bool(false) => ProjectionField::Exclude,
            _ => ProjectionField::Compute(parse_expression(spec)?),
        };
        fields.push((field.clone(), pf));
    }
    Ok(fields)
}

fn parse_unwind(val: &Value) -> Result<(String, bool)> {
    match val {
        Value::String(s) if s.starts_with('$') => Ok((s[1..].to_string(), false)),
        Value::Object(obj) => {
            let path = obj
                .get("path")
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    Error::InvalidPipeline("$unwind requires 'path' string".into())
                })?;
            if !path.starts_with('$') {
                return Err(Error::InvalidPipeline(
                    "$unwind path must start with $".into(),
                ));
            }
            let preserve = obj
                .get("preserveNullAndEmptyArrays")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            Ok((path[1..].to_string(), preserve))
        }
        _ => Err(Error::InvalidPipeline(
            "$unwind must be a string or object".into(),
        )),
    }
}

// ---------------------------------------------------------------------------
// Stage execution
// ---------------------------------------------------------------------------

fn exec_match(docs: Vec<Value>, match_val: &Value) -> Result<Vec<Value>> {
    let query = query::parse_query(match_val)?;
    Ok(docs
        .into_iter()
        .filter(|doc| query::matches_value(&query, doc))
        .collect())
}

/// Hash a &Value the same way as IndexValue::from_json().hash() but without
/// allocating a String for non-date string values.
fn hash_json_value<H: Hasher>(val: &Value, state: &mut H) {
    match val {
        Value::Null => {
            std::mem::discriminant(&IndexValue::Null).hash(state);
        }
        Value::Bool(b) => {
            std::mem::discriminant(&IndexValue::Boolean(false)).hash(state);
            b.hash(state);
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                std::mem::discriminant(&IndexValue::Integer(0)).hash(state);
                i.hash(state);
            } else if let Some(f) = n.as_f64() {
                std::mem::discriminant(&IndexValue::Float(0.0)).hash(state);
                f.to_bits().hash(state);
            }
        }
        Value::String(s) => {
            // Check if it would be parsed as a date
            let b = s.as_bytes();
            if b.len() >= 10
                && b[0].is_ascii_digit()
                && b[1].is_ascii_digit()
                && b[2].is_ascii_digit()
                && b[3].is_ascii_digit()
                && b[4] == b'-'
                && b[5].is_ascii_digit()
                && b[6].is_ascii_digit()
            {
                // Might be a date — fall back to IndexValue for correct hashing
                let iv = IndexValue::from_json(val);
                iv.hash(state);
            } else {
                // Non-date string: hash directly without allocation
                std::mem::discriminant(&IndexValue::String(String::new())).hash(state);
                s.hash(state);
            }
        }
        other => {
            std::mem::discriminant(&IndexValue::String(String::new())).hash(state);
            other.to_string().hash(state);
        }
    }
}

/// Fast group key hash computed directly from &Value references (zero allocation
/// for common cases like string/number group keys).
#[derive(Clone)]
struct FastGroupKey(u64);

impl PartialEq for FastGroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for FastGroupKey {}

impl Hash for FastGroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

fn compute_fast_key_single(val: &Value) -> FastGroupKey {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    1usize.hash(&mut hasher);
    hash_json_value(val, &mut hasher);
    FastGroupKey(hasher.finish())
}

fn compute_fast_key_multi<'a>(vals: impl Iterator<Item = &'a Value>, len: usize) -> FastGroupKey {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    len.hash(&mut hasher);
    for val in vals {
        hash_json_value(val, &mut hasher);
    }
    FastGroupKey(hasher.finish())
}

#[inline(always)]
fn update_accumulator(state: &mut AccumulatorState, acc: &Accumulator, doc: &Value) {
    match (acc, state) {
        (Accumulator::Sum(expr), AccumulatorState::Sum(s)) => {
            if let Some(n) = expr.eval_num(doc) {
                *s += n;
            }
        }
        (Accumulator::Avg(expr), AccumulatorState::Avg { sum, count }) => {
            if let Some(n) = expr.eval_num(doc) {
                *sum += n;
                *count += 1;
            }
        }
        (Accumulator::Min(expr), AccumulatorState::Min(current)) => {
            let vr = expr.eval_ref(doc);
            let val = vr.as_value();
            if !val.is_null() {
                let new_iv = IndexValue::from_json(val);
                let should_replace = match current {
                    None => true,
                    Some((_, cur_iv)) => new_iv < *cur_iv,
                };
                if should_replace {
                    *current = Some((val.clone(), new_iv));
                }
            }
        }
        (Accumulator::Max(expr), AccumulatorState::Max(current)) => {
            let vr = expr.eval_ref(doc);
            let val = vr.as_value();
            if !val.is_null() {
                let new_iv = IndexValue::from_json(val);
                let should_replace = match current {
                    None => true,
                    Some((_, cur_iv)) => new_iv > *cur_iv,
                };
                if should_replace {
                    *current = Some((val.clone(), new_iv));
                }
            }
        }
        (Accumulator::Count, AccumulatorState::Count(c)) => {
            *c += 1;
        }
        (Accumulator::First(expr), AccumulatorState::First(current)) => {
            if current.is_none() {
                *current = Some(expr.eval_ref(doc).into_owned());
            }
        }
        (Accumulator::Last(expr), AccumulatorState::Last(current)) => {
            *current = Some(expr.eval_ref(doc).into_owned());
        }
        (Accumulator::Push(expr), AccumulatorState::Push(vec)) => {
            vec.push(expr.eval_ref(doc).into_owned());
        }
        (Accumulator::AddToSet(expr), AccumulatorState::AddToSet(vec)) => {
            let val = expr.eval_ref(doc).into_owned();
            if !vec.contains(&val) {
                vec.push(val);
            }
        }
        _ => {}
    }
}

/// Update an accumulator from raw JSONB without full deserialization.
#[inline(always)]
fn update_accumulator_raw(state: &mut AccumulatorState, acc: &Accumulator, raw: &jsonb::RawJsonb) {
    match (acc, state) {
        (Accumulator::Sum(expr), AccumulatorState::Sum(s)) => {
            if let Some(n) = eval_expr_f64_raw(expr, raw) {
                *s += n;
            }
        }
        (Accumulator::Avg(expr), AccumulatorState::Avg { sum, count }) => {
            if let Some(n) = eval_expr_f64_raw(expr, raw) {
                *sum += n;
                *count += 1;
            }
        }
        (Accumulator::Min(expr), AccumulatorState::Min(current)) => {
            if let Some(owned) = eval_expr_raw_owned(expr, raw) {
                if let Some(new_iv) = raw_to_index_value(&owned) {
                    let should_replace = match current {
                        None => true,
                        Some((_, cur_iv)) => new_iv < *cur_iv,
                    };
                    if should_replace {
                        *current = Some((raw_owned_to_value(&owned), new_iv));
                    }
                }
            }
        }
        (Accumulator::Max(expr), AccumulatorState::Max(current)) => {
            if let Some(owned) = eval_expr_raw_owned(expr, raw) {
                if let Some(new_iv) = raw_to_index_value(&owned) {
                    let should_replace = match current {
                        None => true,
                        Some((_, cur_iv)) => new_iv > *cur_iv,
                    };
                    if should_replace {
                        *current = Some((raw_owned_to_value(&owned), new_iv));
                    }
                }
            }
        }
        (Accumulator::Count, AccumulatorState::Count(c)) => {
            *c += 1;
        }
        (Accumulator::First(expr), AccumulatorState::First(current)) => {
            if current.is_none() {
                if let Some(owned) = eval_expr_raw_owned(expr, raw) {
                    *current = Some(raw_owned_to_value(&owned));
                }
            }
        }
        (Accumulator::Last(expr), AccumulatorState::Last(current)) => {
            if let Some(owned) = eval_expr_raw_owned(expr, raw) {
                *current = Some(raw_owned_to_value(&owned));
            }
        }
        (Accumulator::Push(expr), AccumulatorState::Push(vec)) => {
            if let Some(owned) = eval_expr_raw_owned(expr, raw) {
                vec.push(raw_owned_to_value(&owned));
            }
        }
        (Accumulator::AddToSet(expr), AccumulatorState::AddToSet(vec)) => {
            if let Some(owned) = eval_expr_raw_owned(expr, raw) {
                let val = raw_owned_to_value(&owned);
                if !vec.contains(&val) {
                    vec.push(val);
                }
            }
        }
        _ => {}
    }
}

/// Evaluate a simple expression as f64 from raw JSONB.
#[inline(always)]
fn eval_expr_f64_raw(expr: &Expression, raw: &jsonb::RawJsonb) -> Option<f64> {
    match expr {
        Expression::Literal(v) => v.as_f64(),
        Expression::FieldRef(path) => raw_field_f64(raw, path),
        _ => None,
    }
}

/// Evaluate a simple expression as OwnedValue from raw JSONB.
#[inline(always)]
fn eval_expr_raw_owned(expr: &Expression, raw: &jsonb::RawJsonb) -> Option<jsonb::OwnedJsonb> {
    match expr {
        Expression::FieldRef(path) => extract_raw_field(raw, path),
        Expression::Literal(_) => {
            None // Literals use the f64 fast path ($sum:1) — OwnedJsonb not needed
        }
        _ => None,
    }
}

/// Convert a raw JSONB extracted field to IndexValue.
fn raw_to_index_value(owned: &jsonb::OwnedJsonb) -> Option<IndexValue> {
    let val: Value = jsonb::from_raw_jsonb(&owned.as_raw()).ok()?;
    Some(IndexValue::from_json(&val))
}

fn exec_group<D: DocRef>(
    docs: &[D],
    key: &GroupKey,
    accumulators: &[(String, Accumulator)],
) -> Result<Vec<Value>> {
    // Pre-size with a reasonable estimate: sqrt(n) for typical cardinality
    let estimated_groups = (docs.len() as f64).sqrt() as usize + 1;
    let mut groups: HashMap<FastGroupKey, (Value, Vec<AccumulatorState>)> =
        HashMap::with_capacity(estimated_groups);
    let mut insertion_order: Vec<FastGroupKey> = Vec::with_capacity(estimated_groups);

    for doc in docs {
        let doc = doc.as_value();

        // Compute group key hash directly from references — zero allocation
        let key_hash = match key {
            GroupKey::Null => FastGroupKey(0),
            GroupKey::Single(expr) => {
                let vr = expr.eval_ref(doc);
                compute_fast_key_single(vr.as_value())
            }
            GroupKey::Compound(fields) => {
                // Evaluate all field refs first, then hash
                let vals: Vec<ValRef> = fields.iter().map(|(_, expr)| expr.eval_ref(doc)).collect();
                compute_fast_key_multi(vals.iter().map(|vr| vr.as_value()), vals.len())
            }
        };

        // Fast path: get_mut avoids key_hash clone for existing groups (99%+ of iterations)
        if let Some((_, states)) = groups.get_mut(&key_hash) {
            for (i, (_, acc)) in accumulators.iter().enumerate() {
                update_accumulator(&mut states[i], acc, doc);
            }
            continue;
        }

        // New group — materialize key Value only once per group
        let key_val = match key {
            GroupKey::Null => Value::Null,
            GroupKey::Single(expr) => expr.eval_ref(doc).into_owned(),
            GroupKey::Compound(fields) => {
                let mut map = Map::new();
                for (name, expr) in fields {
                    map.insert(name.clone(), expr.eval_ref(doc).into_owned());
                }
                Value::Object(map)
            }
        };
        let mut initial: Vec<AccumulatorState> = accumulators
            .iter()
            .map(|(_, acc)| match acc {
                Accumulator::Sum(_) => AccumulatorState::Sum(0.0),
                Accumulator::Avg(_) => AccumulatorState::Avg {
                    sum: 0.0,
                    count: 0,
                },
                Accumulator::Min(_) => AccumulatorState::Min(None),
                Accumulator::Max(_) => AccumulatorState::Max(None),
                Accumulator::Count => AccumulatorState::Count(0),
                Accumulator::First(_) => AccumulatorState::First(None),
                Accumulator::Last(_) => AccumulatorState::Last(None),
                Accumulator::Push(_) => AccumulatorState::Push(Vec::new()),
                Accumulator::AddToSet(_) => AccumulatorState::AddToSet(Vec::new()),
            })
            .collect();
        for (i, (_, acc)) in accumulators.iter().enumerate() {
            update_accumulator(&mut initial[i], acc, doc);
        }
        insertion_order.push(key_hash.clone());
        groups.insert(key_hash, (key_val, initial));
    }

    let mut results = Vec::with_capacity(insertion_order.len());
    for key_hash in &insertion_order {
        let (key_val, states) = groups.remove(key_hash).unwrap();
        let mut doc = Map::new();
        doc.insert("_id".to_string(), key_val);

        for ((name, _), state) in accumulators.iter().zip(states) {
            let val = match state {
                AccumulatorState::Sum(s) => number_to_value(s),
                AccumulatorState::Avg { sum, count } => {
                    if count == 0 {
                        Value::Null
                    } else {
                        number_to_value(sum / count as f64)
                    }
                }
                AccumulatorState::Min(v) => v.map(|(val, _)| val).unwrap_or(Value::Null),
                AccumulatorState::Max(v) => v.map(|(val, _)| val).unwrap_or(Value::Null),
                AccumulatorState::Count(c) => Value::Number(c.into()),
                AccumulatorState::First(v) => v.unwrap_or(Value::Null),
                AccumulatorState::Last(v) => v.unwrap_or(Value::Null),
                AccumulatorState::Push(v) => Value::Array(v),
                AccumulatorState::AddToSet(v) => Value::Array(v),
            };
            doc.insert(name.clone(), val);
        }

        results.push(Value::Object(doc));
    }

    Ok(results)
}

fn exec_sort(mut docs: Vec<Value>, sort_fields: &[(String, SortOrder)]) -> Vec<Value> {
    docs.sort_by(|a, b| {
        for (field, order) in sort_fields {
            let av = resolve_field_ref(a, field);
            let bv = resolve_field_ref(b, field);
            let aiv = av.map(IndexValue::from_json).unwrap_or(IndexValue::Null);
            let biv = bv.map(IndexValue::from_json).unwrap_or(IndexValue::Null);
            let cmp = aiv.cmp(&biv);
            let cmp = match order {
                SortOrder::Asc => cmp,
                SortOrder::Desc => cmp.reverse(),
            };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
    docs
}

fn exec_skip(docs: Vec<Value>, n: u64) -> Vec<Value> {
    docs.into_iter().skip(n as usize).collect()
}

fn exec_limit(docs: Vec<Value>, n: u64) -> Vec<Value> {
    docs.into_iter().take(n as usize).collect()
}

fn exec_project(docs: Vec<Value>, fields: &[(String, ProjectionField)]) -> Vec<Value> {
    let has_include = fields
        .iter()
        .any(|(name, pf)| name != "_id" && matches!(pf, ProjectionField::Include));
    let has_compute = fields
        .iter()
        .any(|(_, pf)| matches!(pf, ProjectionField::Compute(_)));
    let inclusion_mode = has_include || has_compute;

    docs.into_iter()
        .map(|doc| {
            let mut result = Map::new();

            if inclusion_mode {
                let id_excluded = fields
                    .iter()
                    .any(|(name, pf)| name == "_id" && matches!(pf, ProjectionField::Exclude));

                if !id_excluded {
                    if let Some(id_val) = doc.as_object().and_then(|m| m.get("_id")) {
                        result.insert("_id".to_string(), id_val.clone());
                    }
                }

                for (name, pf) in fields {
                    match pf {
                        ProjectionField::Include => {
                            let val = resolve_field(&doc, name);
                            if !val.is_null()
                                || doc
                                    .as_object()
                                    .map_or(false, |m| m.contains_key(name.as_str()))
                            {
                                result.insert(name.clone(), val);
                            }
                        }
                        ProjectionField::Compute(expr) => {
                            result.insert(name.clone(), expr.eval(&doc));
                        }
                        ProjectionField::Exclude => {}
                    }
                }
            } else {
                // Exclusion mode
                if let Value::Object(map) = &doc {
                    result = map.clone();
                }
                for (name, pf) in fields {
                    if matches!(pf, ProjectionField::Exclude) {
                        result.remove(name.as_str());
                    }
                }
            }

            Value::Object(result)
        })
        .collect()
}

fn exec_count(docs: Vec<Value>, field_name: &str) -> Vec<Value> {
    vec![json!({ field_name: docs.len() })]
}

fn exec_unwind(docs: Vec<Value>, path: &str, preserve_null: bool) -> Vec<Value> {
    let mut result = Vec::new();
    for doc in docs {
        let field_val = resolve_field(&doc, path);
        match field_val {
            Value::Array(arr) => {
                if arr.is_empty() {
                    if preserve_null {
                        result.push(doc);
                    }
                } else {
                    for item in arr {
                        let mut new_doc = doc.clone();
                        set_field(&mut new_doc, path, item);
                        result.push(new_doc);
                    }
                }
            }
            Value::Null => {
                if preserve_null {
                    result.push(doc);
                }
            }
            _ => {
                // Non-array, non-null: pass through unchanged
                result.push(doc);
            }
        }
    }
    result
}

fn exec_add_fields(docs: Vec<Value>, fields: &[(String, Expression)]) -> Vec<Value> {
    docs.into_iter()
        .map(|mut doc| {
            for (name, expr) in fields {
                let val = expr.eval(&doc);
                set_field(&mut doc, name, val);
            }
            doc
        })
        .collect()
}

fn exec_lookup<F>(
    docs: Vec<Value>,
    from: &str,
    local_field: &str,
    foreign_field: &str,
    as_field: &str,
    extra_pairs: &[(String, String)],
    lookup_fn: &F,
) -> Result<Vec<Value>>
where
    F: Fn(&str, &Value) -> Result<Vec<Value>>,
{
    let mut result = Vec::new();
    for mut doc in docs {
        let local_val = resolve_field(&doc, local_field);
        let query = json!({ foreign_field: local_val });
        let mut foreign_docs = lookup_fn(from, &query)?;

        // Filter by additional field pairs (composite join)
        if !extra_pairs.is_empty() {
            foreign_docs.retain(|foreign_doc| {
                extra_pairs.iter().all(|(local_f, foreign_f)| {
                    let lv = resolve_field(&doc, local_f);
                    let fv = resolve_field(foreign_doc, foreign_f);
                    lv == fv
                })
            });
        }

        set_field(&mut doc, as_field, Value::Array(foreign_docs));
        result.push(doc);
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Index-accelerated $group
// ---------------------------------------------------------------------------

/// Pure index-only count aggregation: when the group key is a single FieldRef
/// with a PagedFieldIndex and all accumulators are Count or Sum(Literal), we can
/// read counts directly from the index without touching any documents.
///
/// `total_docs` is the total number of documents in the collection. When
/// `match_query` is `None` (full collection scan), the null group is computed
/// as `total_docs - total_indexed`.
///
/// Returns `Some(results)` if applicable, `None` otherwise.
pub(crate) fn try_index_only_count(
    key: &GroupKey,
    accumulators: &[(String, Accumulator)],
    field_indexes: &HashMap<String, PagedFieldIndex>,
    total_docs: usize,
    match_query: Option<&Value>,
) -> Option<Vec<Value>> {
    // Only works for full collection scans (no $match filter)
    if match_query.is_some() {
        return None;
    }

    // Only single-field group key
    let group_field = match key {
        GroupKey::Single(Expression::FieldRef(field)) => field.as_str(),
        _ => return None,
    };

    let fi = field_indexes.get(group_field)?;

    // All accumulators must be count-only
    let is_count_only = accumulators.iter().all(|(_, acc)| {
        matches!(
            acc,
            Accumulator::Count
                | Accumulator::Sum(Expression::Literal(Value::Number(_)))
        )
    });
    if !is_count_only {
        return None;
    }

    let total_indexed: usize = fi.iter_asc().map(|(_, ids)| ids.len()).sum();
    if total_docs < total_indexed {
        return None;
    }

    let mut results = Vec::new();
    for (idx_val, doc_ids) in fi.iter_asc() {
        if doc_ids.is_empty() {
            continue;
        }
        let group_count = doc_ids.len() as u64;
        let key_val = idx_val.to_json();
        let mut doc = Map::new();
        doc.insert("_id".to_string(), key_val);
        for (name, acc) in accumulators {
            let val = match acc {
                Accumulator::Count => Value::Number(group_count.into()),
                Accumulator::Sum(Expression::Literal(v)) => {
                    if let Some(n) = v.as_f64() {
                        number_to_value(n * group_count as f64)
                    } else {
                        Value::Number(group_count.into())
                    }
                }
                _ => unreachable!(),
            };
            doc.insert(name.clone(), val);
        }
        results.push(Value::Object(doc));
    }

    // Handle docs that don't have the group field (null group)
    let docs_without_field = total_docs - total_indexed;
    if docs_without_field > 0 {
        let group_count = docs_without_field as u64;
        let mut doc = Map::new();
        doc.insert("_id".to_string(), Value::Null);
        for (name, acc) in accumulators {
            let val = match acc {
                Accumulator::Count => Value::Number(group_count.into()),
                Accumulator::Sum(Expression::Literal(v)) => {
                    if let Some(n) = v.as_f64() {
                        number_to_value(n * group_count as f64)
                    } else {
                        Value::Number(group_count.into())
                    }
                }
                _ => unreachable!(),
            };
            doc.insert(name.clone(), val);
        }
        results.push(Value::Object(doc));
    }

    Some(results)
}

/// Try to execute a $group stage using field indexes instead of hashing all docs.
///
/// **Count-only fast path** (Opt 4): When the group key is a single FieldRef and
/// all accumulators are Count or Sum(Literal(1)), we can read counts directly
/// from `PagedFieldIndex::iter_asc()` without touching any documents at all.
///
/// **Index-partitioned fast path** (Opt 5): When the group key is a single FieldRef
/// with a PagedFieldIndex, iterate index entries to get pre-partitioned groups. For each
/// group, look up docs from doc_cache and feed accumulators. Avoids HashMap overhead.
///
/// Returns `Some(results)` if an index path was used, `None` otherwise.
fn try_index_group(
    key: &GroupKey,
    accumulators: &[(String, Accumulator)],
    docs: &[Arc<Value>],
    field_indexes: Option<&HashMap<String, PagedFieldIndex>>,
    doc_lookup: Option<&dyn Fn(DocumentId) -> Option<Arc<Value>>>,
) -> Result<Option<Vec<Value>>> {
    // Only optimize single-field group key
    let group_field = match key {
        GroupKey::Single(Expression::FieldRef(field)) => field.as_str(),
        _ => return Ok(None),
    };

    let fi = match field_indexes.and_then(|fi| fi.get(group_field)) {
        Some(idx) => idx,
        None => return Ok(None),
    };

    // Check if this is a count-only aggregation (Opt 4)
    let is_count_only = accumulators.iter().all(|(_, acc)| {
        matches!(
            acc,
            Accumulator::Count
                | Accumulator::Sum(Expression::Literal(Value::Number(_)))
        )
    });

    if is_count_only {
        // If docs is the full collection (no $match filter), use index directly
        // We detect this by checking if docs.len() >= total indexed doc count
        let total_indexed: usize = fi.iter_asc().map(|(_, ids)| ids.len()).sum();
        if docs.len() >= total_indexed {
            // Pure index-only count: no doc reads at all
            let mut results = Vec::new();
            for (idx_val, doc_ids) in fi.iter_asc() {
                if doc_ids.is_empty() {
                    continue;
                }
                let group_count = doc_ids.len() as u64;
                let key_val = idx_val.to_json();
                let mut doc = Map::new();
                doc.insert("_id".to_string(), key_val);
                for (name, acc) in accumulators {
                    let val = match acc {
                        Accumulator::Count => Value::Number(group_count.into()),
                        Accumulator::Sum(Expression::Literal(v)) => {
                            if let Some(n) = v.as_f64() {
                                number_to_value(n * group_count as f64)
                            } else {
                                Value::Number(group_count.into())
                            }
                        }
                        _ => unreachable!(),
                    };
                    doc.insert(name.clone(), val);
                }
                results.push(Value::Object(doc));
            }
            // Handle docs that don't have the group field (null group)
            let docs_without_field = docs.len() - total_indexed;
            if docs_without_field > 0 {
                let group_count = docs_without_field as u64;
                let mut doc = Map::new();
                doc.insert("_id".to_string(), Value::Null);
                for (name, acc) in accumulators {
                    let val = match acc {
                        Accumulator::Count => Value::Number(group_count.into()),
                        Accumulator::Sum(Expression::Literal(v)) => {
                            if let Some(n) = v.as_f64() {
                                number_to_value(n * group_count as f64)
                            } else {
                                Value::Number(group_count.into())
                            }
                        }
                        _ => unreachable!(),
                    };
                    doc.insert(name.clone(), val);
                }
                results.push(Value::Object(doc));
            }
            return Ok(Some(results));
        }
        // If there was a $match filter, docs is a subset — fall through to Opt 5 path
    }

    // Opt 5: Index-partitioned group with any accumulators.
    // Only works when we have doc_cache to look up individual docs.
    let dl = match doc_lookup {
        Some(dl) => dl,
        None => return Ok(None),
    };

    // Build a set of doc IDs that are in the current result set (post-$match).
    // For large collections, this is more efficient than hashing group keys.
    let total_indexed: usize = fi.iter_asc().map(|(_, ids)| ids.len()).sum();
    // Only use this path when docs came from a full collection scan (no $match filter)
    // or when the dataset is large enough to benefit from index partitioning.
    // For filtered datasets, the standard hash-based path is fine since docs are already few.
    if docs.len() < total_indexed {
        return Ok(None);
    }

    let mut results = Vec::new();
    for (idx_val, doc_ids) in fi.iter_asc() {
        if doc_ids.is_empty() {
            continue;
        }
        let key_val = idx_val.to_json();
        let mut states: Vec<AccumulatorState> = accumulators
            .iter()
            .map(|(_, acc)| match acc {
                Accumulator::Sum(_) => AccumulatorState::Sum(0.0),
                Accumulator::Avg(_) => AccumulatorState::Avg { sum: 0.0, count: 0 },
                Accumulator::Min(_) => AccumulatorState::Min(None),
                Accumulator::Max(_) => AccumulatorState::Max(None),
                Accumulator::Count => AccumulatorState::Count(0),
                Accumulator::First(_) => AccumulatorState::First(None),
                Accumulator::Last(_) => AccumulatorState::Last(None),
                Accumulator::Push(_) => AccumulatorState::Push(Vec::new()),
                Accumulator::AddToSet(_) => AccumulatorState::AddToSet(Vec::new()),
            })
            .collect();

        for &doc_id in doc_ids {
            if let Some(doc_arc) = dl(doc_id) {
                let doc = doc_arc.as_ref();
                for (i, (_, acc)) in accumulators.iter().enumerate() {
                    update_accumulator_state(&mut states[i], acc, doc);
                }
            }
        }

        let mut doc = Map::new();
        doc.insert("_id".to_string(), key_val);
        for ((name, _), state) in accumulators.iter().zip(states) {
            doc.insert(name.clone(), finalize_accumulator(state));
        }
        results.push(Value::Object(doc));
    }

    // Handle docs without the group field (null group)
    let docs_without_field = docs.len() - total_indexed;
    if docs_without_field > 0 {
        // Collect null-group doc IDs: all docs not in any index entry
        let mut null_states: Vec<AccumulatorState> = accumulators
            .iter()
            .map(|(_, acc)| match acc {
                Accumulator::Sum(_) => AccumulatorState::Sum(0.0),
                Accumulator::Avg(_) => AccumulatorState::Avg { sum: 0.0, count: 0 },
                Accumulator::Min(_) => AccumulatorState::Min(None),
                Accumulator::Max(_) => AccumulatorState::Max(None),
                Accumulator::Count => AccumulatorState::Count(0),
                Accumulator::First(_) => AccumulatorState::First(None),
                Accumulator::Last(_) => AccumulatorState::Last(None),
                Accumulator::Push(_) => AccumulatorState::Push(Vec::new()),
                Accumulator::AddToSet(_) => AccumulatorState::AddToSet(Vec::new()),
            })
            .collect();

        // We need to find which docs don't have the group field.
        // Build a HashSet of all indexed doc IDs for fast lookup.
        let mut indexed_ids = std::collections::HashSet::with_capacity(total_indexed);
        for (_, ids) in fi.iter_asc() {
            for &id in ids {
                indexed_ids.insert(id);
            }
        }
        for doc_arc in docs {
            if let Some(id) = doc_arc.get("_id").and_then(|v| v.as_u64()) {
                if !indexed_ids.contains(&id) {
                    let doc = doc_arc.as_ref();
                    for (i, (_, acc)) in accumulators.iter().enumerate() {
                        update_accumulator_state(&mut null_states[i], acc, doc);
                    }
                }
            }
        }

        let mut doc = Map::new();
        doc.insert("_id".to_string(), Value::Null);
        for ((name, _), state) in accumulators.iter().zip(null_states) {
            doc.insert(name.clone(), finalize_accumulator(state));
        }
        results.push(Value::Object(doc));
    }

    Ok(Some(results))
}

/// Update a single accumulator state with a document value.
fn update_accumulator_state(state: &mut AccumulatorState, acc: &Accumulator, doc: &Value) {
    match (acc, state) {
        (Accumulator::Sum(expr), AccumulatorState::Sum(s)) => {
            if let Some(n) = expr.eval_num(doc) {
                *s += n;
            }
        }
        (Accumulator::Avg(expr), AccumulatorState::Avg { sum, count }) => {
            if let Some(n) = expr.eval_num(doc) {
                *sum += n;
                *count += 1;
            }
        }
        (Accumulator::Min(expr), AccumulatorState::Min(current)) => {
            let vr = expr.eval_ref(doc);
            let val = vr.as_value();
            if !val.is_null() {
                let new_iv = IndexValue::from_json(val);
                let should_replace = match current {
                    None => true,
                    Some((_, cur_iv)) => new_iv < *cur_iv,
                };
                if should_replace {
                    *current = Some((val.clone(), new_iv));
                }
            }
        }
        (Accumulator::Max(expr), AccumulatorState::Max(current)) => {
            let vr = expr.eval_ref(doc);
            let val = vr.as_value();
            if !val.is_null() {
                let new_iv = IndexValue::from_json(val);
                let should_replace = match current {
                    None => true,
                    Some((_, cur_iv)) => new_iv > *cur_iv,
                };
                if should_replace {
                    *current = Some((val.clone(), new_iv));
                }
            }
        }
        (Accumulator::Count, AccumulatorState::Count(c)) => {
            *c += 1;
        }
        (Accumulator::First(expr), AccumulatorState::First(current)) => {
            if current.is_none() {
                *current = Some(expr.eval_ref(doc).into_owned());
            }
        }
        (Accumulator::Last(expr), AccumulatorState::Last(current)) => {
            *current = Some(expr.eval_ref(doc).into_owned());
        }
        (Accumulator::Push(expr), AccumulatorState::Push(vec)) => {
            vec.push(expr.eval_ref(doc).into_owned());
        }
        _ => {}
    }
}

/// Convert an accumulator state into its final Value.
fn finalize_accumulator(state: AccumulatorState) -> Value {
    match state {
        AccumulatorState::Sum(s) => number_to_value(s),
        AccumulatorState::Avg { sum, count } => {
            if count == 0 {
                Value::Null
            } else {
                number_to_value(sum / count as f64)
            }
        }
        AccumulatorState::Min(v) => v.map(|(val, _)| val).unwrap_or(Value::Null),
        AccumulatorState::Max(v) => v.map(|(val, _)| val).unwrap_or(Value::Null),
        AccumulatorState::Count(c) => Value::Number(c.into()),
        AccumulatorState::First(v) => v.unwrap_or(Value::Null),
        AccumulatorState::Last(v) => v.unwrap_or(Value::Null),
        AccumulatorState::Push(v) => Value::Array(v),
        AccumulatorState::AddToSet(v) => Value::Array(v),
    }
}

/// Merge two accumulator states (for parallel aggregation).
/// `self_state` is from the earlier segment, `other` from the later.
fn merge_accumulator_state(self_state: &mut AccumulatorState, other: AccumulatorState) {
    match (self_state, other) {
        (AccumulatorState::Sum(s), AccumulatorState::Sum(o)) => *s += o,
        (AccumulatorState::Avg { sum, count }, AccumulatorState::Avg { sum: os, count: oc }) => {
            *sum += os;
            *count += oc;
        }
        (AccumulatorState::Count(c), AccumulatorState::Count(o)) => *c += o,
        (AccumulatorState::Min(cur), AccumulatorState::Min(other_min)) => {
            if let Some((ov, oiv)) = other_min {
                let replace = match cur {
                    None => true,
                    Some((_, civ)) => oiv < *civ,
                };
                if replace {
                    *cur = Some((ov, oiv));
                }
            }
        }
        (AccumulatorState::Max(cur), AccumulatorState::Max(other_max)) => {
            if let Some((ov, oiv)) = other_max {
                let replace = match cur {
                    None => true,
                    Some((_, civ)) => oiv > *civ,
                };
                if replace {
                    *cur = Some((ov, oiv));
                }
            }
        }
        (AccumulatorState::First(cur), AccumulatorState::First(_)) => {
            // Keep self (earlier segment)
            let _ = cur;
        }
        (AccumulatorState::Last(_cur), AccumulatorState::Last(other_last)) => {
            // Take other (later segment) if it has a value
            if other_last.is_some() {
                *_cur = other_last;
            }
        }
        (AccumulatorState::Push(vec), AccumulatorState::Push(mut other_vec)) => {
            vec.append(&mut other_vec);
        }
        (AccumulatorState::AddToSet(vec), AccumulatorState::AddToSet(other_vec)) => {
            for val in other_vec {
                if !vec.contains(&val) {
                    vec.push(val);
                }
            }
        }
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Raw JSONB helpers for zero-decode aggregation
// ---------------------------------------------------------------------------

/// Extract a field from raw JSONB by dot-path (e.g. "address.city").
fn extract_raw_field(raw: &jsonb::RawJsonb, path: &str) -> Option<jsonb::OwnedJsonb> {
    use jsonb::keypath::KeyPath;
    use std::borrow::Cow;
    let parts: Vec<&str> = path.split('.').collect();
    let keypath: Vec<KeyPath> = parts
        .iter()
        .map(|p| KeyPath::Name(Cow::Borrowed(p)))
        .collect();
    raw.get_by_keypath(keypath.iter()).ok()?
}

/// Extract a numeric value from a raw JSONB field.
/// Decodes only the extracted field (not the whole document).
fn raw_field_f64(raw: &jsonb::RawJsonb, path: &str) -> Option<f64> {
    let owned = extract_raw_field(raw, path)?;
    // Decode just this one field value to serde_json::Value
    let val: Value = jsonb::from_raw_jsonb(&owned.as_raw()).ok()?;
    val.as_f64()
}

/// Decode a raw JSONB extracted field into a serde_json::Value.
/// Much cheaper than decoding the full document — only one field.
fn raw_owned_to_value(owned: &jsonb::OwnedJsonb) -> Value {
    jsonb::from_raw_jsonb(&owned.as_raw()).unwrap_or(Value::Null)
}

/// Hash a raw JSONB extracted field consistent with `hash_json_value`.
/// Uses RawJsonb accessors to avoid allocating a serde_json::Value for
/// the common cases (null, bool, number, non-date string).
fn hash_raw_owned<H: Hasher>(owned: &jsonb::OwnedJsonb, state: &mut H) -> bool {
    let raw = owned.as_raw();

    // Null
    if let Ok(true) = raw.is_null() {
        std::mem::discriminant(&IndexValue::Null).hash(state);
        return true;
    }

    // Boolean
    if let Ok(Some(b)) = raw.as_bool() {
        std::mem::discriminant(&IndexValue::Boolean(false)).hash(state);
        b.hash(state);
        return true;
    }

    // Number — must match hash_json_value: integer path first, then float
    if let Ok(true) = raw.is_number() {
        if let Ok(Some(i)) = raw.as_i64() {
            std::mem::discriminant(&IndexValue::Integer(0)).hash(state);
            i.hash(state);
        } else if let Ok(Some(f)) = raw.as_f64() {
            std::mem::discriminant(&IndexValue::Float(0.0)).hash(state);
            f.to_bits().hash(state);
        }
        return true;
    }

    // String — check for date pattern (same heuristic as hash_json_value)
    if let Ok(Some(s)) = raw.as_str() {
        let b = s.as_bytes();
        if b.len() >= 10
            && b[0].is_ascii_digit()
            && b[1].is_ascii_digit()
            && b[2].is_ascii_digit()
            && b[3].is_ascii_digit()
            && b[4] == b'-'
            && b[5].is_ascii_digit()
            && b[6].is_ascii_digit()
        {
            // Possible date string — use IndexValue for correct hashing
            if let Ok(val) = jsonb::from_raw_jsonb(&raw) {
                hash_json_value(&val, state);
                return true;
            }
            return false;
        }
        // Non-date string: hash directly
        std::mem::discriminant(&IndexValue::String(String::new())).hash(state);
        s.hash(state);
        return true;
    }

    // Arrays/Objects: fall back to full Value decode
    let val: Value = match jsonb::from_raw_jsonb(&raw) {
        Ok(v) => v,
        Err(_) => return false,
    };
    hash_json_value(&val, state);
    true
}

/// Check if all expressions in the pipeline are raw-JSONB-compatible
/// (only FieldRef and Literal — no arithmetic expressions).
pub(crate) fn is_raw_eligible(key: &GroupKey, accumulators: &[(String, Accumulator)]) -> bool {
    let key_ok = match key {
        GroupKey::Null => true,
        GroupKey::Single(e) => matches!(e, Expression::FieldRef(_) | Expression::Literal(_)),
        GroupKey::Compound(fields) => fields
            .iter()
            .all(|(_, e)| matches!(e, Expression::FieldRef(_) | Expression::Literal(_))),
    };
    if !key_ok {
        return false;
    }
    accumulators.iter().all(|(_, acc)| match acc {
        Accumulator::Count => true,
        Accumulator::Sum(e)
        | Accumulator::Avg(e)
        | Accumulator::Min(e)
        | Accumulator::Max(e)
        | Accumulator::First(e)
        | Accumulator::Last(e)
        | Accumulator::Push(e)
        | Accumulator::AddToSet(e) => matches!(e, Expression::FieldRef(_) | Expression::Literal(_)),
    })
}

// ---------------------------------------------------------------------------
// Streaming group execution (used by Collection::aggregate_streaming)
// ---------------------------------------------------------------------------

/// Streaming group aggregator that accumulates documents one at a time.
/// Call `feed()` for each document, then `finalize()` to get results.
pub(crate) struct StreamingGroup {
    key: GroupKey,
    accumulators: Vec<(String, Accumulator)>,
    groups: HashMap<FastGroupKey, (Value, Vec<AccumulatorState>)>,
    insertion_order: Vec<FastGroupKey>,
}

impl StreamingGroup {
    pub(crate) fn new(key: &GroupKey, accumulators: &[(String, Accumulator)]) -> Self {
        Self {
            key: key.clone(),
            accumulators: accumulators.to_vec(),
            groups: HashMap::new(),
            insertion_order: Vec::new(),
        }
    }

    /// Feed a single document into the group accumulators.
    pub(crate) fn feed(&mut self, doc: &Value) {
        let key_hash = match &self.key {
            GroupKey::Null => FastGroupKey(0),
            GroupKey::Single(expr) => {
                let vr = expr.eval_ref(doc);
                compute_fast_key_single(vr.as_value())
            }
            GroupKey::Compound(fields) => {
                let vals: Vec<ValRef> =
                    fields.iter().map(|(_, expr)| expr.eval_ref(doc)).collect();
                compute_fast_key_multi(vals.iter().map(|vr| vr.as_value()), vals.len())
            }
        };

        if let Some((_, states)) = self.groups.get_mut(&key_hash) {
            for (i, (_, acc)) in self.accumulators.iter().enumerate() {
                update_accumulator(&mut states[i], acc, doc);
            }
            return;
        }

        // New group — materialize key Value only once
        let key_val = match &self.key {
            GroupKey::Null => Value::Null,
            GroupKey::Single(expr) => expr.eval_ref(doc).into_owned(),
            GroupKey::Compound(fields) => {
                let mut map = Map::new();
                for (name, expr) in fields {
                    map.insert(name.clone(), expr.eval_ref(doc).into_owned());
                }
                Value::Object(map)
            }
        };
        let mut initial: Vec<AccumulatorState> = self
            .accumulators
            .iter()
            .map(|(_, acc)| match acc {
                Accumulator::Sum(_) => AccumulatorState::Sum(0.0),
                Accumulator::Avg(_) => AccumulatorState::Avg {
                    sum: 0.0,
                    count: 0,
                },
                Accumulator::Min(_) => AccumulatorState::Min(None),
                Accumulator::Max(_) => AccumulatorState::Max(None),
                Accumulator::Count => AccumulatorState::Count(0),
                Accumulator::First(_) => AccumulatorState::First(None),
                Accumulator::Last(_) => AccumulatorState::Last(None),
                Accumulator::Push(_) => AccumulatorState::Push(Vec::new()),
                Accumulator::AddToSet(_) => AccumulatorState::AddToSet(Vec::new()),
            })
            .collect();
        for (i, (_, acc)) in self.accumulators.iter().enumerate() {
            update_accumulator(&mut initial[i], acc, doc);
        }
        self.insertion_order.push(key_hash.clone());
        self.groups.insert(key_hash, (key_val, initial));
    }

    /// Feed directly from raw JSONB bytes — extracts only the fields needed
    /// for the group key and accumulators, skipping full deserialization.
    /// Caller must ensure `is_raw_eligible()` is true.
    pub(crate) fn feed_raw(&mut self, raw_bytes: &[u8]) {
        // Legacy JSON text: fall back to full decode
        if raw_bytes.is_empty() || raw_bytes[0] == b'{' || raw_bytes[0] == b'[' {
            if let Ok(doc) = serde_json::from_slice::<Value>(raw_bytes) {
                self.feed(&doc);
            }
            return;
        }

        let raw = jsonb::RawJsonb::new(raw_bytes);

        // Compute group key hash directly from raw JSONB
        let key_hash = match &self.key {
            GroupKey::Null => FastGroupKey(0),
            GroupKey::Single(expr) => {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                1usize.hash(&mut hasher);
                match expr {
                    Expression::FieldRef(path) => {
                        if let Some(owned) = extract_raw_field(&raw, path) {
                            if !hash_raw_owned(&owned, &mut hasher) {
                                let doc: Value =
                                    jsonb::from_raw_jsonb(&raw).unwrap_or(Value::Null);
                                return self.feed(&doc);
                            }
                        } else {
                            std::mem::discriminant(&IndexValue::Null).hash(&mut hasher);
                        }
                    }
                    Expression::Literal(v) => hash_json_value(v, &mut hasher),
                    _ => {
                        let doc: Value = jsonb::from_raw_jsonb(&raw).unwrap_or(Value::Null);
                        return self.feed(&doc);
                    }
                }
                FastGroupKey(hasher.finish())
            }
            GroupKey::Compound(fields) => {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                fields.len().hash(&mut hasher);
                for (_, expr) in fields {
                    match expr {
                        Expression::FieldRef(path) => {
                            if let Some(owned) = extract_raw_field(&raw, path) {
                                if !hash_raw_owned(&owned, &mut hasher) {
                                    let doc: Value =
                                        jsonb::from_raw_jsonb(&raw).unwrap_or(Value::Null);
                                    return self.feed(&doc);
                                }
                            } else {
                                std::mem::discriminant(&IndexValue::Null).hash(&mut hasher);
                            }
                        }
                        Expression::Literal(v) => hash_json_value(v, &mut hasher),
                        _ => {
                            let doc: Value =
                                jsonb::from_raw_jsonb(&raw).unwrap_or(Value::Null);
                            return self.feed(&doc);
                        }
                    }
                }
                FastGroupKey(hasher.finish())
            }
        };

        // Fast path: existing group — update accumulators from raw JSONB
        if let Some((_, states)) = self.groups.get_mut(&key_hash) {
            for (i, (_, acc)) in self.accumulators.iter().enumerate() {
                update_accumulator_raw(&mut states[i], acc, &raw);
            }
            return;
        }

        // New group — materialize key Value (happens only a few times)
        let key_val = match &self.key {
            GroupKey::Null => Value::Null,
            GroupKey::Single(expr) => match expr {
                Expression::FieldRef(path) => extract_raw_field(&raw, path)
                    .as_ref()
                    .map(raw_owned_to_value)
                    .unwrap_or(Value::Null),
                Expression::Literal(v) => v.clone(),
                _ => Value::Null,
            },
            GroupKey::Compound(fields) => {
                let mut map = Map::new();
                for (name, expr) in fields {
                    let v = match expr {
                        Expression::FieldRef(path) => extract_raw_field(&raw, path)
                            .as_ref()
                            .map(raw_owned_to_value)
                            .unwrap_or(Value::Null),
                        Expression::Literal(v) => v.clone(),
                        _ => Value::Null,
                    };
                    map.insert(name.clone(), v);
                }
                Value::Object(map)
            }
        };
        let mut initial: Vec<AccumulatorState> = self
            .accumulators
            .iter()
            .map(|(_, acc)| match acc {
                Accumulator::Sum(_) => AccumulatorState::Sum(0.0),
                Accumulator::Avg(_) => AccumulatorState::Avg {
                    sum: 0.0,
                    count: 0,
                },
                Accumulator::Min(_) => AccumulatorState::Min(None),
                Accumulator::Max(_) => AccumulatorState::Max(None),
                Accumulator::Count => AccumulatorState::Count(0),
                Accumulator::First(_) => AccumulatorState::First(None),
                Accumulator::Last(_) => AccumulatorState::Last(None),
                Accumulator::Push(_) => AccumulatorState::Push(Vec::new()),
                Accumulator::AddToSet(_) => AccumulatorState::AddToSet(Vec::new()),
            })
            .collect();
        for (i, (_, acc)) in self.accumulators.iter().enumerate() {
            update_accumulator_raw(&mut initial[i], acc, &raw);
        }
        self.insertion_order.push(key_hash.clone());
        self.groups.insert(key_hash, (key_val, initial));
    }

    /// Merge another StreamingGroup into this one (for combining parallel results).
    /// The `other` group should come from a later segment so that First/Last
    /// semantics are preserved (self = earlier, other = later).
    pub(crate) fn merge(&mut self, mut other: Self) {
        for key_hash in other.insertion_order {
            let (key_val, other_states) = other.groups.remove(&key_hash).unwrap();
            if let Some((_, self_states)) = self.groups.get_mut(&key_hash) {
                // Merge accumulator states pairwise
                for (s, o) in self_states.iter_mut().zip(other_states) {
                    merge_accumulator_state(s, o);
                }
            } else {
                // New group key from other — insert it
                self.insertion_order.push(key_hash.clone());
                self.groups.insert(key_hash, (key_val, other_states));
            }
        }
    }

    /// Finalize and return the grouped results.
    pub(crate) fn finalize(mut self) -> Vec<Value> {
        let mut results = Vec::with_capacity(self.insertion_order.len());
        for key_hash in &self.insertion_order {
            let (key_val, states) = self.groups.remove(key_hash).unwrap();
            let mut doc = Map::new();
            doc.insert("_id".to_string(), key_val);
            for ((name, _), state) in self.accumulators.iter().zip(states) {
                doc.insert(name.clone(), finalize_accumulator(state));
            }
            results.push(Value::Object(doc));
        }
        results
    }
}

// ---------------------------------------------------------------------------
// Pipeline parsing & execution
// ---------------------------------------------------------------------------

impl Pipeline {
    pub fn parse(pipeline_json: &Value) -> Result<Self> {
        let arr = pipeline_json
            .as_array()
            .ok_or_else(|| Error::InvalidPipeline("pipeline must be an array".into()))?;

        let mut stages = Vec::new();
        for stage_val in arr {
            let obj = stage_val.as_object().ok_or_else(|| {
                Error::InvalidPipeline("each pipeline stage must be an object".into())
            })?;
            if obj.len() != 1 {
                return Err(Error::InvalidPipeline(
                    "each pipeline stage must have exactly one key".into(),
                ));
            }
            let (stage_name, stage_body) = obj.iter().next().unwrap();

            let stage = match stage_name.as_str() {
                "$match" => Stage::Match(stage_body.clone()),
                "$group" => parse_group_stage(stage_body)?,
                "$sort" => Stage::Sort(parse_sort(stage_body)?),
                "$skip" => {
                    let n = stage_body.as_u64().ok_or_else(|| {
                        Error::InvalidPipeline("$skip must be a non-negative integer".into())
                    })?;
                    Stage::Skip(n)
                }
                "$limit" => {
                    let n = stage_body.as_u64().ok_or_else(|| {
                        Error::InvalidPipeline("$limit must be a positive integer".into())
                    })?;
                    Stage::Limit(n)
                }
                "$project" => Stage::Project(parse_project(stage_body)?),
                "$count" => {
                    let field = stage_body.as_str().ok_or_else(|| {
                        Error::InvalidPipeline("$count must be a string".into())
                    })?;
                    Stage::Count(field.to_string())
                }
                "$unwind" => {
                    let (path, preserve) = parse_unwind(stage_body)?;
                    Stage::Unwind {
                        path,
                        preserve_null: preserve,
                    }
                }
                "$addFields" => {
                    let obj = stage_body.as_object().ok_or_else(|| {
                        Error::InvalidPipeline("$addFields must be an object".into())
                    })?;
                    let fields: Result<Vec<_>> = obj
                        .iter()
                        .map(|(k, v)| Ok((k.clone(), parse_expression(v)?)))
                        .collect();
                    Stage::AddFields(fields?)
                }
                "$lookup" => {
                    let obj = stage_body.as_object().ok_or_else(|| {
                        Error::InvalidPipeline("$lookup must be an object".into())
                    })?;
                    let from = obj
                        .get("from")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            Error::InvalidPipeline("$lookup requires 'from' string".into())
                        })?;
                    let local_field = obj
                        .get("localField")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            Error::InvalidPipeline("$lookup requires 'localField' string".into())
                        })?;
                    let foreign_field = obj
                        .get("foreignField")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            Error::InvalidPipeline("$lookup requires 'foreignField' string".into())
                        })?;
                    let as_field = obj
                        .get("as")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| {
                            Error::InvalidPipeline("$lookup requires 'as' string".into())
                        })?;
                    // Optional extra field pairs for composite joins
                    let extra_pairs = match (
                        obj.get("localFields").and_then(|v| v.as_array()),
                        obj.get("foreignFields").and_then(|v| v.as_array()),
                    ) {
                        (Some(lfs), Some(ffs)) => lfs
                            .iter()
                            .zip(ffs.iter())
                            .filter_map(|(l, f)| {
                                Some((l.as_str()?.to_string(), f.as_str()?.to_string()))
                            })
                            .collect(),
                        _ => Vec::new(),
                    };
                    Stage::Lookup {
                        from: from.to_string(),
                        local_field: local_field.to_string(),
                        foreign_field: foreign_field.to_string(),
                        as_field: as_field.to_string(),
                        extra_pairs,
                    }
                }
                "$out" => {
                    let coll = stage_body.as_str().ok_or_else(|| {
                        Error::InvalidPipeline("$out must be a collection name string".into())
                    })?;
                    Stage::Out(coll.to_string())
                }
                _ => {
                    return Err(Error::InvalidPipeline(format!(
                        "unknown stage: {}",
                        stage_name
                    )))
                }
            };
            stages.push(stage);
        }

        Ok(Pipeline { stages })
    }

    /// If the first stage is $match, return its query value and the index to
    /// start execution from (1). Otherwise return (None, 0).
    pub fn take_leading_match(&self) -> (Option<&Value>, usize) {
        if let Some(Stage::Match(val)) = self.stages.first() {
            (Some(val), 1)
        } else {
            (None, 0)
        }
    }

    /// Check whether the stage at `idx` is a $group.
    pub fn is_group_at(&self, idx: usize) -> bool {
        matches!(self.stages.get(idx), Some(Stage::Group { .. }))
    }

    /// Detect if the pipeline (from `start`) begins with a `$group` stage,
    /// and return references to its key, accumulators, and the index of the
    /// next stage after `$group`.  Used by the streaming aggregation path.
    pub(crate) fn try_streaming_group(
        &self,
        start: usize,
    ) -> Option<(&GroupKey, &[(String, Accumulator)], usize)> {
        match self.stages.get(start) {
            Some(Stage::Group { key, accumulators }) => {
                Some((key, accumulators, start + 1))
            }
            _ => None,
        }
    }

    /// Execute pipeline stages from Arc-based input (avoids Value::clone on
    /// initial docs). Stages that only read ($match, $group, $sort, $skip,
    /// $limit, $count) work directly on Arc references. When a mutating stage
    /// is encountered, the remaining Arcs are converted to owned Values.
    pub fn execute_from_arcs<F>(
        &self,
        start: usize,
        mut docs: Vec<Arc<Value>>,
        lookup_fn: &F,
        field_indexes: Option<&HashMap<String, PagedFieldIndex>>,
        doc_lookup: Option<&dyn Fn(DocumentId) -> Option<Arc<Value>>>,
    ) -> Result<Vec<Value>>
    where
        F: Fn(&str, &Value) -> Result<Vec<Value>>,
    {
        for (i, stage) in self.stages[start..].iter().enumerate() {
            match stage {
                Stage::Match(val) => {
                    let query = query::parse_query(val)?;
                    docs.retain(|doc| query::matches_value(&query, doc));
                }
                Stage::Group { key, accumulators } => {
                    // Try index-accelerated group path
                    if let Some(result) = try_index_group(
                        key,
                        accumulators,
                        &docs,
                        field_indexes,
                        doc_lookup,
                    )? {
                        return self.execute_from(start + i + 1, result, lookup_fn);
                    }
                    // $group reads by reference → produces small owned Vec<Value>
                    let result = exec_group(&docs, key, accumulators)?;
                    // Continue with owned pipeline for remaining stages
                    return self.execute_from(start + i + 1, result, lookup_fn);
                }
                Stage::Sort(fields) => {
                    docs.sort_by(|a, b| {
                        for (field, order) in fields {
                            let av = resolve_field_ref(a, field);
                            let bv = resolve_field_ref(b, field);
                            let aiv = av.map(IndexValue::from_json).unwrap_or(IndexValue::Null);
                            let biv = bv.map(IndexValue::from_json).unwrap_or(IndexValue::Null);
                            let cmp = aiv.cmp(&biv);
                            let cmp = match order {
                                SortOrder::Asc => cmp,
                                SortOrder::Desc => cmp.reverse(),
                            };
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
                Stage::Skip(n) => {
                    docs = docs.into_iter().skip(*n as usize).collect();
                }
                Stage::Limit(n) => {
                    docs.truncate(*n as usize);
                }
                Stage::Count(field) => {
                    let count = docs.len();
                    return self.execute_from(
                        start + i + 1,
                        vec![json!({ field.as_str(): count })],
                        lookup_fn,
                    );
                }
                // Stages that need mutation: convert to owned and delegate
                _ => {
                    let owned: Vec<Value> = docs.into_iter().map(|arc| (*arc).clone()).collect();
                    return self.execute_from(start + i, owned, lookup_fn);
                }
            }
        }
        // All stages processed on arcs — convert final result to owned
        Ok(docs.into_iter().map(|arc| (*arc).clone()).collect())
    }

    /// Execute pipeline stages starting from `start` index.
    pub fn execute_from<F>(
        &self,
        start: usize,
        docs: Vec<Value>,
        lookup_fn: &F,
    ) -> Result<Vec<Value>>
    where
        F: Fn(&str, &Value) -> Result<Vec<Value>>,
    {
        let mut current = docs;
        for stage in &self.stages[start..] {
            current = match stage {
                Stage::Match(val) => exec_match(current, val)?,
                Stage::Group { key, accumulators } => exec_group(&current, key, accumulators)?,
                Stage::Sort(fields) => exec_sort(current, fields),
                Stage::Skip(n) => exec_skip(current, *n),
                Stage::Limit(n) => exec_limit(current, *n),
                Stage::Project(fields) => exec_project(current, fields),
                Stage::Count(field) => exec_count(current, field),
                Stage::Unwind {
                    path,
                    preserve_null,
                } => exec_unwind(current, path, *preserve_null),
                Stage::AddFields(fields) => exec_add_fields(current, fields),
                Stage::Lookup {
                    from,
                    local_field,
                    foreign_field,
                    as_field,
                    extra_pairs,
                } => exec_lookup(current, from, local_field, foreign_field, as_field, extra_pairs, lookup_fn)?,
                Stage::Out(_) => {
                    // $out is handled at the engine level after pipeline execution.
                    // The pipeline returns the docs; the engine writes them to the target collection.
                    current
                }
            };
        }
        Ok(current)
    }

    /// If the last stage is $out, return the target collection name.
    pub fn out_collection(&self) -> Option<&str> {
        match self.stages.last() {
            Some(Stage::Out(coll)) => Some(coll),
            _ => None,
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Helper: no-op lookup function for tests that don't use $lookup
    fn no_lookup(_col: &str, _q: &Value) -> Result<Vec<Value>> {
        Ok(vec![])
    }

    // -----------------------------------------------------------------------
    // Expression tests
    // -----------------------------------------------------------------------

    #[test]
    fn expr_field_ref() {
        let doc = json!({"name": "Alice", "age": 30});
        let expr = parse_expression(&json!("$name")).unwrap();
        assert_eq!(expr.eval(&doc), json!("Alice"));
    }

    #[test]
    fn expr_nested_dot_notation() {
        let doc = json!({"user": {"address": {"city": "NYC"}}});
        let expr = parse_expression(&json!("$user.address.city")).unwrap();
        assert_eq!(expr.eval(&doc), json!("NYC"));
    }

    #[test]
    fn expr_missing_field_returns_null() {
        let doc = json!({"name": "Alice"});
        let expr = parse_expression(&json!("$missing")).unwrap();
        assert_eq!(expr.eval(&doc), Value::Null);
    }

    #[test]
    fn expr_literal() {
        let doc = json!({});
        let expr = parse_expression(&json!(42)).unwrap();
        assert_eq!(expr.eval(&doc), json!(42));
    }

    #[test]
    fn expr_add() {
        let doc = json!({"a": 10, "b": 20});
        let expr = parse_expression(&json!({"$add": ["$a", "$b"]})).unwrap();
        assert_eq!(expr.eval(&doc), json!(30));
    }

    #[test]
    fn expr_subtract() {
        let doc = json!({"a": 50, "b": 20});
        let expr = parse_expression(&json!({"$subtract": ["$a", "$b"]})).unwrap();
        assert_eq!(expr.eval(&doc), json!(30));
    }

    #[test]
    fn expr_multiply() {
        let doc = json!({"a": 5, "b": 6});
        let expr = parse_expression(&json!({"$multiply": ["$a", "$b"]})).unwrap();
        assert_eq!(expr.eval(&doc), json!(30));
    }

    #[test]
    fn expr_divide() {
        let doc = json!({"a": 100, "b": 4});
        let expr = parse_expression(&json!({"$divide": ["$a", "$b"]})).unwrap();
        assert_eq!(expr.eval(&doc), json!(25));
    }

    #[test]
    fn expr_divide_by_zero_returns_null() {
        let doc = json!({"a": 100, "b": 0});
        let expr = parse_expression(&json!({"$divide": ["$a", "$b"]})).unwrap();
        assert_eq!(expr.eval(&doc), Value::Null);
    }

    #[test]
    fn expr_arithmetic_with_null_returns_null() {
        let doc = json!({"a": 10});
        let expr = parse_expression(&json!({"$add": ["$a", "$missing"]})).unwrap();
        assert_eq!(expr.eval(&doc), Value::Null);
    }

    // -----------------------------------------------------------------------
    // $match tests
    // -----------------------------------------------------------------------

    #[test]
    fn match_filters_docs() {
        let docs = vec![
            json!({"status": "active", "name": "Alice"}),
            json!({"status": "inactive", "name": "Bob"}),
            json!({"status": "active", "name": "Charlie"}),
        ];
        let result = exec_match(docs, &json!({"status": "active"})).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0]["name"], "Alice");
        assert_eq!(result[1]["name"], "Charlie");
    }

    #[test]
    fn match_with_operators() {
        let docs = vec![
            json!({"age": 15}),
            json!({"age": 25}),
            json!({"age": 35}),
        ];
        let result = exec_match(docs, &json!({"age": {"$gte": 20}})).unwrap();
        assert_eq!(result.len(), 2);
    }

    // -----------------------------------------------------------------------
    // $group tests
    // -----------------------------------------------------------------------

    #[test]
    fn group_sum() {
        let docs = vec![
            json!({"category": "A", "amount": 10}),
            json!({"category": "B", "amount": 20}),
            json!({"category": "A", "amount": 30}),
        ];
        let stage = parse_group_stage(&json!({
            "_id": "$category",
            "total": {"$sum": "$amount"}
        }))
        .unwrap();

        if let Stage::Group { key, accumulators } = &stage {
            let result = exec_group(&docs, key, accumulators).unwrap();
            assert_eq!(result.len(), 2);

            let a = result.iter().find(|d| d["_id"] == "A").unwrap();
            assert_eq!(a["total"], json!(40));
            let b = result.iter().find(|d| d["_id"] == "B").unwrap();
            assert_eq!(b["total"], json!(20));
        } else {
            panic!("expected Group stage");
        }
    }

    #[test]
    fn group_avg() {
        let docs = vec![
            json!({"score": 10}),
            json!({"score": 20}),
            json!({"score": 30}),
        ];
        let stage = parse_group_stage(&json!({
            "_id": null,
            "avg_score": {"$avg": "$score"}
        }))
        .unwrap();

        if let Stage::Group { key, accumulators } = &stage {
            let result = exec_group(&docs, key, accumulators).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0]["avg_score"], json!(20));
        }
    }

    #[test]
    fn group_min_max() {
        let docs = vec![
            json!({"v": 5}),
            json!({"v": 1}),
            json!({"v": 9}),
        ];
        let stage = parse_group_stage(&json!({
            "_id": null,
            "min_v": {"$min": "$v"},
            "max_v": {"$max": "$v"}
        }))
        .unwrap();

        if let Stage::Group { key, accumulators } = &stage {
            let result = exec_group(&docs, key, accumulators).unwrap();
            assert_eq!(result[0]["min_v"], json!(1));
            assert_eq!(result[0]["max_v"], json!(9));
        }
    }

    #[test]
    fn group_count_accumulator() {
        let docs = vec![
            json!({"status": "active"}),
            json!({"status": "active"}),
            json!({"status": "inactive"}),
        ];
        let stage = parse_group_stage(&json!({
            "_id": "$status",
            "n": {"$count": {}}
        }))
        .unwrap();

        if let Stage::Group { key, accumulators } = &stage {
            let result = exec_group(&docs, key, accumulators).unwrap();
            let active = result.iter().find(|d| d["_id"] == "active").unwrap();
            assert_eq!(active["n"], json!(2));
        }
    }

    #[test]
    fn group_first_last() {
        let docs = vec![
            json!({"g": "X", "val": "first"}),
            json!({"g": "X", "val": "middle"}),
            json!({"g": "X", "val": "last"}),
        ];
        let stage = parse_group_stage(&json!({
            "_id": "$g",
            "f": {"$first": "$val"},
            "l": {"$last": "$val"}
        }))
        .unwrap();

        if let Stage::Group { key, accumulators } = &stage {
            let result = exec_group(&docs, key, accumulators).unwrap();
            assert_eq!(result[0]["f"], json!("first"));
            assert_eq!(result[0]["l"], json!("last"));
        }
    }

    #[test]
    fn group_push() {
        let docs = vec![
            json!({"g": "X", "v": 1}),
            json!({"g": "X", "v": 2}),
            json!({"g": "Y", "v": 3}),
        ];
        let stage = parse_group_stage(&json!({
            "_id": "$g",
            "values": {"$push": "$v"}
        }))
        .unwrap();

        if let Stage::Group { key, accumulators } = &stage {
            let result = exec_group(&docs, key, accumulators).unwrap();
            let x = result.iter().find(|d| d["_id"] == "X").unwrap();
            assert_eq!(x["values"], json!([1, 2]));
            let y = result.iter().find(|d| d["_id"] == "Y").unwrap();
            assert_eq!(y["values"], json!([3]));
        }
    }

    #[test]
    fn group_null_key() {
        let docs = vec![json!({"v": 1}), json!({"v": 2}), json!({"v": 3})];
        let stage = parse_group_stage(&json!({
            "_id": null,
            "total": {"$sum": "$v"}
        }))
        .unwrap();

        if let Stage::Group { key, accumulators } = &stage {
            let result = exec_group(&docs, key, accumulators).unwrap();
            assert_eq!(result.len(), 1);
            assert_eq!(result[0]["_id"], Value::Null);
            assert_eq!(result[0]["total"], json!(6));
        }
    }

    #[test]
    fn group_compound_key() {
        let docs = vec![
            json!({"year": 2024, "month": 1, "sales": 10}),
            json!({"year": 2024, "month": 1, "sales": 20}),
            json!({"year": 2024, "month": 2, "sales": 30}),
        ];
        let stage = parse_group_stage(&json!({
            "_id": {"year": "$year", "month": "$month"},
            "total": {"$sum": "$sales"}
        }))
        .unwrap();

        if let Stage::Group { key, accumulators } = &stage {
            let result = exec_group(&docs, key, accumulators).unwrap();
            assert_eq!(result.len(), 2);
        }
    }

    #[test]
    fn group_sum_with_literal() {
        // { "$sum": 1 } is a common pattern for counting
        let docs = vec![
            json!({"cat": "A"}),
            json!({"cat": "A"}),
            json!({"cat": "B"}),
        ];
        let stage = parse_group_stage(&json!({
            "_id": "$cat",
            "count": {"$sum": 1}
        }))
        .unwrap();

        if let Stage::Group { key, accumulators } = &stage {
            let result = exec_group(&docs, key, accumulators).unwrap();
            let a = result.iter().find(|d| d["_id"] == "A").unwrap();
            assert_eq!(a["count"], json!(2));
        }
    }

    // -----------------------------------------------------------------------
    // $sort tests
    // -----------------------------------------------------------------------

    #[test]
    fn sort_asc() {
        let docs = vec![json!({"n": 3}), json!({"n": 1}), json!({"n": 2})];
        let result = exec_sort(docs, &[("n".into(), SortOrder::Asc)]);
        assert_eq!(result[0]["n"], 1);
        assert_eq!(result[1]["n"], 2);
        assert_eq!(result[2]["n"], 3);
    }

    #[test]
    fn sort_desc() {
        let docs = vec![json!({"n": 1}), json!({"n": 3}), json!({"n": 2})];
        let result = exec_sort(docs, &[("n".into(), SortOrder::Desc)]);
        assert_eq!(result[0]["n"], 3);
        assert_eq!(result[1]["n"], 2);
        assert_eq!(result[2]["n"], 1);
    }

    #[test]
    fn sort_type_aware() {
        // Numbers come before strings in IndexValue ordering
        let docs = vec![
            json!({"v": "hello"}),
            json!({"v": 42}),
            json!({"v": null}),
        ];
        let result = exec_sort(docs, &[("v".into(), SortOrder::Asc)]);
        assert_eq!(result[0]["v"], Value::Null);
        assert_eq!(result[1]["v"], 42);
        assert_eq!(result[2]["v"], "hello");
    }

    // -----------------------------------------------------------------------
    // $skip / $limit tests
    // -----------------------------------------------------------------------

    #[test]
    fn skip_and_limit() {
        let docs: Vec<Value> = (0..10).map(|i| json!({"n": i})).collect();
        let result = exec_limit(exec_skip(docs, 3), 4);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0]["n"], 3);
        assert_eq!(result[3]["n"], 6);
    }

    #[test]
    fn skip_past_end() {
        let docs = vec![json!({"n": 1}), json!({"n": 2})];
        let result = exec_skip(docs, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn limit_zero() {
        let docs = vec![json!({"n": 1}), json!({"n": 2})];
        let result = exec_limit(docs, 0);
        assert!(result.is_empty());
    }

    // -----------------------------------------------------------------------
    // $project tests
    // -----------------------------------------------------------------------

    #[test]
    fn project_include() {
        let docs = vec![json!({"_id": 1, "name": "Alice", "age": 30, "email": "a@b.com"})];
        let fields = vec![
            ("name".into(), ProjectionField::Include),
            ("age".into(), ProjectionField::Include),
        ];
        let result = exec_project(docs, &fields);
        assert_eq!(result[0], json!({"_id": 1, "name": "Alice", "age": 30}));
    }

    #[test]
    fn project_exclude() {
        let docs = vec![json!({"_id": 1, "name": "Alice", "age": 30, "email": "a@b.com"})];
        let fields = vec![("email".into(), ProjectionField::Exclude)];
        let result = exec_project(docs, &fields);
        assert_eq!(result[0], json!({"_id": 1, "name": "Alice", "age": 30}));
    }

    #[test]
    fn project_exclude_id() {
        let docs = vec![json!({"_id": 1, "name": "Alice"})];
        let fields = vec![
            ("_id".into(), ProjectionField::Exclude),
            ("name".into(), ProjectionField::Include),
        ];
        let result = exec_project(docs, &fields);
        assert_eq!(result[0], json!({"name": "Alice"}));
    }

    #[test]
    fn project_computed() {
        let docs = vec![json!({"price": 100, "tax": 10})];
        let expr = parse_expression(&json!({"$add": ["$price", "$tax"]})).unwrap();
        let fields = vec![("total".into(), ProjectionField::Compute(expr))];
        let result = exec_project(docs, &fields);
        assert_eq!(result[0]["total"], json!(110));
    }

    // -----------------------------------------------------------------------
    // $count tests
    // -----------------------------------------------------------------------

    #[test]
    fn count_produces_single_doc() {
        let docs = vec![json!({"a": 1}), json!({"a": 2}), json!({"a": 3})];
        let result = exec_count(docs, "total");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["total"], 3);
    }

    #[test]
    fn count_empty_input() {
        let result = exec_count(vec![], "total");
        assert_eq!(result[0]["total"], 0);
    }

    // -----------------------------------------------------------------------
    // $unwind tests
    // -----------------------------------------------------------------------

    #[test]
    fn unwind_array() {
        let docs = vec![json!({"name": "Alice", "tags": ["a", "b", "c"]})];
        let result = exec_unwind(docs, "tags", false);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0]["tags"], "a");
        assert_eq!(result[1]["tags"], "b");
        assert_eq!(result[2]["tags"], "c");
        // Other fields preserved
        assert_eq!(result[0]["name"], "Alice");
    }

    #[test]
    fn unwind_empty_array_dropped() {
        let docs = vec![json!({"name": "Alice", "tags": []})];
        let result = exec_unwind(docs, "tags", false);
        assert!(result.is_empty());
    }

    #[test]
    fn unwind_empty_array_preserved() {
        let docs = vec![json!({"name": "Alice", "tags": []})];
        let result = exec_unwind(docs, "tags", true);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn unwind_null_dropped() {
        let docs = vec![json!({"name": "Alice", "tags": null})];
        let result = exec_unwind(docs, "tags", false);
        assert!(result.is_empty());
    }

    #[test]
    fn unwind_null_preserved() {
        let docs = vec![json!({"name": "Alice", "tags": null})];
        let result = exec_unwind(docs, "tags", true);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn unwind_non_array_passthrough() {
        let docs = vec![json!({"name": "Alice", "tags": "single"})];
        let result = exec_unwind(docs, "tags", false);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["tags"], "single");
    }

    // -----------------------------------------------------------------------
    // $addFields tests
    // -----------------------------------------------------------------------

    #[test]
    fn add_fields_preserves_existing() {
        let docs = vec![json!({"name": "Alice", "a": 10, "b": 20})];
        let fields = vec![(
            "total".into(),
            parse_expression(&json!({"$add": ["$a", "$b"]})).unwrap(),
        )];
        let result = exec_add_fields(docs, &fields);
        assert_eq!(result[0]["name"], "Alice");
        assert_eq!(result[0]["total"], json!(30));
    }

    #[test]
    fn add_fields_overwrites() {
        let docs = vec![json!({"name": "Alice", "status": "old"})];
        let fields = vec![("status".into(), parse_expression(&json!("new")).unwrap())];
        let result = exec_add_fields(docs, &fields);
        assert_eq!(result[0]["status"], "new");
    }

    // -----------------------------------------------------------------------
    // $lookup tests
    // -----------------------------------------------------------------------

    #[test]
    fn lookup_with_mock() {
        let docs = vec![
            json!({"_id": 1, "item": "abc"}),
            json!({"_id": 2, "item": "xyz"}),
        ];
        let mock_lookup = |_col: &str, query: &Value| -> Result<Vec<Value>> {
            let item = query.get("sku").and_then(|v| v.as_str()).unwrap_or("");
            match item {
                "abc" => Ok(vec![json!({"sku": "abc", "qty": 100})]),
                "xyz" => Ok(vec![
                    json!({"sku": "xyz", "qty": 50}),
                    json!({"sku": "xyz", "qty": 25}),
                ]),
                _ => Ok(vec![]),
            }
        };

        let result =
            exec_lookup(docs, "inventory", "item", "sku", "matched", &[], &mock_lookup).unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0]["matched"].as_array().unwrap().len(), 1);
        assert_eq!(result[1]["matched"].as_array().unwrap().len(), 2);
    }

    // -----------------------------------------------------------------------
    // Pipeline parsing tests
    // -----------------------------------------------------------------------

    #[test]
    fn parse_empty_pipeline() {
        let p = Pipeline::parse(&json!([])).unwrap();
        assert!(p.stages.is_empty());
    }

    #[test]
    fn parse_unknown_stage_errors() {
        let result = Pipeline::parse(&json!([{"$unknown": {}}]));
        assert!(result.is_err());
    }

    #[test]
    fn parse_invalid_accumulator_errors() {
        let result = Pipeline::parse(&json!([
            {"$group": {"_id": null, "x": {"$badacc": "$v"}}}
        ]));
        assert!(result.is_err());
    }

    #[test]
    fn parse_missing_group_id_errors() {
        let result = Pipeline::parse(&json!([
            {"$group": {"total": {"$sum": "$v"}}}
        ]));
        assert!(result.is_err());
    }

    #[test]
    fn parse_pipeline_not_array_errors() {
        let result = Pipeline::parse(&json!({"$match": {}}));
        assert!(result.is_err());
    }

    #[test]
    fn parse_stage_not_object_errors() {
        let result = Pipeline::parse(&json!(["not an object"]));
        assert!(result.is_err());
    }

    #[test]
    fn parse_stage_multiple_keys_errors() {
        let result = Pipeline::parse(&json!([{"$match": {}, "$sort": {"a": 1}}]));
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Multi-stage pipeline tests
    // -----------------------------------------------------------------------

    #[test]
    fn pipeline_match_group_sort_limit() {
        let pipeline = Pipeline::parse(&json!([
            {"$match": {"status": "completed"}},
            {"$group": {"_id": "$category", "total": {"$sum": "$amount"}}},
            {"$sort": {"total": -1}},
            {"$limit": 2}
        ]))
        .unwrap();

        let docs = vec![
            json!({"status": "completed", "category": "A", "amount": 100}),
            json!({"status": "pending", "category": "A", "amount": 50}),
            json!({"status": "completed", "category": "B", "amount": 200}),
            json!({"status": "completed", "category": "A", "amount": 150}),
            json!({"status": "completed", "category": "C", "amount": 50}),
        ];

        let result = pipeline.execute_from(0, docs, &no_lookup).unwrap();
        assert_eq!(result.len(), 2);
        // B: 200, A: 250 -> sorted desc -> A(250), B(200)
        assert_eq!(result[0]["_id"], "A");
        assert_eq!(result[0]["total"], json!(250));
        assert_eq!(result[1]["_id"], "B");
        assert_eq!(result[1]["total"], json!(200));
    }

    #[test]
    fn pipeline_unwind_group() {
        let pipeline = Pipeline::parse(&json!([
            {"$unwind": "$tags"},
            {"$group": {"_id": "$tags", "count": {"$sum": 1}}}
        ]))
        .unwrap();

        let docs = vec![
            json!({"tags": ["rust", "db"]}),
            json!({"tags": ["rust", "fast"]}),
            json!({"tags": ["db"]}),
        ];

        let result = pipeline.execute_from(0, docs, &no_lookup).unwrap();
        let rust = result.iter().find(|d| d["_id"] == "rust").unwrap();
        assert_eq!(rust["count"], json!(2));
        let db = result.iter().find(|d| d["_id"] == "db").unwrap();
        assert_eq!(db["count"], json!(2));
        let fast = result.iter().find(|d| d["_id"] == "fast").unwrap();
        assert_eq!(fast["count"], json!(1));
    }

    #[test]
    fn pipeline_empty_input() {
        let pipeline = Pipeline::parse(&json!([
            {"$match": {"status": "active"}},
            {"$group": {"_id": null, "total": {"$sum": "$v"}}}
        ]))
        .unwrap();

        let result = pipeline.execute_from(0, vec![], &no_lookup).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn pipeline_empty_stages() {
        let pipeline = Pipeline::parse(&json!([])).unwrap();
        let docs = vec![json!({"a": 1}), json!({"a": 2})];
        let result = pipeline.execute_from(0, docs.clone(), &no_lookup).unwrap();
        assert_eq!(result, docs);
    }

    #[test]
    fn pipeline_leading_match_optimization() {
        let pipeline = Pipeline::parse(&json!([
            {"$match": {"status": "active"}},
            {"$sort": {"name": 1}}
        ]))
        .unwrap();

        let (leading, start) = pipeline.take_leading_match();
        assert!(leading.is_some());
        assert_eq!(start, 1);
        assert_eq!(leading.unwrap(), &json!({"status": "active"}));
    }

    #[test]
    fn pipeline_no_leading_match() {
        let pipeline = Pipeline::parse(&json!([
            {"$sort": {"name": 1}}
        ]))
        .unwrap();

        let (leading, start) = pipeline.take_leading_match();
        assert!(leading.is_none());
        assert_eq!(start, 0);
    }

    #[test]
    fn pipeline_add_fields_then_project() {
        let pipeline = Pipeline::parse(&json!([
            {"$addFields": {"total": {"$add": ["$a", "$b"]}}},
            {"$project": {"total": 1, "_id": 0}}
        ]))
        .unwrap();

        let docs = vec![json!({"a": 10, "b": 20}), json!({"a": 3, "b": 7})];
        let result = pipeline.execute_from(0, docs, &no_lookup).unwrap();
        assert_eq!(result[0], json!({"total": 30}));
        assert_eq!(result[1], json!({"total": 10}));
    }

    #[test]
    fn pipeline_count_stage() {
        let pipeline = Pipeline::parse(&json!([
            {"$match": {"active": true}},
            {"$count": "total"}
        ]))
        .unwrap();

        let docs = vec![
            json!({"active": true}),
            json!({"active": false}),
            json!({"active": true}),
        ];

        let result = pipeline.execute_from(0, docs, &no_lookup).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0]["total"], 2);
    }

    #[test]
    fn pipeline_skip_limit() {
        let pipeline = Pipeline::parse(&json!([
            {"$sort": {"n": 1}},
            {"$skip": 2},
            {"$limit": 3}
        ]))
        .unwrap();

        let docs: Vec<Value> = (0..10).map(|i| json!({"n": i})).collect();
        let result = pipeline.execute_from(0, docs, &no_lookup).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0]["n"], 2);
        assert_eq!(result[1]["n"], 3);
        assert_eq!(result[2]["n"], 4);
    }

    #[test]
    fn pipeline_unwind_with_preserve() {
        let pipeline = Pipeline::parse(&json!([
            {"$unwind": {"path": "$tags", "preserveNullAndEmptyArrays": true}}
        ]))
        .unwrap();

        let docs = vec![
            json!({"name": "A", "tags": ["x", "y"]}),
            json!({"name": "B", "tags": []}),
            json!({"name": "C", "tags": null}),
            json!({"name": "D"}),
        ];

        let result = pipeline.execute_from(0, docs, &no_lookup).unwrap();
        // A expands to 2, B preserved (empty), C preserved (null), D preserved (missing=null)
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn pipeline_lookup_integration() {
        let pipeline = Pipeline::parse(&json!([
            {"$lookup": {
                "from": "items",
                "localField": "item_id",
                "foreignField": "id",
                "as": "item_details"
            }}
        ]))
        .unwrap();

        let mock_lookup = |_col: &str, query: &Value| -> Result<Vec<Value>> {
            let id = query.get("id").and_then(|v| v.as_i64()).unwrap_or(0);
            if id == 1 {
                Ok(vec![json!({"id": 1, "name": "Widget"})])
            } else {
                Ok(vec![])
            }
        };

        let docs = vec![
            json!({"_id": 1, "item_id": 1}),
            json!({"_id": 2, "item_id": 99}),
        ];

        let result = pipeline.execute_from(0, docs, &mock_lookup).unwrap();
        assert_eq!(result[0]["item_details"].as_array().unwrap().len(), 1);
        assert_eq!(result[1]["item_details"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn feed_raw_avg_basic() {
        let key = GroupKey::Single(Expression::FieldRef("city".to_string()));
        let accs = vec![("avg_age".to_string(), Accumulator::Avg(Expression::FieldRef("age".to_string())))];
        assert!(is_raw_eligible(&key, &accs));

        let mut group = StreamingGroup::new(&key, &accs);

        let doc1 = json!({"name": "Alice", "age": 30, "city": "NYC"});
        let doc2 = json!({"name": "Bob", "age": 25, "city": "LA"});
        let doc3 = json!({"name": "Charlie", "age": 35, "city": "NYC"});

        let enc1 = crate::codec::encode_doc(&doc1).unwrap();
        let enc2 = crate::codec::encode_doc(&doc2).unwrap();
        let enc3 = crate::codec::encode_doc(&doc3).unwrap();

        group.feed_raw(&enc1);
        group.feed_raw(&enc2);
        group.feed_raw(&enc3);

        let results = group.finalize();
        eprintln!("results: {:?}", results);
        assert_eq!(results.len(), 2);

        for doc in &results {
            let city = doc["_id"].as_str().unwrap();
            let avg = doc["avg_age"].as_f64().unwrap();
            match city {
                "NYC" => assert!((avg - 32.5).abs() < 0.01, "NYC avg was {avg}"),
                "LA" => assert!((avg - 25.0).abs() < 0.01, "LA avg was {avg}"),
                other => panic!("unexpected city: {other}"),
            }
        }
    }

    // -----------------------------------------------------------------------
    // $addToSet accumulator
    // -----------------------------------------------------------------------

    #[test]
    fn group_add_to_set() {
        let docs = vec![
            json!({"dept": "eng", "lang": "Rust"}),
            json!({"dept": "eng", "lang": "Go"}),
            json!({"dept": "eng", "lang": "Rust"}), // duplicate
            json!({"dept": "sales", "lang": "Python"}),
        ];
        let pipeline = Pipeline::parse(&json!([
            {"$group": {"_id": "$dept", "languages": {"$addToSet": "$lang"}}}
        ])).unwrap();
        let results = pipeline.execute_from(0, docs, &no_lookup).unwrap();
        assert_eq!(results.len(), 2);
        for doc in &results {
            let langs = doc["languages"].as_array().unwrap();
            if doc["_id"] == "eng" {
                assert_eq!(langs.len(), 2); // Rust and Go, no duplicates
                assert!(langs.contains(&json!("Rust")));
                assert!(langs.contains(&json!("Go")));
            } else {
                assert_eq!(langs.len(), 1);
                assert!(langs.contains(&json!("Python")));
            }
        }
    }

    // -----------------------------------------------------------------------
    // $cond and $ifNull
    // -----------------------------------------------------------------------

    #[test]
    fn expr_cond_array_form() {
        let doc = json!({"score": 85});
        let expr = parse_expression(&json!({"$cond": ["$score", "pass", "fail"]})).unwrap();
        assert_eq!(expr.eval(&doc), json!("pass"));
    }

    #[test]
    fn expr_cond_object_form() {
        let doc = json!({"active": false});
        let expr = parse_expression(&json!({
            "$cond": {"if": "$active", "then": "yes", "else": "no"}
        })).unwrap();
        assert_eq!(expr.eval(&doc), json!("no"));
    }

    #[test]
    fn expr_cond_null_is_falsy() {
        let doc = json!({"val": null});
        let expr = parse_expression(&json!({"$cond": ["$val", "has", "empty"]})).unwrap();
        assert_eq!(expr.eval(&doc), json!("empty"));
    }

    #[test]
    fn expr_ifnull_returns_value_when_present() {
        let doc = json!({"name": "Alice"});
        let expr = parse_expression(&json!({"$ifNull": ["$name", "Unknown"]})).unwrap();
        assert_eq!(expr.eval(&doc), json!("Alice"));
    }

    #[test]
    fn expr_ifnull_returns_replacement_when_null() {
        let doc = json!({"name": null});
        let expr = parse_expression(&json!({"$ifNull": ["$name", "Unknown"]})).unwrap();
        assert_eq!(expr.eval(&doc), json!("Unknown"));
    }

    #[test]
    fn expr_ifnull_returns_replacement_when_missing() {
        let doc = json!({"age": 30});
        let expr = parse_expression(&json!({"$ifNull": ["$name", "N/A"]})).unwrap();
        assert_eq!(expr.eval(&doc), json!("N/A"));
    }

    // -----------------------------------------------------------------------
    // String operators
    // -----------------------------------------------------------------------

    #[test]
    fn expr_concat() {
        let doc = json!({"first": "John", "last": "Doe"});
        let expr = parse_expression(&json!({"$concat": ["$first", " ", "$last"]})).unwrap();
        assert_eq!(expr.eval(&doc), json!("John Doe"));
    }

    #[test]
    fn expr_concat_null_returns_null() {
        let doc = json!({"first": "John"});
        let expr = parse_expression(&json!({"$concat": ["$first", " ", "$missing"]})).unwrap();
        assert_eq!(expr.eval(&doc), Value::Null);
    }

    #[test]
    fn expr_to_lower() {
        let doc = json!({"name": "ALICE"});
        let expr = parse_expression(&json!({"$toLower": "$name"})).unwrap();
        assert_eq!(expr.eval(&doc), json!("alice"));
    }

    #[test]
    fn expr_to_upper() {
        let doc = json!({"name": "alice"});
        let expr = parse_expression(&json!({"$toUpper": "$name"})).unwrap();
        assert_eq!(expr.eval(&doc), json!("ALICE"));
    }

    #[test]
    fn expr_substr() {
        let doc = json!({"s": "hello world"});
        let expr = parse_expression(&json!({"$substr": ["$s", 0, 5]})).unwrap();
        assert_eq!(expr.eval(&doc), json!("hello"));
    }

    #[test]
    fn expr_trim() {
        let doc = json!({"s": "  hello  "});
        let expr = parse_expression(&json!({"$trim": {"input": "$s"}})).unwrap();
        assert_eq!(expr.eval(&doc), json!("hello"));
    }

    #[test]
    fn expr_split() {
        let doc = json!({"s": "a,b,c"});
        let expr = parse_expression(&json!({"$split": ["$s", ","]})).unwrap();
        assert_eq!(expr.eval(&doc), json!(["a", "b", "c"]));
    }

    // -----------------------------------------------------------------------
    // Date operators
    // -----------------------------------------------------------------------

    #[test]
    fn expr_year() {
        let doc = json!({"d": "2024-03-15T10:30:45Z"});
        let expr = parse_expression(&json!({"$year": "$d"})).unwrap();
        assert_eq!(expr.eval(&doc), json!(2024));
    }

    #[test]
    fn expr_month() {
        let doc = json!({"d": "2024-03-15T10:30:45Z"});
        let expr = parse_expression(&json!({"$month": "$d"})).unwrap();
        assert_eq!(expr.eval(&doc), json!(3));
    }

    #[test]
    fn expr_day_of_month() {
        let doc = json!({"d": "2024-03-15"});
        let expr = parse_expression(&json!({"$dayOfMonth": "$d"})).unwrap();
        assert_eq!(expr.eval(&doc), json!(15));
    }

    #[test]
    fn expr_hour_minute_second() {
        let doc = json!({"d": "2024-03-15T10:30:45Z"});
        let h = parse_expression(&json!({"$hour": "$d"})).unwrap();
        let m = parse_expression(&json!({"$minute": "$d"})).unwrap();
        let s = parse_expression(&json!({"$second": "$d"})).unwrap();
        assert_eq!(h.eval(&doc), json!(10));
        assert_eq!(m.eval(&doc), json!(30));
        assert_eq!(s.eval(&doc), json!(45));
    }

    #[test]
    fn expr_day_of_week() {
        // 2024-03-15 is a Friday → MongoDB: 6
        let doc = json!({"d": "2024-03-15"});
        let expr = parse_expression(&json!({"$dayOfWeek": "$d"})).unwrap();
        assert_eq!(expr.eval(&doc), json!(6));
    }

    #[test]
    fn expr_date_only_string() {
        let doc = json!({"d": "2024-01-01"});
        let y = parse_expression(&json!({"$year": "$d"})).unwrap();
        let m = parse_expression(&json!({"$month": "$d"})).unwrap();
        let d = parse_expression(&json!({"$dayOfMonth": "$d"})).unwrap();
        assert_eq!(y.eval(&doc), json!(2024));
        assert_eq!(m.eval(&doc), json!(1));
        assert_eq!(d.eval(&doc), json!(1));
    }

    #[test]
    fn expr_date_null_returns_null() {
        let doc = json!({"d": "not-a-date"});
        let expr = parse_expression(&json!({"$year": "$d"})).unwrap();
        assert_eq!(expr.eval(&doc), Value::Null);
    }

    // -----------------------------------------------------------------------
    // $mod and $size
    // -----------------------------------------------------------------------

    #[test]
    fn expr_mod() {
        let doc = json!({"a": 10});
        let expr = parse_expression(&json!({"$mod": ["$a", 3]})).unwrap();
        assert_eq!(expr.eval(&doc), json!(1));
    }

    #[test]
    fn expr_size() {
        let doc = json!({"tags": ["a", "b", "c"]});
        let expr = parse_expression(&json!({"$size": "$tags"})).unwrap();
        assert_eq!(expr.eval(&doc), json!(3));
    }

    #[test]
    fn expr_size_non_array_returns_null() {
        let doc = json!({"val": "string"});
        let expr = parse_expression(&json!({"$size": "$val"})).unwrap();
        assert_eq!(expr.eval(&doc), Value::Null);
    }

    // -----------------------------------------------------------------------
    // $out stage
    // -----------------------------------------------------------------------

    #[test]
    fn out_collection_parsed() {
        let pipeline = Pipeline::parse(&json!([
            {"$match": {"status": "active"}},
            {"$out": "results"}
        ])).unwrap();
        assert_eq!(pipeline.out_collection(), Some("results"));
    }

    #[test]
    fn pipeline_without_out() {
        let pipeline = Pipeline::parse(&json!([
            {"$match": {"status": "active"}}
        ])).unwrap();
        assert_eq!(pipeline.out_collection(), None);
    }

    // -----------------------------------------------------------------------
    // Combined: expressions in $project and $addFields
    // -----------------------------------------------------------------------

    #[test]
    fn project_with_cond_and_concat() {
        let docs = vec![
            json!({"name": "Alice", "score": 90}),
            json!({"name": "Bob", "score": 40}),
        ];
        let pipeline = Pipeline::parse(&json!([
            {"$project": {
                "name": 1,
                "grade": {"$cond": [{"$subtract": ["$score", 50]}, "pass", "fail"]},
                "label": {"$concat": ["$name", ": ", {"$cond": [{"$subtract": ["$score", 50]}, "P", "F"]}]}
            }}
        ])).unwrap();
        let results = pipeline.execute_from(0, docs, &no_lookup).unwrap();
        assert_eq!(results[0]["grade"], json!("pass"));  // 90-50=40 → truthy
        assert_eq!(results[0]["label"], json!("Alice: P"));
        assert_eq!(results[1]["grade"], json!("pass"));  // 40-50=-10 → truthy (non-zero)
        assert_eq!(results[1]["label"], json!("Bob: P"));
    }

    #[test]
    fn addfields_with_date_and_string() {
        let docs = vec![
            json!({"created": "2024-06-15T08:30:00Z", "name": "test"}),
        ];
        let pipeline = Pipeline::parse(&json!([
            {"$addFields": {
                "year": {"$year": "$created"},
                "month": {"$month": "$created"},
                "upper_name": {"$toUpper": "$name"}
            }}
        ])).unwrap();
        let results = pipeline.execute_from(0, docs, &no_lookup).unwrap();
        assert_eq!(results[0]["year"], json!(2024));
        assert_eq!(results[0]["month"], json!(6));
        assert_eq!(results[0]["upper_name"], json!("TEST"));
    }

    #[test]
    fn group_with_date_key() {
        let docs = vec![
            json!({"date": "2024-01-15", "amount": 100}),
            json!({"date": "2024-01-20", "amount": 200}),
            json!({"date": "2024-02-10", "amount": 150}),
        ];
        let pipeline = Pipeline::parse(&json!([
            {"$group": {
                "_id": {"$month": "$date"},
                "total": {"$sum": "$amount"}
            }}
        ])).unwrap();
        let results = pipeline.execute_from(0, docs, &no_lookup).unwrap();
        assert_eq!(results.len(), 2);
        for doc in &results {
            match doc["_id"].as_u64().unwrap() {
                1 => assert_eq!(doc["total"], json!(300)),
                2 => assert_eq!(doc["total"], json!(150)),
                other => panic!("unexpected month: {other}"),
            }
        }
    }
}
