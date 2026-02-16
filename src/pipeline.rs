use std::collections::HashMap;

use serde_json::{json, Map, Value};

use crate::document::Document;
use crate::error::{Error, Result};
use crate::query::{self, SortOrder};
use crate::value::IndexValue;

// ---------------------------------------------------------------------------
// Expression
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Expression {
    Literal(Value),
    FieldRef(String),
    Add(Vec<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
    Multiply(Vec<Expression>),
    Divide(Box<Expression>, Box<Expression>),
}

// ---------------------------------------------------------------------------
// Group key
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum GroupKey {
    Null,
    Single(Expression),
    Compound(Vec<(String, Expression)>),
}

// ---------------------------------------------------------------------------
// Accumulators
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Accumulator {
    Sum(Expression),
    Avg(Expression),
    Min(Expression),
    Max(Expression),
    Count,
    First(Expression),
    Last(Expression),
    Push(Expression),
}

enum AccumulatorState {
    Sum(f64),
    Avg { sum: f64, count: u64 },
    Min(Option<Value>),
    Max(Option<Value>),
    Count(u64),
    First(Option<Value>),
    Last(Option<Value>),
    Push(Vec<Value>),
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
    },
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

pub(crate) fn resolve_field(doc: &Value, path: &str) -> Value {
    let mut current = doc;
    for part in path.split('.') {
        match current {
            Value::Object(map) => match map.get(part) {
                Some(v) => current = v,
                None => return Value::Null,
            },
            _ => return Value::Null,
        }
    }
    current.clone()
}

pub(crate) fn set_field(doc: &mut Value, path: &str, value: Value) {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = doc;
    for (i, part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            if let Value::Object(map) = current {
                map.insert(part.to_string(), value);
            }
            return;
        }
        if let Value::Object(map) = current {
            if !map.contains_key(*part) || !map[*part].is_object() {
                map.insert(part.to_string(), json!({}));
            }
            current = map.get_mut(*part).unwrap();
        } else {
            return;
        }
    }
}

fn to_f64(v: &Value) -> Option<f64> {
    v.as_f64()
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
                _ => Ok(Expression::Literal(Value::Object(map.clone()))),
            }
        }
        _ => Ok(Expression::Literal(val.clone())),
    }
}

impl Expression {
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
        .filter(|doc| {
            if let Ok(d) = Document::new(0, doc.clone()) {
                query::matches_doc(&query, &d)
            } else {
                false
            }
        })
        .collect())
}

fn exec_group(
    docs: Vec<Value>,
    key: &GroupKey,
    accumulators: &[(String, Accumulator)],
) -> Result<Vec<Value>> {
    let mut groups: HashMap<String, (Value, Vec<AccumulatorState>)> = HashMap::new();
    let mut insertion_order: Vec<String> = Vec::new();

    for doc in &docs {
        let key_val = match key {
            GroupKey::Null => Value::Null,
            GroupKey::Single(expr) => expr.eval(doc),
            GroupKey::Compound(fields) => {
                let mut map = Map::new();
                for (name, expr) in fields {
                    map.insert(name.clone(), expr.eval(doc));
                }
                Value::Object(map)
            }
        };

        let key_str = serde_json::to_string(&key_val).unwrap();

        let (_, states) = groups.entry(key_str.clone()).or_insert_with(|| {
            insertion_order.push(key_str.clone());
            let initial: Vec<AccumulatorState> = accumulators
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
                })
                .collect();
            (key_val.clone(), initial)
        });

        for (i, (_, acc)) in accumulators.iter().enumerate() {
            let state = &mut states[i];
            match (acc, state) {
                (Accumulator::Sum(expr), AccumulatorState::Sum(s)) => {
                    if let Some(n) = to_f64(&expr.eval(doc)) {
                        *s += n;
                    }
                }
                (Accumulator::Avg(expr), AccumulatorState::Avg { sum, count }) => {
                    if let Some(n) = to_f64(&expr.eval(doc)) {
                        *sum += n;
                        *count += 1;
                    }
                }
                (Accumulator::Min(expr), AccumulatorState::Min(current)) => {
                    let val = expr.eval(doc);
                    if !val.is_null() {
                        *current = Some(match current.take() {
                            None => val,
                            Some(cur) => {
                                let cur_iv = IndexValue::from_json(&cur);
                                let new_iv = IndexValue::from_json(&val);
                                if new_iv < cur_iv {
                                    val
                                } else {
                                    cur
                                }
                            }
                        });
                    }
                }
                (Accumulator::Max(expr), AccumulatorState::Max(current)) => {
                    let val = expr.eval(doc);
                    if !val.is_null() {
                        *current = Some(match current.take() {
                            None => val,
                            Some(cur) => {
                                let cur_iv = IndexValue::from_json(&cur);
                                let new_iv = IndexValue::from_json(&val);
                                if new_iv > cur_iv {
                                    val
                                } else {
                                    cur
                                }
                            }
                        });
                    }
                }
                (Accumulator::Count, AccumulatorState::Count(c)) => {
                    *c += 1;
                }
                (Accumulator::First(expr), AccumulatorState::First(current)) => {
                    if current.is_none() {
                        *current = Some(expr.eval(doc));
                    }
                }
                (Accumulator::Last(expr), AccumulatorState::Last(current)) => {
                    *current = Some(expr.eval(doc));
                }
                (Accumulator::Push(expr), AccumulatorState::Push(vec)) => {
                    vec.push(expr.eval(doc));
                }
                _ => {}
            }
        }
    }

    let mut results = Vec::new();
    for key_str in &insertion_order {
        let (key_val, states) = groups.remove(key_str).unwrap();
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
                AccumulatorState::Min(v) => v.unwrap_or(Value::Null),
                AccumulatorState::Max(v) => v.unwrap_or(Value::Null),
                AccumulatorState::Count(c) => Value::Number(c.into()),
                AccumulatorState::First(v) => v.unwrap_or(Value::Null),
                AccumulatorState::Last(v) => v.unwrap_or(Value::Null),
                AccumulatorState::Push(v) => Value::Array(v),
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
            let av = resolve_field(a, field);
            let bv = resolve_field(b, field);
            let aiv = IndexValue::from_json(&av);
            let biv = IndexValue::from_json(&bv);
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
    lookup_fn: &F,
) -> Result<Vec<Value>>
where
    F: Fn(&str, &Value) -> Result<Vec<Value>>,
{
    let mut result = Vec::new();
    for mut doc in docs {
        let local_val = resolve_field(&doc, local_field);
        let query = json!({ foreign_field: local_val });
        let foreign_docs = lookup_fn(from, &query)?;
        set_field(&mut doc, as_field, Value::Array(foreign_docs));
        result.push(doc);
    }
    Ok(result)
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
                    Stage::Lookup {
                        from: from.to_string(),
                        local_field: local_field.to_string(),
                        foreign_field: foreign_field.to_string(),
                        as_field: as_field.to_string(),
                    }
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
                Stage::Group { key, accumulators } => exec_group(current, key, accumulators)?,
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
                } => exec_lookup(current, from, local_field, foreign_field, as_field, lookup_fn)?,
            };
        }
        Ok(current)
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
            let result = exec_group(docs, key, accumulators).unwrap();
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
            let result = exec_group(docs, key, accumulators).unwrap();
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
            let result = exec_group(docs, key, accumulators).unwrap();
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
            let result = exec_group(docs, key, accumulators).unwrap();
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
            let result = exec_group(docs, key, accumulators).unwrap();
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
            let result = exec_group(docs, key, accumulators).unwrap();
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
            let result = exec_group(docs, key, accumulators).unwrap();
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
            let result = exec_group(docs, key, accumulators).unwrap();
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
            let result = exec_group(docs, key, accumulators).unwrap();
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
            exec_lookup(docs, "inventory", "item", "sku", "matched", &mock_lookup).unwrap();
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
}
