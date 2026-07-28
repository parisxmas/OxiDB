use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::pipeline::{resolve_field, resolve_field_ref, set_field};
use crate::value::IndexValue;

/// Apply all update operators in `update` to `doc`.
///
/// `update` must be an object whose keys are operator names (`$set`, `$inc`, etc.)
/// and whose values are objects mapping field paths to operand values.
/// Multiple operators in a single update are applied sequentially.
pub fn apply_update(doc: &mut Value, update: &Value) -> Result<()> {
    let obj = update
        .as_object()
        .ok_or_else(|| Error::InvalidQuery("update must be an object".into()))?;

    for (op, fields) in obj {
        let fields = fields
            .as_object()
            .ok_or_else(|| Error::InvalidQuery(format!("{op} value must be an object")))?;
        match op.as_str() {
            "$set" => apply_set(doc, fields)?,
            "$unset" => apply_unset(doc, fields)?,
            "$inc" => apply_inc(doc, fields)?,
            "$mul" => apply_mul(doc, fields)?,
            "$min" => apply_min(doc, fields)?,
            "$max" => apply_max(doc, fields)?,
            "$rename" => apply_rename(doc, fields)?,
            "$currentDate" => apply_current_date(doc, fields)?,
            "$push" => apply_push(doc, fields)?,
            "$pull" => apply_pull(doc, fields)?,
            "$addToSet" => apply_add_to_set(doc, fields)?,
            "$pop" => apply_pop(doc, fields)?,
            _ => {
                return Err(Error::InvalidQuery(format!(
                    "unknown update operator: {op}"
                )));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Field operators
// ---------------------------------------------------------------------------

fn apply_set(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, value) in fields {
        set_field(doc, path, value.clone());
    }
    Ok(())
}

fn apply_unset(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, _) in fields {
        remove_field(doc, path);
    }
    Ok(())
}

/// Integer-exact arithmetic when both sides are i64 — checked, an overflow
/// is an error like MongoDB's, never a wrapped or lossy value. Falls back to
/// f64 when either side is a float, erroring on a non-finite result:
/// `number_to_value` would map an overflowed f64 (±inf) to `Value::Null`,
/// silently turning a counter into null (and making the NEXT $inc fail).
fn numeric_apply(
    cur: &Value,
    operand: &Value,
    path: &str,
    op: &str,
    int_op: fn(i64, i64) -> Option<i64>,
    float_op: fn(f64, f64) -> f64,
) -> Result<Value> {
    if let (Some(a), Some(b)) = (cur.as_i64(), operand.as_i64()) {
        return int_op(a, b)
            .map(|n| Value::Number(n.into()))
            .ok_or_else(|| {
                Error::InvalidQuery(format!("{op} on '{path}' overflows a 64-bit integer"))
            });
    }
    let (Some(a), Some(b)) = (cur.as_f64(), operand.as_f64()) else {
        return Err(Error::InvalidQuery(format!(
            "{op} cannot be applied to non-numeric field '{path}'"
        )));
    };
    let r = float_op(a, b);
    if !r.is_finite() {
        return Err(Error::InvalidQuery(format!(
            "{op} on '{path}' produced a non-finite number"
        )));
    }
    Ok(number_to_value(r))
}

fn apply_inc(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, inc_val) in fields {
        if !inc_val.is_number() {
            return Err(Error::InvalidQuery(format!(
                "$inc value for '{path}' must be numeric"
            )));
        }
        // Distinguish a missing field (initialize to the operand) from a
        // field that is present but non-numeric — including an explicit
        // `null` — which is an error. `resolve_field` collapses both into
        // `Null`, so use `resolve_field_ref` to tell them apart.
        let new_val = match resolve_field_ref(doc, path) {
            None => inc_val.clone(),
            Some(v) => numeric_apply(v, inc_val, path, "$inc", i64::checked_add, |a, b| a + b)?,
        };
        set_field(doc, path, new_val);
    }
    Ok(())
}

fn apply_mul(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, mul_val) in fields {
        if !mul_val.is_number() {
            return Err(Error::InvalidQuery(format!(
                "$mul value for '{path}' must be numeric"
            )));
        }
        // Missing field → initialize to 0 (MongoDB semantics). A present but
        // non-numeric field (including explicit `null`) is an error.
        let new_val = match resolve_field_ref(doc, path) {
            None => Value::Number(0.into()),
            Some(v) => numeric_apply(v, mul_val, path, "$mul", i64::checked_mul, |a, b| a * b)?,
        };
        set_field(doc, path, new_val);
    }
    Ok(())
}

fn apply_min(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, new_val) in fields {
        // Only a *missing* field is initialized unconditionally. A present
        // value — including explicit `null`, which is the lowest in the cross-
        // type ordering — is compared, so `$min` against a null field leaves
        // it unchanged rather than overwriting it.
        match resolve_field_ref(doc, path) {
            None => set_field(doc, path, new_val.clone()),
            Some(current) => {
                let cur_iv = IndexValue::from_json(current);
                let new_iv = IndexValue::from_json(new_val);
                if new_iv < cur_iv {
                    set_field(doc, path, new_val.clone());
                }
            }
        }
    }
    Ok(())
}

fn apply_max(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, new_val) in fields {
        // See `apply_min`: only a missing field is initialized unconditionally;
        // a present `null` is compared (and, being the lowest value, replaced
        // by any non-null `new_val`).
        match resolve_field_ref(doc, path) {
            None => set_field(doc, path, new_val.clone()),
            Some(current) => {
                let cur_iv = IndexValue::from_json(current);
                let new_iv = IndexValue::from_json(new_val);
                if new_iv > cur_iv {
                    set_field(doc, path, new_val.clone());
                }
            }
        }
    }
    Ok(())
}

fn apply_rename(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (old_path, new_path_val) in fields {
        let new_path = new_path_val.as_str().ok_or_else(|| {
            Error::InvalidQuery(format!("$rename target for '{old_path}' must be a string"))
        })?;
        // resolve_field_ref (not resolve_field): a field holding an explicit
        // `null` must still be renamed — only a MISSING field is a no-op.
        let val = resolve_field_ref(doc, old_path).cloned();
        if let Some(val) = val {
            remove_field(doc, old_path);
            set_field(doc, new_path, val);
        }
    }
    Ok(())
}

fn apply_current_date(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    let now = chrono::Utc::now().to_rfc3339();
    for (path, _) in fields {
        set_field(doc, path, Value::String(now.clone()));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Array operators
// ---------------------------------------------------------------------------

/// Resolve a `$push` / `$addToSet` operand into the values to append.
/// `{$each: [..]}` splices its elements (MongoDB); any other `$`-modifier
/// in the operand object is rejected rather than silently stored as a
/// literal `{"$each": ...}` element — which is what used to happen.
fn push_operand_values(value: &Value, path: &str, op: &str) -> Result<Vec<Value>> {
    if let Some(obj) = value.as_object() {
        if obj.contains_key("$each") {
            let each = obj["$each"].as_array().ok_or_else(|| {
                Error::InvalidQuery(format!("{op} $each for '{path}' must be an array"))
            })?;
            if let Some(other) = obj.keys().find(|k| *k != "$each") {
                return Err(Error::InvalidQuery(format!(
                    "{op} modifier '{other}' for '{path}' is not supported"
                )));
            }
            return Ok(each.clone());
        }
        if let Some(modifier) = obj.keys().find(|k| k.starts_with('$')) {
            return Err(Error::InvalidQuery(format!(
                "{op} modifier '{modifier}' for '{path}' requires $each"
            )));
        }
    }
    Ok(vec![value.clone()])
}

fn apply_push(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, value) in fields {
        let values = push_operand_values(value, path, "$push")?;
        let current = resolve_field(doc, path);
        match &current {
            Value::Null => {
                set_field(doc, path, Value::Array(values));
            }
            Value::Array(arr) => {
                let mut new_arr = arr.clone();
                new_arr.extend(values);
                set_field(doc, path, Value::Array(new_arr));
            }
            _ => {
                return Err(Error::InvalidQuery(format!(
                    "$push requires field '{path}' to be an array"
                )));
            }
        }
    }
    Ok(())
}

fn apply_pull(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, match_val) in fields {
        // An object operand is a per-element CONDITION, exactly the shape
        // $elemMatch takes: `{$gte: 80}` applies operators to the element
        // itself, `{score: {$gte: 8}}` queries element fields. Comparing
        // the operand literally (the old behavior) made every conditional
        // $pull a silent no-op. Non-object operands stay literal equality.
        let condition = if match_val.is_object() {
            Some(crate::query::parse_elem_match_inner(match_val)?)
        } else {
            None
        };
        let current = resolve_field(doc, path);
        match &current {
            Value::Null => {} // no-op
            Value::Array(arr) => {
                let new_arr: Vec<Value> = match &condition {
                    Some(q) => arr
                        .iter()
                        .filter(|el| !crate::query::matches_value(q, el))
                        .cloned()
                        .collect(),
                    None => arr.iter().filter(|el| *el != match_val).cloned().collect(),
                };
                set_field(doc, path, Value::Array(new_arr));
            }
            _ => {
                return Err(Error::InvalidQuery(format!(
                    "$pull requires field '{path}' to be an array"
                )));
            }
        }
    }
    Ok(())
}

fn apply_add_to_set(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, value) in fields {
        let values = push_operand_values(value, path, "$addToSet")?;
        let current = resolve_field(doc, path);
        match &current {
            Value::Null => {
                let mut new_arr: Vec<Value> = Vec::with_capacity(values.len());
                for v in values {
                    if !new_arr.contains(&v) {
                        new_arr.push(v);
                    }
                }
                set_field(doc, path, Value::Array(new_arr));
            }
            Value::Array(arr) => {
                let mut new_arr = arr.clone();
                for v in values {
                    if !new_arr.contains(&v) {
                        new_arr.push(v);
                    }
                }
                set_field(doc, path, Value::Array(new_arr));
            }
            _ => {
                return Err(Error::InvalidQuery(format!(
                    "$addToSet requires field '{path}' to be an array"
                )));
            }
        }
    }
    Ok(())
}

fn apply_pop(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, dir_val) in fields {
        // Validate the operand FIRST: `{$pop: {arr: 99}}` must error even
        // when the array happens to be empty or missing — operand validity
        // should not depend on the document's current state.
        let dir = dir_val
            .as_i64()
            .filter(|d| *d == 1 || *d == -1)
            .ok_or_else(|| {
                Error::InvalidQuery(format!("$pop value for '{path}' must be 1 or -1"))
            })?;
        let current = resolve_field(doc, path);
        match &current {
            Value::Null => {} // no-op
            Value::Array(arr) => {
                if arr.is_empty() {
                    continue;
                }
                let mut new_arr = arr.clone();
                match dir {
                    1 => {
                        new_arr.pop();
                    }
                    _ => {
                        new_arr.remove(0);
                    }
                }
                set_field(doc, path, Value::Array(new_arr));
            }
            _ => {
                return Err(Error::InvalidQuery(format!(
                    "$pop requires field '{path}' to be an array"
                )));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn remove_field(doc: &mut Value, path: &str) {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 1 {
        if let Value::Object(map) = doc {
            map.remove(path);
        }
        return;
    }
    // Navigate to parent, then remove the last key
    let mut current = &mut *doc;
    for part in &parts[..parts.len() - 1] {
        match current {
            Value::Object(map) => match map.get_mut(*part) {
                Some(v) => current = v,
                None => return,
            },
            Value::Array(arr) => match part.parse::<usize>() {
                Ok(idx) if idx < arr.len() => current = &mut arr[idx],
                _ => return,
            },
            _ => return,
        }
    }
    let last = parts[parts.len() - 1];
    match current {
        Value::Object(map) => {
            map.remove(last);
        }
        Value::Array(arr) => {
            // MongoDB sets the element to null rather than removing it —
            // removal shifts every later index, corrupting concurrent
            // positional logic ($set "arr.2", etc.) written against the
            // original positions.
            if let Ok(idx) = last.parse::<usize>()
                && idx < arr.len()
            {
                arr[idx] = Value::Null;
            }
        }
        _ => {}
    }
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

// ===========================================================================
// Tests
// ===========================================================================

/// Expand `arrayFilters` placeholders in an update document against a
/// concrete target document: every operator key containing `$[ident]` (or
/// the all-elements `$[]`) is rewritten into zero or more keys with real
/// array indices, chosen by matching each element — wrapped as
/// `{ident: element}` — against the corresponding filter. Nested
/// placeholders multiply out. Keys whose placeholders match nothing are
/// dropped (that operator becomes a no-op for this document, as in
/// MongoDB).
pub(crate) fn expand_array_filters(doc: &Value, update: &Value, filters: &Value) -> Result<Value> {
    use crate::query;
    // ident -> parsed element filter
    let mut fmap: std::collections::HashMap<String, query::Query> =
        std::collections::HashMap::new();
    for f in filters.as_array().into_iter().flatten() {
        let obj = f
            .as_object()
            .ok_or_else(|| Error::InvalidQuery("arrayFilters entries must be documents".into()))?;
        let ident = obj
            .keys()
            .next()
            .map(|k| k.split('.').next().unwrap_or(k).to_string())
            .ok_or_else(|| Error::InvalidQuery("empty arrayFilters entry".into()))?;
        if obj
            .keys()
            .any(|k| k.split('.').next() != Some(ident.as_str()))
        {
            return Err(Error::InvalidQuery(
                "an arrayFilters entry must use a single identifier".into(),
            ));
        }
        fmap.insert(ident, query::parse_query(f)?);
    }

    fn expand_key(
        doc: &Value,
        key: &str,
        fmap: &std::collections::HashMap<String, crate::query::Query>,
    ) -> Result<Vec<String>> {
        use crate::query;
        // (concrete path so far, current position in the doc)
        let mut frontier: Vec<(String, Option<Value>)> = vec![(String::new(), Some(doc.clone()))];
        for seg in key.split('.') {
            let mut next = Vec::new();
            let ident: Option<&str> = seg.strip_prefix("$[").and_then(|r| r.strip_suffix(']'));
            for (path, cur) in frontier {
                match ident {
                    None => {
                        let np = if path.is_empty() {
                            seg.to_string()
                        } else {
                            format!("{path}.{seg}")
                        };
                        let nv = cur.as_ref().and_then(|c| c.get(seg)).cloned();
                        next.push((np, nv));
                    }
                    Some(id) => {
                        let Some(arr) = cur.as_ref().and_then(|c| c.as_array()) else {
                            continue; // placeholder on a non-array: no match
                        };
                        let filter = if id.is_empty() {
                            None // $[] — every element
                        } else {
                            Some(fmap.get(id).ok_or_else(|| {
                                Error::InvalidQuery(format!(
                                    "no arrayFilters entry for identifier '{id}'"
                                ))
                            })?)
                        };
                        for (i, elem) in arr.iter().enumerate() {
                            let matched = match filter {
                                None => true,
                                Some(q) => {
                                    let wrapper = serde_json::json!({ id: elem });
                                    query::matches_value(q, &wrapper)
                                }
                            };
                            if matched {
                                let np = if path.is_empty() {
                                    i.to_string()
                                } else {
                                    format!("{path}.{i}")
                                };
                                next.push((np, Some(elem.clone())));
                            }
                        }
                    }
                }
            }
            frontier = next;
        }
        Ok(frontier.into_iter().map(|(p, _)| p).collect())
    }

    let update_obj = update
        .as_object()
        .ok_or_else(|| Error::InvalidQuery("update must be an object".into()))?;
    let mut out = serde_json::Map::new();
    for (op, fields) in update_obj {
        let Some(fobj) = fields.as_object() else {
            out.insert(op.clone(), fields.clone());
            continue;
        };
        let mut nf = serde_json::Map::new();
        for (k, v) in fobj {
            if k.contains("$[") {
                for concrete in expand_key(doc, k, &fmap)? {
                    nf.insert(concrete, v.clone());
                }
            } else {
                nf.insert(k.clone(), v.clone());
            }
        }
        if !nf.is_empty() {
            out.insert(op.clone(), Value::Object(nf));
        }
    }
    Ok(Value::Object(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // -----------------------------------------------------------------------
    // $set
    // -----------------------------------------------------------------------

    #[test]
    fn set_top_level_field() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$set": {"age": 30}})).unwrap();
        assert_eq!(doc["age"], 30);
    }

    #[test]
    fn set_nested_field_dot_notation() {
        let mut doc = json!({"user": {"name": "Alice"}});
        apply_update(&mut doc, &json!({"$set": {"user.age": 30}})).unwrap();
        assert_eq!(doc["user"]["age"], 30);
    }

    #[test]
    fn set_overwrite_existing() {
        let mut doc = json!({"name": "Alice", "age": 25});
        apply_update(&mut doc, &json!({"$set": {"age": 30}})).unwrap();
        assert_eq!(doc["age"], 30);
    }

    // -----------------------------------------------------------------------
    // null-vs-missing semantics (regression)
    // -----------------------------------------------------------------------

    #[test]
    fn inc_on_missing_field_initializes() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$inc": {"count": 5}})).unwrap();
        assert_eq!(doc["count"], 5.0);
    }

    #[test]
    fn inc_on_present_null_is_error() {
        // An explicit null is non-numeric → $inc must error, not silently
        // treat it as a missing field initialized to the increment.
        let mut doc = json!({"count": null});
        assert!(apply_update(&mut doc, &json!({"$inc": {"count": 5}})).is_err());
    }

    #[test]
    fn mul_on_missing_field_yields_zero() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$mul": {"price": 2}})).unwrap();
        assert_eq!(doc["price"], 0.0);
    }

    #[test]
    fn mul_on_present_null_is_error() {
        let mut doc = json!({"price": null});
        assert!(apply_update(&mut doc, &json!({"$mul": {"price": 2}})).is_err());
    }

    #[test]
    fn min_leaves_present_null_unchanged() {
        // null is the lowest value in the ordering, so $min against it must
        // not overwrite with a larger value.
        let mut doc = json!({"score": null});
        apply_update(&mut doc, &json!({"$min": {"score": 50}})).unwrap();
        assert_eq!(doc["score"], Value::Null);
    }

    #[test]
    fn max_replaces_present_null() {
        let mut doc = json!({"score": null});
        apply_update(&mut doc, &json!({"$max": {"score": 50}})).unwrap();
        assert_eq!(doc["score"], 50);
    }

    #[test]
    fn min_on_missing_field_initializes() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$min": {"score": 50}})).unwrap();
        assert_eq!(doc["score"], 50);
    }

    // -----------------------------------------------------------------------
    // $unset
    // -----------------------------------------------------------------------

    #[test]
    fn unset_removes_field() {
        let mut doc = json!({"name": "Alice", "age": 30});
        apply_update(&mut doc, &json!({"$unset": {"age": ""}})).unwrap();
        assert!(doc.get("age").is_none());
        assert_eq!(doc["name"], "Alice");
    }

    #[test]
    fn unset_missing_field_noop() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$unset": {"missing": ""}})).unwrap();
        assert_eq!(doc, json!({"name": "Alice"}));
    }

    #[test]
    fn unset_nested_field() {
        let mut doc = json!({"user": {"name": "Alice", "age": 30}});
        apply_update(&mut doc, &json!({"$unset": {"user.age": ""}})).unwrap();
        assert!(doc["user"].get("age").is_none());
        assert_eq!(doc["user"]["name"], "Alice");
    }

    // -----------------------------------------------------------------------
    // $inc
    // -----------------------------------------------------------------------

    #[test]
    fn inc_integer() {
        let mut doc = json!({"count": 5});
        apply_update(&mut doc, &json!({"$inc": {"count": 3}})).unwrap();
        assert_eq!(doc["count"], 8);
    }

    #[test]
    fn inc_float() {
        let mut doc = json!({"val": 1.5});
        apply_update(&mut doc, &json!({"$inc": {"val": 0.5}})).unwrap();
        assert_eq!(doc["val"], 2);
    }

    #[test]
    fn inc_creates_missing_field() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$inc": {"count": 1}})).unwrap();
        assert_eq!(doc["count"], 1);
    }

    #[test]
    fn inc_error_on_non_numeric() {
        let mut doc = json!({"name": "Alice"});
        let result = apply_update(&mut doc, &json!({"$inc": {"name": 1}}));
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // $mul
    // -----------------------------------------------------------------------

    #[test]
    fn mul_existing_field() {
        let mut doc = json!({"price": 10});
        apply_update(&mut doc, &json!({"$mul": {"price": 3}})).unwrap();
        assert_eq!(doc["price"], 30);
    }

    #[test]
    fn mul_missing_field_becomes_zero() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$mul": {"count": 5}})).unwrap();
        assert_eq!(doc["count"], 0);
    }

    #[test]
    fn mul_error_on_non_numeric() {
        let mut doc = json!({"name": "Alice"});
        let result = apply_update(&mut doc, &json!({"$mul": {"name": 2}}));
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // $min
    // -----------------------------------------------------------------------

    #[test]
    fn min_updates_when_less() {
        let mut doc = json!({"score": 100});
        apply_update(&mut doc, &json!({"$min": {"score": 50}})).unwrap();
        assert_eq!(doc["score"], 50);
    }

    #[test]
    fn min_noop_when_greater() {
        let mut doc = json!({"score": 50});
        apply_update(&mut doc, &json!({"$min": {"score": 100}})).unwrap();
        assert_eq!(doc["score"], 50);
    }

    #[test]
    fn min_sets_missing_field() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$min": {"score": 50}})).unwrap();
        assert_eq!(doc["score"], 50);
    }

    // -----------------------------------------------------------------------
    // $max
    // -----------------------------------------------------------------------

    #[test]
    fn max_updates_when_greater() {
        let mut doc = json!({"score": 50});
        apply_update(&mut doc, &json!({"$max": {"score": 100}})).unwrap();
        assert_eq!(doc["score"], 100);
    }

    #[test]
    fn max_noop_when_less() {
        let mut doc = json!({"score": 100});
        apply_update(&mut doc, &json!({"$max": {"score": 50}})).unwrap();
        assert_eq!(doc["score"], 100);
    }

    // -----------------------------------------------------------------------
    // $rename
    // -----------------------------------------------------------------------

    #[test]
    fn rename_field() {
        let mut doc = json!({"old_name": "Alice"});
        apply_update(&mut doc, &json!({"$rename": {"old_name": "new_name"}})).unwrap();
        assert!(doc.get("old_name").is_none());
        assert_eq!(doc["new_name"], "Alice");
    }

    #[test]
    fn rename_with_dot_notation() {
        let mut doc = json!({"user": {"first": "Alice"}});
        apply_update(&mut doc, &json!({"$rename": {"user.first": "user.name"}})).unwrap();
        assert!(doc["user"].get("first").is_none());
        assert_eq!(doc["user"]["name"], "Alice");
    }

    // -----------------------------------------------------------------------
    // $currentDate
    // -----------------------------------------------------------------------

    #[test]
    fn current_date_sets_iso_string() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$currentDate": {"updated_at": true}})).unwrap();
        let val = doc["updated_at"].as_str().unwrap();
        // Should parse as a valid RFC 3339 datetime
        assert!(chrono::DateTime::parse_from_rfc3339(val).is_ok());
    }

    // -----------------------------------------------------------------------
    // $push
    // -----------------------------------------------------------------------

    #[test]
    fn push_to_existing_array() {
        let mut doc = json!({"tags": ["a", "b"]});
        apply_update(&mut doc, &json!({"$push": {"tags": "c"}})).unwrap();
        assert_eq!(doc["tags"], json!(["a", "b", "c"]));
    }

    #[test]
    fn push_creates_array_from_missing() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$push": {"tags": "a"}})).unwrap();
        assert_eq!(doc["tags"], json!(["a"]));
    }

    #[test]
    fn push_error_on_non_array() {
        let mut doc = json!({"tags": "not-an-array"});
        let result = apply_update(&mut doc, &json!({"$push": {"tags": "a"}}));
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // $pull
    // -----------------------------------------------------------------------

    #[test]
    fn pull_removes_matching() {
        let mut doc = json!({"tags": ["a", "b", "c", "b"]});
        apply_update(&mut doc, &json!({"$pull": {"tags": "b"}})).unwrap();
        assert_eq!(doc["tags"], json!(["a", "c"]));
    }

    #[test]
    fn pull_noop_on_missing() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$pull": {"tags": "a"}})).unwrap();
        assert!(doc.get("tags").is_none());
    }

    // -----------------------------------------------------------------------
    // $addToSet
    // -----------------------------------------------------------------------

    #[test]
    fn add_to_set_unique_value() {
        let mut doc = json!({"tags": ["a", "b"]});
        apply_update(&mut doc, &json!({"$addToSet": {"tags": "c"}})).unwrap();
        assert_eq!(doc["tags"], json!(["a", "b", "c"]));
    }

    #[test]
    fn add_to_set_skip_duplicate() {
        let mut doc = json!({"tags": ["a", "b"]});
        apply_update(&mut doc, &json!({"$addToSet": {"tags": "b"}})).unwrap();
        assert_eq!(doc["tags"], json!(["a", "b"]));
    }

    #[test]
    fn add_to_set_creates_array_from_missing() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$addToSet": {"tags": "a"}})).unwrap();
        assert_eq!(doc["tags"], json!(["a"]));
    }

    // -----------------------------------------------------------------------
    // $pop
    // -----------------------------------------------------------------------

    #[test]
    fn pop_last() {
        let mut doc = json!({"arr": [1, 2, 3]});
        apply_update(&mut doc, &json!({"$pop": {"arr": 1}})).unwrap();
        assert_eq!(doc["arr"], json!([1, 2]));
    }

    #[test]
    fn pop_first() {
        let mut doc = json!({"arr": [1, 2, 3]});
        apply_update(&mut doc, &json!({"$pop": {"arr": -1}})).unwrap();
        assert_eq!(doc["arr"], json!([2, 3]));
    }

    #[test]
    fn pop_noop_on_empty_array() {
        let mut doc = json!({"arr": []});
        apply_update(&mut doc, &json!({"$pop": {"arr": 1}})).unwrap();
        assert_eq!(doc["arr"], json!([]));
    }

    #[test]
    fn pop_noop_on_missing() {
        let mut doc = json!({"name": "Alice"});
        apply_update(&mut doc, &json!({"$pop": {"arr": 1}})).unwrap();
        assert!(doc.get("arr").is_none());
    }

    #[test]
    fn pop_invalid_operand_errors_even_on_empty_array() {
        let mut doc = json!({"arr": []});
        assert!(apply_update(&mut doc, &json!({"$pop": {"arr": 99}})).is_err());
    }

    #[test]
    fn pull_with_operator_condition() {
        let mut doc = json!({"scores": [55, 80, 95, 60]});
        apply_update(&mut doc, &json!({"$pull": {"scores": {"$gte": 80}}})).unwrap();
        assert_eq!(doc["scores"], json!([55, 60]));
    }

    #[test]
    fn pull_with_field_condition_on_subdocs() {
        let mut doc = json!({"results": [{"item": "A", "score": 5}, {"item": "B", "score": 8}]});
        apply_update(
            &mut doc,
            &json!({"$pull": {"results": {"score": {"$gte": 8}}}}),
        )
        .unwrap();
        assert_eq!(doc["results"], json!([{"item": "A", "score": 5}]));
    }

    #[test]
    fn push_with_each_splices_elements() {
        let mut doc = json!({"tags": ["a"]});
        apply_update(&mut doc, &json!({"$push": {"tags": {"$each": ["b", "c"]}}})).unwrap();
        assert_eq!(doc["tags"], json!(["a", "b", "c"]));
    }

    #[test]
    fn add_to_set_with_each_dedups() {
        let mut doc = json!({"tags": ["a", "b"]});
        apply_update(
            &mut doc,
            &json!({"$addToSet": {"tags": {"$each": ["b", "c", "c"]}}}),
        )
        .unwrap();
        assert_eq!(doc["tags"], json!(["a", "b", "c"]));
    }

    #[test]
    fn push_unknown_modifier_errors() {
        let mut doc = json!({"tags": ["a"]});
        assert!(
            apply_update(&mut doc, &json!({"$push": {"tags": {"$slice": 2}}})).is_err(),
            "$-modifier without $each must error, not be stored literally"
        );
    }

    #[test]
    fn inc_integer_overflow_errors_instead_of_corrupting() {
        let mut doc = json!({"n": i64::MAX});
        assert!(apply_update(&mut doc, &json!({"$inc": {"n": 1}})).is_err());
        // The field is untouched, not null.
        assert_eq!(doc["n"], json!(i64::MAX));
    }

    #[test]
    fn inc_preserves_big_integer_precision() {
        // 2^53 + 1 is not representable in f64 — the old f64 round-trip
        // silently corrupted counters above 2^53.
        let big = (1i64 << 53) + 1;
        let mut doc = json!({"n": big});
        apply_update(&mut doc, &json!({"$inc": {"n": 1}})).unwrap();
        assert_eq!(doc["n"], json!(big + 1));
    }

    #[test]
    fn rename_null_valued_field() {
        let mut doc = json!({"old": null, "x": 1});
        apply_update(&mut doc, &json!({"$rename": {"old": "new"}})).unwrap();
        assert!(doc.get("old").is_none());
        assert_eq!(doc["new"], Value::Null);
    }

    // -----------------------------------------------------------------------
    // Multiple operators in one update
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_operators() {
        let mut doc = json!({"a": 1, "b": 10});
        apply_update(&mut doc, &json!({"$set": {"a": 99}, "$inc": {"b": 5}})).unwrap();
        assert_eq!(doc["a"], 99);
        assert_eq!(doc["b"], 15);
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn unknown_operator_errors() {
        let mut doc = json!({"a": 1});
        let result = apply_update(&mut doc, &json!({"$bad": {"a": 1}}));
        assert!(result.is_err());
    }

    #[test]
    fn non_object_operator_value_errors() {
        let mut doc = json!({"a": 1});
        let result = apply_update(&mut doc, &json!({"$set": "not-an-object"}));
        assert!(result.is_err());
    }

    #[test]
    fn update_not_object_errors() {
        let mut doc = json!({"a": 1});
        let result = apply_update(&mut doc, &json!("not-an-object"));
        assert!(result.is_err());
    }

    #[test]
    fn inc_on_string_field_errors() {
        let mut doc = json!({"name": "Alice"});
        let result = apply_update(&mut doc, &json!({"$inc": {"name": 1}}));
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Array dot-notation (variants.0.stock bug fix)
    // -----------------------------------------------------------------------

    #[test]
    fn set_array_element_by_index() {
        let mut doc = json!({"variants": [{"size": "M", "stock": 10}, {"size": "L", "stock": 5}]});
        apply_update(&mut doc, &json!({"$set": {"variants.0.stock": 8}})).unwrap();
        assert_eq!(doc["variants"][0]["stock"], 8);
        assert_eq!(doc["variants"][0]["size"], "M"); // preserved
        assert_eq!(doc["variants"][1]["stock"], 5); // untouched
        assert!(doc["variants"].is_array()); // still an array
    }

    #[test]
    fn set_array_second_element() {
        let mut doc = json!({"items": [{"name": "A", "qty": 1}, {"name": "B", "qty": 2}]});
        apply_update(&mut doc, &json!({"$set": {"items.1.qty": 99}})).unwrap();
        assert_eq!(doc["items"][1]["qty"], 99);
        assert_eq!(doc["items"][0]["qty"], 1); // untouched
        assert!(doc["items"].is_array());
    }

    #[test]
    fn set_array_element_top_level_value() {
        let mut doc = json!({"scores": [10, 20, 30]});
        apply_update(&mut doc, &json!({"$set": {"scores.1": 99}})).unwrap();
        assert_eq!(doc["scores"], json!([10, 99, 30]));
    }

    #[test]
    fn inc_array_element_field() {
        let mut doc = json!({"variants": [{"size": "M", "stock": 10}, {"size": "L", "stock": 5}]});
        apply_update(&mut doc, &json!({"$inc": {"variants.0.stock": -3}})).unwrap();
        assert_eq!(doc["variants"][0]["stock"], 7);
        assert!(doc["variants"].is_array());
    }

    #[test]
    fn set_deeply_nested_array() {
        let mut doc = json!({"a": [{"b": [{"c": 1}, {"c": 2}]}]});
        apply_update(&mut doc, &json!({"$set": {"a.0.b.1.c": 99}})).unwrap();
        assert_eq!(doc["a"][0]["b"][1]["c"], 99);
        assert_eq!(doc["a"][0]["b"][0]["c"], 1); // untouched
    }

    #[test]
    fn set_array_out_of_bounds_pads_with_null() {
        // MongoDB pads the array with nulls up to the target index rather than
        // dropping the write.
        let mut doc = json!({"arr": [1, 2]});
        apply_update(&mut doc, &json!({"$set": {"arr.5": 99}})).unwrap();
        assert_eq!(doc["arr"], json!([1, 2, null, null, null, 99]));
    }

    #[test]
    fn inc_array_out_of_bounds_creates_element() {
        // $inc on a missing array slot initializes it to the increment after
        // padding (the slot is "missing", so it starts from the increment).
        let mut doc = json!({"arr": [1, 2]});
        apply_update(&mut doc, &json!({"$inc": {"arr.4": 7}})).unwrap();
        assert_eq!(doc["arr"], json!([1, 2, null, null, 7]));
    }

    #[test]
    fn unset_array_element_field() {
        let mut doc = json!({"items": [{"name": "A", "temp": true}, {"name": "B"}]});
        apply_update(&mut doc, &json!({"$unset": {"items.0.temp": ""}})).unwrap();
        assert!(doc["items"][0].get("temp").is_none());
        assert_eq!(doc["items"][0]["name"], "A");
    }

    #[test]
    fn unset_array_element_nulls_in_place() {
        // MongoDB semantics: $unset on an array element sets it to null —
        // it must NOT remove it and shift the later indices.
        let mut doc = json!({"tags": ["a", "b", "c"]});
        apply_update(&mut doc, &json!({"$unset": {"tags.1": ""}})).unwrap();
        assert_eq!(doc["tags"], json!(["a", null, "c"]));
    }

    #[test]
    fn push_to_nested_array_element() {
        let mut doc =
            json!({"users": [{"name": "A", "roles": ["read"]}, {"name": "B", "roles": ["admin"]}]});
        apply_update(&mut doc, &json!({"$push": {"users.0.roles": "write"}})).unwrap();
        assert_eq!(doc["users"][0]["roles"], json!(["read", "write"]));
        assert_eq!(doc["users"][1]["roles"], json!(["admin"])); // untouched
    }

    // The exact scenario from the bug report: e-commerce variant stock update
    #[test]
    fn scenario_ecommerce_variant_stock_update() {
        let mut doc = json!({
            "name": "T-Shirt",
            "variants": [
                {"size": "S", "color": "red", "stock": 50, "price": 29.99},
                {"size": "M", "color": "red", "stock": 30, "price": 29.99},
                {"size": "L", "color": "blue", "stock": 10, "price": 34.99}
            ]
        });

        // Customer buys 1x Medium Red → decrement stock
        apply_update(&mut doc, &json!({"$inc": {"variants.1.stock": -1}})).unwrap();
        assert_eq!(doc["variants"][1]["stock"], 29);

        // Set Large Blue to out of stock
        apply_update(&mut doc, &json!({"$set": {"variants.2.stock": 0}})).unwrap();
        assert_eq!(doc["variants"][2]["stock"], 0);

        // Verify array integrity
        assert!(doc["variants"].is_array());
        assert_eq!(doc["variants"].as_array().unwrap().len(), 3);
        assert_eq!(doc["variants"][0]["stock"], 50); // untouched
        assert_eq!(doc["variants"][1]["size"], "M"); // fields preserved
        assert_eq!(doc["variants"][2]["color"], "blue"); // fields preserved
    }
}
