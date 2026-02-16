use serde_json::{Map, Value};

use crate::error::{Error, Result};
use crate::pipeline::{resolve_field, set_field};
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
        let fields = fields.as_object().ok_or_else(|| {
            Error::InvalidQuery(format!("{op} value must be an object"))
        })?;
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
                )))
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

fn apply_inc(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, inc_val) in fields {
        let inc = inc_val.as_f64().ok_or_else(|| {
            Error::InvalidQuery(format!("$inc value for '{path}' must be numeric"))
        })?;
        let current = resolve_field(doc, path);
        let new_val = match &current {
            Value::Null => inc,
            v => {
                let cur = v.as_f64().ok_or_else(|| {
                    Error::InvalidQuery(format!(
                        "$inc cannot be applied to non-numeric field '{path}'"
                    ))
                })?;
                cur + inc
            }
        };
        set_field(doc, path, number_to_value(new_val));
    }
    Ok(())
}

fn apply_mul(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, mul_val) in fields {
        let mul = mul_val.as_f64().ok_or_else(|| {
            Error::InvalidQuery(format!("$mul value for '{path}' must be numeric"))
        })?;
        let current = resolve_field(doc, path);
        let new_val = match &current {
            Value::Null => 0.0,
            v => {
                let cur = v.as_f64().ok_or_else(|| {
                    Error::InvalidQuery(format!(
                        "$mul cannot be applied to non-numeric field '{path}'"
                    ))
                })?;
                cur * mul
            }
        };
        set_field(doc, path, number_to_value(new_val));
    }
    Ok(())
}

fn apply_min(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, new_val) in fields {
        let current = resolve_field(doc, path);
        if current.is_null() {
            set_field(doc, path, new_val.clone());
        } else {
            let cur_iv = IndexValue::from_json(&current);
            let new_iv = IndexValue::from_json(new_val);
            if new_iv < cur_iv {
                set_field(doc, path, new_val.clone());
            }
        }
    }
    Ok(())
}

fn apply_max(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, new_val) in fields {
        let current = resolve_field(doc, path);
        if current.is_null() {
            set_field(doc, path, new_val.clone());
        } else {
            let cur_iv = IndexValue::from_json(&current);
            let new_iv = IndexValue::from_json(new_val);
            if new_iv > cur_iv {
                set_field(doc, path, new_val.clone());
            }
        }
    }
    Ok(())
}

fn apply_rename(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (old_path, new_path_val) in fields {
        let new_path = new_path_val.as_str().ok_or_else(|| {
            Error::InvalidQuery(format!(
                "$rename target for '{old_path}' must be a string"
            ))
        })?;
        let val = resolve_field(doc, old_path);
        if !val.is_null() {
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

fn apply_push(doc: &mut Value, fields: &Map<String, Value>) -> Result<()> {
    for (path, value) in fields {
        let current = resolve_field(doc, path);
        match &current {
            Value::Null => {
                set_field(doc, path, Value::Array(vec![value.clone()]));
            }
            Value::Array(arr) => {
                let mut new_arr = arr.clone();
                new_arr.push(value.clone());
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
        let current = resolve_field(doc, path);
        match &current {
            Value::Null => {} // no-op
            Value::Array(arr) => {
                let new_arr: Vec<Value> =
                    arr.iter().filter(|el| *el != match_val).cloned().collect();
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
        let current = resolve_field(doc, path);
        match &current {
            Value::Null => {
                set_field(doc, path, Value::Array(vec![value.clone()]));
            }
            Value::Array(arr) => {
                if !arr.contains(value) {
                    let mut new_arr = arr.clone();
                    new_arr.push(value.clone());
                    set_field(doc, path, Value::Array(new_arr));
                }
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
        let current = resolve_field(doc, path);
        match &current {
            Value::Null => {} // no-op
            Value::Array(arr) => {
                if arr.is_empty() {
                    continue;
                }
                let dir = dir_val.as_i64().ok_or_else(|| {
                    Error::InvalidQuery(format!(
                        "$pop value for '{path}' must be 1 or -1"
                    ))
                })?;
                let mut new_arr = arr.clone();
                match dir {
                    1 => {
                        new_arr.pop();
                    }
                    -1 => {
                        new_arr.remove(0);
                    }
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "$pop value for '{path}' must be 1 or -1"
                        )));
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
            _ => return,
        }
    }
    if let Value::Object(map) = current {
        map.remove(parts[parts.len() - 1]);
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

    // -----------------------------------------------------------------------
    // Multiple operators in one update
    // -----------------------------------------------------------------------

    #[test]
    fn multiple_operators() {
        let mut doc = json!({"a": 1, "b": 10});
        apply_update(
            &mut doc,
            &json!({"$set": {"a": 99}, "$inc": {"b": 5}}),
        )
        .unwrap();
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
}
