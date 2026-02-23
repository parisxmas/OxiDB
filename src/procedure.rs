use std::collections::HashMap;

use serde_json::{json, Value};

use crate::engine::OxiDb;
use crate::error::{Error, Result};
use crate::tx_log::TransactionId;

/// Parsed procedure definition.
pub struct Procedure {
    pub name: String,
    pub params: Vec<String>,
    pub steps: Vec<Value>,
}

/// Runtime context holding parameter values and step result variables.
struct ProcedureContext {
    params: HashMap<String, Value>,
    vars: HashMap<String, Value>,
}

/// Result of executing a single step.
enum StepResult {
    Continue,
    Return(Value),
    Abort(String),
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

pub fn parse_procedure(value: &Value) -> Result<Procedure> {
    let name = value
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::ProcedureError("missing 'name'".into()))?
        .to_string();

    let params = match value.get("params") {
        Some(Value::Array(arr)) => arr
            .iter()
            .map(|v| {
                v.as_str()
                    .map(String::from)
                    .ok_or_else(|| Error::ProcedureError("params must be strings".into()))
            })
            .collect::<Result<Vec<String>>>()?,
        Some(_) => return Err(Error::ProcedureError("'params' must be an array".into())),
        None => Vec::new(),
    };

    let steps = match value.get("steps") {
        Some(Value::Array(arr)) => arr.clone(),
        Some(_) => return Err(Error::ProcedureError("'steps' must be an array".into())),
        None => return Err(Error::ProcedureError("missing 'steps'".into())),
    };

    if steps.is_empty() {
        return Err(Error::ProcedureError("'steps' must not be empty".into()));
    }

    Ok(Procedure { name, params, steps })
}

// ---------------------------------------------------------------------------
// Variable resolution
// ---------------------------------------------------------------------------

fn resolve_vars(value: &Value, ctx: &ProcedureContext) -> Value {
    match value {
        Value::String(s) => resolve_string_var(s, ctx),
        Value::Array(arr) => Value::Array(arr.iter().map(|v| resolve_vars(v, ctx)).collect()),
        Value::Object(obj) => {
            let mut map = serde_json::Map::new();
            for (k, v) in obj {
                map.insert(k.clone(), resolve_vars(v, ctx));
            }
            Value::Object(map)
        }
        other => other.clone(),
    }
}

fn resolve_string_var(s: &str, ctx: &ProcedureContext) -> Value {
    if !s.starts_with('$') {
        return Value::String(s.to_string());
    }

    let path = &s[1..]; // strip leading $

    // $param.foo or $param.foo.bar
    if let Some(rest) = path.strip_prefix("param.") {
        let parts: Vec<&str> = rest.splitn(2, '.').collect();
        let root = parts[0];
        let val = ctx.params.get(root).cloned().unwrap_or(Value::Null);
        if parts.len() == 2 {
            drill_path(&val, parts[1])
        } else {
            val
        }
    } else {
        // $alias or $alias.field.subfield
        let parts: Vec<&str> = path.splitn(2, '.').collect();
        let root = parts[0];
        let val = ctx.vars.get(root).cloned().unwrap_or(Value::Null);
        if parts.len() == 2 {
            drill_path(&val, parts[1])
        } else {
            val
        }
    }
}

fn drill_path(val: &Value, path: &str) -> Value {
    let mut current = val.clone();
    for part in path.split('.') {
        current = match current {
            Value::Object(ref map) => map.get(part).cloned().unwrap_or(Value::Null),
            Value::Array(ref arr) => {
                if let Ok(idx) = part.parse::<usize>() {
                    arr.get(idx).cloned().unwrap_or(Value::Null)
                } else {
                    Value::Null
                }
            }
            _ => Value::Null,
        };
    }
    current
}

// ---------------------------------------------------------------------------
// Condition evaluation
// ---------------------------------------------------------------------------

fn eval_condition(condition: &Value, ctx: &ProcedureContext) -> Result<bool> {
    let resolved = resolve_vars(condition, ctx);

    match resolved.get("$expr") {
        Some(expr) => eval_expr(expr),
        None => Ok(is_truthy(&resolved)),
    }
}

fn eval_expr(expr: &Value) -> Result<bool> {
    // Simple truthy check: { "$expr": <value> }
    if !expr.is_object() {
        return Ok(is_truthy(expr));
    }

    let obj = expr.as_object().unwrap();
    if obj.len() != 1 {
        return Err(Error::ProcedureError("$expr must have exactly one operator".into()));
    }

    let (op, args) = obj.iter().next().unwrap();
    match op.as_str() {
        "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" => {
            let arr = args
                .as_array()
                .ok_or_else(|| Error::ProcedureError(format!("{op} requires an array of 2 elements")))?;
            if arr.len() != 2 {
                return Err(Error::ProcedureError(format!("{op} requires exactly 2 arguments")));
            }
            let a = &arr[0];
            let b = &arr[1];
            let cmp = compare_values(a, b);
            Ok(match op.as_str() {
                "$eq" => cmp == std::cmp::Ordering::Equal,
                "$ne" => cmp != std::cmp::Ordering::Equal,
                "$gt" => cmp == std::cmp::Ordering::Greater,
                "$gte" => cmp != std::cmp::Ordering::Less,
                "$lt" => cmp == std::cmp::Ordering::Less,
                "$lte" => cmp != std::cmp::Ordering::Greater,
                _ => unreachable!(),
            })
        }
        "$and" => {
            let arr = args
                .as_array()
                .ok_or_else(|| Error::ProcedureError("$and requires an array".into()))?;
            for item in arr {
                if !eval_expr(item)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        "$or" => {
            let arr = args
                .as_array()
                .ok_or_else(|| Error::ProcedureError("$or requires an array".into()))?;
            for item in arr {
                if eval_expr(item)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        "$not" => Ok(!eval_expr(args)?),
        _ => Err(Error::ProcedureError(format!("unknown condition operator: {op}"))),
    }
}

fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(_) => true,
    }
}

fn compare_values(a: &Value, b: &Value) -> std::cmp::Ordering {
    match (a.as_f64(), b.as_f64()) {
        (Some(fa), Some(fb)) => fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal),
        _ => {
            let sa = value_to_sort_string(a);
            let sb = value_to_sort_string(b);
            sa.cmp(&sb)
        }
    }
}

fn value_to_sort_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(v).unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// Step execution
// ---------------------------------------------------------------------------

fn execute_step(
    step: &Value,
    ctx: &mut ProcedureContext,
    db: &OxiDb,
    tx_id: TransactionId,
) -> Result<StepResult> {
    let step_type = step
        .get("step")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::ProcedureError("step missing 'step' field".into()))?;

    match step_type {
        "find" => step_find(step, ctx, db, tx_id),
        "find_one" => step_find_one(step, ctx, db, tx_id),
        "insert" => step_insert(step, ctx, db, tx_id),
        "update" => step_update(step, ctx, db, tx_id),
        "delete" => step_delete(step, ctx, db, tx_id),
        "count" => step_count(step, ctx, db, tx_id),
        "aggregate" => step_aggregate(step, ctx, db),
        "set" => step_set(step, ctx),
        "if" => step_if(step, ctx, db, tx_id),
        "abort" => step_abort(step, ctx),
        "return" => step_return(step, ctx),
        _ => Err(Error::ProcedureError(format!("unknown step type: {step_type}"))),
    }
}

fn get_collection(step: &Value, ctx: &ProcedureContext) -> Result<String> {
    let resolved = resolve_vars(step, ctx);
    resolved
        .get("collection")
        .and_then(|v| v.as_str())
        .map(String::from)
        .ok_or_else(|| Error::ProcedureError("step missing 'collection'".into()))
}

fn step_find(
    step: &Value,
    ctx: &mut ProcedureContext,
    db: &OxiDb,
    tx_id: TransactionId,
) -> Result<StepResult> {
    let col = get_collection(step, ctx)?;
    let query = resolve_vars(step.get("query").unwrap_or(&json!({})), ctx);
    let results = db.tx_find(tx_id, &col, &query)?;
    if let Some(alias) = step.get("as").and_then(|v| v.as_str()) {
        ctx.vars.insert(alias.to_string(), json!(results));
    }
    Ok(StepResult::Continue)
}

fn step_find_one(
    step: &Value,
    ctx: &mut ProcedureContext,
    db: &OxiDb,
    tx_id: TransactionId,
) -> Result<StepResult> {
    let col = get_collection(step, ctx)?;
    let query = resolve_vars(step.get("query").unwrap_or(&json!({})), ctx);
    let results = db.tx_find(tx_id, &col, &query)?;
    let result = results.into_iter().next().unwrap_or(Value::Null);
    if let Some(alias) = step.get("as").and_then(|v| v.as_str()) {
        ctx.vars.insert(alias.to_string(), result);
    }
    Ok(StepResult::Continue)
}

fn step_insert(
    step: &Value,
    ctx: &mut ProcedureContext,
    db: &OxiDb,
    tx_id: TransactionId,
) -> Result<StepResult> {
    let col = get_collection(step, ctx)?;
    let doc = step
        .get("document")
        .ok_or_else(|| Error::ProcedureError("insert step missing 'document'".into()))?;
    let doc = resolve_vars(doc, ctx);
    db.tx_insert(tx_id, &col, doc)?;
    if let Some(alias) = step.get("as").and_then(|v| v.as_str()) {
        ctx.vars.insert(alias.to_string(), json!({"inserted": true}));
    }
    Ok(StepResult::Continue)
}

fn step_update(
    step: &Value,
    ctx: &mut ProcedureContext,
    db: &OxiDb,
    tx_id: TransactionId,
) -> Result<StepResult> {
    let col = get_collection(step, ctx)?;
    let query = resolve_vars(step.get("query").unwrap_or(&json!({})), ctx);
    let update = step
        .get("update")
        .ok_or_else(|| Error::ProcedureError("update step missing 'update'".into()))?;
    let update = resolve_vars(update, ctx);
    db.tx_update(tx_id, &col, &query, &update)?;
    if let Some(alias) = step.get("as").and_then(|v| v.as_str()) {
        ctx.vars.insert(alias.to_string(), json!({"modified": true}));
    }
    Ok(StepResult::Continue)
}

fn step_delete(
    step: &Value,
    ctx: &mut ProcedureContext,
    db: &OxiDb,
    tx_id: TransactionId,
) -> Result<StepResult> {
    let col = get_collection(step, ctx)?;
    let query = resolve_vars(step.get("query").unwrap_or(&json!({})), ctx);
    db.tx_delete(tx_id, &col, &query)?;
    if let Some(alias) = step.get("as").and_then(|v| v.as_str()) {
        ctx.vars.insert(alias.to_string(), json!({"deleted": true}));
    }
    Ok(StepResult::Continue)
}

fn step_count(
    step: &Value,
    ctx: &mut ProcedureContext,
    db: &OxiDb,
    tx_id: TransactionId,
) -> Result<StepResult> {
    let col = get_collection(step, ctx)?;
    let query = resolve_vars(step.get("query").unwrap_or(&json!({})), ctx);
    let results = db.tx_find(tx_id, &col, &query)?;
    let count = results.len();
    if let Some(alias) = step.get("as").and_then(|v| v.as_str()) {
        ctx.vars.insert(alias.to_string(), json!(count));
    }
    Ok(StepResult::Continue)
}

fn step_aggregate(
    step: &Value,
    ctx: &mut ProcedureContext,
    db: &OxiDb,
) -> Result<StepResult> {
    let col = get_collection(step, ctx)?;
    let pipeline = step
        .get("pipeline")
        .ok_or_else(|| Error::ProcedureError("aggregate step missing 'pipeline'".into()))?;
    let pipeline = resolve_vars(pipeline, ctx);
    let results = db.aggregate(&col, &pipeline)?;
    if let Some(alias) = step.get("as").and_then(|v| v.as_str()) {
        ctx.vars.insert(alias.to_string(), json!(results));
    }
    Ok(StepResult::Continue)
}

fn step_set(step: &Value, ctx: &mut ProcedureContext) -> Result<StepResult> {
    let name = step
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::ProcedureError("set step missing 'name'".into()))?;
    let value = step
        .get("value")
        .ok_or_else(|| Error::ProcedureError("set step missing 'value'".into()))?;
    let resolved = resolve_vars(value, ctx);
    ctx.vars.insert(name.to_string(), resolved);
    Ok(StepResult::Continue)
}

fn step_if(
    step: &Value,
    ctx: &mut ProcedureContext,
    db: &OxiDb,
    tx_id: TransactionId,
) -> Result<StepResult> {
    let condition = step
        .get("condition")
        .ok_or_else(|| Error::ProcedureError("if step missing 'condition'".into()))?;
    let condition = resolve_vars(condition, ctx);

    let branch = if eval_condition(&condition, ctx)? {
        step.get("then")
    } else {
        step.get("else")
    };

    if let Some(Value::Array(steps)) = branch {
        for sub_step in steps {
            match execute_step(sub_step, ctx, db, tx_id)? {
                StepResult::Continue => {}
                other => return Ok(other),
            }
        }
    }

    Ok(StepResult::Continue)
}

fn step_abort(step: &Value, ctx: &ProcedureContext) -> Result<StepResult> {
    let msg = step
        .get("message")
        .map(|v| resolve_vars(v, ctx))
        .map(|v| match v {
            Value::String(s) => s,
            _ => v.to_string(),
        })
        .unwrap_or_else(|| "procedure aborted".to_string());
    Ok(StepResult::Abort(msg))
}

fn step_return(step: &Value, ctx: &ProcedureContext) -> Result<StepResult> {
    let value = step
        .get("value")
        .map(|v| resolve_vars(v, ctx))
        .unwrap_or_else(|| json!({"ok": true}));
    Ok(StepResult::Return(value))
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

pub fn execute_procedure(db: &OxiDb, proc_def: &Value, params: &Value) -> Result<Value> {
    let procedure = parse_procedure(proc_def)?;

    // Build parameter map
    let mut param_map = HashMap::new();
    match params {
        Value::Object(obj) => {
            for name in &procedure.params {
                let val = obj.get(name).cloned().unwrap_or(Value::Null);
                param_map.insert(name.clone(), val);
            }
        }
        Value::Array(arr) => {
            for (i, name) in procedure.params.iter().enumerate() {
                let val = arr.get(i).cloned().unwrap_or(Value::Null);
                param_map.insert(name.clone(), val);
            }
        }
        _ => {
            if !procedure.params.is_empty() {
                return Err(Error::ProcedureError("params must be an object or array".into()));
            }
        }
    }

    let mut ctx = ProcedureContext {
        params: param_map,
        vars: HashMap::new(),
    };

    let tx_id = db.begin_transaction();

    let result = (|| -> Result<Value> {
        for step in &procedure.steps {
            match execute_step(step, &mut ctx, db, tx_id)? {
                StepResult::Continue => {}
                StepResult::Return(val) => {
                    db.commit_transaction(tx_id)?;
                    return Ok(val);
                }
                StepResult::Abort(msg) => {
                    let _ = db.rollback_transaction(tx_id);
                    return Err(Error::ProcedureError(msg));
                }
            }
        }

        // No explicit return — commit and return ok
        db.commit_transaction(tx_id)?;
        Ok(json!({"ok": true}))
    })();

    match result {
        Ok(val) => Ok(val),
        Err(e) => {
            // Ensure rollback on any error (commit may have already consumed the tx)
            let _ = db.rollback_transaction(tx_id);
            Err(e)
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::tempdir;

    fn temp_db() -> OxiDb {
        let dir = tempdir().unwrap();
        OxiDb::open(dir.path()).unwrap()
    }

    #[test]
    fn test_parse_procedure() {
        let def = json!({
            "name": "greet",
            "params": ["who"],
            "steps": [
                {"step": "return", "value": {"message": "hello"}}
            ]
        });
        let proc = parse_procedure(&def).unwrap();
        assert_eq!(proc.name, "greet");
        assert_eq!(proc.params, vec!["who"]);
        assert_eq!(proc.steps.len(), 1);
    }

    #[test]
    fn test_parse_missing_steps() {
        let def = json!({"name": "bad"});
        assert!(parse_procedure(&def).is_err());
    }

    #[test]
    fn test_resolve_simple_param() {
        let ctx = ProcedureContext {
            params: [("name".into(), json!("Alice"))].into_iter().collect(),
            vars: HashMap::new(),
        };
        let result = resolve_vars(&json!("$param.name"), &ctx);
        assert_eq!(result, json!("Alice"));
    }

    #[test]
    fn test_resolve_nested_param() {
        let ctx = ProcedureContext {
            params: [("user".into(), json!({"name": "Bob", "age": 30}))].into_iter().collect(),
            vars: HashMap::new(),
        };
        let result = resolve_vars(&json!("$param.user.name"), &ctx);
        assert_eq!(result, json!("Bob"));
    }

    #[test]
    fn test_resolve_var() {
        let ctx = ProcedureContext {
            params: HashMap::new(),
            vars: [("result".into(), json!({"balance": 100}))].into_iter().collect(),
        };
        let result = resolve_vars(&json!("$result.balance"), &ctx);
        assert_eq!(result, json!(100));
    }

    #[test]
    fn test_resolve_in_object() {
        let ctx = ProcedureContext {
            params: [("who".into(), json!("world"))].into_iter().collect(),
            vars: HashMap::new(),
        };
        let result = resolve_vars(&json!({"greeting": "hello", "name": "$param.who"}), &ctx);
        assert_eq!(result, json!({"greeting": "hello", "name": "world"}));
    }

    #[test]
    fn test_eval_condition_lt() {
        let ctx = ProcedureContext {
            params: HashMap::new(),
            vars: HashMap::new(),
        };
        let cond = json!({"$expr": {"$lt": [5, 10]}});
        assert!(eval_condition(&cond, &ctx).unwrap());
    }

    #[test]
    fn test_eval_condition_eq() {
        let ctx = ProcedureContext {
            params: HashMap::new(),
            vars: HashMap::new(),
        };
        let cond = json!({"$expr": {"$eq": [42, 42]}});
        assert!(eval_condition(&cond, &ctx).unwrap());
    }

    #[test]
    fn test_eval_condition_and() {
        let ctx = ProcedureContext {
            params: HashMap::new(),
            vars: HashMap::new(),
        };
        let cond = json!({"$expr": {"$and": [{"$gt": [10, 5]}, {"$lt": [10, 20]}]}});
        assert!(eval_condition(&cond, &ctx).unwrap());
    }

    #[test]
    fn test_eval_condition_not() {
        let ctx = ProcedureContext {
            params: HashMap::new(),
            vars: HashMap::new(),
        };
        let cond = json!({"$expr": {"$not": {"$eq": [1, 2]}}});
        assert!(eval_condition(&cond, &ctx).unwrap());
    }

    #[test]
    fn test_eval_truthy() {
        assert!(is_truthy(&json!(true)));
        assert!(is_truthy(&json!(1)));
        assert!(is_truthy(&json!("hello")));
        assert!(!is_truthy(&Value::Null));
        assert!(!is_truthy(&json!(false)));
        assert!(!is_truthy(&json!(0)));
        assert!(!is_truthy(&json!("")));
    }

    #[test]
    fn test_execute_return_only() {
        let db = temp_db();
        let def = json!({
            "name": "greet",
            "params": ["who"],
            "steps": [
                {"step": "return", "value": {"message": "hello", "name": "$param.who"}}
            ]
        });
        let result = execute_procedure(&db, &def, &json!({"who": "world"})).unwrap();
        assert_eq!(result, json!({"message": "hello", "name": "world"}));
    }

    #[test]
    fn test_execute_insert_and_verify_after_commit() {
        let db = temp_db();
        let def = json!({
            "name": "add_user",
            "params": ["name"],
            "steps": [
                {"step": "insert", "collection": "users", "document": {"name": "$param.name"}},
                {"step": "return", "value": {"inserted": "$param.name"}}
            ]
        });
        let result = execute_procedure(&db, &def, &json!({"name": "Alice"})).unwrap();
        assert_eq!(result, json!({"inserted": "Alice"}));
        // Verify the insert was committed
        let docs = db.find("users", &json!({"name": "Alice"})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], "Alice");
    }

    #[test]
    fn test_execute_abort() {
        let db = temp_db();
        let def = json!({
            "name": "fail",
            "params": [],
            "steps": [
                {"step": "insert", "collection": "data", "document": {"key": "val"}},
                {"step": "abort", "message": "something went wrong"}
            ]
        });
        let result = execute_procedure(&db, &def, &json!({}));
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("something went wrong"));
        // Verify rollback: no data should have been committed
        let docs = db.find("data", &json!({})).unwrap();
        assert_eq!(docs.len(), 0);
    }

    #[test]
    fn test_execute_if_then() {
        let db = temp_db();
        let def = json!({
            "name": "conditional",
            "params": ["val"],
            "steps": [
                {
                    "step": "if",
                    "condition": {"$expr": {"$gt": ["$param.val", 10]}},
                    "then": [
                        {"step": "return", "value": {"result": "big"}}
                    ],
                    "else": [
                        {"step": "return", "value": {"result": "small"}}
                    ]
                }
            ]
        });
        let result = execute_procedure(&db, &def, &json!({"val": 20})).unwrap();
        assert_eq!(result, json!({"result": "big"}));

        let result = execute_procedure(&db, &def, &json!({"val": 5})).unwrap();
        assert_eq!(result, json!({"result": "small"}));
    }

    #[test]
    fn test_execute_set_variable() {
        let db = temp_db();
        let def = json!({
            "name": "set_test",
            "params": ["a", "b"],
            "steps": [
                {"step": "set", "name": "greeting", "value": "hello"},
                {"step": "return", "value": {"msg": "$greeting", "a": "$param.a"}}
            ]
        });
        let result = execute_procedure(&db, &def, &json!({"a": 1, "b": 2})).unwrap();
        assert_eq!(result, json!({"msg": "hello", "a": 1}));
    }

    #[test]
    fn test_execute_count() {
        let db = temp_db();
        db.insert("items", json!({"x": 1})).unwrap();
        db.insert("items", json!({"x": 2})).unwrap();
        db.insert("items", json!({"x": 3})).unwrap();

        let def = json!({
            "name": "count_test",
            "params": [],
            "steps": [
                {"step": "count", "collection": "items", "query": {}, "as": "n"},
                {"step": "return", "value": {"count": "$n"}}
            ]
        });
        let result = execute_procedure(&db, &def, &json!({})).unwrap();
        assert_eq!(result, json!({"count": 3}));
    }

    #[test]
    fn test_execute_delete() {
        let db = temp_db();
        db.insert("items", json!({"x": 1})).unwrap();
        db.insert("items", json!({"x": 2})).unwrap();

        let def = json!({
            "name": "delete_test",
            "params": [],
            "steps": [
                {"step": "delete", "collection": "items", "query": {"x": 1}}
            ]
        });
        let result = execute_procedure(&db, &def, &json!({})).unwrap();
        assert_eq!(result, json!({"ok": true}));
        // Verify the delete was committed
        let docs = db.find("items", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["x"], 2);
    }

    #[test]
    fn test_execute_update() {
        let db = temp_db();
        db.insert("items", json!({"name": "Alice", "score": 10})).unwrap();

        let def = json!({
            "name": "update_test",
            "params": [],
            "steps": [
                {"step": "find_one", "collection": "items", "query": {"name": "Alice"}, "as": "doc"},
                {"step": "update", "collection": "items", "query": {"name": "Alice"}, "update": {"$set": {"score": 99}}},
                {"step": "return", "value": {"old_score": "$doc.score"}}
            ]
        });
        let result = execute_procedure(&db, &def, &json!({})).unwrap();
        assert_eq!(result, json!({"old_score": 10}));
        // Verify the update was committed
        let doc = db.find_one("items", &json!({"name": "Alice"})).unwrap().unwrap();
        assert_eq!(doc["score"], 99);
    }

    #[test]
    fn test_execute_no_explicit_return() {
        let db = temp_db();
        let def = json!({
            "name": "no_return",
            "params": [],
            "steps": [
                {"step": "insert", "collection": "data", "document": {"val": 1}}
            ]
        });
        let result = execute_procedure(&db, &def, &json!({})).unwrap();
        assert_eq!(result, json!({"ok": true}));
        // Verify the insert was committed
        let docs = db.find("data", &json!({})).unwrap();
        assert_eq!(docs.len(), 1);
    }

    #[test]
    fn test_execute_transfer_funds() {
        let db = temp_db();
        db.insert("accounts", json!({"account_id": "A", "balance": 100})).unwrap();
        db.insert("accounts", json!({"account_id": "B", "balance": 50})).unwrap();

        let def = json!({
            "name": "transfer",
            "params": ["from", "to", "amount"],
            "steps": [
                {"step": "find_one", "collection": "accounts", "query": {"account_id": "$param.from"}, "as": "sender"},
                {
                    "step": "if",
                    "condition": {"$expr": {"$lt": ["$sender.balance", "$param.amount"]}},
                    "then": [{"step": "abort", "message": "insufficient funds"}]
                },
                {"step": "update", "collection": "accounts",
                 "query": {"account_id": "$param.from"},
                 "update": {"$inc": {"balance": -30}}},
                {"step": "update", "collection": "accounts",
                 "query": {"account_id": "$param.to"},
                 "update": {"$inc": {"balance": 30}}},
                {"step": "return", "value": {"status": "ok"}}
            ]
        });

        let result = execute_procedure(&db, &def, &json!({"from": "A", "to": "B", "amount": 30})).unwrap();
        assert_eq!(result, json!({"status": "ok"}));

        let a = db.find_one("accounts", &json!({"account_id": "A"})).unwrap().unwrap();
        let b = db.find_one("accounts", &json!({"account_id": "B"})).unwrap().unwrap();
        assert_eq!(a["balance"], 70);
        assert_eq!(b["balance"], 80);
    }

    #[test]
    fn test_execute_transfer_insufficient() {
        let db = temp_db();
        db.insert("accounts", json!({"account_id": "A", "balance": 10})).unwrap();
        db.insert("accounts", json!({"account_id": "B", "balance": 50})).unwrap();

        let def = json!({
            "name": "transfer",
            "params": ["from", "to", "amount"],
            "steps": [
                {"step": "find_one", "collection": "accounts", "query": {"account_id": "$param.from"}, "as": "sender"},
                {
                    "step": "if",
                    "condition": {"$expr": {"$lt": ["$sender.balance", "$param.amount"]}},
                    "then": [{"step": "abort", "message": "insufficient funds"}]
                },
                {"step": "return", "value": {"status": "ok"}}
            ]
        });

        let result = execute_procedure(&db, &def, &json!({"from": "A", "to": "B", "amount": 100}));
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("insufficient funds"));
    }

    #[test]
    fn test_execute_params_as_array() {
        let db = temp_db();
        let def = json!({
            "name": "greet",
            "params": ["who"],
            "steps": [
                {"step": "return", "value": {"name": "$param.who"}}
            ]
        });
        let result = execute_procedure(&db, &def, &json!(["world"])).unwrap();
        assert_eq!(result, json!({"name": "world"}));
    }
}
