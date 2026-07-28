//! Security Rules — per-collection document-level access control.
//!
//! Rules are stored in `_security_rules` collection. Each rule set applies to
//! one collection and defines conditions for `read`, `create`, `update`, `delete`.
//!
//! # Rule expressions
//!
//! Simple expression language evaluated at request time:
//!
//! | Expression | Meaning |
//! |------------|---------|
//! | `true` / `false` | Allow / deny |
//! | `auth != null` | Authenticated user |
//! | `auth.role == 'admin'` | Role check |
//! | `auth.username == doc.owner` | Document ownership (on create, `doc` is the row being created) |
//! | `auth.username == newDoc.owner` | The incoming row, when an update rule must compare it with the stored one |
//! | `A && B`, `A \|\| B` | Logical AND / OR |
//!
//! # Commands
//!
//! ```json
//! {"cmd": "set_rules", "collection": "posts", "rules": {"read": "true", "create": "auth != null", "update": "auth.username == doc.author", "delete": "auth.role == 'admin'"}}
//! {"cmd": "get_rules", "collection": "posts"}
//! {"cmd": "delete_rules", "collection": "posts"}
//! ```

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use oxidb::OxiDb;
use serde_json::{Value, json};

const RULES_COLLECTION: &str = "_security_rules";

/// Resolved auth context from JWT claims.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub username: Option<String>,
    pub role: Option<String>,
}

impl AuthContext {
    pub fn anonymous() -> Self {
        Self {
            username: None,
            role: None,
        }
    }

    pub fn from_claims(username: &str, role: &str) -> Self {
        Self {
            username: Some(username.to_string()),
            role: Some(role.to_string()),
        }
    }

    pub fn is_authenticated(&self) -> bool {
        self.username.is_some()
    }
}

/// The four operations that can be controlled.
#[derive(Debug, Clone, Copy)]
pub enum Operation {
    Read,
    Create,
    Update,
    Delete,
}

impl Operation {
    fn as_str(&self) -> &'static str {
        match self {
            Operation::Read => "read",
            Operation::Create => "create",
            Operation::Update => "update",
            Operation::Delete => "delete",
        }
    }
}

/// Rule set for a collection.
#[derive(Debug, Clone)]
pub struct RuleSet {
    pub read: String,
    pub create: String,
    pub update: String,
    pub delete: String,
    /// Optional per-operation rate limits, keyed by operation name.
    pub rate: HashMap<String, Rate>,
}

/// "at most `limit` of this operation per `window_secs`, per identity".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rate {
    pub limit: u32,
    pub window_secs: u64,
}

impl Rate {
    /// Parse the compact spelling a rule is written in: `10/min`, `100/hour`,
    /// `5/sec`, `2000/day`.
    pub fn parse(spec: &str) -> Result<Rate, String> {
        let (count, unit) = spec
            .split_once('/')
            .ok_or_else(|| format!("expected `<count>/<unit>`, got `{spec}`"))?;
        let limit: u32 = count
            .trim()
            .parse()
            .map_err(|_| format!("`{count}` is not a count"))?;
        if limit == 0 {
            return Err("a limit of 0 would deny everything — remove the rule instead".into());
        }
        let window_secs = match unit.trim().to_ascii_lowercase().as_str() {
            "s" | "sec" | "second" => 1,
            "m" | "min" | "minute" => 60,
            "h" | "hr" | "hour" => 3_600,
            "d" | "day" => 86_400,
            other => {
                return Err(format!(
                    "unknown unit `{other}` — use sec, min, hour or day"
                ));
            }
        };
        Ok(Rate { limit, window_secs })
    }

    /// The spelling a rule is written in — so a stored limit can be shown back
    /// to whoever set it, and round-trips through `parse`.
    pub fn spec(&self) -> String {
        let unit = match self.window_secs {
            1 => "sec",
            60 => "min",
            3_600 => "hour",
            _ => "day",
        };
        format!("{}/{unit}", self.limit)
    }

    fn describe(&self) -> String {
        let unit = match self.window_secs {
            1 => "second",
            60 => "minute",
            3_600 => "hour",
            _ => "day",
        };
        format!("{} per {unit}", self.limit)
    }
}

/// Why a write was refused. A rule saying "no" and a rule saying "not yet" are
/// different answers, and the HTTP layer owes the caller different statuses:
/// 403 is permanent, 429 is an invitation to retry.
#[derive(Debug, Clone)]
pub struct Denied {
    pub message: String,
    /// Seconds until the operation would be allowed again, when it is a rate.
    pub retry_after: Option<u64>,
}

impl Denied {
    fn access(message: impl Into<String>) -> Denied {
        Denied {
            message: message.into(),
            retry_after: None,
        }
    }
}

impl std::fmt::Display for Denied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl From<String> for Denied {
    fn from(message: String) -> Denied {
        Denied::access(message)
    }
}

// ---------------------------------------------------------------------------
// Rule storage
// ---------------------------------------------------------------------------

pub fn set_rules(db: &OxiDb, collection: &str, rules: &Value) -> Result<(), String> {
    // Validate every rule expression against the grammar *before* persisting, so
    // a typo can't silently become a fail-closed "deny all". An unknown term
    // (e.g. `dytjuer`) resolves to null → falsy at eval time with no error; here
    // we reject it up front with a clear message instead.
    let read = rules["read"].as_str().unwrap_or("true");
    let create = rules["create"].as_str().unwrap_or("true");
    let update = rules["update"].as_str().unwrap_or("true");
    let delete = rules["delete"].as_str().unwrap_or("true");
    for (field, expr) in [
        ("read", read),
        ("create", create),
        ("update", update),
        ("delete", delete),
    ] {
        validate_rule_expr(expr).map_err(|e| format!("invalid `{field}` rule: {e}"))?;
    }

    // Optional rates: `{"rate": {"create": "10/min", "delete": "30/hour"}}`.
    // Validated here too, so an unparseable spec is a clear error at save time
    // rather than a limit that silently never applies.
    let mut rate = serde_json::Map::new();
    if let Some(spec) = rules.get("rate") {
        let obj = spec
            .as_object()
            .ok_or_else(|| "`rate` must be an object of operation → limit".to_string())?;
        for (op, value) in obj {
            if !matches!(op.as_str(), "read" | "create" | "update" | "delete") {
                return Err(format!("unknown operation `{op}` in `rate`"));
            }
            let text = value
                .as_str()
                .ok_or_else(|| format!("`rate.{op}` must be a string like \"10/min\""))?;
            Rate::parse(text).map_err(|e| format!("invalid `rate.{op}`: {e}"))?;
            rate.insert(op.clone(), json!(text));
        }
    }

    let rule_doc = json!({
        "collection": collection,
        "read": read,
        "create": create,
        "update": update,
        "delete": delete,
        "rate": Value::Object(rate),
    });
    // Upsert: drop any existing rule(s) for this collection, then insert.
    remove_rules(db, collection);
    db.insert(RULES_COLLECTION, rule_doc)
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Validate a rule expression against the same grammar [`eval_rule_expr`]
/// interprets — mirroring its structure exactly, so anything accepted here
/// evaluates meaningfully and anything the evaluator would silently treat as an
/// unknown (falsy) term is rejected with a message. `Ok(())` = syntactically
/// valid, `Err(msg)` = a term/shape the evaluator cannot make sense of.
pub fn validate_rule_expr(expr: &str) -> Result<(), String> {
    let e = expr.trim();
    if e.is_empty() {
        return Err("expression is empty".to_string());
    }
    if e == "true" || e == "false" {
        return Ok(());
    }
    // Boolean combinators (same precedence + paren/string awareness as eval).
    if let Some((l, r)) = split_logical(e, "||") {
        validate_rule_expr(l)?;
        return validate_rule_expr(r);
    }
    if let Some((l, r)) = split_logical(e, "&&") {
        validate_rule_expr(l)?;
        return validate_rule_expr(r);
    }
    if let Some(inner) = e.strip_prefix('!') {
        return validate_rule_expr(inner.trim());
    }
    if e.starts_with('(') && e.ends_with(')') {
        return validate_rule_expr(&e[1..e.len() - 1]);
    }
    // Comparison — both sides must be valid atoms.
    for op in ["!=", "=="] {
        if let Some((l, r)) = e.split_once(op) {
            validate_atom(l.trim())?;
            return validate_atom(r.trim());
        }
    }
    // Bare atom (truthy check at eval time).
    validate_atom(e)
}

/// Validate a single operand: a literal, `auth`, `auth.username`/`auth.role`, or
/// `doc.<field path>`.
fn validate_atom(tok: &str) -> Result<(), String> {
    let t = tok.trim();
    if t.is_empty() {
        return Err("expected a value".to_string());
    }
    // String literal ('x' or "x").
    if (t.starts_with('\'') && t.ends_with('\'') && t.len() >= 2)
        || (t.starts_with('"') && t.ends_with('"') && t.len() >= 2)
    {
        return Ok(());
    }
    // Numeric literal.
    if t.parse::<f64>().is_ok() {
        return Ok(());
    }
    match t {
        "true" | "false" | "null" | "auth" => return Ok(()),
        _ => {}
    }
    if let Some(field) = t.strip_prefix("auth.") {
        return match field {
            "username" | "role" => Ok(()),
            _ => Err(format!(
                "unknown field `auth.{field}` (only auth.username and auth.role)"
            )),
        };
    }
    for prefix in ["doc.", "newDoc."] {
        if let Some(field) = t.strip_prefix(prefix) {
            let ok = !field.is_empty()
                && field.split('.').all(|seg| {
                    !seg.is_empty() && seg.chars().all(|c| c.is_alphanumeric() || c == '_')
                });
            return if ok {
                Ok(())
            } else {
                Err(format!("invalid document field `{prefix}{field}`"))
            };
        }
    }
    Err(format!(
        "unknown term `{t}` — use auth, auth.username, auth.role, doc.<field>, newDoc.<field>, a 'string', true/false/null, or a number"
    ))
}

pub fn get_rules(db: &OxiDb, collection: &str) -> Option<RuleSet> {
    // Scan the (tiny) rules collection and filter in memory rather than relying
    // on a secondary index over `collection`: that index could go stale across a
    // restart, silently dropping rules (fail-closed) and letting duplicates
    // accumulate. Pick the most recent row if duplicates exist.
    let docs = db.find(RULES_COLLECTION, &json!({})).ok()?;
    let doc = docs
        .into_iter()
        .rev()
        .find(|d| d.get("collection").and_then(|v| v.as_str()) == Some(collection))?;
    let mut rate = HashMap::new();
    if let Some(obj) = doc.get("rate").and_then(|v| v.as_object()) {
        for (op, value) in obj {
            if let Some(parsed) = value.as_str().and_then(|t| Rate::parse(t).ok()) {
                rate.insert(op.clone(), parsed);
            }
        }
    }
    Some(RuleSet {
        read: doc["read"].as_str().unwrap_or("true").to_string(),
        create: doc["create"].as_str().unwrap_or("true").to_string(),
        update: doc["update"].as_str().unwrap_or("true").to_string(),
        delete: doc["delete"].as_str().unwrap_or("true").to_string(),
        rate,
    })
}

pub fn delete_rules(db: &OxiDb, collection: &str) -> Result<(), String> {
    remove_rules(db, collection);
    Ok(())
}

/// Delete every rule row for `collection`, addressing each by its `_id`
/// (primary key) after a full scan — so it works regardless of the state of any
/// secondary index and also sweeps up duplicates.
fn remove_rules(db: &OxiDb, collection: &str) {
    let Ok(docs) = db.find(RULES_COLLECTION, &json!({})) else {
        return;
    };
    for d in docs {
        if d.get("collection").and_then(|v| v.as_str()) == Some(collection)
            && let Some(id) = d.get("_id")
        {
            let _ = db.delete(RULES_COLLECTION, &json!({ "_id": id }));
        }
    }
}

// ---------------------------------------------------------------------------
// Rule evaluation
// ---------------------------------------------------------------------------

/// Check if an operation is allowed by the security rules.
///
/// - `auth`: the authenticated user context (or anonymous)
/// - `doc`: the existing document (for read/update/delete), None for create
/// - `new_doc`: the incoming document (for create/update), None for read/delete
///
/// Is this an untrusted caller — the public anon key, or a signed-in end user?
/// Those are the tiers a `_`-prefixed system collection must never be reachable
/// from: the server keeps its own bookkeeping there (request logs, the slow
/// query profile, alerts), those collections are exempt from the rules, and
/// they do not count against a project's collection quota.
fn untrusted(auth: &AuthContext) -> bool {
    matches!(
        auth.role.as_deref().and_then(crate::auth::Role::from_str),
        Some(crate::auth::Role::Read) | Some(crate::auth::Role::Authenticated)
    )
}

/// Returns `Ok(())` if allowed, `Err(message)` if denied.
pub fn check_access(
    db: &OxiDb,
    collection: &str,
    op: Operation,
    auth: &AuthContext,
    doc: Option<&Value>,
    new_doc: Option<&Value>,
) -> Result<(), Denied> {
    // System collections carry no rules — which is precisely why an untrusted
    // key must not reach them. Skipping the rules was meant for the server's
    // own bookkeeping, not for a browser: it let any anon key read the request
    // log and create unlimited `_`-named collections outside the quota.
    if collection.starts_with('_') {
        return if untrusted(auth) {
            Err(Denied::access(format!(
                "access denied: '{collection}' is a system collection"
            )))
        } else {
            Ok(())
        };
    }

    // service_role / admin bypasses rules entirely — the Supabase service_role
    // semantic: a trusted server-side key is not subject to per-row policy.
    if matches!(
        auth.role.as_deref().and_then(crate::auth::Role::from_str),
        Some(crate::auth::Role::Admin)
    ) {
        return Ok(());
    }

    let rules = match get_rules(db, collection) {
        Some(r) => r,
        None => {
            // No rules defined. Reads stay open and trusted roles keep full
            // access (backward compatible). But a WRITE from an untrusted tier —
            // the public anon key (role "read") or a signed-in end-user (role
            // "authenticated") — is denied by default: a collection must opt in
            // with an explicit rule (the Supabase RLS model). An open, no-auth
            // server (role `None`) is unaffected.
            let unprivileged_write = !matches!(op, Operation::Read)
                && matches!(
                    auth.role.as_deref().and_then(crate::auth::Role::from_str),
                    Some(crate::auth::Role::Read) | Some(crate::auth::Role::Authenticated)
                );
            return if unprivileged_write {
                Err(Denied::access(format!(
                    "access denied: {} on '{}' requires a security rule",
                    op.as_str(),
                    collection
                )))
            } else {
                Ok(())
            };
        }
    };

    let expr = match op {
        Operation::Read => &rules.read,
        Operation::Create => &rules.create,
        Operation::Update => &rules.update,
        Operation::Delete => &rules.delete,
    };

    // On create there is no existing row, so `doc.` means the row being
    // created — otherwise every ownership rule (`auth.username == doc.owner`)
    // compares against null and denies the very insert it was written to
    // permit. `newDoc.` stays available for update rules that compare the
    // incoming row against the stored one.
    let doc = match op {
        Operation::Create => new_doc,
        _ => doc,
    };

    if !eval_rule_expr(expr, auth, doc, new_doc) {
        return Err(Denied::access(format!(
            "access denied: {} on '{}' not allowed",
            op.as_str(),
            collection
        )));
    }

    // The rule permits it — but perhaps not this often. A rate is checked last,
    // so a refused write never consumes budget, and it is counted per identity:
    // one account flooding a collection cannot throttle everybody else.
    if let Some(rate) = rules.rate.get(op.as_str()) {
        let identity = auth.username.as_deref().unwrap_or("anonymous");
        if let Some(retry_after) = rate_limited(collection, op.as_str(), identity, *rate) {
            return Err(Denied {
                message: format!(
                    "rate limit exceeded: {} on '{}' is limited to {}",
                    op.as_str(),
                    collection,
                    rate.describe()
                ),
                retry_after: Some(retry_after),
            });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Rate limiting
//
// A fixed window per (collection, operation, identity). In-process, like every
// other counter here: two data-plane nodes would each allow the limit, which is
// the usual trade for not putting a write on the hot path of every request.
// ---------------------------------------------------------------------------

/// Above this many tracked keys, expired windows are swept, so a busy project
/// cannot grow the map without bound.
const RATE_SWEEP_ABOVE: usize = 8_192;

fn rate_counters() -> &'static Mutex<HashMap<String, (u32, u64)>> {
    static C: OnceLock<Mutex<HashMap<String, (u32, u64)>>> = OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `Some(retry_after_secs)` when this operation is over its limit — and it is
/// then *not* counted, so hammering a closed window cannot extend it.
fn rate_limited(collection: &str, op: &str, identity: &str, rate: Rate) -> Option<u64> {
    let mut map = rate_counters().lock().ok()?;
    let now = now_secs();
    if map.len() > RATE_SWEEP_ABOVE {
        map.retain(|_, (_, start)| now.saturating_sub(*start) < 86_400);
    }
    let key = format!("{collection}\u{1}{op}\u{1}{identity}");
    let entry = map.entry(key).or_insert((0, now));
    if now.saturating_sub(entry.1) >= rate.window_secs {
        *entry = (0, now);
    }
    if entry.0 >= rate.limit {
        return Some((rate.window_secs - now.saturating_sub(entry.1)).max(1));
    }
    entry.0 += 1;
    None
}

/// The outcome of a read check that supports **row-level** filtering.
pub enum ReadAccess {
    /// Allowed with no per-row filtering (no rule, or a row-independent rule
    /// that passed).
    All,
    /// Denied entirely (a row-independent rule that failed).
    None,
    /// Allowed, but each returned row must pass this rule expression.
    Filter(String),
}

/// Decide read access for `collection`, enabling **row-level** filtering when
/// the read rule references `doc.<field>` (true RLS: a user sees only the rows
/// the rule admits). System collections and service_role/admin bypass; a
/// collection with no rules is fully readable.
pub fn read_access(db: &OxiDb, collection: &str, auth: &AuthContext) -> ReadAccess {
    if collection.starts_with('_') {
        // See `check_access`: system collections are the server's own, and an
        // untrusted key has no business reading them.
        return if untrusted(auth) {
            ReadAccess::None
        } else {
            ReadAccess::All
        };
    }
    if matches!(
        auth.role.as_deref().and_then(crate::auth::Role::from_str),
        Some(crate::auth::Role::Admin)
    ) {
        return ReadAccess::All;
    }
    let rules = match get_rules(db, collection) {
        Some(r) => r,
        None => return ReadAccess::All, // no rules → reads stay open
    };
    let expr = rules.read.trim();
    // A rule that references the row (`doc.<field>`) is evaluated per returned
    // row; a row-independent rule is a one-shot collection gate.
    if expr.contains("doc.") {
        ReadAccess::Filter(expr.to_string())
    } else if eval_rule_expr(expr, auth, None, None) {
        ReadAccess::All
    } else {
        ReadAccess::None
    }
}

/// Whether one row is visible under a read rule (the per-row RLS check).
pub fn row_visible(rule: &str, auth: &AuthContext, row: &Value) -> bool {
    eval_rule_expr(rule, auth, Some(row), None)
}

/// Evaluate a rule expression string. Returns true if access is granted.
fn eval_rule_expr(
    expr: &str,
    auth: &AuthContext,
    doc: Option<&Value>,
    new_doc: Option<&Value>,
) -> bool {
    let expr = expr.trim();

    // Literals
    if expr == "true" {
        return true;
    }
    if expr == "false" {
        return false;
    }

    // Logical OR (lowest precedence) — split on ||
    if let Some((left, right)) = split_logical(expr, "||") {
        return eval_rule_expr(left, auth, doc, new_doc)
            || eval_rule_expr(right, auth, doc, new_doc);
    }

    // Logical AND — split on &&
    if let Some((left, right)) = split_logical(expr, "&&") {
        return eval_rule_expr(left, auth, doc, new_doc)
            && eval_rule_expr(right, auth, doc, new_doc);
    }

    // Negation
    if let Some(inner) = expr.strip_prefix('!') {
        return !eval_rule_expr(inner.trim(), auth, doc, new_doc);
    }

    // Parenthesized expression
    if expr.starts_with('(') && expr.ends_with(')') {
        return eval_rule_expr(&expr[1..expr.len() - 1], auth, doc, new_doc);
    }

    // Comparison operators
    for op in ["!=", "=="] {
        if let Some((left, right)) = expr.split_once(op) {
            let left_val = resolve_value(left.trim(), auth, doc, new_doc);
            let right_val = resolve_value(right.trim(), auth, doc, new_doc);
            return match op {
                "==" => values_equal(&left_val, &right_val),
                "!=" => !values_equal(&left_val, &right_val),
                _ => false,
            };
        }
    }

    // Bare variable — truthy check (e.g., "auth" → is authenticated?)
    let val = resolve_value(expr, auth, doc, new_doc);
    is_truthy(&val)
}

/// Split an expression on a logical operator, respecting parentheses.
fn split_logical<'a>(expr: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    let mut depth = 0;
    let op_bytes = op.as_bytes();
    let bytes = expr.as_bytes();
    let mut in_string = false;
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'\'' {
            in_string = !in_string;
            i += 1;
            continue;
        }
        if in_string {
            i += 1;
            continue;
        }
        if bytes[i] == b'(' {
            depth += 1;
        }
        if bytes[i] == b')' {
            depth -= 1;
        }
        if depth == 0
            && i + op_bytes.len() <= bytes.len()
            && &bytes[i..i + op_bytes.len()] == op_bytes
        {
            return Some((&expr[..i], &expr[i + op_bytes.len()..]));
        }
        i += 1;
    }
    None
}

/// Resolve a value reference to a concrete Value.
fn resolve_value(
    token: &str,
    auth: &AuthContext,
    doc: Option<&Value>,
    new_doc: Option<&Value>,
) -> Value {
    let token = token.trim();

    // String literal
    if (token.starts_with('\'') && token.ends_with('\''))
        || (token.starts_with('"') && token.ends_with('"'))
    {
        return Value::String(token[1..token.len() - 1].to_string());
    }

    // Numeric literal
    if let Ok(n) = token.parse::<f64>() {
        return json!(n);
    }

    // Boolean / null literals
    match token {
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        "null" => return Value::Null,
        _ => {}
    }

    // auth / auth.field
    if token == "auth" {
        if auth.is_authenticated() {
            return json!({"username": auth.username, "role": auth.role});
        } else {
            return Value::Null;
        }
    }
    if let Some(field) = token.strip_prefix("auth.") {
        return match field {
            "username" => auth
                .username
                .as_ref()
                .map(|s| json!(s))
                .unwrap_or(Value::Null),
            "role" => auth.role.as_ref().map(|s| json!(s)).unwrap_or(Value::Null),
            _ => Value::Null,
        };
    }

    // doc.field — existing document
    if let Some(field) = token.strip_prefix("doc.") {
        return doc
            .and_then(|d| resolve_dotted_field(d, field))
            .unwrap_or(Value::Null);
    }

    // newDoc.field — incoming document
    if let Some(field) = token.strip_prefix("newDoc.") {
        return new_doc
            .and_then(|d| resolve_dotted_field(d, field))
            .unwrap_or(Value::Null);
    }

    Value::Null
}

fn resolve_dotted_field(doc: &Value, path: &str) -> Option<Value> {
    let mut current = doc;
    for part in path.split('.') {
        current = current.get(part)?;
    }
    Some(current.clone())
}

fn values_equal(a: &Value, b: &Value) -> bool {
    // Handle null comparisons
    if a.is_null() && b.is_null() {
        return true;
    }
    if a.is_null() || b.is_null() {
        return false;
    }
    a == b
}

fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(_) => true,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod rate_limit_tests {
    use super::*;
    use serde_json::json;

    fn user(email: &str) -> AuthContext {
        AuthContext::from_claims(email, "authenticated")
    }

    #[test]
    fn rate_specs_parse_the_way_they_are_written() {
        assert_eq!(
            Rate::parse("10/min").unwrap(),
            Rate {
                limit: 10,
                window_secs: 60
            }
        );
        assert_eq!(
            Rate::parse("5/sec").unwrap(),
            Rate {
                limit: 5,
                window_secs: 1
            }
        );
        assert_eq!(
            Rate::parse("100/hour").unwrap(),
            Rate {
                limit: 100,
                window_secs: 3600
            }
        );
        assert_eq!(
            Rate::parse("2000/day").unwrap(),
            Rate {
                limit: 2000,
                window_secs: 86400
            }
        );
        // A limit of zero is a denial dressed as a rate; say so rather than
        // silently blocking every write.
        assert!(Rate::parse("0/min").is_err());
        assert!(Rate::parse("10/fortnight").is_err());
        assert!(Rate::parse("lots/min").is_err());
        assert!(Rate::parse("10").is_err());
    }

    #[test]
    fn a_stored_rate_can_be_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        set_rules(
            &db,
            "posts",
            &json!({ "read": "true", "create": "true", "update": "true", "delete": "true",
                     "rate": { "create": "5/hour", "delete": "20/min" } }),
        )
        .unwrap();
        let stored = get_rules(&db, "posts").unwrap();
        assert_eq!(stored.rate.get("create").unwrap().spec(), "5/hour");
        assert_eq!(stored.rate.get("delete").unwrap().spec(), "20/min");
        // And the spelling round-trips, so what is shown can be saved again.
        assert_eq!(
            Rate::parse("5/hour").unwrap(),
            *stored.rate.get("create").unwrap()
        );
    }

    #[test]
    fn a_rate_is_per_identity_and_per_operation() {
        let dir = tempfile::tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        set_rules(
            &db,
            "posts",
            &json!({
                "read": "true",
                "create": "auth != null",
                "update": "auth != null",
                "delete": "auth != null",
                "rate": { "create": "3/min" }
            }),
        )
        .unwrap();

        let ada = user("ada@example.com");
        let kai = user("kai@example.com");
        let doc = json!({ "body": "hi" });

        for i in 0..3 {
            assert!(
                check_access(&db, "posts", Operation::Create, &ada, None, Some(&doc)).is_ok(),
                "create {i} should pass"
            );
        }
        // The fourth is refused, and says when to come back.
        let denied = check_access(&db, "posts", Operation::Create, &ada, None, Some(&doc))
            .expect_err("fourth create is over the limit");
        assert!(
            denied.retry_after.is_some(),
            "a rate must say how long to wait"
        );
        assert!(denied.message.contains("rate limit"));

        // Another account is unaffected — one user flooding must not throttle
        // everyone else, which a per-collection counter would do.
        assert!(check_access(&db, "posts", Operation::Create, &kai, None, Some(&doc)).is_ok());

        // And a different operation has its own budget.
        assert!(check_access(&db, "posts", Operation::Update, &ada, Some(&doc), None).is_ok());
    }

    #[test]
    fn a_refused_write_does_not_consume_budget() {
        let dir = tempfile::tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        // The rule denies everything, and a rate is also set.
        set_rules(
            &db,
            "locked",
            &json!({ "read": "true", "create": "false", "update": "false", "delete": "false",
                     "rate": { "create": "2/min" } }),
        )
        .unwrap();
        let ada = user("ada@example.com");
        for _ in 0..5 {
            let e = check_access(
                &db,
                "locked",
                Operation::Create,
                &ada,
                None,
                Some(&json!({})),
            )
            .expect_err("denied by the rule");
            // Always the rule's answer, never "you are over your limit" — the
            // rate is checked after the rule, so refused writes cost nothing.
            assert!(
                e.retry_after.is_none(),
                "a denied write must not consume the rate budget"
            );
        }
    }

    #[test]
    fn service_role_is_not_rate_limited() {
        let dir = tempfile::tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        set_rules(
            &db,
            "posts",
            &json!({ "read": "true", "create": "auth != null", "update": "true", "delete": "true",
                     "rate": { "create": "1/day" } }),
        )
        .unwrap();
        let service = AuthContext::from_claims("service", "admin");
        for _ in 0..5 {
            assert!(
                check_access(
                    &db,
                    "posts",
                    Operation::Create,
                    &service,
                    None,
                    Some(&json!({}))
                )
                .is_ok()
            );
        }
    }

    #[test]
    fn an_unparseable_rate_is_refused_at_save_time() {
        let dir = tempfile::tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        let bad = set_rules(
            &db,
            "posts",
            &json!({ "read": "true", "create": "true", "update": "true", "delete": "true",
                     "rate": { "create": "ten a minute" } }),
        );
        assert!(
            bad.is_err(),
            "a limit that cannot be parsed would never apply"
        );
        let unknown_op = set_rules(
            &db,
            "posts",
            &json!({ "read": "true", "create": "true", "update": "true", "delete": "true",
                     "rate": { "publish": "10/min" } }),
        );
        assert!(unknown_op.is_err());
    }
}

#[cfg(test)]
mod system_collection_tests {
    use super::*;
    use serde_json::json;

    fn anon() -> AuthContext {
        AuthContext::from_claims("anon@key", "read")
    }
    fn end_user() -> AuthContext {
        AuthContext::from_claims("ada@example.com", "authenticated")
    }
    fn service() -> AuthContext {
        AuthContext::from_claims("service", "admin")
    }

    #[test]
    fn untrusted_keys_cannot_touch_system_collections() {
        let dir = tempfile::tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        for auth in [anon(), end_user()] {
            // Reads: the request log and slow-query profile live here.
            assert!(matches!(
                read_access(&db, "_profile", &auth),
                ReadAccess::None
            ));
            // Writes: `_`-named collections skip the rules *and* the project's
            // collection quota, so this was a way to fill a tenant's storage.
            assert!(
                check_access(
                    &db,
                    "_evil",
                    Operation::Create,
                    &auth,
                    None,
                    Some(&json!({"x": 1}))
                )
                .is_err()
            );
        }
    }

    #[test]
    fn the_server_and_service_role_still_reach_them() {
        let dir = tempfile::tempdir().unwrap();
        let db = OxiDb::open(dir.path()).unwrap();
        assert!(matches!(
            read_access(&db, "_profile", &service()),
            ReadAccess::All
        ));
        assert!(
            check_access(
                &db,
                "_profile",
                Operation::Create,
                &service(),
                None,
                Some(&json!({}))
            )
            .is_ok()
        );
        // An open, no-auth server (no role at all) is unaffected.
        let open = AuthContext::anonymous();
        assert!(matches!(
            read_access(&db, "_profile", &open),
            ReadAccess::All
        ));
    }
}

#[cfg(test)]
mod create_ownership_tests {
    use super::{Operation, eval_rule_expr, validate_rule_expr};
    use crate::rules::AuthContext;
    use serde_json::json;

    fn user(email: &str) -> AuthContext {
        AuthContext::from_claims(email, "authenticated")
    }

    /// The rule a real app writes: you may only create rows that are yours.
    /// It has to see the row being created — there is no stored one yet.
    #[test]
    fn ownership_on_create_reads_the_incoming_row() {
        let expr = "auth.username == doc.owner";
        assert!(
            validate_rule_expr(expr).is_ok(),
            "the expression must be accepted"
        );

        let incoming = json!({ "owner": "ada@example.com", "note": "hi" });
        // Mirrors check_access's binding for Create: `doc` is the new row.
        assert!(
            eval_rule_expr(
                expr,
                &user("ada@example.com"),
                Some(&incoming),
                Some(&incoming)
            ),
            "creating a row you own must be allowed"
        );
        assert!(
            !eval_rule_expr(
                expr,
                &user("kai@example.com"),
                Some(&incoming),
                Some(&incoming)
            ),
            "creating a row owned by someone else must be refused"
        );
        // Before the fix this was the *only* outcome: with no stored row,
        // `doc.owner` was null and the rule denied everyone.
        assert!(
            !eval_rule_expr(expr, &user("ada@example.com"), None, Some(&incoming)),
            "with no document bound at all, nothing matches"
        );
    }

    #[test]
    fn new_doc_is_a_valid_namespace() {
        // The evaluator always understood `newDoc.`; the validator rejected it,
        // so it could never be saved.
        assert!(validate_rule_expr("auth.username == newDoc.owner").is_ok());
        assert!(validate_rule_expr("newDoc.").is_err());
        assert!(validate_rule_expr("unknownThing.owner").is_err());
    }

    #[test]
    fn update_rules_can_compare_the_stored_row_with_the_incoming_one() {
        let stored = json!({ "owner": "ada@example.com" });
        let incoming = json!({ "owner": "kai@example.com" });
        // "the row must stay mine" — an ownership transfer is refused.
        let expr = "auth.username == newDoc.owner";
        assert!(!eval_rule_expr(
            expr,
            &user("ada@example.com"),
            Some(&stored),
            Some(&incoming)
        ));
        assert!(eval_rule_expr(
            "auth.username == doc.owner",
            &user("ada@example.com"),
            Some(&stored),
            Some(&incoming)
        ));
        let _ = Operation::Update;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn admin_auth() -> AuthContext {
        AuthContext::from_claims("admin_user", "admin")
    }

    fn user_auth(name: &str) -> AuthContext {
        AuthContext::from_claims(name, "readwrite")
    }

    fn anon() -> AuthContext {
        AuthContext::anonymous()
    }

    #[test]
    fn rule_true_false() {
        assert!(eval_rule_expr("true", &anon(), None, None));
        assert!(!eval_rule_expr("false", &anon(), None, None));
    }

    #[test]
    fn rule_auth_not_null() {
        assert!(eval_rule_expr(
            "auth != null",
            &user_auth("alice"),
            None,
            None
        ));
        assert!(!eval_rule_expr("auth != null", &anon(), None, None));
    }

    #[test]
    fn rule_auth_null() {
        assert!(eval_rule_expr("auth == null", &anon(), None, None));
        assert!(!eval_rule_expr(
            "auth == null",
            &user_auth("alice"),
            None,
            None
        ));
    }

    #[test]
    fn rule_role_check() {
        assert!(eval_rule_expr(
            "auth.role == 'admin'",
            &admin_auth(),
            None,
            None
        ));
        assert!(!eval_rule_expr(
            "auth.role == 'admin'",
            &user_auth("bob"),
            None,
            None
        ));
    }

    #[test]
    fn rule_doc_ownership() {
        let doc = json!({"owner": "alice", "title": "My Post"});
        assert!(eval_rule_expr(
            "auth.username == doc.owner",
            &user_auth("alice"),
            Some(&doc),
            None
        ));
        assert!(!eval_rule_expr(
            "auth.username == doc.owner",
            &user_auth("bob"),
            Some(&doc),
            None
        ));
    }

    #[test]
    fn rule_logical_or() {
        let doc = json!({"owner": "alice"});
        // admin OR owner
        assert!(eval_rule_expr(
            "auth.role == 'admin' || auth.username == doc.owner",
            &admin_auth(),
            Some(&doc),
            None
        ));
        assert!(eval_rule_expr(
            "auth.role == 'admin' || auth.username == doc.owner",
            &user_auth("alice"),
            Some(&doc),
            None
        ));
        assert!(!eval_rule_expr(
            "auth.role == 'admin' || auth.username == doc.owner",
            &user_auth("bob"),
            Some(&doc),
            None
        ));
    }

    #[test]
    fn rule_logical_and() {
        assert!(eval_rule_expr(
            "auth != null && auth.role == 'admin'",
            &admin_auth(),
            None,
            None
        ));
        assert!(!eval_rule_expr(
            "auth != null && auth.role == 'admin'",
            &user_auth("bob"),
            None,
            None
        ));
    }

    #[test]
    fn rule_negation() {
        assert!(!eval_rule_expr("!true", &anon(), None, None));
        assert!(eval_rule_expr("!false", &anon(), None, None));
    }

    #[test]
    fn rule_new_doc_field() {
        let new_doc = json!({"author": "alice", "status": "draft"});
        assert!(eval_rule_expr(
            "auth.username == newDoc.author",
            &user_auth("alice"),
            None,
            Some(&new_doc)
        ));
    }

    #[test]
    fn rule_nested_doc_field() {
        let doc = json!({"meta": {"created_by": "alice"}});
        assert!(eval_rule_expr(
            "auth.username == doc.meta.created_by",
            &user_auth("alice"),
            Some(&doc),
            None
        ));
    }

    #[test]
    fn rule_system_collections_always_allowed() {
        let db = oxidb::OxiDb::open_in_memory().unwrap();
        // Even with restrictive rules, system collections pass
        let result = check_access(&db, "_auth_users", Operation::Read, &anon(), None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn rule_no_rules_allows_all() {
        let db = oxidb::OxiDb::open_in_memory().unwrap();
        let result = check_access(&db, "any_collection", Operation::Read, &anon(), None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_rule_expr_accepts_grammar_and_rejects_typos() {
        // Valid shapes across the whole grammar.
        for ok in [
            "true",
            "false",
            "auth",
            "auth.role == 'admin'",
            "auth.username == doc.owner",
            "auth.role == 'authenticated' && doc.published == true",
            "!(auth.username == doc.owner) || auth.role == 'admin'",
            "doc.count != 0",
            "doc.meta.owner_id == auth.username",
        ] {
            assert!(validate_rule_expr(ok).is_ok(), "should accept: {ok}");
        }
        // Typos / unknown terms / malformed shapes — must be rejected, not
        // silently treated as a falsy unknown at eval time.
        for bad in [
            "dytjuer",                   // bare unknown term (the reported case)
            "auth.name == 'x'",          // auth has only username/role
            "auth.username = doc.owner", // single '=' is not a comparison → unknown atom
            "doc. == 'x'",               // empty doc field
            "",                          // empty
            "admin && foo",              // unknown atoms around &&
        ] {
            assert!(validate_rule_expr(bad).is_err(), "should reject: {bad:?}");
        }
    }

    #[test]
    fn read_access_filters_per_row() {
        let db = oxidb::OxiDb::open_in_memory().unwrap();
        // A row-dependent read rule → per-row Filter.
        set_rules(
            &db,
            "tasks",
            &json!({ "read": "auth.username == doc.owner", "create": "true", "update": "true", "delete": "true" }),
        )
        .unwrap();
        let alice = AuthContext::from_claims("alice", "authenticated");
        match read_access(&db, "tasks", &alice) {
            ReadAccess::Filter(expr) => {
                assert!(row_visible(
                    &expr,
                    &alice,
                    &json!({ "owner": "alice", "t": 1 })
                ));
                assert!(!row_visible(
                    &expr,
                    &alice,
                    &json!({ "owner": "bob", "t": 2 })
                ));
            }
            _ => panic!("row-dependent read rule must yield a per-row Filter"),
        }

        // A row-independent rule is a one-shot gate.
        set_rules(
            &db,
            "board",
            &json!({ "read": "auth.role == 'authenticated'", "create": "true", "update": "true", "delete": "true" }),
        )
        .unwrap();
        assert!(matches!(read_access(&db, "board", &alice), ReadAccess::All));
        assert!(matches!(
            read_access(&db, "board", &anon()),
            ReadAccess::None
        ));

        // No rules → All; admin bypasses → All.
        assert!(matches!(read_access(&db, "open", &anon()), ReadAccess::All));
        assert!(matches!(
            read_access(&db, "tasks", &admin_auth()),
            ReadAccess::All
        ));
    }

    #[test]
    fn anon_key_write_needs_a_rule() {
        let db = oxidb::OxiDb::open_in_memory().unwrap();
        // A project's public anon key carries role "read".
        let anon_key = AuthContext::from_claims("read@proj", "read");
        let service_role = AuthContext::from_claims("admin@proj", "admin");

        // No rules on the collection:
        // - anon-key READ is still allowed (reads stay open)
        assert!(check_access(&db, "notes", Operation::Read, &anon_key, None, None).is_ok());
        // - anon-key WRITES are denied by default (must opt in via a rule)
        for op in [Operation::Create, Operation::Update, Operation::Delete] {
            assert!(
                check_access(&db, "notes", op, &anon_key, None, None).is_err(),
                "anon {op:?} with no rule must be denied"
            );
        }
        // - service_role (admin) writes are unaffected
        assert!(check_access(&db, "notes", Operation::Create, &service_role, None, None).is_ok());
        // - an open, no-auth server (role None) is unaffected
        assert!(check_access(&db, "notes", Operation::Create, &anon(), None, None).is_ok());
    }

    #[test]
    fn anon_key_write_allowed_when_rule_grants_it() {
        let db = oxidb::OxiDb::open_in_memory().unwrap();
        set_rules(
            &db,
            "public_notes",
            &json!({ "read": "true", "create": "true", "update": "true", "delete": "true" }),
        )
        .unwrap();
        let anon_key = AuthContext::from_claims("read@proj", "read");
        for op in [
            Operation::Read,
            Operation::Create,
            Operation::Update,
            Operation::Delete,
        ] {
            assert!(
                check_access(&db, "public_notes", op, &anon_key, None, None).is_ok(),
                "rule create:true must allow anon {op:?}"
            );
        }

        // A restrictive rule still denies the anon key.
        set_rules(
            &db,
            "locked",
            &json!({ "read": "true", "create": "false", "update": "false", "delete": "false" }),
        )
        .unwrap();
        assert!(check_access(&db, "locked", Operation::Create, &anon_key, None, None).is_err());
    }

    #[test]
    fn rule_set_and_enforce() {
        let db = oxidb::OxiDb::open_in_memory().unwrap();

        // Set rules: only authenticated users can read
        set_rules(
            &db,
            "secrets",
            &json!({
                "read": "auth != null",
                "create": "auth.role == 'admin'",
                "update": "false",
                "delete": "false"
            }),
        )
        .unwrap();

        // Anonymous read → denied
        let result = check_access(&db, "secrets", Operation::Read, &anon(), None, None);
        assert!(result.is_err());

        // Authenticated read → allowed
        let result = check_access(
            &db,
            "secrets",
            Operation::Read,
            &user_auth("alice"),
            None,
            None,
        );
        assert!(result.is_ok());

        // Non-admin create → denied
        let result = check_access(
            &db,
            "secrets",
            Operation::Create,
            &user_auth("alice"),
            None,
            None,
        );
        assert!(result.is_err());

        // Admin / service_role bypasses rules entirely (Supabase semantic):
        // it is allowed even where the rule would deny (create requires admin,
        // update is hard-`false`).
        let result = check_access(&db, "secrets", Operation::Create, &admin_auth(), None, None);
        assert!(result.is_ok());
        let result = check_access(&db, "secrets", Operation::Update, &admin_auth(), None, None);
        assert!(result.is_ok(), "service_role bypasses rules");

        // A non-admin user is still bound by `update: "false"`.
        let result = check_access(
            &db,
            "secrets",
            Operation::Update,
            &user_auth("alice"),
            None,
            None,
        );
        assert!(result.is_err());
    }

    #[test]
    fn rule_ownership_enforcement() {
        let db = oxidb::OxiDb::open_in_memory().unwrap();

        set_rules(
            &db,
            "posts",
            &json!({
                "read": "true",
                "create": "auth != null",
                "update": "auth.username == doc.author",
                "delete": "auth.role == 'admin' || auth.username == doc.author"
            }),
        )
        .unwrap();

        let post = json!({"author": "alice", "title": "Hello"});

        // Anyone can read
        assert!(check_access(&db, "posts", Operation::Read, &anon(), Some(&post), None).is_ok());

        // Only author can update
        assert!(
            check_access(
                &db,
                "posts",
                Operation::Update,
                &user_auth("alice"),
                Some(&post),
                None
            )
            .is_ok()
        );
        assert!(
            check_access(
                &db,
                "posts",
                Operation::Update,
                &user_auth("bob"),
                Some(&post),
                None
            )
            .is_err()
        );

        // Author or admin can delete
        assert!(
            check_access(
                &db,
                "posts",
                Operation::Delete,
                &user_auth("alice"),
                Some(&post),
                None
            )
            .is_ok()
        );
        assert!(
            check_access(
                &db,
                "posts",
                Operation::Delete,
                &admin_auth(),
                Some(&post),
                None
            )
            .is_ok()
        );
        assert!(
            check_access(
                &db,
                "posts",
                Operation::Delete,
                &user_auth("bob"),
                Some(&post),
                None
            )
            .is_err()
        );
    }

    #[test]
    fn rule_delete_rules() {
        let db = oxidb::OxiDb::open_in_memory().unwrap();

        set_rules(&db, "temp", &json!({"read": "false"})).unwrap();
        assert!(check_access(&db, "temp", Operation::Read, &anon(), None, None).is_err());

        delete_rules(&db, "temp").unwrap();
        assert!(check_access(&db, "temp", Operation::Read, &anon(), None, None).is_ok());
    }
}
