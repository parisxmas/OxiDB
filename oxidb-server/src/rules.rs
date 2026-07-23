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
//! | `auth.username == doc.owner` | Document ownership |
//! | `A && B`, `A \|\| B` | Logical AND / OR |
//!
//! # Commands
//!
//! ```json
//! {"cmd": "set_rules", "collection": "posts", "rules": {"read": "true", "create": "auth != null", "update": "auth.username == doc.author", "delete": "auth.role == 'admin'"}}
//! {"cmd": "get_rules", "collection": "posts"}
//! {"cmd": "delete_rules", "collection": "posts"}
//! ```

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
}

// ---------------------------------------------------------------------------
// Rule storage
// ---------------------------------------------------------------------------

pub fn set_rules(db: &OxiDb, collection: &str, rules: &Value) -> Result<(), String> {
    let rule_doc = json!({
        "collection": collection,
        "read": rules["read"].as_str().unwrap_or("true"),
        "create": rules["create"].as_str().unwrap_or("true"),
        "update": rules["update"].as_str().unwrap_or("true"),
        "delete": rules["delete"].as_str().unwrap_or("true"),
    });
    // Upsert: drop any existing rule(s) for this collection, then insert.
    remove_rules(db, collection);
    db.insert(RULES_COLLECTION, rule_doc)
        .map_err(|e| e.to_string())?;
    Ok(())
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
    Some(RuleSet {
        read: doc["read"].as_str().unwrap_or("true").to_string(),
        create: doc["create"].as_str().unwrap_or("true").to_string(),
        update: doc["update"].as_str().unwrap_or("true").to_string(),
        delete: doc["delete"].as_str().unwrap_or("true").to_string(),
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
        if d.get("collection").and_then(|v| v.as_str()) == Some(collection) {
            if let Some(id) = d.get("_id") {
                let _ = db.delete(RULES_COLLECTION, &json!({ "_id": id }));
            }
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
/// Returns `Ok(())` if allowed, `Err(message)` if denied.
pub fn check_access(
    db: &OxiDb,
    collection: &str,
    op: Operation,
    auth: &AuthContext,
    doc: Option<&Value>,
    new_doc: Option<&Value>,
) -> Result<(), String> {
    // System collections are always accessible (no rules apply)
    if collection.starts_with('_') {
        return Ok(());
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
                Err(format!(
                    "access denied: {} on '{}' requires a security rule",
                    op.as_str(),
                    collection
                ))
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

    if eval_rule_expr(expr, auth, doc, new_doc) {
        Ok(())
    } else {
        Err(format!(
            "access denied: {} on '{}' not allowed",
            op.as_str(),
            collection
        ))
    }
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
        return ReadAccess::All;
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
                assert!(row_visible(&expr, &alice, &json!({ "owner": "alice", "t": 1 })));
                assert!(!row_visible(&expr, &alice, &json!({ "owner": "bob", "t": 2 })));
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
        assert!(matches!(read_access(&db, "board", &anon()), ReadAccess::None));

        // No rules → All; admin bypasses → All.
        assert!(matches!(read_access(&db, "open", &anon()), ReadAccess::All));
        assert!(matches!(read_access(&db, "tasks", &admin_auth()), ReadAccess::All));
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
