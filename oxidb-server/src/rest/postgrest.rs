//! PostgREST-compatible REST surface for the document engine (ADR-0019).
//!
//! PostgREST exposes a database as an auto-generated REST API where the URL
//! *is* the query: `GET /rest/v1/products?price=gt.100&select=name,price&order=price.desc`.
//! This module implements that URL grammar over OxiDB's document engine by
//! translating each request into the engine's existing query AST — no new
//! storage, no new query engine, just a translation layer on top of
//! [`find_with_options`](oxidb::OxiDb::find_with_options), `insert_many`,
//! `update`, and `delete`.
//!
//! It is wire-compatible with the PostgREST/Supabase URL contract for the
//! subset below, so `@supabase/postgrest-js` and any PostgREST client can talk
//! to OxiDB unmodified. A "table" here is a document collection.
//!
//! # Supported grammar
//!
//! | Surface | Example | Maps to |
//! |---------|---------|---------|
//! | filter  | `id=eq.42` `age=gt.18` `name=like.*jo*` | `{"id":{"$eq":42}}` … |
//! | negation| `status=not.eq.done` | `{"status":{"$ne":"done"}}` |
//! | membership | `tag=in.(a,b,c)` | `{"tag":{"$in":[…]}}` |
//! | null/bool | `deleted=is.null` `ok=is.true` | `{"deleted":{"$eq":null}}` |
//! | logic   | `or=(age.gt.18,vip.is.true)` | `{"$or":[…]}` |
//! | select  | `select=name,price` `select=n:name` | column projection |
//! | embed   | `select=*,line_items(sku,qty)` | nested related docs (`$lookup`) |
//! | order   | `order=price.desc,name` | sort |
//! | page    | `limit=20&offset=40` | limit/skip (capped) |
//!
//! Resource embedding (Phase 2) nests documents from a related collection.
//! Direction is inferred: `order?select=*,customers(*)` embeds a single
//! `customers` doc when the order carries a `customer_id` (belongs-to), while
//! `customer?select=*,orders(*)` embeds an array of `orders` whose `customer_id`
//! points back (has-many). An explicit `related!fk(...)` names the foreign-key
//! field when the naming convention does not apply. Nested embeds (embeds
//! inside an embed) are a future phase.
//!
//! Every request runs through [`rules::check_access`] exactly like the native
//! `/api/{col}/documents` handlers — the security-rules layer is OxiDB's
//! row-level-security analog, and it is what makes an auto-generated API safe
//! to expose. Read-only roles are additionally gated by `rest_permitted` in
//! the parent router before these handlers are reached.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use serde_json::{Value, json};

use super::RestState;
use crate::rules::{self, AuthContext, Operation};
use crate::s3::http::{HttpRequest, HttpResponse};
use oxidb::query::{FindOptions, SortOrder};

/// Hard ceiling on rows returned by a single GET, so an unqualified
/// `GET /rest/v1/readings` can never dump an unbounded collection. Overridable
/// via `OXIDB_PGRST_MAX_ROWS` (PostgREST's `db-max-rows`). A caller-supplied
/// `limit` is honored up to this cap.
pub(super) fn max_rows() -> u64 {
    static CAP: OnceLock<u64> = OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("OXIDB_PGRST_MAX_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0)
            .unwrap_or(1000)
    })
}

// ---------------------------------------------------------------------------
// Parsed request shape
// ---------------------------------------------------------------------------

/// The reserved (non-filter) query parameters, extracted from the URL.
struct Modifiers {
    select: Option<String>,
    order: Option<Vec<(String, SortOrder)>>,
    limit: Option<u64>,
    offset: Option<u64>,
}

/// A translated GET/PATCH/DELETE request: the engine query plus URL modifiers.
struct Parsed {
    query: Value,
    mods: Modifiers,
}

type PgResult<T> = Result<T, (u16, String)>;

// ---------------------------------------------------------------------------
// Handlers (return HttpResponse directly so they can set PostgREST headers)
// ---------------------------------------------------------------------------

/// `GET /rest/v1/{table}` — filtered, projected, ordered, paginated read.
/// Responds with a JSON **array** (never a wrapper object) and a
/// `Content-Range` header, matching PostgREST.
pub(super) fn handle_get(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> HttpResponse {
    if let Err(e) = rules::check_access(&state.db, col, Operation::Read, auth, None, None) {
        return access_denied(e);
    }
    let parsed = match parse(&req.query) {
        Ok(p) => p,
        Err((s, m)) => return err(s, &m),
    };

    let cap = max_rows();
    let limit = Some(parsed.mods.limit.map_or(cap, |l| l.min(cap)));
    let opts = FindOptions {
        sort: parsed.mods.order.clone(),
        skip: parsed.mods.offset,
        limit,
    };

    let docs = match state.db.find_with_options(col, &parsed.query, &opts) {
        Ok(d) => d,
        Err(_) => return err(500, "database error"),
    };
    // Projection + resource embedding (Phase 2). Embeds pull related documents
    // from other collections and nest them, mapping PostgREST's `select=*,rel(…)`
    // onto a `$lookup`-style stitch.
    let docs = match project_top(state, col, docs, parsed.mods.select.as_deref()) {
        Ok(d) => d,
        Err((s, m)) => return err(s, &m),
    };

    let n = docs.len();
    let range = if n == 0 {
        "*/*".to_string()
    } else {
        let lo = parsed.mods.offset.unwrap_or(0);
        format!("{lo}-{}/*", lo + n as u64 - 1)
    };
    super::json_response(200, "OK", Value::Array(docs)).with_header("Content-Range", &range)
}

/// `POST /rest/v1/{table}` — insert one object or an array of objects. Returns
/// the created rows when `Prefer: return=representation`, else `201` with an
/// empty array (PostgREST's default-minimal behavior).
pub(super) fn handle_post(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> HttpResponse {
    let body = match serde_json::from_slice::<Value>(&req.body) {
        Ok(v) => v,
        Err(_) => return err(400, "invalid JSON body"),
    };
    let docs: Vec<Value> = match body {
        Value::Array(a) => a,
        obj @ Value::Object(_) => vec![obj],
        _ => return err(400, "body must be an object or array of objects"),
    };
    if docs.is_empty() {
        return err(400, "empty insert");
    }
    // Representative rule check on the first row (mirrors handle_insert_with_rules).
    if let Err(e) = rules::check_access(
        &state.db,
        col,
        Operation::Create,
        auth,
        None,
        Some(&docs[0]),
    ) {
        return access_denied(e);
    }

    let ids = match state.db.insert_many(col, docs) {
        Ok(ids) => ids,
        Err(_) => return err(500, "database error"),
    };

    if !wants_representation(req) {
        return super::json_response(201, "Created", json!([]));
    }
    let select = parse(&req.query).ok().and_then(|p| p.mods.select);
    let rows = fetch_by_ids(state, col, &ids);
    let rows = apply_select(rows, select.as_deref()).unwrap_or_default();
    super::json_response(201, "Created", Value::Array(rows))
}

/// `PATCH /rest/v1/{table}?<filters>` — set the columns in the body on every
/// matching row. Body keys are treated as a column assignment (`$set`) unless
/// they are already update operators (`$inc`, etc.).
pub(super) fn handle_patch(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> HttpResponse {
    let parsed = match parse(&req.query) {
        Ok(p) => p,
        Err((s, m)) => return err(s, &m),
    };
    let body = match serde_json::from_slice::<Value>(&req.body) {
        Ok(v @ Value::Object(_)) => v,
        Ok(_) => return err(400, "PATCH body must be a JSON object"),
        Err(_) => return err(400, "invalid JSON body"),
    };
    let update = to_update(body);

    // Rule check against every matching document, like handle_update_with_rules.
    let matching = state.db.find(col, &parsed.query).unwrap_or_default();
    for doc in &matching {
        if let Err(e) =
            rules::check_access(&state.db, col, Operation::Update, auth, Some(doc), None)
        {
            return access_denied(e);
        }
    }

    let modified = match state.db.update(col, &parsed.query, &update) {
        Ok(n) => n,
        Err(_) => return err(500, "database error"),
    };
    if !wants_representation(req) {
        return super::json_response(200, "OK", json!([]));
    }
    // Re-read the affected rows for the representation response.
    let rows = state
        .db
        .find_with_options(col, &parsed.query, &FindOptions::default())
        .unwrap_or_default();
    let rows = apply_select(rows, parsed.mods.select.as_deref()).unwrap_or_default();
    let _ = modified;
    super::json_response(200, "OK", Value::Array(rows))
}

/// `DELETE /rest/v1/{table}?<filters>` — delete matching rows. Returns the
/// deleted rows when `Prefer: return=representation`, else `204`.
pub(super) fn handle_delete(
    col: &str,
    req: &HttpRequest,
    state: &RestState,
    auth: &AuthContext,
) -> HttpResponse {
    let parsed = match parse(&req.query) {
        Ok(p) => p,
        Err((s, m)) => return err(s, &m),
    };

    let matching = state.db.find(col, &parsed.query).unwrap_or_default();
    for doc in &matching {
        if let Err(e) =
            rules::check_access(&state.db, col, Operation::Delete, auth, Some(doc), None)
        {
            return access_denied(e);
        }
    }

    let representation = wants_representation(req);
    // Snapshot the rows before deleting so we can echo them back.
    let snapshot = if representation {
        apply_select(matching, parsed.mods.select.as_deref()).unwrap_or_default()
    } else {
        Vec::new()
    };

    if state.db.delete(col, &parsed.query).is_err() {
        return err(500, "database error");
    }
    if representation {
        super::json_response(200, "OK", Value::Array(snapshot))
    } else {
        super::json_response(204, "No Content", json!(null))
    }
}

// ---------------------------------------------------------------------------
// Query-string → engine AST translation
// ---------------------------------------------------------------------------

/// Parse the full query string into an engine query + URL modifiers.
fn parse(query: &str) -> PgResult<Parsed> {
    let pairs = split_pairs(query);
    let mut and_conditions: Vec<Value> = Vec::new();
    let mut mods = Modifiers {
        select: None,
        order: None,
        limit: None,
        offset: None,
    };

    for (key, val) in pairs {
        match key.as_str() {
            "db" => {} // consumed by the parent router (ADR-0012)
            "select" => mods.select = Some(val),
            "order" => mods.order = Some(parse_order(&val)?),
            "limit" => {
                mods.limit = Some(
                    val.parse()
                        .map_err(|_| (400, "invalid 'limit'".to_string()))?,
                )
            }
            "offset" => {
                mods.offset = Some(
                    val.parse()
                        .map_err(|_| (400, "invalid 'offset'".to_string()))?,
                )
            }
            "or" => and_conditions.push(parse_logic("$or", &val)?),
            "and" => and_conditions.push(parse_logic("$and", &val)?),
            col => and_conditions.push(json!({ col: parse_op_spec(&val)? })),
        }
    }

    let query = match and_conditions.len() {
        0 => json!({}),
        1 => and_conditions.pop().unwrap(),
        _ => json!({ "$and": and_conditions }),
    };
    Ok(Parsed { query, mods })
}

/// Translate a top-level `op.value` (optionally `not.op.value`) filter spec
/// into an OxiDB condition object, e.g. `gt.100` → `{"$gt":100}`.
fn parse_op_spec(spec: &str) -> PgResult<Value> {
    let (negate, rest) = match spec.strip_prefix("not.") {
        Some(r) => (true, r),
        None => (false, spec),
    };
    let (op, arg) = rest
        .split_once('.')
        .ok_or((400, format!("filter must be 'op.value', got '{spec}'")))?;

    // Negations that have a direct inverse operator are cleaner (and index
    // friendlier) than a generic `$not` wrapper.
    if negate {
        match op {
            "eq" => return Ok(json!({ "$ne": coerce(arg) })),
            "in" => return Ok(json!({ "$nin": parse_in_list(arg) })),
            _ => return Ok(json!({ "$not": op_to_cond(op, arg)? })),
        }
    }
    op_to_cond(op, arg)
}

/// The core operator mapping (PostgREST operator → OxiDB condition object).
fn op_to_cond(op: &str, arg: &str) -> PgResult<Value> {
    Ok(match op {
        "eq" => json!({ "$eq": coerce(arg) }),
        "neq" => json!({ "$ne": coerce(arg) }),
        "gt" => json!({ "$gt": coerce(arg) }),
        "gte" => json!({ "$gte": coerce(arg) }),
        "lt" => json!({ "$lt": coerce(arg) }),
        "lte" => json!({ "$lte": coerce(arg) }),
        "like" => json!({ "$regex": like_to_regex(arg) }),
        "ilike" => json!({ "$regex": like_to_regex(arg), "$options": "i" }),
        "match" => json!({ "$regex": arg }),
        "imatch" => json!({ "$regex": arg, "$options": "i" }),
        "in" => json!({ "$in": parse_in_list(arg) }),
        "is" => match arg {
            "null" => json!({ "$eq": Value::Null }),
            "true" => json!({ "$eq": true }),
            "false" => json!({ "$eq": false }),
            _ => return Err((400, format!("'is' expects null/true/false, got '{arg}'"))),
        },
        other => return Err((400, format!("unsupported operator '{other}'"))),
    })
}

/// Parse an `or=(...)`/`and=(...)` group into `{"$or":[…]}` / `{"$and":[…]}`.
/// Inner conditions use PostgREST's dotted form `col.op.value` and may nest
/// another `or(...)`/`and(...)`.
fn parse_logic(bool_op: &str, raw: &str) -> PgResult<Value> {
    let inner = raw
        .strip_prefix('(')
        .and_then(|s| s.strip_suffix(')'))
        .ok_or((400, format!("{bool_op} group must be '(...)'")))?;
    let mut conds = Vec::new();
    for part in split_top_commas(inner) {
        conds.push(parse_dotted_condition(part.trim())?);
    }
    Ok(json!({ bool_op: conds }))
}

/// A single condition inside a logic group: `col.op.value`, `col.not.op.value`,
/// or a nested `or(...)`/`and(...)`.
fn parse_dotted_condition(cond: &str) -> PgResult<Value> {
    if let Some(rest) = cond.strip_prefix("or") {
        if rest.starts_with('(') {
            return parse_logic("$or", rest);
        }
    }
    if let Some(rest) = cond.strip_prefix("and") {
        if rest.starts_with('(') {
            return parse_logic("$and", rest);
        }
    }
    let (col, spec) = cond.split_once('.').ok_or((
        400,
        format!("condition must be 'col.op.value', got '{cond}'"),
    ))?;
    Ok(json!({ col: parse_op_spec(spec)? }))
}

/// `order=price.desc,name` → `[(price,Desc),(name,Asc)]`. `nullsfirst`/
/// `nullslast` directives are accepted and ignored (single null ordering).
fn parse_order(spec: &str) -> PgResult<Vec<(String, SortOrder)>> {
    let mut out = Vec::new();
    for term in spec.split(',').filter(|s| !s.is_empty()) {
        let mut it = term.split('.');
        let col = it
            .next()
            .filter(|c| !c.is_empty())
            .ok_or((400, "empty order column".to_string()))?;
        let mut order = SortOrder::Asc;
        for dir in it {
            match dir {
                "asc" => order = SortOrder::Asc,
                "desc" => order = SortOrder::Desc,
                "nullsfirst" | "nullslast" => {}
                other => return Err((400, format!("bad order directive '{other}'"))),
            }
        }
        out.push((col.to_string(), order));
    }
    Ok(out)
}

/// Column projection. `None` or `*` keeps whole documents; otherwise keeps the
/// listed columns, honoring `alias:source` renames. Resource embedding
/// (`col(...)`) is Phase 2 and rejected with a clear message.
pub(super) fn apply_select(docs: Vec<Value>, select: Option<&str>) -> PgResult<Vec<Value>> {
    let spec = match select {
        None => return Ok(docs),
        Some(s) if s.trim().is_empty() || s.trim() == "*" => return Ok(docs),
        Some(s) => s,
    };
    // (output_name, source_field)
    let mut cols: Vec<(String, String)> = Vec::new();
    for item in spec.split(',').filter(|s| !s.is_empty()) {
        if item.contains('(') {
            return Err((
                400,
                "resource embedding (select=col(...)) is not yet supported".to_string(),
            ));
        }
        let (out, src) = match item.split_once(':') {
            Some((alias, source)) => (alias.to_string(), source.to_string()),
            None => (item.to_string(), item.to_string()),
        };
        cols.push((out, src));
    }

    let projected = docs
        .into_iter()
        .map(|doc| {
            let mut obj = serde_json::Map::new();
            if let Value::Object(map) = &doc {
                for (out, src) in &cols {
                    if let Some(v) = map.get(src) {
                        obj.insert(out.clone(), v.clone());
                    }
                }
            }
            Value::Object(obj)
        })
        .collect();
    Ok(projected)
}

// ---------------------------------------------------------------------------
// Resource embedding (Phase 2) — `select=*,related(cols)` → nested documents
// ---------------------------------------------------------------------------

/// A parsed top-level `select`: plain columns plus any resource embeds. Shared
/// with the SQL surface (`postgrest_sql`), whose `select` grammar is identical.
pub(super) struct SelectPlan {
    /// `*` present — keep all parent fields (embeds are attached on top).
    pub(super) star: bool,
    /// `(output_name, source_field)` plain projections.
    pub(super) cols: Vec<(String, String)>,
    pub(super) embeds: Vec<Embed>,
}

/// One `related(childcols)` (or `alias:related!fk(childcols)`) embed.
pub(super) struct Embed {
    /// Output key on the parent document (alias, else the target name).
    pub(super) out: String,
    /// The collection to pull related documents from.
    pub(super) target: String,
    /// Optional explicit foreign-key field (`!fk`). Direction is inferred:
    /// if a parent document carries this field it is a belongs-to (parent
    /// `fk` → target `_id`), otherwise a has-many (child `fk` → parent `_id`).
    pub(super) hint: Option<String>,
    /// The nested `select` applied to each embedded document (plain columns
    /// only — nested embeds are a future phase).
    pub(super) child: Option<String>,
}

/// Parse a top-level `select` into a [`SelectPlan`]. Returns `None` for the
/// pass-through cases (`None`, empty, or a bare `*`).
pub(super) fn parse_select_plan(select: Option<&str>) -> PgResult<Option<SelectPlan>> {
    let spec = match select {
        None => return Ok(None),
        Some(s) if s.trim().is_empty() => return Ok(None),
        Some(s) => s,
    };
    let mut plan = SelectPlan {
        star: false,
        cols: Vec::new(),
        embeds: Vec::new(),
    };
    for item in split_top_commas(spec) {
        let item = item.trim();
        if item.is_empty() {
            continue;
        }
        if item == "*" {
            plan.star = true;
            continue;
        }
        if let Some(paren) = item.find('(') {
            if !item.ends_with(')') {
                return Err((400, format!("embed '{item}' must end with ')'")));
            }
            let head = &item[..paren];
            let child = &item[paren + 1..item.len() - 1];
            let (alias, rest) = match head.split_once(':') {
                Some((a, b)) => (Some(a), b),
                None => (None, head),
            };
            let (target, hint) = match rest.split_once('!') {
                Some((t, h)) => (t, Some(h.to_string())),
                None => (rest, None),
            };
            if target.is_empty() {
                return Err((400, format!("embed '{item}' has no target collection")));
            }
            plan.embeds.push(Embed {
                out: alias.unwrap_or(target).to_string(),
                target: target.to_string(),
                hint,
                child: Some(child.to_string()),
            });
        } else {
            let (out, src) = match item.split_once(':') {
                Some((a, b)) => (a.to_string(), b.to_string()),
                None => (item.to_string(), item.to_string()),
            };
            plan.cols.push((out, src));
        }
    }
    // A bare `*` with nothing else is a pass-through.
    if plan.star && plan.cols.is_empty() && plan.embeds.is_empty() {
        return Ok(None);
    }
    Ok(Some(plan))
}

/// Top-level projection: resolve any embeds against the database, then project
/// the parent columns. `parent_col` is the collection the parents came from
/// (used to infer has-many foreign keys).
fn project_top(
    state: &RestState,
    parent_col: &str,
    docs: Vec<Value>,
    select: Option<&str>,
) -> PgResult<Vec<Value>> {
    let plan = match parse_select_plan(select)? {
        None => return apply_select(docs, select),
        Some(p) => p,
    };
    let mut docs = docs;
    for embed in &plan.embeds {
        resolve_embed(state, parent_col, &mut docs, embed)?;
    }
    Ok(docs.into_iter().map(|d| project_doc(d, &plan)).collect())
}

/// Attach one embed's related documents to every parent doc in place.
fn resolve_embed(
    state: &RestState,
    parent_col: &str,
    docs: &mut [Value],
    embed: &Embed,
) -> PgResult<()> {
    // Decide the foreign-key field and the relationship direction.
    let (fk, belongs_to) = match &embed.hint {
        Some(h) => {
            let on_parent = docs.iter().any(|d| d.get(h).is_some_and(|v| !v.is_null()));
            (h.clone(), on_parent)
        }
        None => {
            // Belongs-to convention: parent has `<singular(target)>_id`.
            let bt = format!("{}_id", singular(&embed.target));
            let has_bt = docs
                .iter()
                .any(|d| d.get(&bt).is_some_and(|v| !v.is_null()));
            if has_bt {
                (bt, true)
            } else {
                // Has-many convention: child has `<singular(parent)>_id`.
                (format!("{}_id", singular(parent_col)), false)
            }
        }
    };

    if belongs_to {
        // parent[fk] → target._id : embed a single object (or null).
        let mut seen = HashSet::new();
        let mut ids = Vec::new();
        for d in docs.iter() {
            if let Some(v) = d.get(&fk) {
                if !v.is_null() && seen.insert(join_key(v)) {
                    ids.push(v.clone());
                }
            }
        }
        let mut by_id: HashMap<String, Value> = HashMap::new();
        if !ids.is_empty() {
            let full = state
                .db
                .find(&embed.target, &json!({ "_id": { "$in": ids } }))
                .unwrap_or_default();
            let projected = apply_select(full.clone(), embed.child.as_deref())?;
            for (f, p) in full.iter().zip(projected) {
                if let Some(idv) = f.get("_id") {
                    by_id.insert(join_key(idv), p);
                }
            }
        }
        for d in docs.iter_mut() {
            let val = d
                .get(&fk)
                .and_then(|fkv| by_id.get(&join_key(fkv)).cloned())
                .unwrap_or(Value::Null);
            if let Value::Object(m) = d {
                m.insert(embed.out.clone(), val);
            }
        }
    } else {
        // child[fk] → parent._id : embed an array.
        let parent_ids: Vec<Value> = docs.iter().filter_map(|d| d.get("_id").cloned()).collect();
        let mut groups: HashMap<String, Vec<Value>> = HashMap::new();
        if !parent_ids.is_empty() {
            let full = state
                .db
                .find(&embed.target, &json!({ &fk: { "$in": parent_ids } }))
                .unwrap_or_default();
            let projected = apply_select(full.clone(), embed.child.as_deref())?;
            for (f, p) in full.iter().zip(projected) {
                if let Some(fkv) = f.get(&fk) {
                    groups.entry(join_key(fkv)).or_default().push(p);
                }
            }
        }
        for d in docs.iter_mut() {
            let arr = d
                .get("_id")
                .and_then(|pid| groups.get(&join_key(pid)).cloned())
                .unwrap_or_default();
            if let Value::Object(m) = d {
                m.insert(embed.out.clone(), Value::Array(arr));
            }
        }
    }
    Ok(())
}

/// Project one parent document per the plan (embeds are already attached).
pub(super) fn project_doc(doc: Value, plan: &SelectPlan) -> Value {
    if plan.star {
        return doc; // keep every parent field plus the attached embeds
    }
    let mut obj = serde_json::Map::new();
    if let Value::Object(map) = &doc {
        for (out, src) in &plan.cols {
            if let Some(v) = map.get(src) {
                obj.insert(out.clone(), v.clone());
            }
        }
        for e in &plan.embeds {
            if let Some(v) = map.get(&e.out) {
                obj.insert(e.out.clone(), v.clone());
            }
        }
    }
    Value::Object(obj)
}

/// A stable string key for joining on an id value (numbers and strings alike).
fn join_key(v: &Value) -> String {
    v.to_string()
}

/// Naive singularization for foreign-key conventions: drop a trailing `s`
/// (`customers` → `customer`, `orders` → `order`). Good enough for the common
/// convention; an explicit `!fk` hint overrides it entirely.
fn singular(name: &str) -> String {
    match name.strip_suffix('s') {
        Some(base) if !base.is_empty() => base.to_string(),
        _ => name.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Value + string helpers
// ---------------------------------------------------------------------------

/// Coerce a URL string value to a typed JSON value. The document engine is
/// schemaless, so `id=eq.42` must decide 42 is a number: we try null/bool/int/
/// float and fall back to string. A double-quoted value forces a string
/// (`eq."42"`), matching PostgREST's quoting rule.
pub(super) fn coerce(s: &str) -> Value {
    if s.len() >= 2 && s.starts_with('"') && s.ends_with('"') {
        return Value::String(s[1..s.len() - 1].to_string());
    }
    match s {
        "null" => return Value::Null,
        "true" => return Value::Bool(true),
        "false" => return Value::Bool(false),
        _ => {}
    }
    if let Ok(i) = s.parse::<i64>() {
        return json!(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return json!(f);
    }
    Value::String(s.to_string())
}

/// PostgREST `in.(a,b,c)` → `[a,b,c]` (each coerced). Values may be quoted.
fn parse_in_list(arg: &str) -> Vec<Value> {
    let inner = arg.trim_start_matches('(').trim_end_matches(')');
    split_top_commas(inner)
        .into_iter()
        .map(|s| coerce(s.trim()))
        .collect()
}

/// Translate a `like`/`ilike` pattern to an anchored regex. Both SQL LIKE
/// wildcards are honored — `%` (many) and `_` (single) — since the real
/// `postgrest-js` client emits SQL-native patterns like `%foo%`; PostgREST's
/// `*` alias for `%` is accepted too. Regex metacharacters in the literal
/// portions are escaped.
fn like_to_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 2);
    out.push('^');
    for ch in pattern.chars() {
        match ch {
            '%' | '*' => out.push_str(".*"),
            '_' => out.push('.'),
            '\\' | '.' | '+' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' => {
                out.push('\\');
                out.push(ch);
            }
            c => out.push(c),
        }
    }
    out.push('$');
    out
}

/// A PATCH body is a column assignment unless it already uses update operators
/// (`$set`, `$inc`, …). Bare-column bodies are wrapped in `$set`.
fn to_update(body: Value) -> Value {
    let is_operator_doc =
        matches!(&body, Value::Object(m) if !m.is_empty() && m.keys().all(|k| k.starts_with('$')));
    if is_operator_doc {
        body
    } else {
        json!({ "$set": body })
    }
}

/// Split a raw query string into decoded `(key, value)` pairs, preserving
/// duplicate keys (`age=gt.10&age=lt.20`) — unlike a map-based parser.
pub(super) fn split_pairs(query: &str) -> Vec<(String, String)> {
    if query.is_empty() {
        return Vec::new();
    }
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| match pair.split_once('=') {
            Some((k, v)) => (super::url_decode(k), super::url_decode(v)),
            None => (super::url_decode(pair), String::new()),
        })
        .collect()
}

/// Split on top-level commas, respecting parenthesis depth so that a nested
/// `in.(1,2,3)` or `or(...)` is not split at its inner commas.
pub(super) fn split_top_commas(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut start = 0usize;
    for (i, ch) in s.char_indices() {
        match ch {
            '(' => depth += 1,
            ')' => depth -= 1,
            ',' if depth == 0 => {
                out.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if start <= s.len() {
        out.push(&s[start..]);
    }
    out.into_iter().filter(|p| !p.is_empty()).collect()
}

pub(super) fn wants_representation(req: &HttpRequest) -> bool {
    req.headers
        .get("prefer")
        .is_some_and(|v| v.to_ascii_lowercase().contains("return=representation"))
}

/// Read full documents back by id for a representation response.
fn fetch_by_ids(state: &RestState, col: &str, ids: &[u64]) -> Vec<Value> {
    if ids.is_empty() {
        return Vec::new();
    }
    let q = json!({ "_id": { "$in": ids } });
    state.db.find(col, &q).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn err(status: u16, message: &str) -> HttpResponse {
    let text = match status {
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        _ => "Internal Server Error",
    };
    super::json_response(status, text, json!({ "message": message }))
}

fn access_denied(_e: impl std::fmt::Display) -> HttpResponse {
    super::json_response(403, "Forbidden", json!({ "message": "access denied" }))
}

// ---------------------------------------------------------------------------
// Tests — the pure translation layer (no server required)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn q(s: &str) -> Value {
        parse(s).unwrap().query
    }

    #[test]
    fn simple_eq_coerces_number() {
        assert_eq!(q("id=eq.42"), json!({"id": {"$eq": 42}}));
    }

    #[test]
    fn comparison_operators() {
        assert_eq!(q("age=gt.18"), json!({"age": {"$gt": 18}}));
        assert_eq!(q("price=lte.9.99"), json!({"price": {"$lte": 9.99}}));
    }

    #[test]
    fn multiple_filters_are_anded() {
        // Order within $and is insertion order of the pairs.
        let got = q("age=gte.18&status=eq.active");
        assert_eq!(
            got,
            json!({"$and": [
                {"age": {"$gte": 18}},
                {"status": {"$eq": "active"}}
            ]})
        );
    }

    #[test]
    fn duplicate_column_range() {
        // The map-free splitter must keep both bounds on the same column.
        let got = q("age=gt.10&age=lt.20");
        assert_eq!(
            got,
            json!({"$and": [
                {"age": {"$gt": 10}},
                {"age": {"$lt": 20}}
            ]})
        );
    }

    #[test]
    fn negation_uses_inverse_ops() {
        assert_eq!(q("status=not.eq.done"), json!({"status": {"$ne": "done"}}));
        assert_eq!(q("tag=not.in.(a,b)"), json!({"tag": {"$nin": ["a", "b"]}}));
        assert_eq!(q("age=not.gt.5"), json!({"age": {"$not": {"$gt": 5}}}));
    }

    #[test]
    fn membership_and_null_bool() {
        assert_eq!(q("id=in.(1,2,3)"), json!({"id": {"$in": [1, 2, 3]}}));
        assert_eq!(q("deleted=is.null"), json!({"deleted": {"$eq": null}}));
        assert_eq!(q("ok=is.true"), json!({"ok": {"$eq": true}}));
    }

    #[test]
    fn like_becomes_anchored_regex() {
        assert_eq!(q("name=like.*jo*"), json!({"name": {"$regex": "^.*jo.*$"}}));
        assert_eq!(
            q("name=ilike.jo*"),
            json!({"name": {"$regex": "^jo.*$", "$options": "i"}})
        );
        // The real postgrest-js client emits SQL-native `%`/`_` wildcards
        // (`%` arrives URL-encoded as `%25`, decoded before translation).
        assert_eq!(q("name=like.%25jo%25"), json!({"name": {"$regex": "^.*jo.*$"}}));
        assert_eq!(q("code=like.a_c"), json!({"code": {"$regex": "^a.c$"}}));
    }

    #[test]
    fn quoted_value_forces_string() {
        assert_eq!(q(r#"zip=eq."007""#), json!({"zip": {"$eq": "007"}}));
    }

    #[test]
    fn or_group() {
        let got = q("or=(age.gt.65,vip.is.true)");
        assert_eq!(
            got,
            json!({"$or": [
                {"age": {"$gt": 65}},
                {"vip": {"$eq": true}}
            ]})
        );
    }

    #[test]
    fn nested_logic() {
        let got = q("or=(status.eq.active,and(age.gte.18,age.lt.65))");
        assert_eq!(
            got,
            json!({"$or": [
                {"status": {"$eq": "active"}},
                {"$and": [
                    {"age": {"$gte": 18}},
                    {"age": {"$lt": 65}}
                ]}
            ]})
        );
    }

    #[test]
    fn order_parsing() {
        let mods = parse("order=price.desc,name").unwrap().mods;
        let ord = mods.order.unwrap();
        assert_eq!(ord[0], ("price".to_string(), SortOrder::Desc));
        assert_eq!(ord[1], ("name".to_string(), SortOrder::Asc));
    }

    #[test]
    fn order_ignores_nulls_directive() {
        let ord = parse("order=ts.desc.nullslast")
            .unwrap()
            .mods
            .order
            .unwrap();
        assert_eq!(ord[0], ("ts".to_string(), SortOrder::Desc));
    }

    #[test]
    fn select_projection_with_alias() {
        let docs = vec![json!({"_id": 1, "name": "a", "price": 5, "secret": "x"})];
        let out = apply_select(docs, Some("n:name,price")).unwrap();
        assert_eq!(out, vec![json!({"n": "a", "price": 5})]);
    }

    #[test]
    fn select_star_is_passthrough() {
        let docs = vec![json!({"a": 1})];
        assert_eq!(apply_select(docs.clone(), Some("*")).unwrap(), docs);
        assert_eq!(apply_select(docs.clone(), None).unwrap(), docs);
    }

    #[test]
    fn nested_child_embed_is_rejected() {
        // A plain child projection must not itself contain an embed (one level).
        let docs = vec![json!({"a": 1})];
        assert!(apply_select(docs, Some("id,nested(x)")).is_err());
    }

    #[test]
    fn select_plan_splits_cols_and_embeds() {
        let plan = parse_select_plan(Some("*,name,items(sku,qty)"))
            .unwrap()
            .unwrap();
        assert!(plan.star);
        assert_eq!(plan.cols, vec![("name".to_string(), "name".to_string())]);
        assert_eq!(plan.embeds.len(), 1);
        assert_eq!(plan.embeds[0].out, "items");
        assert_eq!(plan.embeds[0].target, "items");
        assert_eq!(plan.embeds[0].child.as_deref(), Some("sku,qty"));
        assert!(plan.embeds[0].hint.is_none());
    }

    #[test]
    fn select_plan_embed_alias_and_fk_hint() {
        let plan = parse_select_plan(Some("cust:customers!customer_id(name)"))
            .unwrap()
            .unwrap();
        assert!(!plan.star);
        let e = &plan.embeds[0];
        assert_eq!(e.out, "cust");
        assert_eq!(e.target, "customers");
        assert_eq!(e.hint.as_deref(), Some("customer_id"));
        assert_eq!(e.child.as_deref(), Some("name"));
    }

    #[test]
    fn bare_star_is_passthrough_plan() {
        assert!(parse_select_plan(Some("*")).unwrap().is_none());
        assert!(parse_select_plan(None).unwrap().is_none());
    }

    #[test]
    fn unterminated_embed_errors() {
        assert!(parse_select_plan(Some("items(sku")).is_err());
    }

    #[test]
    fn singular_convention() {
        assert_eq!(singular("customers"), "customer");
        assert_eq!(singular("orders"), "order");
        assert_eq!(singular("data"), "data"); // no trailing s cases left alone-ish
        assert_eq!(singular("s"), "s"); // don't produce an empty key
    }

    #[test]
    fn patch_body_wraps_in_set() {
        assert_eq!(
            to_update(json!({"status": "done"})),
            json!({"$set": {"status": "done"}})
        );
        // Already-operator bodies pass through untouched.
        assert_eq!(
            to_update(json!({"$inc": {"n": 1}})),
            json!({"$inc": {"n": 1}})
        );
    }

    #[test]
    fn empty_query_is_match_all() {
        assert_eq!(q(""), json!({}));
    }

    #[test]
    fn limit_offset_parsed() {
        let mods = parse("limit=20&offset=40").unwrap().mods;
        assert_eq!(mods.limit, Some(20));
        assert_eq!(mods.offset, Some(40));
    }
}
