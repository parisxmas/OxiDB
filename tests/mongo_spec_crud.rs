//! MongoDB CRUD specification tests, run against the OxiDB document engine.
//!
//! The MongoDB unified spec tests (mongodb/specifications, `source/crud/
//! tests/unified/*.json`) are language-neutral JSON definitions of CRUD
//! semantics — the same files every official driver is validated against.
//! Running them against OxiDB turns "our CRUD behaves like MongoDB's" from a
//! claim into a checkable statement, with every non-passing test accounted
//! for as either SKIPPED (feature we don't support — listed with a reason)
//! or a KNOWN divergence (listed in `KNOWN_FAILURES` with a justification).
//!
//! The spec files are CC BY-NC-SA licensed, so they are NOT vendored into
//! this repository: the runner reads them from `MONGO_SPECS_DIR` (the root
//! of a mongodb/specifications checkout). Use `scripts/run_mongo_spec.sh`,
//! which clones the repo to a cache dir and runs this test.
//!
//! Adapter notes (documented divergences of the harness itself):
//! - OxiDB assigns document ids itself and does not honor a client-supplied
//!   `_id`. The adapter transparently maps `_id` <-> `__mid` on the way in
//!   and out, so filters/updates/sorts/results keyed on `_id` exercise the
//!   engine's real query semantics. "Client-chosen _id" itself is therefore
//!   NOT covered by this suite — it is a known unsupported feature.
//! - Event assertions (`expectEvents`) are ignored: they describe the wire
//!   protocol of MongoDB drivers, which OxiDB does not implement.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde_json::{Map, Value, json};

// ---------------------------------------------------------------------------
// _id <-> __mid shim
// ---------------------------------------------------------------------------

const MID: &str = "__mid";

fn rename_keys(v: &Value, from: &str, to: &str) -> Value {
    match v {
        Value::Object(m) => Value::Object(
            m.iter()
                .map(|(k, val)| {
                    let nk = if k == from { to.to_string() } else { k.clone() };
                    (nk, rename_keys(val, from, to))
                })
                .collect(),
        ),
        Value::Array(a) => Value::Array(a.iter().map(|x| rename_keys(x, from, to)).collect()),
        other => other.clone(),
    }
}

/// Query/update/document going INTO the engine: user `_id` becomes `__mid`.
fn to_engine(v: &Value) -> Value {
    rename_keys(v, "_id", MID)
}

/// Pipeline-style updates: the engine preserves ITS `_id`; MongoDB's
/// _id-immutability must also cover the shimmed client id, so append a
/// stage that carries `__mid` across $replaceRoot/$project. (Spec documents
/// always have an _id, so the reference never evaluates to null.)
fn to_engine_pipeline(update: &Value) -> Option<Value> {
    // Field-path escape-hatch expressions our evaluator doesn't implement.
    let txt = update.to_string();
    if ["$setField", "$getField", "$literal", "$$ROOT"].iter().any(|t| txt.contains(t)) {
        return None;
    }
    let stages: Vec<Value> = update
        .as_array()
        .map(|a| {
            a.iter()
                .map(|st| {
                    let st = to_engine(st);
                    // MongoDB keeps _id through $project unless _id: 0; the
                    // shimmed client id must survive the same way.
                    if let Some(proj) = st.get("$project").and_then(|p| p.as_object()) {
                        if !proj.contains_key(MID) {
                            let mut proj = proj.clone();
                            proj.insert(MID.to_string(), json!(1));
                            return json!({ "$project": proj });
                        }
                    }
                    st
                })
                .collect()
        })
        .unwrap_or_default();
    Some(Value::Array(stages))
}

/// Document coming OUT of the engine: drop engine bookkeeping (root `_id`,
/// root `_version`), then surface `__mid` as `_id` again.
fn from_engine(v: &Value) -> Value {
    let engine_id = v.get("_id").cloned();
    let mut v = v.clone();
    if let Some(obj) = v.as_object_mut() {
        obj.remove("_id");
        obj.remove("_version");
    }
    let mut out = rename_keys(&v, MID, "_id");
    // A pipeline update ($replaceRoot) may have dropped the shimmed client
    // id; surface the engine id so the document still HAS an _id, like a
    // real MongoDB document always does.
    if let Some(obj) = out.as_object_mut() {
        if !obj.contains_key("_id") {
            if let Some(id) = engine_id {
                obj.insert("_id".to_string(), id);
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Unified-format result matching (subset: $$unsetOrMatches, $$exists, $$type)
// ---------------------------------------------------------------------------

fn num_eq(a: &serde_json::Number, b: &serde_json::Number) -> bool {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x == y;
    }
    match (a.as_f64(), b.as_f64()) {
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}

/// `expected` uses the unified format's matching semantics; `actual` is
/// `None` when the key is absent (for `$$unsetOrMatches` / `$$exists`).
///
/// Unified-format rule: ROOT-level documents (and the elements of a
/// root-level array result) may carry extra fields in the actual value;
/// nested documents must match exactly.
fn matches_root(expected: &Value, actual: Option<&Value>) -> bool {
    match (expected, actual) {
        (Value::Array(e), Some(Value::Array(a))) => {
            e.len() == a.len() && e.iter().zip(a).all(|(ev, av)| matches_root(ev, Some(av)))
        }
        (Value::Object(e), Some(Value::Object(a)))
            if !e.keys().any(|k| k.starts_with("$$")) =>
        {
            e.iter().all(|(k, ev)| matches(ev, a.get(k)))
        }
        _ => matches(expected, actual),
    }
}

fn matches(expected: &Value, actual: Option<&Value>) -> bool {
    if let Some(obj) = expected.as_object() {
        if obj.len() == 1 {
            if let Some(inner) = obj.get("$$unsetOrMatches") {
                return match actual {
                    None => true,
                    Some(a) => matches(inner, Some(a)),
                };
            }
            if let Some(e) = obj.get("$$exists") {
                return e.as_bool() == Some(actual.is_some());
            }
            if obj.contains_key("$$type") {
                return actual.is_some(); // loose: presence check only
            }
        }
    }
    let Some(actual) = actual else { return false };
    match (expected, actual) {
        (Value::Object(e), Value::Object(a)) => {
            for (k, ev) in e {
                if !matches(ev, a.get(k)) {
                    return false;
                }
            }
            // Exact-document semantics: no unexpected extra keys.
            a.keys().all(|k| e.contains_key(k))
        }
        (Value::Array(e), Value::Array(a)) => {
            e.len() == a.len() && e.iter().zip(a).all(|(ev, av)| matches(ev, Some(av)))
        }
        (Value::Number(e), Value::Number(a)) => num_eq(e, a),
        (e, a) => e == a,
    }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------

/// Tests that fail for a documented engine-semantics reason. Format:
/// ("file.json", "test description") -> justification.
const KNOWN_FAILURES: &[(&str, &str, &str)] = &[
    (
        "insertMany.json",
        "InsertMany continue-on-error behavior with unordered",
        "OxiDB assigns document ids itself; a client-supplied _id is not a \
         unique key, so duplicate-_id inserts do not error",
    ),
    (
        "bulkWrite.json",
        "BulkWrite continue-on-error behavior with unordered",
        "same client-_id-uniqueness divergence as insertMany",
    ),
];

#[derive(Default)]
struct Report {
    passed: Vec<String>,
    failed: Vec<(String, String)>, // (test, detail)
    known_failed: Vec<String>,
    skipped: BTreeMap<String, u32>, // reason -> count
}

enum OpOutcome {
    Ok(Option<Value>), // actual result (None = op has no comparable result)
    Err(String),
    Skip(String),
}

struct Runner {
    db: oxidb::OxiDb,
    /// entity id -> collection name (per current test, includes suffix)
    collections: BTreeMap<String, String>,
    suffix: String,
}

impl Runner {
    fn coll(&self, entity: &str) -> Option<String> {
        self.collections.get(entity).cloned()
    }

    fn find_options(args: &Map<String, Value>) -> Result<oxidb::query::FindOptions, String> {
        let mut opts = oxidb::query::FindOptions::default();
        if let Some(sort) = args.get("sort") {
            let sort = to_engine(sort);
            let mut fields = Vec::new();
            for (k, dir) in sort.as_object().ok_or("bad sort")? {
                let ord = match dir.as_i64() {
                    Some(1) => oxidb::query::SortOrder::Asc,
                    Some(-1) => oxidb::query::SortOrder::Desc,
                    _ => return Err("sort dir".into()),
                };
                fields.push((k.clone(), ord));
            }
            opts.sort = Some(fields);
        }
        if let Some(l) = args.get("limit").and_then(|v| v.as_u64()) {
            if l > 0 {
                opts.limit = Some(l);
            }
        }
        if let Some(s) = args.get("skip").and_then(|v| v.as_u64()) {
            opts.skip = Some(s);
        }
        Ok(opts)
    }

    /// Arguments we understand or can safely ignore, per operation.
    fn check_args(op: &str, args: &Map<String, Value>) -> Option<String> {
        let allowed: &[&str] = match op {
            "find" => &["filter", "sort", "limit", "skip", "batchSize", "comment", "hint", "projection"],
            "findOne" => &["filter", "sort", "comment", "hint", "projection"],
            "insertOne" => &["document", "comment"],
            "insertMany" => &["documents", "ordered", "comment"],
            "updateOne" | "updateMany" => &["filter", "update", "comment", "upsert"],
            "deleteOne" | "deleteMany" => &["filter", "comment"],
            "countDocuments" => &["filter", "comment", "skip", "limit"],
            "estimatedDocumentCount" => &["comment", "maxTimeMS"],
            "aggregate" => &["pipeline", "comment", "batchSize", "allowDiskUse"],
            "count" => &["filter", "comment", "skip", "limit"],
            "bulkWrite" => &["requests", "ordered", "comment"],
            "distinct" => &["fieldName", "filter", "comment"],
            "replaceOne" => &["filter", "replacement", "comment"],
            "findOneAndUpdate" => &["filter", "update", "returnDocument", "sort", "comment", "projection", "upsert"],
            "findOneAndReplace" => &["filter", "replacement", "returnDocument", "sort", "comment", "projection"],
            "findOneAndDelete" => &["filter", "sort", "comment", "projection"],
            _ => return Some(format!("op:{op}")),
        };
        for k in args.keys() {
            if !allowed.contains(&k.as_str()) {
                return Some(format!("arg:{op}.{k}"));
            }
        }
        None
    }

    /// Pick the single doc an *AndModify/replaceOne op targets (engine
    /// form, honoring `sort`), or None when nothing matches.
    fn pick_target(
        &self,
        coll: &str,
        filter: &Value,
        args: &Map<String, Value>,
    ) -> Result<Option<Value>, String> {
        if args.get("sort").is_some() {
            let mut opts = Self::find_options(args)?;
            opts.limit = Some(1);
            self.db
                .find_with_options(coll, filter, &opts)
                .map(|mut v| if v.is_empty() { None } else { Some(v.remove(0)) })
                .map_err(|e| e.to_string())
        } else {
            self.db.find_one(coll, filter).map_err(|e| e.to_string())
        }
    }

    /// Spec-mandated client-side validation plus an emulation limit:
    /// atomic modifiers ($-keys) in a replacement must error; literal
    /// dotted top-level keys cannot be represented through our $set-based
    /// emulation (the engine reads dots as paths), so they skip.
    fn replacement_check(replacement: &Value) -> Option<OpOutcome> {
        let keys: Vec<&String> = replacement
            .as_object()
            .map(|o| o.keys().collect())
            .unwrap_or_default();
        if keys.iter().any(|k| k.starts_with('$')) {
            return Some(OpOutcome::Err(
                "replacement document must not contain atomic modifiers".into(),
            ));
        }
        if keys.iter().any(|k| k.contains('.')) {
            return Some(OpOutcome::Skip("replacement:dotted-key".into()));
        }
        None
    }

    /// Emulate a full-document replace as $set(all replacement fields) +
    /// $unset(fields the replacement dropped). A replacement without `_id`
    /// keeps the original one (MongoDB semantics), so the shim field is
    /// preserved unless the replacement supplies its own.
    fn replace_doc(&self, coll: &str, old: &Value, replacement: &Value) -> Result<(), String> {
        let engine_id = old.get("_id").cloned().unwrap_or(Value::Null);
        let repl = to_engine(replacement);
        let repl_obj = repl.as_object().cloned().unwrap_or_default();
        let mut unset = Map::new();
        for k in old.as_object().map(|o| o.keys()).into_iter().flatten() {
            if k == "_id" || k == "_version" {
                continue;
            }
            if k == MID && !repl_obj.contains_key(MID) {
                continue; // replacement without _id keeps the original
            }
            if !repl_obj.contains_key(k) {
                unset.insert(k.clone(), json!(""));
            }
        }
        let mut update = Map::new();
        if !repl_obj.is_empty() {
            update.insert("$set".into(), Value::Object(repl_obj));
        }
        if !unset.is_empty() {
            update.insert("$unset".into(), Value::Object(unset));
        }
        if update.is_empty() {
            return Ok(()); // empty replacement of an already-empty doc
        }
        self.db
            .update_one(coll, &json!({ "_id": engine_id }), &Value::Object(update))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// Apply a simple projection (top-level inclusion or exclusion lists;
    /// `_id` included by default, excludable). Dotted paths and operator
    /// projections ($slice/$elemMatch) are not attempted — caller skips.
    fn project(doc: &Value, projection: &Value) -> Option<Value> {
        let proj = projection.as_object()?;
        if proj.keys().any(|k| k.contains('.') || k.starts_with('$'))
            || proj.values().any(|v| v.is_object())
        {
            return None;
        }
        let truthy = |v: &Value| v.as_i64().map(|n| n != 0).or(v.as_bool()).unwrap_or(false);
        let inclusion = proj.iter().any(|(k, v)| k != "_id" && truthy(v));
        let obj = doc.as_object()?;
        let mut out = Map::new();
        for (k, v) in obj {
            let keep = if k == "_id" {
                proj.get("_id").map(truthy).unwrap_or(true)
            } else if inclusion {
                proj.get(k).map(truthy).unwrap_or(false)
            } else {
                !proj.contains_key(k)
            };
            if keep {
                out.insert(k.clone(), v.clone());
            }
        }
        Some(Value::Object(out))
    }

    fn run_op(&mut self, op: &Value) -> OpOutcome {
        let name = op.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let object = op.get("object").and_then(|v| v.as_str()).unwrap_or("");
        let empty = Map::new();
        let args = op
            .get("arguments")
            .and_then(|v| v.as_object())
            .unwrap_or(&empty);

        let Some(coll) = self.coll(object) else {
            return OpOutcome::Skip(format!("object:{object}"));
        };
        if let Some(reason) = Self::check_args(name, args) {
            return OpOutcome::Skip(reason);
        }
        let filter = || to_engine(args.get("filter").unwrap_or(&json!({})));

        match name {
            "insertOne" => {
                let Some(doc) = args.get("document") else {
                    return OpOutcome::Skip("missing document".into());
                };
                let mid = doc.get("_id").cloned();
                match self.db.insert(&coll, to_engine(doc)) {
                    Ok(id) => {
                        let inserted = mid.unwrap_or_else(|| json!(id));
                        OpOutcome::Ok(Some(json!({ "insertedId": inserted })))
                    }
                    Err(e) => OpOutcome::Err(e.to_string()),
                }
            }
            "insertMany" => {
                let Some(docs) = args.get("documents").and_then(|v| v.as_array()) else {
                    return OpOutcome::Skip("missing documents".into());
                };
                let mut ids = Map::new();
                for (i, d) in docs.iter().enumerate() {
                    let mid = d.get("_id").cloned();
                    match self.db.insert(&coll, to_engine(d)) {
                        Ok(id) => {
                            ids.insert(i.to_string(), mid.unwrap_or_else(|| json!(id)));
                        }
                        Err(e) => return OpOutcome::Err(e.to_string()),
                    }
                }
                OpOutcome::Ok(Some(json!({ "insertedIds": Value::Object(ids) })))
            }
            "find" => {
                let opts = match Self::find_options(args) {
                    Ok(o) => o,
                    Err(e) => return OpOutcome::Skip(e),
                };
                match self.db.find_with_options(&coll, &filter(), &opts) {
                    Ok(docs) => {
                        let mut out: Vec<Value> = docs.iter().map(from_engine).collect();
                        if let Some(proj) = args.get("projection") {
                            let mut projected = Vec::with_capacity(out.len());
                            for d in &out {
                                match Self::project(d, proj) {
                                    Some(p) => projected.push(p),
                                    None => return OpOutcome::Skip("projection:complex".into()),
                                }
                            }
                            out = projected;
                        }
                        OpOutcome::Ok(Some(Value::Array(out)))
                    }
                    Err(e) => OpOutcome::Err(e.to_string()),
                }
            }
            "findOne" => match self.db.find_one(&coll, &filter()) {
                Ok(Some(d)) => {
                    let d = from_engine(&d);
                    match args.get("projection") {
                        None => OpOutcome::Ok(Some(d)),
                        Some(proj) => match Self::project(&d, proj) {
                            Some(p) => OpOutcome::Ok(Some(p)),
                            None => OpOutcome::Skip("projection:complex".into()),
                        },
                    }
                }
                Ok(None) => OpOutcome::Ok(Some(Value::Null)),
                Err(e) => OpOutcome::Err(e.to_string()),
            },
            "updateOne" | "updateMany" => {
                let Some(update) = args.get("update") else {
                    return OpOutcome::Skip("missing update".into());
                };
                let update = if update.is_array() {
                    match to_engine_pipeline(update) {
                        Some(u) => u,
                        None => return OpOutcome::Skip("expr:fieldpath-escape".into()),
                    }
                } else {
                    to_engine(update)
                };
                let upsert = args.get("upsert").and_then(|v| v.as_bool()).unwrap_or(false);
                let multi = name == "updateMany";
                match self.db.update_with_upsert(&coll, &filter(), &update, multi, upsert) {
                    Ok((matched, modified, None)) => OpOutcome::Ok(Some(json!({
                        "matchedCount": matched, "modifiedCount": modified, "upsertedCount": 0
                    }))),
                    Ok((_, _, Some(id))) => {
                        // upsertedId reports the document's _id: the shimmed
                        // client one when the filter seeded it, else the
                        // engine-assigned id.
                        let doc = self
                            .db
                            .find_one(&coll, &json!({ "_id": id }))
                            .ok()
                            .flatten()
                            .unwrap_or(Value::Null);
                        let upserted_id = doc.get(MID).cloned().unwrap_or(json!(id));
                        OpOutcome::Ok(Some(json!({
                            "matchedCount": 0, "modifiedCount": 0,
                            "upsertedCount": 1, "upsertedId": upserted_id
                        })))
                    }
                    Err(e) => OpOutcome::Err(e.to_string()),
                }
            }
            "deleteOne" | "deleteMany" => {
                let r = if name == "deleteOne" {
                    self.db.delete_one(&coll, &filter())
                } else {
                    self.db.delete(&coll, &filter())
                };
                match r {
                    Ok(n) => OpOutcome::Ok(Some(json!({ "deletedCount": n }))),
                    Err(e) => OpOutcome::Err(e.to_string()),
                }
            }
            "countDocuments" => match self.db.count(&coll, &filter()) {
                Ok(mut n) => {
                    if let Some(s) = args.get("skip").and_then(|v| v.as_u64()) {
                        n = n.saturating_sub(s as usize);
                    }
                    if let Some(l) = args.get("limit").and_then(|v| v.as_u64()) {
                        n = n.min(l as usize);
                    }
                    OpOutcome::Ok(Some(json!(n)))
                }
                Err(e) => OpOutcome::Err(e.to_string()),
            },
            "estimatedDocumentCount" => match self.db.count(&coll, &json!({})) {
                Ok(n) => OpOutcome::Ok(Some(json!(n))),
                Err(e) => OpOutcome::Err(e.to_string()),
            },
            "aggregate" => {
                let Some(pipeline) = args.get("pipeline").and_then(|v| v.as_array()) else {
                    return OpOutcome::Skip("missing pipeline".into());
                };
                const STAGES: &[&str] = &[
                    "$match", "$group", "$sort", "$skip", "$limit", "$project", "$count",
                    "$unwind", "$addFields", "$lookup", "$facet",
                ];
                for st in pipeline {
                    let Some(key) = st.as_object().and_then(|o| o.keys().next()) else {
                        return OpOutcome::Skip("stage:empty".into());
                    };
                    if !STAGES.contains(&key.as_str()) {
                        return OpOutcome::Skip(format!("stage:{key}"));
                    }
                }
                let p = to_engine(&Value::Array(pipeline.clone()));
                match self.db.aggregate(&coll, &p) {
                    Ok(docs) => OpOutcome::Ok(Some(Value::Array(
                        docs.iter().map(from_engine).collect(),
                    ))),
                    Err(e) => OpOutcome::Err(e.to_string()),
                }
            }
            "count" => match self.db.count(&coll, &filter()) {
                Ok(mut n) => {
                    if let Some(sk) = args.get("skip").and_then(|v| v.as_u64()) {
                        n = n.saturating_sub(sk as usize);
                    }
                    if let Some(l) = args.get("limit").and_then(|v| v.as_u64()) {
                        n = n.min(l as usize);
                    }
                    OpOutcome::Ok(Some(json!(n)))
                }
                Err(e) => OpOutcome::Err(e.to_string()),
            },
            "distinct" => {
                let Some(field) = args.get("fieldName").and_then(|v| v.as_str()) else {
                    return OpOutcome::Skip("missing fieldName".into());
                };
                let field: String = field
                    .split('.')
                    .map(|seg| if seg == "_id" { MID } else { seg })
                    .collect::<Vec<_>>()
                    .join(".");
                match self.db.find(&coll, &filter()) {
                    Ok(docs) => {
                        let mut out: Vec<Value> = Vec::new();
                        for d in &docs {
                            let mut cur = Some(d);
                            for seg in field.split('.') {
                                cur = cur.and_then(|c| c.get(seg));
                            }
                            if let Some(v) = cur {
                                let v = rename_keys(v, MID, "_id");
                                if !out.iter().any(|x| matches(x, Some(&v))) {
                                    out.push(v);
                                }
                            }
                        }
                        OpOutcome::Ok(Some(Value::Array(out)))
                    }
                    Err(e) => OpOutcome::Err(e.to_string()),
                }
            }
            "replaceOne" => {
                let Some(replacement) = args.get("replacement") else {
                    return OpOutcome::Skip("missing replacement".into());
                };
                if let Some(out) = Self::replacement_check(replacement) {
                    return out;
                }
                match self.pick_target(&coll, &filter(), args) {
                    Err(e) => OpOutcome::Err(e),
                    Ok(None) => OpOutcome::Ok(Some(json!({
                        "matchedCount": 0, "modifiedCount": 0, "upsertedCount": 0
                    }))),
                    Ok(Some(old)) => match self.replace_doc(&coll, &old, replacement) {
                        Ok(()) => OpOutcome::Ok(Some(json!({
                            "matchedCount": 1, "modifiedCount": 1, "upsertedCount": 0
                        }))),
                        Err(e) => OpOutcome::Err(e),
                    },
                }
            }
            "findOneAndUpdate" | "findOneAndReplace" | "findOneAndDelete" => {
                let target = match self.pick_target(&coll, &filter(), args) {
                    Err(e) => return OpOutcome::Err(e),
                    Ok(t) => t,
                };
                let Some(pre) = target else {
                    if name == "findOneAndUpdate"
                        && args.get("upsert").and_then(|v| v.as_bool()) == Some(true)
                    {
                        let Some(update) = args.get("update") else {
                            return OpOutcome::Skip("missing update".into());
                        };
                        let update = if update.is_array() {
                            match to_engine_pipeline(update) {
                                Some(u) => u,
                                None => {
                                    return OpOutcome::Skip("expr:fieldpath-escape".into());
                                }
                            }
                        } else {
                            to_engine(update)
                        };
                        return match self.db.update_with_upsert(
                            &coll,
                            &filter(),
                            &update,
                            false,
                            true,
                        ) {
                            Err(e) => OpOutcome::Err(e.to_string()),
                            Ok((_, _, Some(id))) => {
                                let after = args.get("returnDocument").and_then(|v| v.as_str())
                                    == Some("After");
                                if !after {
                                    return OpOutcome::Ok(Some(Value::Null));
                                }
                                match self.db.find_one(&coll, &json!({ "_id": id })) {
                                    Ok(Some(d)) => OpOutcome::Ok(Some(from_engine(&d))),
                                    Ok(None) => OpOutcome::Ok(Some(Value::Null)),
                                    Err(e) => OpOutcome::Err(e.to_string()),
                                }
                            }
                            Ok((_, _, None)) => OpOutcome::Ok(Some(Value::Null)),
                        };
                    }
                    return OpOutcome::Ok(Some(Value::Null));
                };
                let engine_id = pre.get("_id").cloned().unwrap_or(Value::Null);
                let id_query = json!({ "_id": engine_id });
                let after = args.get("returnDocument").and_then(|v| v.as_str())
                    == Some("After");
                let applied: Result<(), String> = match name {
                    "findOneAndUpdate" => {
                        let Some(update) = args.get("update") else {
                            return OpOutcome::Skip("missing update".into());
                        };
                        let update = if update.is_array() {
                            match to_engine_pipeline(update) {
                                Some(u) => u,
                                None => {
                                    return OpOutcome::Skip("expr:fieldpath-escape".into());
                                }
                            }
                        } else {
                            to_engine(update)
                        };
                        self.db
                            .update_one(&coll, &id_query, &update)
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    }
                    "findOneAndReplace" => {
                        let Some(replacement) = args.get("replacement") else {
                            return OpOutcome::Skip("missing replacement".into());
                        };
                        if let Some(out) = Self::replacement_check(replacement) {
                            return out;
                        }
                        self.replace_doc(&coll, &pre, replacement)
                    }
                    _ => self
                        .db
                        .delete_one(&coll, &id_query)
                        .map(|_| ())
                        .map_err(|e| e.to_string()),
                };
                if let Err(e) = applied {
                    return OpOutcome::Err(e);
                }
                let returned = if name == "findOneAndDelete" || !after {
                    from_engine(&pre)
                } else {
                    match self.db.find_one(&coll, &id_query) {
                        Ok(Some(d)) => from_engine(&d),
                        Ok(None) => Value::Null,
                        Err(e) => return OpOutcome::Err(e.to_string()),
                    }
                };
                let returned = match args.get("projection") {
                    Some(proj) if !returned.is_null() => match Self::project(&returned, proj) {
                        Some(p) => p,
                        None => return OpOutcome::Skip("projection:complex".into()),
                    },
                    _ => returned,
                };
                OpOutcome::Ok(Some(returned))
            }
            "bulkWrite" => {
                let Some(requests) = args.get("requests").and_then(|v| v.as_array()) else {
                    return OpOutcome::Skip("missing requests".into());
                };
                // A standalone bulkWrite IS the sequential application of
                // its sub-operations; decompose and reuse the single-op
                // paths, accumulating the unified result counters.
                let (mut ins, mut mat, mut modc, mut del, mut ups) =
                    (0u64, 0u64, 0u64, 0u64, 0u64);
                let mut inserted_ids = Map::new();
                let mut upserted_ids = Map::new();
                for (i, req) in requests.iter().enumerate() {
                    let Some(obj) = req.as_object() else {
                        return OpOutcome::Skip("bulk:bad-request".into());
                    };
                    let Some((sub_name, sub_args)) = obj.iter().next() else {
                        return OpOutcome::Skip("bulk:empty-request".into());
                    };
                    let sub = json!({
                        "name": sub_name,
                        "object": op["object"],
                        "arguments": sub_args,
                    });
                    match self.run_op(&sub) {
                        OpOutcome::Skip(r) => return OpOutcome::Skip(format!("bulk:{r}")),
                        OpOutcome::Err(e) => return OpOutcome::Err(e),
                        OpOutcome::Ok(res) => {
                            let res = res.unwrap_or(Value::Null);
                            match sub_name.as_str() {
                                "insertOne" => {
                                    ins += 1;
                                    if let Some(id) = res.get("insertedId") {
                                        inserted_ids.insert(i.to_string(), id.clone());
                                    }
                                }
                                "updateOne" | "updateMany" | "replaceOne" => {
                                    mat += res["matchedCount"].as_u64().unwrap_or(0);
                                    modc += res["modifiedCount"].as_u64().unwrap_or(0);
                                    if let Some(id) = res.get("upsertedId") {
                                        ups += 1;
                                        upserted_ids.insert(i.to_string(), id.clone());
                                    }
                                }
                                "deleteOne" | "deleteMany" => {
                                    del += res["deletedCount"].as_u64().unwrap_or(0);
                                }
                                _ => return OpOutcome::Skip(format!("bulk:op:{sub_name}")),
                            }
                        }
                    }
                }
                OpOutcome::Ok(Some(json!({
                    "insertedCount": ins,
                    "matchedCount": mat,
                    "modifiedCount": modc,
                    "deletedCount": del,
                    "upsertedCount": ups,
                    "insertedIds": Value::Object(inserted_ids),
                    "upsertedIds": Value::Object(upserted_ids),
                })))
            }
            other => OpOutcome::Skip(format!("op:{other}")),
        }
    }
}

fn run_file(path: &PathBuf, report: &mut Report) {
    let file_name = path.file_name().unwrap().to_string_lossy().to_string();
    let spec: Value = match serde_json::from_str(&std::fs::read_to_string(path).unwrap()) {
        Ok(v) => v,
        Err(e) => {
            report.failed.push((file_name, format!("parse: {e}")));
            return;
        }
    };

    // Entity map: collection entity id -> raw collection name.
    let mut entity_colls: BTreeMap<String, String> = BTreeMap::new();
    for ent in spec["createEntities"].as_array().unwrap_or(&Vec::new()) {
        if let Some(c) = ent.get("collection") {
            let id = c["id"].as_str().unwrap_or("").to_string();
            let name = c["collectionName"].as_str().unwrap_or("").to_string();
            if c.get("collectionOptions").is_some() {
                *report
                    .skipped
                    .entry(format!("collectionOptions ({file_name})"))
                    .or_default() += spec["tests"].as_array().map_or(0, |t| t.len() as u32);
                return;
            }
            entity_colls.insert(id, name);
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let db = oxidb::OxiDb::open(dir.path()).unwrap();

    for (ti, test) in spec["tests"].as_array().unwrap_or(&Vec::new()).iter().enumerate() {
        let desc = test["description"].as_str().unwrap_or("?").to_string();
        let label = format!("{file_name} :: {desc}");
        let legacy_only = |reqs: &Value| -> bool {
            reqs.as_array().is_some_and(|rs| {
                !rs.is_empty() && rs.iter().all(|r| r.get("maxServerVersion").is_some())
            })
        };
        if legacy_only(&spec["runOnRequirements"]) || legacy_only(&test["runOnRequirements"]) {
            *report.skipped.entry("runOn: legacy-server behavior".into()).or_default() += 1;
            continue;
        }
        if let Some(r) = test.get("skipReason").and_then(|v| v.as_str()) {
            *report.skipped.entry(format!("spec-skip: {r}")).or_default() += 1;
            continue;
        }

        // Per-test namespace so tests never see each other's data.
        let suffix = format!("_t{ti}");
        let mut runner = Runner {
            db: oxidb::OxiDb::open(dir.path()).unwrap(),
            collections: entity_colls
                .iter()
                .map(|(id, name)| (id.clone(), format!("{name}{suffix}")))
                .collect(),
            suffix: suffix.clone(),
        };
        // Seed initialData (per-test copies).
        for seed in spec["initialData"].as_array().unwrap_or(&Vec::new()) {
            let cname = format!("{}{}", seed["collectionName"].as_str().unwrap_or(""), suffix);
            let _ = runner.db.drop_collection(&cname);
            for d in seed["documents"].as_array().unwrap_or(&Vec::new()) {
                runner.db.insert(&cname, to_engine(d)).unwrap();
            }
        }

        let mut failure: Option<String> = None;
        let mut skip: Option<String> = None;
        for op in test["operations"].as_array().unwrap_or(&Vec::new()) {
            let expect_error = op.get("expectError").is_some();
            if op.get("saveResultAsEntity").is_some() {
                skip = Some("saveResultAsEntity".into());
                break;
            }
            match runner.run_op(op) {
                OpOutcome::Skip(r) => {
                    skip = Some(r);
                    break;
                }
                OpOutcome::Err(e) => {
                    if !expect_error {
                        failure = Some(format!("op {} errored: {e}", op["name"]));
                        break;
                    }
                }
                OpOutcome::Ok(actual) => {
                    if expect_error {
                        failure = Some(format!("op {} expected error, got ok", op["name"]));
                        break;
                    }
                    if let Some(expected) = op.get("expectResult") {
                        let has_order = op["arguments"].get("sort").is_some()
                            || op["arguments"]["pipeline"]
                                .as_array()
                                .is_some_and(|p| p.iter().any(|st| st.get("$sort").is_some()));
                        let ordered_ok = matches_root(expected, actual.as_ref());
                        // Without an explicit sort neither engine guarantees
                        // order — retry as a multiset.
                        let ok = ordered_ok
                            || (!has_order
                                && match (expected.as_array(), actual.as_ref().and_then(|a| a.as_array())) {
                                    (Some(e), Some(a)) if e.len() == a.len() => {
                                        let mut used = vec![false; a.len()];
                                        e.iter().all(|ev| {
                                            a.iter().enumerate().any(|(i, av)| {
                                                if !used[i] && matches_root(ev, Some(av)) {
                                                    used[i] = true;
                                                    true
                                                } else {
                                                    false
                                                }
                                            })
                                        })
                                    }
                                    _ => false,
                                });
                        if !ok {
                            failure = Some(format!(
                                "op {} result mismatch\n  expected: {expected}\n  actual:   {}",
                                op["name"],
                                actual.map(|a| a.to_string()).unwrap_or_default()
                            ));
                            break;
                        }
                    }
                }
            }
        }

        // Final collection contents.
        if failure.is_none() && skip.is_none() {
            for out in test["outcome"].as_array().unwrap_or(&Vec::new()) {
                let cname =
                    format!("{}{}", out["collectionName"].as_str().unwrap_or(""), runner.suffix);
                let docs = runner
                    .db
                    .find(&cname, &json!({}))
                    .unwrap_or_default()
                    .iter()
                    .map(from_engine)
                    .collect::<Vec<_>>();
                let expected = out["documents"].clone();
                if !matches(&expected, Some(&Value::Array(docs.clone()))) {
                    failure = Some(format!(
                        "outcome mismatch in {cname}\n  expected: {expected}\n  actual:   {}",
                        Value::Array(docs)
                    ));
                    break;
                }
            }
        }

        match (failure, skip) {
            (_, Some(reason)) => {
                *report.skipped.entry(reason).or_default() += 1;
            }
            (Some(f), None) => {
                let known = KNOWN_FAILURES
                    .iter()
                    .any(|(kf, kd, _)| *kf == file_name && desc.contains(kd));
                if known {
                    report.known_failed.push(label);
                } else {
                    report.failed.push((label, f));
                }
            }
            (None, None) => report.passed.push(label),
        }
    }
}

#[test]
#[ignore = "needs MONGO_SPECS_DIR (mongodb/specifications checkout); run via scripts/run_mongo_spec.sh"]
fn mongo_crud_spec_suite() {
    let root = std::env::var("MONGO_SPECS_DIR").expect("set MONGO_SPECS_DIR");
    let dir = PathBuf::from(root).join("source/crud/tests/unified");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
        .expect("unified test dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();

    let mut report = Report::default();
    for f in &files {
        run_file(f, &mut report);
    }

    let skipped_total: u32 = report.skipped.values().sum();
    println!("\n===== MongoDB CRUD spec suite vs OxiDB =====");
    println!("files:   {}", files.len());
    println!("passed:  {}", report.passed.len());
    println!("failed:  {} (unexpected)", report.failed.len());
    println!("known:   {} (documented divergences)", report.known_failed.len());
    println!("skipped: {skipped_total} (unsupported features)");
    println!("\n-- skip reasons --");
    let mut reasons: Vec<_> = report.skipped.iter().collect();
    reasons.sort_by(|a, b| b.1.cmp(a.1));
    for (r, n) in reasons.iter().take(40) {
        println!("{n:>5}  {r}");
    }
    if !report.failed.is_empty() {
        println!("\n-- unexpected failures --");
        for (label, detail) in report.failed.iter().take(60) {
            println!("FAIL {label}\n  {detail}\n");
        }
    }
    assert!(
        report.failed.is_empty(),
        "{} unexpected failures against the MongoDB CRUD spec",
        report.failed.len()
    );
}
