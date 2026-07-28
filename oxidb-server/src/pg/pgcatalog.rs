//! Rows of the PostgreSQL system catalogs, built from OxiDB's own schema.
//!
//! [`super::catalog`] answers *known questions* ("which tables?"). Some clients
//! — DBeaver above all — instead ask for whole catalog rows: `SELECT t.oid, t.*
//! FROM pg_catalog.pg_type t`. There is no way to answer that without knowing
//! what columns `pg_type` has, so this module holds those column sets and
//! builds plausible rows for them.
//!
//! Two rules keep this honest:
//!
//! * **Column sets are PostgreSQL's**, in PostgreSQL's order, with the type
//!   OIDs PostgreSQL uses. A client that reads `relkind` as a char, or expects
//!   `oid` to be an oid, gets what it expects.
//! * **Values describe OxiDB**, not a fictional PostgreSQL. Where OxiDB has no
//!   equivalent (tablespaces, access methods, ACLs, freeze horizons) the column
//!   is NULL or a documented zero rather than an invented number.
//!
//! Object identifiers: PostgreSQL reserves everything below 16384 for built-in
//! objects, so the fixed ones here keep their real OIDs (`pg_catalog` = 11,
//! `public` = 2200) and OxiDB's own tables are numbered from 16384 up, in
//! catalog order, so they are stable for a given schema.

use oxidb_sql::{SqlEngine, Value};

use super::types;

/// A catalog column: its name and the type OID PostgreSQL gives it.
pub type Col = (&'static str, i32);

// PostgreSQL type OIDs used only by catalog columns.
const NAME: i32 = 19; // `name` — a fixed-width identifier string
const REGPROC: i32 = 24;
const XID: i32 = 28;
const TEXT: i32 = types::OID_TEXT;
const OID: i32 = types::OID_OID;
const BOOL: i32 = types::OID_BOOL;
const CHAR: i32 = types::OID_CHAR;
const INT2: i32 = types::OID_INT2;
const INT4: i32 = types::OID_INT4;
const FLOAT4: i32 = types::OID_FLOAT4;

/// Where OxiDB's own tables start numbering, matching PostgreSQL's boundary
/// for user objects.
const FIRST_USER_OID: i64 = 16384;
pub const PG_CATALOG_OID: i64 = 11;
pub const PUBLIC_OID: i64 = 2200;

fn text(s: impl Into<String>) -> Value {
    Value::Text(s.into().into())
}

// ── pg_namespace ────────────────────────────────────────────────────────────

pub const PG_NAMESPACE: &[Col] = &[
    ("oid", OID),
    ("nspname", NAME),
    ("nspowner", OID),
    ("nspacl", TEXT),
];

pub fn pg_namespace_rows(owner_oid: i64) -> Vec<Vec<Value>> {
    [("pg_catalog", PG_CATALOG_OID), ("public", PUBLIC_OID)]
        .into_iter()
        .map(|(name, oid)| {
            vec![
                Value::Int(oid),
                text(name),
                Value::Int(owner_oid),
                Value::Null, // no ACLs: access control is OxiDB's RBAC, not GRANT
            ]
        })
        .collect()
}

// ── pg_database ─────────────────────────────────────────────────────────────

pub const PG_DATABASE: &[Col] = &[
    ("oid", OID),
    ("datname", NAME),
    ("datdba", OID),
    ("encoding", INT4),
    ("datlocprovider", CHAR),
    ("datistemplate", BOOL),
    ("datallowconn", BOOL),
    ("datconnlimit", INT4),
    ("datfrozenxid", XID),
    ("datminmxid", XID),
    ("dattablespace", OID),
    ("datcollate", TEXT),
    ("datctype", TEXT),
    ("daticulocale", TEXT),
    ("daticurules", TEXT),
    ("datcollversion", TEXT),
    ("datacl", TEXT),
];

pub fn pg_database_rows(database: &str, owner_oid: i64) -> Vec<Vec<Value>> {
    vec![vec![
        Value::Int(FIRST_USER_OID),
        text(database),
        Value::Int(owner_oid),
        Value::Int(6), // PG_UTF8
        text("c"),     // libc locale provider
        Value::Bool(false),
        Value::Bool(true),
        Value::Int(-1), // no connection limit
        Value::Int(0),
        Value::Int(0),
        Value::Int(0), // no tablespaces
        text("C"),
        text("C"),
        Value::Null,
        Value::Null,
        Value::Null,
        Value::Null,
    ]]
}

// ── pg_class ────────────────────────────────────────────────────────────────

pub const PG_CLASS: &[Col] = &[
    ("oid", OID),
    ("relname", NAME),
    ("relnamespace", OID),
    ("reltype", OID),
    ("reloftype", OID),
    ("relowner", OID),
    ("relam", OID),
    ("relfilenode", OID),
    ("reltablespace", OID),
    ("relpages", INT4),
    ("reltuples", FLOAT4),
    ("relallvisible", INT4),
    ("reltoastrelid", OID),
    ("relhasindex", BOOL),
    ("relisshared", BOOL),
    ("relpersistence", CHAR),
    ("relkind", CHAR),
    ("relnatts", INT2),
    ("relchecks", INT2),
    ("relhasrules", BOOL),
    ("relhastriggers", BOOL),
    ("relhassubclass", BOOL),
    ("relrowsecurity", BOOL),
    ("relforcerowsecurity", BOOL),
    ("relispopulated", BOOL),
    ("relreplident", CHAR),
    ("relispartition", BOOL),
    ("relrewrite", OID),
    ("relfrozenxid", XID),
    ("relminmxid", XID),
    ("relacl", TEXT),
    ("reloptions", TEXT),
    ("relpartbound", TEXT),
];

/// One relation OxiDB holds, with the identity a catalog row needs.
pub struct Relation {
    pub oid: i64,
    pub name: String,
    /// `r` = ordinary table, `v` = view, `i` = index.
    pub kind: char,
    pub columns: i64,
    pub rows: Option<i64>,
    pub has_index: bool,
}

/// Every relation, numbered stably: tables in catalog order, then views, then
/// indexes. The numbering is derived from the schema, so it is the same for
/// every connection that sees the same schema.
pub fn relations(engine: &SqlEngine) -> Vec<Relation> {
    let mut out = Vec::new();
    let mut next = FIRST_USER_OID + 1;
    let mut tables = engine.list_tables();
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    let indexes = engine.list_indexes();

    for t in &tables {
        let live = t.columns.iter().filter(|c| !c.dropped).count() as i64;
        out.push(Relation {
            oid: next,
            name: t.name.clone(),
            kind: 'r',
            columns: live,
            rows: engine.row_count(&t.name).ok().map(|n| n as i64),
            has_index: !t.pk_cols().is_empty() || indexes.iter().any(|i| i.table == t.name),
        });
        next += 1;
    }
    let mut views = engine.list_views();
    views.sort_by(|a, b| a.0.cmp(&b.0));
    for (name, _) in views {
        out.push(Relation {
            oid: next,
            name,
            kind: 'v',
            columns: 0,
            rows: None,
            has_index: false,
        });
        next += 1;
    }
    let mut indexes = indexes;
    indexes.sort_by(|a, b| a.name.cmp(&b.name));
    for i in indexes {
        out.push(Relation {
            oid: next,
            name: i.name,
            kind: 'i',
            columns: i.columns.len() as i64,
            rows: None,
            has_index: false,
        });
        next += 1;
    }
    out
}

pub fn pg_class_row(rel: &Relation, owner_oid: i64) -> Vec<Value> {
    vec![
        Value::Int(rel.oid),
        text(rel.name.clone()),
        Value::Int(PUBLIC_OID),
        Value::Int(0), // reltype: no composite type per relation
        Value::Int(0),
        Value::Int(owner_oid),
        Value::Int(0), // relam: one access method, unnamed
        Value::Int(rel.oid),
        Value::Int(0), // no tablespaces
        Value::Int(0), // relpages: OxiDB does not page like this
        rel.rows.map_or(Value::Double(-1.0), |n| Value::Double(n as f64)),
        Value::Int(0),
        Value::Int(0), // no TOAST
        Value::Bool(rel.has_index),
        Value::Bool(false),
        text("p"), // permanent
        text(rel.kind.to_string()),
        Value::Int(rel.columns),
        Value::Int(0),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(true),
        text("d"), // replica identity: default
        Value::Bool(false),
        Value::Int(0),
        Value::Int(0),
        Value::Int(0),
        Value::Null, // relacl
        Value::Null, // reloptions
        Value::Null, // relpartbound
    ]
}

pub fn pg_class_rows(engine: &SqlEngine, owner_oid: i64) -> Vec<Vec<Value>> {
    relations(engine)
        .iter()
        .map(|r| pg_class_row(r, owner_oid))
        .collect()
}

// ── pg_type ─────────────────────────────────────────────────────────────────

pub const PG_TYPE: &[Col] = &[
    ("oid", OID),
    ("typname", NAME),
    ("typnamespace", OID),
    ("typowner", OID),
    ("typlen", INT2),
    ("typbyval", BOOL),
    ("typtype", CHAR),
    ("typcategory", CHAR),
    ("typispreferred", BOOL),
    ("typisdefined", BOOL),
    ("typdelim", CHAR),
    ("typrelid", OID),
    ("typsubscript", REGPROC),
    ("typelem", OID),
    ("typarray", OID),
    ("typinput", REGPROC),
    ("typoutput", REGPROC),
    ("typreceive", REGPROC),
    ("typsend", REGPROC),
    ("typmodin", REGPROC),
    ("typmodout", REGPROC),
    ("typanalyze", REGPROC),
    ("typalign", CHAR),
    ("typstorage", CHAR),
    ("typnotnull", BOOL),
    ("typbasetype", OID),
    ("typtypmod", INT4),
    ("typndims", INT4),
    ("typcollation", OID),
    ("typdefaultbin", TEXT),
    ("typdefault", TEXT),
    ("typacl", TEXT),
];

/// The types OxiDB can produce: real PostgreSQL OIDs and names, so a client's
/// existing handler for each applies unchanged. `category` follows
/// PostgreSQL's `typcategory` letters (N numeric, S string, B boolean,
/// D date/time, U user-defined).
const TYPES: &[(i32, &str, i16, bool, char)] = &[
    (types::OID_BOOL, "bool", 1, true, 'B'),
    (types::OID_BYTEA, "bytea", -1, false, 'U'),
    (types::OID_CHAR, "char", 1, true, 'S'),
    (types::OID_INT8, "int8", 8, true, 'N'),
    (types::OID_INT2, "int2", 2, true, 'N'),
    (types::OID_INT4, "int4", 4, true, 'N'),
    (TEXT, "text", -1, false, 'S'),
    (OID, "oid", 4, true, 'N'),
    (NAME, "name", 64, false, 'S'),
    (types::OID_FLOAT4, "float4", 4, true, 'N'),
    (types::OID_FLOAT8, "float8", 8, true, 'N'),
    (types::OID_VARCHAR, "varchar", -1, false, 'S'),
    (types::OID_TIMESTAMP, "timestamp", 8, true, 'D'),
    (types::OID_TIMESTAMPTZ, "timestamptz", 8, true, 'D'),
    (types::OID_NUMERIC, "numeric", -1, false, 'N'),
];

pub fn pg_type_rows(owner_oid: i64) -> Vec<Vec<Value>> {
    TYPES
        .iter()
        .map(|(oid, name, len, byval, category)| {
            vec![
                Value::Int(*oid as i64),
                text(*name),
                Value::Int(PG_CATALOG_OID),
                Value::Int(owner_oid),
                Value::Int(i64::from(*len)),
                Value::Bool(*byval),
                text("b"), // every type here is a base type
                text(category.to_string()),
                Value::Bool(false),
                Value::Bool(true),
                text(","),
                Value::Int(0), // typrelid: not a composite
                Value::Null,
                Value::Int(0), // typelem: not an array
                Value::Int(0), // typarray: array types are not offered
                text(format!("{name}in")),
                text(format!("{name}out")),
                text(format!("{name}recv")),
                text(format!("{name}send")),
                Value::Null,
                Value::Null,
                Value::Null,
                text("i"),
                text(if *len < 0 { "x" } else { "p" }),
                Value::Bool(false),
                Value::Int(0), // typbasetype: not a domain
                Value::Int(-1),
                Value::Int(0),
                Value::Int(0),
                Value::Null,
                Value::Null,
                Value::Null,
            ]
        })
        .collect()
}

/// The name a type OID goes by, for `format_type`-style lookups.
pub fn type_name(oid: i32) -> Option<&'static str> {
    TYPES.iter().find(|(o, ..)| *o == oid).map(|(_, n, ..)| *n)
}

// ── pg_attribute ────────────────────────────────────────────────────────────

pub const PG_ATTRIBUTE: &[Col] = &[
    ("attrelid", OID),
    ("attname", NAME),
    ("atttypid", OID),
    ("attstattarget", INT4),
    ("attlen", INT2),
    ("attnum", INT2),
    ("attndims", INT4),
    ("attcacheoff", INT4),
    ("atttypmod", INT4),
    ("attbyval", BOOL),
    ("attalign", CHAR),
    ("attstorage", CHAR),
    ("attcompression", CHAR),
    ("attnotnull", BOOL),
    ("atthasdef", BOOL),
    ("atthasmissing", BOOL),
    ("attidentity", CHAR),
    ("attgenerated", CHAR),
    ("attisdropped", BOOL),
    ("attislocal", BOOL),
    ("attinhcount", INT4),
    ("attcollation", OID),
    ("attacl", TEXT),
    ("attoptions", TEXT),
    ("attfdwoptions", TEXT),
    ("attmissingval", TEXT),
];

pub fn pg_attribute_rows(engine: &SqlEngine) -> Vec<Vec<Value>> {
    let mut out = Vec::new();
    for rel in relations(engine).iter().filter(|r| r.kind == 'r') {
        let Some(def) = engine.table_def(&rel.name) else {
            continue;
        };
        for (i, col) in def.columns.iter().filter(|c| !c.dropped).enumerate() {
            let oid = match col.max_len {
                Some(_) => types::OID_VARCHAR,
                None => types::oid_of(Some(col.ty)),
            };
            out.push(vec![
                Value::Int(rel.oid),
                text(col.name.clone()),
                Value::Int(oid as i64),
                Value::Int(-1),
                Value::Int(i64::from(types::type_len(oid))),
                Value::Int(i as i64 + 1),
                Value::Int(0),
                Value::Int(-1),
                Value::Int(col.max_len.map_or(-1, |n| i64::from(n) + 4)),
                Value::Bool(types::type_len(oid) > 0),
                text("i"),
                text("p"),
                text(""),
                Value::Bool(!col.nullable),
                Value::Bool(col.default_value.is_some()),
                Value::Bool(false),
                Value::Null, // no identity columns
                Value::Null, // no generated columns
                Value::Bool(false),
                Value::Bool(true),
                Value::Int(0),
                Value::Int(0),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ]);
        }
    }
    out
}

// ── pg_settings ─────────────────────────────────────────────────────────────

pub const PG_SETTINGS: &[Col] = &[
    ("name", TEXT),
    ("setting", TEXT),
    ("unit", TEXT),
    ("category", TEXT),
    ("short_desc", TEXT),
    ("extra_desc", TEXT),
    ("context", TEXT),
    ("vartype", TEXT),
    ("source", TEXT),
    ("min_val", TEXT),
    ("max_val", TEXT),
    ("enumvals", TEXT),
    ("boot_val", TEXT),
    ("reset_val", TEXT),
    ("sourcefile", TEXT),
    ("sourceline", INT4),
    ("pending_restart", BOOL),
];

pub fn pg_settings_row(name: &str, value: &str) -> Vec<Value> {
    vec![
        text(name),
        text(value),
        Value::Null,
        text("OxiDB"),
        text("Reported for PostgreSQL client compatibility."),
        Value::Null,
        text("internal"),
        text("string"),
        text("default"),
        Value::Null,
        Value::Null,
        Value::Null,
        text(value),
        text(value),
        Value::Null,
        Value::Null,
        Value::Bool(false),
    ]
}

// ── pg_enum / pg_description ────────────────────────────────────────────────

pub const PG_ENUM: &[Col] = &[
    ("oid", OID),
    ("enumtypid", OID),
    ("enumsortorder", FLOAT4),
    ("enumlabel", NAME),
];

pub const PG_DESCRIPTION: &[Col] = &[
    ("objoid", OID),
    ("classoid", OID),
    ("objsubid", INT4),
    ("description", TEXT),
];

// ── catalogs OxiDB genuinely has nothing in ────────────────────────────────
//
// Reported with their real columns and **no rows**, which is the truthful
// answer: this server has no stored functions in `pg_proc`, no roles beyond
// the connected user, no extensions, no tablespaces, no triggers. A client
// walking the catalog gets "none", not an error, and not an invention.

pub const PG_PROC: &[Col] = &[
    ("oid", OID),
    ("proname", NAME),
    ("pronamespace", OID),
    ("proowner", OID),
    ("prolang", OID),
    ("procost", FLOAT4),
    ("prorows", FLOAT4),
    ("provariadic", OID),
    ("prosupport", REGPROC),
    ("prokind", CHAR),
    ("prosecdef", BOOL),
    ("proleakproof", BOOL),
    ("proisstrict", BOOL),
    ("proretset", BOOL),
    ("provolatile", CHAR),
    ("proparallel", CHAR),
    ("pronargs", INT2),
    ("pronargdefaults", INT2),
    ("prorettype", OID),
    ("proargtypes", TEXT),
    ("proallargtypes", TEXT),
    ("proargmodes", TEXT),
    ("proargnames", TEXT),
    ("proargdefaults", TEXT),
    ("protrftypes", TEXT),
    ("prosrc", TEXT),
    ("probin", TEXT),
    ("prosqlbody", TEXT),
    ("proconfig", TEXT),
    ("proacl", TEXT),
];

pub const PG_ROLES: &[Col] = &[
    ("oid", OID),
    ("rolname", NAME),
    ("rolsuper", BOOL),
    ("rolinherit", BOOL),
    ("rolcreaterole", BOOL),
    ("rolcreatedb", BOOL),
    ("rolcanlogin", BOOL),
    ("rolreplication", BOOL),
    ("rolconnlimit", INT4),
    ("rolpassword", TEXT),
    ("rolvaliduntil", types::OID_TIMESTAMPTZ),
    ("rolbypassrls", BOOL),
    ("rolconfig", TEXT),
];

/// The connected user is the one role there is; OxiDB's accounts are not a
/// `GRANT` graph, so reporting more would be fiction.
pub fn pg_roles_rows(user: &str, superuser: bool) -> Vec<Vec<Value>> {
    vec![vec![
        Value::Int(PUBLIC_OID),
        text(user),
        Value::Bool(superuser),
        Value::Bool(true),
        Value::Bool(false),
        Value::Bool(false),
        Value::Bool(true),
        Value::Bool(false),
        Value::Int(-1),
        Value::Null,
        Value::Null,
        Value::Bool(superuser),
        Value::Null,
    ]]
}

pub const PG_CONSTRAINT: &[Col] = &[
    ("oid", OID),
    ("conname", NAME),
    ("connamespace", OID),
    ("contype", CHAR),
    ("condeferrable", BOOL),
    ("condeferred", BOOL),
    ("convalidated", BOOL),
    ("conrelid", OID),
    ("contypid", OID),
    ("conindid", OID),
    ("conparentid", OID),
    ("confrelid", OID),
    ("confupdtype", CHAR),
    ("confdeltype", CHAR),
    ("confmatchtype", CHAR),
    ("conislocal", BOOL),
    ("coninhcount", INT4),
    ("connoinherit", BOOL),
    ("conkey", TEXT),
    ("confkey", TEXT),
    ("conpfeqop", TEXT),
    ("conppeqop", TEXT),
    ("conffeqop", TEXT),
    ("confdelsetcols", TEXT),
    ("conexclop", TEXT),
    ("conbin", TEXT),
];

/// Primary keys and foreign keys, the two constraint kinds OxiDB records.
pub fn pg_constraint_rows(engine: &SqlEngine) -> Vec<Vec<Value>> {
    let rels = relations(engine);
    let oid_of = |name: &str| rels.iter().find(|r| r.name == name).map(|r| r.oid);
    let mut out = Vec::new();
    let mut next = FIRST_USER_OID + 10_000;

    for rel in rels.iter().filter(|r| r.kind == 'r') {
        let Some(def) = engine.table_def(&rel.name) else {
            continue;
        };
        let row = |oid: i64, name: String, kind: char, parent: i64, up: char, del: char| {
            vec![
                Value::Int(oid),
                text(name),
                Value::Int(PUBLIC_OID),
                text(kind.to_string()),
                Value::Bool(false),
                Value::Bool(false),
                Value::Bool(true),
                Value::Int(rel.oid),
                Value::Int(0),
                Value::Int(0),
                Value::Int(0),
                Value::Int(parent),
                text(up.to_string()),
                text(del.to_string()),
                text("s"), // MATCH SIMPLE
                Value::Bool(true),
                Value::Int(0),
                Value::Bool(false),
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Null,
            ]
        };
        if !def.pk_cols().is_empty() {
            out.push(row(next, format!("{}_pkey", rel.name), 'p', 0, 'a', 'a'));
            next += 1;
        }
        for fk in &def.foreign_keys {
            let action = |a: oxidb_sql::FkAction| match a {
                oxidb_sql::FkAction::Cascade => 'c',
                oxidb_sql::FkAction::SetNull => 'n',
                oxidb_sql::FkAction::NoAction => 'a',
            };
            out.push(row(
                next,
                format!("{}_{}_fkey", rel.name, fk.column),
                'f',
                oid_of(&fk.parent_table).unwrap_or(0),
                action(fk.on_update),
                action(fk.on_delete),
            ));
            next += 1;
        }
    }
    out
}

pub const PG_INDEX: &[Col] = &[
    ("indexrelid", OID),
    ("indrelid", OID),
    ("indnatts", INT2),
    ("indnkeyatts", INT2),
    ("indisunique", BOOL),
    ("indnullsnotdistinct", BOOL),
    ("indisprimary", BOOL),
    ("indisexclusion", BOOL),
    ("indimmediate", BOOL),
    ("indisclustered", BOOL),
    ("indisvalid", BOOL),
    ("indcheckxmin", BOOL),
    ("indisready", BOOL),
    ("indislive", BOOL),
    ("indisreplident", BOOL),
    ("indkey", TEXT),
    ("indcollation", TEXT),
    ("indclass", TEXT),
    ("indoption", TEXT),
    ("indexprs", TEXT),
    ("indpred", TEXT),
];

pub fn pg_index_rows(engine: &SqlEngine) -> Vec<Vec<Value>> {
    let rels = relations(engine);
    let mut out = Vec::new();
    for idx in engine.list_indexes() {
        let Some(index_rel) = rels.iter().find(|r| r.kind == 'i' && r.name == idx.name) else {
            continue;
        };
        let Some(table_rel) = rels.iter().find(|r| r.kind == 'r' && r.name == idx.table) else {
            continue;
        };
        out.push(vec![
            Value::Int(index_rel.oid),
            Value::Int(table_rel.oid),
            Value::Int(idx.columns.len() as i64),
            Value::Int(idx.columns.len() as i64),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(true),
            Value::Bool(false),
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
            Value::Null,
        ]);
    }
    out
}

/// Catalogs reported with columns and no rows, because OxiDB has none of the
/// thing they describe.
pub const PG_ATTRDEF: &[Col] = &[
    ("oid", OID),
    ("adrelid", OID),
    ("adnum", INT2),
    ("adbin", TEXT),
];
pub const PG_INHERITS: &[Col] = &[
    ("inhrelid", OID),
    ("inhparent", OID),
    ("inhseqno", INT4),
    ("inhdetachpending", BOOL),
];
pub const PG_EXTENSION: &[Col] = &[
    ("oid", OID),
    ("extname", NAME),
    ("extowner", OID),
    ("extnamespace", OID),
    ("extrelocatable", BOOL),
    ("extversion", TEXT),
    ("extconfig", TEXT),
    ("extcondition", TEXT),
];
pub const PG_TRIGGER: &[Col] = &[
    ("oid", OID),
    ("tgrelid", OID),
    ("tgparentid", OID),
    ("tgname", NAME),
    ("tgfoid", OID),
    ("tgtype", INT2),
    ("tgenabled", CHAR),
    ("tgisinternal", BOOL),
    ("tgconstrrelid", OID),
    ("tgconstrindid", OID),
    ("tgconstraint", OID),
    ("tgdeferrable", BOOL),
    ("tginitdeferred", BOOL),
    ("tgnargs", INT2),
    ("tgattr", TEXT),
    ("tgargs", TEXT),
    ("tgqual", TEXT),
    ("tgoldtable", NAME),
    ("tgnewtable", NAME),
];
pub const PG_TABLESPACE: &[Col] = &[
    ("oid", OID),
    ("spcname", NAME),
    ("spcowner", OID),
    ("spcacl", TEXT),
    ("spcoptions", TEXT),
];
pub const PG_SEQUENCE: &[Col] = &[
    ("seqrelid", OID),
    ("seqtypid", OID),
    ("seqstart", types::OID_INT8),
    ("seqincrement", types::OID_INT8),
    ("seqmax", types::OID_INT8),
    ("seqmin", types::OID_INT8),
    ("seqcache", types::OID_INT8),
    ("seqcycle", BOOL),
];
pub const PG_COLLATION: &[Col] = &[
    ("oid", OID),
    ("collname", NAME),
    ("collnamespace", OID),
    ("collowner", OID),
    ("collprovider", CHAR),
    ("collisdeterministic", BOOL),
    ("collencoding", INT4),
    ("collcollate", TEXT),
    ("collctype", TEXT),
    ("colliculocale", TEXT),
    ("collicurules", TEXT),
    ("collversion", TEXT),
];
pub const PG_AM: &[Col] = &[
    ("oid", OID),
    ("amname", NAME),
    ("amhandler", REGPROC),
    ("amtype", CHAR),
];
pub const PG_AVAILABLE_EXTENSIONS: &[Col] = &[
    ("name", NAME),
    ("default_version", TEXT),
    ("installed_version", TEXT),
    ("comment", TEXT),
];
pub const PG_EVENT_TRIGGER: &[Col] = &[
    ("oid", OID),
    ("evtname", NAME),
    ("evtevent", NAME),
    ("evtowner", OID),
    ("evtfoid", OID),
    ("evtenabled", CHAR),
    ("evttags", TEXT),
];
pub const PG_PUBLICATION: &[Col] = &[
    ("oid", OID),
    ("pubname", NAME),
    ("pubowner", OID),
    ("puballtables", BOOL),
    ("pubinsert", BOOL),
    ("pubupdate", BOOL),
    ("pubdelete", BOOL),
    ("pubtruncate", BOOL),
    ("pubviaroot", BOOL),
];
pub const PG_FOREIGN_SERVER: &[Col] = &[
    ("oid", OID),
    ("srvname", NAME),
    ("srvowner", OID),
    ("srvfdw", OID),
    ("srvtype", TEXT),
    ("srvversion", TEXT),
    ("srvacl", TEXT),
    ("srvoptions", TEXT),
];

/// The SQL keywords `pg_get_keywords()` reports. Clients use this only to
/// decide what to quote, so the list needs to be right rather than complete —
/// these are the words OxiDB's parser treats as reserved.
pub const KEYWORDS: &[&str] = &[
    "all", "alter", "and", "as", "asc", "begin", "between", "by", "case", "cast", "check",
    "column", "commit", "constraint", "create", "cross", "current_date", "current_timestamp",
    "default", "delete", "desc", "distinct", "drop", "else", "end", "except", "exists", "false",
    "for", "foreign", "from", "full", "group", "having", "in", "index", "inner", "insert",
    "intersect", "into", "is", "join", "key", "lateral", "left", "like", "limit", "not", "null",
    "offset", "on", "or", "order", "outer", "primary", "procedure", "references", "right",
    "rollback", "select", "set", "table", "then", "true", "union", "unique", "update", "using",
    "values", "view", "when", "where", "with",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_row_matches_its_column_count() {
        // A row that does not line up with its header is the one bug this
        // module can produce silently: the client reads the wrong column.
        assert_eq!(pg_namespace_rows(10)[0].len(), PG_NAMESPACE.len());
        assert_eq!(pg_database_rows("oxidb", 10)[0].len(), PG_DATABASE.len());
        assert_eq!(pg_type_rows(10)[0].len(), PG_TYPE.len());
        assert_eq!(pg_settings_row("a", "b").len(), PG_SETTINGS.len());
        let rel = Relation {
            oid: 16385,
            name: "t".into(),
            kind: 'r',
            columns: 2,
            rows: Some(5),
            has_index: true,
        };
        assert_eq!(pg_class_row(&rel, 10).len(), PG_CLASS.len());
    }

    #[test]
    fn types_keep_their_real_postgresql_oids() {
        // The whole point of reporting a catalog: a client's existing handler
        // for oid 20 must be the one that decodes our int8.
        assert_eq!(type_name(20), Some("int8"));
        assert_eq!(type_name(25), Some("text"));
        assert_eq!(type_name(1700), Some("numeric"));
        assert_eq!(type_name(9999), None);
    }
}
