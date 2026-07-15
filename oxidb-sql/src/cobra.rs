//! COBRA stored procedures (ADR-0014 Phase 2): CREATE-time bytecode
//! validation, the `db` handle exposed to procedure code, and `CALL`
//! execution through the Cobra VM.
//!
//! A COBRA procedure's `run(db, ...)` function receives a [`NativeObject`]
//! handle whose `db.query(sql[, params])` / `db.execute(sql[, params])`
//! methods run through **the same executor and store** as the surrounding
//! CALL — inside an open transaction they see (and add to) its buffered
//! overlay, so a Cobra procedure is exactly as atomic as a SQL-text one.

use std::cell::RefCell;
use std::rc::Rc;

use oxidb_cobra::Value as CValue;
use oxidb_cobra::value::{Dict, NativeError, NativeObject, hash_key, inspect};

use crate::ast::{QueryResult, Statement};
use crate::catalog::{ProcLanguage, ProcedureDef, base64_decode, base64_encode};
use crate::decimal::Decimal;
use crate::error::{Result, SqlError};
use crate::store::Store;
use crate::types::Value;

/// Instruction budget per CALL. Exceeding it kills the procedure with the
/// non-catchable `instruction limit exceeded` error.
const COBRA_FUEL: u64 = 100_000_000;

/// CREATE-time validation (ADR-0014: reject bad bytecode at CREATE, never
/// mid-call). `def.body` arrives holding the raw base64 payload; on success
/// it is replaced by a display placeholder and `def.bytecode` holds the
/// decoded, validated bytes.
pub(crate) fn validate_cobra_def(def: &mut ProcedureDef) -> Result<()> {
    debug_assert_eq!(def.language, ProcLanguage::Cobra);
    let cleaned: String = def.body.chars().filter(|c| !c.is_whitespace()).collect();
    let bytes = base64_decode(&cleaned)
        .map_err(|()| SqlError::Parse("invalid base64 in COBRA procedure body".into()))?;
    let bc = oxidb_cobra::decode(&bytes).map_err(SqlError::Parse)?;
    oxidb_cobra::validate(&bc).map_err(SqlError::Unsupported)?;

    let run = bc
        .constants
        .iter()
        .find_map(|c| match c {
            oxidb_cobra::bytecode::Constant::Func(f) if f.name == "run" => Some(f),
            _ => None,
        })
        .ok_or_else(|| {
            SqlError::Unsupported("COBRA procedure must define a function 'run'".into())
        })?;
    let want = def.params.len() + 1;
    if run.num_params != want {
        return Err(SqlError::Unsupported(format!(
            "'run' must take {want} parameter(s) (db + {} declared), got {}",
            def.params.len(),
            run.num_params
        )));
    }

    def.body = format!("<cobra bytecode, {} bytes>", bytes.len());
    def.bytecode = bytes;
    Ok(())
}

/// Run a COBRA procedure: decode the stored bytecode, run its main program,
/// call `run(db, args...)`, and shape the return value as a result set.
/// `args` were already coerced to the declared parameter types.
pub(crate) fn exec_call_cobra<S: Store>(
    store: &S,
    name: &str,
    def: &ProcedureDef,
    args: Vec<Value>,
) -> Result<QueryResult> {
    let bc = oxidb_cobra::decode(&def.bytecode)
        .map_err(|e| SqlError::Corrupt(format!("procedure {name:?}: {e}")))?;

    let handle: Rc<dyn NativeObject + '_> = Rc::new(DbHandle { store });
    // SAFETY: `Value::Native` requires `Rc<dyn NativeObject + 'static>`, but
    // the handle borrows `store`. The erasure is sound because nothing built
    // from the handle outlives this frame: the VM (globals, stack, every
    // value) lives inside `run_procedure`, its outcome is consumed by
    // `shape_result` below into an owned `QueryResult`, and both are dropped
    // before this function — whose whole body `store` outlives — returns.
    let handle: Rc<dyn NativeObject> = unsafe {
        std::mem::transmute::<Rc<dyn NativeObject + '_>, Rc<dyn NativeObject + 'static>>(handle)
    };

    let mut cargs = Vec::with_capacity(args.len() + 1);
    cargs.push(CValue::Native(handle));
    for v in &args {
        cargs.push(sql_to_cobra(v));
    }

    let outcome = oxidb_cobra::run_procedure(&bc, "run", cargs, Some(COBRA_FUEL))
        .map_err(|msg| SqlError::Eval(format!("procedure {name:?}: {msg}")))?;

    let inner = shape_result(name, &outcome.result)?;
    let notices = split_notices(&outcome.notices);
    if notices.is_empty() {
        Ok(inner)
    } else {
        Ok(QueryResult::Called {
            inner: Box::new(inner),
            notices,
        })
    }
}

/// Everything `print` wrote, one entry per line: split on `\n`, keep interior
/// empty lines, drop the single trailing empty segment print's `\n` leaves.
fn split_notices(out: &str) -> Vec<String> {
    if out.is_empty() {
        return Vec::new();
    }
    let mut lines: Vec<String> = out.split('\n').map(str::to_string).collect();
    if lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    lines
}

// ─── The db handle ───────────────────────────────────────────────────────

struct DbHandle<'a, S: Store> {
    store: &'a S,
}

impl<S: Store> DbHandle<'_, S> {
    /// Parse the common `(sql [, params])` argument shape and require the
    /// SQL to be exactly one statement.
    fn one_statement(
        &self,
        method: &str,
        args: &[CValue],
    ) -> std::result::Result<(Statement, Vec<Value>), NativeError> {
        if args.is_empty() || args.len() > 2 {
            return Err(NativeError::new(format!(
                "wrong number of arguments to {method}: want=1..2, got={}",
                args.len()
            )));
        }
        let CValue::Str(sql) = &args[0] else {
            return Err(NativeError::new(format!(
                "first argument to {method} must be STRING, got {}",
                args[0].type_name()
            )));
        };
        let params = match args.get(1) {
            None => Vec::new(),
            Some(CValue::List(l)) => l
                .borrow()
                .iter()
                .map(cobra_to_sql_param)
                .collect::<std::result::Result<Vec<_>, _>>()?,
            Some(other) => {
                return Err(NativeError::new(format!(
                    "second argument to {method} must be LIST, got {}",
                    other.type_name()
                )));
            }
        };
        let mut statements = crate::parser::parse(sql).map_err(sql_err)?;
        if statements.len() != 1 {
            return Err(NativeError::new(format!(
                "{method} expects a single statement, got {}",
                statements.len()
            )));
        }
        Ok((statements.remove(0), params))
    }

    fn query(&self, args: &[CValue]) -> std::result::Result<CValue, NativeError> {
        let (stmt, params) = self.one_statement("query", args)?;
        if !matches!(stmt, Statement::Select(_)) {
            return Err(NativeError::new("query expects a single SELECT statement"));
        }
        let result = crate::executor::execute(self.store, stmt, &params).map_err(sql_err)?;
        let QueryResult::Select { columns, rows, .. } = result else {
            return Err(NativeError::new("query expects a single SELECT statement"));
        };
        // Rows as a List of Dicts, column order = dict insertion order.
        let out: Vec<CValue> = rows
            .into_iter()
            .map(|row| {
                let mut dict = Dict::new();
                for (col, cell) in columns.iter().zip(row) {
                    let key = CValue::Str(Rc::from(col.as_str()));
                    let hk = hash_key(&key).expect("strings are hashable");
                    dict.set(hk, key, sql_to_cobra(&cell));
                }
                CValue::Dict(Rc::new(RefCell::new(dict)))
            })
            .collect();
        Ok(CValue::List(Rc::new(RefCell::new(out))))
    }

    fn execute(&self, args: &[CValue]) -> std::result::Result<CValue, NativeError> {
        let (stmt, params) = self.one_statement("execute", args)?;
        if !matches!(
            stmt,
            Statement::Insert { .. } | Statement::Update { .. } | Statement::Delete { .. }
        ) {
            // DDL and transaction control stay banned, exactly like SQL-text
            // procedure bodies.
            return Err(NativeError::new(
                "execute expects a single DML statement (INSERT/UPDATE/DELETE)",
            ));
        }
        let result = crate::executor::execute(self.store, stmt, &params).map_err(sql_err)?;
        let affected = match result {
            QueryResult::Mutation { affected, .. } => affected,
            // DML with RETURNING projects rows; the count is still what
            // `execute` reports.
            QueryResult::Select { rows, .. } => rows.len(),
            _ => 0,
        };
        Ok(CValue::Int(affected as i64))
    }

    /// `db.savepoint(name)` / `db.rollback_to(name)` / `db.release(name)` —
    /// one string argument (the savepoint name); returns null.
    fn savepoint_op(
        &self,
        method: &str,
        args: &[CValue],
        op: impl Fn(&S, &str) -> Result<()>,
    ) -> std::result::Result<CValue, NativeError> {
        let [CValue::Str(name)] = args else {
            return Err(NativeError::new(format!(
                "db.{method}(name) takes one string argument"
            )));
        };
        op(self.store, name).map_err(sql_err)?;
        Ok(CValue::Null)
    }

    /// `db.call(name)` / `db.call(name, [args])` — invoke another stored
    /// procedure (SQL or Cobra) in the SAME transaction and return its result
    /// as a Cobra value (SELECT → list of dicts, DML → affected count, other →
    /// null). Recursion is bounded by the executor's call-depth guard; the
    /// inner procedure's print notices are not surfaced through the return.
    fn call_proc(&self, args: &[CValue]) -> std::result::Result<CValue, NativeError> {
        let (name, values) = match args {
            [CValue::Str(n)] => (n, Vec::new()),
            [CValue::Str(n), CValue::List(l)] => {
                let borrowed = l.borrow();
                let values = borrowed
                    .iter()
                    .map(cobra_to_sql_param)
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                (n, values)
            }
            _ => {
                return Err(NativeError::new(
                    "db.call(name) or db.call(name, [args]) — name must be a string, args a list",
                ));
            }
        };
        let result =
            crate::executor::exec_call_values(self.store, name, values).map_err(sql_err)?;
        Ok(query_result_to_cobra(result))
    }
}

/// Shape a procedure result as a Cobra return value (mirror of the run()
/// return shaping): SELECT → list of row-dicts, DML → affected count,
/// DDL/transaction → null. A nested `Called` result unwraps to its inner
/// result (notices are dropped).
fn query_result_to_cobra(result: QueryResult) -> CValue {
    match result {
        QueryResult::Select { columns, rows, .. } => {
            let out: Vec<CValue> = rows
                .into_iter()
                .map(|row| {
                    let mut dict = Dict::new();
                    for (col, cell) in columns.iter().zip(row) {
                        let key = CValue::Str(Rc::from(col.as_str()));
                        let hk = hash_key(&key).expect("strings are hashable");
                        dict.set(hk, key, sql_to_cobra(&cell));
                    }
                    CValue::Dict(Rc::new(RefCell::new(dict)))
                })
                .collect();
            CValue::List(Rc::new(RefCell::new(out)))
        }
        QueryResult::Mutation { affected, .. } => CValue::Int(affected as i64),
        QueryResult::Called { inner, .. } => query_result_to_cobra(*inner),
        _ => CValue::Null,
    }
}

impl<S: Store> NativeObject for DbHandle<'_, S> {
    fn type_name(&self) -> &str {
        "db"
    }

    fn call_method(&self, name: &str, args: &[CValue]) -> std::result::Result<CValue, NativeError> {
        match name {
            "query" => self.query(args),
            "execute" => self.execute(args),
            // Savepoints let a procedure undo part of its own work (past a
            // named point) without aborting the whole CALL — the deterministic
            // building block for nested error recovery.
            "savepoint" => self.savepoint_op(name, args, |s, n| s.savepoint(n)),
            "rollback_to" => self.savepoint_op(name, args, |s, n| s.rollback_to_savepoint(n)),
            "release" => self.savepoint_op(name, args, |s, n| s.release_savepoint(n)),
            "call" => self.call_proc(args),
            _ => Err(NativeError::new(format!("db has no method '{name}'"))),
        }
    }
}

/// A statement error surfaced into the VM — catchable by the procedure's
/// own try/catch.
fn sql_err(e: SqlError) -> NativeError {
    NativeError::new(e.to_string())
}

// ─── Value conversions ───────────────────────────────────────────────────

/// SQL cell/argument -> Cobra value.
fn sql_to_cobra(v: &Value) -> CValue {
    match v {
        Value::Null => CValue::Null,
        Value::Int(n) => CValue::Int(*n),
        Value::Double(f) => CValue::Float(*f),
        Value::Text(s) => CValue::Str(Rc::from(&**s)),
        Value::Bool(b) => CValue::Bool(*b),
        // Epoch milliseconds, exactly as stored (and as the JSON wire shows).
        Value::Timestamp(t) => CValue::Int(*t),
        Value::Bytes(b) => CValue::Str(Rc::from(base64_encode(b).as_str())),
        // The Cobra VM has its own exact decimal; bridge through the string so
        // the value stays exact across the boundary.
        Value::Decimal(d) => match oxidb_cobra::decimal::Decimal::parse(&d.to_string()) {
            Some(cd) => CValue::Decimal(Rc::new(cd)),
            None => CValue::Float(d.to_f64()),
        },
    }
}

/// Cobra value -> SQL bind parameter (for `db.query`/`db.execute` params).
fn cobra_to_sql_param(v: &CValue) -> std::result::Result<Value, NativeError> {
    Ok(match v {
        CValue::Null => Value::Null,
        CValue::Int(n) => Value::Int(*n),
        CValue::Float(f) => Value::Double(*f),
        CValue::Str(s) => Value::Text((s.to_string()).into()),
        CValue::Bool(b) => Value::Bool(*b),
        CValue::Decimal(d) => match Decimal::parse(&d.inspect()) {
            Some(dec) => Value::Decimal(Box::new(dec)),
            None => Value::Double(d.to_f64()),
        },
        other => {
            return Err(NativeError::new(format!(
                "unsupported parameter type: {}",
                other.type_name()
            )));
        }
    })
}

/// Cobra value -> SQL result cell. Nested containers render as their
/// `inspect` text; anything opaque (functions, the db handle) is an error.
fn cobra_to_sql_cell(name: &str, v: &CValue) -> Result<Value> {
    Ok(match v {
        CValue::Null => Value::Null,
        CValue::Int(n) => Value::Int(*n),
        CValue::Float(f) => Value::Double(*f),
        CValue::Str(s) => Value::Text((s.to_string()).into()),
        CValue::Bool(b) => Value::Bool(*b),
        CValue::Decimal(d) => match Decimal::parse(&d.inspect()) {
            Some(dec) => Value::Decimal(Box::new(dec)),
            None => Value::Double(d.to_f64()),
        },
        CValue::List(_) | CValue::Dict(_) => Value::Text((inspect(v)).into()),
        other => {
            return Err(SqlError::Eval(format!(
                "procedure {name:?}: unsupported result value: {}",
                other.type_name()
            )));
        }
    })
}

// ─── Result shaping ──────────────────────────────────────────────────────

/// Shape `run`'s return value as a result set:
/// - `null` -> empty result set;
/// - a dict -> one row, columns = keys in insertion order;
/// - a list of dicts -> one row each, columns = union of keys in first-seen
///   order (missing keys -> NULL);
/// - a list of anything else -> a single `value` column, one row per item;
/// - a scalar -> a single `value` column, one row.
fn shape_result(name: &str, v: &CValue) -> Result<QueryResult> {
    let dict_keys_err = || {
        SqlError::Eval(format!(
            "procedure {name:?}: procedure result dict keys must be strings"
        ))
    };
    match v {
        CValue::Null => Ok(QueryResult::Select {
            columns: vec![],
            types: vec![],
            rows: vec![],
        }),
        CValue::Dict(d) => {
            let d = d.borrow();
            let mut columns = Vec::with_capacity(d.len());
            let mut row = Vec::with_capacity(d.len());
            for (k, val) in d.in_order() {
                let CValue::Str(s) = k else {
                    return Err(dict_keys_err());
                };
                columns.push(s.to_string());
                row.push(cobra_to_sql_cell(name, val)?);
            }
            Ok(QueryResult::Select {
                types: vec![None; columns.len()],
                columns,
                rows: vec![row],
            })
        }
        CValue::List(l) => {
            let l = l.borrow();
            let all_dicts = !l.is_empty() && l.iter().all(|e| matches!(e, CValue::Dict(_)));
            if !all_dicts {
                let rows = l
                    .iter()
                    .map(|e| Ok(vec![cobra_to_sql_cell(name, e)?]))
                    .collect::<Result<Vec<_>>>()?;
                return Ok(QueryResult::Select {
                    columns: vec!["value".into()],
                    types: vec![None],
                    rows,
                });
            }
            // Columns: union of dict keys, first-seen order.
            let mut columns: Vec<String> = Vec::new();
            for e in l.iter() {
                let CValue::Dict(d) = e else { unreachable!() };
                for (k, _) in d.borrow().in_order() {
                    let CValue::Str(s) = k else {
                        return Err(dict_keys_err());
                    };
                    if !columns.iter().any(|c| c == &**s) {
                        columns.push(s.to_string());
                    }
                }
            }
            let mut rows = Vec::with_capacity(l.len());
            for e in l.iter() {
                let CValue::Dict(d) = e else { unreachable!() };
                let d = d.borrow();
                let mut row = Vec::with_capacity(columns.len());
                for col in &columns {
                    let key = CValue::Str(Rc::from(col.as_str()));
                    let hk = hash_key(&key).expect("strings are hashable");
                    row.push(match d.get(&hk) {
                        Some((_, val)) => cobra_to_sql_cell(name, val)?,
                        None => Value::Null,
                    });
                }
                rows.push(row);
            }
            Ok(QueryResult::Select {
                types: vec![None; columns.len()],
                columns,
                rows,
            })
        }
        CValue::Int(_)
        | CValue::Float(_)
        | CValue::Str(_)
        | CValue::Bool(_)
        | CValue::Decimal(_) => Ok(QueryResult::Select {
            columns: vec!["value".into()],
            types: vec![None],
            rows: vec![vec![cobra_to_sql_cell(name, v)?]],
        }),
        other => Err(SqlError::Eval(format!(
            "procedure {name:?}: unsupported result type {}",
            other.type_name()
        ))),
    }
}
