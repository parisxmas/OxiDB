//! Per-connection state for a PostgreSQL-wire client, and the one place a
//! statement actually reaches the SQL engine.
//!
//! `mod.rs` owns protocol I/O; this owns what the connection *is* — who is
//! authenticated, which database, the parked interactive transaction, the
//! prepared statements and portals — and turns a statement into the replies
//! the protocol should emit.

use std::collections::HashMap;
use std::sync::Arc;

use oxidb_sql::{CommandKind, QueryResult, SqlEngine, SqlType, Value};

use super::errors::{PgError, SQLSTATE_IN_FAILED_TRANSACTION, SQLSTATE_READ_ONLY_SQL_TRANSACTION};
use super::types;
use super::wire::{FieldDesc, TX_FAILED, TX_IDLE, TX_IN};
use crate::auth::Role;

/// One unit of what a statement produced, ready for the protocol to write out.
#[derive(Clone)]
pub enum Reply {
    /// A result set (or a chunk of one). `tag` is the `CommandComplete` that
    /// closes it — `None` when the portal suspended part-way and the statement
    /// has not finished.
    Rows {
        fields: Vec<FieldDesc>,
        rows: Vec<Vec<Value>>,
        tag: Option<String>,
    },
    /// A statement with no result set: just its `CommandComplete` tag.
    Tag(String),
    /// A `print` from a COBRA procedure, surfaced as `NoticeResponse`.
    Notice(String),
    /// The row limit was reached: `PortalSuspended` rather than a completion.
    Suspended,
}

/// A `Parse`d statement: the text, plus the parameter types the client
/// declared (0 = "server decides", which this server answers as `text`).
#[derive(Clone)]
pub struct Prepared {
    pub sql: String,
    pub param_oids: Vec<i32>,
}

/// A bound statement: parameters decoded, awaiting `Describe`/`Execute`.
pub struct Portal {
    pub sql: String,
    pub params: Vec<Value>,
    /// Per-column format codes from `Bind`: empty = all text, one entry = that
    /// format for every column, else one per column.
    pub result_formats: Vec<i16>,
    /// The statement's output, produced by whichever of `Describe`/`Execute`
    /// touches the portal first and consumed by `Execute`.
    ///
    /// A portal runs **once**. `Describe` has to answer with the portal's
    /// `RowDescription`, and the engine cannot report a result's shape without
    /// producing it — so the run happens at `Describe` time and `Execute`
    /// streams the buffer. (PostgreSQL describes without executing; the only
    /// visible difference is that a statement's error surfaces one message
    /// earlier, still inside the same batch.)
    pub executed: Option<Executed>,
    /// A `Describe` already sent this portal's `RowDescription`; `Execute`
    /// must not send a second one.
    pub described: bool,
}

/// A portal's buffered output and how far the client has consumed it.
pub struct Executed {
    pub replies: Vec<Reply>,
    /// Index of the next reply to write.
    pub next: usize,
    /// Row cursor inside `replies[next]` when that reply is a result set.
    pub row: usize,
}

pub struct PgSession {
    pub user: String,
    pub database: String,
    pub role: Role,
    /// `Role::Read` may only run SELECT/SHOW — the same gate the OxiWire path
    /// applies (`async_server.rs`, `sql_readonly`).
    pub readonly: bool,
    /// Raft is active: writes on this port would apply to one node only, so
    /// they are refused rather than silently diverging a replica.
    pub cluster: bool,
    pub engine: Arc<SqlEngine>,
    /// The engine-side interactive transaction, parked between statements.
    pub sql_tx: Option<u64>,
    /// A statement failed inside a transaction. The engine has already rolled
    /// it back, but PostgreSQL clients expect every further statement to be
    /// refused until they send ROLLBACK/COMMIT, and psycopg relies on it.
    pub failed_tx: bool,
    pub prepared: HashMap<String, Prepared>,
    pub portals: HashMap<String, Portal>,
    /// Session variables a client SET; remembered so SHOW can answer.
    pub settings: HashMap<String, String>,
}

impl PgSession {
    pub fn new(
        user: String,
        database: String,
        role: Role,
        cluster: bool,
        engine: Arc<SqlEngine>,
    ) -> Self {
        PgSession {
            user,
            database,
            role,
            readonly: role == Role::Read,
            cluster,
            engine,
            sql_tx: None,
            failed_tx: false,
            prepared: HashMap::new(),
            portals: HashMap::new(),
            settings: HashMap::new(),
        }
    }

    /// The `ReadyForQuery` transaction status byte.
    pub fn tx_status(&self) -> u8 {
        if self.failed_tx {
            TX_FAILED
        } else if self.sql_tx.is_some() {
            TX_IN
        } else {
            TX_IDLE
        }
    }

    /// Run one statement text (possibly several statements, in simple query
    /// mode) with already-decoded parameters.
    pub fn execute(&mut self, sql: &str, params: &[Value]) -> Result<Vec<Reply>, PgError> {
        self.run(sql, params, &[])
    }

    fn run(
        &mut self,
        sql: &str,
        params: &[Value],
        formats: &[i16],
    ) -> Result<Vec<Reply>, PgError> {
        if let Some(replies) = super::catalog::intercept(self, sql)? {
            return Ok(replies);
        }
        self.guard(sql)?;

        // Classify first: it validates syntax against the same cached parse
        // execution will use, and the tags have to name the verb, which the
        // results themselves do not carry.
        let kinds = self.engine.command_kinds(sql)?;

        let had_tx = self.sql_tx.is_some();
        let results = self
            .engine
            .execute_params_in_session(sql, params, &mut self.sql_tx)
            .map_err(|e| {
                // A failed statement aborts the engine's transaction. Remember
                // it so the protocol reports 'E' and refuses further work.
                if had_tx {
                    self.failed_tx = true;
                }
                PgError::from(e)
            })?;

        let mut out = Vec::with_capacity(results.len());
        for (i, result) in results.into_iter().enumerate() {
            let kind = kinds.get(i).copied().unwrap_or(CommandKind::Select);
            Self::push_reply(&mut out, result, kind, formats);
        }
        Ok(out)
    }

    /// Refuse a statement the session is not allowed to run at all, before the
    /// engine sees it.
    fn guard(&self, sql: &str) -> Result<(), PgError> {
        if self.failed_tx {
            return Err(PgError::new(
                SQLSTATE_IN_FAILED_TRANSACTION,
                "current transaction is aborted, commands ignored until end of transaction block",
            ));
        }
        if !self.readonly && !self.cluster {
            return Ok(());
        }
        // Both remaining gates ask the same question. An unparseable statement
        // is refused rather than passed through — the same rule the REST SQL
        // surface applies to untrusted callers.
        let read_only = oxidb_sql::is_read_only(sql).map_err(PgError::from)?;
        if read_only {
            return Ok(());
        }
        if self.readonly {
            return Err(PgError::denied(
                "permission denied: role 'read' may only execute SELECT/SHOW statements",
            ));
        }
        Err(PgError::new(
            SQLSTATE_READ_ONLY_SQL_TRANSACTION,
            "writes over the PostgreSQL port are not replicated in cluster mode — \
             use the OxiWire port (OXIDB_ADDR) for writes",
        ))
    }

    /// Turn one engine result into replies, honouring the requested result
    /// formats.
    fn push_reply(out: &mut Vec<Reply>, result: QueryResult, kind: CommandKind, formats: &[i16]) {
        match result {
            QueryResult::Select {
                columns,
                types: col_types,
                rows,
            } => {
                let fields = describe_columns(&columns, &col_types, &rows, formats);
                let tag = Some(kind.tag(rows.len()));
                out.push(Reply::Rows { fields, rows, tag });
            }
            QueryResult::Mutation { affected, .. } => out.push(Reply::Tag(kind.tag(affected))),
            QueryResult::Ddl | QueryResult::Transaction => out.push(Reply::Tag(kind.tag(0))),
            QueryResult::Called { inner, notices } => {
                for n in notices {
                    out.push(Reply::Notice(n));
                }
                Self::push_reply(out, *inner, kind, formats);
            }
        }
    }

    /// Run a portal's statement, once. Later calls are no-ops.
    fn ensure_executed(&mut self, name: &str) -> Result<(), PgError> {
        match self.portals.get(name) {
            None => return Err(PgError::protocol(format!("portal {name:?} does not exist"))),
            Some(p) if p.executed.is_some() => return Ok(()),
            Some(_) => {}
        }
        let (sql, params, formats) = {
            let p = &self.portals[name];
            (p.sql.clone(), p.params.clone(), p.result_formats.clone())
        };
        let replies = self.run(&sql, &params, &formats)?;
        if let Some(p) = self.portals.get_mut(name) {
            p.executed = Some(Executed {
                replies,
                next: 0,
                row: 0,
            });
        }
        Ok(())
    }

    /// The columns a `Describe` on this portal should report — `None` when the
    /// statement returns no rows (`NoData`).
    pub fn portal_fields(&mut self, name: &str) -> Result<Option<Vec<FieldDesc>>, PgError> {
        self.ensure_executed(name)?;
        let fields = self.portals.get(name).and_then(|p| {
            p.executed.as_ref().and_then(|e| {
                e.replies.iter().find_map(|r| match r {
                    Reply::Rows { fields, .. } => Some(fields.clone()),
                    _ => None,
                })
            })
        });
        if fields.is_some()
            && let Some(p) = self.portals.get_mut(name)
        {
            p.described = true;
        }
        Ok(fields)
    }

    /// Take the next slice of a portal's output, at most `max_rows` rows
    /// (`0` = all). What is not taken stays on the portal for the next
    /// `Execute`, which is how the protocol's row limit works.
    pub fn execute_portal(&mut self, name: &str, max_rows: usize) -> Result<Vec<Reply>, PgError> {
        self.ensure_executed(name)?;
        let described = self.portals.get(name).is_some_and(|p| p.described);
        let Some(state) = self.portals.get_mut(name).and_then(|p| p.executed.as_mut()) else {
            return Err(PgError::protocol(format!("portal {name:?} does not exist")));
        };

        let mut out = Vec::new();
        let mut budget = max_rows;
        while state.next < state.replies.len() {
            match &state.replies[state.next] {
                Reply::Rows { fields, rows, tag } => {
                    let end = slice_end(state.row, rows.len(), budget);
                    let taken = end - state.row;
                    let chunk: Vec<Vec<Value>> = rows[state.row..end].to_vec();
                    let finished = end == rows.len();
                    out.push(Reply::Rows {
                        fields: fields.clone(),
                        rows: chunk,
                        // The completion tag only goes out with the last chunk.
                        tag: if finished { tag.clone() } else { None },
                    });
                    state.row = end;
                    if !finished {
                        out.push(Reply::Suspended);
                        return Ok(out);
                    }
                    state.next += 1;
                    state.row = 0;
                    if max_rows > 0 {
                        budget = budget.saturating_sub(taken);
                    }
                }
                other => {
                    out.push(other.clone());
                    state.next += 1;
                }
            }
        }
        // A second Execute on a drained portal is legal and returns nothing;
        // `described` tells the caller not to re-send the row description.
        let _ = described;
        Ok(out)
    }

    /// Roll back whatever this connection left open. Called on disconnect —
    /// a parked transaction otherwise holds its row locks until they time out.
    pub fn close(&mut self) {
        if let Some(id) = self.sql_tx.take() {
            self.engine.rollback_session_txn(id);
        }
    }
}

fn slice_end(from: usize, len: usize, max_rows: usize) -> usize {
    if max_rows == 0 {
        len
    } else {
        (from + max_rows).min(len)
    }
}

/// Build the `RowDescription` for a result set.
///
/// The engine reports column types statically where it can; where it cannot
/// (`None`), the first non-NULL value of the column is a better guess than
/// calling everything text — a client that sees `int8` can parse an integer,
/// one that sees `text` will hand the application a string.
pub fn describe_columns(
    columns: &[String],
    col_types: &[Option<SqlType>],
    rows: &[Vec<Value>],
    formats: &[i16],
) -> Vec<FieldDesc> {
    columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let declared = col_types.get(i).copied().flatten();
            let oid = match declared {
                Some(t) => types::oid_of(Some(t)),
                None => rows
                    .iter()
                    .find_map(|r| match r.get(i) {
                        Some(Value::Null) | None => None,
                        Some(v) => Some(types::oid_of_value(v)),
                    })
                    .unwrap_or(types::OID_TEXT),
            };
            let format = format_for(formats, i);
            FieldDesc {
                name: name.clone(),
                type_oid: oid,
                type_len: types::type_len(oid),
                format,
            }
        })
        .collect()
}

/// The format code for column `i`: no entries = text, one entry = it applies
/// to every column, otherwise one per column (the protocol's rule).
pub fn format_for(formats: &[i16], i: usize) -> i16 {
    match formats.len() {
        0 => types::FORMAT_TEXT,
        1 => formats[0],
        _ => formats.get(i).copied().unwrap_or(types::FORMAT_TEXT),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_codes_follow_the_protocols_broadcast_rule() {
        assert_eq!(format_for(&[], 3), types::FORMAT_TEXT);
        assert_eq!(format_for(&[1], 3), types::FORMAT_BINARY);
        assert_eq!(format_for(&[0, 1], 1), types::FORMAT_BINARY);
        assert_eq!(format_for(&[0, 1], 9), types::FORMAT_TEXT);
    }

    #[test]
    fn an_untyped_column_is_described_from_its_first_non_null_value() {
        let cols = vec!["a".to_string()];
        let rows = vec![vec![Value::Null], vec![Value::Int(1)]];
        let f = describe_columns(&cols, &[None], &rows, &[]);
        assert_eq!(f[0].type_oid, types::OID_INT8);

        // All-NULL falls back to text, which every client can render.
        let f = describe_columns(&cols, &[None], &[vec![Value::Null]], &[]);
        assert_eq!(f[0].type_oid, types::OID_TEXT);
    }

    #[test]
    fn a_declared_column_type_wins_over_the_data() {
        let cols = vec!["a".to_string()];
        let rows = vec![vec![Value::Int(1)]];
        let f = describe_columns(&cols, &[Some(SqlType::Decimal)], &rows, &[]);
        assert_eq!(f[0].type_oid, types::OID_NUMERIC);
    }

    #[test]
    fn row_slicing_respects_the_execute_row_limit() {
        assert_eq!(slice_end(0, 10, 0), 10);
        assert_eq!(slice_end(0, 10, 3), 3);
        assert_eq!(slice_end(8, 10, 5), 10);
    }
}
