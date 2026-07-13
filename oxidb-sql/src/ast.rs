//! The SQL engine's own logical AST.
//!
//! `sqlparser` produces a large, dialect-rich syntax tree; we translate the
//! subset we support (see [`crate::parser`]) into these compact,
//! executor-friendly types so the executor never depends on `sqlparser` shapes.

use crate::catalog::Table;
use crate::types::{SqlType, Value};

/// A single executable statement.
#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    CreateTable {
        table: Table,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    CreateIndex {
        name: String,
        table: String,
        columns: Vec<String>,
        if_not_exists: bool,
    },
    DropIndex {
        name: String,
        if_exists: bool,
    },
    /// `CREATE [OR REPLACE] VIEW name AS SELECT ...` — the view is stored as
    /// SQL text and re-executed when referenced.
    CreateView {
        name: String,
        query_sql: String,
        or_replace: bool,
    },
    DropView {
        name: String,
        if_exists: bool,
    },
    /// `CREATE [OR ALTER] PROCEDURE name(p TYPE, ...) AS BEGIN ...; END` —
    /// the body is stored as SQL text with parameter references already
    /// rewritten to `$1..$N` (see `parser::translate_create_procedure`).
    CreateProcedure {
        name: String,
        def: crate::catalog::ProcedureDef,
        or_alter: bool,
    },
    DropProcedure {
        name: String,
        if_exists: bool,
    },
    /// `CALL name(arg, ...)` — run a stored procedure. Atomic: wrapped in an
    /// implicit transaction unless one is already open.
    Call {
        name: String,
        args: Vec<Expr>,
    },
    Insert {
        table: String,
        /// Column names if an explicit list was given; else insert positionally.
        columns: Option<Vec<String>>,
        /// One expression tuple per row.
        rows: Vec<Vec<Expr>>,
        /// `RETURNING <items>`: project the inserted rows back as a result
        /// set (PostgreSQL-style; how generated keys are read back).
        returning: Option<Vec<SelectItem>>,
    },
    /// `ALTER TABLE <table> <op>` — one operation per statement.
    AlterTable {
        table: String,
        op: AlterOp,
    },
    Select(SelectQuery),
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
        /// `RETURNING <items>`: project the updated rows back as a result
        /// set (EF Core emits `RETURNING 1` to count affected rows).
        returning: Option<Vec<SelectItem>>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
        /// `RETURNING <items>`: project the deleted rows back as a result set.
        returning: Option<Vec<SelectItem>>,
    },
    /// Transaction control. Within one `execute()` call these are
    /// batch-scoped; through `execute_params_in_session` they span calls
    /// (interactive transactions, ADR-0013 Phase B).
    Begin,
    Commit,
    Rollback,
    Savepoint(String),
    RollbackToSavepoint(String),
    ReleaseSavepoint(String),
    /// Catalog introspection: `SHOW TABLES` / `SHOW VIEWS` /
    /// `SHOW INDEXES [FROM table]` / `DESCRIBE table`. Read-only; answered
    /// from the catalog as an ordinary result set.
    Show(ShowKind),
}

/// One `ALTER TABLE` operation. Serialized into the WAL.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[allow(clippy::enum_variant_names)] // the shared "Column" postfix is the point
pub enum AlterOp {
    AddColumn(crate::catalog::Column),
    DropColumn(String),
    RenameColumn { old: String, new: String },
}

/// What a [`Statement::Show`] statement enumerates.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowKind {
    Tables,
    Views,
    /// All stored procedures.
    Procedures,
    /// All secondary indexes, or only those of one table.
    Indexes(Option<String>),
    /// The columns of one table (`DESCRIBE t` / `SHOW COLUMNS FROM t`).
    Columns(String),
}

/// A full query: a plain SELECT or a set operation (UNION [ALL]) tree, plus
/// the outer ORDER BY / LIMIT / OFFSET that apply to the combined result.
///
/// For a plain SELECT the parser pushes ORDER BY / LIMIT / OFFSET *into* the
/// [`SelectStmt`] and leaves the outer clauses empty, so the single-select
/// fast path is unchanged.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectQuery {
    pub body: QueryBody,
    /// Outer sort keys (set-operation results only): bare output-column names
    /// or 1-based positions.
    pub order_by: Vec<(Expr, bool)>,
    pub limit: Option<LimitExpr>,
    pub offset: Option<LimitExpr>,
}

/// A LIMIT/OFFSET operand: a literal count, or a bind parameter (EF Core
/// parameterizes Skip/Take). Resolved against `params` at execution time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LimitExpr {
    Count(usize),
    /// 0-based parameter slot (`$N` - 1).
    Param(usize),
}

/// Which set operation combines two query bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetOpKind {
    Union,
    Except,
    Intersect,
}

impl SetOpKind {
    pub fn name(self) -> &'static str {
        match self {
            SetOpKind::Union => "UNION",
            SetOpKind::Except => "EXCEPT",
            SetOpKind::Intersect => "INTERSECT",
        }
    }
}

/// The body of a query: one SELECT, or a set operation
/// (UNION/EXCEPT/INTERSECT [ALL]) of two bodies.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryBody {
    Select(Box<SelectStmt>),
    SetOp {
        op: SetOpKind,
        /// `true` = ALL (bag semantics); `false` = distinct (set semantics).
        all: bool,
        left: Box<QueryBody>,
        right: Box<QueryBody>,
    },
}

/// A SELECT, possibly with inner joins and aggregation.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    /// `SELECT DISTINCT`: deduplicate output rows (after projection and
    /// ordering, before LIMIT/OFFSET).
    pub distinct: bool,
    /// `SELECT DISTINCT ON (exprs)`: keep the first row of each group of these
    /// expressions after ORDER BY (Postgres semantics). Empty = not used.
    pub distinct_on: Vec<Expr>,
    /// `None` = FROM-less SELECT (`SELECT 1`): expressions are evaluated once
    /// against a single implicit row with no columns.
    pub from: Option<TableRef>,
    pub joins: Vec<Join>,
    pub projection: Vec<SelectItem>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    /// `(expr, ascending)` sort keys, in priority order.
    pub order_by: Vec<(Expr, bool)>,
    pub limit: Option<LimitExpr>,
    pub offset: Option<LimitExpr>,
}

/// A table reference in FROM/JOIN, with optional alias. A derived table
/// (`FROM (SELECT ...) AS alias`) carries its subquery here; `name` is then
/// the alias (for error messages) and the catalog is never consulted.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
    pub subquery: Option<Box<SelectQuery>>,
    /// `JOIN LATERAL (SELECT ...)`: the subquery may reference columns of the
    /// tables to its left and is re-executed per left row. Only meaningful on
    /// a join's derived table.
    pub lateral: bool,
}

impl TableRef {
    /// The name columns are qualified by: the alias if present, else the table.
    pub fn key(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// A JOIN clause with its kind and `ON` predicate.
#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub table: TableRef,
    pub kind: JoinKind,
    pub on: Expr,
}

/// The kind of join. `Right` is executed by swapping sides and running a
/// `Left`; `Full` is a `Left` plus the unmatched right rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Left,
    Right,
    Full,
}

/// One item in a SELECT projection.
#[derive(Debug, Clone, PartialEq)]
pub enum SelectItem {
    /// `*`
    Wildcard,
    /// `t.*`
    QualifiedWildcard(String),
    /// An expression with an optional output alias.
    Expr { expr: Expr, alias: Option<String> },
}

/// A scalar (or aggregate) expression.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    /// A column reference, optionally table-qualified (`t.col`).
    Column {
        table: Option<String>,
        name: String,
    },
    /// A column reference resolved to a positional index into the current row.
    /// Produced by the executor's bind pass; evaluating it is O(1) (no name
    /// lookup). Not produced by the parser.
    Col(usize),
    Literal(Value),
    /// A bind parameter: `?` (assigned left-to-right) or `$N` (`index = N-1`).
    Param(usize),
    Binary {
        op: BinOp,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    Unary {
        op: UnOp,
        expr: Box<Expr>,
    },
    /// `expr IS NULL` (`negated = false`) or `expr IS NOT NULL` (`negated = true`).
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// An aggregate function call. `arg` is `None` for `COUNT(*)`.
    /// `distinct` folds each distinct value once (`COUNT(DISTINCT x)`,
    /// `SUM(DISTINCT x)`, `AVG(DISTINCT x)`; a no-op for MIN/MAX).
    Aggregate {
        func: AggFunc,
        arg: Option<Box<Expr>>,
        distinct: bool,
    },
    /// A scalar function evaluated per row (`COALESCE`, `NULLIF`, ...).
    Func {
        func: ScalarFunc,
        args: Vec<Expr>,
    },
    /// `expr [NOT] IN (e1, e2, ...)` with SQL three-valued NULL semantics.
    In {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// An uncorrelated scalar subquery (`(SELECT ...)`): one column, at most
    /// one row (zero rows evaluate to NULL). Resolved to a `Literal` before
    /// row evaluation.
    Subquery(Box<SelectQuery>),
    /// `expr [NOT] IN (SELECT ...)`: one output column. Resolved to `In`
    /// before row evaluation.
    InSubquery {
        expr: Box<Expr>,
        query: Box<SelectQuery>,
        negated: bool,
    },
    /// A **correlated** scalar subquery: `outer[k]` evaluates against the
    /// outer row and binds to `Param(base + k)` inside `query`, which is
    /// re-executed per outer row. Produced by the executor's resolution pass;
    /// never by the parser.
    CorrScalar {
        query: Box<SelectQuery>,
        outer: Vec<Expr>,
        base: usize,
    },
    /// A **correlated** `expr [NOT] IN (SELECT ...)`, same mechanism as
    /// [`Expr::CorrScalar`].
    CorrIn {
        expr: Box<Expr>,
        query: Box<SelectQuery>,
        outer: Vec<Expr>,
        base: usize,
        negated: bool,
    },
    /// A window function call: `func() OVER (PARTITION BY ... ORDER BY ...)`.
    /// Whole-partition (or, with ORDER BY, running) evaluation; explicit
    /// frames are not supported.
    Window {
        func: WindowFunc,
        partition_by: Vec<Expr>,
        order_by: Vec<(Expr, bool)>,
    },
}

/// A row-scalar function. All of these ride the single [`Expr::Func`] node,
/// so adding one changes no expression traversals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarFunc {
    /// First non-NULL argument (also spelled `IFNULL` with two arguments).
    Coalesce,
    /// Smallest / largest non-NULL argument (variadic; NULLs ignored).
    Least,
    Greatest,
    /// NULL when the two arguments are equal, else the first.
    NullIf,
    Upper,
    Lower,
    /// Character length of a string.
    Length,
    /// `SUBSTRING(s, start [, len])` — 1-based, character-based.
    Substring,
    /// Variadic string concatenation (NULL-propagating, like `||`).
    Concat,
    Trim,
    Ltrim,
    Rtrim,
    /// `REPLACE(s, from, to)`.
    Replace,
    Abs,
    /// `ROUND(x)` (to integer) or `ROUND(x, n)` (to `n` fractional digits,
    /// half-up). Exact for DECIMAL, IEEE-754 for DOUBLE, identity for INT.
    Round,
    /// `CAST(expr AS type)`.
    Cast(SqlType),
    /// `expr [NOT] LIKE pattern [ESCAPE c]` — args: `[expr, pattern]`.
    Like {
        negated: bool,
        escape: Option<char>,
    },
    /// `CASE WHEN c THEN v ... [ELSE e] END` — args: `[c1, v1, c2, v2, ...]`
    /// with the ELSE expression last when `has_else`. Lazily evaluated, so
    /// branches short-circuit.
    Case {
        has_else: bool,
    },
    /// `NOW()` / `CURRENT_TIMESTAMP` — the current UTC timestamp
    /// (millisecond precision; evaluated at expression time).
    Now,
    /// `EXTRACT(part FROM ts)` — one component of a timestamp, in UTC.
    /// Integer for every part except EPOCH (fractional seconds, DOUBLE).
    Extract(DatePart),
    /// `DATE_TRUNC('part', ts)` — the timestamp truncated to the start of
    /// `part`, in UTC. The part is resolved from the literal at parse time.
    DateTrunc(DatePart),
    /// `FLOOR(x)` / `CEILING(x)` — integer-valued result in the argument's
    /// numeric type (like ABS).
    Floor,
    Ceil,
    /// `POWER(x, y)` — always DOUBLE.
    Power,
    /// `SQRT(x)` — always DOUBLE; negative argument errors.
    Sqrt,
    /// `POSITION(sub IN s)` / `STRPOS(s, sub)` — 1-based character index of
    /// the first occurrence, 0 when absent. args: `[s, sub]`.
    Position,
    /// `LPAD(s, len [, fill])` / `RPAD(s, len [, fill])` — pad (or truncate)
    /// `s` to `len` characters; `fill` defaults to a space.
    Lpad,
    Rpad,
}

/// A date/time component for [`ScalarFunc::Extract`] / [`ScalarFunc::DateTrunc`].
/// All computation is calendar-UTC over epoch-millisecond timestamps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatePart {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    /// Day of week, PostgreSQL numbering: Sunday = 0 .. Saturday = 6.
    Dow,
    /// Day of year: 1..=366.
    Doy,
    /// ISO 8601 week number (EXTRACT only).
    Week,
    /// Seconds since epoch including the fractional part (EXTRACT only).
    Epoch,
}

/// The function of a window expression.
#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunc {
    RowNumber,
    Rank,
    DenseRank,
    /// An aggregate over the window: whole partition without ORDER BY, a
    /// running (peers-inclusive) aggregate with it.
    Agg(AggFunc, Option<Box<Expr>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    /// `mode() WITHIN GROUP (ORDER BY expr)` — the most frequent value of the
    /// within-group expression (ties broken by the smallest value). The arg
    /// carries that within-group expression.
    Mode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
    Add,
    Sub,
    Mul,
    Div,
    /// `%` — remainder (integer or float; sign follows the dividend).
    Mod,
    /// `||` — string concatenation (NULL-propagating).
    Concat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    Not,
    Neg,
}

/// The result of executing one statement.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    /// A SELECT result set. `types` carries the statically-known column
    /// types (`None` = unknown), aligned with `columns`.
    Select {
        columns: Vec<String>,
        types: Vec<Option<SqlType>>,
        rows: Vec<Vec<Value>>,
    },
    /// An INSERT/UPDATE/DELETE, with the number of rows affected. For an
    /// INSERT that assigned AUTO_INCREMENT values, `last_insert_id` is the
    /// last one assigned.
    Mutation {
        affected: usize,
        last_insert_id: Option<i64>,
    },
    /// A DDL statement (CREATE/DROP table or index) executed.
    Ddl,
    /// A transaction control statement (`BEGIN`/`COMMIT`/`ROLLBACK`).
    Transaction,
    /// A `CALL` of a COBRA procedure that printed notices: the shaped result
    /// plus everything `print` wrote (one entry per line). Only constructed
    /// when `notices` is non-empty.
    Called {
        inner: Box<QueryResult>,
        notices: Vec<String>,
    },
}
