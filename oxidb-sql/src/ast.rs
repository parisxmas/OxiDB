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
    Insert {
        table: String,
        /// Column names if an explicit list was given; else insert positionally.
        columns: Option<Vec<String>>,
        /// One expression tuple per row.
        rows: Vec<Vec<Expr>>,
    },
    Select(SelectQuery),
    Update {
        table: String,
        assignments: Vec<(String, Expr)>,
        filter: Option<Expr>,
    },
    Delete {
        table: String,
        filter: Option<Expr>,
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

/// What a [`Statement::Show`] statement enumerates.
#[derive(Debug, Clone, PartialEq)]
pub enum ShowKind {
    Tables,
    Views,
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
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// The body of a query: one SELECT, or a UNION [ALL] of two bodies.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryBody {
    Select(Box<SelectStmt>),
    SetOp {
        /// `true` = UNION ALL (keep duplicates); `false` = UNION (distinct).
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
    pub from: TableRef,
    pub joins: Vec<Join>,
    pub projection: Vec<SelectItem>,
    pub filter: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
    /// `(expr, ascending)` sort keys, in priority order.
    pub order_by: Vec<(Expr, bool)>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// A table reference in FROM/JOIN, with optional alias.
#[derive(Debug, Clone, PartialEq)]
pub struct TableRef {
    pub name: String,
    pub alias: Option<String>,
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
    Aggregate {
        func: AggFunc,
        arg: Option<Box<Expr>>,
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
}
