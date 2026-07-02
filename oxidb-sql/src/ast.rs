//! The SQL engine's own logical AST.
//!
//! `sqlparser` produces a large, dialect-rich syntax tree; we translate the
//! subset we support (see [`crate::parser`]) into these compact,
//! executor-friendly types so the executor never depends on `sqlparser` shapes.

use crate::catalog::Table;
use crate::types::Value;

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
    /// Transaction control (scoped to a single `execute()` call in Phase 2).
    Begin,
    Commit,
    Rollback,
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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnOp {
    Not,
    Neg,
}

/// The result of executing one statement.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryResult {
    /// A SELECT result set.
    Select {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
    },
    /// An INSERT/UPDATE/DELETE, with the number of rows affected.
    Mutation { affected: usize },
    /// A DDL statement (CREATE/DROP table or index) executed.
    Ddl,
    /// A transaction control statement (`BEGIN`/`COMMIT`/`ROLLBACK`).
    Transaction,
}
