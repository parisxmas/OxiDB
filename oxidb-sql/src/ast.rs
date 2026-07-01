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
        column: String,
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
    Select(SelectStmt),
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
