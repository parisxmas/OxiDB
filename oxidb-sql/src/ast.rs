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
}

/// A single-table SELECT (the Phase 1 subset).
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub table: String,
    pub projection: Projection,
    pub filter: Option<Expr>,
    /// `(column, ascending)` sort keys, in priority order.
    pub order_by: Vec<(String, bool)>,
    pub limit: Option<usize>,
}

/// SELECT projection: either all columns or an explicit list.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    All,
    Columns(Vec<String>),
}

/// A scalar expression appearing in WHERE / VALUES / SET / ORDER BY.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column(String),
    Literal(Value),
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
    /// A DDL statement (CREATE/DROP) executed.
    Ddl,
}
