//! Translate `sqlparser` syntax trees into our logical [`ast`](crate::ast).
//!
//! Only the supported subset is accepted; anything outside it becomes
//! [`SqlError::Unsupported`] rather than being silently mis-handled.

use sqlparser::ast as sp;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::ast::{
    AggFunc, AlterOp, BinOp, Expr, Join, JoinKind, LimitExpr, QueryBody, ScalarFunc, SelectItem,
    SelectQuery, SelectStmt, ShowKind, Statement, TableRef, UnOp, WindowFunc,
};
use crate::catalog::{Column, Table};
use crate::error::{Result, SqlError};
use crate::types::{SqlType, Value};

/// Parse a SQL string into zero or more logical statements.
///
/// `?` placeholders are numbered left-to-right across the whole string; `$N`
/// placeholders use `N-1` directly. Both resolve against the params slice at
/// execution time.
pub fn parse(sql: &str) -> Result<Vec<Statement>> {
    // `SHOW INDEXES [FROM t]` has no sqlparser variant; recognize it directly
    // when it is the whole input (the only shape clients send it in).
    if let Some(stmt) = parse_show_indexes(sql) {
        return Ok(vec![stmt]);
    }
    // `CREATE PROCEDURE ... LANGUAGE COBRA AS '<base64>'` — sqlparser cannot
    // parse an `AS '<string>'` body ("Expected: an SQL statement"), so the
    // COBRA form is recognized directly from the token stream (ADR-0014).
    if let Some(result) = parse_cobra_procedure(sql) {
        return result.map(|stmt| vec![stmt]);
    }
    let dialect = GenericDialect {};
    let statements =
        Parser::parse_sql(&dialect, sql).map_err(|e| SqlError::Parse(e.to_string()))?;
    let mut next_param = 0usize;
    statements
        .into_iter()
        .map(|s| translate(s, &mut next_param))
        .collect()
}

/// A database-level statement recognized in SQL text. The engine itself is
/// single-database; these are served by the host (the server's session
/// layer), which owns the database registry and session state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseStatement {
    Create { name: String, if_not_exists: bool },
    Drop { name: String, if_exists: bool },
    Show,
    Use { name: String },
}

/// Recognize a **single** `CREATE DATABASE` / `DROP DATABASE` /
/// `SHOW DATABASES` / `USE <db>` statement as the whole input. Returns
/// `None` for any other SQL (including multi-statement batches — database
/// DDL cannot be mixed with ordinary statements).
pub fn parse_database_statement(sql: &str) -> Option<DatabaseStatement> {
    // Cheap prefix gate so ordinary queries never pay a second parse.
    let head = sql.trim_start().get(..16).unwrap_or(sql.trim_start());
    let head = head.to_ascii_lowercase();
    if !(head.starts_with("create database")
        || head.starts_with("drop database")
        || head.starts_with("show databases")
        || head.starts_with("use ")
        || head == "use")
    {
        return None;
    }

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).ok()?;
    let [stmt] = statements.as_slice() else {
        return None;
    };
    match stmt {
        sp::Statement::CreateDatabase {
            db_name,
            if_not_exists,
            ..
        } => Some(DatabaseStatement::Create {
            name: object_name_to_string(db_name).ok()?,
            if_not_exists: *if_not_exists,
        }),
        sp::Statement::Drop {
            object_type: sp::ObjectType::Database,
            names,
            if_exists,
            ..
        } => Some(DatabaseStatement::Drop {
            name: single_object_name(names).ok()?,
            if_exists: *if_exists,
        }),
        sp::Statement::ShowDatabases { .. } => Some(DatabaseStatement::Show),
        sp::Statement::Use(sp::Use::Object(name) | sp::Use::Database(name)) => {
            Some(DatabaseStatement::Use {
                name: object_name_to_string(name).ok()?,
            })
        }
        _ => None,
    }
}

/// A user-management statement recognized in SQL text. Like
/// [`DatabaseStatement`], these are served by the host (which owns the user
/// store), not by the engine. Role strings are validated by the host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserStatement {
    /// `CREATE USER name WITH PASSWORD 'pw' [ROLE role]`
    Create {
        name: String,
        password: String,
        role: Option<String>,
    },
    /// `ALTER USER name [WITH PASSWORD 'pw'] [ROLE role]` (at least one clause)
    Alter {
        name: String,
        password: Option<String>,
        role: Option<String>,
    },
    /// `DROP USER [IF EXISTS] name`
    Drop { name: String, if_exists: bool },
    /// `SHOW USERS`
    Show,
    /// `GRANT role ON DATABASE db TO user`
    Grant {
        role: String,
        database: String,
        user: String,
    },
    /// `REVOKE [role | ALL] ON DATABASE db FROM user`
    Revoke { database: String, user: String },
}

/// Recognize a **single** user-management statement as the whole input.
/// `None` for any other SQL. Tokenized with sqlparser's tokenizer (so quoted
/// passwords behave), matched against the exact grammar above.
pub fn parse_user_statement(sql: &str) -> Option<UserStatement> {
    // Cheap prefix gate so ordinary queries never pay tokenization.
    let head = sql.trim_start().get(..12).unwrap_or(sql.trim_start());
    let head = head.to_ascii_lowercase();
    if !(head.starts_with("create user")
        || head.starts_with("alter user")
        || head.starts_with("drop user")
        || head.starts_with("show users")
        || head.starts_with("grant ")
        || head.starts_with("revoke "))
    {
        return None;
    }

    // Tokenize into just the shapes our grammar uses; anything else bails.
    #[derive(PartialEq)]
    enum Tok {
        Word(String),
        Str(String),
    }
    let dialect = GenericDialect {};
    let raw = sqlparser::tokenizer::Tokenizer::new(&dialect, sql)
        .tokenize()
        .ok()?;
    let mut toks: Vec<Tok> = Vec::new();
    for t in raw {
        use sqlparser::tokenizer::Token;
        match t {
            Token::Whitespace(_) | Token::EOF => {}
            Token::Word(w) => toks.push(Tok::Word(w.value)),
            Token::SingleQuotedString(s) => toks.push(Tok::Str(s)),
            Token::SemiColon => break, // statement end; nothing may follow
            _ => return None,
        }
    }

    let mut i = 0usize;
    let kw = |k: &str, i: &mut usize| -> bool {
        match toks.get(*i) {
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case(k) => {
                *i += 1;
                true
            }
            _ => false,
        }
    };
    fn ident(toks: &[Tok], i: &mut usize) -> Option<String> {
        match toks.get(*i) {
            Some(Tok::Word(w)) => {
                *i += 1;
                Some(w.clone())
            }
            _ => None,
        }
    }
    fn string(toks: &[Tok], i: &mut usize) -> Option<String> {
        match toks.get(*i) {
            Some(Tok::Str(s)) => {
                *i += 1;
                Some(s.clone())
            }
            _ => None,
        }
    }
    let done = |i: usize| i == toks.len();

    if kw("create", &mut i) {
        if !kw("user", &mut i) {
            return None;
        }
        let name = ident(&toks, &mut i)?;
        if !kw("with", &mut i) || !kw("password", &mut i) {
            return None;
        }
        let password = string(&toks, &mut i)?;
        let role = if kw("role", &mut i) {
            Some(ident(&toks, &mut i)?)
        } else {
            None
        };
        return done(i).then_some(UserStatement::Create {
            name,
            password,
            role,
        });
    }
    if kw("alter", &mut i) {
        if !kw("user", &mut i) {
            return None;
        }
        let name = ident(&toks, &mut i)?;
        let mut password = None;
        let mut role = None;
        loop {
            if kw("with", &mut i) {
                if !kw("password", &mut i) {
                    return None;
                }
                password = Some(string(&toks, &mut i)?);
            } else if kw("password", &mut i) {
                password = Some(string(&toks, &mut i)?);
            } else if kw("role", &mut i) {
                role = Some(ident(&toks, &mut i)?);
            } else {
                break;
            }
        }
        if password.is_none() && role.is_none() {
            return None;
        }
        return done(i).then_some(UserStatement::Alter {
            name,
            password,
            role,
        });
    }
    if kw("drop", &mut i) {
        if !kw("user", &mut i) {
            return None;
        }
        let if_exists = if kw("if", &mut i) {
            if !kw("exists", &mut i) {
                return None;
            }
            true
        } else {
            false
        };
        let name = ident(&toks, &mut i)?;
        return done(i).then_some(UserStatement::Drop { name, if_exists });
    }
    if kw("show", &mut i) {
        if !kw("users", &mut i) {
            return None;
        }
        return done(i).then_some(UserStatement::Show);
    }
    if kw("grant", &mut i) {
        let role = ident(&toks, &mut i)?;
        if !kw("on", &mut i) || !kw("database", &mut i) {
            return None;
        }
        let database = ident(&toks, &mut i)?;
        if !kw("to", &mut i) {
            return None;
        }
        let user = ident(&toks, &mut i)?;
        return done(i).then_some(UserStatement::Grant {
            role,
            database,
            user,
        });
    }
    if kw("revoke", &mut i) {
        // Optional role name (or ALL) before ON — the revoke drops the whole
        // per-database override either way.
        if !kw("on", &mut i) {
            let _role = ident(&toks, &mut i)?;
            if !kw("on", &mut i) {
                return None;
            }
        }
        if !kw("database", &mut i) {
            return None;
        }
        let database = ident(&toks, &mut i)?;
        if !kw("from", &mut i) {
            return None;
        }
        let user = ident(&toks, &mut i)?;
        return done(i).then_some(UserStatement::Revoke { database, user });
    }
    None
}

/// Recognize `SHOW INDEXES` / `SHOW INDEX [FROM table]` (optionally
/// `;`-terminated) as the whole input. Returns `None` when the input is
/// anything else, letting `sqlparser` handle it.
fn parse_show_indexes(sql: &str) -> Option<Statement> {
    let mut words = sql.trim().trim_end_matches(';').split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("show") {
        return None;
    }
    let kw = words.next()?;
    if !kw.eq_ignore_ascii_case("indexes") && !kw.eq_ignore_ascii_case("index") {
        return None;
    }
    match (words.next(), words.next(), words.next()) {
        (None, ..) => Some(Statement::Show(ShowKind::Indexes(None))),
        (Some(from), Some(table), None) if from.eq_ignore_ascii_case("from") => {
            let table = table.trim_matches('"').trim_matches('`');
            if table.is_empty() {
                return None;
            }
            Some(Statement::Show(ShowKind::Indexes(Some(table.to_string()))))
        }
        _ => None,
    }
}

/// Recognize a **single** COBRA stored-procedure definition as the whole
/// input (ADR-0014):
///
/// ```sql
/// CREATE [OR ALTER] PROCEDURE name(param TYPE, ...) LANGUAGE COBRA AS '<base64>';
/// ```
///
/// Returns `None` when the input is anything else (including `CREATE
/// PROCEDURE` with a SQL body or another LANGUAGE — sqlparser handles
/// those), and `Some(Err(..))` when the statement *is* `LANGUAGE COBRA` but
/// malformed. The base64 payload rides in `def.body`; the executor decodes
/// and validates it at CREATE time.
fn parse_cobra_procedure(sql: &str) -> Option<Result<Statement>> {
    // Cheap gate so ordinary statements never pay tokenization.
    let head = sql.trim_start().get(..6).unwrap_or(sql.trim_start());
    if !head.eq_ignore_ascii_case("create") || !sql.to_ascii_lowercase().contains("language") {
        return None;
    }

    // Tokenize into just the shapes the grammar uses; anything else bails to
    // sqlparser (which owns the error message for non-COBRA input).
    #[derive(PartialEq)]
    enum Tok {
        Word(String),
        Str(String),
        LParen,
        RParen,
        Comma,
    }
    let dialect = GenericDialect {};
    let raw = sqlparser::tokenizer::Tokenizer::new(&dialect, sql)
        .tokenize()
        .ok()?;
    let mut toks: Vec<Tok> = Vec::new();
    for t in raw {
        use sqlparser::tokenizer::Token;
        match t {
            Token::Whitespace(_) | Token::EOF => {}
            Token::Word(w) => toks.push(Tok::Word(w.value)),
            Token::SingleQuotedString(s) => toks.push(Tok::Str(s)),
            Token::LParen => toks.push(Tok::LParen),
            Token::RParen => toks.push(Tok::RParen),
            Token::Comma => toks.push(Tok::Comma),
            Token::SemiColon => break, // statement end; nothing may follow
            _ => return None,
        }
    }

    let mut i = 0usize;
    let kw = |k: &str, i: &mut usize| -> bool {
        match toks.get(*i) {
            Some(Tok::Word(w)) if w.eq_ignore_ascii_case(k) => {
                *i += 1;
                true
            }
            _ => false,
        }
    };
    fn ident(toks: &[Tok], i: &mut usize) -> Option<String> {
        match toks.get(*i) {
            Some(Tok::Word(w)) => {
                *i += 1;
                Some(w.clone())
            }
            _ => None,
        }
    }

    if !kw("create", &mut i) {
        return None;
    }
    let or_alter = kw("or", &mut i);
    if or_alter && !kw("alter", &mut i) {
        return None;
    }
    if !kw("procedure", &mut i) {
        return None;
    }
    let name = ident(&toks, &mut i)?;

    // Optional parameter list; the whole clause is committed to only once
    // LANGUAGE COBRA is seen, so failures before that fall back to sqlparser.
    let mut declared: Vec<(String, SqlType)> = Vec::new();
    let mut malformed: Option<SqlError> = None;
    if toks.get(i) == Some(&Tok::LParen) {
        i += 1;
        if toks.get(i) == Some(&Tok::RParen) {
            i += 1;
        } else {
            loop {
                let pname = ident(&toks, &mut i)?;
                let tyword = ident(&toks, &mut i)?;
                match param_type_from_word(&tyword) {
                    Some(ty) => {
                        if declared.iter().any(|(n, _)| n.eq_ignore_ascii_case(&pname)) {
                            malformed.get_or_insert(SqlError::Parse(format!(
                                "duplicate procedure parameter {pname:?}"
                            )));
                        }
                        declared.push((pname, ty));
                    }
                    None => {
                        malformed.get_or_insert(SqlError::Unsupported(format!(
                            "data type {tyword} in a COBRA procedure parameter"
                        )));
                    }
                }
                match toks.get(i) {
                    Some(Tok::Comma) => i += 1,
                    Some(Tok::RParen) => {
                        i += 1;
                        break;
                    }
                    _ => return None,
                }
            }
        }
    }

    if !kw("language", &mut i) {
        return None;
    }
    let lang = ident(&toks, &mut i)?;
    if !lang.eq_ignore_ascii_case("cobra") {
        // Another LANGUAGE: let sqlparser parse it (a SQL-statement body
        // works there) and `translate_create_procedure` reject it with the
        // established message.
        return None;
    }

    // From here on the statement is claimed: errors are COBRA-shaped.
    if let Some(e) = malformed {
        return Some(Err(e));
    }
    if !kw("as", &mut i) {
        return Some(Err(SqlError::Parse(
            "LANGUAGE COBRA procedure: expected AS '<base64 .cobrac>'".into(),
        )));
    }
    let Some(Tok::Str(payload)) = toks.get(i) else {
        return Some(Err(SqlError::Parse(
            "LANGUAGE COBRA body must be a single-quoted base64 string".into(),
        )));
    };
    i += 1;

    // Optional `SOURCE '<base64 of the .cobra text>'` — kept verbatim so
    // tooling can show/edit the procedure; never executed.
    let mut source = String::new();
    if kw("source", &mut i) {
        let Some(Tok::Str(src_b64)) = toks.get(i) else {
            return Some(Err(SqlError::Parse(
                "COBRA procedure SOURCE must be a single-quoted base64 string".into(),
            )));
        };
        i += 1;
        match crate::catalog::base64_decode(src_b64) {
            Ok(bytes) => match String::from_utf8(bytes) {
                Ok(s) => source = s,
                Err(_) => {
                    return Some(Err(SqlError::Parse(
                        "COBRA procedure SOURCE is not valid UTF-8".into(),
                    )));
                }
            },
            Err(()) => {
                return Some(Err(SqlError::Parse(
                    "COBRA procedure SOURCE is not valid base64".into(),
                )));
            }
        }
    }

    if i != toks.len() {
        return Some(Err(SqlError::Parse(
            "unexpected tokens after the COBRA procedure body".into(),
        )));
    }

    Some(Ok(Statement::CreateProcedure {
        name,
        def: crate::catalog::ProcedureDef {
            params: declared,
            body: payload.clone(),
            language: crate::catalog::ProcLanguage::Cobra,
            bytecode: Vec::new(),
            source,
        },
        or_alter,
    }))
}

/// The single-word parameter types accepted in a COBRA procedure signature —
/// the same names [`map_data_type`] accepts for SQL procedures, minus the
/// parenthesized/multi-word spellings.
fn param_type_from_word(w: &str) -> Option<SqlType> {
    let ty = match w.to_ascii_uppercase().as_str() {
        "INT" | "INTEGER" | "BIGINT" | "SMALLINT" | "TINYINT" => SqlType::Int,
        "DOUBLE" | "FLOAT" | "REAL" | "DECIMAL" | "NUMERIC" | "DEC" => SqlType::Double,
        "TEXT" | "VARCHAR" | "CHAR" | "STRING" | "NVARCHAR" => SqlType::Text,
        "BOOL" | "BOOLEAN" => SqlType::Bool,
        "TIMESTAMP" | "DATETIME" => SqlType::Timestamp,
        "BLOB" | "BYTEA" | "BINARY" | "VARBINARY" => SqlType::Blob,
        _ => return None,
    };
    Some(ty)
}

fn translate(stmt: sp::Statement, p: &mut usize) -> Result<Statement> {
    match stmt {
        sp::Statement::CreateTable(ct) => translate_create_table(ct),
        sp::Statement::CreateIndex(ci) => translate_create_index(ci),
        sp::Statement::CreateView {
            name,
            query,
            or_replace,
            columns,
            materialized,
            ..
        } => {
            if materialized {
                return Err(SqlError::Unsupported("MATERIALIZED VIEW".into()));
            }
            if !columns.is_empty() {
                return Err(SqlError::Unsupported(
                    "CREATE VIEW with a column list (alias in the SELECT instead)".into(),
                ));
            }
            Ok(Statement::CreateView {
                name: object_name_to_string(&name)?,
                // Store the view body as SQL text (re-parsed on use).
                query_sql: query.to_string(),
                or_replace,
            })
        }
        sp::Statement::CreateProcedure {
            or_alter,
            name,
            params,
            language,
            body,
        } => translate_create_procedure(or_alter, name, params, language, body),
        sp::Statement::DropProcedure {
            if_exists,
            proc_desc,
            ..
        } => {
            let [desc] = proc_desc.as_slice() else {
                return Err(SqlError::Unsupported(
                    "DROP PROCEDURE of multiple procedures".into(),
                ));
            };
            if desc.args.is_some() {
                return Err(SqlError::Unsupported(
                    "DROP PROCEDURE with an argument list".into(),
                ));
            }
            Ok(Statement::DropProcedure {
                name: object_name_to_string(&desc.name)?,
                if_exists,
            })
        }
        sp::Statement::Call(func) => translate_call(func, p),
        sp::Statement::Drop {
            object_type,
            if_exists,
            names,
            ..
        } => match object_type {
            sp::ObjectType::Table => Ok(Statement::DropTable {
                name: single_object_name(&names)?,
                if_exists,
            }),
            sp::ObjectType::Index => Ok(Statement::DropIndex {
                name: single_object_name(&names)?,
                if_exists,
            }),
            sp::ObjectType::View => Ok(Statement::DropView {
                name: single_object_name(&names)?,
                if_exists,
            }),
            other => Err(SqlError::Unsupported(format!("DROP {other:?}"))),
        },
        sp::Statement::Insert(insert) => translate_insert(insert, p),
        sp::Statement::Query(query) => Ok(Statement::Select(translate_query(*query, p)?)),
        sp::Statement::Update {
            table,
            assignments,
            from,
            selection,
            returning,
            ..
        } => {
            if from.is_some() {
                return Err(SqlError::Unsupported("UPDATE ... FROM".into()));
            }
            let table = table_name_from_twj(&table)?;
            let assignments = assignments
                .into_iter()
                .map(|a| translate_assignment(a, p))
                .collect::<Result<Vec<_>>>()?;
            let filter = selection.map(|e| translate_expr(e, p)).transpose()?;
            let returning = translate_returning(returning.as_ref(), p)?;
            Ok(Statement::Update {
                table,
                assignments,
                filter,
                returning,
            })
        }
        sp::Statement::Delete(del) => translate_delete(del, p),
        sp::Statement::StartTransaction { .. } => Ok(Statement::Begin),
        sp::Statement::Commit { .. } => Ok(Statement::Commit),
        sp::Statement::Rollback {
            savepoint: Some(name),
            ..
        } => Ok(Statement::RollbackToSavepoint(name.value)),
        sp::Statement::Rollback { .. } => Ok(Statement::Rollback),
        sp::Statement::Savepoint { name } => Ok(Statement::Savepoint(name.value)),
        sp::Statement::ReleaseSavepoint { name } => Ok(Statement::ReleaseSavepoint(name.value)),
        sp::Statement::AlterTable {
            name, operations, ..
        } => {
            let table = object_name_to_string(&name)?;
            let [op] = operations.as_slice() else {
                return Err(SqlError::Unsupported(
                    "ALTER TABLE with multiple operations (send one per statement)".into(),
                ));
            };
            let op = match op {
                sp::AlterTableOperation::AddColumn { column_def, .. } => {
                    AlterOp::AddColumn(translate_column(&column_def.clone())?)
                }
                sp::AlterTableOperation::DropColumn { column_names, .. } => {
                    let [name] = column_names.as_slice() else {
                        return Err(SqlError::Unsupported(
                            "DROP COLUMN with multiple columns".into(),
                        ));
                    };
                    AlterOp::DropColumn(name.value.clone())
                }
                sp::AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => AlterOp::RenameColumn {
                    old: old_column_name.value.clone(),
                    new: new_column_name.value.clone(),
                },
                other => {
                    return Err(SqlError::Unsupported(format!(
                        "ALTER TABLE operation {other:?}"
                    )));
                }
            };
            Ok(Statement::AlterTable { table, op })
        }
        sp::Statement::ShowTables { .. } => Ok(Statement::Show(ShowKind::Tables)),
        // `SHOW PROCEDURES` parses as a generic SHOW <variable>.
        sp::Statement::ShowVariable { variable }
            if matches!(variable.as_slice(),
                [v] if v.value.eq_ignore_ascii_case("procedures")) =>
        {
            Ok(Statement::Show(ShowKind::Procedures))
        }
        sp::Statement::ShowViews { .. } => Ok(Statement::Show(ShowKind::Views)),
        sp::Statement::ShowColumns { show_options, .. } => {
            let table = show_options
                .show_in
                .and_then(|i| i.parent_name)
                .ok_or_else(|| SqlError::Unsupported("SHOW COLUMNS without FROM <table>".into()))?;
            Ok(Statement::Show(ShowKind::Columns(object_name_to_string(
                &table,
            )?)))
        }
        sp::Statement::ExplainTable { table_name, .. } => Ok(Statement::Show(ShowKind::Columns(
            object_name_to_string(&table_name)?,
        ))),
        other => Err(SqlError::Unsupported(format!("statement: {other}"))),
    }
}

/// Translate `CREATE [OR ALTER] PROCEDURE`. The body's parameter references
/// (unqualified identifiers matching a declared parameter, in expression
/// position only) are rewritten to `$1..$N`, so a CALL is exactly a
/// parameterized batch execution of the stored text.
fn translate_create_procedure(
    or_alter: bool,
    name: sp::ObjectName,
    params: Option<Vec<sp::ProcedureParam>>,
    language: Option<sp::Ident>,
    body: sp::ConditionalStatements,
) -> Result<Statement> {
    if let Some(lang) = language {
        return Err(SqlError::Unsupported(format!(
            "procedure LANGUAGE {lang} (only SQL bodies are supported)"
        )));
    }
    let name = object_name_to_string(&name)?;
    let mut declared: Vec<(String, SqlType)> = Vec::new();
    for prm in params.unwrap_or_default() {
        if prm.mode.is_some() {
            return Err(SqlError::Unsupported("IN/OUT/INOUT parameter modes".into()));
        }
        if declared
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case(&prm.name.value))
        {
            return Err(SqlError::Parse(format!(
                "duplicate procedure parameter {:?}",
                prm.name.value
            )));
        }
        declared.push((prm.name.value.clone(), map_data_type(&prm.data_type)?));
    }

    let mut statements = body.statements().clone();
    if statements.is_empty() {
        return Err(SqlError::Unsupported(
            "procedure body has no statements".into(),
        ));
    }
    // v1 body surface: DML + SELECT. The body runs inside a transaction (a
    // CALL is atomic), which rules out DDL and BEGIN/COMMIT/ROLLBACK (they'd
    // break the wrapping); nested CALL is rejected to keep recursion out of
    // v1. Savepoints ARE allowed — they roll back part of the CALL's work
    // without touching the outer transaction boundary.
    for stmt in &statements {
        match stmt {
            sp::Statement::Insert(_)
            | sp::Statement::Update { .. }
            | sp::Statement::Delete(_)
            | sp::Statement::Query(_)
            | sp::Statement::Call(_)
            | sp::Statement::Savepoint { .. }
            | sp::Statement::ReleaseSavepoint { .. } => {}
            // `ROLLBACK TO SAVEPOINT` is a Rollback statement carrying a
            // savepoint name; a plain ROLLBACK (no savepoint) is rejected.
            sp::Statement::Rollback {
                savepoint: Some(_), ..
            } => {}
            other => {
                return Err(SqlError::Unsupported(format!(
                    "statement in procedure body: {other} (bodies are DML/SELECT, CALL, savepoints)"
                )));
            }
        }
    }
    rewrite_procedure_params(&mut statements, &declared)?;
    let body: String = statements
        .iter()
        .map(|st| st.to_string())
        .collect::<Vec<_>>()
        .join("; ");
    Ok(Statement::CreateProcedure {
        name,
        def: crate::catalog::ProcedureDef {
            params: declared,
            body,
            language: crate::catalog::ProcLanguage::Sql,
            bytecode: Vec::new(),
            source: String::new(),
        },
        or_alter,
    })
}

/// Rewrite unqualified identifiers that name a declared parameter into
/// `$N` placeholders — expression positions only, so INSERT column lists,
/// table names, and qualified `t.col` references are untouched. Pre-existing
/// placeholders in the body are rejected (they would collide with the
/// parameter slots).
fn rewrite_procedure_params(
    statements: &mut [sp::Statement],
    declared: &[(String, SqlType)],
) -> Result<()> {
    use core::ops::ControlFlow;
    let mut err: Option<SqlError> = None;
    for stmt in statements.iter_mut() {
        let _ = sp::visit_expressions_mut(stmt, |e: &mut sp::Expr| {
            match e {
                sp::Expr::Value(v) => {
                    if matches!(v.value, sp::Value::Placeholder(_)) {
                        err.get_or_insert_with(|| {
                            SqlError::Unsupported(
                                "placeholders in a procedure body (reference parameters by name)"
                                    .into(),
                            )
                        });
                    }
                }
                sp::Expr::Identifier(id) if id.quote_style.is_none() => {
                    if let Some(slot) = declared
                        .iter()
                        .position(|(n, _)| n.eq_ignore_ascii_case(&id.value))
                    {
                        *e = sp::Expr::Value(
                            sp::Value::Placeholder(format!("${}", slot + 1)).with_empty_span(),
                        );
                    }
                }
                _ => {}
            }
            ControlFlow::<()>::Continue(())
        });
    }
    match err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}

/// Translate `CALL name(args...)`.
fn translate_call(func: sp::Function, p: &mut usize) -> Result<Statement> {
    let name = object_name_to_string(&func.name)?;
    let args = match func.args {
        sp::FunctionArguments::List(list) => list
            .args
            .into_iter()
            .map(|a| match a {
                sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) => translate_expr(e, p),
                other => Err(SqlError::Unsupported(format!("CALL argument {other}"))),
            })
            .collect::<Result<Vec<_>>>()?,
        sp::FunctionArguments::None => Vec::new(),
        other => {
            return Err(SqlError::Unsupported(format!("CALL arguments {other}")));
        }
    };
    Ok(Statement::Call { name, args })
}

fn translate_create_table(ct: sp::CreateTable) -> Result<Statement> {
    let mut pk_col: Option<String> = None;
    let mut unique_cols: Vec<String> = Vec::new();
    for c in &ct.constraints {
        match c {
            // Accepted and ignored (documented): referential integrity is
            // not enforced in v1, but EF/PG-style DDL must parse.
            sp::TableConstraint::ForeignKey { .. } => {}
            // Table-level `[CONSTRAINT name] PRIMARY KEY (col)` — the form
            // EF Core migrations and pg_dump emit. Single column only.
            sp::TableConstraint::PrimaryKey { columns, .. } => {
                pk_col = Some(single_index_column(columns, "PRIMARY KEY")?);
            }
            sp::TableConstraint::Unique { columns, .. } => {
                unique_cols.push(single_index_column(columns, "UNIQUE")?);
            }
            other => {
                return Err(SqlError::Unsupported(format!("table constraint {other:?}")));
            }
        }
    }
    let name = object_name_to_string(&ct.name)?;
    let mut columns = Vec::with_capacity(ct.columns.len());
    for col in &ct.columns {
        columns.push(translate_column(col)?);
    }
    for (target, is_pk) in pk_col
        .iter()
        .map(|c| (c, true))
        .chain(unique_cols.iter().map(|c| (c, false)))
    {
        let col = columns
            .iter_mut()
            .find(|c| &c.name == target)
            .ok_or_else(|| {
                SqlError::Unsupported(format!("table constraint on unknown column {target}"))
            })?;
        if is_pk {
            col.primary_key = true;
        } else {
            col.unique = true;
        }
    }
    if columns.iter().filter(|c| c.primary_key).count() > 1 {
        return Err(SqlError::Unsupported(
            "multiple PRIMARY KEY columns (a table has at most one primary key)".into(),
        ));
    }
    for c in &columns {
        if c.auto_increment && !(c.primary_key && c.ty == SqlType::Int) {
            return Err(SqlError::Unsupported(format!(
                "AUTO_INCREMENT on {:?} — only an INT PRIMARY KEY column can auto-increment",
                c.name
            )));
        }
    }
    Ok(Statement::CreateTable {
        table: Table::new(name, columns),
        if_not_exists: ct.if_not_exists,
    })
}

fn translate_create_index(ci: sp::CreateIndex) -> Result<Statement> {
    let table = object_name_to_string(&ci.table_name)?;
    let name = match &ci.name {
        Some(n) => object_name_to_string(n)?,
        None => return Err(SqlError::Unsupported("CREATE INDEX without a name".into())),
    };
    if ci.columns.is_empty() {
        return Err(SqlError::Unsupported("CREATE INDEX without columns".into()));
    }
    let columns = ci
        .columns
        .iter()
        .map(|c| match &c.column.expr {
            sp::Expr::Identifier(id) => Ok(id.value.clone()),
            other => Err(SqlError::Unsupported(format!(
                "index on expression {other:?} (columns only)"
            ))),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Statement::CreateIndex {
        name,
        table,
        columns,
        if_not_exists: ci.if_not_exists,
    })
}

/// A table-level constraint's single column name (multi-column is v2).
fn single_index_column(cols: &[sp::IndexColumn], what: &str) -> Result<String> {
    let [ic] = cols else {
        return Err(SqlError::Unsupported(format!(
            "multi-column table-level {what} constraint"
        )));
    };
    match &ic.column.expr {
        sp::Expr::Identifier(id) => Ok(id.value.clone()),
        other => Err(SqlError::Unsupported(format!(
            "table-level {what} constraint on expression {other:?}"
        ))),
    }
}

fn translate_column(col: &sp::ColumnDef) -> Result<Column> {
    let ty = map_data_type(&col.data_type)?;
    let mut column = Column::new(col.name.value.clone(), ty);
    for opt in &col.options {
        match &opt.option {
            sp::ColumnOption::NotNull => column = column.not_null(),
            sp::ColumnOption::Null => {}
            sp::ColumnOption::Unique { is_primary, .. } if *is_primary => {
                column = column.primary_key();
            }
            sp::ColumnOption::Unique { .. } => {
                column.unique = true;
            }
            sp::ColumnOption::Default(expr) => {
                // Literal defaults only (constants a migration would emit).
                let mut p0 = 0usize;
                match translate_expr(expr.clone(), &mut p0)? {
                    Expr::Literal(v) => column.default_value = Some(v),
                    Expr::Unary {
                        op: UnOp::Neg,
                        expr,
                    } if matches!(*expr, Expr::Literal(_)) => {
                        if let Expr::Literal(v) = *expr {
                            column.default_value = Some(match v {
                                Value::Int(n) => Value::Int(-n),
                                Value::Double(f) => Value::Double(-f),
                                other => other,
                            });
                        }
                    }
                    _ => {
                        return Err(SqlError::Unsupported(format!(
                            "non-literal DEFAULT on {:?}",
                            col.name.value
                        )));
                    }
                }
            }
            // Referential integrity is not enforced in v1; accept the syntax
            // so EF/PG-style DDL round-trips (documented limitation).
            sp::ColumnOption::ForeignKey { .. } => {}
            // MySQL `AUTO_INCREMENT` / SQLite `AUTOINCREMENT`.
            sp::ColumnOption::DialectSpecific(tokens)
                if tokens.iter().any(|t| {
                    matches!(t, sqlparser::tokenizer::Token::Word(w)
                        if w.value.eq_ignore_ascii_case("AUTO_INCREMENT")
                            || w.value.eq_ignore_ascii_case("AUTOINCREMENT"))
                }) =>
            {
                column = column.auto_increment();
            }
            // PostgreSQL `GENERATED [ALWAYS | BY DEFAULT] AS IDENTITY`.
            sp::ColumnOption::Generated {
                generation_expr: None,
                ..
            } => {
                column = column.auto_increment();
            }
            other => {
                return Err(SqlError::Unsupported(format!(
                    "column option {other:?} on {:?}",
                    col.name.value
                )));
            }
        }
    }
    Ok(column)
}

fn map_data_type(dt: &sp::DataType) -> Result<SqlType> {
    use sp::DataType as D;
    let ty = match dt {
        D::Int(_) | D::Integer(_) | D::BigInt(_) | D::SmallInt(_) | D::TinyInt(_) => SqlType::Int,
        D::Double(_) | D::DoublePrecision | D::Float(_) | D::Real => SqlType::Double,
        D::Text
        | D::Varchar(_)
        | D::Char(_)
        | D::CharVarying(_)
        | D::String(_)
        | D::Nvarchar(_) => SqlType::Text,
        D::Bool | D::Boolean => SqlType::Bool,
        D::Timestamp(_, _) | D::Datetime(_) => SqlType::Timestamp,
        // DECIMAL/NUMERIC are exact fixed-point (backed by `Decimal`).
        // A declared precision/scale is accepted but not enforced.
        D::Decimal(_) | D::Numeric(_) | D::Dec(_) | D::BigNumeric(_) | D::BigDecimal(_) => {
            SqlType::Decimal
        }
        D::Blob(_) | D::Bytea | D::Binary(_) | D::Varbinary(_) => SqlType::Blob,
        other => return Err(SqlError::Unsupported(format!("data type {other:?}"))),
    };
    Ok(ty)
}

/// Translate a `RETURNING <items>` clause.
fn translate_returning(
    items: Option<&Vec<sp::SelectItem>>,
    p: &mut usize,
) -> Result<Option<Vec<SelectItem>>> {
    items
        .map(|items| {
            items
                .iter()
                .map(|item| translate_select_item(item, p))
                .collect::<Result<Vec<_>>>()
        })
        .transpose()
}

fn translate_insert(insert: sp::Insert, p: &mut usize) -> Result<Statement> {
    let table = match &insert.table {
        sp::TableObject::TableName(name) => object_name_to_string(name)?,
        other => return Err(SqlError::Unsupported(format!("INSERT target {other:?}"))),
    };
    let columns = if insert.columns.is_empty() {
        None
    } else {
        Some(insert.columns.iter().map(|i| i.value.clone()).collect())
    };
    let returning = translate_returning(insert.returning.as_ref(), p)?;
    let source = insert
        .source
        .ok_or_else(|| SqlError::Unsupported("INSERT without VALUES".into()))?;
    let rows = match *source.body {
        sp::SetExpr::Values(values) => values
            .rows
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|e| translate_expr(e, p))
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?,
        _ => return Err(SqlError::Unsupported("INSERT ... SELECT".into())),
    };
    Ok(Statement::Insert {
        table,
        columns,
        rows,
        returning,
    })
}

fn translate_delete(del: sp::Delete, p: &mut usize) -> Result<Statement> {
    let twjs = match &del.from {
        sp::FromTable::WithFromKeyword(t) | sp::FromTable::WithoutKeyword(t) => t,
    };
    if twjs.len() != 1 {
        return Err(SqlError::Unsupported("DELETE from multiple tables".into()));
    }
    let table = table_name_from_twj(&twjs[0])?;
    let filter = del.selection.map(|e| translate_expr(e, p)).transpose()?;
    let returning = translate_returning(del.returning.as_ref(), p)?;
    Ok(Statement::Delete {
        table,
        filter,
        returning,
    })
}

/// Translate a full query: a plain SELECT, or a UNION [ALL] tree with outer
/// ORDER BY / LIMIT / OFFSET. For a plain SELECT the outer clauses are pushed
/// into the [`SelectStmt`] itself (single-select fast path).
fn translate_query(query: sp::Query, p: &mut usize) -> Result<SelectQuery> {
    // WITH (CTE): non-recursive common table expressions are sugar for derived
    // tables. Translate each CTE body first (so positional `?` params bind in
    // textual order), inlining earlier CTEs into it, then inline the whole set
    // into the main query. A CTE referenced N times is inlined N times.
    let resolved: Vec<(String, SelectQuery)> = if let Some(with) = query.with {
        if with.recursive {
            return Err(SqlError::Unsupported("WITH RECURSIVE".into()));
        }
        let mut resolved: Vec<(String, SelectQuery)> = Vec::new();
        for cte in with.cte_tables {
            if !cte.alias.columns.is_empty() {
                return Err(SqlError::Unsupported(
                    "WITH column aliases (`WITH c(a, b) AS ...`)".into(),
                ));
            }
            let name = cte.alias.name.value.clone();
            let mut q = translate_query(*cte.query, p)?;
            inline_ctes_query(&mut q, &resolved);
            resolved.push((name, q));
        }
        resolved
    } else {
        Vec::new()
    };

    let order_by = translate_order_by(query.order_by, p)?;
    let (limit, offset) = translate_limit(&query.limit_clause, p)?;
    let mut result = match *query.body {
        sp::SetExpr::Select(s) => {
            let mut stmt = translate_select_core(*s, p)?;
            stmt.order_by = order_by;
            stmt.limit = limit;
            stmt.offset = offset;
            SelectQuery {
                body: QueryBody::Select(Box::new(stmt)),
                order_by: Vec::new(),
                limit: None,
                offset: None,
            }
        }
        body @ sp::SetExpr::SetOperation { .. } => SelectQuery {
            body: translate_set_expr(body, p)?,
            order_by,
            limit,
            offset,
        },
        other => return Err(SqlError::Unsupported(format!("query body {other}"))),
    };
    if !resolved.is_empty() {
        inline_ctes_query(&mut result, &resolved);
    }
    Ok(result)
}

/// Inline CTE definitions into a query: any bare table reference whose name
/// matches a CTE becomes a derived table wrapping that CTE's (already fully
/// resolved) query. Recurses through joins, derived tables, and subqueries so
/// a CTE is visible everywhere in the query it scopes.
fn inline_ctes_query(q: &mut SelectQuery, ctes: &[(String, SelectQuery)]) {
    inline_ctes_body(&mut q.body, ctes);
    for (e, _) in &mut q.order_by {
        inline_ctes_expr(e, ctes);
    }
}

fn inline_ctes_body(b: &mut QueryBody, ctes: &[(String, SelectQuery)]) {
    match b {
        QueryBody::Select(s) => inline_ctes_select(s, ctes),
        QueryBody::SetOp { left, right, .. } => {
            inline_ctes_body(left, ctes);
            inline_ctes_body(right, ctes);
        }
    }
}

fn inline_ctes_select(s: &mut SelectStmt, ctes: &[(String, SelectQuery)]) {
    inline_ctes_tableref(&mut s.from, ctes);
    for j in &mut s.joins {
        inline_ctes_tableref(&mut j.table, ctes);
        inline_ctes_expr(&mut j.on, ctes);
    }
    for item in &mut s.projection {
        if let SelectItem::Expr { expr, .. } = item {
            inline_ctes_expr(expr, ctes);
        }
    }
    if let Some(f) = &mut s.filter {
        inline_ctes_expr(f, ctes);
    }
    for e in &mut s.group_by {
        inline_ctes_expr(e, ctes);
    }
    if let Some(h) = &mut s.having {
        inline_ctes_expr(h, ctes);
    }
    for (e, _) in &mut s.order_by {
        inline_ctes_expr(e, ctes);
    }
    for e in &mut s.distinct_on {
        inline_ctes_expr(e, ctes);
    }
}

fn inline_ctes_tableref(t: &mut TableRef, ctes: &[(String, SelectQuery)]) {
    // An existing derived table shadows any same-named CTE within it — recurse
    // rather than replace.
    if let Some(sub) = &mut t.subquery {
        inline_ctes_query(sub, ctes);
        return;
    }
    if let Some((_, q)) = ctes.iter().find(|(n, _)| n == &t.name) {
        // The derived-table convention is name == alias == the visible key.
        let key = t.alias.clone().unwrap_or_else(|| t.name.clone());
        t.subquery = Some(Box::new(q.clone()));
        t.name = key.clone();
        t.alias = Some(key);
    }
}

fn inline_ctes_expr(e: &mut Expr, ctes: &[(String, SelectQuery)]) {
    match e {
        Expr::Subquery(q) => inline_ctes_query(q, ctes),
        Expr::InSubquery { expr, query, .. } => {
            inline_ctes_expr(expr, ctes);
            inline_ctes_query(query, ctes);
        }
        Expr::CorrScalar { query, outer, .. } => {
            inline_ctes_query(query, ctes);
            for o in outer {
                inline_ctes_expr(o, ctes);
            }
        }
        Expr::CorrIn {
            expr, query, outer, ..
        } => {
            inline_ctes_expr(expr, ctes);
            inline_ctes_query(query, ctes);
            for o in outer {
                inline_ctes_expr(o, ctes);
            }
        }
        Expr::Binary { left, right, .. } => {
            inline_ctes_expr(left, ctes);
            inline_ctes_expr(right, ctes);
        }
        Expr::Unary { expr, .. } | Expr::IsNull { expr, .. } => inline_ctes_expr(expr, ctes),
        Expr::In { expr, list, .. } => {
            inline_ctes_expr(expr, ctes);
            for x in list {
                inline_ctes_expr(x, ctes);
            }
        }
        Expr::Func { args, .. } => {
            for a in args {
                inline_ctes_expr(a, ctes);
            }
        }
        Expr::Aggregate { arg, .. } => {
            if let Some(a) = arg {
                inline_ctes_expr(a, ctes);
            }
        }
        Expr::Window {
            partition_by,
            order_by,
            ..
        } => {
            for e in partition_by {
                inline_ctes_expr(e, ctes);
            }
            for (e, _) in order_by {
                inline_ctes_expr(e, ctes);
            }
        }
        Expr::Column { .. } | Expr::Col(_) | Expr::Literal(_) | Expr::Param(_) => {}
    }
}

/// Translate one side of a set operation (or the whole tree).
fn translate_set_expr(e: sp::SetExpr, p: &mut usize) -> Result<QueryBody> {
    match e {
        sp::SetExpr::Select(s) => Ok(QueryBody::Select(Box::new(translate_select_core(*s, p)?))),
        sp::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            if op != sp::SetOperator::Union {
                return Err(SqlError::Unsupported(format!("set operation {op}")));
            }
            let all = match set_quantifier {
                sp::SetQuantifier::All => true,
                sp::SetQuantifier::None | sp::SetQuantifier::Distinct => false,
                other => {
                    return Err(SqlError::Unsupported(format!("UNION quantifier {other}")));
                }
            };
            Ok(QueryBody::SetOp {
                all,
                left: Box::new(translate_set_expr(*left, p)?),
                right: Box::new(translate_set_expr(*right, p)?),
            })
        }
        // A parenthesized branch: allowed as long as it has no ORDER BY/LIMIT
        // of its own (per-branch ordering is meaningless under UNION).
        sp::SetExpr::Query(inner) => {
            if inner.order_by.is_some() || inner.limit_clause.is_some() {
                return Err(SqlError::Unsupported(
                    "ORDER BY / LIMIT inside a UNION branch".into(),
                ));
            }
            translate_set_expr(*inner.body, p)
        }
        other => Err(SqlError::Unsupported(format!("query body {other}"))),
    }
}

fn translate_select_core(select: sp::Select, p: &mut usize) -> Result<SelectStmt> {
    if select.from.len() != 1 {
        return Err(SqlError::Unsupported(
            "SELECT must reference exactly one table in FROM (use JOIN)".into(),
        ));
    }
    let (distinct, distinct_on) = match select.distinct.clone() {
        None => (false, Vec::new()),
        Some(sp::Distinct::Distinct) => (true, Vec::new()),
        Some(sp::Distinct::On(exprs)) => {
            let on = exprs
                .into_iter()
                .map(|e| translate_expr(e, p))
                .collect::<Result<Vec<_>>>()?;
            if on.is_empty() {
                return Err(SqlError::Unsupported(
                    "DISTINCT ON () with no expressions".into(),
                ));
            }
            (false, on)
        }
    };

    let from = table_ref_from_factor(&select.from[0].relation, p)?;

    // JOINs: INNER / LEFT / RIGHT / FULL, all with an ON predicate.
    let mut joins = Vec::new();
    for j in &select.from[0].joins {
        let table = table_ref_from_factor(&j.relation, p)?;
        let (kind, constraint) = match &j.join_operator {
            sp::JoinOperator::Inner(c) | sp::JoinOperator::Join(c) => (JoinKind::Inner, c),
            sp::JoinOperator::Left(c) | sp::JoinOperator::LeftOuter(c) => (JoinKind::Left, c),
            sp::JoinOperator::Right(c) | sp::JoinOperator::RightOuter(c) => (JoinKind::Right, c),
            sp::JoinOperator::FullOuter(c) => (JoinKind::Full, c),
            other => {
                return Err(SqlError::Unsupported(format!(
                    "join type {other:?} (INNER/LEFT/RIGHT/FULL ... ON only)"
                )));
            }
        };
        let on = match constraint {
            sp::JoinConstraint::On(expr) => translate_expr(expr.clone(), p)?,
            other => {
                return Err(SqlError::Unsupported(format!(
                    "join constraint {other:?} (only ON <expr> is supported)"
                )));
            }
        };
        joins.push(Join { table, kind, on });
    }

    let projection = select
        .projection
        .iter()
        .map(|item| translate_select_item(item, p))
        .collect::<Result<Vec<_>>>()?;

    let filter = select.selection.map(|e| translate_expr(e, p)).transpose()?;

    let group_by = match &select.group_by {
        sp::GroupByExpr::Expressions(exprs, _) => exprs
            .iter()
            .map(|e| translate_expr(e.clone(), p))
            .collect::<Result<Vec<_>>>()?,
        sp::GroupByExpr::All(_) => return Err(SqlError::Unsupported("GROUP BY ALL".into())),
    };

    let having = select.having.map(|e| translate_expr(e, p)).transpose()?;

    Ok(SelectStmt {
        distinct,
        distinct_on,
        from,
        joins,
        projection,
        filter,
        group_by,
        having,
        order_by: Vec::new(),
        limit: None,
        offset: None,
    })
}

fn translate_select_item(item: &sp::SelectItem, p: &mut usize) -> Result<SelectItem> {
    match item {
        sp::SelectItem::Wildcard(_) => Ok(SelectItem::Wildcard),
        sp::SelectItem::QualifiedWildcard(kind, _) => match kind {
            sp::SelectItemQualifiedWildcardKind::ObjectName(name) => {
                Ok(SelectItem::QualifiedWildcard(object_name_to_string(name)?))
            }
            sp::SelectItemQualifiedWildcardKind::Expr(_) => {
                Err(SqlError::Unsupported("expr.* wildcard".into()))
            }
        },
        sp::SelectItem::UnnamedExpr(expr) => Ok(SelectItem::Expr {
            expr: translate_expr(expr.clone(), p)?,
            alias: None,
        }),
        sp::SelectItem::ExprWithAlias { expr, alias } => Ok(SelectItem::Expr {
            expr: translate_expr(expr.clone(), p)?,
            alias: Some(alias.value.clone()),
        }),
    }
}

fn translate_order_by(order_by: Option<sp::OrderBy>, p: &mut usize) -> Result<Vec<(Expr, bool)>> {
    let Some(ob) = order_by else {
        return Ok(Vec::new());
    };
    let exprs = match ob.kind {
        sp::OrderByKind::Expressions(e) => e,
        sp::OrderByKind::All(_) => return Err(SqlError::Unsupported("ORDER BY ALL".into())),
    };
    let mut keys = Vec::with_capacity(exprs.len());
    for e in exprs {
        let asc = e.options.asc.unwrap_or(true);
        keys.push((translate_expr(e.expr, p)?, asc));
    }
    Ok(keys)
}

/// Translate LIMIT / OFFSET into `(limit, offset)`.
fn translate_limit(
    limit: &Option<sp::LimitClause>,
    p: &mut usize,
) -> Result<(Option<LimitExpr>, Option<LimitExpr>)> {
    let Some(clause) = limit else {
        return Ok((None, None));
    };
    match clause {
        sp::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if !limit_by.is_empty() {
                return Err(SqlError::Unsupported("LIMIT BY".into()));
            }
            let limit = limit.as_ref().map(|e| limit_operand(e, p)).transpose()?;
            let offset = offset
                .as_ref()
                .map(|o| limit_operand(&o.value, p))
                .transpose()?;
            Ok((limit, offset))
        }
        sp::LimitClause::OffsetCommaLimit { .. } => {
            Err(SqlError::Unsupported("MySQL LIMIT offset,count".into()))
        }
    }
}

/// A LIMIT/OFFSET operand: a literal count or a bind placeholder.
fn limit_operand(e: &sp::Expr, p: &mut usize) -> Result<LimitExpr> {
    if let sp::Expr::Value(v) = e
        && matches!(&v.value, sp::Value::Placeholder(_))
    {
        return match translate_value(&v.value, p)? {
            Expr::Param(i) => Ok(LimitExpr::Param(i)),
            _ => unreachable!("placeholder translated to a non-param"),
        };
    }
    Ok(LimitExpr::Count(expr_to_u64(e)? as usize))
}

fn translate_assignment(a: sp::Assignment, p: &mut usize) -> Result<(String, Expr)> {
    let col = match a.target {
        sp::AssignmentTarget::ColumnName(name) => object_name_to_string(&name)?,
        sp::AssignmentTarget::Tuple(_) => {
            return Err(SqlError::Unsupported("tuple assignment".into()));
        }
    };
    Ok((col, translate_expr(a.value, p)?))
}

fn translate_expr(expr: sp::Expr, p: &mut usize) -> Result<Expr> {
    match expr {
        sp::Expr::Identifier(id) => Ok(Expr::Column {
            table: None,
            name: id.value,
        }),
        sp::Expr::CompoundIdentifier(parts) => {
            // `t.col` -> qualifier + final segment. Deeper paths (a.b.c) are
            // reduced to (b, c) which is enough for single-schema qualification.
            if parts.len() < 2 {
                let name = parts
                    .into_iter()
                    .next()
                    .ok_or_else(|| SqlError::Parse("empty compound identifier".into()))?
                    .value;
                return Ok(Expr::Column { table: None, name });
            }
            let name = parts.last().unwrap().value.clone();
            let table = parts[parts.len() - 2].value.clone();
            Ok(Expr::Column {
                table: Some(table),
                name,
            })
        }
        sp::Expr::Value(v) => translate_value(&v.value, p),
        sp::Expr::Nested(inner) => translate_expr(*inner, p),
        sp::Expr::IsNull(inner) => Ok(Expr::IsNull {
            expr: Box::new(translate_expr(*inner, p)?),
            negated: false,
        }),
        sp::Expr::IsNotNull(inner) => Ok(Expr::IsNull {
            expr: Box::new(translate_expr(*inner, p)?),
            negated: true,
        }),
        sp::Expr::UnaryOp { op, expr } => {
            let un = match op {
                sp::UnaryOperator::Not => UnOp::Not,
                sp::UnaryOperator::Minus => UnOp::Neg,
                sp::UnaryOperator::Plus => return translate_expr(*expr, p),
                other => return Err(SqlError::Unsupported(format!("unary operator {other:?}"))),
            };
            Ok(Expr::Unary {
                op: un,
                expr: Box::new(translate_expr(*expr, p)?),
            })
        }
        sp::Expr::BinaryOp { left, op, right } => Ok(Expr::Binary {
            op: map_binary_op(&op)?,
            left: Box::new(translate_expr(*left, p)?),
            right: Box::new(translate_expr(*right, p)?),
        }),
        sp::Expr::Function(f) => translate_function(f, p),
        sp::Expr::InList {
            expr,
            list,
            negated,
        } => Ok(Expr::In {
            expr: Box::new(translate_expr(*expr, p)?),
            list: list
                .into_iter()
                .map(|e| translate_expr(e, p))
                .collect::<Result<_>>()?,
            negated,
        }),
        sp::Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => Ok(Expr::InSubquery {
            expr: Box::new(translate_expr(*expr, p)?),
            query: Box::new(translate_subquery(*subquery, p)?),
            negated,
        }),
        sp::Expr::Subquery(q) => Ok(Expr::Subquery(Box::new(translate_subquery(*q, p)?))),
        // `TIMESTAMP '2026-01-02 03:04:05'` and friends.
        sp::Expr::TypedString(ts) => {
            let ty = map_data_type(&ts.data_type)?;
            let s = match &ts.value.value {
                sp::Value::SingleQuotedString(s) | sp::Value::DoubleQuotedString(s) => s.clone(),
                other => {
                    return Err(SqlError::Unsupported(format!("typed literal {other:?}")));
                }
            };
            match ty {
                SqlType::Timestamp => Ok(Expr::Literal(Value::Timestamp(parse_timestamp(&s)?))),
                other => Err(SqlError::Unsupported(format!(
                    "typed string literal for {other:?}"
                ))),
            }
        }
        sp::Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            // Simple form (`CASE x WHEN v ...`) desugars to the searched form
            // (`CASE WHEN x = v ...`).
            let operand = operand.map(|o| translate_expr(*o, p)).transpose()?;
            let mut args = Vec::with_capacity(conditions.len() * 2 + 1);
            for cw in conditions {
                let cond = translate_expr(cw.condition, p)?;
                let cond = match &operand {
                    Some(op) => Expr::Binary {
                        op: BinOp::Eq,
                        left: Box::new(op.clone()),
                        right: Box::new(cond),
                    },
                    None => cond,
                };
                args.push(cond);
                args.push(translate_expr(cw.result, p)?);
            }
            let has_else = else_result.is_some();
            if let Some(e) = else_result {
                args.push(translate_expr(*e, p)?);
            }
            if args.is_empty() {
                return Err(SqlError::Unsupported("CASE with no WHEN branches".into()));
            }
            Ok(Expr::Func {
                func: ScalarFunc::Case { has_else },
                args,
            })
        }
        sp::Expr::Like {
            negated,
            expr,
            pattern,
            escape_char,
            any,
        } => {
            if any {
                return Err(SqlError::Unsupported("LIKE ANY".into()));
            }
            let escape = match escape_char {
                None => None,
                Some(sp::Value::SingleQuotedString(s)) if s.chars().count() == 1 => {
                    s.chars().next()
                }
                Some(other) => {
                    return Err(SqlError::Unsupported(format!(
                        "LIKE ESCAPE {other:?} (single character only)"
                    )));
                }
            };
            Ok(Expr::Func {
                func: ScalarFunc::Like { negated, escape },
                args: vec![translate_expr(*expr, p)?, translate_expr(*pattern, p)?],
            })
        }
        sp::Expr::Cast {
            expr, data_type, ..
        } => Ok(Expr::Func {
            func: ScalarFunc::Cast(map_data_type(&data_type)?),
            args: vec![translate_expr(*expr, p)?],
        }),
        sp::Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => {
            let mut args = vec![translate_expr(*expr, p)?];
            let from = substring_from
                .ok_or_else(|| SqlError::Unsupported("SUBSTRING without a start".into()))?;
            args.push(translate_expr(*from, p)?);
            if let Some(f) = substring_for {
                args.push(translate_expr(*f, p)?);
            }
            Ok(Expr::Func {
                func: ScalarFunc::Substring,
                args,
            })
        }
        sp::Expr::Trim {
            expr,
            trim_where,
            trim_what,
            trim_characters,
        } => {
            if trim_where.is_some() || trim_what.is_some() || trim_characters.is_some() {
                return Err(SqlError::Unsupported(
                    "TRIM with LEADING/TRAILING/characters".into(),
                ));
            }
            Ok(Expr::Func {
                func: ScalarFunc::Trim,
                args: vec![translate_expr(*expr, p)?],
            })
        }
        sp::Expr::Exists { subquery, negated } => {
            // `EXISTS (SELECT ...)` == `1 IN (SELECT 1 ...)` — reuses the
            // (correlated) IN-subquery machinery wholesale.
            let mut query = translate_subquery(*subquery, p)?;
            exists_projection(&mut query.body)?;
            Ok(Expr::InSubquery {
                expr: Box::new(Expr::Literal(Value::Int(1))),
                query: Box::new(query),
                negated,
            })
        }
        other => Err(SqlError::Unsupported(format!("expression {other:?}"))),
    }
}

/// Translate a subquery, which may carry its own ORDER BY / LIMIT / OFFSET.
fn translate_subquery(q: sp::Query, p: &mut usize) -> Result<SelectQuery> {
    translate_query(q, p)
}

/// Parse a SQL timestamp string to epoch **milliseconds** (UTC).
///
/// Accepted: `YYYY-MM-DD`, `YYYY-MM-DD[ T]HH:MM:SS[.fff]`, optionally with a
/// trailing `Z` or `±HH[:MM]` offset (applied to convert to UTC).
pub(crate) fn parse_timestamp(s: &str) -> Result<i64> {
    let bad = || SqlError::Parse(format!("bad timestamp literal {s:?}"));
    let s = s.trim();

    // Split off a trailing zone: 'Z' or ±HH[:MM] (only after a time part).
    let (body, offset_min) = if let Some(b) = s.strip_suffix('Z').or_else(|| s.strip_suffix('z')) {
        (b, 0i64)
    } else if s.len() > 10
        && let Some(pos) = s.rfind(['+', '-']).filter(|&p| p >= 10)
    {
        let (b, z) = s.split_at(pos);
        let sign = if z.starts_with('-') { -1i64 } else { 1 };
        let z = &z[1..];
        let (zh, zm) = match z.split_once(':') {
            Some((h, m)) => (h, m),
            None if z.len() == 4 => z.split_at(2),
            None => (z, "0"),
        };
        let zh: i64 = zh.parse().map_err(|_| bad())?;
        let zm: i64 = zm.parse().map_err(|_| bad())?;
        (b, sign * (zh * 60 + zm))
    } else {
        (s, 0)
    };

    let (date, time) = match body.split_once([' ', 'T']) {
        Some((d, t)) => (d, Some(t)),
        None => (body, None),
    };

    let mut dp = date.split('-');
    let year: i64 = dp.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let month: i64 = dp.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    let day: i64 = dp.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
    if dp.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(bad());
    }

    let (mut hour, mut min, mut sec, mut millis) = (0i64, 0i64, 0i64, 0i64);
    if let Some(t) = time {
        let (hms, frac) = match t.split_once('.') {
            Some((a, f)) => (a, Some(f)),
            None => (t, None),
        };
        let mut tp = hms.split(':');
        hour = tp.next().ok_or_else(bad)?.parse().map_err(|_| bad())?;
        min = tp.next().unwrap_or("0").parse().map_err(|_| bad())?;
        sec = tp.next().unwrap_or("0").parse().map_err(|_| bad())?;
        if tp.next().is_some()
            || !(0..=23).contains(&hour)
            || !(0..=59).contains(&min)
            || !(0..=59).contains(&sec)
        {
            return Err(bad());
        }
        if let Some(f) = frac {
            let f: String = f.chars().take(3).collect();
            let scale = 10i64.pow(3 - f.len() as u32);
            millis = f.parse::<i64>().map_err(|_| bad())? * scale;
        }
    }

    // Days since epoch (Howard Hinnant's days-from-civil).
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;

    let secs = days * 86_400 + hour * 3_600 + min * 60 + sec - offset_min * 60;
    Ok(secs * 1_000 + millis)
}

/// Rewrite an EXISTS subquery body's projection to the literal `1` (its
/// output values are irrelevant; only row existence matters). Aggregated
/// bodies keep their aggregate-driven row count semantics and are rejected.
fn exists_projection(body: &mut QueryBody) -> Result<()> {
    match body {
        QueryBody::Select(sel) => {
            if !sel.group_by.is_empty() || sel.having.is_some() {
                return Err(SqlError::Unsupported(
                    "EXISTS over an aggregated subquery".into(),
                ));
            }
            sel.projection = vec![SelectItem::Expr {
                expr: Expr::Literal(Value::Int(1)),
                alias: None,
            }];
            Ok(())
        }
        QueryBody::SetOp { left, right, .. } => {
            exists_projection(left)?;
            exists_projection(right)
        }
    }
}

fn translate_function(f: sp::Function, p: &mut usize) -> Result<Expr> {
    let fname = object_name_to_string(&f.name)?.to_ascii_lowercase();
    let args = match f.args {
        sp::FunctionArguments::List(list) => {
            if list.duplicate_treatment == Some(sp::DuplicateTreatment::Distinct) {
                return Err(SqlError::Unsupported("aggregate DISTINCT".into()));
            }
            list.args
        }
        sp::FunctionArguments::None => Vec::new(),
        sp::FunctionArguments::Subquery(_) => {
            return Err(SqlError::Unsupported("aggregate over subquery".into()));
        }
    };
    // Row-scalar functions (before the single-argument aggregate path).
    if let Some(func) = match fname.as_str() {
        "coalesce" | "ifnull" => Some(ScalarFunc::Coalesce),
        "least" => Some(ScalarFunc::Least),
        "greatest" => Some(ScalarFunc::Greatest),
        "nullif" => Some(ScalarFunc::NullIf),
        "upper" | "ucase" => Some(ScalarFunc::Upper),
        "lower" | "lcase" => Some(ScalarFunc::Lower),
        "length" | "char_length" | "character_length" | "len" => Some(ScalarFunc::Length),
        "substring" | "substr" => Some(ScalarFunc::Substring),
        "concat" => Some(ScalarFunc::Concat),
        "trim" => Some(ScalarFunc::Trim),
        "ltrim" => Some(ScalarFunc::Ltrim),
        "rtrim" => Some(ScalarFunc::Rtrim),
        "replace" => Some(ScalarFunc::Replace),
        "abs" => Some(ScalarFunc::Abs),
        "round" => Some(ScalarFunc::Round),
        _ => None,
    } {
        if f.over.is_some() {
            return Err(SqlError::Unsupported(format!("{fname}() OVER (...)")));
        }
        let exprs: Vec<Expr> = args
            .into_iter()
            .map(|a| match a {
                sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e)) => translate_expr(e, p),
                other => Err(SqlError::Unsupported(format!(
                    "{fname}() argument {other:?}"
                ))),
            })
            .collect::<Result<_>>()?;
        let ok_arity = match func {
            ScalarFunc::Coalesce if fname == "ifnull" => exprs.len() == 2,
            ScalarFunc::Coalesce
            | ScalarFunc::Concat
            | ScalarFunc::Least
            | ScalarFunc::Greatest => !exprs.is_empty(),
            ScalarFunc::NullIf | ScalarFunc::Replace => {
                exprs.len() == if func == ScalarFunc::Replace { 3 } else { 2 }
            }
            ScalarFunc::Upper
            | ScalarFunc::Lower
            | ScalarFunc::Length
            | ScalarFunc::Trim
            | ScalarFunc::Ltrim
            | ScalarFunc::Rtrim
            | ScalarFunc::Abs => exprs.len() == 1,
            ScalarFunc::Round => exprs.len() == 1 || exprs.len() == 2,
            ScalarFunc::Substring => exprs.len() == 2 || exprs.len() == 3,
            // Cast/Like/Case never arrive through the function-name path.
            ScalarFunc::Cast(_) | ScalarFunc::Like { .. } | ScalarFunc::Case { .. } => false,
        };
        if !ok_arity {
            return Err(SqlError::Unsupported(format!(
                "{fname}() with {} arguments",
                exprs.len()
            )));
        }
        return Ok(Expr::Func { func, args: exprs });
    }

    // Ordered-set aggregate: `mode() WITHIN GROUP (ORDER BY expr)`. The value
    // whose mode is taken comes from the WITHIN GROUP clause, not the arg list.
    if fname == "mode" {
        if !args.is_empty() {
            return Err(SqlError::Unsupported(
                "mode() takes no direct arguments".into(),
            ));
        }
        if f.over.is_some() {
            return Err(SqlError::Unsupported("mode() OVER (...)".into()));
        }
        if f.within_group.len() != 1 {
            return Err(SqlError::Unsupported(
                "mode() requires exactly one WITHIN GROUP (ORDER BY <expr>)".into(),
            ));
        }
        let arg = translate_expr(f.within_group.into_iter().next().unwrap().expr, p)?;
        return Ok(Expr::Aggregate {
            func: AggFunc::Mode,
            arg: Some(Box::new(arg)),
        });
    }
    if !f.within_group.is_empty() {
        return Err(SqlError::Unsupported(
            "WITHIN GROUP is only supported for mode()".into(),
        ));
    }

    if args.len() > 1 {
        return Err(SqlError::Unsupported(
            "aggregate with multiple arguments".into(),
        ));
    }
    let arg = match args.into_iter().next() {
        None => None,
        Some(sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Wildcard)) => None, // COUNT(*)
        Some(sp::FunctionArg::Unnamed(sp::FunctionArgExpr::Expr(e))) => {
            Some(Box::new(translate_expr(e, p)?))
        }
        Some(other) => {
            return Err(SqlError::Unsupported(format!(
                "aggregate argument {other:?}"
            )));
        }
    };

    // Window function? (`... OVER (...)`)
    if let Some(over) = f.over {
        let spec = match over {
            sp::WindowType::WindowSpec(spec) => spec,
            sp::WindowType::NamedWindow(_) => {
                return Err(SqlError::Unsupported("named WINDOW clause".into()));
            }
        };
        if spec.window_frame.is_some() {
            return Err(SqlError::Unsupported(
                "explicit window frames (ROWS/RANGE BETWEEN ...)".into(),
            ));
        }
        let func = match fname.as_str() {
            "row_number" => WindowFunc::RowNumber,
            "rank" => WindowFunc::Rank,
            "dense_rank" => WindowFunc::DenseRank,
            "count" => WindowFunc::Agg(AggFunc::Count, arg),
            "sum" => WindowFunc::Agg(AggFunc::Sum, arg),
            "avg" => WindowFunc::Agg(AggFunc::Avg, arg),
            "min" => WindowFunc::Agg(AggFunc::Min, arg),
            "max" => WindowFunc::Agg(AggFunc::Max, arg),
            other => {
                return Err(SqlError::Unsupported(format!("window function {other}()")));
            }
        };
        let partition_by = spec
            .partition_by
            .into_iter()
            .map(|e| translate_expr(e, p))
            .collect::<Result<_>>()?;
        let mut order_by = Vec::with_capacity(spec.order_by.len());
        for ob in spec.order_by {
            let asc = ob.options.asc.unwrap_or(true);
            order_by.push((translate_expr(ob.expr, p)?, asc));
        }
        return Ok(Expr::Window {
            func,
            partition_by,
            order_by,
        });
    }

    let func = match fname.as_str() {
        "count" => AggFunc::Count,
        "sum" => AggFunc::Sum,
        "avg" => AggFunc::Avg,
        "min" => AggFunc::Min,
        "max" => AggFunc::Max,
        other => return Err(SqlError::Unsupported(format!("function {other}()"))),
    };
    Ok(Expr::Aggregate { func, arg })
}

fn map_binary_op(op: &sp::BinaryOperator) -> Result<BinOp> {
    use sp::BinaryOperator as B;
    let mapped = match op {
        B::Eq => BinOp::Eq,
        B::NotEq => BinOp::Ne,
        B::Lt => BinOp::Lt,
        B::LtEq => BinOp::Le,
        B::Gt => BinOp::Gt,
        B::GtEq => BinOp::Ge,
        B::And => BinOp::And,
        B::Or => BinOp::Or,
        B::Plus => BinOp::Add,
        B::Minus => BinOp::Sub,
        B::Multiply => BinOp::Mul,
        B::Divide => BinOp::Div,
        B::StringConcat => BinOp::Concat,
        other => return Err(SqlError::Unsupported(format!("binary operator {other:?}"))),
    };
    Ok(mapped)
}

fn translate_value(v: &sp::Value, p: &mut usize) -> Result<Expr> {
    match v {
        sp::Value::Placeholder(s) => {
            let idx = if s == "?" {
                let i = *p;
                *p += 1;
                i
            } else if let Some(n) = s.strip_prefix('$') {
                n.parse::<usize>()
                    .map_err(|_| SqlError::Parse(format!("bad placeholder {s:?}")))?
                    .checked_sub(1)
                    .ok_or_else(|| SqlError::Parse("placeholder index must be >= 1".into()))?
            } else {
                return Err(SqlError::Unsupported(format!("placeholder {s:?}")));
            };
            Ok(Expr::Param(idx))
        }
        other => Ok(Expr::Literal(literal_from_value(other)?)),
    }
}

fn literal_from_value(v: &sp::Value) -> Result<Value> {
    match v {
        sp::Value::Number(s, _) => parse_number(s),
        sp::Value::SingleQuotedString(s)
        | sp::Value::DoubleQuotedString(s)
        | sp::Value::EscapedStringLiteral(s) => Ok(Value::Text(s.clone())),
        sp::Value::Boolean(b) => Ok(Value::Bool(*b)),
        sp::Value::Null => Ok(Value::Null),
        other => Err(SqlError::Unsupported(format!("literal {other:?}"))),
    }
}

fn parse_number(s: &str) -> Result<Value> {
    let has_exp = s.contains('e') || s.contains('E');
    if s.contains('.') && !has_exp {
        // A plain fractional literal (`9.99`) is an *exact* DECIMAL, matching
        // standard SQL numeric literals — so money math stays lossless.
        if let Some(dec) = crate::decimal::Decimal::parse(s) {
            return Ok(Value::Decimal(dec));
        }
        // Fall back to float if it somehow isn't exactly representable.
        return s
            .parse::<f64>()
            .map(Value::Double)
            .map_err(|_| SqlError::Parse(format!("bad number literal {s:?}")));
    }
    if has_exp {
        // Exponent form stays a float (IEEE-754), like `1e3`.
        return s
            .parse::<f64>()
            .map(Value::Double)
            .map_err(|_| SqlError::Parse(format!("bad number literal {s:?}")));
    }
    s.parse::<i64>()
        .map(Value::Int)
        .map_err(|_| SqlError::Parse(format!("bad integer literal {s:?}")))
}

fn expr_to_u64(expr: &sp::Expr) -> Result<u64> {
    match expr {
        sp::Expr::Value(v) => match &v.value {
            sp::Value::Number(s, _) => s
                .parse::<u64>()
                .map_err(|_| SqlError::Parse(format!("bad LIMIT value {s:?}"))),
            other => Err(SqlError::Unsupported(format!("LIMIT value {other:?}"))),
        },
        other => Err(SqlError::Unsupported(format!("LIMIT expression {other:?}"))),
    }
}

// ── name helpers ──────────────────────────────────────────────────────────

fn object_name_to_string(name: &sp::ObjectName) -> Result<String> {
    let ident = name
        .0
        .last()
        .and_then(|p| p.as_ident())
        .ok_or_else(|| SqlError::Parse("empty object name".into()))?;
    Ok(ident.value.clone())
}

fn single_object_name(names: &[sp::ObjectName]) -> Result<String> {
    if names.len() != 1 {
        return Err(SqlError::Unsupported("DROP of multiple objects".into()));
    }
    object_name_to_string(&names[0])
}

fn table_ref_from_factor(factor: &sp::TableFactor, p: &mut usize) -> Result<TableRef> {
    match factor {
        sp::TableFactor::Table { name, alias, .. } => Ok(TableRef {
            name: object_name_to_string(name)?,
            alias: alias.as_ref().map(|a| a.name.value.clone()),
            subquery: None,
        }),
        sp::TableFactor::Derived {
            lateral,
            subquery,
            alias,
        } => {
            if *lateral {
                return Err(SqlError::Unsupported("LATERAL subquery in FROM".into()));
            }
            let Some(alias) = alias else {
                return Err(SqlError::Unsupported(
                    "derived table without an alias (add `AS name`)".into(),
                ));
            };
            let q = translate_query((**subquery).clone(), p)?;
            Ok(TableRef {
                name: alias.name.value.clone(),
                alias: Some(alias.name.value.clone()),
                subquery: Some(Box::new(q)),
            })
        }
        other => Err(SqlError::Unsupported(format!("table factor {other:?}"))),
    }
}

fn table_name_from_twj(twj: &sp::TableWithJoins) -> Result<String> {
    if !twj.joins.is_empty() {
        return Err(SqlError::Unsupported("JOIN not allowed here".into()));
    }
    let r = table_ref_from_factor(&twj.relation, &mut 0)?;
    if r.subquery.is_some() {
        return Err(SqlError::Unsupported("subquery not allowed here".into()));
    }
    Ok(r.name)
}
