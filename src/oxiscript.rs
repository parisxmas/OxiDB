//! OxiScript — a lightweight scripting language for OxiDB stored procedures.
//!
//! OxiScript compiles to the existing JSON step representation, so all
//! execution, transaction, and variable binding logic is reused.
//!
//! ```text
//! proc transfer_funds(from, to, amount) {
//!     let sender = find_one("accounts", {account_id: from})
//!     if sender.balance < amount {
//!         abort "insufficient funds"
//!     }
//!     update("accounts", {account_id: from}, {$inc: {balance: -amount}})
//!     update("accounts", {account_id: to}, {$inc: {balance: amount}})
//!     return {status: "ok"}
//! }
//! ```

use serde_json::{json, Value};

// ─── Tokens ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Proc,
    Let,
    If,
    Else,
    Return,
    Abort,
    For,
    In,
    True,
    False,
    Null,

    // Literals
    Number(f64),
    Str(String),
    Ident(String),       // variable or field name
    DollarIdent(String), // $set, $inc, $push, etc.

    // Operators
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Lt,
    Gt,
    LtEq,
    GtEq,
    EqEq,
    BangEq,
    And,
    Or,
    Bang,
    Eq, // =

    // Delimiters
    LParen,
    RParen,
    LBrace,
    RBrace,
    LBracket,
    RBracket,
    Comma,
    Colon,
    Dot,
    Semicolon,

    Eof,
}

// ─── Lexer ──────────────────────────────────────────────────────────

pub struct Lexer {
    chars: Vec<char>,
    pos: usize,
    line: usize,
    col: usize,
}

impl Lexer {
    pub fn new(source: &str) -> Self {
        Self {
            chars: source.chars().collect(),
            pos: 0,
            line: 1,
            col: 1,
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>, String> {
        let mut tokens = Vec::new();
        loop {
            self.skip_whitespace_and_comments();
            if self.pos >= self.chars.len() {
                tokens.push(Token::Eof);
                break;
            }
            let tok = self.next_token()?;
            tokens.push(tok);
        }
        Ok(tokens)
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let ch = self.chars.get(self.pos).copied()?;
        self.pos += 1;
        if ch == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(ch)
    }

    fn peek_next(&self) -> Option<char> {
        self.chars.get(self.pos + 1).copied()
    }

    fn skip_whitespace_and_comments(&mut self) {
        loop {
            // Skip whitespace
            while self.peek().is_some_and(|c| c.is_whitespace()) {
                self.advance();
            }
            // Skip // line comments
            if self.peek() == Some('/') && self.peek_next() == Some('/') {
                while self.peek().is_some_and(|c| c != '\n') {
                    self.advance();
                }
                continue;
            }
            // Skip /* block comments */
            if self.peek() == Some('/') && self.peek_next() == Some('*') {
                self.advance();
                self.advance();
                loop {
                    if self.peek().is_none() {
                        break;
                    }
                    if self.peek() == Some('*') && self.peek_next() == Some('/') {
                        self.advance();
                        self.advance();
                        break;
                    }
                    self.advance();
                }
                continue;
            }
            break;
        }
    }

    fn next_token(&mut self) -> Result<Token, String> {
        let ch = self.peek().unwrap();

        // Numbers
        if ch.is_ascii_digit() {
            return self.read_number();
        }

        // Strings
        if ch == '"' || ch == '\'' {
            return self.read_string();
        }

        // Dollar-prefixed identifiers ($set, $inc, etc.)
        if ch == '$' {
            self.advance();
            let mut name = String::from("$");
            while self.peek().is_some_and(|c| c.is_alphanumeric() || c == '_') {
                name.push(self.advance().unwrap());
            }
            return Ok(Token::DollarIdent(name));
        }

        // Identifiers and keywords
        if ch.is_alphabetic() || ch == '_' {
            return self.read_ident();
        }

        // Operators and delimiters
        self.advance();
        match ch {
            '+' => Ok(Token::Plus),
            '-' => Ok(Token::Minus),
            '*' => Ok(Token::Star),
            '/' => Ok(Token::Slash),
            '%' => Ok(Token::Percent),
            '(' => Ok(Token::LParen),
            ')' => Ok(Token::RParen),
            '{' => Ok(Token::LBrace),
            '}' => Ok(Token::RBrace),
            '[' => Ok(Token::LBracket),
            ']' => Ok(Token::RBracket),
            ',' => Ok(Token::Comma),
            ':' => Ok(Token::Colon),
            '.' => Ok(Token::Dot),
            ';' => Ok(Token::Semicolon),
            '=' => {
                if self.peek() == Some('=') {
                    self.advance();
                    Ok(Token::EqEq)
                } else {
                    Ok(Token::Eq)
                }
            }
            '!' => {
                if self.peek() == Some('=') {
                    self.advance();
                    Ok(Token::BangEq)
                } else {
                    Ok(Token::Bang)
                }
            }
            '<' => {
                if self.peek() == Some('=') {
                    self.advance();
                    Ok(Token::LtEq)
                } else {
                    Ok(Token::Lt)
                }
            }
            '>' => {
                if self.peek() == Some('=') {
                    self.advance();
                    Ok(Token::GtEq)
                } else {
                    Ok(Token::Gt)
                }
            }
            '&' => {
                if self.peek() == Some('&') {
                    self.advance();
                    Ok(Token::And)
                } else {
                    Err(format!("line {}: unexpected '&', did you mean '&&'?", self.line))
                }
            }
            '|' => {
                if self.peek() == Some('|') {
                    self.advance();
                    Ok(Token::Or)
                } else {
                    Err(format!("line {}: unexpected '|', did you mean '||'?", self.line))
                }
            }
            _ => Err(format!("line {}: unexpected character '{}'", self.line, ch)),
        }
    }

    fn read_number(&mut self) -> Result<Token, String> {
        let mut s = String::new();
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            s.push(self.advance().unwrap());
        }
        if self.peek() == Some('.') && self.peek_next().is_some_and(|c| c.is_ascii_digit()) {
            s.push(self.advance().unwrap()); // .
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                s.push(self.advance().unwrap());
            }
        }
        s.parse::<f64>()
            .map(Token::Number)
            .map_err(|e| format!("line {}: invalid number '{}': {}", self.line, s, e))
    }

    fn read_string(&mut self) -> Result<Token, String> {
        let quote = self.advance().unwrap();
        let mut s = String::new();
        loop {
            match self.advance() {
                Some('\\') => match self.advance() {
                    Some('n') => s.push('\n'),
                    Some('t') => s.push('\t'),
                    Some('\\') => s.push('\\'),
                    Some(c) if c == quote => s.push(c),
                    Some(c) => {
                        s.push('\\');
                        s.push(c);
                    }
                    None => return Err(format!("line {}: unterminated string", self.line)),
                },
                Some(c) if c == quote => return Ok(Token::Str(s)),
                Some(c) => s.push(c),
                None => return Err(format!("line {}: unterminated string", self.line)),
            }
        }
    }

    fn read_ident(&mut self) -> Result<Token, String> {
        let mut name = String::new();
        while self.peek().is_some_and(|c| c.is_alphanumeric() || c == '_') {
            name.push(self.advance().unwrap());
        }
        Ok(match name.as_str() {
            "proc" => Token::Proc,
            "let" => Token::Let,
            "if" => Token::If,
            "else" => Token::Else,
            "return" => Token::Return,
            "abort" => Token::Abort,
            "for" => Token::For,
            "in" => Token::In,
            "true" => Token::True,
            "false" => Token::False,
            "null" => Token::Null,
            _ => Token::Ident(name),
        })
    }
}

// ─── AST ────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Expr {
    Number(f64),
    Str(String),
    Bool(bool),
    Null,
    Ident(String),
    DollarIdent(String),
    Field(Box<Expr>, String),
    Index(Box<Expr>, Box<Expr>),
    BinOp(Box<Expr>, BinOp, Box<Expr>),
    UnaryNot(Box<Expr>),
    UnaryNeg(Box<Expr>),
    Object(Vec<(Expr, Expr)>), // key can be string, ident, or dollar-ident
    Array(Vec<Expr>),
    Call(String, Vec<Expr>),
}

#[derive(Debug, Clone, Copy)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Lt,
    Gt,
    LtEq,
    GtEq,
    Eq,
    Neq,
    And,
    Or,
}

#[derive(Debug, Clone)]
pub enum Stmt {
    Let(String, Expr),
    If(Expr, Vec<Stmt>, Option<Vec<Stmt>>),
    For(String, Expr, Vec<Stmt>),
    Return(Option<Expr>),
    Abort(Option<Expr>),
    ExprStmt(Expr),
}

#[derive(Debug, Clone)]
pub struct ProcDef {
    pub name: String,
    pub params: Vec<String>,
    pub body: Vec<Stmt>,
}

// ─── Parser ─────────────────────────────────────────────────────────

pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> Token {
        let tok = self.tokens.get(self.pos).cloned().unwrap_or(Token::Eof);
        self.pos += 1;
        tok
    }

    fn expect(&mut self, expected: &Token) -> Result<(), String> {
        let tok = self.advance();
        if &tok == expected {
            Ok(())
        } else {
            Err(format!("expected {:?}, got {:?}", expected, tok))
        }
    }

    fn expect_ident(&mut self) -> Result<String, String> {
        match self.advance() {
            Token::Ident(s) => Ok(s),
            other => Err(format!("expected identifier, got {:?}", other)),
        }
    }

    // ── Top-level ──

    pub fn parse_procedure(&mut self) -> Result<ProcDef, String> {
        self.expect(&Token::Proc)?;
        let name = self.expect_ident()?;
        self.expect(&Token::LParen)?;

        let mut params = Vec::new();
        if self.peek() != &Token::RParen {
            params.push(self.expect_ident()?);
            while self.peek() == &Token::Comma {
                self.advance();
                params.push(self.expect_ident()?);
            }
        }
        self.expect(&Token::RParen)?;

        let body = self.parse_block()?;

        Ok(ProcDef { name, params, body })
    }

    /// Parse multiple procedures from a single source.
    pub fn parse_all(&mut self) -> Result<Vec<ProcDef>, String> {
        let mut procs = Vec::new();
        while self.peek() != &Token::Eof {
            procs.push(self.parse_procedure()?);
        }
        Ok(procs)
    }

    fn parse_block(&mut self) -> Result<Vec<Stmt>, String> {
        self.expect(&Token::LBrace)?;
        let mut stmts = Vec::new();
        while self.peek() != &Token::RBrace && self.peek() != &Token::Eof {
            stmts.push(self.parse_stmt()?);
            // Optional semicolons
            while self.peek() == &Token::Semicolon {
                self.advance();
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(stmts)
    }

    // ── Statements ──

    fn parse_stmt(&mut self) -> Result<Stmt, String> {
        match self.peek().clone() {
            Token::Let => self.parse_let(),
            Token::If => self.parse_if(),
            Token::For => self.parse_for(),
            Token::Return => self.parse_return(),
            Token::Abort => self.parse_abort(),
            _ => {
                let expr = self.parse_expr()?;
                Ok(Stmt::ExprStmt(expr))
            }
        }
    }

    fn parse_let(&mut self) -> Result<Stmt, String> {
        self.advance(); // let
        let name = self.expect_ident()?;
        self.expect(&Token::Eq)?;
        let expr = self.parse_expr()?;
        Ok(Stmt::Let(name, expr))
    }

    fn parse_if(&mut self) -> Result<Stmt, String> {
        self.advance(); // if
        let cond = self.parse_expr()?;
        let then_block = self.parse_block()?;
        let else_block = if self.peek() == &Token::Else {
            self.advance();
            if self.peek() == &Token::If {
                // else if → single-element else block containing another if
                Some(vec![self.parse_if()?])
            } else {
                Some(self.parse_block()?)
            }
        } else {
            None
        };
        Ok(Stmt::If(cond, then_block, else_block))
    }

    fn parse_for(&mut self) -> Result<Stmt, String> {
        self.advance(); // for
        let var = self.expect_ident()?;
        self.expect(&Token::In)?;
        let iter = self.parse_expr()?;
        let body = self.parse_block()?;
        Ok(Stmt::For(var, iter, body))
    }

    fn parse_return(&mut self) -> Result<Stmt, String> {
        self.advance(); // return
        if matches!(
            self.peek(),
            Token::RBrace | Token::Semicolon | Token::Eof
        ) {
            Ok(Stmt::Return(None))
        } else {
            Ok(Stmt::Return(Some(self.parse_expr()?)))
        }
    }

    fn parse_abort(&mut self) -> Result<Stmt, String> {
        self.advance(); // abort
        if matches!(
            self.peek(),
            Token::RBrace | Token::Semicolon | Token::Eof
        ) {
            Ok(Stmt::Abort(None))
        } else {
            Ok(Stmt::Abort(Some(self.parse_expr()?)))
        }
    }

    // ── Expressions (precedence climbing) ──

    fn parse_expr(&mut self) -> Result<Expr, String> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and()?;
        while self.peek() == &Token::Or {
            self.advance();
            let right = self.parse_and()?;
            left = Expr::BinOp(Box::new(left), BinOp::Or, Box::new(right));
        }
        Ok(left)
    }

    fn parse_and(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_equality()?;
        while self.peek() == &Token::And {
            self.advance();
            let right = self.parse_equality()?;
            left = Expr::BinOp(Box::new(left), BinOp::And, Box::new(right));
        }
        Ok(left)
    }

    fn parse_equality(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_comparison()?;
        loop {
            match self.peek() {
                Token::EqEq => {
                    self.advance();
                    let right = self.parse_comparison()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Eq, Box::new(right));
                }
                Token::BangEq => {
                    self.advance();
                    let right = self.parse_comparison()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Neq, Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_additive()?;
        loop {
            match self.peek() {
                Token::Lt => {
                    self.advance();
                    let right = self.parse_additive()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Lt, Box::new(right));
                }
                Token::Gt => {
                    self.advance();
                    let right = self.parse_additive()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Gt, Box::new(right));
                }
                Token::LtEq => {
                    self.advance();
                    let right = self.parse_additive()?;
                    left = Expr::BinOp(Box::new(left), BinOp::LtEq, Box::new(right));
                }
                Token::GtEq => {
                    self.advance();
                    let right = self.parse_additive()?;
                    left = Expr::BinOp(Box::new(left), BinOp::GtEq, Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_additive(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_multiplicative()?;
        loop {
            match self.peek() {
                Token::Plus => {
                    self.advance();
                    let right = self.parse_multiplicative()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Add, Box::new(right));
                }
                Token::Minus => {
                    self.advance();
                    let right = self.parse_multiplicative()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Sub, Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_multiplicative(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_unary()?;
        loop {
            match self.peek() {
                Token::Star => {
                    self.advance();
                    let right = self.parse_unary()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Mul, Box::new(right));
                }
                Token::Slash => {
                    self.advance();
                    let right = self.parse_unary()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Div, Box::new(right));
                }
                Token::Percent => {
                    self.advance();
                    let right = self.parse_unary()?;
                    left = Expr::BinOp(Box::new(left), BinOp::Mod, Box::new(right));
                }
                _ => break,
            }
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, String> {
        match self.peek() {
            Token::Bang => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryNot(Box::new(expr)))
            }
            Token::Minus => {
                self.advance();
                let expr = self.parse_unary()?;
                Ok(Expr::UnaryNeg(Box::new(expr)))
            }
            _ => self.parse_postfix(),
        }
    }

    fn parse_postfix(&mut self) -> Result<Expr, String> {
        let mut expr = self.parse_primary()?;
        loop {
            match self.peek() {
                Token::Dot => {
                    self.advance();
                    let field = self.expect_ident()?;
                    expr = Expr::Field(Box::new(expr), field);
                }
                Token::LBracket => {
                    self.advance();
                    let index = self.parse_expr()?;
                    self.expect(&Token::RBracket)?;
                    expr = Expr::Index(Box::new(expr), Box::new(index));
                }
                _ => break,
            }
        }
        Ok(expr)
    }

    fn parse_primary(&mut self) -> Result<Expr, String> {
        match self.peek().clone() {
            Token::Number(n) => {
                self.advance();
                Ok(Expr::Number(n))
            }
            Token::Str(s) => {
                self.advance();
                Ok(Expr::Str(s))
            }
            Token::True => {
                self.advance();
                Ok(Expr::Bool(true))
            }
            Token::False => {
                self.advance();
                Ok(Expr::Bool(false))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::Null)
            }
            Token::DollarIdent(s) => {
                self.advance();
                Ok(Expr::DollarIdent(s))
            }
            Token::Ident(name) => {
                self.advance();
                // Check if it's a function call
                if self.peek() == &Token::LParen {
                    self.advance();
                    let mut args = Vec::new();
                    if self.peek() != &Token::RParen {
                        args.push(self.parse_expr()?);
                        while self.peek() == &Token::Comma {
                            self.advance();
                            args.push(self.parse_expr()?);
                        }
                    }
                    self.expect(&Token::RParen)?;
                    Ok(Expr::Call(name, args))
                } else {
                    Ok(Expr::Ident(name))
                }
            }
            Token::LBrace => self.parse_object(),
            Token::LBracket => self.parse_array(),
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            other => Err(format!("unexpected token: {:?}", other)),
        }
    }

    fn parse_object(&mut self) -> Result<Expr, String> {
        self.advance(); // {
        let mut pairs = Vec::new();
        if self.peek() != &Token::RBrace {
            loop {
                let key = match self.peek().clone() {
                    Token::Ident(s) => {
                        self.advance();
                        Expr::Str(s)
                    }
                    Token::Str(s) => {
                        self.advance();
                        Expr::Str(s)
                    }
                    Token::DollarIdent(s) => {
                        self.advance();
                        Expr::Str(s)
                    }
                    other => return Err(format!("expected object key, got {:?}", other)),
                };
                self.expect(&Token::Colon)?;
                let val = self.parse_expr()?;
                pairs.push((key, val));
                if self.peek() != &Token::Comma {
                    break;
                }
                self.advance();
                // Allow trailing comma
                if self.peek() == &Token::RBrace {
                    break;
                }
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(Expr::Object(pairs))
    }

    fn parse_array(&mut self) -> Result<Expr, String> {
        self.advance(); // [
        let mut elems = Vec::new();
        if self.peek() != &Token::RBracket {
            loop {
                elems.push(self.parse_expr()?);
                if self.peek() != &Token::Comma {
                    break;
                }
                self.advance();
                if self.peek() == &Token::RBracket {
                    break;
                }
            }
        }
        self.expect(&Token::RBracket)?;
        Ok(Expr::Array(elems))
    }
}

// ─── Compiler ───────────────────────────────────────────────────────
//
// Compiles OxiScript AST into the JSON step format used by
// procedure.rs's execute_procedure().

/// Database operation function names.
const DB_FUNCTIONS: &[&str] = &[
    "find",
    "find_one",
    "insert",
    "update",
    "update_one",
    "delete",
    "delete_one",
    "count",
    "aggregate",
];

pub struct Compiler {
    /// Counter for auto-generated variable names.
    var_counter: usize,
    /// Parameter names for the current procedure — used to emit `$param.x` references.
    params: Vec<String>,
}

impl Compiler {
    pub fn new() -> Self {
        Self {
            var_counter: 0,
            params: Vec::new(),
        }
    }

    fn fresh_var(&mut self) -> String {
        self.var_counter += 1;
        format!("_t{}", self.var_counter)
    }

    fn is_param(&self, name: &str) -> bool {
        self.params.iter().any(|p| p == name)
    }

    /// Compile a procedure definition to the JSON format expected by
    /// `create_procedure`.
    pub fn compile(&mut self, proc: &ProcDef) -> Result<Value, String> {
        self.params = proc.params.clone();
        let steps = self.compile_stmts(&proc.body)?;
        Ok(json!({
            "name": proc.name,
            "params": proc.params,
            "steps": steps
        }))
    }

    fn compile_stmts(&mut self, stmts: &[Stmt]) -> Result<Vec<Value>, String> {
        let mut steps = Vec::new();
        for stmt in stmts {
            self.compile_stmt(stmt, &mut steps)?;
        }
        Ok(steps)
    }

    fn compile_stmt(&mut self, stmt: &Stmt, steps: &mut Vec<Value>) -> Result<(), String> {
        match stmt {
            Stmt::Let(name, expr) => {
                self.compile_let(name, expr, steps)?;
            }
            Stmt::If(cond, then_block, else_block) => {
                let condition = self.compile_condition(cond)?;
                let then_steps = self.compile_stmts(then_block)?;
                let else_steps = match else_block {
                    Some(stmts) => Some(self.compile_stmts(stmts)?),
                    None => None,
                };
                let mut step = json!({
                    "step": "if",
                    "condition": condition,
                    "then": then_steps
                });
                if let Some(else_s) = else_steps {
                    step["else"] = json!(else_s);
                }
                steps.push(step);
            }
            Stmt::For(var, iter_expr, body) => {
                // For loops compile to: find → iterate pattern
                // let _items = <iter_expr>
                // for each item, set var = item and run body
                // Currently: compile as a find + manual unrolling note
                // The procedure engine doesn't support loops natively,
                // so we compile to a workaround using aggregate $unwind
                // or note it as unsupported for now.
                let iter_val = self.compile_expr(iter_expr)?;
                let body_steps = self.compile_stmts(body)?;
                steps.push(json!({
                    "step": "for",
                    "var": var,
                    "in": iter_val,
                    "body": body_steps
                }));
            }
            Stmt::Return(expr) => {
                let value = match expr {
                    Some(e) => self.compile_expr(e)?,
                    None => json!({"ok": true}),
                };
                steps.push(json!({"step": "return", "value": value}));
            }
            Stmt::Abort(expr) => {
                let message = match expr {
                    Some(e) => self.compile_expr(e)?,
                    None => json!("aborted"),
                };
                steps.push(json!({"step": "abort", "message": message}));
            }
            Stmt::ExprStmt(expr) => {
                // A standalone expression — must be a DB function call
                self.compile_expr_stmt(expr, steps)?;
            }
        }
        Ok(())
    }

    fn compile_let(&mut self, name: &str, expr: &Expr, steps: &mut Vec<Value>) -> Result<(), String> {
        match expr {
            Expr::Call(func, args) if DB_FUNCTIONS.contains(&func.as_str()) => {
                let mut step = self.compile_db_call(func, args)?;
                step["as"] = json!(name);
                steps.push(step);
            }
            Expr::Call(func, args) => {
                // Unknown function → procedure call
                let mut step = self.compile_proc_call(func, args)?;
                step["as"] = json!(name);
                steps.push(step);
            }
            _ => {
                let value = self.compile_expr(expr)?;
                steps.push(json!({"step": "set", "name": name, "value": value}));
            }
        }
        Ok(())
    }

    fn compile_expr_stmt(&mut self, expr: &Expr, steps: &mut Vec<Value>) -> Result<(), String> {
        match expr {
            Expr::Call(func, args) if DB_FUNCTIONS.contains(&func.as_str()) => {
                let step = self.compile_db_call(func, args)?;
                steps.push(step);
            }
            Expr::Call(func, args) => {
                // Unknown function → procedure call (result discarded)
                let step = self.compile_proc_call(func, args)?;
                steps.push(step);
            }
            _ => {
                return Err("standalone expression must be a function call".into());
            }
        }
        Ok(())
    }

    fn compile_db_call(&mut self, func: &str, args: &[Expr]) -> Result<Value, String> {
        match func {
            "find" => {
                let collection = self.expect_string_arg(args, 0, "find")?;
                let query = if args.len() > 1 {
                    self.compile_expr(&args[1])?
                } else {
                    json!({})
                };
                let mut step = json!({"step": "find", "collection": collection, "query": query});
                if args.len() > 2 {
                    step["sort"] = self.compile_expr(&args[2])?;
                }
                if args.len() > 3 {
                    step["limit"] = self.compile_expr(&args[3])?;
                }
                Ok(step)
            }
            "find_one" => {
                let collection = self.expect_string_arg(args, 0, "find_one")?;
                let query = if args.len() > 1 {
                    self.compile_expr(&args[1])?
                } else {
                    json!({})
                };
                Ok(json!({"step": "find_one", "collection": collection, "query": query}))
            }
            "insert" => {
                let collection = self.expect_string_arg(args, 0, "insert")?;
                let doc = if args.len() > 1 {
                    self.compile_expr(&args[1])?
                } else {
                    return Err("insert requires a document argument".into());
                };
                Ok(json!({"step": "insert", "collection": collection, "document": doc}))
            }
            "update" | "update_one" => {
                let collection = self.expect_string_arg(args, 0, func)?;
                if args.len() < 3 {
                    return Err(format!("{} requires (collection, query, update)", func));
                }
                let query = self.compile_expr(&args[1])?;
                let update = self.compile_expr(&args[2])?;
                Ok(json!({"step": func, "collection": collection, "query": query, "update": update}))
            }
            "delete" | "delete_one" => {
                let collection = self.expect_string_arg(args, 0, func)?;
                let query = if args.len() > 1 {
                    self.compile_expr(&args[1])?
                } else {
                    json!({})
                };
                Ok(json!({"step": func, "collection": collection, "query": query}))
            }
            "count" => {
                let collection = self.expect_string_arg(args, 0, "count")?;
                let query = if args.len() > 1 {
                    self.compile_expr(&args[1])?
                } else {
                    json!({})
                };
                Ok(json!({"step": "count", "collection": collection, "query": query}))
            }
            "aggregate" => {
                let collection = self.expect_string_arg(args, 0, "aggregate")?;
                let pipeline = if args.len() > 1 {
                    self.compile_expr(&args[1])?
                } else {
                    return Err("aggregate requires a pipeline argument".into());
                };
                Ok(json!({"step": "aggregate", "collection": collection, "pipeline": pipeline}))
            }
            _ => Err(format!("unknown db function: {}", func)),
        }
    }

    /// Compile a call to another stored procedure.
    /// `get_balance(user_id)` → `{"step": "call_procedure", "name": "get_balance", "params": {...}}`
    fn compile_proc_call(&mut self, func: &str, args: &[Expr]) -> Result<Value, String> {
        // Build params object from positional args
        // Convention: single object arg → pass as params directly
        // Multiple args or non-object → pass as positional array "_args"
        if args.len() == 1 {
            if let Expr::Object(_) = &args[0] {
                let params = self.compile_expr(&args[0])?;
                return Ok(json!({
                    "step": "call_procedure",
                    "name": func,
                    "params": params
                }));
            }
        }

        // For positional args, build a params object with arg0, arg1, ...
        // or if single non-object arg, use {"_arg": value}
        if args.is_empty() {
            Ok(json!({
                "step": "call_procedure",
                "name": func,
                "params": {}
            }))
        } else if args.len() == 1 {
            let val = self.compile_expr(&args[0])?;
            Ok(json!({
                "step": "call_procedure",
                "name": func,
                "params": {"_arg": val}
            }))
        } else {
            let mut params = serde_json::Map::new();
            for (i, arg) in args.iter().enumerate() {
                let val = self.compile_expr(arg)?;
                params.insert(format!("_arg{}", i), val);
            }
            Ok(json!({
                "step": "call_procedure",
                "name": func,
                "params": Value::Object(params)
            }))
        }
    }

    fn expect_string_arg(&self, args: &[Expr], idx: usize, func: &str) -> Result<String, String> {
        match args.get(idx) {
            Some(Expr::Str(s)) => Ok(s.clone()),
            Some(_) => Err(format!("{}: argument {} must be a string literal", func, idx)),
            None => Err(format!("{}: missing argument {}", func, idx)),
        }
    }

    // ── Expression compilation ──

    fn compile_expr(&mut self, expr: &Expr) -> Result<Value, String> {
        match expr {
            Expr::Number(n) => {
                if *n == (*n as i64) as f64 && n.is_finite() {
                    Ok(json!(*n as i64))
                } else {
                    Ok(json!(n))
                }
            }
            Expr::Str(s) => Ok(json!(s)),
            Expr::Bool(b) => Ok(json!(b)),
            Expr::Null => Ok(Value::Null),
            Expr::Ident(name) => {
                // Parameter → $param.name, variable → $name
                if self.is_param(name) {
                    Ok(json!(format!("$param.{}", name)))
                } else {
                    Ok(json!(format!("${}", name)))
                }
            }
            Expr::DollarIdent(name) => {
                // MongoDB operator like $set, $inc — pass through as-is
                Ok(json!(name))
            }
            Expr::Field(base, field) => {
                // a.b.c → "$a.b.c"
                let path = self.flatten_field_path(base, field);
                Ok(json!(path))
            }
            Expr::Index(base, idx) => {
                // a[0] → "$a.0"
                let base_path = match self.compile_expr(base)? {
                    Value::String(s) => s,
                    _ => return Err("index base must be an identifier".into()),
                };
                let idx_val = self.compile_expr(idx)?;
                match idx_val {
                    Value::Number(n) => Ok(json!(format!("{}.{}", base_path, n))),
                    Value::String(s) => Ok(json!(format!("{}.{}", base_path, s))),
                    _ => Err("index must be a number or string".into()),
                }
            }
            Expr::Object(pairs) => {
                let mut map = serde_json::Map::new();
                for (key, val) in pairs {
                    let k = match self.compile_expr(key)? {
                        Value::String(s) => s,
                        _ => return Err("object key must be a string".into()),
                    };
                    let v = self.compile_expr(val)?;
                    map.insert(k, v);
                }
                Ok(Value::Object(map))
            }
            Expr::Array(elems) => {
                let vals: Result<Vec<Value>, String> =
                    elems.iter().map(|e| self.compile_expr(e)).collect();
                Ok(Value::Array(vals?))
            }
            Expr::UnaryNeg(inner) => {
                let val = self.compile_expr(inner)?;
                match val {
                    Value::Number(ref n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(json!(-i))
                        } else if let Some(f) = n.as_f64() {
                            Ok(json!(-f))
                        } else {
                            Ok(json!({"$multiply": [val, -1]}))
                        }
                    }
                    // Runtime negation: -variable → {"$multiply": ["$var", -1]}
                    _ => Ok(json!({"$multiply": [val, -1]})),
                }
            }
            Expr::UnaryNot(inner) => {
                let val = self.compile_expr(inner)?;
                Ok(json!({"$not": val}))
            }
            Expr::BinOp(left, op, right) => {
                let l = self.compile_expr(left)?;
                let r = self.compile_expr(right)?;
                let op_str = match op {
                    BinOp::Lt => "$lt",
                    BinOp::Gt => "$gt",
                    BinOp::LtEq => "$lte",
                    BinOp::GtEq => "$gte",
                    BinOp::Eq => "$eq",
                    BinOp::Neq => "$ne",
                    BinOp::And => "$and",
                    BinOp::Or => "$or",
                    BinOp::Add => "$add",
                    BinOp::Sub => "$subtract",
                    BinOp::Mul => "$multiply",
                    BinOp::Div => "$divide",
                    BinOp::Mod => "$mod",
                };
                match op {
                    BinOp::And | BinOp::Or => Ok(json!({op_str: [l, r]})),
                    _ => Ok(json!({"$expr": {op_str: [l, r]}})),
                }
            }
            Expr::Call(func, _args) => {
                // Function calls in expression context must be assigned:
                // let x = find_one(...) or let x = my_proc(...)
                Err(format!(
                    "function {}() must be assigned to a variable: let x = {}(...)",
                    func, func
                ))
            }
        }
    }

    fn flatten_field_path(&self, base: &Expr, field: &str) -> String {
        match base {
            Expr::Ident(name) => {
                if self.is_param(name) {
                    format!("$param.{}.{}", name, field)
                } else {
                    format!("${}.{}", name, field)
                }
            }
            Expr::Field(inner_base, inner_field) => {
                let prefix = self.flatten_field_path(inner_base, inner_field);
                format!("{}.{}", prefix, field)
            }
            _ => format!("$.{}", field),
        }
    }

    fn compile_condition(&mut self, expr: &Expr) -> Result<Value, String> {
        match expr {
            Expr::BinOp(left, op, right) => {
                let l = self.compile_expr(left)?;
                let r = self.compile_expr(right)?;
                let op_str = match op {
                    BinOp::Lt => "$lt",
                    BinOp::Gt => "$gt",
                    BinOp::LtEq => "$lte",
                    BinOp::GtEq => "$gte",
                    BinOp::Eq => "$eq",
                    BinOp::Neq => "$ne",
                    BinOp::And => return Ok(json!({"$and": [self.compile_condition(left)?, self.compile_condition(right)?]})),
                    BinOp::Or => return Ok(json!({"$or": [self.compile_condition(left)?, self.compile_condition(right)?]})),
                    _ => return Ok(json!({"$expr": {match op {
                        BinOp::Add => "$add",
                        BinOp::Sub => "$subtract",
                        BinOp::Mul => "$multiply",
                        BinOp::Div => "$divide",
                        BinOp::Mod => "$mod",
                        _ => unreachable!(),
                    }: [l, r]}})),
                };
                Ok(json!({"$expr": {op_str: [l, r]}}))
            }
            Expr::UnaryNot(inner) => {
                let cond = self.compile_condition(inner)?;
                Ok(json!({"$not": cond}))
            }
            _ => self.compile_expr(expr),
        }
    }
}

// ─── Public API ─────────────────────────────────────────────────────

/// Parse and compile OxiScript source to the JSON procedure definition.
pub fn compile(source: &str) -> Result<Value, String> {
    let mut lexer = Lexer::new(source);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let proc = parser.parse_procedure()?;
    let mut compiler = Compiler::new();
    compiler.compile(&proc)
}

/// Parse and compile multiple procedures from a single source.
pub fn compile_all(source: &str) -> Result<Vec<Value>, String> {
    let mut lexer = Lexer::new(source);
    let tokens = lexer.tokenize()?;
    let mut parser = Parser::new(tokens);
    let procs = parser.parse_all()?;
    let mut compiler = Compiler::new();
    procs.iter().map(|p| compiler.compile(p)).collect()
}

// ─── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lex_basic_tokens() {
        let mut lexer = Lexer::new("proc foo(a, b) { }");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Proc);
        assert_eq!(tokens[1], Token::Ident("foo".into()));
        assert_eq!(tokens[2], Token::LParen);
    }

    #[test]
    fn lex_dollar_idents() {
        let mut lexer = Lexer::new("$set $inc $push");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::DollarIdent("$set".into()));
        assert_eq!(tokens[1], Token::DollarIdent("$inc".into()));
        assert_eq!(tokens[2], Token::DollarIdent("$push".into()));
    }

    #[test]
    fn lex_operators() {
        let mut lexer = Lexer::new("<= >= == != && ||");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::LtEq);
        assert_eq!(tokens[1], Token::GtEq);
        assert_eq!(tokens[2], Token::EqEq);
        assert_eq!(tokens[3], Token::BangEq);
        assert_eq!(tokens[4], Token::And);
        assert_eq!(tokens[5], Token::Or);
    }

    #[test]
    fn lex_strings() {
        let mut lexer = Lexer::new(r#""hello" 'world'"#);
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Str("hello".into()));
        assert_eq!(tokens[1], Token::Str("world".into()));
    }

    #[test]
    fn lex_numbers() {
        let mut lexer = Lexer::new("42 3.14");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Number(42.0));
        assert_eq!(tokens[1], Token::Number(3.14));
    }

    #[test]
    fn lex_comments() {
        let mut lexer = Lexer::new("// comment\nproc /* block */ foo() {}");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Proc);
        assert_eq!(tokens[1], Token::Ident("foo".into()));
    }

    #[test]
    fn parse_minimal_proc() {
        let src = r#"proc test() { return null }"#;
        let result = compile(src).unwrap();
        assert_eq!(result["name"], "test");
        assert_eq!(result["params"].as_array().unwrap().len(), 0);
        assert_eq!(result["steps"][0]["step"], "return");
    }

    #[test]
    fn parse_proc_with_params() {
        let src = r#"proc greet(name, age) { return {greeting: name} }"#;
        let result = compile(src).unwrap();
        assert_eq!(result["params"][0], "name");
        assert_eq!(result["params"][1], "age");
    }

    #[test]
    fn compile_let_find_one() {
        let src = r#"
            proc test(id) {
                let user = find_one("users", {_id: id})
                return user
            }
        "#;
        let result = compile(src).unwrap();
        let steps = result["steps"].as_array().unwrap();
        assert_eq!(steps[0]["step"], "find_one");
        assert_eq!(steps[0]["collection"], "users");
        assert_eq!(steps[0]["as"], "user");
        assert_eq!(steps[1]["step"], "return");
    }

    #[test]
    fn compile_insert() {
        let src = r#"
            proc test() {
                insert("logs", {level: "info", msg: "hello"})
            }
        "#;
        let result = compile(src).unwrap();
        let step = &result["steps"][0];
        assert_eq!(step["step"], "insert");
        assert_eq!(step["collection"], "logs");
        assert_eq!(step["document"]["level"], "info");
    }

    #[test]
    fn compile_update_with_operators() {
        let src = r#"
            proc test(id, amount) {
                update("accounts", {_id: id}, {$inc: {balance: amount}})
            }
        "#;
        let result = compile(src).unwrap();
        let step = &result["steps"][0];
        assert_eq!(step["step"], "update");
        assert_eq!(step["query"]["_id"], "$param.id");
        assert_eq!(step["update"]["$inc"]["balance"], "$param.amount");
    }

    #[test]
    fn compile_if_else() {
        let src = r#"
            proc test(x) {
                if x > 10 {
                    return "big"
                } else {
                    return "small"
                }
            }
        "#;
        let result = compile(src).unwrap();
        let step = &result["steps"][0];
        assert_eq!(step["step"], "if");
        assert!(step["then"].is_array());
        assert!(step["else"].is_array());
    }

    #[test]
    fn compile_abort() {
        let src = r#"
            proc test() {
                abort "something went wrong"
            }
        "#;
        let result = compile(src).unwrap();
        assert_eq!(result["steps"][0]["step"], "abort");
        assert_eq!(result["steps"][0]["message"], "something went wrong");
    }

    #[test]
    fn compile_field_access() {
        let src = r#"
            proc test() {
                let user = find_one("users", {name: "Alice"})
                return user.email
            }
        "#;
        let result = compile(src).unwrap();
        assert_eq!(result["steps"][1]["value"], "$user.email");
    }

    #[test]
    fn compile_nested_field() {
        let src = r#"
            proc test() {
                let user = find_one("users", {})
                return user.address.city
            }
        "#;
        let result = compile(src).unwrap();
        assert_eq!(result["steps"][1]["value"], "$user.address.city");
    }

    #[test]
    fn compile_transfer_funds() {
        let src = r#"
            proc transfer_funds(from, to, amount) {
                let sender = find_one("accounts", {account_id: from})
                if sender.balance < amount {
                    abort "insufficient funds"
                }
                update("accounts", {account_id: from}, {$inc: {balance: -amount}})
                update("accounts", {account_id: to}, {$inc: {balance: amount}})
                return {status: "ok"}
            }
        "#;
        let result = compile(src).unwrap();
        assert_eq!(result["name"], "transfer_funds");
        assert_eq!(result["params"].as_array().unwrap().len(), 3);
        let steps = result["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 5);
        assert_eq!(steps[0]["step"], "find_one");
        assert_eq!(steps[1]["step"], "if");
        assert_eq!(steps[2]["step"], "update");
        assert_eq!(steps[3]["step"], "update");
        assert_eq!(steps[4]["step"], "return");
    }

    #[test]
    fn compile_count() {
        let src = r#"
            proc test() {
                let n = count("users", {active: true})
                return n
            }
        "#;
        let result = compile(src).unwrap();
        assert_eq!(result["steps"][0]["step"], "count");
        assert_eq!(result["steps"][0]["as"], "n");
    }

    #[test]
    fn compile_aggregate() {
        let src = r#"
            proc test() {
                let results = aggregate("orders", [
                    {$match: {status: "completed"}},
                    {$group: {_id: "$customer", total: {$sum: "$amount"}}}
                ])
                return results
            }
        "#;
        let result = compile(src).unwrap();
        assert_eq!(result["steps"][0]["step"], "aggregate");
        assert!(result["steps"][0]["pipeline"].is_array());
    }

    #[test]
    fn compile_delete() {
        let src = r#"
            proc cleanup(days) {
                let deleted = delete("logs", {age: {$gt: days}})
                return deleted
            }
        "#;
        let result = compile(src).unwrap();
        assert_eq!(result["steps"][0]["step"], "delete");
        assert_eq!(result["steps"][0]["as"], "deleted");
    }

    #[test]
    fn compile_array_literal() {
        let src = r#"
            proc test() {
                return [1, 2, 3]
            }
        "#;
        let result = compile(src).unwrap();
        let val = &result["steps"][0]["value"];
        assert_eq!(val[0], 1);
        assert_eq!(val[1], 2);
        assert_eq!(val[2], 3);
    }

    #[test]
    fn compile_set_variable() {
        let src = r#"
            proc test() {
                let x = 42
                let y = "hello"
                return {x: x, y: y}
            }
        "#;
        let result = compile(src).unwrap();
        assert_eq!(result["steps"][0]["step"], "set");
        assert_eq!(result["steps"][0]["name"], "x");
        assert_eq!(result["steps"][0]["value"], 42);
        assert_eq!(result["steps"][1]["step"], "set");
    }

    #[test]
    fn compile_negative_number_in_update() {
        let src = r#"
            proc test(id, amount) {
                update("accounts", {_id: id}, {$inc: {balance: -100}})
            }
        "#;
        let result = compile(src).unwrap();
        assert_eq!(result["steps"][0]["query"]["_id"], "$param.id");
        assert_eq!(result["steps"][0]["update"]["$inc"]["balance"], -100);
    }

    #[test]
    fn compile_else_if() {
        let src = r#"
            proc test(x) {
                if x > 100 {
                    return "large"
                } else if x > 10 {
                    return "medium"
                } else {
                    return "small"
                }
            }
        "#;
        let result = compile(src).unwrap();
        let step = &result["steps"][0];
        assert_eq!(step["step"], "if");
        // else contains another if
        let else_block = step["else"].as_array().unwrap();
        assert_eq!(else_block[0]["step"], "if");
    }

    #[test]
    fn compile_multiple_procs() {
        let src = r#"
            proc foo() { return 1 }
            proc bar() { return 2 }
        "#;
        let results = compile_all(src).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["name"], "foo");
        assert_eq!(results[1]["name"], "bar");
    }

    #[test]
    fn error_on_missing_collection_arg() {
        let src = r#"proc test() { find_one() }"#;
        assert!(compile(src).is_err());
    }

    #[test]
    fn unknown_function_becomes_proc_call() {
        let src = r#"proc test() { notify_admin("alert") }"#;
        let result = compile(src).unwrap();
        let step = &result["steps"][0];
        assert_eq!(step["step"], "call_procedure");
        assert_eq!(step["name"], "notify_admin");
    }

    #[test]
    fn proc_call_with_object_params() {
        let src = r#"
            proc test(user_id) {
                let balance = get_balance({user: user_id})
                return balance
            }
        "#;
        let result = compile(src).unwrap();
        let step = &result["steps"][0];
        assert_eq!(step["step"], "call_procedure");
        assert_eq!(step["name"], "get_balance");
        assert_eq!(step["as"], "balance");
        assert!(step["params"].is_object());
    }

    #[test]
    fn proc_call_no_args() {
        let src = r#"proc test() { cleanup() }"#;
        let result = compile(src).unwrap();
        assert_eq!(result["steps"][0]["step"], "call_procedure");
        assert_eq!(result["steps"][0]["name"], "cleanup");
    }

    #[test]
    fn proc_call_single_value_arg() {
        let src = r#"
            proc test(id) {
                let result = get_user(id)
                return result
            }
        "#;
        let result = compile(src).unwrap();
        let step = &result["steps"][0];
        assert_eq!(step["step"], "call_procedure");
        assert_eq!(step["name"], "get_user");
        assert_eq!(step["params"]["_arg"], "$param.id");
    }

    #[test]
    fn proc_call_multiple_args() {
        let src = r#"
            proc test() {
                let r = transfer("acc1", "acc2", 100)
                return r
            }
        "#;
        let result = compile(src).unwrap();
        let step = &result["steps"][0];
        assert_eq!(step["step"], "call_procedure");
        assert_eq!(step["params"]["_arg0"], "acc1");
        assert_eq!(step["params"]["_arg1"], "acc2");
        assert_eq!(step["params"]["_arg2"], 100);
    }
}
