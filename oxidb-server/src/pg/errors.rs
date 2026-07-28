//! Engine errors as PostgreSQL `ErrorResponse`s.
//!
//! The SQLSTATE is the part clients act on: psycopg raises `UniqueViolation`
//! for `23505` and `UndefinedTable` for `42P01` regardless of the message
//! text, so a wrong code turns a recoverable application error into an opaque
//! one. The engine's own wording is carried through as the message, unchanged.

use oxidb_sql::SqlError;

/// A wire-ready error: SQLSTATE plus message.
pub struct PgError {
    pub code: &'static str,
    pub message: String,
}

impl PgError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        PgError {
            code,
            message: message.into(),
        }
    }

    /// `0A000 feature_not_supported` — the honest answer for anything this
    /// server understands but does not implement.
    pub fn unsupported(message: impl Into<String>) -> Self {
        PgError::new(SQLSTATE_FEATURE_NOT_SUPPORTED, message)
    }

    /// `42601 syntax_error`.
    pub fn syntax(message: impl Into<String>) -> Self {
        PgError::new(SQLSTATE_SYNTAX_ERROR, message)
    }

    /// `42501 insufficient_privilege`.
    pub fn denied(message: impl Into<String>) -> Self {
        PgError::new(SQLSTATE_INSUFFICIENT_PRIVILEGE, message)
    }

    /// `08P01 protocol_violation`.
    pub fn protocol(message: impl Into<String>) -> Self {
        PgError::new(SQLSTATE_PROTOCOL_VIOLATION, message)
    }

    /// `XX000 internal_error`.
    pub fn internal(message: impl Into<String>) -> Self {
        PgError::new(SQLSTATE_INTERNAL_ERROR, message)
    }
}

pub const SQLSTATE_INTERNAL_ERROR: &str = "XX000";
pub const SQLSTATE_FEATURE_NOT_SUPPORTED: &str = "0A000";
pub const SQLSTATE_SYNTAX_ERROR: &str = "42601";
pub const SQLSTATE_INSUFFICIENT_PRIVILEGE: &str = "42501";
pub const SQLSTATE_PROTOCOL_VIOLATION: &str = "08P01";
pub const SQLSTATE_UNIQUE_VIOLATION: &str = "23505";
pub const SQLSTATE_FOREIGN_KEY_VIOLATION: &str = "23503";
pub const SQLSTATE_NOT_NULL_VIOLATION: &str = "23502";
pub const SQLSTATE_UNDEFINED_TABLE: &str = "42P01";
pub const SQLSTATE_UNDEFINED_COLUMN: &str = "42703";
pub const SQLSTATE_UNDEFINED_OBJECT: &str = "42704";
pub const SQLSTATE_DUPLICATE_TABLE: &str = "42P07";
pub const SQLSTATE_DUPLICATE_OBJECT: &str = "42710";
pub const SQLSTATE_DATATYPE_MISMATCH: &str = "42804";
pub const SQLSTATE_STRING_DATA_RIGHT_TRUNCATION: &str = "22001";
pub const SQLSTATE_LOCK_NOT_AVAILABLE: &str = "55P03";
pub const SQLSTATE_IN_FAILED_TRANSACTION: &str = "25P02";
pub const SQLSTATE_READ_ONLY_SQL_TRANSACTION: &str = "25006";
pub const SQLSTATE_CONFIGURATION_LIMIT_EXCEEDED: &str = "53400";
pub const SQLSTATE_DATA_CORRUPTED: &str = "XX001";
pub const SQLSTATE_IO_ERROR: &str = "58030";
pub const SQLSTATE_INVALID_AUTHORIZATION: &str = "28000";
pub const SQLSTATE_INVALID_PASSWORD: &str = "28P01";
pub const SQLSTATE_UNDEFINED_DATABASE: &str = "3D000";

impl From<SqlError> for PgError {
    fn from(e: SqlError) -> Self {
        let code = match &e {
            SqlError::DuplicateKey(_) => SQLSTATE_UNIQUE_VIOLATION,
            SqlError::ForeignKeyViolation(_) => SQLSTATE_FOREIGN_KEY_VIOLATION,
            SqlError::NoSuchTable(_) => SQLSTATE_UNDEFINED_TABLE,
            SqlError::NoSuchColumn(_) => SQLSTATE_UNDEFINED_COLUMN,
            SqlError::NoSuchIndex(_) | SqlError::NoSuchView(_) | SqlError::NoSuchProcedure(_) => {
                SQLSTATE_UNDEFINED_OBJECT
            }
            SqlError::TableExists(_) => SQLSTATE_DUPLICATE_TABLE,
            SqlError::IndexExists(_) => SQLSTATE_DUPLICATE_OBJECT,
            SqlError::Parse(_) => SQLSTATE_SYNTAX_ERROR,
            SqlError::Unsupported(_) => SQLSTATE_FEATURE_NOT_SUPPORTED,
            SqlError::LockTimeout { .. } => SQLSTATE_LOCK_NOT_AVAILABLE,
            SqlError::ValueTooLong { .. } => SQLSTATE_STRING_DATA_RIGHT_TRUNCATION,
            // The engine reports a NOT NULL breach through SchemaMismatch, so
            // the message is the only thing that distinguishes it. Clients key
            // recovery off 23502 specifically, which is worth the sniff.
            SqlError::SchemaMismatch(m) if m.contains("NOT NULL") => SQLSTATE_NOT_NULL_VIOLATION,
            SqlError::SchemaMismatch(_) => SQLSTATE_DATATYPE_MISMATCH,
            SqlError::TableLimitExceeded(_) => SQLSTATE_CONFIGURATION_LIMIT_EXCEEDED,
            SqlError::Corrupt(_) => SQLSTATE_DATA_CORRUPTED,
            SqlError::Io(_) => SQLSTATE_IO_ERROR,
            SqlError::Eval(_) | SqlError::Serde(_) => SQLSTATE_INTERNAL_ERROR,
        };
        PgError::new(code, e.to_string())
    }
}

/// The bridge hands back `String` errors in a few places (the read-only gate,
/// the engine registry). Recover the interesting codes from the wording rather
/// than flattening everything to XX000.
impl From<String> for PgError {
    fn from(msg: String) -> Self {
        let code = if msg.contains("permission denied") {
            SQLSTATE_INSUFFICIENT_PRIVILEGE
        } else if msg.contains("no such database") || msg.contains("SQL engine is disabled") {
            SQLSTATE_UNDEFINED_DATABASE
        } else {
            SQLSTATE_INTERNAL_ERROR
        };
        PgError::new(code, msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_codes_clients_recover_from_are_exact() {
        // psycopg maps these to UniqueViolation / UndefinedTable / etc.; a
        // wrong code here turns a handled error into an opaque one.
        let dup: PgError = SqlError::DuplicateKey("x".into()).into();
        assert_eq!(dup.code, "23505");
        let missing: PgError = SqlError::NoSuchTable("t".into()).into();
        assert_eq!(missing.code, "42P01");
        let fk: PgError = SqlError::ForeignKeyViolation("f".into()).into();
        assert_eq!(fk.code, "23503");
        let syntax: PgError = SqlError::Parse("bad".into()).into();
        assert_eq!(syntax.code, "42601");
        let unsup: PgError = SqlError::Unsupported("nope".into()).into();
        assert_eq!(unsup.code, "0A000");
    }

    #[test]
    fn a_not_null_breach_is_23502_not_a_generic_type_error() {
        let e: PgError = SqlError::SchemaMismatch("column \"a\" is NOT NULL but got NULL".into())
            .into();
        assert_eq!(e.code, SQLSTATE_NOT_NULL_VIOLATION);
        let other: PgError = SqlError::SchemaMismatch("column \"a\" expects Int".into()).into();
        assert_eq!(other.code, SQLSTATE_DATATYPE_MISMATCH);
    }

    #[test]
    fn the_engine_message_survives_translation() {
        let e: PgError = SqlError::NoSuchTable("users".into()).into();
        assert!(e.message.contains("users"), "{}", e.message);
    }
}
