use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("document not found: {0}")]
    NotFound(u64),

    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    /// A read named a snapshot that has ended or outlived
    /// `OXIDB_SNAPSHOT_MAX_SECS`. Reads through a dead snapshot must fail,
    /// never silently degrade to latest (ADR-0017).
    #[error("snapshot {0} has expired or ended")]
    SnapshotExpired(u64),

    #[error("collection already exists: {0}")]
    CollectionAlreadyExists(String),

    #[error("collection limit reached ({0}) — cannot create another collection")]
    CollectionLimitExceeded(usize),

    #[error("document limit reached ({0}) — cannot insert more documents")]
    DocumentLimitExceeded(usize),

    #[error("index already exists: {0}")]
    IndexAlreadyExists(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("invalid query: {0}")]
    InvalidQuery(String),

    #[error("unique constraint violated: field '{field}' value already exists")]
    UniqueViolation { field: String },

    #[error("invalid pipeline: {0}")]
    InvalidPipeline(String),

    #[error("document must be a JSON object")]
    NotAnObject,

    #[error("blob not found: {bucket}/{key}")]
    BlobNotFound { bucket: String, key: String },

    #[error("bucket not found: {0}")]
    BucketNotFound(String),

    #[error(
        "transaction conflict on '{collection}' doc {doc_id}: expected version {expected_version}, found {actual_version}"
    )]
    TransactionConflict {
        collection: String,
        doc_id: u64,
        expected_version: u64,
        actual_version: u64,
    },

    #[error("lock timeout on '{collection}' doc {doc_id}: held by another transaction")]
    LockTimeout { collection: String, doc_id: u64 },

    #[error("transaction not found: {0}")]
    TransactionNotFound(u64),

    /// An interactive transaction sat idle past `OXIDB_TX_MAX_IDLE_SECS`
    /// and was rolled back by the engine. Distinct from
    /// `TransactionNotFound` so a client that abandoned a transaction and
    /// came back learns what actually happened to its buffered writes.
    #[error("transaction {0} expired: idle past the transaction idle timeout and rolled back")]
    TransactionExpired(u64),

    #[error("no active transaction")]
    NoActiveTransaction,

    #[error("index not found: {0}")]
    IndexNotFound(String),

    #[error("encryption error: {0}")]
    Encryption(String),

    #[error("decryption error: {0}")]
    Decryption(String),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("backup error: {0}")]
    Backup(String),

    #[error("procedure not found: {0}")]
    ProcedureNotFound(String),

    #[error("procedure error: {0}")]
    ProcedureError(String),

    #[error("schedule error: {0}")]
    ScheduleError(String),

    #[error("database not found: {0}")]
    DatabaseNotFound(String),

    #[error("database already exists: {0}")]
    DatabaseAlreadyExists(String),

    #[error("cannot drop default database")]
    CannotDropDefaultDatabase,

    #[error("invalid database name: {0}")]
    InvalidDatabaseName(String),

    /// On-disk format version is newer than this engine recognises.
    /// Triggered by the forward-compatibility tripwires landing in
    /// the format-spec compliance work (docs/format/*.md). Better to
    /// refuse loudly than to attempt a best-effort read of fields
    /// whose semantics may have changed.
    #[error("incompatible on-disk format: {0}")]
    IncompatibleFormat(String),

    /// WORM phase 2 — document is engine-level locked and the
    /// requested mutation (update / delete / find_and_modify) is
    /// refused at the storage layer. Distinct from the
    /// application-level WORMViolation that DMS surfaces from
    /// CheckMutation; this one fires regardless of whether the
    /// caller is the DMS API or a direct admin connection.
    #[error(
        "document is WORM-locked on '{collection}' doc {doc_id} until {locked_until_micros} (now {now_micros})"
    )]
    DocumentWormLocked {
        collection: String,
        doc_id: u64,
        locked_until_micros: u64,
        now_micros: u64,
    },
}

pub type Result<T> = std::result::Result<T, Error>;
