use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("document not found: {0}")]
    NotFound(u64),

    #[error("collection not found: {0}")]
    CollectionNotFound(String),

    #[error("collection already exists: {0}")]
    CollectionAlreadyExists(String),

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
}

pub type Result<T> = std::result::Result<T, Error>;
