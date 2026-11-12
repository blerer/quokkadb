use crate::storage::storage_engine::StorageError;
use std::fmt;
use std::io;

/// Errors returned by QuokkaDB operations.
///
/// User-input problems are reported as [`InvalidRequest`](Self::InvalidRequest)
/// or [`BsonError`](Self::BsonError). Storage and operating-system failures are
/// preserved in the corresponding variants so applications can log or match
/// them separately.
#[derive(Debug)]
pub enum Error {
    /// An operating-system or filesystem error.
    Io(io::Error),
    /// A stored document could not be deserialized into the requested Rust type.
    DeserializationError(String),
    /// A query, update, projection, or other user request is invalid.
    InvalidRequest(String),
    /// BSON serialization or deserialization failed.
    BsonError(bson::error::Error),
    /// An operation is incompatible with the current database mode.
    ErrorMode(String),
    /// An internal operation failed without a more specific public category.
    UnexpectedError(String),
    /// A concurrent catalog or schema version changed during the operation.
    VersionConflict(String),
    /// A write-ahead log record failed framing or checksum validation.
    LogCorruption {
        /// Byte offset of the corrupt record in the log.
        record_offset: u64,
        /// Description of the corruption detected.
        reason: String,
    },
    /// A collection with the requested name already exists.
    CollectionAlreadyExists(String),
    /// The requested collection does not exist.
    CollectionNotFound {
        /// Collection name used by the request.
        name: String,
        /// Catalog ID, when one was available while reporting the error.
        id: Option<u32>,
    },
    /// The requested index does not exist.
    IndexNotFound {
        /// Collection containing the requested index.
        collection_name: String,
        /// Requested index name.
        index_name: String,
        /// Catalog ID, when one was available while reporting the error.
        id: Option<u32>,
    },
    /// An existing index was requested with incompatible options.
    IndexOptionsConflict {
        /// Collection containing the conflicting index.
        collection_name: String,
        /// Conflicting index name.
        index_name: String,
        /// Explanation of the incompatible options.
        reason: String,
    },
    /// Database options failed validation.
    InvalidOptions(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{}", e),
            Error::BsonError(e) => write!(f, "{}", e),
            Error::DeserializationError(reason)
            | Error::InvalidRequest(reason)
            | Error::ErrorMode(reason)
            | Error::UnexpectedError(reason)
            | Error::VersionConflict(reason) => write!(f, "{}", reason),
            Error::LogCorruption {
                record_offset,
                reason,
            } => {
                write!(f, "Log corruption at offset {}: {}", record_offset, reason)
            }
            Error::CollectionAlreadyExists(name) => {
                write!(f, "Collection already exists: {}", name)
            }
            Error::CollectionNotFound { name, id } => {
                if let Some(id) = id {
                    write!(f, "Collection does not exist: {} (id {})", name, id)
                } else {
                    write!(f, "Collection does not exist: {}", name)
                }
            }
            Error::IndexNotFound {
                collection_name,
                index_name,
                id,
            } => {
                if let Some(id) = id {
                    write!(
                        f,
                        "Index does not exist: {}.{} (id: {})",
                        collection_name, index_name, id
                    )
                } else {
                    write!(
                        f,
                        "Index does not exist: {}.{}",
                        collection_name, index_name
                    )
                }
            }
            Error::IndexOptionsConflict {
                collection_name,
                index_name,
                reason,
            } => {
                write!(
                    f,
                    "Index options conflict: {}.{}: {}",
                    collection_name, index_name, reason
                )
            }
            Error::InvalidOptions(reason) => write!(f, "Invalid options: {}", reason),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<bson::error::Error> for Error {
    fn from(err: bson::error::Error) -> Self {
        Error::BsonError(err)
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::Io(e) => Error::Io(e),
            StorageError::UnexpectedError(reason) => Error::UnexpectedError(reason),
            StorageError::ErrorMode(reason) => Error::ErrorMode(reason),
            StorageError::VersionConflict {
                user_key: _,
                reason,
            } => Error::VersionConflict(reason),
            StorageError::SchemaVersionConflict {
                collection,
                expected,
                actual,
            } => Error::VersionConflict(format!(
                "Schema version conflict for collection {}: expected {}, found {:?}",
                collection, expected, actual
            )),
            StorageError::IndexBuildInProgress { collection } => Error::VersionConflict(format!(
                "Cannot write to collection {} while an index is being built",
                collection
            )),
            StorageError::LogCorruption {
                record_offset,
                reason,
            } => Error::LogCorruption {
                record_offset,
                reason,
            },
            StorageError::CollectionAlreadyExists(name) => Error::CollectionAlreadyExists(name),
            StorageError::CollectionNotFound { name, id } => Error::CollectionNotFound { name, id },
            StorageError::IndexNotFound {
                collection_name,
                index_name,
                id,
            } => Error::IndexNotFound {
                collection_name,
                index_name,
                id,
            },
            StorageError::IndexOptionsConflict {
                collection_name,
                index_name,
                reason,
            } => Error::IndexOptionsConflict {
                collection_name,
                index_name,
                reason,
            },
        }
    }
}

/// Constructs a collection-not-found error for a collection name.
pub(crate) fn collection_not_found_error(name: &str) -> Error {
    Error::CollectionNotFound {
        name: name.to_string(),
        id: None,
    }
}

/// Constructs an index-not-found error for a collection and index name.
pub(crate) fn index_not_found_error(collection: &str, index: &str) -> Error {
    Error::IndexNotFound {
        collection_name: collection.to_string(),
        index_name: index.to_string(),
        id: None,
    }
}

/// Result type used throughout the public QuokkaDB API.
pub type Result<T> = std::result::Result<T, Error>;
