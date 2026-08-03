use crate::storage::storage_engine::StorageError;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    DeserializationError(String),
    InvalidRequest(String),
    BsonError(bson::error::Error),
    ErrorMode(String),
    UnexpectedError(String),
    VersionConflict(String),
    LogCorruption {
        record_offset: u64,
        reason: String,
    },
    CollectionAlreadyExists(String),
    CollectionNotFound {
        name: String,
        id: Option<u32>,
    },
    IndexNotFound {
        collection_name: String,
        index_name: String,
        id: Option<u32>,
    },
    IndexOptionsConflict {
        collection_name: String,
        index_name: String,
        reason: String,
    },
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

pub type Result<T> = std::result::Result<T, Error>;
