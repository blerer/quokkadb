use std::fmt;
use std::io;
use crate::storage::storage_engine::StorageError;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    DeserializationError(String),
    InvalidRequest(String),
    BsonDeError(bson::de::Error),
    BsonSerError(bson::ser::Error),
    BsonRawError(bson::raw::Error),
    ErrorMode(String),
    UnexpectedError(String),
    VersionConflict(String),
    LogCorruption { record_offset: u64, reason: String },
    CollectionAlreadyExists(String),
    CollectionNotFound { name: String, id: Option<u32> },
    IndexNotFound { collection_name: String, index_name: String, id: Option<u32> } ,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{}", e),
            Error::BsonSerError(e) => write!(f, "{}", e),
            Error::BsonDeError(e) => write!(f, "{}", e),
            Error::BsonRawError(e) => write!(f, "{}", e),
            Error::DeserializationError(reason)
            | Error::InvalidRequest(reason)
            | Error::ErrorMode(reason)
            | Error::UnexpectedError(reason)
            | Error::VersionConflict(reason)
                 => write!(f, "{}", reason),
            Error::LogCorruption { record_offset, reason } => {
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
            Error::IndexNotFound { collection_name, index_name, id} => {
                if let Some(id) = id {
                    write!(f, "Index does not exist: {}.{} (id: {})", collection_name, index_name, id)
                } else {
                    write!(f, "Index does not exist: {}.{}", collection_name, index_name)
                }
            }
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<bson::de::Error> for Error {
    fn from(err: bson::de::Error) -> Self {
        Error::BsonDeError(err)
    }
}

impl From<bson::ser::Error> for Error {
    fn from(err: bson::ser::Error) -> Self {
        Error::BsonSerError(err)
    }
}

impl From<bson::raw::Error> for Error {
    fn from(err: bson::raw::Error) -> Self {
        Error::BsonRawError(err)
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::Io(e) => Error::Io(e),
            StorageError::UnexpectedError(reason) => Error::UnexpectedError(reason),
            StorageError::ErrorMode(reason) => Error::ErrorMode(reason),
            StorageError::VersionConflict{ user_key: _, reason } => Error::VersionConflict(reason),
            StorageError::LogCorruption { record_offset, reason } =>
                Error::LogCorruption{ record_offset, reason },
            StorageError::CollectionAlreadyExists(name) => Error::CollectionAlreadyExists(name),
            StorageError::CollectionNotFound { name, id} => Error::CollectionNotFound { name, id },
            StorageError::IndexNotFound { collection_name, index_name, id} =>
                Error::IndexNotFound { collection_name, index_name, id },
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;