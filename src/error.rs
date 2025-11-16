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
    LogCorruption{ record_offset: u64, reason: String },
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
            | Error::VersionConflict(reason) => write!(f, "{}", reason),
            Error::LogCorruption { record_offset, reason } => {
                write!(f, "Log corruption at offset {}: {}", record_offset, reason)
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
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;