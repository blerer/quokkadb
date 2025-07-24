use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    DeserializationError(String),
    InvalidArgument(String),
    InvalidState(String),
    BsonDeError(bson::de::Error),
    BsonSerError(bson::ser::Error),
    BsonRawError(bson::raw::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{}", e),
            Error::DeserializationError(reason) | Error::InvalidArgument(reason) | Error::InvalidState(reason) => write!(f, "{}", reason),
            Error::BsonSerError(e) => write!(f, "{}", e),
            Error::BsonDeError(e) => write!(f, "{}", e),
            Error::BsonRawError(e) => write!(f, "{}", e),
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

pub type Result<T> = std::result::Result<T, Error>;