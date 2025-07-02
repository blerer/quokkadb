use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    DeserializationError(String),
    InvalidArgument(String),
    BsonDeError(bson::de::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "{}", e),
            Error::DeserializationError(reason) => write!(f, "{}", reason),
            Error::InvalidArgument(reason) => write!(f, "{}", reason),
            Error::BsonDeError(e) => write!(f, "{}", e),
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

pub type Result<T> = std::result::Result<T, Error>;