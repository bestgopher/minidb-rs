use std::io::ErrorKind;
use std::sync::PoisonError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Io(std::io::Error),
    #[error("EOF")]
    Eof,
    #[error("db file not exist")]
    DBFileNotExist,
    #[error("empty key")]
    EmptyKey,
    #[error("key not exist")]
    KeyNotExists,
    #[error("mutex poison error")]
    MutexPoison,
    #[error("the read content is empty")]
    EmptyContent,
    #[error("invalid offset")]
    InvalidOffset,
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        match err.kind() {
            ErrorKind::UnexpectedEof => Error::Eof,
            _ => Error::Io(err),
        }
    }
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Error::MutexPoison
    }
}
