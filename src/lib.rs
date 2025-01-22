/*!
 *! # LiteDB in Rust
 *! This is a reimplementation of [LiteDB] in Rust.
 *!
 *! This implementation (currently?) only supports single-threaded operation.
 *!
 *! [LiteDB]: https://www.litedb.org/
 */

use std::fmt::Display;

mod engine;
mod utils;

pub type Result<T> = std::result::Result<T, Error>;

pub struct Error {
    message: String,
}

impl Error {
    pub(crate) fn invalid_database() -> Error {
        Error::err("Invalid database file")
    }

    pub(crate) fn invalid_page() -> Error {
        Error::err("Invalid database file")
    }

    pub(crate) fn datetime_overflow() -> Self {
        Self::err("DateTime overflow")
    }
}

impl Error {
    pub fn err(message: impl Display) -> Self {
        Error {
            message: message.to_string(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error {
            message: err.to_string(),
        }
    }
}

impl From<bson::de::Error> for Error {
    fn from(err: bson::de::Error) -> Self {
        Error {
            message: err.to_string(),
        }
    }
}

impl From<bson::ser::Error> for Error {
    fn from(err: bson::ser::Error) -> Self {
        Error {
            message: err.to_string(),
        }
    }
}
