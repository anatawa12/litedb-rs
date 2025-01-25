/*!
 *! # LiteDB in Rust
 *! This is a reimplementation of [LiteDB] in Rust.
 *!
 *! This implementation (currently?) only supports single-threaded operation.
 *!
 *! [LiteDB]: https://www.litedb.org/
 */

use std::fmt::Display;
use crate::engine::{BasePage, PageType};

mod engine;
mod utils;
pub mod bson;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
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

    pub(crate) fn encrypted_no_password() -> Self {
        Self::err("Encrypted database without password")
    }

    pub(crate) fn collation_not_match() -> Error {
        Error::err("Collation not match")
    }

    pub(crate) fn invalid_page_type(expected: PageType, page: BasePage) -> Error {
        Error::err(format!("Invalid page type: expected {:?}, got {:?}", expected, page.page_type()))
    }

    pub(crate) fn collection_index_limit_reached() -> Error {
        Error::err("Collection index limit reached")
    }

    pub(crate) fn name_length_header_space(name: &str) -> Error {
        Error::err(format!("Name length exceeds available header space: {}", name))
    }

    pub(crate) fn invalid_collection_name(name: &str) -> Error {
        Error::err(format!("Invalid collection name: {}", name))
    }

    pub(crate) fn no_free_space_page(page_id: u32, available: usize, need: usize) -> Error {
        Error::err(format!("No free space in page: {} (available: {}, need: {})", page_id, available, need))
    }

    pub(crate) fn invalid_bson() -> Error {
        Error::err("Invalid BSON")
    }

    pub(crate) fn size_limit_reached() -> Self {
        Self::err("Size limit reached")
    }

    pub(crate) fn transaction_limit() -> Error {
        Self::err("Maximum number of transactions reached")
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

impl From<::bson::de::Error> for Error {
    fn from(err: ::bson::de::Error) -> Self {
        Error {
            message: err.to_string(),
        }
    }
}

impl From<::bson::ser::Error> for Error {
    fn from(err: ::bson::ser::Error) -> Self {
        Error {
            message: err.to_string(),
        }
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Self::err(err)
    }
}
