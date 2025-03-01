//! # LiteDB in Rust
//! This is a reimplementation of [LiteDB] in Rust.
//!
//! This implementation (currently?) only supports single-threaded operation.
//!
//! [LiteDB]: <https://www.litedb.org/>

#![allow(clippy::too_many_arguments)]

use crate::bson::Value;
use crate::engine::{BasePage, PageType};
use std::fmt::Display;

#[macro_use]
pub mod bson;
pub mod engine;
pub mod expression;
mod utils;

#[cfg(all(feature = "shared-mutex", windows))]
pub mod shared_mutex;
#[cfg(feature = "tokio-fs")]
pub mod tokio_fs;

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
        Error::err(format!(
            "Invalid page type: expected {:?}, got {:?}",
            expected,
            page.page_type()
        ))
    }

    pub(crate) fn collection_index_limit_reached() -> Error {
        Error::err("Collection index limit reached")
    }

    pub(crate) fn name_length_header_space(name: &str) -> Error {
        Error::err(format!(
            "Name length exceeds available header space: {}",
            name
        ))
    }

    pub(crate) fn invalid_collection_name(name: &str) -> Error {
        Error::err(format!("Invalid collection name: {}", name))
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

    pub(crate) fn invalid_index_key(message: &str) -> Error {
        Error::err(format!("Invalid index key: {}", message))
    }

    pub(crate) fn index_duplicate_key(index: &str, key: Value) -> Error {
        Error::err(format!("Duplicate key in index {index}: {key:?}"))
    }

    pub(crate) fn already_exists_collection_name(name: &str) -> Error {
        Error::err(format!("Already exists collection name: {}", name))
    }

    pub(crate) fn document_size_exceed_limit() -> Self {
        Error::err("DocumentSize exceed limit")
    }

    pub(crate) fn index_already_exists(name: &str) -> Error {
        Error::err(format!("Index already exists: {}", name))
    }

    pub(crate) fn drop_id_index() -> Error {
        Error::err("Drop _id index is forbidden")
    }

    #[cfg(feature = "sequential-index")]
    pub(crate) fn bad_auto_id(
        auto_id: engine::BsonAutoId,
        collection_name: &str,
        last_id: Value,
    ) -> Self {
        Error::err(format!(
            "It's not possible use AutoId={auto_id:?} because '{collection_name}' collection contains not only numbers in _id index ({last_id:?})."
        ))
    }

    pub(crate) fn invalid_data_type(field: &str, value: &Value) -> Error {
        Error::err(format!(
            "Invalid data type for field {}: {:?}",
            field, value
        ))
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

impl From<bson::ParseError> for Error {
    fn from(err: bson::ParseError) -> Self {
        Error {
            message: err.to_string(),
        }
    }
}

impl From<expression::ParseError> for Error {
    fn from(err: expression::ParseError) -> Self {
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

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for Error {}
