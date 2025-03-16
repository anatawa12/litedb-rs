//! # LiteDB in Rust
//! This is a reimplementation of [LiteDB] in Rust.
//!
//! This implementation (currently?) only supports single-threaded operation.
//!
//! [LiteDB]: <https://www.litedb.org/>

#![allow(clippy::too_many_arguments)]

use crate::bson::Value;
use std::fmt::Display;

#[macro_use]
pub mod bson;
pub mod expression;
mod utils;

mod buffer_reader;
mod buffer_writer;
mod constants;
pub mod file_io;
#[cfg(all(feature = "shared-mutex", windows))]
pub mod shared_mutex;
#[cfg(feature = "tokio-fs")]
pub mod tokio_fs;

pub type Result<T> = std::result::Result<T, Error>;

pub struct Error(Box<ErrorImpl>);

use err_impl::Error as ErrorImpl;

mod err_impl {
    use super::*;
    #[derive(Debug)]
    pub(crate) enum Error {
        Io(std::io::Error),
        Parser(expression::ParseError),

        Eval(String),

        InvalidPage,
        DatetimeOverflow,
        InvalidBson,
        InvalidIndexKeyType,
        IndexKeySizeExceeded,
        DuplicatedIndexKey { index: String, key: Value },
        IndexAlreadyExists(String),
        InvalidFieldType { field: String, value: Value },
    }
}

impl Error {
    fn new(inner: ErrorImpl) -> Error {
        Error(Box::new(inner))
    }

    pub(crate) fn invalid_page() -> Error {
        Error::new(ErrorImpl::InvalidPage)
    }

    pub(crate) fn datetime_overflow() -> Error {
        Error::new(ErrorImpl::DatetimeOverflow)
    }

    pub(crate) fn invalid_bson() -> Error {
        Error::new(ErrorImpl::InvalidBson)
    }

    pub(crate) fn invalid_index_key_type() -> Error {
        Error::new(ErrorImpl::InvalidIndexKeyType)
    }

    pub(crate) fn index_key_too_long() -> Error {
        Error::new(ErrorImpl::IndexKeySizeExceeded)
    }

    pub(crate) fn index_duplicate_key(index: &str, key: Value) -> Error {
        Error::new(ErrorImpl::DuplicatedIndexKey {
            index: index.to_string(),
            key,
        })
    }

    pub(crate) fn index_already_exists(name: &str) -> Error {
        Error::new(ErrorImpl::IndexAlreadyExists(name.to_string()))
    }

    pub(crate) fn invalid_data_type(field: &str, value: &Value) -> Error {
        Error::new(ErrorImpl::InvalidFieldType {
            field: field.to_string(),
            value: value.clone(),
        })
    }

    pub(crate) fn expr_run_error(str: &str) -> Self {
        Self::new(ErrorImpl::Eval(format!("executing: {}", str)))
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::new(ErrorImpl::Io(err))
    }
}

impl From<bson::ParseError> for Error {
    fn from(_: bson::ParseError) -> Self {
        Error::new(ErrorImpl::InvalidBson)
    }
}

impl From<expression::ParseError> for Error {
    fn from(err: expression::ParseError) -> Self {
        Error::new(ErrorImpl::Parser(err))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            ErrorImpl::Io(e) => e.fmt(f),
            ErrorImpl::Parser(e) => e.fmt(f),
            ErrorImpl::Eval(e) => e.fmt(f),

            ErrorImpl::InvalidPage => f.write_str("Invalid page"),
            ErrorImpl::DatetimeOverflow => f.write_str("DateTime overflow"),
            ErrorImpl::InvalidBson => f.write_str("Invalid BSON"),
            ErrorImpl::InvalidIndexKeyType => f.write_str(
                "Invalid index key: Min/Max or Document Value are not supported as index key",
            ),
            ErrorImpl::IndexKeySizeExceeded => f.write_str("Invalid index key: Index key too long"),
            ErrorImpl::DuplicatedIndexKey { index, key } => write!(
                f,
                "Duplicate index key in unique index `{index}`, key: {key:?}"
            ),
            ErrorImpl::IndexAlreadyExists(name) => write!(f, "Index '{}' already exists", name),
            ErrorImpl::InvalidFieldType { field, value } => {
                write!(f, "Invalid field type: {field}, value: {value:?}")
            }
        }
    }
}

impl std::fmt::Debug for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <ErrorImpl as std::fmt::Debug>::fmt(&self.0, f)
    }
}

impl std::error::Error for Error {}

impl From<Error> for std::io::Error {
    fn from(value: Error) -> Self {
        use std::io::ErrorKind::*;
        let kind = match *value.0 {
            ErrorImpl::Io(e) => return e,
            ErrorImpl::Parser(_) => InvalidInput,
            ErrorImpl::Eval(_) => InvalidInput,
            ErrorImpl::InvalidPage => InvalidData,
            ErrorImpl::DatetimeOverflow => InvalidData,
            ErrorImpl::InvalidBson => InvalidData,
            ErrorImpl::InvalidIndexKeyType => InvalidData,
            ErrorImpl::IndexKeySizeExceeded => InvalidData,
            ErrorImpl::DuplicatedIndexKey { .. } => InvalidData,
            ErrorImpl::IndexAlreadyExists(_) => AlreadyExists,
            ErrorImpl::InvalidFieldType { .. } => InvalidInput,
        };

        std::io::Error::new(kind, value)
    }
}
