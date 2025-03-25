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

pub type Result<T> = std::result::Result<T, Error>;
type ParseResult<T> = std::result::Result<T, ParseError>;

pub struct Error(Box<ErrorImpl>);

use err_impl::Error as ErrorImpl;
use err_impl::ParseError as ParseErrorImpl;

mod err_impl {
    use super::*;
    #[derive(Debug)]
    pub(crate) enum Error {
        Eval(String),

        InvalidIndexKeyType,
        IndexKeySizeExceeded,
        DuplicatedIndexKey { index: String, key: Value },
        IndexAlreadyExists(String),
        InvalidFieldType { field: String, value: Value },
    }

    #[derive(Debug)]
    pub(crate) enum ParseError {
        InvalidDatabase,
        InvalidPage(u32),
        InvalidBson,
        BadReference,
        NoIdIndex,
        Expression(expression::ParseError),
    }
}

impl Error {
    fn new(inner: ErrorImpl) -> Error {
        Error(Box::new(inner))
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

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            ErrorImpl::Eval(e) => e.fmt(f),

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

pub struct ParseError(Box<ParseErrorImpl>);

impl ParseError {
    fn invalid_database() -> Self {
        Self::new(ParseErrorImpl::InvalidDatabase)
    }

    fn invalid_page(id: u32) -> Self {
        Self::new(ParseErrorImpl::InvalidPage(id))
    }

    fn bad_reference() -> Self {
        Self::new(ParseErrorImpl::BadReference)
    }

    fn no_id_index() -> Self {
        Self::new(ParseErrorImpl::NoIdIndex)
    }

    fn invalid_bson() -> Self {
        Self::new(ParseErrorImpl::InvalidBson)
    }

    fn new(inner: ParseErrorImpl) -> ParseError {
        Self(Box::new(inner))
    }
}

impl From<expression::ParseError> for ParseError {
    fn from(value: expression::ParseError) -> Self {
        Self::new(ParseErrorImpl::Expression(value))
    }
}

impl From<bson::ParseError> for ParseError {
    fn from(_: bson::ParseError) -> Self {
        Self::invalid_bson()
    }
}

impl From<ParseError> for std::io::Error {
    fn from(value: ParseError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, value)
    }
}

impl std::error::Error for ParseError {}

impl std::fmt::Debug for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <ParseErrorImpl as std::fmt::Debug>::fmt(&self.0, f)
    }
}

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0.as_ref() {
            ParseErrorImpl::InvalidDatabase => write!(f, "Invalid database"),
            ParseErrorImpl::InvalidPage(id) => write!(f, "Invalid page at {id}"),
            ParseErrorImpl::InvalidBson => write!(f, "Invalid BSON"),
            ParseErrorImpl::BadReference => write!(f, "Bad reference"),
            ParseErrorImpl::NoIdIndex => write!(f, "No _id index found for collection"),
            ParseErrorImpl::Expression(inner) => Display::fmt(inner, f),
        }
    }
}
