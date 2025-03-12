//! # LiteDB in Rust
//! This is a reimplementation of [LiteDB] in Rust.
//!
//! This implementation (currently?) only supports single-threaded operation.
//!
//! [LiteDB]: <https://www.litedb.org/>

#![allow(clippy::too_many_arguments)]

use crate::bson::Value;
use crate::engine::{BasePage, PageBufferRef, PageType};
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
mod file_io;

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

        InvalidDatabase,
        InvalidPage,
        DatetimeOverflow,
        Encrypted,
        CollationMismatch,
        InvalidPageType {
            expected: PageType,
            actual: PageType,
            page: u32,
        },
        CollectionIndexLimitReached,
        CollectionNameHeaderSpaceExceeds(String),
        InvalidCollectionName(String),
        InvalidBson,
        SizeLimitReached,
        TransactionLimitReached,
        InvalidIndexKeyType,
        IndexKeySizeExceeded,
        DuplicatedIndexKey {
            index: String,
            key: Value,
        },
        CollectionNameAlreadyExists(String),
        DocumentSizeLimitExceeded,
        IndexAlreadyExists(String),
        DropIdIndex,
        BadAutoId {
            auto_id: engine::BsonAutoId,
            collection_name: String,
            last_id: Value,
        },
        InvalidFieldType {
            field: String,
            value: Value,
        },
    }
}

impl Error {
    fn new(inner: ErrorImpl) -> Error {
        Error(Box::new(inner))
    }

    pub(crate) fn invalid_database() -> Error {
        Error::new(ErrorImpl::InvalidDatabase)
    }

    pub(crate) fn invalid_page() -> Error {
        Error::new(ErrorImpl::InvalidPage)
    }

    pub(crate) fn datetime_overflow() -> Error {
        Error::new(ErrorImpl::DatetimeOverflow)
    }

    pub(crate) fn encrypted_no_password() -> Error {
        Error::new(ErrorImpl::Encrypted)
    }

    pub(crate) fn collation_not_match() -> Error {
        Error::new(ErrorImpl::CollationMismatch)
    }

    pub(crate) fn invalid_page_type<Buffer: PageBufferRef>(
        expected: PageType,
        page: BasePage<Buffer>,
    ) -> Error {
        Error::new(ErrorImpl::InvalidPageType {
            expected,
            actual: page.page_type(),
            page: page.page_id(),
        })
    }

    pub(crate) fn collection_index_limit_reached() -> Error {
        Error::new(ErrorImpl::CollectionIndexLimitReached)
    }

    pub(crate) fn name_length_header_space(name: &str) -> Error {
        Error::new(ErrorImpl::CollectionNameHeaderSpaceExceeds(
            name.to_string(),
        ))
    }

    pub(crate) fn invalid_collection_name(name: &str) -> Error {
        Error::new(ErrorImpl::InvalidCollectionName(name.to_string()))
    }

    pub(crate) fn invalid_bson() -> Error {
        Error::new(ErrorImpl::InvalidBson)
    }

    pub(crate) fn size_limit_reached() -> Self {
        Error::new(ErrorImpl::SizeLimitReached)
    }

    pub(crate) fn transaction_limit() -> Error {
        Error::new(ErrorImpl::TransactionLimitReached)
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

    pub(crate) fn already_exists_collection_name(name: &str) -> Error {
        Error::new(ErrorImpl::CollectionNameAlreadyExists(name.to_string()))
    }

    pub(crate) fn document_size_exceed_limit() -> Self {
        Error::new(ErrorImpl::DocumentSizeLimitExceeded)
    }

    pub(crate) fn index_already_exists(name: &str) -> Error {
        Error::new(ErrorImpl::IndexAlreadyExists(name.to_string()))
    }

    pub(crate) fn drop_id_index() -> Error {
        Error::new(ErrorImpl::DropIdIndex)
    }

    #[allow(dead_code)]
    pub(crate) fn bad_auto_id(
        auto_id: engine::BsonAutoId,
        collection_name: &str,
        last_id: Value,
    ) -> Self {
        Error::new(ErrorImpl::BadAutoId {
            auto_id,
            collection_name: collection_name.to_string(),
            last_id,
        })
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

            ErrorImpl::InvalidDatabase => f.write_str("Invalid database"),
            ErrorImpl::InvalidPage => f.write_str("Invalid page"),
            ErrorImpl::DatetimeOverflow => f.write_str("DateTime overflow"),
            ErrorImpl::Encrypted => f.write_str("Encrypted database without password"),
            ErrorImpl::CollationMismatch => f.write_str("Collation not match"),
            ErrorImpl::InvalidPageType {
                expected,
                actual,
                page,
            } => write!(
                f,
                "Invalid page type: expected {:?}, got {:?} at page {}",
                expected, actual, page
            ),
            ErrorImpl::CollectionIndexLimitReached => f.write_str("Collection index limit reached"),
            ErrorImpl::CollectionNameHeaderSpaceExceeds(name) => write!(
                f,
                "Collection name length exceeds available header space: {}",
                name
            ),
            ErrorImpl::InvalidCollectionName(name) => {
                write!(f, "Invalid collection name: {}", name)
            }
            ErrorImpl::InvalidBson => f.write_str("Invalid BSON"),
            ErrorImpl::SizeLimitReached => f.write_str("Size limit reached"),
            ErrorImpl::TransactionLimitReached => {
                f.write_str("Maximum number of transactions reached")
            }
            ErrorImpl::InvalidIndexKeyType => f.write_str(
                "Invalid index key: Min/Max or Document Value are not supported as index key",
            ),
            ErrorImpl::IndexKeySizeExceeded => f.write_str("Invalid index key: Index key too long"),
            ErrorImpl::DuplicatedIndexKey { index, key } => write!(
                f,
                "Duplicate index key in unique index `{index}`, key: {key:?}"
            ),
            ErrorImpl::CollectionNameAlreadyExists(name) => {
                write!(f, "Collection name '{}' already exists", name)
            }
            ErrorImpl::DocumentSizeLimitExceeded => f.write_str("Document size limit reached"),
            ErrorImpl::IndexAlreadyExists(name) => write!(f, "Index '{}' already exists", name),
            ErrorImpl::DropIdIndex => f.write_str("Drop _id index"),
            ErrorImpl::BadAutoId {
                auto_id,
                collection_name,
                last_id,
            } => write!(
                f,
                "It's not possible use AutoId={auto_id:?} because '{collection_name}' collection contains not only numbers in _id index ({last_id:?})."
            ),
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
            ErrorImpl::InvalidDatabase => InvalidData,
            ErrorImpl::InvalidPage => InvalidData,
            ErrorImpl::DatetimeOverflow => InvalidData,
            ErrorImpl::Encrypted => InvalidInput,
            ErrorImpl::CollationMismatch => InvalidInput,
            ErrorImpl::InvalidPageType { .. } => InvalidInput,
            ErrorImpl::CollectionIndexLimitReached => InvalidInput,
            ErrorImpl::CollectionNameHeaderSpaceExceeds(_) => InvalidInput,
            ErrorImpl::InvalidCollectionName(_) => InvalidInput,
            ErrorImpl::InvalidBson => InvalidData,
            ErrorImpl::SizeLimitReached => InvalidInput,
            ErrorImpl::TransactionLimitReached => QuotaExceeded,
            ErrorImpl::InvalidIndexKeyType => InvalidData,
            ErrorImpl::IndexKeySizeExceeded => InvalidData,
            ErrorImpl::DuplicatedIndexKey { .. } => InvalidData,
            ErrorImpl::CollectionNameAlreadyExists(_) => InvalidInput,
            ErrorImpl::DocumentSizeLimitExceeded => InvalidData,
            ErrorImpl::IndexAlreadyExists(_) => AlreadyExists,
            ErrorImpl::DropIdIndex => PermissionDenied,
            ErrorImpl::BadAutoId { .. } => InvalidData,
            ErrorImpl::InvalidFieldType { .. } => InvalidInput,
        };

        std::io::Error::new(kind, value)
    }
}
