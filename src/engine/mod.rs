#[macro_use]
mod macros;

mod collection_index;
mod collection_service;
mod data_block;
pub(crate) mod data_service;
mod disk;
mod engine_pragmas;
mod index_node;
mod index_service;
mod lite_engine;
mod lock_service;
mod page_buffer;
mod page_position;
mod pages;
mod snapshot;
mod transaction_monitor;
mod transaction_pages;
mod transaction_service;
pub(crate) mod utils;
mod wal_index_service;

pub(crate) use super::Result;
pub(crate) use crate::buffer_reader::*;
pub(crate) use crate::buffer_writer::*;
pub(crate) use crate::constants::*;
use futures::{AsyncRead, AsyncSeek, AsyncWrite};
pub(crate) use index_node::*;
pub(crate) use page_buffer::*;
pub(crate) use pages::*;
use std::pin::Pin;
pub(crate) type PageBufferArray = [u8; PAGE_SIZE];

// public uses
pub use lite_engine::*;

pub trait FileStream: AsyncRead + AsyncWrite + AsyncSeek + Unpin + Send + Sync {
    // Should we use poll method instead?
    fn set_len(&self, len: u64) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + '_>>;
}

#[allow(clippy::len_without_is_empty)]
pub trait StreamFactory: Send + Sync {
    #[allow(clippy::type_complexity)]
    fn get_stream(
        &self,
        writable: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn FileStream>>> + Send + Sync + '_>>;
    fn exists(&self) -> Pin<Box<dyn Future<Output = bool> + Send + Sync + '_>>;
    fn len(&self) -> Pin<Box<dyn Future<Output = Result<u64>> + Send + Sync + '_>>;
    fn delete(&self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + Sync + '_>>;
}
