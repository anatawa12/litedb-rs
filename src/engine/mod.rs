#[macro_use]
mod macros;

mod buffer_reader;
mod buffer_writer;
mod collection_index;
mod collection_service;
mod constants;
mod data_block;
mod disk;
mod engine_pragmas;
mod index_node;
mod index_service;
mod lite_engine;
mod lock_service;
mod page_address;
mod page_buffer;
mod page_position;
mod pages;
mod snapshot;
mod sort_disk;
mod transaction_monitor;
mod transaction_pages;
mod transaction_service;
mod wal_index_service;

pub(crate) use super::Result;
pub(crate) use buffer_reader::*;
pub(crate) use buffer_writer::*;
pub(crate) use constants::*;
use futures::{AsyncRead, AsyncSeek, AsyncWrite};
pub(crate) use page_address::*;
pub(crate) use page_buffer::*;
pub(crate) use pages::*;
use std::pin::Pin;
pub(crate) type PageBufferArray = [u8; PAGE_SIZE];

// public uses
pub use lite_engine::*;

pub trait Stream: AsyncRead + AsyncWrite + AsyncSeek + Unpin + Send {
    // Should we use poll method instead?
    fn set_len(&self, len: u64) -> Pin<Box<dyn Future<Output = Result<()>> + '_>>;
}

#[allow(clippy::len_without_is_empty)]
pub trait StreamFactory: Send + Sync {
    #[allow(clippy::type_complexity)]
    fn get_stream(
        &self,
        writable: bool,
    ) -> Pin<Box<dyn Future<Output = Result<Box<dyn Stream>>> + '_>>;
    fn exists(&self) -> Pin<Box<dyn Future<Output = bool> + '_>>;
    fn len(&self) -> Pin<Box<dyn Future<Output = Result<u64>> + '_>>;
    fn delete(&self) -> Pin<Box<dyn Future<Output = Result<()>> + '_>>;
}

#[cfg(feature = "tokio-fs")]
impl Stream for tokio_util::compat::Compat<tokio::fs::File> {
    fn set_len(&self, len: u64) -> Pin<Box<dyn Future<Output = Result<()>> + '_>> {
        Box::pin(async move { Ok(self.get_ref().set_len(len).await?) })
    }
}
