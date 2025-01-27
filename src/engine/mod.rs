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

use futures::{AsyncRead, AsyncSeek, AsyncWrite};

pub(crate) use super::Result;
pub(crate) use buffer_reader::*;
pub(crate) use buffer_writer::*;
pub(crate) use constants::*;
pub(crate) use page_address::*;
pub(crate) use page_buffer::*;
pub(crate) use pages::*;
pub(crate) type PageBufferArray = [u8; PAGE_SIZE];

pub(crate) trait Stream: AsyncRead + AsyncWrite + AsyncSeek + Unpin {}

impl<T: AsyncRead + AsyncWrite + AsyncSeek + Unpin> Stream for T {}

pub(crate) trait StreamFactory {
    type Stream: Stream;
    fn get_stream(&self) -> Box<dyn Future<Output = Result<&mut Self::Stream>> + Unpin>;
    fn exists(&self) -> Box<dyn Future<Output = bool> + Unpin>;
    fn len(&self) -> Box<dyn Future<Output = Result<i64>> + Unpin>;
    fn set_len(&self, len: i64) -> Box<dyn Future<Output = Result<()>> + Unpin>;
}
