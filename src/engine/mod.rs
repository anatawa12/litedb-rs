mod disk;
mod page_buffer;
mod constants;
mod pages;
mod engine_pragmas;
mod buffer_reader;
mod buffer_writer;
mod lite_engine;
mod lock_service;

use futures::{AsyncSeek, AsyncWrite, AsyncRead};

pub(crate) use page_buffer::*;
pub(crate) use constants::*;
pub(crate) use super::{Result, Error};
pub(crate) type PageBufferArray = [u8; PAGE_SIZE];

pub(crate) trait Stream: AsyncRead + AsyncWrite + AsyncSeek + Unpin {
}

impl<T: AsyncRead + AsyncWrite + AsyncSeek + Unpin> Stream for T {
}

pub(crate) trait StreamFactory {
    type Stream: Stream;
    fn get_stream(&self) -> Box<dyn Future<Output = Result<&mut Self::Stream>> + Unpin>;
    fn exists(&self) -> Box<dyn Future<Output = bool> + Unpin>;
    fn len(&self) -> Box<dyn Future<Output = Result<i64>> + Unpin>;
}
