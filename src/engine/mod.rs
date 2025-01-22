mod disk;
mod page_buffer;
mod constants;

use futures::{AsyncSeek, AsyncWrite};
use futures::io::AsyncRead;

pub(crate) use page_buffer::*;
pub(crate) use constants::*;
pub(crate) use super::{Result, Error};
pub(crate) type PageBufferArray = [u8; PAGE_SIZE];

pub(crate) trait Stream: AsyncRead + AsyncWrite + AsyncSeek + Unpin {
}

impl<T: AsyncRead + AsyncWrite + AsyncSeek + Unpin> Stream for T {
}
