use crate::Result;
use crate::engine::disk::memory_cache::MemoryCache;
use crate::engine::{FileOrigin, PageBuffer, Stream};
use futures::io;
use futures::prelude::*;
use std::rc::Rc;

pub(crate) struct DiskReader<'a, S: Stream> {
    cache: &'a mut MemoryCache,
    data_stream: &'a mut S,
    log_stream: &'a mut S,
}

impl<'a, S: Stream> DiskReader<'a, S> {
    pub fn new(cache: &'a mut MemoryCache, data_stream: &'a mut S, log_stream: &'a mut S) -> Self {
        DiskReader {
            cache,
            data_stream,
            log_stream,
        }
    }

    pub async fn read_page(&mut self, position: u64, origin: FileOrigin) -> Result<Rc<PageBuffer>> {
        let stream = match origin {
            FileOrigin::Data => &mut self.data_stream,
            FileOrigin::Log => &mut self.log_stream,
        };
        self.cache
            .get_readable_page(position, origin, async |pos, buf| {
                Self::read_stream(stream, pos, buf).await
            })
            .await
    }

    pub async fn read_writable_page(
        &mut self,
        position: u64,
        origin: FileOrigin,
    ) -> Result<Box<PageBuffer>> {
        let stream = match origin {
            FileOrigin::Data => &mut self.data_stream,
            FileOrigin::Log => &mut self.log_stream,
        };
        self.cache
            .get_writable_page(position, origin, async |pos, buf| {
                Self::read_stream(stream, pos, buf).await
            })
            .await
    }

    pub fn new_page(&mut self) -> Box<PageBuffer> {
        self.cache.new_page()
    }

    async fn read_stream(stream: &mut S, position: u64, buf: &mut [u8]) -> Result<()> {
        stream.seek(io::SeekFrom::Start(position)).await?;
        stream.read_exact(buf).await?;
        Ok(())
    }
}
