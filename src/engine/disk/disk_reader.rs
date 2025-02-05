use crate::Result;
use crate::engine::disk::memory_cache::MemoryCache;
use crate::engine::disk::stream_pool::{StreamGuard, StreamPool};
use crate::engine::{FileOrigin, PageBuffer, Stream};
use futures::io;
use futures::prelude::*;
use std::cell::OnceCell;
use std::ops::DerefMut;
use std::rc::Rc;

pub(crate) struct DiskReader<'a> {
    cache: &'a MemoryCache,
    streams: StreamHolder<'a>,
}

struct StreamHolder<'a> {
    data_pool: &'a StreamPool,
    log_pool: &'a StreamPool,
    data_stream: OnceCell<StreamGuard<'a>>,
    log_stream: OnceCell<StreamGuard<'a>>,
}

impl StreamHolder<'_> {
    async fn get_stream(&mut self, origin: FileOrigin) -> Result<&mut dyn Stream> {
        async fn inner<'a, 'b>(
            pool: &'b StreamPool,
            cell: &'a mut OnceCell<StreamGuard<'b>>,
        ) -> Result<&'a mut dyn Stream> {
            if cell.get_mut().is_none() {
                let stream = pool.rent().await?;
                cell.set(stream).ok().unwrap();
            }
            Ok(StreamGuard::deref_mut(cell.get_mut().unwrap()))
        }

        match origin {
            FileOrigin::Data => inner(self.data_pool, &mut self.data_stream).await,
            FileOrigin::Log => inner(self.log_pool, &mut self.log_stream).await,
        }
    }
}

impl<'a> DiskReader<'a> {
    pub fn new(
        cache: &'a MemoryCache,
        data_pool: &'a StreamPool,
        log_pool: &'a StreamPool,
    ) -> Self {
        DiskReader {
            cache,
            streams: StreamHolder {
                data_pool,
                log_pool,
                data_stream: OnceCell::new(),
                log_stream: OnceCell::new(),
            },
        }
    }

    pub async fn read_page(&mut self, position: u64, origin: FileOrigin) -> Result<Rc<PageBuffer>> {
        let stream = self.streams.get_stream(origin).await?;
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
        let stream = self.streams.get_stream(origin).await?;
        self.cache
            .get_writable_page(position, origin, async |pos, buf| {
                Self::read_stream(stream, pos, buf).await
            })
            .await
    }

    pub fn new_page(&mut self) -> Box<PageBuffer> {
        self.cache.new_page()
    }

    async fn read_stream(stream: &mut dyn Stream, position: u64, buf: &mut [u8]) -> Result<()> {
        stream.seek(io::SeekFrom::Start(position)).await?;
        stream.read_exact(buf).await?;
        Ok(())
    }
}
