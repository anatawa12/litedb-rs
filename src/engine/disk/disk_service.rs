use super::memory_cache::MemoryCache;
use crate::Result;
use crate::engine::FileStream;
use crate::engine::disk::disk_reader::DiskReader;
use crate::engine::disk::stream_pool::StreamPool;
use crate::engine::page_position::PagePosition;
use crate::engine::pages::HeaderPage;
use crate::engine::*;
use crate::utils::Collation;
use async_lock::Mutex;
use futures::prelude::*;
use std::cmp::max;
use std::io::SeekFrom;
use std::sync::atomic::AtomicI64;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};

pub(crate) struct DiskService {
    cache: MemoryCache,
    data_pool: StreamPool,
    log_pool: StreamPool,
    log_lock: Mutex<()>,
    data_length: AtomicI64,
    log_length: AtomicI64,
}

impl DiskService {
    pub async fn new(
        data_stream: Box<dyn StreamFactory>,
        log_stream: Box<dyn StreamFactory>,
        collation: Option<Collation>,
    ) -> Result<Self> {
        let mut disk_service = DiskService {
            cache: MemoryCache::new(),
            data_pool: StreamPool::new(data_stream),
            log_pool: StreamPool::new(log_stream),
            log_lock: Mutex::new(()),
            data_length: AtomicI64::new(0),
            log_length: AtomicI64::new(0),
        };

        if disk_service.data_pool.factory().len().await? == 0 {
            Self::initialize(disk_service.data_pool.writeable_mut().await?, collation).await?;
        }

        disk_service.data_length.store(
            disk_service.data_pool.factory().len().await? as i64 - PAGE_SIZE as i64,
            Relaxed,
        );

        if disk_service.log_pool.factory().exists().await {
            disk_service.log_length.store(
                disk_service.log_pool.factory().len().await? as i64 - PAGE_SIZE as i64,
                Relaxed,
            );
        } else {
            disk_service.log_length.store(-(PAGE_SIZE as i64), Relaxed)
        }

        Ok(disk_service)
    }

    pub fn cache(&self) -> &MemoryCache {
        &self.cache
    }

    async fn initialize(stream: &mut dyn FileStream, collation: Option<Collation>) -> Result<()> {
        let collation = collation.unwrap_or_default();

        let buffer = Box::new(PageBuffer::new(0));
        let mut header = HeaderPage::new(buffer);

        header.pragmas().set_collation(collation);

        header.update_buffer();

        stream.write_all(header.as_mut().buffer().buffer()).await?;

        // initial size

        stream.flush().await?;
        Ok(())
    }

    pub fn get_reader(&self) -> DiskReader {
        DiskReader::new(&self.cache, &self.data_pool, &self.log_pool)
    }

    pub fn max_items_count(&self) -> u32 {
        ((self.data_length.load(Relaxed) + self.log_length.load(Relaxed) / PAGE_SIZE as i64 + 10)
            * u8::MAX as i64) as u32
    }

    pub fn new_page(&self) -> Box<PageBuffer> {
        self.cache.new_page()
    }

    pub(crate) fn discard_dirty_pages(&self, pages: Vec<Box<PageBuffer>>) {
        // no reusing buffer in rust impl for now
        // only for ROLLBACK action
        for page in pages {
            // complete discard page and content
            // no page reuse
            drop(page)
            //self.cache.discard_page(page);
        }
    }

    pub(crate) fn discard_clean_pages(&self, pages: Vec<Box<PageBuffer>>) {
        // no reusing buffer in rust impl for now
        for page in pages {
            if let Ok(page) = self.cache.try_move_to_readable(page) {
                // no page reuse
                drop(page)
                // self.cache.discard_page(page)
            }
        }
    }

    pub(crate) async fn write_log_disk(
        &self,
        pages: Vec<(u32, Box<PageBuffer>)>,
        mut new_page_location: impl FnMut(PagePosition),
    ) -> Result<usize> {
        let mut count = 0;
        let mut stream = self.log_pool.writeable().await?;
        let _log_write_lock = self.log_lock.lock().await;

        // lock on stream
        for (page_id, mut page) in pages {
            let new_length =
                self.log_length.fetch_add(PAGE_SIZE as i64, Relaxed) + PAGE_SIZE as i64;
            page.set_position_origin(new_length as u64, FileOrigin::Log);

            let page = self.cache.move_to_readable(page);

            stream.seek(SeekFrom::Start(page.position())).await?;

            stream.write_all(page.buffer()).await?;

            count += 1;

            new_page_location(PagePosition::new(page_id, new_length as u64));
        }

        Ok(count)
    }

    pub fn get_file_length(&self, origin: FileOrigin) -> i64 {
        match origin {
            FileOrigin::Data => self.data_length.load(Relaxed) + PAGE_SIZE as i64,
            FileOrigin::Log => self.log_length.load(Relaxed) + PAGE_SIZE as i64,
        }
    }

    pub fn read_full(
        &self,
        origin: FileOrigin,
    ) -> impl futures::Stream<Item = Result<Box<PageBuffer>>> {
        async_stream::try_stream! {
            let pool = if origin == FileOrigin::Log { &self.log_pool } else { &self.data_pool };
            let mut stream = pool.rent().await?;

            let length = self.get_file_length(origin);

            stream.seek(SeekFrom::Start(0)).await?;
            let mut position = 0;

            while position < length {
                let mut buffer = Box::new(PageBuffer::new(0));
                buffer.set_position_origin(position as u64, origin);
                stream.read_exact(buffer.buffer_mut()).await?;
                position += PAGE_SIZE as i64;

                yield buffer
            }
        }
    }

    /// This method must be externally mutably excluded
    pub(crate) async fn write_data_disk(&self, pages: &[Box<PageBuffer>]) -> Result<()> {
        let mut stream = self.data_pool.writeable().await?;

        for page in pages {
            self.data_length.store(
                max(self.data_length.load(Relaxed), page.position() as i64),
                Relaxed,
            );

            stream.seek(SeekFrom::Start(page.position())).await?;
            stream.write_all(page.buffer()).await?;
        }

        stream.flush().await?;

        Ok(())
    }

    pub(crate) async fn set_length(&self, size: u64, origin: FileOrigin) -> Result<()> {
        match origin {
            FileOrigin::Data => {
                self.data_length
                    .store(size as i64 - PAGE_SIZE as i64, SeqCst);
                self.data_pool.writeable().await?.set_len(size).await?;
            }
            FileOrigin::Log => {
                self.log_length
                    .store(size as i64 - PAGE_SIZE as i64, SeqCst);
                self.log_pool.writeable().await?.set_len(size).await?;
            }
        }
        Ok(())
    }

    pub(crate) async fn dispose(self) {
        let to_remove = self.log_pool.factory().exists().await
            && self.log_pool.factory().len().await.ok() == Some(0);
        if to_remove {
            let _ = self.log_pool.factory().delete().await;
        }
    }
}
