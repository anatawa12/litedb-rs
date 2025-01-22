use std::cmp::max;
use std::io::SeekFrom;
use futures::prelude::*;
use super::memory_cache::MemoryCache;
use crate::engine::disk::disk_reader::DiskReader;
use crate::engine::*;
use crate::engine::pages::HeaderPage;
use crate::Result;
use crate::utils::Collation;

pub(crate) struct DiskService<SF : StreamFactory> {
    cache: MemoryCache,
    data_stream: SF,
    log_stream: SF,
    data_length: i64,
    log_length: i64,
}

impl<SF: StreamFactory> DiskService<SF> {
    pub async fn new(
        data_stream: SF,
        log_stream: SF,
        collation: Option<Collation>,
    ) -> Result<Self> {
        let mut disk_service = DiskService {
            cache: MemoryCache::new(),
            data_stream,
            log_stream,
            data_length: 0,
            log_length: 0,
        };

        if disk_service.data_stream.len().await? == 0 {
            disk_service.initialize(collation).await?;
        }

        disk_service.data_length = disk_service.data_stream.len().await? - PAGE_SIZE as i64;

        if disk_service.log_stream.exists().await {
            disk_service.log_length = disk_service.log_stream.len().await? - PAGE_SIZE as i64;
        } else {
            disk_service.log_length = -(PAGE_SIZE as i64);
        }

        Ok(disk_service)
    }

    pub fn cache(&self) -> &MemoryCache {
        &self.cache
    }

    async fn initialize(&mut self, collation: Option<Collation>) -> Result<()> {
        let stream = self.data_stream.get_stream().await?;
        let collation = collation.unwrap_or_default();
        //let initial_size = 0;

        let buffer = Box::new(PageBuffer::new());
        let mut header = HeaderPage::new(buffer);

        header.pragmas().set_collation(collation);

        header.update_buffer()?;

        stream.write_all(header.buffer().buffer()).await?;

        // initial size
        stream.flush().await?;
        Ok(())
    }

    pub fn cache_mut(&mut self) -> &mut MemoryCache {
        &mut self.cache
    }

    pub async fn get_reader(&mut self) -> Result<DiskReader<SF::Stream>> {
        Ok(DiskReader::new(&mut self.cache, self.data_stream.get_stream().await?, self.log_stream.get_stream().await?))
    }

    pub fn get_file_length(&self, origin: FileOrigin) -> i64 {
        match origin {
            FileOrigin::Data => self.data_length + PAGE_SIZE as i64,
            FileOrigin::Log => self.log_length + PAGE_SIZE as i64,
        }
    }

    pub fn read_full(&mut self, origin: FileOrigin) -> impl futures::Stream<Item = Result<Box<PageBuffer>>> {
        futures::stream::try_unfold((self, 0, origin), async |(this, mut position, origin)| {
            let length = this.get_file_length(origin);
            let stream = this.data_stream.get_stream().await?;

            if position >= length {
                return Ok(None);
            }

            let mut buffer = Box::new(PageBuffer::new());
            buffer.set_position_origin(position as u64, origin);
            stream.read_exact(buffer.buffer_mut()).await?;

            position += PAGE_SIZE as i64;

            Ok(Some((buffer, (this, position, origin))))
        })
    }

    pub(crate) async fn write_data_disk(&mut self, pages: &[PageBuffer]) -> Result<()> {
        let stream = self.data_stream.get_stream().await?;

        for page in pages {
            self.data_length = max(self.data_length, page.position() as i64);

            stream.seek(SeekFrom::Start(page.position())).await?;
            stream.write_all(page.buffer()).await?;
        }

        stream.flush().await?;

        Ok(())
    }

    pub(crate) async fn set_length(&mut self, size: i64, origin: FileOrigin) -> Result<()> {
        match origin {
            FileOrigin::Data => {
                self.data_length = size - PAGE_SIZE as i64;
                self.data_stream.set_len(size).await?;
            }
            FileOrigin::Log => {
                self.log_length = size - PAGE_SIZE as i64;
                self.log_stream.set_len(size).await?;
            }
        }
        Ok(())
    }
}
