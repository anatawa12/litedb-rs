use futures::AsyncWriteExt;
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
    ) -> Result<Self> {
        let mut disk_service = DiskService {
            cache: MemoryCache::new(),
            data_stream,
            log_stream,
            data_length: 0,
            log_length: 0,
        };

        if disk_service.data_stream.len().await? == 0 {
            disk_service.initialize().await?;
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

    async fn initialize(&mut self) -> Result<()> {
        let stream = self.data_stream.get_stream().await?;
        let collation = Collation::default(); // TODO: specify collation from settings
        //let initial_size = 0;

        let buffer = PageBuffer::new();
        let mut header = HeaderPage::new(buffer);

        header.pragmas.collation = collation;
        header.base.dirty = true;

        header.update_buffer()?;

        stream.write_all(header.base.buffer.buffer()).await?;

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
}
