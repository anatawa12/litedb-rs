use crate::engine::disk::disk_reader::DiskReader;
use crate::engine::Stream;
use super::memory_cache::MemoryCache;

pub(crate) struct DiskService<S : Stream> {
    cache: MemoryCache,
    data_stream: S,
    log_stream: S,
}

impl<S:Stream> DiskService<S> {
    pub fn new() -> Self {
        todo!()
    }

    pub fn get_reader(&mut self) -> DiskReader<S> {
        DiskReader::new(&mut self.cache, &mut self.data_stream, &mut self.log_stream)
    }
}
