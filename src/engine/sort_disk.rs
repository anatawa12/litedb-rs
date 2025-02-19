use crate::Result;
use crate::engine::disk::{StreamGuard, StreamPool};
use crate::engine::{PAGE_SIZE, StreamFactory};
use futures::prelude::*;
use std::collections::HashSet;
use std::io::SeekFrom;

pub(crate) struct SortDisk {
    pool: StreamPool,
    free_positions: HashSet<i64>,
    last_container_position: i64,
    container_size: usize,
}

impl SortDisk {
    pub fn new(temp_stream: Box<dyn StreamFactory>, container_size: usize) -> Self {
        SortDisk {
            pool: StreamPool::new(temp_stream),
            container_size,
            last_container_position: 0,
            free_positions: HashSet::new(),
        }
    }

    pub fn container_size(&self) -> usize {
        self.container_size
    }

    pub async fn get_reader(&self) -> Result<StreamGuard> {
        self.pool.rent().await
    }

    pub fn get_container_position(&mut self) -> u64 {
        if let Some(&position) = self.free_positions.iter().next() {
            self.free_positions.remove(&position);
            position as u64
        } else {
            self.last_container_position += self.container_size as i64;
            self.last_container_position as u64
        }
    }

    pub async fn write(&mut self, position: u64, data: &[u8]) -> Result<()> {
        let writer = self.pool.writeable_mut().await?;

        for i in 0..(self.container_size / PAGE_SIZE) {
            writer
                .seek(SeekFrom::Start(position + i as u64 * PAGE_SIZE as u64))
                .await?;
            writer
                .write_all(&data[i * PAGE_SIZE..][..PAGE_SIZE])
                .await?;
        }

        Ok(())
    }
}
