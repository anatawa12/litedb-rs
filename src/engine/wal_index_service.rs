use crate::Result;
use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::page_position::PagePosition;
use crate::engine::pages::{BasePage, HeaderPage, PageType};
use crate::engine::{FileOrigin, PAGE_SIZE, StreamFactory};
use futures::prelude::*;
use std::collections::{HashMap, HashSet};
use std::pin::pin;

pub(crate) struct WalIndexService {
    current_read_version: i32,
    last_transaction_id: i32,
    index: HashMap<u32, Vec<(i32, u64)>>,
    confirm_transactions: HashSet<u32>,
}

impl WalIndexService {
    pub(crate) fn new() -> Self {
        Self {
            current_read_version: 0,
            last_transaction_id: 0,
            index: HashMap::new(),
            confirm_transactions: HashSet::new(),
        }
    }

    pub fn current_read_version(&self) -> i32 {
        self.current_read_version
    }

    pub fn last_transaction_id(&self) -> i32 {
        self.last_transaction_id
    }

    pub async fn clear(&mut self, disk: &mut DiskService<impl StreamFactory>) -> Result<()> {
        self.confirm_transactions.clear();
        self.index.clear();

        self.current_read_version = 0;
        self.last_transaction_id = 0;

        disk.cache_mut().clear();
        disk.set_length(0, FileOrigin::Log).await?;

        Ok(())
    }

    pub fn next_transaction_id(&mut self) -> u32 {
        self.last_transaction_id += 1;
        self.last_transaction_id as u32
    }

    pub fn get_page_index(&mut self, page_id: u32, version: i32) -> (i32, u64) {
        if version == 0 {
            return (0, u64::MAX);
        }

        if let Some(index) = self.index.get(&page_id) {
            for (wal_version, position) in index.iter().rev() {
                if *wal_version <= version {
                    return (*wal_version, *position);
                }
            }

            return (version, u64::MAX);
        }
        (i32::MAX, u64::MAX)
    }

    pub fn confirm_transaction(&mut self, transaction_id: u32, positions: &[PagePosition]) {
        self.current_read_version += 1;

        for pos in positions {
            let slot = self.index.entry(pos.page_id()).or_default();
            slot.push((self.current_read_version, pos.position()));
        }

        self.confirm_transactions.insert(transaction_id);
    }

    pub async fn restore_index(
        &mut self,
        header: &mut HeaderPage,
        disk: &mut DiskService<impl StreamFactory>,
    ) -> Result<()> {
        let mut positions = HashMap::<i64, Vec<PagePosition>>::new();
        let mut current = 0;

        let mut read_stream = pin!(disk.read_full(FileOrigin::Log));
        while let Some(buffer) = read_stream.try_next().await? {
            if buffer.is_blank() {
                current += PAGE_SIZE;
                continue;
            }

            let page_id = buffer.read_u32(BasePage::P_PAGE_ID);
            let is_confirmed = buffer.read_bool(BasePage::P_IS_CONFIRMED);
            let transaction_id = buffer.read_u32(BasePage::P_TRANSACTION_ID);

            let position = PagePosition::new(page_id, current as u64);

            let positions_for_transaction = positions.entry(transaction_id as i64).or_default();
            positions_for_transaction.push(position);

            if is_confirmed {
                self.confirm_transaction(transaction_id, positions_for_transaction);

                let page_type = buffer.read_byte(BasePage::P_PAGE_TYPE);

                // when a header is modified in transaction, must always be the last page inside log file (per transaction)
                if page_type == PageType::Header as u8 {
                    let header_buffer = header.buffer_mut();

                    *header_buffer.buffer_mut() = *buffer.buffer();

                    // reload header
                    header.reload_fully()?;
                    header.set_transaction_id(u32::MAX);
                    header.set_confirmed(false);
                }
            }

            self.last_transaction_id = transaction_id as i32;
            current += PAGE_SIZE;
        }

        Ok(())
    }

    pub async fn checkpoint(
        &mut self,
        disk: &mut DiskService<impl StreamFactory>,
        locker: &LockService,
    ) -> Result<()> {
        if disk.get_file_length(FileOrigin::Log) == 0 || self.confirm_transactions.is_empty() {
            return Ok(());
        }

        let _scope = locker.enter_exclusive().await;

        self.checkpoint_internal(disk).await?;

        Ok(())
    }

    async fn checkpoint_internal(
        &mut self,
        disk: &mut DiskService<impl StreamFactory>,
    ) -> Result<usize> {
        // LOG("Checkpointing WAL");

        let mut buffers = Vec::new();

        {
            let mut reader = pin!(disk.read_full(FileOrigin::Log));
            while let Some(mut buffer) = reader.try_next().await? {
                if buffer.is_blank() {
                    continue;
                }

                let transaction_id = buffer.read_u32(BasePage::P_TRANSACTION_ID);

                if self.confirm_transactions.contains(&transaction_id) {
                    let page_id = buffer.read_u32(BasePage::P_PAGE_ID);

                    buffer.write_u32(BasePage::P_TRANSACTION_ID, u32::MAX);
                    buffer.write_bool(BasePage::P_IS_CONFIRMED, false);
                    buffer.set_position(BasePage::get_page_position(page_id));

                    buffers.push(buffer);
                }
            }
        }

        disk.write_data_disk(&buffers).await?;

        self.clear(disk).await?;

        Ok(buffers.len())
    }
}
