use crate::Result;
use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::page_position::PagePosition;
use crate::engine::pages::{BasePage, HeaderPage, PageType};
use crate::engine::{FileOrigin, PAGE_SIZE, StreamFactory};
use async_lock::RwLock;
use futures::prelude::*;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::ops::DerefMut;
use std::pin::pin;
use std::sync::atomic::{AtomicU32, Ordering};

pub(crate) struct WalIndexService {
    last_transaction_id: AtomicU32,
    lock: RwLock<InLock>,
    // This variable is loked by either
    // - Transaction (Read) Lock and RwLock above (Write)
    // - Transaction Exclusive (Wrote) Lock
    confirm_transactions: RefCell<HashSet<u32>>,
}

struct InLock {
    current_read_version: i32,
    index: HashMap<u32, Vec<(i32, u64)>>,
}

impl WalIndexService {
    pub(crate) fn new() -> Self {
        Self {
            last_transaction_id: AtomicU32::new(0),
            lock: RwLock::new(InLock {
                current_read_version: 0,
                index: HashMap::new(),
            }),
            confirm_transactions: RefCell::new(HashSet::new()),
        }
    }

    // async for lock
    pub async fn current_read_version(&self) -> i32 {
        self.lock.read().await.current_read_version
    }

    pub fn last_transaction_id(&self) -> i32 {
        self.last_transaction_id.load(Ordering::Relaxed) as i32
    }

    pub async fn clear(&self, disk: &mut DiskService<impl StreamFactory>) -> Result<()> {
        let mut in_lock = self.lock.write().await;
        self.confirm_transactions.borrow_mut().clear();
        in_lock.index.clear();

        in_lock.current_read_version = 0;
        self.last_transaction_id.store(0, Ordering::SeqCst);

        disk.cache_mut().clear();
        disk.set_length(0, FileOrigin::Log).await?;

        Ok(())
    }

    pub fn next_transaction_id(&self) -> u32 {
        self.last_transaction_id.fetch_add(1, Ordering::SeqCst) + 1
    }

    pub async fn get_page_index(&self, page_id: u32, version: i32) -> (i32, u64) {
        if version == 0 {
            return (0, u64::MAX);
        }

        let in_lock = self.lock.read().await;

        if let Some(index) = in_lock.index.get(&page_id) {
            for (wal_version, position) in index.iter().rev() {
                if *wal_version <= version {
                    return (*wal_version, *position);
                }
            }

            return (version, u64::MAX);
        }
        (i32::MAX, u64::MAX)
    }

    pub async fn confirm_transaction(&self, transaction_id: u32, positions: &[PagePosition]) {
        let mut in_lock = self.lock.write().await;
        let in_lock = in_lock.deref_mut();
        in_lock.current_read_version += 1;

        for pos in positions {
            let slot = in_lock.index.entry(pos.page_id()).or_default();
            slot.push((in_lock.current_read_version, pos.position()));
        }

        self.confirm_transactions
            .borrow_mut()
            .insert(transaction_id);
    }

    pub async fn restore_index(
        &self,
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
                self.confirm_transaction(transaction_id, positions_for_transaction)
                    .await;

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

            self.last_transaction_id
                .store(transaction_id, Ordering::SeqCst);
            current += PAGE_SIZE;
        }

        Ok(())
    }

    pub async fn checkpoint(
        &self,
        disk: &mut DiskService<impl StreamFactory>,
        locker: &LockService,
    ) -> Result<()> {
        if disk.get_file_length(FileOrigin::Log) == 0
            || self.confirm_transactions.borrow().is_empty()
        {
            return Ok(());
        }

        let _scope = locker.enter_exclusive().await;

        self.checkpoint_internal(disk).await?;

        Ok(())
    }

    async fn checkpoint_internal(
        &self,
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

                if self.confirm_transactions.borrow().contains(&transaction_id) {
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
