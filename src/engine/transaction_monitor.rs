use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::transaction_service::TransactionService;
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{HeaderPage, MAX_OPEN_TRANSACTIONS, MAX_TRANSACTION_SIZE, StreamFactory};
use crate::utils::Shared;
use crate::{Error, Result};
use std::collections::HashMap;

pub(crate) struct TransactionMonitorShared {
    pub free_pages: u32,
    pub initial_size: u32,
}

pub(crate) struct TransactionMonitor<'engine, SF: StreamFactory> {
    header: &'engine mut HeaderPage,
    locker: &'engine LockService,
    disk: &'engine mut DiskService<SF>,
    // reader will be created each time
    wal_index: &'engine WalIndexService,

    shared: Shared<TransactionMonitorShared>,
    transactions: HashMap<u32, TransactionService<'engine, SF>>,
    slot_id: Option<u32>, // thread local
}

impl<'engine, SF: StreamFactory> TransactionMonitor<'engine, SF> {
    pub fn new(
        header: &'engine mut HeaderPage,
        locker: &'engine LockService,
        disk: &'engine mut DiskService<SF>,
        // reader will be created each time
        wal_index: &'engine WalIndexService,
    ) -> Self {
        Self {
            header,
            locker,
            disk,
            wal_index,
            shared: Shared::new(TransactionMonitorShared {
                free_pages: MAX_TRANSACTION_SIZE,
                initial_size: MAX_TRANSACTION_SIZE / MAX_OPEN_TRANSACTIONS as u32,
            }),
            transactions: HashMap::new(),
            slot_id: None,
        }
    }

    // 2nd is is_new
    pub async fn get_or_create_transaction<'a: 'engine>(
        &'a mut self,
        query_only: bool,
    ) -> Result<(&'a mut TransactionService<'engine, SF>, bool)> {
        let is_new;
        let transaction_mut: &'a mut TransactionService<'engine, SF>;
        if let Some(slot_id) = self.slot_id {
            is_new = false;
            transaction_mut = self.transactions.get_mut(&slot_id).unwrap();
        } else {
            is_new = true;

            if self.transactions.len() >= MAX_OPEN_TRANSACTIONS {
                return Err(Error::transaction_limit());
            }

            let initial_size = self.get_initial_size();
            let already_lock = self
                .transactions
                .values()
                .any(|x| x.thread_id() == std::thread::current().id());

            let transaction = TransactionService::new(
                self.header,
                self.locker,
                self.disk,
                self.wal_index,
                initial_size,
                self.shared.clone(),
                query_only,
            );

            transaction_mut = self
                .transactions
                .entry(transaction.transaction_id())
                .insert_entry(transaction)
                .into_mut();

            if !already_lock {
                self.locker.enter_transaction().await;
                // return page when error occurs
            }

            if !query_only {
                self.slot_id = Some(transaction_mut.transaction_id());
            }
        }

        Ok((transaction_mut, is_new))
    }

    // 2nd is is_new
    pub async fn get_transaction(&mut self) -> Option<&mut TransactionService<'engine, SF>> {
        if let Some(slot_id) = self.slot_id {
            Some(self.transactions.get_mut(&slot_id).unwrap())
        } else {
            None
        }
    }

    pub async fn release_transaction(&mut self, transaction_id: u32) -> Result<()> {
        // remove Result?
        let keep_locked;
        let transaction;

        // no lock
        {
            let mut shared = self.shared.borrow_mut();
            transaction = self
                .transactions
                .remove(&transaction_id)
                .expect("the transaction not exists");
            shared.free_pages += transaction.max_transaction_size();
            keep_locked = self
                .transactions
                .values()
                .any(|x| x.thread_id() == std::thread::current().id())
        }

        if !keep_locked {
            self.locker.exit_transaction();
        }

        if !transaction.query_only() {
            self.slot_id = None;
        }

        Ok(())
    }

    pub async fn get_thread_transaction(&self) -> Option<&TransactionService<'engine, SF>> {
        if let Some(slot_id) = self.slot_id {
            Some(self.transactions.get(&slot_id).unwrap())
        } else {
            self.transactions
                .values()
                .find(|x| x.thread_id() == std::thread::current().id())
        }
    }

    fn get_initial_size(&mut self) -> u32 {
        let mut shared = self.shared.borrow_mut();

        if shared.free_pages >= shared.initial_size {
            shared.free_pages -= shared.initial_size;
            shared.initial_size
        } else {
            let mut sum = 0;

            // if there is no available pages, reduce all open transactions
            for trans in self.transactions.values_mut() {
                //TODO(upstream): revisar estas contas, o reduce tem que fechar 1000
                let reduce = trans.max_transaction_size() / shared.initial_size;

                trans.set_max_transaction_size(trans.max_transaction_size() - reduce);

                sum += reduce;
            }

            sum
        }
    }
}
