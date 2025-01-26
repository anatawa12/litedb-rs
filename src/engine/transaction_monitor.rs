use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::transaction_service::TransactionService;
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{HeaderPage, MAX_OPEN_TRANSACTIONS, MAX_TRANSACTION_SIZE, StreamFactory};
use crate::utils::Shared;
use crate::{Error, Result};
use std::collections::HashMap;
use std::rc::Rc;

pub(crate) struct TransactionMonitorShared {
    pub free_pages: u32,
    pub initial_size: u32,
}

pub(crate) struct TransactionMonitor<SF: StreamFactory> {
    header: Shared<HeaderPage>,
    locker: Rc<LockService>,
    disk: Rc<DiskService<SF>>,
    // reader will be created each time
    wal_index: Rc<WalIndexService>,

    shared: Shared<TransactionMonitorShared>,
    transactions: HashMap<u32, Shared<TransactionService<SF>>>,
    slot: Option<Shared<TransactionService<SF>>>, // thread local
}

impl<SF: StreamFactory> TransactionMonitor<SF> {
    pub fn new(
        header: Shared<HeaderPage>,
        locker: Rc<LockService>,
        disk: Rc<DiskService<SF>>,
        // reader will be created each time
        wal_index: Rc<WalIndexService>,
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
            slot: None,
        }
    }

    // 2nd is is_new
    pub async fn get_or_create_transaction(
        &mut self,
        query_only: bool,
    ) -> Result<(Shared<TransactionService<SF>>, bool)> {
        let is_new;
        let transaction_shared: Shared<TransactionService<SF>>;
        if let Some(ref slot_id) = self.slot {
            is_new = false;
            transaction_shared = Shared::clone(slot_id);
        } else {
            is_new = true;

            if self.transactions.len() >= MAX_OPEN_TRANSACTIONS {
                return Err(Error::transaction_limit());
            }

            let initial_size = self.get_initial_size();
            let already_lock = self
                .transactions
                .values()
                .any(|x| x.borrow().thread_id() == std::thread::current().id());

            let transaction = TransactionService::new(
                Shared::clone(&self.header),
                Rc::clone(&self.locker),
                Rc::clone(&self.disk),
                Rc::clone(&self.wal_index),
                initial_size,
                Shared::clone(&self.shared),
                query_only,
            );

            let transaction_id = transaction.transaction_id();
            transaction_shared = Shared::new(transaction);

            self.transactions
                .insert(transaction_id, Shared::clone(&transaction_shared));

            if !already_lock {
                self.locker.enter_transaction().await;
                // return page when error occurs
            }

            if !query_only {
                self.slot = Some(Shared::clone(&transaction_shared));
            }
        }

        Ok((transaction_shared, is_new))
    }

    // 2nd is is_new
    pub async fn get_transaction(&self) -> Option<Shared<TransactionService<SF>>> {
        self.slot.clone()
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
            shared.free_pages += transaction.borrow().max_transaction_size();
            keep_locked = self
                .transactions
                .values()
                .any(|x| x.borrow().thread_id() == std::thread::current().id())
        }

        if !keep_locked {
            self.locker.exit_transaction();
        }

        if !transaction.borrow().query_only() {
            self.slot = None;
        }

        Ok(())
    }

    pub async fn get_thread_transaction(&self) -> Option<Shared<TransactionService<SF>>> {
        if let Some(ref slot) = self.slot {
            Some(Shared::clone(slot))
        } else {
            self.transactions
                .values()
                .find(|x| x.borrow().thread_id() == std::thread::current().id())
                .cloned()
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
                let mut trans = trans.borrow_mut();
                let trans = &mut trans;
                //TODO(upstream): revisar estas contas, o reduce tem que fechar 1000
                let reduce = trans.max_transaction_size() / shared.initial_size;

                let max_transaction_size = trans.max_transaction_size();
                trans.set_max_transaction_size(max_transaction_size - reduce);

                sum += reduce;
            }

            sum
        }
    }
}
