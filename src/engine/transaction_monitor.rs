use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::transaction_service::TransactionService;
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{MAX_OPEN_TRANSACTIONS, MAX_TRANSACTION_SIZE};
use crate::{Error, Result};
use std::collections::HashMap;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::{Arc, Mutex as StdMutex};

pub(crate) struct TransactionMonitorShared {
    inner: Arc<StdMutex<InTransactionsLock>>,
}

pub(crate) struct TransactionMonitor {
    locker: Arc<LockService>,
    disk: Arc<DiskService>,
    // reader will be created each time
    wal_index: Arc<WalIndexService>,

    // each operation(s) in this mutex is small so using StdMutex instead of async one
    transactions: Arc<StdMutex<InTransactionsLock>>,
    // RustChange: No ThreadLocal Slot because API in rust won't need that I feel
    //slot: Option<Shared<TransactionService>>, // thread local
}

struct InTransactionsLock {
    transaction_max_transaction_sizes: HashMap<u32, Arc<AtomicU32>>,
    pub free_pages: u32,
    pub initial_size: u32,
}

impl TransactionMonitor {
    pub fn new(
        locker: Arc<LockService>,
        disk: Arc<DiskService>,
        // reader will be created each time
        wal_index: Arc<WalIndexService>,
    ) -> Self {
        Self {
            locker,
            disk,
            wal_index,
            transactions: Arc::new(StdMutex::new(InTransactionsLock {
                transaction_max_transaction_sizes: HashMap::new(),
                free_pages: MAX_TRANSACTION_SIZE,
                initial_size: MAX_TRANSACTION_SIZE / MAX_OPEN_TRANSACTIONS as u32,
            })),
            // RustChange: No ThreadLocal Slot
            //slot: None,
        }
    }

    // 2nd is is_new
    // pub async fn get_or_create_transaction(
    pub async fn create_transaction(&self, query_only: bool) -> Result<TransactionService> {
        let mut transaction;
        // RustChange: No ThreadLocal Slot
        //if let Some(ref slot_id) = self.slot {
        //    is_new = false;
        //    transaction_shared = Shared::clone(slot_id);
        //} else
        {
            {
                let mut lock = self.transactions.lock().unwrap();
                if lock.transaction_max_transaction_sizes.len() >= MAX_OPEN_TRANSACTIONS {
                    return Err(Error::transaction_limit());
                }

                let initial_size = lock.get_initial_size();
                //let already_lock = lock
                //    .transactions
                //    .values()
                //    .any(|x| x.borrow().thread_id() == std::thread::current().id());
                let max_transaction_size_rc = Arc::new(AtomicU32::new(initial_size));

                transaction = TransactionService::new(
                    Arc::clone(&self.locker),
                    Arc::clone(&self.disk),
                    Arc::clone(&self.wal_index),
                    max_transaction_size_rc.clone(),
                    TransactionMonitorShared {
                        inner: self.transactions.clone(),
                    },
                    query_only,
                );

                let transaction_id = transaction.transaction_id();

                lock.transaction_max_transaction_sizes
                    .insert(transaction_id, max_transaction_size_rc);
            }

            let lock_scope = self.locker.enter_transaction().await;
            transaction.set_lock_scope(lock_scope);
            // RustChange: always enter / exit transaction
            // if !already_lock {
            //    // return page when error occurs
            //}

            if !query_only {
                // RustChange: No ThreadLocal Slot
                //self.slot = Some(Shared::clone(&transaction_shared));
            }
        }

        Ok(transaction)
    }

    // 2nd is is_new
    // RustChange: No ThreadLocal Slot
    //pub async fn get_transaction(&self) -> Option<Shared<TransactionService<SF>>> {
    //    self.slot.clone()
    //}

    // RustChange: No ThreadLocal Slot
    //pub async fn get_thread_transaction(&self) -> Option<Shared<TransactionService<SF>>> {
    //    if let Some(ref slot) = self.slot {
    //        Some(Shared::clone(slot))
    //    } else {
    //        self.transactions
    //            .lock()
    //            .unwrap()
    //            .transactions
    //            .values()
    //            .find(|x| x.borrow().thread_id() == std::thread::current().id())
    //            .cloned()
    //    }
    //}
}

impl InTransactionsLock {
    fn get_initial_size(&mut self) -> u32 {
        if self.free_pages >= self.initial_size {
            self.free_pages -= self.initial_size;
            self.initial_size
        } else {
            let mut sum = 0;

            // if there is no available pages, reduce all open transactions
            for max_transaction_size in self.transaction_max_transaction_sizes.values() {
                //TODO(upstream): revisar estas contas, o reduce tem que fechar 1000
                let reduce = max_transaction_size.load(Relaxed) / self.initial_size;

                // Note: all writes to max_transaction_size are globally synchronized with lock
                // so no need to use fetch_sub
                max_transaction_size.fetch_sub(reduce, Relaxed);

                sum += reduce;
            }

            sum
        }
    }
}

impl TransactionMonitorShared {
    fn try_extend_max_transaction_size(&self, max_transaction_size: &AtomicU32) -> bool {
        let mut lock = self.inner.lock().unwrap();

        if lock.free_pages >= lock.initial_size {
            max_transaction_size.store(lock.initial_size, Relaxed);
            lock.free_pages -= lock.initial_size;
            true
        } else {
            false
        }
    }

    pub fn check_safe_point(
        &self,
        transaction_size: u32,
        max_transaction_size: &AtomicU32,
    ) -> bool {
        (transaction_size >= max_transaction_size.load(Relaxed))
            && !self.try_extend_max_transaction_size(max_transaction_size)
    }

    pub(crate) fn release_transaction(&self, transaction_id: u32, max_transaction_size: u32) {
        // remove Result?
        //let keep_locked;
        //let transaction;

        // no lock
        {
            let mut lock = self.inner.lock().unwrap();
            lock.transaction_max_transaction_sizes
                .remove(&transaction_id);
            lock.free_pages += max_transaction_size;
            //keep_locked = lock
            //    .transactions
            //    .values()
            //    .any(|x| x.borrow().thread_id() == std::thread::current().id())
        }

        // RustChange: always enter / exit transaction
        // RustChange: RAII Transaction Lock
        //self.locker.exit_transaction();
        //if !keep_locked {
        //    self.locker.exit_transaction();
        //}

        //if !transaction.borrow().query_only() {
        //    // RustChange: No ThreadLocal Slot
        //    //self.slot = None;
        //}
    }
}
