use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::transaction_service::TransactionService;
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{HeaderPage, MAX_OPEN_TRANSACTIONS, MAX_TRANSACTION_SIZE, StreamFactory};
use crate::utils::Shared;
use crate::{Error, Result};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;

pub(crate) struct TransactionMonitorShared<SF: StreamFactory> {
    inner: Rc<StdMutex<InTransactionsLock<SF>>>,
}

pub(crate) struct TransactionMonitor<SF: StreamFactory> {
    header: Shared<HeaderPage>,
    locker: Rc<LockService>,
    disk: Rc<DiskService<SF>>,
    // reader will be created each time
    wal_index: Rc<WalIndexService>,

    // each operation(s) in this mutex is small so using StdMutex instead of async one
    transactions: Rc<StdMutex<InTransactionsLock<SF>>>,
    // RustChange: No ThreadLocal Slot because API in rust won't need that I feel
    //slot: Option<Shared<TransactionService<SF>>>, // thread local
}

struct InTransactionsLock<SF: StreamFactory> {
    transactions: HashMap<u32, Shared<TransactionService<SF>>>,
    pub free_pages: u32,
    pub initial_size: u32,
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
            transactions: Rc::new(StdMutex::new(InTransactionsLock {
                transactions: HashMap::new(),
                free_pages: MAX_TRANSACTION_SIZE,
                initial_size: MAX_TRANSACTION_SIZE / MAX_OPEN_TRANSACTIONS as u32,
            })),
            // RustChange: No ThreadLocal Slot
            //slot: None,
        }
    }

    // 2nd is is_new
    // pub async fn get_or_create_transaction(
    pub async fn create_transaction(
        &mut self,
        query_only: bool,
    ) -> Result<(Shared<TransactionService<SF>>, bool)> {
        let is_new;
        let transaction_shared: Shared<TransactionService<SF>>;
        // RustChange: No ThreadLocal Slot
        //if let Some(ref slot_id) = self.slot {
        //    is_new = false;
        //    transaction_shared = Shared::clone(slot_id);
        //} else
        {
            is_new = true;
            {
                let mut lock = self.transactions.lock().unwrap();
                if lock.transactions.len() >= MAX_OPEN_TRANSACTIONS {
                    return Err(Error::transaction_limit());
                }

                let initial_size = lock.get_initial_size();
                //let already_lock = lock
                //    .transactions
                //    .values()
                //    .any(|x| x.borrow().thread_id() == std::thread::current().id());

                let transaction = TransactionService::new(
                    Shared::clone(&self.header),
                    Rc::clone(&self.locker),
                    Rc::clone(&self.disk),
                    Rc::clone(&self.wal_index),
                    initial_size,
                    TransactionMonitorShared {
                        inner: self.transactions.clone(),
                    },
                    query_only,
                );

                let transaction_id = transaction.transaction_id();
                transaction_shared = Shared::new(transaction);

                lock.transactions
                    .insert(transaction_id, Shared::clone(&transaction_shared));
            }

            self.locker.enter_transaction().await;
            // RustChange: always enter / exit transaction
            // if !already_lock {
            //    // return page when error occurs
            //}

            if !query_only {
                // RustChange: No ThreadLocal Slot
                //self.slot = Some(Shared::clone(&transaction_shared));
            }
        }

        Ok((transaction_shared, is_new))
    }

    // 2nd is is_new
    // RustChange: No ThreadLocal Slot
    //pub async fn get_transaction(&self) -> Option<Shared<TransactionService<SF>>> {
    //    self.slot.clone()
    //}

    pub async fn release_transaction(&mut self, transaction_id: u32) -> Result<()> {
        // remove Result?
        //let keep_locked;
        let transaction;

        // no lock
        {
            let mut lock = self.transactions.lock().unwrap();
            transaction = lock
                .transactions
                .remove(&transaction_id)
                .expect("the transaction not exists");
            lock.free_pages += transaction.borrow().max_transaction_size().load(Relaxed);
            //keep_locked = lock
            //    .transactions
            //    .values()
            //    .any(|x| x.borrow().thread_id() == std::thread::current().id())
        }

        // RustChange: always enter / exit transaction
        self.locker.exit_transaction();
        //if !keep_locked {
        //    self.locker.exit_transaction();
        //}

        if !transaction.borrow().query_only() {
            // RustChange: No ThreadLocal Slot
            //self.slot = None;
        }

        Ok(())
    }

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

impl<SF: StreamFactory> InTransactionsLock<SF> {
    fn get_initial_size(&mut self) -> u32 {
        if self.free_pages >= self.initial_size {
            self.free_pages -= self.initial_size;
            self.initial_size
        } else {
            let mut sum = 0;

            // if there is no available pages, reduce all open transactions
            for trans in self.transactions.values_mut() {
                let trans = trans.borrow();
                let max_transaction_size = trans.max_transaction_size();

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

impl<SF: StreamFactory> TransactionMonitorShared<SF> {
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
}
