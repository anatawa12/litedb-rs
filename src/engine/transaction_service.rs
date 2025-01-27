use crate::Result;
use crate::engine::disk::DiskService;
use crate::engine::lock_service::{LockService, TransactionScope};
use crate::engine::page_position::PagePosition;
use crate::engine::pages::HeaderPage;
use crate::engine::snapshot::Snapshot;
use crate::engine::transaction_monitor::TransactionMonitorShared;
use crate::engine::transaction_pages::TransactionPages;
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{BasePage, PageType, StreamFactory};
use crate::utils::Shared;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem::forget;
use std::rc::Rc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering::Relaxed;
use std::thread::ThreadId;
use std::time::SystemTime;

pub(crate) struct TransactionService<SF: StreamFactory> {
    header: Shared<HeaderPage>,
    locker: Rc<LockService>,
    disk: Rc<DiskService<SF>>,
    // reader will be created each time
    wal_index: Rc<WalIndexService>,
    monitor: TransactionMonitorShared<SF>, // TransactionService will be owned by TransactionMonitor so Rc here
    snapshots: HashMap<String, Snapshot<SF>>,
    trans_pages: Shared<TransactionPages>, // Fn TransactionPages will be shared with SnapShot so Rc

    transaction_id: u32,
    start_time: SystemTime, // TODO: which DateTime type to use?
    query_only: bool,

    mode: LockMode,
    state: TransactionState,

    thread_id: ThreadId,
    max_transaction_size: Rc<AtomicU32>,
    trans_lock_scope: Option<TransactionScope>,
}

impl<SF: StreamFactory> TransactionService<SF> {
    pub fn new(
        header: Shared<HeaderPage>,
        locker: Rc<LockService>,
        disk: Rc<DiskService<SF>>,
        // reader will be created each time
        wal_index: Rc<WalIndexService>,
        max_transaction_size: Rc<AtomicU32>,
        monitor: TransactionMonitorShared<SF>,
        query_only: bool,
    ) -> Self {
        Self {
            transaction_id: wal_index.next_transaction_id(),

            header,
            locker,
            disk,
            wal_index,
            max_transaction_size,
            monitor,
            snapshots: HashMap::new(),
            query_only,

            mode: LockMode::Read,
            start_time: SystemTime::now(),
            thread_id: std::thread::current().id(),
            trans_pages: Shared::new(TransactionPages::new()),
            state: TransactionState::Active,
            trans_lock_scope: None,
        }
    }

    pub(crate) fn set_lock_scope(&mut self, trans_lock_scope: TransactionScope) {
        self.trans_lock_scope = Some(trans_lock_scope);
    }

    pub fn transaction_id(&self) -> u32 {
        self.transaction_id
    }

    pub fn start_time(&self) -> SystemTime {
        self.start_time
    }

    pub fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    pub fn query_only(&self) -> bool {
        self.query_only
    }

    pub fn max_transaction_size(&self) -> &AtomicU32 {
        &self.max_transaction_size
    }

    pub fn pages(&self) -> &Shared<TransactionPages> {
        &self.trans_pages
    }

    pub async fn create_snapshot<'a>(
        &'a mut self,
        mode: LockMode,
        collection: &str,
        add_if_not_exists: bool,
    ) -> Result<&'a mut Snapshot<SF>> {
        debug_assert_eq!(self.state, TransactionState::Active);

        match self.snapshots.entry(collection.to_string()) {
            Entry::Occupied(mut o) => {
                if mode == LockMode::Write && o.get().mode() == LockMode::Read
                    || add_if_not_exists && o.get().collection_page().is_none()
                {
                    // then create new
                    let new = Snapshot::new(
                        mode,
                        collection,
                        Shared::clone(&self.header),
                        self.transaction_id,
                        self.trans_pages.clone(),
                        Rc::clone(&self.locker),
                        Rc::clone(&self.wal_index),
                        Rc::clone(&self.disk),
                        add_if_not_exists,
                    )
                    .await?;

                    o.insert(new);
                }

                Ok(o.into_mut())
            }
            Entry::Vacant(v) => {
                let new = Snapshot::<SF>::new(
                    mode,
                    collection,
                    Shared::clone(&self.header),
                    self.transaction_id,
                    self.trans_pages.clone(),
                    Rc::clone(&self.locker),
                    Rc::clone(&self.wal_index),
                    Rc::clone(&self.disk),
                    add_if_not_exists,
                )
                .await?;

                Ok(v.insert(new))
            }
        }
    }

    pub async fn safe_point(&mut self) -> Result<()> {
        debug_assert_eq!(self.state, TransactionState::Active);

        let transaction_size = self.trans_pages.borrow().transaction_size;
        if self
            .monitor
            .check_safe_point(transaction_size, &self.max_transaction_size)
        {
            // LOG($"safepoint flushing transaction pages: {_transPages.TransactionSize}", "TRANSACTION");

            if self.mode == LockMode::Write {
                self.persist_dirty_page(false).await?;
            }

            for snapshot in self.snapshots.values_mut() {
                snapshot.clear()
            }

            self.trans_pages.borrow_mut().transaction_size = 0;
        }

        Ok(())
    }

    async fn persist_dirty_page(&mut self, commit: bool) -> Result<usize> {
        let mut buffers = vec![];

        // build buffers
        {
            let mut header_if_commit = if commit {
                Some(self.header.borrow_mut())
            } else {
                None
            };
            let pages = self
                .snapshots
                .values_mut()
                .filter(|x| x.mode() == LockMode::Write)
                .flat_map(|x| x.writable_pages_removing(true, commit));
            let mut pages = pages.peekable();

            let mark_last_as_confirmed = commit && !self.trans_pages.borrow().header_changed();

            while let Some(mut page) = pages.next() {
                let page_mut = page.as_mut().as_mut();
                page_mut.set_transaction_id(self.transaction_id);

                if pages.peek().is_none() {
                    // is_last
                    page_mut.set_confirmed(mark_last_as_confirmed);
                }

                //if self.trans_pages.borrow().last_deleted_page() == page_mut.page_id() && commit {
                if let Some(ref mut header) = header_if_commit {
                    if self.trans_pages.borrow().last_deleted_page() == page_mut.page_id() {
                        debug_assert!(
                            self.trans_pages.borrow().header_changed(),
                            "header must be in lock"
                        );
                        debug_assert!(
                            page_mut.page_type() == PageType::Empty,
                            "must be marked as deleted page"
                        );

                        page_mut.set_next_page_id(header.free_empty_page_list());
                        header.set_free_empty_page_list(
                            self.trans_pages.borrow().first_deleted_page(),
                        );
                    }
                }

                let page_id = page_mut.page_id();

                let buffer = page.update_buffer();
                let position = buffer.position();

                buffers.push(page.into_base().into_buffer());
                self.trans_pages
                    .borrow_mut()
                    .dirty_pages
                    .insert(page_id, PagePosition::new(page_id, position));
            }

            //if commit && self.trans_pages.borrow().header_changed() {
            if let Some(ref mut header) = header_if_commit {
                if self.trans_pages.borrow().header_changed() {
                    header.set_transaction_id(self.transaction_id);
                    header.set_confirmed(true);

                    self.trans_pages.borrow_mut().call_on_commit(header);

                    let buffer = header.update_buffer();
                    let mut new = self.disk.new_page();

                    *new.buffer_mut() = *buffer.buffer();

                    buffers.push(new);
                }
            }
        }

        // write all dirty pages, in sequence on log-file and store references into log pages on transPages
        // (works only for Write snapshots)
        let count = self.disk.write_log_disk(buffers).await?;

        // now, discard all clean pages (because those pages are writable and must be readable)
        // from write snapshots
        self.disk.discard_clean_pages(
            self.snapshots
                .values_mut()
                .filter(|x| x.mode() == LockMode::Write)
                .flat_map(|x| x.writable_pages_removing(false, commit))
                .map(|x| x.into_base().into_buffer())
                .collect::<Vec<_>>(),
        );

        Ok(count)
    }

    pub async fn commit(mut self) -> Result<()> {
        debug_assert_eq!(self.state, TransactionState::Active);
        // LOG($"commit transaction ({_transPages.TransactionSize} pages)", "TRANSACTION");

        if self.mode == LockMode::Write || self.trans_pages.borrow().header_changed() {
            // lock on header
            let count = self.persist_dirty_page(true).await?;
            if count > 0 {
                let dirty_pages = self
                    .trans_pages
                    .borrow_mut()
                    .dirty_pages
                    .values()
                    .copied()
                    .collect::<Vec<_>>();
                self.wal_index
                    .confirm_transaction(self.transaction_id, &dirty_pages)
                    .await;
            }
        }

        self.snapshots.clear();

        self.state = TransactionState::Committed;

        Ok(())
    }

    pub async fn rollback(mut self) -> Result<()> {
        debug_assert_eq!(self.state, TransactionState::Active);

        // LOG($"rollback transaction ({_transPages.TransactionSize} pages with {_transPages.NewPages.Count} returns)", "TRANSACTION");

        // if transaction contains new pages, must return to database in another transaction
        if !self.trans_pages.borrow().new_pages().is_empty() {
            self.return_new_pages().await?;
        }

        for mut snapshot in std::mem::take(&mut self.snapshots).into_values() {
            if snapshot.mode() == LockMode::Write {
                // discard all dirty pages
                self.disk.discard_dirty_pages(
                    snapshot
                        .writable_pages_removing(true, true)
                        .map(|x| x.into_base().into_buffer())
                        .collect::<Vec<_>>(),
                );

                // discard all clean pages
                self.disk.discard_clean_pages(
                    snapshot
                        .writable_pages_removing(false, true)
                        .map(|x| x.into_base().into_buffer())
                        .collect::<Vec<_>>(),
                );
            }
            drop(snapshot); // release page
        }

        self.state = TransactionState::Aborted;

        Ok(())
    }

    #[allow(clippy::await_holding_refcell_ref)]
    async fn return_new_pages(&mut self) -> Result<()> {
        let transaction_id = self.wal_index.next_transaction_id();

        // lock on header
        let mut page_positions = HashMap::<u32, PagePosition>::new();

        #[allow(clippy::await_holding_refcell_ref)]
        let mut header = self.header.borrow_mut();
        let r = header.save_point();

        let mut buffers = Vec::new();
        // build buffers
        {
            let trans_pages = self.trans_pages.borrow();
            let new_pages = trans_pages.new_pages();
            for (idx, &page_id) in new_pages.iter().enumerate() {
                let next = new_pages
                    .get(idx + 1)
                    .copied()
                    .unwrap_or(r.header.free_empty_page_list());

                let buffer = self.disk.new_page();
                let position = buffer.position();
                let mut page = BasePage::new(buffer, page_id, PageType::Empty);
                page.set_next_page_id(next);
                page.set_transaction_id(transaction_id);
                page.update_buffer();
                buffers.push(page.into_buffer());

                page_positions.insert(page_id, PagePosition::new(page_id, position));
            }

            r.header.set_transaction_id(transaction_id);
            r.header
                .set_free_empty_page_list(self.trans_pages.borrow().new_pages()[0]);
            r.header.set_confirmed(true);

            let buf = r.header.update_buffer();
            let mut clone = self.disk.new_page();
            *clone.buffer_mut() = *buf.buffer();

            buffers.push(clone);
        }

        self.disk.write_log_disk(buffers).await?;

        forget(r);
        drop(header);

        self.wal_index
            .confirm_transaction(
                self.transaction_id,
                &page_positions.values().copied().collect::<Vec<_>>(),
            )
            .await;

        Ok(())
    }
}

impl<SF: StreamFactory> Drop for TransactionService<SF> {
    fn drop(&mut self) {
        if self.state == TransactionState::Active && !self.snapshots.is_empty() {
            for mut snapshot in std::mem::take(&mut self.snapshots).into_values() {
                if snapshot.mode() == LockMode::Write {
                    // discard all dirty pages
                    self.disk.discard_dirty_pages(
                        snapshot
                            .writable_pages_removing(true, true)
                            .map(|x| x.into_base().into_buffer())
                            .collect::<Vec<_>>(),
                    );

                    // discard all clean pages
                    self.disk.discard_clean_pages(
                        snapshot
                            .writable_pages_removing(false, true)
                            .map(|x| x.into_base().into_buffer())
                            .collect::<Vec<_>>(),
                    );
                }
                drop(snapshot); // release page
            }
        }

        self.monitor
            .release_transaction(self.transaction_id, self.max_transaction_size.load(Relaxed));
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum TransactionState {
    Active,
    Committed,
    Aborted,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum LockMode {
    Read,
    Write,
}
