use crate::Result;
use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::page_position::PagePosition;
use crate::engine::pages::HeaderPage;
use crate::engine::snapshot::Snapshot;
use crate::engine::transaction_monitor::TransactionMonitorShared;
use crate::engine::transaction_pages::TransactionPages;
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{BasePage, PageBuffer, PageType, StreamFactory};
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::mem::forget;
use std::rc::Rc;
use std::thread::ThreadId;
use std::time::SystemTime;

pub(crate) struct TransactionService<'engine, SF: StreamFactory> {
    header: &'engine mut HeaderPage,
    locker: &'engine LockService,
    disk: &'engine mut DiskService<SF>,
    // reader will be created each time
    wal_index: &'engine mut WalIndexService,
    monitor: Rc<RefCell<TransactionMonitorShared>>, // TransactionService will be owned by TransactionMonitor so Rc here
    snapshots: HashMap<String, Snapshot<'engine, SF>>,
    trans_pages: Rc<RefCell<TransactionPages>>, // Fn TransactionPages will be shared with SnapShot so Rc

    transaction_id: u32,
    start_time: SystemTime, // TODO: which DateTime type to use?
    query_only: bool,

    mode: LockMode,
    state: TransactionState,

    thread_id: ThreadId,
    max_transaction_size: u32,
}

impl<'engine, SF: StreamFactory> TransactionService<'engine, SF> {
    pub fn new(
        header: &'engine mut HeaderPage,
        locker: &'engine LockService,
        disk: &'engine mut DiskService<SF>,
        // reader will be created each time
        wal_index: &'engine mut WalIndexService,
        max_transaction_size: u32,
        monitor: Rc<RefCell<TransactionMonitorShared>>,
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
            trans_pages: Rc::new(RefCell::new(TransactionPages::new())),
            state: TransactionState::Active,
        }
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

    pub fn max_transaction_size(&self) -> u32 {
        self.max_transaction_size
    }

    pub fn set_max_transaction_size(&mut self, size: u32) {
        self.max_transaction_size = size;
    }

    pub async fn create_snapshot<'a: 'engine>(
        &'a mut self,
        mode: LockMode,
        collection: &str,
        add_if_not_exists: bool,
    ) -> Result<&'a mut Snapshot<'engine, SF>> {
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
                        self.header,
                        self.transaction_id,
                        self.trans_pages.clone(),
                        self.locker,
                        self.wal_index,
                        self.disk,
                        add_if_not_exists,
                    )
                    .await?;

                    o.insert(new);
                }

                Ok(o.into_mut())
            }
            Entry::Vacant(v) => {
                let new = Snapshot::<'engine, SF>::new(
                    mode,
                    collection,
                    self.header,
                    self.transaction_id,
                    self.trans_pages.clone(),
                    self.locker,
                    self.wal_index,
                    self.disk,
                    add_if_not_exists,
                )
                .await?;

                Ok(v.insert(new))
            }
        }
    }

    // Originally in TransactionMonitor (TryExtend)
    fn try_extend_max_transaction_size(&mut self) -> bool {
        let mut monitor = self.monitor.borrow_mut();

        if monitor.free_pages >= monitor.initial_size {
            self.max_transaction_size += monitor.initial_size;
            monitor.free_pages -= monitor.initial_size;
            true
        } else {
            false
        }
    }

    // Originally in TransactionMonitor (CheckSafePoint)
    fn check_safe_point(&mut self) -> bool {
        if self.trans_pages.borrow().transaction_size >= self.max_transaction_size {
            return true;
        }
        !self.try_extend_max_transaction_size()
    }

    pub async fn safe_point(&mut self) -> Result<()> {
        debug_assert_eq!(self.state, TransactionState::Active);

        if self.check_safe_point() {
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
            let pages = std::mem::take(&mut self.snapshots)
                .into_values()
                .filter(|x| x.mode() == LockMode::Write)
                .flat_map(|x| x.into_writable_pages(true, commit));
            let mut pages = pages.peekable();

            let mark_last_as_confirmed = commit && !self.trans_pages.borrow().header_changed();

            while let Some(mut page) = pages.next() {
                let page_mut = page.as_mut().as_mut();
                page_mut.set_transaction_id(self.transaction_id);

                if pages.peek().is_none() {
                    // is_last
                    page_mut.set_confirmed(mark_last_as_confirmed);
                }

                if self.trans_pages.borrow().last_deleted_page() == page_mut.page_id() && commit {
                    debug_assert!(
                        self.trans_pages.borrow().header_changed(),
                        "header must be in lock"
                    );
                    debug_assert!(
                        page_mut.page_type() == PageType::Empty,
                        "must be marked as deleted page"
                    );

                    page_mut.set_next_page_id(self.header.free_empty_page_list());
                    self.header
                        .set_free_empty_page_list(self.trans_pages.borrow().first_deleted_page());
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

            if commit && self.trans_pages.borrow().header_changed() {
                self.header.set_transaction_id(self.transaction_id);
                self.header.set_confirmed(true);

                self.trans_pages.borrow_mut().call_on_commit(self.header);

                let buffer = self.header.update_buffer();
                let mut new = self.disk.new_page();

                *new.buffer_mut() = *buffer.buffer();

                buffers.push(new);
            }
        }

        // write all dirty pages, in sequence on log-file and store references into log pages on transPages
        // (works only for Write snapshots)
        let count = self.disk.write_log_disk(buffers).await?;

        // now, discard all clean pages (because those pages are writable and must be readable)
        // from write snapshots
        self.disk.discard_clean_pages(
            &self
                .snapshots
                .values()
                .filter(|x| x.mode() == LockMode::Write)
                .flat_map(|x| x.get_writable_pages(false, commit))
                .map(|x| x.as_ref().buffer())
                .collect::<Vec<_>>(),
        );

        Ok(count)
    }

    pub async fn commit(&mut self) -> Result<()> {
        debug_assert_eq!(self.state, TransactionState::Active);
        // LOG($"commit transaction ({_transPages.TransactionSize} pages)", "TRANSACTION");

        if self.mode == LockMode::Write || self.trans_pages.borrow().header_changed() {
            // lock on header
            let count = self.persist_dirty_page(true).await?;
            if count > 0 {
                self.wal_index.confirm_transaction(
                    self.transaction_id,
                    &self
                        .trans_pages
                        .borrow_mut()
                        .dirty_pages
                        .values()
                        .copied()
                        .collect::<Vec<_>>(),
                );
            }
        }

        self.snapshots.clear();

        self.state = TransactionState::Committed;

        Ok(())
    }

    pub async fn rollback(&mut self) -> Result<()> {
        debug_assert_eq!(self.state, TransactionState::Active);

        // LOG($"rollback transaction ({_transPages.TransactionSize} pages with {_transPages.NewPages.Count} returns)", "TRANSACTION");

        // if transaction contains new pages, must return to database in another transaction
        if !self.trans_pages.borrow().new_pages().is_empty() {
            self.return_new_pages().await?;
        }

        for snapshot in std::mem::take(&mut self.snapshots).into_values() {
            if snapshot.mode() == LockMode::Write {
                // discard all dirty pages
                self.disk.discard_dirty_pages(
                    &snapshot
                        .get_writable_pages(true, true)
                        .map(|x| x.as_ref().buffer())
                        .collect::<Vec<_>>(),
                );

                // discard all clean pages
                self.disk.discard_clean_pages(
                    &snapshot
                        .get_writable_pages(false, true)
                        .map(|x| x.as_ref().buffer())
                        .collect::<Vec<_>>(),
                );
            }
            drop(snapshot); // release page
        }

        self.state = TransactionState::Aborted;

        Ok(())
    }

    async fn return_new_pages(&mut self) -> Result<()> {
        let transaction_id = self.wal_index.next_transaction_id();

        // lock on header
        let mut page_positions = HashMap::<u32, PagePosition>::new();

        struct RestoreOnDrop<'a> {
            header: &'a mut HeaderPage,
            safe_point: Box<PageBuffer>,
        }

        impl Drop for RestoreOnDrop<'_> {
            fn drop(&mut self) {
                self.header.restore(&self.safe_point).unwrap();
            }
        }

        let r = RestoreOnDrop {
            safe_point: self.header.save_point(),
            header: self.header,
        };

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

        // destruct to drop changes
        forget(r);

        self.wal_index.confirm_transaction(
            self.transaction_id,
            &page_positions.values().copied().collect::<Vec<_>>(),
        );

        Ok(())
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
