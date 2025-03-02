use crate::engine::collection_service::CollectionService;
use crate::engine::disk::DiskService;
use crate::engine::index_service::{IndexService, PartialIndexNodeAccessorMut};
use crate::engine::lock_service::{CollectionLockScope, LockService};
use crate::engine::pages::HeaderPage;
use crate::engine::transaction_pages::TransactionPages;
use crate::engine::transaction_service::LockMode;
use crate::engine::utils::SendPtr;
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{
    BasePage, CollectionIndexesMut, CollectionPage, DataPage, DirtyFlag, FileOrigin,
    FreeDataPageList, IndexPage, PAGE_SIZE, Page, PageType,
};
use crate::utils::{Order, Shared};
use crate::{Error, Result};
use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::mem::forget;
use std::pin::Pin;
use std::sync::Arc;

macro_rules! inner {
    ($this: expr) => {
        // possibly inside of unsafe block
        #[allow(unused_unsafe)]
        unsafe {
            &mut *$this.inner.0
        }
    };
}

pub(crate) struct Snapshot {
    #[allow(dead_code)] // the socpe variable
    lock_scope: Option<CollectionLockScope>,

    #[allow(dead_code)] // used in $snapshots
    transaction_id: u32,

    mode: LockMode,
    collection_name: String,
    collection_page: Option<Pin<Box<CollectionPage>>>,

    page_collection: SnapshotPages,
}

// for lifetime reasons, we split page collection to one struct
pub(crate) struct SnapshotPages {
    header: Arc<HeaderPage>,
    data: SnapshotPagesData,
}

pub(crate) struct SnapshotPagesData {
    disk: Arc<DiskService>,
    wal_index: Arc<WalIndexService>,
    trans_pages: Shared<TransactionPages>,
    read_version: i32,
    transaction_id: u32,
    local_pages: HashMap<u32, Pin<Box<dyn Page>>>,
    collection_page_id: Option<u32>,
}

pub(crate) struct SnapshotDataPages<'a> {
    inner: SendPtr<SnapshotPages>,
    dirty: &'a DirtyFlag,
    free_data_page_list: &'a mut FreeDataPageList,
    _phantom: PhantomData<&'a SnapshotPages>,
}

pub(crate) struct SnapshotIndexPages<'a> {
    inner: SendPtr<SnapshotPages>,
    dirty: &'a DirtyFlag,
    _phantom: PhantomData<&'a SnapshotPages>,
}

impl Snapshot {
    pub async fn new(
        mode: LockMode,
        collection_name: &str,
        header: Arc<HeaderPage>,
        transaction_id: u32,
        trans_pages: Shared<TransactionPages>,
        locker: Arc<LockService>,
        wal_index: Arc<WalIndexService>,
        disk: Arc<DiskService>,
        add_if_not_exists: bool,
    ) -> Result<Self> {
        let lock_scope = if mode == LockMode::Write {
            Some(locker.enter_lock(collection_name).await)
        } else {
            None
        };

        let read_version = wal_index.current_read_version().await;

        let mut snapshot = Self {
            lock_scope,
            transaction_id,
            mode,
            collection_name: collection_name.to_string(),
            collection_page: None,
            page_collection: SnapshotPages {
                header,
                data: SnapshotPagesData {
                    disk,
                    trans_pages,
                    wal_index,
                    read_version,
                    transaction_id,
                    collection_page_id: None,
                    local_pages: HashMap::new(),
                },
            },
        };

        let mut srv = CollectionService::new(&mut snapshot);

        let (_, collection_page) = srv.get(collection_name, add_if_not_exists).await?;

        // replace with owned one
        let collection_page = if let Some(collection_page) = collection_page {
            let collection_page_id = collection_page.page_id();
            let collection_page = snapshot
                .page_collection
                .data
                .local_pages
                .remove(&collection_page_id)
                .unwrap()
                .downcast_pin::<CollectionPage>()
                .ok()
                .unwrap();
            Some(collection_page)
        } else {
            None
        };

        snapshot.collection_page = collection_page;
        snapshot.page_collection.data.collection_page_id =
            snapshot.collection_page.as_ref().map(|x| x.page_id());

        Ok(snapshot)
    }

    pub fn header(&mut self) -> &Arc<HeaderPage> {
        &self.page_collection.header
    }

    pub fn trans_pages(&self) -> &Shared<TransactionPages> {
        &self.page_collection.data.trans_pages
    }

    pub fn disk(&self) -> &Arc<DiskService> {
        &self.page_collection.data.disk
    }

    pub fn mode(&self) -> LockMode {
        self.mode
    }

    #[allow(dead_code)]
    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }

    pub fn collection_page(&self) -> Option<&CollectionPage> {
        self.collection_page
            .as_ref()
            .map(Pin::as_ref)
            .map(Pin::get_ref)
    }

    #[allow(dead_code)]
    pub fn collection_page_mut(&mut self) -> Option<&mut CollectionPage> {
        self.collection_page
            .as_mut()
            .map(Pin::as_mut)
            .map(Pin::get_mut)
    }

    #[allow(dead_code)]
    pub fn local_pages(&self) -> impl Iterator<Item = Pin<&dyn Page>> {
        self.page_collection
            .data
            .local_pages
            .values()
            .map(Pin::as_ref)
    }

    #[allow(dead_code)] // used in $snapshots
    pub fn read_version(&self) -> i32 {
        self.page_collection.data.read_version
    }

    #[allow(dead_code)]
    pub fn get_writable_pages(
        &self,
        dirty: bool,
        with_collection_page: bool,
    ) -> impl Iterator<Item = &dyn Page> {
        let x = if self.mode != LockMode::Write {
            None
        } else {
            let pages = self
                .page_collection
                .data
                .local_pages
                .values()
                .filter(move |p| p.as_ref().get_ref().as_ref().is_dirty() == dirty)
                .map(|p| p.as_ref().get_ref());
            let collection_page = self
                .collection_page
                .as_ref()
                .take_if(|p| with_collection_page && p.is_dirty() == dirty)
                .map(|x| -> &dyn Page { x.as_ref().get_ref() });
            let collection_page = collection_page.into_iter();

            Some(pages.chain(collection_page))
        };

        x.into_iter().flatten()
    }

    pub fn writable_pages_removing(
        &mut self,
        dirty: bool,
        with_collection_page: bool,
    ) -> impl Iterator<Item = Pin<Box<dyn Page>>> {
        let x = if self.mode != LockMode::Write {
            None
        } else {
            let page_ids = self
                .page_collection
                .data
                .local_pages
                .iter()
                .filter(|(_, p)| p.as_ref().get_ref().as_ref().is_dirty() == dirty)
                .map(|(&k, _)| k)
                .collect::<Vec<_>>();
            let pages = page_ids
                .into_iter()
                .map(|k| self.page_collection.data.local_pages.remove(&k).unwrap());
            let collection_page = self
                .collection_page
                .take_if(|p| with_collection_page && p.is_dirty() == dirty)
                .map(|x| -> Pin<Box<dyn Page>> { x });
            let collection_page = collection_page.into_iter();

            Some(pages.chain(collection_page))
        };

        x.into_iter().flatten()
    }

    #[allow(dead_code)] // basically removing vairant is used
    pub fn get_writable_pages_mut(
        &mut self,
        dirty: bool,
        with_collection_page: bool,
    ) -> impl Iterator<Item = Pin<&mut dyn Page>> {
        let x = if self.mode != LockMode::Write {
            None
        } else {
            let pages = self
                .page_collection
                .data
                .local_pages
                .values_mut()
                .filter(move |p| p.as_ref().get_ref().as_ref().is_dirty() == dirty)
                .map(|p| p.as_mut());
            let collection_page = self
                .collection_page
                .as_mut()
                .take_if(|p| with_collection_page && p.is_dirty() == dirty)
                .map(|x| -> Pin<&mut dyn Page> { x.as_mut() });
            let collection_page = collection_page.into_iter();

            Some(pages.chain(collection_page))
        };

        x.into_iter().flatten()
    }

    pub fn clear(&mut self) {
        self.page_collection.data.local_pages.clear();
    }
}

// region Page Version functions
impl SnapshotPages {
    pub async fn get_page<T: Page>(
        &mut self,
        page_id: u32,
        use_latest_version: bool, /* = false*/
    ) -> Result<Pin<&mut T>> {
        assert!(page_id != u32::MAX && page_id <= self.header.last_page_id());
        Ok(self
            .data
            .get_page_with_additional_info::<T>(page_id, use_latest_version)
            .await?
            .page)
    }
}

impl SnapshotPagesData {
    pub async fn get_page_with_additional_info<T: Page>(
        &mut self,
        page_id: u32,
        use_latest_version: bool,
    ) -> Result<PageWithAdditionalInfo<Pin<&mut T>>> {
        // check for header page (return header single instance)
        //TODO(upstream): remove this
        if page_id == 0 {
            panic!("Header page not supported")
            /*
            return Ok(PageWithAdditionalInfo::<&mut T> {
                page: (self.header as &mut dyn Page).downcast_mut().unwrap(),
                origin: None,
                position: 0,
                wal_version: 0,
            });
            // */
        }

        // TODO: I want to be like below but borrow checker doesn't allow us so we use double query
        //let result = if let Some(page) = self.local_pages.get_mut(&page_id) {
        if self.local_pages.contains_key(&page_id) {
            let page = self.local_pages.get_mut(&page_id).unwrap();
            Ok(PageWithAdditionalInfo {
                page: page.as_mut().downcast_mut_pin().unwrap(),
                origin: None,
                position: 0,
                wal_version: 0,
            })
        } else {
            // if page is not in local cache, get from disk (log/wal/data)
            let page = self.read_page::<T>(page_id, use_latest_version).await?;

            let page_box = self
                .local_pages
                .entry(page_id)
                .insert_entry(Box::pin(page.page))
                .into_mut();
            // It must success because we just have inserted the Box<T>
            let page_box = page_box.as_mut().downcast_mut_pin::<T>().unwrap();
            self.trans_pages.borrow_mut().transaction_size += 1;

            Ok(PageWithAdditionalInfo {
                page: page_box,
                origin: page.origin,
                position: page.position,
                wal_version: page.wal_version,
            })
        }
    }

    async fn read_page<T: Page>(
        &mut self,
        page_id: u32,
        use_latest_version: bool,
    ) -> Result<PageWithAdditionalInfo<T>> {
        // if not inside local pages can be a dirty page saved in log file
        let wal_position = self.trans_pages.borrow().dirty_pages.get(&page_id).copied();
        if let Some(wal_position) = wal_position {
            // read page from log file if exists
            // TODO: use read_page when read only snapshot
            let buffer = self
                .disk
                .get_reader()
                .read_writable_page(wal_position.position(), FileOrigin::Log)
                .await?;
            let dirty = T::load(buffer)?;

            debug_assert!(dirty.as_ref().transaction_id() == self.transaction_id);

            return Ok(PageWithAdditionalInfo {
                page: dirty,
                origin: Some(FileOrigin::Log),
                position: wal_position.position(),
                wal_version: self.read_version,
            });
        }

        // now, look inside wal-index
        let (wal_version, pos) = self
            .wal_index
            .get_page_index(
                page_id,
                if use_latest_version {
                    i32::MAX
                } else {
                    self.read_version
                },
            )
            .await;

        if pos != u64::MAX {
            // TODO: use read_page when read only snapshot
            let buffer = self
                .disk
                .get_reader()
                .read_writable_page(pos, FileOrigin::Log)
                .await?;
            let mut log_page = T::load(buffer)?;

            log_page.as_mut().set_transaction_id(0);
            log_page.as_mut().set_confirmed(false);

            Ok(PageWithAdditionalInfo {
                page: log_page,
                origin: Some(FileOrigin::Log),
                position: pos,
                wal_version,
            })
        } else {
            // for last chance, look inside original disk data file
            let page_position = BasePage::get_page_position(page_id);

            let buffer = self
                .disk
                .get_reader()
                .read_writable_page(page_position, FileOrigin::Data)
                .await?;
            let data_page = T::load(buffer)?;

            debug_assert!(
                data_page.as_ref().transaction_id() != 0 || !data_page.as_ref().is_confirmed()
            );

            Ok(PageWithAdditionalInfo {
                page: data_page,
                origin: Some(FileOrigin::Data),
                position: page_position,
                wal_version,
            })
        }
    }
}

impl SnapshotDataPages<'_> {
    pub async fn get_free_data_page(&mut self, length: usize) -> Result<Pin<&mut DataPage>> {
        let length = length + BasePage::SLOT_SIZE;

        let start_slot = DataPage::get_minimum_index_slot(length);

        for current_slot in (0..=start_slot).rev() {
            let free_page_id = self.free_data_page_list[current_slot as usize];
            if free_page_id == u32::MAX {
                continue;
            }

            let mut page = inner!(self)
                .get_page::<DataPage>(free_page_id, false)
                .await?;

            debug_assert_eq!(
                page.page_list_slot() as i32,
                current_slot,
                "stored slot must be same as called"
            );
            debug_assert!(page.free_bytes() >= length, "free bytes must be enough");

            page.set_dirty();

            return Ok(page);
        }

        inner!(self).new_page::<DataPage>().await
    }
}

impl SnapshotIndexPages<'_> {
    pub async fn get_free_index_page(
        &mut self,
        length: usize,
        free_index_page_list: u32,
    ) -> Result<Pin<&mut IndexPage>> {
        let page;

        if free_index_page_list == u32::MAX {
            page = inner!(self).new_page::<IndexPage>().await?;
        } else {
            page = inner!(self)
                .get_page::<IndexPage>(free_index_page_list, false)
                .await?;

            assert!(page.free_bytes() >= length, "free bytes must be enough");
            assert_eq!(page.page_list_slot(), 0, "should be sloot #0");
        }

        Ok(page)
    }
}

impl SnapshotPages {
    pub async fn new_page<T: Page>(&mut self) -> Result<Pin<&mut T>> {
        let page_id;
        let buffer;

        {
            let mut header = self.header.lock().await;

            let free_empty_page_list = header.free_empty_page_list();
            if free_empty_page_list != u32::MAX {
                let free = self
                    .data
                    .get_page_with_additional_info::<BasePage>(free_empty_page_list, true)
                    .await?
                    .page;
                page_id = free.page_id();
                let free = self.data.local_pages.remove(&page_id).unwrap();
                let mut free = match free.downcast_pin::<BasePage>() {
                    Ok(page) => page,
                    Err(_) => unreachable!("the cast should not fail"),
                };
                //

                assert_eq!(
                    free.page_type(),
                    PageType::Empty,
                    "empty page must be defined as empty type ({page_id})"
                );

                header.set_free_empty_page_list(free.next_page_id());

                free.set_next_page_id(u32::MAX);

                //page_id = free.page_id(); //assigned above
                buffer = Pin::into_inner(free).into_buffer();
            } else {
                let new_length = (header.last_page_id() as usize + 1) * PAGE_SIZE;
                if new_length > header.pragmas().limit_size() as usize {
                    return Err(Error::size_limit_reached());
                }

                let save_point = header.save_point();

                page_id = save_point.header.last_page_id() + 1;
                save_point.header.set_last_page_id(page_id);

                buffer = self.data.disk.get_reader().new_page();
                forget(save_point);
            }

            self.data.trans_pages.borrow_mut().add_new_page(page_id);
        }

        let mut page = T::new(buffer, page_id);
        page.as_mut()
            .set_col_id(self.data.collection_page_id.unwrap_or(page_id));
        page.as_mut().set_dirty();

        self.data.trans_pages.borrow_mut().transaction_size += 1;

        if page.as_ref().page_type() != PageType::Collection {
            let page = self
                .data
                .local_pages
                .entry(page_id)
                .insert_entry(Box::pin(page))
                .into_mut()
                .as_mut()
                .downcast_mut_pin::<T>()
                .unwrap();

            Ok(page)
        } else {
            // UNSAFE: for collection page, leak
            Ok(unsafe { Pin::new_unchecked(Box::leak(Box::new(page))) })
        }
    }
}

impl SnapshotDataPages<'_> {
    pub async fn add_or_remove_free_data_list(&mut self, page_id: u32) -> Result<()> {
        // TODO: safety with partial borrow
        let mut page = inner!(self).get_page::<DataPage>(page_id, false).await?;
        let page = unsafe {
            std::mem::transmute::<&mut DataPage, &mut DataPage>(std::ops::DerefMut::deref_mut(
                &mut page,
            ))
        };
        let new_slot = DataPage::free_index_slot(page.free_bytes());
        let initial_slot = page.page_list_slot();

        // there is no slot change - just exit (no need any change) [except if has no more items]
        if new_slot == initial_slot && page.items_count() > 0 {
            return Ok(());
        }
        //let collection_page = self.collection_page.as_mut().unwrap().as_mut().get_mut();

        // remove from intial slot
        #[allow(clippy::collapsible_if)]
        if initial_slot != u8::MAX {
            inner!(self)
                .remove_free_list(
                    page,
                    &mut self.free_data_page_list[initial_slot as usize],
                    self.dirty,
                )
                .await?;
        }

        // if there is no items, delete page
        if page.items_count() == 0 {
            inner!(self).delete_page(page);
        } else {
            // add into current slot
            inner!(self)
                .add_free_list(page, &mut self.free_data_page_list[new_slot as usize])
                .await?;
            self.dirty.set();

            page.set_page_list_slot(new_slot);
        }

        Ok(())
    }
}

impl SnapshotIndexPages<'_> {
    pub async fn add_or_remove_free_index_list(
        &mut self,
        page: SendPtr<IndexPage>,
        start_page_id: &mut u32,
    ) -> Result<()> {
        let page = unsafe { &mut *page.0 };
        let new_slot = IndexPage::free_index_slot(page.free_bytes());
        let is_on_list = page.page_list_slot() == 0;
        let must_keep = new_slot == 0;

        // first, test if page should be deleted
        if page.items_count() == 0 {
            #[allow(clippy::collapsible_if)]
            if is_on_list {
                inner!(self)
                    .remove_free_list(page, start_page_id, self.dirty)
                    .await?;
            }

            inner!(self).delete_page(page);
        } else {
            if is_on_list && !must_keep {
                inner!(self)
                    .remove_free_list(page, start_page_id, self.dirty)
                    .await?;
            } else if !is_on_list && must_keep {
                inner!(self).add_free_list(page, start_page_id).await?;
                self.dirty.set()
            }

            page.set_page_list_slot(new_slot);
            page.set_dirty();

            // otherwise, nothing was changed
        }

        Ok(())
    }
}

impl SnapshotPages {
    async fn add_free_list<T: Page>(
        &mut self,
        page: &mut T,
        start_page_id: &mut u32,
    ) -> Result<()> {
        assert_eq!(
            page.as_ref().prev_page_id(),
            u32::MAX,
            "only non-linked page can be added in linked list"
        );
        assert_eq!(
            page.as_ref().next_page_id(),
            u32::MAX,
            "only non-linked page can be added in linked list"
        );

        if *start_page_id != u32::MAX {
            let next = self.get_page::<T>(*start_page_id, false).await?;
            let mut next = next.as_base_mut();
            next.set_prev_page_id(page.as_ref().page_id());
            next.set_dirty();
        }

        page.as_mut().set_prev_page_id(u32::MAX);
        page.as_mut().set_next_page_id(*start_page_id);
        page.as_mut().set_dirty();

        assert!(
            page.as_ref().page_type() == PageType::Data
                || page.as_ref().page_type() == PageType::Index,
            "only data/index pages must be first on free stack"
        );

        *start_page_id = page.as_ref().page_id();

        Ok(())
    }

    async fn remove_free_list<T: Page>(
        &mut self,
        page: &mut T,
        start_page_id: &mut u32,
        dirty: &DirtyFlag,
    ) -> Result<()> {
        // fix prev page
        if page.as_ref().prev_page_id() != u32::MAX {
            let prev = self
                .get_page::<T>(page.as_ref().prev_page_id(), false)
                .await?;
            let mut prev = prev.as_base_mut();
            prev.set_next_page_id(page.as_ref().next_page_id());
            prev.set_dirty();
        }

        // fix next page
        if page.as_ref().next_page_id() != u32::MAX {
            let next = self
                .get_page::<T>(page.as_ref().next_page_id(), false)
                .await?;
            let mut next = next.as_base_mut();
            next.set_prev_page_id(page.as_ref().prev_page_id());
            next.set_dirty();
        }

        // if page is first of the list set firstPage as next page
        if *start_page_id == page.as_ref().page_id() {
            *start_page_id = page.as_ref().next_page_id();

            //debug_assert!(page.as_ref().next_page_id() == u32::MAX || self.get_page::<BasePage>(page.NextPageID).PageType != PageType.Empty, "first page on free stack must be non empty page");

            dirty.set();
            //self.collection_page.as_mut().unwrap().set_dirty()
        }

        // clear page pointer (MaxValue = not used)
        page.as_mut().set_prev_page_id(u32::MAX);
        page.as_mut().set_next_page_id(u32::MAX);
        //page.PrevPageID = page.NextPageID = uint.MaxValue;
        page.as_mut().set_dirty();

        Ok(())
    }

    fn delete_page<T: Page>(&mut self, page: &mut T) {
        assert!(
            page.as_ref().prev_page_id() == u32::MAX && page.as_ref().next_page_id() == u32::MAX,
            "before delete a page, no linked list with any another page"
        );
        assert!(
            page.as_ref().items_count() == 0
                && page.as_ref().used_bytes() == 0
                && page.as_ref().highest_index() == u8::MAX
                && page.as_ref().fragmented_bytes() == 0,
            "no items on page when delete this page"
        );
        assert!(
            page.as_ref().page_type() == PageType::Data
                || page.as_ref().page_type() == PageType::Index,
            "only data/index page can be deleted"
        );
        //DEBUG(!_collectionPage.FreeDataPageList.Any(x => x == page.PageID), "this page cann't be deleted because free data list page is linked o this page");
        //DEBUG(!_collectionPage.GetCollectionIndexes().Any(x => x.FreeIndexPageList == page.PageID), "this page cann't be deleted because free index list page is linked o this page");
        //DEBUG(page.Buffer.Slice(PAGE_HEADER_SIZE, PAGE_SIZE - PAGE_HEADER_SIZE - 1).All(0), "page content shloud be empty");

        // mark page as empty and dirty
        page.as_mut().mark_as_empty();

        if self.data.trans_pages.borrow().first_deleted_page() == u32::MAX {
            assert_eq!(
                self.data.trans_pages.borrow().deleted_pages(),
                0,
                "if has no firstDeletedPageID must has deleted pages"
            );

            // set first and last deleted page as current deleted page
            self.data
                .trans_pages
                .borrow_mut()
                .set_first_deleted_page(page.as_ref().page_id());
            self.data
                .trans_pages
                .borrow_mut()
                .set_last_deleted_page(page.as_ref().page_id());
        } else {
            assert!(
                self.data.trans_pages.borrow().deleted_pages() > 0,
                "must have at least 1 deleted page"
            );

            // set next link from current deleted page to first deleted page
            page.as_mut()
                .set_next_page_id(self.data.trans_pages.borrow().first_deleted_page());

            // and then, set this current deleted page as first page making a linked list
            self.data
                .trans_pages
                .borrow_mut()
                .set_first_deleted_page(page.as_ref().page_id());
        }

        self.data.trans_pages.borrow_mut().inc_deleted_pages();
    }
}

// region drop collection
impl Snapshot {
    pub async fn drop_collection(
        &mut self,
        mut safe_point: impl AsyncFnMut() -> Result<()>,
    ) -> Result<()> {
        //let collation = self.page_collection.header.borrow().pragmas().collation();
        //let max_items_count = self.page_collection.disk.max_items_count();

        let pages = &mut self.page_collection;
        let collection_page = self.collection_page.as_mut().unwrap();
        let trans_pages_shared = pages.data.trans_pages.clone();
        {
            let mut trans_pages = trans_pages_shared.borrow_mut();
            trans_pages.set_first_deleted_page(collection_page.page_id());
            trans_pages.set_last_deleted_page(collection_page.page_id());

            collection_page.mark_as_empty();

            trans_pages.set_deleted_pages(1);

            drop(trans_pages);
        }

        let mut index_pages = HashSet::new();

        // getting all indexes pages from all indexes
        for index in collection_page.get_collection_indexes() {
            // add head/tail (same page) to be deleted
            index_pages.insert(index.head().page_id());

            let dirty = DirtyFlag::new();
            let mut accessor = PartialIndexNodeAccessorMut::new(SnapshotIndexPages {
                inner: SendPtr(pages as *mut _),
                dirty: &dirty,
                _phantom: PhantomData,
            });
            for node in
                IndexService::find_all_accessor(&mut accessor, index, Order::Ascending).await?
            {
                index_pages.insert(unsafe { &*node.page_ptr().0 }.page_id());
                safe_point().await?;
            }
        }

        // now, mark all pages as deleted
        for page_id in index_pages {
            let page = pages.get_page::<IndexPage>(page_id, true).await?;
            let mut page = page.as_base_mut();

            // mark page as delete and fix deleted page list
            page.mark_as_empty();

            {
                let mut trans_pages = trans_pages_shared.borrow_mut();
                page.set_next_page_id(trans_pages.first_deleted_page());
                trans_pages.set_first_deleted_page(page.page_id());
                trans_pages.inc_deleted_pages();
            }

            safe_point().await?;
        }

        // adding all data pages
        for start_page_id in collection_page.free_data_page_list {
            let mut next = start_page_id;

            while next != u32::MAX {
                let mut page = pages.get_page::<DataPage>(next, false).await?;

                next = page.next_page_id();

                // mark page as delete and fix deleted page list
                page.mark_as_empty();

                {
                    let mut trans_pages = trans_pages_shared.borrow_mut();
                    page.set_next_page_id(trans_pages.first_deleted_page());
                    trans_pages.set_first_deleted_page(page.page_id());
                    trans_pages.inc_deleted_pages();
                }

                safe_point().await?;
            }
        }

        // remove collection name (in header) at commit time
        let collection_name = self.collection_name.clone();
        let mut trans_pages = trans_pages_shared.borrow_mut();
        trans_pages.on_commit(move |h| h.delete_collection(&collection_name));

        Ok(())
    }
}
// rust lifetime utilities
impl Snapshot {
    pub fn pages(&mut self) -> &mut SnapshotPages {
        &mut self.page_collection
    }

    pub fn as_parts(&mut self) -> SnapshotParts {
        let collection_page = self.collection_page.as_mut().unwrap().as_mut().get_mut();
        let page_collection_pointer = &mut self.page_collection as *mut _;
        SnapshotParts {
            data_pages: SnapshotDataPages {
                inner: SendPtr(page_collection_pointer),
                dirty: &collection_page.base.dirty,
                free_data_page_list: &mut collection_page.free_data_page_list,
                _phantom: PhantomData,
            },
            index_pages: SnapshotIndexPages {
                inner: SendPtr(page_collection_pointer),
                dirty: &collection_page.base.dirty,
                _phantom: PhantomData,
            },
            collection_page: CollectionIndexesMut::new(
                &mut collection_page.indexes,
                &collection_page.base.dirty,
            ),
        }
    }
}
pub(crate) struct SnapshotParts<'a> {
    pub data_pages: SnapshotDataPages<'a>,
    pub index_pages: SnapshotIndexPages<'a>,
    pub collection_page: CollectionIndexesMut<'a>,
}

impl SnapshotDataPages<'_> {
    pub async fn get_page(&mut self, page_id: u32) -> Result<Pin<&mut DataPage>> {
        inner!(self).get_page(page_id, false).await
    }

    #[allow(dead_code)]
    pub async fn new_page(&mut self) -> Result<Pin<&mut DataPage>> {
        inner!(self).new_page().await
    }
}
impl SnapshotIndexPages<'_> {
    pub async fn get_page(&mut self, page_id: u32) -> Result<Pin<&mut IndexPage>> {
        inner!(self).get_page(page_id, false).await
    }

    pub async fn new_page(&mut self) -> Result<Pin<&mut IndexPage>> {
        inner!(self).new_page().await
    }
}

pub(crate) struct PageWithAdditionalInfo<T> {
    pub page: T,
    pub origin: Option<FileOrigin>,
    pub position: u64,
    pub wal_version: i32,
}
