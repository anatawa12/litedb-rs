use crate::engine::collection_service::CollectionService;
use crate::engine::disk::DiskService;
use crate::engine::lock_service::{CollectionLockScope, LockService};
use crate::engine::pages::HeaderPage;
use crate::engine::transaction_pages::TransactionPages;
use crate::engine::transaction_service::LockMode;
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{
    BasePage, CollectionPage, DataPage, FileOrigin, IndexPage, PAGE_SIZE, Page, PageType,
    StreamFactory,
};
use crate::utils::Shared;
use crate::{Error, Result};
use std::collections::HashMap;
use std::mem::forget;

pub(crate) struct Snapshot<'engine, SF: StreamFactory> {
    header: &'engine mut HeaderPage,
    lock_scope: Option<CollectionLockScope>,
    disk: &'engine mut DiskService<SF>,
    wal_index: &'engine WalIndexService,

    transaction_id: u32,
    trans_pages: Shared<TransactionPages>,

    read_version: i32,
    mode: LockMode,
    collection_name: String,
    collection_page: Option<CollectionPage>,

    local_pages: HashMap<u32, Box<dyn Page>>,
}

impl<'engine, SF: StreamFactory> Snapshot<'engine, SF> {
    pub async fn new(
        mode: LockMode,
        collection_name: &str,
        header: &'engine mut HeaderPage,
        transaction_id: u32,
        trans_pages: Shared<TransactionPages>,
        locker: &'engine LockService,
        wal_index: &'engine WalIndexService,
        disk: &'engine mut DiskService<SF>,
        add_if_not_exists: bool,
    ) -> Result<Self> {
        let lock_scope = if mode == LockMode::Write {
            Some(locker.enter_lock(collection_name).await)
        } else {
            None
        };

        let read_version = wal_index.current_read_version().await;

        let mut snapshot = Self {
            header,
            lock_scope,
            disk,
            wal_index,
            transaction_id,
            trans_pages,
            read_version,
            mode,
            collection_name: collection_name.to_string(),
            collection_page: None,
            local_pages: HashMap::new(),
        };

        let mut srv = CollectionService::new(&mut snapshot);

        let (_, collection_page) = srv.get(collection_name, add_if_not_exists).await?;

        // replace with owned one
        let collection_page = if let Some(collection_page) = collection_page {
            let collection_page_id = collection_page.page_id();
            Some(
                *snapshot
                    .local_pages
                    .remove(&collection_page_id)
                    .unwrap()
                    .downcast::<CollectionPage>()
                    .ok()
                    .unwrap(),
            )
        } else {
            None
        };

        snapshot.collection_page = collection_page;

        Ok(snapshot)
    }

    pub fn header(&mut self) -> &mut HeaderPage {
        self.header
    }

    pub fn trans_pages(&self) -> &Shared<TransactionPages> {
        &self.trans_pages
    }

    pub fn disk(&mut self) -> &mut DiskService<SF> {
        self.disk
    }

    pub fn wal_index(&mut self) -> &WalIndexService {
        self.wal_index
    }

    pub fn mode(&self) -> LockMode {
        self.mode
    }

    pub fn collection_name(&self) -> &str {
        &self.collection_name
    }

    pub fn collection_page(&self) -> Option<&CollectionPage> {
        self.collection_page.as_ref()
    }

    pub fn collection_page_mut(&mut self) -> Option<&mut CollectionPage> {
        self.collection_page.as_mut()
    }

    pub fn local_pages(&self) -> impl Iterator<Item = &dyn Page> {
        self.local_pages.values().map(AsRef::as_ref)
    }

    pub fn read_version(&self) -> i32 {
        self.read_version
    }

    pub fn get_writable_pages(
        &self,
        dirty: bool,
        with_collection_page: bool,
    ) -> impl Iterator<Item = &dyn Page> {
        let x = if self.mode != LockMode::Write {
            None
        } else {
            let pages = self
                .local_pages
                .values()
                .filter(move |p| p.as_ref().as_ref().is_dirty() == dirty)
                .map(|p| p.as_ref());
            let collection_page = self
                .collection_page
                .as_ref()
                .take_if(|p| with_collection_page && p.is_dirty() == dirty)
                .map(|x| -> &dyn Page { x });
            let collection_page = collection_page.into_iter();

            Some(pages.chain(collection_page))
        };

        x.into_iter().flatten()
    }

    pub fn writable_pages_removing(
        &mut self,
        dirty: bool,
        with_collection_page: bool,
    ) -> impl Iterator<Item = Box<dyn Page>> {
        let x = if self.mode != LockMode::Write {
            None
        } else {
            let page_ids = self
                .local_pages
                .iter()
                .filter(|(_, p)| p.as_ref().as_ref().is_dirty() == dirty)
                .map(|(&k, _)| k)
                .collect::<Vec<_>>();
            let pages = page_ids
                .into_iter()
                .map(|k| self.local_pages.remove(&k).unwrap());
            let collection_page = self
                .collection_page
                .take_if(|p| with_collection_page && p.is_dirty() == dirty)
                .map(|x| -> Box<dyn Page> { Box::new(x) });
            let collection_page = collection_page.into_iter();

            Some(pages.chain(collection_page))
        };

        x.into_iter().flatten()
    }

    pub fn get_writable_pages_mut(
        &mut self,
        dirty: bool,
        with_collection_page: bool,
    ) -> impl Iterator<Item = &mut dyn Page> {
        let x = if self.mode != LockMode::Write {
            None
        } else {
            let pages = self
                .local_pages
                .values_mut()
                .filter(move |p| p.as_ref().as_ref().is_dirty() == dirty)
                .map(|p| p.as_mut());
            let collection_page = self
                .collection_page
                .as_mut()
                .take_if(|p| with_collection_page && p.is_dirty() == dirty)
                .map(|x| -> &mut dyn Page { &mut *x });
            let collection_page = collection_page.into_iter();

            Some(pages.chain(collection_page))
        };

        x.into_iter().flatten()
    }

    pub fn clear(&mut self) {
        self.local_pages.clear();
    }
}

// region Page Version functions
impl<SF: StreamFactory> Snapshot<'_, SF> {
    pub async fn get_page<T: Page>(
        &mut self,
        page_id: u32,
        use_latest_version: bool, /* = false*/
    ) -> Result<&mut T> {
        Ok(self
            .get_page_with_additional_info::<T>(page_id, use_latest_version)
            .await?
            .page)
    }

    pub async fn get_page_with_additional_info<T: Page>(
        &mut self,
        page_id: u32,
        use_latest_version: bool,
    ) -> Result<PageWithAdditionalInfo<&mut T>> {
        assert!(page_id != u32::MAX && page_id < self.header.last_page_id());

        // check for header page (return header single instance)
        //TODO(upstream): remove this
        if page_id == 0 {
            return Ok(PageWithAdditionalInfo::<&mut T> {
                page: (self.header as &mut dyn Page).downcast_mut().unwrap(),
                origin: None,
                position: 0,
                wal_version: 0,
            });
        }

        // TODO: I want to be like below but borrow checker doesn't allow us so we use double query
        //let result = if let Some(page) = self.local_pages.get_mut(&page_id) {
        if self.local_pages.contains_key(&page_id) {
            let page = self.local_pages.get_mut(&page_id).unwrap();
            Ok(PageWithAdditionalInfo::<&mut T> {
                page: page.downcast_mut().unwrap(),
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
                .insert_entry(Box::new(page.page))
                .into_mut();
            // It must success because we just have inserted the Box<T>
            let page_box = page_box.downcast_mut::<T>().unwrap();
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
                .await?
                .read_writable_page(wal_position.position(), FileOrigin::Log)
                .await?;
            let dirty = T::load(buffer)?;

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
                .await?
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
                .await?
                .read_writable_page(page_position, FileOrigin::Data)
                .await?;
            let data_page = T::load(buffer)?;

            Ok(PageWithAdditionalInfo {
                page: data_page,
                origin: Some(FileOrigin::Data),
                position: page_position,
                wal_version,
            })
        }
    }

    async fn get_free_data_page(&mut self, length: i32) -> Result<&mut DataPage> {
        let length = length as usize + BasePage::SLOT_SIZE;

        let start_slot = DataPage::get_minimum_index_slot(length);
        let collection_page = self.collection_page.as_ref().unwrap();

        for current_slot in (0..=start_slot).rev() {
            let free_page_id = collection_page.free_data_page_list[current_slot as usize];
            if free_page_id == u32::MAX {
                continue;
            }

            let page = self.get_page::<DataPage>(free_page_id, false).await?;

            debug_assert_eq!(
                page.page_list_slot() as i32,
                current_slot,
                "stored slot must be same as called"
            );
            debug_assert!(page.free_bytes() >= length, "free bytes must be enough");

            page.set_dirty();

            return Ok(page);
        }

        self.new_page::<DataPage>().await
    }

    pub async fn get_free_index_page(
        &mut self,
        length: usize,
        free_index_page_list: u32,
    ) -> Result<&mut IndexPage> {
        let page;

        if free_index_page_list == u32::MAX {
            page = self.new_page::<IndexPage>().await?;
        } else {
            page = self
                .get_page::<IndexPage>(free_index_page_list, false)
                .await?;

            assert!(page.free_bytes() >= length, "free bytes must be enough");
            assert_eq!(page.page_list_slot(), 0, "should be sloot #0");
        }

        Ok(page)
    }

    pub async fn new_page<T: Page>(&mut self) -> Result<&mut T> {
        if self.collection_page.is_none() {
            debug_assert_eq!(
                std::any::TypeId::of::<T>(),
                std::any::TypeId::of::<CollectionPage>()
            );
        }
        if std::any::TypeId::of::<T>() == std::any::TypeId::of::<CollectionPage>() {
            debug_assert!(self.collection_page.is_none())
        }

        let page_id;
        let buffer;

        // no locks

        if self.header.free_empty_page_list() != u32::MAX {
            let free = self
                .get_page::<BasePage>(self.header.free_empty_page_list(), true)
                .await?;
            page_id = free.page_id();
            let free = self.local_pages.remove(&page_id).unwrap();
            let mut free = match free.downcast::<BasePage>() {
                Ok(page) => page,
                Err(_) => unreachable!("the cast should not fail"),
            };
            //

            assert_eq!(
                free.page_type(),
                PageType::Empty,
                "empty page must be defined as empty type"
            );

            self.header.set_free_empty_page_list(free.next_page_id());

            free.set_next_page_id(u32::MAX);

            //page_id = free.page_id(); //assigned above
            buffer = free.into_buffer();
        } else {
            let new_length = (self.header.last_page_id() as usize + 1) * PAGE_SIZE;
            if new_length > self.header.pragmas().limit_size() as usize {
                return Err(Error::size_limit_reached());
            }

            let save_point = self.header.save_point();

            page_id = save_point.header.last_page_id() + 1;
            save_point.header.set_last_page_id(page_id);

            buffer = self.disk.get_reader().await?.new_page();
            forget(save_point);
        }

        self.trans_pages.borrow_mut().add_new_page(page_id);

        let mut page = T::new(buffer, page_id);
        page.as_mut().set_col_id(
            self.collection_page
                .as_ref()
                .map(|x| x.page_id())
                .unwrap_or(page_id),
        );
        page.as_mut().set_dirty();

        self.trans_pages.borrow_mut().transaction_size += 1;

        if page.as_ref().page_type() != PageType::Collection {
            let page = self
                .local_pages
                .entry(page_id)
                .insert_entry(Box::new(page))
                .into_mut()
                .as_mut()
                .downcast_mut::<T>()
                .unwrap();

            Ok(page)
        } else {
            // UNSAFE: for collection page, leak
            Ok(Box::leak(Box::new(page)))
        }
    }
}

pub(crate) struct PageWithAdditionalInfo<T> {
    pub page: T,
    pub origin: Option<FileOrigin>,
    pub position: u64,
    pub wal_version: i32,
}
