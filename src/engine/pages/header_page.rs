use crate::bson;
use crate::engine::buffer_reader::BufferReader;
use crate::engine::buffer_writer::BufferWriter;
use crate::engine::engine_pragmas::EnginePragmas;
use crate::engine::pages::PageType;
use crate::engine::pages::base_page::BasePage;
use crate::engine::{DirtyFlag, PageBuffer};
use crate::{Error, Result};
use async_lock::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard};
use std::ops::Deref;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::{Arc, Mutex as StdMutex};

const HEADER_INFO: &[u8] = b"** This is a LiteDB file **";
const FILE_VERSION: u8 = 8;

const P_HEADER_INFO: usize = 32; // 32-58 (27 bytes)
const P_FILE_VERSION: usize = 59; // 59-59 (1 byte)
const P_FREE_EMPTY_PAGE_ID: usize = 60; // 60-63 (4 bytes)
const P_LAST_PAGE_ID: usize = 64; // 64-67 (4 bytes)
const P_CREATION_TIME: usize = 68; // 68-75 (8 bytes)

//const P_PRAGMAS: usize = 76; // 76-190 (115 bytes)
const P_INVALID_DATAFILE_STATE: usize = 191; // 191-191 (1 byte)

const P_COLLECTIONS: usize = 192; // 192-8159 (8064 bytes)
const COLLECTIONS_SIZE: usize = 8000; // 250 blocks with 32 bytes each

pub(crate) struct HeaderPage {
    lock: AsyncMutex<BasePage>,
    inner: HeaderPageInner,
}

struct HeaderPageInner {
    creation_time: AtomicU64,
    pragmas: Arc<EnginePragmas>,
    // RustChange: we use mutex for safety, upstream may have concurrent issue
    collections: StdMutex<bson::Document>,
    last_page_id: AtomicU32,
    free_empty_page_list: AtomicU32,

    collections_changed: DirtyFlag,
}

pub(crate) struct HeaderPageLocked<'a> {
    page: &'a HeaderPage,
    base_page: AsyncMutexGuard<'a, BasePage>,
}

impl HeaderPage {
    pub const P_INVALID_DATAFILE_STATE: usize = P_INVALID_DATAFILE_STATE;

    pub(crate) fn new(buffer: Box<PageBuffer>) -> Self {
        let mut header = HeaderPage {
            lock: AsyncMutex::new(BasePage::new(buffer, 0, PageType::Header)),

            inner: HeaderPageInner {
                creation_time: bson::DateTime::now().ticks().into(),
                free_empty_page_list: 0.into(),
                last_page_id: 0.into(),
                pragmas: Arc::new(EnginePragmas::default()),
                collections: StdMutex::new(bson::Document::new()),

                collections_changed: DirtyFlag::new(),
            },
        };

        let buffer = header.lock.get_mut().buffer_mut();
        buffer.write_bytes(P_HEADER_INFO, HEADER_INFO);
        buffer.write_byte(P_FILE_VERSION, FILE_VERSION);
        buffer.write_date_time(
            P_CREATION_TIME,
            bson::DateTime::from_ticks(header.inner.creation_time.load(Relaxed)).unwrap(),
        );

        header
    }

    pub fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        let mut header = HeaderPage {
            lock: AsyncMutex::new(BasePage::load(buffer)?),

            inner: HeaderPageInner {
                creation_time: bson::DateTime::now().ticks().into(),
                free_empty_page_list: 0.into(),
                last_page_id: 0.into(),
                pragmas: Arc::new(EnginePragmas::default()),
                collections: StdMutex::new(bson::Document::new()),

                collections_changed: DirtyFlag::new(),
            },
        };

        header
            .inner
            .load_header_page(header.lock.get_mut().buffer())?;

        Ok(header)
    }

    // instead of recreating, reload header page
    pub fn reload_fully(&mut self) -> Result<()> {
        self.lock.get_mut().reload_fully()?;
        self.inner.load_header_page(self.lock.get_mut().buffer())?;
        Ok(())
    }
}

impl HeaderPageInner {
    fn load_header_page(&self, buffer: &PageBuffer) -> Result<()> {
        let info = buffer.read_bytes(P_HEADER_INFO, HEADER_INFO.len());
        let version = buffer.read_byte(P_FILE_VERSION);

        if info != HEADER_INFO || version != FILE_VERSION {
            return Err(Error::invalid_database());
        }

        self.creation_time
            .store(buffer.read_date_time(P_CREATION_TIME)?.ticks(), Relaxed);

        self.free_empty_page_list
            .store(buffer.read_u32(P_FREE_EMPTY_PAGE_ID), Relaxed);
        self.last_page_id
            .store(buffer.read_u32(P_LAST_PAGE_ID), Relaxed);

        self.pragmas.read(buffer)?;
        let area = buffer.slice(P_COLLECTIONS, COLLECTIONS_SIZE);
        *self.collections.lock().unwrap() = BufferReader::single(area).read_document()?;

        Ok(())
    }
}

impl HeaderPage {
    pub fn update_buffer(&mut self) {
        self.inner.update_buffer_impl(self.lock.get_mut())
    }
}

impl HeaderPageInner {
    fn update_buffer_impl(&self, base: &mut BasePage) {
        let buffer = base.buffer_mut();

        buffer.write_u32(
            P_FREE_EMPTY_PAGE_ID,
            self.free_empty_page_list.load(Relaxed),
        );
        buffer.write_u32(P_LAST_PAGE_ID, self.last_page_id.load(Relaxed));
        self.pragmas.update_buffer(buffer);

        if self.collections_changed.is_set() {
            let area = buffer.slice_mut(P_COLLECTIONS, COLLECTIONS_SIZE);

            let mut writer = BufferWriter::single(area);
            writer.write_document(&self.collections.lock().unwrap());

            self.collections_changed.reset()
        }

        base.update_buffer();
    }
}

impl HeaderPage {
    pub fn pragmas(&self) -> &Arc<EnginePragmas> {
        &self.inner.pragmas
    }

    pub fn free_empty_page_list(&self) -> u32 {
        self.inner.free_empty_page_list.load(Relaxed)
    }

    pub fn last_page_id(&self) -> u32 {
        self.inner.last_page_id.load(Relaxed)
    }

    pub fn get_collection_page_id(&self, collection: &str) -> u32 {
        (self.inner.collections.lock().unwrap())
            .try_get(collection)
            .map(|x| x.as_i32().unwrap() as u32)
            .unwrap_or(u32::MAX)
    }

    pub fn collection_names(&self) -> Vec<String> {
        (self.inner.collections.lock().unwrap())
            .iter()
            .map(|x| x.0.to_string())
            .collect()
    }

    pub fn get_available_collection_space(&self) -> usize {
        COLLECTIONS_SIZE - self.inner.collections.lock().unwrap().get_serialized_value_len()
            - 1 // for int32 type (0x10)
            - 1 // for new CString ('\0')
            - 4 // for PageID (int32)
            - 8 // reserved
    }

    pub async fn lock(&self) -> HeaderPageLocked {
        HeaderPageLocked {
            page: self,
            base_page: self.lock.lock().await,
        }
    }
}

impl<'a> HeaderPageLocked<'a> {
    pub fn set_free_empty_page_list(&mut self, page_id: u32) {
        self.page.inner.free_empty_page_list.store(page_id, Relaxed);
    }

    pub fn set_last_page_id(&mut self, page_id: u32) {
        self.page.inner.last_page_id.store(page_id, Relaxed);
    }

    pub fn save_point(&mut self) -> SavePointScope<'_, 'a> {
        self.page.inner.update_buffer_impl(&mut self.base_page);

        let mut save_point = Box::new(PageBuffer::new(0));

        *save_point.buffer_mut() = *self.base_page().buffer().buffer();

        SavePointScope {
            header: self,
            save_point,
        }
    }

    pub fn update_buffer(&mut self) -> &PageBuffer {
        let buffer = self.base_page.buffer_mut();

        buffer.write_u32(
            P_FREE_EMPTY_PAGE_ID,
            self.page.inner.free_empty_page_list.load(Relaxed),
        );
        buffer.write_u32(P_LAST_PAGE_ID, self.page.inner.last_page_id.load(Relaxed));
        self.page.inner.pragmas.update_buffer(buffer);

        if self.page.inner.collections_changed.is_set() {
            let area = buffer.slice_mut(P_COLLECTIONS, COLLECTIONS_SIZE);

            let mut writer = BufferWriter::single(area);
            writer.write_document(&self.page.inner.collections.lock().unwrap());

            self.page.inner.collections_changed.reset()
        }

        self.base_page.update_buffer()
    }

    pub fn base_page(&mut self) -> &mut BasePage {
        &mut self.base_page
    }

    fn restore(&mut self, save_point: &PageBuffer) {
        *self.base_page.buffer_mut().buffer_mut() = *save_point.buffer();
        // The original page must be good to parse so except here
        self.inner
            .load_header_page(self.base_page.buffer())
            .expect("failed to load save_point page");
    }

    pub fn insert_collection(&self, collection: &str, page_id: u32) {
        self.page
            .inner
            .collections
            .lock()
            .unwrap()
            .insert(collection.to_string(), page_id as i32);
        self.page.inner.collections_changed.set();
    }

    pub fn delete_collection(&self, collection: &str) {
        self.page
            .inner
            .collections
            .lock()
            .unwrap()
            .remove(collection);
        self.page.inner.collections_changed.set();
    }

    pub fn rename_collection(&self, old_name: &str, new_name: &str) {
        let mut collections = self.page.inner.collections.lock().unwrap();
        let page_id = collections.remove(old_name).unwrap();
        collections.insert(new_name.to_string(), page_id);
        self.page.inner.collections_changed.set();
    }
}

impl Deref for HeaderPageLocked<'_> {
    type Target = HeaderPage;

    fn deref(&self) -> &Self::Target {
        self.page
    }
}

/// Drops the SavePoint when the scope exits
pub(crate) struct SavePointScope<'a, 'b> {
    save_point: Box<PageBuffer>,
    pub header: &'a mut HeaderPageLocked<'b>,
}

impl Drop for SavePointScope<'_, '_> {
    fn drop(&mut self) {
        self.header.restore(&self.save_point);
    }
}

impl AsMut<BasePage> for HeaderPage {
    fn as_mut(&mut self) -> &mut BasePage {
        self.lock.get_mut()
    }
}

/*
impl Page for HeaderPage {
    fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        Self::load(buffer)
    }

    fn new(_: Box<PageBuffer>, _: u32) -> Self {
        panic!("create HeaderPage")
    }

    fn update_buffer(&mut self) -> &PageBuffer {
        self.update_buffer()
    }

    fn into_base(self: Box<Self>) -> BasePage {
        self.base
    }
}
// */
