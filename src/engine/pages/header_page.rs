use crate::bson;
use crate::engine::buffer_reader::BufferReader;
use crate::engine::buffer_writer::BufferWriter;
use crate::engine::engine_pragmas::EnginePragmas;
use crate::engine::pages::PageType;
use crate::engine::pages::base_page::BasePage;
use crate::engine::{Page, PageBuffer};
use crate::{Error, Result};
use std::ops::{Deref, DerefMut};

const HEADER_INFO: &[u8] = b"** This is a LiteDB file **";
const FILE_VERSION: u8 = 8;

const P_HEADER_INFO: usize = 32; // 32-58 (27 bytes)
const P_FILE_VERSION: usize = 59; // 59-59 (1 byte)
const P_FREE_EMPTY_PAGE_ID: usize = 60; // 60-63 (4 bytes)
const P_LAST_PAGE_ID: usize = 64; // 64-67 (4 bytes)
const P_CREATION_TIME: usize = 68; // 68-75 (8 bytes)

const P_PRAGMAS: usize = 76; // 76-190 (115 bytes)
const P_INVALID_DATAFILE_STATE: usize = 191; // 191-191 (1 byte)

const P_COLLECTIONS: usize = 192; // 192-8159 (8064 bytes)
const COLLECTIONS_SIZE: usize = 8000; // 250 blocks with 32 bytes each

pub(crate) struct HeaderPage {
    base: BasePage,

    creation_time: bson::DateTime,
    free_empty_page_list: u32,
    last_page_id: u32,
    pragmas: EnginePragmas,
    collections: bson::Document,

    collections_changed: bool,
}

impl HeaderPage {
    pub const P_INVALID_DATAFILE_STATE: usize = P_INVALID_DATAFILE_STATE;

    pub(crate) fn new(buffer: Box<PageBuffer>) -> Self {
        let mut header = HeaderPage {
            base: BasePage::new(buffer, 0, PageType::Header),
            creation_time: bson::DateTime::now(),
            free_empty_page_list: 0,
            last_page_id: 0,
            pragmas: EnginePragmas::default(),
            collections: bson::Document::new(),

            collections_changed: false,
        };

        let buffer = header.base.buffer_mut();
        buffer.write_bytes(P_HEADER_INFO, HEADER_INFO);
        buffer.write_byte(P_FILE_VERSION, FILE_VERSION);
        buffer.write_date_time(P_CREATION_TIME, header.creation_time);

        header
    }

    pub fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        let mut header = HeaderPage {
            base: BasePage::load(buffer)?,
            creation_time: bson::DateTime::now(),
            free_empty_page_list: 0,
            last_page_id: 0,
            pragmas: EnginePragmas::default(),
            collections: bson::Document::new(),

            collections_changed: false,
        };

        header.load_header_page()?;

        Ok(header)
    }

    // instead of recreating, reload header page
    pub fn reload_fully(&mut self) -> Result<()> {
        self.base.reload_fully()?;
        self.load_header_page()?;
        Ok(())
    }

    fn load_header_page(&mut self) -> Result<()> {
        let buffer = &self.base.buffer();

        let info = buffer.read_bytes(P_HEADER_INFO, HEADER_INFO.len());
        let version = buffer.read_byte(P_FILE_VERSION);

        if info != HEADER_INFO || version != FILE_VERSION {
            return Err(Error::invalid_database());
        }

        self.creation_time = buffer.read_date_time(P_CREATION_TIME)?;

        self.free_empty_page_list = buffer.read_u32(P_FREE_EMPTY_PAGE_ID);
        self.last_page_id = buffer.read_u32(P_LAST_PAGE_ID);

        self.pragmas = EnginePragmas::read(buffer)?;
        let area = buffer.slice(P_COLLECTIONS, COLLECTIONS_SIZE);
        self.collections = BufferReader::new(area).read_document()?;

        Ok(())
    }

    pub fn update_buffer(&mut self) -> &PageBuffer {
        let buffer = self.base.buffer_mut();

        buffer.write_u32(P_FREE_EMPTY_PAGE_ID, self.free_empty_page_list);
        buffer.write_u32(P_LAST_PAGE_ID, self.last_page_id);
        self.pragmas.update_buffer(buffer);

        if self.collections_changed {
            let area = buffer.slice_mut(P_COLLECTIONS, COLLECTIONS_SIZE);

            let mut writer = BufferWriter::new(area);
            writer.write_document(&self.collections);

            self.collections_changed = false;
        }

        self.base.update_buffer()
    }

    pub fn pragmas(&self) -> &EnginePragmas {
        &self.pragmas
    }

    pub fn pragmas_mut(&mut self) -> &mut EnginePragmas {
        &mut self.pragmas
    }

    pub fn free_empty_page_list(&self) -> u32 {
        self.free_empty_page_list
    }

    pub fn set_free_empty_page_list(&mut self, page_id: u32) {
        self.free_empty_page_list = page_id;
    }

    pub fn last_page_id(&self) -> u32 {
        self.last_page_id
    }

    pub fn set_last_page_id(&mut self, page_id: u32) {
        self.last_page_id = page_id;
    }

    // TODO: create RAII struct for save_point and resore pair
    pub fn save_point(&mut self) -> Box<PageBuffer> {
        self.update_buffer();

        let mut save_point = Box::new(PageBuffer::new(0));

        *save_point.buffer_mut() = *self.buffer().buffer();

        save_point
    }

    pub fn restore(&mut self, save_point: &PageBuffer) {
        *self.buffer_mut().buffer_mut() = *save_point.buffer();
        // The original 
        self.load_header_page().expect("failed to load save_point page");
    }

    pub fn get_collection_page_id(&self, collection: &str) -> u32 {
        self.collections
            .get(collection)
            .map(|x| x.as_i32().unwrap() as u32)
            .unwrap_or(u32::MAX)
    }

    pub fn collections(&self) -> impl Iterator<Item = (&str, u32)> {
        self.collections
            .iter()
            .map(|(k, v)| (k, v.as_i32().unwrap() as u32))
    }

    pub fn insert_collection(&mut self, collection: &str, page_id: u32) {
        self.collections
            .insert(collection.to_string(), page_id as i32);
        self.collections_changed = true;
    }

    pub fn delete_collection(&mut self, collection: &str) {
        self.collections.remove(collection);
        self.collections_changed = true;
    }

    pub fn rename_collection(&mut self, old_name: &str, new_name: &str) {
        let page_id = self.collections.remove(old_name).unwrap();
        self.collections.insert(new_name.to_string(), page_id);
        self.collections_changed = true;
    }

    pub fn get_available_collection_space(&self) -> usize {
        COLLECTIONS_SIZE - self.collections.get_serialized_value_len()
            - 1 // for int32 type (0x10)
            - 1 // for new CString ('\0')
            - 4 // for PageID (int32)
            - 8 // reserved
    }
}

impl Deref for HeaderPage {
    type Target = BasePage;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl DerefMut for HeaderPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl AsRef<BasePage> for HeaderPage {
    fn as_ref(&self) -> &BasePage {
        &self.base
    }
}

impl AsMut<BasePage> for HeaderPage {
    fn as_mut(&mut self) -> &mut BasePage {
        &mut self.base
    }
}

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
