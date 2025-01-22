use std::env::var;
use bson::DateTime;
use crate::engine::PageBuffer;
use crate::{Error, Result};
use crate::engine::buffer_reader::BufferReader;
use crate::engine::engine_pragmas::EnginePragmas;
use crate::engine::pages::base_page::BasePage;

const HEADER_INFO: &[u8] = b"** This is a LiteDB file **";
const FILE_VERSION: u8 = 8;

const P_HEADER_INFO: usize = 32;  // 32-58 (27 bytes)
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

    creation_time: DateTime,
    free_empty_page_list: u32,
    last_page_id: u32,
    pragmas: EnginePragmas,
    collections: bson::Document,
}

impl HeaderPage {
    pub fn load(buffer: PageBuffer) -> Result<Self> {
        let mut header = HeaderPage {
            base: BasePage::load(buffer)?,
            creation_time: DateTime::now(),
            free_empty_page_list: 0,
            last_page_id: 0,
            pragmas: EnginePragmas::default(),
            collections: bson::Document::new(),
        };

        header.load_header_page()?;

        Ok(header)
    }

    fn load_header_page(&mut self) -> Result<()> {
        let buffer = &self.base.buffer;

        let info = buffer.read_bytes(P_HEADER_INFO, HEADER_INFO.len());
        let version = buffer.read_byte(P_FILE_VERSION);

        if info != HEADER_INFO || version != FILE_VERSION {
            return Err(Error::invalid_database());
        }

        self.creation_time = buffer.read_date_time(P_CREATION_TIME);

        self.free_empty_page_list = buffer.read_u32(P_FREE_EMPTY_PAGE_ID);
        self.last_page_id = buffer.read_u32(P_LAST_PAGE_ID);

        self.pragmas = EnginePragmas::read(&buffer)?;
        let area = buffer.slice(P_COLLECTIONS, COLLECTIONS_SIZE);
        self.collections = BufferReader::new(area).read_document()?;

        Ok(())
    }
}
