use crate::engine::{PageBuffer, PAGE_HEADER_SIZE};
use crate::Error;
use crate::Result;

/// The common variables for each page

const SLOT_SIZE: usize = 4;

const P_PAGE_ID: usize = 0; // 00-03 [uint]
const P_PAGE_TYPE: usize = 4; // 04-04 [byte]
const P_PREV_PAGE_ID: usize = 5; // 05-08 [uint]
const P_NEXT_PAGE_ID: usize = 9; // 09-12 [uint]
const P_INITIAL_SLOT: usize = 13; // 13-13 [byte]

const P_TRANSACTION_ID: usize = 14; // 14-17 [uint]
const P_IS_CONFIRMED: usize = 18; // 18-18 [byte]
const P_COL_ID: usize = 19; // 19-22 [uint]

const P_ITEMS_COUNT: usize = 23; // 23-23 [byte]
const P_USED_BYTES: usize = 24; // 24-25 [ushort]
const P_FRAGMENTED_BYTES: usize = 26; // 26-27 [ushort]
const P_NEXT_FREE_POSITION: usize = 28; // 28-29 [ushort]
const P_HIGHEST_INDEX: usize = 30; // 30-30 [byte]

pub(crate) struct BasePage {
    // TODO: should we use a reference, or passed from caller when needed instead of storing it here?
    pub buffer: Box<PageBuffer>,
    page_id: u32,
    page_type: PageType,
    prev_page_id: u32,
    next_page_id: u32,
    page_list_slot: u8,
    transaction_id: u32,
    is_confirmed: bool,
    col_id: u32,
    items_count: u8,
    used_bytes: u16,
    fragmented_bytes: u16,
    next_free_position: u16,
    highest_index: u8,

    pub(crate) dirty: bool,
}

impl BasePage {
    pub fn new(buffer: Box<PageBuffer>, page_id: u32, page_type: PageType) -> Self {
        let mut base = BasePage {
            buffer,

            // page info
            page_id,
            page_type,
            prev_page_id: u32::MAX,
            next_page_id: u32::MAX,
            page_list_slot: u8::MAX,

            // transaction info
            transaction_id: u32::MAX,
            is_confirmed: false,
            col_id: u32::MAX,

            items_count: 0,
            used_bytes: 0,
            fragmented_bytes: 0,
            next_free_position: PAGE_HEADER_SIZE as u16,
            highest_index: u8::MAX,

            dirty: false,
        };

        base.buffer.write_u32(P_PAGE_ID, base.page_id);
        base.buffer.write_byte(P_PAGE_TYPE, page_type as u8);

        base
    }

    pub fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        // page information
        let page_id = buffer.read_u32(P_PAGE_ID);
        let page_type: PageType = buffer.read_byte(P_PAGE_TYPE).try_into()?;
        let prev_page_id = buffer.read_u32(P_PREV_PAGE_ID);
        let next_page_id = buffer.read_u32(P_NEXT_PAGE_ID);
        let page_list_slot = buffer.read_byte(P_INITIAL_SLOT);

        // transaction
        let transaction_id = buffer.read_u32(P_TRANSACTION_ID);
        let is_confirmed = buffer.read_bool(P_IS_CONFIRMED);
        let col_id = buffer.read_u32(P_COL_ID);

        // blocks
        let items_count = buffer.read_byte(P_ITEMS_COUNT);
        let used_bytes = buffer.read_u16(P_USED_BYTES);
        let fragmented_bytes = buffer.read_u16(P_FRAGMENTED_BYTES);
        let next_free_position = buffer.read_u16(P_NEXT_FREE_POSITION);
        let highest_index = buffer.read_byte(P_HIGHEST_INDEX);

        Ok(BasePage {
            buffer,
            page_id,
            page_type,
            prev_page_id,
            next_page_id,
            page_list_slot,
            transaction_id,
            is_confirmed,
            col_id,
            items_count,
            used_bytes,
            fragmented_bytes,
            next_free_position,
            highest_index,

            dirty: false,
        })
    }

    pub(crate) fn update_buffer(&mut self) -> Result<&PageBuffer> {
        let buffer = &mut self.buffer;

        assert_eq!(buffer.read_u32(P_PAGE_ID), self.page_id, "Page id cannot be changed");

        // page info
        buffer.write_u32(P_PREV_PAGE_ID, self.prev_page_id);
        buffer.write_u32(P_NEXT_PAGE_ID, self.next_page_id);
        buffer.write_byte(P_INITIAL_SLOT, self.page_list_slot);

        // transaction info
        buffer.write_u32(P_TRANSACTION_ID, self.transaction_id);
        buffer.write_bool(P_IS_CONFIRMED, self.is_confirmed);
        buffer.write_u32(P_COL_ID, self.col_id);

        // blocks
        buffer.write_byte(P_ITEMS_COUNT, self.items_count);
        buffer.write_u16(P_USED_BYTES, self.used_bytes);
        buffer.write_u16(P_FRAGMENTED_BYTES, self.fragmented_bytes);
        buffer.write_u16(P_NEXT_FREE_POSITION, self.next_free_position);
        buffer.write_byte(P_HIGHEST_INDEX, self.highest_index);

        Ok(buffer)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    Empty = 0,
    Header = 1,
    Collection = 2,
    Index = 3,
    Data = 4,
}

impl TryFrom<u8> for PageType {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(PageType::Empty),
            1 => Ok(PageType::Header),
            2 => Ok(PageType::Collection),
            3 => Ok(PageType::Index),
            4 => Ok(PageType::Data),
            _ => Err(Error::invalid_page()),
        }
    }
}
