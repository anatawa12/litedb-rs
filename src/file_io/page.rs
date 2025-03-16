use crate::Error;
use crate::constants::{PAGE_HEADER_SIZE, PAGE_SIZE};
use crate::utils::BufferSlice;
use std::ops::{Deref, DerefMut};

// Slot for page blocks
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PageType {
    Empty = 0,
    Header = 1,
    Collection = 2,
    Index = 3,
    Data = 4,
}

impl TryFrom<u8> for PageType {
    type Error = Error;

    fn try_from(value: u8) -> crate::Result<Self> {
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

pub(crate) struct PageBuffer {
    inner: BufferSlice,
}

impl PageBuffer {
    pub fn new(buffer: &[u8]) -> &Self {
        debug_assert!(buffer.len() == PAGE_SIZE);
        unsafe { &*(BufferSlice::new(buffer) as *const BufferSlice as *const Self) }
    }

    pub fn new_mut(buffer: &mut [u8]) -> &mut Self {
        debug_assert!(buffer.len() == PAGE_SIZE);
        unsafe { &mut *(BufferSlice::new_mut(buffer) as *mut BufferSlice as *mut Self) }
    }

    pub fn initialize_page(&mut self, page_id: u32, page_type: PageType) {
        // page information
        self.set_page_id(page_id);
        self.set_page_type(page_type);
        self.set_prev_page_id(u32::MAX);
        self.set_next_page_id(u32::MAX);
        self.set_page_list_slot(u8::MAX);
        // transaction information
        self.set_col_id(u32::MAX);
        self.set_transaction_id(u32::MAX);
        self.set_confirmed(false);
        // block information
        self.set_items_count(0);
        self.set_used_bytes(0);
        self.set_fragmented_bytes(0);
        self.set_next_free_position(PAGE_HEADER_SIZE);
        self.set_highest_index(u8::MAX);
    }

    // region basic page information
    pub fn page_id(&self) -> u32 {
        self.inner.read_u32(P_PAGE_ID)
    }

    fn set_page_id(&mut self, page_id: u32) {
        self.inner.write_u32(P_PAGE_ID, page_id);
    }

    pub fn page_type(&self) -> Option<PageType> {
        self.inner.read_u8(P_PAGE_TYPE).try_into().ok()
    }

    pub fn set_page_type(&mut self, page_type: PageType) {
        self.inner.write_u8(P_PAGE_TYPE, page_type as u8)
    }
    // endregion

    // region Free / Removed Page Linked List
    // Those header slots will be used in free page linked list.
    pub fn prev_page_id(&self) -> u32 {
        self.inner.read_u32(P_PREV_PAGE_ID)
    }

    pub fn set_prev_page_id(&mut self, prev_page_id: u32) {
        self.inner.write_u32(P_PREV_PAGE_ID, prev_page_id);
    }

    pub fn next_page_id(&self) -> u32 {
        self.inner.read_u32(P_NEXT_PAGE_ID)
    }

    pub fn set_next_page_id(&mut self, next_page_id: u32) {
        self.inner.write_u32(P_NEXT_PAGE_ID, next_page_id);
    }
    // endregion

    pub fn page_list_slot(&self) -> u8 {
        self.inner.read_u8(P_INITIAL_SLOT)
    }

    pub fn set_page_list_slot(&mut self, initial_slot: u8) {
        self.inner.write_u8(P_INITIAL_SLOT, initial_slot);
    }

    // transaction
    #[allow(dead_code)]
    pub fn transaction_id(&self) -> u32 {
        self.inner.read_u32(P_TRANSACTION_ID)
    }

    pub fn set_transaction_id(&mut self, transaction_id: u32) {
        self.inner.write_u32(P_TRANSACTION_ID, transaction_id);
    }

    #[allow(dead_code)]
    pub fn is_confirmed(&self) -> bool {
        self.inner.read_bool(P_IS_CONFIRMED)
    }

    pub fn set_confirmed(&mut self, confirmed: bool) {
        self.inner.write_bool(P_IS_CONFIRMED, confirmed);
    }

    #[allow(dead_code)]
    pub fn col_id(&self) -> u32 {
        self.inner.read_u32(P_COL_ID)
    }

    pub fn set_col_id(&mut self, col_id: u32) {
        self.inner.write_u32(P_COL_ID, col_id);
    }

    // blocks
    pub fn items_count(&self) -> u8 {
        self.inner.read_u8(P_ITEMS_COUNT)
    }

    fn set_items_count(&mut self, items_count: u8) {
        self.inner.write_u8(P_ITEMS_COUNT, items_count);
    }

    fn used_bytes(&self) -> usize {
        self.inner.read_u16(P_USED_BYTES) as usize
    }

    fn set_used_bytes(&mut self, used_bytes: usize) {
        debug_assert!(used_bytes <= u16::MAX as usize);
        self.inner.write_u16(P_USED_BYTES, used_bytes as u16);
    }

    fn fragmented_bytes(&self) -> usize {
        self.inner.read_u16(P_FRAGMENTED_BYTES) as usize
    }

    fn set_fragmented_bytes(&mut self, fragmented_bytes: usize) {
        debug_assert!(fragmented_bytes <= u16::MAX as usize);
        self.inner
            .write_u16(P_FRAGMENTED_BYTES, fragmented_bytes as u16);
    }

    fn next_free_position(&self) -> usize {
        self.inner.read_u16(P_NEXT_FREE_POSITION) as usize
    }

    fn set_next_free_position(&mut self, next_free_position: usize) {
        self.inner
            .write_u16(P_NEXT_FREE_POSITION, next_free_position as u16);
    }

    fn highest_index(&self) -> u8 {
        self.inner.read_u8(P_HIGHEST_INDEX)
    }

    fn set_highest_index(&mut self, highest_index: u8) {
        self.inner.write_u8(P_HIGHEST_INDEX, highest_index);
    }
}

// Block Accessors
impl PageBuffer {
    fn calc_position_addr(index: u8) -> usize {
        PAGE_SIZE - (index + 1) as usize * SLOT_SIZE + 2
    }

    fn calc_length_addr(index: u8) -> usize {
        PAGE_SIZE - (index + 1) as usize * SLOT_SIZE
    }

    fn footer_size(&self) -> usize {
        if self.highest_index() == u8::MAX {
            0
        } else {
            (self.highest_index() as usize + 1) * SLOT_SIZE
        }
    }

    fn valid_position(&self, position: usize, length: usize) -> bool {
        (position >= PAGE_HEADER_SIZE && position < (PAGE_SIZE - self.footer_size()))
            && (length > 0 && length <= PAGE_SIZE - PAGE_HEADER_SIZE - self.footer_size())
    }

    fn block_addr(&self, index: u8) -> (usize, usize) {
        debug_assert!(self.items_count() > 0, "should have items in this page");
        debug_assert_ne!(
            self.highest_index(),
            u8::MAX,
            "should have at least 1 index in this page"
        );
        debug_assert!(
            index <= self.highest_index(),
            "get only index below highest index"
        );

        let position_addr = Self::calc_position_addr(index);
        let length_addr = Self::calc_length_addr(index);

        let position = self.inner.read_u16(position_addr) as usize;
        let length = self.inner.read_u16(length_addr) as usize;

        debug_assert!(
            self.valid_position(position, length),
            "invalid position or length"
        );
        (position, length)
    }

    pub fn block_exists(&self, index: u8) -> bool {
        self.items_count() > 0
            && index <= self.highest_index()
            && self.inner.read_u16(Self::calc_length_addr(index)) > 0
    }

    pub fn blocks(&self) -> impl Iterator<Item = (u8, &BufferSlice)> {
        (0..=self.highest_index())
            .filter(|&index| self.block_exists(index))
            .map(move |index| (index, self.get_block(index)))
    }

    pub fn get_block(&self, index: u8) -> &BufferSlice {
        let (position, length) = self.block_addr(index);

        self.inner.slice(position, length)
    }

    pub fn get_block_mut(&mut self, index: u8) -> &mut BufferSlice {
        let (position, length) = self.block_addr(index);

        self.inner.slice_mut(position, length)
    }

    pub fn free_bytes(&self) -> usize {
        if self.items_count() == u8::MAX {
            0
        } else {
            PAGE_SIZE - PAGE_HEADER_SIZE - self.used_bytes() - self.footer_size()
        }
    }

    fn get_free_index(&self) -> u8 {
        // check for all slot area to get first empty slot [safe for byte loop]
        for index in 0..self.highest_index() {
            let position_addr = Self::calc_position_addr(index);
            let position = self.inner.read_u16(position_addr);

            // if position = 0 means this slot is not used
            if position == 0 {
                return index;
            }
        }

        self.highest_index().wrapping_add(1)
    }

    pub fn insert_block(&mut self, bytes_length: usize) -> u8 {
        //ENSURE(_buffer.ShareCounter == BUFFER_WRITABLE, "page must be writable to support changes");
        debug_assert!(bytes_length > 0, "must insert more than 0 bytes");
        assert!(
            self.free_bytes() >= bytes_length + SLOT_SIZE,
            "length must be always lower than current free space"
        );
        debug_assert!(self.items_count() < u8::MAX, "page full");
        debug_assert!(
            self.free_bytes() >= self.fragmented_bytes(),
            "fragmented bytes must be at most free bytes"
        );

        // We've checked with assert.
        //if !(self.free_bytes() >= bytes_length + SLOT_SIZE) {
        //    throw LiteException.InvalidFreeSpacePage(self.page_id(), self.free_bytes(), bytes_length + SLOT_SIZE);
        //}

        // calculate how many continuous bytes are avaiable in this page
        let continuous_blocks = self.free_bytes() - self.fragmented_bytes() - SLOT_SIZE;

        debug_assert!(
            continuous_blocks
                == PAGE_SIZE - self.next_free_position() - self.footer_size() - SLOT_SIZE,
            "continuousBlock must be same as from NextFreePosition"
        );

        assert!(
            self.fragmented_bytes() == 0,
            "Our implementation doesn't remove blocks so not fragmented"
        );
        // if continuous blocks are not enough for this data, must run page defrag
        //if (bytes_length > continuous_blocks)
        //{
        //    this.Defrag();
        //}

        // if index is new insert segment, must request for new Index
        // get new free index must run after defrag
        let index = self.get_free_index();

        if index > self.highest_index() || self.highest_index() == u8::MAX {
            debug_assert!(
                index == self.highest_index().wrapping_add(1),
                "new index must be next highest index"
            );

            self.set_highest_index(index);
        }

        // get segment addresses
        let position_addr = Self::calc_position_addr(index);
        let length_addr = Self::calc_length_addr(index);

        debug_assert!(
            self.read_u16(position_addr) == 0,
            "slot position must be empty before use"
        );
        debug_assert!(
            self.read_u16(length_addr) == 0,
            "slot length must be empty before use"
        );

        // get next free position in page
        let position = self.next_free_position();

        // write this page position in my position address
        self.write_u16(position_addr, position as u16);

        // write page segment length in my length address
        self.write_u16(length_addr, bytes_length as u16);

        // update next free position and counters
        self.set_items_count(self.items_count() + 1);
        self.set_used_bytes(self.used_bytes() + bytes_length);
        self.set_next_free_position(self.next_free_position() + bytes_length);

        debug_assert!(
            position + bytes_length
                <= (PAGE_SIZE - (self.highest_index() as usize + 1) * SLOT_SIZE),
            "new buffer slice could not override footer area"
        );

        // create page segment based new inserted segment
        index
    }
}

impl Deref for PageBuffer {
    type Target = BufferSlice;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for PageBuffer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
