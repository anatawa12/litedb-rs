use crate::Error;
use crate::Result;
use crate::engine::{PAGE_HEADER_SIZE, PAGE_SIZE, Page, PageBuffer};
use crate::utils::BufferSlice;
use std::cmp::Ordering;
use std::fmt::Debug;

// The common variables for each page

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
    buffer: Box<PageBuffer>,
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

    dirty: bool,
    // cache for GetFreeIndex
    start_index: u8,
}

impl BasePage {
    pub const P_PAGE_ID: usize = P_PAGE_ID;
    pub const P_PAGE_TYPE: usize = P_PAGE_TYPE;
    pub const P_IS_CONFIRMED: usize = P_IS_CONFIRMED;
    pub const P_TRANSACTION_ID: usize = P_TRANSACTION_ID;
    pub const SLOT_SIZE: usize = SLOT_SIZE;

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
            start_index: 0,
        };

        base.buffer.write_u32(P_PAGE_ID, base.page_id);
        base.buffer.write_byte(P_PAGE_TYPE, page_type as u8);

        base
    }

    pub fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        let mut new = Self::new(buffer, 0, PageType::Empty);
        new.reload_fully()?;

        Ok(new)
    }

    pub(crate) fn reload_fully(&mut self) -> Result<()> {
        let buffer = self.buffer.as_mut();

        // page information
        self.page_id = buffer.read_u32(P_PAGE_ID);
        self.page_type = buffer.read_byte(P_PAGE_TYPE).try_into()?;
        self.prev_page_id = buffer.read_u32(P_PREV_PAGE_ID);
        self.next_page_id = buffer.read_u32(P_NEXT_PAGE_ID);
        self.page_list_slot = buffer.read_byte(P_INITIAL_SLOT);

        // transaction
        self.transaction_id = buffer.read_u32(P_TRANSACTION_ID);
        self.is_confirmed = buffer.read_bool(P_IS_CONFIRMED);
        self.col_id = buffer.read_u32(P_COL_ID);

        // blocks
        self.items_count = buffer.read_byte(P_ITEMS_COUNT);
        self.used_bytes = buffer.read_u16(P_USED_BYTES);
        self.fragmented_bytes = buffer.read_u16(P_FRAGMENTED_BYTES);
        self.next_free_position = buffer.read_u16(P_NEXT_FREE_POSITION);
        self.highest_index = buffer.read_byte(P_HIGHEST_INDEX);

        Ok(())
    }

    pub(crate) fn update_buffer(&mut self) -> &PageBuffer {
        let buffer = &mut self.buffer;

        assert_eq!(
            buffer.read_u32(P_PAGE_ID),
            self.page_id,
            "Page id cannot be changed"
        );

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

        buffer
    }

    pub fn mark_as_empty(&mut self) {
        self.set_dirty();

        // page information
        // PageID never change
        self.page_type = PageType::Empty;
        self.prev_page_id = u32::MAX;
        self.next_page_id = u32::MAX;
        self.page_list_slot = u8::MAX;

        // transaction information
        self.col_id = u32::MAX;
        self.transaction_id = u32::MAX;
        self.is_confirmed = false;

        // block information
        self.items_count = 0;
        self.used_bytes = 0;
        self.fragmented_bytes = 0;
        self.next_free_position = PAGE_HEADER_SIZE as u16;
        self.highest_index = u8::MAX;

        // clear content
        self.buffer
            .clear(PAGE_HEADER_SIZE, PAGE_SIZE - PAGE_HEADER_SIZE);
        self.buffer.write_u8(P_PAGE_TYPE, self.page_type as u8);
    }

    pub fn page_id(&self) -> u32 {
        self.page_id
    }

    pub fn page_type(&self) -> PageType {
        self.page_type
    }

    pub fn transaction_id(&self) -> u32 {
        self.transaction_id
    }

    pub fn prev_page_id(&self) -> u32 {
        self.prev_page_id
    }

    pub fn next_page_id(&self) -> u32 {
        self.next_page_id
    }

    pub fn set_next_page_id(&mut self, next_page_id: u32) {
        self.page_id = next_page_id;
    }

    pub fn page_list_slot(&self) -> u8 {
        self.page_list_slot
    }

    pub fn set_page_list_slot(&mut self, page_list_slot: u8) {
        self.page_list_slot = page_list_slot;
    }

    pub fn items_count(&self) -> u8 {
        self.items_count
    }

    pub fn used_bytes(&self) -> u16 {
        self.used_bytes
    }

    pub fn fragmented_bytes(&self) -> u16 {
        self.fragmented_bytes
    }

    pub fn next_free_position(&self) -> u16 {
        self.next_free_position
    }

    pub fn highest_index(&self) -> u8 {
        self.highest_index
    }

    pub fn col_id(&self) -> u32 {
        self.col_id
    }

    pub fn set_col_id(&mut self, col_id: u32) {
        self.col_id = col_id;
    }

    pub fn set_transaction_id(&mut self, value: u32) {
        self.transaction_id = value;
    }

    pub fn is_confirmed(&self) -> bool {
        self.is_confirmed
    }

    pub fn set_confirmed(&mut self, value: bool) {
        self.is_confirmed = value;
    }

    pub(crate) fn buffer(&self) -> &PageBuffer {
        &self.buffer
    }

    pub(crate) fn buffer_mut(&mut self) -> &mut PageBuffer {
        &mut self.buffer
    }

    pub(crate) fn into_buffer(self) -> Box<PageBuffer> {
        self.buffer
    }

    pub(crate) fn set_dirty(&mut self) {
        self.dirty = true;
    }

    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    pub(crate) fn dirty_mut(&mut self) -> &mut bool {
        &mut self.dirty
    }

    pub(crate) fn free_bytes(&self) -> usize {
        if self.items_count == u8::MAX {
            0
        } else {
            PAGE_SIZE - PAGE_HEADER_SIZE - self.used_bytes as usize - self.footer_size()
        }
    }

    pub(crate) fn footer_size(&self) -> usize {
        if self.highest_index == u8::MAX {
            0
        } else {
            (self.highest_index as usize + 1) * SLOT_SIZE
        }
    }
}

// Access/Manipulate PageSegments
impl BasePage {
    pub fn get(&self, index: u8) -> &BufferSlice {
        assert!(self.items_count > 0, "should have items in this page");
        assert_ne!(
            self.highest_index,
            u8::MAX,
            "should have at least 1 index in this page"
        );
        assert!(
            index < self.highest_index,
            "get only index below highest index"
        );

        let position_addr = Self::calc_position_addr(index);
        let length_addr = Self::calc_length_addr(index);

        let position = self.buffer.read_u16(position_addr) as usize;
        let length = self.buffer.read_u16(length_addr) as usize;

        assert!(
            self.valid_position(position, length),
            "invalid position or length"
        );

        self.buffer.slice(position, length)
    }

    pub fn get_mut(&mut self, index: u8) -> &mut BufferSlice {
        self.get_mut_with_dirty(index).0
    }

    pub fn get_mut_with_dirty(&mut self, index: u8) -> (&mut BufferSlice, &mut bool) {
        assert!(self.items_count > 0, "should have items in this page");
        assert_ne!(
            self.highest_index,
            u8::MAX,
            "should have at least 1 index in this page"
        );
        assert!(
            index < self.highest_index,
            "get only index below highest index"
        );

        let position_addr = Self::calc_position_addr(index);
        let length_addr = Self::calc_length_addr(index);

        let position = self.buffer.read_u16(position_addr) as usize;
        let length = self.buffer.read_u16(length_addr) as usize;

        assert!(
            self.valid_position(position, length),
            "invalid position or length"
        );

        (self.buffer.slice_mut(position, length), &mut self.dirty)
    }

    pub fn insert(&mut self, length: usize) -> (&mut BufferSlice, u8) {
        let (slice, index, _) = self.internal_insert(length, u8::MAX);
        (slice, index)
    }

    pub fn insert_with_dirty(&mut self, length: usize) -> (&mut BufferSlice, u8, &mut bool) {
        self.internal_insert(length, u8::MAX)
    }

    fn internal_insert(
        &mut self,
        length: usize,
        mut index: u8,
    ) -> (&mut BufferSlice, u8, &mut bool) {
        let is_new = index == u8::MAX;

        // assert!(self.buffer.writable)
        assert!(length > 0, "length should be greater than 0");
        // the assert below is to avoid corrupted pages essential
        assert!(
            self.free_bytes() >= length + (if is_new { SLOT_SIZE } else { 0 }),
            "not enough space"
        );
        assert!(self.items_count < u8::MAX, "page full");
        assert!(
            self.free_bytes() >= self.fragmented_bytes as usize,
            "fragmented bytes must be at most free bytes"
        );

        // We've checked with assert.
        //if !(self.free_bytes() >= length + (if is_new { SLOT_SIZE } else { 0 })) {
        //    return Err(Error::no_free_space_page(
        //        self.page_id(),
        //        self.free_bytes(),
        //        length,
        //    ));
        //}

        let continuous_blocks = self.free_bytes()
            - self.fragmented_bytes as usize
            - (if is_new { SLOT_SIZE } else { 0 });

        // PAGE_SIZE - this.NextFreePosition - this.FooterSize - (isNewInsert ? SLOT_SIZE : 0)
        debug_assert_eq!(
            continuous_blocks,
            PAGE_SIZE
                - self.next_free_position as usize
                - self.footer_size()
                - (if is_new { SLOT_SIZE } else { 0 }),
            "continuousBlock must be same as from NextFreePosition"
        );

        if length > continuous_blocks {
            self.defrag();
        }

        if index == u8::MAX {
            index = self.get_free_index();
        }

        if index > self.highest_index || self.highest_index == u8::MAX {
            debug_assert_eq!(
                index,
                self.highest_index.wrapping_add(1),
                "index should be highest index + 1"
            );
            self.highest_index = index;
        }

        let position_addr = Self::calc_position_addr(index);
        let length_addr = Self::calc_length_addr(index);

        debug_assert!(
            self.buffer.read_u16(position_addr) == 0,
            "slot position should be 0 before use"
        );
        debug_assert!(
            self.buffer.read_u16(length_addr) == 0,
            "slot length should be 0 before use"
        );

        let position = self.next_free_position;

        self.buffer.write_u16(position_addr, position);
        self.buffer.write_u16(length_addr, length as u16);

        self.items_count += 1;
        self.used_bytes += length as u16;
        self.next_free_position += length as u16;

        self.set_dirty();

        (
            self.buffer.slice_mut(position as usize, length),
            index,
            &mut self.dirty,
        )
    }

    pub fn delete(&mut self, index: u8) {
        // assert!(this.buffer.writable)

        let position_addr = Self::calc_position_addr(index);
        let length_addr = Self::calc_length_addr(index);

        let position = self.buffer.read_u16(position_addr) as usize;
        let length = self.buffer.read_u16(length_addr) as usize;

        assert!(
            self.valid_position(position, length),
            "invalid position or length"
        );

        self.buffer.write_u16(position_addr, 0);
        self.buffer.write_u16(length_addr, 0);

        self.items_count -= 1;
        self.used_bytes -= length as u16;

        self.buffer.clear(position, length);

        let is_last_segment = position + length == self.next_free_position as usize;

        if is_last_segment {
            self.next_free_position = position as u16;
        } else {
            self.fragmented_bytes += length as u16;
        }

        if index == self.highest_index {
            self.update_highest_index();
        }

        self.start_index = 0;

        if self.items_count == 0 {
            debug_assert_eq!(
                self.highest_index,
                u8::MAX,
                "if there is no items, HighestIndex must be clear"
            );
            debug_assert_eq!(self.used_bytes, 0, "should be no bytes used in clean page");
            debug_assert!(
                self.buffer
                    .slice(PAGE_HEADER_SIZE, PAGE_SIZE - PAGE_HEADER_SIZE - 1)
                    .as_bytes()
                    .iter()
                    .all(|&x| x == 0),
                "all content area must be 0"
            );

            self.next_free_position = PAGE_HEADER_SIZE as u16;
            self.fragmented_bytes = 0;
        }

        self.set_dirty();
    }

    pub fn update(&mut self, index: u8, length: usize) -> &mut BufferSlice {
        self.update_with_dirty(index, length).0
    }

    pub fn update_with_dirty(&mut self, index: u8, length: usize) -> (&mut BufferSlice, &mut bool) {
        // debug_assert!(this.buffer.writable)
        debug_assert!(length > 0, "length should be greater than 0");

        let position_addr = Self::calc_position_addr(index);
        let length_addr = Self::calc_length_addr(index);

        let position = self.buffer.read_u16(position_addr) as usize;
        let old_length = self.buffer.read_u16(length_addr) as usize;

        assert!(
            self.valid_position(position, old_length),
            "invalid position or length"
        );

        let is_last_segment = position + old_length == self.next_free_position as usize;
        self.set_dirty();

        match length.cmp(&old_length) {
            Ordering::Equal => {
                // length unchanged; nothing special to do
                (self.buffer.slice_mut(position, old_length), &mut self.dirty)
            }
            Ordering::Less => {
                // if the new length is smaller than the old length,
                // we can just update the length, and increase fragmented / next free position

                let diff = old_length - length;

                if is_last_segment {
                    self.next_free_position -= diff as u16;
                } else {
                    self.fragmented_bytes += diff as u16;
                }

                self.used_bytes -= diff as u16;

                self.buffer.write_u16(length_addr, length as u16);

                // clear fragmented bytes
                self.buffer.clear(position + length, diff);

                (self.buffer.slice_mut(position, length), &mut self.dirty)
            }
            Ordering::Greater => {
                // if the new length is greater than the old length,
                // remove the old segment, and insert a new one

                self.buffer.clear(position, old_length);

                self.items_count -= 1;
                self.used_bytes -= old_length as u16;

                if is_last_segment {
                    self.next_free_position = position as u16;
                } else {
                    self.fragmented_bytes += old_length as u16;
                }

                self.buffer.write_u16(position_addr, 0);
                self.buffer.write_u16(length_addr, 0);

                let (slice, _, dirty) = self.internal_insert(length, index);
                (slice, dirty)
            }
        }
    }

    pub fn defrag(&mut self) {
        // assert!(this.buffer.writable)
        debug_assert!(
            self.fragmented_bytes > 0,
            "should have fragmented bytes to defrag"
        );
        debug_assert!(
            self.highest_index < u8::MAX,
            "should have at least 1 index in this page"
        );

        // log.debug("Defrag page", this.PageId, this.FragmentedBytes);

        let mut segments = Vec::with_capacity(self.highest_index as usize);

        for index in 0..self.highest_index {
            let position_addr = Self::calc_position_addr(index);
            let position = self.buffer.read_u16(position_addr) as usize;

            if position != 0 {
                segments.push((position, index));
            }
        }

        segments.sort_by_key(|(position, _)| *position);

        let mut next_position = PAGE_HEADER_SIZE;

        for (position, index) in segments {
            let length_addr = Self::calc_length_addr(index);
            let position_addr = Self::calc_position_addr(index);

            let length = self.buffer.read_u16(length_addr) as usize;
            //let position = self.buffer.read_u16(position_addr) as usize;

            debug_assert!(
                self.valid_position(position, length),
                "invalid position or length"
            );

            if position != next_position {
                self.buffer
                    .buffer_mut()
                    .copy_within(position..position + length, next_position);
                self.buffer.write_u16(position_addr, next_position as u16);
            }

            next_position += length;
        }

        let empty_length = PAGE_SIZE - next_position - self.footer_size();
        self.buffer.clear(next_position, empty_length);

        self.fragmented_bytes = 0;
        self.next_free_position = next_position as u16;
    }

    fn get_free_index(&mut self) -> u8 {
        for index in self.start_index..=self.highest_index {
            let position_addr = Self::calc_position_addr(index);
            let position = self.buffer.read_u16(position_addr) as usize;

            if position == 0 {
                self.start_index = index + 1;
                return index;
            }
        }

        self.highest_index + 1
    }

    pub fn get_used_indices(&self) -> impl Iterator<Item = u8> {
        (0..=self.highest_index).filter(move |&index| {
            let position_addr = Self::calc_position_addr(index);
            let position = self.buffer.read_u16(position_addr) as usize;
            position != 0
        })
    }

    fn update_highest_index(&mut self) {
        self.highest_index = self.get_used_indices().max().unwrap_or(u8::MAX);
    }

    fn valid_position(&self, position: usize, length: usize) -> bool {
        (position >= PAGE_HEADER_SIZE && position < (PAGE_SIZE - self.footer_size()))
            && (length > 0 && length <= PAGE_SIZE - PAGE_HEADER_SIZE - self.footer_size())
    }
}

// static helpers
impl BasePage {
    pub fn get_page_position(page_id: u32) -> u64 {
        page_id as u64 * PAGE_SIZE as u64
    }

    pub fn calc_position_addr(index: u8) -> usize {
        PAGE_HEADER_SIZE + index as usize * SLOT_SIZE + 2
    }

    pub fn calc_length_addr(index: u8) -> usize {
        PAGE_HEADER_SIZE + index as usize * SLOT_SIZE
    }
}

impl AsRef<BasePage> for BasePage {
    fn as_ref(&self) -> &BasePage {
        self
    }
}

impl AsMut<BasePage> for BasePage {
    fn as_mut(&mut self) -> &mut BasePage {
        self
    }
}

impl Page for BasePage {
    fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        Self::load(buffer)
    }

    fn new(buffer: Box<PageBuffer>, page_id: u32) -> Self {
        Self::new(buffer, page_id, PageType::Empty)
    }

    fn update_buffer(&mut self) -> &PageBuffer {
        self.update_buffer()
    }

    fn into_base(self: Box<Self>) -> BasePage {
        *self
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
