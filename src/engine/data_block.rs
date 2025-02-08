use crate::engine::DirtyFlag;
use crate::engine::page_address::PageAddress;
use crate::utils::BufferSlice;

const P_EXTEND: usize = 0; // 00-00 [byte]
const P_NEXT_BLOCK: usize = 1; // 01-05 [pageAddress]
const P_BUFFER: usize = 6; // 06-EOF [byte[]]

pub(crate) struct DataBlock<'a> {
    segment: &'a BufferSlice,
    position: PageAddress,
    extend: bool,
    next_block: PageAddress,
    buffer: &'a BufferSlice,
}

impl<'a> DataBlock<'a> {
    pub const DATA_BLOCK_FIXED_SIZE: usize = 1 + PageAddress::SERIALIZED_SIZE;
    pub const P_EXTEND: usize = P_EXTEND;

    pub fn load(page_id: u32, index: u8, segment: &'a BufferSlice) -> Self {
        let position = PageAddress::new(page_id, index);

        let extend = segment.read_bool(P_EXTEND);
        let next_block = segment.read_page_address(P_NEXT_BLOCK);
        let buffer = segment.slice(P_BUFFER, segment.len() - P_BUFFER);

        Self {
            segment,
            position,
            extend,
            next_block,
            buffer,
        }
    }

    pub fn position(&self) -> PageAddress {
        self.position
    }

    pub fn extend(&self) -> bool {
        self.extend
    }

    pub fn next_block(&self) -> PageAddress {
        self.next_block
    }

    pub fn buffer(&self) -> &BufferSlice {
        self.buffer
    }
}

pub(crate) struct DataBlockMut<'a> {
    segment: &'a mut BufferSlice,
    position: PageAddress,
    extend: bool,
    next_block: PageAddress,
    dirty_ptr: &'a DirtyFlag,
}

extend_lifetime!(DataBlockMut);

impl<'a> DataBlockMut<'a> {
    pub fn new(
        page_id: u32,
        dirty_ptr: &'a DirtyFlag,
        index: u8,
        segment: &'a mut BufferSlice,
        extend: bool,
        next_block: PageAddress,
    ) -> Self {
        let position = PageAddress::new(page_id, index);

        segment.write_bool(P_EXTEND, extend);
        segment.write_page_address(P_NEXT_BLOCK, next_block);

        dirty_ptr.set();

        Self {
            segment,
            position,
            extend,
            next_block,
            dirty_ptr,
        }
    }

    pub fn load(
        page_id: u32,
        dirty_ptr: &'a DirtyFlag,
        index: u8,
        segment: &'a mut BufferSlice,
    ) -> Self {
        let position = PageAddress::new(page_id, index);

        let extend = segment.read_bool(P_EXTEND);
        let next_block = segment.read_page_address(P_NEXT_BLOCK);

        Self {
            segment,
            position,
            extend,
            next_block,
            dirty_ptr,
        }
    }

    pub fn position(&self) -> PageAddress {
        self.position
    }

    pub fn extend(&self) -> bool {
        self.extend
    }

    pub fn next_block(&self) -> PageAddress {
        self.next_block
    }

    pub fn buffer(&self) -> &BufferSlice {
        let len = self.segment.len() - P_BUFFER;
        self.segment.slice(P_BUFFER, len)
    }

    pub fn buffer_mut(&mut self) -> &mut BufferSlice {
        let len = self.segment.len() - P_BUFFER;
        self.segment.slice_mut(P_BUFFER, len)
    }

    pub fn set_next_block(&mut self, next_block: PageAddress) {
        self.next_block = next_block;
        self.segment.write_page_address(P_NEXT_BLOCK, next_block);
        self.dirty_ptr.set();
    }
}
