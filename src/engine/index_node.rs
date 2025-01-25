use crate::Result;
use crate::engine::PageAddress;
use crate::utils::{BufferSlice, Order};
use std::ops::Deref;

const INDEX_NODE_FIXED_SIZE: usize =
    1 + 1 + PageAddress::SERIALIZED_SIZE + PageAddress::SERIALIZED_SIZE;
const P_SLOT: usize = 0; // 00-00 [byte]
const P_LEVELS: usize = 1; // 01-01 [byte]
const P_DATA_BLOCK: usize = 2; // 02-06 [PageAddress]
const P_NEXT_NODE: usize = 7; // 07-11 [PageAddress]
const P_PREV_NEXT: usize = 12; // 12-(_level * 5 [PageAddress] * 2 [prev-next])

pub(crate) struct IndexNodeShared<S, D> {
    segment: S,
    position: PageAddress,
    slot: u8,
    levels: u8,
    key: bson::Bson,
    data_block: PageAddress,
    next_node: PageAddress,
    prev: Vec<PageAddress>,
    next: Vec<PageAddress>,
    dirty_ptr: D,
}

pub(crate) type IndexNode<'a> = IndexNodeShared<&'a BufferSlice, ()>;
pub(crate) type IndexNodeMut<'a> = IndexNodeShared<&'a mut BufferSlice, &'a mut bool>;

fn calc_key_ptr(levels: u8) -> usize {
    P_PREV_NEXT + levels as usize * PageAddress::SERIALIZED_SIZE * 2
}

impl<S: Deref<Target = BufferSlice>, D> IndexNodeShared<S, D> {
    fn load_inner(page_id: u32, index: u8, segment: S, dirty_ptr: D) -> Result<Self> {
        let position = PageAddress::new(page_id, index);
        let slot = segment.read_u8(P_SLOT);
        let levels = segment.read_u8(P_LEVELS);
        let data_block = segment.read_page_address(P_DATA_BLOCK);
        let next_node = segment.read_page_address(P_NEXT_NODE);

        let mut next = Vec::with_capacity(levels as usize);
        let mut prev = Vec::with_capacity(levels as usize);

        for i in 0..levels as usize {
            let prev_addr =
                segment.read_page_address(P_PREV_NEXT + i * PageAddress::SERIALIZED_SIZE * 2);
            let next_addr = segment.read_page_address(
                P_PREV_NEXT + i * PageAddress::SERIALIZED_SIZE * 2 + PageAddress::SERIALIZED_SIZE,
            );
            prev.push(prev_addr);
            next.push(next_addr);
        }

        let key_ptr = calc_key_ptr(levels);
        let key = segment.read_index_key(key_ptr)?;

        Ok(Self {
            segment,
            position,
            slot,
            levels,
            key,
            data_block,
            next_node,
            prev,
            next,
            dirty_ptr,
        })
    }

    pub fn position(&self) -> PageAddress {
        self.position
    }

    fn get_next_prev(&self, level: u8, order: Order) -> PageAddress {
        match order {
            Order::Ascending => self.next[level as usize],
            Order::Descending => self.prev[level as usize],
        }
    }

    pub fn get_key_length(_: &bson::Bson, _: bool) -> usize {
        todo!("reimplement when bson is reimplemented")
    }

    pub fn get_node_length(level: u8, key: &bson::Bson) -> usize {
        let key_length = Self::get_key_length(key, false);

        INDEX_NODE_FIXED_SIZE + level as usize * PageAddress::SERIALIZED_SIZE * 2 + key_length
    }
}

impl<'a> IndexNode<'a> {
    pub fn load(page_id: u32, index: u8, segment: &'a BufferSlice) -> Result<Self> {
        Self::load_inner(page_id, index, segment, ())
    }
}

impl<'a> IndexNodeMut<'a> {
    pub fn load(
        page_id: u32,
        dirty_ptr: &'a mut bool,
        index: u8,
        segment: &'a mut BufferSlice,
    ) -> Result<Self> {
        Self::load_inner(page_id, index, segment, dirty_ptr)
    }

    pub fn new(
        page_id: u32,
        index: u8,
        dirty_ptr: &'a mut bool,
        segment: &'a mut BufferSlice,
        slot: u8,
        levels: u8,
        key: bson::Bson,
        data_block: PageAddress,
    ) -> Self {
        let position = PageAddress::new(page_id, index);
        let next_node = PageAddress::default();
        let next = vec![PageAddress::default(); levels as usize];
        let prev = vec![PageAddress::default(); levels as usize];

        // write to buffer (read only data)
        segment.write_u8(P_SLOT, slot);
        segment.write_u8(P_LEVELS, levels);
        segment.write_u8(P_DATA_BLOCK, levels);
        segment.write_page_address(P_NEXT_NODE, next_node);

        let mut result = Self {
            segment,
            position,
            slot,
            levels,
            key,
            data_block,
            next_node,
            prev,
            next,
            dirty_ptr,
        };

        // write data
        for i in 0..levels {
            result.set_prev(i, PageAddress::default());
            result.set_next(i, PageAddress::default());
        }

        let key_ptr = calc_key_ptr(levels);
        result.segment.write_index_key(key_ptr, &result.key);
        result.set_dirty();

        result
    }

    fn set_dirty(&mut self) {
        *self.dirty_ptr = true;
    }

    pub fn set_next_node(&mut self, values: PageAddress) {
        self.next_node = values;
        self.segment.write_page_address(P_NEXT_NODE, values);
        self.set_dirty();
    }

    pub fn set_prev(&mut self, level: u8, address: PageAddress) {
        self.prev[level as usize] = address;
        self.segment.write_page_address(
            P_PREV_NEXT + (level as usize * PageAddress::SERIALIZED_SIZE * 2),
            address,
        );
        self.set_dirty();
    }

    pub fn set_next(&mut self, level: u8, address: PageAddress) {
        self.prev[level as usize] = address;
        self.segment.write_page_address(
            P_PREV_NEXT
                + (level as usize * PageAddress::SERIALIZED_SIZE * 2)
                + PageAddress::SERIALIZED_SIZE,
            address,
        );
        self.set_dirty();
    }
}
