use crate::Result;
use crate::bson;
use crate::engine::{IndexPage, PageAddress};
use crate::utils::{BufferSlice, Order};
use std::ops::Deref;
use std::pin::Pin;

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
    key: bson::Value,
    data_block: PageAddress,
    next_node: PageAddress,
    prev: Vec<PageAddress>,
    next: Vec<PageAddress>,
    ptr: D,
}

pub(crate) type IndexNode = IndexNodeShared<(), ()>;
pub(crate) type IndexNodeMut<'a> = IndexNodeShared<&'a mut BufferSlice, *mut IndexPage>;

extend_lifetime!(IndexNodeMut);

fn calc_key_ptr(levels: u8) -> usize {
    P_PREV_NEXT + levels as usize * PageAddress::SERIALIZED_SIZE * 2
}

impl<S, D> IndexNodeShared<S, D> {
    fn load_inner<Seg>(
        page_id: u32,
        index: u8,
        segment: Seg,
        store_segment: impl FnOnce(Seg) -> S,
        dirty_ptr: D,
    ) -> Result<Self>
    where
        Seg: Deref<Target = BufferSlice>,
    {
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
            segment: store_segment(segment),
            position,
            slot,
            levels,
            key,
            data_block,
            next_node,
            prev,
            next,
            ptr: dirty_ptr,
        })
    }

    fn copy_data<S1, D1>(base: IndexNodeShared<S1, D1>, segment: S, dirty_ptr: D) -> Self {
        Self {
            segment,
            position: base.position,
            slot: base.slot,
            levels: base.levels,
            key: base.key,
            data_block: base.data_block,
            next_node: base.next_node,
            prev: base.prev,
            next: base.next,
            ptr: dirty_ptr,
        }
    }
}

impl<S, D> IndexNodeShared<S, D> {
    pub fn position(&self) -> PageAddress {
        self.position
    }

    pub fn get_next_prev(&self, level: u8, order: Order) -> PageAddress {
        match order {
            Order::Ascending => self.next[level as usize],
            Order::Descending => self.prev[level as usize],
        }
    }

    pub fn get_key_length(v: &bson::Value, _: bool) -> usize {
        let byte_len = match v {
            bson::Value::MinValue => 0,
            bson::Value::Null => 0,
            bson::Value::MaxValue => 0,
            bson::Value::Int32(_) => 4,
            bson::Value::Int64(_) => 8,
            bson::Value::Double(_) => 8,
            bson::Value::Decimal(_) => 16,
            bson::Value::String(s) => s.len(),

            bson::Value::Binary(b) => b.bytes().len(),
            bson::Value::ObjectId(_) => 12,
            bson::Value::Guid(_) => 16,

            bson::Value::Boolean(_) => 1,
            bson::Value::DateTime(_) => 8,

            bson::Value::Document(d) => d.get_serialized_value_len(),
            bson::Value::Array(a) => a.get_serialized_value_len(),
        };

        let has_len_byte = matches!(v, bson::Value::String(_) | bson::Value::Binary(_));

        1 // tag
            + (if has_len_byte {1} else {0})
            + byte_len
    }

    pub fn get_node_length(level: u8, key: &bson::Value) -> (usize, usize) {
        let key_length = Self::get_key_length(key, false);
        let bytes_length =
            INDEX_NODE_FIXED_SIZE + level as usize * PageAddress::SERIALIZED_SIZE * 2 + key_length;

        (bytes_length, key_length)
    }

    pub fn get_next(&self, level: u8) -> PageAddress {
        self.next[level as usize]
    }

    pub fn get_prev(&self, level: u8) -> PageAddress {
        self.prev[level as usize]
    }

    pub fn key(&self) -> &bson::Value {
        &self.key
    }

    pub fn slot(&self) -> u8 {
        self.slot
    }

    pub fn levels(&self) -> u8 {
        self.levels
    }

    pub fn next_node(&self) -> PageAddress {
        self.next_node
    }

    pub fn data_block(&self) -> PageAddress {
        self.data_block
    }

    // used when creating error
    pub(crate) fn into_key(self) -> bson::Value {
        self.key
    }
}

impl IndexNode {
    pub fn load(page_id: u32, index: u8, segment: &BufferSlice) -> Result<Self> {
        Self::load_inner(page_id, index, segment, |_| (), ())
    }
}

impl<'a> IndexNodeMut<'a> {
    pub fn load(
        page_id: u32,
        dirty_ptr: *mut IndexPage,
        index: u8,
        segment: &'a mut BufferSlice,
    ) -> Result<Self> {
        Self::load_inner(page_id, index, segment, |s| s, dirty_ptr)
    }

    pub fn new(
        page_id: u32,
        index: u8,
        dirty_ptr: *mut IndexPage,
        segment: &'a mut BufferSlice,
        slot: u8,
        levels: u8,
        key: bson::Value,
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
            ptr: dirty_ptr,
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
        unsafe { IndexPage::set_dirty_ptr(self.ptr) };
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

    pub fn page_ptr(&self) -> *mut IndexPage {
        self.ptr
    }

    pub fn into_segment(self) -> &'a mut BufferSlice {
        self.segment
    }

    pub fn into_read_only(self) -> IndexNode {
        IndexNode::copy_data(self, (), ())
    }
}

// lifetime utility
impl IndexNodeMut<'_> {
    pub(crate) fn remove_from_page(self) {
        let page = unsafe { Pin::new_unchecked(&mut *self.page_ptr()) };
        page.delete_index_node_with_buffer(self);
    }
}
