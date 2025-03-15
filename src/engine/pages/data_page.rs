use crate::Result;
use crate::engine::data_block::{DataBlock, DataBlockMut};
use crate::engine::pages::PageBufferRef;
use crate::engine::{
    BasePage, DATA_BLOCK_FIXED_SIZE, PAGE_FREE_LIST_SLOTS, PAGE_HEADER_SIZE, PAGE_SIZE, Page,
    PageBuffer, PageBufferMut, PageType,
};
use crate::utils::PageAddress;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;

pub(crate) struct DataPage<Buffer: PageBufferRef = Box<PageBuffer>> {
    base: BasePage<Buffer>,
}

impl<Buffer: PageBufferRef> DataPage<Buffer> {
    pub fn new(buffer: Buffer, page_id: u32) -> Self
    where
        Buffer: PageBufferMut,
    {
        Self {
            base: BasePage::new(buffer, page_id, PageType::Data),
        }
    }

    pub fn load(buffer: Buffer) -> Result<Self> {
        Ok(Self {
            base: BasePage::load(buffer)?,
        })
    }

    pub fn get_data_block(&self, index: u8) -> DataBlock {
        let segment = self.base.get(index);
        DataBlock::load(self.base.page_id(), index, segment)
    }

    pub fn get_data_block_mut(&mut self, index: u8) -> DataBlockMut
    where
        Buffer: PageBufferMut,
    {
        let page_id = self.base.page_id();
        let (segment, dirty) = self.base.get_mut_with_dirty(index);
        DataBlockMut::load(page_id, dirty, index, segment)
    }

    pub fn insert_block(&mut self, length: usize, extend: bool) -> DataBlockMut
    where
        Buffer: PageBufferMut,
    {
        let page_id = self.base.page_id();
        let (segment, index, dirty) = self.base.insert_with_dirty(length + DATA_BLOCK_FIXED_SIZE);
        DataBlockMut::new(page_id, dirty, index, segment, extend, PageAddress::EMPTY)
    }

    #[allow(dead_code)] // we dont def lag generally due to implementation limitations
    pub fn update_block(
        &mut self,
        index: u8,
        extend: bool,
        next_block: PageAddress,
        length: usize,
    ) -> DataBlockMut
    where
        Buffer: PageBufferMut,
    {
        let page_id = self.base.page_id();
        let (buffer, dirty) = self
            .base
            .update_with_dirty(index, length + DATA_BLOCK_FIXED_SIZE);

        DataBlockMut::new(page_id, dirty, index, buffer, extend, next_block)
    }

    pub fn delete_block(&mut self, index: u8)
    where
        Buffer: PageBufferMut,
    {
        self.base.delete(index)
    }

    #[allow(dead_code)] // unused in upstream
    pub fn get_blocks(&self) -> impl Iterator<Item = PageAddress> {
        self.base
            .get_used_indices()
            .filter(|&index| {
                let position_addr = BasePage::calc_position_addr(index);
                let position = self.base.buffer().read_u16(position_addr) as usize;
                let extend = self.base.buffer().read_bool(position + DataBlock::P_EXTEND);
                !extend
            })
            .map(|index| PageAddress::new(self.base.page_id(), index))
    }
}

impl DataPage {
    const FREE_PAGE_SLOTS: [usize; 4] = [
        ((PAGE_SIZE - PAGE_HEADER_SIZE) as f64 * 0.90) as usize, // 0
        ((PAGE_SIZE - PAGE_HEADER_SIZE) as f64 * 0.75) as usize, // 1
        ((PAGE_SIZE - PAGE_HEADER_SIZE) as f64 * 0.60) as usize, // 2
        ((PAGE_SIZE - PAGE_HEADER_SIZE) as f64 * 0.30) as usize, // 3
    ];

    pub fn free_index_slot(free_bytes: usize) -> u8 {
        Self::FREE_PAGE_SLOTS
            .iter()
            .enumerate()
            .find(|&(_, &slot)| slot >= free_bytes)
            .map(|(index, _)| index as u8)
            .unwrap_or((PAGE_FREE_LIST_SLOTS - 1) as u8)
    }

    pub fn get_minimum_index_slot(length: usize) -> i32 {
        Self::free_index_slot(length) as i32 - 1
    }
}

impl Deref for DataPage {
    type Target = BasePage;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl DerefMut for DataPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl AsRef<BasePage> for DataPage {
    fn as_ref(&self) -> &BasePage {
        &self.base
    }
}

impl AsMut<BasePage> for DataPage {
    fn as_mut(&mut self) -> &mut BasePage {
        &mut self.base
    }
}

impl Page for DataPage {
    fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        Self::load(buffer)
    }

    fn new(buffer: Box<PageBuffer>, page_id: u32) -> Self {
        Self::new(buffer, page_id)
    }

    fn update_buffer(self: Pin<&mut Self>) -> &PageBuffer {
        Pin::into_inner(self).base.update_buffer()
    }

    fn into_base(self: Pin<Box<Self>>) -> BasePage {
        Pin::into_inner(self).base
    }

    fn as_base_mut(self: Pin<&mut Self>) -> Pin<&mut BasePage> {
        unsafe { self.map_unchecked_mut(|page| &mut page.base) }
    }
}
