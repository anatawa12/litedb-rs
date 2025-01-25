use crate::Result;
use crate::engine::data_block::{DataBlock, DataBlockMut};
use crate::engine::page_address::PageAddress;
use crate::engine::{
    BasePage, PAGE_FREE_LIST_SLOTS, PAGE_HEADER_SIZE, PAGE_SIZE, Page, PageBuffer, PageType,
};
use std::ops::{Deref, DerefMut};

pub(crate) struct DataPage {
    base: BasePage,
}

impl DataPage {
    pub fn new(buffer: Box<PageBuffer>, page_id: u32) -> Self {
        DataPage {
            base: BasePage::new(buffer, page_id, PageType::Data),
        }
    }

    pub fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        Ok(DataPage {
            base: BasePage::load(buffer)?,
        })
    }

    pub fn get_data_block(&self, index: u8) -> DataBlock {
        let segment = self.get(index);
        DataBlock::load(self.page_id(), index, segment)
    }

    pub fn get_data_block_mut(&mut self, index: u8) -> DataBlockMut {
        let page_id = self.base.page_id();
        let (segment, dirty) = self.base.get_mut_with_dirty(index);
        DataBlockMut::load(page_id, dirty, index, segment)
    }

    pub fn insert_block(&mut self, length: usize, extend: bool) -> DataBlockMut {
        let page_id = self.page_id();
        let (segment, index, dirty) =
            self.insert_with_dirty(length + DataBlock::DATA_BLOCK_FIXED_SIZE);
        DataBlockMut::new(
            page_id,
            dirty,
            index,
            segment,
            extend,
            PageAddress::default(),
        )
    }

    pub fn update_block(
        &mut self,
        index: u8,
        extend: bool,
        next_block: PageAddress,
        length: usize,
    ) -> DataBlockMut {
        let page_id = self.page_id();
        let (buffer, dirty) =
            self.update_with_dirty(index, length + DataBlock::DATA_BLOCK_FIXED_SIZE);

        DataBlockMut::new(page_id, dirty, index, buffer, extend, next_block)
    }

    pub fn delete_block(&mut self, index: u8) {
        self.delete(index)
    }

    pub fn get_blocks(&self) -> impl Iterator<Item = PageAddress> {
        self.base
            .get_used_indices()
            .filter(|&index| {
                let position_addr = BasePage::calc_position_addr(index);
                let position = self.base.buffer().read_u16(position_addr) as usize;
                let extend = self.base.buffer().read_bool(position + DataBlock::P_EXTEND);
                !extend
            })
            .map(|index| PageAddress::new(self.page_id(), index))
    }

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

    fn update_buffer(&mut self) -> &PageBuffer {
        self.base.update_buffer()
    }

    fn into_base(self: Box<Self>) -> BasePage {
        self.base
    }
}
