use crate::engine::index_node::{IndexNode, IndexNodeMut};
use crate::engine::{BasePage, PageBuffer, PageType, MAX_INDEX_LENGTH};
use crate::Result;
use std::ops::{Deref, DerefMut};

pub(crate) struct IndexPage {
    base: BasePage,
}

impl IndexPage {
    pub fn new(buffer: Box<PageBuffer>, page_id: u32) -> Self {
        Self {
            base: BasePage::new(buffer, page_id, PageType::Index),
        }
    }

    pub fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        let base = BasePage::load(buffer)?;
        assert_eq!(base.page_type(), PageType::Index);
        Ok(Self { base })
    }

    pub fn get_index_node(&self, index: u8) -> Result<IndexNode> {
        let segment = self.base.get(index);
        IndexNode::load(self.page_id(), index, segment)
    }

    pub fn get_index_node_mut(&mut self, index: u8) -> Result<IndexNodeMut> {
        let page_id = self.page_id();
        let (segment, dirty_ptr) = self.base.get_mut_with_dirty(index);
        IndexNodeMut::load(page_id, dirty_ptr, index, segment)
    }

    pub fn delete_index_node(&mut self, index: u8) {
        self.base.delete(index);
    }

    pub fn get_index_nodes(&self) -> impl Iterator<Item = Result<IndexNode>> {
        self.base.get_used_indices().map(|i| self.get_index_node(i))
    }

    pub fn free_index_slot(free_bytes: usize) -> u8 {
        if free_bytes >= MAX_INDEX_LENGTH { 0 } else { 1 }
    }
}

impl Deref for IndexPage {
    type Target = BasePage;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl DerefMut for IndexPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
