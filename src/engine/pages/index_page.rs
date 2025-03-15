use crate::Result;
use crate::bson;
use crate::engine::index_node::{IndexNode, IndexNodeMut};
use crate::engine::pages::PageBufferRef;
use crate::engine::{BasePage, MAX_INDEX_LENGTH, Page, PageBuffer, PageBufferMut, PageType};
use crate::utils::PageAddress;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;

pub(crate) struct IndexPage<Buffer: PageBufferRef = Box<PageBuffer>> {
    base: BasePage<Buffer>,
    _phantom: std::marker::PhantomPinned,
}

impl<Buffer: PageBufferRef> IndexPage<Buffer> {
    pub fn new(buffer: Buffer, page_id: u32) -> Self
    where
        Buffer: PageBufferMut,
    {
        Self {
            base: BasePage::new(buffer, page_id, PageType::Index),
            _phantom: std::marker::PhantomPinned,
        }
    }

    pub fn load(buffer: Buffer) -> Result<Self> {
        let base = BasePage::load(buffer)?;
        assert_eq!(base.page_type(), PageType::Index);
        Ok(Self {
            base,
            _phantom: std::marker::PhantomPinned,
        })
    }

    #[allow(dead_code)] // basically mutable variant is used
    pub fn get_index_node(&self, index: u8) -> Result<IndexNode> {
        let segment = self.base.get(index);
        IndexNode::load(self.base.page_id(), index, segment)
    }

    fn as_ptr(self: Pin<&mut Self>) -> *mut Self {
        unsafe { Pin::into_inner_unchecked(self) as *mut Self }
    }

    fn base(self: Pin<&mut Self>) -> &mut BasePage<Buffer> {
        unsafe { &mut Pin::into_inner_unchecked(self).base }
    }

    pub fn get_index_node_mut(mut self: Pin<&mut Self>, index: u8) -> Result<IndexNodeMut<Buffer>>
    where
        Buffer: PageBufferMut,
    {
        let ptr = self.as_mut().as_ptr();
        let base = self.base();
        let page_id = base.page_id();
        let segment = base.get_mut(index);
        IndexNodeMut::<Buffer>::load(page_id, ptr, index, segment)
    }

    pub fn insert_index_node(
        mut self: Pin<&mut Self>,
        slot: u8,
        level: u8,
        key: bson::Value,
        data_block: PageAddress,
        length: usize,
    ) -> IndexNodeMut<Buffer>
    where
        Buffer: PageBufferMut,
    {
        let ptr = self.as_mut().as_ptr();
        let base = self.base();
        let page_id = base.page_id();
        let (segment, index) = base.insert(length);

        IndexNodeMut::<Buffer>::new(page_id, index, ptr, segment, slot, level, key, data_block)
    }

    pub fn delete_index_node(self: Pin<&mut Self>, index: u8)
    where
        Buffer: PageBufferMut,
    {
        self.base().delete(index);
    }

    #[allow(dead_code)] // upstream unused
    pub fn get_index_nodes(&self) -> impl Iterator<Item = Result<IndexNode>> {
        self.base.get_used_indices().map(|i| self.get_index_node(i))
    }
}

impl IndexPage {
    pub fn free_index_slot(free_bytes: usize) -> u8 {
        if free_bytes >= MAX_INDEX_LENGTH { 0 } else { 1 }
    }
}

// Rust lifetime utility
impl<Buffer: PageBufferMut> IndexPage<Buffer> {
    pub(crate) unsafe fn set_dirty_ptr(ptr: *mut IndexPage<Buffer>) {
        unsafe {
            // SAFETY: &raw mut (*ptr).base.dirty is just a pointer math
            // the ptr should have ownership for dirty
            (*ptr).base.dirty.set();
        }
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

impl AsRef<BasePage> for IndexPage {
    fn as_ref(&self) -> &BasePage {
        &self.base
    }
}

impl AsMut<BasePage> for IndexPage {
    fn as_mut(&mut self) -> &mut BasePage {
        &mut self.base
    }
}

impl Page for IndexPage {
    fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        Self::load(buffer)
    }

    fn new(buffer: Box<PageBuffer>, page_id: u32) -> Self {
        Self::new(buffer, page_id)
    }

    fn update_buffer(self: Pin<&mut Self>) -> &PageBuffer {
        unsafe { Pin::into_inner_unchecked(self) }
            .base
            .update_buffer()
    }

    fn into_base(self: Pin<Box<Self>>) -> BasePage {
        unsafe { Pin::into_inner_unchecked(self) }.base
    }

    fn as_base_mut(self: Pin<&mut Self>) -> Pin<&mut BasePage> {
        unsafe { self.map_unchecked_mut(|page| &mut page.base) }
    }
}
