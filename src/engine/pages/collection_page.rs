use crate::engine::buffer_reader::BufferReader;
use crate::engine::buffer_writer::BufferWriter;
use crate::engine::collection_index::CollectionIndex;
use crate::engine::pages::{BasePage, PageType};
use crate::engine::{
    DirtyFlag, PAGE_FREE_LIST_SLOTS, PAGE_HEADER_SIZE, PAGE_SIZE, Page, PageBuffer,
};
use crate::{Error, Result};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;

const P_INDEXES: usize = 96; // 96-8192 (64 + 32 header = 96)
const P_INDEXES_COUNT: usize = PAGE_SIZE - P_INDEXES;

pub(crate) type FreeDataPageList = [u32; PAGE_FREE_LIST_SLOTS];
pub(crate) struct CollectionIndexes(HashMap<String, CollectionIndex>);
pub(crate) struct CollectionIndexesMut<'a>(&'a mut CollectionIndexes, &'a DirtyFlag);

// all fields are accessed by snapshot for partial borrowing
pub(crate) struct CollectionPage {
    pub base: BasePage, // for Dirty flag, temporary

    pub free_data_page_list: FreeDataPageList,
    pub indexes: CollectionIndexes,
}

impl CollectionPage {
    pub fn new(buffer: Box<PageBuffer>, page_id: u32) -> Self {
        let base = BasePage::new(buffer, page_id, PageType::Collection);
        let free_data_page_list = [u32::MAX; PAGE_FREE_LIST_SLOTS];

        Self {
            base,
            free_data_page_list,
            indexes: CollectionIndexes(HashMap::new()),
        }
    }

    pub fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        let base = BasePage::load(buffer)?;
        let mut free_data_page_list = [u32::MAX; PAGE_FREE_LIST_SLOTS];
        let mut indexes = HashMap::new();

        if base.page_type() != PageType::Collection {
            return Err(Error::invalid_page_type(PageType::Collection, base));
        }

        let area = base
            .buffer()
            .slice(PAGE_HEADER_SIZE, PAGE_SIZE - PAGE_HEADER_SIZE);
        let mut reader = BufferReader::single(area);

        for item in free_data_page_list.iter_mut() {
            *item = reader.read_u32();
        }

        reader.skip(P_INDEXES - PAGE_HEADER_SIZE - reader.position());

        let count = reader.read_u8().into();

        for _ in 0..count {
            let index = CollectionIndex::load(&mut reader)?;
            indexes.insert(index.name().to_string(), index);
        }

        Ok(Self {
            base,
            free_data_page_list,
            indexes: CollectionIndexes(indexes),
        })
    }

    pub fn update_buffer(&mut self) -> &PageBuffer {
        if self.page_type() == PageType::Empty {
            return self.base.update_buffer();
        }
        let buffer = self.base.buffer_mut();
        let mut writer = BufferWriter::single(buffer);

        for i in 0..PAGE_FREE_LIST_SLOTS {
            writer.write_u32(self.free_data_page_list[i]);
        }

        writer.skip(P_INDEXES - PAGE_HEADER_SIZE - writer.position());

        writer.write_u8(self.indexes.len() as u8);

        for index in self.indexes.values() {
            index.update_buffer(&mut writer);
        }

        self.base.update_buffer()
    }

    pub fn pk_index(&self) -> &CollectionIndex {
        self.indexes.pk_index()
    }

    pub fn get_collection_index(&self, name: &str) -> Option<&CollectionIndex> {
        self.indexes.get(name)
    }

    pub fn get_collection_index_mut(&mut self, name: &str) -> Option<&mut CollectionIndex> {
        self.indexes.get_mut(name)
    }

    pub fn get_collection_indexes(&self) -> impl Iterator<Item = &CollectionIndex> {
        self.indexes.values()
    }
}

impl CollectionIndexes {
    pub fn get_collection_indexes_slots(&self) -> Vec<Option<&CollectionIndex>> {
        let len = self
            .values()
            .map(|x| x.slot())
            .max()
            .map(|x| x as usize + 1)
            .unwrap_or(0);
        let mut indexes = vec![None; len];

        for index in self.values() {
            indexes[index.slot() as usize] = Some(index);
        }

        indexes
    }

    pub fn get_collection_indexes_slots_mut(&mut self) -> Vec<Option<&mut CollectionIndex>> {
        let len = self
            .values()
            .map(|x| x.slot())
            .max()
            .map(|x| x as usize + 1)
            .unwrap_or(0);
        let mut indexes = vec![];
        indexes.resize_with(len, || None);

        for index in self.values_mut() {
            let slot = index.slot();
            indexes[slot as usize] = Some(index);
        }

        indexes
    }

    fn insert_collection_index(
        &mut self,
        name: &str,
        expr: &str,
        unique: bool,
        dirty: &DirtyFlag,
    ) -> Result<&mut CollectionIndex> {
        let total_length = 1
            + self
                .values()
                .map(CollectionIndex::get_length)
                .sum::<usize>()
            + CollectionIndex::get_length_static(name, expr);

        if self.len() == 255 || total_length >= P_INDEXES_COUNT {
            return Err(Error::collection_index_limit_reached());
        }

        let next_slot = self
            .values()
            .map(|x| x.slot())
            .max()
            .map(|x| x as usize + 1)
            .unwrap_or(0) as u8;

        let index = CollectionIndex::new(next_slot, 0, name.into(), expr.into(), unique);

        let result = self.entry(name.into()).insert_entry(index).into_mut();
        dirty.set();

        Ok(result)
    }
}

impl CollectionPage {
    pub fn get_collection_indexes_slots(&self) -> Vec<Option<&CollectionIndex>> {
        self.indexes.get_collection_indexes_slots()
    }

    pub fn get_collection_indexes_slots_mut_with_dirty(
        &mut self,
    ) -> (Vec<Option<&mut CollectionIndex>>, &DirtyFlag) {
        (
            self.indexes.get_collection_indexes_slots_mut(),
            &self.base.dirty,
        )
    }

    pub fn get_collection_indexes_slots_mut(&mut self) -> Vec<Option<&mut CollectionIndex>> {
        self.indexes.get_collection_indexes_slots_mut()
    }

    pub fn insert_collection_index(
        &mut self,
        name: &str,
        expr: &str,
        unique: bool,
    ) -> Result<&mut CollectionIndex> {
        self.indexes
            .insert_collection_index(name, expr, unique, &self.base.dirty)
    }

    pub fn update_collection_index(&mut self, name: &str) -> &mut CollectionIndex {
        self.set_dirty();
        self.indexes.get_mut(name).unwrap()
    }

    pub fn delete_collection_index(&mut self, name: &str) {
        self.indexes.remove(name);
        self.base.set_dirty();
    }
}

impl Deref for CollectionPage {
    type Target = BasePage;

    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl DerefMut for CollectionPage {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}

impl AsRef<BasePage> for CollectionPage {
    fn as_ref(&self) -> &BasePage {
        &self.base
    }
}

impl AsMut<BasePage> for CollectionPage {
    fn as_mut(&mut self) -> &mut BasePage {
        &mut self.base
    }
}

impl Page for CollectionPage {
    fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        Self::load(buffer)
    }

    fn new(buffer: Box<PageBuffer>, page_id: u32) -> Self {
        Self::new(buffer, page_id)
    }

    fn update_buffer(self: Pin<&mut Self>) -> &PageBuffer {
        Pin::into_inner(self).update_buffer()
    }

    fn into_base(self: Pin<Box<Self>>) -> BasePage {
        Pin::into_inner(self).base
    }

    fn as_base_mut(self: Pin<&mut Self>) -> Pin<&mut BasePage> {
        unsafe { self.map_unchecked_mut(|page| &mut page.base) }
    }
}

impl CollectionIndexes {
    pub(crate) fn pk_index(&self) -> &CollectionIndex {
        &self["_id"]
    }
}

impl<'a> CollectionIndexesMut<'a> {
    pub fn new(indexes: &'a mut CollectionIndexes, dirty: &'a DirtyFlag) -> Self {
        Self(indexes, dirty)
    }

    pub fn insert_collection_index(
        &mut self,
        name: &str,
        expr: &str,
        unique: bool,
    ) -> Result<&mut CollectionIndex> {
        self.0.insert_collection_index(name, expr, unique, self.1)
    }
}

impl Deref for CollectionIndexes {
    type Target = HashMap<String, CollectionIndex>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for CollectionIndexes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for CollectionIndexesMut<'_> {
    type Target = CollectionIndexes;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl DerefMut for CollectionIndexesMut<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.0
    }
}
