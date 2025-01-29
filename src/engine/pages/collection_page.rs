use crate::engine::buffer_reader::BufferReader;
use crate::engine::buffer_writer::BufferWriter;
use crate::engine::collection_index::CollectionIndex;
use crate::engine::pages::{BasePage, PageType};
use crate::engine::{PAGE_FREE_LIST_SLOTS, PAGE_HEADER_SIZE, PAGE_SIZE, Page, PageBuffer};
use crate::{Error, Result};
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};

const P_INDEXES: usize = 96; // 96-8192 (64 + 32 header = 96)
const P_INDEXES_COUNT: usize = PAGE_SIZE - P_INDEXES;

pub(crate) struct CollectionPage {
    base: BasePage,

    pub free_data_page_list: [u32; PAGE_FREE_LIST_SLOTS],
    indexes: HashMap<String, CollectionIndex>,
}

impl CollectionPage {
    pub fn new(buffer: Box<PageBuffer>, page_id: u32) -> Self {
        let base = BasePage::new(buffer, page_id, PageType::Collection);
        let free_data_page_list = [u32::MAX; PAGE_FREE_LIST_SLOTS];

        Self {
            base,
            free_data_page_list,
            indexes: HashMap::new(),
        }
    }

    pub fn load(buffer: Box<PageBuffer>) -> Result<Self> {
        let base = BasePage::load(buffer)?;
        let mut free_data_page_list = [u32::MAX; PAGE_FREE_LIST_SLOTS];
        let mut indexes = HashMap::new();

        if base.page_type() != PageType::Collection {
            return Err(Error::invalid_page_type(PageType::Collection, base));
        }

        let area = base.buffer().slice(P_INDEXES, P_INDEXES_COUNT);
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
            indexes,
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
        &self.indexes["_id"]
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

    pub fn get_collection_indexes_slots(&self) -> Vec<Option<&CollectionIndex>> {
        let len = self
            .indexes
            .values()
            .map(|x| x.slot())
            .max()
            .map(|x| x as usize + 1)
            .unwrap_or(0);
        let mut indexes = vec![None; len];

        for index in self.indexes.values() {
            indexes[index.slot() as usize] = Some(index);
        }

        indexes
    }

    pub fn insert_collection_index(
        &mut self,
        name: &str,
        expr: &str,
        unique: bool,
    ) -> Result<&mut CollectionIndex> {
        let total_length = 1
            + self
                .indexes
                .values()
                .map(CollectionIndex::get_length)
                .sum::<usize>()
            + CollectionIndex::get_length_static(name, expr);

        if self.indexes.len() == 255 || total_length >= P_INDEXES_COUNT {
            return Err(Error::collection_index_limit_reached());
        }

        let next_slot = self
            .indexes
            .values()
            .map(|x| x.slot())
            .max()
            .map(|x| x as usize + 1)
            .unwrap_or(0) as u8;

        let index = CollectionIndex::new(next_slot, 0, name.into(), expr.into(), unique);

        let result = self
            .indexes
            .entry(name.into())
            .insert_entry(index)
            .into_mut();
        self.base.set_dirty();

        Ok(result)
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

    fn update_buffer(&mut self) -> &PageBuffer {
        self.update_buffer()
    }

    fn into_base(self: Box<Self>) -> BasePage {
        self.base
    }
}
