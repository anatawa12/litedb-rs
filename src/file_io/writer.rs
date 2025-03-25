use super::*;
use crate::bson;
use crate::buffer_writer::BufferWriter;
use crate::constants::{
    DATA_BLOCK_FIXED_SIZE, MAX_DATA_BYTES_PER_PAGE, MAX_DOCUMENT_SIZE, MAX_INDEX_LENGTH,
    PAGE_FREE_LIST_SLOTS, PAGE_HEADER_SIZE, PAGE_SIZE, PAGE_SLOT_SIZE,
};
use crate::file_io::index_helper::IndexHelper;
use crate::file_io::offsets::collection_page::P_INDEXES;
use crate::file_io::page::{PageBuffer, PageType};
use crate::utils::{BufferSlice, PageAddress};
use std::cmp::min;
use std::ops::{Index, IndexMut};

impl LiteDBFile {
    pub fn serialize(&self) -> Vec<u8> {
        write(self)
    }
}

type PageId = u32;

pub fn write(file: &LiteDBFile) -> Vec<u8> {
    let mut pages = PageCollection::new_collection();

    let header = pages.new(PageType::Header);
    assert!(header == 0, "header page must be 0");

    let mut collections = bson::Document::new();

    for (name, collection) in &file.collections {
        let page_id = write_collection(&mut pages, &file.index_arena, &file.data, collection);
        collections.insert(&name.0, page_id as i32);
    }

    // TODO: write header page
    write_header(&mut pages, header, file, &collections);

    pages.data
}

// region utility for index and data pages
fn add_free_list(pages: &mut PageCollection, page_id: PageId, start_page_id: &mut PageId) {
    debug_assert!(
        pages[page_id].prev_page_id() == u32::MAX && pages[page_id].next_page_id() == u32::MAX,
        "only non-linked page can be added in linked list"
    );

    // fix first/next page
    if *start_page_id != u32::MAX {
        pages[*start_page_id].set_prev_page_id(page_id);
    }

    pages[page_id].set_prev_page_id(u32::MAX);
    pages[page_id].set_next_page_id(*start_page_id);

    assert!(
        matches!(
            pages[page_id].page_type(),
            Some(PageType::Data | PageType::Index)
        ),
        "only data/index pages must be first on free stack"
    );

    *start_page_id = page_id;
}

fn remove_free_list(pages: &mut PageCollection, page_id: PageId, start_page_id: &mut PageId) {
    // fix prev page
    if pages[page_id].prev_page_id() != u32::MAX {
        let prev_page_id = pages[page_id].prev_page_id();
        pages[prev_page_id].set_next_page_id(page_id);
    }

    // fix next page
    if pages[page_id].next_page_id() != u32::MAX {
        let next_page_id = pages[page_id].next_page_id();
        pages[next_page_id].set_next_page_id(page_id);
    }

    // if page is first of the list set firstPage as next page
    if *start_page_id == page_id {
        *start_page_id = pages[page_id].next_page_id();

        debug_assert!(
            pages[page_id].next_page_id() == u32::MAX
                || pages[pages[page_id].next_page_id()].page_type() != Some(PageType::Empty),
            "first page on free stack must be non empty page"
        );
    }

    // clear page pointer (MaxValue = not used)
    pages[page_id].set_prev_page_id(u32::MAX);
    pages[page_id].set_next_page_id(u32::MAX);
}

// endregion

fn write_collection(
    pages: &mut PageCollection,
    indexes: &KeyArena<IndexNode>,
    data_arena: &KeyArena<DbDocument>,
    collection: &Collection,
) -> PageId {
    let collection_page = pages.new(PageType::Collection);
    pages[collection_page].set_col_id(collection_page);

    // first, write data to data pages
    let (data_blocks, free_data_pages) =
        write_collection_data(pages, indexes, data_arena, collection_page, collection);

    // then insert index nodes
    let (index_nodes, free_index_pages) =
        write_indexes(pages, indexes, collection, collection_page, &data_blocks);

    // finally serialize collection page
    let page = &mut pages[collection_page];

    let area = page.slice_mut(PAGE_HEADER_SIZE, PAGE_SIZE - PAGE_HEADER_SIZE);
    let mut writer = BufferWriter::single(area);

    for x in free_data_pages {
        writer.write_u32(x);
    }

    writer.skip(P_INDEXES - PAGE_HEADER_SIZE - writer.position());

    writer.write_u8(collection.indexes.len() as u8);

    for index in collection.indexes.values() {
        writer.write_u8(index.slot);
        writer.write_u8(index.index_type);
        writer.write_cstring(&index.name);
        writer.write_cstring(&index.expression);
        writer.write_bool(index.unique);
        writer.write_page_address(index_nodes[&index.head]);
        writer.write_page_address(index_nodes[&index.tail]);
        writer.write_u8(index.reserved);
        writer.write_u32(free_index_pages[index.name.as_str()]);
    }

    collection_page
}

// region data page utilities

pub fn data_free_index_slot(free_bytes: usize) -> u8 {
    const FREE_PAGE_SLOTS: [usize; 4] = [
        ((PAGE_SIZE - PAGE_HEADER_SIZE) as f64 * 0.90) as usize, // 0
        ((PAGE_SIZE - PAGE_HEADER_SIZE) as f64 * 0.75) as usize, // 1
        ((PAGE_SIZE - PAGE_HEADER_SIZE) as f64 * 0.60) as usize, // 2
        ((PAGE_SIZE - PAGE_HEADER_SIZE) as f64 * 0.30) as usize, // 3
    ];
    FREE_PAGE_SLOTS
        .iter()
        .enumerate()
        .find(|&(_, &slot)| slot >= free_bytes)
        .map(|(index, _)| index as u8)
        .unwrap_or((PAGE_FREE_LIST_SLOTS - 1) as u8)
}

pub fn data_get_minimum_index_slot(length: usize) -> i32 {
    data_free_index_slot(length) as i32 - 1
}

struct DataPageManager {
    free_pages: [u32; PAGE_FREE_LIST_SLOTS],
    col_id: PageId,
}

impl DataPageManager {
    fn new(col_id: PageId) -> DataPageManager {
        DataPageManager {
            free_pages: [u32::MAX; PAGE_FREE_LIST_SLOTS],
            col_id,
        }
    }

    fn get_free_data_page(&self, pages: &mut PageCollection, size: usize) -> PageId {
        let length = size + PAGE_SLOT_SIZE; // add +4 bytes for footer slot

        // get minimum slot to check for free page. Returns -1 if need NewPage
        let start_slot = data_get_minimum_index_slot(length);

        // check for available re-usable page
        for current_slot in (0..=start_slot).rev() {
            let free_page_id = self.free_pages[current_slot as usize];

            // there is no free page here, try find princess in another castle
            if free_page_id == u32::MAX {
                continue;
            }

            let page = &pages[free_page_id];

            debug_assert_eq!(
                page.page_list_slot() as i32,
                current_slot,
                "stored slot must be same as called"
            );
            debug_assert!(page.free_bytes() >= length, "free bytes must be enough");

            return free_page_id;
        }

        // if there is no re-usable page, create a new one
        let new = pages.new(PageType::Data);
        pages[new].set_col_id(self.col_id);
        new
    }

    fn add_or_remove_free_data_list(&mut self, pages: &mut PageCollection, page_id: PageId) {
        let page = &pages[page_id];
        let new_slot = data_free_index_slot(page.free_bytes());
        let initial_slot = page.page_list_slot();
        let items_count = page.items_count();

        // there is no slot change - just exit (no need any change) [except if has no more items]
        if new_slot == initial_slot && items_count > 0 {
            return;
        }

        // remove from intial slot
        if initial_slot != u8::MAX {
            remove_free_list(pages, page_id, &mut self.free_pages[new_slot as usize]);
        }

        // if there is no items, delete page
        if items_count == 0 {
            unreachable!("No items will removed");
            //this.DeletePage(page);
        } else {
            // add into current slot
            add_free_list(pages, page_id, &mut self.free_pages[new_slot as usize]);

            pages[page_id].set_page_list_slot(new_slot);
        }
    }
}

// endregion

fn write_collection_data(
    pages: &mut PageCollection,
    indexes: &KeyArena<IndexNode>,
    data_arena: &KeyArena<DbDocument>,
    collection_page: PageId,
    collection: &Collection,
) -> (
    HashMap<ArenaKey<DbDocument>, PageAddress>,
    [u32; PAGE_FREE_LIST_SLOTS],
) {
    let mut data_slots = HashMap::new();

    let mut data_pages = DataPageManager::new(collection_page);
    for index_key in IndexHelper::find_all(indexes, collection.pk_index(), InternalOrder::Ascending)
    {
        let data_key = indexes[index_key].data.unwrap();
        let data = &data_arena[data_key].data;
        let length = data.get_serialized_value_len();
        assert!(length <= MAX_DOCUMENT_SIZE);

        struct DataSegmentIterator<'a, 'b> {
            pages: &'a mut PageCollection,
            data_pages: &'b mut DataPageManager,

            remaining: usize,
            block_index: usize,
            last_block: &'b mut Option<PageAddress>,
            first_block: &'b mut Option<PageAddress>,
        }

        impl<'a> Iterator for DataSegmentIterator<'a, '_> {
            type Item = &'a mut BufferSlice;

            fn next(&mut self) -> Option<Self::Item> {
                if self.remaining == 0 {
                    return None;
                }

                let bytes_to_copy = min(self.remaining, MAX_DATA_BYTES_PER_PAGE);
                let data_page = self
                    .data_pages
                    .get_free_data_page(self.pages, bytes_to_copy + DATA_BLOCK_FIXED_SIZE);
                let index =
                    self.pages[data_page].insert_block(bytes_to_copy + DATA_BLOCK_FIXED_SIZE);

                self.data_pages
                    .add_or_remove_free_data_list(self.pages, data_page);

                let buffer = self.pages[data_page].get_block_mut(index);
                buffer.write_bool(offsets::data_block::P_EXTEND, self.block_index > 0);
                buffer.write_page_address(offsets::data_block::P_NEXT_BLOCK, PageAddress::EMPTY);
                self.block_index += 1;
                let position = PageAddress::new(data_page, index);

                if let Some(last_block) = self.last_block {
                    self.pages[last_block.page_id()]
                        .get_block_mut(last_block.index())
                        .write_page_address(offsets::data_block::P_NEXT_BLOCK, position);
                }
                self.first_block.get_or_insert(position);
                *self.last_block = Some(position);
                self.remaining -= bytes_to_copy;

                let page = self.pages[data_page]
                    .get_block_mut(index)
                    .slice_mut(offsets::data_block::P_BUFFER, bytes_to_copy);
                // SAFETY: user of the slice won't use slice after next 'next()' invocation
                Some(unsafe { &mut *(page as *mut BufferSlice) })
            }
        }

        let mut last_block: Option<PageAddress> = None;
        let mut first_block: Option<PageAddress> = None;

        let iterator = DataSegmentIterator {
            pages,
            data_pages: &mut data_pages,
            remaining: length,
            block_index: 0,
            last_block: &mut last_block,
            first_block: &mut first_block,
        };

        BufferWriter::fragmented(iterator).write_document(data);

        data_slots.insert(data_key, first_block.unwrap());
    }

    (data_slots, data_pages.free_pages)
}

// region index page utilities

fn get_index_node_length(level: u8, key: &bson::Value) -> usize {
    // slot + levels + data + next
    const INDEX_NODE_FIXED_SIZE: usize =
        1 + 1 + PageAddress::SERIALIZED_SIZE + PageAddress::SERIALIZED_SIZE;

    let key_length = get_key_length(key);

    INDEX_NODE_FIXED_SIZE +
        (level as usize * 2 * PageAddress::SERIALIZED_SIZE) + // prev/next
        key_length // key
}

pub(crate) fn get_key_length(key: &bson::Value) -> usize {
    1 + if matches!(key, bson::Value::String(..) | bson::Value::Binary(..)) {
        1
    } else {
        0
    } + key.get_serialized_value_len()
}

struct IndexPageManager {
    free_page: u32,
    col_id: PageId,
}

impl IndexPageManager {
    fn new(col_id: PageId) -> Self {
        Self {
            free_page: u32::MAX,
            col_id,
        }
    }

    fn get_free_index_page(&self, pages: &mut PageCollection, size: usize) -> PageId {
        if self.free_page == u32::MAX {
            // if there is not page in list pages, create new page
            let page = pages.new(PageType::Index);
            pages[page].set_col_id(self.col_id);
            page
        } else {
            // get first page of free list
            let page = &pages[self.free_page];

            debug_assert!(
                page.free_bytes() > size,
                "this page shout be space enouth for this new node"
            );
            debug_assert!(page.page_list_slot() == 0, "this page should be in slot #0");
            self.free_page
        }
    }

    fn add_or_remove_free_index_list(&mut self, pages: &mut PageCollection, page_id: PageId) {
        //var newSlot = IndexPage.FreeIndexSlot(page.FreeBytes);
        let is_on_list = pages[page_id].page_list_slot() == 0;
        let must_keep = pages[page_id].free_bytes() >= MAX_INDEX_LENGTH;
        let new_slot = if must_keep { 0 } else { 1 };

        // first, test if page should be deleted
        if pages[page_id].items_count() == 0 {
            unreachable!("no nodes will be removed");
            //if (is_on_list)
            //{
            //    this.RemoveFreeList(page, ref startPageID);
            //}
            //
            //this.DeletePage(page);
        } else {
            if is_on_list && !must_keep {
                remove_free_list(pages, page_id, &mut self.free_page);
            } else if !is_on_list && must_keep {
                add_free_list(pages, page_id, &mut self.free_page);
            }

            pages[page_id].set_page_list_slot(new_slot);

            // otherwise, nothing was changed
        }
    }
}

// endregion

fn write_indexes<'a>(
    pages: &mut PageCollection,
    indexes: &KeyArena<IndexNode>,
    collection: &'a Collection,
    collection_page: PageId,
    data_blocks: &HashMap<ArenaKey<DbDocument>, PageAddress>,
) -> (
    HashMap<ArenaKey<IndexNode>, PageAddress>,
    HashMap<&'a str, u32>,
) {
    let mut index_nodes = HashMap::new();
    let mut free_page = HashMap::new();

    // first pass: write most information except for linking information
    for index in collection.indexes.values() {
        let mut index_manager = IndexPageManager::new(collection_page);

        fn add_index_node(
            pages: &mut PageCollection,
            indexes: &KeyArena<IndexNode>,
            data_blocks: &HashMap<ArenaKey<DbDocument>, PageAddress>,
            index_manager: &mut IndexPageManager,
            index_key: ArenaKey<IndexNode>,
        ) -> PageAddress {
            let index_node = &indexes[index_key];
            let size = get_index_node_length(index_node.levels, &index_node.key);

            let index_page = index_manager.get_free_index_page(pages, size);
            let node_idx = pages[index_page].insert_block(size);
            index_manager.add_or_remove_free_index_list(pages, index_page);

            let block = pages[index_page].get_block_mut(node_idx);

            block.write_u8(offsets::index_node::P_SLOT, index_node.slot);
            block.write_u8(offsets::index_node::P_LEVELS, index_node.levels);
            block.write_page_address(
                offsets::index_node::P_DATA_BLOCK,
                index_node
                    .data
                    .map(|x| data_blocks[&x])
                    .unwrap_or(PageAddress::EMPTY),
            );
            // next and prev/next are later
            block.write_index_key(
                offsets::index_node::calc_key_ptr(index_node.levels),
                &index_node.key,
            );

            PageAddress::new(index_page, node_idx)
        }

        // first add head / tail node
        index_nodes.insert(
            index.head,
            add_index_node(pages, indexes, data_blocks, &mut index_manager, index.head),
        );
        index_nodes.insert(
            index.tail,
            add_index_node(pages, indexes, data_blocks, &mut index_manager, index.tail),
        );

        // and then actual data nodes
        for index_key in IndexHelper::find_all(indexes, index, InternalOrder::Ascending) {
            index_nodes.insert(
                index_key,
                add_index_node(pages, indexes, data_blocks, &mut index_manager, index_key),
            );
        }

        free_page.insert(index.name.as_str(), index_manager.free_page);
    }

    // second pass: link nodes
    for index in collection.indexes.values() {
        fn link(
            block: &mut BufferSlice,
            offset: usize,
            node: Option<ArenaKey<IndexNode>>,
            index_nodes: &HashMap<ArenaKey<IndexNode>, PageAddress>,
        ) {
            block.write_page_address(
                offset,
                node.map(|x| index_nodes[&x]).unwrap_or(PageAddress::EMPTY),
            )
        }

        fn link_node(
            pages: &mut PageCollection,
            indexes: &KeyArena<IndexNode>,
            index_nodes: &HashMap<ArenaKey<IndexNode>, PageAddress>,
            index_key: ArenaKey<IndexNode>,
        ) {
            let address = index_nodes[&index_key];
            let block = pages[address.page_id()].get_block_mut(address.index());
            let node = &indexes[index_key];

            link(
                block,
                offsets::index_node::P_NEXT_NODE,
                node.next_node,
                index_nodes,
            );
            for i in 0..node.levels as usize {
                link(
                    block,
                    offsets::index_node::P_PREV_NEXT + i * PageAddress::SERIALIZED_SIZE * 2,
                    node.prev[i],
                    index_nodes,
                );
                link(
                    block,
                    offsets::index_node::P_PREV_NEXT
                        + i * PageAddress::SERIALIZED_SIZE * 2
                        + PageAddress::SERIALIZED_SIZE,
                    node.next[i],
                    index_nodes,
                );
            }
        }

        link_node(pages, indexes, &index_nodes, index.head);
        link_node(pages, indexes, &index_nodes, index.tail);
        for index_key in IndexHelper::find_all(indexes, index, InternalOrder::Ascending) {
            link_node(pages, indexes, &index_nodes, index_key);
        }
    }

    (index_nodes, free_page)
}

fn write_header(
    pages: &mut PageCollection,
    header: PageId,
    file: &LiteDBFile,
    collections: &bson::Document,
) {
    use offsets::header_page::*;
    let last_page_id = pages.len() - 1;

    let header_page = &mut pages[header];
    header_page.write_bytes(P_HEADER_INFO, HEADER_INFO);
    header_page.write_byte(P_FILE_VERSION, FILE_VERSION);
    header_page.write_u32(P_FREE_EMPTY_PAGE_ID, u32::MAX);
    header_page.write_u32(P_LAST_PAGE_ID, last_page_id);
    header_page.write_u64(P_CREATION_TIME, file.creation_time.ticks());
    file.pragmas.update_buffer(header_page);
    header_page.write_u8(P_INVALID_DATAFILE_STATE, 0);
    let collections_area = header_page.slice_mut(P_COLLECTIONS, COLLECTIONS_SIZE);
    BufferWriter::single(collections_area).write_document(collections);
}

/// The struct to manage pages
/// You can access page with Index impl
struct PageCollection {
    data: Vec<u8>,
}

impl PageCollection {
    pub fn new_collection() -> PageCollection {
        PageCollection { data: vec![] }
    }

    /// Returns page id for newly allocated page
    #[allow(clippy::new_ret_no_self, clippy::wrong_self_convention)]
    pub fn new(&mut self, page_type: PageType) -> PageId {
        let new_page = self.len();
        self.data.extend_from_slice(&[0; PAGE_SIZE]);
        self[new_page].initialize_page(new_page, page_type);
        new_page
    }

    pub fn len(&self) -> u32 {
        (self.data.len() / PAGE_SIZE) as u32
    }
}

impl Index<PageId> for PageCollection {
    type Output = PageBuffer;

    fn index(&self, index: PageId) -> &Self::Output {
        let offset = index as usize * PAGE_SIZE;
        PageBuffer::new(&self.data[offset..][..PAGE_SIZE])
    }
}

impl IndexMut<PageId> for PageCollection {
    fn index_mut(&mut self, index: PageId) -> &mut Self::Output {
        let offset = index as usize * PAGE_SIZE;
        PageBuffer::new_mut(&mut self.data[offset..][..PAGE_SIZE])
    }
}
