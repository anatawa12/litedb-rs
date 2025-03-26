use crate::buffer_reader::BufferReader;
use crate::constants::{PAGE_FREE_LIST_SLOTS, PAGE_HEADER_SIZE, PAGE_SIZE};
use crate::utils::{ArenaKey, BufferSlice, CaseInsensitiveString, KeyArena, PageAddress};
use crate::{ParseError, ParseResult, bson};
use std::collections::HashMap;

use super::*;
use crate::file_io::page::PageBuffer;
use crate::file_io::page::PageType;
use crate::file_io::parser::collection_page::{RawCollectionIndex, RawCollectionPage};
use crate::file_io::parser::header_page::HeaderPage;
use crate::file_io::parser::raw_data_block::RawDataBlock;

use raw_index_node::RawIndexNode;

impl LiteDBFile {
    pub fn parse(data: &[u8]) -> ParseResult<Self> {
        parse(data)
    }
}

#[allow(dead_code)]
pub(super) fn parse(data: &[u8]) -> ParseResult<LiteDBFile> {
    // if the length is not multiple of PAGE_SIZE, crop
    let data = &data[..(data.len() & !(PAGE_SIZE - 1))];

    let pages = data
        .chunks(PAGE_SIZE)
        .map(PageBuffer::new)
        .collect::<Vec<_>>();

    for (index, &page) in pages.iter().enumerate() {
        if index as u32 != page.page_id() {
            return Err(ParseError::invalid_page(index as u32));
        }
        if page.page_type().is_none() {
            return Err(ParseError::invalid_page(index as u32));
        }
    }

    let header = HeaderPage::parse(pages[0])?;

    // parse index nodes
    let index_nodes = {
        let mut index_nodes = HashMap::<PageAddress, RawIndexNode>::new();

        for &page in pages.iter() {
            if page.page_type() == Some(PageType::Index) {
                for (index, buffer) in page.blocks() {
                    index_nodes.insert(
                        PageAddress::new(page.page_id(), index),
                        RawIndexNode::parse(buffer)?,
                    );
                }
            }
        }

        index_nodes
    };

    // parse data blocks
    let data_blocks = {
        let mut data_blocks = HashMap::<PageAddress, RawDataBlock>::new();

        for &page in pages.iter() {
            if page.page_type() == Some(PageType::Data) {
                for (index, buffer) in page.blocks() {
                    data_blocks.insert(
                        PageAddress::new(page.page_id(), index),
                        RawDataBlock::parse(buffer),
                    );
                }
            }
        }

        data_blocks
    };

    struct DataBuilder<'buf> {
        arena: KeyArena<DbDocument>,
        raw_node: HashMap<PageAddress, RawDataBlock<'buf>>,
    }

    impl<'buf> DataBuilder<'buf> {
        pub fn new(raw_node: HashMap<PageAddress, RawDataBlock<'buf>>) -> Self {
            Self {
                arena: KeyArena::new(),
                raw_node,
            }
        }

        fn parse(&mut self, position: PageAddress) -> ParseResult<ArenaKey<DbDocument>> {
            let mut buffers = vec![];

            {
                let mut cur = position;

                while !cur.is_empty() {
                    let raw = self
                        .raw_node
                        .remove(&cur)
                        .ok_or_else(ParseError::bad_reference)?;
                    buffers.push(raw.buffer());
                    cur = raw.next_block();
                }
            }

            let mut reader = BufferReader::fragmented(buffers);
            let document = reader.read_document()?;

            Ok(self.arena.alloc(DbDocument::new(document)))
        }
    }

    struct IndexBuilder<'buf, 'a> {
        arena: KeyArena<IndexNode>,
        raw_node: HashMap<PageAddress, RawIndexNode>,
        data_builder: &'a mut DataBuilder<'buf>,
    }

    impl<'buf, 'a> IndexBuilder<'buf, 'a> {
        fn new(
            raw_node: HashMap<PageAddress, RawIndexNode>,
            data_builder: &'a mut DataBuilder<'buf>,
        ) -> Self {
            Self {
                arena: KeyArena::new(),
                raw_node,
                data_builder,
            }
        }

        pub fn build(
            &mut self,
            index: RawCollectionIndex,
            mut get_data_builder: impl FnMut(
                &mut DataBuilder<'buf>,
                PageAddress,
            ) -> ParseResult<Option<ArenaKey<DbDocument>>>,
        ) -> ParseResult<CollectionIndex> {
            // result structure
            let mut index_keys = HashMap::<PageAddress, ArenaKey<IndexNode>>::new();
            let mut head_key = None;
            let mut tail_key = None;

            struct RawIndexAddress {
                data_block: PageAddress,
                prev: Vec<PageAddress>,
                next: Vec<PageAddress>,
                key: ArenaKey<IndexNode>,
                valid: bool,
            }

            let mut address_map = Vec::<RawIndexAddress>::new();

            // 1st pass: parse nodes
            let mut current_addr = Some(index.head);
            while let Some(current) = current_addr {
                let raw = self
                    .raw_node
                    .remove(&current)
                    .ok_or_else(ParseError::bad_reference)?;

                let index_key;
                let valid;

                if current == index.head || current == index.tail {
                    // head / tail node
                    let index_node = IndexNode::new(raw.slot, raw.levels, raw.key);
                    index_key = self.arena.alloc(index_node);
                    valid = true;
                } else {
                    // data node
                    if let Some(data) = get_data_builder(self.data_builder, raw.data_block)? {
                        let mut index_node = IndexNode::new(raw.slot, raw.levels, raw.key);
                        index_node.data = Some(data);
                        index_key = self.arena.alloc(index_node);
                        self.data_builder.arena[data].index_nodes.push(index_key);
                        valid = true;
                    } else {
                        let index_node = IndexNode::new(raw.slot, raw.levels, raw.key);
                        index_key = self.arena.alloc(index_node);
                        valid = false;
                    }
                }
                index_keys.insert(current, index_key);
                head_key.get_or_insert(index_key);
                tail_key = Some(index_key);

                #[allow(clippy::collapsible_if)]
                if current == index.head {
                    if !raw.prev.iter().all(PageAddress::is_empty) {
                        return Err(ParseError::bad_reference());
                    }
                } else if current == index.tail {
                    if !raw.next.iter().all(PageAddress::is_empty) {
                        return Err(ParseError::bad_reference());
                    }
                }

                current_addr = Some(raw.next[0]).filter(|x| !x.is_empty());

                // if new current_addr is none, current node must be tail node
                assert!(current_addr.is_some() || current == index.tail);

                address_map.push(RawIndexAddress {
                    key: index_key,
                    data_block: raw.data_block,
                    prev: raw.prev,
                    next: raw.next,
                    valid,
                });
            }

            // 2nd pass: parse
            fn get(
                keys: &mut HashMap<PageAddress, ArenaKey<IndexNode>>,
                addr: PageAddress,
            ) -> ParseResult<Option<ArenaKey<IndexNode>>> {
                if addr.is_empty() {
                    Ok(None)
                } else {
                    Ok(Some(
                        *keys.get(&addr).ok_or_else(ParseError::bad_reference)?,
                    ))
                }
            }

            for addresses in &address_map {
                let node = &mut self.arena[addresses.key];
                for (node, &addr) in node.prev.iter_mut().zip(addresses.prev.iter()) {
                    *node = get(&mut index_keys, addr)?;
                }
                for (node, &addr) in node.next.iter_mut().zip(addresses.next.iter()) {
                    *node = get(&mut index_keys, addr)?;
                }
            }

            // 3rd pass: remove invalid nodes
            for addresses in &address_map {
                if !addresses.valid {
                    let node = self.arena.free(addresses.key);
                    for level in 0..(node.levels as usize) {
                        if let Some(prev) = node.prev[level] {
                            self.arena[prev].next[level] = node.next[level]
                        };
                        if let Some(next) = node.next[level] {
                            self.arena[next].prev[level] = node.prev[level]
                        };
                    }
                }
            }

            let index_parsed = CollectionIndex {
                slot: index.slot,
                index_type: index.index_type,
                name: index.name,
                expression: index.expression,
                unique: index.unique,
                reserved: index.reserved,
                bson_expr: index.bson_expr,
                head: head_key.unwrap(),
                tail: tail_key.unwrap(),
            };
            self.arena[index_parsed.head].key = bson::Value::MinValue;
            self.arena[index_parsed.tail].key = bson::Value::MaxValue;

            Ok(index_parsed)
        }
    }

    let mut data_builder = DataBuilder::new(data_blocks);
    let mut index_builder = IndexBuilder::new(index_nodes, &mut data_builder);

    let mut collections = IndexMap::new();

    // parse collection pages
    for (key, page) in header.collections.iter() {
        let page = page.as_i32().ok_or_else(ParseError::invalid_database)? as u32;
        let page_buffer = *pages
            .get(page as usize)
            .ok_or_else(ParseError::invalid_database)?;
        let mut collection = RawCollectionPage::parse(page_buffer)?;

        let mut indexes = IndexMap::new();

        let mut data_keys = HashMap::<PageAddress, ArenaKey<DbDocument>>::new();

        {
            let index = collection
                .indexes
                .remove("_id")
                .ok_or_else(ParseError::no_id_index)?;
            indexes.insert(
                "_id".to_string(),
                index_builder.build(index, |data_builder, data_block| {
                    let data = data_builder.parse(data_block)?;
                    data_keys.insert(data_block, data);
                    Ok(Some(data))
                })?,
            );
        }

        for (name, index) in collection.indexes {
            if name.as_str() == "_id" {
                continue;
            }

            indexes.insert(
                name.clone(),
                index_builder.build(index, |_, data_block| {
                    Ok(data_keys.get(&data_block).cloned())
                })?,
            );
        }

        let collection = Collection {
            indexes,
            #[cfg(feature = "sequential-index")]
            last_id: None,
        };

        collections.insert(CaseInsensitiveString(key.to_string()), collection);
    }

    Ok(LiteDBFile {
        collections,
        creation_time: header.creation_time,
        pragmas: header.pragmas,

        index_arena: index_builder.arena,
        data: data_builder.arena,
    })
}

mod raw_index_node {
    use super::*;

    use offsets::index_node::*;

    #[derive(Debug)]
    pub(super) struct RawIndexNode {
        pub slot: u8,
        pub levels: u8,
        pub key: bson::Value,
        pub data_block: PageAddress,
        //pub next_node: PageAddress,
        pub prev: Vec<PageAddress>,
        pub next: Vec<PageAddress>,
    }

    impl RawIndexNode {
        pub fn parse(block: &BufferSlice) -> ParseResult<Self> {
            let slot = block.read_u8(P_SLOT);
            let levels = block.read_u8(P_LEVELS);
            let data_block = block.read_page_address(P_DATA_BLOCK);
            //let next_node = block.read_page_address(P_NEXT_NODE);

            let mut next = Vec::with_capacity(levels as usize);
            let mut prev = Vec::with_capacity(levels as usize);

            for i in 0..levels as usize {
                let prev_addr =
                    block.read_page_address(P_PREV_NEXT + i * PageAddress::SERIALIZED_SIZE * 2);
                let next_addr = block.read_page_address(
                    P_PREV_NEXT
                        + i * PageAddress::SERIALIZED_SIZE * 2
                        + PageAddress::SERIALIZED_SIZE,
                );
                prev.push(prev_addr);
                next.push(next_addr);
            }

            let key_ptr = calc_key_ptr(levels);
            let key = block.read_index_key(key_ptr)?;

            Ok(Self {
                slot,
                levels,
                key,
                data_block,
                //next_node,
                prev,
                next,
            })
        }
    }
}

mod raw_data_block {
    use super::*;
    use std::fmt::Debug;

    use offsets::data_block::*;

    pub(super) struct RawDataBlock<'a> {
        extend: bool,
        next_block: PageAddress,
        buffer: &'a BufferSlice,
    }

    impl<'a> RawDataBlock<'a> {
        pub fn parse(segment: &'a BufferSlice) -> Self {
            let extend = segment.read_bool(P_EXTEND);
            let next_block = segment.read_page_address(P_NEXT_BLOCK);
            let buffer = segment.slice(P_BUFFER, segment.len() - P_BUFFER);

            Self {
                extend,
                next_block,
                buffer,
            }
        }

        pub fn buffer(&self) -> &'a BufferSlice {
            self.buffer
        }

        pub fn next_block(&self) -> PageAddress {
            self.next_block
        }
    }

    impl Debug for RawDataBlock<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RawDataBlock")
                .field("extend", &self.extend)
                .field("next_block", &self.next_block)
                .field("buffer", &self.buffer.as_bytes())
                .finish()
        }
    }
}

mod header_page {
    use super::*;

    use offsets::header_page::*;

    #[derive(Debug)]
    pub(super) struct HeaderPage {
        pub creation_time: bson::DateTime,
        pub pragmas: EnginePragmas,
        pub collections: bson::Document,
        #[allow(dead_code)] // for page structure; not needed for
        last_page_id: u32,
        #[allow(dead_code)] // for page structure; not needed for
        free_empty_page_list: u32,
    }

    impl HeaderPage {
        pub fn parse(buffer: &PageBuffer) -> ParseResult<Self> {
            let info = buffer.read_bytes(P_HEADER_INFO, HEADER_INFO.len());
            let version = buffer.read_byte(P_FILE_VERSION);

            if info != HEADER_INFO || version != FILE_VERSION {
                return Err(ParseError::invalid_database());
            }

            let collections_area = buffer.slice(P_COLLECTIONS, COLLECTIONS_SIZE);

            Ok(Self {
                creation_time: buffer.read_date_time(P_CREATION_TIME)?,
                pragmas: EnginePragmas::parse(buffer),
                free_empty_page_list: buffer.read_u32(P_FREE_EMPTY_PAGE_ID),
                last_page_id: buffer.read_u32(P_LAST_PAGE_ID),
                collections: BufferReader::single(collections_area).read_document()?,
            })
        }
    }
}

mod collection_page {
    use super::*;

    use offsets::collection_page::*;

    #[derive(Debug)]
    pub(super) struct RawCollectionPage {
        #[allow(dead_code)]
        pub free_data_page_list: [u32; 5],
        pub indexes: HashMap<String, RawCollectionIndex>,
    }

    impl RawCollectionPage {
        pub fn parse(buffer: &PageBuffer) -> ParseResult<Self> {
            let mut free_data_page_list = [u32::MAX; PAGE_FREE_LIST_SLOTS];
            let mut indexes = HashMap::new();

            if buffer.page_type() != Some(PageType::Collection) {
                return Err(ParseError::bad_reference());
            }

            let area = buffer.slice(PAGE_HEADER_SIZE, PAGE_SIZE - PAGE_HEADER_SIZE);
            let mut reader = BufferReader::single(area);

            for item in free_data_page_list.iter_mut() {
                *item = reader.read_u32();
            }

            reader.skip(P_INDEXES - PAGE_HEADER_SIZE - reader.position());

            let count = reader.read_u8().into();

            for _ in 0..count {
                let index = RawCollectionIndex::parse(&mut reader)?;
                indexes.insert(index.name.clone(), index);
            }

            Ok(Self {
                free_data_page_list,
                indexes,
            })
        }
    }

    #[derive(Debug)]
    pub(super) struct RawCollectionIndex {
        // same as CollectionIndex
        pub slot: u8,
        pub index_type: u8,
        pub name: String,
        pub expression: String,
        pub unique: bool,
        pub reserved: u8,
        pub head: PageAddress,
        pub tail: PageAddress,
        #[allow(dead_code)] // pages format only
        pub free_index_page_list: u32,
        pub bson_expr: BsonExpression,
    }

    impl RawCollectionIndex {
        fn parse(reader: &mut BufferReader) -> ParseResult<Self> {
            let slot = reader.read_u8();
            let index_type = reader.read_u8();
            let name = reader
                .read_cstring()
                .ok_or_else(ParseError::invalid_database)?;
            let expression = reader
                .read_cstring()
                .ok_or_else(ParseError::invalid_database)?;
            let unique = reader.read_bool();
            let head = reader.read_page_address();
            let tail = reader.read_page_address();
            let reserved = reader.read_u8();
            let free_index_page_list = reader.read_u32();
            let parsed = BsonExpression::create(&expression)?;

            Ok(Self {
                slot,
                index_type,
                name,
                expression,
                unique,
                head,
                tail,
                reserved,
                free_index_page_list,
                bson_expr: parsed,
            })
        }
    }
}
