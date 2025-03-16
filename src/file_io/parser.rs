use crate::bson;
use crate::buffer_reader::BufferReader;
use crate::constants::{PAGE_FREE_LIST_SLOTS, PAGE_HEADER_SIZE, PAGE_SIZE};
use crate::utils::{ArenaKey, BufferSlice, CaseInsensitiveString, KeyArena, PageAddress};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};

use super::*;
use crate::file_io::page::PageBuffer;
use crate::file_io::page::PageType;
use crate::file_io::parser::collection_page::RawCollectionPage;
use crate::file_io::parser::header_page::HeaderPage;
use crate::file_io::parser::raw_data_block::RawDataBlock;

use raw_index_node::RawIndexNode;

#[derive(Debug)]
pub enum ParseError {
    InvalidDatabase,
    BadPageId,
    PagePageType,
    BsonExpression(crate::expression::ParseError),
    BadBlockReference,
}

impl ParseError {
    fn bad_block_reference() -> Self {
        Self::BadBlockReference
    }
}

impl From<crate::Error> for ParseError {
    fn from(value: crate::Error) -> Self {
        todo!("{:?}", value)
    }
}

impl From<crate::expression::ParseError> for ParseError {
    fn from(value: crate::expression::ParseError) -> Self {
        Self::BsonExpression(value)
    }
}

type ParseResult<T> = Result<T, ParseError>;

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
            return Err(ParseError::BadPageId);
        }
        if page.page_type().is_none() {
            return Err(ParseError::PagePageType);
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
        arena: KeyArena<bson::Document>,
        raw_node: HashMap<PageAddress, RawDataBlock<'buf>>,
        keys: HashMap<PageAddress, ArenaKey<bson::Document>>,
    }

    impl<'buf> DataBuilder<'buf> {
        pub fn new(raw_node: HashMap<PageAddress, RawDataBlock<'buf>>) -> Self {
            Self {
                arena: KeyArena::new(),
                keys: HashMap::new(),
                raw_node,
            }
        }

        fn get_opt(
            &mut self,
            position: PageAddress,
        ) -> ParseResult<Option<ArenaKey<bson::Document>>> {
            if position.is_empty() {
                Ok(None)
            } else {
                Ok(Some(self.get(position)?))
            }
        }

        fn get(&mut self, position: PageAddress) -> ParseResult<ArenaKey<bson::Document>> {
            match self.keys.entry(position) {
                Entry::Occupied(e) => Ok(*e.get()),
                Entry::Vacant(e) => {
                    let mut buffers = vec![];

                    {
                        let mut cur = position;

                        while !cur.is_empty() {
                            let raw = self
                                .raw_node
                                .remove(&cur)
                                .ok_or_else(ParseError::bad_block_reference)?;
                            buffers.push(raw.buffer());
                            cur = raw.next_block();
                        }
                    }

                    let mut reader = BufferReader::fragmented(buffers);
                    let document = reader.read_document()?;

                    Ok(*e.insert(self.arena.alloc(document)))
                }
            }
        }
    }

    struct IndexBuilder<'buf, 'a> {
        arena: KeyArena<IndexNode>,
        raw_node: HashMap<PageAddress, RawIndexNode>,
        keys: HashMap<PageAddress, ArenaKey<IndexNode>>,
        data_builder: &'a mut DataBuilder<'buf>,
    }

    impl<'buf, 'a> IndexBuilder<'buf, 'a> {
        fn new(
            raw_node: HashMap<PageAddress, RawIndexNode>,
            data_builder: &'a mut DataBuilder<'buf>,
        ) -> Self {
            Self {
                arena: KeyArena::new(),
                keys: HashMap::new(),
                raw_node,
                data_builder,
            }
        }

        fn get(&mut self, position: PageAddress) -> ParseResult<ArenaKey<IndexNode>> {
            if let Some(key) = self.keys.get(&position) {
                return Ok(*key);
            }

            struct RawIndexAddress {
                data_block: PageAddress,
                next_node: PageAddress,
                prev: Vec<PageAddress>,
                next: Vec<PageAddress>,
                key: ArenaKey<IndexNode>,
            }

            let mut address_map = Vec::<RawIndexAddress>::new();

            let mut processing = HashSet::<PageAddress>::new();
            let mut process_queue = VecDeque::new();
            process_queue.push_back(position);
            processing.insert(position);

            while let Some(current) = process_queue.pop_front() {
                let raw = self
                    .raw_node
                    .remove(&current)
                    .ok_or_else(ParseError::bad_block_reference)?;

                let key = *self
                    .keys
                    .entry(current)
                    .insert_entry(self.arena.alloc(IndexNode {
                        slot: raw.slot,
                        levels: raw.levels,
                        key: raw.key,
                        data: self.data_builder.get_opt(raw.data_block)?,
                        next_node: None,
                        prev: vec![None; raw.levels as usize],
                        next: vec![None; raw.levels as usize],
                    }))
                    .get();

                for &addr in (raw.next.iter())
                    .chain(raw.prev.iter())
                    .chain([&raw.next_node])
                    .filter(|x| !x.is_empty())
                {
                    if processing.insert(addr) && !self.keys.contains_key(&addr) {
                        process_queue.push_back(addr);
                    }
                }

                address_map.push(RawIndexAddress {
                    key,
                    data_block: raw.data_block,
                    next_node: raw.next_node,
                    prev: raw.prev,
                    next: raw.next,
                });
            }

            fn get(
                keys: &mut HashMap<PageAddress, ArenaKey<IndexNode>>,
                addr: PageAddress,
            ) -> Option<ArenaKey<IndexNode>> {
                if addr.is_empty() {
                    None
                } else {
                    Some(keys[&addr])
                }
            }

            for addresses in address_map {
                let node = &mut self.arena[addresses.key];
                node.next_node = get(&mut self.keys, addresses.next_node);
                for (node, &addr) in node.prev.iter_mut().zip(addresses.prev.iter()) {
                    *node = get(&mut self.keys, addr);
                }
                for (node, &addr) in node.next.iter_mut().zip(addresses.next.iter()) {
                    *node = get(&mut self.keys, addr);
                }
            }

            Ok(*self.keys.get(&position).unwrap())
        }
    }

    let mut data_builder = DataBuilder::new(data_blocks);
    let mut index_builder = IndexBuilder::new(index_nodes, &mut data_builder);

    let mut collections = HashMap::new();

    // parse collection pages
    for (key, page) in header.collections.iter() {
        let page = page.as_i32().ok_or(ParseError::InvalidDatabase)? as u32;
        let page_buffer = *pages
            .get(page as usize)
            .ok_or(ParseError::InvalidDatabase)?;
        let collection = RawCollectionPage::parse(page_buffer)?;

        let mut indexes = HashMap::new();

        for (name, index) in collection.indexes {
            indexes.insert(
                name.clone(),
                CollectionIndex {
                    slot: index.slot,
                    index_type: index.index_type,
                    name: index.name,
                    expression: index.expression,
                    unique: index.unique,
                    reserved: index.reserved,
                    bson_expr: index.bson_expr,
                    head: index_builder.get(index.head)?,
                    tail: index_builder.get(index.tail)?,
                },
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
        pub next_node: PageAddress,
        pub prev: Vec<PageAddress>,
        pub next: Vec<PageAddress>,
    }

    impl RawIndexNode {
        pub fn parse(block: &BufferSlice) -> ParseResult<Self> {
            let slot = block.read_u8(P_SLOT);
            let levels = block.read_u8(P_LEVELS);
            let data_block = block.read_page_address(P_DATA_BLOCK);
            let next_node = block.read_page_address(P_NEXT_NODE);

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
                next_node,
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
                return Err(ParseError::InvalidDatabase);
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
                return Err(ParseError::PagePageType);
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
                .ok_or_else(crate::Error::invalid_page)?;
            let expression = reader
                .read_cstring()
                .ok_or_else(crate::Error::invalid_page)?;
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

#[test]
fn test_parse() {
    let buffer = std::fs::read(
        "/Users/anatawa12/.local/share/VRChatCreatorCompanion/vcc.liteDb backup copy",
    )
    .unwrap();
    let file = parse(&buffer).unwrap();
    println!("{:#?}", file);
}
