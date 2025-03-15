use super::page::PageBuffer;
use super::{Collection, CollectionIndex, IndexNode, LiteDBFile};
use crate::bson;
use crate::engine::{BufferReader, PAGE_SIZE, PageAddress, PageType};
use crate::utils::{ArenaKey, BufferSlice, CaseInsensitiveString, KeyArena};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};

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
    for (key, page) in header.collections().iter() {
        let page = page.as_i32().ok_or(ParseError::InvalidDatabase)? as u32;
        let page_buffer = *pages
            .get(page as usize)
            .ok_or(ParseError::InvalidDatabase)?;
        let collection = RawCollectionPage::parse(page_buffer)?;

        let mut indexes = HashMap::new();

        for (name, index) in collection.indexes() {
            indexes.insert(
                name.clone(),
                CollectionIndex {
                    slot: index.slot(),
                    index_type: index.index_type(),
                    name: index.name().to_string(),
                    expression: index.expression().to_string(),
                    unique: index.unique(),
                    reserved: index.reserved(),
                    bson_expr: index.bson_expr().clone(),
                    head: index_builder.get(index.head())?,
                    tail: index_builder.get(index.tail())?,
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
        creation_time: header.creation_time(),
        pragmas: header.pragmas().clone(),

        index_arena: index_builder.arena,
        data: data_builder.arena,
    })
}

mod raw_index_node {
    use super::*;

    const P_SLOT: usize = 0; // 00-00 [byte]
    const P_LEVELS: usize = 1; // 01-01 [byte]
    const P_DATA_BLOCK: usize = 2; // 02-06 [PageAddress]
    const P_NEXT_NODE: usize = 7; // 07-11 [PageAddress]
    const P_PREV_NEXT: usize = 12; // 12-(_level * 5 [PageAddress] * 2 [prev-next])

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

    fn calc_key_ptr(levels: u8) -> usize {
        P_PREV_NEXT + levels as usize * PageAddress::SERIALIZED_SIZE * 2
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

    const P_EXTEND: usize = 0; // 00-00 [byte]
    const P_NEXT_BLOCK: usize = 1; // 01-05 [pageAddress]
    const P_BUFFER: usize = 6; // 06-EOF [byte[]]

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
    use crate::engine::{BufferReader, EnginePragmas};

    const HEADER_INFO: &[u8] = b"** This is a LiteDB file **";
    const FILE_VERSION: u8 = 8;

    const P_HEADER_INFO: usize = 32; // 32-58 (27 bytes)
    const P_FILE_VERSION: usize = 59; // 59-59 (1 byte)
    const P_FREE_EMPTY_PAGE_ID: usize = 60; // 60-63 (4 bytes)
    const P_LAST_PAGE_ID: usize = 64; // 64-67 (4 bytes)
    const P_CREATION_TIME: usize = 68; // 68-75 (8 bytes)

    //const P_PRAGMAS: usize = 76; // 76-190 (115 bytes)
    const P_INVALID_DATAFILE_STATE: usize = 191; // 191-191 (1 byte)

    const P_COLLECTIONS: usize = 192; // 192-8159 (8064 bytes)
    const COLLECTIONS_SIZE: usize = 8000; // 250 blocks with 32 bytes each

    #[derive(Debug)]
    pub(super) struct HeaderPage {
        creation_time: bson::DateTime,
        pragmas: EnginePragmas,
        collections: bson::Document,
        last_page_id: u32,
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

            let pragmas = EnginePragmas::default();

            pragmas.read(buffer)?;

            Ok(Self {
                creation_time: buffer.read_date_time(P_CREATION_TIME)?,
                pragmas,
                free_empty_page_list: buffer.read_u32(P_FREE_EMPTY_PAGE_ID),
                last_page_id: buffer.read_u32(P_LAST_PAGE_ID),
                collections: BufferReader::single(collections_area).read_document()?,
            })
        }

        pub fn creation_time(&self) -> bson::DateTime {
            self.creation_time
        }

        pub fn pragmas(&self) -> &EnginePragmas {
            &self.pragmas
        }

        pub fn collections(&self) -> &bson::Document {
            &self.collections
        }
    }
}

mod collection_page {
    use super::*;
    use crate::engine::{BufferReader, PAGE_FREE_LIST_SLOTS, PAGE_HEADER_SIZE};
    use crate::expression::BsonExpression;

    const P_INDEXES: usize = 96; // 96-8192 (64 + 32 header = 96)
    const P_INDEXES_COUNT: usize = PAGE_SIZE - P_INDEXES;

    #[derive(Debug)]
    pub(super) struct RawCollectionPage {
        free_data_page_list: [u32; 5],
        indexes: HashMap<String, RawCollectionIndex>,
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

        pub fn indexes(&self) -> &HashMap<String, RawCollectionIndex> {
            &self.indexes
        }
    }

    #[derive(Debug)]
    pub(super) struct RawCollectionIndex {
        // same as CollectionIndex
        slot: u8,
        index_type: u8,
        name: String,
        expression: String,
        unique: bool,
        reserved: u8,
        head: PageAddress,
        tail: PageAddress,
        free_index_page_list: u32,
        bson_expr: BsonExpression,
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

        pub fn slot(&self) -> u8 {
            self.slot
        }

        pub fn index_type(&self) -> u8 {
            self.index_type
        }

        pub fn name(&self) -> &str {
            &self.name
        }

        pub fn expression(&self) -> &str {
            &self.expression
        }

        pub fn unique(&self) -> bool {
            self.unique
        }

        pub fn reserved(&self) -> u8 {
            self.reserved
        }

        pub fn head(&self) -> PageAddress {
            self.head
        }

        pub fn tail(&self) -> PageAddress {
            self.tail
        }

        #[allow(dead_code)]
        pub fn free_index_page_list(&self) -> u32 {
            self.free_index_page_list
        }

        pub fn bson_expr(&self) -> &BsonExpression {
            &self.bson_expr
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
