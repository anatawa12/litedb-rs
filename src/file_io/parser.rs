use super::page::PageBuffer;
use super::LiteDBFile;
use crate::bson;
use crate::engine::{PageAddress, PageType, PAGE_SIZE};
use crate::utils::BufferSlice;
use std::collections::HashMap;

use crate::file_io::parser::header_page::HeaderPage;
use crate::file_io::parser::raw_data_block::RawDataBlock;
use raw_index_node::RawIndexNode;
use crate::file_io::parser::collection_page::RawCollectionPage;

#[derive(Debug)]
enum ParseError {
    InvalidDatabase,
    BadPageId,
    PagePageType,
    BsonExpression(crate::expression::ParseError)
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

pub(super) fn parse(data: &[u8]) -> ParseResult<LiteDBFile> {
    // if the length is not multiple of PAGE_SIZE, crop
    let data = &data[..(data.len() & !PAGE_SIZE)];

    let pages = data
        .chunks(PAGE_SIZE)
        .map(|page| PageBuffer::new(page))
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

    println!("header: {:#?}", header);

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

    println!("{:#?}", index_nodes);
    println!("{:#?}", data_blocks);

    // parse collection pages
    for (key, page) in header.collections().iter() {
        let page = page.as_i32().ok_or(ParseError::InvalidDatabase)? as u32;
        let page_buffer = *pages.get(page as usize).ok_or(ParseError::InvalidDatabase)?;
        let collection = RawCollectionPage::parse(page_buffer)?;
        println!("{key:#}: {collection:#?}");
    }

    todo!()
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
        slot: u8,
        levels: u8,
        key: bson::Value,
        data_block: PageAddress,
        next_node: PageAddress,
        prev: Vec<PageAddress>,
        next: Vec<PageAddress>,
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
    }

    impl<'a> Debug for RawDataBlock<'a> {
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
                let index = load_collection_index(&mut reader)?;
                indexes.insert(index.name.clone(), index);
            }

            Ok(Self {
                free_data_page_list,
                indexes,
            })
        }
    }

    #[derive(Debug)]
    struct RawCollectionIndex {
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

    fn load_collection_index(reader: &mut BufferReader) -> ParseResult<RawCollectionIndex> {
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

        Ok(RawCollectionIndex {
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

#[test]
fn test_parse() {
    let buffer = std::fs::read(
        "/Users/anatawa12/.local/share/VRChatCreatorCompanion/vcc.liteDb backup copy",
    )
    .unwrap();
    parse(&buffer).unwrap();
}
