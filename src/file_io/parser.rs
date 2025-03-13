use super::LiteDBFile;
use super::page::PageBuffer;
use crate::bson;
use crate::engine::{PAGE_SIZE, PageAddress, PageType};
use crate::utils::BufferSlice;
use std::collections::HashMap;

use raw_index_node::RawIndexNode;

#[derive(Debug)]
enum ParseError {
    BadPageId,
    PagePageType,
}

impl From<crate::Error> for ParseError {
    fn from(value: crate::Error) -> Self {
        todo!("{:?}", value)
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

    println!("{:#?}", index_nodes);

    //let header = HeaderPage::load(&pages[0])?;

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

#[test]
fn test_parse() {
    let buffer = std::fs::read(
        "/Users/anatawa12/.local/share/VRChatCreatorCompanion/vcc.liteDb backup copy",
    )
    .unwrap();
    parse(&buffer).unwrap();
}
