mod index_helper;
mod offsets;
mod operations;
mod page;
mod parser;
mod pragma;
mod writer;

use crate::bson;
use crate::expression::BsonExpression;
use crate::utils::{ArenaKey, CaseInsensitiveString, KeyArena, Order as InternalOrder};
use std::collections::HashMap;

pub use operations::Order;
use pragma::EnginePragmas;

pub(crate) use writer::get_key_length;

#[derive(Debug)]
pub struct LiteDBFile {
    collections: HashMap<CaseInsensitiveString, Collection>,
    creation_time: bson::DateTime,
    pragmas: EnginePragmas,
    index_arena: KeyArena<IndexNode>,
    data: KeyArena<DbDocument>,
}

#[derive(Debug, Copy, Clone)]
pub enum BsonAutoId {
    #[cfg(feature = "sequential-index")]
    Int32 = 2,
    #[cfg(feature = "sequential-index")]
    Int64 = 3,
    ObjectId = 10,
    Guid = 11,
}

#[derive(Debug, Default)]
struct Collection {
    indexes: HashMap<String, CollectionIndex>,
    #[cfg(feature = "sequential-index")]
    last_id: Option<i64>,
}

impl Collection {
    fn pk_index(&self) -> &CollectionIndex {
        &self.indexes["_id"]
    }
}

#[derive(Debug)]
struct CollectionIndex {
    // same as CollectionIndex
    slot: u8,
    #[allow(dead_code)] // legacy; reserved
    index_type: u8,
    name: String,
    expression: String,
    unique: bool,
    #[allow(dead_code)] // reserved
    reserved: u8,
    bson_expr: BsonExpression,
    head: ArenaKey<IndexNode>,
    tail: ArenaKey<IndexNode>,
}

#[derive(Debug)]
struct DbDocument {
    data: bson::Document,
}

impl DbDocument {
    fn new(data: bson::Document) -> DbDocument {
        Self { data }
    }
}

#[derive(Debug)]
struct IndexNode {
    slot: u8,
    levels: u8,
    key: bson::Value,
    data: Option<ArenaKey<DbDocument>>,
    next_node: Option<ArenaKey<IndexNode>>, // next index targeting same data
    prev: Vec<Option<ArenaKey<IndexNode>>>, // prev key in index skip list
    next: Vec<Option<ArenaKey<IndexNode>>>, // prev key in index skip list
}

impl IndexNode {
    pub(crate) fn new(slot: u8, levels: u8, key: bson::Value) -> Self {
        IndexNode {
            slot,
            levels,
            key,
            data: None,
            next_node: None,
            prev: vec![None; levels as usize],
            next: vec![None; levels as usize],
        }
    }

    pub(crate) fn get_next_prev(
        &self,
        level: u8,
        order: InternalOrder,
    ) -> Option<ArenaKey<IndexNode>> {
        match order {
            InternalOrder::Ascending => self.next[level as usize],
            InternalOrder::Descending => self.prev[level as usize],
        }
    }
}
