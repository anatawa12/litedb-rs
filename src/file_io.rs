mod index_helper;
mod offsets;
mod operations;
mod page;
mod parser;
mod pragma;
mod writer;

use std::sync::OnceLock;
use crate::bson;
use crate::expression::BsonExpression;
use crate::utils::{ArenaKey, CaseInsensitiveString, KeyArena, Order as InternalOrder};
use indexmap::IndexMap;
pub use operations::Order;
use pragma::EnginePragmas;

pub(crate) use writer::get_key_length;
use crate::file_io::index_helper::IndexHelper;

#[derive(Debug)]
pub struct LiteDBFile {
    collections: IndexMap<CaseInsensitiveString, Collection>,
    creation_time: bson::DateTime,
    pragmas: EnginePragmas,
    index_arena: KeyArena<IndexNode>,
    data: KeyArena<DbDocument>,
}

impl Default for LiteDBFile {
    fn default() -> Self {
        Self::new()
    }
}

impl LiteDBFile {
    pub fn new() -> Self {
        Self {
            collections: IndexMap::new(),
            creation_time: bson::DateTime::now(),
            pragmas: EnginePragmas::default(),
            index_arena: KeyArena::new(),
            data: KeyArena::new(),
        }
    }
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

#[derive(Debug)]
struct Collection {
    indexes: IndexMap<String, CollectionIndex>,
    #[cfg(feature = "sequential-index")]
    last_id: Option<i64>,
}

impl Collection {
    fn new(
        index_arena: &mut KeyArena<IndexNode>,
    ) -> Self {
        let mut collection = Self {
                indexes: IndexMap::new(),
                #[cfg(feature = "sequential-index")]
                last_id: None,
            };

        static EXPRESSION: OnceLock<BsonExpression> = OnceLock::new();

        let expression = EXPRESSION.get_or_init(|| BsonExpression::create("$._id").unwrap()).clone();

        IndexHelper::create_index(
            index_arena,
            &mut collection,
            "_id",
            expression,
            true,
        );

        collection
    }

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
    // First node in this list must be _id PK index
    index_nodes: Vec<ArenaKey<IndexNode>>,
}

impl DbDocument {
    fn new(data: bson::Document) -> DbDocument {
        Self {
            data,
            index_nodes: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct IndexNode {
    slot: u8,
    levels: u8,
    key: bson::Value,
    data: Option<ArenaKey<DbDocument>>,
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
