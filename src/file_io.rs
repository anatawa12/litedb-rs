mod operations;
mod page;
mod parser;
mod index_helper;

use crate::bson;
use crate::engine::{EnginePragmas};
use crate::expression::BsonExpression;
use crate::utils::{ArenaKey, CaseInsensitiveString, KeyArena, Order};
use std::collections::HashMap;

#[derive(Debug)]
pub struct LiteDBFile {
    collections: HashMap<CaseInsensitiveString, Collection>,
    creation_time: bson::DateTime,
    pragmas: EnginePragmas,
    index_arena: KeyArena<IndexNode>,
    data: KeyArena<bson::Document>,
}

#[derive(Debug)]
struct Collection {
    indexes: HashMap<String, CollectionIndex>,
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
    index_type: u8,
    name: String,
    expression: String,
    unique: bool,
    reserved: u8,
    bson_expr: BsonExpression,
    head: ArenaKey<IndexNode>,
    tail: ArenaKey<IndexNode>,
}

#[derive(Debug)]
struct IndexNode {
    slot: u8,
    levels: u8,
    key: bson::Value,
    data: Option<ArenaKey<bson::Document>>,
    next_node: Option<ArenaKey<IndexNode>>, // next index targeting same data
    prev: Vec<Option<ArenaKey<IndexNode>>>, // prev key in index skip list
    next: Vec<Option<ArenaKey<IndexNode>>>, // prev key in index skip list
}

impl IndexNode {
    pub(crate) fn new(
        slot: u8,
        levels: u8,
        key: bson::Value,
    ) -> Self {
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

    pub(crate) fn get_next_prev(&self, level: u8, order: Order) -> Option<ArenaKey<IndexNode>> {
        match order {
            Order::Ascending => self.next[level as usize],
            Order::Descending => self.prev[level as usize],
        }
    }
}
