mod page;
mod parser;

use crate::bson;
use crate::expression::BsonExpression;
use crate::utils::{ArenaKey, CaseInsensitiveString, KeyArena};
use std::collections::HashMap;

pub struct LiteDBFile {
    collections: HashMap<CaseInsensitiveString, Collection>,
    index_arena: KeyArena<IndexNode>,
    data: KeyArena<bson::Document>,
}

struct Collection {
    indexes: HashMap<String, CollectionIndex>,
}

struct CollectionIndex {
    // same as CollectionIndex
    index_type: u8,
    name: String,
    expression: String,
    unique: bool,
    reserved: u8,
    bson_expr: BsonExpression,
    head: ArenaKey<IndexNode>,
    tail: ArenaKey<IndexNode>,
}

struct IndexNode {
    slot: u8,
    levels: u8,
    key: bson::Value,
    data: ArenaKey<bson::Document>,
    next_node: ArenaKey<IndexNode>, // next index targeting same data
    prev: Vec<ArenaKey<IndexNode>>, // prev key in index skip list
    next: Vec<ArenaKey<IndexNode>>, // prev key in index skip list
}
