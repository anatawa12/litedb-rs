use crate::bson;
use crate::engine::{ MAX_LEVEL_LENGTH};
use crate::expression::BsonExpression;
use crate::file_io::{Collection, CollectionIndex, IndexNode};
use crate::utils::{ArenaKey, Collation, KeyArena, Order};

pub(crate) struct IndexHelper;

impl IndexHelper {
    pub fn create_index<'a>(
        arena: &mut KeyArena<IndexNode>,

        collection: &'a mut Collection,

        name: &str,
        expression: BsonExpression,
        unique: bool,
    ) -> &'a mut CollectionIndex {
        let slot = collection.indexes.values().map(|x| x.slot + 1).max().unwrap_or(0);

        let head = arena.alloc(IndexNode::new(slot, MAX_LEVEL_LENGTH, bson::Value::MinValue));
        let tail = arena.alloc(IndexNode::new(slot, MAX_LEVEL_LENGTH, bson::Value::MinValue));

        arena[head].next[0] = Some(tail);
        arena[tail].prev[0] = Some(head);

        collection.indexes.entry(name.into()).insert_entry(CollectionIndex {
            slot,
            index_type: 0,
            name: name.into(),
            expression: expression.source().into(),
            unique,
            reserved: 0,
            bson_expr: expression,
            head, 
            tail,
        }).into_mut()
    }

    // TODO: add_node

    pub fn delete_all(
        arena: &mut KeyArena<IndexNode>,
        first: ArenaKey<IndexNode>,
    ) {
        // TODO? check for recursion
        let mut current = Some(first);
        while let Some(current_key) = current {
            let node = arena.free(current_key);
            current = node.next_node;

            Self::delete_single_node(arena, node)
        }
    }

    fn delete_single_node(arena: &mut KeyArena<IndexNode>, node: IndexNode) {
        for level in (0..node.levels).rev() {
            // get previous and next nodes (between my deleted node)

            if let Some(prev) = node.prev[level as usize] {
                let prev_node = &mut arena[prev];
                prev_node.next[level as usize] = node.next[level as usize];
            }

            if let Some(next) = node.next[level as usize] {
                let next_node = &mut arena[next];
                next_node.prev[level as usize] = node.prev[level as usize];
            }
        }
    }

    pub fn find_all<'a> (
        arena: &'a KeyArena<IndexNode>,
        index: &CollectionIndex,
        order: Order,
    ) -> Vec<&'a IndexNode> {
        let mut cur = if order == Order::Ascending {
            &arena[index.head]
        } else {
            &arena[index.tail]
        };
        let mut nodes = vec![];
        //let counter = 0u;

        let mut current = cur.get_next_prev(0, order);
        while let Some(key) = current {
            //ENSURE(counter++ < _maxItemsCount, "Detected loop in FindAll({0})", index.Name);

            cur = &arena[key];

            // stop if node is head/tail
            if matches!(cur.key, bson::Value::MaxValue | bson::Value::MinValue) {
                break;
            }

            current = cur.get_next_prev(0, order);

            nodes.push(cur);
        }

        nodes
    }

    pub fn find<'a>(
        arena: &'a KeyArena<IndexNode>,
        collation: &Collation,
        index: &CollectionIndex,
        value: &bson::Value,
        sibling: bool,
        order: Order,
    ) -> Option<(&'a IndexNode, ArenaKey<IndexNode>)> {
        let mut left_node = if order == Order::Ascending {
            &arena[index.head]
        } else {
            &arena[index.tail]
        };

        let mut counter = 0;

        for level in (0..=(MAX_LEVEL_LENGTH - 1)).rev() {
            let mut right = left_node.get_next_prev(level, order);

            while let Some(right_key) = right {
                assert!(
                    counter < arena.len(),
                    "Detected loop in Find({}, {:?})",
                    index.name,
                    value
                );
                counter += 1;

                let right_node = &arena[right_key];

                let diff = collation.compare(&right_node.key, value);

                if order == diff && (level > 0 || !sibling) {
                    break; // go down one level
                }

                if order == diff && level == 0 && sibling {
                    // is head/tail?
                    if matches!(
                        &right_node.key,
                        bson::Value::MinValue | bson::Value::MaxValue
                    ) {
                        return None;
                    } else {
                        return Some((right_node, right_key));
                    };
                }

                // if equals, return index node
                if diff.is_eq() {
                    return Some((right_node, right_key));
                }

                right = right_node.get_next_prev(level, order);
                left_node = right_node;
            }
        }

        None
    }
}
