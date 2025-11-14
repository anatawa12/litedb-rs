use crate::constants::{MAX_INDEX_KEY_LENGTH, MAX_LEVEL_LENGTH};
use crate::expression::BsonExpression;
use crate::file_io::{Collection, CollectionIndex, DbDocument, IndexNode, get_key_length};
use crate::utils::{ArenaKey, Collation, KeyArena, Order};
use crate::{Error, bson};
use std::collections::HashSet;
use std::hash::{BuildHasher, RandomState};

pub(crate) struct IndexHelper;

impl IndexHelper {
    pub fn create_index<'a>(
        arena: &mut KeyArena<IndexNode>,

        collection: &'a mut Collection,

        name: &str,
        expression: BsonExpression,
        unique: bool,
    ) -> &'a mut CollectionIndex {
        let slot = collection
            .indexes
            .values()
            .map(|x| x.slot + 1)
            .max()
            .unwrap_or(0);

        let head = arena.alloc(IndexNode::new(
            slot,
            MAX_LEVEL_LENGTH,
            bson::Value::MinValue,
        ));
        let tail = arena.alloc(IndexNode::new(
            slot,
            MAX_LEVEL_LENGTH,
            bson::Value::MaxValue,
        ));

        arena[head].next[0] = Some(tail);
        arena[tail].prev[0] = Some(head);

        collection
            .indexes
            .entry(name.into())
            .insert_entry(CollectionIndex {
                slot,
                index_type: 0,
                name: name.into(),
                expression: expression.source().into(),
                unique,
                reserved: 0,
                bson_expr: expression,
                head,
                tail,
            })
            .into_mut()
    }

    pub fn add_node(
        arena: &mut KeyArena<IndexNode>,
        data_arena: &mut KeyArena<DbDocument>,
        collation: &Collation,
        index: &CollectionIndex,
        key: bson::Value,
        data_block: ArenaKey<DbDocument>,
    ) -> Result<ArenaKey<IndexNode>, Error> {
        // RustChange: Document is valid since its order is not determinable
        if key == bson::Value::MinValue
            || key == bson::Value::MaxValue
            || key.ty() == bson::BsonType::Document
        {
            return Err(Error::invalid_index_key_type());
        }

        let levels = Self::flip();

        Self::add_node_with_levels(arena, data_arena, collation, index, key, data_block, levels)
    }

    pub fn add_node_with_levels(
        arena: &mut KeyArena<IndexNode>,
        data_arena: &mut KeyArena<DbDocument>,
        collation: &Collation,
        index: &CollectionIndex,
        key: bson::Value,
        data_block: ArenaKey<DbDocument>,
        insert_levels: u8,
    ) -> Result<ArenaKey<IndexNode>, Error> {
        let key_length = get_key_length(&key);

        if key_length > MAX_INDEX_KEY_LENGTH {
            return Err(Error::index_key_too_long());
        }

        let node_key = arena.alloc(IndexNode::new(index.slot, insert_levels, key));
        arena[node_key].data = Some(data_block);

        let mut left_node = index.head;

        for current_level in (0..=(MAX_LEVEL_LENGTH - 1)).rev() {
            let mut right = arena[left_node].next[current_level as usize];

            let mut counter = 0;
            while let Some(right_key) = right.take_if(|&mut right| right != index.tail) {
                assert!(
                    counter < arena.len(),
                    "Detected loop in AddNode({:?})",
                    node_key
                );
                counter += 1;

                let diff = collation.compare(&arena[right_key].key, &arena[node_key].key);

                if diff.is_eq() && index.unique {
                    return Err(Error::index_duplicate_key(
                        &index.name,
                        arena[node_key].key.clone(),
                    ));
                }

                if diff.is_gt() {
                    break;
                }

                right = arena[right_key].next[current_level as usize];
                left_node = right_key;
            }

            if current_level < insert_levels {
                // level == length
                // prev: immediately before new node
                // node: new inserted node
                // next: right node from prev (where left is pointing)

                let prev = left_node;
                let next = arena[left_node].next[current_level as usize].unwrap_or(index.tail);

                // set new node pointer links with current level sibling
                arena[node_key].next[current_level as usize] = Some(next);
                arena[node_key].prev[current_level as usize] = Some(prev);

                // fix sibling pointer to new node
                arena[left_node].next[current_level as usize] = Some(node_key);

                right = arena[node_key].next[current_level as usize]; // next

                // mark right page as dirty (after change PrevID)
                arena[right.unwrap()].prev[current_level as usize] = Some(node_key);
            }
        }

        data_arena[data_block].index_nodes.push(node_key);

        Ok(node_key)
    }

    fn flip() -> u8 {
        let mut levels = 1;

        //for (int R = Randomizer.Next(); (R & 1) == 1; R >>= 1)
        let mut random = (RandomState::new().hash_one(0) & 0xFFFFFFFF) as u32;
        while (random & 1) == 1 {
            levels += 1;
            if levels == MAX_LEVEL_LENGTH {
                break;
            }
            random >>= 1;
        }

        levels
    }

    pub fn get_node_list(
        index_nodes: &[ArenaKey<IndexNode>],
    ) -> impl Iterator<Item = ArenaKey<IndexNode>> {
        index_nodes
            .iter()
            .skip(1) // skip pk node
            .copied()
    }

    pub fn delete_all(arena: &mut KeyArena<IndexNode>, index_nodes: &[ArenaKey<IndexNode>]) {
        for &current_key in index_nodes {
            let node = arena.free(current_key);
            Self::delete_single_node(arena, node);
        }
    }

    pub fn delete_list(
        arena: &mut KeyArena<IndexNode>,
        index_nodes: &mut Vec<ArenaKey<IndexNode>>,
        to_delete: HashSet<ArenaKey<IndexNode>>,
    ) {
        index_nodes.retain(|&current_key| {
            let retain = !to_delete.contains(&current_key);
            if !retain {
                let node = arena.free(current_key);
                Self::delete_single_node(arena, node);
            }
            retain
        });
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

    pub fn drop_index(
        arena: &mut KeyArena<IndexNode>,
        data: &mut KeyArena<DbDocument>,
        pk_index: &CollectionIndex,
        index: CollectionIndex,
    ) {
        let slot = index.slot;

        for pk_node in Self::find_all(arena, pk_index, Order::Ascending) {
            data[arena[pk_node].data.unwrap()]
                .index_nodes
                .retain(|&index_key| {
                    let retain = arena[index_key].slot != slot;
                    if !retain {
                        // remove node
                        arena.free(index_key);
                    }
                    retain
                });
        }

        // removing head/tail index nodes
        arena.free(index.head);
        arena.free(index.tail);
    }

    pub fn find_all(
        arena: &KeyArena<IndexNode>,
        index: &CollectionIndex,
        order: Order,
    ) -> Vec<ArenaKey<IndexNode>> {
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

            nodes.push(key);
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
    ) -> Option<&'a IndexNode> {
        let mut left_node = if order == Order::Ascending {
            &arena[index.head]
        } else {
            &arena[index.tail]
        };

        for level in (0..=(MAX_LEVEL_LENGTH - 1)).rev() {
            let mut right = left_node.get_next_prev(level, order);

            let mut counter = 0;
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
                        return Some(right_node);
                    };
                }

                // if equals, return index node
                if diff.is_eq() {
                    return Some(right_node);
                }

                right = right_node.get_next_prev(level, order);
                left_node = right_node;
            }
        }

        None
    }
}
