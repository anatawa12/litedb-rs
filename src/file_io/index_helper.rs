use crate::constants::{MAX_INDEX_KEY_LENGTH, MAX_LEVEL_LENGTH};
use crate::expression::BsonExpression;
use crate::file_io::{Collection, CollectionIndex, IndexNode, get_key_length};
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
        collation: &Collation,
        index: &CollectionIndex,
        key: bson::Value,
        data_block: ArenaKey<bson::Document>,
        last: Option<ArenaKey<IndexNode>>,
    ) -> Result<ArenaKey<IndexNode>, Error> {
        // RustChange: Document is valid since its order is not determinable
        if key == bson::Value::MinValue
            || key == bson::Value::MaxValue
            || key.ty() == bson::BsonType::Document
        {
            return Err(Error::invalid_index_key_type());
        }

        let levels = Self::flip();

        Self::add_node_with_levels(arena, collation, index, key, data_block, levels, last)
    }

    pub fn add_node_with_levels(
        arena: &mut KeyArena<IndexNode>,
        collation: &Collation,
        index: &CollectionIndex,
        key: bson::Value,
        data_block: ArenaKey<bson::Document>,
        insert_levels: u8,
        last: Option<ArenaKey<IndexNode>>,
    ) -> Result<ArenaKey<IndexNode>, Error> {
        let key_length = get_key_length(&key);

        if key_length > MAX_INDEX_KEY_LENGTH {
            return Err(Error::index_key_too_long());
        }

        let node_key = arena.alloc(IndexNode::new(index.slot, insert_levels, key));
        arena[node_key].data = Some(data_block);

        let mut left_node = index.head;
        let mut counter = 0;

        for current_level in (0..=(MAX_LEVEL_LENGTH - 1)).rev() {
            let mut right = arena[left_node].next[current_level as usize];

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

        if let Some(last) = last {
            let last = &mut arena[last];
            assert_eq!(last.next_node, None, "last index node must point to null");

            last.next_node = Some(node_key);
        }

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
        arena: &KeyArena<IndexNode>,
        first: Option<ArenaKey<IndexNode>>,
    ) -> Vec<ArenaKey<IndexNode>> {
        let mut result = Vec::new();

        let mut current = first;
        while let Some(node_key) = current {
            let node = &arena[node_key];
            current = node.next_node;
            result.push(node_key)
        }

        result
    }

    pub fn delete_all(arena: &mut KeyArena<IndexNode>, first: ArenaKey<IndexNode>) {
        // TODO? check for recursion
        let mut current = Some(first);
        while let Some(current_key) = current {
            let node = arena.free(current_key);
            current = node.next_node;

            Self::delete_single_node(arena, node)
        }
    }

    pub fn delete_list(
        arena: &mut KeyArena<IndexNode>,
        first_address: ArenaKey<IndexNode>,
        to_delete: HashSet<ArenaKey<IndexNode>>,
    ) -> ArenaKey<IndexNode> {
        let mut last = first_address;
        // TODO? recursion check?

        let mut current = arena[last].next_node; // starts in first node after PK

        while let Some(current_key) = current {
            let node = arena.free(current_key);
            current = node.next_node;

            if to_delete.contains(&current_key) {
                let position = node.next_node;
                Self::delete_single_node(arena, node);
                arena[last].next_node = position;
            } else {
                last = current_key;
            }
        }

        last
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
        pk_index: &CollectionIndex,
        index: CollectionIndex,
    ) {
        let slot = index.slot;

        for pk_node in Self::find_all(arena, pk_index, Order::Ascending) {
            let mut next = arena[pk_node].next_node;
            let mut last = pk_node;

            while let Some(next_key) = next {
                let node = &arena[next_key];
                next = node.next_node;

                if node.slot == slot {
                    // remove the key
                    arena[last].next_node = node.next_node;

                    // delete node from page (mark as dirty)
                    arena.free(next_key);
                } else {
                    last = next_key;
                }
            }
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
