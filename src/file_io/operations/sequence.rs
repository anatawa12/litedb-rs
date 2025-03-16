use crate::bson;
use crate::file_io::{BsonAutoId, Collection, CollectionIndex, IndexNode, LiteDBFile};
use crate::utils::KeyArena;
use std::cmp::max;

impl LiteDBFile {
    pub(super) fn get_sequence(
        collection: &mut Collection,
        arena: &KeyArena<IndexNode>,
        auto_id: BsonAutoId,
    ) -> bson::Value {
        let next = match collection.last_id {
            Some(ref mut last) => {
                let id = *last + 1;
                *last = id;
                id
            }
            None => {
                let last_id = Self::get_last_id(arena, &collection.indexes["_id"]);

                let new_id = {
                    if matches!(last_id, bson::Value::MinValue) {
                        1
                    } else {
                        let last_id = last_id.to_i64().expect("bad key");
                        last_id.wrapping_add(1)
                    }
                };

                collection.last_id = Some(new_id);
                new_id
            }
        };

        match auto_id {
            BsonAutoId::Int32 => bson::Value::Int32((next & 0xFFFFFFFF) as u32 as i32),
            _ => bson::Value::Int64(next),
        }
    }

    pub(super) fn set_sequence(
        collection: &mut Collection,
        arena: &KeyArena<IndexNode>,
        new_id: i64,
    ) {
        match collection.last_id {
            Some(ref mut last) => {
                *last = max(*last, new_id);
            }
            None => {
                let last_id = Self::get_last_id(arena, &collection.indexes["_id"]);

                collection.last_id = if let Some(last_id) = last_id.as_i64() {
                    Some(max(last_id, new_id))
                } else {
                    Some(new_id)
                };
            }
        }
    }

    fn get_last_id<'a>(arena: &'a KeyArena<IndexNode>, pk: &CollectionIndex) -> &'a bson::Value {
        let node = &arena[pk.tail];
        if node.prev[0] == Some(pk.head) {
            &bson::Value::MinValue
        } else {
            &arena[pk.tail].key
        }
    }
}
