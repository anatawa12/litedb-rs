use crate::expression::ExecutionScope;
use crate::file_io::index_helper::IndexHelper;
use crate::file_io::{Collection, IndexNode, LiteDBFile};
use crate::utils::{CaseInsensitiveStr, Collation, KeyArena, Order};
use crate::{Error, bson};
use std::collections::HashSet;

impl LiteDBFile {
    pub fn update(&mut self, collection: &str, docs: Vec<bson::Document>) -> crate::Result<usize> {
        let Some(collection) = self
            .collections
            .get_mut(CaseInsensitiveStr::new(collection))
        else {
            return Ok(0);
        };

        let mut count = 0;

        for doc in docs {
            if Self::update_document(
                &mut self.index_arena,
                &mut self.data,
                collection,
                self.pragmas.collation(),
                doc,
            )?
            .is_none()
            {
                count += 1;
            }
        }

        Ok(count)
    }

    pub(super) fn update_document(
        index_arena: &mut KeyArena<IndexNode>,
        data_arena: &mut KeyArena<bson::Document>,
        collection: &Collection,

        collation: Collation,
        doc: bson::Document,
    ) -> crate::Result<Option<bson::Document>> {
        let id = doc.get("_id");

        // validate id for null, min/max values
        if matches!(
            id,
            bson::Value::Null | bson::Value::MinValue | bson::Value::MaxValue
        ) {
            return Err(Error::invalid_data_type("_id", id));
        }

        // find indexNode from pk index
        let Some((pk_node, pk_key)) = IndexHelper::find(
            index_arena,
            &collation,
            collection.pk_index(),
            id,
            false,
            Order::Ascending,
        ) else {
            // if not found document, no updates
            return Ok(Some(doc));
        };

        // update data storage
        data_arena[pk_node.data.unwrap()] = doc.clone();

        // get all current non-pk index nodes from this data block (slot, key, nodePosition)
        let old_keys = IndexHelper::get_node_list(index_arena, pk_node.next_node)
            .into_iter()
            .map(|x| (index_arena[x].slot, index_arena[x].key.clone(), x))
            .collect::<Vec<_>>();

        let doc_value = bson::Value::Document(doc);

        // build a list of all new key index keys
        let mut new_keys: Vec<(u8, &bson::Value, &str)> = vec![];

        let scope = ExecutionScope::new(collation);
        for index in collection.indexes.values().filter(|x| x.name != "_id") {
            // getting all keys from expression over document
            let keys = scope.get_index_keys(&index.bson_expr, &doc_value);

            for key in keys {
                let key = key?;

                new_keys.push((index.slot, key, &index.name));
            }
        }

        if old_keys.is_empty() && new_keys.is_empty() {
            // early return if no indexes to be updated
            return Ok(None);
        }

        let to_delete = old_keys
            .iter()
            .filter(|&x| !new_keys.iter().any(|n| n.0 == x.0 && n.1 == &x.1))
            .map(|x| x.2)
            .collect::<HashSet<_>>();

        let to_insert = new_keys
            .into_iter()
            .filter(|x| !old_keys.iter().any(|n| n.0 == x.0 && &n.1 == x.1))
            .collect::<Vec<_>>();

        if to_delete.is_empty() && to_insert.is_empty() {
            return Ok(None);
        }

        let pk_data = pk_node.data.unwrap();

        let mut last = IndexHelper::delete_list(index_arena, pk_key, to_delete);

        for (_, key, name) in to_insert {
            let index = collection.indexes.get(name).unwrap();

            last = IndexHelper::add_node(
                index_arena,
                &collation,
                index,
                key.clone(),
                pk_data,
                Some(last),
            )?;
        }

        Ok(None)
    }
}
