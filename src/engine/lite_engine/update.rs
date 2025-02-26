use super::*;
use crate::engine::CollectionIndexesMut;
use crate::engine::data_service::DataService;
use crate::engine::index_service::IndexService;
use crate::expression::ExecutionScope;
use crate::utils::Order;
use std::collections::HashSet;

impl TransactionLiteEngine<'_> {
    pub async fn update(&mut self, collection: &str, docs: Vec<bson::Document>) -> Result<usize> {
        let snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, collection, false, self.header)
            .await?;
        if snapshot.collection_page().is_none() {
            return Ok(0);
        }
        let mut count = 0;
        let mut parts = snapshot.as_parts();
        let mut indexer = IndexService::new(
            parts.index_pages,
            self.header.pragmas().collation(),
            self.disk.max_items_count(),
        );
        let mut data = DataService::new(parts.data_pages, self.disk.max_items_count());

        debug_log!(COMMAND: "update `{collection}`");

        for doc in docs {
            let collation = self.header.pragmas().collation();
            if Self::update_document(
                collation,
                &mut parts.collection_page,
                doc,
                &mut indexer,
                &mut data,
            )
            .await?
            .is_none()
            {
                count += 1;
            }
        }

        self.transaction.safe_point().await?;

        Ok(count)
    }

    pub(super) async fn update_document(
        collation: Collation,
        indexes: &mut CollectionIndexesMut<'_>,
        doc: bson::Document,
        indexer: &mut IndexService<'_>,
        data: &mut DataService<'_>,
    ) -> Result<Option<bson::Document>> {
        let id = doc.get("_id");

        // validate id for null, min/max values
        if matches!(
            id,
            bson::Value::Null | bson::Value::MinValue | bson::Value::MaxValue
        ) {
            return Err(Error::invalid_data_type("_id", id));
        }

        // find indexNode from pk index
        let Some(pk_node) = indexer
            .find(indexes.pk_index(), id, false, Order::Ascending)
            .await?
        else {
            // if not found document, no updates
            return Ok(Some(doc));
        };

        // update data storage
        data.update(pk_node.data_block(), &doc).await?;

        let pk_node = (pk_node.to_read_only(), drop(pk_node)).0;

        // get all current non-pk index nodes from this data block (slot, key, nodePosition)
        let old_keys = indexer
            .get_node_list(pk_node.next_node())
            .await?
            .into_iter()
            .map(|x| (x.slot(), x.key().clone(), x.position()))
            .collect::<Vec<_>>();

        let doc_value = bson::Value::Document(doc);

        // build a list of all new key index keys
        let mut new_keys: Vec<(u8, &bson::Value, String)> = vec![];

        let scope = ExecutionScope::new(collation);
        for index in indexes
            .get_collection_indexes_mut()
            .filter(|x| x.name() != "_id")
        {
            // getting all keys from expression over document
            let keys = scope.get_index_keys(index.bson_expr(), &doc_value);

            for key in keys {
                let key = key?;

                new_keys.push((index.slot(), key, index.name().to_string()));
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

        let mut last = indexer
            .delete_list(pk_node.position(), to_delete, indexes)
            .await?;

        for (_, key, name) in to_insert {
            let index = indexes.get_mut(&name).unwrap();

            last = indexer
                .add_node(
                    index.as_mut(),
                    key.clone(),
                    pk_node.data_block(),
                    Some(&mut last),
                )
                .await?;
        }

        Ok(None)
    }
}

transaction_wrapper!(pub async fn update(&mut self, collection: &str, docs: Vec<bson::Document>) -> Result<usize>);
