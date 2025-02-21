// implements operations defined in Index.cs

use super::*;
use crate::engine::CollectionIndexesMut;
use crate::engine::data_service::DataService;
use crate::engine::index_service::{IndexNodeMutRef, IndexService};
use crate::expression::ExecutionScope;

#[derive(Debug, Copy, Clone)]
pub enum BsonAutoId {
    #[cfg(feature = "sequential-index")]
    Int32 = 2,
    #[cfg(feature = "sequential-index")]
    Int64 = 3,
    ObjectId = 10,
    Guid = 11,
}

impl TransactionLiteEngine<'_> {
    pub async fn insert(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
        auto_id: BsonAutoId,
    ) -> Result<usize> {
        let snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, collection, true)
            .await?;
        let mut count = 0;
        let mut parts = snapshot.as_parts();
        let mut indexer = IndexService::new(
            parts.index_pages,
            self.header.borrow().pragmas().collation(),
            self.disk.max_items_count(),
        );
        let mut data = DataService::new(parts.data_pages, self.disk.max_items_count());

        debug_log!(COMMAND: "insert `{collection}`");

        for doc in docs {
            let collation = self.header.borrow().pragmas().collation();
            Self::insert_document(
                collation,
                #[cfg(feature = "sequential-index")]
                self.sequences,
                #[cfg(feature = "sequential-index")]
                collection,
                &mut parts.collection_page,
                doc,
                auto_id,
                &mut indexer,
                &mut data,
            )
            .await?;

            count += 1;
        }
        self.transaction.safe_point().await?;

        Ok(count)
    }

    pub(super) async fn insert_document(
        collation: Collation,
        #[cfg(feature = "sequential-index")] sequences: &Mutex<HashMap<CaseInsensitiveString, i64>>,
        #[cfg(feature = "sequential-index")] collection: &str,
        indexes: &mut CollectionIndexesMut<'_>,
        mut doc: bson::Document,
        auto_id: BsonAutoId,
        indexer: &mut IndexService<'_>,
        data: &mut DataService<'_>,
    ) -> Result<()> {
        // if no _id, use AutoId
        let id = if let Some(id) = doc.try_get("_id") {
            #[cfg(feature = "sequential-index")]
            if let Some(id) = id.as_i64() {
                // update memory sequence of numeric _id
                Self::set_sequence(sequences, collection, indexes, indexer, id).await?;
            }
            id
        } else {
            let id = match auto_id {
                BsonAutoId::ObjectId => bson::Value::ObjectId(bson::ObjectId::new()),
                BsonAutoId::Guid => bson::Value::Guid(bson::Guid::new()),
                #[cfg(feature = "sequential-index")]
                _ => Self::get_sequence(sequences, collection, indexes, indexer, auto_id).await?,
            };
            doc.insert("_id".into(), id);
            doc.get("_id")
        };

        if matches!(
            id,
            bson::Value::Null | bson::Value::MinValue | bson::Value::MaxValue
        ) {
            return Err(Error::invalid_data_type("_id", id));
        }

        let data_block = data.insert(&doc).await?;
        let doc_value = bson::Value::Document(doc);

        let scope = ExecutionScope::new(collation);

        let mut last: Option<IndexNodeMutRef<'_>> = None;

        for index in indexes.get_collection_indexes_mut() {
            for key in scope.get_index_keys(&index.bson_expr().clone(), &doc_value) {
                let key = key?.clone();

                let node = indexer
                    .add_node(index, key, data_block, last.as_mut())
                    .await?;
                last = Some(node);
            }
        }

        Ok(())
    }
}

transaction_wrapper!(pub async fn insert(
    &mut self,
    collection: &str,
    docs: Vec<bson::Document>,
    auto_id: BsonAutoId,
) -> Result<usize>);
