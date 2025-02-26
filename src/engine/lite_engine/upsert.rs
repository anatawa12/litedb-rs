use super::*;
use crate::engine::data_service::DataService;
use crate::engine::index_service::IndexService;

impl TransactionLiteEngine<'_> {
    pub async fn upsert(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
        auto_id: BsonAutoId,
    ) -> Result<usize> {
        let snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, collection, true, self.header)
            .await?;
        let mut count = 0;
        let mut parts = snapshot.as_parts();
        let mut indexer = IndexService::new(
            parts.index_pages,
            self.header.pragmas().collation(),
            self.disk.max_items_count(),
        );
        let mut data = DataService::new(parts.data_pages, self.disk.max_items_count());

        debug_log!(COMMAND: "upsert `{collection}`");

        for doc in docs {
            let collation = self.header.pragmas().collation();

            // first try update document (if exists _id), if not found, do insert

            let doc_to_insert = if !matches!(doc.get("_id"), bson::Value::Null) {
                Self::update_document(
                    collation,
                    &mut parts.collection_page,
                    doc,
                    &mut indexer,
                    &mut data,
                )
                .await?
            } else {
                Some(doc)
            };

            if let Some(doc) = doc_to_insert {
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
        }
        self.transaction.safe_point().await?;

        // returns how many document was inserted
        Ok(count)
    }
}

transaction_wrapper!(pub async fn upsert(
    &mut self,
    collection: &str,
    docs: Vec<bson::Document>,
    auto_id: BsonAutoId,
) -> Result<usize>);
