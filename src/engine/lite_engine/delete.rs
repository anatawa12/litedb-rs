// implements operations defined in Delete.cs

use super::*;
use crate::engine::data_service::DataService;
use crate::engine::index_service::IndexService;
use crate::utils::Order;

impl TransactionLiteEngine<'_> {
    pub async fn delete(&mut self, collection: &str, ids: &[bson::Value]) -> Result<usize> {
        let snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, collection, false)
            .await?;
        if snapshot.collection_page().is_none() {
            return Ok(0);
        }

        let mut parts = snapshot.as_parts();

        debug_log!(COMMAND: "delete `{collection}`");

        let mut indexer = IndexService::new(
            parts.index_pages,
            self.header.borrow().pragmas().collation(),
            self.disk.max_items_count(),
        );
        let mut data = DataService::new(parts.data_pages, self.disk.max_items_count());

        let mut count = 0;
        //let pk = parts.collection_page.pk_index();

        for id in ids {
            let Some(pk_node) = indexer
                .find(
                    parts.collection_page.pk_index(),
                    id,
                    false,
                    Order::Ascending,
                )
                .await?
            else {
                continue;
            };

            data.delete(pk_node.data_block()).await?;
            let index_position = pk_node.position();

            drop(pk_node);

            indexer
                .delete_all(index_position, &mut parts.collection_page)
                .await?;

            //transaction.safe_point().await?;

            count += 1;
        }
        self.transaction.safe_point().await?;

        Ok(count)
    }
}

transaction_wrapper!(pub async fn delete(&mut self, collection: &str, ids: &[bson::Value]) -> Result<usize>);
