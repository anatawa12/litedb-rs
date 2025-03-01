// implements operations defined in Collection.cs

use super::*;
use crate::engine::collection_service::CollectionService;
#[cfg(feature = "sequential-index")]
use crate::utils::CaseInsensitiveStr;

impl LiteEngine {
    pub fn get_collection_names(&self) -> Vec<String> {
        self.header.collection_names()
    }
}

impl TransactionLiteEngine<'_> {
    // not public since no user transaction is allowed for drop collection
    async fn drop_collection(&mut self, name: &str) -> Result<bool> {
        let snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, name, false, self.header)
            .await?;
        if snapshot.collection_page().is_none() {
            return Ok(false);
        }

        debug_log!(COMMAND: "Drop collection `{}`", name);

        snapshot.drop_collection(async || Ok(())).await?;
        self.transaction.safe_point().await?;

        #[cfg(feature = "sequential-index")]
        self.sequences
            .lock()
            .await
            .remove(CaseInsensitiveStr::new(name));

        Ok(true)
    }

    // not public since no user transaction is allowed for drop collection
    async fn rename_collection(&mut self, collection: &str, new_name: &str) -> Result<bool> {
        if collection == new_name {
            return Ok(true); // Original: errors, this: OK
        }

        CollectionService::check_name(new_name, self.header)?;

        let _new_snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, new_name, false, self.header)
            .await?;
        let current_snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, collection, false, self.header)
            .await?;

        // not exists
        if current_snapshot.collection_page().is_none() {
            return Ok(false);
        }

        if self.header.get_collection_page_id(new_name) != u32::MAX {
            return Err(Error::already_exists_collection_name(new_name));
        }

        let collection = collection.to_string();
        let new_name = new_name.to_string();
        self.transaction
            .pages()
            .borrow_mut()
            .on_commit(move |h| h.rename_collection(&collection, &new_name));
        Ok(true)
    }
}

transaction_wrapper!(pub async fn drop_collection(&mut self, name: &str) -> Result<bool>);
transaction_wrapper!(pub async fn rename_collection(&mut self, collection: &str, new_name: &str) -> Result<bool>);

#[allow(dead_code)]
fn _type_check() {
    use crate::utils::checker::*;

    check_sync_send(dummy::<LiteEngine>().drop_collection(dummy()));
    check_sync_send(dummy::<LiteEngine>().rename_collection(dummy(), dummy()));
}
