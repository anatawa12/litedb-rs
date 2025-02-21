// implements operations defined in Collection.cs

use super::*;
use crate::engine::collection_service::CollectionService;
#[cfg(feature = "sequential-index")]
use crate::utils::CaseInsensitiveStr;

impl LiteEngine {
    pub fn get_collection_names(&self) -> Vec<String> {
        self.header
            .borrow()
            .collections()
            .map(|x| x.0.to_string())
            .collect()
    }
}

impl TransactionLiteEngine<'_> {
    pub async fn drop_collection(&mut self, name: &str) -> Result<bool> {
        let snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, name, false)
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

    pub async fn rename_collection(&mut self, collection: &str, new_name: &str) -> Result<bool> {
        if collection == new_name {
            return Ok(true); // Original: errors, this: OK
        }

        CollectionService::check_name(new_name, &self.header.borrow())?;

        let _new_snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, new_name, false)
            .await?;
        let current_snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, collection, false)
            .await?;

        // not exists
        if current_snapshot.collection_page().is_none() {
            return Ok(false);
        }

        if self.header.borrow().get_collection_page_id(new_name) != u32::MAX {
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
