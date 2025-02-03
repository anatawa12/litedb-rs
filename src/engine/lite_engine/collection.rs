// implements operations defined in Collection.cs

use super::*;

impl<SF: StreamFactory> LiteEngine<SF> {
    pub fn get_collection_names(&self) -> Vec<String> {
        self.header
            .borrow()
            .collections()
            .map(|x| x.0.to_string())
            .collect()
    }

    pub async fn drop_collection(&self, name: &str) -> Result<bool> {
        let mut transaction = self.monitor.create_transaction(false).await?;
        let snapshot = transaction
            .create_snapshot(LockMode::Write, name, false)
            .await?;
        if snapshot.collection_page().is_none() {
            return Ok(false);
        }

        debug_log!(COMMAND: "Drop collection `{}`", name);

        //snapshot.drop_collection();

        Ok(true)
    }
}
