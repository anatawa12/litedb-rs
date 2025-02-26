// implements operations defined in Index.cs

use super::*;

impl LiteEngine {
    pub async fn with_transaction<R>(
        &self,
        f: impl AsyncFnOnce(&mut TransactionLiteEngine) -> Result<R>,
    ) -> Result<R> {
        let mut transaction = self.monitor.create_transaction(false).await?;

        let mut engine = TransactionLiteEngine {
            disk: &self.disk,
            header: &self.header,
            #[cfg(feature = "sequential-index")]
            sequences: &self.sequences,
            transaction: &mut transaction,
        };

        match f(&mut engine).await {
            Ok(result) => {
                // commit

                transaction.commit(&self.header).await?;

                #[allow(clippy::collapsible_if)]
                if self.header.pragmas().checkpoint() > 0 {
                    if self.disk.get_file_length(FileOrigin::Log)
                        > self.header.pragmas().checkpoint() as i64
                            * crate::engine::PAGE_SIZE as i64
                    {
                        self.wal_index
                            .try_checkpoint(&self.disk, &self.locker)
                            .await?;
                    }
                }
                Ok(result)
            }
            Err(err) => {
                // Rollback
                // TODO: check if the error is io error

                transaction.rollback(&self.header).await?;

                Err(err)
            }
        }
    }
}
