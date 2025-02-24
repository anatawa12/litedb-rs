use super::*;
use crate::engine::BufferReader;
use crate::engine::data_service::DataService;
use crate::engine::index_service::IndexService;
use crate::utils::Order;
use async_stream::try_stream;
use futures::Stream;
use futures::prelude::*;

/// In this module we create stream instead of returning single value so we use custom wrapper
/// without commit / rollback.
/// Our streams does change nothing so this is acceptable
///
/// In addition we cannot use simple macro who calls something like `with_transaction_stream`
/// because `impl for<'a> FnOnce(&'a mut TransactionLiteEngine) -> impl Stream<Item=something> + 'a`
/// is not allowed
macro_rules! transaction_stream_wrapper {
    (
        $vis: vis
        fn $name:ident(
            &mut self,
            $(
            $arg_name:ident: $arg_type:ty
            ),*
            $(,)?
        ) -> $return_type:ty
    ) => {
        impl LiteEngine {
            $vis fn $name(
                &self,
                $( $arg_name: $arg_type, )*
            ) -> $return_type {
                try_stream! {
                    let mut transaction = self.monitor.create_transaction(false).await?;

                    let mut engine = TransactionLiteEngine {
                        locker: &self.locker,
                        disk: &self.disk,
                        header: &self.header,
                        sort_disk: &self.sort_disk,
                        #[cfg(feature = "sequential-index")]
                        sequences: &self.sequences,
                        transaction: &mut transaction,
                    };

                    // this scope ensures stream dropped before is_read_only check
                    {
                        let mut iterator = pin!(engine.$name(
                            $( $arg_name, )*
                        ));
                        while let Some(value) = iterator.try_next().await? {
                            yield value;
                        }
                    }

                    debug_assert!(transaction.is_read_only());
                }
            }
        }
    };
}

impl TransactionLiteEngine<'_> {
    pub fn get_all(&mut self, collection: &str) -> impl Stream<Item = Result<bson::Document>> {
        try_stream! {
            let snapshot = self
                .transaction
                .create_snapshot(LockMode::Read, collection, false)
                .await?;
            if snapshot.collection_page().is_none() {
                return;
            }

            let mut parts = snapshot.as_parts();

            let mut collection_page = parts.collection_page.partial_borrow();
            let mut indexer = IndexService::new(
                parts.index_pages,
                self.header.borrow().pragmas().collation(),
                self.disk.max_items_count(),
            );
            let mut data = DataService::new(parts.data_pages, self.disk.max_items_count());

            let pk_index = collection_page.get("_id").unwrap();
            for pk_node in indexer.find_all(&pk_index, Order::Ascending).await? {
                let parts = data.read(pk_node.data_block()).await?;
                let mut buffer_reader =
                    BufferReader::fragmented(parts.iter().map(|x| x.buffer()).collect::<Vec<_>>());

                let doc = buffer_reader.read_document()?;

                yield doc;
            }

            drop(pk_index);
            drop(collection_page);
            self.transaction.safe_point().await?;
        }
    }
}

transaction_stream_wrapper!(pub fn get_all(
    &mut self,
    collection: &str,
) -> impl Stream<Item = Result<bson::Document>>);
