use super::*;
use crate::bson::TotalOrd;
use crate::engine::BufferReader;
use crate::engine::data_service::DataService;
use crate::engine::index_service::IndexService;
use crate::utils::{Order as InternalOrder, PageAddress};
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
                        disk: &self.disk,
                        header: &self.header,
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

#[derive(Copy, Clone)]
#[repr(i8)]
pub enum Order {
    Ascending = 1,
    Descending = -1,
}

impl Order {
    fn to_internal(self) -> InternalOrder {
        match self {
            Order::Ascending => InternalOrder::Ascending,
            Order::Descending => InternalOrder::Descending,
        }
    }
}

impl TransactionLiteEngine<'_> {
    fn find_range_by_index(
        &mut self,
        collection: &str,
        index: &str,
        min_inclusive: &bson::Value,
        max_inclusive: &bson::Value,
        order: Order,
    ) -> impl Stream<Item = Result<bson::Document>> {
        try_stream! {
            if max_inclusive.total_cmp(min_inclusive).is_lt() {
                return;
            }

            let snapshot = self
                .transaction
                .create_snapshot(LockMode::Read, collection, false, self.header)
                .await?;
            if snapshot.collection_page().is_none() {
                return;
            }

            let mut parts = snapshot.as_parts();

            let mut collection_page = parts.collection_page.partial_borrow();
            let mut indexer = IndexService::new(
                parts.index_pages,
                self.header.pragmas().collation(),
                self.disk.max_items_count(),
            );
            let mut data = DataService::new(parts.data_pages, self.disk.max_items_count());
            let collation = self.header.pragmas().collation();

            async fn read_data(
                data: &mut DataService<'_>,
                data_block: PageAddress,
            ) -> Result<bson::Document> {
                let parts = data.read(data_block).await?;
                let mut buffer_reader =
                    BufferReader::fragmented(parts.iter().map(|x| x.buffer()).collect::<Vec<_>>());

                buffer_reader.read_document()
            }

            pub(crate) fn is_edge(this: &bson::Value) -> bool {
                matches!(this, bson::Value::MinValue | bson::Value::MaxValue)
            }

            let index = collection_page.get(index).unwrap();

            let (start, end) = match order {
                Order::Ascending => (min_inclusive, max_inclusive),
                Order::Descending => (max_inclusive, min_inclusive),
            };
            let order = order.to_internal();

            let first = match start {
                bson::Value::MinValue => Some(indexer.get_node(index.head()).await?),
                bson::Value::MaxValue => Some(indexer.get_node(index.tail()).await?),
                start => indexer.find(&index, start, true, order).await?,
            };

            let mut node = first;

            if let Some(mut node) = node.as_ref() {
                let mut new_node;
                // going backward in same value list to get first value
                while let Some(next_prev) = {
                    let next_prev = node.get_next_prev(0, -order);
                    if next_prev.is_empty() {
                        None
                    } else {
                        Some(next_prev)
                    }
                } {
                    new_node = indexer.get_node(next_prev).await?;
                    if is_edge(new_node.key()) || collation.compare(new_node.key(), start).is_ne() {
                        break;
                    }
                    yield read_data(&mut data, new_node.data_block()).await?;
                    node = &new_node;
                }
            }

            // returns (or not) equals start value
            while let Some(cur_node) = node.as_ref() {
                let diff = collation.compare(cur_node.key(), start);

                // if current value are not equals start, go out this loop
                if diff.is_ne() {
                    break;
                }

                if !is_edge(cur_node.key()) {
                    yield read_data(&mut data, cur_node.data_block()).await?;
                }

                node = indexer.get_node_opt(cur_node.get_next_prev(0, order)).await?;
            }

            // navigate using next[0] do next node - if less or equals returns
            while let Some(cur_node) = node.as_ref() {
                let diff = collation.compare(cur_node.key(), end);

                if is_edge(cur_node.key()) || order == diff {
                    break;
                } else {
                    yield read_data(&mut data, cur_node.data_block()).await?;
                }

                node = indexer.get_node_opt(cur_node.get_next_prev(0, order)).await?;
            }

            drop(node);
            drop(index);
            drop(collection_page);
            self.transaction.safe_point().await?;
        }
    }

    pub fn get_all(&mut self, collection: &str) -> impl Stream<Item = Result<bson::Document>> {
        self.find_range_by_index(
            collection,
            "_id",
            &bson::Value::MinValue,
            &bson::Value::MaxValue,
            Order::Ascending,
        )
    }

    pub fn get_range_indexed(
        &mut self,
        collection: &str,
        index: &str,
        min_inclusive: &bson::Value,
        max_inclusive: &bson::Value,
        order: Order,
    ) -> impl Stream<Item = Result<bson::Document>> {
        self.find_range_by_index(collection, index, min_inclusive, max_inclusive, order)
    }

    pub fn get_by_index(
        &mut self,
        collection: &str,
        index: &str,
        find: &bson::Value,
    ) -> impl Stream<Item = Result<bson::Document>> {
        self.find_range_by_index(collection, index, find, find, Order::Ascending)
    }
}

transaction_stream_wrapper!(pub fn get_all(
    &mut self,
    collection: &str,
) -> impl Stream<Item = Result<bson::Document>>);

transaction_stream_wrapper!(pub fn get_range_indexed(
    &mut self,
    collection: &str,
    index: &str,
    min_inclusive: &bson::Value,
    max_inclusive: &bson::Value,
    order: Order,
) -> impl Stream<Item = Result<bson::Document>>);

transaction_stream_wrapper!(pub fn get_by_index(
    &mut self,
    collection: &str,
    index: &str,
    find: &bson::Value,
) -> impl Stream<Item = Result<bson::Document>>);

#[allow(dead_code)]
fn _type_check() {
    use crate::utils::checker::*;

    check_sync_send(dummy::<TransactionLiteEngine>().get_all(dummy()));
    check_sync_send(dummy::<LiteEngine>().get_all(dummy()));

    check_sync_send(dummy::<TransactionLiteEngine>().get_range_indexed(
        dummy(),
        dummy(),
        dummy(),
        dummy(),
        dummy(),
    ));
    check_sync_send(dummy::<LiteEngine>().get_range_indexed(
        dummy(),
        dummy(),
        dummy(),
        dummy(),
        dummy(),
    ));

    check_sync_send(dummy::<TransactionLiteEngine>().get_by_index(dummy(), dummy(), dummy()));
    check_sync_send(dummy::<LiteEngine>().get_by_index(dummy(), dummy(), dummy()));
}
