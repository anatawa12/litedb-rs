use crate::bson;
use crate::bson::TotalOrd;
use crate::file_io::LiteDBFile;
use crate::file_io::index_helper::IndexHelper;
use crate::utils::{CaseInsensitiveStr, Order as InternalOrder};
use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

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

struct IteratorContext<T> {
    data: Rc<Cell<Option<T>>>,
}

impl<T> IteratorContext<T> {
    async fn yields(&self, value: T) {
        let old = self.data.replace(Some(value));

        assert!(old.is_none());

        struct SuspendOnce {
            suspend: bool,
        }
        impl SuspendOnce {
            fn new() -> Self {
                Self { suspend: false }
            }
        }

        impl Future for SuspendOnce {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
                if self.suspend {
                    Poll::Ready(())
                } else {
                    self.suspend = true;
                    Poll::Pending
                }
            }
        }

        SuspendOnce::new().await
    }
}

fn iterator<T, F, Fut>(closure: F) -> impl Iterator<Item = T>
where
    F: FnOnce(IteratorContext<T>) -> Fut,
    Fut: Future<Output = ()>,
{
    let data: Rc<Cell<Option<T>>> = Rc::new(Cell::new(None));
    let ctx = IteratorContext { data: data.clone() };
    let future = Box::pin(closure(ctx));

    struct IteratorImpl<T, Fut> {
        data: Rc<Cell<Option<T>>>,
        future: Pin<Box<Fut>>,
    }

    impl<T, Fut> Iterator for IteratorImpl<T, Fut>
    where
        Fut: Future<Output = ()>,
    {
        type Item = T;

        fn next(&mut self) -> Option<Self::Item> {
            assert!(self.data.take().is_none());

            match self
                .future
                .as_mut()
                .poll(&mut Context::from_waker(std::task::Waker::noop()))
            {
                Poll::Pending => Some(self.data.take().expect("iterator returns")),
                Poll::Ready(()) => None,
            }
        }
    }

    IteratorImpl { data, future }.fuse()
}

impl LiteDBFile {
    fn find_range_by_index(
        &self,
        collection: &str,
        index: &str,
        min_inclusive: &bson::Value,
        max_inclusive: &bson::Value,
        order: Order,
    ) -> impl Iterator<Item = &bson::Document> {
        iterator(async move |ctx: IteratorContext<&bson::Document>| {
            if max_inclusive.total_cmp(min_inclusive).is_lt() {
                return;
            }

            let Some(collection) = self.collections.get(CaseInsensitiveStr::new(collection)) else {
                return;
            };

            let collation = self.pragmas.collation;

            pub(crate) fn is_edge(this: &bson::Value) -> bool {
                matches!(this, bson::Value::MinValue | bson::Value::MaxValue)
            }

            let indexes = &self.index_arena;

            let index = collection.indexes.get(index).unwrap();

            let (start, end) = match order {
                Order::Ascending => (min_inclusive, max_inclusive),
                Order::Descending => (max_inclusive, min_inclusive),
            };
            let order = order.to_internal();

            let first = match start {
                bson::Value::MinValue => Some(&indexes[index.head]),
                bson::Value::MaxValue => Some(&indexes[index.tail]),
                start => IndexHelper::find(indexes, &collation, index, start, true, order)
                    .map(|(node, _)| node),
            };

            let mut node = first;

            if let Some(mut node) = node.as_ref() {
                let mut new_node;
                // going backward in same value list to get first value
                while let Some(next_prev) = node.get_next_prev(0, -order) {
                    new_node = &indexes[next_prev];
                    if is_edge(&new_node.key) || collation.compare(&new_node.key, start).is_ne() {
                        break;
                    }
                    ctx.yields(&self.data[new_node.data.unwrap()]).await;
                    node = &new_node;
                }
            }

            // returns (or not) equals start value
            while let Some(cur_node) = node {
                let diff = collation.compare(&cur_node.key, start);

                // if current value are not equals start, go out this loop
                if diff.is_ne() {
                    break;
                }

                if !is_edge(&cur_node.key) {
                    ctx.yields(&self.data[cur_node.data.unwrap()]).await;
                }

                node = cur_node.get_next_prev(0, order).map(|key| &indexes[key]);
            }

            // navigate using next[0] do next node - if less or equals returns
            while let Some(cur_node) = node.as_ref() {
                let diff = collation.compare(&cur_node.key, end);

                if is_edge(&cur_node.key) || order == diff {
                    break;
                } else {
                    ctx.yields(&self.data[cur_node.data.unwrap()]).await;
                }

                node = cur_node.get_next_prev(0, order).map(|key| &indexes[key]);
            }
        })
    }

    pub fn get_all(&self, collection: &str) -> impl Iterator<Item = &bson::Document> {
        self.find_range_by_index(
            collection,
            "_id",
            &bson::Value::MinValue,
            &bson::Value::MaxValue,
            Order::Ascending,
        )
    }

    pub fn get_range_indexed(
        &self,
        collection: &str,
        index: &str,
        min_inclusive: &bson::Value,
        max_inclusive: &bson::Value,
        order: Order,
    ) -> impl Iterator<Item = &bson::Document> {
        self.find_range_by_index(collection, index, min_inclusive, max_inclusive, order)
    }

    pub fn get_by_index(
        &self,
        collection: &str,
        index: &str,
        find: &bson::Value,
    ) -> impl Iterator<Item = &bson::Document> {
        self.find_range_by_index(collection, index, find, find, Order::Ascending)
    }
}
