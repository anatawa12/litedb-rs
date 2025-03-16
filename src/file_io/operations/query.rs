use crate::bson;
use crate::bson::TotalOrd;
use crate::file_io::LiteDBFile;
use crate::file_io::index_helper::IndexHelper;
use crate::utils::{CaseInsensitiveStr, Order as InternalOrder};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

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

static ITERATOR_WAKER_V_TABLE: RawWakerVTable = RawWakerVTable::new(
    |_| RawWaker::new(std::ptr::null(), &ITERATOR_WAKER_V_TABLE),
    |_| (),
    |_| (),
    |_| (),
);

struct IteratorContext<T> {
    phantom: PhantomData<T>,
}

impl<T: Unpin> IteratorContext<T> {
    async fn yields(&self, value: T) {
        struct SuspendOnce<T> {
            value: Option<T>,
        }
        impl<T> SuspendOnce<T> {
            fn new(value: T) -> Self {
                Self { value: Some(value) }
            }
        }

        impl<T: Unpin> Future for SuspendOnce<T> {
            type Output = ();
            fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
                let waker = ctx.waker();
                assert!(
                    waker.vtable() == &ITERATOR_WAKER_V_TABLE && !waker.data().is_null(),
                    "yields is called from invalid position"
                );
                if let Some(value) = self.get_mut().value.take() {
                    let data = waker.data() as *mut Option<T>;
                    let data = unsafe { &mut *data };
                    assert!(data.replace(value).is_none());
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
        }

        SuspendOnce::new(value).await
    }
}

fn iterator<T, F, Fut>(closure: F) -> impl Iterator<Item = T>
where
    F: FnOnce(IteratorContext<T>) -> Fut,
    Fut: Future<Output = ()>,
{
    let ctx = IteratorContext::<T> {
        phantom: PhantomData,
    };
    let future = Box::pin(closure(ctx));

    struct IteratorImpl<T, Fut> {
        future: Pin<Box<Fut>>,
        phantom: PhantomData<T>,
    }

    impl<T, Fut> Iterator for IteratorImpl<T, Fut>
    where
        Fut: Future<Output = ()>,
    {
        type Item = T;

        fn next(&mut self) -> Option<Self::Item> {
            // TODO: replace with ext or local_waker when they become stable
            let mut slot: Option<T> = None;

            let raw_waker = RawWaker::new(&mut slot as *mut _ as *mut (), &ITERATOR_WAKER_V_TABLE);
            let waker = unsafe { Waker::from_raw(raw_waker) };

            match self.future.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Pending => Some(slot.take().expect("iterator returns")),
                Poll::Ready(()) => None,
            }
        }
    }

    IteratorImpl {
        future,
        phantom: PhantomData,
    }
    .fuse()
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
