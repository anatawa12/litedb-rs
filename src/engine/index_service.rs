use crate::bson;
use crate::engine::collection_index::CollectionIndex;
use crate::engine::index_node::{IndexNode, IndexNodeMut};
use crate::engine::snapshot::SnapshotIndexPages;
use crate::engine::utils::{PartialBorrower, PartialRefMut};
use crate::engine::{
    CollectionIndexRef, CollectionIndexes, CollectionIndexesMut, CollectionIndexesPartialBorrow,
    MAX_INDEX_KEY_LENGTH, MAX_LEVEL_LENGTH, Page, PageAddress,
};
use crate::expression::BsonExpression;
use crate::utils::{Collation, Order};
use crate::{Error, Result};
use std::collections::HashSet;
use std::hash::{BuildHasher, RandomState};
use std::pin::Pin;

// see http://igoro.com/archive/skip-lists-are-fascinating/ for index structure
pub(crate) struct IndexService<'snapshot> {
    index_nodes: PartialIndexNodeAccessorMut<'snapshot>,
    collation: Collation,
    max_item_count: u32,
}

impl<'snapshot> IndexService<'snapshot> {
    pub fn new(
        index_pages: SnapshotIndexPages<'snapshot>,
        collation: Collation,
        max_item_count: u32,
    ) -> Self {
        IndexService {
            index_nodes: PartialIndexNodeAccessorMut::new(index_pages),
            collation,
            max_item_count,
        }
    }

    pub async fn get_node(&mut self, address: PageAddress) -> Result<IndexNodeMutRef<'snapshot>> {
        self.index_nodes.get_node_mut(address).await
    }

    pub async fn get_node_opt(
        &mut self,
        address: PageAddress,
    ) -> Result<Option<IndexNodeMutRef<'snapshot>>> {
        if address.is_empty() {
            Ok(None)
        } else {
            Ok(Some(self.index_nodes.get_node_mut(address).await?))
        }
    }
}

impl<'snapshot> IndexService<'snapshot> {
    #[allow(dead_code)]
    pub fn collation(&self) -> &Collation {
        &self.collation
    }

    pub async fn create_index<'indexes>(
        &mut self,
        name: &'indexes str,
        expression: BsonExpression,
        unique: bool,
        collection_page: &mut CollectionIndexesPartialBorrow<'indexes>,
    ) -> Result<CollectionIndexRef<'indexes>> {
        let (length, _) = IndexNode::get_node_length(MAX_LEVEL_LENGTH, &bson::Value::MinValue);

        let mut index = collection_page.insert_collection_index(name, expression, unique)?;
        let index_slot = index.slot();

        let mut index_page = self.index_nodes.snapshot_mut().new_page().await?;
        index_page.as_mut().as_base_mut().set_page_list_slot(0);
        let page_id = index_page.page_id();

        let mut head = self
            .index_nodes
            .insert_index_node(
                page_id,
                index_slot,
                MAX_LEVEL_LENGTH,
                bson::Value::MinValue,
                PageAddress::EMPTY,
                length,
            )
            .await?;
        let mut tail = self
            .index_nodes
            .insert_index_node(
                page_id,
                index_slot,
                MAX_LEVEL_LENGTH,
                bson::Value::MaxValue,
                PageAddress::EMPTY,
                length,
            )
            .await?;
        head.set_next(0, tail.position());
        tail.set_prev(0, head.position());

        index.set_free_index_page_list(page_id);
        index.set_head(head.position());
        index.set_tail(tail.position());

        Ok(index)
    }

    pub async fn add_node<'a>(
        &'a mut self,
        index: &mut CollectionIndex,
        key: bson::Value,
        data_block: PageAddress,
        last: Option<&mut IndexNodeMutRef<'_>>,
    ) -> Result<IndexNodeMutRef<'snapshot>> {
        // RustChange: Document is valid since its order is not determinable
        if key == bson::Value::MinValue
            || key == bson::Value::MaxValue
            || key.ty() == bson::BsonType::Document
        {
            return Err(Error::invalid_index_key_type());
        }

        let levels = self.flip();

        self.add_node_with_levels(index, key, data_block, levels, last)
            .await
    }

    async fn add_node_with_levels<'a>(
        &'a mut self,
        index: &mut CollectionIndex,
        key: bson::Value,
        data_block: PageAddress,
        insert_levels: u8,
        last: Option<&mut IndexNodeMutRef<'_>>,
    ) -> Result<IndexNodeMutRef<'snapshot>> {
        let (bytes_length, key_length) = IndexNode::get_node_length(insert_levels, &key);

        if key_length > MAX_INDEX_KEY_LENGTH {
            return Err(Error::index_key_too_long());
        }

        let mut node = self
            .index_nodes
            .insert_index_node(
                index.free_index_page_list(),
                index.slot(),
                insert_levels,
                key,
                data_block,
                bytes_length,
            )
            .await?;

        let mut left_node = self.index_nodes.get_node_mut(index.head()).await?;
        let mut counter = 0;

        for current_level in (0..=(MAX_LEVEL_LENGTH - 1)).rev() {
            let mut right = left_node.get_next(current_level);

            while !right.is_empty() && right != index.tail() {
                assert!(
                    counter < self.max_item_count,
                    "Detected loop in AddNode({:?})",
                    node.position()
                );
                counter += 1;

                let right_node = self.index_nodes.get_node_mut(right).await?;

                let diff = self.collation.compare(right_node.key(), node.key());

                if diff.is_eq() && index.unique() {
                    return Err(Error::index_duplicate_key(
                        index.name(),
                        node.into_value().into_key(),
                    ));
                }

                if diff.is_gt() {
                    break;
                }

                right = right_node.get_next(current_level);
                left_node = right_node;
            }

            if current_level < insert_levels {
                // level == length
                // prev: immediately before new node
                // node: new inserted node
                // next: right node from prev (where left is pointing)

                let prev = left_node.position();
                let mut next = left_node.get_next(current_level);

                if next.is_empty() {
                    next = index.tail();
                }

                // set new node pointer links with current level sibling
                node.set_next(current_level, next);
                node.set_prev(current_level, prev);

                // fix sibling pointer to new node
                left_node.set_next(current_level, node.position());

                right = node.get_next(current_level); // next

                let mut right_node = self.index_nodes.get_node_mut(right).await?;

                // mark right page as dirty (after change PrevID)
                right_node.set_prev(current_level, node.position());
            }
        }

        if let Some(last) = last {
            assert_eq!(
                last.next_node(),
                PageAddress::EMPTY,
                "last index node must point to null"
            );

            last.set_next_node(node.position());
        }

        self.index_nodes
            .inner
            .target_mut()
            .add_or_remove_free_index_list(node.page_ptr(), index.free_index_page_list_mut())
            .await?;

        Ok(node)
    }

    fn flip(&self) -> u8 {
        let mut levels = 1;

        //for (int R = Randomizer.Next(); (R & 1) == 1; R >>= 1)
        let mut random = (RandomState::new().hash_one(0) & 0xFFFFFFFF) as u32;
        while (random & 1) == 1 {
            levels += 1;
            if levels == MAX_LEVEL_LENGTH {
                break;
            }
            random >>= 1;
        }

        levels
    }

    pub async fn get_node_list(
        &mut self,
        first_address: PageAddress,
    ) -> Result<Vec<IndexNodeMutRef>> {
        let mut result = Vec::new();

        let mut current = first_address;
        while !current.is_empty() {
            let node = self.index_nodes.get_node_mut(current).await?;
            current = node.next_node();
            result.push(node)
        }

        Ok(result)
    }

    pub async fn delete_all(
        &mut self,
        first_address: PageAddress,
        collection_page: &mut CollectionIndexes,
    ) -> Result<()> {
        // Rust: no count check since we've checked recursion with PartialIndexNodeAccessorMut
        let mut indexes = collection_page.get_collection_indexes_slots_mut();

        let mut current = first_address;
        while !current.is_empty() {
            let node = self.index_nodes.get_node_mut(current).await?;
            current = node.next_node();

            let index = indexes[node.slot() as usize].as_mut().unwrap();
            Self::delete_single_node(&mut self.index_nodes, node, index).await?
        }

        Ok(())
    }

    pub async fn delete_list(
        &mut self,
        first_address: PageAddress,
        to_delete: HashSet<PageAddress>,
        collection_page: &mut CollectionIndexesMut<'_>,
    ) -> Result<IndexNodeMutRef<'snapshot>> {
        let mut last = first_address;
        // Rust: no count check since we've checked recursion with PartialIndexNodeAccessorMut
        let mut indexes = collection_page.get_collection_indexes_slots_mut();

        let mut current = self.index_nodes.get_node_mut(last).await?.next_node(); // starts in first node after PK

        while !current.is_empty() {
            let node = self.index_nodes.get_node_mut(current).await?;
            current = node.next_node();

            if to_delete.contains(&node.position()) {
                let index = indexes[node.slot() as usize].as_mut().unwrap();
                let position = node.next_node();
                Self::delete_single_node(&mut self.index_nodes, node, index).await?;
                self.index_nodes
                    .get_node_mut(last)
                    .await?
                    .set_next_node(position);
            } else {
                last = node.position();
            }
        }

        self.index_nodes.get_node_mut(last).await
    }

    /// Delete a single index node - fix tree double-linked list levels
    async fn delete_single_node(
        accessor: &mut PartialIndexNodeAccessorMut<'_>,
        node: IndexNodeMutRef<'_>,
        index: &mut CollectionIndex,
    ) -> Result<()> {
        for i in (0..node.levels()).rev() {
            // get previous and next nodes (between my deleted node)
            let prev_node = accessor.get_node_mut_opt(node.get_prev(i)).await?;
            let next_node = accessor.get_node_mut_opt(node.get_next(i)).await?;

            if let Some(mut prev_node) = prev_node {
                prev_node.set_next(i, node.get_next(i));
            }
            if let Some(mut next_node) = next_node {
                next_node.set_prev(i, node.get_prev(i));
            }
        }

        let page_ptr = node.page_ptr();

        node.removing(|x| x.remove_from_page());

        accessor
            .snapshot_mut()
            .add_or_remove_free_index_list(page_ptr, index.free_index_page_list_mut())
            .await
    }

    pub async fn drop_index(
        &mut self,
        collection_page: &mut CollectionIndexesMut<'_>,
        name: &str,
    ) -> Result<()> {
        let index = &collection_page[name];
        let slot = index.slot();
        let pk_index = collection_page.pk_index();

        for pk_node in
            Self::find_all_accessor(&mut self.index_nodes, pk_index, Order::Ascending).await?
        {
            let mut next = pk_node.next_node();
            let mut last = pk_node;

            while !next.is_empty() {
                let node = self.index_nodes.get_node_mut(next).await?;
                next = node.next_node();

                if node.slot() == slot {
                    last.set_next_node(node.next_node());

                    // delete node from page (mark as dirty)
                    node.removing(|node| {
                        unsafe { Pin::new_unchecked(&mut *node.page_ptr().0) }
                            .delete_index_node(node.position().index())
                    })
                } else {
                    last = node;
                }
            }
        }

        // removing head/tail index nodes
        self.index_nodes
            .get_node_mut(index.head())
            .await?
            .removing(|x| x.remove_from_page());
        self.index_nodes
            .get_node_mut(index.tail())
            .await?
            .removing(|x| x.remove_from_page());

        Ok(())
    }
}

// region Find
impl<'snapshot> IndexService<'snapshot> {
    pub async fn find_all(
        &mut self,
        index: &CollectionIndex,
        order: Order,
    ) -> Result<Vec<IndexNodeMutRef<'snapshot>>> {
        Self::find_all_accessor(&mut self.index_nodes, index, order).await
    }
    pub async fn find_all_accessor<'s>(
        accessor: &mut PartialIndexNodeAccessorMut<'s>,
        index: &CollectionIndex,
        order: Order,
    ) -> Result<Vec<IndexNodeMutRef<'s>>> {
        let mut cur = if order == Order::Ascending {
            accessor.get_node_mut(index.head()).await?
        } else {
            accessor.get_node_mut(index.tail()).await?
        };
        let mut nodes = vec![];
        //let counter = 0u;

        let mut current = cur.get_next_prev(0, order);
        while !current.is_empty() {
            //ENSURE(counter++ < _maxItemsCount, "Detected loop in FindAll({0})", index.Name);

            cur = accessor.get_node_mut(current).await?;

            // stop if node is head/tail
            if matches!(cur.key(), bson::Value::MaxValue | bson::Value::MinValue) {
                break;
            }

            current = cur.get_next_prev(0, order);

            nodes.push(cur);
        }

        Ok(nodes)
    }

    pub async fn find(
        &mut self,
        index: &CollectionIndex,
        value: &bson::Value,
        sibling: bool,
        order: Order,
    ) -> Result<Option<IndexNodeMutRef<'snapshot>>> {
        let mut left_node = if order == Order::Ascending {
            self.index_nodes.get_node_mut(index.head()).await?
        } else {
            self.index_nodes.get_node_mut(index.tail()).await?
        };

        let mut counter = 0;

        for level in (0..=(MAX_LEVEL_LENGTH - 1)).rev() {
            let mut right = left_node.get_next_prev(level, order);

            while !right.is_empty() {
                assert!(
                    counter < self.max_item_count,
                    "Detected loop in Find({}, {:?})",
                    index.name(),
                    value
                );
                counter += 1;

                let right_node = self.index_nodes.get_node_mut(right).await?;

                let diff = self.collation.compare(right_node.key(), value);

                if order == diff && (level > 0 || !sibling) {
                    break; // go down one level
                }

                if order == diff && level == 0 && sibling {
                    // is head/tail?
                    if matches!(
                        right_node.key(),
                        bson::Value::MinValue | bson::Value::MaxValue
                    ) {
                        return Ok(None);
                    } else {
                        return Ok(Some(right_node));
                    };
                }

                // if equals, return index node
                if diff.is_eq() {
                    return Ok(Some(right_node));
                }

                right = right_node.get_next_prev(level, order);
                left_node = right_node;
            }
        }

        Ok(None)
    }
}

pub(crate) struct PartialIndexNodeAccessorMut<'snapshot> {
    inner: PartialBorrower<SnapshotIndexPages<'snapshot>, PageAddress>,
}

pub(crate) type IndexNodeMutRef<'snapshot> = PartialRefMut<IndexNodeMut<'snapshot>, PageAddress>;

impl<'snapshot> PartialIndexNodeAccessorMut<'snapshot> {
    pub(crate) fn new(snapshot: SnapshotIndexPages<'snapshot>) -> Self {
        Self {
            inner: PartialBorrower::new(snapshot),
        }
    }

    fn snapshot_mut(&mut self) -> &mut SnapshotIndexPages<'snapshot> {
        self.inner.target_mut()
    }

    async fn insert_index_node(
        &mut self,
        free_index_page_list: u32,
        slot: u8,
        level: u8,
        key: bson::Value,
        data_block: PageAddress,
        length: usize,
    ) -> Result<IndexNodeMutRef<'snapshot>> {
        unsafe {
            self.inner
                .try_create_borrow_async(
                    async |snapshot: &mut SnapshotIndexPages| {
                        Ok(snapshot
                            .get_free_index_page(length, free_index_page_list)
                            .await?
                            .insert_index_node(slot, level, key, data_block, length))
                    },
                    |s| s.position(),
                )
                .await
        }
    }

    async fn get_node_mut(&mut self, address: PageAddress) -> Result<IndexNodeMutRef<'snapshot>> {
        Ok(self.get_node_mut_opt(address).await?.expect("not found"))
    }

    async fn get_node_mut_opt(
        &mut self,
        address: PageAddress,
    ) -> Result<Option<IndexNodeMutRef<'snapshot>>> {
        if address.page_id() == u32::MAX {
            return Ok(None);
        }

        unsafe {
            Ok(Some(
                self.inner
                    .try_get_borrow_async::<_, IndexNodeMut<'snapshot>, Error, _>(
                        &address,
                        async |snapshot, address| {
                            snapshot
                                .get_page(address.page_id())
                                .await?
                                .get_index_node_mut(address.index())
                        },
                    )
                    .await?,
            ))
        }
    }
}
