use crate::Result;
use crate::bson;
use crate::engine::collection_index::CollectionIndex;
use crate::engine::index_node::IndexNode;
use crate::engine::snapshot::Snapshot;
use crate::engine::{IndexPage, MAX_LEVEL_LENGTH, PageAddress, StreamFactory};
use crate::utils::Collation;

pub(crate) struct IndexService<'snapshot, SF: StreamFactory> {
    snapshot: &'snapshot mut Snapshot<SF>,
    collation: Collation,
    max_item_count: u32,
}

impl<'snapshot, SF: StreamFactory> IndexService<'snapshot, SF> {
    pub fn new(
        snapshot: &'snapshot mut Snapshot<SF>,
        collation: Collation,
        max_item_count: u32,
    ) -> Self {
        IndexService {
            snapshot,
            collation,
            max_item_count,
        }
    }
}

impl<SF: StreamFactory> IndexService<'_, SF> {
    pub fn collation(&self) -> &Collation {
        &self.collation
    }

    pub async fn create_index(
        &mut self,
        name: &str,
        expression: &str,
        unique: bool,
    ) -> Result<&mut CollectionIndex> {
        let length = IndexNode::get_node_length(MAX_LEVEL_LENGTH as u8, &bson::Value::MinValue);

        let index = self
            .snapshot
            .collection_page_mut()
            .unwrap()
            .insert_collection_index(name, expression, unique)?;
        let index_slot = index.slot();

        let index_page = self.snapshot.new_page::<IndexPage>().await?;
        let head = index_page.insert_index_node(
            index_slot,
            MAX_LEVEL_LENGTH as u8,
            bson::Value::MinValue,
            PageAddress::default(),
            length,
        );
        let head_position = head.position();
        let mut tail = index_page.insert_index_node(
            index_slot,
            MAX_LEVEL_LENGTH as u8,
            bson::Value::MaxValue,
            PageAddress::default(),
            length,
        );
        let tail_position = tail.position();
        tail.set_prev(0, head_position);
        let mut head = index_page
            .get_index_node_mut(head_position.index())
            .unwrap();
        head.set_prev(0, tail_position);
        index_page.set_page_list_slot(0);

        let page_id = index_page.page_id();

        let index = self
            .snapshot
            .collection_page_mut()
            .unwrap()
            .get_collection_index_mut(name)
            .unwrap();
        index.set_free_index_page_list(page_id);
        index.set_head(head_position);
        index.set_tail(head_position);

        Ok(index)
    }
}
