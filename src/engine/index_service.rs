use crate::engine::collection_index::CollectionIndex;
use crate::engine::snapshot::Snapshot;
use crate::utils::Collation;

pub(crate) struct IndexService<'snapshot, 'engine> {
    snapshot: &'snapshot Snapshot<'engine>,
    collation: Collation,
    max_item_count: u32,
}

impl<'snapshot, 'engine> IndexService<'snapshot, 'engine> {
    pub fn new(snapshot: &'snapshot Snapshot<'engine>, collation: Collation, max_item_count: u32) -> IndexService<'snapshot, 'engine> {
        IndexService { snapshot, collation, max_item_count }
    }
}

impl IndexService<'_, '_> {
    pub fn collation(&self) -> &Collation {
        &self.collation
    }

    pub fn create_index(&self, name: &str, expression: &str, unique: bool) -> CollectionIndex {
        todo!()
    }
}
