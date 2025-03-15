use crate::bson;
use crate::expression::ExecutionScope;
use crate::file_io::index_helper::IndexHelper;
use crate::file_io::{BsonAutoId, Collection, IndexNode, LiteDBFile};
use crate::utils::{ArenaKey, CaseInsensitiveString, Collation, KeyArena};

impl LiteDBFile {
    pub fn insert(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
        auto_id: BsonAutoId,
    ) -> crate::Result<usize> {
        let collection = self
            .collections
            .entry(CaseInsensitiveString(collection.into()))
            .or_default();

        let mut count = 0;

        for doc in docs {
            Self::insert_document(
                &mut self.index_arena,
                &mut self.data,
                self.pragmas.collation,
                collection,
                doc,
                auto_id,
            )?;

            count += 1;
        }

        Ok(count)
    }

    pub(super) fn insert_document(
        index_arena: &mut KeyArena<IndexNode>,
        data_arena: &mut KeyArena<bson::Document>,
        collation: Collation,
        collection: &mut Collection,
        mut doc: bson::Document,
        auto_id: BsonAutoId,
    ) -> crate::Result<()> {
        // if no _id, use AutoId
        let id = if let Some(id) = doc.try_get("_id") {
            #[cfg(feature = "sequential-index")]
            if let Some(id) = id.as_i64() {
                // update memory sequence of numeric _id
                Self::set_sequence(collection, index_arena, id);
            }
            id
        } else {
            let id = match auto_id {
                BsonAutoId::ObjectId => bson::Value::ObjectId(bson::ObjectId::new()),
                BsonAutoId::Guid => bson::Value::Guid(bson::Guid::new()),
                #[cfg(feature = "sequential-index")]
                _ => Self::get_sequence(collection, index_arena, auto_id),
            };
            doc.insert("_id", id);
            doc.get("_id")
        };

        assert!(
            !matches!(
                id,
                bson::Value::Null | bson::Value::MinValue | bson::Value::MaxValue
            ),
            "_id is not indexable type"
        );

        let data_key = data_arena.alloc(doc.clone());
        let doc_value = bson::Value::Document(doc);

        let scope = ExecutionScope::new(collation);

        let mut last: Option<ArenaKey<IndexNode>> = None;

        for index in collection.indexes.values() {
            for key in scope.get_index_keys(&index.bson_expr.clone(), &doc_value) {
                let key = key?.clone();

                let node =
                    IndexHelper::add_node(index_arena, &collation, index, key, data_key, last)?;
                last = Some(node);
            }
        }

        Ok(())
    }
}
