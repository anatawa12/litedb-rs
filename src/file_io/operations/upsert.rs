use crate::bson;
use crate::file_io::{BsonAutoId, LiteDBFile};
use crate::utils::CaseInsensitiveString;

impl LiteDBFile {
    pub async fn upsert(
        &mut self,
        collection: &str,
        docs: Vec<bson::Document>,
        auto_id: BsonAutoId,
    ) -> crate::Result<usize> {
        let collection = self.collections
            .entry(CaseInsensitiveString(collection.into()))
            .or_default();

        let collation = self.pragmas.collation();

        let mut count = 0;

        for doc in docs {
            // first try update document (if exists _id), if not found, do insert

            let doc_to_insert = if !matches!(doc.get("_id"), bson::Value::Null) {
                Self::update_document(
                    &mut self.index_arena,
                    &mut self.data,
                    collection,
                    collation,
                    doc,
                )?
            } else {
                Some(doc)
            };

            if let Some(doc) = doc_to_insert {
                Self::insert_document(
                    &mut self.index_arena,
                    &mut self.data,
                    collation,
                    collection,
                    doc,
                    auto_id,
                )?;

                count += 1;
            }
        }

        // returns how many document was inserted
        Ok(count)
    }
}
