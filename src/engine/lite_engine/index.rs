// implements operations defined in Index.cs

use super::*;
use crate::engine::data_service::DataService;
use crate::engine::index_node::IndexNode;
use crate::engine::index_service::{IndexNodeMutRef, IndexService};
use crate::engine::{BufferReader, INDEX_NAME_MAX_LENGTH};
use crate::expression::{BsonExpression, ExecutionScope};
use crate::utils::{Order, StrExtension};

impl TransactionLiteEngine<'_> {
    /// # Panics
    /// This function will panics if
    /// - the `name` is not valid (not a word or starting with '$', or too long)
    /// - the `expression` is not suitable for index; this means:
    ///   - the `expression` does not read any fields from source
    ///   - the `expression` is NOT deterministic; like time dependent or uses randomness.
    ///   - the `expression` consumes multiple values, but `unique` is enabled
    pub async fn ensure_index(
        &mut self,
        collection: &str,
        name: &str,
        expression: BsonExpression,
        unique: bool,
    ) -> Result<bool> {
        assert!(
            !name.is_empty()
                && name.is_word()
                && !name.starts_with('$')
                && name.len() < INDEX_NAME_MAX_LENGTH,
            "invalid index name"
        );
        assert!(expression.is_indexable(), "invalid expression");
        assert!(
            expression.is_scalar() || !unique,
            "scalar expression is needed for unique index"
        );

        if expression.source() == "$._id" {
            return Ok(false); // always exists
        }

        let snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, collection, true)
            .await?;

        let mut parts = snapshot.as_parts();

        let mut collection_page = parts.collection_page.partial_borrow();
        let mut indexer = IndexService::new(
            parts.index_pages,
            self.header.borrow().pragmas().collation(),
            self.disk.max_items_count(),
        );
        let mut data = DataService::new(parts.data_pages, self.disk.max_items_count());

        if let Some(current) = collection_page.get(name) {
            // if already exists, just exit
            if current.expression() != expression.source() {
                Err(Error::index_already_exists(name))
            } else {
                Ok(false)
            }
        } else {
            let mut index = indexer
                .create_index(name, expression.clone(), unique, &mut collection_page)
                .await?;
            let exec_context = ExecutionScope::new(self.header.borrow().pragmas().collation());

            let pk_index = collection_page.pk_index();
            for mut pk_node in indexer.find_all(&pk_index, Order::Ascending).await? {
                let parts = data.read(pk_node.data_block()).await?;
                let mut buffer_reader =
                    BufferReader::fragmented(parts.iter().map(|x| x.buffer()).collect::<Vec<_>>());

                let mut first: Option<IndexNode> = None;
                let mut last: Option<IndexNodeMutRef> = None;

                let doc = buffer_reader.read_document()?;

                for key in exec_context.get_index_keys(&expression, &doc.into()) {
                    let key = key?;
                    let node = indexer
                        .add_node(&mut index, key.clone(), pk_node.data_block(), last.as_mut())
                        .await?;
                    first.get_or_insert_with(|| node.to_read_only());
                    last = Some(node);
                }

                if let Some(first) = first {
                    let mut last = last.unwrap();
                    last.set_next_node(pk_node.next_node());
                    pk_node.set_next_node(first.position());
                }
            }

            drop(index);
            drop(pk_index);
            drop(collection_page);
            self.transaction.safe_point().await?;

            Ok(true)
        }
    }

    pub async fn drop_index(&mut self, collection: &str, name: &str) -> Result<bool> {
        if name == "_id" {
            return Err(Error::drop_id_index());
        }

        let snapshot = self
            .transaction
            .create_snapshot(LockMode::Write, collection, true)
            .await?;

        if snapshot.collection_page().is_none() {
            return Ok(false);
        }

        let mut parts = snapshot.as_parts();
        let mut indexer = IndexService::new(
            parts.index_pages,
            self.header.borrow().pragmas().collation(),
            self.disk.max_items_count(),
        );

        if parts.collection_page.get(name).is_none() {
            return Ok(false);
        };

        indexer.drop_index(&mut parts.collection_page, name).await?;

        parts.collection_page.delete_collection_index(name);

        Ok(true)
    }
}

transaction_wrapper!(pub async fn ensure_index(
    &mut self,
    collection: &str,
    name: &str,
    expression: BsonExpression,
    unique: bool,
) -> Result<bool>);
transaction_wrapper!(pub async fn drop_index(&mut self, collection: &str, name: &str) -> Result<bool>);
