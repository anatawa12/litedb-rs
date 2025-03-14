use crate::Error;
use crate::engine::INDEX_NAME_MAX_LENGTH;
use crate::expression::{BsonExpression, ExecutionScope};
use crate::file_io::index_helper::IndexHelper;
use crate::file_io::{IndexNode, LiteDBFile};
use crate::utils::{ArenaKey, CaseInsensitiveStr, CaseInsensitiveString, Order, StrExtension};

impl LiteDBFile {
    /// # Panics
    /// This function will panics if
    /// - the `name` is not valid (not a word or starting with '$', or too long)
    /// - the `expression` is not suitable for index; this means:
    ///   - the `expression` does not read any fields from source
    ///   - the `expression` is NOT deterministic; like time dependent or uses randomness.
    ///   - the `expression` consumes multiple values, but `unique` is enabled
    pub fn ensure_index(
        &mut self,
        collection: &str,
        name: &str,
        expression: BsonExpression,
        unique: bool,
    ) -> crate::Result<bool> {
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

        let collection = self
            .collections
            .entry(CaseInsensitiveString(collection.into()))
            .or_default();

        if let Some(current) = collection.indexes.get(name) {
            // if already exists, just exit
            if current.expression != expression.source() {
                Err(Error::index_already_exists(name))
            } else {
                Ok(false)
            }
        } else {
            IndexHelper::create_index(
                &mut self.index_arena,
                collection,
                name,
                expression.clone(),
                unique,
            );
            let index = &collection.indexes[name];

            let exec_context = ExecutionScope::new(self.pragmas.collation());

            let pk_index = collection.pk_index();
            for pk_key in IndexHelper::find_all(&self.index_arena, pk_index, Order::Ascending) {
                let data_key = self.index_arena[pk_key].data.unwrap();
                let doc = self.data[data_key].clone().into();

                let mut first: Option<ArenaKey<IndexNode>> = None;
                let mut last: Option<ArenaKey<IndexNode>> = None;

                for key in exec_context.get_index_keys(&expression, &doc) {
                    let key = key?;
                    let node_key = IndexHelper::add_node(
                        &mut self.index_arena,
                        &self.pragmas.collation(),
                        index,
                        key.clone(),
                        data_key,
                        last,
                    )?;
                    first.get_or_insert(node_key);
                    last = Some(node_key);
                }

                if let Some(first) = first {
                    let last = last.unwrap();
                    self.index_arena[last].next_node = self.index_arena[pk_key].next_node;
                    self.index_arena[pk_key].next_node = Some(first);
                }
            }

            Ok(true)
        }
    }

    /// # Panics
    /// This function will panics if you're dropping primary index, in other words index named `"_id"`
    pub fn drop_index(&mut self, collection: &str, name: &str) -> bool {
        assert!(name != "_id", "dropping primary key index");

        let Some(collection) = self
            .collections
            .get_mut(CaseInsensitiveStr::new(collection))
        else {
            return false;
        };

        let Some(index) = collection.indexes.remove(name) else {
            return false;
        };

        IndexHelper::drop_index(&mut self.index_arena, collection.pk_index(), index);

        true
    }
}
