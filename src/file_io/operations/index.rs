use crate::Error;
use crate::constants::INDEX_NAME_MAX_LENGTH;
use crate::expression::{BsonExpression, ExecutionScope};
use crate::file_io::index_helper::IndexHelper;
use crate::file_io::{Collection, LiteDBFile};
use crate::utils::{CaseInsensitiveStr, CaseInsensitiveString, Collation, Order, StrExtension};
use indexmap::IndexMap;

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
            .or_insert_with(|| Collection::new(&mut self.index_arena));

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

            let exec_context = ExecutionScope::new(self.pragmas.collation);

            let pk_index = collection.pk_index();
            for pk_key in IndexHelper::find_all(&self.index_arena, pk_index, Order::Ascending) {
                let data_key = self.index_arena[pk_key].data.unwrap();
                let doc = self.data[data_key].data.clone().into();

                for key in exec_context.get_index_keys(&expression, &doc) {
                    let key = key?;
                    IndexHelper::add_node(
                        &mut self.index_arena,
                        &mut self.data,
                        &self.pragmas.collation,
                        index,
                        key.clone(),
                        data_key,
                    )?;
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

        let Some(index) = collection.indexes.shift_remove(name) else {
            return false;
        };

        IndexHelper::drop_index(
            &mut self.index_arena,
            &mut self.data,
            collection.pk_index(),
            index,
        );

        true
    }

    pub fn drop_indexes_and_update_collation_if_collation_not_supported(&mut self) -> bool {
        if self.pragmas.collation == Collation::default() {
            // the collation is supported so no need to drop
            return false;
        }

        self.pragmas.collation = Collation::default();
        for collection in self.collections.values_mut() {
            let index = collection.indexes.swap_remove("_id").unwrap();
            let removing_indexes = std::mem::replace(&mut collection.indexes, {
                let mut indexes = IndexMap::new();
                indexes.insert("_id".to_string(), index);
                indexes
            });
            for index in removing_indexes.into_values() {
                IndexHelper::drop_index(
                    &mut self.index_arena,
                    &mut self.data,
                    collection.pk_index(),
                    index,
                );
            }
        }

        false
    }
}
