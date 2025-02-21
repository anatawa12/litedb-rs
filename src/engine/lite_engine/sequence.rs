use super::*;
use crate::engine::index_service::IndexService;
use crate::engine::lite_engine::insert::BsonAutoId;
use crate::engine::{CollectionIndexesMut, TransactionLiteEngine};
use crate::utils::CaseInsensitiveString;
use std::cmp::max;
use std::collections::HashMap;
use std::collections::hash_map::Entry;

impl TransactionLiteEngine<'_> {
    pub(super) async fn get_sequence(
        sequences: &Mutex<HashMap<CaseInsensitiveString, i64>>,
        collection: &str,
        indexes: &CollectionIndexesMut<'_>,
        index: &mut IndexService<'_>,
        auto_id: BsonAutoId,
    ) -> Result<bson::Value> {
        let mut sequences = sequences.lock().await;

        let next = match sequences.entry(CaseInsensitiveString(collection.into())) {
            Entry::Occupied(mut entry) => {
                let id = *entry.get() + 1;
                entry.insert(id);
                id
            }
            Entry::Vacant(entry) => {
                let last_id = Self::get_last_id(index, indexes).await?;

                let new_id = {
                    if matches!(last_id, bson::Value::MinValue) {
                        1
                    } else {
                        let last_id = last_id
                            .to_i64()
                            .ok_or_else(|| Error::bad_auto_id(auto_id, collection, last_id))?;
                        last_id.wrapping_add(1)
                    }
                };

                *entry.insert(new_id)
            }
        };

        match auto_id {
            BsonAutoId::Int32 => Ok(bson::Value::Int32((next & 0xFFFFFFFF) as u32 as i32)),
            _ => Ok(bson::Value::Int64(next)),
        }
    }

    pub(super) async fn set_sequence(
        sequences: &Mutex<HashMap<CaseInsensitiveString, i64>>,
        collection: &str,
        indexes: &CollectionIndexesMut<'_>,
        index: &mut IndexService<'_>,
        new_id: i64,
    ) -> Result<()> {
        match sequences
            .lock()
            .await
            .entry(CaseInsensitiveString(collection.into()))
        {
            Entry::Vacant(entry) => {
                let last_id = Self::get_last_id(index, indexes).await?;

                entry.insert({
                    if let Some(last_id) = last_id.as_i64() {
                        max(last_id, new_id)
                    } else {
                        new_id
                    }
                });
            }
            Entry::Occupied(mut entry) => {
                entry.insert(max(*entry.get(), new_id));
            }
        };
        Ok(())
    }

    async fn get_last_id(
        index: &mut IndexService<'_>,
        indexes: &CollectionIndexesMut<'_>,
    ) -> Result<bson::Value> {
        let pk = indexes.pk_index();

        let node = index.get_node(pk.tail()).await?;
        if node.get_prev(0) == pk.head() {
            Ok(bson::Value::MinValue)
        } else {
            let last_node = index.get_node(pk.tail()).await?;

            Ok(last_node.key().clone())
        }
    }
}
