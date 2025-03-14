use crate::bson;
use crate::file_io::LiteDBFile;
use crate::file_io::index_helper::IndexHelper;
use crate::utils::{CaseInsensitiveStr, Order};

impl LiteDBFile {
    pub async fn delete(&mut self, collection: &str, ids: &[bson::Value]) -> crate::Result<usize> {
        let Some(collection) = self
            .collections
            .get_mut(CaseInsensitiveStr::new(collection))
        else {
            return Ok(0);
        };

        let pk = collection.pk_index();

        let mut count = 0;
        //let pk = parts.collection_page.pk_index();

        for id in ids {
            let Some((pk_node, pk_key)) = IndexHelper::find(
                &self.index_arena,
                &self.pragmas.collation(),
                pk,
                id,
                false,
                Order::Ascending,
            ) else {
                continue;
            };

            self.data.free(pk_node.data.unwrap());

            IndexHelper::delete_all(&mut self.index_arena, pk_key);

            count += 1;
        }

        Ok(count)
    }
}
