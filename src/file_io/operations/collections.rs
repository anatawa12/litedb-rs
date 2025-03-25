use crate::file_io::LiteDBFile;
use crate::utils::{CaseInsensitiveStr, CaseInsensitiveString};
use std::collections::HashSet;

pub enum RenameCollectionResult {
    Renamed,
    SameName,
    OldNotExists,
    NewNameAlreadyExists,
}

impl LiteDBFile {
    pub fn get_collection_names(&self) -> Vec<String> {
        self.collections.keys().cloned().map(|x| x.0).collect()
    }

    pub fn drop_collection(&mut self, name: &str) -> bool {
        let Some(collection) = self.collections.shift_remove(CaseInsensitiveStr::new(name)) else {
            return false;
        };

        let mut data_keys = HashSet::new();

        // remove all index nodes
        // TODO? Use general logic for traverse
        for (_, index) in collection.indexes {
            // remove all index nodes
            let mut cur = Some(index.head);
            while let Some(current) = cur {
                let node = self.index_arena.free(current);
                if let Some(data) = node.data {
                    data_keys.insert(data);
                }
                cur = node.next[0];
            }
        }

        // remove all data nodes
        for data_key in data_keys {
            self.data.free(data_key);
        }

        true
    }

    pub fn rename_collection(&mut self, old_name: &str, new_name: &str) -> RenameCollectionResult {
        if old_name == new_name {
            return RenameCollectionResult::SameName;
        }

        // TODO: check for collection new name

        if self
            .collections
            .contains_key(CaseInsensitiveStr::new(new_name))
        {
            return RenameCollectionResult::NewNameAlreadyExists;
        }

        let Some(collection) = self
            .collections
            .shift_remove(CaseInsensitiveStr::new(old_name))
        else {
            return RenameCollectionResult::OldNotExists;
        };

        let result = self
            .collections
            .insert(CaseInsensitiveString(new_name.to_string()), collection);
        debug_assert!(result.is_none());

        RenameCollectionResult::Renamed
    }
}
