use crate::engine::CollectionPage;
use crate::engine::index_service::IndexService;
use crate::engine::pages::HeaderPage;
use crate::engine::snapshot::Snapshot;
use crate::expression::BsonExpression;
use crate::{Error, Result};
use std::pin::Pin;

pub(crate) struct CollectionService<'snapshot> {
    snapshot: &'snapshot mut Snapshot,
}

impl<'snapshot> CollectionService<'snapshot> {
    pub fn new(snapshot: &'snapshot mut Snapshot) -> Self {
        Self { snapshot }
    }

    pub fn check_name(name: &str, header: &HeaderPage) -> Result<()> {
        if name.len() > header.get_available_collection_space() {
            return Err(Error::name_length_header_space(name));
        }
        if !is_word(name) {
            return Err(Error::invalid_collection_name(name));
        }
        if name.starts_with("$") {
            return Err(Error::invalid_collection_name(name));
        }

        return Ok(());

        fn is_word(s: &str) -> bool {
            // TODO: move to common place
            // TODO: support unicode letter?
            if s.is_empty() {
                return false;
            }

            let mut is_first = true;

            for c in s.chars() {
                let valid_c = if is_first {
                    c.is_alphabetic() || c == '_' || c == '$'
                } else {
                    c.is_alphanumeric() || c == '_' || c == '$'
                };
                if !valid_c {
                    return false;
                }
                is_first = false;
            }

            true
        }
    }

    pub async fn get(
        &mut self,
        name: &str,
        add_if_not_exists: bool,
    ) -> Result<(bool, Option<Pin<&mut CollectionPage>>)> {
        let page_id = self.snapshot.header().borrow().get_collection_page_id(name);

        if page_id != u32::MAX {
            let page = self
                .snapshot
                .pages()
                .get_page::<CollectionPage>(page_id, false)
                .await?;
            Ok((false, Some(page)))
        } else if add_if_not_exists {
            Ok((true, Some(self.add(name).await?)))
        } else {
            Ok((false, None))
        }
    }

    pub async fn add(&mut self, name: &str) -> Result<Pin<&mut CollectionPage>> {
        Self::check_name(name, &self.snapshot.header().borrow())?;

        let page = self.snapshot.pages().new_page::<CollectionPage>().await?;
        let page_id = page.page_id();

        self.snapshot.trans_pages().borrow_mut().on_commit({
            let name = name.to_string();
            move |h| h.insert_collection(&name, page_id)
        });

        let collation = self.snapshot.header().borrow().pragmas().collation();
        let max_items_count = self.snapshot.disk().max_items_count();
        let mut parts = self.snapshot.as_parts();
        let mut indexer = IndexService::new(parts.index_pages, collation, max_items_count);
        let expr = BsonExpression::create("$._id").expect("bad bson expression for id");

        indexer
            .create_index("_id", expr, true, &mut parts.collection_page)
            .await?;

        self.snapshot
            .pages()
            .get_page::<CollectionPage>(page_id, false)
            .await
    }
}
