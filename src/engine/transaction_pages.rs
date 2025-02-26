use crate::engine::HeaderPageLocked;
use crate::engine::page_position::PagePosition;
use std::collections::HashMap;

pub(crate) struct TransactionPages {
    pub transaction_size: u32,
    pub dirty_pages: HashMap<u32, PagePosition>,
    new_pages: Vec<u32>,
    first_deleted_page: u32,
    last_deleted_page: u32,
    // number of deleted pages
    deleted_pages: usize,

    #[allow(clippy::type_complexity)]
    on_commit: Vec<Box<dyn Fn(&mut HeaderPageLocked) + Send + Sync>>,
}

impl TransactionPages {
    pub fn new() -> Self {
        Self {
            transaction_size: 0,
            dirty_pages: HashMap::new(),
            new_pages: Vec::new(),
            first_deleted_page: 0,
            last_deleted_page: 0,
            deleted_pages: 0,
            on_commit: Vec::new(),
        }
    }

    pub fn header_changed(&self) -> bool {
        !self.new_pages.is_empty() || self.deleted_pages > 0 || !self.on_commit.is_empty()
    }

    pub fn on_commit(&mut self, f: impl Fn(&mut HeaderPageLocked) + 'static + Send + Sync) {
        self.on_commit.push(Box::new(f));
    }

    pub fn call_on_commit(&mut self, page: &mut HeaderPageLocked) {
        for on_commit in &self.on_commit {
            on_commit(page);
        }
    }

    pub fn first_deleted_page(&self) -> u32 {
        self.first_deleted_page
    }

    pub fn last_deleted_page(&self) -> u32 {
        self.last_deleted_page
    }

    pub fn set_first_deleted_page(&mut self, first_deleted_page: u32) {
        self.first_deleted_page = first_deleted_page;
    }

    pub fn new_pages(&self) -> &[u32] {
        &self.new_pages
    }

    pub fn set_last_deleted_page(&mut self, last_deleted_page: u32) {
        self.last_deleted_page = last_deleted_page;
    }

    pub fn add_new_page(&mut self, page_id: u32) {
        self.new_pages.push(page_id);
    }

    pub fn deleted_pages(&self) -> usize {
        self.deleted_pages
    }

    pub(crate) fn inc_deleted_pages(&mut self) {
        self.deleted_pages += 1;
    }

    pub fn set_deleted_pages(&mut self, deleted_pages: usize) {
        self.deleted_pages = deleted_pages;
    }
}
