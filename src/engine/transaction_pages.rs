use std::collections::HashMap;
use crate::engine::HeaderPage;
use crate::engine::page_position::PagePosition;

pub(crate) struct TransactionPages {
    pub transaction_size: u32,
    pub dirty_pages: HashMap<u32, PagePosition>,
    new_pages: Vec<u32>,
    first_deleted_page: u32,
    last_deleted_page: u32,
    // number of deleted pages
    deleted_pages: usize,

    on_commit: Vec<Box<dyn Fn(&mut HeaderPage)>>,
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
        self.new_pages.len() > 0 || self.deleted_pages > 0 || self.on_commit.len() > 0
    }

    pub fn on_commit(&mut self, f: impl Fn(&mut HeaderPage) + 'static) {
        self.on_commit.push(Box::new(f));
    }

    pub fn call_on_commit(&mut self, page: &mut HeaderPage) {
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
}
