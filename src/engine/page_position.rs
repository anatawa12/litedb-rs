#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PagePosition {
    page_id: u32,
    position: u64,
}

impl PagePosition {
    pub(crate) fn new(page_id: u32, position: u64) -> Self {
        Self { page_id, position }
    }

    pub fn page_id(&self) -> u32 {
        self.page_id
    }

    pub fn position(&self) -> u64 {
        self.position
    }
}
