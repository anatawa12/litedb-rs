#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PageAddress {
    page_id: u32,
    index: u8,
}

impl PageAddress {
    pub(crate) fn default() -> PageAddress {
        PageAddress {
            page_id: 0,
            index: 0,
        }
    }
}

impl PageAddress {
    pub const SERIALIZED_SIZE: usize = 5;

    pub(crate) fn new(page_id: u32, index: u8) -> Self {
        Self { page_id, index }
    }

    pub(crate) fn page_id(&self) -> u32 {
        self.page_id
    }

    pub(crate) fn index(&self) -> u8 {
        self.index
    }
}
