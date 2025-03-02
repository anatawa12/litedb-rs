#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PageAddress {
    page_id: u32,
    index: u8,
}

impl PageAddress {
    pub const EMPTY: PageAddress = PageAddress {
        page_id: u32::MAX,
        index: u8::MAX,
    };

    pub(crate) fn is_empty(&self) -> bool {
        self.page_id == u32::MAX && self.index == u8::MAX
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
