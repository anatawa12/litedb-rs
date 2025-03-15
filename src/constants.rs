use crate::utils::PageAddress;

/// The size of each page in disk - use 8192 as all major databases
pub(crate) const PAGE_SIZE: usize = 8192;
/// header size of each page
pub(crate) const PAGE_HEADER_SIZE: usize = 32;
pub(crate) const INDEX_NAME_MAX_LENGTH: usize = 32;
pub(crate) const PAGE_FREE_LIST_SLOTS: usize = 5;
pub(crate) const MAX_INDEX_LENGTH: usize = 1400;
pub(crate) const MAX_LEVEL_LENGTH: u8 = 32;
pub(crate) const MAX_OPEN_TRANSACTIONS: usize = 100;
pub(crate) const MAX_TRANSACTION_SIZE: u32 = 100_000;
pub(crate) const MAX_INDEX_KEY_LENGTH: usize = 1023;
pub(crate) const PAGE_SLOT_SIZE: usize = 4;
pub(crate) const DATA_BLOCK_FIXED_SIZE: usize = 1 + PageAddress::SERIALIZED_SIZE;
pub(crate) const MAX_DATA_BYTES_PER_PAGE: usize =
    PAGE_SIZE - PAGE_HEADER_SIZE - PAGE_SLOT_SIZE - DATA_BLOCK_FIXED_SIZE;
pub(crate) const MAX_DOCUMENT_SIZE: usize = 2047 * MAX_DATA_BYTES_PER_PAGE;
