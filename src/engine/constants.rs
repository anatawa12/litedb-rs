use crate::engine::data_service::DataService;

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
pub(crate) const MAX_DOCUMENT_SIZE: usize = 2047 * DataService::MAX_DATA_BYTES_PER_PAGE;
