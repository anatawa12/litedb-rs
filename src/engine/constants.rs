
/// The size of each page in disk - use 8192 as all major databases
pub(crate) const PAGE_SIZE: usize = 8192;
/// header size of each page
pub(crate) const PAGE_HEADER_SIZE: usize = 32;
pub(crate) const CONTAINER_SORT_SIZE: usize = 100 * PAGE_SIZE;
pub(crate) const PAGE_FREE_LIST_SLOTS: usize = 5;
