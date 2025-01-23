use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::pages::HeaderPage;
use crate::engine::transaction_pages::TransactionPages;
use crate::engine::transaction_service::LockMode;
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::StreamFactory;

pub(crate) struct Snapshot<'engine> {
    pub mode: LockMode,
}

impl<'engine> Snapshot<'engine> {
    pub fn new<SF: StreamFactory>(
        mode: LockMode,
        collection_name: &str,
        header_page: &'engine HeaderPage,
        transaction_id: u32,
        trans_pages: &mut TransactionPages,
        locker: &LockService,
        wal_index: &WalIndexService,
        disk: &DiskService<SF>,
        add_if_not_exists: bool,
    ) -> _ {
        todo!()
    }
}
