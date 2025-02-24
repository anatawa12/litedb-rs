use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::pages::HeaderPage;
use crate::engine::sort_disk::SortDisk;
use crate::engine::transaction_monitor::TransactionMonitor;
use crate::engine::transaction_service::{LockMode, TransactionService};
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{CONTAINER_SORT_SIZE, FileOrigin, StreamFactory};
#[cfg(feature = "sequential-index")]
use crate::utils::CaseInsensitiveString;
use crate::utils::{Collation, Shared};
use crate::{Error, Result};
#[cfg(feature = "sequential-index")]
use async_lock::Mutex;
use futures::StreamExt;
#[cfg(feature = "sequential-index")]
use std::collections::HashMap;
use std::pin::pin;
use std::rc::Rc;

// common imports for child modules
use crate::bson;

pub use insert::BsonAutoId;
pub use query::Order;

macro_rules! transaction_wrapper {
    (
        $vis: vis
        async fn $name:ident(
            &mut self,
            $(
            $arg_name:ident: $arg_type:ty
            ),*
            $(,)?
        ) -> $return_type:ty
    ) => {
        impl LiteEngine {
            $vis async fn $name(
                &self,
                $( $arg_name: $arg_type, )*
            ) -> $return_type {
                self.with_transaction(async move |engine| engine.$name(
                    $( $arg_name, )*
                ).await).await
            }
        }
    };
}

// method implementations
mod collection;
mod delete;
mod index;
mod insert;
mod query;
#[cfg(feature = "sequential-index")]
mod sequence;
mod transaction;
mod update;
mod upsert;

pub struct LiteSettings {
    pub data_stream: Box<dyn StreamFactory>,
    pub log_stream: Box<dyn StreamFactory>,
    pub temp_stream: Box<dyn StreamFactory>,
    pub auto_build: bool,
    pub collation: Option<Collation>,
}

pub struct LiteEngine {
    locker: Rc<LockService>,
    disk: Rc<DiskService>,
    wal_index: Rc<WalIndexService>,
    header: Shared<HeaderPage>,
    monitor: Rc<TransactionMonitor>,
    sort_disk: Rc<SortDisk>,
    // state,
    // settings,
    // system_collections, // we use match
    #[cfg(feature = "sequential-index")]
    sequences: Mutex<HashMap<CaseInsensitiveString, i64>>,
}

pub struct TransactionLiteEngine<'a> {
    locker: &'a Rc<LockService>,
    disk: &'a Rc<DiskService>,
    header: &'a Shared<HeaderPage>,
    sort_disk: &'a Rc<SortDisk>,
    #[cfg(feature = "sequential-index")]
    sequences: &'a Mutex<HashMap<CaseInsensitiveString, i64>>,
    transaction: &'a mut TransactionService,
}

impl LiteEngine {
    pub async fn new(settings: LiteSettings) -> Result<Self> {
        // SystemCollection
        // sequences
        // TODO: upgrade

        let disk = DiskService::new(
            settings.data_stream,
            settings.log_stream,
            settings.collation,
        )
        .await?;

        let header_buffer = pin!(disk.read_full(FileOrigin::Data))
            .next()
            .await
            .transpose()?
            .expect("no header page");

        if header_buffer.buffer()[0] == 1 {
            return Err(Error::encrypted_no_password());
        }

        let mut header = HeaderPage::load(header_buffer)?;

        if header.buffer().buffer()[HeaderPage::P_INVALID_DATAFILE_STATE] != 0
            && settings.auto_build
        {
            todo!("rebuild when invalid");
        }

        if let Some(collation) = settings.collation {
            if header.pragmas().collation() != collation {
                return Err(Error::collation_not_match());
            }
        }

        let locker = LockService::new(header.pragmas().clone());

        // no services are passed; they are passed when needed
        let wal_index = WalIndexService::new();

        if disk.get_file_length(FileOrigin::Log) > 0 {
            wal_index.restore_index(&mut header, &disk).await?;
        }

        let sort_disk = SortDisk::new(settings.temp_stream, CONTAINER_SORT_SIZE);
        let sort_disk = Rc::new(sort_disk);

        let header = Shared::new(header);
        let locker = Rc::new(locker);
        let disk = Rc::new(disk);
        let wal_index = Rc::new(wal_index);
        let monitor = TransactionMonitor::new(
            Shared::clone(&header),
            Rc::clone(&locker),
            Rc::clone(&disk),
            Rc::clone(&wal_index),
        );
        let monitor = Rc::new(monitor);

        // TODO: consider not using RefCell<HeaderPage>

        // system collections

        debug_log!(ENGINE: "initialization completed");

        Ok(Self {
            locker,
            disk,
            wal_index,
            header,
            monitor,
            sort_disk,
            #[cfg(feature = "sequential-index")]
            sequences: Mutex::new(HashMap::new()),
        })
    }

    pub async fn soft_close(&mut self) -> Result<()> {
        // TODO: close other services
        self.wal_index
            .try_checkpoint(&self.disk, &self.locker)
            .await?;

        Ok(())
    }

    pub async fn checkpoint(&self) -> Result<()> {
        self.wal_index.checkpoint(&self.disk, &self.locker).await
    }

    pub async fn dispose(self) -> Result<()> {
        drop(self.monitor);
        if let Some(disk) = Rc::into_inner(self.disk) {
            disk.dispose().await;
        }
        Ok(())
    }
}
