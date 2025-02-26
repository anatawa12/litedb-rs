use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::pages::HeaderPage;
use crate::engine::transaction_monitor::TransactionMonitor;
use crate::engine::transaction_service::{LockMode, TransactionService};
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{FileOrigin, StreamFactory};
#[cfg(feature = "sequential-index")]
use crate::utils::CaseInsensitiveString;
use crate::utils::Collation;
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
    pub auto_build: bool,
    pub collation: Option<Collation>,
}

pub struct LiteEngine {
    locker: Rc<LockService>,
    disk: Rc<DiskService>,
    wal_index: Rc<WalIndexService>,
    header: Rc<HeaderPage>,
    monitor: Rc<TransactionMonitor>,
    // state,
    // settings,
    // system_collections, // we use match
    #[cfg(feature = "sequential-index")]
    sequences: Mutex<HashMap<CaseInsensitiveString, i64>>,
}

pub struct TransactionLiteEngine<'a> {
    disk: &'a Rc<DiskService>,
    header: &'a Rc<HeaderPage>,
    #[cfg(feature = "sequential-index")]
    sequences: &'a Mutex<HashMap<CaseInsensitiveString, i64>>,
    transaction: &'a mut TransactionService,
}

impl LiteEngine {
    pub async fn new(settings: LiteSettings) -> Result<Self> {
        // SystemCollection
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

        if header.as_mut().buffer().buffer()[HeaderPage::P_INVALID_DATAFILE_STATE] != 0
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

        let header = Rc::new(header);
        let locker = Rc::new(locker);
        let disk = Rc::new(disk);
        let wal_index = Rc::new(wal_index);
        let monitor =
            TransactionMonitor::new(Rc::clone(&locker), Rc::clone(&disk), Rc::clone(&wal_index));
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
            #[cfg(feature = "sequential-index")]
            sequences: Mutex::new(HashMap::new()),
        })
    }

    pub async fn checkpoint(&self) -> Result<()> {
        self.wal_index.checkpoint(&self.disk, &self.locker).await
    }

    pub async fn dispose(self) -> Result<()> {
        drop(self.monitor);
        if self.header.pragmas().checkpoint() > 0 {
            self.wal_index
                .try_checkpoint(&self.disk, &self.locker)
                .await?;
        }
        if let Some(disk) = Rc::into_inner(self.disk) {
            disk.dispose().await;
        }
        // sort_disk
        drop(self.locker);
        Ok(())
    }
}
