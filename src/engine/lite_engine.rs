use crate::engine::disk::DiskService;
use crate::engine::lock_service::LockService;
use crate::engine::pages::HeaderPage;
use crate::engine::sort_disk::SortDisk;
use crate::engine::wal_index_service::WalIndexService;
use crate::engine::{CONTAINER_SORT_SIZE, FileOrigin, StreamFactory};
use crate::utils::Collation;
use crate::{Error, Result};
use futures::StreamExt;
use std::marker::PhantomData;
use std::pin::pin;

pub struct LiteSettings<SF: StreamFactory> {
    pub data_stream: SF,
    pub log_stream: SF,
    pub temp_stream: SF,
    pub auto_build: bool,
    pub collation: Option<Collation>,
}

pub struct LiteEngine<SF: StreamFactory> {
    _unused: PhantomData<SF>,
}

impl<SF: StreamFactory> LiteEngine<SF> {
    pub async fn new(settings: LiteSettings<SF>) -> Result<Self> {
        // TODO: SystemCollection
        // TODO: sequences
        // TODO: upgrade

        let mut disk = DiskService::new(
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

        let mut header = HeaderPage::new(header_buffer);

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
        let mut wal_index = WalIndexService::new();

        if disk.get_file_length(FileOrigin::Log) > 0 {
            wal_index.restore_index(&mut header, &mut disk).await?;
        }

        let sort_disk = SortDisk::new(settings.temp_stream, CONTAINER_SORT_SIZE);

        drop(locker);
        drop(sort_disk);

        todo!();
    }
}
