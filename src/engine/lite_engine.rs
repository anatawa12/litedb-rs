use futures::StreamExt;
use crate::engine::disk::DiskService;
use crate::engine::{FileOrigin, StreamFactory};
use crate::{Error, Result};
use crate::engine::pages::HeaderPage;
use crate::utils::Collation;

pub struct LiteSettings<SF: StreamFactory> {
    pub data_stream: SF,
    pub log_stream: SF,
    pub auto_build: bool,
    pub collation: Option<Collation>,
}

pub struct LiteEngine<SF: StreamFactory> {
    
}

impl<SF:StreamFactory> LiteEngine<SF> {
    pub async fn new(
        settings: LiteSettings<SF>,
    ) -> Result<Self> {
        // TODO: SystemCollection
        // TODO: sequences
        // TODO: upgrade

        let mut disk = DiskService::new(settings.data_stream, settings.log_stream, settings.collation).await?;

        let header_buffer = disk.read_full(FileOrigin::Data).next().await.transpose()?.expect("no header page");

        if header_buffer.buffer()[0] == 1 {
            return Err(Error::encrypted_no_password());
        }

        let mut header = HeaderPage::new(header_buffer);

        if header.base.buffer.buffer()[HeaderPage::P_INVALID_DATAFILE_STATE] != 0 && settings.auto_build {
            todo!("rebuild when invalid");
        }

        if let Some(collation) = settings.collation {
            if header.pragmas.collation != collation {
                return Err(Error::collation_not_match());
            }
        }

        todo!();
    }
}
