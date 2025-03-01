use crate::engine::FileStream;
use futures::prelude::*;
use std::pin::Pin;
use tokio_util::compat::TokioAsyncReadCompatExt;

pub struct TokioStreamFactory {
    path: std::path::PathBuf,
}

impl TokioStreamFactory {
    pub fn new(path: std::path::PathBuf) -> Self {
        Self { path }
    }
}

impl crate::engine::StreamFactory for TokioStreamFactory {
    fn get_stream(
        &self,
        writable: bool,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Box<dyn FileStream>>> + Send + Sync + '_>> {
        Box::pin(async move {
            if writable {
                Ok(
                    Box::new(tokio::fs::File::create(self.path.clone()).await?.compat())
                        as Box<dyn FileStream>,
                )
            } else {
                Ok(
                    Box::new(tokio::fs::File::open(self.path.clone()).await?.compat())
                        as Box<dyn FileStream>,
                )
            }
        })
    }

    fn exists(&self) -> Pin<Box<dyn Future<Output = bool> + Send + Sync + '_>> {
        Box::pin(tokio::fs::metadata(self.path.clone()).map(|x| x.is_ok()))
    }

    fn len(&self) -> Pin<Box<dyn Future<Output = crate::Result<u64>> + Send + Sync>> {
        Box::pin(tokio::fs::metadata(self.path.clone()).map(|x| match x {
            Ok(metadata) => Ok(metadata.len()),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e.into()),
        }))
    }

    fn delete(&self) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + Sync + '_>> {
        Box::pin(async move { Ok(tokio::fs::remove_file(self.path.clone()).await?) })
    }
}

impl FileStream for tokio_util::compat::Compat<tokio::fs::File> {
    fn set_len(
        &self,
        len: u64,
    ) -> Pin<Box<dyn Future<Output = crate::Result<()>> + Send + Sync + '_>> {
        Box::pin(async move { Ok(self.get_ref().set_len(len).await?) })
    }
}
