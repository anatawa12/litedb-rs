use std::pin::Pin;
use tokio_util::compat::TokioAsyncReadCompatExt;
use crate::engine::Stream;
use futures::prelude::*;

struct TokioStreamFactory {
    path: std::path::PathBuf,
}

impl TokioStreamFactory {
    fn new(path: std::path::PathBuf) -> Self {
        Self { path }
    }
}

impl crate::engine::StreamFactory for TokioStreamFactory {
    fn get_stream(
        &self,
        writable: bool,
    ) -> Pin<Box<dyn Future<Output = crate::Result<Box<dyn Stream>>> + '_>> {
        Box::pin(async move {
            if writable {
                Ok(
                    Box::new(tokio::fs::File::create(self.path.clone()).await?.compat())
                        as Box<dyn Stream>,
                )
            } else {
                Ok(
                    Box::new(tokio::fs::File::open(self.path.clone()).await?.compat())
                        as Box<dyn Stream>,
                )
            }
        })
    }

    fn exists(&self) -> Pin<Box<dyn Future<Output = bool>>> {
        Box::pin(tokio::fs::metadata(self.path.clone()).map(|x| x.is_ok()))
    }

    fn len(&self) -> Pin<Box<dyn Future<Output = crate::Result<u64>>>> {
        Box::pin(tokio::fs::metadata(self.path.clone()).map(|x| match x {
            Ok(metadata) => Ok(metadata.len()),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e.into()),
        }))
    }

    fn delete(&self) -> Pin<Box<dyn Future<Output = crate::Result<()>> + '_>> {
        Box::pin(async move { Ok(tokio::fs::remove_file(self.path.clone()).await?) })
    }
}

impl Stream for tokio_util::compat::Compat<tokio::fs::File> {
    fn set_len(&self, len: u64) -> Pin<Box<dyn Future<Output =crate::Result<()>> + '_>> {
        Box::pin(async move { Ok(self.get_ref().set_len(len).await?) })
    }
}
