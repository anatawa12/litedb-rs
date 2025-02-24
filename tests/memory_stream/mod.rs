use futures::prelude::*;
use litedb::engine::FileStream;
use std::cmp::max;
use std::future::Future;
use std::io;
use std::io::SeekFrom;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll::Ready;
use std::task::{Context, Poll};

pub(crate) struct MemoryStream {
    buffer: Arc<Mutex<Vec<u8>>>,
    position: usize,
}

pub(crate) struct MemoryStreamFactory {
    buffer: async_lock::Mutex<Option<Arc<Mutex<Vec<u8>>>>>,
}

impl MemoryStreamFactory {
    pub fn with_data(buffer: Arc<Mutex<Vec<u8>>>) -> Self {
        Self {
            buffer: async_lock::Mutex::new(Some(buffer)),
        }
    }

    pub fn absent() -> Self {
        Self {
            buffer: async_lock::Mutex::new(None),
        }
    }
}

impl litedb::engine::StreamFactory for MemoryStreamFactory {
    fn get_stream(
        &self,
        writable: bool,
    ) -> Pin<Box<dyn Future<Output = litedb::Result<Box<dyn FileStream>>> + '_>> {
        Box::pin(async move {
            let mut buffer = self.buffer.lock().await;
            if !writable && buffer.is_none() {
                return Err(io::Error::from(io::ErrorKind::NotFound).into());
            }
            let buffer = buffer
                .get_or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
                .clone();

            Ok(Box::new(MemoryStream {
                buffer,
                position: 0,
            }) as Box<dyn FileStream>)
        })
    }

    fn exists(&self) -> Pin<Box<dyn Future<Output = bool> + '_>> {
        Box::pin(async move { self.buffer.lock().await.is_some() })
    }

    fn len(&self) -> Pin<Box<dyn Future<Output = litedb::Result<u64>> + '_>> {
        Box::pin(async move {
            Ok(self
                .buffer
                .lock()
                .await
                .as_ref()
                .map(|x| x.lock().unwrap().len() as u64)
                .unwrap_or(0))
        })
    }

    fn delete(&self) -> Pin<Box<dyn Future<Output = litedb::Result<()>> + '_>> {
        Box::pin(async move {
            *self.buffer.lock().await = None;
            Ok(())
        })
    }
}

impl litedb::engine::FileStream for MemoryStream {
    fn set_len(&self, len: u64) -> Pin<Box<dyn Future<Output = litedb::Result<()>> + '_>> {
        Box::pin(async move {
            self.buffer.lock().unwrap().resize(len as usize, 0);
            Ok(())
        })
    }
}

impl AsyncRead for MemoryStream {
    fn poll_read(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = Pin::into_inner(self);
        let buffer = this.buffer.lock().unwrap();
        let (_, mut to_read) = buffer.split_at(this.position);

        let read = io::Read::read(&mut to_read, buf)?;
        this.position += read;

        Ready(Ok(read))
    }
}

impl AsyncWrite for MemoryStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = Pin::into_inner(self);
        let mut buffer = this.buffer.lock().unwrap();

        let write_end = this
            .position
            .checked_add(buf.len())
            .ok_or(io::ErrorKind::InvalidInput)?;
        let new_len = max(write_end, buffer.len());
        buffer.resize(new_len, 0u8);
        buffer[this.position..][..buf.len()].copy_from_slice(buf);
        this.position += buf.len();

        Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Ready(Ok(()))
    }
}

impl AsyncSeek for MemoryStream {
    fn poll_seek(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        pos: SeekFrom,
    ) -> Poll<io::Result<u64>> {
        match pos {
            SeekFrom::Start(p) => {
                self.position = p.try_into().map_err(|_| io::ErrorKind::InvalidInput)?;
                Ready(Ok(p))
            }
            SeekFrom::End(d) => {
                let base = self.buffer.lock().unwrap().len();
                let d = d.try_into().map_err(|_| io::ErrorKind::InvalidInput)?;
                self.position = base
                    .checked_add_signed(d)
                    .ok_or(io::ErrorKind::InvalidInput)?;
                Ready(Ok(self.position as u64))
            }
            SeekFrom::Current(d) => {
                let base = self.position;
                let d = d.try_into().map_err(|_| io::ErrorKind::InvalidInput)?;
                self.position = base
                    .checked_add_signed(d)
                    .ok_or(io::ErrorKind::InvalidInput)?;
                Ready(Ok(self.position as u64))
            }
        }
    }
}
