use crate::Result;
use crate::engine::{Stream, StreamFactory};
use async_lock::{Mutex, OnceCell};
use std::ops::{Deref, DerefMut};

type StreamStorage = Option<Box<dyn Stream>>;

pub(crate) struct StreamPool {
    streams: opool::Pool<StreamStorageFactory, StreamStorage>,
    writable_cell: OnceCell<Mutex<Box<dyn Stream>>>,
    factory: Box<dyn StreamFactory>,
}

struct StreamStorageFactory;

impl opool::PoolAllocator<StreamStorage> for StreamStorageFactory {
    fn allocate(&self) -> StreamStorage {
        None
    }
}

pub(crate) struct StreamGuard<'a> {
    inner: opool::RefGuard<'a, StreamStorageFactory, StreamStorage>,
}

impl Deref for StreamGuard<'_> {
    type Target = dyn Stream;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref().as_ref().unwrap().deref()
    }
}

impl DerefMut for StreamGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut().unwrap().deref_mut()
    }
}

pub(crate) struct WriteableScope<'a> {
    inner: async_lock::MutexGuard<'a, Box<dyn Stream>>,
}

impl Deref for WriteableScope<'_> {
    type Target = dyn Stream;

    fn deref(&self) -> &Self::Target {
        self.inner.as_ref()
    }
}

impl DerefMut for WriteableScope<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.inner.as_mut()
    }
}

impl StreamPool {
    pub(crate) fn new(factory: Box<dyn StreamFactory>) -> Self {
        Self {
            streams: opool::Pool::new(10, StreamStorageFactory),
            writable_cell: OnceCell::new(),
            factory,
        }
    }

    pub async fn rent(&self) -> Result<StreamGuard> {
        let mut inner = self.streams.get();
        if inner.is_none() {
            *inner = Some(self.factory.get_stream(false).await?);
        }
        Ok(StreamGuard { inner })
    }

    pub fn factory(&self) -> &dyn StreamFactory {
        self.factory.as_ref()
    }

    pub async fn writeable(&self) -> Result<WriteableScope> {
        let mutex = self
            .writable_cell
            .get_or_try_init(async || -> Result<_> {
                Ok(Mutex::new(self.factory.get_stream(true).await?))
            })
            .await?;
        let inner = mutex.lock().await;
        Ok(WriteableScope { inner })
    }

    pub async fn writeable_mut(&mut self) -> Result<&mut dyn Stream> {
        self.writeable().await?;
        Ok(self.writable_cell.get_mut().unwrap().get_mut().as_mut())
    }
}
