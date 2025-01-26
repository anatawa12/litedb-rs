use crate::engine::*;
use futures::lock::Mutex;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::ops::AsyncFnOnce;
use std::rc::Rc;
// Difference between original MemoryCache.cs
// - Reference counter is with Arc instead of ShareCounter
// - Writable page is represented as Box<PageBuffer>

// TODO: Implement FreePageCache
pub(crate) struct MemoryCache {
    readable: Mutex<HashMap<PositionOrigin, Rc<PageBuffer>>>,
    free_page_cache: FreePageCache,
}

impl MemoryCache {
    pub fn new() -> Self {
        MemoryCache {
            readable: Mutex::new(HashMap::new()),
            free_page_cache: FreePageCache::new(),
        }
    }

    pub async fn get_readable_page(
        &self,
        position: u64,
        origin: FileOrigin,
        factory: impl AsyncFnOnce(u64, &mut PageBufferArray) -> Result<()>,
    ) -> Result<Rc<PageBuffer>> {
        let key = PositionOrigin::new(position, origin);
        let page = match self.readable.lock().await.entry(key) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => {
                let mut new_page = self.free_page_cache.get_free_page();

                let as_mut = Rc::get_mut(&mut new_page).unwrap();
                as_mut.set_position_origin(position, origin);
                factory(position, as_mut.buffer_mut()).await?;

                v.insert(new_page.clone());
                new_page
            }
        };
        page.update_time();
        Ok(page)
    }

    pub async fn get_writable_page(
        &self,
        position: u64,
        origin: FileOrigin,
        factory: impl AsyncFnOnce(u64, &mut PageBufferArray) -> Result<()>,
    ) -> Result<Box<PageBuffer>> {
        let key = PositionOrigin::new(position, origin);
        let mut new_page = self.free_page_cache.new_page(position, origin);

        if let Some(readable) = self.readable.lock().await.get(&key) {
            *new_page.buffer_mut() = *readable.buffer();
        } else {
            factory(position, new_page.as_mut().buffer_mut()).await?;
        }

        Ok(new_page)
    }

    pub fn new_page(&self) -> Box<PageBuffer> {
        self.free_page_cache
            .new_page(i64::MAX as u64, FileOrigin::Data)
    }

    fn get_key(position: u64, origin: FileOrigin) -> u64 {
        assert!(
            position < i64::MAX as u64,
            "offset must not exceed i64::MAX"
        );
        if origin == FileOrigin::Data {
            position
        } else {
            !position
        }
    }

    pub async fn try_move_to_readable(
        &self,
        page: Box<PageBuffer>,
    ) -> std::result::Result<Rc<PageBuffer>, Box<PageBuffer>> {
        debug_assert!(page.position() != u64::MAX);
        // page.wriable
        debug_assert!(page.origin().is_some());

        let key = PositionOrigin::new(page.position(), page.origin().unwrap());

        match self.readable.lock().await.entry(key) {
            Entry::Occupied(_) => {
                // there already is. failed to make readable
                Err(page)
            }
            Entry::Vacant(v) => Ok(v.insert(Rc::new(*page)).clone()),
        }
    }

    pub(crate) async fn move_to_readable(&self, page: Box<PageBuffer>) -> Rc<PageBuffer> {
        debug_assert!(page.position() != u64::MAX);
        // page.wriable
        debug_assert!(page.origin().is_some());

        let origin = page.origin().unwrap();
        let key = PositionOrigin::new(page.position(), origin);

        match self.readable.lock().await.entry(key) {
            Entry::Occupied(mut o) => {
                //assert_eq!(Rc::strong_count(o.get()), 1, "user must ensure this page is not in use when marked as read only");
                debug_assert_eq!(o.get().origin(), Some(origin), "origin must be same");

                *Rc::get_mut(o.get_mut())
                    .expect("user must ensure this page is not in use when marked as read only")
                    .buffer_mut() = *page.buffer();

                //self.discard_page(page); no cache reuse system

                o.get().clone()
            }
            Entry::Vacant(v) => v.insert(Rc::new(*page)).clone(),
        }
    }

    pub async fn pages_in_use(&self) -> usize {
        self.readable
            .lock()
            .await
            .values()
            .map(|x| Rc::strong_count(x) - 1)
            .sum()
    }

    pub(crate) async fn clear(&self) {
        assert_eq!(self.pages_in_use().await, 0, "all pages must be released");
        self.readable.lock().await.clear();
    }
}

struct FreePageCache {}

impl FreePageCache {
    fn new() -> Self {
        FreePageCache {}
    }

    fn get_free_page(&self) -> Rc<PageBuffer> {
        // NO free page cache
        Rc::new(PageBuffer::new(0))
    }

    fn new_page(&self, position: u64, origin: FileOrigin) -> Box<PageBuffer> {
        let mut buffer = Box::new(PageBuffer::new(0));
        buffer.set_position_origin(position, origin);
        buffer
    }
}
