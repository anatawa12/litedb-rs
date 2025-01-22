use crate::engine::*;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::AsyncFnOnce;
use std::rc::Rc;

// Difference between original MemoryCache.cs
// - Reference counter is with Arc instead of ShareCounter
// - Writable page is represented as Box<PageBuffer>

// TODO: Implement FreePageCache
pub(crate) struct MemoryCache {
    readable: HashMap<PositionOrigin, Rc<PageBuffer>>,
    free_page_cache: FreePageCache,
}

impl MemoryCache {
    pub fn new() -> Self {
        MemoryCache {
            readable: HashMap::new(),
            free_page_cache: FreePageCache::new(),
        }
    }

    pub async fn get_readable_page(
        &mut self,
        position: u64,
        origin: FileOrigin,
        factory: impl AsyncFnOnce(u64, &mut PageBuffer) -> Result<()>,
    ) -> Result<Rc<PageBuffer>> {
        let key = PositionOrigin::new(position, origin);
        let page = match self.readable.entry(key) {
            Entry::Occupied(o) => o.get().clone(),
            Entry::Vacant(v) => {
                let mut new_page = self.free_page_cache.get_free_page();

                let as_mut = Rc::get_mut(&mut new_page).unwrap();
                as_mut.set_position_origin(position, origin);
                factory(position, as_mut).await?;

                v.insert(new_page.clone());
                new_page
            }
        };
        page.update_time();
        Ok(page)
    }

    pub async fn get_writable_page(
        &mut self,
        position: u64,
        origin: FileOrigin,
        factory: impl AsyncFnOnce(u64, &mut PageBuffer) -> Result<()>,
    ) -> Result<Box<PageBuffer>> {
        let key = PositionOrigin::new(position, origin);
        let mut new_page = self.free_page_cache.new_page(position, origin);

        if let Some(readable) = self.readable.get(&key) {
            *new_page.buffer_mut() = *readable.buffer();
        } else {
            factory(position, new_page.as_mut()).await?;
        }

        Ok(new_page)
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
}

struct FreePageCache {
}

impl FreePageCache {
    fn new() -> Self {
        FreePageCache {}
    }

    fn get_free_page(&self) -> Rc<PageBuffer> {
        // NO free page cache
        Rc::new(PageBuffer::new())
    }

    fn new_page(&self, position: u64, origin: FileOrigin) -> Box<PageBuffer> {
        let mut buffer = Box::new(PageBuffer::new());
        buffer.set_position_origin(position, origin);
        buffer
    }
}
