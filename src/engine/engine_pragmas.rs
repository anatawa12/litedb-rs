use std::cell::RefCell;
use std::rc::Rc;
use crate::engine::PageBuffer;
use crate::utils::{Collation, CompareOptions};
use std::time::Duration;

const P_USER_VERSION: usize = 76; // 76-79 (4 bytes)
const P_COLLATION_LCID: usize = 80; // 80-83 (4 bytes)
const P_COLLATION_SORT: usize = 84; // 84-87 (4 bytes)
const P_TIMEOUT: usize = 88; // 88-91 (4 bytes)
// reserved 92-95 (4 bytes)
const P_UTC_DATE: usize = 96; // 96-96 (1 byte)
const P_CHECKPOINT: usize = 97; // 97-100 (4 bytes)
const P_LIMIT_SIZE: usize = 101; // 101-108 (8 bytes)

/// Clone this class will share the same inner object
#[derive(Clone)]
pub(crate) struct EnginePragmas {
    inner: Rc<RefCell<EnginePragmasInner>>,
}

struct EnginePragmasInner {
    user_version: i32,
    collation: Collation,
    timeout: Duration,
    limit_size: i64,
    utc_date: bool,
    checkpoint: i32,
    dirty: bool,
}

impl Default for EnginePragmas {
    fn default() -> Self {
        EnginePragmas {
            inner: Rc::new(RefCell::new(EnginePragmasInner {
                user_version: 0,
                collation: Collation::default(),
                timeout: Duration::from_secs(60),
                limit_size: i64::MAX,
                utc_date: false,
                checkpoint: 1000,
                dirty: false,
            })),
        }
    }
}

impl EnginePragmas {
    pub fn read(buffer: &PageBuffer) -> crate::Result<Self> {
        let mut pragmas = EnginePragmas::default();
        let mut inner = pragmas.inner.borrow_mut();

        inner.user_version = buffer.read_i32(P_USER_VERSION);
        inner.collation = Collation::new(
            buffer.read_i32(P_COLLATION_LCID),
            CompareOptions(buffer.read_i32(P_COLLATION_SORT)),
        );
        // TODO: -1 for unlimited?
        inner.timeout = Duration::from_secs(buffer.read_i32(P_TIMEOUT) as u64);
        let limit_size = buffer.read_i64(P_LIMIT_SIZE);
        inner.limit_size = if limit_size == 0 { i64::MAX } else { limit_size };
        inner.utc_date = buffer.read_bool(P_UTC_DATE);
        inner.checkpoint = buffer.read_i32(P_CHECKPOINT);
        inner.dirty = false;
        drop(inner);

        Ok(pragmas)
    }

    pub(crate) fn update_buffer(&self, buffer: &mut PageBuffer) {
        let inner = self.inner.borrow();
        buffer.write_i32(P_USER_VERSION, inner.user_version);
        buffer.write_i32(P_COLLATION_LCID, inner.collation.lcid);
        buffer.write_i32(P_COLLATION_SORT, inner.collation.sort_options.0);
        buffer.write_i32(P_TIMEOUT, inner.timeout.as_secs() as i32);
        buffer.write_i64(P_LIMIT_SIZE, inner.limit_size);
        buffer.write_byte(P_UTC_DATE, inner.utc_date as u8);
        buffer.write_i32(P_CHECKPOINT, inner.checkpoint);
    }

    pub fn user_version(&self) -> i32 {
        self.inner.borrow().user_version
    }

    pub fn collation(&self) -> Collation {
        self.inner.borrow().collation
    }

    pub fn timeout(&self) -> Duration {
        self.inner.borrow().timeout
    }

    pub fn limit_size(&self) -> i64 {
        self.inner.borrow().limit_size
    }

    pub fn utc_date(&self) -> bool {
        self.inner.borrow().utc_date
    }

    pub fn checkpoint(&self) -> i32 {
        self.inner.borrow().checkpoint
    }

    pub fn set_user_version(&self, user_version: i32) {
        let mut inner = self.inner.borrow_mut();
        inner.user_version = user_version;
        inner.dirty = true;
    }

    pub fn set_collation(&self, collation: Collation) {
        let mut inner = self.inner.borrow_mut();
        inner.collation = collation;
        inner.dirty = true;
    }

    pub fn set_timeout(&self, timeout: Duration) {
        let mut inner = self.inner.borrow_mut();
        inner.timeout = timeout;
        inner.dirty = true;
    }
    

    pub fn set_limit_size(&self, limit_size: i64) {
        let mut inner = self.inner.borrow_mut();
        inner.limit_size = limit_size;
        inner.dirty = true;
    }

    pub fn set_utc_date(&self, utc_date: bool) {
        let mut inner = self.inner.borrow_mut();
        inner.utc_date = utc_date;
        inner.dirty = true;
    }

    pub fn set_checkpoint(&self, checkpoint: i32) {
        let mut inner = self.inner.borrow_mut();
        inner.checkpoint = checkpoint;
        inner.dirty = true;
    }
}
