use crate::engine::{DirtyFlag, PageBuffer};
use crate::utils::{Collation, CompareOptions};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicI64, AtomicU64};
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
pub(crate) struct EnginePragmas {
    user_version: AtomicI32,
    collation: AtomicU64,
    timeout_seconds: AtomicI32,
    limit_size: AtomicI64,
    utc_date: AtomicBool,
    checkpoint: AtomicI32,
    dirty: DirtyFlag,
}

impl Default for EnginePragmas {
    fn default() -> Self {
        EnginePragmas {
            user_version: 0.into(),
            collation: Collation::default().to_u64().into(),
            timeout_seconds: 60.into(),
            limit_size: i64::MAX.into(),
            utc_date: false.into(),
            checkpoint: 1000.into(),
            dirty: DirtyFlag::new(),
        }
    }
}

#[allow(dead_code)]
impl EnginePragmas {
    pub fn read(&self, buffer: &PageBuffer) -> crate::Result<()> {
        self.user_version
            .store(buffer.read_i32(P_USER_VERSION), Relaxed);
        self.collation.store(
            Collation::new(
                buffer.read_i32(P_COLLATION_LCID),
                CompareOptions(buffer.read_i32(P_COLLATION_SORT)),
            )
            .to_u64(),
            Relaxed,
        );
        self.timeout_seconds
            .store(buffer.read_i32(P_TIMEOUT), Relaxed);
        let limit_size = buffer.read_i64(P_LIMIT_SIZE);
        self.limit_size.store(
            if limit_size == 0 {
                i64::MAX
            } else {
                limit_size
            },
            Relaxed,
        );
        self.utc_date.store(buffer.read_bool(P_UTC_DATE), Relaxed);
        self.checkpoint
            .store(buffer.read_i32(P_CHECKPOINT), Relaxed);
        self.dirty.reset();

        Ok(())
    }

    pub(crate) fn update_buffer(&self, buffer: &mut PageBuffer) {
        buffer.write_i32(P_USER_VERSION, self.user_version.load(Relaxed));
        let collation = Collation::from_u64(self.collation.load(Relaxed));
        buffer.write_i32(P_COLLATION_LCID, collation.lcid);
        buffer.write_i32(P_COLLATION_SORT, collation.sort_options.0);
        buffer.write_i32(P_TIMEOUT, self.timeout_seconds.load(Relaxed));
        buffer.write_i64(P_LIMIT_SIZE, self.limit_size.load(Relaxed));
        buffer.write_bool(P_UTC_DATE, self.utc_date.load(Relaxed));
        buffer.write_i32(P_CHECKPOINT, self.checkpoint.load(Relaxed));
    }

    pub fn user_version(&self) -> i32 {
        self.user_version.load(Relaxed)
    }

    pub fn collation(&self) -> Collation {
        Collation::from_u64(self.collation.load(Relaxed))
    }

    pub fn timeout(&self) -> Duration {
        Duration::from_secs(self.timeout_seconds.load(Relaxed) as u64)
    }

    pub fn limit_size(&self) -> i64 {
        self.limit_size.load(Relaxed)
    }

    pub fn utc_date(&self) -> bool {
        self.utc_date.load(Relaxed)
    }

    pub fn checkpoint(&self) -> i32 {
        self.checkpoint.load(Relaxed)
    }

    pub fn set_user_version(&self, user_version: i32) {
        self.user_version.store(user_version, Relaxed);
        self.dirty.set();
    }

    pub fn set_collation(&self, collation: Collation) {
        self.collation.store(collation.to_u64(), Relaxed);
        self.dirty.set();
    }

    pub fn set_timeout(&self, timeout: Duration) {
        self.timeout_seconds
            .store(timeout.as_secs() as i32, Relaxed);
        self.dirty.set()
    }

    pub fn set_limit_size(&self, limit_size: i64) {
        self.limit_size.store(limit_size, Relaxed);
        self.dirty.set();
    }

    pub fn set_utc_date(&self, utc_date: bool) {
        self.utc_date.store(utc_date, Relaxed);
        self.dirty.set();
    }

    pub fn set_checkpoint(&self, checkpoint: i32) {
        self.checkpoint.store(checkpoint, Relaxed);
        self.dirty.set();
    }
}
