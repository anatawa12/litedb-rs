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

pub(crate) struct EnginePragmas {
    pub user_version: i32,
    pub collation: Collation,
    pub timeout: Duration,
    pub limit_size: i64,
    pub utc_date: bool,
    pub checkpoint: i32,
    pub dirty: bool,
}

impl Default for EnginePragmas {
    fn default() -> Self {
        EnginePragmas {
            user_version: 0,
            collation: Collation::default(), // TODO: CultureInfo.CurrentCulture.LCID
            timeout: Duration::from_secs(60),
            limit_size: i64::MAX,
            utc_date: false,
            checkpoint: 1000,
            dirty: false,
        }
    }
}

impl EnginePragmas {
    pub fn read(buffer: &PageBuffer) -> crate::Result<Self> {
        let mut pragmas = EnginePragmas::default();

        pragmas.user_version = buffer.read_i32(P_USER_VERSION);
        pragmas.collation = Collation::new(
            buffer.read_i32(P_COLLATION_LCID),
            CompareOptions(buffer.read_i32(P_COLLATION_SORT)),
        );
        // TODO: -1 for unlimited?
        pragmas.timeout = Duration::from_secs(buffer.read_i32(P_TIMEOUT) as u64);
        let limit_size = buffer.read_i64(P_LIMIT_SIZE);
        pragmas.limit_size = if limit_size == 0 { i64::MAX } else { limit_size };
        pragmas.utc_date = buffer.read_bool(P_UTC_DATE);
        pragmas.checkpoint = buffer.read_i32(P_CHECKPOINT);
        pragmas.dirty = false;

        Ok(pragmas)
    }

    pub(crate) fn update_buffer(&self, buffer: &mut PageBuffer) {
        buffer.write_i32(P_USER_VERSION, self.user_version);
        buffer.write_i32(P_COLLATION_LCID, self.collation.lcid);
        buffer.write_i32(P_COLLATION_SORT, self.collation.sort_options.0);
        buffer.write_i32(P_TIMEOUT, self.timeout.as_secs() as i32);
        buffer.write_i64(P_LIMIT_SIZE, self.limit_size);
        buffer.write_byte(P_UTC_DATE, self.utc_date as u8);
        buffer.write_i32(P_CHECKPOINT, self.checkpoint);
    }
}
