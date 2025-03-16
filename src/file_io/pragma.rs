use crate::utils::{BufferSlice, Collation, CompareOptions};

const P_USER_VERSION: usize = 76; // 76-79 (4 bytes)
const P_COLLATION_LCID: usize = 80; // 80-83 (4 bytes)
const P_COLLATION_SORT: usize = 84; // 84-87 (4 bytes)
const P_TIMEOUT: usize = 88; // 88-91 (4 bytes)
// reserved 92-95 (4 bytes)
const P_UTC_DATE: usize = 96; // 96-96 (1 byte)
const P_CHECKPOINT: usize = 97; // 97-100 (4 bytes)
const P_LIMIT_SIZE: usize = 101; // 101-108 (8 bytes)

#[derive(Debug)]
pub(crate) struct EnginePragmas {
    pub user_version: i32,
    pub collation: Collation,
    pub timeout_seconds: i32,
    pub limit_size: i64,
    pub utc_date: bool,
    pub checkpoint: i32,
}

impl Default for EnginePragmas {
    fn default() -> Self {
        EnginePragmas {
            user_version: 0,
            collation: Collation::default(),
            timeout_seconds: 60,
            limit_size: i64::MAX,
            utc_date: false,
            checkpoint: 1000,
        }
    }
}

impl EnginePragmas {
    pub fn parse(buffer: &BufferSlice) -> Self {
        Self {
            user_version: buffer.read_i32(P_USER_VERSION),
            collation: Collation::new(
                buffer.read_i32(P_COLLATION_LCID),
                CompareOptions(buffer.read_i32(P_COLLATION_SORT)),
            ),
            timeout_seconds: buffer.read_i32(P_TIMEOUT),
            limit_size: {
                let limit_size = buffer.read_i64(P_LIMIT_SIZE);
                if limit_size == 0 {
                    i64::MAX
                } else {
                    limit_size
                }
            },
            utc_date: buffer.read_bool(P_UTC_DATE),
            checkpoint: buffer.read_i32(P_CHECKPOINT),
        }
    }

    pub(crate) fn update_buffer(&self, buffer: &mut BufferSlice) {
        buffer.write_i32(P_USER_VERSION, self.user_version);
        buffer.write_i32(P_COLLATION_LCID, self.collation.lcid);
        buffer.write_i32(P_COLLATION_SORT, self.collation.sort_options.0);
        buffer.write_i32(P_TIMEOUT, self.timeout_seconds);
        buffer.write_i64(P_LIMIT_SIZE, self.limit_size);
        buffer.write_bool(P_UTC_DATE, self.utc_date);
        buffer.write_i32(P_CHECKPOINT, self.checkpoint);
    }
}
