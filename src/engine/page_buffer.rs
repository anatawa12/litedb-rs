use std::fmt;
use crate::engine::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FileOrigin {
    Data = 1,
    Log = 2,
}

/// Represents the pair of a position and a file origin.
/// position must be less than i64::MAX.
// highest bit is used to distinguish between data and log files
// 0 means data file, 1 means log file
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PositionOrigin(u64);

impl PositionOrigin {
    const ORIGIN_MASK: u64 = 1 << 63;

    pub fn new(position: u64, origin: FileOrigin) -> Self {
        assert!(
            position < i64::MAX as u64,
            "offset must not exceed i64::MAX"
        );
        let origin = match origin {
            FileOrigin::Data => 0,
            FileOrigin::Log => Self::ORIGIN_MASK,
        };
        PositionOrigin(position | origin)
    }

    pub fn position(&self) -> u64 {
        self.0 & !Self::ORIGIN_MASK
    }

    pub fn origin(&self) -> FileOrigin {
        if self.0 & Self::ORIGIN_MASK == 0 {
            FileOrigin::Data
        } else {
            FileOrigin::Log
        }
    }
}

impl fmt::Debug for PositionOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PositionOrigin")
            .field("position", &self.position())
            .field("origin", &self.origin())
            .finish()
    }
}

pub(crate) struct PageBuffer {
    position_origin: PositionOrigin,
    buffer: [u8; PAGE_SIZE],
}

impl PageBuffer {
    pub fn new() -> Self {
        PageBuffer {
            position_origin: PositionOrigin(0),
            buffer: [0; PAGE_SIZE],
        }
    }

    pub fn set_position_origin(&mut self, position: u64, origin: FileOrigin) {
        self.position_origin = PositionOrigin::new(position, origin);
    }

    pub fn buffer(&self) -> &[u8; PAGE_SIZE] {
        &self.buffer
    }

    pub fn buffer_mut(&mut self) -> &mut [u8; PAGE_SIZE] {
        &mut self.buffer
    }

    pub(super) fn update_time(&self) {
        // NO LRU for now
        // Interlocked.Exchange(ref page.Timestamp, DateTime.UtcNow.Ticks)
    }
}

// TODO: BufferSlice
impl PageBuffer {
    pub fn read_bool(&self, offset: usize) -> bool {
        self.buffer[offset] != 0
    }

    pub fn read_byte(&self, offset: usize) -> u8 {
        self.buffer[offset]
    }

    pub fn read_i16(&self, offset: usize) -> i16 {
        i16::from_le_bytes(self.buffer[offset..][..2].try_into().unwrap())
    }

    pub fn read_i32(&self, offset: usize) -> i32 {
        i32::from_le_bytes(self.buffer[offset..][..4].try_into().unwrap())
    }

    pub fn read_i64(&self, offset: usize) -> i64 {
        i64::from_le_bytes(self.buffer[offset..][..8].try_into().unwrap())
    }

    pub fn read_u16(&self, offset: usize) -> u16 {
        u16::from_le_bytes(self.buffer[offset..][..2].try_into().unwrap())
    }

    pub fn read_u32(&self, offset: usize) -> u32 {
        u32::from_le_bytes(self.buffer[offset..][..4].try_into().unwrap())
    }

    pub fn read_u64(&self, offset: usize) -> u64 {
        u64::from_le_bytes(self.buffer[offset..][..8].try_into().unwrap())
    }

    pub fn read_f64(&self, offset: usize) -> f64 {
        f64::from_le_bytes(self.buffer[offset..][..8].try_into().unwrap())
    }

    // TODO: Decimal

    pub fn read_bytes(&self, offset: usize, length: usize) -> &[u8] {
        &self.buffer[offset..][..length]
    }

    pub fn read_string(&self, offset: usize, length: usize) -> Result<&str> {
        std::str::from_utf8(self.read_bytes(offset, length)).map_err(Error::err)
    }
}
