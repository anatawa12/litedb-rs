use std::fmt;
use std::ops::Deref;
use crate::engine::*;
use crate::utils::BufferSlice;

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

impl Deref for PageBuffer {
    type Target = BufferSlice;

    fn deref(&self) -> &Self::Target {
        BufferSlice::new(&self.buffer)
    }
}
