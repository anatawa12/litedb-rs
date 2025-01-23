use std::time::SystemTime;
use crate::Error;

// TODO: Implement the CompareOptions struct
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompareOptions(pub i32);

impl CompareOptions {
    pub const IGNORE_CASE: CompareOptions = CompareOptions(1);
    pub const IGNORE_KANA_TYPE: CompareOptions = CompareOptions(8);
    pub const IGNORE_NON_SPACE: CompareOptions = CompareOptions(2);
    pub const IGNORE_SYMBOLS: CompareOptions = CompareOptions(4);
    pub const IGNORE_WIDTH: CompareOptions = CompareOptions(16);
    pub const ORDINAL: CompareOptions = CompareOptions(1073741824);
    pub const STRING_SORT: CompareOptions = CompareOptions(536870912);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Collation {
    pub lcid: i32,
    pub sort_options: CompareOptions,
}

impl Default for Collation {
    fn default() -> Self {
        Collation {
            lcid: 127, // invariant
            sort_options: CompareOptions::IGNORE_CASE,
        }
    }
}

impl Collation {
    pub fn new(lcid: i32, sort_options: CompareOptions) -> Self {
        Collation {
            lcid,
            sort_options
        }
    }
}

#[repr(transparent)]
pub struct BufferSlice {
    buffer: [u8],
}

impl BufferSlice {
    
    pub fn new(buffer: &[u8]) -> &Self {
        unsafe { &*(buffer as *const _ as *const Self) }
    }

    pub fn new_mut(buffer: &mut [u8]) -> &mut Self {
        unsafe { &mut *(buffer as *mut _ as *mut Self) }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl BufferSlice {
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

    pub fn read_string(&self, offset: usize, length: usize) -> crate::Result<&str> {
        std::str::from_utf8(self.read_bytes(offset, length)).map_err(Error::err)
    }

    pub fn read_date_time(&self, offset: usize) -> crate::Result<CsDateTime> {
        CsDateTime::from_ticks(self.read_u64(offset)).ok_or_else(|| Error::datetime_overflow())
    }

    pub(crate) fn slice(&self, offset: usize, count: usize) -> &Self {
        Self::new(&self.buffer[offset..][..count])
    }

    pub fn clear(&mut self, offset: usize, count: usize) {
        self.buffer[offset..][..count].fill(0);
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }
}

// writers
impl BufferSlice {
    pub fn write_bool(&mut self, offset: usize, value: bool) {
        self.buffer[offset] = value as u8;
    }

    pub fn write_byte(&mut self, offset: usize, value: u8) {
        self.buffer[offset] = value;
    }

    pub fn write_i16(&mut self, offset: usize, value: i16) {
        self.buffer[offset..][..2].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_i32(&mut self, offset: usize, value: i32) {
        self.buffer[offset..][..4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_i64(&mut self, offset: usize, value: i64) {
        self.buffer[offset..][..8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_u8(&mut self, offset: usize, value: u8) {
        self.buffer[offset] = value;
    }

    pub fn write_u16(&mut self, offset: usize, value: u16) {
        self.buffer[offset..][..2].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_u32(&mut self, offset: usize, value: u32) {
        self.buffer[offset..][..4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_u64(&mut self, offset: usize, value: u64) {
        self.buffer[offset..][..8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_f64(&mut self, offset: usize, value: f64) {
        self.buffer[offset..][..8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_bytes(&mut self, offset: usize, value: &[u8]) {
        self.buffer[offset..][..value.len()].copy_from_slice(value);
    }

    pub fn write_string(&mut self, offset: usize, value: &str) {
        self.write_bytes(offset, value.as_bytes());
    }

    pub fn write_date_time(&mut self, offset: usize, value: CsDateTime) {
        self.write_u64(offset, value.ticks());
    }

    pub(crate) fn slice_mut(&mut self, offset: usize, count: usize) -> &mut Self {
        Self::new_mut(&mut self.buffer[offset..][..count])
    }
}

/// C# DateTime in Rust
/// 
/// This represents number of 100 nano seconds since 0001-01-01 00:00:00 UTC
/// This can represent 0001-01-01 00:00:00 ~ 9999-12-31 23:59:59.99999999
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd)]
pub(crate) struct CsDateTime(pub u64);

impl CsDateTime {
    const MAX_TICKS: u64 = 3155378975999999999;
    const UNIX_EPOC_TICKS: u64 = 621355968000000000;

    pub const MIN: CsDateTime = CsDateTime(0);
    pub const MAX: CsDateTime = CsDateTime(Self::MAX_TICKS);

    pub fn now() -> Self {
        // now must not exceed MAX_TICKS / MIN_TICKS
        Self::from_system(SystemTime::now()).unwrap()
    }

    pub fn from_system(system: SystemTime) -> Option<Self> {
        let ticks = match system.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(duration) => {
                let nanos = duration.as_nanos();

                let after_epoc_ticks = Self::MAX_TICKS - Self::UNIX_EPOC_TICKS;
                let after_epoc_nanos = after_epoc_ticks as u128 * 100;
                if nanos > after_epoc_nanos {
                    return None;
                }
                let ticks = Self::UNIX_EPOC_TICKS + (nanos / 100) as u64;
                ticks
            },
            Err(e) => {
                let duration = e.duration();
                let nanos = duration.as_nanos();

                let unix_epoc_nanos = Self::UNIX_EPOC_TICKS as u128 * 100;

                if nanos > unix_epoc_nanos {
                    return None;
                }
                let ticks = Self::UNIX_EPOC_TICKS - (nanos / 100) as u64;
                ticks
            },
        };
        Some(CsDateTime(ticks))
    }

    pub fn from_ticks(ticks: u64) -> Option<CsDateTime> {
        if ticks > Self::MAX_TICKS {
            None
        } else {
            Some(CsDateTime(ticks))
        }
    }
}

impl CsDateTime {
    pub fn ticks(&self) -> u64 {
        self.0
    }
}
