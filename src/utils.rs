use crate::Error;

// TODO: Implement the CompareOptions struct
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

pub(crate) struct Collation {
    lcid: i32,
    sort_options: CompareOptions
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

    pub fn read_date_time(&self, offset: usize) -> bson::DateTime {
        bson::DateTime::from_millis(self.read_i64(offset))
    }

    pub(crate) fn slice(&self, offset: usize, count: usize) -> &Self {
        Self::new(&self.buffer[offset..][..count])
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

    pub fn write_date_time(&mut self, offset: usize, value: bson::DateTime) {
        self.write_i64(offset, value.timestamp_millis());
    }
}
