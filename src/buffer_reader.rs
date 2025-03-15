use crate::Result;
use crate::bson;
use crate::utils::{BufferSlice, PageAddress};

pub(crate) struct BufferReader<'a> {
    slices: Box<[&'a BufferSlice]>,
    slice_index: usize,
    position_in_slice: usize,
    global_position: usize,
}

#[allow(dead_code)]
impl<'a> BufferReader<'a> {
    pub fn single(slice: &'a BufferSlice) -> Self {
        Self::fragmented([slice])
    }

    pub fn fragmented(slices: impl Into<Box<[&'a BufferSlice]>>) -> Self {
        Self {
            slices: slices.into(),
            slice_index: 0,
            position_in_slice: 0,
            global_position: 0,
        }
    }

    fn peek(&mut self, buffer: &mut [u8]) {
        let slice_index = self.slice_index;
        let position_in_slice = self.position_in_slice;
        let global_position = self.global_position;
        self.read_buffer(buffer);
        self.slice_index = slice_index;
        self.position_in_slice = position_in_slice;
        self.global_position = global_position;
    }

    fn read_buffer(&mut self, mut buffer: &mut [u8]) {
        while !buffer.is_empty() {
            assert!(self.slice_index < self.slices.len(), "End of Stream");
            let current = self.slices[self.slice_index];
            let current_remaining = current.len() - self.position_in_slice;
            if buffer.len() < current_remaining {
                // we can write data in current slice

                buffer.copy_from_slice(current.read_bytes(self.position_in_slice, buffer.len()));
                self.position_in_slice += buffer.len();
                self.global_position += buffer.len();
                assert!(self.position_in_slice > 0 && self.position_in_slice <= current.len());
                buffer = &mut [];
            } else {
                // we use current slice fully
                let (to_current, next) = buffer.split_at_mut(current_remaining);
                to_current
                    .copy_from_slice(current.read_bytes(self.position_in_slice, to_current.len()));
                self.global_position += current_remaining;
                buffer = next;
                self.slice_index += 1;
                self.position_in_slice = 0;
            }
        }
    }

    pub fn read_document(&mut self) -> Result<bson::Document> {
        Ok(bson::Document::parse_document(self)?)
    }

    pub(crate) fn read_array(&mut self) -> Result<bson::Array> {
        Ok(bson::Array::parse_array(self)?)
    }

    pub fn skip(&mut self, mut bytes: usize) {
        while bytes > 0 {
            assert!(self.slice_index < self.slices.len(), "End of Stream");
            let current = &mut self.slices[self.slice_index];
            let current_remaining = current.len() - self.position_in_slice;
            if bytes < current_remaining {
                // we can consume bytes from current slice
                self.position_in_slice += bytes;
                self.global_position += bytes;
                assert!(self.position_in_slice > 0 && self.position_in_slice <= current.len());
                bytes = 0;
            } else {
                // use current slice fully
                self.global_position += current_remaining;
                bytes -= current_remaining;
                self.slice_index += 1;
                self.position_in_slice = 0;
            }
        }
    }

    pub fn position(&self) -> usize {
        self.global_position
    }
}

#[allow(dead_code)]
impl BufferReader<'_> {
    fn read<T, const S: usize>(&mut self, f: impl Fn([u8; S]) -> T) -> T {
        let mut array = [0u8; S];
        self.read_buffer(&mut array[..]);
        f(array)
    }

    pub fn read_i32(&mut self) -> i32 {
        self.read(i32::from_le_bytes)
    }

    pub fn read_u32(&mut self) -> u32 {
        self.read(u32::from_le_bytes)
    }

    pub fn read_u16(&mut self) -> u16 {
        self.read(u16::from_le_bytes)
    }

    pub fn read_u8(&mut self) -> u8 {
        self.read(u8::from_le_bytes)
    }

    pub fn read_i8(&mut self) -> i8 {
        self.read(i8::from_le_bytes)
    }

    pub fn read_i64(&mut self) -> i64 {
        self.read(i64::from_le_bytes)
    }

    pub fn read_u64(&mut self) -> u64 {
        self.read(u64::from_le_bytes)
    }

    pub fn read_f64(&mut self) -> f64 {
        self.read(f64::from_le_bytes)
    }

    pub fn read_bool(&mut self) -> bool {
        self.read_u8() != 0
    }

    pub fn read_cstring(&mut self) -> Option<String> {
        let mut bytes = Vec::new();
        loop {
            let byte = self.read_u8();
            if byte == 0 {
                break;
            }
            bytes.push(byte);
        }
        String::from_utf8(bytes).ok()
    }

    pub fn read_page_address(&mut self) -> PageAddress {
        let page_id = self.read_u32();
        let slot = self.read_u8();
        PageAddress::new(page_id, slot)
    }
}

impl bson::BsonReader for BufferReader<'_> {
    type Error = bson::ParseError;

    fn read_fully(&mut self, bytes: &mut [u8]) -> std::result::Result<(), Self::Error> {
        self.read_buffer(bytes);
        Ok(())
    }

    fn is_end(&self) -> bool {
        true
    }
}
