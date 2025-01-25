use crate::bson;
use crate::engine::page_address::PageAddress;
use crate::utils::BufferSlice;
use std::convert::Infallible;

pub struct BufferWriter<'a> {
    slice: &'a mut BufferSlice,
    position: usize,
}

impl BufferWriter<'_> {
    pub fn new(slice: &mut BufferSlice) -> BufferWriter {
        BufferWriter { slice, position: 0 }
    }

    pub fn write_document(&mut self, document: &bson::Document) {
        into_ok!(document.write_value(self));
    }

    pub(crate) fn write_array(&mut self, array: &bson::Array) {
        into_ok!(array.write_value(self));
    }

    pub fn skip(&mut self, bytes: usize) {
        self.position += bytes;
    }

    pub fn position(&self) -> usize {
        self.position
    }
}

impl BufferWriter<'_> {
    fn write(&mut self, data: &[u8]) {
        self.slice.write_bytes(self.position, data);
        self.position += data.len();
    }

    pub fn write_i32(&mut self, value: i32) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_u32(&mut self, value: u32) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_u16(&mut self, value: u16) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_u8(&mut self, value: u8) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_i8(&mut self, value: i8) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_i64(&mut self, value: i64) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_u64(&mut self, value: u64) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_f64(&mut self, value: f64) {
        self.write(&value.to_le_bytes());
    }

    pub fn write_bool(&mut self, value: bool) {
        self.write_u8(value as u8);
    }

    pub fn write_cstring(&mut self, value: &str) {
        // TODO: check if value does not contain null byte
        self.write(value.as_bytes());
        self.write(&[0]);
    }

    pub fn write_bytes(&mut self, value: &[u8]) {
        self.write(value);
    }

    pub fn write_page_address(&mut self, value: PageAddress) {
        self.write_u32(value.page_id());
        self.write_u8(value.index());
    }
}

impl bson::BsonWriter for BufferWriter<'_> {
    type Error = Infallible;

    fn when_too_large(size: usize) -> Self::Error {
        panic!("The content size too long ({} bytes)", size);
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.write_bytes(bytes);
        Ok(())
    }
}
