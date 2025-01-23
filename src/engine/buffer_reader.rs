use crate::engine::page_address::PageAddress;
use crate::Result;
use crate::utils::BufferSlice;

pub(crate) struct BufferReader<'a> {
    slice: &'a BufferSlice,
    position: usize,
}

impl BufferReader<'_> {
    pub fn new(slice: &BufferSlice) -> BufferReader {
        BufferReader { slice, position: 0 }
    }

    pub fn read_document(&mut self) -> Result<bson::Document> {
        let length = self.slice.read_i32(self.position) as usize;
        let document_bin = self.slice.read_bytes(self.position, length);
        self.position += length;
        Ok(bson::Document::from_reader(document_bin)?)
    }

    pub fn skip(&mut self, bytes: usize) {
        self.position += bytes;
    }

    pub fn position(&self) -> usize {
        self.position
    }
}

impl BufferReader<'_> {
    fn read<T, const S : usize>(&mut self, f: impl Fn([u8; S]) -> T) -> T {
        let array = self.slice.read_bytes(self.position, S);
        self.position += S;
        f(array.try_into().unwrap())
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

    pub fn read_cstring(&mut self) -> Result<String> {
        let mut bytes = Vec::new();
        loop {
            let byte = self.read_u8();
            if byte == 0 {
                break;
            }
            bytes.push(byte);
        }
        Ok(String::from_utf8(bytes)?)
    }

    pub fn read_page_address(&mut self) -> PageAddress {
        let page_id = self.read_u32();
        let slot = self.read_u8();
        PageAddress::new(page_id, slot)
    }
}
