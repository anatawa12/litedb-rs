use crate::utils::BufferSlice;

pub(crate) struct BufferReader<'a> {
    slice: &'a BufferSlice,
    position: usize,
}

impl BufferReader<'_> {
    pub fn new(slice: &BufferSlice) -> BufferReader {
        BufferReader { slice, position: 0 }
    }

    pub fn read_document(&mut self) -> crate::Result<bson::Document> {
        let length = self.slice.read_i32(self.position) as usize;
        let document_bin = self.slice.read_bytes(self.position, length);
        self.position += length;
        Ok(bson::Document::from_reader(document_bin)?)
    }
}
