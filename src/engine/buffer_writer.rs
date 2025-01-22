use crate::utils::BufferSlice;

pub struct BufferWriter<'a> {
    slice: &'a mut BufferSlice,
    position: usize,
}

impl BufferWriter<'_> {
    pub fn new(slice: &mut BufferSlice) -> BufferWriter {
        BufferWriter { slice, position: 0 }
    }

    pub fn write_document(&mut self, document: &bson::Document) -> crate::Result<()> {
        // TODO? we may just unwrap here
        let mut bytes = bson::to_vec(document)?;
        self.slice.write_bytes(self.position, &bytes);
        self.position += bytes.len();
        Ok(())
    }
}
