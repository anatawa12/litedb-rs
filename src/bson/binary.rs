use crate::bson::BsonWriter;
use std::fmt::Debug;

#[derive(Eq, PartialEq, Hash, Debug, Clone, Ord, PartialOrd)]
pub struct Binary {
    bytes: Vec<u8>,
}

impl Binary {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl Binary {
    /// Returns the size of serialized value.
    ///
    /// This doesn't include tag or name of key.
    pub fn get_serialized_value_len(&self) -> usize {
        4 + 1 + self.bytes.len()
    }

    /// Writes the value to the BsonWriter
    pub fn write_value<W: BsonWriter>(&self, w: &mut W) -> Result<(), W::Error> {
        let len =
            i32::try_from(self.bytes.len()).map_err(|_| W::when_too_large(self.bytes.len()))?;

        w.write_bytes(&len.to_le_bytes())?;
        w.write_bytes(&[0x00])?;
        w.write_bytes(&self.bytes)?;

        Ok(())
    }
}

impl From<Vec<u8>> for Binary {
    fn from(bytes: Vec<u8>) -> Self {
        Binary::new(bytes)
    }
}

#[test]
fn cmp_test() {
    assert!(Binary::new(vec![]) < Binary::new(vec![1, 2, 3]));
    assert!(Binary::new(vec![0]) < Binary::new(vec![1, 2, 3]));
}
