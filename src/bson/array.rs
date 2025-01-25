use super::{BsonWriter, Value};
use std::fmt::Debug;
use std::vec;

#[derive(Clone, PartialEq)]
pub struct Array {
    data: Vec<Value>,
}

impl Default for Array {
    fn default() -> Self {
        Self::new()
    }
}

impl Array {
    pub fn new() -> Array {
        Array { data: Vec::new() }
    }

    pub fn as_slice(&self) -> &[Value] {
        &self.data
    }

    pub fn as_mut_slice(&mut self) -> &mut [Value] {
        &mut self.data
    }

    pub fn push(&mut self, value: impl Into<Value>) {
        self.data.push(value.into());
    }

    pub fn pop(&mut self) -> Option<Value> {
        self.data.pop()
    }

    pub fn iter(&self) -> std::slice::Iter<Value> {
        self.data.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<Value> {
        self.data.iter_mut()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl Array {
    /// Returns the size of serialized value.
    ///
    /// This doesn't include tag or name of key.
    pub fn get_serialized_value_len(&self) -> usize {
        4 // total bytes of the document
            + self.data.iter().enumerate().map(|(index, value)| {
            1 // tag byte
                + super::utils::dec_len(index)
                + value.get_serialized_value_len()
        }).sum::<usize>()
            + 1 // trailing 0 tag
    }

    /// Writes the value to the BsonWriter
    pub fn write_value<W: BsonWriter>(&self, w: &mut W) -> Result<(), W::Error> {
        let len = self.get_serialized_value_len();
        let len = i32::try_from(len).map_err(|_| W::when_too_large(len))?;

        w.write_bytes(&len.to_be_bytes())?;

        for (index, value) in self.data.iter().enumerate() {
            w.write_bytes(&[value.ty().bson_tag()])?;
            super::utils::write_c_string(w, &index.to_string())?;
            value.write_value(w)?;
        }

        w.write_bytes(&[0])?;
        Ok(())
    }
}

impl Debug for Array {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        Debug::fmt(&self.data[..], formatter)
    }
}

impl From<Vec<Value>> for Array {
    fn from(data: Vec<Value>) -> Array {
        Array { data }
    }
}

impl<const L: usize> From<[Value; L]> for Array {
    fn from(data: [Value; L]) -> Array {
        Self::from(Vec::from(data))
    }
}

impl From<&[Value]> for Array {
    fn from(data: &[Value]) -> Array {
        Self::from(Vec::from(data))
    }
}

impl<'a, T> From<&'a [T]> for Array
where
    Value: From<&'a T>,
{
    fn from(data: &'a [T]) -> Array {
        Self::from(data.iter().map(Into::into).collect::<Vec<Value>>())
    }
}

impl<T: Into<Value>> FromIterator<T> for Array {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        iter.into_iter()
            .map(Into::into)
            .collect::<Vec<Value>>()
            .into()
    }
}

impl IntoIterator for Array {
    type Item = Value;
    type IntoIter = vec::IntoIter<Value>;
    fn into_iter(self) -> Self::IntoIter {
        self.data.into_iter()
    }
}
