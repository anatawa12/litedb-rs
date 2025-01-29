use super::{BsonReader, BsonWriter, ParseError, Value};
use crate::utils::{CaseInsensitiveStr, CaseInsensitiveString};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{Debug, Formatter};

/// The bson document.
///
/// Since bson in litedb uses case-insensitive key comparison, this implementation does so.
#[derive(Clone, PartialEq)]
pub struct Document {
    inner: HashMap<CaseInsensitiveString, Value>,
}

impl Default for Document {
    fn default() -> Self {
        Self::new()
    }
}

impl Document {
    pub fn new() -> Document {
        Self {
            inner: HashMap::new(),
        }
    }

    /// Adds value to document.
    ///
    /// ### Panics
    /// This function will panic if the key contains null char (`'\0'`)
    pub fn insert(&mut self, key: String, value: impl Into<Value>) {
        check_key(&key);
        self.inner.insert(key.into(), value.into());
    }

    /// Gets the value with `key`.
    pub fn get(&self, key: impl AsRef<str>) -> Option<&Value> {
        self.inner.get(CaseInsensitiveStr::new(key.as_ref()))
    }

    pub fn get_mut(&mut self, key: impl AsRef<str>) -> Option<&mut Value> {
        self.inner.get_mut(CaseInsensitiveStr::new(key.as_ref()))
    }

    pub fn contains_key(&self, key: impl AsRef<str>) -> bool {
        self.inner
            .contains_key(CaseInsensitiveStr::new(key.as_ref()))
    }

    pub fn remove(&mut self, key: impl AsRef<str>) -> Option<Value> {
        self.inner.remove(CaseInsensitiveStr::new(key.as_ref()))
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.inner.iter().map(|(k, v)| (k.as_str(), v))
    }
}

impl Document {
    /// Returns the size of serialized value.
    ///
    /// This doesn't include tag or name of key.
    pub fn get_serialized_value_len(&self) -> usize {
        4 // total bytes of the document
            + self.inner.iter().map(|(key, value)| {
            1 // tag byte
                + (key.len() + 1) // cstring for key
                + value.get_serialized_value_len()
        }).sum::<usize>()
            + 1 // trailing 0 tag
    }

    /// Writes the value to the BsonWriter
    pub fn write_value<W: BsonWriter>(&self, w: &mut W) -> Result<(), <W as BsonWriter>::Error> {
        let len = self.get_serialized_value_len();
        let len = i32::try_from(len).map_err(|_| W::when_too_large(len))?;

        w.write_bytes(&len.to_le_bytes())?;

        for (key, value) in &self.inner {
            w.write_bytes(&[value.ty().bson_tag() as u8])?;
            super::utils::write_c_string(w, key.as_str())?;
            value.write_value(w)?;
        }

        w.write_bytes(&[0])?;
        Ok(())
    }

    /// Parses the document
    pub fn parse_document<R: BsonReader>(r: &mut R) -> Result<Document, <R as BsonReader>::Error> {
        let result = Self::parse_document_inner(&mut super::de::LimitReader::new(r))?;
        if !r.is_end() {
            return Err(ParseError::RemainingDataInDocument.into());
        }
        Ok(result)
    }

    pub(super) fn parse_document_inner<R: BsonReader>(
        r: &mut super::de::LimitReader<R>,
    ) -> Result<Document, <R as BsonReader>::Error> {
        let r = super::de::limit_reader(r)?;

        let mut document = Self::new();

        while let Some((key, value)) = super::de::parse_element(r.reader)? {
            //document.inner.try_insert()
            match document.inner.entry(key.into()) {
                Entry::Occupied(e) => {
                    return Err(ParseError::DuplicatedKey(e.remove_entry().0.into()).into());
                }
                Entry::Vacant(e) => {
                    e.insert(value);
                }
            }
        }

        if !r.reader.is_end() {
            return Err(ParseError::RemainingDataInDocument.into());
        }

        Ok(document)
    }
}

#[track_caller]
fn check_key(key: &String) {
    assert!(
        !key.as_bytes().contains(&0),
        "Key contains null char, which is disallowed for bson"
    );
}

impl Debug for Document {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.inner, f)
    }
}
