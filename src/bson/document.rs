use super::{BsonReader, BsonWriter, ParseError, Value};
use crate::utils::{CaseInsensitiveStr, CaseInsensitiveString};
use indexmap::IndexMap;
use indexmap::map::Entry as IndexMapEntry;
use indexmap::map::OccupiedEntry as IndexMapOccupiedEntry;
use indexmap::map::VacantEntry as IndexMapVacantEntry;
use std::fmt::{Debug, Formatter};
use std::ops::Index;

/// The bson document.
///
/// Since bson in litedb uses case-insensitive key comparison, this implementation does so.
#[derive(Clone, PartialEq)]
pub struct Document {
    inner: IndexMap<CaseInsensitiveString, Value>,
}

impl Default for Document {
    fn default() -> Self {
        Self::new()
    }
}

impl Document {
    pub fn new() -> Document {
        Self {
            inner: IndexMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Document {
        Self {
            inner: IndexMap::with_capacity(capacity),
        }
    }

    /// Adds value to document.
    ///
    /// ### Panics
    /// This function will panic if the key contains null char (`'\0'`)
    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        let key = key.into();
        check_key(&key);
        self.inner.insert(key.into(), value.into());
    }

    /// Gets the value with `key`, or None if not exists
    pub fn try_get(&self, key: impl AsRef<str>) -> Option<&Value> {
        self.inner.get(CaseInsensitiveStr::new(key.as_ref()))
    }

    /// Gets the value with `key`.
    pub fn get(&self, key: impl AsRef<str>) -> &Value {
        self.inner
            .get(CaseInsensitiveStr::new(key.as_ref()))
            .unwrap_or(&Value::Null)
    }

    pub fn get_mut(&mut self, key: impl AsRef<str>) -> Option<&mut Value> {
        self.inner.get_mut(CaseInsensitiveStr::new(key.as_ref()))
    }

    pub fn contains_key(&self, key: impl AsRef<str>) -> bool {
        self.inner
            .contains_key(CaseInsensitiveStr::new(key.as_ref()))
    }

    pub fn remove(&mut self, key: impl AsRef<str>) -> Option<Value> {
        self.inner
            .shift_remove(CaseInsensitiveStr::new(key.as_ref()))
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> + Clone {
        self.inner.iter().map(|(k, v)| (k.as_str(), v))
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub fn entry(&mut self, key: impl Into<String>) -> Entry<'_> {
        let key = key.into();
        check_key(&key);

        Entry::new(self.inner.entry(CaseInsensitiveString(key)))
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
            match document.entry(key) {
                Entry::Occupied(e) => {
                    return Err(ParseError::DuplicatedKey(e.remove_entry().0).into());
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

impl Index<&str> for Document {
    type Output = Value;

    fn index(&self, index: &str) -> &Self::Output {
        self.get(index)
    }
}

pub enum Entry<'a> {
    Occupied(OccupiedEntry<'a>),
    Vacant(VacantEntry<'a>),
}

impl<'a> Entry<'a> {
    fn new(inner: IndexMapEntry<'a, CaseInsensitiveString, Value>) -> Self {
        match inner {
            IndexMapEntry::Occupied(e) => Self::Occupied(OccupiedEntry::new(e)),
            IndexMapEntry::Vacant(e) => Self::Vacant(VacantEntry::new(e)),
        }
    }

    fn into_index(self) -> IndexMapEntry<'a, CaseInsensitiveString, Value> {
        match self {
            Entry::Occupied(e) => IndexMapEntry::Occupied(e.inner),
            Entry::Vacant(e) => IndexMapEntry::Vacant(e.inner),
        }
    }

    pub fn insert_entry(self, value: impl Into<Value>) -> OccupiedEntry<'a> {
        OccupiedEntry::new(self.into_index().insert_entry(value.into()))
    }

    pub fn or_insert(self, value: impl Into<Value>) -> &'a mut Value {
        self.into_index().or_insert(value.into())
    }

    pub fn or_insert_with<F: FnOnce() -> V, V: Into<Value>>(self, f: F) -> &'a mut Value {
        self.into_index().or_insert_with(|| f().into())
    }

    pub fn or_insert_with_key<F: FnOnce(&str) -> V, V: Into<Value>>(self, f: F) -> &'a mut Value {
        self.into_index()
            .or_insert_with_key(|k| f(k.as_str()).into())
    }

    pub fn key(&self) -> &str {
        match self {
            Entry::Occupied(e) => e.key(),
            Entry::Vacant(e) => e.key(),
        }
    }

    pub fn and_modify<F: FnOnce(&mut Value)>(self, f: F) -> Self {
        Self::new(self.into_index().and_modify(f))
    }

    pub fn document_or_replace(self) -> &'a mut Document {
        match self {
            Entry::Occupied(mut e) => {
                if !matches!(e.get(), Value::Document(_)) {
                    e.insert(Value::Document(Document::new()));
                }
                match e.into_mut() {
                    Value::Document(d) => d,
                    _ => unreachable!(),
                }
            }
            Entry::Vacant(e) => match e.insert(Value::Document(Document::new())) {
                Value::Document(d) => d,
                _ => unreachable!(),
            },
        }
    }
}

pub struct OccupiedEntry<'a> {
    inner: IndexMapOccupiedEntry<'a, CaseInsensitiveString, Value>,
}

impl<'a> OccupiedEntry<'a> {
    fn new(inner: IndexMapOccupiedEntry<'a, CaseInsensitiveString, Value>) -> Self {
        Self { inner }
    }

    pub fn key(&self) -> &str {
        self.inner.key().as_str()
    }

    pub fn get(&self) -> &Value {
        self.inner.get()
    }

    pub fn get_mut(&mut self) -> &mut Value {
        self.inner.get_mut()
    }

    pub fn into_mut(self) -> &'a mut Value {
        self.inner.into_mut()
    }

    pub fn insert(&mut self, value: impl Into<Value>) -> Value {
        self.inner.insert(value.into())
    }

    pub fn remove(self) -> Value {
        self.inner.shift_remove()
    }

    pub fn remove_entry(self) -> (String, Value) {
        let (k, v) = self.inner.shift_remove_entry();
        (k.0, v)
    }
}

pub struct VacantEntry<'a> {
    inner: IndexMapVacantEntry<'a, CaseInsensitiveString, Value>,
}

impl<'a> VacantEntry<'a> {
    fn new(inner: IndexMapVacantEntry<'a, CaseInsensitiveString, Value>) -> Self {
        Self { inner }
    }

    pub fn key(&self) -> &str {
        self.inner.key().as_str()
    }

    pub fn into_key(self) -> String {
        self.inner.into_key().0
    }

    pub fn insert(self, value: impl Into<Value>) -> &'a mut Value {
        self.inner.insert(value.into())
    }

    pub fn insert_entry(self, value: impl Into<Value>) -> OccupiedEntry<'a> {
        OccupiedEntry::new(self.inner.insert_entry(value.into()))
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
