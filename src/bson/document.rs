use super::{BsonReader, BsonWriter, ParseError, Value};
use std::borrow::Borrow;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};

/// The bson document.
///
/// Since bson in litedb uses case-insensitive key comparison, this implementation does so.
#[derive(Clone, PartialEq)]
pub struct Document {
    inner: HashMap<CaseInsensitiveString, Value>,
}

#[repr(transparent)]
struct CaseInsensitiveStr(str);
#[derive(Clone)]
struct CaseInsensitiveString(String);

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
        self.inner.insert(CaseInsensitiveString(key), value.into());
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
        self.inner.iter().map(|(k, v)| (k.0.as_ref(), v))
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
                + (key.0.len() + 1) // cstring for key
                + value.get_serialized_value_len()
        }).sum::<usize>()
            + 1 // trailing 0 tag
    }

    /// Writes the value to the BsonWriter
    pub fn write_value<W: BsonWriter>(&self, w: &mut W) -> Result<(), <W as BsonWriter>::Error> {
        let len = self.get_serialized_value_len();
        let len = i32::try_from(len).map_err(|_| W::when_too_large(len))?;

        w.write_bytes(&len.to_be_bytes())?;

        for (key, value) in &self.inner {
            w.write_bytes(&[value.ty().bson_tag() as u8])?;
            super::utils::write_c_string(w, &key.0)?;
            value.write_value(w)?;
        }

        w.write_bytes(&[0])?;
        Ok(())
    }

    /// Parses the document
    pub fn parse_document<R: BsonReader>(r: &mut R) -> Result<Document, <R as BsonReader>::Error> {
        let result = Self::parse_document_inner(r)?;
        if !r.is_end() {
            return Err(ParseError::RemainingDataInDocument.into());
        }
        Ok(result)
    }
    pub(super) fn parse_document_inner<R: BsonReader>(
        r: &mut R,
    ) -> Result<Document, <R as BsonReader>::Error> {
        let mut r = super::de::limit_reader(r)?;

        let mut document = Self::new();

        while let Some((key, value)) = super::de::parse_element(&mut r)? {
            //document.inner.try_insert()
            match document.inner.entry(CaseInsensitiveString(key)) {
                Entry::Occupied(e) => {
                    return Err(ParseError::DuplicatedKey(e.remove_entry().0.0).into());
                }
                Entry::Vacant(e) => {
                    e.insert(value);
                }
            }
        }

        if !r.is_end() {
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

impl Debug for CaseInsensitiveString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl CaseInsensitiveStr {
    fn new(s: &str) -> &CaseInsensitiveStr {
        // SAFETY: CaseInsensitiveStr is transparent to str
        unsafe { &*(s as *const str as *const CaseInsensitiveStr) }
    }
}

impl Hash for CaseInsensitiveStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for c in self.0.chars() {
            for c in c.to_uppercase() {
                state.write_u32(c as u32);
            }
        }
    }
}

impl PartialEq for CaseInsensitiveStr {
    fn eq(&self, other: &Self) -> bool {
        let this = self.0.chars().flat_map(char::to_uppercase);
        let other = other.0.chars().flat_map(char::to_uppercase);
        this.eq(other)
    }
}

impl Eq for CaseInsensitiveStr {}

// basically string implementation is based on CaseInsensitiveStr
impl Borrow<CaseInsensitiveStr> for CaseInsensitiveString {
    fn borrow(&self) -> &CaseInsensitiveStr {
        self.as_ref()
    }
}

impl AsRef<CaseInsensitiveStr> for CaseInsensitiveString {
    fn as_ref(&self) -> &CaseInsensitiveStr {
        CaseInsensitiveStr::new(&self.0)
    }
}

impl Hash for CaseInsensitiveString {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state)
    }
}

impl PartialEq for CaseInsensitiveString {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl Eq for CaseInsensitiveString {}
