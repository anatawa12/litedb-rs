use super::Value;
use std::borrow::Borrow;
use std::collections::HashMap;
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

    pub fn len(&self) -> usize {
        self.inner.len()
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

impl Eq for CaseInsensitiveString {
}
