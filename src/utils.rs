use crate::Error;
use crate::bson;
use crate::bson::TotalOrd;
use crate::engine::{BufferReader, BufferWriter, IndexNode, MAX_INDEX_KEY_LENGTH, PageAddress};
use bson::BsonType;
use either::Either;
use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{Deref, DerefMut, Neg};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

// TODO: Implement the CompareOptions struct
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompareOptions(pub i32);

impl CompareOptions {
    pub const IGNORE_CASE: CompareOptions = CompareOptions(1);
    pub const IGNORE_KANA_TYPE: CompareOptions = CompareOptions(8);
    pub const IGNORE_NON_SPACE: CompareOptions = CompareOptions(2);
    pub const IGNORE_SYMBOLS: CompareOptions = CompareOptions(4);
    pub const IGNORE_WIDTH: CompareOptions = CompareOptions(16);
    pub const ORDINAL: CompareOptions = CompareOptions(1073741824);
    pub const STRING_SORT: CompareOptions = CompareOptions(536870912);
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Collation {
    pub lcid: i32,
    pub sort_options: CompareOptions,
}

impl Default for Collation {
    fn default() -> Self {
        Collation {
            lcid: 127, // invariant
            sort_options: CompareOptions::IGNORE_CASE,
        }
    }
}

const _: () = {
    // static aserts
    assert!(size_of::<Collation>() == size_of::<u64>());
};

impl Collation {
    pub fn new(lcid: i32, sort_options: CompareOptions) -> Self {
        Collation { lcid, sort_options }
    }

    pub(crate) fn compare(&self, left: &bson::Value, right: &bson::Value) -> Ordering {
        use bson::Value::*;
        match (left, right) {
            (String(l), String(r)) => l.cmp(r), // TODO: compare with collation settings
            (l, r) => l.total_cmp(r),
        }
    }

    //    pub(crate) fn sql_like(&self, left: &str, right: &str) -> bool {
    //    }

    pub(crate) fn to_u64(self) -> u64 {
        unsafe { std::mem::transmute::<Self, u64>(self) }
    }

    pub(crate) fn from_u64(from: u64) -> Self {
        unsafe { std::mem::transmute::<u64, Self>(from) }
    }
}

#[repr(transparent)]
pub struct BufferSlice {
    buffer: [u8],
}

impl BufferSlice {
    pub fn new(buffer: &[u8]) -> &Self {
        unsafe { &*(buffer as *const _ as *const Self) }
    }

    pub fn new_mut(buffer: &mut [u8]) -> &mut Self {
        unsafe { &mut *(buffer as *mut _ as *mut Self) }
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }
}

impl BufferSlice {
    pub fn read_bool(&self, offset: usize) -> bool {
        self.buffer[offset] != 0
    }

    pub fn read_byte(&self, offset: usize) -> u8 {
        self.buffer[offset]
    }

    pub fn read_u8(&self, offset: usize) -> u8 {
        self.buffer[offset]
    }

    #[allow(dead_code)]
    pub fn read_i16(&self, offset: usize) -> i16 {
        i16::from_le_bytes(self.buffer[offset..][..2].try_into().unwrap())
    }

    pub fn read_i32(&self, offset: usize) -> i32 {
        i32::from_le_bytes(self.buffer[offset..][..4].try_into().unwrap())
    }

    pub fn read_i64(&self, offset: usize) -> i64 {
        i64::from_le_bytes(self.buffer[offset..][..8].try_into().unwrap())
    }

    pub fn read_u16(&self, offset: usize) -> u16 {
        u16::from_le_bytes(self.buffer[offset..][..2].try_into().unwrap())
    }

    pub fn read_u32(&self, offset: usize) -> u32 {
        u32::from_le_bytes(self.buffer[offset..][..4].try_into().unwrap())
    }

    pub fn read_u64(&self, offset: usize) -> u64 {
        u64::from_le_bytes(self.buffer[offset..][..8].try_into().unwrap())
    }

    pub fn read_f64(&self, offset: usize) -> f64 {
        f64::from_le_bytes(self.buffer[offset..][..8].try_into().unwrap())
    }

    pub fn read_bytes(&self, offset: usize, length: usize) -> &[u8] {
        &self.buffer[offset..][..length]
    }

    pub fn read_string(&self, offset: usize, length: usize) -> crate::Result<&str> {
        std::str::from_utf8(self.read_bytes(offset, length)).map_err(Error::err)
    }

    pub fn read_date_time(&self, offset: usize) -> crate::Result<bson::DateTime> {
        bson::DateTime::from_ticks(self.read_u64(offset)).ok_or_else(Error::datetime_overflow)
    }

    pub fn read_page_address(&self, offset: usize) -> PageAddress {
        PageAddress::new(self.read_u32(offset), self.read_byte(offset + 4))
    }

    pub fn read_index_key(&self, offset: usize) -> crate::Result<bson::Value> {
        // extended length: use two bytes for type and length pair
        let type_byte = self.read_byte(offset);
        let type_ = BsonType::from_u8(type_byte & 0b0011_1111).ok_or_else(Error::invalid_bson)?;

        // RustChange: no out of bounds are allowed so we check for length byte before access
        let length = if matches!(type_, BsonType::Binary | BsonType::String) {
            let length_byte = self.read_byte(offset + 1);
            ((type_byte as u16 & 0b1100_0000) << 2) | (length_byte as u16)
        } else {
            0
        };

        let offset = offset + 1; // length byte might not be used

        let value = match type_ {
            BsonType::MinValue => bson::Value::MinValue,
            BsonType::Null => bson::Value::Null,
            BsonType::Int32 => bson::Value::Int32(self.read_i32(offset)),
            BsonType::Int64 => bson::Value::Int64(self.read_i64(offset)),
            BsonType::Double => bson::Value::Double(self.read_f64(offset)),
            BsonType::Decimal => bson::Value::Decimal(
                bson::Decimal128::from_bytes(self.read_bytes(offset, 16).try_into().unwrap())
                    .ok_or_else(Error::invalid_bson)?,
            ), // known to be 16 bytes
            BsonType::String => {
                let offset = offset + 1; // using length byte
                bson::Value::String(self.read_string(offset, length as usize)?.to_owned())
            }
            BsonType::Document => bson::Value::Document(
                BufferReader::single(self.slice(offset, self.len() - offset)).read_document()?,
            ),
            BsonType::Array => bson::Value::Array(
                BufferReader::single(self.slice(offset, self.len() - offset)).read_array()?,
            ),
            BsonType::Binary => {
                let length = length + 1; // using length byte
                bson::Value::Binary(bson::Binary::new(
                    self.read_bytes(offset, length as usize).to_vec(),
                ))
            }
            BsonType::ObjectId => bson::Value::ObjectId(bson::ObjectId::from_bytes(
                self.read_bytes(offset, 12).try_into().unwrap(),
            )),
            BsonType::Guid => bson::Value::Guid(bson::Guid::from_bytes(
                self.read_bytes(offset, 16).try_into().unwrap(),
            )),
            BsonType::Boolean => bson::Value::Boolean(self.read_bool(offset)),
            BsonType::DateTime => bson::Value::DateTime(self.read_date_time(offset)?),
            BsonType::MaxValue => bson::Value::MaxValue,
        };

        Ok(value)
    }

    pub(crate) fn slice(&self, offset: usize, count: usize) -> &Self {
        Self::new(&self.buffer[offset..][..count])
    }

    pub fn clear(&mut self, offset: usize, count: usize) {
        self.buffer[offset..][..count].fill(0);
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }
}

// writers
impl BufferSlice {
    pub fn write_bool(&mut self, offset: usize, value: bool) {
        self.buffer[offset] = value as u8;
    }

    pub fn write_byte(&mut self, offset: usize, value: u8) {
        self.buffer[offset] = value;
    }

    #[allow(dead_code)]
    pub fn write_i16(&mut self, offset: usize, value: i16) {
        self.buffer[offset..][..2].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_i32(&mut self, offset: usize, value: i32) {
        self.buffer[offset..][..4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_i64(&mut self, offset: usize, value: i64) {
        self.buffer[offset..][..8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_u8(&mut self, offset: usize, value: u8) {
        self.buffer[offset] = value;
    }

    pub fn write_u16(&mut self, offset: usize, value: u16) {
        self.buffer[offset..][..2].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_u32(&mut self, offset: usize, value: u32) {
        self.buffer[offset..][..4].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_u64(&mut self, offset: usize, value: u64) {
        self.buffer[offset..][..8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_f64(&mut self, offset: usize, value: f64) {
        self.buffer[offset..][..8].copy_from_slice(&value.to_le_bytes());
    }

    pub fn write_bytes(&mut self, offset: usize, value: &[u8]) {
        self.buffer[offset..][..value.len()].copy_from_slice(value);
    }

    #[allow(dead_code)]
    pub fn write_string(&mut self, offset: usize, value: &str) {
        self.write_bytes(offset, value.as_bytes());
    }

    pub fn write_date_time(&mut self, offset: usize, value: bson::DateTime) {
        self.write_u64(offset, value.ticks());
    }

    pub fn write_page_address(&mut self, offset: usize, value: PageAddress) {
        self.write_u32(offset, value.page_id());
        self.write_u8(offset + 4, value.index());
    }

    pub fn write_index_key(&mut self, offset: usize, value: &bson::Value) {
        debug_assert!(IndexNode::get_key_length(value, true) <= MAX_INDEX_KEY_LENGTH);

        fn make_extended_length(tag: BsonType, length: usize) -> [u8; 2] {
            assert!(length <= 1024);

            let type_ = tag as u8;
            let length_lsb = (length & 0xFF) as u8;
            let length_msb = ((length & 0x300) >> 2) as u8;
            [type_ | length_msb, length_lsb]
        }

        match value {
            // variable length values
            bson::Value::Binary(bin) => {
                self.write_bytes(
                    offset,
                    &make_extended_length(BsonType::Binary, bin.bytes().len()),
                );
                self.write_bytes(offset + 2, bin.bytes());
            }
            bson::Value::String(str) => {
                self.write_bytes(offset, &make_extended_length(BsonType::String, str.len()));
                self.write_bytes(offset + 2, str.as_bytes());
            }

            // single tag values
            bson::Value::Null => self.write_u8(offset, BsonType::Null as u8),
            bson::Value::MaxValue => self.write_u8(offset, BsonType::MaxValue as u8),
            bson::Value::MinValue => self.write_u8(offset, BsonType::MinValue as u8),

            // simple values
            bson::Value::Int32(v) => {
                self.write_u8(offset, BsonType::Int32 as u8);
                self.write_i32(offset + 1, *v);
            }
            bson::Value::Int64(v) => {
                self.write_u8(offset, BsonType::Int64 as u8);
                self.write_i64(offset + 1, *v);
            }
            bson::Value::Double(d) => {
                self.write_u8(offset, BsonType::Double as u8);
                self.write_f64(offset + 1, *d);
            }
            bson::Value::Decimal(d) => {
                self.write_u8(offset, BsonType::Decimal as u8);
                self.write_bytes(offset + 1, &d.bytes());
            }
            bson::Value::Boolean(b) => {
                self.write_u8(offset, BsonType::Boolean as u8);
                self.write_bool(offset + 1, *b);
            }
            &bson::Value::DateTime(v) => {
                self.write_u8(offset, BsonType::DateTime as u8);
                self.write_date_time(offset + 1, v);
            }

            bson::Value::Document(d) => {
                self.write_u8(offset, BsonType::Document as u8);
                BufferWriter::single(self.slice_mut(offset + 1, self.len() - offset - 1))
                    .write_document(d)
            }
            bson::Value::Array(a) => {
                self.write_u8(offset, BsonType::Array as u8);
                BufferWriter::single(self.slice_mut(offset + 1, self.len() - offset - 1))
                    .write_array(a)
            }

            bson::Value::ObjectId(oid) => {
                self.write_u8(offset, BsonType::ObjectId as u8);
                self.write_bytes(offset + 1, oid.as_bytes());
            }
            bson::Value::Guid(uuid) => {
                self.write_u8(offset, BsonType::Guid as u8);
                self.write_bytes(offset + 1, &uuid.to_bytes());
            }
        }
    }

    pub(crate) fn slice_mut(&mut self, offset: usize, count: usize) -> &mut Self {
        Self::new_mut(&mut self.buffer[offset..][..count])
    }

    #[allow(dead_code)]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.buffer
    }
}

#[derive(Debug, Eq, PartialEq, Copy, Clone)]
#[repr(i8)]
pub(crate) enum Order {
    Ascending = 1,
    Descending = -1,
}

impl PartialEq<Ordering> for Order {
    fn eq(&self, other: &Ordering) -> bool {
        *self as i8 == *other as i8
    }
}

impl Neg for Order {
    type Output = Self;

    fn neg(self) -> Self::Output {
        match self {
            Order::Ascending => Order::Descending,
            Order::Descending => Order::Ascending,
        }
    }
}

/// The wrapper struct for Arc<RwLock<T>>
///
/// We may extend to Arc<Mutex<T>> in the future
pub(crate) struct Shared<T> {
    inner: Arc<RwLock<T>>,
}

impl<T> Shared<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    pub fn borrow(&self) -> Ref<T> {
        Ref {
            guard: self.inner.read().unwrap(),
        }
    }

    pub fn borrow_mut(&self) -> RefMut<T> {
        RefMut {
            guard: self.inner.write().unwrap(),
        }
    }
}

pub(crate) struct Ref<'a, T> {
    guard: RwLockReadGuard<'a, T>,
}

pub(crate) struct RefMut<'a, T> {
    guard: RwLockWriteGuard<'a, T>,
}

impl<T> Deref for Ref<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<T> Deref for RefMut<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl<T> DerefMut for RefMut<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

impl<T> Clone for Shared<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[repr(transparent)]
pub(crate) struct CaseInsensitiveStr(str);
#[derive(Clone)]
pub(crate) struct CaseInsensitiveString(pub String);

impl Debug for CaseInsensitiveString {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self.0, f)
    }
}

impl CaseInsensitiveStr {
    pub(crate) fn new(s: &str) -> &CaseInsensitiveStr {
        // SAFETY: CaseInsensitiveStr is transparent to str
        unsafe { &*(s as *const str as *const CaseInsensitiveStr) }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl Hash for CaseInsensitiveStr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for c in self.0.chars().flat_map(char::to_upper_invariant) {
            state.write_u32(c as u32);
        }
    }
}

impl PartialEq for CaseInsensitiveStr {
    fn eq(&self, other: &Self) -> bool {
        let this = self.0.chars().flat_map(char::to_upper_invariant);
        let other = other.0.chars().flat_map(char::to_upper_invariant);
        this.eq(other)
    }
}

impl Eq for CaseInsensitiveStr {}

// basically string implementation is based on CaseInsensitiveStr
impl std::borrow::Borrow<CaseInsensitiveStr> for CaseInsensitiveString {
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

impl Deref for CaseInsensitiveString {
    type Target = CaseInsensitiveStr;

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl From<String> for CaseInsensitiveString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<CaseInsensitiveString> for String {
    fn from(value: CaseInsensitiveString) -> Self {
        value.0
    }
}

#[repr(transparent)]
#[derive(Clone)]
pub(super) struct OrdBsonValue<T>(pub T);

impl<T: std::borrow::Borrow<bson::Value>> Eq for OrdBsonValue<T> {}

impl<T: std::borrow::Borrow<bson::Value>> PartialEq<Self> for OrdBsonValue<T> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl<T: std::borrow::Borrow<bson::Value>> PartialOrd for OrdBsonValue<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: std::borrow::Borrow<bson::Value>> Ord for OrdBsonValue<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.borrow().total_cmp(other.0.borrow())
    }
}

pub(crate) trait CSharpCharUtils: Sized {
    fn internal_to_char(self) -> char;

    fn to_lower_invariant(self) -> ToLowerInvariant {
        let c = self.internal_to_char();
        if c == '\u{0131}' {
            ToLowerInvariant(Either::Left(true))
        } else {
            ToLowerInvariant(Either::Right(c.to_lowercase()))
        }
    }

    fn to_upper_invariant(self) -> ToUpperInvariant {
        let c = self.internal_to_char();
        if c == '\u{0130}' {
            ToUpperInvariant(Either::Left(true))
        } else {
            ToUpperInvariant(Either::Right(c.to_uppercase()))
        }
    }
}

impl CSharpCharUtils for char {
    fn internal_to_char(self) -> char {
        self
    }
}

pub(crate) struct ToLowerInvariant(Either<bool, std::char::ToLowercase>);

impl Iterator for ToLowerInvariant {
    type Item = char;

    fn next(&mut self) -> Option<Self::Item> {
        // On Windows with InvariantCulture, the LATIN SMALL LETTER DOTLESS I (U+0131)
        // capitalizes to itself, whereas with Rust it capitalizes to LATIN CAPITAL LETTER I (U+0049).
        // We special case it to match the Windows invariant behavior.
        match self.0 {
            Either::Left(ref mut to_consume) => {
                if *to_consume {
                    *to_consume = false;
                    Some('\u{0131}')
                } else {
                    None
                }
            }
            Either::Right(ref mut iter) => iter.next(),
        }
    }
}

pub(crate) struct ToUpperInvariant(Either<bool, std::char::ToUppercase>);

impl Iterator for ToUpperInvariant {
    type Item = char;

    fn next(&mut self) -> Option<Self::Item> {
        // On Windows with InvariantCulture, the LATIN SMALL LETTER DOTLESS I (U+0131)
        // capitalizes to itself, whereas with Rust it capitalizes to LATIN CAPITAL LETTER I (U+0049).
        // We special case it to match the Windows invariant behavior.
        match self.0 {
            Either::Left(ref mut to_consume) => {
                if *to_consume {
                    *to_consume = false;
                    Some('\u{0130}')
                } else {
                    None
                }
            }
            Either::Right(ref mut iter) => iter.next(),
        }
    }
}

#[allow(dead_code)]
pub(crate) trait CSharpStringUtils {
    // see https://github.com/dotnet/runtime/blob/ea63985c1cf56b07324f87c54acb4d49875fa360/src/native/libs/System.Globalization.Native/pal_casing.c
    fn internal_as_str(&self) -> &str;

    fn to_lower_invariant(&self) -> String {
        self.internal_as_str()
            .chars()
            .flat_map(char::to_lower_invariant)
            .collect()
    }
    fn to_upper_invariant(&self) -> String {
        // On Windows with InvariantCulture, the LATIN CAPITAL LETTER I WITH DOT ABOVE (U+0130)
        // lower cases to itself, whereas with Rust it lower cases to LATIN SMALL LETTER I (U+0069).
        // We special case it to match the Windows invariant behavior.
        self.internal_as_str()
            .chars()
            .flat_map(char::to_upper_invariant)
            .collect()
    }
}

impl CSharpStringUtils for str {
    fn internal_as_str(&self) -> &str {
        self
    }
}

pub(crate) trait StrExtension {
    fn as_str(&self) -> &str;
    fn is_word(&self) -> bool {
        self.as_str()
            .chars()
            .enumerate()
            .all(|(i, c)| is_word_char(c, i == 0))
    }
}

impl StrExtension for str {
    fn as_str(&self) -> &str {
        self
    }
}

impl StrExtension for String {
    fn as_str(&self) -> &str {
        self
    }
}

pub(crate) fn is_word_char(c: char, first: bool) -> bool {
    if first {
        is_letter(c) || c == '_' || c == '$'
    } else {
        is_letter_or_digit(c) || c == '_' || c == '$'
    }
}

fn is_letter(c: char) -> bool {
    use unicode_properties::*;
    matches!(
        c.general_category(),
        GeneralCategory::UppercaseLetter
            | GeneralCategory::LowercaseLetter
            | GeneralCategory::TitlecaseLetter
            | GeneralCategory::ModifierLetter
            | GeneralCategory::OtherLetter
    )
}

fn is_letter_or_digit(c: char) -> bool {
    use unicode_properties::*;
    matches!(
        c.general_category(),
        GeneralCategory::UppercaseLetter
            | GeneralCategory::LowercaseLetter
            | GeneralCategory::TitlecaseLetter
            | GeneralCategory::ModifierLetter
            | GeneralCategory::OtherLetter
            | GeneralCategory::DecimalNumber
    )
}

pub(crate) mod checker {
    pub(crate) fn dummy<T: Send>() -> T {
        unimplemented!()
    }
    pub(crate) fn check_sync_send<'a, T: Send + Sync + 'a>(_: T) {}
}
