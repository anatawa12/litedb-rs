use crate::utils::Shared;
use std::collections::HashSet;
use std::hash::Hash;
use std::ops::{AsyncFnOnce, Deref, DerefMut};

/// Safe Partial Borrow Helper
///
/// This class is a helper for partial borrow that collision can be avoided by the Key.
/// If double borrow is being happening, it will panic.
// TODO? support read only borrows
pub(crate) struct PartialBorrower<'target, Target, Key> {
    target: &'target mut Target,
    borrowed: Shared<HashSet<Key>>,
}

/// # Safety
/// Extended should have same lifetime as 'target
pub(crate) unsafe trait ExtendLifetime<'target> {
    type Extended;
    unsafe fn extend_lifetime(self) -> Self::Extended;
}

impl<'target, Target, Key: Hash + Eq + Copy> PartialBorrower<'target, Target, Key> {
    pub fn new(target: &'target mut Target) -> Self {
        Self {
            target,
            borrowed: Shared::new(HashSet::new()),
        }
    }

    pub fn target(&self) -> &Target {
        self.target
    }

    pub fn target_mut(&mut self) -> &mut Target {
        self.target
    }

    pub async unsafe fn try_create_borrow_async<'s, ShortLives, Extended, Error>(
        &'s mut self,
        new: impl AsyncFnOnce(&'s mut Target) -> Result<ShortLives, Error>,
        key: impl FnOnce(&ShortLives) -> Key,
    ) -> Result<PartialRefMut<Extended, Key>, Error>
    where
        ShortLives: ExtendLifetime<'target, Extended = Extended>,
    {
        let value: ShortLives = new(self.target).await?;
        let key = key(&value);
        self.borrowed.borrow_mut().insert(key);
        Ok(PartialRefMut {
            value: unsafe { ShortLives::extend_lifetime(value) },
            key,
            borrowed: self.borrowed.clone(),
        })
    }

    pub async unsafe fn try_get_borrow_async<'s, ShortLives, Extended, Error>(
        &'s mut self,
        key: Key,
        get: impl AsyncFnOnce(&'s mut Target, &Key) -> Result<ShortLives, Error>,
    ) -> Result<PartialRefMut<Extended, Key>, Error>
    where
        ShortLives: ExtendLifetime<'target, Extended = Extended>,
    {
        assert!(!self.borrowed.borrow().contains(&key), "double reference"); // TODO: make non-hard error?

        let value: ShortLives = get(self.target, &key).await?;
        self.borrowed.borrow_mut().insert(key);
        Ok(PartialRefMut {
            value: unsafe { ShortLives::extend_lifetime(value) },
            key,
            borrowed: self.borrowed.clone(),
        })
    }
}

into_non_drop! {
    pub(crate) struct PartialRefMut<Value, Key>
        where Key : Hash
        where Key : Eq
    {
        value: Value,
        key: Key,
        borrowed: Shared<HashSet<Key>>,
    }
}

impl<Value, Key: Hash + Eq> PartialRefMut<Value, Key> {
    pub fn into_value(self) -> Value {
        let destruct = self.into_destruct();
        destruct.value
    }
}

impl<Value, Key: Hash + Eq> PartialRefMut<Value, Key> {
    fn drop(&mut self) {
        self.borrowed.borrow_mut().remove(&self.key);
    }
}

impl<Value, Key: Hash + Eq> Deref for PartialRefMut<Value, Key> {
    type Target = Value;

    fn deref(&self) -> &Value {
        &self.value
    }
}

impl<Value, Key: Hash + Eq> DerefMut for PartialRefMut<Value, Key> {
    fn deref_mut(&mut self) -> &mut Value {
        &mut self.value
    }
}
