use crate::utils::Shared;
use std::collections::HashSet;
use std::hash::Hash;
use std::ops::{AsyncFnOnce, Deref, DerefMut};

/// Safe Partial Borrow Helper
///
/// This class is a helper for partial borrow that collision can be avoided by the Key.
/// If double borrow is being happening, it will panic.
// TODO? support read only borrows
pub(crate) struct PartialBorrower<TargetRef, Key> {
    target: TargetRef,
    borrowed: Shared<HashSet<Key>>,
}

/// # Safety
/// Extended should have same lifetime as 'target
pub(crate) unsafe trait ExtendLifetime<'target> {
    type Extended;
    unsafe fn extend_lifetime(self) -> Self::Extended;
}

impl<TargetRef, Key: Hash + Eq + Copy> PartialBorrower<TargetRef, Key> {
    pub fn new(target: TargetRef) -> Self {
        Self {
            target,
            borrowed: Shared::new(HashSet::new()),
        }
    }

    pub fn target(&self) -> &TargetRef {
        &self.target
    }

    pub fn target_mut(&mut self) -> &mut TargetRef {
        &mut self.target
    }

    pub async unsafe fn try_create_borrow_async<'s, 'r, ShortLives, Extended, Error>(
        &'s mut self,
        new: impl AsyncFnOnce(&'s mut TargetRef) -> Result<ShortLives, Error>,
        key: impl FnOnce(&ShortLives) -> Key,
    ) -> Result<PartialRefMut<Extended, Key>, Error>
    where
        ShortLives: ExtendLifetime<'r, Extended = Extended>,
        TargetRef: 'r,
    {
        let value: ShortLives = new(&mut self.target).await?;
        let key = key(&value);
        self.borrowed.borrow_mut().insert(key);
        Ok(PartialRefMut {
            value: unsafe { ShortLives::extend_lifetime(value) },
            key,
            borrowed: self.borrowed.clone(),
        })
    }

    pub async unsafe fn try_get_borrow_async<'s, 'r, ShortLives, Extended, Error>(
        &'s mut self,
        key: Key,
        get: impl AsyncFnOnce(&'s mut TargetRef, &Key) -> Result<ShortLives, Error>,
    ) -> Result<PartialRefMut<Extended, Key>, Error>
    where
        ShortLives: ExtendLifetime<'r, Extended = Extended>,
        TargetRef: 'r,
    {
        assert!(!self.borrowed.borrow().contains(&key), "double reference"); // TODO: make non-hard error?

        let value: ShortLives = get(&mut self.target, &key).await?;
        self.borrowed.borrow_mut().insert(key);
        Ok(PartialRefMut {
            value: unsafe { ShortLives::extend_lifetime(value) },
            key,
            borrowed: self.borrowed.clone(),
        })
    }

    pub async unsafe fn try_delete_borrow_async<'s, Result>(
        &'s mut self,
        key: Key,
        delete: impl AsyncFnOnce(&'s mut TargetRef, &Key) -> Result,
    ) -> Result {
        assert!(
            !self.borrowed.borrow().contains(&key),
            "removing using reference"
        ); // TODO: make non-hard error?
        delete(&mut self.target, &key).await
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
