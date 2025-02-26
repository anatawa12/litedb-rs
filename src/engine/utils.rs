use crate::utils::Shared;
use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::{AsyncFnOnce, Deref, DerefMut};

type BorrowChecker<Key> = Shared<HashMap<Key, borrow_status::BorrowStatus>>;

/// Safe Partial Borrow Helper
///
/// This class is a helper for partial borrow that collision can be avoided by the Key.
/// If double borrow is being happening, it will panic.
pub(crate) struct PartialBorrower<TargetRef, Key> {
    target: TargetRef,
    borrowed: BorrowChecker<Key>,
}

/// # Safety
/// Extended should have same lifetime as 'target
pub(crate) unsafe trait ExtendLifetime<'target> {
    type Extended;
    unsafe fn extend_lifetime(self) -> Self::Extended;
}

unsafe impl<'target, T: 'target> ExtendLifetime<'target> for &'_ mut T {
    type Extended = &'target mut T;
    unsafe fn extend_lifetime(self) -> Self::Extended {
        unsafe { std::mem::transmute::<Self, Self::Extended>(self) }
    }
}

impl<TargetRef, Key: Hash + Eq + Copy + Debug> PartialBorrower<TargetRef, Key> {
    pub fn new(target: TargetRef) -> Self {
        Self {
            target,
            borrowed: Shared::new(HashMap::new()),
        }
    }

    #[allow(dead_code)]
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
        self.borrowed
            .borrow_mut()
            .insert(key, borrow_status::BorrowStatus::new());
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
        if let Some(borrow) = self.borrowed.borrow().get(&key) {
            panic!("double reference with key {key:?}. previous reference is {borrow}");
        }

        let value: ShortLives = get(&mut self.target, &key).await?;
        self.borrowed
            .borrow_mut()
            .insert(key, borrow_status::BorrowStatus::new());
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
        if let Some(borrow) = self.borrowed.borrow().get(&key) {
            panic!("removing using reference {key:?}. previous reference is {borrow}");
        }

        delete(&mut self.target, &key).await
    }

    pub unsafe fn try_get_borrow<'s, 'r, ShortLives, Extended, Error>(
        &'s mut self,
        key: Key,
        get: impl FnOnce(&'s mut TargetRef, &Key) -> Result<ShortLives, Error>,
    ) -> Result<PartialRefMut<Extended, Key>, Error>
    where
        ShortLives: ExtendLifetime<'r, Extended = Extended>,
        TargetRef: 'r,
    {
        if let Some(borrow) = self.borrowed.borrow().get(&key) {
            panic!("double reference with key {key:?}. previous reference is {borrow}");
        }

        let value: ShortLives = get(&mut self.target, &key)?;
        self.borrowed
            .borrow_mut()
            .insert(key, borrow_status::BorrowStatus::new());
        Ok(PartialRefMut {
            value: unsafe { ShortLives::extend_lifetime(value) },
            key,
            borrowed: self.borrowed.clone(),
        })
    }

    #[allow(dead_code)]
    pub unsafe fn try_delete_borrow<Result>(
        &mut self,
        key: Key,
        delete: impl FnOnce(&mut TargetRef, &Key) -> Result,
    ) -> Result {
        if let Some(borrow) = self.borrowed.borrow().get(&key) {
            panic!("removing using reference {key:?}. previous reference is {borrow}");
        }

        delete(&mut self.target, &key)
    }
}

into_non_drop! {
    pub(crate) struct PartialRefMut<Value, Key>
        where
            Key : Hash,
            Key : Eq,
    {
        value: Value,
        key: Key,
        borrowed: BorrowChecker<Key>,
    }
}

impl<Value, Key: Hash + Eq> PartialRefMut<Value, Key> {
    pub fn into_value(self) -> Value {
        let destruct = self.into_destruct();
        if let Some(status) = destruct.borrowed.borrow_mut().get_mut(&destruct.key) {
            status.leak();
        }
        destruct.value
    }

    pub fn removing<R>(self, delete: impl FnOnce(Value) -> R) -> R {
        let destruct = self.into_destruct();
        // _defers is used to run self.borrowed.borrow_mut().remove(&self.key)
        // when exiting this function, even when panics
        let _defers = PartialRefMut {
            value: (),
            key: destruct.key,
            borrowed: destruct.borrowed,
        };
        delete(destruct.value)
    }
}

impl<Value, Key: Hash + Eq> Drop for PartialRefMut<Value, Key> {
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

mod borrow_status {
    use std::backtrace::Backtrace;
    use std::fmt::{Display, Formatter};

    pub(super) struct BorrowStatus {
        borrowed: Backtrace,
        leaked: Option<Backtrace>,
    }

    impl BorrowStatus {
        pub fn new() -> Self {
            Self {
                borrowed: Backtrace::capture(),
                leaked: None,
            }
        }

        pub fn leak(&mut self) {
            self.leaked = Some(Backtrace::capture());
        }
    }

    impl Display for BorrowStatus {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            if let Some(leaked) = &self.leaked {
                write!(f, "borrowed at {} and leaked at {}", self.borrowed, leaked)
            } else {
                write!(f, "borrowed at {} and still in track", self.borrowed)
            }
        }
    }
}
