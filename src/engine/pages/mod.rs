mod base_page;
mod collection_page;
mod data_page;
mod header_page;
mod index_page;

use crate::Result;
use crate::engine::PageBuffer;
pub(crate) use base_page::*;
pub(crate) use collection_page::*;
pub(crate) use data_page::*;
pub(crate) use header_page::*;
pub(crate) use index_page::*;
use std::any::{Any, TypeId};
use std::pin::Pin;

pub(crate) trait Page: AsRef<BasePage> + AsMut<BasePage> + Any + Send + Sync {
    fn load(buffer: Box<PageBuffer>) -> Result<Self>
    where
        Self: Sized;
    fn new(buffer: Box<PageBuffer>, page_id: u32) -> Self
    where
        Self: Sized;
    fn update_buffer(self: Pin<&mut Self>) -> &PageBuffer;
    fn into_base(self: Pin<Box<Self>>) -> BasePage;
    fn as_base_mut(self: Pin<&mut Self>) -> Pin<&mut BasePage>;
}

// No Trait Upcasting yet.(trait_upcasting unstable) so I copied implementation of any here
#[allow(dead_code)]
impl dyn Page {
    #[inline]
    pub fn is<T: Page>(&self) -> bool {
        // Get `TypeId` of the type this function is instantiated with.
        let t = TypeId::of::<T>();

        // Get `TypeId` of the type in the trait object (`self`).
        let concrete = self.type_id();

        // Compare both `TypeId`s on equality.
        t == concrete
    }

    #[inline]
    pub fn downcast_mut<T: Page>(&mut self) -> Option<&mut T> {
        if self.is::<T>() {
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe { Some(self.downcast_mut_unchecked()) }
        } else {
            None
        }
    }

    #[inline]
    pub fn downcast_mut_pin<'a, T: Page>(self: Pin<&'a mut Self>) -> Option<Pin<&'a mut T>> {
        if self.is::<T>() {
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe { Some(self.downcast_mut_unchecked_pin()) }
        } else {
            None
        }
    }

    #[inline]
    pub unsafe fn downcast_mut_unchecked<T: Page>(&mut self) -> &mut T {
        debug_assert!(self.is::<T>());
        // SAFETY: caller guarantees that T is the correct type
        unsafe { &mut *(self as *mut dyn Page as *mut T) }
    }

    #[inline]
    pub unsafe fn downcast_mut_unchecked_pin<'a, T: Page>(
        self: Pin<&'a mut Self>,
    ) -> Pin<&'a mut T> {
        debug_assert!(self.is::<T>());
        // SAFETY: caller guarantees that T is the correct type
        unsafe { Pin::new_unchecked(&mut *(self.get_unchecked_mut() as *mut dyn Page as *mut T)) }
    }

    #[inline]
    pub fn downcast<T: Page>(self: Box<dyn Page>) -> std::result::Result<Box<T>, Box<Self>> {
        if self.is::<T>() {
            unsafe { Ok(self.downcast_unchecked::<T>()) }
        } else {
            Err(self)
        }
    }

    #[inline]
    pub fn downcast_pin<T: Page>(
        self: Pin<Box<dyn Page>>,
    ) -> std::result::Result<Pin<Box<T>>, Pin<Box<Self>>> {
        if self.is::<T>() {
            unsafe { Ok(self.downcast_unchecked_pin::<T>()) }
        } else {
            Err(self)
        }
    }

    #[inline]
    pub unsafe fn downcast_unchecked<T: Page>(self: Box<dyn Page>) -> Box<T> {
        debug_assert!(self.is::<T>());
        unsafe {
            let raw: *mut dyn Page = Box::into_raw(self);
            Box::from_raw(raw as *mut T)
        }
    }

    #[inline]
    pub unsafe fn downcast_unchecked_pin<T: Page>(self: Pin<Box<dyn Page>>) -> Pin<Box<T>> {
        debug_assert!(self.is::<T>());
        unsafe {
            let raw: *mut dyn Page = Box::into_raw(Pin::into_inner_unchecked(self));
            Pin::new_unchecked(Box::from_raw(raw as *mut T))
        }
    }
}
