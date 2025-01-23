mod base_page;
mod collection_page;
mod header_page;

use crate::engine::PageBuffer;
use crate::Result;
pub use base_page::*;
pub use collection_page::*;
pub use header_page::*;
use std::ops::{Deref, DerefMut};

pub(crate) trait Page: Deref<Target = &BasePage> + DerefMut + Sized {
    fn load(buffer: Box<PageBuffer>) -> Result<Self>;
    fn new(buffer: Box<PageBuffer>, page_id: u32) -> Self;
}
