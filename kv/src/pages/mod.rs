pub(crate) mod checksum;
pub(crate) mod file_handler;
pub(crate) mod header;
pub(crate) mod page;
mod page_allocator;
mod page_cache;
pub(crate) mod page_io;
pub(crate) mod page_manager;

pub(crate) type PageId = u64;
pub(crate) const HEADER_PAGE_ID: PageId = 0;
pub(crate) const INITIAL_ROOT_PAGE_ID: PageId = 1;
pub(crate) const DEFAULT_PAGE_SIZE: usize = 4096;
pub(crate) const DEFAULT_PAGE_SIZE_U64: u64 = DEFAULT_PAGE_SIZE as u64;
pub(crate) const MIN_PAGE_SIZE: u64 = DEFAULT_PAGE_SIZE_U64;
