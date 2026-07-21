use crate::error::Result;
use crate::pages::PageId;
use crate::pages::file_handler::FileHandler;
use crate::pages::page::{Page, PageMut};
use crate::pages::page_allocator::PageAllocator;
use crate::pages::page_io::PageIO;
use crate::transactions::ReadTracker;
use std::sync::Arc;

pub(crate) struct PageManager {
    page_reader: PageIO,
    page_allocator: PageAllocator,
}

impl PageManager {
    pub(crate) fn new(
        page_reader: PageIO,
        file_handler: Arc<FileHandler>,
        read_tracker: Arc<ReadTracker>,
        next_page_id: PageId,
    ) -> Result<Self> {
        let page_size = page_reader.page_size();
        Ok(PageManager {
            page_reader,
            page_allocator: PageAllocator::new(
                file_handler,
                read_tracker,
                page_size,
                next_page_id,
            )?,
        })
    }

    pub(crate) fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.page_reader.read_page(page_id)
    }

    pub(crate) fn write_page(&self, page: Page) -> Result<()> {
        self.page_reader.write_page(page)
    }

    pub(crate) fn allocate_page(&self) -> Result<PageMut> {
        let page_id = self.page_allocator.allocate_page_id()?;
        self.page_reader.remove_page(page_id);
        Ok(PageMut::zeroed(page_id, self.page_size() as usize))
    }

    pub(crate) fn free_page(&self, page_id: PageId, transaction_id: u64) {
        self.page_allocator.free_page(page_id, transaction_id);
    }

    pub(crate) fn next_page_id(&self) -> PageId {
        self.page_allocator.next_page_id()
    }

    pub(crate) fn sync_data(&self) -> Result<()> {
        self.page_reader.sync_data()
    }

    pub(crate) fn page_size(&self) -> u64 {
        self.page_reader.page_size()
    }
}
