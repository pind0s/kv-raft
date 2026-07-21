use crate::error::{Error, Result};
use crate::pages::PageId;
use crate::pages::file_handler::FileHandler;
use crate::pages::page::Page;
use crate::pages::page_cache::PageCache;
use std::sync::Arc;

pub(crate) struct PageIO {
    file_handler: Arc<FileHandler>,
    page_cache: PageCache,
    page_size: u64,
}

impl PageIO {
    pub(crate) fn new(file_handler: Arc<FileHandler>, page_size: u64, cache_capacity: usize) -> Self {
        PageIO {
            file_handler,
            page_cache: PageCache::new(cache_capacity),
            page_size,
        }
    }

    pub(crate) fn read_page(&self, page_id: PageId) -> Result<Page> {
        if let Some(data) = self.page_cache.get(page_id) {
            return Ok(Page::new(page_id, data));
        }

        let mut data = vec![0; self.page_size as usize];
        self.file_handler
            .read_at(page_id * self.page_size, &mut data)?;
        let data: Arc<[u8]> = data.into();
        self.page_cache.insert(page_id, data.clone());
        Ok(Page::new(page_id, data))
    }

    pub(crate) fn write_page(&self, page: Page) -> Result<()> {
        let page_id = page.id();
        let data = page.into_arc();
        if data.len() as u64 != self.page_size {
            return Err(Error::DatabaseError(format!(
                "page {page_id} has size {}, expected {}",
                data.len(),
                self.page_size
            )));
        }
        self.file_handler
            .write_at_page(page_id, self.page_size, data.as_ref())?;

        self.page_cache.insert(page_id, data);
        Ok(())
    }

    pub(crate) fn remove_page(&self, page_id: PageId) {
        self.page_cache.remove(page_id);
    }

    pub(crate) fn sync_data(&self) -> Result<()> {
        self.file_handler.sync_data()
    }

    pub(crate) fn page_size(&self) -> u64 {
        self.page_size
    }
}
