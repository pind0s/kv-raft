use crate::error::Result;
use crate::pages::PageId;
use crate::pages::file_handler::FileHandler;
use crate::transactions::ReadTracker;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

// Preallocate disk space for pages in big batches
const PAGE_ALLOCATION_BATCH: PageId = 1024;

struct FreedPage {
    page_id: PageId,
    transaction_id: u64,
}

struct PageAllocatorState {
    pending_free_pages: VecDeque<FreedPage>,
    next_page_id: PageId,
    reserved_page_count: u64,
}

pub(super) struct PageAllocator {
    file_handler: Arc<FileHandler>,
    read_tracker: Arc<ReadTracker>, // so we can check if we can reuse a freed page
    page_size: u64,
    state: Mutex<PageAllocatorState>,
}

impl PageAllocator {
    pub(super) fn new(
        file_handler: Arc<FileHandler>,
        read_tracker: Arc<ReadTracker>,
        page_size: u64,
        next_page_id: PageId,
    ) -> Result<Self> {
        let reserved_page_count = file_handler.file_len()? / page_size;

        Ok(PageAllocator {
            file_handler,
            read_tracker,
            page_size,
            state: Mutex::new(PageAllocatorState {
                pending_free_pages: VecDeque::new(),
                next_page_id,
                reserved_page_count,
            }),
        })
    }

    pub(super) fn allocate_page_id(&self) -> Result<PageId> {
        let mut state = self.state.lock().unwrap();

        // check if we can reuse any freed pages
        if let Some(freed_page) = state.pending_free_pages.front() {
            let oldest_active_tx = self
                .read_tracker
                .oldest_active_transaction()
                .unwrap_or(u64::MAX);

            if freed_page.transaction_id < oldest_active_tx {
                let freed_page = state.pending_free_pages.pop_front().unwrap();
                return Ok(freed_page.page_id);
            }
        }

        let result = state.next_page_id;
        state.next_page_id += 1;

        if state.next_page_id > state.reserved_page_count {
            state.reserved_page_count = self
                .file_handler
                .preallocate_pages(PAGE_ALLOCATION_BATCH, self.page_size)?;
        }

        Ok(result)
    }

    pub(super) fn next_page_id(&self) -> PageId {
        self.state.lock().unwrap().next_page_id
    }

    pub(super) fn free_page(&self, page_id: PageId, transaction_id: u64) {
        let mut state = self.state.lock().unwrap();
        state.pending_free_pages.push_back(FreedPage {
            page_id,
            transaction_id,
        });
    }
}
