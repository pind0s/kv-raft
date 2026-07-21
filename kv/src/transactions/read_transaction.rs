use crate::btree::tree::Tree;
use crate::error::Result;
use crate::pages::PageId;
use crate::pages::page::Page;
use crate::pages::page_manager::PageManager;
use crate::transactions::TransactionId;
use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

// Keep track of active readers for each transaction id, this way we can know which pages are safe to reuse
type TransactionReaderCount = BTreeMap<TransactionId, u64>;
pub(crate) struct ReadTracker {
    active_readers: Arc<RwLock<TransactionReaderCount>>,
}

pub(crate) struct ReadGuard {
    transaction: TransactionId,
    active_readers: Arc<RwLock<TransactionReaderCount>>,
}

pub struct ReadTransaction {
    root_page_id: PageId,
    pages: Arc<PageManager>,
    _read_guard: ReadGuard,
}

impl ReadTransaction {
    pub(crate) fn new(root_page: PageId, pages: Arc<PageManager>, read_guard: ReadGuard) -> Self {
        ReadTransaction {
            root_page_id: root_page,
            pages,
            _read_guard: read_guard,
        }
    }

    pub fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Tree::get(self, key)
    }

    pub(crate) fn read_page(&self, page_id: PageId) -> Result<Page> {
        self.pages.read_page(page_id)
    }

    pub(crate) fn get_root_page_id(&self) -> PageId {
        self.root_page_id
    }
}

impl ReadTracker {
    pub(crate) fn new() -> Self {
        ReadTracker {
            active_readers: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub(crate) fn begin_read(&self, transaction: TransactionId) -> ReadGuard {
        {
            let mut readers = self.active_readers.write().unwrap();

            readers
                .entry(transaction)
                .and_modify(|count| *count += 1)
                .or_insert(1);
        }

        ReadGuard {
            transaction,
            active_readers: self.active_readers.clone(),
        }
    }

    pub(crate) fn oldest_active_transaction(&self) -> Option<TransactionId> {
        let readers = self.active_readers.read().unwrap();
        readers.keys().next().copied()
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        let mut readers = self.active_readers.write().unwrap();

        if let Some(count) = readers.get_mut(&self.transaction) {
            *count -= 1;

            if *count == 0 {
                readers.remove(&self.transaction);
            }
        }
    }
}
