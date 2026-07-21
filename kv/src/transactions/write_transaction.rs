use crate::btree::tree::Tree;
use crate::db::DatabaseState;
use crate::error::{Error, Result};
use crate::pages::PageId;
use crate::pages::header::CommitSlot;
use crate::pages::page::PageMut;
use crate::transactions::TransactionId;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::sync::{Arc, MutexGuard};

enum DirtyPageSlot {
    Available(PageMut),
    CheckedOut,
}

struct DirtyPageTracker {
    pages: RefCell<HashMap<PageId, DirtyPageSlot>>,
}

pub(crate) struct DirtyPage<'a> {
    page: Option<PageMut>,
    tracker: &'a DirtyPageTracker,
}

impl Drop for DirtyPage<'_> {
    fn drop(&mut self) {
        let page = self.page.take().unwrap();
        self.tracker.return_page(page);
    }
}

pub struct WriteTransaction<'a> {
    db_state: Arc<DatabaseState>,
    transaction: TransactionId,
    root_page: Cell<PageId>,
    root_page_checksum: Cell<u128>,
    dirty_pages: DirtyPageTracker,
    freed_pages: RefCell<Vec<PageId>>,
    failed: Cell<bool>,
    _writer_guard: MutexGuard<'a, ()>,
}

impl<'a> WriteTransaction<'a> {
    pub(crate) fn new(
        state: Arc<DatabaseState>,
        commit_slot: CommitSlot,
        writer_guard: MutexGuard<'a, ()>,
    ) -> Self {
        WriteTransaction {
            db_state: state,
            transaction: commit_slot.transaction_id.get() + 1,
            root_page: Cell::new(commit_slot.root_page_id.get()),
            root_page_checksum: Cell::new(commit_slot.root_page_checksum()),
            dirty_pages: DirtyPageTracker::new(),
            freed_pages: RefCell::new(Vec::new()),
            failed: Cell::new(false),
            _writer_guard: writer_guard,
        }
    }

    pub fn commit(self) -> Result<()> {
        if self.failed.get() {
            return Err(Error::TransactionFailed);
        }

        for page in self.dirty_pages.into_pages() {
            self.db_state.page_manager.write_page(page.freeze())?;
        }

        self.db_state.commit.publish(
            &self.db_state.page_manager,
            self.root_page.into_inner(),
            self.root_page_checksum.into_inner(),
            self.transaction,
        )?;

        for page_id in self.freed_pages.into_inner() {
            self.db_state
                .page_manager
                .free_page(page_id, self.transaction);
        }

        Ok(())
    }

    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.failed.get() {
            return Err(Error::TransactionFailed);
        }

        Tree::insert(self, key, value).inspect_err(|_| self.failed.set(true))
    }

    pub(crate) fn allocate_page(&self) -> Result<DirtyPage<'_>> {
        let page = self.db_state.page_manager.allocate_page()?;
        Ok(self.dirty_pages.add(page))
    }

    pub(crate) fn cow_page(&self, page_id: PageId) -> Result<DirtyPage<'_>> {
        if self.dirty_pages.contains(page_id) {
            return Ok(self.dirty_pages.check_out(page_id));
        }

        let old_page = self.db_state.page_manager.read_page(page_id)?;
        let mut page = self.db_state.page_manager.allocate_page()?;
        page.data_mut().copy_from_slice(old_page.data());
        self.freed_pages.borrow_mut().push(page_id);

        Ok(self.dirty_pages.add(page))
    }

    pub(crate) fn get_root_page(&self) -> PageId {
        self.root_page.get()
    }

    pub(crate) fn set_root_page(&self, page_id: PageId, checksum: u128) {
        self.root_page.set(page_id);
        self.root_page_checksum.set(checksum);
    }
}

impl DirtyPage<'_> {
    pub(crate) fn id(&self) -> PageId {
        self.page.as_ref().unwrap().id()
    }

    pub(crate) fn data(&self) -> &[u8] {
        self.page.as_ref().unwrap().data()
    }

    pub(crate) fn data_mut(&mut self) -> &mut [u8] {
        self.page.as_mut().unwrap().data_mut()
    }

    pub(crate) fn copy_page_from(&mut self, src: &[u8]) {
        self.data_mut().copy_from_slice(src);
    }
}

impl DirtyPageTracker {
    fn new() -> Self {
        Self {
            pages: RefCell::new(HashMap::new()),
        }
    }

    fn contains(&self, page_id: PageId) -> bool {
        self.pages.borrow().contains_key(&page_id)
    }

    fn add(&self, page: PageMut) -> DirtyPage<'_> {
        let page_id = page.id();
        let old_slot = self
            .pages
            .borrow_mut()
            .insert(page_id, DirtyPageSlot::CheckedOut);

        assert!(
            old_slot.is_none(),
            "page {page_id} is already dirty in this transaction"
        );

        DirtyPage {
            page: Some(page),
            tracker: self,
        }
    }

    fn check_out(&self, page_id: PageId) -> DirtyPage<'_> {
        let mut pages = self.pages.borrow_mut();
        let slot = pages
            .get_mut(&page_id)
            .unwrap_or_else(|| panic!("page {page_id} is not dirty"));

        let page = match std::mem::replace(slot, DirtyPageSlot::CheckedOut) {
            DirtyPageSlot::Available(page) => page,
            DirtyPageSlot::CheckedOut => panic!("dirty page {page_id} is already checked out"),
        };

        DirtyPage {
            page: Some(page),
            tracker: self,
        }
    }

    fn return_page(&self, page: PageMut) {
        let page_id = page.id();
        let mut pages = self.pages.borrow_mut();

        match pages.get_mut(&page_id) {
            Some(slot @ DirtyPageSlot::CheckedOut) => *slot = DirtyPageSlot::Available(page),
            Some(DirtyPageSlot::Available(_)) => {
                panic!("dirty page {page_id} was returned while it was not checked out")
            }
            None => panic!("dirty page {page_id} was removed while it was checked out"),
        }
    }

    fn into_pages(self) -> impl Iterator<Item = PageMut> {
        self.pages
            .into_inner()
            .into_values()
            .map(|slot| match slot {
                DirtyPageSlot::Available(page) => page,
                DirtyPageSlot::CheckedOut => {
                    panic!("cannot commit while a dirty page is checked out")
                }
            })
    }
}
