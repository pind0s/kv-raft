use crate::btree::get_empty_leaf_page;
use crate::btree::tree::Tree;
use crate::error::{Error, Result};
use crate::pages::checksum::checksum_page;
use crate::pages::file_handler::FileHandler;
use crate::pages::header::{CommitSlot, DbHeader};
use crate::pages::page::Page;
use crate::pages::page_io::PageIO;
use crate::pages::page_manager::PageManager;
use crate::pages::{
    PageId, DEFAULT_PAGE_SIZE_U64, HEADER_PAGE_ID, INITIAL_ROOT_PAGE_ID, MIN_PAGE_SIZE,
};
use crate::transactions::{ReadTracker, ReadTransaction, TransactionId, WriteTransaction};
use bon::bon;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

const DEFAULT_CACHE_CAPACITY: usize = 1024 * 1024;

#[derive(Clone)]
pub struct Database {
    state: Arc<DatabaseState>,
}

pub(crate) struct DatabaseState {
    pub(crate) page_manager: Arc<PageManager>,
    pub(crate) read_tracker: Arc<ReadTracker>,
    pub(crate) commit: CommitState,
    pub(crate) writer_lock: Mutex<()>,
}

pub(crate) struct CommitState {
    header: Mutex<DbHeader>,
    snapshot: RwLock<(PageId, TransactionId)>,
}

#[bon]
impl Database {
    #[builder(finish_fn = build)]
    pub fn open_existing(
        #[builder(start_fn, into)] path: PathBuf,
        #[builder(default = DEFAULT_CACHE_CAPACITY)] cache_capacity: usize,
    ) -> Result<Self> {
        let file = Arc::new(FileHandler::open_existing(path)?);
        let header = Self::read_header(&file)?;
        let page_io = PageIO::new(file.clone(), header.page_size(), cache_capacity);
        let header = Self::recover_header(&file, &page_io, header)?;

        Self::from_parts(file, page_io, header)
    }

    #[builder(finish_fn = build)]
    pub fn open_new(
        #[builder(start_fn, into)] path: PathBuf,
        #[builder(default = DEFAULT_PAGE_SIZE_U64)] page_size: u64,
        #[builder(default = DEFAULT_CACHE_CAPACITY)] cache_capacity: usize,
    ) -> Result<Self> {
        Self::validate_page_size(page_size)?;

        let file = Arc::new(FileHandler::open_new(path)?);
        let page_io = PageIO::new(file.clone(), page_size, cache_capacity);
        let header = Self::new_file(&file, &page_io)?;

        Self::from_parts(file, page_io, header)
    }

    pub fn begin_read(&self) -> Result<ReadTransaction> {
        let (root_page_id, transaction_id) = *self.state.commit.snapshot.read().unwrap();
        let read_guard = self.state.read_tracker.begin_read(transaction_id);

        Ok(ReadTransaction::new(
            root_page_id,
            self.state.page_manager.clone(),
            read_guard,
        ))
    }

    pub fn begin_write(&self) -> Result<WriteTransaction<'_>> {
        let writer_guard = self.state.writer_lock.lock().expect("writer lock poisoned");
        let state = self.state.clone();
        let commit_slot = self.state.commit.active_slot();

        Ok(WriteTransaction::new(state, commit_slot, writer_guard))
    }

    fn from_parts(file: Arc<FileHandler>, page_io: PageIO, header: DbHeader) -> Result<Self> {
        let active_slot = header.active_slot();
        let read_tracker = Arc::new(ReadTracker::new());
        let page_manager = Arc::new(PageManager::new(
            page_io,
            file,
            read_tracker.clone(),
            active_slot.next_page_id.into(),
        )?);

        Ok(Database {
            state: Arc::new(DatabaseState {
                page_manager,
                read_tracker,
                commit: CommitState::new(header),
                writer_lock: Mutex::new(()),
            }),
        })
    }

    fn new_file(file: &FileHandler, page_io: &PageIO) -> Result<DbHeader> {
        let empty_leaf = get_empty_leaf_page(page_io.page_size() as usize);
        let header = DbHeader::initial(page_io.page_size(), checksum_page(&empty_leaf));
        let next_page_id = header.active_slot().next_page_id;

        file.preallocate_pages(next_page_id.get(), page_io.page_size())?;

        page_io.write_page(Page::new(HEADER_PAGE_ID, header.encode_page().into()))?;
        page_io.write_page(Page::new(INITIAL_ROOT_PAGE_ID, empty_leaf.into()))?;

        Ok(header)
    }

    fn recover_header(
        file: &FileHandler,
        page_io: &PageIO,
        mut header: DbHeader,
    ) -> Result<DbHeader> {
        if file.file_len()? < header.page_size() {
            return Err(Error::DatabaseError(
                "Database file size is less than page size".to_string(),
            ));
        }

        let active_slot = header.active_slot();
        match Tree::verify_pages(
            page_io,
            active_slot.root_page_id.get(),
            active_slot.root_page_checksum(),
        ) {
            Ok(()) => return Ok(header),
            Err(Error::DatabaseError(_)) => {}
            Err(err) => return Err(err),
        }

        let inactive_slot = header.inactive_slot();
        Tree::verify_pages(
            page_io,
            inactive_slot.root_page_id.get(),
            inactive_slot.root_page_checksum(),
        )?;

        header.switch_to_inactive_slot();

        Ok(header)
    }

    fn read_header(file: &FileHandler) -> Result<DbHeader> {
        if file.file_len()? < size_of::<DbHeader>() as u64 {
            return Err(Error::DatabaseError(
                "Database file size is less than header size".to_string(),
            ));
        }

        let mut header_page = vec![0; size_of::<DbHeader>()];
        file.read_at(HEADER_PAGE_ID, &mut header_page)?;
        let header = DbHeader::parse(&header_page)?;
        Self::validate_page_size(header.page_size())?;
        Ok(header)
    }

    fn validate_page_size(page_size: u64) -> Result<()> {
        if page_size < MIN_PAGE_SIZE {
            return Err(Error::DatabaseError(format!(
                "page size must be at least {MIN_PAGE_SIZE}, got {page_size}"
            )));
        }

        Ok(())
    }
}

impl CommitState {
    fn new(header: DbHeader) -> Self {
        let active_slot = header.active_slot();

        CommitState {
            header: Mutex::new(header),
            snapshot: RwLock::new((
                active_slot.root_page_id.get(),
                active_slot.transaction_id.get(),
            )),
        }
    }

    pub(crate) fn active_slot(&self) -> CommitSlot {
        self.header.lock().unwrap().active_slot()
    }

    pub(crate) fn publish(
        &self,
        pages: &PageManager,
        root_page_id: PageId,
        root_page_checksum: u128,
        transaction_id: TransactionId,
    ) -> Result<()> {
        let mut header = self.header.lock().unwrap();
        let new_header = (*header).with_commit(
            root_page_id,
            root_page_checksum,
            transaction_id,
            pages.next_page_id(),
        );

        pages.write_page(Page::new(HEADER_PAGE_ID, new_header.encode_page().into()))?;
        pages.sync_data()?;

        let active_slot = new_header.active_slot();
        *header = new_header;
        let new_snapshot = (
            active_slot.root_page_id.get(),
            active_slot.transaction_id.get(),
        );
        *self.snapshot.write().unwrap() = new_snapshot;

        Ok(())
    }
}
