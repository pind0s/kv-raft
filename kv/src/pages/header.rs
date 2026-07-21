use crate::error::{Error, Result};
use crate::pages::{INITIAL_ROOT_PAGE_ID, PageId};
use crate::transactions::TransactionId;
use zerocopy::byteorder::{LE, U128};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, U64};

const HEADER_MAGIC: [u8; 8] = *b"RUSTDB01";
const FIRST_COMMIT_SLOT: U64<LE> = U64::new(0);

#[derive(Debug, Copy, Clone, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
pub(crate) struct DbHeader {
    magic: [u8; 8],
    active_slot: U64<LE>,
    page_size: U64<LE>,
    commit_slots: [CommitSlot; 2],
}

impl DbHeader {
    pub(crate) fn parse(bytes: &[u8]) -> Result<Self> {
        let (header, _) = Self::read_from_prefix(bytes)?;

        if header.magic != HEADER_MAGIC {
            return Err(Error::DatabaseError(
                "invalid database header magic".to_string(),
            ));
        }

        if header.active_slot.get() > 1 {
            return Err(Error::DatabaseError(
                "invalid active commit slot".to_string(),
            ));
        }

        Ok(header)
    }

    pub(crate) fn initial(page_size: u64, root_page_checksum: u128) -> Self {
        let next_page_id = INITIAL_ROOT_PAGE_ID + 1;

        Self {
            magic: HEADER_MAGIC,
            active_slot: FIRST_COMMIT_SLOT,
            page_size: page_size.into(),
            commit_slots: [CommitSlot::new(
                INITIAL_ROOT_PAGE_ID,
                root_page_checksum,
                0,
                next_page_id,
            ); 2],
        }
    }

    pub(crate) fn encode_page(&self) -> Vec<u8> {
        let mut page = vec![0; self.page_size.get() as usize];
        page[..self.as_bytes().len()].copy_from_slice(self.as_bytes());
        page
    }

    pub(crate) fn page_size(&self) -> u64 {
        self.page_size.get()
    }

    pub(crate) fn with_commit(
        mut self,
        root_page_id: PageId,
        root_page_checksum: u128,
        transaction_id: TransactionId,
        next_page_id: PageId,
    ) -> Self {
        let inactive_slot = self.inactive_slot_index();
        self.commit_slots[inactive_slot] = CommitSlot::new(
            root_page_id,
            root_page_checksum,
            transaction_id,
            next_page_id,
        );
        self.switch_to_inactive_slot();

        self
    }

    pub(crate) fn active_slot(&self) -> CommitSlot {
        self.commit_slots[self.active_slot_index()]
    }

    pub(crate) fn inactive_slot(&self) -> CommitSlot {
        self.commit_slots[self.inactive_slot_index()]
    }

    pub(crate) fn switch_to_inactive_slot(&mut self) {
        self.active_slot = U64::new(self.active_slot.get() ^ 1);
    }

    fn active_slot_index(&self) -> usize {
        self.active_slot.get() as usize
    }

    fn inactive_slot_index(&self) -> usize {
        self.active_slot_index() ^ 1
    }
}

#[derive(Debug, Copy, Clone, FromBytes, IntoBytes, Immutable, KnownLayout)]
#[repr(C)]
pub(crate) struct CommitSlot {
    pub(crate) root_page_id: U64<LE>,
    root_page_checksum: U128<LE>,
    pub(crate) transaction_id: U64<LE>,
    pub(crate) next_page_id: U64<LE>,
}

impl CommitSlot {
    fn new(
        root_page_id: PageId,
        root_page_checksum: u128,
        transaction_id: TransactionId,
        next_page_id: PageId,
    ) -> Self {
        Self {
            root_page_id: root_page_id.into(),
            root_page_checksum: root_page_checksum.into(),
            transaction_id: transaction_id.into(),
            next_page_id: next_page_id.into(),
        }
    }

    pub(crate) fn root_page_checksum(&self) -> u128 {
        self.root_page_checksum.get()
    }
}
