use crate::btree::NodeKind;
use crate::btree::write_data_at_offset;
use crate::error::Result;
use zerocopy::byteorder::{LE, U64, U128};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[derive(Debug, Copy, Clone, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct BranchHeader {
    node_type: U64<LE>,
    num_entries: U64<LE>,
    key_data_end_offset: U64<LE>,
}

impl BranchHeader {
    fn new(num_entries: u64, key_data_end_offset: u64) -> Self {
        BranchHeader {
            node_type: (NodeKind::Branch as u64).into(),
            num_entries: num_entries.into(),
            key_data_end_offset: key_data_end_offset.into(),
        }
    }

    fn num_entries(&self) -> usize {
        self.num_entries.get() as usize
    }

    fn key_data_end_offset(&self) -> usize {
        self.key_data_end_offset.get() as usize
    }
}

#[derive(Debug, Copy, Clone, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct BranchEntryMetadata {
    key_size: U64<LE>,
    key_offset: U64<LE>,
    child_page_id: U64<LE>,
    child_page_checksum: U128<LE>,
}

impl BranchEntryMetadata {
    fn new(
        key_size: usize,
        key_offset: usize,
        child_page_id: u64,
        child_page_checksum: u128,
    ) -> Self {
        BranchEntryMetadata {
            key_size: (key_size as u64).into(),
            key_offset: (key_offset as u64).into(),
            child_page_id: child_page_id.into(),
            child_page_checksum: child_page_checksum.into(),
        }
    }

    fn key_size(&self) -> usize {
        self.key_size.get() as usize
    }

    fn key_offset(&self) -> usize {
        self.key_offset.get() as usize
    }

    fn child_page_id(&self) -> u64 {
        self.child_page_id.get()
    }

    fn child_page_checksum(&self) -> u128 {
        self.child_page_checksum.get()
    }
}

pub(super) struct BranchPage<'a> {
    #[allow(dead_code)]
    header: &'a BranchHeader,
    entries: &'a [BranchEntryMetadata],
    page: &'a [u8],
}

impl<'a> BranchPage<'a> {
    pub(super) fn from_page(page: &'a [u8]) -> Result<Self> {
        let (header, remaining_page) = BranchHeader::ref_from_prefix(page)?;
        let (entries, _) = <[BranchEntryMetadata]>::ref_from_prefix_with_elems(
            remaining_page,
            header.num_entries(),
        )?;

        Ok(BranchPage {
            header,
            entries,
            page,
        })
    }

    pub(super) fn entries(&self) -> impl ExactSizeIterator<Item = BranchEntry<'a>> + '_ {
        self.entries.iter().map(move |entry_metadata| BranchEntry {
            key: self.entry_key(entry_metadata),
            child_page_id: entry_metadata.child_page_id(),
            child_page_checksum: entry_metadata.child_page_checksum(),
        })
    }

    fn entry_key(&self, entry_metadata: &BranchEntryMetadata) -> &'a [u8] {
        let key_offset = entry_metadata.key_offset();
        let key_size = entry_metadata.key_size();
        &self.page[key_offset..key_offset + key_size]
    }

    fn search_entry_index(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        self.entries[1..]
            .binary_search_by(|entry| self.entry_key(entry).cmp(key))
            .map(|index| index + 1)
            .map_err(|index| index + 1)
    }

    fn child_index_for_key(&self, key: &[u8]) -> usize {
        self.entries[1..].partition_point(|entry| self.entry_key(entry) <= key)
    }

    pub(super) fn child_page_id_for_key(&self, key: &[u8]) -> u64 {
        let index = self.child_index_for_key(key);
        self.entries[index].child_page_id()
    }

    fn child_page_id_at_index(&self, index: usize) -> u64 {
        self.entries[index].child_page_id()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct BranchEntry<'a> {
    key: &'a [u8],
    pub(super) child_page_id: u64,
    pub(super) child_page_checksum: u128,
}

pub(super) struct BranchBuilder<'a> {
    // Entry 0 is the leftmost child; its key is ignored
    branch_entries: Vec<BranchEntry<'a>>,
}

impl<'a> BranchBuilder<'a> {
    pub(super) fn from_page(old_page: &'a [u8]) -> Result<Self> {
        let page = BranchPage::from_page(old_page)?;
        let mut branch_entries = Vec::with_capacity(page.entries.len() + 1);
        branch_entries.extend(page.entries());

        Ok(Self { branch_entries })
    }

    pub(super) fn new_root(
        left_page_id: u64,
        left_page_checksum: u128,
        separator_key: &'a [u8],
        right_page_id: u64,
        right_page_checksum: u128,
    ) -> Self {
        BranchBuilder {
            branch_entries: vec![
                BranchEntry {
                    key: b"",
                    child_page_id: left_page_id,
                    child_page_checksum: left_page_checksum,
                },
                BranchEntry {
                    key: separator_key,
                    child_page_id: right_page_id,
                    child_page_checksum: right_page_checksum,
                },
            ],
        }
    }

    pub(super) fn insert(&mut self, key: &'a [u8], child_page_id: u64, child_page_checksum: u128) {
        match self.branch_entries[1..].binary_search_by(|entry| entry.key.cmp(key)) {
            Ok(index) => {
                self.branch_entries[index + 1].child_page_id = child_page_id;
                self.branch_entries[index + 1].child_page_checksum = child_page_checksum;
            }

            Err(index) => {
                self.branch_entries.insert(
                    index + 1,
                    BranchEntry {
                        key,
                        child_page_id,
                        child_page_checksum,
                    },
                );
            }
        }
    }

    pub(super) fn build_split_pages(&self, left_page: &mut [u8], right_page: &mut [u8]) -> Vec<u8> {
        debug_assert!(self.branch_entries.len() >= 2);

        let mid = self.branch_entries.len() / 2;
        let (left_entries, right_entries) = self.branch_entries.split_at(mid);
        let separator_key = right_entries.first().unwrap().key.to_vec();

        Self::build_entries_at_buf(left_entries, left_page);
        Self::build_entries_at_buf(right_entries, right_page);

        separator_key
    }

    pub(super) fn build_page_at_buf(&self, page: &mut [u8]) {
        Self::build_entries_at_buf(&self.branch_entries, page);
    }

    fn build_entries_at_buf(entries: &[BranchEntry<'_>], page: &mut [u8]) {
        let mut write_cursor = size_of::<BranchHeader>();
        let mut key_data_cursor = page.len();

        for (index, entry) in entries.iter().enumerate() {
            let key: &[u8] = if index == 0 { b"" } else { entry.key };

            let metadata_end = write_cursor + size_of::<BranchEntryMetadata>();
            debug_assert!(key_data_cursor >= key.len());
            debug_assert!(metadata_end <= key_data_cursor - key.len());

            key_data_cursor -= key.len();
            write_data_at_offset(page, key, key_data_cursor);

            let metadata = BranchEntryMetadata::new(
                key.len(),
                key_data_cursor,
                entry.child_page_id,
                entry.child_page_checksum,
            );

            metadata.write_to_prefix(&mut page[write_cursor..]).unwrap();
            write_cursor = metadata_end;
        }

        let header = BranchHeader::new(entries.len() as u64, key_data_cursor as u64);
        header.write_to_prefix(page).unwrap();
    }
}

pub(super) struct BranchPageMutator<'a> {
    page: &'a mut [u8],
}

impl<'a> BranchPageMutator<'a> {
    pub(super) fn from_page(page: &'a mut [u8]) -> Self {
        BranchPageMutator { page }
    }

    fn reader(&self) -> Result<BranchPage<'_>> {
        BranchPage::from_page(&*self.page)
    }

    fn header(&self) -> Result<&BranchHeader> {
        let (header, _) = BranchHeader::ref_from_prefix(&self.page[..])?;
        Ok(header)
    }

    fn header_mut(&mut self) -> Result<&mut BranchHeader> {
        let (header, _) = BranchHeader::mut_from_prefix(&mut self.page[..])?;
        Ok(header)
    }

    fn entries(&self) -> Result<&[BranchEntryMetadata]> {
        let (header, remaining_page) = BranchHeader::ref_from_prefix(&self.page[..])?;
        let (entries, _) = <[BranchEntryMetadata]>::ref_from_prefix_with_elems(
            remaining_page,
            header.num_entries(),
        )?;
        Ok(entries)
    }

    fn entries_mut(&mut self) -> Result<&mut [BranchEntryMetadata]> {
        let (header, remaining_page) = BranchHeader::mut_from_prefix(&mut self.page[..])?;
        let (entries, _) = <[BranchEntryMetadata]>::mut_from_prefix_with_elems(
            remaining_page,
            header.num_entries(),
        )?;
        Ok(entries)
    }

    pub(super) fn fits(&self, key: &[u8]) -> Result<bool> {
        let header = self.header()?;
        let Some(key_offset) = header.key_data_end_offset().checked_sub(key.len()) else {
            return Ok(false);
        };

        let metadata_end_offset = size_of::<BranchHeader>()
            + (header.num_entries() + 1) * size_of::<BranchEntryMetadata>();
        Ok(metadata_end_offset <= key_offset)
    }

    pub(super) fn insert(
        &mut self,
        key: &[u8],
        child_page_id: u64,
        child_page_checksum: u128,
    ) -> Result<()> {
        let search_result = self.reader()?.search_entry_index(key);

        match search_result {
            Ok(entry_index) => {
                self.set_child_at_index(entry_index, child_page_id, child_page_checksum)
            }
            Err(insert_index) => {
                debug_assert!(self.fits(key)?);
                self.insert_new_entry(insert_index, key, child_page_id, child_page_checksum)
            }
        }
    }

    pub(super) fn child_page_for_key(&self, key: &[u8]) -> Result<(usize, u64)> {
        let reader = self.reader()?;
        let child_index = reader.child_index_for_key(key);
        Ok((child_index, reader.child_page_id_at_index(child_index)))
    }

    pub(super) fn set_child_at_index(
        &mut self,
        index: usize,
        child_page_id: u64,
        child_page_checksum: u128,
    ) -> Result<()> {
        let entry = &mut self.entries_mut()?[index];
        entry.child_page_id = child_page_id.into();
        entry.child_page_checksum = child_page_checksum.into();
        Ok(())
    }

    fn insert_new_entry(
        &mut self,
        insert_index: usize,
        key: &[u8],
        child_page_id: u64,
        child_page_checksum: u128,
    ) -> Result<()> {
        let old_num_entries = self.entries()?.len();
        let new_num_entries = old_num_entries + 1;
        let key_offset = self.append_key(key)?;
        let new_entry =
            BranchEntryMetadata::new(key.len(), key_offset, child_page_id, child_page_checksum);

        let header = self.header_mut()?;
        header.num_entries = (new_num_entries as u64).into();
        header.key_data_end_offset = (key_offset as u64).into();

        self.insert_metadata(insert_index, old_num_entries, new_entry)
    }

    fn append_key(&mut self, key: &[u8]) -> Result<usize> {
        let key_offset = self.header()?.key_data_end_offset() - key.len();
        write_data_at_offset(self.page, key, key_offset);
        Ok(key_offset)
    }

    fn insert_metadata(
        &mut self,
        insert_index: usize,
        old_num_entries: usize,
        new_entry: BranchEntryMetadata,
    ) -> Result<()> {
        let entries = self.entries_mut()?;
        entries.copy_within(insert_index..old_num_entries, insert_index + 1);
        entries[insert_index] = new_entry;
        Ok(())
    }
}
