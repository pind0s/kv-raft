use crate::btree::NodeKind;
use crate::btree::write_data_at_offset;
use crate::error::Result;
use zerocopy::byteorder::{LE, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

#[derive(Debug, Copy, Clone, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct LeafHeader {
    // todo everything u64 just for convenience
    node_type: U64<LE>,
    num_pairs: U64<LE>,
    kv_pairs_end_offset: U64<LE>,
}

#[derive(Debug, Copy, Clone, FromBytes, IntoBytes, Immutable, KnownLayout, Unaligned)]
#[repr(C)]
struct LeafEntryMetadata {
    key_size: U64<LE>,
    key_offset: U64<LE>,
    value_size: U64<LE>,
    value_offset: U64<LE>,
}

impl LeafEntryMetadata {
    fn kv_size(&self) -> usize {
        self.key_size() + self.value_size()
    }
}

pub(super) struct LeafPage<'a> {
    header: &'a LeafHeader,
    entries: &'a [LeafEntryMetadata],
    page: &'a [u8],
}

impl<'a> LeafPage<'a> {
    pub(super) fn from_page(page: &'a [u8]) -> Result<Self> {
        let (header, remaining_page) = LeafHeader::ref_from_prefix(page)?;
        let (entries, _) =
            <[LeafEntryMetadata]>::ref_from_prefix_with_elems(remaining_page, header.num_pairs())?;

        Ok(LeafPage {
            header,
            entries,
            page,
        })
    }

    fn entries(&self) -> impl ExactSizeIterator<Item = LeafEntry<'a>> + '_ {
        self.entries.iter().map(move |entry_metadata| {
            let key = &self.page[entry_metadata.key_offset()
                ..entry_metadata.key_offset() + entry_metadata.key_size()];
            let value = &self.page[entry_metadata.value_offset()
                ..entry_metadata.value_offset() + entry_metadata.value_size()];

            LeafEntry { key, value }
        })
    }

    fn search_entry_index(&self, key: &[u8]) -> std::result::Result<usize, usize> {
        self.entries.binary_search_by(|entry| {
            let key_offset = entry.key_offset();
            let key_size = entry.key_size();
            self.page[key_offset..key_offset + key_size].cmp(key)
        })
    }

    fn find_entry_index(&self, key: &[u8]) -> Option<usize> {
        self.search_entry_index(key).ok()
    }

    pub(super) fn get_value(&self, key: &[u8]) -> Option<&'a [u8]> {
        let index = self.find_entry_index(key)?;

        let entry = &self.entries[index];
        let value_offset = entry.value_offset();
        let value_size = entry.value_size();

        Some(&self.page[value_offset..value_offset + value_size])
    }

    fn num_pairs(&self) -> usize {
        self.header.num_pairs()
    }
}

impl LeafHeader {
    fn new(node_type: u64, num_pairs: u64, kv_pairs_end_offset: u64) -> Self {
        LeafHeader {
            node_type: node_type.into(),
            num_pairs: num_pairs.into(),
            kv_pairs_end_offset: kv_pairs_end_offset.into(),
        }
    }

    fn num_pairs(&self) -> usize {
        self.num_pairs.get() as usize
    }

    fn kv_pairs_end_offset(&self) -> usize {
        self.kv_pairs_end_offset.get() as usize
    }
}

impl LeafEntryMetadata {
    fn new(key_size: usize, key_offset: usize, value_size: usize, value_offset: usize) -> Self {
        LeafEntryMetadata {
            key_size: (key_size as u64).into(),
            key_offset: (key_offset as u64).into(),
            value_size: (value_size as u64).into(),
            value_offset: (value_offset as u64).into(),
        }
    }

    fn key_size(&self) -> usize {
        self.key_size.get() as usize
    }

    fn key_offset(&self) -> usize {
        self.key_offset.get() as usize
    }

    fn value_size(&self) -> usize {
        self.value_size.get() as usize
    }

    fn value_offset(&self) -> usize {
        self.value_offset.get() as usize
    }
}

struct LeafEntry<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

pub(super) struct LeafBuilder<'a> {
    kv_pairs: Vec<LeafEntry<'a>>,
}

impl<'a> LeafBuilder<'a> {
    pub(super) fn empty() -> Self {
        LeafBuilder {
            kv_pairs: Vec::new(),
        }
    }

    pub(super) fn from_page(old_page: &'a [u8]) -> Result<Self> {
        let page = LeafPage::from_page(old_page)?;
        let mut kv_pairs = Vec::with_capacity(page.num_pairs() + 1);
        kv_pairs.extend(page.entries());

        Ok(LeafBuilder { kv_pairs })
    }

    pub(super) fn insert(&mut self, key: &'a [u8], value: &'a [u8]) {
        match self.kv_pairs.binary_search_by(|pair| pair.key.cmp(key)) {
            Ok(index) => {
                self.kv_pairs[index].value = value;
            }
            Err(index) => {
                self.kv_pairs.insert(index, LeafEntry { key, value });
            }
        }
    }

    pub(super) fn build_split_pages(&self, left_page: &mut [u8], right_page: &mut [u8]) -> Vec<u8> {
        assert!(self.kv_pairs.len() >= 2);

        let mid = self.kv_pairs.len() / 2;
        let (left_entries, right_entries) = self.kv_pairs.split_at(mid);
        let separator_key = right_entries.first().unwrap().key.to_vec();

        Self::build_entries_at_buf(left_entries, left_page);
        Self::build_entries_at_buf(right_entries, right_page);

        separator_key
    }

    pub(super) fn build_page_at_buf(&self, page: &mut [u8]) {
        Self::build_entries_at_buf(&self.kv_pairs, page);
    }

    fn build_entries_at_buf(entries: &[LeafEntry<'_>], page: &mut [u8]) {
        let mut write_cursor = size_of::<LeafHeader>();
        let mut kv_data_cursor = page.len();

        for kv_pair in entries {
            let key_size = kv_pair.key.len();
            let value_size = kv_pair.value.len();

            let metadata_end = write_cursor + size_of::<LeafEntryMetadata>();
            debug_assert!(kv_data_cursor >= key_size + value_size);
            debug_assert!(metadata_end <= kv_data_cursor - key_size - value_size);

            kv_data_cursor -= value_size;
            write_data_at_offset(page, kv_pair.value, kv_data_cursor);

            kv_data_cursor -= key_size;
            write_data_at_offset(page, kv_pair.key, kv_data_cursor);

            let metadata = LeafEntryMetadata::new(
                key_size,
                kv_data_cursor,
                value_size,
                kv_data_cursor + key_size,
            );

            metadata.write_to_prefix(&mut page[write_cursor..]).unwrap();
            write_cursor = metadata_end;
        }

        let header = LeafHeader::new(
            NodeKind::Leaf as u64,
            entries.len() as u64,
            kv_data_cursor as u64,
        );

        header.write_to_prefix(page).unwrap();
    }
}

pub(super) struct LeafPageMutator<'a> {
    page: &'a mut [u8],
}

impl<'a> LeafPageMutator<'a> {
    pub(super) fn from_page(page: &'a mut [u8]) -> Self {
        LeafPageMutator { page }
    }

    fn reader(&self) -> Result<LeafPage<'_>> {
        LeafPage::from_page(&*self.page)
    }

    fn header(&self) -> Result<&LeafHeader> {
        let (header, _) = LeafHeader::ref_from_prefix(&self.page[..])?;
        Ok(header)
    }

    fn header_mut(&mut self) -> Result<&mut LeafHeader> {
        let (header, _) = LeafHeader::mut_from_prefix(&mut self.page[..])?;
        Ok(header)
    }

    fn entries(&self) -> Result<&[LeafEntryMetadata]> {
        let (header, remaining) = LeafHeader::ref_from_prefix(&self.page[..])?;
        let (entries, _) =
            <[LeafEntryMetadata]>::ref_from_prefix_with_elems(remaining, header.num_pairs())?;
        Ok(entries)
    }

    fn entries_mut(&mut self) -> Result<&mut [LeafEntryMetadata]> {
        let (header, remaining) = LeafHeader::mut_from_prefix(&mut self.page[..])?;
        let len = header.num_pairs();
        let (entries, _) = <[LeafEntryMetadata]>::mut_from_prefix_with_elems(remaining, len)?;
        Ok(entries)
    }

    // todo we should probably check if the key already exists and can fit in the existing space
    pub(super) fn fits(&self, key: &[u8], value: &[u8]) -> Result<bool> {
        let header = self.header()?;
        let kv_size = key.len() + value.len();
        let Some(key_offset) = header.kv_pairs_end_offset().checked_sub(kv_size) else {
            return Ok(false);
        };

        let metadata_end_offset =
            size_of::<LeafHeader>() + (header.num_pairs() + 1) * size_of::<LeafEntryMetadata>();
        Ok(metadata_end_offset <= key_offset)
    }

    pub(super) fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let search_result = self.reader()?.search_entry_index(key);

        match search_result {
            Ok(entry_index) => self.insert_existing_kv(entry_index, key, value),
            Err(insert_index) => self.insert_new_kv(insert_index, key, value),
        }
    }

    fn insert_existing_kv(&mut self, entry_index: usize, key: &[u8], value: &[u8]) -> Result<()> {
        let entries = self.entries()?;
        let old_entry = entries[entry_index];

        let old_kv_size = old_entry.kv_size();
        let new_kv_size = key.len() + value.len();

        let (key_offset, value_offset) = if old_kv_size >= new_kv_size {
            let key_offset = old_entry.key_offset();
            let value_offset = key_offset + key.len();
            self.write_kv(key, value, key_offset, value_offset);
            (key_offset, value_offset)
        } else {
            let (key_offset, value_offset) = self.append_kv(key, value)?;
            self.header_mut()?.kv_pairs_end_offset = (key_offset as u64).into();
            (key_offset, value_offset)
        };

        self.entries_mut()?[entry_index] =
            LeafEntryMetadata::new(key.len(), key_offset, value.len(), value_offset);

        Ok(())
    }

    fn insert_new_kv(&mut self, insert_index: usize, key: &[u8], value: &[u8]) -> Result<()> {
        let old_num_pairs = self.entries()?.len();
        let new_num_pairs = old_num_pairs + 1;
        let (key_offset, value_offset) = self.append_kv(key, value)?;
        let new_entry = LeafEntryMetadata::new(key.len(), key_offset, value.len(), value_offset);

        let header = self.header_mut()?;
        header.num_pairs = (new_num_pairs as u64).into();
        header.kv_pairs_end_offset = (key_offset as u64).into();

        self.insert_metadata(insert_index, old_num_pairs, new_entry)
    }

    fn append_kv(&mut self, key: &[u8], value: &[u8]) -> Result<(usize, usize)> {
        let key_offset = self.header()?.kv_pairs_end_offset() - key.len() - value.len();
        let value_offset = key_offset + key.len();

        self.write_kv(key, value, key_offset, value_offset);
        Ok((key_offset, value_offset))
    }

    fn write_kv(&mut self, key: &[u8], value: &[u8], key_offset: usize, value_offset: usize) {
        write_data_at_offset(self.page, key, key_offset);
        write_data_at_offset(self.page, value, value_offset);
    }

    fn insert_metadata(
        &mut self,
        insert_index: usize,
        old_num_pairs: usize,
        new_entry: LeafEntryMetadata,
    ) -> Result<()> {
        let entries = self.entries_mut()?;
        entries.copy_within(insert_index..old_num_pairs, insert_index + 1);
        entries[insert_index] = new_entry;
        Ok(())
    }
}
