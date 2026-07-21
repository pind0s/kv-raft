use crate::btree::NodeKind;
use crate::btree::branch_node::{BranchBuilder, BranchPage, BranchPageMutator};
use crate::btree::leaf_node::{LeafBuilder, LeafPage, LeafPageMutator};
use crate::error::{Error, Result};
use crate::pages::PageId;
use crate::pages::checksum::checksum_page;
use crate::pages::page_io::PageIO;
use crate::{ReadTransaction, WriteTransaction};

struct PageSplit {
    separator_key: Vec<u8>,
    right_page_id: PageId,
    right_page_checksum: u128,
}

struct InsertionResult {
    replacement_page_id: PageId,
    replacement_page_checksum: u128,
    split: Option<PageSplit>,
}

enum NodePage<'a> {
    Leaf(LeafPage<'a>),
    Branch(BranchPage<'a>),
}

enum NodeMutator<'a> {
    Leaf(LeafPageMutator<'a>),
    Branch(BranchPageMutator<'a>),
}

impl<'a> NodePage<'a> {
    fn from_page(page: &'a [u8]) -> Result<Self> {
        match NodeKind::parse(page)? {
            NodeKind::Leaf => Ok(Self::Leaf(LeafPage::from_page(page)?)),
            NodeKind::Branch => Ok(Self::Branch(BranchPage::from_page(page)?)),
        }
    }
}

impl<'a> NodeMutator<'a> {
    fn from_page_mut(page: &'a mut [u8]) -> Result<Self> {
        match NodeKind::parse(page)? {
            NodeKind::Leaf => Ok(Self::Leaf(LeafPageMutator::from_page(page))),
            NodeKind::Branch => Ok(Self::Branch(BranchPageMutator::from_page(page))),
        }
    }
}

pub(crate) struct Tree;

impl Tree {
    pub(crate) fn verify_pages(
        pages: &PageIO,
        page_id: PageId,
        expected_checksum: u128,
    ) -> Result<()> {
        let page = pages.read_page(page_id)?;
        if checksum_page(page.data()) != expected_checksum {
            return Err(Error::DatabaseError(format!(
                "Checksum mismatch for page {page_id}"
            )));
        }

        match NodePage::from_page(page.data())? {
            NodePage::Leaf(_) => Ok(()),
            NodePage::Branch(branch_page) => {
                for entry in branch_page.entries() {
                    Self::verify_pages(pages, entry.child_page_id, entry.child_page_checksum)?;
                }

                Ok(())
            }
        }
    }

    pub(crate) fn get(read_transaction: &ReadTransaction, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut node_page_id = read_transaction.get_root_page_id();

        loop {
            let node_page = read_transaction.read_page(node_page_id)?;
            match NodePage::from_page(node_page.data())? {
                NodePage::Leaf(leaf_page) => {
                    return Ok(leaf_page.get_value(key).map(<[u8]>::to_vec));
                }
                NodePage::Branch(branch_page) => {
                    node_page_id = branch_page.child_page_id_for_key(key);
                }
            }
        }
    }

    pub(crate) fn insert(
        write_transaction: &WriteTransaction,
        key: &[u8],
        value: &[u8],
    ) -> Result<()> {
        let insertion_result = Self::insert_recursive(
            write_transaction,
            write_transaction.get_root_page(),
            key,
            value,
        )?;

        if let Some(split) = insertion_result.split {
            let branch_builder = BranchBuilder::new_root(
                insertion_result.replacement_page_id,
                insertion_result.replacement_page_checksum,
                &split.separator_key,
                split.right_page_id,
                split.right_page_checksum,
            );

            let mut new_root_page = write_transaction.allocate_page()?;
            let new_root_id = new_root_page.id();

            branch_builder.build_page_at_buf(new_root_page.data_mut());
            write_transaction.set_root_page(new_root_id, checksum_page(new_root_page.data()));
        } else {
            write_transaction.set_root_page(
                insertion_result.replacement_page_id,
                insertion_result.replacement_page_checksum,
            );
        }

        Ok(())
    }

    fn insert_recursive(
        write_transaction: &WriteTransaction,
        node_page_id: PageId,
        key: &[u8],
        value: &[u8],
    ) -> Result<InsertionResult> {
        let mut page = write_transaction.cow_page(node_page_id)?;
        let page_size = page.data().len();

        match NodeMutator::from_page_mut(page.data_mut())? {
            NodeMutator::Leaf(mut leaf_mutator) => {
                if leaf_mutator.fits(key, value)? {
                    leaf_mutator.insert(key, value)?;

                    return Ok(InsertionResult {
                        replacement_page_id: page.id(),
                        replacement_page_checksum: checksum_page(page.data()),
                        split: None,
                    });
                }

                let mut right_page = write_transaction.allocate_page()?;
                let mut left_scratch = vec![0; page_size]; //todo kinda stupid, but rn we can't split pages in place :(

                let separator_key = {
                    let mut leaf_builder = LeafBuilder::from_page(page.data())?;
                    leaf_builder.insert(key, value);
                    leaf_builder.build_split_pages(&mut left_scratch, right_page.data_mut())
                };

                page.copy_page_from(&left_scratch);
                let left_page_checksum = checksum_page(page.data());
                let right_page_checksum = checksum_page(right_page.data());

                Ok(InsertionResult {
                    replacement_page_id: page.id(),
                    replacement_page_checksum: left_page_checksum,
                    split: Some(PageSplit {
                        right_page_id: right_page.id(),
                        right_page_checksum,
                        separator_key,
                    }),
                })
            }

            NodeMutator::Branch(mut branch_mutator) => {
                let (child_index, child_page_id) = branch_mutator.child_page_for_key(key)?;

                let insertion_result =
                    Self::insert_recursive(write_transaction, child_page_id, key, value)?;

                branch_mutator.set_child_at_index(
                    child_index,
                    insertion_result.replacement_page_id,
                    insertion_result.replacement_page_checksum,
                )?;

                if insertion_result.split.is_none() {
                    return Ok(InsertionResult {
                        replacement_page_id: page.id(),
                        replacement_page_checksum: checksum_page(page.data()),
                        split: None,
                    });
                }

                let child_split = insertion_result.split.unwrap();
                if branch_mutator.fits(&child_split.separator_key)? {
                    branch_mutator.insert(
                        &child_split.separator_key,
                        child_split.right_page_id,
                        child_split.right_page_checksum,
                    )?;

                    return Ok(InsertionResult {
                        replacement_page_id: page.id(),
                        replacement_page_checksum: checksum_page(page.data()),
                        split: None,
                    });
                }

                let mut right_page = write_transaction.allocate_page()?;
                let mut left_scratch = vec![0; page_size];

                let separator_key = {
                    let mut branch_builder = BranchBuilder::from_page(page.data())?;
                    branch_builder.insert(
                        &child_split.separator_key,
                        child_split.right_page_id,
                        child_split.right_page_checksum,
                    );
                    branch_builder.build_split_pages(&mut left_scratch, right_page.data_mut())
                };
                page.copy_page_from(&left_scratch);

                let right_page_checksum = checksum_page(right_page.data());
                let left_page_checksum = checksum_page(page.data());

                Ok(InsertionResult {
                    replacement_page_id: page.id(),
                    replacement_page_checksum: left_page_checksum,
                    split: Some(PageSplit {
                        right_page_id: right_page.id(),
                        right_page_checksum,
                        separator_key,
                    }),
                })
            }
        }
    }
}
